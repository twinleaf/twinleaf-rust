//! Serving a running spec as a TIO device tree.
//!
//! One port carries everything. That is the sensors, mounted at `/0`,
//! `/1`, … by the twinleaf proxy, and beside them a device that this module
//! implements, which publishes the spec's outputs as streams and its params
//! as RPCs. Anything that can talk to a sensor can talk to it.
//!
//! The proxy has no place to put a device that lives in its own process, so
//! the device listens on a loopback port and the proxy mounts it like any
//! other. The samples to compute from are read back through the proxy, as a
//! client would read them. A sample therefore crosses loopback twice inside
//! this process before a client sees what was made of it.

use crate::lang::diag::{self, Diagnostic};
use crate::publish::{Identity, Publisher};
use crate::run::{RunError, RunOptions};
use crate::session::{Session, SetError};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use twinleaf::proto::log::{LogLevel, LogMessage};
use twinleaf::proto::rpc::{Method, RpcError};
use twinleaf::proto::SessionId;
use twinleaf::proxy::{HostOptions, Layout, Listen, Mount, Proxy};
use twinleaf::tio::packet::{DecodeError, Payload};
use twinleaf::tio::Packet;
use twinleaf::{Connection, DeviceRoute, RecvError, SampleBatch};
use twinleaf_device::data::metadata;
use twinleaf_device::rpc::Reply;

/// How often the device says it is alive, as a sensor does.
const HEARTBEAT: Duration = Duration::from_millis(200);

/// The most of the spec one `dsp.spec.read` returns or one `dsp.spec.write`
/// carries. A request holds its name and an offset beside it, and a reply at
/// most 498 bytes.
pub const SPEC_CHUNK: usize = 448;

/// The most text one log message holds.
const LOG_TEXT: usize = 480;

/// A sensor to mount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountSpec {
    pub url: String,
    /// Where clients see it.
    pub prefix: DeviceRoute,
    /// Which device behind the URL it is. The root, unless the URL is a hub.
    pub source: DeviceRoute,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServeOptions {
    pub mounts: Vec<MountSpec>,
    /// The port to serve on. Zero takes any free port.
    pub port: u16,
    /// Whether other machines may connect. Otherwise loopback only.
    pub all_interfaces: bool,
    pub run: RunOptions,
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("the spec does not check")]
    Spec(Vec<Diagnostic>),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("no route from 255 down is free for the outputs")]
    NoRoute,
}

/// The device that publishes a spec, listening on a loopback port for the
/// proxy that mounts it. Dropping it stops it.
///
/// It is two halves of a loop that something else closes. Its
/// [`url`](Self::url) is mounted in a device tree beside the sensors, and
/// [`read_from`](Self::read_from) is given a way to open that same tree, from
/// which it reads the sensors' samples. [`Server`] closes the loop with a
/// proxy of its own. An application that already has a proxy mounts the URL
/// there, so that each sensor is opened once.
pub struct Hosted {
    url: String,
    shared: Arc<Mutex<Device>>,
    /// Set to end the threads that outlive a connection.
    stop: Arc<AtomicBool>,
}

impl Drop for Hosted {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

impl Hosted {
    /// Checks `source` and starts the device that publishes it.
    pub fn start(source: String, run: RunOptions) -> Result<Self, ServeError> {
        let device = Device::new(source, run).map_err(ServeError::Spec)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("tcp://{}", listener.local_addr()?);
        let shared = Arc::new(Mutex::new(device));

        // The device must be answering before a proxy opens it.
        let stop = Arc::new(AtomicBool::new(false));
        let accepting = Arc::clone(&shared);
        let stopping = Arc::clone(&stop);
        listener.set_nonblocking(true)?;
        std::thread::Builder::new()
            .name("dsp-device".to_owned())
            .spawn(move || {
                while !stopping.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let _ = stream.set_nonblocking(false);
                            let shared = Arc::clone(&accepting);
                            std::thread::spawn(move || link(stream, &shared));
                        }
                        Err(_) => std::thread::sleep(Duration::from_millis(25)),
                    }
                }
            })?;
        Ok(Self { url, shared, stop })
    }

    /// The URL to mount the device by.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Starts reading the sensors' samples from the tree `open` connects to,
    /// which must be the tree this device is mounted in. The tree is opened
    /// again whenever it drops, until the device is dropped.
    pub fn read_from(
        &self,
        open: impl Fn() -> std::io::Result<Connection> + Send + 'static,
    ) -> std::io::Result<()> {
        let shared = Arc::clone(&self.shared);
        let stop = Arc::clone(&self.stop);
        std::thread::Builder::new()
            .name("dsp-intake".to_owned())
            .spawn(move || read_sensors(&open, &shared, &stop))?;
        Ok(())
    }
}

/// Where a tree's outputs go, which is 255, or the next route down that
/// nothing is mounted at.
pub fn output_route(mounted: &[DeviceRoute]) -> Option<DeviceRoute> {
    (0..=255u8)
        .rev()
        .map(|hop| {
            format!("/{hop}")
                .parse::<DeviceRoute>()
                .expect("a hop is a route")
        })
        .find(|route| !mounted.contains(route))
}

/// A spec being served on a port of its own, with the sensors it reads.
/// Dropping it stops serving.
pub struct Server {
    /// Present until the server is dropped.
    proxy: Option<Proxy>,
    endpoint: String,
    route: DeviceRoute,
    _hosted: Hosted,
}

impl Drop for Server {
    fn drop(&mut self) {
        // Stopping the proxy closes its connection to the device, which
        // ends that connection's threads. The device stops after it.
        self.proxy.take();
    }
}

impl Server {
    /// Checks `source`, starts the device that publishes it, and serves it
    /// with the sensors on one port.
    pub fn start(source: String, options: ServeOptions) -> Result<Self, ServeError> {
        let hosted = Hosted::start(source, options.run.clone())?;
        let prefixes: Vec<DeviceRoute> = options.mounts.iter().map(|mount| mount.prefix).collect();
        let route = output_route(&prefixes).ok_or(ServeError::NoRoute)?;

        let mut mounts: Vec<Mount> = options
            .mounts
            .iter()
            .map(|mount| Mount {
                url: mount.url.clone(),
                source: mount.source,
                prefix: mount.prefix,
            })
            .collect();
        mounts.push(Mount {
            url: hosted.url().to_owned(),
            source: DeviceRoute::root(),
            prefix: route,
        });
        let proxy = Proxy::host(HostOptions {
            listen: if options.all_interfaces {
                Listen::AllInterfaces
            } else {
                Listen::Loopback
            },
            port: options.port,
            default: Some(Layout { mounts }),
            name: format!("{} serve", options.run.library.command()),
            ..HostOptions::default()
        })?;
        let endpoint = proxy.endpoint();

        // A local URL opens the proxy's own layout: every sensor, and the
        // outputs, which the spec does not read.
        let url = endpoint.clone();
        hosted.read_from(move || Connection::open(&url))?;
        Ok(Self {
            proxy: Some(proxy),
            endpoint,
            route,
            _hosted: hosted,
        })
    }

    /// Where clients connect, as a URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Where the outputs are in the tree.
    pub fn route(&self) -> DeviceRoute {
        self.route
    }

    /// Blocks until the server stops.
    pub fn wait(&self) {
        if let Some(proxy) = &self.proxy {
            proxy.wait();
        }
    }
}

/// Reads every sensor batch from the tree and runs the spec on it, until
/// told to stop.
fn read_sensors(
    open: &dyn Fn() -> std::io::Result<Connection>,
    shared: &Mutex<Device>,
    stop: &AtomicBool,
) {
    let stopped = || stop.load(Ordering::SeqCst);
    while !stopped() {
        match open() {
            Ok(connection) => {
                let batches = connection.samples();
                while !stopped() {
                    match batches.recv_timeout(Duration::from_millis(200)) {
                        Ok(batch) => lock(shared).take(&batch),
                        // Nothing yet, or batches were dropped on the way.
                        // After a drop the next batch does not follow the
                        // last, which is a gap like any other.
                        Err(RecvError::Timeout | RecvError::Lagged(_)) => {}
                        Err(_) => break,
                    }
                }
            }
            Err(error) if !stopped() => {
                lock(shared).problem(format!("cannot reach the sensors: {error}"));
            }
            Err(_) => {}
        }
        if !stopped() {
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

fn lock(shared: &Mutex<Device>) -> MutexGuard<'_, Device> {
    // A panic elsewhere must not take the device down with it.
    shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One connection to the device, which is the proxy's.
fn link(stream: TcpStream, shared: &Arc<Mutex<Device>>) {
    let _ = stream.set_nodelay(true);
    let Ok(mut writer) = stream.try_clone() else {
        return;
    };
    let (id, session, sender, outbox) = {
        let mut device = lock(shared);
        let session = device.publisher.identity().session.value();
        let (id, sender, outbox) = device.open_link();
        (id, session, sender, outbox)
    };

    std::thread::spawn(move || {
        let heartbeat = Packet::heartbeat_session(session, DeviceRoute::root());
        let mut beat = Instant::now();
        loop {
            let packet = match outbox.recv_timeout(HEARTBEAT) {
                Ok(packet) => Some(packet),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            if packet.is_some_and(|packet| writer.write_all(packet.as_bytes()).is_err()) {
                break;
            }
            if beat.elapsed() >= HEARTBEAT {
                if writer.write_all(heartbeat.as_bytes()).is_err() {
                    break;
                }
                beat = Instant::now();
            }
        }
        let _ = writer.shutdown(Shutdown::Both);
    });

    let mut reader = stream;
    let mut received = Vec::new();
    let mut chunk = [0u8; 2048];
    'link: loop {
        match reader.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(count) => received.extend_from_slice(&chunk[..count]),
        }
        loop {
            match Packet::from_slice_prefix(&received) {
                Ok((packet, used)) => {
                    received.drain(..used);
                    if let Payload::RpcRequest(request) = packet.payload() {
                        let id = request.id.value();
                        let answer = lock(shared).call(request.method, request.args);
                        let reply = match answer {
                            Ok(value) => Packet::rpc_reply(id, &value, packet.route())
                                .unwrap_or_else(|_| {
                                    Packet::rpc_error(id, RpcError::Internal, packet.route())
                                }),
                            Err(code) => Packet::rpc_error(id, code, packet.route()),
                        };
                        let _ = sender.send(reply);
                    }
                }
                Err(DecodeError::NeedMore) => break,
                // The stream no longer falls on packets. Start over.
                Err(_) => break 'link,
            }
        }
    }
    lock(shared).links.retain(|(link, _)| *link != id);
    let _ = reader.shutdown(Shutdown::Both);
}

/// What a listed RPC does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Param(usize),
    Min(usize),
    Max(usize),
    SpecSize,
    SpecRead,
    SpecWrite,
    SpecApply,
}

struct Entry {
    name: String,
    /// What a client is told of the RPC's type and what it allows.
    meta: u16,
    action: Action,
}

/// A value with a type and a size.
const TYPED: u16 = 0x8000;
const READABLE: u16 = 0x0100;
const WRITABLE: u16 = 0x0200;
/// A double, which is eight bytes, in the high nibble, of type float.
const F64: u16 = 0x82;
/// An unsigned 32-bit integer.
const U32: u16 = 0x40;

/// The device that publishes a spec, with its streams, its RPCs and its log,
/// and without the sockets. [`Server`] puts it behind one.
pub struct Device {
    session: Session,
    publisher: Publisher,
    /// The RPCs a client can list, in the order it lists them.
    table: Vec<Entry>,
    /// A hash of the table, by which a client knows it has changed.
    hash: u32,
    /// The revision and build of the session the streams were described for.
    described: (u64, u64),
    /// The text `dsp.spec.write` has gathered so far.
    staged: Vec<u8>,
    /// Counts the changes made through RPCs.
    settings: u32,
    links: Vec<(u64, mpsc::Sender<Packet>)>,
    next_link: u64,
    /// The last problem reported, so that it is not reported over and over.
    last_problem: String,
    /// What the device is called, which is also how it signs what it
    /// writes on stderr.
    name: &'static str,
    booted: Instant,
}

impl Device {
    /// Creates the device for a spec, which must check.
    pub fn new(source: String, options: RunOptions) -> Result<Self, Vec<Diagnostic>> {
        // The device goes by the name of the build that serves it.
        let name = options.library.name();
        let session = Session::open(source, options, "the device")?;
        // A new session each start, so that a client sees a new boot.
        let session_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |since| since.subsec_nanos() ^ since.as_secs() as u32)
            | 1;
        let identity = Identity {
            name: name.to_owned(),
            serial: format!("DSP{session_id:08X}"),
            firmware: env!("CARGO_PKG_VERSION").to_owned(),
            session: SessionId::new(session_id),
        };
        let mut device = Self {
            session,
            publisher: Publisher::new(identity, DeviceRoute::root()),
            table: Vec::new(),
            hash: 0,
            described: (0, 0),
            staged: Vec::new(),
            settings: 1,
            links: Vec::new(),
            next_link: 0,
            last_problem: String::new(),
            name,
            booted: Instant::now(),
        };
        device.refresh_table();
        Ok(device)
    }

    /// The spec as it is running now.
    pub fn source(&self) -> &str {
        self.session.source()
    }

    /// Opens a connection to the device and returns what it sends. That is
    /// first that it is alive and what it is, then everything it publishes.
    pub fn attach(&mut self) -> mpsc::Receiver<Packet> {
        self.open_link().2
    }

    fn open_link(&mut self) -> (u64, mpsc::Sender<Packet>, mpsc::Receiver<Packet>) {
        let (sender, outbox) = mpsc::channel::<Packet>();
        let session = self.publisher.identity().session.value();
        let _ = sender.send(Packet::heartbeat_session(session, DeviceRoute::root()));
        for packet in self.publisher.metadata().unwrap_or_default() {
            let _ = sender.send(packet);
        }
        let id = self.next_link;
        self.next_link += 1;
        self.links.push((id, sender.clone()));
        self.announce();
        (id, sender, outbox)
    }

    /// Lists the RPCs the running spec gives the device.
    fn refresh_table(&mut self) {
        let mut table = Vec::new();
        for (index, param) in self.session.params().iter().enumerate() {
            let name = format!("dsp.{}", param.name);
            if param.range.is_some() {
                for (suffix, action) in [("min", Action::Min(index)), ("max", Action::Max(index))] {
                    table.push(Entry {
                        name: format!("{name}.{suffix}"),
                        meta: TYPED | READABLE | F64,
                        action,
                    });
                }
            }
            table.push(Entry {
                name,
                meta: TYPED | READABLE | WRITABLE | F64,
                action: Action::Param(index),
            });
        }
        let spec = [
            ("dsp.spec.size", TYPED | READABLE | U32, Action::SpecSize),
            // An offset in, text out; an offset and text in. Neither is one
            // typed value, so a client is told only that they exist.
            ("dsp.spec.read", 0, Action::SpecRead),
            ("dsp.spec.write", 0, Action::SpecWrite),
            ("dsp.spec.apply", TYPED, Action::SpecApply),
        ];
        table.extend(spec.map(|(name, meta, action)| Entry {
            name: name.to_owned(),
            meta,
            action,
        }));
        // FNV-1a over what a client is told.
        let mut hash = 0x811C_9DC5_u32;
        for entry in &table {
            for byte in entry
                .name
                .bytes()
                .chain(entry.meta.to_le_bytes())
                .chain([0])
            {
                hash = (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193);
            }
        }
        self.table = table;
        self.hash = hash;
    }

    /// Answers an RPC.
    pub fn call(&mut self, method: Method<'_>, args: &[u8]) -> Result<Vec<u8>, RpcError> {
        let action = match method {
            Method::ById(id) => self
                .table
                .get(usize::from(id.value()))
                .map(|entry| entry.action),
            Method::ByName(name) => match name {
                // What every device answers.
                b"dev.name" => return Ok(self.publisher.identity().name.as_bytes().to_vec()),
                b"dev.session" => {
                    return Ok(self
                        .publisher
                        .identity()
                        .session
                        .value()
                        .to_le_bytes()
                        .to_vec())
                }
                b"rpc.hash" => return Ok(self.hash.to_le_bytes().to_vec()),
                b"settings.version" => return Ok(self.settings.to_le_bytes().to_vec()),
                b"rpc.listinfo" if args.is_empty() => {
                    return Ok((self.table.len() as u16).to_le_bytes().to_vec())
                }
                b"rpc.listinfo" => {
                    let index =
                        u16::from_le_bytes(args.try_into().map_err(|_| RpcError::ArgsSize)?);
                    let entry = self.table.get(usize::from(index)).ok_or(RpcError::Range)?;
                    return Ok([&entry.meta.to_le_bytes()[..], entry.name.as_bytes()].concat());
                }
                b"rpc.info" => {
                    let entry = self
                        .table
                        .iter()
                        .find(|entry| entry.name.as_bytes() == args);
                    return Ok(entry.ok_or(RpcError::NotFound)?.meta.to_le_bytes().to_vec());
                }
                b"dev.metadata" => {
                    let mut reply = Reply::new();
                    metadata::reply(self.publisher.device(), &self.publisher, args, &mut reply)?;
                    return Ok(reply.to_vec());
                }
                _ => self
                    .table
                    .iter()
                    .find(|entry| entry.name.as_bytes() == name)
                    .map(|entry| entry.action),
            },
        };
        let read_only = |value: f64| {
            if args.is_empty() {
                Ok(value.to_le_bytes().to_vec())
            } else {
                Err(RpcError::ReadOnly)
            }
        };
        let offset = |args: &[u8]| -> Result<usize, RpcError> {
            let bytes = args.first_chunk::<4>().ok_or(RpcError::ArgsSize)?;
            Ok(u32::from_le_bytes(*bytes) as usize)
        };
        match action.ok_or(RpcError::NotFound)? {
            Action::Param(index) => {
                if !args.is_empty() {
                    let value =
                        f64::from_le_bytes(args.try_into().map_err(|_| RpcError::ArgsSize)?);
                    self.set(index, value)?;
                }
                // The reply to a write is the value now held.
                Ok(self.session.params()[index].value.to_le_bytes().to_vec())
            }
            Action::Min(index) => {
                read_only(self.session.params()[index].range.map_or(0.0, |r| r.0))
            }
            Action::Max(index) => {
                read_only(self.session.params()[index].range.map_or(0.0, |r| r.1))
            }
            Action::SpecSize => Ok((self.session.source().len() as u32).to_le_bytes().to_vec()),
            Action::SpecRead => {
                let text = self.session.source().as_bytes();
                let from = offset(args)?;
                if from > text.len() {
                    return Err(RpcError::Range);
                }
                Ok(text[from..(from + SPEC_CHUNK).min(text.len())].to_vec())
            }
            Action::SpecWrite => {
                // With no argument, how much has been taken. Otherwise the
                // text that goes at an offset, which must be where the last
                // write stopped. Offset zero starts over.
                if !args.is_empty() {
                    let at = offset(args)?;
                    if at == 0 {
                        self.staged.clear();
                    }
                    if at != self.staged.len() {
                        return Err(RpcError::Range);
                    }
                    self.staged.extend_from_slice(&args[4..]);
                }
                Ok((self.staged.len() as u32).to_le_bytes().to_vec())
            }
            Action::SpecApply => {
                if !args.is_empty() {
                    return Err(RpcError::ArgsSize);
                }
                self.apply()?;
                Ok(Vec::new())
            }
        }
    }

    /// Sets a param, which rewrites the spec.
    fn set(&mut self, index: usize, value: f64) -> Result<(), RpcError> {
        let name = self.session.params()[index].name.clone();
        match self.session.set_param(&name, value) {
            Ok(()) => {
                self.settings = self.settings.wrapping_add(1);
                self.refresh_table();
                self.log(LogLevel::INFO, &format!("dsp.{name} = {value}"));
                Ok(())
            }
            Err(error) => {
                self.log(LogLevel::WARNING, &error.to_string());
                Err(match error {
                    SetError::OutOfRange { .. } => RpcError::Range,
                    SetError::Unknown(_) => RpcError::NotFound,
                    SetError::NotANumber { .. } | SetError::Rejected { .. } => RpcError::Invalid,
                })
            }
        }
    }

    /// Takes the text written so far as the spec, if it checks.
    fn apply(&mut self) -> Result<(), RpcError> {
        let text = String::from_utf8(std::mem::take(&mut self.staged)).map_err(|_| {
            self.log(LogLevel::ERROR, "dsp.spec: the text written is not UTF-8");
            RpcError::Invalid
        })?;
        match self.session.replace(text.clone()) {
            Ok(()) => {
                self.settings = self.settings.wrapping_add(1);
                self.refresh_table();
                self.announce();
                Ok(())
            }
            Err(diagnostics) => {
                // What was wrong goes where a client already looks for what
                // a device has to say. The spec that was running still is.
                // Only what stopped it: a warning did not.
                let errors: Vec<Diagnostic> = diagnostics
                    .into_iter()
                    .filter(|diagnostic| diagnostic.severity == crate::lang::Severity::Error)
                    .collect();
                let rendered = diag::render_all(&errors, "dsp.spec", &text);
                self.log(LogLevel::ERROR, &rendered);
                Err(RpcError::Invalid)
            }
        }
    }

    /// Takes a batch from a sensor and publishes what the spec makes of it.
    pub fn take(&mut self, batch: &SampleBatch) {
        let offered = self
            .session
            .offer(batch)
            .and_then(|()| self.session.deliver());
        match offered {
            Ok(()) => {}
            Err(RunError::Spec(diagnostics)) => {
                // The spec does not fit what the sensors turned out to be.
                let rendered = diag::render_all(&diagnostics, "dsp.spec", self.session.source());
                self.problem(rendered);
                return;
            }
            Err(error) => {
                self.problem(format!("the graph stopped: {error}"));
                self.session.restart();
                return;
            }
        }
        if !self.session.is_running() {
            return;
        }
        let now = (self.session.revision(), self.session.build());
        if self.described != now {
            match self.publisher.configure(self.session.outputs()) {
                Ok(packets) => {
                    self.described = now;
                    self.send(packets);
                }
                Err(error) => {
                    self.problem(error.to_string());
                    return;
                }
            }
        }
        for emitted in self.session.drain() {
            match self.publisher.publish(&emitted) {
                Ok(packets) => self.send(packets),
                Err(error) => self.problem(error.to_string()),
            }
        }
    }

    /// Says what is running. It sends the spec's text as log messages, so
    /// that whatever records the outputs records what made them.
    fn announce(&mut self) {
        let text = format!(
            "twinleaf-dsp {}, spec revision {}:\n{}",
            env!("CARGO_PKG_VERSION"),
            self.session.revision(),
            self.session.source()
        );
        self.log(LogLevel::INFO, &text);
    }

    /// Reports a problem once, however often it recurs.
    fn problem(&mut self, text: String) {
        if text != self.last_problem {
            eprintln!("{}: {text}", self.name);
            self.log(LogLevel::ERROR, &text);
            self.last_problem = text;
        }
    }

    /// Sends text as log messages, whole lines to a message where they fit.
    fn log(&mut self, level: LogLevel, text: &str) {
        let uptime = self.booted.elapsed().as_millis() as u32;
        let mut packets = Vec::new();
        let mut message = String::new();
        let mut flush = |message: &mut String| {
            let mut buffer = [0u8; 512];
            let log = LogMessage {
                level,
                data: uptime,
                message: message.as_bytes(),
            };
            let written = log.write(&mut buffer);
            if let Some(Ok((packet, _))) = written.map(|n| Packet::from_slice_prefix(&buffer[..n]))
            {
                packets.push(packet);
            }
            message.clear();
        };
        for line in text.lines() {
            // A line too long for one message is cut where it must be.
            let mut line = line;
            while line.len() > LOG_TEXT {
                let cut = (0..=LOG_TEXT)
                    .rev()
                    .find(|at| line.is_char_boundary(*at))
                    .unwrap_or(0);
                if !message.is_empty() {
                    flush(&mut message);
                }
                message.push_str(&line[..cut]);
                flush(&mut message);
                line = &line[cut..];
            }
            if message.len() + line.len() + 1 > LOG_TEXT && !message.is_empty() {
                flush(&mut message);
            }
            if !message.is_empty() {
                message.push('\n');
            }
            message.push_str(line);
        }
        if !message.is_empty() {
            flush(&mut message);
        }
        self.send(packets);
    }

    /// Sends packets to every connection, and forgets those that have gone.
    fn send(&mut self, packets: Vec<Packet>) {
        self.links.retain(|(_, link)| {
            packets
                .iter()
                .all(|packet| link.send(packet.clone()).is_ok())
        });
    }
}

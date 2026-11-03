//! The link worker, a task that owns one device's port and shares it among
//! many ports of clients. [`new`] creates it, and it reports its state in a
//! [`Readiness`].
//!
//! The worker reopens a device whose port closes, fails with an I/O error,
//! or goes quiet. A serial or UDP device that has answered goes quiet after
//! 1 s without a packet. A device on a TCP connection goes quiet after 3 s
//! without one, counted from the start of the attempt that opened the
//! connection. A proxy of this build at the other end of a TCP connection
//! goes quiet after 10 s without one, counted from its answer to the open,
//! as the open waits for that proxy's own device. Its connection ends first
//! if it hears nothing for 5 s, the quiet time of the link's keep alive,
//! while a proxy that is serving beats each second. The worker follows a
//! serial device that a replug moved to another port. Its ports stay open
//! meanwhile. The link's engine hears the device drop out as
//! [`SensorDisconnected`](crate::tio::packet::LinkStatus::SensorDisconnected)
//! and come back as
//! [`SensorReconnected`](crate::tio::packet::LinkStatus::SensorReconnected).
//! A reopened port counts only once the device answers on it. One the device
//! never answered on is a failed attempt, and the ports hear nothing of it.
//! Attempts run beside the ports' traffic and are tried each second.
//!
//! The worker stops when its first open fails, when its command sender
//! drops, or when a reconnect gives up after the reconnect timeout. The engine
//! hears a reconnect that gives up as
//! [`FailedToReconnect`](crate::tio::packet::LinkStatus::FailedToReconnect).
//! Its task ends once the device's port has let go of the device. An open in
//! flight on a blocking thread runs to its end first. A TCP open in flight is
//! dropped.
//!
//! The worker negotiates the serial rate and hands what arrives to the link's
//! engine, the one port that hears every packet. Each other port takes what
//! one client's mount sends, and the answers to that client's requests go
//! straight to its mount, which queues them on the client's urgent queue.
//! The worker renumbers the RPCs the ports make and gives each reply back its
//! original id. A request the device does not
//! answer in time, or that a lost or restarted device leaves, is answered
//! with an RPC error. Control packets a port sends go to the proxy and never
//! reach the device.
//!
//! The worker logs its device's events under the target `proxy::<url>`, the
//! device's own log packets under `device::<route>` at their own level, and
//! the traffic [`DumpFlags`] asks for.

use super::engine::Mount;
use super::DumpFlags;
use crate::chan::StreamMap;
use crate::proto::heartbeat::Heartbeat;
use crate::proto::log::LogLevel;
use crate::proto::rpc::RpcError;
use crate::proto::{DeviceRoute, PacketType};
use crate::rt::{finish, sleep_until_some, BoxFuture, Supervisor};
use crate::session::{
    decode, older_build, Introduction, Layout, Request, Session, AUTO, CLIENT_KEEPALIVE,
    HELLO_TIMEOUT, MAX_RPC_TIMEOUT,
};
use crate::tio::packet::{self, Packet};
use crate::tio::transport;
use crate::tio::transport::Port as HardwarePort;
use crate::tio::transport::{ReceiveResult, TransportEvent};

use futures_util::{FutureExt, StreamExt};
use std::env;
use std::fs;
use std::io;
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_stream::StreamNotifyClose;

/// How long a new link may take to deliver its first packet.
pub(super) const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Returns the depth of the queue each port fills for the proxy.
/// `TWINLEAF_PROXY_INTERFACE_TX_BUFSIZE` may raise it above the default.
pub(super) fn client_tx_channel_size() -> usize {
    let min_size = transport::DEFAULT_TX_CHANNEL_SIZE;
    if let Ok(req) = env::var("TWINLEAF_PROXY_INTERFACE_TX_BUFSIZE") {
        std::cmp::max(req.parse().unwrap_or(0), min_size)
    } else {
        min_size
    }
}

/// Whether a link's device has answered. The worker sets it, and the
/// proxy's link table reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) enum Readiness {
    /// Nothing has arrived yet.
    #[default]
    Pending,
    /// The device sent its first packet.
    Ready,
    /// The first open failed, for this reason.
    Failed(String),
}

/// What a link asks of its worker, to adopt the port of a client's mount.
pub(super) struct OpenPort {
    /// Where the answers to the client's requests go.
    pub(super) mount: Arc<Mount>,
    /// What the client sends, at the device's routes.
    pub(super) from_client: mpsc::Receiver<Packet>,
}

/// The port of the link's engine. It hears every packet of the device, and
/// the answers to the engine's own requests.
const ENGINE: u64 = 0;

/// The two ends of the engine's port that the worker holds.
struct EnginePort {
    /// Hears every packet of the device, without limit.
    to_engine: mpsc::UnboundedSender<Packet>,
    /// What the engine sends.
    from_engine: mpsc::Receiver<Packet>,
}

/// A worker [`new`] created, and what its link holds of it.
pub(super) struct Started {
    /// The sender of the ports for the worker to adopt. The worker stops
    /// once every clone of it has dropped.
    pub(super) commands: mpsc::Sender<OpenPort>,
    /// The worker's readiness.
    pub(super) readiness: watch::Receiver<Readiness>,
    /// The session over the port of the link's engine.
    pub(super) engine: Session,
    /// Runs the worker, as its link's task.
    pub(super) run: BoxFuture<'static, ()>,
}

/// What opens a device's port. It takes the sender the port delivers to,
/// and returns a future of the open port and of what answers at its other
/// end.
pub(super) struct Opener {
    opens: Box<dyn Fn(crate::chan::Sender<ReceiveResult>) -> Opening + Send + Sync>,
    /// Whether the worker may drop an open in flight when it stops.
    droppable: bool,
}

/// A future of an open port and of what answers at its other end.
type Opening = BoxFuture<'static, io::Result<Reached>>;

/// A port an open opened, and what answers at its other end.
pub(super) struct Reached {
    pub(super) port: HardwarePort,
    pub(super) peer: PeerKind,
}

/// What answers at the other end of a device's port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PeerKind {
    /// The device, or a proxy from an older build, which forwards its
    /// device's packets as its own.
    Device,
    /// A proxy of this build, whose `auto` the open opened.
    Proxy,
}

/// A port alone is one a device answers on.
impl From<HardwarePort> for Reached {
    fn from(port: HardwarePort) -> Reached {
        Reached {
            port,
            peer: PeerKind::Device,
        }
    }
}

impl Opener {
    /// Returns an opener that opens with `open`. The worker lets an open in
    /// flight run to its end before it stops.
    pub(super) fn new<F, R>(
        open: impl Fn(crate::chan::Sender<ReceiveResult>) -> F + Send + Sync + 'static,
    ) -> Opener
    where
        F: std::future::Future<Output = io::Result<R>> + Send + 'static,
        R: Into<Reached>,
    {
        Opener {
            opens: Opener::boxed(open),
            droppable: false,
        }
    }

    /// Returns an opener that opens with `open`, whose open in flight the
    /// worker drops when it stops. Dropping the open must leave nothing open.
    pub(super) fn droppable<F, R>(
        open: impl Fn(crate::chan::Sender<ReceiveResult>) -> F + Send + Sync + 'static,
    ) -> Opener
    where
        F: std::future::Future<Output = io::Result<R>> + Send + 'static,
        R: Into<Reached>,
    {
        Opener {
            opens: Opener::boxed(open),
            droppable: true,
        }
    }

    /// Returns `open` as an opener's function, whose future resolves to what
    /// the open reached.
    #[allow(clippy::type_complexity)]
    fn boxed<F, R>(
        open: impl Fn(crate::chan::Sender<ReceiveResult>) -> F + Send + Sync + 'static,
    ) -> Box<dyn Fn(crate::chan::Sender<ReceiveResult>) -> Opening + Send + Sync>
    where
        F: std::future::Future<Output = io::Result<R>> + Send + 'static,
        R: Into<Reached>,
    {
        Box::new(move |results| Box::pin(open(results).map(|opened| opened.map(Into::into))))
    }

    /// Returns a future that opens a port delivering to `results`.
    pub(super) fn open(&self, results: crate::chan::Sender<ReceiveResult>) -> Opening {
        (self.opens)(results)
    }
}

/// The name a TCP link gives in its hello to a proxy at the other end.
const LINK_NAME: &str = "a proxy's link";

/// The ids of a TCP link's hello and of its open of a proxy's `auto`.
const HELLO_ID: u16 = 1;
const OPEN_ID: u16 = 2;

/// What answers at the other end of a TCP link.
enum Peer {
    /// A proxy of this build, whose `auto` the link opened.
    Proxy,
    /// A device, or a proxy from an older build, with what it sent first if
    /// it sent anything.
    Device(Option<ReceiveResult>),
}

/// Connects to the TCP peer at `url`, and returns its port, which delivers
/// to `results`, and what answers on it. The link says hello first. A proxy of this build answers,
/// and the link then opens its `auto` as a control client does, so that it
/// hears the proxy's link statuses for each route and keeps the connection
/// alive. Any other peer answers with something else, or with nothing
/// within [`HELLO_TIMEOUT`], and its packets are a device's. A
/// proxy from an older build forwards its device's packets, so it is such a
/// peer.
///
/// The task `link relay <url>` on `tasks` hands what the port receives to
/// `results`. No one holds it. It ends once the port's task has ended and
/// so closed what the relay reads, which the drop of the port brings about,
/// or once nothing receives from `results`.
///
/// # Errors
///
/// Returns an error if the connection fails, closes before the peer
/// answers, or a proxy at the other end refuses the hello or the open.
async fn open_tcp(
    url: &str,
    results: crate::chan::Sender<ReceiveResult>,
    tasks: &Supervisor,
) -> io::Result<Reached> {
    let (deliver, received) = crate::chan::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
    let port = HardwarePort::open(url, deliver, tasks).await?;
    let (peer, first) = match greet(&port, &received, url).await? {
        Peer::Proxy => (PeerKind::Proxy, None),
        Peer::Device(first) => (PeerKind::Device, first),
    };
    let proxied = peer == PeerKind::Proxy;
    tasks.spawn(format_args!("link relay {url}"), async move {
        if let Some(first) = first {
            if results.send_async(first).await.is_err() {
                return;
            }
        }
        while let Ok(result) = received.recv_async().await {
            if proxied && !relayed(&result) {
                continue;
            }
            if results.send_async(result).await.is_err() {
                break;
            }
        }
    });
    Ok(Reached { port, peer })
}

/// Says hello on `port` and tells from the answer, read from `received`,
/// what is at `url`. Opens the `auto` of a proxy that answers.
async fn greet(
    port: &HardwarePort,
    received: &crate::chan::Receiver<ReceiveResult>,
    url: &str,
) -> io::Result<Peer> {
    let (name, hello) = Request::Hello(Introduction::of_this_process(LINK_NAME)).encode();
    let request = Packet::proxy_request(name, &hello, HELLO_ID).map_err(io::Error::other)?;
    port.send_async(request).await.map_err(io::Error::other)?;
    let first = match tokio::time::timeout(HELLO_TIMEOUT, received.recv_async()).await {
        Err(_) => return Ok(Peer::Device(None)),
        Ok(Err(_)) => return Err(closed(url)),
        Ok(Ok(first)) => first,
    };
    let host = match &first {
        Ok(TransportEvent::Packet(pkt)) => match pkt.payload() {
            packet::Payload::ProxyRx(packet::ProxyRx::Reply(reply))
                if reply.req_id.value() == HELLO_ID =>
            {
                decode::<Introduction>(reply.value).map_err(|_| older_build(url))?
            }
            packet::Payload::ProxyRx(packet::ProxyRx::Error(error))
                if error.req_id.value() == HELLO_ID =>
            {
                return Err(refused(url, error.message));
            }
            _ => return Ok(Peer::Device(Some(first))),
        },
        _ => return Ok(Peer::Device(Some(first))),
    };
    log::debug!("the proxy at {url} is {host}");
    let (name, target) = Request::Open {
        target: AUTO.to_string(),
    }
    .encode();
    let request = Packet::proxy_request(name, &target, OPEN_ID).map_err(io::Error::other)?;
    port.send_async(request).await.map_err(io::Error::other)?;
    // The other proxy hangs up on a client that is quiet too long, and an
    // open may wait longer than that for its device.
    port.keep_alive(CLIENT_KEEPALIVE).await;
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        let event = tokio::time::timeout_at(deadline, received.recv_async())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("{url} did not open its auto in time"),
                )
            })?
            .map_err(|_| closed(url))?;
        let Ok(TransportEvent::Packet(pkt)) = event else {
            continue;
        };
        match pkt.payload() {
            packet::Payload::ProxyRx(packet::ProxyRx::Reply(reply))
                if reply.req_id.value() == OPEN_ID =>
            {
                let opened = String::from_utf8_lossy(reply.value);
                log::debug!("{url} is {host}, which opened {opened} for this link");
                break;
            }
            packet::Payload::ProxyRx(packet::ProxyRx::Error(error))
                if error.req_id.value() == OPEN_ID =>
            {
                return Err(refused(url, error.message));
            }
            packet::Payload::ProxyRx(packet::ProxyRx::Link(packet::LinkStatus::Stopped)) => {
                return Err(closed(url));
            }
            _ => {}
        }
    }
    Ok(Peer::Proxy)
}

/// Returns `true` if a link to a proxy passes `result` on to its worker. A
/// status of a subtree is passed on, and so is a sign of life, which keeps
/// the link alive. Replies are the link's own, and a stop of the proxy is
/// heard as the connection closes.
fn relayed(result: &ReceiveResult) -> bool {
    let Ok(TransportEvent::Packet(pkt)) = result else {
        return true;
    };
    match pkt.payload() {
        packet::Payload::ProxyRx(packet::ProxyRx::Link(packet::LinkStatus::Stopped)) => false,
        packet::Payload::ProxyRx(packet::ProxyRx::Link(_) | packet::ProxyRx::Alive) => true,
        packet::Payload::ProxyRx(_) => false,
        _ => true,
    }
}

/// Returns the error for a TCP peer at `url` that closed before it answered.
fn closed(url: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        format!("{url} closed the connection before it answered"),
    )
}

/// Returns the error for a proxy at `url` that refused a request, saying
/// `message`.
fn refused(url: &str, message: &[u8]) -> io::Error {
    let message = String::from_utf8_lossy(message);
    io::Error::other(format!("the proxy at {url} refused this link: {message}"))
}

/// How many commands may wait for a worker.
const COMMAND_LANE: usize = 64;

/// Creates the worker of the device at `url`, which `open` opens. A lost
/// device is retried for `reconnect_timeout`, or forever with `None`. The
/// worker logs the traffic `dump` asks for, and names the device's routes as
/// the proxy's `default` layout shows them. The session over the engine's
/// port runs its task on `tasks`, and the queue of what the engine hears has
/// no limit.
pub(super) fn new(
    url: &str,
    reconnect_timeout: Option<Duration>,
    dump: DumpFlags,
    default: watch::Receiver<Option<Layout>>,
    open: Opener,
    tasks: &Supervisor,
) -> Started {
    let (commands, command_queue) = mpsc::channel(COMMAND_LANE);
    let (readiness, watched) = watch::channel(Readiness::Pending);
    let (to_engine, heard) = mpsc::unbounded_channel();
    let (sent, from_engine) = mpsc::channel(client_tx_channel_size());
    let heard = UnboundedReceiverStream::new(heard);
    let engine = Session::over_port(sent, heard, format!("the state of {url}"), tasks);
    let worker = Worker {
        default: Some(default),
        ..Worker::new(
            url.to_string(),
            reconnect_timeout,
            command_queue,
            dump,
            readiness,
            open,
            EnginePort {
                to_engine,
                from_engine,
            },
        )
    };
    Started {
        commands,
        readiness: watched,
        engine,
        run: Box::pin(worker.run()),
    }
}

/// Returns the opener of the device at `url`. A TCP device connects as a
/// future, which the worker drops if it stops meanwhile. Any other device
/// opens in one blocking step on `tasks`. If that port does not open, the
/// step looks for a serial device that a replug moved to another port, and
/// opens that.
pub(super) fn opener(tasks: &Supervisor, url: &str) -> Opener {
    let tasks = tasks.clone();
    let url = url.to_string();
    if HardwarePort::opens_as_future(&url) {
        return Opener::droppable(move |results| {
            let (url, tasks) = (url.clone(), tasks.clone());
            async move {
                match url.starts_with("tcp") {
                    true => open_tcp(&url, results, &tasks).await,
                    false => HardwarePort::open(&url, results, &tasks)
                        .await
                        .map(Reached::from),
                }
            }
        });
    }
    // The key of a serial device, taken on the first attempt while the
    // device is likely present.
    let identity = Arc::new(OnceLock::new());
    Opener::new(move |results| {
        let (url, identity, on) = (url.clone(), identity.clone(), tasks.clone());
        let opening = tasks.spawn_blocking(move || {
            let key: &Option<String> = identity.get_or_init(|| self::identity(&url));
            match HardwarePort::new_on(&url, results.clone(), &on) {
                Ok(port) => Ok(port),
                Err(error) => match relocate(&url, key.as_deref()) {
                    Some(moved) => HardwarePort::new_on(&moved, results, &on),
                    None => Err(error),
                },
            }
        });
        async move { opening.await.map_err(io::Error::other)? }
    })
}

/// A state of the rate autonegotiation.
#[derive(Debug, Clone)]
enum RateChange {
    DoNothing,
    WaitingForSession,
    QueryDeviceRate,
    WaitingDeviceRate,
    SetDeviceRate,
    WaitingNewRate,
    RateChanged,
    GaveUp,
}

/// A device's port and the state of its rate negotiation.
struct Device {
    port: HardwarePort,
    /// What answers at the other end of the port.
    peer: PeerKind,
    /// What the port receives.
    results: crate::chan::RecvStream<'static, ReceiveResult>,
    rate_change_state: RateChange,
    /// When the port last received a packet or text. Before it has received
    /// either, it is when a proxy's open of its `auto` answered, or for any
    /// other peer when the attempt that opened the port started.
    last_rx: Instant,
    seen_since_connect: bool,
    last_session: Option<u32>,
    restarted: bool,
}

impl Device {
    /// Creates a device reached as `reached` says, whose results arrive on
    /// `results`, by an attempt to open it that started at `started`. It is
    /// called as the open ends.
    fn new(
        reached: Reached,
        results: crate::chan::Receiver<ReceiveResult>,
        started: Instant,
    ) -> Device {
        let Reached { port, peer } = reached;
        // Kickstart rate autonegotiation only if the port supports
        // changing rates and the target rate differs from the default.
        let mut rate_change_state = RateChange::DoNothing;
        if let Some(rates) = port.rate_info() {
            if rates.target_bps != rates.default_bps {
                rate_change_state = RateChange::WaitingForSession;
            }
        }
        // A proxy's answer to the open is a sign of life. The open itself
        // may wait for the proxy's own device for as long as
        // `READY_TIMEOUT`.
        let last_rx = match peer {
            PeerKind::Proxy => Instant::now(),
            PeerKind::Device => started,
        };
        Device {
            port,
            peer,
            results: results.into_stream(),
            rate_change_state,
            last_rx,
            seen_since_connect: false,
            last_session: None,
            restarted: false,
        }
    }

    /// Returns `true` if the device's rate is not negotiated.
    fn has_static_rate(&self) -> bool {
        matches!(self.rate_change_state, RateChange::DoNothing)
    }

    /// Returns `true` while the rate negotiation runs. It stays `true` after
    /// the higher rate is negotiated, while the new rate is on trial.
    fn needs_autonegotiation(&self) -> bool {
        !matches!(
            self.rate_change_state,
            RateChange::DoNothing | RateChange::GaveUp
        )
    }

    /// Returns `false` while the device's rate is changing. Packets for the
    /// device are held back until it returns `true`.
    fn safe_to_forward(&self) -> bool {
        !matches!(
            self.rate_change_state,
            RateChange::SetDeviceRate | RateChange::WaitingNewRate
        )
    }

    /// Returns how long the device may stay silent before the watchdog tears
    /// it down.
    fn liveness_timeout(&self) -> Duration {
        match (self.peer, self.port.kind()) {
            (PeerKind::Proxy, _) => LIVENESS_TIMEOUT_PROXY,
            (PeerKind::Device, transport::TransportKind::Tcp) => LIVENESS_TIMEOUT_TCP,
            (
                PeerKind::Device,
                transport::TransportKind::Serial | transport::TransportKind::Udp,
            ) => LIVENESS_TIMEOUT,
        }
    }

    /// Returns when the device counts as lost if nothing more arrives. A TCP
    /// port's clock runs from the moment [`Device::new`] sets, so a
    /// connection nothing ever answers on is lost too. Any other port has no
    /// deadline, and returns `None`, before the device has answered.
    fn liveness_deadline(&self) -> Option<Instant> {
        let tcp = matches!(self.port.kind(), transport::TransportKind::Tcp);
        (tcp || self.seen_since_connect).then(|| self.last_rx + self.liveness_timeout())
    }

    /// Returns when the port goes back to its default rate if nothing
    /// arrives at the new rate, or `None` while no new rate is on trial.
    fn rate_trial_deadline(&self) -> Option<Instant> {
        matches!(self.rate_change_state, RateChange::RateChanged).then(|| self.last_rx + RATE_TRIAL)
    }

    /// Returns the device's rate information. Panics if the port's rate is
    /// not settable.
    fn rates(&self) -> transport::RateInfo {
        self.port
            .rate_info()
            .expect("Rates requested for unsupported device")
    }

    /// Notes `result`, which the port received. Packets and text refresh
    /// the device's liveness. A heartbeat of the root device moves the rate
    /// negotiation on, and marks the device restarted when its session
    /// changes.
    fn received(&mut self, result: &ReceiveResult) {
        // Any received packet (text included) refreshes liveness, for every
        // device — the watchdog needs a timestamp regardless of rate state.
        if let Ok(TransportEvent::Packet(_) | TransportEvent::Text(_)) = result {
            self.last_rx = Instant::now();
        }
        if !self.has_static_rate() {
            if let Ok(TransportEvent::Packet(pkt)) = result {
                if let packet::Payload::Heartbeat(Heartbeat::Session { session, .. }) =
                    pkt.payload()
                {
                    let session = session.value();
                    if pkt.route().is_empty() {
                        // This is a heartbeat for the root sensor
                        let old_session = self.last_session.replace(session);
                        if let RateChange::WaitingForSession = self.rate_change_state {
                            self.rate_change_state = RateChange::QueryDeviceRate;
                        } else if (self.last_session != old_session) && old_session.is_some() {
                            // It has restarted, restart autonegotiation if needed.
                            self.rate_change_state = match self.rate_change_state {
                                RateChange::DoNothing => RateChange::DoNothing,
                                RateChange::WaitingForSession => RateChange::WaitingForSession, // never happens
                                _ => RateChange::QueryDeviceRate,
                            };
                            self.restarted = true;
                        }
                    }
                }
            }
        }
    }
}

enum RpcTarget {
    Port { port: u64, original_id: u16 },
    Internal(u16),
}

enum RpcOrigin {
    Port(u64),
    Internal,
}

#[derive(Debug, Clone, Copy)]
enum RpcFailure {
    Timeout,
    DeviceDisconnected,
    ProxyClosed,
}

impl RpcFailure {
    fn wire_error(self) -> RpcError {
        match self {
            Self::Timeout => RpcError::Timeout,
            Self::DeviceDisconnected | Self::ProxyClosed => RpcError::Undefined,
        }
    }
}

struct RpcMapEntry {
    target: RpcTarget,
    route: DeviceRoute,
    timeout: Instant,
}

/// The state of one device's link. [`new`] creates it, with the future that
/// runs it as the link's task.
struct Worker {
    url: String,
    reconnect_timeout: Option<Duration>,
    command_queue: mpsc::Receiver<OpenPort>,
    /// The target the worker logs under, `proxy::<url>`.
    log_target: String,
    /// The traffic to log.
    dump: DumpFlags,
    /// The worker's readiness, which the link watches.
    readiness: watch::Sender<Readiness>,
    /// What opens the device's port.
    open: Opener,
    /// The proxy's default layout, which says where its clients see this
    /// device, for the log. `None` for a worker on its own.
    default: Option<watch::Receiver<Option<Layout>>>,

    device: Option<Device>,
    /// The attempt in flight to reopen the device.
    attempt: Option<BoxFuture<'static, io::Result<Device>>>,

    ever_connected: bool,
    /// When reconnecting gives up, counted from when the device was lost. It
    /// is cleared once the device answers again. `None` while the device is
    /// not being reconnected, or if it is retried forever.
    reconnect_deadline: Option<Instant>,
    /// When the next attempt to reopen the device is due.
    next_attempt: Instant,

    /// The number to give the next port.
    next_port: u64,
    /// Hears every packet of the device.
    to_engine: mpsc::UnboundedSender<Packet>,
    /// The mount of each client's port.
    mounts: BTreeMap<u64, Arc<Mount>>,
    /// Packet sources report their port ID and notify when the port closes.
    inputs: StreamMap<u64, StreamNotifyClose<ReceiverStream<Packet>>>,

    next_rpc_id: u16,
    rpc_map: HashMap<u16, RpcMapEntry>,
    rpc_timeouts: BTreeMap<Instant, BTreeSet<u16>>,
}

/// How long a device on a serial or UDP port that has already sent a packet
/// may stay silent before its port is torn down and reopened.
const LIVENESS_TIMEOUT: Duration = Duration::from_millis(1000);

/// How long a device on a TCP connection may stay silent. It allows one RTO
/// plus slack. The silence counts from the start of the attempt to open the
/// connection, so the connect, the hello and the wait for the first packet
/// together take at most this long.
const LIVENESS_TIMEOUT_TCP: Duration = Duration::from_millis(3000);

/// How long a proxy of this build at the other end of a TCP connection may
/// stay silent. The silence counts from the proxy's answer to the open of
/// its `auto`, as that open may itself take up to [`READY_TIMEOUT`]
/// while the proxy waits for its own device.
///
/// The connection ends sooner on its own. The link keeps it alive as a
/// client does, with [`CLIENT_KEEPALIVE`], whose transport ends a
/// connection that hears nothing for 5 s, and the worker then loses the
/// device as on any failed port. The proxy beats each second it sends
/// nothing else, so a proxy that is serving is never silent that long.
const LIVENESS_TIMEOUT_PROXY: Duration = Duration::from_secs(10);

/// How long the worker waits after an attempt to reopen a device fails, or
/// opens a port the device never answers on, before the next attempt.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

/// How long a port at a new rate may go without receiving before it goes
/// back to its default rate.
const RATE_TRIAL: Duration = Duration::from_millis(1000);

static QUERY_RATE_RPC_ID: u16 = 0x101;
static SET_RATE_RPC_ID: u16 = 0x102;

impl Worker {
    /// Creates the worker of the device at `url`, which `open` opens. It
    /// hands the engine every packet of the device, and the answers to the
    /// engine's requests, through `engine`. It adopts the ports
    /// `command_queue` brings, reports its state in `readiness`, and stops
    /// once every command sender has dropped.
    fn new(
        url: String,
        reconnect_timeout: Option<Duration>,
        command_queue: mpsc::Receiver<OpenPort>,
        dump: DumpFlags,
        readiness: watch::Sender<Readiness>,
        open: Opener,
        engine: EnginePort,
    ) -> Worker {
        let mut inputs = StreamMap::new();
        let from_engine = StreamNotifyClose::new(ReceiverStream::new(engine.from_engine));
        inputs.insert(ENGINE, from_engine);
        Worker {
            log_target: format!("proxy::{url}"),
            url,
            reconnect_timeout,
            command_queue,
            dump,
            readiness,
            open,
            device: None,
            attempt: None,
            ever_connected: false,
            reconnect_deadline: None,
            next_attempt: Instant::now(),
            // Start from port 1, as 0 is the engine's.
            next_port: 1,
            to_engine: engine.to_engine,
            mounts: BTreeMap::new(),
            inputs,
            next_rpc_id: 0,
            rpc_map: HashMap::new(),
            rpc_timeouts: BTreeMap::new(),
            default: None,
        }
    }

    /// The route at which a client of the proxy's default layout sees `route`
    /// of this device. A device the layout does not mount there is named by
    /// its URL and the route on it.
    fn shown(&self, route: DeviceRoute) -> String {
        let default = self.default.as_ref().map(|default| default.borrow());
        let mounted = default
            .as_deref()
            .and_then(Option::as_ref)
            .into_iter()
            .flat_map(|layout| &layout.mounts)
            .filter(|mount| mount.url == self.url)
            .filter_map(|mount| {
                let relative = mount.source.relative_route(&route).ok()?;
                Some((
                    mount.source.len(),
                    mount.prefix.absolute_route(&relative).ok()?,
                ))
            })
            .max_by_key(|(depth, _)| *depth);
        match mounted {
            Some((_, shown)) => shown.to_string(),
            None => format!("{} {route}", self.url),
        }
    }

    /// Returns a future that opens the device's port.
    fn open_device(&self) -> BoxFuture<'static, io::Result<Device>> {
        let (sender, results) = crate::chan::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
        let started = Instant::now();
        let opening = self.open.open(sender);
        Box::pin(async move { Ok(Device::new(opening.await?, results, started)) })
    }

    /// Drops the port after it closed, failed, or went quiet. The ports
    /// stay open, and the worker reopens the device until the reconnect
    /// timeout passes. If the device had answered on the port, every port
    /// hears that it was lost. If not, the ports hear nothing, and the next
    /// attempt is put off as after a failed open.
    fn lose_device(&mut self) {
        let Some(device) = self.device.take() else {
            return;
        };
        let now = Instant::now();
        if self.reconnect_deadline.is_none() {
            self.reconnect_deadline = self.reconnect_timeout.map(|timeout| now + timeout);
        }
        if device.seen_since_connect {
            log::warn!(target: &self.log_target, "Sensor disconnected");
            self.status_to_engine(packet::LinkStatus::SensorDisconnected);
        } else {
            log::debug!("lost {} before it answered", self.url);
            self.next_attempt = now + RECONNECT_INTERVAL;
        }
    }

    /// Adopts the port of a client's mount that `open` brings.
    fn adopt(&mut self, open: OpenPort) {
        let port = self.next_port;
        self.next_port += 1;
        let from_client = ReceiverStream::new(open.from_client);
        self.inputs
            .insert(port, StreamNotifyClose::new(from_client));
        self.mounts.insert(port, open.mount);
        log::trace!(target: &self.log_target, "Port {port} opened");
    }

    /// Hands `pkt`, the answer to a request of `port`, to the port. The
    /// engine takes it in order with every packet of the device. A client's
    /// mount queues it ahead of the client's stream, as the client matches an
    /// answer by its id.
    fn answer(&self, port: u64, pkt: Packet) {
        match self.mounts.get(&port) {
            Some(mount) => mount.urgent(&pkt),
            None if port == ENGINE => self.to_engine(pkt),
            None => log::trace!(target: &self.log_target, "Port {port} left before its answer"),
        }
    }

    /// Hands `pkt` to the link's engine, which takes every packet of the
    /// device in the order the worker hands them over.
    fn to_engine(&self, pkt: Packet) {
        let _ = self.to_engine.send(pkt);
    }

    fn rpc_restore(&mut self, wire_id: u16, route: &DeviceRoute) -> Option<RpcMapEntry> {
        let remap = match self.rpc_map.remove(&wire_id) {
            None => {
                return None;
            }
            Some(r) => r,
        };
        if remap.route != *route {
            self.rpc_map.insert(wire_id, remap);
            return None;
        }
        if let Some(ids) = self.rpc_timeouts.get_mut(&remap.timeout) {
            ids.remove(&wire_id);
            if ids.is_empty() {
                self.rpc_timeouts.remove(&remap.timeout);
            }
        } else {
            log::warn!("Failed to find RPC timeout in map");
        }
        Some(remap)
    }

    fn next_wire_rpc_id(&mut self) -> Option<u16> {
        for _ in 0..=u16::MAX {
            let wire_id = self.next_rpc_id;
            self.next_rpc_id = self.next_rpc_id.wrapping_add(1);
            if !self.rpc_map.contains_key(&wire_id) {
                return Some(wire_id);
            }
        }
        None
    }

    /// Forwards a packet from port `port` to the device. Drops a control
    /// packet, and answers a request the device cannot be sent with an RPC
    /// error.
    async fn port_packet(&mut self, port: u64, pkt: Packet) {
        // A control packet talks to the proxy itself, never to a device.
        // Type 64 is also USER, which apps may send, so it is forwarded like
        // any other packet.
        if matches!(pkt.ptype(), PacketType::PROXY_TX | PacketType::PROXY_RX) {
            log::debug!("dropping a control packet from port {port}: {pkt:?}");
            return;
        }
        let reply_target = match pkt.payload() {
            packet::Payload::RpcRequest(req) => Some((pkt.route(), req.id.value())),
            _ => None,
        };
        if let Err(error) = self.forward_to_device(pkt, RpcOrigin::Port(port)).await {
            let (route, id) = reply_target.expect("only RPC requests produce forwarding errors");
            self.answer(port, Packet::rpc_error(id, error, route));
        }
    }

    // RPC failures are returned so the caller can notify the requesting port.
    async fn forward_to_device(
        &mut self,
        mut pkt: Packet,
        origin: RpcOrigin,
    ) -> Result<(), RpcError> {
        let mut mapped: Option<(u16, Instant)> = None;
        let from_port = match &origin {
            RpcOrigin::Port(port) => Some(*port),
            RpcOrigin::Internal => None,
        };
        // Decide from the borrowed view before rewriting the packet's bytes.
        let request = match pkt.payload() {
            packet::Payload::RpcRequest(req) => Some(req.id.value()),
            _ => None,
        };
        if let Some(original_id) = request {
            let Some(wire_id) = self.next_wire_rpc_id() else {
                return Err(RpcError::NoBufs);
            };
            let (target, timeout) = match origin {
                RpcOrigin::Port(port) => (
                    RpcTarget::Port { port, original_id },
                    // The port's own deadline comes first. This one only
                    // frees the wire id of a request never answered.
                    Instant::now() + MAX_RPC_TIMEOUT,
                ),
                RpcOrigin::Internal => (
                    RpcTarget::Internal(original_id),
                    Instant::now() + Duration::from_secs(1),
                ),
            };
            self.rpc_map.insert(
                wire_id,
                RpcMapEntry {
                    target,
                    route: pkt.route(),
                    timeout,
                },
            );
            if let Some(port) = from_port {
                log::trace!(
                    target: &self.log_target,
                    "RPC {original_id} of port {port} goes out as {wire_id}"
                );
            }
            pkt = pkt.with_rpc_id(wire_id);
            mapped = Some((wire_id, timeout));
        } else if from_port.is_none() {
            return Err(RpcError::Malformed);
        }
        if let Some(dev) = &self.device {
            if let Ok(()) = dev.port.send_async(pkt).await {
                if let Some((rpc_id, timeout)) = mapped {
                    self.rpc_timeouts.entry(timeout).or_default().insert(rpc_id);
                }
                return Ok(());
            }
        }
        // If we got here, the packet was not sent. avoid erroring out since if
        // there is something wrong with the device we'll notice in the main
        // loop soon but remove the rpc from the map and send back an error to
        // the port.
        if let Some((rpc_id, _)) = mapped {
            self.rpc_map
                .remove(&rpc_id)
                .expect("Unexpected missing timeout set");
            Err(RpcError::Undefined)
        } else {
            Ok(())
        }
    }

    fn status_to_engine(&self, status: packet::LinkStatus) {
        self.to_engine(Packet::link(status));
    }

    /// Handles everything the device's port has received so far, without
    /// waiting.
    async fn receive_device(&mut self) {
        while let Some(result) = self
            .device
            .as_mut()
            .and_then(|device| device.results.next().now_or_never().flatten())
        {
            self.device_received(result).await;
        }
    }

    /// Handles `result`, which the device's port received. An I/O error or
    /// a disconnect loses the device.
    async fn device_received(&mut self, result: ReceiveResult) {
        let Some(device) = self.device.as_mut() else {
            return;
        };
        device.received(&result);
        match result {
            // A proxy's sign of life keeps the link alive, which `received`
            // noted. It is no answer of the device, and no port hears it.
            Ok(TransportEvent::Packet(pkt))
                if matches!(
                    pkt.payload(),
                    packet::Payload::ProxyRx(packet::ProxyRx::Alive)
                ) => {}
            Ok(TransportEvent::Packet(pkt)) => {
                // First packet since (re)connect: the device is live, announce it.
                let first_packet = !std::mem::replace(&mut device.seen_since_connect, true);
                if first_packet {
                    self.reconnect_deadline = None;
                    if self.ever_connected {
                        log::info!(target: &self.log_target, "Sensor reconnected");
                        self.status_to_engine(packet::LinkStatus::SensorReconnected);
                    } else {
                        // Initial connect needs no wire status.
                        self.ever_connected = true;
                        log::trace!(target: &self.log_target, "Sensor connected");
                        self.readiness.send_replace(Readiness::Ready);
                    }
                }
                log_device_packet(&pkt, self.dump, || self.shown(pkt.route()));
                self.dispatch_device_packet(pkt).await;
            }
            Ok(TransportEvent::Text(text)) => {
                log::info!(target: &self.log_target, "Text: {text}");
            }
            Ok(TransportEvent::RejectedFrame(perror)) => {
                log::debug!(target: &self.log_target, "Protocol error: {perror:?}");
            }
            // A port that failed is lost as one that closed is, and the
            // device is reopened.
            Err(err) => {
                log::debug!("lost {}: {err}", self.url);
                self.lose_device();
            }
        }
    }

    async fn dispatch_device_packet(&mut self, pkt: Packet) {
        let route = pkt.route();
        let Some(wire_id) = (match pkt.payload() {
            packet::Payload::RpcReply(reply) => Some(reply.req_id.value()),
            packet::Payload::RpcError(error) => Some(error.req_id.value()),
            _ => None,
        }) else {
            return self.to_engine(pkt);
        };

        let Some(remap) = self.rpc_restore(wire_id, &route) else {
            log::trace!(target: &self.log_target, "No request waits for reply {wire_id}");
            return;
        };
        match remap.target {
            RpcTarget::Internal(original_id) => match pkt.payload() {
                packet::Payload::RpcReply(reply) => {
                    self.internal_rpc_reply(original_id, reply.value).await
                }
                packet::Payload::RpcError(error) => {
                    self.internal_rpc_error(RpcError::from(error.code))
                }
                _ => unreachable!(),
            },
            RpcTarget::Port { port, original_id } => {
                log::trace!(
                    target: &self.log_target,
                    "Reply {wire_id} goes to port {port} as {original_id}"
                );
                self.answer(port, pkt.with_rpc_id(original_id));
            }
        }
    }

    /// Fails every request whose timeout has come by `until`, or every
    /// request with `None`. Each port gets an RPC error for `failure` in
    /// place of its reply. It times requests out, and fails those still
    /// waiting when the device disconnects or restarts.
    fn dispatch_rpc_errors(&mut self, failure: RpcFailure, until: Option<Instant>) {
        let mut to_remove = Vec::new();
        let mut failed = Vec::new();
        for (timeout, rpc_ids) in self.rpc_timeouts.iter() {
            if let Some(timeout_bound) = until {
                if *timeout > timeout_bound {
                    break;
                }
            }
            to_remove.push(*timeout);
            for rpc_id in rpc_ids {
                log::trace!(target: &self.log_target, "Request {rpc_id} failed: {failure:?}");
                let remap = self
                    .rpc_map
                    .remove(rpc_id)
                    .expect("RPC ID from timeout missing in main map");
                failed.push(remap);
            }
        }
        for timeout in to_remove {
            self.rpc_timeouts.remove(&timeout);
        }
        for remap in failed {
            match remap.target {
                RpcTarget::Port { port, original_id } => {
                    let error = Packet::rpc_error(original_id, failure.wire_error(), remap.route);
                    self.answer(port, error);
                }
                RpcTarget::Internal(_) => {
                    self.internal_rpc_error(failure.wire_error());
                }
            }
        }
    }

    /// Fails every request whose timeout has come.
    fn process_rpc_timeouts(&mut self) {
        self.dispatch_rpc_errors(RpcFailure::Timeout, Some(Instant::now()));
    }

    async fn send_internal_rpc(
        &mut self,
        name: &str,
        arg: &[u8],
        id: u16,
        route: DeviceRoute,
    ) -> Result<(), RpcError> {
        let pkt = Packet::rpc_request(name, arg, id, route).map_err(|_| RpcError::ArgsSize)?;
        self.forward_to_device(pkt, RpcOrigin::Internal).await
    }

    /// Handles a reply to an RPC the worker issued itself. Panics if `id` is
    /// not one of the worker's own request ids.
    async fn internal_rpc_reply(&mut self, id: u16, value: &[u8]) {
        fn get_rate_vars(worker: &Worker) -> Option<(RateChange, u32)> {
            if let Some(dev) = worker.device.as_ref() {
                dev.port
                    .rate_info()
                    .map(|rate_info| (dev.rate_change_state.clone(), rate_info.target_bps))
            } else {
                None
            }
        }

        if id == QUERY_RATE_RPC_ID {
            if let Some((RateChange::WaitingDeviceRate, target)) = get_rate_vars(self) {
                let next_state = if let Ok(raw) = <[u8; 4]>::try_from(value) {
                    let value = u32::from_le_bytes(raw);
                    let error = (((target as f64) - (value as f64)) / (value as f64)).abs();
                    if value == 0 || error > 0.015 {
                        log::trace!(
                            target: &self.log_target,
                            "The device offers {value} bps for {target}, so the rate stays"
                        );
                        RateChange::GaveUp
                    } else {
                        log::trace!(target: &self.log_target, "The device supports {value} bps");
                        RateChange::SetDeviceRate
                    }
                } else {
                    log::trace!(target: &self.log_target, "The device's rate reply did not decode");
                    RateChange::GaveUp
                };
                self.device.as_mut().expect("").rate_change_state = next_state;
                return;
            }
        } else if id == SET_RATE_RPC_ID {
            if let Some((RateChange::WaitingNewRate, target)) = get_rate_vars(self) {
                log::trace!(target: &self.log_target, "Setting the port to {target} bps");
                let dev = self.device.as_mut().expect("");
                let next_state = match dev.port.set_rate(target).await {
                    Ok(_) => RateChange::RateChanged,
                    Err(error) => {
                        log::trace!(target: &self.log_target, "The port refused the rate: {error}");
                        RateChange::GaveUp
                    }
                };
                dev.rate_change_state = next_state;
                return;
            }
        } else {
            // Note: internal RPCs still get remapped with all other RPCs,
            // so this ID does not come from the device itself, but from the
            // proxy remapping, and it should never be an unexpected value.
            panic!("Unexpected reply ID to internal RPC: {}", id)
        }

        log::debug!(
            "Unexpected internal rpc reply 0x{:x} in state {:?}",
            id,
            get_rate_vars(self)
        );
    }

    fn internal_rpc_error(&mut self, error: RpcError) {
        // We could handle this better, but just keep the device to the default speed until the port is reset
        log::trace!(target: &self.log_target, "The rate change failed: {error}");
        if let Some(dev) = self.device.as_mut() {
            dev.rate_change_state = RateChange::GaveUp;
        }
    }

    async fn autonegotiation(&mut self) {
        // When this is called, device will be Some, and it does not change
        // from any of the called methods, except where a failed port is lost
        // and this returns at once
        fn device(worker: &mut Worker) -> &mut Device {
            worker
                .device
                .as_mut()
                .expect("No device but in autonegotiation")
        }
        let next_state = match device(self).rate_change_state.clone() {
            RateChange::QueryDeviceRate => {
                let target = device(self).rates().target_bps;
                if let Err(rpc_error) = self
                    .send_internal_rpc(
                        "dev.port.rate.near",
                        &target.to_le_bytes(),
                        QUERY_RATE_RPC_ID,
                        DeviceRoute::root(),
                    )
                    .await
                {
                    log::trace!(target: &self.log_target, "Could not ask for a rate: {rpc_error}");
                    RateChange::GaveUp
                } else {
                    log::trace!(target: &self.log_target, "Asked the device for {target} bps");
                    RateChange::WaitingDeviceRate
                }
            }
            RateChange::SetDeviceRate => {
                if self.rpc_map.is_empty() {
                    let target = device(self).rates().target_bps;
                    if let Err(rpc_error) = self
                        .send_internal_rpc(
                            "dev.port.rate",
                            &target.to_le_bytes(),
                            SET_RATE_RPC_ID,
                            DeviceRoute::root(),
                        )
                        .await
                    {
                        log::trace!(
                            target: &self.log_target,
                            "Could not set the rate: {rpc_error}"
                        );
                        RateChange::GaveUp
                    } else {
                        log::trace!(
                            target: &self.log_target,
                            "Told the device to use {target} bps"
                        );
                        RateChange::WaitingNewRate
                    }
                } else {
                    log::trace!(target: &self.log_target, "Waiting for requests to finish");
                    RateChange::SetDeviceRate
                }
            }
            RateChange::RateChanged => {
                let trial_over = device(self)
                    .rate_trial_deadline()
                    .is_some_and(|deadline| Instant::now() >= deadline);
                if trial_over {
                    let dev = device(self);
                    let default_bps = dev.rates().default_bps;
                    if let Err(error) = dev.port.set_rate(default_bps).await {
                        log::debug!(
                            target: &self.log_target,
                            "The port could not go back to {default_bps} bps: {error}"
                        );
                        // The worker opens the device again, at the default rate.
                        self.lose_device();
                        return;
                    }
                    log::trace!(
                        target: &self.log_target,
                        "Nothing arrived at the new rate, so the port is back at {default_bps} bps"
                    );
                    RateChange::GaveUp
                } else {
                    RateChange::RateChanged
                }
            }
            // In any other case, do nothing
            current_state => current_state,
        };
        device(self).rate_change_state = next_state;
    }

    fn cancel_active_rpcs(&mut self) {
        self.dispatch_rpc_errors(RpcFailure::DeviceDisconnected, None);
    }

    /// Returns the earliest deadline the worker keeps. It is the first RPC
    /// timeout, the reconnect deadline, when the device counts as lost, or
    /// when the next attempt to reopen it is due.
    fn next_deadline(&self) -> Option<Instant> {
        let reopen = self.device.is_none() && self.attempt.is_none();
        [
            self.rpc_timeouts.keys().next().copied(),
            self.reconnect_deadline,
            self.device.as_ref().and_then(Device::liveness_deadline),
            self.device.as_ref().and_then(Device::rate_trial_deadline),
            reopen.then_some(self.next_attempt),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Opens the device, then runs the worker until it stops, and shuts it
    /// down. A stop during an open that the opener lets the worker drop
    /// shuts the worker down at once.
    async fn run(mut self) {
        let mut opening = self.open_device();
        let opened = match self.open.droppable {
            true => loop {
                tokio::select! {
                    opened = &mut opening => break opened,
                    command = self.command_queue.recv() => match command {
                        Some(open) => self.adopt(open),
                        None => {
                            log::trace!(target: &self.log_target, "The link closed while it opened");
                            self.shutdown().await;
                            return;
                        }
                    },
                }
            },
            false => opening.await,
        };
        match opened {
            Ok(device) => self.device = Some(device),
            Err(error) => {
                log::debug!(target: &self.log_target, "Failed to connect: {error}");
                let failed = Readiness::Failed("failed to connect".to_string());
                self.readiness.send_replace(failed);
                self.shutdown().await;
                return;
            }
        }
        while self.turn().await.is_continue() {}
        self.shutdown().await;
    }

    /// Runs one turn of the worker. Handles what the device has sent, acts
    /// on each deadline that has come, and takes the step of the rate
    /// negotiation that is due. Then waits for the next deadline or a
    /// channel, and handles it. Returns [`ControlFlow::Break`] once the
    /// worker stops.
    async fn turn(&mut self) -> ControlFlow<()> {
        // What arrived before a deadline counts before the deadline is
        // checked.
        self.receive_device().await;
        let now = Instant::now();
        self.process_rpc_timeouts();
        if self.device.is_none() {
            self.cancel_active_rpcs();
            if self.attempt.is_none() && now >= self.next_attempt {
                self.attempt = Some(self.open_device());
            }
        }
        // Giving up is the only way a lost device ends the worker. Only the
        // device answering clears the deadline, so a port that opens again
        // but never answers is given up too.
        if self
            .reconnect_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            log::error!(
                target: &self.log_target,
                "Stopping reconnection attempts due to timeout"
            );
            self.status_to_engine(packet::LinkStatus::FailedToReconnect);
            return ControlFlow::Break(());
        }
        let liveness = self.device.as_ref().and_then(Device::liveness_deadline);
        if liveness.is_some_and(|deadline| now >= deadline) {
            self.lose_device();
            return ControlFlow::Continue(());
        }
        let restarted = self
            .device
            .as_mut()
            .is_some_and(|device| std::mem::take(&mut device.restarted));
        if restarted {
            log::trace!(target: &self.log_target, "The root device restarted");
            self.cancel_active_rpcs();
        }
        // A step falls due when what the device sent was just handled, or
        // when the trial of a new rate runs out, which `next_deadline` wakes
        // for. A step that is not due does nothing.
        if self
            .device
            .as_ref()
            .is_some_and(Device::needs_autonegotiation)
        {
            self.autonegotiation().await;
        }
        let wake = self.next_deadline();

        // The ports' packets wait in their queues while the device's rate
        // changes. Without a device, forwarding answers requests with errors.
        let forwarding = self.device.as_ref().is_none_or(Device::safe_to_forward);
        let connected = self.device.is_some();
        tokio::select! {
            command = self.command_queue.recv() => match command {
                Some(open) => self.adopt(open),
                None => {
                    log::trace!(target: &self.log_target, "The link closed");
                    return ControlFlow::Break(());
                }
            },
            Some((port, sent)) = self.inputs.next(), if forwarding && !self.inputs.is_empty() => {
                match sent {
                    Some(pkt) => self.port_packet(port, pkt).await,
                    None => {
                        self.mounts.remove(&port);
                        log::trace!(target: &self.log_target, "Port {port} terminated");
                    }
                }
            }
            received = async { self.device.as_mut().expect("a device is present").results.next().await }, if connected => {
                let received = received.unwrap_or(Err(transport::RecvError::Disconnected));
                self.device_received(received).await;
            }
            opened = finish(&mut self.attempt) => match opened {
                Ok(device) => self.device = Some(device),
                Err(error) => {
                    log::debug!("failed to reopen {}: {error}", self.url);
                    self.next_attempt = Instant::now() + RECONNECT_INTERVAL;
                }
            },
            () = sleep_until_some(wake) => {}
        }
        ControlFlow::Continue(())
    }

    /// Refuses new ports and fails every request still waiting. Waits for an
    /// attempt in flight to reopen the device, or drops it if the opener
    /// allows. Closes the device's port and waits until it has let go of the
    /// device, for at most [`transport::CLOSE_TIMEOUT`]. Then drops the ports.
    async fn shutdown(&mut self) {
        self.command_queue.close();
        self.dispatch_rpc_errors(RpcFailure::ProxyClosed, None);
        // An open on a blocking thread cannot be stopped, and the device it
        // opens must be closed before the next worker opens it.
        if let Some(attempt) = self.attempt.take() {
            if !self.open.droppable {
                self.device = attempt.await.ok();
            }
        }
        if let Some(device) = self.device.take() {
            if !device.port.close(transport::CLOSE_TIMEOUT).await {
                log::warn!(
                    target: &self.log_target,
                    "The transport did not let go of the device within {:?}",
                    transport::CLOSE_TIMEOUT
                );
            }
        }
        self.inputs.clear();
        self.mounts.clear();
    }
}

/// Logs a packet from the device if `dump` asks for its kind. Logs a device's
/// log packet under `device::<route>` at the device's level. Either names the
/// device's route as `shown` does.
fn log_device_packet(pkt: &Packet, dump: DumpFlags, shown: impl Fn() -> String) {
    let payload = pkt.payload();
    let dump = match payload {
        packet::Payload::Heartbeat(_) => dump.heartbeats,
        packet::Payload::Metadata(..) => dump.meta,
        packet::Payload::Samples(_) => dump.data,
        _ => dump.traffic,
    };
    if dump {
        log::info!("Packet from {} -- {:?}", shown(), payload);
    }
    if let packet::Payload::Log(message) = payload {
        // Map the device-reported level onto the log crate's level
        // so the logger filter and prefix reflect it.
        let level = match message.level {
            LogLevel::CRITICAL | LogLevel::ERROR => log::Level::Error,
            LogLevel::WARNING => log::Level::Warn,
            LogLevel::DEBUG => log::Level::Debug,
            _ => log::Level::Info,
        };
        let text = String::from_utf8_lossy(message.message);
        log::log!(target: &format!("device::{}", shown()), level, "{text}");
    }
}

fn is_serial(url: &str) -> bool {
    url.starts_with("serial://") || url.starts_with("/dev/") || url.starts_with("COM")
}

fn key_from(identity: &str) -> String {
    identity
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn serial_locator(url: &str) -> (&str, &str) {
    let locator = url.strip_prefix("serial://").unwrap_or(url);
    let path = locator.split(':').next().unwrap_or_default();
    (path, &locator[path.len()..])
}

/// Returns the key the proxy files a device's link under. A network URL is
/// its own key. A serial port is keyed by its USB serial number if it
/// reports one, else by its path. The baud rate is ignored.
pub(super) fn device_key(url: &str) -> String {
    if !is_serial(url) {
        return key_from(url);
    }
    let (path, _) = serial_locator(url);
    let path = match path.strip_prefix("/dev/tty.") {
        Some(rest) if cfg!(target_os = "macos") => format!("/dev/cu.{rest}"),
        _ => path.to_string(),
    };
    key_from(&usb_identity(&path).unwrap_or_else(|| {
        let canonical = fs::canonicalize(&path).unwrap_or_else(|_| path.into());
        format!("path:{}", canonical.display())
    }))
}

/// Returns the key of a serial `url`, or `None` for a network transport.
fn identity(url: &str) -> Option<String> {
    is_serial(url).then(|| device_key(url))
}

/// Returns the URL of the device with `identity` if a replug moved it off
/// `url`.
fn relocate(url: &str, identity: Option<&str>) -> Option<String> {
    let key = identity?;
    let (_, baud) = serial_locator(url);
    transport::enumerate_serial(true)
        .into_iter()
        .map(|device| device.url)
        .filter(|candidate| {
            !(cfg!(target_os = "macos") && candidate.starts_with("serial:///dev/tty."))
        })
        .find(|candidate| device_key(candidate) == key)
        .map(|candidate| format!("{candidate}{baud}"))
        .filter(|moved| moved != url)
}

/// Returns `usb:<vid>:<pid>:<serial>` for the port at `path`. Returns `None`
/// if the port reports no serial number or several ports share it.
#[cfg(feature = "serial")]
fn usb_identity(path: &str) -> Option<String> {
    let ports = serialport::available_ports().ok()?;
    let usb = |port: &serialport::SerialPortInfo| match &port.port_type {
        serialport::SerialPortType::UsbPort(info) => info
            .serial_number
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| format!("usb:{:04x}:{:04x}:{s}", info.vid, info.pid)),
        _ => None,
    };
    let identity = usb(ports.iter().find(|p| p.port_name == path)?)?;
    let sharing = ports
        .iter()
        .filter(|p| !(cfg!(target_os = "macos") && p.port_name.starts_with("/dev/tty.")))
        .filter(|p| usb(p).as_ref() == Some(&identity))
        .count();
    (sharing == 1).then_some(identity)
}

#[cfg(not(feature = "serial"))]
fn usb_identity(_path: &str) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chan::{JoinHandle, Notify};
    use crate::proxy::outbox::Outbox;
    use crate::proxy::tests::fixtures::{assert_took, PAUSED_WAIT, UNREACHABLE, WAIT};
    use crate::rt;
    use crate::tio::transport::tests::fake::{self, FakeEnd};
    use crate::tio::transport::{RateInfo, TransportKind};
    use std::future::{self, Future};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::time::timeout;

    /// An opener that never opens anything.
    fn never() -> Opener {
        Opener::new(|_| future::pending::<io::Result<Reached>>())
    }

    fn idle_worker() -> Worker {
        let (_commands, command_queue) = mpsc::channel(1);
        let (readiness, _) = watch::channel(Readiness::Pending);
        let dump = DumpFlags::default();
        Worker::new(
            String::new(),
            None,
            command_queue,
            dump,
            readiness,
            never(),
            unheard(),
        )
    }

    /// An engine's port that nobody hears.
    fn unheard() -> EnginePort {
        EnginePort {
            to_engine: mpsc::unbounded_channel().0,
            from_engine: mpsc::channel(1).1,
        }
    }

    /// A device's routes are logged where the proxy's default layout mounts
    /// them, and by the device's URL where it does not.
    #[test]
    fn a_device_is_logged_where_the_default_layout_mounts_it() {
        let route = |path: &str| path.parse::<DeviceRoute>().unwrap();
        let mount = |url: &str, source: &str, prefix: &str| crate::session::Mount {
            url: url.into(),
            source: route(source),
            prefix: route(prefix),
        };
        let layout = Layout {
            mounts: vec![
                mount("udp://a", "/", "/2"),
                mount("udp://a", "/1", "/5"),
                mount("udp://b", "/", "/"),
            ],
        };
        let (_default, watched) = watch::channel(Some(layout));
        let worker = Worker {
            url: "udp://a".into(),
            default: Some(watched),
            ..idle_worker()
        };

        assert_eq!(worker.shown(route("/")), "/2");
        assert_eq!(worker.shown(route("/0/3")), "/2/0/3");
        assert_eq!(
            worker.shown(route("/1/0")),
            "/5/0",
            "the deepest mount names it"
        );

        let alone = Worker {
            url: "udp://c".into(),
            ..idle_worker()
        };
        assert_eq!(alone.shown(route("/1")), "udp://c /1");
    }

    /// A client's mount of the subtree at `source`, seen at the root, and
    /// its outbox.
    fn mount(source: &str) -> (Arc<Mount>, Arc<Outbox>) {
        let outbox = Arc::default();
        let mount = Mount::new(source.parse().unwrap(), DeviceRoute::root(), &outbox);
        (mount, outbox)
    }

    /// What `outbox` holds.
    fn answers(outbox: &Outbox) -> Vec<Packet> {
        outbox.take().packets.into()
    }

    fn insert_rpc(
        worker: &mut Worker,
        wire_id: u16,
        port: u64,
        original_id: u16,
        route: DeviceRoute,
        timeout: Instant,
    ) {
        worker.rpc_map.insert(
            wire_id,
            RpcMapEntry {
                target: RpcTarget::Port { port, original_id },
                route,
                timeout,
            },
        );
        worker
            .rpc_timeouts
            .entry(timeout)
            .or_default()
            .insert(wire_id);
    }

    #[test]
    fn keys_ignore_baud_and_stay_filesystem_safe() {
        assert_eq!(device_key("tcp://192.0.2.1:7855"), "tcp---192.0.2.1-7855");
        assert_eq!(
            device_key("serial:///dev/twinleaf-test"),
            device_key("serial:///dev/twinleaf-test:115200:115200")
        );
        assert_eq!(
            device_key("serial:///dev/twinleaf-none:9600"),
            key_from("path:/dev/twinleaf-none")
        );
        assert!(key_from("mount:usb/1=/1+tcp://h:1=/2")
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-'));
    }

    /// Waits for a worker's `task` to end, and checks that it did not panic.
    fn join(task: JoinHandle<Option<()>>) {
        let (ended, outcome) = crate::chan::bounded(1);
        crate::chan::spawn_on(rt::handle(), async move {
            let _ = ended.send(task.await);
        });
        let outcome = outcome
            .recv_timeout(WAIT)
            .expect("the worker never stopped");
        assert!(matches!(outcome, Ok(Some(()))), "the worker panicked");
    }

    /// A control packet talks to the proxy, so neither control type a port
    /// sends reaches the device. Type 64 is also `USER`, which apps may send,
    /// so it still does.
    #[test]
    fn a_ports_control_packets_never_reach_the_device() {
        use std::io::Read;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("tcp://{}", listener.local_addr().unwrap());
        let dump = DumpFlags::default();
        let tasks = Supervisor::new();
        let open = opener(&tasks, &url);
        let default = crate::proxy::link::no_default();
        let started = new(&url, None, dump, default, open, &tasks);
        let (commands, engine) = (started.commands, started.engine);
        let worker = tasks.spawn("a test worker", started.run);
        let (mut device, _) = listener.accept().unwrap();

        let user = Packet::legacy_status(packet::LinkStatus::SensorReconnected);
        for pkt in [
            Packet::proxy_request("proxy.open", b"auto", 1).unwrap(),
            Packet::proxy_reply(1, b"").unwrap(),
            Packet::link(packet::LinkStatus::SensorReconnected),
            user.clone(),
        ] {
            engine.send(pkt);
        }

        // The link's own hello comes first, as it asks whether a proxy is at
        // the other end. The port's packets are forwarded in order after it,
        // so a control packet of the port that got through would arrive
        // ahead of the USER packet.
        device
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut header = [0; 4];
        device.read_exact(&mut header).unwrap();
        assert_eq!(header[0], 66, "the link did not say hello first");
        let mut hello = vec![0; usize::from(u16::from_le_bytes([header[2], header[3]]))];
        device.read_exact(&mut hello).unwrap();
        let mut first = vec![0; user.as_bytes().len()];
        device.read_exact(&mut first).unwrap();
        assert_eq!(
            first,
            user.as_bytes(),
            "a control packet reached the device"
        );
        device
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        assert!(
            device.read(&mut [0; 1]).is_err(),
            "the device was sent more"
        );
        drop((commands, engine));
        join(worker);
    }

    /// A status names a subtree, so a mount must receive it in its own
    /// coordinates, never in the proxy's, and never for a disjoint mount. The
    /// link packet and the legacy status are clamped alike.
    #[test]
    fn a_status_is_clamped_into_the_ports_subtree_or_skipped() {
        let forms: [fn(packet::LinkStatus) -> Packet; 2] = [Packet::link, Packet::legacy_status];
        for form in forms {
            let status = |route: &str| {
                form(packet::LinkStatus::SensorDisconnected).with_route(route.parse().unwrap())
            };
            let ptype = status("/").ptype();
            let delivered_routes = |outbox: &Outbox| -> Vec<DeviceRoute> {
                answers(outbox)
                    .iter()
                    .map(|pkt| {
                        assert_eq!(pkt.ptype(), ptype, "the status keeps its form");
                        pkt.route()
                    })
                    .collect()
            };

            let (mount, outbox) = mount("/1");
            mount.urgent(&status("/"));
            mount.urgent(&status("/1/3"));
            mount.urgent(&status("/2"));
            assert_eq!(
                delivered_routes(&outbox),
                [DeviceRoute::root(), "/3".parse().unwrap()],
                "{ptype:?}: clamped into port coordinates, the disjoint mount skipped"
            );
        }
    }

    #[test]
    fn rpc_replies_restore_independently_and_out_of_order() {
        let mut worker = idle_worker();
        let first_route: DeviceRoute = "/1".parse().unwrap();
        let second_route: DeviceRoute = "/2".parse().unwrap();
        let timeout = Instant::now() + Duration::from_secs(3);
        insert_rpc(&mut worker, 20, 3, 100, first_route, timeout);
        insert_rpc(&mut worker, 21, 4, 101, second_route, timeout);

        let remap = worker.rpc_restore(21, &second_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                port: 4,
                original_id: 101
            }
        ));
        let remap = worker.rpc_restore(20, &first_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                port: 3,
                original_id: 100
            }
        ));
        assert!(worker.rpc_map.is_empty());
        assert!(worker.rpc_timeouts.is_empty());
    }

    #[test]
    fn rpc_reply_from_the_wrong_route_does_not_consume_the_request() {
        let mut worker = idle_worker();
        let requested_route: DeviceRoute = "/1".parse().unwrap();
        let wrong_route: DeviceRoute = "/2".parse().unwrap();
        let timeout = Instant::now() + Duration::from_secs(3);
        insert_rpc(&mut worker, 20, 3, 100, requested_route, timeout);

        assert!(worker.rpc_restore(20, &wrong_route).is_none());
        assert!(worker.rpc_map.contains_key(&20));
        assert!(worker.rpc_timeouts[&timeout].contains(&20));
        let remap = worker.rpc_restore(20, &requested_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                port: 3,
                original_id: 100
            }
        ));
    }

    #[test]
    fn rpc_timeout_is_delivered_with_the_ports_request_id() {
        let mut worker = idle_worker();

        let (mount, outbox) = mount("/");
        let port = 1;
        worker.adopt(OpenPort {
            mount,
            from_client: mpsc::channel(1).1,
        });

        let wire_id = 12;
        let original_id = 7855;
        let timeout = Instant::now() - Duration::from_millis(1);
        insert_rpc(
            &mut worker,
            wire_id,
            port,
            original_id,
            DeviceRoute::root(),
            timeout,
        );

        worker.process_rpc_timeouts();

        let packet = answers(&outbox).remove(0);
        let packet::Payload::RpcError(error) = packet.payload() else {
            panic!("expected an RPC timeout packet");
        };
        assert_eq!(error.req_id.value(), original_id);
        assert!(matches!(RpcError::from(error.code), RpcError::Timeout));
        assert!(worker.rpc_map.is_empty());
        assert!(worker.rpc_timeouts.is_empty());
    }

    /// A device that goes away answers nobody, so every port with a request
    /// waiting hears that it failed, under the id it asked with, and nothing
    /// is replayed.
    #[test]
    fn a_disconnect_fails_every_ports_request_without_replaying_it() {
        let mut worker = idle_worker();
        let (first, first_delivered) = mount("/");
        let (second, second_delivered) = mount("/");
        worker.mounts.insert(1, first);
        worker.mounts.insert(2, second);
        let deadline = Instant::now() + Duration::from_secs(3);
        insert_rpc(&mut worker, 20, 1, 100, DeviceRoute::root(), deadline);
        insert_rpc(&mut worker, 21, 2, 200, DeviceRoute::root(), deadline);

        worker.cancel_active_rpcs();

        for (delivered, id) in [(first_delivered, 100), (second_delivered, 200)] {
            let failed = answers(&delivered);
            assert_eq!(failed.len(), 1, "a request was failed twice");
            assert_eq!(rpc_error(&failed[0]), (id, RpcError::Undefined));
        }
        assert!(worker.rpc_map.is_empty());
        assert!(worker.rpc_timeouts.is_empty());
    }

    /// Returns an opener whose attempt number `n`, counted from 0, runs
    /// `attempt(n, results)`.
    fn scripted<F, R>(
        attempt: impl Fn(usize, crate::chan::Sender<ReceiveResult>) -> F + Send + Sync + 'static,
    ) -> Opener
    where
        F: Future<Output = io::Result<R>> + Send + 'static,
        R: Into<Reached>,
    {
        let attempts = AtomicUsize::new(0);
        Opener::new(move |results| {
            let number = attempts.fetch_add(1, Ordering::Relaxed);
            attempt(number, results)
        })
    }

    /// Opens a fake port of `kind` that delivers to `results`, and hands its
    /// end to the test through `ends`.
    fn fake(
        kind: TransportKind,
        results: crate::chan::Sender<ReceiveResult>,
        ends: &mpsc::UnboundedSender<FakeEnd>,
    ) -> io::Result<HardwarePort> {
        let (port, end) = fake::port(kind, None, results);
        let _ = ends.send(end);
        Ok(port)
    }

    /// Returns an opener of fake ports of `kind`, and the receiver of their
    /// ends.
    fn fakes(kind: TransportKind) -> (Opener, mpsc::UnboundedReceiver<FakeEnd>) {
        let (ends, opened) = mpsc::unbounded_channel();
        let open = scripted(move |_, results| future::ready(fake(kind, results, &ends)));
        (open, opened)
    }

    /// Waits for the opener to open the next fake port, and returns its end.
    async fn opened(ends: &mut mpsc::UnboundedReceiver<FakeEnd>) -> FakeEnd {
        let opened = timeout(WAIT, ends.recv()).await;
        let opened = opened.expect("the worker opened nothing");
        opened.expect("the worker stopped")
    }

    /// A worker running as a task on the test's runtime, with its engine's
    /// port.
    struct Running {
        commands: mpsc::Sender<OpenPort>,
        readiness: watch::Receiver<Readiness>,
        /// Sends what the engine sends to the device.
        to_device: mpsc::Sender<Packet>,
        /// What the engine hears.
        port: mpsc::UnboundedReceiver<Packet>,
        task: JoinHandle<Option<()>>,
    }

    impl Running {
        /// Runs a worker that opens its device with `open`, and gives up a
        /// lost device after `reconnect_timeout`.
        fn start(open: Opener, reconnect_timeout: Option<Duration>) -> Running {
            let (commands, command_queue) = mpsc::channel(4);
            let (readiness, watched) = watch::channel(Readiness::Pending);
            let (to_engine, port) = mpsc::unbounded_channel();
            let (to_device, from_engine) = mpsc::channel(4);
            let worker = Worker::new(
                "fake".to_string(),
                reconnect_timeout,
                command_queue,
                DumpFlags::default(),
                readiness,
                open,
                EnginePort {
                    to_engine,
                    from_engine,
                },
            );
            let task = Supervisor::current().spawn("a test worker", worker.run());
            Running {
                commands,
                readiness: watched,
                to_device,
                port,
                task,
            }
        }

        /// Waits until the worker's readiness is no longer `from`, and
        /// returns it.
        async fn settled(&mut self, from: Readiness) -> Readiness {
            let changed = self.readiness.wait_for(|readiness| *readiness != from);
            let settled = timeout(WAIT, changed).await;
            let settled = settled.unwrap_or_else(|_| panic!("the worker stayed {from:?}"));
            settled.expect("the worker kept its readiness").clone()
        }

        /// Closes the link, and waits for the worker to stop.
        async fn stop(self) {
            drop(self.commands);
            ended(self.task).await;
        }
    }

    /// Waits for a worker's `task` to end, and checks that it did not panic.
    async fn ended(task: JoinHandle<Option<()>>) {
        let ended = timeout(WAIT, task).await.expect("the worker never stopped");
        assert!(matches!(ended, Ok(Some(()))), "the worker panicked");
    }

    /// Waits for the next packet delivered to `port`. Lets the other tasks
    /// run before it lets time pass, a millisecond at a time.
    async fn next(port: &mut mpsc::UnboundedReceiver<Packet>) -> Packet {
        let deadline = Instant::now() + WAIT;
        for polls in 0.. {
            if let Ok(pkt) = port.try_recv() {
                return pkt;
            }
            assert!(Instant::now() < deadline, "nothing was delivered");
            if polls < 10 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        unreachable!()
    }

    /// Waits for the first packet queued in `outbox`, as [`next`] does.
    async fn answered(outbox: &Outbox) -> Packet {
        let deadline = Instant::now() + WAIT;
        for polls in 0.. {
            if let Some(pkt) = answers(outbox).into_iter().next() {
                return pkt;
            }
            assert!(Instant::now() < deadline, "nothing was answered");
            if polls < 10 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        unreachable!()
    }

    /// Waits for the next `count` packets delivered to `port`, and returns
    /// each one's link status or packet type.
    async fn delivered(port: &mut mpsc::UnboundedReceiver<Packet>, count: usize) -> Vec<String> {
        let mut seen = Vec::new();
        for _ in 0..count {
            let pkt = next(port).await;
            seen.push(match pkt.link_status() {
                Some(status) => format!("{status:?}"),
                None => format!("{:?}", pkt.ptype()),
            });
        }
        seen
    }

    /// Returns the id and error of `pkt`, an RPC error.
    fn rpc_error(pkt: &Packet) -> (u16, RpcError) {
        let packet::Payload::RpcError(error) = pkt.payload() else {
            panic!("expected an RPC error, got {:?}", pkt.payload());
        };
        (error.req_id.value(), RpcError::from(error.code))
    }

    fn heartbeat() -> Packet {
        Packet::heartbeat(DeviceRoute::root())
    }

    /// The type of [`heartbeat`], as [`delivered`] gives it.
    fn beat() -> String {
        format!("{:?}", heartbeat().ptype())
    }

    /// A closed input drains its queued packets and releases its port.
    /// Other inputs keep forwarding both ways, and a mount hears the answer
    /// to its request in its own coordinates.
    #[tokio::test(start_paused = true)]
    async fn a_closed_input_drains_without_disrupting_other_scoped_ports() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let mut worker = Running::start(open, None);
        let mut device = opened(&mut ends).await;
        device.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

        let (first, _) = mount("/1");
        let (second, second_outbox) = mount("/2");
        let (first_input, first_rx) = mpsc::channel(4);
        let (second_input, second_rx) = mpsc::channel(4);
        for (mount, rx) in [(&first, first_rx), (&second, second_rx)] {
            let command = OpenPort {
                mount: Arc::clone(mount),
                from_client: rx,
            };
            worker.commands.send(command).await.unwrap();
        }
        let at = |route: &str| heartbeat().with_route(route.parse().unwrap());
        first_input.send(at("/1")).await.unwrap();
        first_input.send(at("/1/3")).await.unwrap();
        drop(first_input);
        second_input.send(at("/2/4")).await.unwrap();

        let mut routes = Vec::new();
        for _ in 0..3 {
            let sent = timeout(WAIT, device.recv()).await.unwrap().unwrap();
            routes.push(sent.route().to_string());
        }
        routes.sort();
        assert_eq!(routes, ["/1", "/1/3", "/2/4"]);
        while Arc::strong_count(&first) > 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let request = Packet::rpc_request("dev.name", &[], 9, "/2/5".parse().unwrap());
        second_input.send(request.unwrap()).await.unwrap();
        let sent = timeout(WAIT, device.recv()).await.unwrap().unwrap();
        assert_eq!(sent.route(), "/2/5".parse().unwrap());
        let packet::Payload::RpcRequest(request) = sent.payload() else {
            panic!("expected the request, got {:?}", sent.payload());
        };
        let reply = Packet::rpc_reply(request.id.value(), b"d", sent.route()).unwrap();
        device.send(&reply).await;
        let answer = answered(&second_outbox).await;
        assert_eq!(answer.route(), "/5".parse().unwrap());
        let packet::Payload::RpcReply(reply) = answer.payload() else {
            panic!("expected the reply, got {:?}", answer.payload());
        };
        assert_eq!(reply.req_id.value(), 9);
        worker.stop().await;
    }

    /// A port that fails with an I/O error loses the device as one that
    /// closes does, so the worker reconnects instead of stopping. The port it
    /// had stays, and hears the device drop out, come back, and send again.
    /// The link is ready from the first packet on.
    #[tokio::test(start_paused = true)]
    async fn an_io_error_is_a_disconnect_and_the_worker_reconnects() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let mut worker = Running::start(open, None);
        let mut first = opened(&mut ends).await;
        first.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

        first.fail(io::ErrorKind::ConnectionReset);

        let mut second = opened(&mut ends).await;
        second.send(&heartbeat()).await;
        assert_eq!(
            delivered(&mut worker.port, 4).await,
            [
                beat(),
                "SensorDisconnected".to_string(),
                "SensorReconnected".to_string(),
                beat(),
            ]
        );
        assert!(!worker.task.is_finished());
        assert_eq!(*worker.readiness.borrow(), Readiness::Ready);
        worker.stop().await;
    }

    /// A device lost for good ends the worker once the reconnect timeout
    /// passes, and its port hears that reconnecting failed before it closes.
    /// The link's readiness stays as it was.
    #[tokio::test(start_paused = true)]
    async fn a_device_that_never_comes_back_ends_the_worker_with_failed_to_reconnect() {
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |attempt, results| {
            future::ready(match attempt {
                0 => fake(TransportKind::Tcp, results, &ends),
                _ => Err(io::ErrorKind::ConnectionRefused.into()),
            })
        });
        let reconnect_timeout = Duration::from_millis(200);
        let mut worker = Running::start(open, Some(reconnect_timeout));
        let mut first = opened(&mut opened_ends).await;
        first.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

        first.fail(io::ErrorKind::ConnectionReset);
        let lost = Instant::now();

        ended(worker.task).await;
        assert_took(lost.elapsed(), reconnect_timeout, "giving up");
        assert_eq!(*worker.readiness.borrow(), Readiness::Ready);
        assert_eq!(
            delivered(&mut worker.port, 3).await[1..],
            ["SensorDisconnected", "FailedToReconnect"]
        );
        assert!(matches!(
            worker.port.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
    }

    /// A port lost before its device answers on it is a failed attempt. The
    /// ports hear nothing more of it, the next attempt waits the reconnect
    /// interval, and the reconnect timeout still ends the worker however
    /// readily the port opens.
    #[tokio::test(start_paused = true)]
    async fn a_transport_lost_before_its_device_answers_is_a_failed_attempt() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let mut worker = Running::start(open, Some(Duration::from_millis(200)));
        let mut first = opened(&mut ends).await;
        first.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

        first.fail(io::ErrorKind::ConnectionReset);

        // Each attempt opens a port, which closes before the device says
        // anything. The ends stop once the worker has stopped.
        let mut attempts = 0;
        while let Some(end) = timeout(WAIT, ends.recv()).await.expect("never gave up") {
            attempts += 1;
            assert_eq!(attempts, 1, "an attempt came before the interval");
            drop(end);
        }
        assert_eq!(attempts, 1);
        assert_eq!(
            delivered(&mut worker.port, 3).await[1..],
            ["SensorDisconnected", "FailedToReconnect"],
            "a failed attempt was reported to the port"
        );
        ended(worker.task).await;
        assert_eq!(*worker.readiness.borrow(), Readiness::Ready);
    }

    /// A device that answers again clears the reconnect timeout, so the
    /// worker keeps it once the timeout has passed.
    #[tokio::test(start_paused = true)]
    async fn a_device_that_answers_again_outlasts_the_reconnect_timeout() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let reconnect_timeout = Duration::from_millis(200);
        let mut worker = Running::start(open, Some(reconnect_timeout));
        let mut first = opened(&mut ends).await;
        first.send(&heartbeat()).await;
        first.fail(io::ErrorKind::ConnectionReset);
        let mut second = opened(&mut ends).await;
        second.send(&heartbeat()).await;
        assert_eq!(
            delivered(&mut worker.port, 3).await[1..],
            ["SensorDisconnected", "SensorReconnected"]
        );

        // Well within how long a TCP device may stay silent.
        tokio::time::sleep(2 * reconnect_timeout).await;

        assert!(!worker.task.is_finished(), "the worker gave up");
        assert_eq!(*worker.readiness.borrow(), Readiness::Ready);
        worker.stop().await;
    }

    /// A device that has answered is lost once it stays silent for a second,
    /// or three over TCP. Its port is opened again at once.
    #[tokio::test(start_paused = true)]
    async fn a_silent_device_is_lost_after_a_second_or_three_over_tcp() {
        for (kind, silence) in [
            (TransportKind::Serial, LIVENESS_TIMEOUT),
            (TransportKind::Udp, LIVENESS_TIMEOUT),
            (TransportKind::Tcp, LIVENESS_TIMEOUT_TCP),
        ] {
            let (open, mut ends) = fakes(kind);
            let mut worker = Running::start(open, None);
            let mut first = opened(&mut ends).await;
            first.send(&heartbeat()).await;
            let answered = Instant::now();

            let _second = opened(&mut ends).await;

            assert_took(answered.elapsed(), silence, kind);
            assert_eq!(
                delivered(&mut worker.port, 2).await,
                [beat(), "SensorDisconnected".to_string()]
            );
            worker.stop().await;
        }
    }

    /// How long each attempt after the first takes to open its port, unless
    /// a test says otherwise.
    const CONNECT: Duration = Duration::from_millis(1200);

    /// Runs a worker of fake ports of `kind`, at whose other end `peer`
    /// answers. The device answers on the first port and then loses it.
    /// Each later attempt takes `connect` to open its port. Returns the
    /// worker, the receiver of when each later attempt starts, the end of the
    /// port the second attempt opens, and when the second attempt started.
    async fn reopened(
        kind: TransportKind,
        peer: PeerKind,
        connect: Duration,
    ) -> (Running, mpsc::UnboundedReceiver<Instant>, FakeEnd, Instant) {
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let (starts, mut started) = mpsc::unbounded_channel();
        let open = scripted(move |attempt, results| {
            let _ = starts.send(Instant::now());
            let port = fake(kind, results, &ends);
            async move {
                if attempt > 0 {
                    tokio::time::sleep(connect).await;
                }
                port.map(|port| Reached { port, peer })
            }
        });
        let mut worker = Running::start(open, None);
        let mut first = opened(&mut opened_ends).await;
        first.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);
        first.fail(io::ErrorKind::ConnectionReset);
        let silent = opened(&mut opened_ends).await;
        started.recv().await.expect("the first attempt");
        let second = started.recv().await.expect("the second attempt");
        (worker, started, silent, second)
    }

    /// A TCP connection reopened after the device was lost, on which nothing
    /// ever arrives, is a failed attempt once `LIVENESS_TIMEOUT_TCP` has
    /// passed from the start of the attempt, the connect included. The ports
    /// hear nothing of it, and the next attempt waits the reconnect interval.
    #[tokio::test(start_paused = true)]
    async fn a_tcp_reconnection_the_device_never_answers_on_is_given_up() {
        let (mut worker, mut started, silent, second) =
            reopened(TransportKind::Tcp, PeerKind::Device, CONNECT).await;

        let third = timeout(WAIT, started.recv()).await.expect("never given up");
        let third = third.expect("the worker stopped");

        assert!(silent.released(), "the silent connection was kept");
        assert_took(
            third - second,
            LIVENESS_TIMEOUT_TCP + RECONNECT_INTERVAL,
            "giving up the silent connection and trying again",
        );
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        assert!(
            matches!(worker.port.try_recv(), Err(TryRecvError::Empty)),
            "a failed attempt was reported to the port"
        );
        worker.stop().await;
    }

    /// A serial port reopened after the device was lost, on which nothing
    /// ever arrives, is kept, as a serial port has no deadline before the
    /// device answers on it.
    #[tokio::test(start_paused = true)]
    async fn a_serial_port_the_device_never_answers_on_is_kept() {
        let (mut worker, mut started, silent, _) =
            reopened(TransportKind::Serial, PeerKind::Device, CONNECT).await;

        tokio::time::sleep(10 * LIVENESS_TIMEOUT_TCP).await;

        assert!(!silent.released(), "the silent port was given up");
        assert!(started.try_recv().is_err(), "the worker tried again");
        assert!(!worker.task.is_finished(), "the worker stopped");
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        worker.stop().await;
    }

    /// A proxy reopened after its device was lost, from which nothing more
    /// arrives, is kept until `LIVENESS_TIMEOUT_PROXY` has passed from its
    /// answer to the open, and is then a failed attempt. The ports hear
    /// nothing of it, and the next attempt waits the reconnect interval.
    #[tokio::test(start_paused = true)]
    async fn a_reopened_proxy_is_given_up_once_silent_for_its_timeout_from_its_open() {
        let (mut worker, mut started, silent, second) =
            reopened(TransportKind::Tcp, PeerKind::Proxy, CONNECT).await;
        let answered = second + CONNECT;

        crate::chan::sleep_until(answered + LIVENESS_TIMEOUT_PROXY - Duration::from_millis(1))
            .await;
        assert!(!silent.released(), "the proxy was given up before its time");
        assert!(started.try_recv().is_err(), "the worker tried again");

        let third = timeout(PAUSED_WAIT, started.recv()).await;
        let third = third.expect("never given up").expect("the worker stopped");
        assert!(silent.released(), "the silent proxy was kept");
        assert_took(
            third - answered,
            LIVENESS_TIMEOUT_PROXY + RECONNECT_INTERVAL,
            "giving up the silent proxy and trying again",
        );
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        assert!(
            matches!(worker.port.try_recv(), Err(TryRecvError::Empty)),
            "a failed attempt was reported to the port"
        );
        worker.stop().await;
    }

    /// A proxy whose open took nearly as long as an open may, as it waited
    /// for its own device, is kept once the open ends, for longer than
    /// `LIVENESS_TIMEOUT_TCP`. Its silence counts from its answer to the
    /// open, not from the start of the attempt.
    #[tokio::test(start_paused = true)]
    async fn a_proxy_whose_open_outlasts_the_tcp_timeout_is_not_given_up_at_once() {
        let slow = READY_TIMEOUT - Duration::from_secs(1);
        let (mut worker, mut started, silent, second) =
            reopened(TransportKind::Tcp, PeerKind::Proxy, slow).await;

        crate::chan::sleep_until(second + slow + 2 * LIVENESS_TIMEOUT_TCP).await;

        assert!(!silent.released(), "the proxy was given up as it opened");
        assert!(started.try_recv().is_err(), "the worker tried again");
        assert!(!worker.task.is_finished(), "the worker stopped");
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        worker.stop().await;
    }

    /// While an attempt to reopen the device hangs, a port that joins is
    /// adopted at once. A port's request fails at once, as does the request
    /// the device took before it was lost.
    #[tokio::test(start_paused = true)]
    async fn a_hung_reopen_delays_neither_a_new_port_nor_a_ports_rpc_errors() {
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |attempt, results| {
            let opened = (attempt == 0).then(|| fake(TransportKind::Tcp, results, &ends));
            async move {
                match opened {
                    Some(opened) => opened,
                    None => future::pending().await,
                }
            }
        });
        let mut worker = Running::start(open, None);
        let mut device = opened(&mut opened_ends).await;
        device.send(&heartbeat()).await;
        let request = |id| Packet::rpc_request("dev.name", &[], id, DeviceRoute::root()).unwrap();
        worker.to_device.send(request(7)).await.unwrap();
        let taken = timeout(WAIT, device.recv()).await.unwrap().unwrap();
        assert!(matches!(taken.payload(), packet::Payload::RpcRequest(_)));

        device.fail(io::ErrorKind::ConnectionReset);
        let lost = Instant::now();

        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        assert_eq!(
            rpc_error(&next(&mut worker.port).await),
            (7, RpcError::Undefined)
        );
        let (mount, joined) = mount("/");
        let (to_device, rx) = mpsc::channel(1);
        worker
            .commands
            .try_send(OpenPort {
                mount,
                from_client: rx,
            })
            .unwrap();
        to_device.try_send(request(8)).unwrap();
        assert_eq!(
            rpc_error(&answered(&joined).await),
            (8, RpcError::Undefined)
        );
        assert_eq!(
            lost.elapsed(),
            Duration::ZERO,
            "a port waited for the reopen"
        );
    }

    /// A worker that stops while an attempt to reopen the device is in
    /// flight waits for the attempt, however long it takes. It closes the
    /// port the attempt opened before it ends, and drops its ports last.
    #[tokio::test(start_paused = true)]
    async fn shutdown_waits_for_a_reopen_in_flight() {
        let gate = Arc::new(Notify::new());
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = {
            let gate = gate.clone();
            scripted(move |attempt, results| {
                let (gate, ends) = (gate.clone(), ends.clone());
                async move {
                    if attempt > 0 {
                        gate.notified().await;
                    }
                    fake(TransportKind::Tcp, results, &ends)
                }
            })
        };
        let mut worker = Running::start(open, None);
        let mut first = opened(&mut opened_ends).await;
        first.send(&heartbeat()).await;
        first.fail(io::ErrorKind::ConnectionReset);
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        let Running {
            commands,
            mut port,
            task,
            ..
        } = worker;

        drop(commands);
        tokio::time::sleep(WAIT).await;

        assert!(!task.is_finished(), "the worker left an attempt in flight");
        assert!(matches!(port.try_recv(), Err(TryRecvError::Empty)));
        gate.notify_one();
        let reopened = opened(&mut opened_ends).await;
        ended(task).await;
        assert!(
            reopened.released(),
            "the reopened port still holds the device"
        );
        assert!(matches!(port.try_recv(), Err(TryRecvError::Disconnected)));
    }

    /// Returns a droppable opener whose first open is a fake TCP port, handed
    /// to the test through `ends`, if `first_opens`. Every other open never
    /// ends, and the receiver it returns disconnects once the open is
    /// dropped.
    fn droppable_hang(
        first_opens: bool,
        ends: mpsc::UnboundedSender<FakeEnd>,
    ) -> (Opener, crate::chan::oneshot::Receiver<()>) {
        let (held, dropped) = crate::chan::oneshot::channel();
        let held = std::sync::Mutex::new(Some(held));
        let attempts = AtomicUsize::new(0);
        let open = Opener::droppable(move |results| -> Opening {
            if first_opens && attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                let port = fake(TransportKind::Tcp, results, &ends);
                return Box::pin(future::ready(port.map(Reached::from)));
            }
            let held = crate::chan::lock(&held).take();
            Box::pin(async move {
                let _held = held;
                future::pending().await
            })
        });
        (open, dropped)
    }

    /// A worker whose opener lets it drop an open in flight, as a TCP opener
    /// does, ends at once when its link closes during the first open. The
    /// open is dropped, and the link never became ready.
    #[tokio::test(start_paused = true)]
    async fn a_link_closed_during_a_droppable_first_open_ends_at_once() {
        let (ends, _opened_ends) = mpsc::unbounded_channel();
        let (open, mut dropped) = droppable_hang(false, ends);
        let Running {
            commands,
            readiness,
            task,
            ..
        } = Running::start(open, None);
        tokio::time::sleep(WAIT).await;

        drop(commands);
        let closed = Instant::now();

        ended(task).await;
        assert_eq!(
            closed.elapsed(),
            Duration::ZERO,
            "the worker waited for the open"
        );
        assert_eq!(*readiness.borrow(), Readiness::Pending);
        assert!(dropped.try_recv().is_err(), "the open in flight was kept");
    }

    /// A worker whose opener lets it drop an open in flight ends at once when
    /// its link closes while it reopens the device, and drops the open.
    #[tokio::test(start_paused = true)]
    async fn a_link_closed_during_a_droppable_reopen_ends_at_once() {
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let (open, mut dropped) = droppable_hang(true, ends);
        let mut worker = Running::start(open, None);
        let mut first = opened(&mut opened_ends).await;
        first.send(&heartbeat()).await;
        first.fail(io::ErrorKind::ConnectionReset);
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        tokio::time::sleep(WAIT).await;
        assert!(
            dropped
                .try_recv()
                .is_err_and(|error| error == crate::chan::oneshot::error::TryRecvError::Empty),
            "no reopen is in flight"
        );

        drop(worker.commands);
        let closed = Instant::now();

        ended(worker.task).await;
        assert_eq!(
            closed.elapsed(),
            Duration::ZERO,
            "the worker waited for the reopen"
        );
        assert!(
            dropped
                .try_recv()
                .is_err_and(|error| error == crate::chan::oneshot::error::TryRecvError::Closed),
            "the reopen in flight was kept"
        );
    }

    /// The rate negotiation gives up as soon as the device turns the target
    /// rate down, and then wakes the worker no more.
    #[tokio::test(start_paused = true)]
    async fn the_rate_negotiation_gives_up_at_once_and_wakes_no_more() {
        let rates = RateInfo {
            default_bps: 115200,
            target_bps: 230400,
        };
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |_, results| {
            let kind = TransportKind::Serial;
            let (port, end) = fake::port(kind, Some(rates.clone()), results);
            let _ = ends.send(end);
            future::ready(Ok(port))
        });
        let (_commands, command_queue) = mpsc::channel(1);
        let (readiness, _readiness) = watch::channel(Readiness::Pending);
        let dump = DumpFlags::default();
        let mut worker = Worker::new(
            "fake".to_string(),
            None,
            command_queue,
            dump,
            readiness,
            open,
            unheard(),
        );
        worker.device = Some(worker.open_device().await.unwrap());
        // The device starts a session, and offers its default rate when it
        // is asked for the target.
        let mut device = opened(&mut opened_ends).await;
        crate::chan::spawn(async move {
            device
                .send(&Packet::heartbeat_session(1, DeviceRoute::root()))
                .await;
            while let Some(pkt) = device.recv().await {
                let reply = match pkt.payload() {
                    packet::Payload::RpcRequest(request) => {
                        let rate = 115200u32.to_le_bytes();
                        Packet::rpc_reply(request.id.value(), &rate, pkt.route()).ok()
                    }
                    _ => None,
                };
                if let Some(reply) = reply {
                    device.send(&reply).await;
                }
            }
        });
        let began = Instant::now();
        let gave_up = |worker: &Worker| {
            let device = worker.device.as_ref().expect("the device stays");
            matches!(device.rate_change_state, RateChange::GaveUp)
        };

        while !gave_up(&worker) {
            let turned = timeout(WAIT, worker.turn()).await;
            assert!(turned.is_ok(), "the negotiation stalled");
        }
        assert_eq!(began.elapsed(), Duration::ZERO, "the negotiation waited");

        let turned = timeout(LIVENESS_TIMEOUT / 2, worker.turn()).await;
        assert!(turned.is_err(), "the negotiation still wakes the worker");
    }

    /// Each step of the rate negotiation goes out as soon as what makes it
    /// due arrives. The device's first session heartbeat brings the query
    /// for the target rate, and the device's answer brings the switch, with
    /// no time passing in between.
    #[tokio::test(start_paused = true)]
    async fn each_step_of_the_rate_negotiation_goes_out_at_once() {
        use crate::proto::rpc::Method;

        let rates = RateInfo {
            default_bps: 115200,
            target_bps: 230400,
        };
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |_, results| {
            let kind = TransportKind::Serial;
            let (port, end) = fake::port(kind, Some(rates.clone()), results);
            let _ = ends.send(end);
            future::ready(Ok(port))
        });
        let _worker = Running::start(open, None);
        let mut device = opened(&mut opened_ends).await;
        let began = Instant::now();

        device
            .send(&Packet::heartbeat_session(1, DeviceRoute::root()))
            .await;
        let mut asked = Vec::new();
        while asked.len() < 2 {
            let pkt = timeout(WAIT, device.recv()).await.unwrap().unwrap();
            let packet::Payload::RpcRequest(request) = pkt.payload() else {
                continue;
            };
            let Method::ByName(name) = request.method else {
                continue;
            };
            asked.push((String::from_utf8_lossy(name).into_owned(), began.elapsed()));
            let rate = 230400u32.to_le_bytes();
            let reply = Packet::rpc_reply(request.id.value(), &rate, pkt.route()).unwrap();
            device.send(&reply).await;
        }

        let at_once = |name: &str| (name.to_string(), Duration::ZERO);
        assert_eq!(
            asked,
            [at_once("dev.port.rate.near"), at_once("dev.port.rate")]
        );
    }

    /// A port that fails as it goes back to the default rate loses the
    /// device, and the worker runs on to open it again.
    #[tokio::test(start_paused = true)]
    async fn a_port_that_cannot_go_back_to_the_default_rate_loses_the_device() {
        let rates = RateInfo {
            default_bps: 115200,
            target_bps: 230400,
        };
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |_, results| {
            let kind = TransportKind::Serial;
            let (port, end) = fake::port(kind, Some(rates.clone()), results);
            let _ = ends.send(end);
            future::ready(Ok(port))
        });
        let (_commands, command_queue) = mpsc::channel(1);
        let (readiness, _readiness) = watch::channel(Readiness::Pending);
        let dump = DumpFlags::default();
        let mut worker = Worker::new(
            "fake".to_string(),
            None,
            command_queue,
            dump,
            readiness,
            open,
            unheard(),
        );
        worker.device = Some(worker.open_device().await.unwrap());
        worker.device.as_mut().unwrap().rate_change_state = RateChange::RateChanged;

        // The device is unplugged, so its port ends, and nothing arrives at
        // the new rate.
        drop(opened(&mut opened_ends).await);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        worker.autonegotiation().await;

        assert!(worker.device.is_none(), "the worker kept a failed port");
    }

    /// A port that opens again is no reconnect until its device answers on
    /// it. So a silent TCP peer, or a UDP device, whose port always opens,
    /// still ends the worker once the reconnect timeout passes.
    #[tokio::test]
    async fn a_device_that_never_answers_again_ends_the_worker_with_failed_to_reconnect() {
        // Neither ever answers, and the TCP one never even accepts.
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        for url in [
            format!("tcp://{}", tcp.local_addr().unwrap()),
            format!("udp://{}", udp.local_addr().unwrap()),
        ] {
            // The device answers on a fake port at first, and is reopened at
            // `url`.
            let (ends, mut opened_ends) = mpsc::unbounded_channel();
            let reopen = opener(&Supervisor::current(), &url);
            let open = scripted(move |attempt, results| -> Opening {
                match attempt {
                    0 => {
                        let port = fake(TransportKind::Tcp, results, &ends);
                        Box::pin(future::ready(port.map(Reached::from)))
                    }
                    _ => reopen.open(results),
                }
            });
            let mut worker = Running::start(open, Some(Duration::from_millis(200)));
            let mut first = opened(&mut opened_ends).await;
            first.send(&heartbeat()).await;
            assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

            first.fail(io::ErrorKind::ConnectionReset);

            assert_eq!(
                delivered(&mut worker.port, 3).await[1..],
                ["SensorDisconnected", "FailedToReconnect"],
                "{url}"
            );
            ended(worker.task).await;
        }
    }

    /// A worker whose first open fails stops, and the link learns why.
    #[tokio::test]
    async fn a_first_open_that_fails_ends_the_worker_and_says_so() {
        let (_commands, command_queue) = mpsc::channel(1);
        let (readiness, watched) = watch::channel(Readiness::Pending);
        let dump = DumpFlags::default();
        let open = opener(&Supervisor::current(), UNREACHABLE);

        Worker::new(
            UNREACHABLE.to_string(),
            None,
            command_queue,
            dump,
            readiness,
            open,
            unheard(),
        )
        .run()
        .await;

        assert_eq!(
            *watched.borrow(),
            Readiness::Failed("failed to connect".to_string())
        );
    }
}

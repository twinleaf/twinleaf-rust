//! Serial, TCP and UDP links that carry packets, each driven by a task.
//!
//! A [`Port`] is the host's end of one link. Its task runs on the crate's
//! internal tokio runtime. It frames bytes into [`Packet`]s, writes queued
//! packets, sets serial rates, and sends heartbeats. The
//! [proxy](crate::proxy) opens one for its link to each device and one for
//! each client socket it accepts. A [`Connection`](crate::Connection) opens
//! one for its socket to the proxy. Applications do not open ports directly.
//!
//! What arrives goes to a flume channel of [`ReceiveResult`], which a task
//! reads with `recv_async` and a thread with `recv`. What leaves goes through
//! [`try_send`](Port::try_send), or [`send_async`](Port::send_async) from a
//! task.

mod codec;
#[cfg(test)]
mod fake;
#[cfg(feature = "serial")]
mod serial;
mod task;
mod tcp;
mod udp;

#[cfg(test)]
pub(crate) use fake::FakeEnd;
pub(crate) use tcp::{connect_blocking, connect_socket, CONNECT_TIMEOUT};

use super::packet::{self, Packet};
use crate::proto;
use crate::rt::Supervisor;
use std::fmt::Display;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use task::{Rules, Task};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// One item received from a port. Text and rejected frames leave the link usable.
#[derive(Debug)]
pub enum TransportEvent {
    /// A complete, validated packet.
    Packet(Packet),
    /// Plain text outside the packet protocol, on a serial link.
    #[cfg_attr(not(feature = "serial"), allow(dead_code))]
    Text(String),
    /// A malformed frame that was consumed. Decoding continues at the next boundary.
    RejectedFrame(packet::DecodeError),
}

/// A terminal failure receiving from a port.
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    /// The port's task ended and no more events remain.
    #[error("port disconnected")]
    Disconnected,
    /// A TCP header left no way to find the next packet boundary.
    #[error("lost packet framing: {0}")]
    LostFraming(packet::DecodeError),
    /// A low level I/O error ended the link.
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
}

/// A received event, or a failure that ends this receive operation.
pub type ReceiveResult = Result<TransportEvent, RecvError>;

/// A consumed frame error is an event. Lost stream framing ends the link.
fn rejected_frame(error: codec::Error) -> ReceiveResult {
    match error {
        codec::Error::Invalid(error) => Ok(TransportEvent::RejectedFrame(error)),
        codec::Error::TrailingBytes => Ok(TransportEvent::RejectedFrame(
            packet::DecodeError::PacketTooBig,
        )),
        codec::Error::LostFraming(error) => Err(RecvError::LostFraming(error)),
    }
}

/// An error sending to a [`Port`].
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The port's outgoing queue is full.
    #[error("outgoing queue full")]
    Full,
    /// The port is not connected.
    #[error("port disconnected")]
    Disconnected,
}

/// The data rates of a port that can change its rate.
#[derive(Clone)]
pub(crate) struct RateInfo {
    /// The default rate, which the port falls back to.
    pub default_bps: u32,
    /// The target rate. If communication fails, the port should fall back to
    /// `default_bps`.
    pub target_bps: u32,
}

/// The kind of link a [`Port`] talks over, for policies which depend on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransportKind {
    #[cfg_attr(not(feature = "serial"), allow(dead_code))]
    Serial,
    Tcp,
    Udp,
}

/// The IP version an address must resolve to. It forces IPv4 or IPv6 where
/// the default resolution does not work.
#[derive(Clone, Copy)]
enum AddrFamilyRestrict {
    V4,
    V6,
    Either,
}

impl AddrFamilyRestrict {
    /// Returns `true` if `addr` is of a version this allows.
    fn allows(&self, addr: &SocketAddr) -> bool {
        match self {
            AddrFamilyRestrict::V4 => addr.is_ipv4(),
            AddrFamilyRestrict::V6 => addr.is_ipv6(),
            AddrFamilyRestrict::Either => true,
        }
    }
}

/// Returns the host of `addr` if it is a `.local` name, in lower case and
/// without a trailing dot, with the port `addr` gives or else the default
/// port. Returns `None` for any other host or for a port that does not
/// parse.
#[cfg_attr(not(feature = "mdns"), allow(dead_code))]
fn local_name(addr: &str) -> Option<(String, u16)> {
    let (host, port) = match addr.rsplit_once(':') {
        Some((host, port)) => (host, port.parse().ok()?),
        None => (addr, proto::TCP_DEFAULT_PORT),
    };
    let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
    let label = host.strip_suffix(".local")?;
    let valid = !label.is_empty() && !label.contains([':', '[', ']']);
    valid.then_some((host, port))
}

/// Returns the address to connect to of those mDNS `found` for a name. Takes
/// only an address of a version `family` allows. Prefers a routable address,
/// IPv4 first, then a link-local one, then loopback. Takes the lowest address
/// of the best kind. Returns `None` if no address fits.
#[cfg_attr(not(feature = "mdns"), allow(dead_code))]
fn preferred(found: Vec<SocketAddr>, family: &AddrFamilyRestrict) -> Option<SocketAddr> {
    let rank = |addr: &SocketAddr| match addr.ip() {
        IpAddr::V4(ip) if ip.is_loopback() => 4,
        IpAddr::V6(ip) if ip.is_loopback() => 4,
        IpAddr::V4(ip) if ip.is_link_local() => 2,
        IpAddr::V6(ip) if ip.is_unicast_link_local() => 3,
        IpAddr::V4(_) => 0,
        IpAddr::V6(_) => 1,
    };
    found
        .into_iter()
        .filter(|addr| family.allows(addr) && !addr.ip().is_unspecified())
        .min_by_key(|addr| (rank(addr), addr.ip()))
}

/// Returns `true` if `url` is a TCP URL whose host is this machine, by
/// [`IpAddr::is_loopback`] or the name `localhost`. Resolves nothing.
pub(crate) fn is_local_tcp(url: &str) -> bool {
    let Some(("tcp" | "tcp4" | "tcp6", addr)) = url.split_once("://") else {
        return false;
    };
    let ip = addr
        .parse::<SocketAddr>()
        .map(|sa| sa.ip())
        .or_else(|_| addr.trim_matches(['[', ']']).parse::<IpAddr>());
    match ip {
        Ok(ip) => ip.is_loopback(),
        Err(_) => addr.rsplit_once(':').map_or(addr, |(host, _)| host) == "localhost",
    }
}

/// How a discovered device is reached.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PortInterface {
    /// Serial device behind an FTDI USB interface.
    FTDI,
    /// Serial device behind an STM32 USB interface.
    STM32,
    /// Networked device discovered via mDNS (`_twinleaf._tcp`/`_twinleaf._udp`).
    Network,
    /// Other serial port, identified by its USB `(vid, pid)`.
    Unknown(u16, u16),
}

/// A device found during discovery.
#[derive(Debug, Clone)]
pub struct DiscoveredDevice {
    /// The locator [`Connection::open`](crate::Connection::open) takes.
    pub url: String,
    /// How the device is reached.
    pub interface: PortInterface,
    /// Its `dev.name`, once known.
    pub name: Option<String>,
}

/// Lists the Twinleaf devices on local serial ports, found by USB VID and
/// PID. With `include_unknown`, it also lists every other USB serial port as
/// [`PortInterface::Unknown`]. Returns an empty list without the `serial`
/// feature.
pub fn enumerate_serial(include_unknown: bool) -> Vec<DiscoveredDevice> {
    #[cfg(not(feature = "serial"))]
    {
        let _ = include_unknown;
        Vec::new()
    }

    #[cfg(feature = "serial")]
    {
        let mut ports: Vec<DiscoveredDevice> = Vec::new();

        if let Ok(avail_ports) = serialport::available_ports() {
            for p in avail_ports.iter() {
                if let serialport::SerialPortType::UsbPort(info) = &p.port_type {
                    let interface = match (info.vid, info.pid) {
                        (0x0403, 0x6015) => PortInterface::FTDI,
                        (0x0483, 0x5740) => PortInterface::STM32,
                        (vid, pid) => {
                            if !include_unknown {
                                continue;
                            }
                            PortInterface::Unknown(vid, pid)
                        }
                    };
                    #[cfg(target_os = "macos")]
                    if p.port_name.starts_with("/dev/tty.") && !include_unknown {
                        continue;
                    }
                    ports.push(DiscoveredDevice {
                        url: format!("serial://{}", p.port_name),
                        interface,
                        name: None,
                    });
                }
            }
        }

        ports
    }
}

/// How long the system's resolver may take to look up a name.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns the socket addresses `addr` names, of the IP version `family`
/// allows, in the order to try them. Adds the default port, which TCP and UDP
/// share, if `addr` has none.
///
/// With the `mdns` feature, a `.local` name in any case, with or without a
/// trailing dot, is resolved over mDNS first. That takes at most 1.5 s. The
/// system's resolver resolves the name when mDNS finds no address that
/// fits, and any other name, within [`LOOKUP_TIMEOUT`].
///
/// Returns an error if nothing resolves in time.
async fn resolve(addr: &str, family: AddrFamilyRestrict) -> io::Result<Vec<SocketAddr>> {
    #[cfg(feature = "mdns")]
    if let Some((host, port)) = local_name(addr) {
        match preferred(super::mdns::resolve(&host, port).await, &family) {
            Some(found) => return Ok(vec![found]),
            None => log::debug!("mDNS found no address for {host}, so the system resolves it"),
        }
    }
    let looking_up = tokio::net::lookup_host(with_port(addr));
    let looked_up = tokio::time::timeout(LOOKUP_TIMEOUT, looking_up).await;
    let late = |_| {
        let message = format!("{addr} did not resolve within {LOOKUP_TIMEOUT:?}");
        io::Error::new(io::ErrorKind::TimedOut, message)
    };
    let found: Vec<_> = looked_up
        .map_err(late)??
        .filter(|found| family.allows(found))
        .collect();
    match found.is_empty() {
        true => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{addr} has no address to use"),
        )),
        false => Ok(found),
    }
}

/// Returns `addr` with the default port if it names none. An IPv6 address
/// gets the brackets a port needs.
fn with_port(addr: &str) -> String {
    let port = proto::TCP_DEFAULT_PORT;
    if addr.parse::<SocketAddr>().is_ok() {
        return addr.to_string();
    }
    if let Ok(ip) = addr.trim_matches(['[', ']']).parse::<IpAddr>() {
        return SocketAddr::new(ip, port).to_string();
    }
    match addr.rsplit_once(':').map(|(_, given)| given.parse::<u16>()) {
        Some(Ok(_)) => addr.to_string(),
        Some(Err(_)) | None => format!("{addr}:{port}"),
    }
}

/// The IP links a URL can name.
#[derive(Clone, Copy)]
enum IpLink {
    Tcp,
    Udp,
}

/// Returns the link and the address part of an IP `url`, with the IP version
/// its scheme allows. Returns `None` for any other URL.
fn ip_address(url: &str) -> Option<(IpLink, &str, AddrFamilyRestrict)> {
    let (scheme, address) = url.split_once("://")?;
    let (link, family) = match scheme {
        "tcp" => (IpLink::Tcp, AddrFamilyRestrict::Either),
        "tcp4" => (IpLink::Tcp, AddrFamilyRestrict::V4),
        "tcp6" => (IpLink::Tcp, AddrFamilyRestrict::V6),
        "udp" => (IpLink::Udp, AddrFamilyRestrict::Either),
        "udp4" => (IpLink::Udp, AddrFamilyRestrict::V4),
        "udp6" => (IpLink::Udp, AddrFamilyRestrict::V6),
        _ => return None,
    };
    Some((link, address, family))
}

/// The channel a port's task delivers what its link receives to. The task
/// drops what does not fit.
type Delivery = flume::Sender<ReceiveResult>;

/// A message to a port's task, a packet or a control request. Both share
/// one queue.
enum PacketOrControl {
    Pkt(Packet),
    /// A rate change, with where its result goes.
    SetRate(u32, oneshot::Sender<io::Result<()>>),
    /// The start of the heartbeats and the quiet limit both ends agreed on.
    KeepAlive(KeepAlive),
}

/// How a link proves both of its ends alive, once they agree to.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeepAlive {
    /// What this end sends after `every` without sending.
    pub beat: fn() -> Packet,
    pub every: Duration,
    /// How long the link may go without receiving before it is given up.
    pub quiet: Duration,
}

/// One link and the task that drives it. Packets arrive on a channel and
/// leave through [`try_send`](Self::try_send). Dropping the port closes the
/// link once the task has written what was queued, and
/// [`close`](Self::close) also waits for the task to end.
pub struct Port {
    /// The queue of packets and rate changes to the task.
    tx: mpsc::Sender<PacketOrControl>,
    /// The task, whose link is closed once it has ended.
    task: JoinHandle<Option<()>>,
    rates: Option<RateInfo>,
    kind: TransportKind,
}

/// A port whose task has not started.
struct Unstarted {
    tx: mpsc::Sender<PacketOrControl>,
    rates: Option<RateInfo>,
    kind: TransportKind,
}

impl Unstarted {
    /// Spawns `run`, the port's task, on `tasks` as the task named `name`,
    /// and returns the port.
    fn start(
        self,
        tasks: &Supervisor,
        name: impl Display,
        run: impl Future<Output = ()> + Send + 'static,
    ) -> Port {
        Port {
            tx: self.tx,
            task: tasks.spawn(name, run),
            rates: self.rates,
            kind: self.kind,
        }
    }
}

/// The default size of a port's receive channel.
pub(crate) static DEFAULT_RX_CHANNEL_SIZE: usize = 32768;

/// The default size of a port's send queue.
pub(crate) static DEFAULT_TX_CHANNEL_SIZE: usize = 32768;

/// How long a port's task may take to write one packet before it gives up
/// the link.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// How long a closed port's task may take to write what was queued and let
/// go of its link. A peer that reads none of it in that time is cut off.
pub(crate) const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

impl Port {
    /// Returns a port of the given `kind` and `rates` that has not started,
    /// and the state of its task, which drives the link by `rules`. The task
    /// delivers what it receives to `rx`. The port queues up to `tx_size`
    /// packets to it.
    fn with_task(
        kind: TransportKind,
        rates: Option<RateInfo>,
        rules: Rules,
        rx: Delivery,
        tx_size: usize,
    ) -> (Unstarted, Task) {
        let (tx, queue) = mpsc::channel(tx_size.max(1));
        let port = Unstarted { tx, rates, kind };
        (port, Task::new(rx, queue, rules))
    }

    #[cfg(feature = "serial")]
    fn open_serial(serial_config: &str, rx: Delivery, tasks: &Supervisor) -> io::Result<Port> {
        let (stream, rates) = serial::open(serial_config)?;
        let (port, task) = Port::with_task(
            TransportKind::Serial,
            Some(rates),
            serial::RULES,
            rx,
            DEFAULT_TX_CHANNEL_SIZE,
        );
        let framing =
            codec::serial::Codec::with_timing(serial::RULES.holdoff, serial::RULES.stale_after);
        let run = task.run_stream(stream, framing);
        Ok(port.start(tasks, format_args!("twinleaf port {serial_config}"), run))
    }

    /// Opens a [`Port`] over `link` to `address`, with its task on `tasks`.
    /// Tries each address `address` resolves to in turn.
    async fn open_ip(
        link: IpLink,
        address: &str,
        family: AddrFamilyRestrict,
        rx: Delivery,
        tasks: &Supervisor,
    ) -> io::Result<Port> {
        let found = resolve(address, family).await?;
        match link {
            IpLink::Tcp => {
                let stream = tcp::connect(&found).await?;
                Ok(Port::start_tcp(
                    stream,
                    tcp::RULES,
                    rx,
                    DEFAULT_TX_CHANNEL_SIZE,
                    tasks,
                ))
            }
            IpLink::Udp => {
                let peer = found[0];
                let socket = udp::bind(&peer).await?;
                let (port, task) = Port::with_task(
                    TransportKind::Udp,
                    None,
                    udp::RULES,
                    rx,
                    DEFAULT_TX_CHANNEL_SIZE,
                );
                let run = task.run_datagram(socket, peer);
                Ok(port.start(tasks, format_args!("twinleaf port udp://{peer}"), run))
            }
        }
    }

    /// Starts the task of a port over the TCP `stream` by `rules` on `tasks`.
    /// The task delivers what it receives to `rx`, and the port queues up to
    /// `tx_size` packets to send.
    fn start_tcp(
        stream: tokio::net::TcpStream,
        rules: Rules,
        rx: Delivery,
        tx_size: usize,
        tasks: &Supervisor,
    ) -> Port {
        let peer = stream
            .peer_addr()
            .map_or(String::new(), |peer| peer.to_string());
        let (port, task) = Port::with_task(TransportKind::Tcp, None, rules, rx, tx_size);
        let run = task.run_stream(stream, codec::tcp::Codec::default());
        port.start(tasks, format_args!("twinleaf port tcp://{peer}"), run)
    }

    /// Opens a [`Port`] for the device or proxy at `url`. The port delivers
    /// what it receives to `rx` and drops what does not fit. It closes once
    /// nothing receives from `rx`. Blocks while it resolves the address and
    /// opens the link.
    ///
    /// A valid `url` has one of these formats.
    ///
    /// - `serial://port[:target_bps[:default_bps]]`, which requires the
    ///   `serial` feature. `target_bps` and `default_bps` default to 115200.
    ///   `serial://` may be omitted for a port that starts with `COM` on
    ///   Windows or `/dev/` on Unix.
    /// - `tcp://address[:port]`. The schemes `tcp4` and `tcp6` force IPv4 or
    ///   IPv6.
    /// - `udp://address[:port]`. The schemes `udp4` and `udp6` force IPv4 or
    ///   IPv6.
    ///
    /// Returns an error if `url` is not valid or the link cannot be opened.
    pub(crate) fn new(url: &str, rx: Delivery) -> io::Result<Port> {
        Port::new_on(url, rx, &Supervisor::new())
    }

    /// Opens a [`Port`] for `url` as [`new`](Self::new) does, with its task
    /// on `tasks`. Blocks while it resolves the address and opens the link.
    ///
    /// # Errors
    ///
    /// Returns an error if `url` is not valid or the link cannot be opened.
    pub(crate) fn new_on(url: &str, rx: Delivery, tasks: &Supervisor) -> io::Result<Port> {
        // Special case: serial ports can be given directly
        #[cfg(all(feature = "serial", unix))]
        if url.starts_with("/dev/") {
            return Self::open_serial(url, rx, tasks);
        }
        #[cfg(all(feature = "serial", target_os = "windows"))]
        if url.starts_with("COM") {
            return Self::open_serial(url, rx, tasks);
        }

        if Port::opens_as_future(url) {
            let (url, on) = (url.to_string(), tasks.clone());
            return Supervisor::new().run("twinleaf port open", async move {
                Port::open(&url, rx, &on).await
            });
        }
        match url.split_once("://") {
            #[cfg(feature = "serial")]
            Some(("serial", port)) => Self::open_serial(port, rx, tasks),
            #[cfg(not(feature = "serial"))]
            Some(("serial", _)) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "serial support is disabled",
            )),
            Some(_) | None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid TIO transport locator: {url}"),
            )),
        }
    }

    /// Opens a [`Port`] for `url` as [`new`](Self::new) does, without
    /// blocking a worker of the runtime. A TCP or UDP link resolves its
    /// address and opens as a future, and dropping it at any await leaves
    /// nothing open. Its task runs on `tasks`. Any other link opens on a blocking
    /// thread of `tasks`.
    ///
    /// # Errors
    ///
    /// Returns an error if `url` is not valid or the link cannot be opened.
    pub(crate) async fn open(url: &str, rx: Delivery, tasks: &Supervisor) -> io::Result<Port> {
        let Some((link, address, family)) = ip_address(url) else {
            let (url, on) = (url.to_string(), tasks.clone());
            let opening = tasks.spawn_blocking(move || Port::new_on(&url, rx, &on));
            return opening.await.map_err(io::Error::other)?;
        };
        Port::open_ip(link, address, family, rx, tasks).await
    }

    /// Returns `true` if dropping an [`open`](Self::open) of `url` in flight
    /// leaves nothing open, as for a TCP or UDP link.
    pub(crate) fn opens_as_future(url: &str) -> bool {
        ip_address(url).is_some()
    }

    /// Closes the port. Waits until its task has written what was queued and
    /// closed the link, for at most `within`. Then ends the task, which
    /// closes the link at once. Returns `true` if the task ended in time.
    pub(crate) async fn close(self, within: Duration) -> bool {
        let Port { tx, mut task, .. } = self;
        drop(tx);
        if tokio::time::timeout(within, &mut task).await.is_ok() {
            return true;
        }
        // An aborted task has dropped its link by the time its handle
        // resolves.
        task.abort();
        let _ = task.await;
        false
    }

    /// Creates a [`Port`] over the connected TCP `stream`, such as the one a
    /// control call opens to the proxy. It delivers what it receives to `rx`
    /// as [`new`](Self::new) does, and queues up to `tx_size` packets to
    /// send. Returns an error if the stream cannot be set up.
    pub(crate) fn from_tcp_stream(
        stream: std::net::TcpStream,
        rx: Delivery,
        tx_size: usize,
    ) -> io::Result<Port> {
        Ok(Port::start_tcp(
            tcp::from_std(stream)?,
            tcp::RULES,
            rx,
            tx_size,
            &Supervisor::new(),
        ))
    }

    /// Creates a [`Port`] over `stream`, a connected TCP stream of the
    /// crate's runtime, such as a client the proxy accepted. Its task runs on
    /// `tasks`. It delivers what it receives to `rx` as [`new`](Self::new)
    /// does, and queues up to `tx_size` packets to send. Once the port is
    /// dropped and its queue is written, the link closes in stages. The task
    /// shuts down its sending side, and waits up to 500 ms for the client to
    /// close first.
    ///
    /// # Errors
    ///
    /// Returns an error if Nagle's algorithm cannot be turned off.
    pub(crate) fn from_tcp(
        stream: tokio::net::TcpStream,
        rx: Delivery,
        tx_size: usize,
        tasks: &Supervisor,
    ) -> io::Result<Port> {
        stream.set_nodelay(true)?;
        Ok(Port::start_tcp(
            stream,
            tcp::CLIENT_RULES,
            rx,
            tx_size,
            tasks,
        ))
    }

    /// Sends `packet` to the port, and waits while the port's queue is full.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::Disconnected`] if the port's task has stopped.
    pub(crate) async fn send_async(&self, packet: Packet) -> Result<(), SendError> {
        let sent = self.tx.send(PacketOrControl::Pkt(packet)).await;
        sent.map_err(|_| SendError::Disconnected)
    }

    /// Sends `packet` to the port without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`SendError::Full`] if the queue is full, and
    /// [`SendError::Disconnected`] if the port's task has stopped.
    pub fn try_send(&self, packet: Packet) -> Result<(), SendError> {
        self.tx
            .try_send(PacketOrControl::Pkt(packet))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SendError::Full,
                mpsc::error::TrySendError::Closed(_) => SendError::Disconnected,
            })
    }

    /// Returns the kind of link this port talks over.
    pub(crate) fn kind(&self) -> TransportKind {
        self.kind
    }

    /// Returns the link's rate information, or `None` if its rate cannot
    /// change.
    pub(crate) fn rate_info(&self) -> Option<RateInfo> {
        self.rates.clone()
    }

    /// Has the link send heartbeats and give up a quiet peer from now on, as
    /// `keep_alive` says. A port whose task has stopped has nothing to keep
    /// alive, and its receiver hears that it ended.
    pub(crate) async fn keep_alive(&self, keep_alive: KeepAlive) {
        let _ = self.tx.send(PacketOrControl::KeepAlive(keep_alive)).await;
    }

    /// Sets the link's data rate on the host side, once the packets queued
    /// before are written, and waits until the task has applied it.
    ///
    /// # Errors
    ///
    /// Returns an [`io::ErrorKind::BrokenPipe`] error if the port has
    /// closed, and an error if the link's rate cannot change.
    pub(crate) async fn set_rate(&self, rate: u32) -> io::Result<()> {
        let closed = || io::Error::new(io::ErrorKind::BrokenPipe, "the port closed");
        let (result, applied) = oneshot::channel();
        let queued = self.tx.send(PacketOrControl::SetRate(rate, result)).await;
        queued.map_err(|_| closed())?;
        applied.await.map_err(|_| closed())?
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serial")]
    use bytes::BytesMut;
    use fake::{FakeStream, PIPE_SIZE};
    use flume::{Receiver, RecvTimeoutError};
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        time::{Duration, Instant},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    use super::*;
    use crate::rt;

    /// A TCP URL on this machine is local by address or by `localhost`. Any
    /// other scheme or host is not.
    #[test]
    fn only_a_tcp_url_on_this_machine_is_local_tcp() {
        for url in [
            "tcp://127.0.0.1:7855",
            "tcp://localhost",
            "tcp4://localhost:17855",
            "tcp6://[::1]:7855",
            "tcp6://::1",
        ] {
            assert!(is_local_tcp(url), "{url}");
        }
        for url in [
            "udp://127.0.0.1:7855",
            "udp4://localhost",
            "tcp://192.0.2.1:7855",
            "tcp://sensor.local",
            "127.0.0.1:7855",
            "serial:///dev/ttyACM0",
            "auto",
        ] {
            assert!(!is_local_tcp(url), "{url}");
        }
    }

    /// A `.local` name is found in any case, with or without its trailing
    /// dot, and with or without a port. Nothing else is a `.local` name.
    #[test]
    fn a_local_name_is_found_in_any_case_with_or_without_its_dot() {
        let found = |host: &str, port| Some((host.to_string(), port));
        assert_eq!(local_name("Sensor.LOCAL"), found("sensor.local", 7855));
        assert_eq!(
            local_name("sensor.local.:7000"),
            found("sensor.local", 7000)
        );
        assert_eq!(local_name("sensor.local:7000"), found("sensor.local", 7000));
        assert_eq!(local_name("sensor.local."), found("sensor.local", 7855));

        for other in [
            ".local",
            "local",
            "sensor.local:port",
            "sensor.example.com:7855",
            "192.168.1.2:7855",
            "[fe80::1]:7855",
            "fe80::1",
        ] {
            assert_eq!(local_name(other), None, "{other}");
        }
    }

    /// Of the addresses mDNS finds, the one taken is of the version the URL
    /// allows, routable before link-local and IPv4 before IPv6. A link-local
    /// IPv6 address keeps its scope.
    #[test]
    fn an_mdns_address_is_chosen_by_version_and_reach() {
        let addrs = |list: &[&str]| -> Vec<SocketAddr> {
            list.iter().map(|addr| addr.parse().unwrap()).collect()
        };
        let all = addrs(&[
            "127.0.0.1:7855",
            "169.254.3.4:7855",
            "[fe80::1%3]:7855",
            "[fd00::5]:7855",
            "192.168.1.20:7855",
            "10.0.0.9:7855",
            "0.0.0.0:7855",
        ]);
        let chosen = |found: &[SocketAddr], family| preferred(found.to_vec(), &family);

        assert_eq!(
            chosen(&all, AddrFamilyRestrict::Either),
            Some("10.0.0.9:7855".parse().unwrap())
        );
        assert_eq!(
            chosen(&all, AddrFamilyRestrict::V4),
            Some("10.0.0.9:7855".parse().unwrap())
        );
        assert_eq!(
            chosen(&all, AddrFamilyRestrict::V6),
            Some("[fd00::5]:7855".parse().unwrap())
        );

        let local = addrs(&["[fe80::1%3]:7855", "169.254.3.4:7855", "127.0.0.1:7855"]);
        assert_eq!(
            chosen(&local, AddrFamilyRestrict::Either),
            Some("169.254.3.4:7855".parse().unwrap())
        );
        let Some(SocketAddr::V6(scoped)) = chosen(&local, AddrFamilyRestrict::V6) else {
            panic!("no IPv6 address was chosen");
        };
        assert_eq!((scoped.ip().segments()[0], scoped.scope_id()), (0xfe80, 3));

        let v4_only = addrs(&["192.168.1.20:7855", "0.0.0.0:7855"]);
        assert_eq!(chosen(&v4_only, AddrFamilyRestrict::V6), None);
        assert_eq!(chosen(&[], AddrFamilyRestrict::Either), None);
    }

    /// A loopback connect waits for a handshake as long as any other, but a
    /// port nothing listens on refuses it at once.
    #[test]
    fn a_refused_loopback_connect_fails_at_once() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let started = Instant::now();
        let refused = rt::handle().block_on(tcp::connect(&[address])).map(drop);

        let took = started.elapsed();
        assert_eq!(
            refused.map_err(|error| error.kind()),
            Err(io::ErrorKind::ConnectionRefused)
        );
        assert!(took < Duration::from_secs(1), "it took {took:?}");
    }

    #[test]
    fn dropping_idle_port_closes_transport() {
        let (port, mut peer, results) = tcp_port_reporting();
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

        // Confirm the task is receiving, with nothing queued to send.
        peer.write_all(Packet::heartbeat(proto::DeviceRoute::root()).as_bytes())
            .unwrap();
        assert!(results
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .is_ok());
        drop(port);

        // The peer stays connected and sends nothing else to wake the task.
        assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    }

    /// Backpressure delays packets, it never eats them: every packet a caller
    /// queued reaches the peer once the peer starts reading again.
    #[test]
    fn a_peer_that_stops_reading_loses_no_queued_packet() {
        let (port, mut peer, _results) = tcp_port_reporting();

        let packet = Packet::rpc_reply(0, &[0u8; 498], proto::DeviceRoute::root()).unwrap();
        let queued = 4000;
        for _ in 0..queued {
            port.try_send(packet.clone()).unwrap();
        }
        drop(port);

        let mut sunk = Vec::new();
        peer.read_to_end(&mut sunk).unwrap();
        assert_eq!(sunk.len(), queued * packet.as_bytes().len());
    }

    /// A port over a [`FakeStream`], and what a test watches it with.
    struct Fake {
        port: Port,
        /// What the port receives.
        results: Receiver<ReceiveResult>,
        /// Disconnects once the task has dropped the stream.
        gone: mpsc::Receiver<()>,
        /// The other end of the pipe.
        peer: DuplexStream,
    }

    /// Returns a port over a [`FakeStream`] whose task runs on the test's
    /// runtime, follows `rules` and frames packets with `framing`. A `stuck`
    /// stream starts with its pipe full, so it writes nothing until the peer
    /// reads.
    fn fake<C>(rules: Rules, framing: C, stuck: bool) -> Fake
    where
        C: tokio_util::codec::Decoder<Item = TransportEvent, Error = RecvError>
            + tokio_util::codec::Encoder<Packet, Error = RecvError>
            + Send
            + Unpin
            + 'static,
    {
        let (pipe, peer) = tokio::io::duplex(PIPE_SIZE);
        let (mut stream, gone, _) = FakeStream::new(pipe);
        let (rx, results) = flume::bounded(DEFAULT_RX_CHANNEL_SIZE);
        let (port, task) =
            Port::with_task(TransportKind::Tcp, None, rules, rx, DEFAULT_TX_CHANNEL_SIZE);
        let run = async move {
            if stuck {
                stream.write_all(&[0; PIPE_SIZE]).await.unwrap();
            }
            task.run_stream(stream, framing).await;
        };
        Fake {
            port: port.start(&Supervisor::current(), "a test port", run),
            results,
            gone,
            peer,
        }
    }

    /// A peer that never takes another byte cannot pin the port's task,
    /// whoever still holds the port. The task lets go of the link once its
    /// write budget is spent, and not before.
    #[tokio::test(start_paused = true)]
    async fn a_port_stuck_draining_gives_up_on_its_write_budget() {
        let Fake {
            port,
            gone,
            peer: _peer,
            ..
        } = fake(Rules::NONE, codec::tcp::Codec::default(), true);

        port.try_send(Packet::heartbeat(proto::DeviceRoute::root()))
            .unwrap();

        tokio::time::sleep(DRAIN_TIMEOUT - Duration::from_millis(1)).await;
        assert!(matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)));
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(matches!(
            gone.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    /// A link that closes in stages shuts down its sending side once the
    /// port is dropped and its queue is written. The peer reads all of it,
    /// then the end of the stream, and the task lets go once the peer closes.
    #[tokio::test(start_paused = true)]
    async fn a_link_closing_in_stages_lets_go_once_its_peer_closes() {
        let Fake {
            port,
            gone,
            mut peer,
            ..
        } = fake(tcp::CLIENT_RULES, codec::tcp::Codec::default(), false);
        let last = Packet::heartbeat(proto::DeviceRoute::root());
        port.try_send(last.clone()).unwrap();
        drop(port);

        let mut read = Vec::new();
        peer.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, last.as_bytes());
        assert!(
            matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "the task let go before its peer closed"
        );

        drop(peer);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(matches!(
            gone.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    /// A peer that never closes holds a link closing in stages for the time
    /// the link waits, and no longer, though it keeps sending.
    #[tokio::test(start_paused = true)]
    async fn a_link_closing_in_stages_lets_go_of_a_peer_that_never_closes() {
        let Fake {
            port,
            gone,
            mut peer,
            ..
        } = fake(tcp::CLIENT_RULES, codec::tcp::Codec::default(), false);
        drop(port);
        let mut read = Vec::new();
        peer.read_to_end(&mut read).await.unwrap();

        let beat = Packet::heartbeat(proto::DeviceRoute::root());
        let sending = tokio::spawn(async move {
            while peer.write_all(beat.as_bytes()).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        tokio::time::sleep(tcp::CLIENT_HALF_CLOSE - Duration::from_millis(1)).await;
        assert!(matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)));
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(matches!(
            gone.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        sending.await.unwrap();
    }

    /// Closing a port returns once its task has dropped the link. A task
    /// stuck writing is ended once the wait runs out, which drops the link
    /// before the close returns.
    #[tokio::test(start_paused = true)]
    async fn closing_a_port_waits_for_its_task_and_then_ends_it() {
        let idle = fake(Rules::NONE, codec::tcp::Codec::default(), false);
        assert!(idle.port.close(Duration::from_secs(2)).await);
        assert!(matches!(
            idle.gone.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));

        let stuck = fake(Rules::NONE, codec::tcp::Codec::default(), true);
        stuck
            .port
            .try_send(Packet::heartbeat(proto::DeviceRoute::root()))
            .unwrap();
        let closing = tokio::time::Instant::now();
        assert!(!stuck.port.close(Duration::from_millis(200)).await);
        assert_eq!(closing.elapsed(), Duration::from_millis(200));
        assert!(matches!(
            stuck.gone.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    /// A kept alive port sends its beat while idle, stays open while its peer
    /// sends, and gives the peer up once it has been quiet too long.
    #[tokio::test(start_paused = true)]
    async fn a_kept_alive_port_beats_and_gives_up_a_quiet_peer() {
        let (send, receive) = flume::unbounded();
        let (port, mut peer) = Port::fake(TransportKind::Tcp, None, send);
        let keep_alive = KeepAlive {
            beat: Packet::alive,
            every: Duration::from_secs(1),
            quiet: Duration::from_secs(5),
        };
        port.keep_alive(keep_alive).await;
        let kept = tokio::time::Instant::now();

        for _ in 0..8 {
            let beat = peer.recv().await.expect("the port beats");
            assert!(matches!(
                beat.payload(),
                packet::Payload::ProxyRx(packet::ProxyRx::Alive)
            ));
            peer.send(&Packet::heartbeat(proto::DeviceRoute::root()))
                .await;
        }
        assert_eq!(kept.elapsed(), Duration::from_secs(8));
        let quiet = tokio::time::Instant::now();
        while let Ok(Ok(_)) = receive.recv_async().await {}

        assert_eq!(quiet.elapsed(), keep_alive.quiet);
        assert!(peer.released());
    }

    /// A terminal read error is reported once and releases the link even
    /// while the caller retains its port handle.
    #[tokio::test]
    async fn a_read_error_ends_the_transport_without_dropping_the_port() {
        let (send, receive) = flume::unbounded();
        let (port, mut peer) = Port::fake(TransportKind::Tcp, None, send);
        peer.fail(io::ErrorKind::ConnectionReset);
        assert!(matches!(
            receive.recv_async().await.unwrap(),
            Err(RecvError::IO(error)) if error.kind() == io::ErrorKind::ConnectionReset
        ));
        assert!(matches!(
            receive.recv_async().await,
            Err(flume::RecvError::Disconnected)
        ));
        assert!(peer.released());
        assert!(matches!(
            port.try_send(Packet::heartbeat(proto::DeviceRoute::root())),
            Err(SendError::Disconnected)
        ));
    }

    /// A stalled sink does not keep incoming packets from the framed source.
    #[tokio::test(start_paused = true)]
    async fn a_blocked_framed_write_keeps_receiving() {
        let Fake {
            port,
            results,
            gone,
            mut peer,
        } = fake(Rules::NONE, codec::tcp::Codec::default(), true);
        let packet = Packet::heartbeat_session(7, proto::DeviceRoute::root());
        port.try_send(packet.clone()).unwrap();
        peer.write_all(packet.as_bytes()).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(1), results.recv_async())
            .await
            .expect("a pending write must not block reads")
            .unwrap()
            .unwrap();
        let TransportEvent::Packet(received) = received else {
            panic!("expected the incoming heartbeat")
        };
        assert_eq!(received.as_bytes(), packet.as_bytes());
        assert!(matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }

    /// Datagram framing rejects malformed and empty datagrams independently,
    /// and sends each outgoing packet as exactly one datagram.
    #[tokio::test]
    async fn udp_framing_recovers_at_the_next_datagram() {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let local = socket.local_addr().unwrap();
        let (send, received) = flume::unbounded();
        let (port, task) = Port::with_task(
            TransportKind::Udp,
            None,
            Rules::NONE,
            send,
            DEFAULT_TX_CHANNEL_SIZE,
        );
        let port = port.start(
            &Supervisor::current(),
            "a UDP framing test",
            task.run_datagram(socket, peer.local_addr().unwrap()),
        );
        let packet = Packet::heartbeat_session(7, proto::DeviceRoute::root());
        let mut trailing = packet.as_bytes().to_vec();
        trailing.push(0);
        let stranger = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        stranger
            .send_to(
                Packet::heartbeat_session(999, proto::DeviceRoute::root()).as_bytes(),
                local,
            )
            .await
            .unwrap();
        peer.send_to(&trailing, local).await.unwrap();
        peer.send_to(&[], local).await.unwrap();
        peer.send_to(packet.as_bytes(), local).await.unwrap();
        let arrivals = async {
            assert!(matches!(
                received.recv_async().await.unwrap(),
                Ok(TransportEvent::RejectedFrame(
                    packet::DecodeError::PacketTooBig
                ))
            ));
            assert!(matches!(
                received.recv_async().await.unwrap(),
                Ok(TransportEvent::RejectedFrame(
                    packet::DecodeError::PacketTooSmall
                ))
            ));
            let Ok(TransportEvent::Packet(actual)) = received.recv_async().await.unwrap() else {
                panic!("the following datagram did not decode")
            };
            assert_eq!(actual.as_bytes(), packet.as_bytes());
        };
        tokio::time::timeout(Duration::from_secs(2), arrivals)
            .await
            .unwrap();
        port.try_send(packet.clone()).unwrap();
        let mut raw = [0; 1024];
        let size = tokio::time::timeout(Duration::from_secs(2), peer.recv(&mut raw))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "UDP send did not arrive, transport reported {:?}",
                    received.try_recv()
                )
            })
            .unwrap();
        assert_eq!(&raw[..size], packet.as_bytes());
    }

    /// Both checksum failures and valid-checksum frames with trailing bytes
    /// are consumed as rejected frames, and leave the next packet readable.
    #[cfg(feature = "serial")]
    #[tokio::test]
    async fn rejected_serial_frames_leave_the_transport_open() {
        use codec::serial::Codec;
        let Fake {
            port: _port,
            results,
            gone,
            mut peer,
        } = fake(Rules::NONE, Codec::default(), false);
        let packet = Packet::heartbeat(proto::DeviceRoute::root());
        let mut frame = BytesMut::new();
        Codec::default().encode(&packet, &mut frame);
        let mut bad_crc = frame.to_vec();
        bad_crc[2] ^= 1;
        peer.write_all(&bad_crc).await.unwrap();

        let mut trailing = packet.as_bytes().to_vec();
        trailing.push(0);
        let mut encoded = vec![0; proto::serial::max_serialized_size(trailing.len())];
        let size = proto::serial::serialize(&trailing, &mut encoded).unwrap();
        peer.write_all(&encoded[..size]).await.unwrap();
        peer.write_all(&frame).await.unwrap();

        assert!(matches!(
            results.recv_async().await.unwrap(),
            Ok(TransportEvent::RejectedFrame(packet::DecodeError::CRC32))
        ));
        assert!(matches!(
            results.recv_async().await.unwrap(),
            Ok(TransportEvent::RejectedFrame(
                packet::DecodeError::PacketTooBig
            ))
        ));
        let Ok(TransportEvent::Packet(next)) = results.recv_async().await.unwrap() else {
            panic!("the valid packet after the rejected frames did not arrive")
        };
        assert_eq!(next.as_bytes(), packet.as_bytes());
        assert!(matches!(gone.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }

    /// Reads one packet of `size` bytes from `peer`.
    async fn read_packet(peer: &mut DuplexStream, size: usize) -> Vec<u8> {
        let mut packet = vec![0; size];
        peer.read_exact(&mut packet).await.unwrap();
        packet
    }

    /// A port with a heartbeat sends one once it has written nothing for
    /// the interval. Any packet it writes puts the next heartbeat off.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_port_sends_a_heartbeat_each_interval() {
        let interval = Duration::from_millis(100);
        let rules = Rules {
            heartbeat: Some(interval),
            ..Rules::NONE
        };
        let Fake { port, mut peer, .. } = fake(rules, codec::tcp::Codec::default(), false);
        let heartbeat = Packet::heartbeat(proto::DeviceRoute::root());
        let request = Packet::rpc_request("dev.name", &[], 7, proto::DeviceRoute::root()).unwrap();
        let opened = tokio::time::Instant::now();
        let at = |offset: u64| {
            let elapsed = opened.elapsed().as_millis();
            assert!(
                (offset as u128..=offset as u128 + 1).contains(&elapsed),
                "arrived at {elapsed} ms, not {offset} ms"
            );
        };

        let first = read_packet(&mut peer, heartbeat.as_bytes().len()).await;
        assert_eq!(first, heartbeat.as_bytes());
        at(100);

        tokio::time::sleep(Duration::from_millis(50)).await;
        port.try_send(request.clone()).unwrap();
        assert_eq!(
            read_packet(&mut peer, request.as_bytes().len()).await,
            request.as_bytes()
        );
        at(150);

        read_packet(&mut peer, heartbeat.as_bytes().len()).await;
        at(250);
    }

    /// During its holdoff a serial port discards the first bytes to arrive,
    /// even text, and every packet after them, but passes text on. It sends
    /// heartbeats and holds what is queued until the holdoff ends.
    #[cfg(feature = "serial")]
    #[tokio::test(start_paused = true)]
    async fn a_serial_port_in_its_holdoff_passes_only_text() {
        use codec::serial::{Codec, Frame};

        let Fake {
            port,
            results,
            mut peer,
            ..
        } = fake(
            serial::RULES,
            Codec::with_timing(serial::RULES.holdoff, serial::RULES.stale_after),
            false,
        );
        let opened = tokio::time::Instant::now();
        let frame = |route: &str| {
            let packet = Packet::heartbeat(route.parse().unwrap());
            let mut frame = BytesMut::new();
            Codec::default().encode(&packet, &mut frame);
            frame
        };
        let step = Duration::from_millis(10);

        port.try_send(Packet::heartbeat("/7".parse().unwrap()))
            .unwrap();
        peer.write_all(b"stale\n").await.unwrap();
        tokio::time::sleep(step).await;
        peer.write_all(b"boot\n").await.unwrap();
        peer.write_all(&frame("/2")).await.unwrap();

        // The peer sees heartbeats to the root, then the queued packet once
        // the holdoff is over.
        let mut decoder = Codec::default();
        let mut sent = BytesMut::new();
        let first_route = loop {
            peer.read_buf(&mut sent).await.unwrap();
            if let Some(Frame::Packet(packet)) = decoder.decode(&mut sent).unwrap() {
                if packet.route() != proto::DeviceRoute::root() {
                    break packet.route();
                }
            }
        };
        assert_eq!(first_route, "/7".parse().unwrap());
        assert!(opened.elapsed() >= serial::RULES.holdoff.unwrap());

        peer.write_all(&frame("/3")).await.unwrap();
        tokio::time::sleep(step).await;
        assert!(matches!(results.try_recv(), Ok(Ok(TransportEvent::Text(text))) if text == "boot"));
        let TransportEvent::Packet(after) = results.try_recv().unwrap().unwrap() else {
            panic!("expected a packet")
        };
        assert_eq!(after.route(), "/3".parse().unwrap());
        assert!(results.try_recv().is_err());
    }

    /// A task that sends to a full queue waits for room, and every packet
    /// still reaches the peer in order.
    #[test]
    fn sending_to_a_full_queue_waits_for_room() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        let (rx, _results) = flume::bounded(DEFAULT_RX_CHANNEL_SIZE);
        let port = Port::from_tcp_stream(stream, rx, 1).unwrap();

        let packets: Vec<Packet> = (0..2000)
            .map(|id| Packet::rpc_reply(id, &[0u8; 498], proto::DeviceRoute::root()).unwrap())
            .collect();
        let expected: Vec<u8> = packets.iter().flat_map(|p| p.as_bytes()).copied().collect();
        let reader = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            let mut sunk = Vec::new();
            peer.read_to_end(&mut sunk).unwrap();
            sunk
        });
        rt::handle().spawn(async move {
            for packet in packets {
                port.send_async(packet).await.unwrap();
            }
        });

        assert!(reader.join().unwrap() == expected);
    }

    /// A TCP port, its peer, and the channel it delivers what it receives
    /// to.
    fn tcp_port_reporting() -> (Port, std::net::TcpStream, Receiver<ReceiveResult>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("tcp://{}", listener.local_addr().unwrap());
        let (received, results) = flume::bounded(DEFAULT_RX_CHANNEL_SIZE);
        let port = Port::new(&url, received).unwrap();
        let (peer, _) = listener.accept().unwrap();
        (port, peer, results)
    }

    /// An address that names no port gets the default one, in brackets for
    /// an IPv6 address. One that names a port keeps it.
    #[test]
    fn an_address_without_a_port_gets_the_default() {
        assert_eq!(with_port("10.0.0.2"), "10.0.0.2:7855");
        assert_eq!(with_port("10.0.0.2:99"), "10.0.0.2:99");
        assert_eq!(with_port("::1"), "[::1]:7855");
        assert_eq!(with_port("[::1]"), "[::1]:7855");
        assert_eq!(with_port("[::1]:99"), "[::1]:99");
        assert_eq!(with_port("sensor.example"), "sensor.example:7855");
        assert_eq!(with_port("sensor.example:99"), "sensor.example:99");
    }

    /// A name resolves to every address of the IP version its scheme allows,
    /// and a connect goes on to the next address when one refuses.
    #[test]
    fn a_connect_tries_each_address_the_name_resolves_to() {
        let resolved = rt::handle().block_on(resolve("localhost:99", AddrFamilyRestrict::V4));
        assert!(resolved
            .unwrap()
            .iter()
            .all(|found| found.is_ipv4() && found.port() == 99));
        let ip_v6 = rt::handle().block_on(resolve("127.0.0.1", AddrFamilyRestrict::V6));
        assert_eq!(
            ip_v6.map_err(|error| error.kind()),
            Err(io::ErrorKind::NotFound)
        );

        let closed = TcpListener::bind("127.0.0.1:0").unwrap();
        let refusing = closed.local_addr().unwrap();
        drop(closed);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let answering = listener.local_addr().unwrap();
        let connected = rt::handle().block_on(tcp::connect(&[refusing, answering]));
        assert_eq!(connected.unwrap().peer_addr().unwrap(), answering);
    }

    /// A TCP port opened as a future connects without a blocking thread, and
    /// delivers what its peer sends.
    #[test]
    fn a_tcp_port_opened_as_a_future_delivers_what_arrives() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("tcp://{}", listener.local_addr().unwrap());
        assert!(Port::opens_as_future(&url));
        let (received, results) = flume::bounded(DEFAULT_RX_CHANNEL_SIZE);
        let tasks = Supervisor::new();
        let opening = tasks.run("a test open", {
            let tasks = tasks.clone();
            async move { Port::open(&url, received, &tasks).await }
        });
        let _port = opening.unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        let heartbeat = Packet::heartbeat(proto::DeviceRoute::root());

        peer.write_all(heartbeat.as_bytes()).unwrap();

        let arrived = results.recv_timeout(Duration::from_secs(2)).unwrap();
        let TransportEvent::Packet(arrived) = arrived.unwrap() else {
            panic!("expected a packet")
        };
        assert_eq!(arrived.as_bytes(), heartbeat.as_bytes());
    }

    /// A packet whose header parses but whose payload does not is skipped on
    /// its own, and the packet after it still arrives.
    #[test]
    fn a_malformed_tcp_packet_is_skipped_and_the_next_arrives() {
        let (_port, mut peer, results) = tcp_port_reporting();
        let heartbeat = Packet::heartbeat(proto::DeviceRoute::root());

        // A PROXY_RX header announcing an empty payload, which has no kind byte.
        peer.write_all(&[proto::PacketType::PROXY_RX.value(), 0, 0, 0])
            .unwrap();
        peer.write_all(heartbeat.as_bytes()).unwrap();

        let wait = Duration::from_secs(2);
        assert!(matches!(
            results.recv_timeout(wait).unwrap(),
            Ok(TransportEvent::RejectedFrame(
                packet::DecodeError::PayloadTooShort
            ))
        ));
        let TransportEvent::Packet(next) = results.recv_timeout(wait).unwrap().unwrap() else {
            panic!("expected the next packet")
        };
        assert_eq!(next.as_bytes(), heartbeat.as_bytes());
    }

    /// A header that does not parse leaves no way to find the next packet, so
    /// the error is reported once and the port closes.
    #[test]
    fn a_tcp_stream_that_loses_its_framing_closes() {
        let (_port, mut peer, results) = tcp_port_reporting();
        let wait = Duration::from_secs(2);
        peer.set_read_timeout(Some(wait)).unwrap();

        let mut wire = vec![0, 0, 0, 0];
        wire.extend_from_slice(Packet::heartbeat(proto::DeviceRoute::root()).as_bytes());
        peer.write_all(&wire).unwrap();
        assert!(matches!(
            results.recv_timeout(wait).unwrap(),
            Err(RecvError::LostFraming(
                packet::DecodeError::InvalidPacketType
            ))
        ));
        assert!(matches!(
            results.recv_timeout(wait),
            Err(RecvTimeoutError::Disconnected)
        ));
        // The port may close with the heartbeat still unread, which resets
        // the connection rather than ending it. Either way it is closed.
        match peer.read(&mut [0]) {
            Ok(0) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            other => panic!("the stream did not close: {other:?}"),
        }
    }
}

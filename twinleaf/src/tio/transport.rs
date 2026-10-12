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
pub(crate) use tcp::LOOPBACK_CONNECT_TIMEOUT;

use super::packet::{self, Packet};
use crate::proto;
use crate::rt::Supervisor;
use std::fmt::Display;
use std::future::Future;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use task::{Rules, Task};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// An error receiving from a [`Port`].
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    /// The port disconnected.
    #[error("port disconnected")]
    Disconnected,
    /// The data did not decode as a packet.
    #[error("protocol error: {0}")]
    Protocol(#[from] packet::DecodeError),
    /// Plain text received outside the packet protocol, which only a serial
    /// port reads.
    #[cfg_attr(not(feature = "serial"), allow(dead_code))]
    #[error("{0}")]
    Text(String),
    /// A low level I/O error.
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
}

impl From<codec::Error> for RecvError {
    fn from(error: codec::Error) -> Self {
        match error {
            codec::Error::Invalid(error) => RecvError::Protocol(error),
            codec::Error::TrailingBytes => {
                RecvError::IO(io::Error::from(io::ErrorKind::InvalidData))
            }
            codec::Error::LostFraming => RecvError::Disconnected,
        }
    }
}

/// A packet, or the error encountered while receiving it from a [`Port`].
pub type ReceiveResult = Result<Packet, RecvError>;

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

/// How a discovered device is reached.
// Its names follow the chips' own, and some variants go unused, until
// discovery exports it.
#[allow(clippy::upper_case_acronyms, dead_code)]
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
// Its name goes unread until discovery exports it.
#[allow(dead_code)]
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

/// Resolves `addr` to a socket address of the IP version `family` allows.
/// Adds the default port, which TCP and UDP share, if `addr` has none.
/// Returns an error if nothing resolves.
fn find_addr(addr: &str, family: AddrFamilyRestrict) -> Result<SocketAddr, io::Error> {
    // If the port is missing, append the default. It would
    // be possible to determine if it's needed, but it's simpler
    // to try to parse as-is, and if it fails try again with the port.
    let iter = match addr.to_socket_addrs() {
        Ok(iter) => iter,
        Err(err) => {
            // Attempt to append the port number
            let addr_port = format!("{}:{}", addr, proto::TCP_DEFAULT_PORT);
            match addr_port.to_socket_addrs() {
                Ok(iter) => iter,
                Err(_) => {
                    // Final attempt: if the address was a numeric IPv6 address
                    // append the port in the right format.
                    let addr_port = format!("[{}]:{}", addr, proto::TCP_DEFAULT_PORT);
                    match addr_port.to_socket_addrs() {
                        Ok(iter) => iter,
                        _ => {
                            return Err(err);
                        }
                    }
                }
            }
        }
    };
    for sa in iter {
        match sa {
            SocketAddr::V4(_) => {
                if let AddrFamilyRestrict::V6 = family {
                    continue;
                }
            }
            SocketAddr::V6(_) => {
                if let AddrFamilyRestrict::V4 = family {
                    continue;
                }
            }
        }
        return Ok(sa);
    }
    Err(io::Error::other("address resolution failed"))
}

/// Resolves `addr` as [`find_addr`] does, on a blocking thread, without
/// blocking a worker of the runtime.
///
/// Returns an error if nothing resolves.
async fn find_addr_async(addr: &str, family: AddrFamilyRestrict) -> io::Result<SocketAddr> {
    // A lookup cannot be stopped, and holds nothing once it is dropped. The
    // supervisor of its thread is its own, so no stop waits for it.
    let addr = addr.to_string();
    let lookup = Supervisor::new().spawn_blocking(move || find_addr(&addr, family));
    lookup.await.map_err(io::Error::other)?
}

/// Returns the address part of a TCP `url`, with the IP version its scheme
/// allows. Returns `None` for any other URL.
fn tcp_address(url: &str) -> Option<(&str, AddrFamilyRestrict)> {
    match url.split_once("://")? {
        ("tcp", address) => Some((address, AddrFamilyRestrict::Either)),
        ("tcp4", address) => Some((address, AddrFamilyRestrict::V4)),
        ("tcp6", address) => Some((address, AddrFamilyRestrict::V6)),
        _ => None,
    }
}

/// Returns the socket address that `addr` spells out, as [`find_addr`] reads
/// it, if it is an IP address of the version `family` allows. Looks nothing
/// up.
fn literal_addr(addr: &str, family: AddrFamilyRestrict) -> Option<SocketAddr> {
    let port = proto::TCP_DEFAULT_PORT;
    [
        addr.to_string(),
        format!("{addr}:{port}"),
        format!("[{addr}]:{port}"),
    ]
    .iter()
    .find_map(|candidate| candidate.parse::<SocketAddr>().ok())
    .filter(|found| family.allows(found))
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

/// How long a closed port's task may take to end. It covers a write to a
/// stalled peer that the task gives up after [`DRAIN_TIMEOUT`].
pub(crate) const CLOSE_TIMEOUT: Duration = DRAIN_TIMEOUT.saturating_add(Duration::from_millis(500));

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
        let run = task.run_stream(stream, codec::serial::Codec::default());
        Ok(port.start(tasks, format_args!("twinleaf port {serial_config}"), run))
    }

    fn open_tcp(
        address: &str,
        family: AddrFamilyRestrict,
        rx: Delivery,
        tasks: &Supervisor,
    ) -> io::Result<Port> {
        let stream = tcp::connect(&find_addr(address, family)?)?;
        Ok(Port::start_tcp(
            stream,
            tcp::RULES,
            rx,
            DEFAULT_TX_CHANNEL_SIZE,
            tasks,
        ))
    }

    fn open_udp(
        address: &str,
        family: AddrFamilyRestrict,
        rx: Delivery,
        tasks: &Supervisor,
    ) -> io::Result<Port> {
        let peer = find_addr(address, family)?;
        let socket = udp::connect(&peer)?;
        let (port, task) = Port::with_task(
            TransportKind::Udp,
            None,
            udp::RULES,
            rx,
            DEFAULT_TX_CHANNEL_SIZE,
        );
        let run = task.run_datagram(socket);
        Ok(port.start(tasks, format_args!("twinleaf port udp://{peer}"), run))
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
    // A connection opens its port to the proxy with it.
    #[cfg_attr(not(test), allow(dead_code))]
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

        let split_url: Vec<&str> = url.splitn(2, "://").collect();
        match split_url[..] {
            #[cfg(feature = "serial")]
            ["serial", port] => Self::open_serial(port, rx, tasks),
            #[cfg(not(feature = "serial"))]
            ["serial", _] => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "serial support is disabled",
            )),
            ["tcp", addr] => Self::open_tcp(addr, AddrFamilyRestrict::Either, rx, tasks),
            ["udp", addr] => Self::open_udp(addr, AddrFamilyRestrict::Either, rx, tasks),
            ["tcp4", addr] => Self::open_tcp(addr, AddrFamilyRestrict::V4, rx, tasks),
            ["udp4", addr] => Self::open_udp(addr, AddrFamilyRestrict::V4, rx, tasks),
            ["tcp6", addr] => Self::open_tcp(addr, AddrFamilyRestrict::V6, rx, tasks),
            ["udp6", addr] => Self::open_udp(addr, AddrFamilyRestrict::V6, rx, tasks),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid TIO transport locator: {url}"),
            )),
        }
    }

    /// Opens a [`Port`] for `url` as [`new`](Self::new) does, without
    /// blocking a worker of the runtime. A TCP link resolves its address and
    /// connects as a future, and dropping it at any await leaves nothing
    /// open. Its task runs on `tasks`. Any other link opens on a blocking
    /// thread of `tasks`.
    ///
    /// # Errors
    ///
    /// Returns an error if `url` is not valid or the link cannot be opened.
    pub(crate) async fn open(url: &str, rx: Delivery, tasks: &Supervisor) -> io::Result<Port> {
        let Some((address, family)) = tcp_address(url) else {
            let (url, on) = (url.to_string(), tasks.clone());
            let opening = tasks.spawn_blocking(move || Port::new_on(&url, rx, &on));
            return opening.await.map_err(io::Error::other)?;
        };
        let address = match literal_addr(address, family) {
            Some(address) => address,
            None => find_addr_async(address, family).await?,
        };
        let stream = tcp::connect_async(address).await?;
        Ok(Port::start_tcp(
            stream,
            tcp::RULES,
            rx,
            DEFAULT_TX_CHANNEL_SIZE,
            tasks,
        ))
    }

    /// Returns `true` if dropping an [`open`](Self::open) of `url` in flight
    /// leaves nothing open, as for a TCP link.
    pub(crate) fn opens_as_future(url: &str) -> bool {
        tcp_address(url).is_some()
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

    /// A connection to a loopback address gives up long before one to a
    /// remote device does, here on a port whose queue is full, where the
    /// kernel drops the handshake rather than refusing it.
    #[test]
    fn a_loopback_connect_that_hangs_gives_up_quickly() {
        use socket2::{Domain, Socket, Type};

        let listener = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        let address = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0));
        listener.bind(&address.into()).unwrap();
        listener.listen(0).unwrap();
        let address = listener.local_addr().unwrap().as_socket().unwrap();
        // Nothing accepts these, so they fill the queue.
        let _queued: Vec<_> = (0..2)
            .map(|_| std::net::TcpStream::connect_timeout(&address, Duration::from_millis(100)))
            .collect();

        let started = Instant::now();
        let _ = tcp::connect(&address);

        let took = started.elapsed();
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
    fn fake(rules: Rules, framing: impl task::Framing, stuck: bool) -> Fake {
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
        } = fake(serial::RULES, Codec::default(), false);
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
        assert!(matches!(results.try_recv(), Ok(Err(RecvError::Text(text))) if text == "boot"));
        let after = results.try_recv().unwrap().unwrap();
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

    /// An address written as an IP address resolves without a lookup, with
    /// the default port when it names none. A name does not.
    #[test]
    fn a_literal_address_resolves_without_a_lookup() {
        let either = AddrFamilyRestrict::Either;
        let resolved = |address: &str| Some(address.parse::<SocketAddr>().unwrap());
        assert_eq!(literal_addr("10.0.0.2", either), resolved("10.0.0.2:7855"));
        assert_eq!(literal_addr("10.0.0.2:99", either), resolved("10.0.0.2:99"));
        assert_eq!(literal_addr("::1", either), resolved("[::1]:7855"));
        assert_eq!(literal_addr("[::1]:99", either), resolved("[::1]:99"));
        assert_eq!(literal_addr("10.0.0.2", AddrFamilyRestrict::V6), None);
        assert_eq!(literal_addr("sensor.local", either), None);
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
        assert_eq!(arrived.unwrap().as_bytes(), heartbeat.as_bytes());
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
            Err(RecvError::Protocol(packet::DecodeError::PayloadTooShort))
        ));
        assert_eq!(
            results.recv_timeout(wait).unwrap().unwrap().as_bytes(),
            heartbeat.as_bytes()
        );
    }

    /// A header that does not parse leaves no way to find the next packet, so
    /// the error is reported once and the port closes.
    #[test]
    fn a_tcp_stream_that_loses_its_framing_closes() {
        let (_port, mut peer, results) = tcp_port_reporting();

        peer.write_all(&[0, 0, 0, 0]).unwrap();
        peer.write_all(Packet::heartbeat(proto::DeviceRoute::root()).as_bytes())
            .unwrap();

        let wait = Duration::from_secs(2);
        assert!(matches!(
            results.recv_timeout(wait).unwrap(),
            Err(RecvError::Protocol(packet::DecodeError::InvalidPacketType))
        ));
        assert!(matches!(
            results.recv_timeout(wait),
            Err(RecvTimeoutError::Disconnected)
        ));
        // The port may close with the heartbeat still unread, which resets
        // the connection rather than ending it. Either way it is closed.
        peer.set_read_timeout(Some(wait)).unwrap();
        match peer.read(&mut [0]) {
            Ok(0) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            other => panic!("the stream did not close: {other:?}"),
        }
    }
}

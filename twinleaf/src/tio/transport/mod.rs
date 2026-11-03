//! Serial, TCP and UDP links that carry packets, each driven by a task.
//!
//! A [`Port`] is the host's end of one link. Its task runs on the runtime
//! of the supervisor it was opened with, the crate's own for the
//! synchronous API and the caller's for an async entry point. It frames
//! bytes into [`Packet`]s, writes queued packets, sets serial rates, and
//! sends heartbeats. The
//! [proxy](crate::proxy) opens one for its link to each device and one for
//! each client socket it accepts. A [`Connection`](crate::Connection) opens
//! one for its socket to the proxy. Applications do not open ports directly.
//!
//! What arrives goes to a flume channel of [`ReceiveResult`], which a task
//! reads with `recv_async` and a thread with `recv`. What leaves goes through
//! [`try_send`](Port::try_send), or [`send_async`](Port::send_async) from a
//! task.

mod codec;
mod port;
#[cfg(feature = "serial")]
mod serial;
mod task;
mod tcp;
#[cfg(test)]
pub(crate) mod tests;
mod udp;

pub(crate) use port::Port;
pub(crate) use tcp::CONNECT_TIMEOUT;

use super::packet::{self, Packet};
use crate::chan::oneshot;
use crate::proto;
use crate::rt::Supervisor;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

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

/// Returns the one address to use of those `found` for a name, where only one
/// can be tried. That is any address mDNS finds, and the address of a UDP
/// link, which has no handshake to tell an address that answers from one
/// that does not. Takes only an address of a version `family` allows. Prefers
/// a routable address, IPv4 first, then a link-local one, then loopback.
/// Takes the lowest address of the best kind. Returns `None` if no address
/// fits.
fn preferred(found: &[SocketAddr], family: &AddrFamilyRestrict) -> Option<SocketAddr> {
    let rank = |addr: &SocketAddr| match addr.ip() {
        IpAddr::V4(ip) if ip.is_loopback() => 4,
        IpAddr::V6(ip) if ip.is_loopback() => 4,
        IpAddr::V4(ip) if ip.is_link_local() => 2,
        IpAddr::V6(ip) if ip.is_unicast_link_local() => 3,
        IpAddr::V4(_) => 0,
        IpAddr::V6(_) => 1,
    };
    found
        .iter()
        .copied()
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
/// fits, and any other name, within [`LOOKUP_TIMEOUT`]. The mDNS daemon
/// starts on a blocking thread of `tasks`.
///
/// Returns an error if nothing resolves in time.
async fn resolve(
    addr: &str,
    family: AddrFamilyRestrict,
    tasks: &Supervisor,
) -> io::Result<Vec<SocketAddr>> {
    #[cfg(not(feature = "mdns"))]
    let _ = tasks;
    #[cfg(feature = "mdns")]
    if let Some((host, port)) = local_name(addr) {
        match preferred(&super::mdns::resolve(&host, port, tasks).await, &family) {
            Some(found) => return Ok(vec![found]),
            None => log::debug!("mDNS found no address for {host}, so the system resolves it"),
        }
    }
    let looking_up = crate::net::lookup_host(with_port(addr));
    let looked_up = tokio::time::timeout(LOOKUP_TIMEOUT, looking_up).await;
    let late = |_| {
        let message = format!("{addr} did not resolve within {LOOKUP_TIMEOUT:?}");
        io::Error::new(io::ErrorKind::TimedOut, message)
    };
    let found: Vec<_> = looked_up
        .map_err(late)??
        .into_iter()
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
type Delivery = crate::chan::Sender<ReceiveResult>;

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

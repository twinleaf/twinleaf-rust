//! The UDP transport.
//!
//! Opens the connected UDP sockets that ports drive. Each TIO packet travels
//! unchanged in a datagram of its own, as
//! [`codec::udp`](super::codec::udp) frames it.

use super::task::Rules;
use crate::rt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;

/// The rules of a UDP port's task. A heartbeat goes out every 200 ms.
pub(super) const RULES: Rules = Rules {
    heartbeat: Some(Duration::from_millis(200)),
    ..Rules::NONE
};

/// The size of a UDP port's receive buffer, which holds one datagram.
pub(super) const RX_SIZE: usize = 1024;

/// Binds a UDP socket and connects it to `address`.
///
/// # Errors
///
/// Returns an error if binding or connecting fails.
pub(super) fn connect(address: &SocketAddr) -> io::Result<UdpSocket> {
    let bind_addr_str = match address {
        std::net::SocketAddr::V4(_) => "0.0.0.0:0",
        std::net::SocketAddr::V6(_) => "[::0]:0",
    };
    let bind_addr: SocketAddr = if let Ok(addr) = bind_addr_str.parse() {
        addr
    } else {
        // This should never happen.
        return Err(io::Error::from(io::ErrorKind::Other));
    };
    let sock = std::net::UdpSocket::bind(bind_addr)?;
    sock.connect(address)?;
    sock.set_nonblocking(true)?;
    let _runtime = rt::handle().enter();
    UdpSocket::from_std(sock)
}

/// Returns `true` for an error that a connected UDP socket reports from ICMP,
/// such as port or host unreachable. Receiving treats these errors as
/// transient. The watchdog of the proxy's device link judges whether the
/// device is gone.
pub(super) fn is_advisory_network_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
    )
}

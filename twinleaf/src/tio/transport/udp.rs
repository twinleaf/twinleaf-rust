//! The UDP transport.
//!
//! Opens the UDP sockets that ports drive. Each TIO packet travels
//! unchanged in a datagram of its own, as
//! [`codec::udp`](super::codec::udp) frames it.

use super::task::Rules;
use crate::rt::Supervisor;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;

/// The rules of a UDP port's task. A heartbeat goes out every 200 ms.
pub(super) const RULES: Rules = Rules {
    heartbeat: Some(Duration::from_millis(200)),
    ..Rules::NONE
};

/// Binds a UDP socket of the address family needed to reach `address`.
/// UdpFramed supplies each destination. Leaving the socket unconnected
/// supports its send-to operation on macOS, and the task filters the peer.
///
/// # Errors
///
/// Returns an error if binding fails.
pub(super) fn bind(address: &SocketAddr) -> io::Result<UdpSocket> {
    let address = *address;
    Supervisor::new().run("udp bind", async move {
        let bind = if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = UdpSocket::bind(bind).await?;
        Ok(socket)
    })
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

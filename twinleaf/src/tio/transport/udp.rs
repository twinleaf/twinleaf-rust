//! The UDP transport.
//!
//! Opens the UDP sockets that ports drive. Each TIO packet travels
//! unchanged in a datagram of its own, as
//! [`codec::udp`](super::codec::udp) frames it.

use super::task::Rules;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;

/// The rules of a UDP port's task. A heartbeat goes out every 200 ms.
pub(super) const RULES: Rules = Rules {
    heartbeat: Some(Duration::from_millis(200)),
    ..Rules::NONE
};

/// Binds a UDP socket of the address family needed to reach `address`.
/// `UdpFramed` supplies each destination. Leaving the socket unconnected
/// supports its send-to operation on macOS, and the task filters the peer.
///
/// # Errors
///
/// Returns an error if binding fails.
pub(super) async fn bind(address: &SocketAddr) -> io::Result<UdpSocket> {
    UdpSocket::bind(local_address(address)).await
}

/// Returns the address a socket that reaches `address` binds, with a port
/// the OS picks. That is the loopback address of its family when `address`
/// is on loopback, and every interface of its family otherwise.
fn local_address(address: &SocketAddr) -> SocketAddr {
    let ip: IpAddr = match (address.ip().is_loopback(), address) {
        (true, SocketAddr::V4(_)) => Ipv4Addr::LOCALHOST.into(),
        (true, SocketAddr::V6(_)) => Ipv6Addr::LOCALHOST.into(),
        (false, SocketAddr::V4(_)) => Ipv4Addr::UNSPECIFIED.into(),
        (false, SocketAddr::V6(_)) => Ipv6Addr::UNSPECIFIED.into(),
    };
    (ip, 0).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A socket that reaches loopback binds loopback of the same family. One
    /// that reaches any other address binds every interface of its family.
    #[test]
    fn a_socket_binds_loopback_only_to_reach_loopback() {
        for (peer, local) in [
            ("127.0.0.1:7855", "127.0.0.1:0"),
            ("127.1.2.3:7855", "127.0.0.1:0"),
            ("[::1]:7855", "[::1]:0"),
            ("192.168.1.20:7855", "0.0.0.0:0"),
            ("[fd00::5]:7855", "[::]:0"),
        ] {
            let peer: SocketAddr = peer.parse().unwrap();
            assert_eq!(local_address(&peer), local.parse().unwrap(), "{peer}");
        }
    }
}

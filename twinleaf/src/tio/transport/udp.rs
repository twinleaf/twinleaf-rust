//! UDP transport
//!
//! Implements a `RawPort` for a UDP socket, and an MIO event source.
//! Tio packets are sent and received unchanged in individual UDP datagrams.

use super::{proto, Packet, RawPort, RecvError, SendError};
use mio::net::UdpSocket;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

/// RawPort to communicate via UDP
pub struct Port {
    /// Underlying socket
    sock: UdpSocket,
    /// Datagram held back when the OS send buffer was full
    pending: Option<Vec<u8>>,
}

impl Port {
    /// Returns a new `udp::Port` for communication with the given `address`.
    pub fn new(address: &SocketAddr) -> Result<Port, io::Error> {
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
        let sock = UdpSocket::bind(bind_addr)?;
        sock.connect(*address)?;
        Ok(Port {
            sock,
            pending: None,
        })
    }
}

/// ICMP-sourced errors on a connected UDP socket (port/host unreachable, etc.)
/// are advisory, not a real disconnect: liveness is owned by the proxy_core
/// watchdog, so callers treat these as transient instead of tearing down.
fn is_advisory_network_error(err: &io::Error) -> bool {
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

impl RawPort for Port {
    fn kind(&self) -> super::TransportKind {
        super::TransportKind::Udp
    }

    fn recv(&mut self) -> Result<Packet, RecvError> {
        let mut buf = [0u8; 1024];
        let size = match self.sock.recv(&mut buf) {
            Ok(s) => s,
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock || is_advisory_network_error(&e) {
                    return Err(RecvError::NotReady);
                } else {
                    return Err(RecvError::IO(e));
                }
            }
        };
        match Packet::deserialize(&buf[..size]) {
            Ok((pkt, parsed_size)) => {
                if parsed_size != size {
                    // For UDP, this is an error that does not turn into NotReady
                    // since we should get a full tio packet in a packet.
                    Err(RecvError::IO(io::Error::from(io::ErrorKind::InvalidData)))
                } else {
                    Ok(pkt)
                }
            }
            Err(e) => {
                // Since here we should get the whole packet in a single datagram,
                // if something is missing at the end we don't want to pass along NeedMore
                if let proto::DecodeError::NeedMore = e {
                    Err(RecvError::Protocol(proto::DecodeError::PacketTooSmall(
                        buf[..size].to_vec(),
                    )))
                } else {
                    Err(RecvError::Protocol(e))
                }
            }
        }
    }

    fn send(&mut self, pkt: &Packet) -> Result<(), SendError> {
        if self.pending.is_some() {
            return Err(SendError::Full);
        }

        let raw = pkt.serialize()?;
        match self.sock.send(&raw) {
            Ok(size) => {
                if size == raw.len() {
                    Ok(())
                } else {
                    panic!("Unexpected UDP short write");
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // OS send buffer full; hold the datagram for drain().
                self.pending = Some(raw);
                Err(SendError::MustDrain)
            }
            Err(e) if is_advisory_network_error(&e) => Ok(()),
            Err(e) => Err(SendError::IO(e)),
        }
    }

    fn drain(&mut self) -> Result<(), SendError> {
        let Some(raw) = &self.pending else {
            return Ok(());
        };
        match self.sock.send(raw) {
            Ok(_) => {
                self.pending = None;
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(SendError::MustDrain),
            Err(e) if is_advisory_network_error(&e) => {
                self.pending = None;
                Ok(())
            }
            Err(e) => Err(SendError::IO(e)),
        }
    }

    fn has_data_to_drain(&self) -> bool {
        self.pending.is_some()
    }

    fn max_send_interval(&self) -> Option<Duration> {
        Some(Duration::from_millis(200))
    }
}

impl mio::event::Source for Port {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.sock.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.sock.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        self.sock.deregister(registry)
    }
}

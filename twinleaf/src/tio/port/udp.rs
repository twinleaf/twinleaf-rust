//! UDP Port
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
        Ok(Port { sock })
    }
}

impl RawPort for Port {
    fn recv(&mut self) -> Result<Packet, RecvError> {
        let mut buf = [0u8; 1024];
        let size = match self.sock.recv(&mut buf) {
            Ok(s) => s,
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
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
                if let proto::Error::NeedMore = e {
                    Err(RecvError::Protocol(proto::Error::PacketTooSmall(
                        buf[..size].to_vec(),
                    )))
                } else {
                    Err(RecvError::Protocol(e))
                }
            }
        }
    }

    fn send(&mut self, pkt: &Packet) -> Result<(), SendError> {
        let raw = if let Ok(raw) = pkt.serialize() {
            raw
        } else {
            return Err(SendError::Serialization);
        };
        match self.sock.send(&raw) {
            Ok(size) => {
                if size == raw.len() {
                    Ok(())
                } else {
                    panic!("Unexpected UDP short write");
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // This could potentially happen on some implementations if
                // the os buffers are full. In practice, it should never
                // happen, but we can re-evaluate if we bump into it.
                // The correct way to handle it to be consistent would be
                // to buffer up the packet and use MustDrain/Full.
                panic!("Unexpected UDP would block");
            }
            Err(e) => Err(SendError::IO(e)),
        }
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

use super::{Packet, RawPort, RecvError, SendError};
use mio::net::UdpSocket;
use std::io;
use std::net::SocketAddr;

pub struct Port {
    sock: UdpSocket,
}

impl Port {
    pub fn new(address: &SocketAddr) -> Result<Port, io::Error> {
        let sock = UdpSocket::bind("0.0.0.0:0".parse().unwrap())?;
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
            Err(e) => Err(RecvError::Protocol(e)),
        }
    }

    fn send(&mut self, pkt: &Packet) -> Result<(), SendError> {
        let raw = pkt.serialize();
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

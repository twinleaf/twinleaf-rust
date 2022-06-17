/*
use mio::net::UdpSocket;

pub struct TioUDP {
    sock: UdpSocket,
}

impl TioUDP {
    pub fn new(address: &SocketAddr) -> Result<TioUDP, io::Error> {
        let sock = UdpSocket::bind("0.0.0.0:0".parse().unwrap())?;
        sock.connect(*address)?;
        Ok(TioUDP { sock })
    }
}

impl TioRawPort for TioUDP {
    fn recv(&mut self) -> Result<ReadResult, ReadError> {
        let mut buf = [0u8; 1024];
        let size = match self.sock.recv(&mut buf) {
            Ok(s) => s,
            Err(err) => {
                return Err(ReadError::IO(err));
            }
        };
        if size < 4 {
            Ok(ReadResult::WouldBlock)
        } else {
            let payload_len = (buf[2] as usize) + (buf[3] as usize) * 256;
            let routing_size = (buf[1] & 0xF) as usize;
            if size == 4 + payload_len + routing_size {
                Ok(ReadResult::Packet(buf[..size].to_vec()))
            } else {
                Ok(ReadResult::WouldBlock)
            }
        }
    }

    fn send(&mut self, pkt: &[u8]) -> Result<WriteResult, WriteError> {
        match self.sock.send(pkt) {
            Ok(_size) => Ok(WriteResult::Ok), // TODO
            _ => {
                panic!("TODO");
            }
        }
    }

    fn drain(&self) -> Result<WriteResult, WriteError> {
        Ok(WriteResult::Ok)
    }

    fn needs_periodic_heartbeats() -> bool {
        true
    }
}

impl mio::event::Source for TioUDP {
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
*/

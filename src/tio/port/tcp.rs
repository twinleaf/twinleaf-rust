use super::{iobuf::IOBuf, proto, Packet, RawPort, RecvError, SendError};
use mio::net::TcpStream;
use std::io;
use std::io::Write;
use std::net::SocketAddr;

pub struct Port {
    stream: TcpStream,
    rxbuf: IOBuf,
    txbuf: IOBuf,
}

impl Port {
    pub fn from_stream(stream: TcpStream) -> Result<Port, io::Error> {
        Ok(Port {
            stream: stream,
            rxbuf: IOBuf::new(),
            txbuf: IOBuf::new(),
        })
    }

    pub fn new(address: &SocketAddr) -> Result<Port, io::Error> {
        let stream = TcpStream::connect(*address)?;
        Port::from_stream(stream)
    }

    fn recv_buffered(&mut self) -> Result<Packet, RecvError> {
        match Packet::deserialize(self.rxbuf.data()) {
            Ok((pkt, size)) => {
                self.rxbuf.consume(size);
                Ok(pkt)
            }
            Err(proto::Error::NeedMore) => Err(RecvError::NotReady),
            Err(perr) => Err(RecvError::Protocol(perr)),
        }
    }
}

impl RawPort for Port {
    fn recv(&mut self) -> Result<Packet, RecvError> {
        let mut res = self.recv_buffered();
        if let Err(RecvError::NotReady) = res {
            if let Err(e) = self.rxbuf.refill(&mut self.stream) {
                return Err(e);
            }
            res = self.recv_buffered();
        }
        res
    }

    fn send(&mut self, pkt: &Packet) -> Result<(), SendError> {
        if self.has_data_to_drain() {
            return Err(SendError::Full);
        }

        let raw = pkt.serialize();
        match self.stream.write(&raw) {
            Ok(size) => {
                if size == raw.len() {
                    Ok(())
                } else {
                    // IOBuf sized such that it can always store at least a full packet.
                    self.txbuf.add_data(&raw[size..]).unwrap();
                    Err(SendError::MustDrain)
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // This can happen if we happen to send with the TCP buffer completely full.
                // Maintain the same semantics and buffer the whole thing in txbuf.
                // IOBuf sized such that it can always store at least a full packet.
                self.txbuf.add_data(&raw[..]).unwrap();
                Err(SendError::MustDrain)
            }
            Err(e) => Err(SendError::IO(e)),
        }
    }

    fn drain(&mut self) -> Result<(), SendError> {
        self.txbuf.drain(&mut self.stream)
    }

    fn has_data_to_drain(&self) -> bool {
        !self.txbuf.empty()
    }
}

impl mio::event::Source for Port {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.stream.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.stream.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        self.stream.deregister(registry)
    }
}

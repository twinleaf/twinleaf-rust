use std::io;
use std::io::Write;
use super::{RecvError,SendError,IOBuf, proto, Packet,RawPort};
use std::net::SocketAddr;
use mio::net::TcpStream;

pub struct Port {
    stream: TcpStream,
    rxbuf: IOBuf,
    //    txbuf: IOBuf,
}

impl Port {
    pub fn from_stream(stream: TcpStream) -> Result<Port, io::Error> {
        Ok(Port {
            stream: stream,
            rxbuf: IOBuf::new(),
            //txbuf: IOBuf::new(),
        })
    }

    pub fn new(address: &SocketAddr) -> Result<Port, io::Error> {
        let stream = TcpStream::connect(*address)?;
        Port::from_stream(stream)
    }

    fn recv_buffered(&mut self) -> Result<Packet, RecvError> {
        let buf = &self.rxbuf.buf;
        let start = &mut self.rxbuf.start;
        let end = self.rxbuf.end;
        match Packet::deserialize(&buf[*start..end]) {
            Ok((pkt,size)) => {
                *start += size;
                Ok(pkt)
            }
            Err(proto::Error::NeedMore) => {
                Err(RecvError::NotReady)
            }
            Err(perr) => {
                Err(RecvError::Protocol(perr))
            }
        }
    }
}

impl RawPort for Port {
    fn recv(&mut self) -> Result<Packet, RecvError> {
        let mut res = self.recv_buffered();
        if let Err(RecvError::NotReady) = res {
            if let Err(e) = self.rxbuf.refill(&mut self.stream) {
                //println!("PORT  RET ERR: {:?}", e);
                return Err(e)
            }
            res = self.recv_buffered();
        }
        //println!("PORT  RET NORM: {:?}", res);
        res
    }

    fn send(&mut self, pkt: &Packet) -> Result<(), SendError> {
        let raw = pkt.serialize();
        match self.stream.write(&raw) {
            Ok(size) => {
                if size == raw.len() {
                    Ok(())
                } else {
                    panic!("TODO")
                }
            }
            Err(err) => { Err(SendError::IO(err)) }
        }
    }

    fn drain(&self) -> Result<(), SendError> {
        panic!("TODO");
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

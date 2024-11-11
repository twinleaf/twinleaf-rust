//! TCP Port
//!
//! Implements a `RawPort` for a TCP stream, and an MIO event source.
//! TIO packets are sent unmodified to the TCP stream. The TIO protocol
//! packets have a header that allows for figuring out the total size
//! of a packet, so it can be split up again at the receiving end.

use super::{iobuf::IOBuf, proto, Packet, RawPort, RecvError, SendError};
use mio::net::TcpStream;
use std::io;
use std::io::Write;
use std::net::SocketAddr;

/// RawPort to communicate via TCP
pub struct Port {
    /// Underlying stream
    stream: TcpStream,
    /// Incoming buffer, used to buffer partial packets.
    rxbuf: IOBuf,
    /// Outgoing buffer, used for all-or-none sends of packets
    /// when the TCP buffer fills up.
    txbuf: IOBuf,
}

impl Port {
    /// Takes ownership of a MIO `TcpStream` and constructs a `Port` over it.
    pub fn from_stream(stream: TcpStream) -> Result<Port, io::Error> {
        Ok(Port {
            stream: stream,
            rxbuf: IOBuf::new(),
            txbuf: IOBuf::new(),
        })
    }

    /// Returns a new `tcp::Port` for communication with the given `address`.
    pub fn new(address: &SocketAddr) -> Result<Port, io::Error> {
        let stream = TcpStream::connect(*address)?;
        Port::from_stream(stream)
    }

    /// Attempts to receive a packet only from the data currently present
    /// in the incoming buffer.
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

        let raw = if let Ok(raw) = pkt.serialize() {
            raw
        } else {
            return Err(SendError::Serialization);
        };
        match self.stream.write(&raw) {
            Ok(size) => {
                if size == raw.len() {
                    // The entire packet was written out
                    Ok(())
                } else {
                    // Partial write, the TCP buffer is full. To guarantee packetization
                    // we must send the remaining data, so add it to the outgoing buffer.
                    // IOBuf sized such that it can always store at least a full packet,
                    // so this should never happen.
                    self.txbuf.add_data(&raw[size..]).expect("No fit in IOBuf");
                    Err(SendError::MustDrain)
                }
            }
            Err(err) => {
                match err.kind() {
                    io::ErrorKind::WouldBlock | io::ErrorKind::NotConnected => {
                        // These errors can occur when a packet is sent right after the
                        // nonblocking connection is initiated and before the handshake
                        // completes. WouldBlock can also occur if we happen to send with
                        // the TCP buffer completely full.
                        // Maintain the same semantics and buffer the whole thing in txbuf.
                        // IOBuf sized such that it can always store at least a full packet.
                        self.txbuf.add_data(&raw[..]).expect("No fit in IOBuf");
                        Err(SendError::MustDrain)
                    }
                    _ => Err(SendError::IO(err)),
                }
            }
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

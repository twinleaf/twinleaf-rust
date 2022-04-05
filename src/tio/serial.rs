use std::io;
use std::io::Write;
use super::{RecvError,SendError,IOBuf, proto, Packet,RawPort};
use mio_serial::SerialPortBuilderExt;
use crc::{Crc, CRC_32_ISO_HDLC};

pub struct Port {
    port: mio_serial::SerialStream,
    //default_bitrate: u32,
    rxbuf: IOBuf,
}

impl Port {
    pub fn new(port_name: &str) -> Result<Port, io::Error> {
        Ok(Port{
            port: mio_serial::new(port_name, 115200).open_native_async()?,
            rxbuf: IOBuf::new(),
        })
    }

    fn recv_buffered(&mut self) -> Result<Packet, RecvError> {
        let buf = &self.rxbuf.buf;
        let start = &mut self.rxbuf.start;
        let end = self.rxbuf.end;
        let inbuf = end - *start;
        let mut pkt = Vec::<u8>::new();
        let mut esc = false;
        let mut offset = *start;
        while offset < end {
            // TODO: validation
            if (buf[offset] == 0xC0) || (pkt.len() > 600) {
                *start = offset + 1;
                if let Ok(parseres) = Packet::deserialize(&pkt) {
                    return Ok(parseres.0)
                }
                pkt.truncate(0);
                esc = false;
                offset = *start;
                continue;
            }
//            let ch = char::from_u32_unchecked(buf[offset] as u32);
            if esc {
                if buf[offset] == 0xDC {
                    pkt.push(0xC0);
                } else {
                    pkt.push(0xDB);
                }
                esc = false;
            } else {
                if buf[offset] == 0xDB {
                    esc = true;
                } else {
                    pkt.push(buf[offset]);
                }
            }
            offset += 1;
        }
        Err(RecvError::NotReady)
    }
}

impl RawPort for Port {
    fn recv(&mut self) -> Result<Packet, RecvError> {
        let mut res = self.recv_buffered();
        if let Err(RecvError::NotReady) = res {
            if let Err(e) = self.rxbuf.refill(&mut self.port) {
                return Err(e)
            }
            res = self.recv_buffered();
        }
        res
    }

    fn send(&mut self, pkt: &Packet) -> Result<(), SendError> {
        // TODO: much inefficient. rework.
        let raw = pkt.serialize();
        let crc32 = Crc::<u32>::new(&CRC_32_ISO_HDLC);
        let mut encoded = vec![0xC0u8];
        for byte in [&raw, &crc32.checksum(&raw).to_le_bytes()[..]].concat() {
            match byte {
                0xC0 => {
                    encoded.push(0xDB);
                    encoded.push(0xDC);
                }
                0xDB => {
                    encoded.push(0xDB);
                    encoded.push(0xDD);
                }
                any => {
                    encoded.push(any);
                }
            }
        }
        encoded.push(0xC0);

        match self.port.write(&encoded) {
            Ok(size) => {
                if size == encoded.len() {
                    Ok(())
                } else {
                    panic!("TODO");
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
        self.port.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> io::Result<()> {
        self.port.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &mio::Registry) -> io::Result<()> {
        self.port.deregister(registry)
    }
}

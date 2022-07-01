use super::{iobuf::IOBuf, Packet, RateError, RateInfo, RawPort, RecvError, SendError};
use crc::{Crc, CRC_32_ISO_HDLC};
use mio_serial::{SerialPort, SerialPortBuilderExt};
use std::io;
use std::io::Write;
use std::time::Duration;

pub struct Port {
    port: mio_serial::SerialStream,
    rates: RateInfo,
    rxbuf: IOBuf,
    need_refill: bool,
    txbuf: IOBuf,
}

impl Port {
    pub fn new(port_name: &str) -> Result<Port, io::Error> {
        let mio_port = mio_serial::new(port_name, 115200).open_native_async()?;
        #[cfg(windows)]
        {
            use winapi::um::commapi::SetCommTimeouts;
            use winapi::um::winbase::COMMTIMEOUTS;
            let handle = mio_port.as_handle();
            let mut timeouts = COMMTIMEOUTS {
                ReadIntervalTimeout: 0xFFFFFFFF,
                ReadTotalTimeoutMultiplier: 0xFFFFFFFF,
                ReadTotalTimeoutConstant: 0xFFFFFFFE,
                WriteTotalTimeoutMultiplier: 0,
                WriteTotalTimeoutConstant: 0,
            };
            if unsafe { SetCommTimeouts(handle, &mut timeouts) } == 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(Port {
            port: mio_port,
            rates: RateInfo {
                default_bps: 115200,
                min_bps: 9600,
                max_bps: 400000,
            },
            rxbuf: IOBuf::new(),
            need_refill: true,
            txbuf: IOBuf::new(),
        })
    }

    fn recv_buffered(&mut self) -> Result<Packet, RecvError> {
        let mut pkt = Vec::<u8>::new();
        let mut esc = false;
        let mut offset = 0;
        let mut consume_to = 0;
        let data = &self.rxbuf.data();
        while offset < data.len() {
            // TODO: better validation, timeouts, text
            if (data[offset] == 0xC0) || (pkt.len() > 600) {
                if let Ok(parseres) = Packet::deserialize(&pkt) {
                    self.rxbuf.consume(offset + 1);
                    return Ok(parseres.0);
                } else {
                    consume_to = offset + 1;
                }
                pkt.truncate(0);
                esc = false;
            } else {
                if esc {
                    if data[offset] == 0xDC {
                        pkt.push(0xC0);
                    } else {
                        pkt.push(0xDB);
                    }
                    esc = false;
                } else {
                    if data[offset] == 0xDB {
                        esc = true;
                    } else {
                        pkt.push(data[offset]);
                    }
                }
            }
            offset += 1;
        }
        self.rxbuf.consume(consume_to);
        Err(RecvError::NotReady)
    }
}

impl RawPort for Port {
    fn recv(&mut self) -> Result<Packet, RecvError> {
        let mut refilled = false;
        while !refilled {
            if self.need_refill {
                if let Err(e) = self.rxbuf.refill(&mut self.port) {
                    return Err(e);
                }
                refilled = true;
            }
            match self.recv_buffered() {
                Ok(pkt) => {
                    self.need_refill = self.rxbuf.empty();
                    return Ok(pkt);
                }
                Err(RecvError::NotReady) => {
                    self.need_refill = true;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Err(RecvError::NotReady)
    }

    fn send(&mut self, pkt: &Packet) -> Result<(), SendError> {
        if self.has_data_to_drain() {
            return Err(SendError::Full);
        }

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
                    // IOBuf sized such that it can always store at least a full encoded packet.
                    self.txbuf.add_data(&encoded[size..]).unwrap();
                    Err(SendError::MustDrain)
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // This can happen if we happen to send with the OS buffer completely full.
                // Maintain the same semantics and buffer the whole thing in txbuf.
                // IOBuf sized such that it can always store at least a full encoded packet.
                self.txbuf.add_data(&encoded[..]).unwrap();
                Err(SendError::MustDrain)
            }
            Err(e) => Err(SendError::IO(e)),
        }
    }

    fn drain(&mut self) -> Result<(), SendError> {
        self.txbuf.drain(&mut self.port)
    }

    fn has_data_to_drain(&self) -> bool {
        !self.txbuf.empty()
    }

    fn set_rate(&mut self, rate: u32) -> Result<(), RateError> {
        match self.port.set_baud_rate(rate) {
            Ok(()) => Ok(()),
            Err(e) if (e.kind == mio_serial::ErrorKind::InvalidInput) => {
                Err(RateError::InvalidRate)
            }
            Err(_) => Err(RateError::Failed),
        }
    }

    fn rate_info(&self) -> Option<RateInfo> {
        Some(self.rates.clone())
    }

    fn max_send_interval(&self) -> Option<Duration> {
        Some(Duration::from_millis(100))
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

//! Serial Port
//!
//! Implements a `RawPort` for a serial port, and an MIO event source.
//! Tio packets have their CRC32 appended, and are then encoded on the
//! serial stream using SLIP.
//! When receiving, this implementation also attempts to parse newline
//! delimited, plain text ascii, which is returned as a
//! `RecvError::Protocol(proto::Error::Text(textual_data))`

use super::{iobuf::IOBuf, proto, Packet, RateError, RateInfo, RawPort, RecvError, SendError};
use crc::{Crc, CRC_32_ISO_HDLC};
use mio_serial::{SerialPort, SerialPortBuilderExt};
use std::io;
use std::io::Write;
use std::time::{Duration, Instant};

/// RawPort to communicate via a serial port
pub struct Port {
    /// Underlying serial port stream
    port: mio_serial::SerialStream,
    /// This contains the default and target data rates,
    /// for the higher level ports to switch speeds.
    rates: RateInfo,
    /// Incoming buffer, used to buffer partial packets.
    rxbuf: IOBuf,
    /// Instant when we received data most recently. This is used
    /// to clear out stale data from `rxbuf`.
    last_rx: Instant,
    /// Outgoing buffer, used for all-or-none sends of packets
    /// when the OS buffer fills up.
    txbuf: IOBuf,
    /// Time when the port is initialized, used for startup_holdoff
    startup_time: Instant,
    /// If true, the next data received will be the first data and
    /// should be discarded since it's usually corrupt/stale.
    first_rx: bool,
}

/// Default data rate on the serial port.
static DEFAULT_RATE: u32 = 115200;

/// Discard anything for this long after the port is opened.
static HOLDOFF_TIME: Duration = Duration::from_millis(50);

impl Port {
    /// Returns a new `tcp::Port`. The `url` should look like
    /// `serial_port[:target_rate[:default_rate]]``. It must start with a serial port,
    /// like `/dev/tty??` or `COMn`. The second parameter is optional, and it
    /// indicates the rate at which tio should try to configure the connected device.
    /// The final parameter is the default rate: this is the data rate that the device
    /// will start at, and to which we fall back to if issues arise with the communication.
    /// Both optional parameters default to 115200.
    ///
    /// For example, `COM3:400000:115200` will start off at 115.2k and try to
    /// negotiate 400k. If it fails to do so, or at any point later, it will
    /// fall back to 115.2k.
    pub fn new(url: &str) -> Result<Port, io::Error> {
        let url_tokens: Vec<&str> = url.split(':').collect();
        if (url_tokens.len() < 1) || (url_tokens.len() > 3) {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let port_name = url_tokens[0];
        let target_rate = if url_tokens.len() > 1 {
            if let Ok(rate) = url_tokens[1].parse::<u32>() {
                rate
            } else {
                return Err(io::Error::from(io::ErrorKind::InvalidInput));
            }
        } else {
            DEFAULT_RATE
        };
        let default_rate = if url_tokens.len() > 2 {
            if let Ok(rate) = url_tokens[2].parse::<u32>() {
                rate
            } else {
                return Err(io::Error::from(io::ErrorKind::InvalidInput));
            }
        } else {
            DEFAULT_RATE
        };
        let mio_port = mio_serial::new(port_name, default_rate).open_native_async()?;
        #[cfg(windows)]
        {
            // Windows requires some custom settings to replicate the unix behavior.
            use std::os::windows::io::AsRawHandle;
            use winapi::um::commapi::SetCommTimeouts;
            use winapi::um::winbase::COMMTIMEOUTS;
            let handle = mio_port.as_raw_handle();
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
                default_bps: default_rate,
                target_bps: target_rate,
            },
            rxbuf: IOBuf::new(),
            last_rx: Instant::now(),
            txbuf: IOBuf::new(),
            startup_time: Instant::now(),
            first_rx: true,
        })
    }

    /// Attempts to receive a packet only from the data currently present
    /// in the incoming buffer.
    fn recv_buffered(&mut self) -> Result<Packet, RecvError> {
        let mut pkt = Vec::<u8>::new();
        let mut esc = false;
        let mut text = true;
        let mut offset = 0;
        let mut consume_to = 0;
        let data = &self.rxbuf.data();
        while offset < data.len() {
            // Avoid packets that are too long, since we know they are invalid.
            // If pkt's size reached the max packet length + CRC32 + separator,
            // we know it's too long.
            if pkt.len() >= (proto::TIO_PACKET_MAX_TOTAL_SIZE + std::mem::size_of::<u32>() + 1) {
                self.rxbuf.consume(offset);
                return Err(RecvError::Protocol(proto::Error::PacketTooBig(pkt)));
            }
            // This will always succeed when converting an u8.
            let c = char::from_u32(data[offset].into()).expect("byte to char conversion");
            if text && ((c == '\n') || (c == '\r')) {
                // Newline character preceded by valid text characters (possibly none).
                // By the way the tio wire protocol over serial is designed, this can
                // only be a text packet.
                if pkt.len() > 0 {
                    self.rxbuf.consume(offset + 1);
                    return Err(RecvError::Protocol(proto::Error::Text(
                        String::from_utf8_lossy(&pkt).to_string(),
                    )));
                } else {
                    consume_to = offset + 1;
                }
            } else if data[offset] == 0xC0 {
                // This denotes the end of a SLIP packet. no matter what, we'll return
                // from here, either successfully with a packet, or with an error,
                // so consume the data so far.
                self.rxbuf.consume(offset + 1);
                if pkt.len() < 4 + std::mem::size_of::<u32>() {
                    // A packet must fit at least the header and its final CRC32
                    return Err(RecvError::Protocol(proto::Error::PacketTooSmall(pkt)));
                }
                let len = pkt.len() - std::mem::size_of::<u32>();
                let expected_crc = Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(&pkt[..len]);
                // This will always succeed, because the vec slice must be 4 bytes
                let received_crc = u32::from_le_bytes(pkt[len..].try_into().expect("array size"));
                if received_crc != expected_crc {
                    return Err(RecvError::Protocol(proto::Error::CRC32(pkt)));
                }
                // At this point the whole packet should be here, and there should not
                // be any bytes left over.
                return match Packet::deserialize(&pkt[..len]) {
                    Ok((tio_pkt, size)) => {
                        if size != len {
                            Err(RecvError::IO(io::Error::from(io::ErrorKind::InvalidData)))
                        } else {
                            Ok(tio_pkt)
                        }
                    }
                    Err(proto::Error::NeedMore) => {
                        Err(RecvError::Protocol(proto::Error::PacketTooSmall(pkt)))
                    }
                    Err(perr) => Err(RecvError::Protocol(perr)),
                };
            } else {
                if !c.is_ascii_graphic() && (c != ' ') && (c != '\t') {
                    text = false;
                }
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
        let mut res = self.recv_buffered();
        if let Err(RecvError::NotReady) = res {
            // First discard stale data if there is any in the buffer.
            // This could happen e.g. reprogramming a board mid-packet.
            let now = Instant::now();
            if now.duration_since(self.last_rx) > Duration::from_millis(200) {
                self.rxbuf.flush();
            }
            if let Err(e) = self.rxbuf.refill(&mut self.port) {
                #[cfg(target_os = "macos")]
                // On macos, disconnecting a serial port while connected will
                // generate this error, so translate it.
                if let RecvError::IO(ioerr) = &e {
                    if Some(6) == ioerr.raw_os_error() {
                        return Err(RecvError::Disconnected);
                    }
                }
                return Err(e);
            }
            // If this is the very first data we receive, discard it. Likely it's
            // a combination of stale data and possibly corrupted initial data
            // from the driver, so it's better to throw it away otherwise the
            // parser gets confused and waits for a large amount of data before
            // declaring it invalid.
            if self.first_rx && !self.rxbuf.empty() {
                self.rxbuf.flush();
                self.first_rx = false;
                return Err(RecvError::NotReady);
            }
            self.last_rx = now;
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
                    self.txbuf
                        .add_data(&encoded[size..])
                        .expect("No fit in IOBuf");
                    Err(SendError::MustDrain)
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // This can happen if we happen to send with the OS buffer completely full.
                // Maintain the same semantics and buffer the whole thing in txbuf.
                // IOBuf sized such that it can always store at least a full encoded packet.
                self.txbuf.add_data(&encoded[..]).expect("No fit in IOBuf");
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

    fn startup_holdoff(&self) -> bool {
        self.startup_time.elapsed() < HOLDOFF_TIME
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

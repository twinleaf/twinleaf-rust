//! Serial transport
//!
//! Implements a `RawPort` for a serial port, and an MIO event source.
//! Tio packets have their CRC32 appended, and are then encoded on the
//! serial stream using SLIP.
//! When receiving, this implementation also attempts to parse newline
//! delimited, plain text ascii, which is returned as a
//! `RecvError::Text(textual_data)`

use super::{iobuf::IOBuf, packet, Packet, RateError, RateInfo, RawPort, RecvError, SendError};
use crate::proto::serial as wire;
use crate::proto::{MAX_PACKET_SIZE, SLIP_END};
use mio_serial::{SerialPort, SerialPortBuilderExt};
use std::io;
use std::io::Write;
use std::time::{Duration, Instant};

/// Deserializer capacity: the largest packet plus its trailing CRC32.
const RX_CAPACITY: usize = MAX_PACKET_SIZE + wire::CRC_SIZE;

fn io_error(error: mio_serial::Error) -> io::Error {
    let kind = match error.kind() {
        mio_serial::ErrorKind::NoDevice => io::ErrorKind::NotFound,
        mio_serial::ErrorKind::InvalidInput => io::ErrorKind::InvalidInput,
        mio_serial::ErrorKind::Unknown => io::ErrorKind::Other,
        mio_serial::ErrorKind::Io(kind) => kind,
    };
    io::Error::new(kind, error)
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn parse_rate(value: &str) -> io::Result<u32> {
    value
        .parse()
        .map_err(|_| invalid_input(format!("invalid serial data rate: {value}")))
}

fn parse_config(url: &str) -> io::Result<(&str, u32, u32)> {
    let mut fields = url.split(':');
    let port = fields.next().unwrap_or_default();
    let target_rate = fields
        .next()
        .map(parse_rate)
        .transpose()?
        .unwrap_or(DEFAULT_RATE);
    let default_rate = fields
        .next()
        .map(parse_rate)
        .transpose()?
        .unwrap_or(DEFAULT_RATE);
    if port.is_empty() || fields.next().is_some() {
        return Err(invalid_input(format!(
            "invalid serial configuration: {url}"
        )));
    }
    Ok((port, target_rate, default_rate))
}

/// RawPort to communicate via a serial port
pub struct Port {
    /// Underlying serial port stream
    port: mio_serial::SerialStream,
    /// This contains the default and target data rates,
    /// for the higher level ports to switch speeds.
    rates: RateInfo,
    /// Incoming buffer, used to buffer partial packets.
    rxbuf: IOBuf,
    /// SLIP/CRC decoder fed from `rxbuf`.
    deserializer: wire::Deserializer<RX_CAPACITY>,
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
    /// Returns a new serial `Port`. The `url` should look like
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
    pub fn new(url: &str) -> io::Result<Port> {
        let (port_name, target_rate, default_rate) = parse_config(url)?;
        let mio_port = mio_serial::new(port_name, default_rate)
            .open_native_async()
            .map_err(io_error)?;
        #[cfg(target_os = "windows")]
        {
            // Windows requires some custom settings to replicate the unix behavior.
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Devices::Communication::{SetCommTimeouts, COMMTIMEOUTS};
            use windows_sys::Win32::Foundation::HANDLE;
            let handle: HANDLE = mio_port.as_raw_handle() as HANDLE;
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
            deserializer: wire::Deserializer::new(),
            last_rx: Instant::now(),
            txbuf: IOBuf::new(),
            startup_time: Instant::now(),
            first_rx: true,
        })
    }

    /// Attempts to receive a packet only from the data currently present
    /// in the incoming buffer.
    fn recv_buffered(&mut self) -> Result<Packet, RecvError> {
        let (consumed, frame) = self.deserializer.push(self.rxbuf.data());
        let res = match frame {
            Some(frame) => decode_frame(&frame),
            None => Err(RecvError::NotReady),
        };
        self.rxbuf.consume(consumed);
        res
    }

    /// Discards all the received data, both buffered and partially decoded.
    fn flush_rx(&mut self) {
        self.rxbuf.flush();
        self.deserializer = wire::Deserializer::new();
    }
}

/// Turns a deserialized frame into a packet, or into the error it represents.
fn decode_frame(frame: &wire::Frame) -> Result<Packet, RecvError> {
    use wire::FrameErrors;
    let data = frame.data;
    if let Some(packet) = frame.packet() {
        return match Packet::from_slice_prefix(packet) {
            Ok((tio_pkt, size)) => {
                if size != packet.len() {
                    Err(RecvError::IO(io::Error::from(io::ErrorKind::InvalidData)))
                } else {
                    Ok(tio_pkt)
                }
            }
            // A frame is a whole packet or nothing, so a short one is not
            // a packet still arriving.
            Err(packet::DecodeError::NeedMore) => {
                Err(RecvError::Protocol(packet::DecodeError::PacketTooSmall))
            }
            Err(perr) => Err(RecvError::Protocol(perr)),
        };
    }
    let errors = frame.errors;
    Err(if errors.contains(FrameErrors::TEXT) {
        RecvError::Text(String::from_utf8_lossy(data).to_string())
    } else if errors.contains(FrameErrors::TOO_BIG) {
        RecvError::Protocol(packet::DecodeError::PacketTooBig)
    } else if errors.contains(FrameErrors::SHORT) {
        RecvError::Protocol(packet::DecodeError::PacketTooSmall)
    } else {
        // CRC mismatch, or a bad escape which corrupted the frame.
        RecvError::Protocol(packet::DecodeError::CRC32)
    })
}

impl RawPort for Port {
    fn kind(&self) -> super::TransportKind {
        super::TransportKind::Serial
    }

    fn recv(&mut self) -> Result<Packet, RecvError> {
        let mut res = self.recv_buffered();
        if let Err(RecvError::NotReady) = res {
            // First discard stale data if there is any in the buffer.
            // This could happen e.g. reprogramming a board mid-packet.
            let now = Instant::now();
            if now.duration_since(self.last_rx) > Duration::from_millis(200) {
                self.flush_rx();
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
            // If this is the very first data we receive, discard it if received
            // before the startup holdoff. Likely it's a combination of stale
            // data and possibly corrupted initial data from the driver, so it's
            // better to throw it away otherwise the parser gets confused and
            // waits for a large amount of data before declaring it invalid.
            if self.first_rx && !self.rxbuf.empty() {
                self.first_rx = false;
                if self.startup_holdoff() {
                    self.flush_rx();
                    return Err(RecvError::NotReady);
                }
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

        let raw = pkt.as_bytes();
        // The leading separator terminates any partial frame at the receiver.
        let mut encoded = vec![SLIP_END; 1 + wire::max_serialized_size(raw.len())];
        let size = wire::serialize(raw, &mut encoded[1..]).expect("No fit in frame buffer");
        encoded.truncate(1 + size);

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

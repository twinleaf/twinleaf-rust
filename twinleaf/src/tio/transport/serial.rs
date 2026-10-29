//! The serial transport.
//!
//! Opens the serial ports that ports drive. Each TIO packet gets its CRC32
//! appended and is SLIP encoded on the serial stream, as its
//! [codec](super::codec::serial::Codec) does.
//! Newline delimited ASCII text that arrives is returned as
//! [`super::TransportEvent::Text`].

use super::task::{ByteStream, Rules};
use super::{RateInfo, RecvError};
use crate::rt;
use std::io;
use std::time::Duration;
use tokio_serial::{SerialPort, SerialPortBuilderExt, SerialStream};

fn io_error(error: tokio_serial::Error) -> io::Error {
    let kind = match error.kind() {
        tokio_serial::ErrorKind::NoDevice => io::ErrorKind::NotFound,
        tokio_serial::ErrorKind::InvalidInput => io::ErrorKind::InvalidInput,
        tokio_serial::ErrorKind::Unknown => io::ErrorKind::Other,
        tokio_serial::ErrorKind::Io(kind) => kind,
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

/// The default data rate of a serial port.
static DEFAULT_RATE: u32 = 115200;

/// The rules of a serial port's task. A heartbeat goes out every 100 ms.
/// Everything received in the first 50 ms is discarded. Bytes that wait over
/// 200 ms for the rest of their frame are discarded as stale.
pub(super) const RULES: Rules = Rules {
    heartbeat: Some(Duration::from_millis(100)),
    quiet: None,
    holdoff: Some(Duration::from_millis(50)),
    stale_after: Some(Duration::from_millis(200)),
    half_close: None,
};

/// Opens the serial port `url`, which looks like
/// `serial_port[:target_rate[:default_rate]]`. It starts with a serial
/// port, like `/dev/tty??` or `COMn`. `target_rate` is the rate tio tries
/// to configure the device to. `default_rate` is the rate the device
/// starts at, and the rate tio falls back to if communication fails. Both
/// rates default to 115200. Returns the stream at `default_rate`, with both
/// rates.
///
/// For example, `COM3:400000:115200` starts at 115.2k and tries to
/// negotiate 400k. It falls back to 115.2k if that fails, then or later.
///
/// # Errors
///
/// Returns an error if `url` is malformed or the port cannot be opened.
pub(super) fn open(url: &str) -> io::Result<(SerialStream, RateInfo)> {
    let (port_name, target_rate, default_rate) = parse_config(url)?;
    let port = {
        let _runtime = rt::handle().enter();
        tokio_serial::new(port_name, default_rate)
            .open_native_async()
            .map_err(io_error)?
    };
    #[cfg(target_os = "windows")]
    {
        // Windows requires some custom settings to replicate the unix behavior.
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Devices::Communication::{SetCommTimeouts, COMMTIMEOUTS};
        use windows_sys::Win32::Foundation::HANDLE;
        let handle: HANDLE = port.as_raw_handle() as HANDLE;
        let timeouts = COMMTIMEOUTS {
            ReadIntervalTimeout: 0xFFFFFFFF,
            ReadTotalTimeoutMultiplier: 0xFFFFFFFF,
            ReadTotalTimeoutConstant: 0xFFFFFFFE,
            WriteTotalTimeoutMultiplier: 0,
            WriteTotalTimeoutConstant: 0,
        };
        if unsafe { SetCommTimeouts(handle, &timeouts) } == 0 {
            return Err(io::Error::last_os_error());
        }
    }
    let rates = RateInfo {
        default_bps: default_rate,
        target_bps: target_rate,
    };
    Ok((port, rates))
}

/// Returns [`RecvError::Disconnected`] for the error 6 that macOS reports when
/// an open serial port is unplugged. Returns any other error unchanged.
fn disconnect_on_unplug(error: RecvError) -> RecvError {
    #[cfg(target_os = "macos")]
    if let RecvError::IO(io) = &error {
        if io.raw_os_error() == Some(6) {
            return RecvError::Disconnected;
        }
    }
    error
}

impl ByteStream for SerialStream {
    fn set_rate(&mut self, rate: u32) -> io::Result<()> {
        self.set_baud_rate(rate).map_err(io_error)
    }

    fn read_error(error: io::Error) -> RecvError {
        disconnect_on_unplug(RecvError::IO(error))
    }
}

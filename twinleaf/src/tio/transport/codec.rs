//! The packet framing of each transport, without any I/O.
//!
//! A codec turns the bytes a link receives into packets, and packets into
//! the bytes the link sends. It opens no socket or serial port and keeps no
//! clock or thread. The port that uses it owns the buffers and does the I/O.
//!
//! - `tcp` frames packets back to back on a byte stream.
//! - `serial` frames each packet with SLIP and a CRC32, and passes on the
//!   text a device prints between frames.
//! - `udp` frames one packet per datagram.

#[cfg(feature = "serial")]
pub(crate) mod serial;
pub(crate) mod tcp;
pub(crate) mod udp;

use crate::tio::packet::{self, Packet};

/// Why received bytes did not decode as a packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    /// Bytes that are not a valid packet, with the reason.
    Invalid(packet::DecodeError),
    /// A frame that holds more bytes after its packet.
    TrailingBytes,
    /// A stream with no packet boundary left to find, after a header that
    /// did not decode.
    LostFraming,
}

/// Decodes `raw`, a whole frame or datagram, as exactly one packet.
///
/// # Errors
///
/// Returns [`Error::TrailingBytes`] if the packet ends before `raw` does.
/// Returns [`Error::Invalid`] if `raw` does not start with a valid packet,
/// with [`PacketTooSmall`](packet::DecodeError::PacketTooSmall) if it ends
/// before the packet does.
fn decode_exact(raw: &[u8]) -> Result<Packet, Error> {
    match Packet::from_slice_prefix(raw) {
        Ok((packet, size)) if size == raw.len() => Ok(packet),
        Ok(_) => Err(Error::TrailingBytes),
        // A frame is a whole packet or nothing, so a short one is not a
        // packet still arriving.
        Err(packet::DecodeError::NeedMore) => {
            Err(Error::Invalid(packet::DecodeError::PacketTooSmall))
        }
        Err(error) => Err(Error::Invalid(error)),
    }
}

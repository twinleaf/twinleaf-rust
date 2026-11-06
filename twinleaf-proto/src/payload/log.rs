//! The `LOG` packet, a log message.
//!
//! The payload is `{ data: u32le, level: u8 }` followed by the message bytes.
//! The message runs to the end of the payload and is not NUL-terminated.
//!
//! `data` is chosen by the sender. Twinleaf devices send their uptime in
//! milliseconds. [`LogMessage`] parses and writes the packet.

use crate::packet::{Header, Packet, PacketType};

/// Size of the `{ data, level }` head before the message.
pub const LOG_HEADER_SIZE: usize = 5;
/// Longest message one `LOG` packet carries (`TL_LOG_MAX_MESSAGE_SIZE`).
pub const MAX_MESSAGE_SIZE: usize = Packet::MAX_PAYLOAD - LOG_HEADER_SIZE;

/// Severity of a log message (`TL_LOG_*`), 0 for critical up to 4 for debug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct LogLevel(u8);

impl LogLevel {
    /// Critical, 0.
    pub const CRITICAL: Self = Self(0);
    /// Error, 1.
    pub const ERROR: Self = Self(1);
    /// Warning, 2.
    pub const WARNING: Self = Self(2);
    /// Info, 3.
    pub const INFO: Self = Self(3);
    /// Debug, 4.
    pub const DEBUG: Self = Self(4);

    /// Creates a level from its byte.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// Returns the level byte.
    pub const fn value(self) -> u8 {
        self.0
    }
}

/// One log message, as it travels in a `LOG` packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogMessage<'a> {
    /// Severity.
    pub level: LogLevel,
    /// Sender-defined value, the uptime in milliseconds on Twinleaf devices.
    pub data: u32,
    /// Message text, not NUL-terminated.
    pub message: &'a [u8],
}

impl<'a> LogMessage<'a> {
    /// Parses a message from a `LOG` packet payload, without the packet header.
    /// Returns `None` if the payload is shorter than [`LOG_HEADER_SIZE`].
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        if payload.len() < LOG_HEADER_SIZE {
            return None;
        }
        Some(Self {
            level: LogLevel::new(payload[4]),
            data: u32::from_le_bytes(*payload.first_chunk()?),
            message: &payload[LOG_HEADER_SIZE..],
        })
    }

    /// Writes a full `LOG` packet into `buf`. Returns its length, or `None` if
    /// it does not fit or the message exceeds [`MAX_MESSAGE_SIZE`].
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        if self.message.len() > MAX_MESSAGE_SIZE {
            return None;
        }
        let payload_len = LOG_HEADER_SIZE + self.message.len();
        let total = Header::SIZE + payload_len;
        if buf.len() < total {
            return None;
        }
        let hdr = Header::new(PacketType::LOG, payload_len as u16);
        hdr.write(buf.first_chunk_mut()?);
        buf[Header::SIZE..Header::SIZE + 4].copy_from_slice(&self.data.to_le_bytes());
        buf[Header::SIZE + 4] = self.level.value();
        buf[Header::SIZE + LOG_HEADER_SIZE..total].copy_from_slice(self.message);
        Some(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::strategy::{buffer, bytes, bytes_to, octet, slack, word, write_over_any};
    use proptest::prelude::*;

    /// A level byte of any value. Two in three are 0 or 0xFF.
    fn level() -> impl Strategy<Value = LogLevel> {
        octet().prop_map(LogLevel::new)
    }

    /// A message of any length up to one past the longest a packet carries.
    fn any_length() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            bytes_to(MAX_MESSAGE_SIZE),
            bytes(MAX_MESSAGE_SIZE + 1),
            bytes(MAX_MESSAGE_SIZE + 2),
        ]
    }

    proptest! {
        /// A message of up to [`MAX_MESSAGE_SIZE`] bytes writes a `LOG` packet
        /// of exactly its length, whose payload parses back to it, whatever
        /// the buffer held before.
        #[test]
        fn a_log_message_decodes_as_it_was_encoded(
            level in level(),
            data in word(),
            message in bytes_to(MAX_MESSAGE_SIZE),
        ) {
            let log = LogMessage { level, data, message: &message };
            let payload_len = LOG_HEADER_SIZE + message.len();
            let (len, buf) = write_over_any(Header::SIZE + payload_len, |buf| log.write(buf))?;
            prop_assert_eq!(len, Some(buf.len()));
            let header = Header::parse(buf[..Header::SIZE].try_into().unwrap()).unwrap();
            prop_assert_eq!(header, Header::new(PacketType::LOG, payload_len as u16));
            prop_assert_eq!(LogMessage::parse(&buf[Header::SIZE..]), Some(log));
        }

        /// A payload of five bytes or more parses, and writing what was parsed
        /// gives back the same payload, whatever the buffer held before. A
        /// shorter one does not parse.
        #[test]
        fn any_payload_parses_when_it_holds_the_head(payload in prop_oneof![
            (0..=LOG_HEADER_SIZE + 1).prop_flat_map(bytes),
            bytes_to(Packet::MAX_PAYLOAD),
        ]) {
            let Some(log) = LogMessage::parse(&payload) else {
                prop_assert!(payload.len() < LOG_HEADER_SIZE);
                return Ok(());
            };
            prop_assert!(payload.len() >= LOG_HEADER_SIZE);
            let (len, buf) = write_over_any(Header::SIZE + payload.len(), |buf| log.write(buf))?;
            prop_assert_eq!(len, Some(buf.len()));
            prop_assert_eq!(&buf[Header::SIZE..], &payload[..]);
        }

        /// A message is written when it is at most [`MAX_MESSAGE_SIZE`] bytes
        /// and the buffer holds the whole packet, and refused otherwise.
        #[test]
        fn a_log_message_is_written_only_when_it_fits(message in any_length(), slack in slack()) {
            let log = LogMessage { level: LogLevel::INFO, data: 0, message: &message };
            let needed = Header::SIZE + LOG_HEADER_SIZE + message.len();
            let mut buf = buffer(needed, slack);
            let fits = message.len() <= MAX_MESSAGE_SIZE && buf.len() >= needed;
            prop_assert_eq!(log.write(&mut buf), fits.then_some(needed));
        }
    }

    /// Byte-exact packed layout of the log payload behind its packet header
    /// (`type = TL_PTYPE_LOG`, `routing_size_and_ttl = 0`, `payload_size`
    /// covering the fixed header and the message).
    #[test]
    fn matches_the_packed_wire_layout() {
        let log = LogMessage {
            level: LogLevel::INFO,
            data: 0x0102_0304,
            message: b"up",
        };
        let mut buf = [0u8; 32];
        let len = log.write(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            1, 0, 7, 0,                 // header: LOG, no routing, payload 7
            0x04, 0x03, 0x02, 0x01,     // data, little-endian
            3,                          // level, TL_LOG_INFO
            b'u', b'p',
        ]);
    }

    /// A receiver takes the level and data from the fixed header, and the
    /// message running from there to the end of the payload.
    #[test]
    fn decodes_level_data_and_message() {
        // A packet as it arrives on the wire.
        let raw = [
            1, 0, 13, 0, // LOG, no routing, payload_size 13
            0xE8, 0x03, 0x00, 0x00, // data = 1000 ms
            1,    // TL_LOG_ERROR
            b'i', b'm', b'u', b' ', b'f', b'a', b'i', b'l',
        ];
        let header = Header::parse((&raw[..Header::SIZE]).try_into().unwrap()).unwrap();
        assert_eq!(header.ptype, PacketType::LOG);
        assert_eq!(header.packet_len(), raw.len());
        // tl_log_packet_message_size
        assert_eq!(
            header.payload_size as usize - LOG_HEADER_SIZE,
            "imu fail".len()
        );

        let log = LogMessage::parse(&raw[header.payload_range()]).unwrap();
        assert_eq!(log.level, LogLevel::ERROR);
        assert_eq!(log.data, 1000);
        assert_eq!(log.message, b"imu fail");
    }

    #[test]
    fn levels_match_the_wire_and_order_by_verbosity() {
        assert_eq!(LogLevel::CRITICAL.value(), 0);
        assert_eq!(LogLevel::ERROR.value(), 1);
        assert_eq!(LogLevel::WARNING.value(), 2);
        assert_eq!(LogLevel::INFO.value(), 3);
        assert_eq!(LogLevel::DEBUG.value(), 4);
        // `level > logThreshold` drops the message, so debug is dropped at an
        // info threshold and critical never is.
        assert!(LogLevel::DEBUG > LogLevel::INFO);
        assert!(LogLevel::CRITICAL < LogLevel::INFO);
    }

    #[test]
    fn max_message_size_matches_the_wire() {
        // TL_PACKET_MAX_PAYLOAD_SIZE - sizeof(tl_log_header)
        assert_eq!(MAX_MESSAGE_SIZE, 500 - 5);
    }
}

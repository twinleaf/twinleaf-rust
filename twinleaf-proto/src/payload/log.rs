//! LOG packet, a log message.
//!
//! Payload: `{ data: u32le, level: u8 }` then the message bytes, not
//! NUL-terminated, running to the end of the payload.
//!
//! `data` is chosen by the sender. Twinleaf devices send milliseconds since
//! boot.

use crate::packet::{Header, Packet, PacketType};

/// `{ data, level }` preceding the message.
pub const LOG_HEADER_SIZE: usize = 5;
/// Longest message one LOG packet carries (`TL_LOG_MAX_MESSAGE_SIZE`).
pub const MAX_MESSAGE_SIZE: usize = Packet::MAX_PAYLOAD - LOG_HEADER_SIZE;

/// Severity of a log message (`TL_LOG_*`), including unknown values.
///
/// Ordered by verbosity, not by importance: [`CRITICAL`](Self::CRITICAL) is
/// zero and [`DEBUG`](Self::DEBUG) is four, so a sender drops everything above
/// its threshold with a single compare.
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

    /// Level from its byte.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// The level byte.
    pub const fn value(self) -> u8 {
        self.0
    }
}

/// One log message, as it travels in a LOG packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogMessage<'a> {
    /// Severity.
    pub level: LogLevel,
    /// Sender-defined value, milliseconds since boot on Twinleaf devices.
    pub data: u32,
    /// Message text, not NUL-terminated.
    pub message: &'a [u8],
}

impl<'a> LogMessage<'a> {
    /// Parse a message from a LOG packet payload (packet header excluded).
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        if payload.len() < LOG_HEADER_SIZE {
            return None;
        }
        Some(Self {
            level: LogLevel::new(payload[4]),
            data: u32::from_le_bytes(payload[..4].try_into().unwrap()),
            message: &payload[LOG_HEADER_SIZE..],
        })
    }

    /// Serialize a full LOG packet (header included) into `buf`; returns its
    /// length. Returns None if `buf` is too small or the message exceeds
    /// [`MAX_MESSAGE_SIZE`] — senders truncate rather than growing the packet.
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
        hdr.write((&mut buf[..Header::SIZE]).try_into().unwrap());
        buf[Header::SIZE..Header::SIZE + 4].copy_from_slice(&self.data.to_le_bytes());
        buf[Header::SIZE + 4] = self.level.value();
        buf[Header::SIZE + LOG_HEADER_SIZE..total].copy_from_slice(self.message);
        Some(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let log = LogMessage {
            level: LogLevel::WARNING,
            data: 12_345,
            message: b"vbus low",
        };
        let mut buf = [0u8; 64];
        let len = log.write(&mut buf).unwrap();
        assert_eq!(len, Header::SIZE + LOG_HEADER_SIZE + 8);
        assert_eq!(LogMessage::parse(&buf[Header::SIZE..len]), Some(log));
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

    /// What a receiver does with the packet: level and data from the fixed
    /// header, the message running from there to the end of the payload.
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
    fn an_empty_message_is_a_valid_packet() {
        let log = LogMessage {
            level: LogLevel::DEBUG,
            data: 0,
            message: b"",
        };
        let mut buf = [0u8; 16];
        let len = log.write(&mut buf).unwrap();
        assert_eq!(len, Header::SIZE + LOG_HEADER_SIZE);
        assert_eq!(LogMessage::parse(&buf[Header::SIZE..len]), Some(log));
    }

    #[test]
    fn rejects_truncated_payload_and_oversize_message() {
        assert_eq!(LogMessage::parse(&[0; LOG_HEADER_SIZE - 1]), None);
        let log = LogMessage {
            level: LogLevel::INFO,
            data: 0,
            message: &[b'x'; MAX_MESSAGE_SIZE + 1],
        };
        assert_eq!(log.write(&mut [0u8; Packet::MAX_SIZE]), None);
        // A message that fits exactly still does.
        let full = LogMessage {
            message: &[b'x'; MAX_MESSAGE_SIZE],
            ..log
        };
        let mut buf = [0u8; Packet::MAX_SIZE];
        assert_eq!(
            full.write(&mut buf),
            Some(Header::SIZE + Packet::MAX_PAYLOAD),
            "the longest message fills the payload exactly"
        );
    }

    #[test]
    fn max_message_size_matches_the_wire() {
        // TL_PACKET_MAX_PAYLOAD_SIZE - sizeof(tl_log_header)
        assert_eq!(MAX_MESSAGE_SIZE, 500 - 5);
    }
}

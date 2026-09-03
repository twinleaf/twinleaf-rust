//! SYNC packet (timeref broadcast) wire format
//!
//! Payload: `{ type: u8, epoch: u8, serial_len: u8, pad: u8, time: u32le,
//! session: u32le }` then a serial string of `serial_len` bytes (not
//! NUL-terminated).
//!
//! A hub broadcasts one SYNC packet per second to every child port. `time` is
//! the current second in `epoch`'s timescale at transmission; receivers anchor
//! `time + 1` to their next PPS edge.
//! `session` identifies the timebase instance and `serial` its source device,
//! so a child can detect a reference change.

use crate::packet::{Header, PacketType};
use crate::SessionId;

/// Timeref header bytes preceding the serial string.
pub const TIMEREF_HEADER_SIZE: usize = 12;
/// Longest serial receivers keep (tl_stream_timeref's `char serial[33]`).
pub const MAX_SERIAL_SIZE: usize = 32;

/// Timescale identifier (TL_METADATA_EPOCH_*), including unknown values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Epoch(u8);

impl Epoch {
    pub const INVALID: Self = Self(0);
    pub const ZERO: Self = Self(1);
    pub const SYSTIME: Self = Self(2);
    pub const UNIX: Self = Self(3);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u8 {
        self.0
    }
}

impl core::fmt::Display for Epoch {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {
            Self::INVALID => f.write_str("invalid"),
            Self::ZERO => f.write_str("zero"),
            Self::SYSTIME => f.write_str("systime"),
            Self::UNIX => f.write_str("unix"),
            Self(value) => write!(f, "epoch{value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeref<'a> {
    pub epoch: Epoch,
    /// Current second in `epoch`'s timescale when the packet was sent.
    pub time: u32,
    pub session: SessionId,
    /// Serial of the timebase source (not NUL-terminated).
    pub serial: &'a [u8],
}

impl<'a> Timeref<'a> {
    /// Parse a timeref from a SYNC packet payload (header excluded). Returns
    /// `None` for unknown timeref types: only type 0's layout is defined.
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        if payload.len() < TIMEREF_HEADER_SIZE || payload[0] != 0 {
            return None;
        }
        let serial_len = payload[2] as usize;
        let rest = &payload[TIMEREF_HEADER_SIZE..];
        if rest.len() < serial_len {
            return None;
        }
        Some(Self {
            epoch: Epoch::new(payload[1]),
            time: u32::from_le_bytes(payload[4..8].try_into().unwrap()),
            session: SessionId::from_le_bytes(payload[8..12].try_into().unwrap()),
            serial: &rest[..serial_len],
        })
    }

    /// Serialize a full SYNC packet (header included) into `buf`; returns
    /// length. Returns None if `buf` is too small or the serial exceeds
    /// [`MAX_SERIAL_SIZE`].
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        if self.serial.len() > MAX_SERIAL_SIZE {
            return None;
        }
        let payload_len = TIMEREF_HEADER_SIZE + self.serial.len();
        let total = Header::SIZE + payload_len;
        if buf.len() < total {
            return None;
        }
        let hdr = Header::new(PacketType::SYNC, payload_len as u16);
        hdr.write((&mut buf[..Header::SIZE]).try_into().unwrap());
        buf[4] = 0; // timeref type
        buf[5] = self.epoch.value();
        buf[6] = self.serial.len() as u8;
        buf[7] = 0; // pad
        buf[8..12].copy_from_slice(&self.time.to_le_bytes());
        buf[12..16].copy_from_slice(&self.session.to_le_bytes());
        buf[16..total].copy_from_slice(self.serial);
        Some(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let timeref = Timeref {
            epoch: Epoch::UNIX,
            time: 1_774_137_600,
            session: SessionId::new(0xBAE5_B410),
            serial: b"twinleaf-37383731",
        };
        let mut buf = [0u8; 64];
        let len = timeref.write(&mut buf).unwrap();
        assert_eq!(len, Header::SIZE + TIMEREF_HEADER_SIZE + 17);
        assert_eq!(Timeref::parse(&buf[Header::SIZE..len]), Some(timeref));
    }

    /// Byte-exact packed layout of the timeref payload.
    #[test]
    fn matches_the_packed_wire_layout() {
        let timeref = Timeref {
            epoch: Epoch::UNIX,
            time: 0x0102_0304,
            session: SessionId::new(0x0A0B_0C0D),
            serial: b"AB",
        };
        let mut buf = [0u8; 32];
        let len = timeref.write(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            62, 0, 14, 0,               // header: SYNC, no routing, payload 14
            0, 3, 2, 0,                 // type, epoch UNIX, serial_len, pad
            0x04, 0x03, 0x02, 0x01,     // time, little-endian
            0x0D, 0x0C, 0x0B, 0x0A,     // session, little-endian
            b'A', b'B',
        ]);
    }

    #[test]
    fn rejects_truncated_and_unknown_type() {
        assert_eq!(Timeref::parse(&[0; TIMEREF_HEADER_SIZE - 1]), None);
        let mut unknown_type = [0u8; TIMEREF_HEADER_SIZE];
        unknown_type[0] = 1;
        assert_eq!(Timeref::parse(&unknown_type), None);
        let mut short_serial = [0u8; TIMEREF_HEADER_SIZE];
        short_serial[2] = 1; // serial_len beyond payload
        assert_eq!(Timeref::parse(&short_serial), None);
    }

    #[test]
    fn rejects_oversize_serial() {
        let timeref = Timeref {
            epoch: Epoch::SYSTIME,
            time: 0,
            session: SessionId::new(0),
            serial: &[b'x'; MAX_SERIAL_SIZE + 1],
        };
        assert_eq!(timeref.write(&mut [0u8; 64]), None);
    }
}

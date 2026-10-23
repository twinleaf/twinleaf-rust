//! SYNC packet, a time reference.
//!
//! Payload: `{ type: u8, epoch: u8, serial_len: u8, pad: u8, time: u32le,
//! session: u32le }` then a serial string of `serial_len` bytes, not
//! NUL-terminated, then optional TLVs `{ type: u8, length: u8, value }`, as
//! PTP extends its messages. A reader of the older layout stops at the serial.
//!
//! A hub sends one SYNC packet per second to each child. `time` is the
//! current second in `epoch`'s timescale, and the receiver labels its next
//! pulse `time + 1`. `session` and `serial` identify the timebase source.

use crate::packet::{Header, PacketType};
use crate::SessionId;

/// Timeref header bytes preceding the serial string.
pub const TIMEREF_HEADER_SIZE: usize = 12;
/// Longest serial, 32 bytes.
pub const MAX_SERIAL_SIZE: usize = 32;
/// TLV type of a [`Clock`].
pub const CLOCK_TLV: u8 = 1;
/// A clock TLV's bytes: type, length, and its four-byte value.
pub const CLOCK_TLV_SIZE: usize = 6;

/// Timescale identifier (TL_METADATA_EPOCH_*), including unknown values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Epoch(u8);

impl Epoch {
    /// No timescale, 0.
    pub const INVALID: Self = Self(0);
    /// Seconds from an arbitrary zero, 1.
    pub const ZERO: Self = Self(1);
    /// Seconds since the source booted, 2.
    pub const SYSTIME: Self = Self(2);
    /// Seconds since the Unix epoch, 3.
    pub const UNIX: Self = Self(3);

    /// Epoch from its byte.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// The epoch byte.
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

/// IEEE 1588 timeSource: what a root's time comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum TimeSource {
    /// ATOMIC_CLOCK, 0x10.
    AtomicClock,
    /// GNSS, 0x20.
    Gnss,
    /// TERRESTRIAL_RADIO, 0x30.
    TerrestrialRadio,
    /// PTP, 0x40.
    Ptp,
    /// NTP, 0x50.
    Ntp,
    /// HAND_SET, 0x60.
    HandSet,
    /// OTHER, 0x90.
    Other,
    /// INTERNAL_OSCILLATOR, 0xA0.
    InternalOscillator,
    /// A code none of the above names.
    Unlisted(u8),
}

impl TimeSource {
    /// The source a code names.
    pub const fn from_code(code: u8) -> Self {
        match code {
            0x10 => Self::AtomicClock,
            0x20 => Self::Gnss,
            0x30 => Self::TerrestrialRadio,
            0x40 => Self::Ptp,
            0x50 => Self::Ntp,
            0x60 => Self::HandSet,
            0x90 => Self::Other,
            0xA0 => Self::InternalOscillator,
            code => Self::Unlisted(code),
        }
    }

    /// The code byte.
    pub const fn code(self) -> u8 {
        match self {
            Self::AtomicClock => 0x10,
            Self::Gnss => 0x20,
            Self::TerrestrialRadio => 0x30,
            Self::Ptp => 0x40,
            Self::Ntp => 0x50,
            Self::HandSet => 0x60,
            Self::Other => 0x90,
            Self::InternalOscillator => 0xA0,
            Self::Unlisted(code) => code,
        }
    }
}

/// Upper bounds of clockAccuracy 0x20 through 0x30, in nanoseconds.
const ACCURACY_BOUNDS_NS: [u64; 17] = [
    25,
    100,
    250,
    1_000,
    2_500,
    10_000,
    25_000,
    100_000,
    250_000,
    1_000_000,
    2_500_000,
    10_000_000,
    25_000_000,
    100_000_000,
    250_000_000,
    1_000_000_000,
    10_000_000_000,
];

/// IEEE 1588 clockAccuracy, including codes outside its nanosecond table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ClockAccuracy(u8);

impl ClockAccuracy {
    /// Within 25 ns, 0x20.
    pub const WITHIN_25_NS: Self = Self(0x20);
    /// Beyond 10 s, 0x31.
    pub const BEYOND_10_S: Self = Self(0x31);
    /// Unknown, 0xFE.
    pub const UNKNOWN: Self = Self(0xFE);

    /// Accuracy from its code.
    pub const fn new(code: u8) -> Self {
        Self(code)
    }

    /// The code byte.
    pub const fn code(self) -> u8 {
        self.0
    }

    /// The tightest code that `ns` is within, from 25 ns up.
    pub fn within_ns(ns: u64) -> Self {
        ACCURACY_BOUNDS_NS
            .iter()
            .position(|&bound| ns <= bound)
            .map_or(Self::BEYOND_10_S, |index| {
                Self(Self::WITHIN_25_NS.0 + index as u8)
            })
    }

    /// The bound this code promises, or `None` if it promises none.
    pub fn bound_ns(self) -> Option<u64> {
        ACCURACY_BOUNDS_NS
            .get(usize::from(self.0.wrapping_sub(Self::WITHIN_25_NS.0)))
            .copied()
    }
}

/// The first octet of IEEE 1588's flagField, bits it does not name included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ClockFlags(u8);

impl ClockFlags {
    /// None set.
    pub const EMPTY: Self = Self(0);
    /// leap61, bit 0.
    pub const LEAP_61: Self = Self(1 << 0);
    /// leap59, bit 1.
    pub const LEAP_59: Self = Self(1 << 1);
    /// currentUtcOffsetValid, bit 2.
    pub const UTC_OFFSET_VALID: Self = Self(1 << 2);
    /// ptpTimescale, bit 3.
    pub const PTP_TIMESCALE: Self = Self(1 << 3);
    /// timeTraceable, bit 4.
    pub const TIME_TRACEABLE: Self = Self(1 << 4);
    /// frequencyTraceable, bit 5.
    pub const FREQUENCY_TRACEABLE: Self = Self(1 << 5);

    /// Flags from their octet.
    pub const fn new(bits: u8) -> Self {
        Self(bits)
    }

    /// The octet.
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Whether every flag of `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

/// Where a time reference stands relative to its root, as a PTP Announce says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Clock {
    /// Hops from the root, which is 0: PTP's stepsRemoved.
    pub steps_removed: u8,
    /// What the root's time comes from.
    pub time_source: TimeSource,
    /// How close the root is to its timescale.
    pub accuracy: ClockAccuracy,
    /// Leap second and traceability flags.
    pub flags: ClockFlags,
}

impl Clock {
    /// A root timed by `time_source`, of unknown accuracy and no traceability.
    pub const fn root(time_source: TimeSource) -> Self {
        Self {
            steps_removed: 0,
            time_source,
            accuracy: ClockAccuracy::UNKNOWN,
            flags: ClockFlags::EMPTY,
        }
    }

    /// The same clock one hop further from its root.
    pub const fn forwarded(self) -> Self {
        Self {
            steps_removed: self.steps_removed.saturating_add(1),
            ..self
        }
    }

    /// The first clock TLV of `tlvs`, or `None` if there is none or a TLV
    /// runs past the end.
    fn find(mut tlvs: &[u8]) -> Option<Self> {
        while let [kind, len, rest @ ..] = tlvs {
            let (value, next) = rest.split_at_checked(usize::from(*len))?;
            if *kind == CLOCK_TLV {
                let [steps_removed, time_source, accuracy, flags, ..] = *value else {
                    return None;
                };
                return Some(Self {
                    steps_removed,
                    time_source: TimeSource::from_code(time_source),
                    accuracy: ClockAccuracy(accuracy),
                    flags: ClockFlags(flags),
                });
            }
            tlvs = next;
        }
        None
    }
}

/// Time reference carried by a SYNC packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeref<'a> {
    /// Timescale of `time`.
    pub epoch: Epoch,
    /// Current second in `epoch`'s timescale when the packet was sent.
    pub time: u32,
    /// Session id of the timebase source.
    pub session: SessionId,
    /// Serial of the timebase source (not NUL-terminated).
    pub serial: &'a [u8],
    /// Where the reference stands relative to its root, absent from a source
    /// that predates the clock TLV.
    pub clock: Option<Clock>,
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
            clock: Clock::find(&rest[serial_len..]),
        })
    }

    /// The pad byte of a SYNC payload, which a hub's sequence and status
    /// ride in. Zero on a payload that carries neither.
    pub fn pad(payload: &[u8]) -> u8 {
        payload.get(3).copied().unwrap_or(0)
    }

    /// Write a full SYNC packet into `buf`. Returns its length, or None if it
    /// does not fit or the serial exceeds [`MAX_SERIAL_SIZE`].
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        self.write_with_pad(buf, 0)
    }

    /// Write a full SYNC packet whose pad byte carries `pad`, and a clock TLV
    /// after the serial when there is a clock.
    pub fn write_with_pad(&self, buf: &mut [u8], pad: u8) -> Option<usize> {
        if self.serial.len() > MAX_SERIAL_SIZE {
            return None;
        }
        let serial_end = Header::SIZE + TIMEREF_HEADER_SIZE + self.serial.len();
        let total = serial_end + self.clock.map_or(0, |_| CLOCK_TLV_SIZE);
        if buf.len() < total {
            return None;
        }
        let hdr = Header::new(PacketType::SYNC, (total - Header::SIZE) as u16);
        hdr.write((&mut buf[..Header::SIZE]).try_into().unwrap());
        buf[4] = 0; // timeref type
        buf[5] = self.epoch.value();
        buf[6] = self.serial.len() as u8;
        buf[7] = pad;
        buf[8..12].copy_from_slice(&self.time.to_le_bytes());
        buf[12..16].copy_from_slice(&self.session.to_le_bytes());
        buf[16..serial_end].copy_from_slice(self.serial);
        if let Some(clock) = self.clock {
            buf[serial_end..total].copy_from_slice(&[
                CLOCK_TLV,
                (CLOCK_TLV_SIZE - 2) as u8,
                clock.steps_removed,
                clock.time_source.code(),
                clock.accuracy.code(),
                clock.flags.bits(),
            ]);
        }
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
            clock: None,
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
            clock: None,
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
    fn a_pad_byte_rides_beside_the_serial_length() {
        let timeref = Timeref {
            epoch: Epoch::UNIX,
            time: 7,
            session: SessionId::new(1),
            serial: b"AB",
            clock: None,
        };
        let mut buf = [0u8; 32];
        let len = timeref.write_with_pad(&mut buf, 0xC5).unwrap();
        let payload = &buf[Header::SIZE..len];
        assert_eq!(Timeref::pad(payload), 0xC5);
        assert_eq!(Timeref::parse(payload), Some(timeref));

        let mut plain = [0u8; 32];
        assert_eq!(timeref.write(&mut plain), Some(len));
        assert_eq!(Timeref::pad(&plain[Header::SIZE..len]), 0);
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

    fn gps_two_hops() -> Timeref<'static> {
        Timeref {
            epoch: Epoch::UNIX,
            time: 0x0102_0304,
            session: SessionId::new(0x0A0B_0C0D),
            serial: b"AB",
            clock: Some(Clock {
                steps_removed: 2,
                time_source: TimeSource::Gnss,
                accuracy: ClockAccuracy::new(0x21),
                flags: ClockFlags::new(0x34),
            }),
        }
    }

    #[test]
    fn a_clock_round_trips() {
        let mut buf = [0u8; 64];
        let len = gps_two_hops().write(&mut buf).unwrap();
        assert_eq!(len, Header::SIZE + TIMEREF_HEADER_SIZE + 2 + CLOCK_TLV_SIZE);
        assert_eq!(
            Timeref::parse(&buf[Header::SIZE..len]),
            Some(gps_two_hops())
        );
    }

    #[test]
    fn a_clock_tlv_follows_the_serial() {
        let mut buf = [0u8; 32];
        let len = gps_two_hops().write(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            62, 0, 20, 0,
            0, 3, 2, 0,
            0x04, 0x03, 0x02, 0x01,
            0x0D, 0x0C, 0x0B, 0x0A,
            b'A', b'B',
            CLOCK_TLV, 4, 2, 0x20, 0x21, 0x34,
        ]);
    }

    /// A reader of the old layout takes the header and `serial_len` bytes,
    /// which a clock leaves exactly as they were.
    #[test]
    fn a_reader_of_the_old_layout_sees_the_same_timeref() {
        let mut new = [0u8; 64];
        let new_len = gps_two_hops().write(&mut new).unwrap();
        let mut old = [0u8; 64];
        let old_len = Timeref {
            clock: None,
            ..gps_two_hops()
        }
        .write(&mut old)
        .unwrap();
        assert_eq!(new[Header::SIZE..old_len], old[Header::SIZE..old_len]);
        assert_eq!(new_len - old_len, CLOCK_TLV_SIZE);
    }

    #[test]
    fn an_old_packet_parses_with_no_clock() {
        let mut buf = [0u8; 64];
        let len = Timeref {
            clock: None,
            ..gps_two_hops()
        }
        .write(&mut buf)
        .unwrap();
        assert_eq!(Timeref::parse(&buf[Header::SIZE..len]).unwrap().clock, None);
    }

    #[test]
    fn an_unknown_tlv_is_skipped_by_its_length() {
        let mut buf = [0u8; 64];
        let len = Timeref {
            clock: None,
            ..gps_two_hops()
        }
        .write(&mut buf)
        .unwrap();
        let mut payload = buf[Header::SIZE..len].to_vec();
        payload.extend_from_slice(&[0x7F, 3, 9, 9, 9, CLOCK_TLV, 4, 2, 0x20, 0x21, 0x34]);
        assert_eq!(Timeref::parse(&payload), Some(gps_two_hops()));
    }

    #[test]
    fn a_truncated_tlv_leaves_the_timeref_without_a_clock() {
        let mut buf = [0u8; 64];
        let len = gps_two_hops().write(&mut buf).unwrap();
        let payload = &buf[Header::SIZE..len];
        for cut in 1..CLOCK_TLV_SIZE {
            let parsed = Timeref::parse(&payload[..payload.len() - cut]);
            assert_eq!(
                parsed,
                Some(Timeref {
                    clock: None,
                    ..gps_two_hops()
                })
            );
        }
        let short_clock = [
            &payload[..payload.len() - CLOCK_TLV_SIZE],
            &[CLOCK_TLV, 1, 2],
        ]
        .concat();
        assert_eq!(Timeref::parse(&short_clock).unwrap().clock, None);
    }

    #[test]
    fn the_longest_timeref_with_a_clock_fits_a_packet() {
        let longest = Timeref {
            serial: &[b'x'; MAX_SERIAL_SIZE],
            ..gps_two_hops()
        };
        let mut buf = [0u8; crate::packet::Packet::MAX_SIZE];
        assert_eq!(
            longest.write(&mut buf),
            Some(Header::SIZE + TIMEREF_HEADER_SIZE + MAX_SERIAL_SIZE + CLOCK_TLV_SIZE)
        );
    }

    #[test]
    fn time_sources_keep_their_codes() {
        assert!((0..=u8::MAX).all(|code| TimeSource::from_code(code).code() == code));
        assert_eq!(TimeSource::from_code(0x20), TimeSource::Gnss);
        assert_eq!(TimeSource::from_code(0xA0), TimeSource::InternalOscillator);
        assert_eq!(TimeSource::from_code(0xF0), TimeSource::Unlisted(0xF0));
    }

    #[test]
    fn an_accuracy_names_the_tightest_bound_it_is_within() {
        assert_eq!(ClockAccuracy::within_ns(0), ClockAccuracy::WITHIN_25_NS);
        assert_eq!(ClockAccuracy::within_ns(26), ClockAccuracy::new(0x21));
        assert_eq!(ClockAccuracy::within_ns(1_000), ClockAccuracy::new(0x23));
        assert_eq!(
            ClockAccuracy::within_ns(10_000_000_000),
            ClockAccuracy::new(0x30)
        );
        assert_eq!(
            ClockAccuracy::within_ns(10_000_000_001),
            ClockAccuracy::BEYOND_10_S
        );
        assert_eq!(ClockAccuracy::new(0x23).bound_ns(), Some(1_000));
        assert_eq!(ClockAccuracy::BEYOND_10_S.bound_ns(), None);
        assert_eq!(ClockAccuracy::UNKNOWN.bound_ns(), None);
        assert_eq!(ClockAccuracy::new(0x1F).bound_ns(), None);
    }

    #[test]
    fn flags_sit_where_ptp_puts_them() {
        let flags = ClockFlags::new(0x30);
        assert!(flags.contains(ClockFlags::TIME_TRACEABLE));
        assert!(flags.contains(ClockFlags::FREQUENCY_TRACEABLE));
        assert!(!flags.contains(ClockFlags::PTP_TIMESCALE));
        assert_eq!(
            ClockFlags::LEAP_61.bits() | ClockFlags::UTC_OFFSET_VALID.bits(),
            0x05
        );
    }

    #[test]
    fn a_forwarded_clock_is_one_more_step_from_its_root() {
        let root = Clock::root(TimeSource::Ptp);
        assert_eq!(root.steps_removed, 0);
        assert_eq!(
            root.forwarded(),
            Clock {
                steps_removed: 1,
                ..root
            }
        );
        assert_eq!(
            Clock {
                steps_removed: u8::MAX,
                ..root
            }
            .forwarded()
            .steps_removed,
            u8::MAX
        );
    }

    #[test]
    fn rejects_oversize_serial() {
        let timeref = Timeref {
            epoch: Epoch::SYSTIME,
            time: 0,
            session: SessionId::new(0),
            serial: &[b'x'; MAX_SERIAL_SIZE + 1],
            clock: None,
        };
        assert_eq!(timeref.write(&mut [0u8; 64]), None);
    }
}

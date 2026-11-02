//! The `SYNC` packet, a time reference.
//!
//! The payload is a 12-byte header followed by a serial string of
//! `serial_len` bytes, not NUL-terminated, then optional TLVs
//! `{ type: u8, length: u8, value }`, as PTP extends its messages. A reader of
//! the older layout stops at the serial. The header is laid out as follows.
//!
//! ```text
//! { type: u8, epoch: u8, serial_len: u8, pad: u8, time: u32le, session: u32le }
//! ```
//!
//! A hub sends one `SYNC` packet per second to each child. `time` is the
//! current second in `epoch`'s timescale, and the receiver labels its next
//! pulse `time + 1`. `session` and `serial` identify the timebase source.
//! [`Timeref`] parses and writes the packet.

use super::array_at;
use crate::packet::{Header, PacketType};
use crate::SessionId;

/// Size of the timeref header before the serial string.
pub const TIMEREF_HEADER_SIZE: usize = 12;
/// Longest serial, 32 bytes.
pub const MAX_SERIAL_SIZE: usize = 32;
/// TLV type of a [`Clock`].
pub const CLOCK_TLV: u8 = 1;
/// Length of a clock TLV, its type and length bytes and its four-byte value.
pub const CLOCK_TLV_SIZE: usize = 6;

/// Timescale identifier (`TL_METADATA_EPOCH_*`), including unknown values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Epoch(u8);

impl Epoch {
    /// No timescale, 0.
    pub const INVALID: Self = Self(0);
    /// Seconds from an arbitrary zero, 1.
    pub const ZERO: Self = Self(1);
    /// Uptime of the source in seconds, 2.
    pub const SYSTIME: Self = Self(2);
    /// Unix time in seconds, 3.
    pub const UNIX: Self = Self(3);

    /// Creates an epoch from its byte.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// Returns the epoch byte.
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

/// What a root's time comes from, IEEE 1588's `timeSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum TimeSource {
    /// `ATOMIC_CLOCK`, 0x10.
    AtomicClock,
    /// GNSS, 0x20.
    Gnss,
    /// `TERRESTRIAL_RADIO`, 0x30.
    TerrestrialRadio,
    /// PTP, 0x40.
    Ptp,
    /// NTP, 0x50.
    Ntp,
    /// `HAND_SET`, 0x60.
    HandSet,
    /// OTHER, 0x90.
    Other,
    /// `INTERNAL_OSCILLATOR`, 0xA0.
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
    /// Hops from the root, which is 0, as PTP's `stepsRemoved` counts them.
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

/// Time reference carried by a `SYNC` packet.
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
    /// Parses a timeref from a `SYNC` packet payload, without the packet
    /// header. Returns `None` if the payload is truncated or its timeref type
    /// is not 0, the only type defined.
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
            time: u32::from_le_bytes(array_at(payload, 4)?),
            session: SessionId::from_le_bytes(array_at(payload, 8)?),
            serial: &rest[..serial_len],
            clock: Clock::find(&rest[serial_len..]),
        })
    }

    /// Returns the pad byte of a `SYNC` payload, which a hub's sequence and
    /// status ride in. Zero on a payload that carries neither.
    pub fn pad(payload: &[u8]) -> u8 {
        payload.get(3).copied().unwrap_or(0)
    }

    /// Writes a full `SYNC` packet into `buf`. Returns its length, or `None` if
    /// it does not fit or the serial exceeds [`MAX_SERIAL_SIZE`].
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        self.write_with_pad(buf, 0)
    }

    /// Writes a full `SYNC` packet whose pad byte carries `pad`, and a clock
    /// TLV after the serial when there is a clock. Returns its length, or
    /// `None` if it does not fit or the serial exceeds [`MAX_SERIAL_SIZE`].
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
        hdr.write(buf.first_chunk_mut()?);
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
    use crate::payload::strategy::{buffer, bytes, len_to, octet, slack, word, write_over_any};
    use proptest::prelude::*;
    use proptest::sample::Index;

    /// The four value bytes of a clock TLV, steps removed, source code,
    /// accuracy code and flags, each of any value. A third of the source
    /// codes are ones IEEE 1588 names.
    fn clock_bytes() -> impl Strategy<Value = [u8; 4]> {
        let source = prop_oneof![
            2 => octet(),
            1 => prop::sample::select(vec![0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x90, 0xA0]),
        ];
        (octet(), source, octet(), octet())
            .prop_map(|(steps, source, accuracy, flags)| [steps, source, accuracy, flags])
    }

    /// The clock that four value bytes describe.
    fn clock_of([steps_removed, source, accuracy, flags]: [u8; 4]) -> Clock {
        Clock {
            steps_removed,
            time_source: TimeSource::from_code(source),
            accuracy: ClockAccuracy::new(accuracy),
            flags: ClockFlags::new(flags),
        }
    }

    /// A clock of any field values.
    fn clock() -> impl Strategy<Value = Clock> {
        clock_bytes().prop_map(clock_of)
    }

    /// Where the bytes after a serial are cut short.
    #[derive(Debug, Clone)]
    enum Cut {
        /// Not at all.
        Whole,
        /// Exactly at the end of the clock TLV.
        AtClockEnd,
        /// One byte before the end of the clock TLV.
        BeforeClockEnd,
        /// Anywhere.
        Anywhere(Index),
    }

    /// A cut, a quarter of them each whole, at the end of the clock TLV, a
    /// byte before it, or anywhere.
    fn cut() -> impl Strategy<Value = Cut> {
        prop_oneof![
            Just(Cut::Whole),
            Just(Cut::AtClockEnd),
            Just(Cut::BeforeClockEnd),
            any::<Index>().prop_map(Cut::Anywhere),
        ]
    }

    /// The fields of a timeref, owning its serial.
    #[derive(Debug, Clone)]
    struct Fields {
        epoch: u8,
        time: u32,
        session: u32,
        serial: Vec<u8>,
        clock: Option<Clock>,
    }

    impl Fields {
        fn timeref(&self) -> Timeref<'_> {
            Timeref {
                epoch: Epoch::new(self.epoch),
                time: self.time,
                session: SessionId::new(self.session),
                serial: &self.serial,
                clock: self.clock,
            }
        }

        /// The length of the packet that carries them.
        fn packet_len(&self) -> usize {
            let clock = self.clock.map_or(0, |_| CLOCK_TLV_SIZE);
            Header::SIZE + TIMEREF_HEADER_SIZE + self.serial.len() + clock
        }
    }

    /// Timeref fields with a serial of up to `max_serial` bytes and a clock
    /// or none.
    fn fields(max_serial: usize) -> impl Strategy<Value = Fields> {
        (
            octet(),
            word(),
            word(),
            len_to(max_serial).prop_flat_map(bytes),
            prop::option::of(clock()),
        )
            .prop_map(|(epoch, time, session, serial, clock)| Fields {
                epoch,
                time,
                session,
                serial,
                clock,
            })
    }

    /// The bytes that follow a serial. Up to three TLVs of a type other than
    /// a clock, then a clock TLV or none, its value up to eight bytes and
    /// most often three to five, then any bytes after a clock. Returned with
    /// the clock TLV's value and where it ends.
    fn tlvs() -> impl Strategy<Value = (Vec<u8>, Option<(Clock, usize, usize)>)> {
        let other = (
            prop_oneof![Just(0), Just(u8::MAX), CLOCK_TLV + 1..=u8::MAX],
            len_to(8).prop_flat_map(bytes),
        );
        let clock_value = (
            clock_bytes(),
            prop_oneof![Just(3), Just(4), Just(5), 0..=8usize],
            bytes(4),
        );
        (
            prop::collection::vec(other, 0..=3),
            prop::option::of(clock_value),
            len_to(8).prop_flat_map(bytes),
        )
            .prop_map(|(others, clock, after)| {
                let mut tail = Vec::new();
                for (kind, value) in others {
                    tail.extend([kind, value.len() as u8]);
                    tail.extend(value);
                }
                let clock = clock.map(|(fields, len, extra)| {
                    let value = [&fields[..], &extra].concat();
                    tail.extend([CLOCK_TLV, len as u8]);
                    tail.extend(&value[..len]);
                    tail.extend(&after);
                    (clock_of(fields), len, tail.len() - after.len())
                });
                (tail, clock)
            })
    }

    /// A payload too short for the header, or a header of type 0, 1, or any,
    /// a serial length of 0, 32, 33, 255, or any, and a body that holds the
    /// serial, a byte short of it, or a few bytes over.
    fn any_payload() -> impl Strategy<Value = Vec<u8>> {
        let head = (
            prop_oneof![Just(0), Just(1), any::<u8>()],
            prop_oneof![
                Just(0),
                Just(MAX_SERIAL_SIZE as u8),
                Just(MAX_SERIAL_SIZE as u8 + 1),
                Just(u8::MAX),
                any::<u8>()
            ],
            bytes(TIMEREF_HEADER_SIZE),
            prop_oneof![Just(-1), Just(0), 1..=8isize],
        );
        prop_oneof![
            1 => (0..TIMEREF_HEADER_SIZE).prop_flat_map(bytes),
            4 => head.prop_flat_map(|(kind, serial_len, mut header, over)| {
                header[0] = kind;
                header[2] = serial_len;
                let body = (serial_len as usize).checked_add_signed(over).unwrap_or(0);
                bytes(body).prop_map(move |body| [&header[..], &body].concat())
            }),
        ]
    }

    proptest! {
        /// A timeref with a serial of up to [`MAX_SERIAL_SIZE`] bytes writes
        /// a `SYNC` packet of exactly its length, with the clock TLV after the
        /// serial when there is a clock and the pad byte as given. Its payload
        /// parses back to the timeref and the pad. A plain write is the same
        /// packet with a zero pad. Without the clock the payload is the same
        /// up to the end of the serial, which is all a reader of the layout
        /// without TLVs takes. Each is the same whatever the buffer held
        /// before.
        #[test]
        fn a_timeref_decodes_as_it_was_encoded(
            fields in fields(MAX_SERIAL_SIZE),
            pad in octet(),
        ) {
            let timeref = fields.timeref();
            let (len, buf) =
                write_over_any(fields.packet_len(), |buf| timeref.write_with_pad(buf, pad))?;
            prop_assert_eq!(len, Some(buf.len()));
            let header = Header::parse(buf[..Header::SIZE].try_into().unwrap()).unwrap();
            let payload = &buf[Header::SIZE..];
            prop_assert_eq!(header, Header::new(PacketType::SYNC, payload.len() as u16));
            prop_assert_eq!(Timeref::pad(payload), pad);
            prop_assert_eq!(Timeref::parse(payload), Some(timeref));

            let (len, mut plain) = write_over_any(buf.len(), |buf| timeref.write(buf))?;
            prop_assert_eq!(len, Some(buf.len()));
            prop_assert_eq!(Timeref::pad(&plain[Header::SIZE..]), 0);
            plain[Header::SIZE + 3] = pad;
            prop_assert_eq!(&plain, &buf);

            let old = Timeref { clock: None, ..timeref };
            let (len, old_buf) = write_over_any(
                Header::SIZE + TIMEREF_HEADER_SIZE + fields.serial.len(),
                |buf| old.write_with_pad(buf, pad),
            )?;
            prop_assert_eq!(len, Some(old_buf.len()));
            prop_assert_eq!(&old_buf[Header::SIZE..], &buf[Header::SIZE..old_buf.len()]);
        }

        /// The TLVs after the serial are walked by their lengths. The clock is
        /// the first clock TLV when the whole of it is there with a value of
        /// four bytes or more, and none when the TLVs before it or the clock
        /// TLV itself are cut short. Bytes after the clock TLV are ignored.
        #[test]
        fn the_clock_is_the_first_whole_clock_tlv(
            fields in fields(MAX_SERIAL_SIZE),
            (mut tail, clock) in tlvs(),
            cut in cut(),
        ) {
            let base = Fields { clock: None, ..fields };
            let mut buf = vec![0u8; base.packet_len()];
            base.timeref().write(&mut buf).unwrap();
            let clock_end = clock.map_or(tail.len(), |(.., end)| end);
            let kept = match cut {
                Cut::Whole => tail.len(),
                Cut::AtClockEnd => clock_end,
                Cut::BeforeClockEnd => clock_end.saturating_sub(1),
                Cut::Anywhere(at) => at.index(tail.len() + 1),
            };
            tail.truncate(kept);
            let expected = clock
                .filter(|&(_, len, end)| len >= 4 && kept >= end)
                .map(|(clock, ..)| clock);
            let payload = [&buf[Header::SIZE..], &tail].concat();
            prop_assert_eq!(
                Timeref::parse(&payload),
                Some(Timeref { clock: expected, ..base.timeref() })
            );
        }

        /// A payload parses when it holds the 12-byte header with type 0 and
        /// the serial the header measures. Writing what was parsed, with the
        /// payload's pad, gives back the payload up to the end of the serial,
        /// or nothing when the serial is over [`MAX_SERIAL_SIZE`] bytes,
        /// whatever the buffer held before. The pad of a payload too short to
        /// hold one is zero.
        #[test]
        fn any_payload_parses_when_it_holds_its_header_and_serial(payload in any_payload()) {
            let Some(timeref) = Timeref::parse(&payload) else {
                let holds = payload.len() >= TIMEREF_HEADER_SIZE
                    && payload.len() >= TIMEREF_HEADER_SIZE + payload[2] as usize;
                prop_assert!(!holds || payload[0] != 0);
                if payload.len() < 4 {
                    prop_assert_eq!(Timeref::pad(&payload), 0);
                }
                return Ok(());
            };
            let serial_end = TIMEREF_HEADER_SIZE + payload[2] as usize;
            prop_assert_eq!(payload[0], 0);
            prop_assert!(payload.len() >= serial_end);
            prop_assert_eq!(timeref.serial, &payload[TIMEREF_HEADER_SIZE..serial_end]);
            let (written, buf) = write_over_any(Header::SIZE + payload.len() + CLOCK_TLV_SIZE, |buf| {
                timeref.write_with_pad(buf, Timeref::pad(&payload))
            })?;
            if timeref.serial.len() > MAX_SERIAL_SIZE {
                prop_assert_eq!(written, None);
            } else {
                prop_assert!(written.is_some());
                prop_assert_eq!(&buf[Header::SIZE..][..serial_end], &payload[..serial_end]);
            }
        }

        /// A timeref is written when its serial is at most
        /// [`MAX_SERIAL_SIZE`] bytes and the buffer holds the whole packet,
        /// and refused otherwise.
        #[test]
        fn a_timeref_is_written_only_when_it_fits(
            fields in fields(MAX_SERIAL_SIZE + 1),
            slack in slack(),
        ) {
            let needed = fields.packet_len();
            let mut buf = buffer(needed, slack);
            let fits = fields.serial.len() <= MAX_SERIAL_SIZE && buf.len() >= needed;
            prop_assert_eq!(fields.timeref().write(&mut buf), fits.then_some(needed));
        }

        /// Each named clock flag is the bit IEEE 1588 gives it in the first
        /// octet of flagField, from leap61 at bit 0 to frequencyTraceable at
        /// bit 5, and an octet contains a set of flags when it has all their
        /// bits.
        #[test]
        fn clock_flags_are_the_bits_ptp_names(bits in octet(), other in octet()) {
            let flags = ClockFlags::new(bits);
            prop_assert_eq!(flags.bits(), bits);
            for (bit, named) in [
                ClockFlags::LEAP_61,
                ClockFlags::LEAP_59,
                ClockFlags::UTC_OFFSET_VALID,
                ClockFlags::PTP_TIMESCALE,
                ClockFlags::TIME_TRACEABLE,
                ClockFlags::FREQUENCY_TRACEABLE,
            ]
            .into_iter()
            .enumerate()
            {
                prop_assert_eq!(named.bits(), 1 << bit);
                prop_assert_eq!(flags.contains(named), bits & 1 << bit != 0);
            }
            prop_assert!(flags.contains(ClockFlags::EMPTY));
            prop_assert_eq!(flags.contains(ClockFlags::new(other)), bits & other == other);
        }
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
}

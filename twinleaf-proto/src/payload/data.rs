//! Sample packets and metadata records.
//!
//! A sample packet has type `STREAM0 + stream id` and a payload of
//! `[first sample number u24le][segment id u8]` followed by packed samples.
//!
//! A `METADATA` packet payload is `[record type u8][flags u8]` followed by one
//! record. A record is a fixed-size head whose first byte is its own length,
//! then the strings whose lengths the head records. A device may append head
//! fields, and readers take the strings from the declared head length.
//!
//! A `dev.metadata` RPC selects records with [`MetadataSelector`] triples and
//! replies with `[type][record length u8][record]` frames ([`MetadataReply`]).
//! The records inside a reply are bare, with no `[type][flags]` header.
//!
//! [`Samples`] parses and writes sample packets. [`Metadata`] parses and
//! writes records.

use zerocopy::byteorder::little_endian::{F32, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::packet::{Header, Packet, PacketType};
use crate::sync::Epoch;
use crate::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};

/// Size of the `[record type][flags]` header before every metadata record.
pub const METADATA_HEADER_SIZE: usize = 2;
/// Size of the `[first sample number u24le][segment id]` header before a
/// packet's samples.
pub const SAMPLE_HEADER_SIZE: usize = 4;
/// Largest sample number on the wire. Sample numbers wrap at 24 bits.
pub const MAX_SAMPLE_NUMBER: u32 = crate::SampleNumber::MAX;

/// Lowest stream id a device may allocate, 1. Id 0 is reserved for `STREAM0`,
/// which carries a 32-bit sample number and no segment id.
pub const FIRST_STREAM_ID: u8 = crate::StreamId::MIN;
/// Highest stream id, 127.
pub const LAST_STREAM_ID: u8 = crate::StreamId::MAX;

/// Column sample encoding (`TL_DATA_TYPE_*`). The high nibble is the byte size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct DataType(u8);

impl DataType {
    /// Unsigned 8-bit, 0x10.
    pub const U8: Self = Self(0x10);
    /// Signed 8-bit, 0x11.
    pub const I8: Self = Self(0x11);
    /// Unsigned 16-bit, 0x20.
    pub const U16: Self = Self(0x20);
    /// Signed 16-bit, 0x21.
    pub const I16: Self = Self(0x21);
    /// Unsigned 24-bit, 0x30.
    pub const U24: Self = Self(0x30);
    /// Signed 24-bit, 0x31.
    pub const I24: Self = Self(0x31);
    /// Unsigned 32-bit, 0x40.
    pub const U32: Self = Self(0x40);
    /// Signed 32-bit, 0x41.
    pub const I32: Self = Self(0x41);
    /// Unsigned 64-bit, 0x80.
    pub const U64: Self = Self(0x80);
    /// Signed 64-bit, 0x81.
    pub const I64: Self = Self(0x81);
    /// 32-bit float, 0x42.
    pub const F32: Self = Self(0x42);
    /// 64-bit float, 0x82.
    pub const F64: Self = Self(0x82);

    /// Creates a data type from its byte.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// Returns the type byte.
    pub const fn value(self) -> u8 {
        self.0
    }

    /// Returns the size of one value of this type, in bytes.
    pub const fn size(self) -> usize {
        (self.0 >> 4) as usize
    }
}

impl core::fmt::Display for DataType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match *self {
            Self::U8 => "u8",
            Self::I8 => "i8",
            Self::U16 => "u16",
            Self::I16 => "i16",
            Self::U24 => "u24",
            Self::I24 => "i24",
            Self::U32 => "u32",
            Self::I32 => "i32",
            Self::U64 => "u64",
            Self::I64 => "i64",
            Self::F32 => "f32",
            Self::F64 => "f64",
            Self(value) => return write!(f, "raw{value}"),
        };
        f.write_str(name)
    }
}

/// Why a metadata record was sent (`TL_METADATA_*` flags).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct MetadataFlags(u8);

impl MetadataFlags {
    /// Part of the periodic broadcast sweep.
    pub const PERIODIC: Self = Self(1 << 0);
    /// Sent after the thing it describes changed.
    pub const UPDATE: Self = Self(1 << 1);
    /// Closes a sweep. The host has now seen every record once.
    pub const LAST: Self = Self(1 << 2);

    /// Creates flags from their byte.
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Returns the raw bits.
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Returns `true` if no flags are set.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns `true` if every bit of `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns the flags set in either `self` or `other`.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl core::ops::BitOr for MetadataFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        self.union(rhs)
    }
}

/// State of the segment a metadata record describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct SegmentFlags(u8);

impl SegmentFlags {
    /// The rest of the segment record is populated. Without this, ignore it.
    pub const VALID: Self = Self(1);
    /// New samples are still being generated for this segment.
    pub const ACTIVE: Self = Self(2);
    /// The time reference's pulses were absent when this segment began.
    pub const HOLDOVER: Self = Self(4);

    /// Creates flags from their byte.
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Returns the raw bits.
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Returns `true` if no flags are set.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns `true` if every bit of `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns the flags set in either `self` or `other`.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl core::ops::BitOr for SegmentFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        self.union(rhs)
    }
}

/// Filter applied to floating-point columns before decimation
/// (`TL_METADATA_FILTER_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct FilterType(u8);

impl FilterType {
    /// No filter, 0.
    pub const NONE: Self = Self(0);
    /// First order IIR low-pass, 1.
    pub const IIR_SP_LPF1: Self = Self(1);
    /// Second order IIR low-pass, 2.
    pub const IIR_SP_LPF2: Self = Self(2);
    /// Fourth order IIR Butterworth low-pass, 3.
    pub const IIR_BW_LPF4: Self = Self(3);

    /// Creates a filter type from its byte.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// Returns the filter byte.
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl core::fmt::Display for FilterType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {
            Self::NONE => f.write_str("none"),
            Self::IIR_SP_LPF1 => f.write_str("iir-sp-lpf1"),
            Self::IIR_SP_LPF2 => f.write_str("iir-sp-lpf2"),
            Self::IIR_BW_LPF4 => f.write_str("iir-bw-lpf4"),
            Self(value) => write!(f, "filter{value}"),
        }
    }
}

/// Device identity and stream count (`TL_METADATA_DEVICE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Device<'a> {
    /// Session id of the device.
    pub session: SessionId,
    /// Streams the host should expect to be described in this sweep.
    pub n_streams: u8,
    /// Device name.
    pub name: &'a str,
    /// Device serial.
    pub serial: &'a str,
    /// Firmware build serial.
    pub firmware: &'a str,
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
struct DeviceHead {
    head_len: u8,
    name_len: u8,
    session: U32,
    serial_len: u8,
    firmware_len: u8,
    n_streams: u8,
}

impl<'a> Device<'a> {
    /// Length of the head this build reads. A device may declare a longer one.
    pub const HEAD_SIZE: usize = size_of::<DeviceHead>();

    /// Writes the bare record, without a `[type][flags]` header, into `buf`.
    /// Returns its length, or `None` if `buf` cannot hold the head. Strings
    /// are truncated to the room left.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, Self::HEAD_SIZE)?;
        let name_len = record.push(self.name);
        let serial_len = record.push(self.serial);
        let firmware_len = record.push(self.firmware);
        Some(record.finish(DeviceHead {
            head_len: Self::HEAD_SIZE as u8,
            name_len,
            session: self.session.value().into(),
            serial_len,
            firmware_len,
            n_streams: self.n_streams,
        }))
    }

    /// Parses a bare record, without a `[type][flags]` header. Returns `None`
    /// if the record is truncated or a string is not UTF-8. Fields past a shorter
    /// head than this build's read as zero.
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        let (head, mut strings) = RecordReader::split_defaulting::<DeviceHead>(buf)?;
        Some(Self {
            session: SessionId::new(head.session.get()),
            n_streams: head.n_streams,
            name: strings.take(head.name_len)?,
            serial: strings.take(head.serial_len)?,
            firmware: strings.take(head.firmware_len)?,
        })
    }
}

/// Shape of one data stream (`TL_METADATA_STREAM`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stream<'a> {
    /// Stream id.
    pub stream_id: StreamId,
    /// Number of columns in a sample.
    pub n_columns: u8,
    /// Number of segments in the ring.
    pub n_segments: u8,
    /// Bytes per sample, the packed size of all columns.
    pub sample_size: u16,
    /// Samples the device retains for retransmission.
    pub buf_samples: u16,
    /// Stream name.
    pub name: &'a str,
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
struct StreamHead {
    head_len: u8,
    stream_id: u8,
    n_columns: u8,
    n_segments: u8,
    sample_size: U16,
    buf_samples: U16,
    name_len: u8,
}

impl<'a> Stream<'a> {
    /// Length of the head this build reads. A device may declare a longer one.
    pub const HEAD_SIZE: usize = size_of::<StreamHead>();

    /// Writes the bare record, without a `[type][flags]` header, into `buf`.
    /// Returns its length, or `None` if `buf` cannot hold the head. Strings
    /// are truncated to the room left.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, Self::HEAD_SIZE)?;
        let name_len = record.push(self.name);
        Some(record.finish(StreamHead {
            head_len: Self::HEAD_SIZE as u8,
            stream_id: self.stream_id.value(),
            n_columns: self.n_columns,
            n_segments: self.n_segments,
            sample_size: self.sample_size.into(),
            buf_samples: self.buf_samples.into(),
            name_len,
        }))
    }

    /// Parses a bare record, without a `[type][flags]` header. Returns `None`
    /// if the record is truncated, the stream id is outside 1 to
    /// 127, or a string is not UTF-8. Fields past a shorter head than this
    /// build's read as zero.
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        let (head, mut strings) = RecordReader::split_defaulting::<StreamHead>(buf)?;
        Some(Self {
            stream_id: StreamId::try_new(head.stream_id)?,
            n_columns: head.n_columns,
            n_segments: head.n_segments,
            sample_size: head.sample_size.get(),
            buf_samples: head.buf_samples.get(),
            name: strings.take(head.name_len)?,
        })
    }
}

/// Acquisition parameters for one contiguous run of samples
/// (`TL_METADATA_SEGMENT`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Segment<'a> {
    /// Stream id.
    pub stream_id: StreamId,
    /// Segment id.
    pub segment_id: SegmentId,
    /// Segment state.
    pub flags: SegmentFlags,
    /// Timescale of `start_time`.
    pub epoch: Epoch,
    /// Serial of the device that owns the timebase.
    pub timeref_serial: &'a str,
    /// Session id of the timebase source.
    pub timeref_session: SessionId,
    /// Time of sample zero, in seconds after `epoch`.
    pub start_time: u32,
    /// Samples per second before decimation.
    pub sampling_rate: u32,
    /// Samples averaged into each output sample.
    pub decimation: u32,
    /// Filter cutoff in hertz.
    pub filter_cutoff: f32,
    /// Filter applied before decimation.
    pub filter_type: FilterType,
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
struct SegmentHead {
    head_len: u8,
    stream_id: u8,
    segment_id: u8,
    flags: u8,
    epoch: u8,
    timeref_serial_len: u8,
    timeref_session: U32,
    start_time: U32,
    sampling_rate: U32,
    decimation: U32,
    filter_cutoff: F32,
    filter_type: u8,
}

impl<'a> Segment<'a> {
    /// Length of the head this build reads. A device may declare a longer one.
    pub const HEAD_SIZE: usize = size_of::<SegmentHead>();

    /// Writes the bare record, without a `[type][flags]` header, into `buf`.
    /// Returns its length, or `None` if `buf` cannot hold the head. Strings
    /// are truncated to the room left.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, Self::HEAD_SIZE)?;
        let timeref_serial_len = record.push(self.timeref_serial);
        Some(record.finish(SegmentHead {
            head_len: Self::HEAD_SIZE as u8,
            stream_id: self.stream_id.value(),
            segment_id: self.segment_id.value(),
            flags: self.flags.bits(),
            epoch: self.epoch.value(),
            timeref_serial_len,
            timeref_session: self.timeref_session.value().into(),
            start_time: self.start_time.into(),
            sampling_rate: self.sampling_rate.into(),
            decimation: self.decimation.into(),
            filter_cutoff: self.filter_cutoff.into(),
            filter_type: self.filter_type.value(),
        }))
    }

    /// Parses a bare record, without a `[type][flags]` header. Returns `None`
    /// if the record is truncated, the stream id is outside 1 to
    /// 127, or a string is not UTF-8. Fields past a shorter head than this
    /// build's read as zero.
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        let (head, mut strings) = RecordReader::split_defaulting::<SegmentHead>(buf)?;
        Some(Self {
            stream_id: StreamId::try_new(head.stream_id)?,
            segment_id: SegmentId::new(head.segment_id),
            flags: SegmentFlags::from_bits(head.flags),
            epoch: Epoch::new(head.epoch),
            timeref_session: SessionId::new(head.timeref_session.get()),
            start_time: head.start_time.get(),
            sampling_rate: head.sampling_rate.get(),
            decimation: head.decimation.get(),
            filter_cutoff: head.filter_cutoff.get(),
            filter_type: FilterType::new(head.filter_type),
            timeref_serial: strings.take(head.timeref_serial_len)?,
        })
    }
}

/// One column of a stream's sample (`TL_METADATA_COLUMN`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column<'a> {
    /// Stream id.
    pub stream_id: StreamId,
    /// Position of this column within the packed sample.
    pub index: ColumnId,
    /// Sample encoding.
    pub data_type: DataType,
    /// Column name.
    pub name: &'a str,
    /// Units of the value.
    pub units: &'a str,
    /// Description of the column.
    pub description: &'a str,
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
struct ColumnHead {
    head_len: u8,
    stream_id: u8,
    index: u8,
    data_type: u8,
    name_len: u8,
    units_len: u8,
    description_len: u8,
}

impl<'a> Column<'a> {
    /// Length of the head this build reads. A device may declare a longer one.
    pub const HEAD_SIZE: usize = size_of::<ColumnHead>();

    /// Writes the bare record, without a `[type][flags]` header, into `buf`.
    /// Returns its length, or `None` if `buf` cannot hold the head. Strings
    /// are truncated to the room left.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, Self::HEAD_SIZE)?;
        let name_len = record.push(self.name);
        let units_len = record.push(self.units);
        let description_len = record.push(self.description);
        Some(record.finish(ColumnHead {
            head_len: Self::HEAD_SIZE as u8,
            stream_id: self.stream_id.value(),
            index: self.index.value(),
            data_type: self.data_type.value(),
            name_len,
            units_len,
            description_len,
        }))
    }

    /// Parses a bare record, without a `[type][flags]` header. Returns `None`
    /// if the record is truncated, the stream id is outside 1 to
    /// 127, or a string is not UTF-8. Fields past a shorter head than this
    /// build's read as zero.
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        let (head, mut strings) = RecordReader::split_defaulting::<ColumnHead>(buf)?;
        Some(Self {
            stream_id: StreamId::try_new(head.stream_id)?,
            index: ColumnId::new(head.index),
            data_type: DataType::new(head.data_type),
            name: strings.take(head.name_len)?,
            units: strings.take(head.units_len)?,
            description: strings.take(head.description_len)?,
        })
    }
}

/// Defines a view that reads one kind of bare record in place.
macro_rules! record_view {
    ($view:ident, $fields:ident, $head:ident) => {
        #[doc = concat!("A bare [`", stringify!($fields), "`] record, read in place.")]
        ///
        /// [`new_checked`](Self::new_checked) checks the record once. Each
        /// accessor then reads its field from the record's bytes, without
        /// copying the record or checking its strings again in full. A field
        /// past a shorter head than this build's reads as zero. Equality is
        /// byte equality, so a head field this build does not know still
        /// counts.
        #[derive(Clone, Copy, PartialEq, Eq)]
        pub struct $view<'a> {
            record: &'a [u8],
        }

        impl<'a> $view<'a> {
            /// Returns a view of `record`, or `None` if it does not parse as
            /// this record.
            pub fn new_checked(record: &'a [u8]) -> Option<Self> {
                $fields::parse(record)?;
                Some(Self { record })
            }

            /// Returns a view of `record` without checking it.
            ///
            /// # Panics
            ///
            /// Accessors panic if `record` would not pass
            /// [`new_checked`](Self::new_checked).
            pub fn new_unchecked(record: &'a [u8]) -> Self {
                Self { record }
            }

            /// Returns the bare record the view reads.
            pub fn bytes(&self) -> &'a [u8] {
                self.record
            }

            /// Returns every field this build knows, for writing the record
            /// again.
            pub fn fields(&self) -> $fields<'a> {
                $fields::parse(self.record).expect("a view's record was checked")
            }

            /// Reads the head field of type `T` at `offset`, or zero when the
            /// record's head ends before it.
            fn field<T: FromBytes>(&self, offset: usize) -> T {
                head_field(self.record, offset)
            }

            fn split(&self) -> ($head, RecordReader<'a>) {
                RecordReader::split_defaulting::<$head>(self.record)
                    .expect("a view's record was checked")
            }
        }

        impl core::fmt::Debug for $view<'_> {
            fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                self.fields().fmt(formatter)
            }
        }
    };
}

/// Reads the `n`th string of a checked record, after skipping the lengths
/// before it.
fn nth_string<'a>(mut strings: RecordReader<'a>, before: &[u8], len: u8) -> &'a str {
    for &skipped in before {
        strings.skip(skipped).expect("a view's record was checked");
    }
    strings.take(len).expect("a view's record was checked")
}

/// Reads the head field of type `T` at `offset` of a checked record, or zero
/// when the head the record declares ends before the field, the default the
/// protocol gives a field an older device does not send.
fn head_field<T: FromBytes>(record: &[u8], offset: usize) -> T {
    let end = offset + size_of::<T>();
    record
        .get(offset..end)
        .filter(|_| end <= usize::from(record[0]))
        .and_then(|bytes| T::read_from_bytes(bytes).ok())
        .unwrap_or_else(T::new_zeroed)
}

record_view!(DeviceView, Device, DeviceHead);
record_view!(StreamView, Stream, StreamHead);
record_view!(SegmentView, Segment, SegmentHead);
record_view!(ColumnView, Column, ColumnHead);

impl<'a> DeviceView<'a> {
    /// Session id of the device.
    pub fn session(&self) -> SessionId {
        SessionId::new(
            self.field::<U32>(core::mem::offset_of!(DeviceHead, session))
                .get(),
        )
    }

    /// Streams the host should expect to be described in this sweep.
    pub fn n_streams(&self) -> u8 {
        self.field::<u8>(core::mem::offset_of!(DeviceHead, n_streams))
    }

    /// Device name.
    pub fn name(&self) -> &'a str {
        let (head, strings) = self.split();
        nth_string(strings, &[], head.name_len)
    }

    /// Device serial.
    pub fn serial(&self) -> &'a str {
        let (head, strings) = self.split();
        nth_string(strings, &[head.name_len], head.serial_len)
    }

    /// Firmware build serial.
    pub fn firmware(&self) -> &'a str {
        let (head, strings) = self.split();
        nth_string(
            strings,
            &[head.name_len, head.serial_len],
            head.firmware_len,
        )
    }
}

impl<'a> StreamView<'a> {
    /// Stream id.
    ///
    /// # Panics
    ///
    /// Panics if the view reads a record that
    /// [`new_checked`](Self::new_checked) would refuse.
    pub fn stream_id(&self) -> StreamId {
        StreamId::try_new(self.field::<u8>(core::mem::offset_of!(StreamHead, stream_id)))
            .expect("a view's record was checked")
    }

    /// Number of columns in a sample.
    pub fn n_columns(&self) -> u8 {
        self.field::<u8>(core::mem::offset_of!(StreamHead, n_columns))
    }

    /// Number of segments in the ring.
    pub fn n_segments(&self) -> u8 {
        self.field::<u8>(core::mem::offset_of!(StreamHead, n_segments))
    }

    /// Bytes per sample, the packed size of all columns.
    pub fn sample_size(&self) -> u16 {
        self.field::<U16>(core::mem::offset_of!(StreamHead, sample_size))
            .get()
    }

    /// Samples the device retains for retransmission.
    pub fn buf_samples(&self) -> u16 {
        self.field::<U16>(core::mem::offset_of!(StreamHead, buf_samples))
            .get()
    }

    /// Stream name.
    pub fn name(&self) -> &'a str {
        let (head, strings) = self.split();
        nth_string(strings, &[], head.name_len)
    }
}

impl<'a> SegmentView<'a> {
    /// Stream id.
    ///
    /// # Panics
    ///
    /// Panics if the view reads a record that
    /// [`new_checked`](Self::new_checked) would refuse.
    pub fn stream_id(&self) -> StreamId {
        StreamId::try_new(self.field::<u8>(core::mem::offset_of!(SegmentHead, stream_id)))
            .expect("a view's record was checked")
    }

    /// Segment id.
    pub fn segment_id(&self) -> SegmentId {
        SegmentId::new(self.field::<u8>(core::mem::offset_of!(SegmentHead, segment_id)))
    }

    /// Segment state.
    pub fn flags(&self) -> SegmentFlags {
        SegmentFlags::from_bits(self.field::<u8>(core::mem::offset_of!(SegmentHead, flags)))
    }

    /// Timescale of `start_time`.
    pub fn epoch(&self) -> Epoch {
        Epoch::new(self.field::<u8>(core::mem::offset_of!(SegmentHead, epoch)))
    }

    /// Serial of the device that owns the timebase.
    pub fn timeref_serial(&self) -> &'a str {
        let (head, strings) = self.split();
        nth_string(strings, &[], head.timeref_serial_len)
    }

    /// Session id of the timebase source.
    pub fn timeref_session(&self) -> SessionId {
        SessionId::new(
            self.field::<U32>(core::mem::offset_of!(SegmentHead, timeref_session))
                .get(),
        )
    }

    /// Time of sample zero, in seconds after `epoch`.
    pub fn start_time(&self) -> u32 {
        self.field::<U32>(core::mem::offset_of!(SegmentHead, start_time))
            .get()
    }

    /// Samples per second before decimation.
    pub fn sampling_rate(&self) -> u32 {
        self.field::<U32>(core::mem::offset_of!(SegmentHead, sampling_rate))
            .get()
    }

    /// Samples averaged into each output sample.
    pub fn decimation(&self) -> u32 {
        self.field::<U32>(core::mem::offset_of!(SegmentHead, decimation))
            .get()
    }

    /// Filter cutoff in hertz.
    pub fn filter_cutoff(&self) -> f32 {
        self.field::<F32>(core::mem::offset_of!(SegmentHead, filter_cutoff))
            .get()
    }

    /// Filter applied before decimation.
    pub fn filter_type(&self) -> FilterType {
        FilterType::new(self.field::<u8>(core::mem::offset_of!(SegmentHead, filter_type)))
    }
}

impl<'a> ColumnView<'a> {
    /// Stream id.
    ///
    /// # Panics
    ///
    /// Panics if the view reads a record that
    /// [`new_checked`](Self::new_checked) would refuse.
    pub fn stream_id(&self) -> StreamId {
        StreamId::try_new(self.field::<u8>(core::mem::offset_of!(ColumnHead, stream_id)))
            .expect("a view's record was checked")
    }

    /// Position of this column within the packed sample.
    pub fn index(&self) -> ColumnId {
        ColumnId::new(self.field::<u8>(core::mem::offset_of!(ColumnHead, index)))
    }

    /// Sample encoding.
    pub fn data_type(&self) -> DataType {
        DataType::new(self.field::<u8>(core::mem::offset_of!(ColumnHead, data_type)))
    }

    /// Column name.
    pub fn name(&self) -> &'a str {
        let (head, strings) = self.split();
        nth_string(strings, &[], head.name_len)
    }

    /// Units of the value.
    pub fn units(&self) -> &'a str {
        let (head, strings) = self.split();
        nth_string(strings, &[head.name_len], head.units_len)
    }

    /// Description of the column.
    pub fn description(&self) -> &'a str {
        let (head, strings) = self.split();
        nth_string(
            strings,
            &[head.name_len, head.units_len],
            head.description_len,
        )
    }
}

/// Record type byte of a metadata record, as both the `dev.metadata` RPC and
/// the `METADATA` packet spell it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MetadataType {
    /// Device record, 1.
    Device,
    /// Stream record, 2.
    Stream,
    /// Segment record, 3.
    Segment,
    /// Column record, 4.
    Column,
    /// Any other record type.
    Unknown(u8),
}

impl From<u8> for MetadataType {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Device,
            2 => Self::Stream,
            3 => Self::Segment,
            4 => Self::Column,
            other => Self::Unknown(other),
        }
    }
}

impl From<MetadataType> for u8 {
    fn from(kind: MetadataType) -> Self {
        match kind {
            MetadataType::Device => 1,
            MetadataType::Stream => 2,
            MetadataType::Segment => 3,
            MetadataType::Column => 4,
            MetadataType::Unknown(value) => value,
        }
    }
}

/// One metadata record.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Metadata<'a> {
    /// Device identity and stream count.
    Device(Device<'a>),
    /// Shape of one stream.
    Stream(Stream<'a>),
    /// Acquisition parameters of one segment.
    Segment(Segment<'a>),
    /// One column of a stream.
    Column(Column<'a>),
}

impl<'a> Metadata<'a> {
    /// Returns `true` if record type `kind` has a defined layout, 1 to 4.
    pub const fn defines(kind: u8) -> bool {
        matches!(kind, 1..=4)
    }

    /// Writes the bare record into `buf`. Returns its type byte and length, or
    /// `None` if `buf` cannot hold the head.
    pub fn write_record(&self, buf: &mut [u8]) -> Option<(u8, usize)> {
        let (kind, len) = match self {
            Self::Device(record) => (MetadataType::Device, record.write(buf)?),
            Self::Stream(record) => (MetadataType::Stream, record.write(buf)?),
            Self::Segment(record) => (MetadataType::Segment, record.write(buf)?),
            Self::Column(record) => (MetadataType::Column, record.write(buf)?),
        };
        Some((kind.into(), len))
    }

    /// Writes one `dev.metadata` reply frame, `[type][record length u8][record]`,
    /// into `buf`. Returns its length, or `None` if the record does not fit
    /// `buf` or a `u8` length.
    pub fn write_reply_frame(&self, buf: &mut [u8]) -> Option<usize> {
        let (kind, len) = self.write_record(buf.get_mut(METADATA_REPLY_FRAME_HEADER..)?)?;
        buf[0] = kind;
        buf[1] = u8::try_from(len).ok()?;
        Some(METADATA_REPLY_FRAME_HEADER + len)
    }

    /// Returns the length of the bare record, with each string capped at 255
    /// bytes.
    pub fn record_len(&self) -> usize {
        fn string(value: &str) -> usize {
            value.len().min(u8::MAX as usize)
        }
        match self {
            Self::Device(r) => {
                Device::HEAD_SIZE + string(r.name) + string(r.serial) + string(r.firmware)
            }
            Self::Stream(r) => Stream::HEAD_SIZE + string(r.name),
            Self::Segment(r) => Segment::HEAD_SIZE + string(r.timeref_serial),
            Self::Column(r) => {
                Column::HEAD_SIZE + string(r.name) + string(r.units) + string(r.description)
            }
        }
    }

    /// Writes a full `METADATA` packet with `flags` into `buf`. Returns its
    /// length, or `None` if the head does not fit or the payload exceeds
    /// [`Packet::MAX_PAYLOAD`]. Strings are truncated to the room left.
    pub fn write(&self, flags: MetadataFlags, buf: &mut [u8]) -> Option<usize> {
        let (kind, len) = self.write_record(buf.get_mut(Header::SIZE + METADATA_HEADER_SIZE..)?)?;
        write_metadata_headers(buf, kind, flags, len)
    }

    /// Parses a record and its flags from a `METADATA` packet payload, without
    /// the packet header. Returns `None` for an unknown record type or a
    /// malformed record.
    pub fn parse(payload: &'a [u8]) -> Option<(Self, MetadataFlags)> {
        let (kind, flags, record) = split_metadata(payload)?;
        let metadata = match MetadataType::from(kind) {
            MetadataType::Device => Self::Device(Device::parse(record)?),
            MetadataType::Stream => Self::Stream(Stream::parse(record)?),
            MetadataType::Segment => Self::Segment(Segment::parse(record)?),
            MetadataType::Column => Self::Column(Column::parse(record)?),
            MetadataType::Unknown(_) => return None,
        };
        Some((metadata, flags))
    }
}

/// Writes the packet and metadata headers around a record already in place.
/// Returns the packet length, or `None` if the payload is too big.
fn write_metadata_headers(
    buf: &mut [u8],
    kind: u8,
    flags: MetadataFlags,
    record_len: usize,
) -> Option<usize> {
    let payload_len = METADATA_HEADER_SIZE + record_len;
    if payload_len > Packet::MAX_PAYLOAD {
        return None;
    }
    Header::new(PacketType::METADATA, payload_len as u16)
        .write((&mut buf[..Header::SIZE]).try_into().unwrap());
    buf[Header::SIZE] = kind;
    buf[Header::SIZE + 1] = flags.bits();
    Some(Header::SIZE + payload_len)
}

/// Writes a full `METADATA` packet that carries the bare `record` unchanged.
/// Returns its length, or `None` if it does not fit `buf` or a packet.
pub fn write_metadata_record(
    buf: &mut [u8],
    kind: u8,
    flags: MetadataFlags,
    record: &[u8],
) -> Option<usize> {
    let start = Header::SIZE + METADATA_HEADER_SIZE;
    buf.get_mut(start..start + record.len())?
        .copy_from_slice(record);
    write_metadata_headers(buf, kind, flags, record.len())
}

/// Splits a `METADATA` payload into its record type, flags, and bare record,
/// without parsing the record. Returns `None` if the payload is under two
/// bytes.
pub fn split_metadata(payload: &[u8]) -> Option<(u8, MetadataFlags, &[u8])> {
    let (head, record) = payload.split_at_checked(METADATA_HEADER_SIZE)?;
    Some((head[0], MetadataFlags::from_bits(head[1]), record))
}

/// Shortest head a record can declare, its own length byte and one field.
const MIN_RECORD_HEAD: usize = 2;

/// Splits a record into the head it declares and the strings after it.
/// Returns `None` if the record is truncated or the head is under two bytes.
pub fn split_record(record: &[u8]) -> Option<(&[u8], &[u8])> {
    let declared = *record.first()? as usize;
    if declared < MIN_RECORD_HEAD || record.len() < declared {
        return None;
    }
    Some(record.split_at(declared))
}

/// Name of the RPC that reads metadata records on demand.
pub const METADATA_RPC_METHOD: &str = "dev.metadata";
/// Most selectors one `dev.metadata` request may carry.
pub const MAX_METADATA_SELECTORS: usize = 16;
/// Segment index selecting whichever segment is currently acquiring.
pub const CURRENT_SEGMENT: u8 = 0xff;
/// Size of the `[type][record length u8]` header before the record in each
/// reply frame.
pub const METADATA_REPLY_FRAME_HEADER: usize = 2;
/// Most frame bytes in one `dev.metadata` reply. A reply may hold fewer
/// records than were selected.
pub const MAX_METADATA_REPLY_SIZE: usize = Packet::MAX_PAYLOAD - crate::rpc::REPLY_HEADER_SIZE;

/// One `dev.metadata` request selector, encoded as `[type][stream id][index]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct MetadataSelector {
    /// Record type requested.
    pub mtype: MetadataType,
    /// Stream id. Ignored for a device record.
    pub stream_id: u8,
    /// Segment or column index, with [`CURRENT_SEGMENT`] naming the segment
    /// being acquired. Ignored for device and stream records.
    pub index: u8,
}

impl MetadataSelector {
    /// Encoded size of one selector.
    pub const SIZE: usize = 3;

    /// Creates the selector for the device record.
    pub const fn device() -> Self {
        Self {
            mtype: MetadataType::Device,
            stream_id: 0,
            index: 0,
        }
    }

    /// Creates a selector for the record of stream `stream_id`.
    pub const fn stream(stream_id: u8) -> Self {
        Self {
            mtype: MetadataType::Stream,
            stream_id,
            index: 0,
        }
    }

    /// Creates a selector for segment `index` of stream `stream_id`. `index`
    /// may be [`CURRENT_SEGMENT`].
    pub const fn segment(stream_id: u8, index: u8) -> Self {
        Self {
            mtype: MetadataType::Segment,
            stream_id,
            index,
        }
    }

    /// Creates a selector for column `index` of stream `stream_id`.
    pub const fn column(stream_id: u8, index: u8) -> Self {
        Self {
            mtype: MetadataType::Column,
            stream_id,
            index,
        }
    }

    /// Returns the three request bytes.
    pub fn encode(self) -> [u8; Self::SIZE] {
        [self.mtype.into(), self.stream_id, self.index]
    }
}

/// Why a `dev.metadata` request argument does not parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MetadataRequestError {
    /// Argument length is not a multiple of three.
    #[error("argument length is not a whole number of selectors")]
    Misaligned,
    /// More than sixteen selectors.
    #[error("more selectors than a request may carry")]
    TooManySelectors,
}

/// The selectors of a `dev.metadata` request argument. An empty argument
/// selects a device-chosen prefix of all records.
#[derive(Debug, Clone, Copy)]
pub struct MetadataRequest<'a> {
    arg: &'a [u8],
}

impl<'a> MetadataRequest<'a> {
    /// Parses the selectors of a request argument.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataRequestError::Misaligned`] if the length of `arg` is
    /// not a multiple of three, and [`MetadataRequestError::TooManySelectors`]
    /// if it holds more than [`MAX_METADATA_SELECTORS`].
    pub fn parse(arg: &'a [u8]) -> Result<Self, MetadataRequestError> {
        if !arg.len().is_multiple_of(MetadataSelector::SIZE) {
            return Err(MetadataRequestError::Misaligned);
        }
        if arg.len() / MetadataSelector::SIZE > MAX_METADATA_SELECTORS {
            return Err(MetadataRequestError::TooManySelectors);
        }
        Ok(Self { arg })
    }

    /// Returns `true` if the argument is empty, which requests the device's
    /// default set.
    pub fn is_bootstrap(&self) -> bool {
        self.arg.is_empty()
    }

    /// Returns the selectors in request order.
    pub fn selectors(&self) -> impl Iterator<Item = MetadataSelector> + 'a {
        let (selectors, _) = self.arg.as_chunks::<{ MetadataSelector::SIZE }>();

        selectors
            .iter()
            .map(|&[record_type, stream_id, index]| MetadataSelector {
                mtype: record_type.into(),
                stream_id,
                index,
            })
    }
}

/// The `[type][record length u8][record]` frames of a `dev.metadata` reply.
#[derive(Debug, Clone, Copy)]
pub struct MetadataReply<'a> {
    frames: &'a [u8],
}

impl<'a> MetadataReply<'a> {
    /// Parses the frames of a reply payload. Returns `None` if any frame is
    /// truncated.
    pub fn parse(reply: &'a [u8]) -> Option<Self> {
        let mut remaining = reply;
        while !remaining.is_empty() {
            let (head, rest) = remaining.split_at_checked(METADATA_REPLY_FRAME_HEADER)?;
            remaining = rest.get(usize::from(head[1])..)?;
        }
        Some(Self { frames: reply })
    }
}

impl<'a> Iterator for MetadataReply<'a> {
    type Item = (MetadataType, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let (head, rest) = self.frames.split_at_checked(METADATA_REPLY_FRAME_HEADER)?;
        let (record, remaining) = rest.split_at_checked(usize::from(head[1]))?;
        self.frames = remaining;
        Some((head[0].into(), record))
    }
}

/// A run of consecutive samples from one stream segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Samples<'a> {
    /// Stream id.
    pub stream_id: StreamId,
    /// Segment id.
    pub segment_id: SegmentId,
    /// Sample number of the first sample in `data`.
    pub first: SampleNumber,
    /// Samples packed back to back, each `Stream::sample_size` bytes.
    pub data: &'a [u8],
}

impl<'a> Samples<'a> {
    /// Byte offset of the first sample in a data packet.
    pub const DATA_OFFSET: usize = Header::SIZE + SAMPLE_HEADER_SIZE;

    /// Writes a full sample packet into `buf`. Returns its length, or `None` if
    /// the sample number or samples do not fit.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let total = Self::write_header(
            buf,
            self.stream_id,
            self.segment_id,
            self.first,
            self.data.len(),
        )?;
        buf[Self::DATA_OFFSET..total].copy_from_slice(self.data);
        Some(total)
    }

    /// Writes the headers of a packet carrying `data_len` bytes of samples
    /// already in place at [`Self::DATA_OFFSET`]. Returns the packet length, or
    /// `None` if the sample number or samples do not fit.
    pub fn write_header(
        buf: &mut [u8],
        stream_id: StreamId,
        segment_id: SegmentId,
        first: SampleNumber,
        data_len: usize,
    ) -> Option<usize> {
        if !first.fits_stream_packet() {
            return None;
        }
        let payload_len = SAMPLE_HEADER_SIZE + data_len;
        let total = Header::SIZE + payload_len;
        if payload_len > Packet::MAX_PAYLOAD || buf.len() < total {
            return None;
        }
        Header::new(PacketType::stream(stream_id.value())?, payload_len as u16)
            .write(buf.first_chunk_mut()?);
        buf[Header::SIZE..Header::SIZE + 3].copy_from_slice(&first.to_le_bytes()[..3]);
        buf[Header::SIZE + 3] = segment_id.value();
        Some(total)
    }

    /// Parses a sample packet from its header and payload. Returns `None` if it
    /// is not a sample packet of streams 1 to 127 or the payload is truncated.
    pub fn parse(header: Header, payload: &'a [u8]) -> Option<Self> {
        let stream_id = header.ptype.stream_id()?;
        if stream_id < FIRST_STREAM_ID {
            return None;
        }
        let (head, data) = payload.split_at_checked(SAMPLE_HEADER_SIZE)?;
        Some(Self {
            stream_id: StreamId::try_new(stream_id)?,
            segment_id: SegmentId::new(head[3]),
            first: SampleNumber::new(u32::from_le_bytes([head[0], head[1], head[2], 0])),
            data,
        })
    }
}

/// Writer of a record, a fixed-size head whose first byte is its own length
/// followed by the strings the head measures. Strings are appended in call
/// order and truncated on a character boundary when the buffer fills.
pub(crate) struct RecordWriter<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl<'a> RecordWriter<'a> {
    pub(crate) fn new(buf: &'a mut [u8], head: usize) -> Option<Self> {
        (buf.len() >= head).then_some(Self { buf, len: head })
    }

    /// Appends `value` and returns how many of its bytes fit.
    pub(crate) fn push(&mut self, value: &str) -> u8 {
        let mut len = value
            .len()
            .min(self.buf.len() - self.len)
            .min(u8::MAX as usize);
        while !value.is_char_boundary(len) {
            len -= 1;
        }
        self.buf[self.len..self.len + len].copy_from_slice(&value.as_bytes()[..len]);
        self.len += len;
        len as u8
    }

    /// Lays `head` over the room reserved for it and returns the record length.
    pub(crate) fn finish(self, head: impl IntoBytes + Immutable) -> usize {
        self.buf[..size_of_val(&head)].copy_from_slice(head.as_bytes());
        self.len
    }
}

/// Reader of a record written by [`RecordWriter`]. Strings start at the head
/// length found on the wire. A record with head fields this build does not know
/// still parses.
pub(crate) struct RecordReader<'a> {
    strings: &'a [u8],
}

impl<'a> RecordReader<'a> {
    /// Returns the head this build reads and a reader of the strings after the
    /// head on the wire, which may be longer but never shorter.
    pub(crate) fn split<H: FromBytes + KnownLayout + Immutable>(
        record: &'a [u8],
    ) -> Option<(&'a H, Self)> {
        let (declared, strings) = split_record(record)?;
        let (head, _) = H::ref_from_prefix(declared).ok()?;
        Some((head, Self { strings }))
    }

    /// Returns the head this build reads and a reader of the strings after the
    /// head on the wire. A head shorter than this build's leaves the fields it
    /// does not reach zero, the default the protocol gives a field an older
    /// device does not send. A longer one is read as far as this build knows.
    pub(crate) fn split_defaulting<H: FromBytes + IntoBytes>(
        record: &'a [u8],
    ) -> Option<(H, Self)> {
        let (declared, strings) = split_record(record)?;
        let mut head = H::new_zeroed();
        let known = declared.len().min(size_of::<H>());
        head.as_mut_bytes()[..known].copy_from_slice(&declared[..known]);
        Some((head, Self { strings }))
    }

    /// Skips the next string of `len` bytes. Returns `None` if the record ends
    /// first.
    fn skip(&mut self, len: u8) -> Option<()> {
        self.strings = self.strings.get(len as usize..)?;
        Some(())
    }

    pub(crate) fn take(&mut self, len: u8) -> Option<&'a str> {
        let (value, rest) = self.strings.split_at_checked(len as usize)?;
        self.strings = rest;
        core::str::from_utf8(value).ok()
    }
}

#[cfg(test)]
mod tests {
    //! Properties of metadata records and sample packets.
    //!
    //! # Observations
    //!
    //! The bytes each record writer leaves in its buffer and the length it
    //! returns. What [`Metadata::parse`], the parse of each record kind and
    //! every accessor of each record view read from bytes.
    //!
    //! # Sources
    //!
    //! The module doc. A record is a head whose first byte is its own length,
    //! then the strings whose lengths the head records, and readers take the
    //! strings from the declared head length, which a device may make longer.
    //! The docs of each record's `write` and `parse`, and of
    //! [`Metadata::write`], [`Metadata::write_reply_frame`],
    //! [`Metadata::record_len`] and [`MetadataType`]. The firmware structs
    //! the records mirror, `tl_metadata_device`, `tl_metadata_stream`,
    //! `tl_metadata_segment` and `tl_metadata_column`, field for field and
    //! little endian, and the `METADATA` packet type 11.
    //!
    //! # Model
    //!
    //! [`Fields`] holds one record of any kind with owned strings.
    //! [`Fields::head`] lays its head out byte by byte from the struct, with
    //! no writer and no head type of the code. The heads are these.
    //!
    //! ```text
    //! device  [9, name_len, session u32, serial_len, firmware_len, n_streams]
    //! stream  [9, stream_id, n_columns, n_segments, sample_size u16,
    //!          buf_samples u16, name_len]
    //! segment [27, stream_id, segment_id, flags, epoch, timeref_serial_len,
    //!          timeref_session u32, start_time u32, sampling_rate u32,
    //!          decimation u32, filter_cutoff f32, filter_type]
    //! column  [7, stream_id, index, data_type, name_len, units_len,
    //!          description_len]
    //! ```
    //!
    //! [`Fields::wire`] is the head and then the strings in that order.
    //! [`Fields::older`] is the record an older device sends with a head cut
    //! at a field boundary, every field past the cut zero and every string
    //! the cut head does not measure empty. [`cut_in_turn`] gives the
    //! prefixes of the strings a buffer with a given room keeps.
    //! [`record_strings`] reads the strings of a bare record from its bytes
    //! alone, at the offsets of the length bytes in these heads, or names why
    //! the record is refused. Records are compared with the bits of a segment's
    //! cutoff, so a NaN equals itself.
    //!
    //! # Laws
    //!
    //! - M1, model based layout. A record whose strings are at most 255 bytes
    //!   writes [`Fields::wire`] into a buffer of that length, with its type
    //!   byte, whatever the buffer held before, and [`Metadata::record_len`]
    //!   is that length. Source, the structs. It rules out a stream record
    //!   with `n_columns` and `n_segments` swapped, or `sample_size` and
    //!   `buf_samples`, a big endian word, or a head length byte that is not
    //!   the head's size.
    //! - M2, round trip. The parse of [`Fields::wire`] gives the record back,
    //!   and the view of it reads every field the same, through its own
    //!   accessor, [`fields`](DeviceView::fields) and `Debug`. Source, the
    //!   docs of `parse` and of the views. With M1 it rules out a reader and a
    //!   writer that agree on a wrong offset, and a view that reads a field
    //!   from another's offset.
    //! - M3, model based packet. [`Metadata::write`] gives the header
    //!   `[11, 0, payload u16]`, then `[type, flags]`, then the record, when
    //!   the payload is at most 500 bytes, and refuses otherwise. The payload
    //!   parses back to the record and the flags, and
    //!   [`write_metadata_record`] gives the same packet from the record's
    //!   bytes. [`Metadata::write_reply_frame`] gives `[type, length]` and
    //!   the record when the record is at most 255 bytes, and refuses
    //!   otherwise, and [`MetadataReply`] yields the frame back. A reply of up
    //!   to [`MAX_METADATA_REPLY_SIZE`] frame bytes fits one RPC reply packet.
    //!   Source, the docs of those functions and the `METADATA` header. It
    //!   rules out an off by one at either limit.
    //! - M4, postcondition. Into a buffer that holds the head and some room
    //!   more, each string in turn keeps the longest prefix that ends on a
    //!   character boundary and fits both the room left and 255 bytes. The
    //!   record is the head and those prefixes, it parses with them, and a
    //!   packet carries it the same. A buffer short of the head is refused.
    //!   [`Metadata::record_len`] is at least the length written into a buffer
    //!   with room for every string. Source, the docs of `write` and
    //!   `record_len`. It rules out a cut inside a character, a length byte
    //!   that wraps past 255, a writer that refuses a record it can shorten,
    //!   and a `record_len` that sizes a buffer too small.
    //! - M5, metamorphic. Raising the head length by `k` and putting `k` bytes
    //!   of any value after the head leaves what `parse`, the view and
    //!   [`Metadata::parse`] read unchanged. Source, the module doc. It rules
    //!   out a reader that takes the strings from this build's head size.
    //! - M6, metamorphic. A head cut at a field boundary, at least two bytes
    //!   long, reads as [`Fields::older`], through `parse` and the view. Source,
    //!   the docs of `parse`, fields past a shorter head read as zero. It
    //!   rules out a reader that refuses an older device's head, or reads its
    //!   strings from this build's head size.
    //! - M7, completeness and implicit oracle. [`Metadata::parse`] returns
    //!   for any bytes. A payload under two bytes, or of a record type other
    //!   than 1 to 4, gives none, and [`Metadata::defines`] holds for 1 to 4
    //!   only. Any other gives what the parse of its kind gives for the bare
    //!   record, with the flags byte. The parse of a kind accepts a record
    //!   exactly when [`record_strings`] does from its bytes.
    //!   Its head length byte is at least 2 and within the record, a stream,
    //!   segment or column record has a stream id from 1 to 127, each string
    //!   length the head holds fits the bytes left in turn, and every string
    //!   is UTF-8. The strings it reads are those bytes. A record that parses
    //!   with a whole head reads the same through its view. Source, the docs
    //!   of `parse` and [`split_record`]. It rules out a parse that reads text
    //!   that is not UTF-8 as empty, or a string that runs past the record as
    //!   a shorter one.
    //! - M8, postcondition. Metadata and segment flags are sets of bits.
    //!   `contains`, `union`, `|` and `is_empty` give what the bits give.
    //!   Source, the docs of those methods.
    //!
    //! # Generator obligations
    //!
    //! - Each record kind a quarter of the time.
    //! - Strings from [`text`], empty, one byte, 255 bytes, and runs of a
    //!   multibyte character that end within three bytes of a limit.
    //! - Numbers at zero and all ones, stream ids of 1 and 127, cutoffs that
    //!   are zeros, infinities and NaNs with payloads.
    //! - M3 at a record of 255 and 256 bytes and a payload of 500 and 501.
    //! - M4 with strings of up to 300 bytes and a room at any byte, so cuts
    //!   fall inside characters and at the 255 byte cap.
    //! - M5 with 1 to `255 - head` extra bytes, and M6 at every boundary.
    //! - M7 on any bytes, and on written records with one byte changed to 0,
    //!   1, 2, 127, 128, 255 or any value and cut anywhere, with record
    //!   types 0 to 5 and 255. About a quarter of the payloads parse, and
    //!   each reason [`record_strings`] gives for a refusal is reached. In M4
    //!   about a third of the cases cut a string inside a character.
    //!
    //! `class_counts` prints the share of each class with
    //! `TWINLEAF_PAYLOAD_CLASSES` set.
    //!
    //! # Exclusions
    //!
    //! - A head cut inside a field. `parse` reads the bytes of the field
    //!   that are there and the view reads zero, and the docs do not say
    //!   which is meant. No device sends one.
    //! - How much [`Metadata::record_len`] overcounts when a string over 255
    //!   bytes would be cut inside a character. It counts 255 and the writer
    //!   keeps fewer, so it is checked only as an upper bound.
    //! - The selector and request encodings, covered by the examples.
    //!
    //! # Examples that stay
    //!
    //! The pinned device, stream, column and segment records, each field a
    //! value no other field of the record holds so that swapped fields fail,
    //! the relayed record, the selectors, the reply frames with an unknown
    //! type and a truncated frame, and the sample header. The sample packet
    //! properties have their own docs.

    use super::*;
    use crate::payload::strategy::{
        buffer, bytes, bytes_to, cut_classes, cut_in_turn, float, half, len_to, octet, slack,
        strings_of, tally, text, word, write_over_any,
    };
    use proptest::prelude::*;
    use proptest::sample::Index;
    use proptest::test_runner::TestCaseError;

    const SESSION: SessionId = SessionId::new(0x0A0B_0C0D);

    fn device() -> Device<'static> {
        Device {
            session: SESSION,
            n_streams: 2,
            name: "COMM-USB",
            serial: "twinleaf-37383731",
            firmware: "2026-07-24/a1b2c3-DEV",
        }
    }

    fn segment() -> Segment<'static> {
        Segment {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new(3),
            flags: SegmentFlags::VALID | SegmentFlags::ACTIVE,
            epoch: Epoch::UNIX,
            timeref_serial: "twinleaf-37383731",
            timeref_session: SESSION,
            start_time: 1_774_137_600,
            sampling_rate: 1000,
            decimation: 100,
            filter_cutoff: 0.0,
            filter_type: FilterType::NONE,
        }
    }

    fn round_trip(metadata: Metadata<'_>, flags: MetadataFlags) {
        let mut buf = [0u8; 256];
        let len = metadata.write(flags, &mut buf).unwrap();
        let header = Header::parse((&buf[..Header::SIZE]).try_into().unwrap()).unwrap();
        assert_eq!(header.ptype, PacketType::METADATA);
        assert_eq!(header.packet_len(), len);
        assert_eq!(
            Metadata::parse(&buf[header.payload_range()]),
            Some((metadata, flags))
        );
    }

    #[test]
    fn records_round_trip() {
        round_trip(Metadata::Device(device()), MetadataFlags::PERIODIC);
        round_trip(
            Metadata::Stream(Stream {
                stream_id: StreamId::new(1),
                n_columns: 6,
                n_segments: 2,
                sample_size: 24,
                buf_samples: 128,
                name: "imu",
            }),
            MetadataFlags::PERIODIC | MetadataFlags::LAST,
        );
        round_trip(Metadata::Segment(segment()), MetadataFlags::UPDATE);
        round_trip(
            Metadata::Column(Column {
                stream_id: StreamId::new(1),
                index: ColumnId::new(0),
                data_type: DataType::F32,
                name: "imu.accel.x",
                units: "m/s^2",
                description: "Acceleration (X)",
            }),
            MetadataFlags::default(),
        );
    }

    /// Byte-exact packed layout of a device record, wrapped in a METADATA
    /// packet.
    #[test]
    fn device_record_matches_the_packed_wire_layout() {
        let record = Device {
            session: SESSION,
            n_streams: 5,
            name: "AB",
            serial: "CDE",
            firmware: "F",
        };
        let mut buf = [0u8; 64];
        let flags = MetadataFlags::PERIODIC | MetadataFlags::LAST;
        let len = record_bytes(Metadata::Device(record), flags, &mut buf);
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            11, 0, 17, 0,               // header: METADATA, no routing, payload 17
            1, 0b101,                   // record type DEVICE, PERIODIC | LAST
            9,                          // fixed_len
            2,                          // name_varlen
            0x0D, 0x0C, 0x0B, 0x0A,     // session_id, little-endian
            3, 1,                       // serial_varlen, firmware_varlen
            5,                          // n_streams
            b'A', b'B', b'C', b'D', b'E', b'F',
        ]);
    }

    /// A `tl_metadata_stream` is a 9-byte head with two little-endian u16s,
    /// then the name.
    #[test]
    fn stream_record_matches_the_packed_wire_layout() {
        let record = Stream {
            stream_id: StreamId::new(4),
            n_columns: 6,
            n_segments: 2,
            sample_size: 0x1234,
            buf_samples: 0x5678,
            name: "imu",
        };
        let mut buf = [0u8; 64];
        let len = record_bytes(Metadata::Stream(record), MetadataFlags::UPDATE, &mut buf);
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            11, 0, 14, 0,
            2, 0b010,                   // record type STREAM, UPDATE
            9,                          // fixed_len
            4,                          // stream_id
            6, 2,                       // n_columns, n_segments
            0x34, 0x12,                 // sample_size, little-endian
            0x78, 0x56,                 // buf_samples, little-endian
            3,                          // name_varlen
            b'i', b'm', b'u',
        ]);
    }

    /// A `tl_metadata_column` is a 7-byte head, then name, units, description.
    #[test]
    fn column_record_matches_the_packed_wire_layout() {
        let record = Column {
            stream_id: StreamId::new(6),
            index: ColumnId::new(3),
            data_type: DataType::F32,
            name: "vbus",
            units: "V",
            description: "In",
        };
        let mut buf = [0u8; 64];
        let len = record_bytes(Metadata::Column(record), MetadataFlags::PERIODIC, &mut buf);
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            11, 0, 16, 0,
            4, 0b001,                   // record type COLUMN, PERIODIC
            7, 6, 3, 0x42,              // fixed_len, stream_id, index, FLOAT32
            4, 1, 2,                    // name, units, description lengths
            b'v', b'b', b'u', b's', b'V', b'I', b'n',
        ]);
    }

    /// A `tl_metadata_segment` is a 27-byte head with four little-endian u32s
    /// and an f32 cutoff, then the timeref serial.
    #[test]
    fn segment_record_matches_the_packed_wire_layout() {
        let record = Segment {
            stream_id: StreamId::new(6),
            segment_id: SegmentId::new(9),
            flags: SegmentFlags::VALID | SegmentFlags::HOLDOVER,
            timeref_serial: "XY",
            filter_cutoff: 1.0,
            filter_type: FilterType::IIR_SP_LPF1,
            ..segment()
        };
        let mut buf = [0u8; 64];
        let len = record_bytes(Metadata::Segment(record), MetadataFlags::UPDATE, &mut buf);
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            11, 0, 31, 0,
            3, 0b010,                   // record type SEGMENT, UPDATE
            27,                         // fixed_len
            6, 9,                       // stream_id, segment_id
            0b101,                      // VALID | HOLDOVER
            3,                          // epoch UNIX
            2,                          // time_ref_serial_varlen
            0x0D, 0x0C, 0x0B, 0x0A,     // time_ref_session_id
            0x00, 0x31, 0xBF, 0x69,     // start_time 1774137600
            0xE8, 0x03, 0x00, 0x00,     // sampling_rate 1000
            0x64, 0x00, 0x00, 0x00,     // decimation 100
            0x00, 0x00, 0x80, 0x3F,     // filter_cutoff 1.0f
            1,                          // filter_type IIR_SP_LPF1
            b'X', b'Y',
        ]);
    }

    fn record_bytes(metadata: Metadata<'_>, flags: MetadataFlags, buf: &mut [u8]) -> usize {
        let len = metadata.write(flags, buf).unwrap();
        assert_eq!(
            Metadata::parse(&buf[Header::SIZE..len]),
            Some((metadata, flags))
        );
        len
    }

    /// Relaying a record produces the same packet as writing it from fields.
    #[test]
    fn a_relayed_record_matches_the_packet_it_came_from() {
        for (metadata, kind) in [
            (Metadata::Device(device()), 1),
            (Metadata::Segment(segment()), 3),
        ] {
            let flags = MetadataFlags::UPDATE;
            let mut written = [0u8; 128];
            let len = metadata.write(flags, &mut written).unwrap();
            let (_, flags, record) = split_metadata(&written[Header::SIZE..len]).unwrap();

            let mut relayed = [0u8; 128];
            let relayed_len = write_metadata_record(&mut relayed, kind, flags, record).unwrap();
            assert_eq!(&relayed[..relayed_len], &written[..len]);
        }
    }

    #[test]
    fn head_length_lets_readers_skip_unknown_fields() {
        // A device that appended two fields to the column head: strings still
        // start where the wire says they do.
        #[rustfmt::skip]
        let record = [
            9, 4, 1, 0x42, 4, 1, 2,     // the head this build knows
            0xAA, 0xBB,                 // two fields it does not
            b'v', b'b', b'u', b's', b'V', b'I', b'n',
        ];
        let column = Column::parse(&record).unwrap();
        assert_eq!(column.name, "vbus");
        assert_eq!(column.units, "V");
        assert_eq!(column.description, "In");
    }

    #[test]
    fn parse_rejects_short_and_unknown_records() {
        assert_eq!(Metadata::parse(&[]), None);
        assert_eq!(Metadata::parse(&[1]), None);
        // Record type 5 is not defined; its layout is unknown.
        assert_eq!(Metadata::parse(&[5, 0, 7, 0, 0, 0, 0, 0, 0]), None);
        // A head under two bytes holds no field.
        assert_eq!(Column::parse(&[1, 0]), None);
        // A head that reaches the stream id must name a valid one.
        assert_eq!(Column::parse(&[6, 0, 0, 0, 0, 0]), None);
        // name_varlen runs past the end of the record.
        assert_eq!(Column::parse(&[7, 0, 0, 0, 9, 0, 0, b'x']), None);
    }

    fn written(metadata: Metadata<'_>) -> Vec<u8> {
        let mut buf = vec![0; metadata.record_len()];
        let (_, len) = metadata.write_record(&mut buf).unwrap();
        buf.truncate(len);
        buf
    }

    fn column() -> Column<'static> {
        Column {
            stream_id: StreamId::new(1),
            index: ColumnId::new(2),
            data_type: DataType::F32,
            name: "field",
            units: "nT",
            description: "Total field",
        }
    }

    /// Every view accessor reads what `parse` reads.
    #[test]
    fn views_read_what_parse_reads() {
        let record = written(Metadata::Device(device()));
        let view = DeviceView::new_checked(&record).unwrap();
        assert_eq!(view.fields(), device());
        assert_eq!(
            (
                view.session(),
                view.n_streams(),
                view.name(),
                view.serial(),
                view.firmware()
            ),
            (
                SESSION,
                2,
                "COMM-USB",
                "twinleaf-37383731",
                "2026-07-24/a1b2c3-DEV"
            )
        );

        let stream = Stream {
            stream_id: StreamId::new(1),
            n_columns: 6,
            n_segments: 2,
            sample_size: 24,
            buf_samples: 128,
            name: "imu",
        };
        let record = written(Metadata::Stream(stream));
        let view = StreamView::new_checked(&record).unwrap();
        assert_eq!(view.fields(), stream);
        assert_eq!(
            (
                view.stream_id(),
                view.n_columns(),
                view.n_segments(),
                view.sample_size(),
                view.buf_samples(),
                view.name()
            ),
            (StreamId::new(1), 6, 2, 24, 128, "imu")
        );

        let record = written(Metadata::Segment(segment()));
        let view = SegmentView::new_checked(&record).unwrap();
        assert_eq!(view.fields(), segment());
        assert_eq!(view.segment_id(), SegmentId::new(3));
        assert_eq!(view.flags(), SegmentFlags::VALID | SegmentFlags::ACTIVE);
        assert_eq!(view.epoch(), Epoch::UNIX);
        assert_eq!(view.timeref_serial(), "twinleaf-37383731");
        assert_eq!(view.timeref_session(), SESSION);
        assert_eq!(
            (view.start_time(), view.sampling_rate(), view.decimation()),
            (1_774_137_600, 1000, 100)
        );
        assert_eq!(view.filter_cutoff(), 0.0);
        assert_eq!(view.filter_type(), FilterType::NONE);

        let record = written(Metadata::Column(column()));
        let view = ColumnView::new_checked(&record).unwrap();
        assert_eq!(view.fields(), column());
        assert_eq!(
            (
                view.stream_id(),
                view.index(),
                view.data_type(),
                view.name(),
                view.units(),
                view.description()
            ),
            (
                StreamId::new(1),
                ColumnId::new(2),
                DataType::F32,
                "field",
                "nT",
                "Total field"
            )
        );
    }

    /// An older device's shorter head leaves out its last fields, which read
    /// as zero, and the strings start where its head ends.
    #[test]
    fn fields_past_a_shorter_head_read_as_zero() {
        // A column head that stops before `units_len` and `description_len`.
        let record = [5, 1, 2, DataType::F32.value(), 1, b'x'];
        let view = ColumnView::new_checked(&record).unwrap();
        assert_eq!(view.data_type(), DataType::F32);
        assert_eq!(
            (view.name(), view.units(), view.description()),
            ("x", "", "")
        );
        assert_eq!(
            Column::parse(&record),
            Some(Column {
                stream_id: StreamId::new(1),
                index: ColumnId::new(2),
                data_type: DataType::F32,
                name: "x",
                units: "",
                description: "",
            })
        );
    }

    /// A newer device's longer head is read as far as this build knows. Its
    /// strings start after the whole head, and the extra bytes stay in the
    /// record, so two records that differ only there are unequal.
    #[test]
    fn a_longer_head_is_skipped_and_kept() {
        let record = written(Metadata::Column(column()));
        let (head, strings) = split_record(&record).unwrap();
        let mut longer = vec![head[0] + 2];
        longer.extend_from_slice(&head[1..]);
        longer.extend_from_slice(&[0xAB, 0xCD]);
        longer.extend_from_slice(strings);

        let view = ColumnView::new_checked(&longer).unwrap();
        assert_eq!(view.fields(), column());
        assert_eq!(view.description(), "Total field");

        let mut changed = longer.clone();
        changed[Column::HEAD_SIZE] = 0xEE;
        assert_ne!(view, ColumnView::new_checked(&changed).unwrap());
    }

    #[test]
    fn record_len_sizes_a_buffer_that_does_not_truncate() {
        let metadata = Metadata::Device(device());
        let mut buf = vec![0; metadata.record_len()];
        let (kind, len) = metadata.write_record(&mut buf).unwrap();
        assert_eq!((kind, len), (1, metadata.record_len()));
        assert_eq!(Device::parse(&buf[..len]), Some(device()));
        let (head, strings) = split_record(&buf[..len]).unwrap();
        assert_eq!(head.len(), Device::HEAD_SIZE);
        assert_eq!(strings.len(), len - Device::HEAD_SIZE);
    }

    #[test]
    fn split_record_rejects_a_head_it_cannot_trust() {
        assert_eq!(split_record(&[]), None);
        // A head of one byte holds nothing but its own length.
        assert_eq!(split_record(&[1, 0]), None);
        // The declared head runs past the record.
        assert_eq!(split_record(&[7, 0, 0]), None);
        assert_eq!(split_record(&[2, 0, b'x']), Some((&[2, 0][..], &b"x"[..])));
    }

    #[test]
    fn strings_truncate_on_a_character_boundary() {
        let record = Column {
            stream_id: StreamId::new(1),
            index: ColumnId::new(0),
            data_type: DataType::F32,
            name: "bar.therm",
            units: "°C",
            description: "",
        };
        // Room for the packet header, the metadata header, the 7-byte head,
        // "bar.therm", and one byte of the two-byte degree sign.
        let mut buf = [0u8; Header::SIZE + METADATA_HEADER_SIZE + 7 + 9 + 1];
        let len = record
            .write(&mut buf[Header::SIZE + METADATA_HEADER_SIZE..])
            .unwrap();
        let parsed = Column::parse(&buf[Header::SIZE + METADATA_HEADER_SIZE..][..len]).unwrap();
        assert_eq!(parsed.name, "bar.therm");
        assert_eq!(parsed.units, "", "split a multi-byte character");
    }

    #[test]
    fn write_rejects_a_buffer_too_small_for_the_head() {
        let mut buf = [0u8; Header::SIZE + METADATA_HEADER_SIZE + 6];
        assert_eq!(
            Metadata::Device(device()).write(MetadataFlags::PERIODIC, &mut buf),
            None
        );
    }

    #[test]
    fn selectors_encode_as_packed_triples() {
        assert_eq!(MetadataSelector::device().encode(), [1, 0, 0]);
        assert_eq!(MetadataSelector::stream(7).encode(), [2, 7, 0]);
        assert_eq!(MetadataSelector::segment(7, 3).encode(), [3, 7, 3]);
        assert_eq!(
            MetadataSelector::segment(7, CURRENT_SEGMENT).encode(),
            [3, 7, 0xff]
        );
        assert_eq!(MetadataSelector::column(2, 5).encode(), [4, 2, 5]);
    }

    #[test]
    fn a_query_round_trips_its_selectors() {
        let selectors = [
            MetadataSelector::device(),
            MetadataSelector::stream(1),
            MetadataSelector::segment(1, CURRENT_SEGMENT),
            MetadataSelector::column(1, 2),
        ];
        let arg: Vec<u8> = selectors.iter().flat_map(|s| s.encode()).collect();
        let request = MetadataRequest::parse(&arg).unwrap();
        assert!(!request.is_bootstrap());
        assert_eq!(request.selectors().collect::<Vec<_>>(), selectors);
    }

    #[test]
    fn the_empty_request_is_the_bootstrap_and_selects_nothing_explicitly() {
        let request = MetadataRequest::parse(&[]).unwrap();
        assert!(request.is_bootstrap());
        assert_eq!(request.selectors().count(), 0);
    }

    #[test]
    fn a_query_rejects_misaligned_or_oversized_arguments() {
        assert_eq!(
            MetadataRequest::parse(&[1, 0]).unwrap_err(),
            MetadataRequestError::Misaligned
        );
        let oversized = [0u8; (MAX_METADATA_SELECTORS + 1) * MetadataSelector::SIZE];
        assert_eq!(
            MetadataRequest::parse(&oversized).unwrap_err(),
            MetadataRequestError::TooManySelectors
        );
        let full = [0u8; MAX_METADATA_SELECTORS * MetadataSelector::SIZE];
        assert!(MetadataRequest::parse(&full).is_ok());
    }

    #[test]
    fn a_reply_frame_is_the_type_the_length_and_the_bare_record() {
        let record = Column {
            stream_id: StreamId::new(6),
            index: ColumnId::new(3),
            data_type: DataType::F32,
            name: "vbus",
            units: "V",
            description: "In",
        };
        let mut buf = [0u8; 64];
        let len = Metadata::Column(record)
            .write_reply_frame(&mut buf)
            .unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            4, 14,                      // record type COLUMN, record length
            7, 6, 3, 0x42,              // fixed_len, stream_id, index, FLOAT32
            4, 1, 2,                    // name, units, description lengths
            b'v', b'b', b'u', b's', b'V', b'I', b'n',
        ]);

        let mut frames = MetadataReply::parse(&buf[..len]).unwrap();
        let (kind, bare) = frames.next().unwrap();
        assert_eq!(kind, MetadataType::Column);
        assert_eq!(Column::parse(bare), Some(record));
        assert_eq!(frames.next(), None);
    }

    #[test]
    fn a_reply_iterates_every_frame_including_unknown_types() {
        let mut reply = Vec::new();
        let mut buf = [0u8; 256];
        let len = Metadata::Device(device())
            .write_reply_frame(&mut buf)
            .unwrap();
        reply.extend_from_slice(&buf[..len]);
        reply.extend_from_slice(&[9, 2, 0xAA, 0xBB]); // unknown record type 9
        let len = Metadata::Segment(segment())
            .write_reply_frame(&mut buf)
            .unwrap();
        reply.extend_from_slice(&buf[..len]);

        let frames: Vec<_> = MetadataReply::parse(&reply).unwrap().collect();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].0, MetadataType::Device);
        assert_eq!(Device::parse(frames[0].1), Some(device()));
        assert_eq!(frames[1], (MetadataType::Unknown(9), &[0xAA, 0xBB][..]));
        assert_eq!(frames[2].0, MetadataType::Segment);
        assert_eq!(Segment::parse(frames[2].1), Some(segment()));
    }

    #[test]
    fn a_truncated_reply_fails_validation_instead_of_yielding_a_prefix() {
        let mut reply = Vec::new();
        let mut buf = [0u8; 256];
        let len = Metadata::Device(device())
            .write_reply_frame(&mut buf)
            .unwrap();
        reply.extend_from_slice(&buf[..len]);
        reply.extend_from_slice(&[4, 10, 0]); // frame promising more than remains

        assert!(MetadataReply::parse(&reply).is_none());
        assert!(MetadataReply::parse(&[3]).is_none());
        assert!(MetadataReply::parse(&[]).is_some());
    }

    #[test]
    fn a_record_too_long_for_a_frame_length_byte_is_refused() {
        let long = "n".repeat(120);
        let record = Metadata::Device(Device {
            session: SESSION,
            n_streams: 1,
            name: &long,
            serial: &long,
            firmware: &long,
        });
        let mut buf = [0u8; 512];
        assert!(record.record_len() > usize::from(u8::MAX));
        assert_eq!(record.write_reply_frame(&mut buf), None);
    }

    #[test]
    fn samples_round_trip_with_a_24_bit_sample_number() {
        let samples = Samples {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new(7),
            first: SampleNumber::new(0x00AB_CDEF),
            data: &[1, 2, 3, 4, 5, 6, 7, 8],
        };
        let mut buf = [0u8; 64];
        let len = samples.write(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..Header::SIZE + SAMPLE_HEADER_SIZE], &[
            129, 0, 12, 0,              // header: STREAM0 + 1, payload 12
            0xEF, 0xCD, 0xAB,           // first sample number, 24-bit LE
            7,                          // segment_id
        ]);
        let header = Header::parse((&buf[..Header::SIZE]).try_into().unwrap()).unwrap();
        assert_eq!(header.packet_len(), len);
        assert_eq!(
            Samples::parse(header, &buf[header.payload_range()]),
            Some(samples)
        );
    }

    /// Stream 0 is reserved and sample packets carry streams 1 to 127.
    #[test]
    fn stream_ids_run_from_1_to_127() {
        for value in 0..=u8::MAX {
            let in_range = (1..=127).contains(&value);
            assert_eq!(StreamId::try_new(value).is_some(), in_range, "{value}");
        }
    }

    /// The largest sample payload, after the head.
    const MAX_SAMPLE_DATA: usize = Packet::MAX_PAYLOAD - SAMPLE_HEADER_SIZE;

    /// A stream id byte of 1, 127, or any between.
    fn stream_byte() -> impl Strategy<Value = u8> {
        prop_oneof![Just(1), Just(127), 1..=127u8]
    }

    /// A stream id of 1, 127, or any between.
    fn stream_id() -> impl Strategy<Value = StreamId> {
        stream_byte().prop_map(StreamId::new)
    }

    /// A segment id of 0, 255, or any.
    fn segment_id() -> impl Strategy<Value = SegmentId> {
        prop_oneof![Just(0), Just(u8::MAX), any::<u8>()].prop_map(SegmentId::new)
    }

    proptest! {
        /// Samples whose first number fits 24 bits and whose data fits a
        /// packet write a sample packet of exactly its length for their
        /// stream, whose payload parses back to them. Writing the headers in
        /// front of data already in place gives the same packet. Both are the
        /// same whatever the buffer held before.
        #[test]
        fn samples_decode_as_they_were_encoded(
            stream_id in stream_id(),
            segment_id in segment_id(),
            first in prop_oneof![Just(0), Just(MAX_SAMPLE_NUMBER), 0..=MAX_SAMPLE_NUMBER],
            data in bytes_to(MAX_SAMPLE_DATA),
        ) {
            let samples = Samples { stream_id, segment_id, first: SampleNumber::new(first), data: &data };
            let payload_len = SAMPLE_HEADER_SIZE + data.len();
            let (len, buf) = write_over_any(Header::SIZE + payload_len, |buf| samples.write(buf))?;
            prop_assert_eq!(len, Some(buf.len()));
            let header = Header::parse(buf[..Header::SIZE].try_into().unwrap()).unwrap();
            let ptype = PacketType::new(128 + stream_id.value());
            prop_assert_eq!(header, Header::new(ptype, payload_len as u16));
            prop_assert_eq!(Samples::parse(header, &buf[Header::SIZE..]), Some(samples));

            let (len, in_place) = write_over_any(buf.len(), |in_place| {
                in_place[Samples::DATA_OFFSET..].copy_from_slice(&data);
                Samples::write_header(in_place, stream_id, segment_id, samples.first, data.len())
            })?;
            prop_assert_eq!(len, Some(buf.len()));
            prop_assert_eq!(in_place, buf);
        }

        /// Samples are written when the first number fits 24 bits, the
        /// payload is at most [`Packet::MAX_PAYLOAD`] bytes, and the buffer
        /// holds the packet, and refused otherwise.
        #[test]
        fn samples_are_written_only_when_they_fit(
            first in prop_oneof![
                Just(MAX_SAMPLE_NUMBER),
                Just(MAX_SAMPLE_NUMBER + 1),
                Just(u32::MAX),
                any::<u32>()
            ],
            data_len in len_to(MAX_SAMPLE_DATA + 1),
            slack in slack(),
        ) {
            let data = vec![0u8; data_len];
            let samples = Samples {
                stream_id: StreamId::new(1),
                segment_id: SegmentId::new(0),
                first: SampleNumber::new(first),
                data: &data,
            };
            let needed = Samples::DATA_OFFSET + data_len;
            let mut buf = buffer(needed, slack);
            let fits = first <= MAX_SAMPLE_NUMBER
                && data_len <= MAX_SAMPLE_DATA
                && buf.len() >= needed;
            prop_assert_eq!(samples.write(&mut buf), fits.then_some(needed));
            prop_assert_eq!(
                Samples::write_header(&mut buf, samples.stream_id, samples.segment_id, samples.first, data_len),
                fits.then_some(needed)
            );
        }

        /// A payload parses as samples when its packet type carries a stream
        /// from 1 to 127 and it holds the four-byte head. Writing what was
        /// parsed gives back the same packet, whatever the buffer held before.
        #[test]
        fn any_payload_parses_as_samples_when_it_holds_the_head(
            ptype in prop_oneof![
                Just(128),
                Just(129),
                Just(u8::MAX),
                any::<u8>(),
            ]
            .prop_filter_map("a packet type", PacketType::try_new),
            payload in prop_oneof![
                (0..=SAMPLE_HEADER_SIZE + 1).prop_flat_map(bytes),
                bytes_to(Packet::MAX_PAYLOAD),
            ],
        ) {
            let header = Header::new(ptype, payload.len() as u16);
            let Some(samples) = Samples::parse(header, &payload) else {
                prop_assert!(ptype.value() <= 128 || payload.len() < SAMPLE_HEADER_SIZE);
                return Ok(());
            };
            prop_assert!(ptype.value() > 128);
            let (len, buf) = write_over_any(Header::SIZE + payload.len(), |buf| samples.write(buf))?;
            prop_assert_eq!(len, Some(buf.len()));
            let mut head = [0u8; Header::SIZE];
            header.write(&mut head);
            prop_assert_eq!(&buf[..Header::SIZE], &head[..]);
            prop_assert_eq!(&buf[Header::SIZE..], &payload[..]);
        }
    }

    /// The longest string a length byte measures.
    const MAX_STRING: usize = u8::MAX as usize;

    /// One metadata record of any kind, with owned strings.
    #[derive(Debug, Clone)]
    enum Fields {
        Device {
            session: u32,
            n_streams: u8,
            name: String,
            serial: String,
            firmware: String,
        },
        Stream {
            stream_id: u8,
            n_columns: u8,
            n_segments: u8,
            sample_size: u16,
            buf_samples: u16,
            name: String,
        },
        Segment {
            stream_id: u8,
            segment_id: u8,
            flags: u8,
            epoch: u8,
            timeref_serial: String,
            timeref_session: u32,
            start_time: u32,
            sampling_rate: u32,
            decimation: u32,
            filter_cutoff: f32,
            filter_type: u8,
        },
        Column {
            stream_id: u8,
            index: u8,
            data_type: u8,
            name: String,
            units: String,
            description: String,
        },
    }

    /// `value` when its field ends within a head of `end` bytes, and zero
    /// or empty otherwise.
    fn kept<T: Default>(value: T, field_end: usize, end: usize) -> T {
        if field_end <= end {
            value
        } else {
            T::default()
        }
    }

    impl Fields {
        /// The record type byte.
        fn kind(&self) -> u8 {
            match self {
                Self::Device { .. } => 1,
                Self::Stream { .. } => 2,
                Self::Segment { .. } => 3,
                Self::Column { .. } => 4,
            }
        }

        /// The record type a reply frame names.
        fn mtype(&self) -> MetadataType {
            match self {
                Self::Device { .. } => MetadataType::Device,
                Self::Stream { .. } => MetadataType::Stream,
                Self::Segment { .. } => MetadataType::Segment,
                Self::Column { .. } => MetadataType::Column,
            }
        }

        /// The size of the head the firmware struct lays out.
        fn head_size(&self) -> usize {
            match self {
                Self::Device { .. } | Self::Stream { .. } => 9,
                Self::Segment { .. } => 27,
                Self::Column { .. } => 7,
            }
        }

        /// Where each head field after the length byte ends, short of the
        /// whole head.
        fn shorter_ends(&self) -> Vec<usize> {
            match self {
                Self::Device { .. } => vec![2, 6, 7, 8],
                Self::Stream { .. } => vec![2, 3, 4, 6, 8],
                Self::Segment { .. } => vec![2, 3, 4, 5, 6, 10, 14, 18, 22, 26],
                Self::Column { .. } => vec![2, 3, 4, 5, 6],
            }
        }

        /// The strings in the order the record carries them.
        fn strings(&self) -> Vec<&str> {
            match self {
                Self::Device {
                    name,
                    serial,
                    firmware,
                    ..
                } => vec![name, serial, firmware],
                Self::Stream { name, .. } => vec![name],
                Self::Segment { timeref_serial, .. } => vec![timeref_serial],
                Self::Column {
                    name,
                    units,
                    description,
                    ..
                } => vec![name, units, description],
            }
        }

        /// The same record with its strings replaced, in their order.
        fn with_strings(&self, strings: &[&str]) -> Self {
            let mut record = self.clone();
            let slots: Vec<&mut String> = match &mut record {
                Self::Device {
                    name,
                    serial,
                    firmware,
                    ..
                } => vec![name, serial, firmware],
                Self::Stream { name, .. } => vec![name],
                Self::Segment { timeref_serial, .. } => vec![timeref_serial],
                Self::Column {
                    name,
                    units,
                    description,
                    ..
                } => vec![name, units, description],
            };
            for (slot, text) in slots.into_iter().zip(strings) {
                *slot = (*text).to_owned();
            }
            record
        }

        /// The head, byte by byte as the firmware struct lays it out.
        fn head(&self) -> Vec<u8> {
            let len = |text: &String| text.len() as u8;
            match self {
                Self::Device {
                    session,
                    n_streams,
                    name,
                    serial,
                    firmware,
                } => [
                    &[9, len(name)][..],
                    &session.to_le_bytes(),
                    &[len(serial), len(firmware), *n_streams],
                ]
                .concat(),
                Self::Stream {
                    stream_id,
                    n_columns,
                    n_segments,
                    sample_size,
                    buf_samples,
                    name,
                } => [
                    &[9, *stream_id, *n_columns, *n_segments][..],
                    &sample_size.to_le_bytes(),
                    &buf_samples.to_le_bytes(),
                    &[len(name)],
                ]
                .concat(),
                Self::Segment {
                    stream_id,
                    segment_id,
                    flags,
                    epoch,
                    timeref_serial,
                    timeref_session,
                    start_time,
                    sampling_rate,
                    decimation,
                    filter_cutoff,
                    filter_type,
                } => [
                    &[
                        27,
                        *stream_id,
                        *segment_id,
                        *flags,
                        *epoch,
                        len(timeref_serial),
                    ][..],
                    &timeref_session.to_le_bytes(),
                    &start_time.to_le_bytes(),
                    &sampling_rate.to_le_bytes(),
                    &decimation.to_le_bytes(),
                    &filter_cutoff.to_bits().to_le_bytes(),
                    &[*filter_type],
                ]
                .concat(),
                Self::Column {
                    stream_id,
                    index,
                    data_type,
                    name,
                    units,
                    description,
                } => vec![
                    7,
                    *stream_id,
                    *index,
                    *data_type,
                    len(name),
                    len(units),
                    len(description),
                ],
            }
        }

        /// The record, its head then its strings.
        fn wire(&self) -> Vec<u8> {
            let mut record = self.head();
            for text in self.strings() {
                record.extend_from_slice(text.as_bytes());
            }
            record
        }

        /// The record an older device sends with a head of `end` bytes, every
        /// field past `end` zero and every string it does not measure empty.
        fn older(&self, end: usize) -> Self {
            match self.clone() {
                Self::Device {
                    session,
                    n_streams,
                    name,
                    serial,
                    firmware,
                } => Self::Device {
                    name: kept(name, 2, end),
                    session: kept(session, 6, end),
                    serial: kept(serial, 7, end),
                    firmware: kept(firmware, 8, end),
                    n_streams: kept(n_streams, 9, end),
                },
                Self::Stream {
                    stream_id,
                    n_columns,
                    n_segments,
                    sample_size,
                    buf_samples,
                    name,
                } => Self::Stream {
                    stream_id,
                    n_columns: kept(n_columns, 3, end),
                    n_segments: kept(n_segments, 4, end),
                    sample_size: kept(sample_size, 6, end),
                    buf_samples: kept(buf_samples, 8, end),
                    name: kept(name, 9, end),
                },
                Self::Segment {
                    stream_id,
                    segment_id,
                    flags,
                    epoch,
                    timeref_serial,
                    timeref_session,
                    start_time,
                    sampling_rate,
                    decimation,
                    filter_cutoff,
                    filter_type,
                } => Self::Segment {
                    stream_id,
                    segment_id: kept(segment_id, 3, end),
                    flags: kept(flags, 4, end),
                    epoch: kept(epoch, 5, end),
                    timeref_serial: kept(timeref_serial, 6, end),
                    timeref_session: kept(timeref_session, 10, end),
                    start_time: kept(start_time, 14, end),
                    sampling_rate: kept(sampling_rate, 18, end),
                    decimation: kept(decimation, 22, end),
                    filter_cutoff: kept(filter_cutoff, 26, end),
                    filter_type: kept(filter_type, 27, end),
                },
                Self::Column {
                    stream_id,
                    index,
                    data_type,
                    name,
                    units,
                    description,
                } => Self::Column {
                    stream_id,
                    index: kept(index, 3, end),
                    data_type: kept(data_type, 4, end),
                    name: kept(name, 5, end),
                    units: kept(units, 6, end),
                    description: kept(description, 7, end),
                },
            }
        }

        /// The fields of a parsed record.
        fn of(metadata: Metadata<'_>) -> Self {
            match metadata {
                Metadata::Device(record) => Self::Device {
                    session: record.session.value(),
                    n_streams: record.n_streams,
                    name: record.name.to_owned(),
                    serial: record.serial.to_owned(),
                    firmware: record.firmware.to_owned(),
                },
                Metadata::Stream(record) => Self::Stream {
                    stream_id: record.stream_id.value(),
                    n_columns: record.n_columns,
                    n_segments: record.n_segments,
                    sample_size: record.sample_size,
                    buf_samples: record.buf_samples,
                    name: record.name.to_owned(),
                },
                Metadata::Segment(record) => Self::Segment {
                    stream_id: record.stream_id.value(),
                    segment_id: record.segment_id.value(),
                    flags: record.flags.bits(),
                    epoch: record.epoch.value(),
                    timeref_serial: record.timeref_serial.to_owned(),
                    timeref_session: record.timeref_session.value(),
                    start_time: record.start_time,
                    sampling_rate: record.sampling_rate,
                    decimation: record.decimation,
                    filter_cutoff: record.filter_cutoff,
                    filter_type: record.filter_type.value(),
                },
                Metadata::Column(record) => Self::Column {
                    stream_id: record.stream_id.value(),
                    index: record.index.value(),
                    data_type: record.data_type.value(),
                    name: record.name.to_owned(),
                    units: record.units.to_owned(),
                    description: record.description.to_owned(),
                },
            }
        }

        /// The record the code writes.
        fn metadata(&self) -> Metadata<'_> {
            match self {
                Self::Device {
                    session,
                    n_streams,
                    name,
                    serial,
                    firmware,
                } => Metadata::Device(Device {
                    session: SessionId::new(*session),
                    n_streams: *n_streams,
                    name,
                    serial,
                    firmware,
                }),
                Self::Stream {
                    stream_id,
                    n_columns,
                    n_segments,
                    sample_size,
                    buf_samples,
                    name,
                } => Metadata::Stream(Stream {
                    stream_id: StreamId::new(*stream_id),
                    n_columns: *n_columns,
                    n_segments: *n_segments,
                    sample_size: *sample_size,
                    buf_samples: *buf_samples,
                    name,
                }),
                Self::Segment {
                    stream_id,
                    segment_id,
                    flags,
                    epoch,
                    timeref_serial,
                    timeref_session,
                    start_time,
                    sampling_rate,
                    decimation,
                    filter_cutoff,
                    filter_type,
                } => Metadata::Segment(Segment {
                    stream_id: StreamId::new(*stream_id),
                    segment_id: SegmentId::new(*segment_id),
                    flags: SegmentFlags::from_bits(*flags),
                    epoch: Epoch::new(*epoch),
                    timeref_serial,
                    timeref_session: SessionId::new(*timeref_session),
                    start_time: *start_time,
                    sampling_rate: *sampling_rate,
                    decimation: *decimation,
                    filter_cutoff: *filter_cutoff,
                    filter_type: FilterType::new(*filter_type),
                }),
                Self::Column {
                    stream_id,
                    index,
                    data_type,
                    name,
                    units,
                    description,
                } => Metadata::Column(Column {
                    stream_id: StreamId::new(*stream_id),
                    index: ColumnId::new(*index),
                    data_type: DataType::new(*data_type),
                    name,
                    units,
                    description,
                }),
            }
        }
    }

    /// A record with a segment's cutoff set aside as its bits, so that a NaN
    /// cutoff equals itself.
    fn by_bits(metadata: Metadata<'_>) -> (Metadata<'_>, u32) {
        match metadata {
            Metadata::Segment(segment) => (
                Metadata::Segment(Segment {
                    filter_cutoff: 0.0,
                    ..segment
                }),
                segment.filter_cutoff.to_bits(),
            ),
            other => (other, 0),
        }
    }

    /// Where the head of record type `kind` holds the length of each string,
    /// in the order the strings follow the head.
    fn length_offsets(kind: u8) -> &'static [usize] {
        match kind {
            1 => &[1, 6, 7],
            2 => &[8],
            3 => &[5],
            4 => &[4, 5, 6],
            _ => &[],
        }
    }

    /// The strings of the bare record `record` of type `kind`, read from its
    /// bytes alone, or why it is refused. A stream, segment or column record
    /// whose head holds a stream id outside 1 to 127 is refused.
    fn record_strings(kind: u8, record: &[u8]) -> Result<Vec<&str>, &'static str> {
        let head = record.first().map_or(0, |&head| usize::from(head));
        if (2..=record.len()).contains(&head) && kind != 1 && !(1..=127).contains(&record[1]) {
            return Err("refused, stream id");
        }
        strings_of(record, 2, length_offsets(kind))
    }

    /// Writes the bare record of `fields` over a buffer of `len` bytes, as
    /// [`write_over_any`] does, and fails the case unless the type byte is
    /// the record's. Returns the length and the zeroed buffer.
    fn write_record_over_any(
        fields: &Fields,
        len: usize,
    ) -> Result<(Option<usize>, Vec<u8>), TestCaseError> {
        let metadata = fields.metadata();
        write_over_any(len, |buf| {
            let (kind, len) = metadata.write_record(buf)?;
            (kind == fields.kind()).then_some(len)
        })
    }

    /// Fails the case unless the parse of `record` as its kind, and every
    /// accessor of its view, read `fields`.
    fn reads_as(record: &[u8], fields: &Fields) -> Result<(), TestCaseError> {
        let expected = fields.metadata();
        let (parsed, viewed) = match expected {
            Metadata::Device(device) => {
                let view = DeviceView::new_checked(record);
                prop_assert!(view.is_some(), "the view refused the record");
                let view = view.unwrap();
                prop_assert_eq!(
                    (
                        view.session(),
                        view.n_streams(),
                        view.name(),
                        view.serial(),
                        view.firmware()
                    ),
                    (
                        device.session,
                        device.n_streams,
                        device.name,
                        device.serial,
                        device.firmware
                    )
                );
                prop_assert_eq!(format!("{view:?}"), format!("{device:?}"));
                prop_assert_eq!(view.bytes(), record);
                (
                    Device::parse(record).map(Metadata::Device),
                    Metadata::Device(view.fields()),
                )
            }
            Metadata::Stream(stream) => {
                let view = StreamView::new_checked(record);
                prop_assert!(view.is_some(), "the view refused the record");
                let view = view.unwrap();
                prop_assert_eq!(
                    (
                        view.stream_id(),
                        view.n_columns(),
                        view.n_segments(),
                        view.sample_size(),
                        view.buf_samples(),
                        view.name()
                    ),
                    (
                        stream.stream_id,
                        stream.n_columns,
                        stream.n_segments,
                        stream.sample_size,
                        stream.buf_samples,
                        stream.name
                    )
                );
                prop_assert_eq!(format!("{view:?}"), format!("{stream:?}"));
                prop_assert_eq!(view.bytes(), record);
                (
                    Stream::parse(record).map(Metadata::Stream),
                    Metadata::Stream(view.fields()),
                )
            }
            Metadata::Segment(segment) => {
                let view = SegmentView::new_checked(record);
                prop_assert!(view.is_some(), "the view refused the record");
                let view = view.unwrap();
                prop_assert_eq!(
                    (
                        view.stream_id(),
                        view.segment_id(),
                        view.flags(),
                        view.epoch(),
                        view.timeref_serial(),
                        view.timeref_session()
                    ),
                    (
                        segment.stream_id,
                        segment.segment_id,
                        segment.flags,
                        segment.epoch,
                        segment.timeref_serial,
                        segment.timeref_session
                    )
                );
                prop_assert_eq!(
                    (
                        view.start_time(),
                        view.sampling_rate(),
                        view.decimation(),
                        view.filter_cutoff().to_bits(),
                        view.filter_type()
                    ),
                    (
                        segment.start_time,
                        segment.sampling_rate,
                        segment.decimation,
                        segment.filter_cutoff.to_bits(),
                        segment.filter_type
                    )
                );
                prop_assert_eq!(format!("{view:?}"), format!("{segment:?}"));
                prop_assert_eq!(view.bytes(), record);
                (
                    Segment::parse(record).map(Metadata::Segment),
                    Metadata::Segment(view.fields()),
                )
            }
            Metadata::Column(column) => {
                let view = ColumnView::new_checked(record);
                prop_assert!(view.is_some(), "the view refused the record");
                let view = view.unwrap();
                prop_assert_eq!(
                    (
                        view.stream_id(),
                        view.index(),
                        view.data_type(),
                        view.name(),
                        view.units(),
                        view.description()
                    ),
                    (
                        column.stream_id,
                        column.index,
                        column.data_type,
                        column.name,
                        column.units,
                        column.description
                    )
                );
                prop_assert_eq!(format!("{view:?}"), format!("{column:?}"));
                prop_assert_eq!(view.bytes(), record);
                (
                    Column::parse(record).map(Metadata::Column),
                    Metadata::Column(view.fields()),
                )
            }
        };
        prop_assert_eq!(parsed.map(by_bits), Some(by_bits(expected)));
        prop_assert_eq!(by_bits(viewed), by_bits(expected));
        Ok(())
    }

    /// A record of each kind, a quarter each, with numbers from [`octet`],
    /// [`half`], [`word`] and [`float`], stream ids from [`stream_byte`], and
    /// strings from [`text`] of up to `max` bytes.
    fn fields(max: usize) -> BoxedStrategy<Fields> {
        prop_oneof![
            (word(), octet(), text(max), text(max), text(max))
                .prop_map(
                    |(session, n_streams, name, serial, firmware)| Fields::Device {
                        session,
                        n_streams,
                        name,
                        serial,
                        firmware,
                    }
                )
                .boxed(),
            (stream_byte(), octet(), octet(), half(), half(), text(max))
                .prop_map(
                    |(stream_id, n_columns, n_segments, sample_size, buf_samples, name)| {
                        Fields::Stream {
                            stream_id,
                            n_columns,
                            n_segments,
                            sample_size,
                            buf_samples,
                            name,
                        }
                    }
                )
                .boxed(),
            (
                (stream_byte(), octet(), octet(), octet(), text(max)),
                (word(), word(), word(), word(), float(), octet()),
            )
                .prop_map(
                    |(
                        (stream_id, segment_id, flags, epoch, timeref_serial),
                        (
                            timeref_session,
                            start_time,
                            sampling_rate,
                            decimation,
                            filter_cutoff,
                            filter_type,
                        ),
                    )| Fields::Segment {
                        stream_id,
                        segment_id,
                        flags,
                        epoch,
                        timeref_serial,
                        timeref_session,
                        start_time,
                        sampling_rate,
                        decimation,
                        filter_cutoff,
                        filter_type,
                    }
                )
                .boxed(),
            (
                stream_byte(),
                octet(),
                octet(),
                text(max),
                text(max),
                text(max)
            )
                .prop_map(|(stream_id, index, data_type, name, units, description)| {
                    Fields::Column {
                        stream_id,
                        index,
                        data_type,
                        name,
                        units,
                        description,
                    }
                })
                .boxed(),
        ]
        .boxed()
    }

    /// A device record whose strings together are any length up to 600
    /// bytes, aimed at a record of 255 or 256 bytes and a payload of 500 or
    /// 501.
    fn device_at_the_limits() -> impl Strategy<Value = Fields> {
        let head = 9;
        let record = prop_oneof![
            Just(255),
            Just(256),
            Just(Packet::MAX_PAYLOAD - METADATA_HEADER_SIZE),
            Just(Packet::MAX_PAYLOAD - METADATA_HEADER_SIZE + 1),
            head..=head + 600,
        ];
        (record, word(), octet()).prop_map(move |(record, session, n_streams)| {
            let strings = record - head;
            let name = strings.min(MAX_STRING);
            let serial = (strings - name).min(MAX_STRING);
            Fields::Device {
                session,
                n_streams,
                name: "n".repeat(name),
                serial: "s".repeat(serial),
                firmware: "f".repeat(strings - name - serial),
            }
        })
    }

    /// A `METADATA` payload. A fifth are under two bytes or any bytes. The
    /// rest are a written record of any kind with one byte set to 0, 1, 2,
    /// 127, 128, 255 or any value, a quarter of them cut anywhere, behind its
    /// own record type three times in five and otherwise 0 to 5 or 255. About
    /// a quarter of all payloads parse.
    fn any_metadata() -> impl Strategy<Value = Vec<u8>> {
        let kind = prop_oneof![
            3 => Just(None),
            1 => (0..=5u8).prop_map(Some),
            1 => Just(Some(u8::MAX)),
        ];
        let byte = prop_oneof![
            Just(0),
            Just(1),
            Just(2),
            Just(127),
            Just(128),
            Just(u8::MAX),
            any::<u8>(),
        ];
        let cut = prop_oneof![3 => Just(None), 1 => any::<Index>().prop_map(Some)];
        prop_oneof![
            1 => (0..=2usize).prop_flat_map(bytes),
            1 => bytes_to(80),
            8 => (fields(16), kind, any::<u8>(), any::<Index>(), byte, cut).prop_map(
                |(fields, kind, flags, at, byte, cut)| {
                    let mut record = fields.wire();
                    let at = at.index(record.len());
                    record[at] = byte;
                    if let Some(cut) = cut {
                        record.truncate(cut.index(record.len() + 1));
                    }
                    [vec![kind.unwrap_or(fields.kind()), flags], record].concat()
                }
            ),
        ]
    }

    proptest! {
        /// M1. A record whose strings are at most 255 bytes is laid out as
        /// its firmware struct, whatever the buffer held before.
        #[test]
        fn a_record_is_laid_out_as_its_struct(fields in fields(MAX_STRING)) {
            let wire = fields.wire();
            let (len, record) = write_record_over_any(&fields, wire.len())?;
            prop_assert_eq!(len, Some(wire.len()));
            prop_assert_eq!(&record, &wire);
            prop_assert_eq!(fields.metadata().record_len(), wire.len());
        }

        /// M2. A record parses back from its bytes, and its view reads every
        /// field the same.
        #[test]
        fn a_record_reads_back_as_it_was_written(fields in fields(MAX_STRING)) {
            reads_as(&fields.wire(), &fields)?;
        }

        /// M3. A record travels in a `METADATA` packet when the payload is at
        /// most 500 bytes, and in a reply frame when it is at most 255 bytes,
        /// whatever the buffer held before.
        #[test]
        fn a_record_travels_in_a_packet_and_a_reply_frame(
            fields in prop_oneof![fields(MAX_STRING), device_at_the_limits()],
            flags in octet(),
        ) {
            let wire = fields.wire();
            let metadata = fields.metadata();
            let flags = MetadataFlags::from_bits(flags);
            let payload_len = METADATA_HEADER_SIZE + wire.len();
            let packet_len = Header::SIZE + payload_len;
            let (written, packet) = write_over_any(packet_len, |buf| metadata.write(flags, buf))?;
            let (relayed, relay) = write_over_any(packet_len, |buf| {
                write_metadata_record(buf, fields.kind(), flags, &wire)
            })?;
            if payload_len <= Packet::MAX_PAYLOAD {
                let expected = [
                    &[11, 0][..],
                    &(payload_len as u16).to_le_bytes(),
                    &[fields.kind(), flags.bits()],
                    &wire,
                ]
                .concat();
                prop_assert_eq!(written, Some(packet_len));
                prop_assert_eq!(&packet, &expected);
                prop_assert_eq!(relayed, Some(packet_len));
                prop_assert_eq!(&relay, &expected);
                let parsed = Metadata::parse(&packet[Header::SIZE..]);
                prop_assert_eq!(
                    parsed.map(|(record, flags)| (by_bits(record), flags)),
                    Some((by_bits(metadata), flags))
                );
            } else {
                prop_assert_eq!(written, None);
                prop_assert_eq!(relayed, None);
            }

            let frame_len = METADATA_REPLY_FRAME_HEADER + wire.len();
            let (framed, frame) = write_over_any(frame_len, |buf| metadata.write_reply_frame(buf))?;
            if wire.len() <= MAX_STRING {
                prop_assert_eq!(framed, Some(frame_len));
                prop_assert_eq!(&frame, &[&[fields.kind(), wire.len() as u8][..], &wire].concat());
                let frames: Option<Vec<_>> = MetadataReply::parse(&frame).map(Iterator::collect);
                prop_assert_eq!(frames, Some(vec![(fields.mtype(), &wire[..])]));
            } else {
                prop_assert_eq!(framed, None);
            }
        }

        /// M3. A reply of up to [`MAX_METADATA_REPLY_SIZE`] frame bytes fits
        /// one RPC reply packet, and one byte more does not.
        #[test]
        fn a_full_reply_fits_one_rpc_reply(
            len in prop_oneof![
                Just(MAX_METADATA_REPLY_SIZE),
                Just(MAX_METADATA_REPLY_SIZE + 1),
                len_to(Packet::MAX_PAYLOAD),
            ],
        ) {
            let mut buf = [0u8; Packet::MAX_SIZE];
            let frames = vec![0u8; len];
            let fits = crate::rpc::write_reply(&mut buf, crate::RpcRequestId::new(1), &frames);
            prop_assert_eq!(fits.is_some(), len <= MAX_METADATA_REPLY_SIZE);
        }

        /// M4. Into a buffer that holds the head and some room more, each
        /// string in turn keeps what fits the room left and 255 bytes, cut on
        /// a character boundary, whatever the buffer held before. A buffer
        /// short of the head is refused. [`Metadata::record_len`] is enough
        /// for every string to fit.
        #[test]
        fn strings_are_cut_to_the_room_left(
            fields in fields(300),
            room in any::<Index>(),
            flags in octet(),
        ) {
            let strings = fields.strings();
            let total: usize = strings.iter().map(|text| text.len()).sum();
            let room = room.index(total + 2);
            let expected = fields.with_strings(&cut_in_turn(&strings, room));
            let wire = expected.wire();
            let head = fields.head_size();

            let (len, record) = write_record_over_any(&fields, head + room)?;
            prop_assert_eq!(len, Some(wire.len()));
            prop_assert_eq!(&record[..wire.len()], &wire[..]);
            reads_as(&wire, &expected)?;
            prop_assert_eq!(write_record_over_any(&fields, head - 1)?.0, None);
            let whole: usize = cut_in_turn(&strings, usize::MAX).iter().map(|text| text.len()).sum();
            prop_assert!(fields.metadata().record_len() >= head + whole);

            let metadata = fields.metadata();
            let flags = MetadataFlags::from_bits(flags);
            let start = Header::SIZE + METADATA_HEADER_SIZE;
            let (len, packet) = write_over_any(start + head + room, |buf| metadata.write(flags, buf))?;
            if METADATA_HEADER_SIZE + wire.len() <= Packet::MAX_PAYLOAD {
                prop_assert_eq!(len, Some(start + wire.len()));
                prop_assert_eq!(&packet[start..start + wire.len()], &wire[..]);
            } else {
                prop_assert_eq!(len, None);
            }
            let (len, _) = write_over_any(start + head - 1, |buf| metadata.write(flags, buf))?;
            prop_assert_eq!(len, None);
        }

        /// M5. A longer head than this build's is read as far as this build
        /// knows, and the strings start after it.
        #[test]
        fn a_longer_head_is_read_as_far_as_this_build_knows(
            (fields, extra) in fields(MAX_STRING).prop_flat_map(|fields| {
                let room = MAX_STRING - fields.head_size();
                let extra = prop_oneof![Just(1), Just(room), 1..=room].prop_flat_map(bytes);
                (Just(fields), extra)
            }),
            flags in octet(),
        ) {
            let mut record = fields.head();
            record[0] += extra.len() as u8;
            record.extend_from_slice(&extra);
            for text in fields.strings() {
                record.extend_from_slice(text.as_bytes());
            }
            reads_as(&record, &fields)?;
            let payload = [&[fields.kind(), flags][..], &record].concat();
            prop_assert_eq!(
                Metadata::parse(&payload).map(|(record, flags)| (by_bits(record), flags)),
                Some((by_bits(fields.metadata()), MetadataFlags::from_bits(flags)))
            );
        }

        /// M6. A head cut at a field boundary reads every field past the cut
        /// as zero and every string it does not measure as empty.
        #[test]
        fn a_shorter_head_reads_zero_past_its_end(
            (fields, end) in fields(MAX_STRING).prop_flat_map(|fields| {
                let ends = prop::sample::select(fields.shorter_ends());
                (Just(fields), ends)
            }),
        ) {
            let older = fields.older(end);
            let mut record = fields.head()[..end].to_vec();
            record[0] = end as u8;
            for text in older.strings() {
                record.extend_from_slice(text.as_bytes());
            }
            reads_as(&record, &older)?;
        }

        /// M7. Any payload parses as the record its type byte names, or not
        /// at all, without a panic. A record parses exactly when its head
        /// length, stream id and strings are valid, and its strings are the
        /// bytes its head measures.
        #[test]
        fn any_payload_parses_as_its_record_type_says(payload in any_metadata()) {
            let parsed = Metadata::parse(&payload);
            let [kind, flags, ref record @ ..] = payload[..] else {
                prop_assert!(parsed.is_none());
                return Ok(());
            };
            prop_assert_eq!(Metadata::defines(kind), (1..=4).contains(&kind));
            let bare = match kind {
                1 => Device::parse(record).map(Metadata::Device),
                2 => Stream::parse(record).map(Metadata::Stream),
                3 => Segment::parse(record).map(Metadata::Segment),
                4 => Column::parse(record).map(Metadata::Column),
                _ => None,
            };
            prop_assert_eq!(
                parsed.map(|(record, flags)| (by_bits(record), flags)),
                bare.map(|record| (by_bits(record), MetadataFlags::from_bits(flags)))
            );
            if !(1..=4).contains(&kind) {
                prop_assert!(bare.is_none());
                return Ok(());
            }
            let expected = record_strings(kind, record);
            prop_assert_eq!(bare.is_some(), expected.is_ok(), "{:?}", expected);
            if let (Some(bare), Ok(strings)) = (bare, expected) {
                let fields = Fields::of(bare);
                prop_assert_eq!(fields.strings(), strings);
                if usize::from(record[0]) >= fields.head_size() {
                    reads_as(record, &fields)?;
                }
            }
        }

        /// M8. Metadata and segment flags are sets of bits.
        #[test]
        fn flags_are_sets_of_bits(a in octet(), b in octet()) {
            let (meta, other) = (MetadataFlags::from_bits(a), MetadataFlags::from_bits(b));
            prop_assert_eq!(meta.bits(), a);
            prop_assert_eq!(meta.is_empty(), a == 0);
            prop_assert_eq!(meta.contains(other), a & b == b);
            prop_assert_eq!(meta.union(other).bits(), a | b);
            prop_assert_eq!((meta | other).bits(), a | b);
            let (segment, other) = (SegmentFlags::from_bits(a), SegmentFlags::from_bits(b));
            prop_assert_eq!(segment.bits(), a);
            prop_assert_eq!(segment.is_empty(), a == 0);
            prop_assert_eq!(segment.contains(other), a & b == b);
            prop_assert_eq!(segment.union(other).bits(), a | b);
            prop_assert_eq!((segment | other).bits(), a | b);
        }
    }

    /// Prints the share of the classes the generator obligations name, when
    /// `TWINLEAF_PAYLOAD_CLASSES` is set.
    #[test]
    fn class_counts() {
        tally("M7 payloads", any_metadata(), |payload| {
            let class = match payload[..] {
                [kind @ 1..=4, _, ref record @ ..] => {
                    record_strings(kind, record).map_or_else(|refused| refused, |_| "parses")
                }
                [_, _, ..] => "record type not defined",
                _ => "under two bytes",
            };
            vec![class]
        });
        tally(
            "M4 strings and room",
            (fields(300), any::<Index>()),
            |(fields, room)| {
                let strings = fields.strings();
                let total: usize = strings.iter().map(|text| text.len()).sum();
                cut_classes(&strings, room.index(total + 2))
            },
        );
    }

    #[test]
    fn data_type_sizes() {
        assert_eq!(DataType::U8.size(), 1);
        assert_eq!(DataType::I24.size(), 3);
        assert_eq!(DataType::F32.size(), 4);
        assert_eq!(DataType::F64.size(), 8);
        assert_eq!(DataType::U64.size(), 8);
    }

    #[test]
    fn data_type_names_the_known_types_and_falls_back_to_raw() {
        assert_eq!(DataType::U8.to_string(), "u8");
        assert_eq!(DataType::I24.to_string(), "i24");
        assert_eq!(DataType::F64.to_string(), "f64");
        assert_eq!(DataType::new(0x35).to_string(), "raw53");
    }

    #[test]
    fn filter_type_names_the_known_filters_and_falls_back_to_its_byte() {
        assert_eq!(FilterType::IIR_BW_LPF4.value(), 3);
        assert_eq!(FilterType::IIR_BW_LPF4.to_string(), "iir-bw-lpf4");
        assert_eq!(FilterType::NONE.to_string(), "none");
        assert_eq!(FilterType::new(9).to_string(), "filter9");
    }
}

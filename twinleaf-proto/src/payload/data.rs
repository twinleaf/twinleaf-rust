//! Stream sample and metadata wire formats.
//!
//! A METADATA packet payload is `[record type u8][flags u8]` followed by one
//! record: a fixed-size head whose first byte is the head's own length, then
//! the variable-length strings whose lengths that head records. Readers take
//! the strings from the declared head length rather than a compiled-in offset,
//! so a device may append head fields without breaking older hosts. A data
//! packet has type `STREAM0 + stream_id` and a payload of
//! `[first sample number u24le][segment id u8]` followed by packed samples.
//!
//! A `dev.metadata` RPC selects records with [`MetadataSelector`] triples and
//! replies with `[type][record length u8][record]` frames ([`MetadataReply`]);
//! the records are bare, with no `[type][flags]` header, so every record
//! parses and writes on its own as well as through [`Metadata`].
//!
//! Every layout here is normative, locked by the byte-exact tests below.

use crate::packet::{Header, Packet, PacketType};
use crate::sync::Epoch;
use crate::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};

/// `[record type][flags]` preceding every metadata record.
pub const METADATA_HEADER_SIZE: usize = 2;
/// `[first sample number u24le][segment id]` preceding a packet's samples.
pub const SAMPLE_HEADER_SIZE: usize = 4;
/// Sample numbers wrap at 24 bits on the wire.
pub const MAX_SAMPLE_NUMBER: u32 = crate::SampleNumber::MAX;

/// Lowest stream id a device may allocate. Id 0 is reserved: `STREAM0` carries
/// a 32-bit sample number instead of the 24-bit-plus-segment form used here.
pub const FIRST_STREAM_ID: u8 = crate::StreamId::MIN;
pub const LAST_STREAM_ID: u8 = crate::StreamId::MAX;

/// Column sample encoding (`TL_DATA_TYPE_*`). The high nibble is the byte size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct DataType(u8);

impl DataType {
    pub const U8: Self = Self(0x10);
    pub const I8: Self = Self(0x11);
    pub const U16: Self = Self(0x20);
    pub const I16: Self = Self(0x21);
    pub const U24: Self = Self(0x30);
    pub const I24: Self = Self(0x31);
    pub const U32: Self = Self(0x40);
    pub const I32: Self = Self(0x41);
    pub const U64: Self = Self(0x80);
    pub const I64: Self = Self(0x81);
    pub const F32: Self = Self(0x42);
    pub const F64: Self = Self(0x82);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u8 {
        self.0
    }

    /// Size of one value of this type, in bytes.
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
    /// Sent because the thing it describes changed.
    pub const UPDATE: Self = Self(1 << 1);
    /// Closes a sweep: the host has now seen every record once.
    pub const LAST: Self = Self(1 << 2);

    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

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

    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

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
    pub const NONE: Self = Self(0);
    pub const IIR_SP_LPF1: Self = Self(1);
    pub const IIR_SP_LPF2: Self = Self(2);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

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
            Self(value) => write!(f, "filter{value}"),
        }
    }
}

/// Device identity and stream count (`TL_METADATA_DEVICE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Device<'a> {
    pub session: SessionId,
    /// Streams the host should expect to be described in this sweep.
    pub n_streams: u8,
    pub name: &'a str,
    pub serial: &'a str,
    pub firmware: &'a str,
}

impl<'a> Device<'a> {
    /// Head this build reads; a device may declare a longer one.
    pub const HEAD_SIZE: usize = 9;

    /// Serialize a bare record (no `[type][flags]` header) into `buf`.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, Self::HEAD_SIZE)?;
        let name = record.push(self.name);
        let serial = record.push(self.serial);
        let firmware = record.push(self.firmware);
        let head = record.head();
        head[1] = name;
        head[2..6].copy_from_slice(&self.session.to_le_bytes());
        head[6] = serial;
        head[7] = firmware;
        head[8] = self.n_streams;
        Some(record.finish())
    }

    /// Parse a bare record (no `[type][flags]` header).
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        let mut record = RecordReader::new(buf, Self::HEAD_SIZE)?;
        let head = record.head;
        Some(Self {
            session: SessionId::from_le_bytes(head[2..6].try_into().unwrap()),
            n_streams: head[8],
            name: record.take(head[1])?,
            serial: record.take(head[6])?,
            firmware: record.take(head[7])?,
        })
    }
}

/// Shape of one data stream (`TL_METADATA_STREAM`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stream<'a> {
    pub stream_id: StreamId,
    pub n_columns: u8,
    pub n_segments: u8,
    /// Bytes per sample: the packed size of all columns.
    pub sample_size: u16,
    /// Samples the device retains for retransmission.
    pub buf_samples: u16,
    pub name: &'a str,
}

impl<'a> Stream<'a> {
    /// Head this build reads; a device may declare a longer one.
    pub const HEAD_SIZE: usize = 9;

    /// Serialize a bare record (no `[type][flags]` header) into `buf`.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, Self::HEAD_SIZE)?;
        let name = record.push(self.name);
        let head = record.head();
        head[1] = self.stream_id.value();
        head[2] = self.n_columns;
        head[3] = self.n_segments;
        head[4..6].copy_from_slice(&self.sample_size.to_le_bytes());
        head[6..8].copy_from_slice(&self.buf_samples.to_le_bytes());
        head[8] = name;
        Some(record.finish())
    }

    /// Parse a bare record (no `[type][flags]` header).
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        let mut record = RecordReader::new(buf, Self::HEAD_SIZE)?;
        let head = record.head;
        Some(Self {
            stream_id: StreamId::try_new(head[1])?,
            n_columns: head[2],
            n_segments: head[3],
            sample_size: u16::from_le_bytes(head[4..6].try_into().unwrap()),
            buf_samples: u16::from_le_bytes(head[6..8].try_into().unwrap()),
            name: record.take(head[8])?,
        })
    }
}

/// Acquisition parameters for one contiguous run of samples
/// (`TL_METADATA_SEGMENT`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Segment<'a> {
    pub stream_id: StreamId,
    pub segment_id: SegmentId,
    pub flags: SegmentFlags,
    pub epoch: Epoch,
    /// Serial of the device that owns the timebase.
    pub timeref_serial: &'a str,
    pub timeref_session: SessionId,
    /// Time of sample zero, in seconds after `epoch`.
    pub start_time: u32,
    /// Samples per second before decimation.
    pub sampling_rate: u32,
    pub decimation: u32,
    pub filter_cutoff: f32,
    pub filter_type: FilterType,
}

impl<'a> Segment<'a> {
    /// Head this build reads; a device may declare a longer one.
    pub const HEAD_SIZE: usize = 27;

    /// Serialize a bare record (no `[type][flags]` header) into `buf`.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, Self::HEAD_SIZE)?;
        let serial = record.push(self.timeref_serial);
        let head = record.head();
        head[1] = self.stream_id.value();
        head[2] = self.segment_id.value();
        head[3] = self.flags.bits();
        head[4] = self.epoch.value();
        head[5] = serial;
        head[6..10].copy_from_slice(&self.timeref_session.to_le_bytes());
        head[10..14].copy_from_slice(&self.start_time.to_le_bytes());
        head[14..18].copy_from_slice(&self.sampling_rate.to_le_bytes());
        head[18..22].copy_from_slice(&self.decimation.to_le_bytes());
        head[22..26].copy_from_slice(&self.filter_cutoff.to_le_bytes());
        head[26] = self.filter_type.value();
        Some(record.finish())
    }

    /// Parse a bare record (no `[type][flags]` header).
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        let mut record = RecordReader::new(buf, Self::HEAD_SIZE)?;
        let head = record.head;
        Some(Self {
            stream_id: StreamId::try_new(head[1])?,
            segment_id: SegmentId::new(head[2]),
            flags: SegmentFlags::from_bits(head[3]),
            epoch: Epoch::new(head[4]),
            timeref_session: SessionId::from_le_bytes(head[6..10].try_into().unwrap()),
            start_time: u32::from_le_bytes(head[10..14].try_into().unwrap()),
            sampling_rate: u32::from_le_bytes(head[14..18].try_into().unwrap()),
            decimation: u32::from_le_bytes(head[18..22].try_into().unwrap()),
            filter_cutoff: f32::from_le_bytes(head[22..26].try_into().unwrap()),
            filter_type: FilterType::new(head[26]),
            timeref_serial: record.take(head[5])?,
        })
    }
}

/// One column of a stream's sample (`TL_METADATA_COLUMN`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column<'a> {
    pub stream_id: StreamId,
    /// Position of this column within the packed sample.
    pub index: ColumnId,
    pub data_type: DataType,
    pub name: &'a str,
    pub units: &'a str,
    pub description: &'a str,
}

impl<'a> Column<'a> {
    /// Head this build reads; a device may declare a longer one.
    pub const HEAD_SIZE: usize = 7;

    /// Serialize a bare record (no `[type][flags]` header) into `buf`.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, Self::HEAD_SIZE)?;
        let name = record.push(self.name);
        let units = record.push(self.units);
        let description = record.push(self.description);
        let head = record.head();
        head[1] = self.stream_id.value();
        head[2] = self.index.value();
        head[3] = self.data_type.value();
        head[4] = name;
        head[5] = units;
        head[6] = description;
        Some(record.finish())
    }

    /// Parse a bare record (no `[type][flags]` header).
    pub fn parse(buf: &'a [u8]) -> Option<Self> {
        let mut record = RecordReader::new(buf, Self::HEAD_SIZE)?;
        let head = record.head;
        Some(Self {
            stream_id: StreamId::try_new(head[1])?,
            index: ColumnId::new(head[2]),
            data_type: DataType::new(head[3]),
            name: record.take(head[4])?,
            units: record.take(head[5])?,
            description: record.take(head[6])?,
        })
    }
}

/// Record type byte of a metadata record, as the `dev.metadata` RPC and the
/// METADATA packet both spell it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MetadataType {
    Device,
    Stream,
    Segment,
    Column,
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
    Device(Device<'a>),
    Stream(Stream<'a>),
    Segment(Segment<'a>),
    Column(Column<'a>),
}

impl<'a> Metadata<'a> {
    /// Whether this crate knows the layout of record type `kind`. A host
    /// forwards a record it does not know; it rejects one it does and cannot
    /// parse.
    pub const fn defines(kind: u8) -> bool {
        matches!(kind, 1..=4)
    }

    /// Serialize just the record, without the packet or metadata headers, and
    /// return its type byte and length. This is the form a `dev.metadata` RPC
    /// reply carries inside each frame.
    pub fn write_record(&self, buf: &mut [u8]) -> Option<(u8, usize)> {
        let (kind, len) = match self {
            Self::Device(record) => (MetadataType::Device, record.write(buf)?),
            Self::Stream(record) => (MetadataType::Stream, record.write(buf)?),
            Self::Segment(record) => (MetadataType::Segment, record.write(buf)?),
            Self::Column(record) => (MetadataType::Column, record.write(buf)?),
        };
        Some((kind.into(), len))
    }

    /// Serialize this record as one `dev.metadata` reply frame,
    /// `[type][record length u8][record]`, and return the frame's length.
    /// `None` when the frame does not fit `buf` or the record is too long for
    /// its `u8` length field.
    pub fn write_reply_frame(&self, buf: &mut [u8]) -> Option<usize> {
        let (kind, len) = self.write_record(buf.get_mut(METADATA_REPLY_FRAME_HEADER..)?)?;
        buf[0] = kind;
        buf[1] = u8::try_from(len).ok()?;
        Some(METADATA_REPLY_FRAME_HEADER + len)
    }

    /// Bytes [`Self::write_record`] produces: the head, plus each string capped
    /// at what its `u8` length field can measure. A caller that must not
    /// truncate sizes its buffer with this.
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

    /// Serialize a full METADATA packet (header included) into `buf`; returns
    /// its length, or `None` if `buf` cannot hold the record's fixed head.
    /// Strings are truncated to fit whatever room remains.
    pub fn write(&self, flags: MetadataFlags, buf: &mut [u8]) -> Option<usize> {
        let (kind, len) = self.write_record(buf.get_mut(Header::SIZE + METADATA_HEADER_SIZE..)?)?;
        write_metadata_headers(buf, kind, flags, len)
    }

    /// Parse a record from a METADATA packet payload (header excluded). Returns
    /// `None` for unknown record types, whose layout is not known here.
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

/// Stamp the packet and metadata headers around a record of `record_len` bytes
/// already staged at `Header::SIZE + METADATA_HEADER_SIZE`, and return the
/// packet's length.
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

/// Serialize a full METADATA packet (header included) carrying `record` as it
/// stands, for a host relaying a record it did not build. Returns its length.
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

/// Split a METADATA packet payload into its record type byte, flags, and the
/// record itself. Unlike [`Metadata::parse`] this needs no knowledge of the
/// record's layout, so a host can forward a type this build does not define.
pub fn split_metadata(payload: &[u8]) -> Option<(u8, MetadataFlags, &[u8])> {
    let (head, record) = payload.split_at_checked(METADATA_HEADER_SIZE)?;
    Some((head[0], MetadataFlags::from_bits(head[1]), record))
}

/// Shortest head a record can declare: its own length byte and one field.
const MIN_RECORD_HEAD: usize = 2;

/// Split a record into the head it declares and the strings that follow.
/// Returns `None` when the record is truncated or declares a head too short to
/// be one. This is the boundary every record parses from, offered on its own
/// for a caller that keeps the two halves apart or checks the framing of a
/// record type it cannot parse.
pub fn split_record(record: &[u8]) -> Option<(&[u8], &[u8])> {
    let declared = *record.first()? as usize;
    if declared < MIN_RECORD_HEAD || record.len() < declared {
        return None;
    }
    Some(record.split_at(declared))
}

/// RPC that reads metadata records on demand.
pub const METADATA_RPC_METHOD: &str = "dev.metadata";
/// Most selectors one `dev.metadata` request may carry.
pub const MAX_METADATA_SELECTORS: usize = 16;
/// Segment index selecting whichever segment is currently acquiring.
pub const CURRENT_SEGMENT: u8 = 0xff;
/// `[type][record length u8]` preceding the record in each reply frame.
pub const METADATA_REPLY_FRAME_HEADER: usize = 2;
/// Most record-frame bytes one `dev.metadata` reply can carry: the RPC reply
/// payload minus its request id. A device stops a reply here, so a query may
/// legitimately be answered with fewer records than it selected.
pub const MAX_METADATA_REPLY_SIZE: usize = Packet::MAX_PAYLOAD - crate::rpc::REPLY_HEADER_SIZE;

/// One `dev.metadata` request selector, encoded as `[type][stream id][index]`.
///
/// `stream_id` is an actual TIO stream id, not the zero-based position
/// [`Subject`] speaks in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct MetadataSelector {
    pub mtype: MetadataType,
    /// Ignored for a device record.
    pub stream_id: u8,
    /// Segment or column index, with [`CURRENT_SEGMENT`] naming the segment
    /// being acquired. Ignored for device and stream records.
    pub index: u8,
}

impl MetadataSelector {
    /// Encoded size of one selector.
    pub const SIZE: usize = 3;

    pub const fn device() -> Self {
        Self {
            mtype: MetadataType::Device,
            stream_id: 0,
            index: 0,
        }
    }

    pub const fn stream(stream_id: u8) -> Self {
        Self {
            mtype: MetadataType::Stream,
            stream_id,
            index: 0,
        }
    }

    pub const fn segment(stream_id: u8, index: u8) -> Self {
        Self {
            mtype: MetadataType::Segment,
            stream_id,
            index,
        }
    }

    pub const fn column(stream_id: u8, index: u8) -> Self {
        Self {
            mtype: MetadataType::Column,
            stream_id,
            index,
        }
    }

    pub fn encode(self) -> [u8; Self::SIZE] {
        [self.mtype.into(), self.stream_id, self.index]
    }
}

/// Why a `dev.metadata` request argument does not parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MetadataQueryError {
    #[error("argument length is not a whole number of selectors")]
    Misaligned,
    #[error("more selectors than a request may carry")]
    TooManySelectors,
}

/// The validated selectors of a `dev.metadata` request argument.
///
/// The empty argument is the bootstrap query: it selects a device-chosen
/// prefix of all records, as many as fit one reply.
#[derive(Debug, Clone, Copy)]
pub struct MetadataQuery<'a> {
    arg: &'a [u8],
}

impl<'a> MetadataQuery<'a> {
    pub fn parse(arg: &'a [u8]) -> Result<Self, MetadataQueryError> {
        if arg.len() % MetadataSelector::SIZE != 0 {
            return Err(MetadataQueryError::Misaligned);
        }
        if arg.len() / MetadataSelector::SIZE > MAX_METADATA_SELECTORS {
            return Err(MetadataQueryError::TooManySelectors);
        }
        Ok(Self { arg })
    }

    pub fn is_bootstrap(&self) -> bool {
        self.arg.is_empty()
    }

    pub fn selectors(&self) -> impl Iterator<Item = MetadataSelector> + 'a {
        self.arg
            .chunks_exact(MetadataSelector::SIZE)
            .map(|selector| MetadataSelector {
                mtype: selector[0].into(),
                stream_id: selector[1],
                index: selector[2],
            })
    }
}

/// The `[type][record length u8][record]` frames of a `dev.metadata` reply.
///
/// [`parse`](Self::parse) validates the whole framing up front: a truncated
/// frame fails there instead of yielding the valid frames before it.
#[derive(Debug, Clone, Copy)]
pub struct MetadataReply<'a> {
    frames: &'a [u8],
}

impl<'a> MetadataReply<'a> {
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
    pub stream_id: StreamId,
    pub segment_id: SegmentId,
    /// Sample number of the first sample in `data`.
    pub first: SampleNumber,
    /// Samples packed back to back, each `Stream::sample_size` bytes.
    pub data: &'a [u8],
}

impl<'a> Samples<'a> {
    /// Byte offset of the first sample in a data packet.
    pub const DATA_OFFSET: usize = Header::SIZE + SAMPLE_HEADER_SIZE;

    /// Serialize a full data packet (header included) into `buf`; returns its
    /// length. Returns `None` for an out-of-range stream id or sample number,
    /// or when the samples do not fit.
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

    /// Write both headers of a packet carrying `data_len` bytes of samples,
    /// which a producer may have staged in place at [`Self::DATA_OFFSET`].
    /// Returns the packet length.
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
            .write((&mut buf[..Header::SIZE]).try_into().unwrap());
        buf[Header::SIZE..Header::SIZE + 3].copy_from_slice(&first.to_le_bytes()[..3]);
        buf[Header::SIZE + 3] = segment_id.value();
        Some(total)
    }

    /// Parse a data packet from its header and payload.
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

/// What one step of a metadata sweep asks the caller to describe.
///
/// `stream` and `column` are zero-based positions in the device's own ordering,
/// not tio ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Subject {
    Device,
    Stream { stream: u8 },
    Segment { stream: u8 },
    Column { stream: u8, column: u8 },
}

/// Round-robin walk over a device's metadata: the device, then for each stream
/// its shape, its current segment, and its columns. The step that closes a pass
/// is flagged [`MetadataFlags::LAST`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Sweep {
    position: Position,
}

impl Sweep {
    pub const fn new() -> Self {
        Self {
            position: Position::Device,
        }
    }

    /// Advance one step. `columns[i]` is the column count of the device's `i`th
    /// stream, re-read on every call: a position that no longer exists restarts
    /// the pass at [`Subject::Device`]. The returned flag marks the record that
    /// closes this pass.
    pub fn step(&mut self, columns: &[u8]) -> (Subject, bool) {
        if !self.position.exists_in(columns) {
            self.position = Position::Device;
        }
        let subject = self.position.subject();
        self.position = self.position.advance(columns);
        (subject, matches!(self.position, Position::Device))
    }
}

#[derive(Debug, Clone, Copy, Default)]
enum Position {
    #[default]
    Device,
    Stream(u8),
    Segment(u8),
    Column(u8, u8),
}

impl Position {
    fn exists_in(self, columns: &[u8]) -> bool {
        let stream = |stream: u8| (stream as usize) < columns.len();
        match self {
            Self::Device => true,
            Self::Stream(s) | Self::Segment(s) => stream(s),
            Self::Column(s, c) => stream(s) && c < columns[s as usize],
        }
    }

    fn subject(self) -> Subject {
        match self {
            Self::Device => Subject::Device,
            Self::Stream(stream) => Subject::Stream { stream },
            Self::Segment(stream) => Subject::Segment { stream },
            Self::Column(stream, column) => Subject::Column { stream, column },
        }
    }

    fn advance(self, columns: &[u8]) -> Self {
        // Streams with no columns are skipped rather than stalling the sweep.
        let from_stream = |stream: u8| match columns.get(stream as usize + 1) {
            Some(_) => Self::Stream(stream + 1),
            None => Self::Device,
        };
        match self {
            Self::Device if columns.is_empty() => Self::Device,
            Self::Device => Self::Stream(0),
            Self::Stream(stream) => Self::Segment(stream),
            Self::Segment(stream) if columns[stream as usize] > 0 => Self::Column(stream, 0),
            Self::Segment(stream) => from_stream(stream),
            Self::Column(stream, column) if column + 1 < columns[stream as usize] => {
                Self::Column(stream, column + 1)
            }
            Self::Column(stream, _) => from_stream(stream),
        }
    }
}

/// Builds a record: a fixed-size head whose first byte is its own length,
/// followed by the strings the head measures. Strings are appended in call
/// order and truncated on a character boundary when the buffer fills.
pub(crate) struct RecordWriter<'a> {
    buf: &'a mut [u8],
    head: usize,
    len: usize,
}

impl<'a> RecordWriter<'a> {
    pub(crate) fn new(buf: &'a mut [u8], head: usize) -> Option<Self> {
        if buf.len() < head {
            return None;
        }
        buf[..head].fill(0);
        buf[0] = head as u8;
        Some(Self {
            buf,
            head,
            len: head,
        })
    }

    /// Append a string; returns how many of its bytes fit.
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

    pub(crate) fn head(&mut self) -> &mut [u8] {
        &mut self.buf[..self.head]
    }

    pub(crate) fn finish(self) -> usize {
        self.len
    }
}

/// Reads a record written by [`RecordWriter`]. Strings start at the head length
/// found on the wire, so a record carrying head fields this build does not know
/// about still parses.
pub(crate) struct RecordReader<'a> {
    pub(crate) head: &'a [u8],
    strings: &'a [u8],
}

impl<'a> RecordReader<'a> {
    /// `head` is the length this build knows how to read; the wire may declare
    /// more.
    pub(crate) fn new(record: &'a [u8], head: usize) -> Option<Self> {
        let (declared, strings) = split_record(record)?;
        if declared.len() < head {
            return None;
        }
        Some(Self {
            head: declared,
            strings,
        })
    }

    pub(crate) fn take(&mut self, len: u8) -> Option<&'a str> {
        let (value, rest) = self.strings.split_at_checked(len as usize)?;
        self.strings = rest;
        core::str::from_utf8(value).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            n_streams: 2,
            name: "AB",
            serial: "CD",
            firmware: "EF",
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
            2, 2,                       // serial_varlen, firmware_varlen
            2,                          // n_streams
            b'A', b'B', b'C', b'D', b'E', b'F',
        ]);
    }

    /// `tl_metadata_column`: a 7-byte head, then name, units, description.
    #[test]
    fn column_record_matches_the_packed_wire_layout() {
        let record = Column {
            stream_id: StreamId::new(4),
            index: ColumnId::new(1),
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
            7, 4, 1, 0x42,              // fixed_len, stream_id, index, FLOAT32
            4, 1, 2,                    // name, units, description lengths
            b'v', b'b', b'u', b's', b'V', b'I', b'n',
        ]);
    }

    /// `tl_metadata_segment`: 27-byte head with four little-endian u32s and an
    /// f32 cutoff, then the timeref serial.
    #[test]
    fn segment_record_matches_the_packed_wire_layout() {
        let record = Segment {
            timeref_serial: "XY",
            filter_cutoff: 1.0,
            filter_type: FilterType::IIR_SP_LPF2,
            ..segment()
        };
        let mut buf = [0u8; 64];
        let len = record_bytes(Metadata::Segment(record), MetadataFlags::UPDATE, &mut buf);
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            11, 0, 31, 0,
            3, 0b010,                   // record type SEGMENT, UPDATE
            27,                         // fixed_len
            1, 3,                       // stream_id, segment_id
            0b11,                       // VALID | ACTIVE
            3,                          // epoch UNIX
            2,                          // time_ref_serial_varlen
            0x0D, 0x0C, 0x0B, 0x0A,     // time_ref_session_id
            0x00, 0x31, 0xBF, 0x69,     // start_time 1774137600
            0xE8, 0x03, 0x00, 0x00,     // sampling_rate 1000
            0x64, 0x00, 0x00, 0x00,     // decimation 100
            0x00, 0x00, 0x80, 0x3F,     // filter_cutoff 1.0f
            2,                          // filter_type IIR_SP_LPF2
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
        // Head shorter than this build's known fields.
        assert_eq!(Column::parse(&[6, 0, 0, 0, 0, 0]), None);
        // name_varlen runs past the end of the record.
        assert_eq!(Column::parse(&[7, 0, 0, 0, 9, 0, 0, b'x']), None);
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
        let query = MetadataQuery::parse(&arg).unwrap();
        assert!(!query.is_bootstrap());
        assert_eq!(query.selectors().collect::<Vec<_>>(), selectors);
    }

    #[test]
    fn the_empty_query_is_the_bootstrap_and_selects_nothing_explicitly() {
        let query = MetadataQuery::parse(&[]).unwrap();
        assert!(query.is_bootstrap());
        assert_eq!(query.selectors().count(), 0);
    }

    #[test]
    fn a_query_rejects_misaligned_or_oversized_arguments() {
        assert_eq!(
            MetadataQuery::parse(&[1, 0]).unwrap_err(),
            MetadataQueryError::Misaligned
        );
        let oversized = [0u8; (MAX_METADATA_SELECTORS + 1) * MetadataSelector::SIZE];
        assert_eq!(
            MetadataQuery::parse(&oversized).unwrap_err(),
            MetadataQueryError::TooManySelectors
        );
        let full = [0u8; MAX_METADATA_SELECTORS * MetadataSelector::SIZE];
        assert!(MetadataQuery::parse(&full).is_ok());
    }

    #[test]
    fn a_reply_frame_is_the_type_the_length_and_the_bare_record() {
        let record = Column {
            stream_id: StreamId::new(4),
            index: ColumnId::new(1),
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
            7, 4, 1, 0x42,              // fixed_len, stream_id, index, FLOAT32
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

    #[test]
    fn samples_reject_an_out_of_range_number() {
        let mut buf = [0u8; 64];
        let samples = |first| Samples {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new(0),
            first: SampleNumber::new(first),
            data: &[0; 4],
        };
        assert_eq!(StreamId::try_new(0), None, "stream 0 is reserved");
        assert_eq!(StreamId::try_new(128), None);
        assert_eq!(samples(MAX_SAMPLE_NUMBER + 1).write(&mut buf), None);
        assert!(samples(MAX_SAMPLE_NUMBER).write(&mut buf).is_some());
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

    fn sweep(columns: &[u8], steps: usize) -> Vec<(Subject, bool)> {
        let mut sweep = Sweep::new();
        (0..steps).map(|_| sweep.step(columns)).collect()
    }

    #[test]
    fn sweep_walks_device_then_each_stream_with_its_columns() {
        use Subject::*;
        #[rustfmt::skip]
        assert_eq!(sweep(&[2, 3], 11), vec![
            (Device, false),
            (Stream { stream: 0 }, false),
            (Segment { stream: 0 }, false),
            (Column { stream: 0, column: 0 }, false),
            (Column { stream: 0, column: 1 }, false),
            (Stream { stream: 1 }, false),
            (Segment { stream: 1 }, false),
            (Column { stream: 1, column: 0 }, false),
            (Column { stream: 1, column: 1 }, false),
            (Column { stream: 1, column: 2 }, true),
            (Device, false),
        ]);
    }

    #[test]
    fn sweep_handles_a_device_with_no_streams() {
        assert_eq!(sweep(&[], 2), vec![(Subject::Device, true); 2]);
    }

    #[test]
    fn sweep_skips_a_stream_with_no_columns() {
        use Subject::*;
        #[rustfmt::skip]
        assert_eq!(sweep(&[0, 1], 6), vec![
            (Device, false),
            (Stream { stream: 0 }, false),
            (Segment { stream: 0 }, false),
            (Stream { stream: 1 }, false),
            (Segment { stream: 1 }, false),
            (Column { stream: 1, column: 0 }, true),
        ]);
    }

    #[test]
    fn sweep_restarts_when_its_position_disappears() {
        let mut sweep = Sweep::new();
        for _ in 0..4 {
            sweep.step(&[2, 3]); // ends on Column { stream: 0, column: 0 }
        }
        // The second stream went away and the first lost a column.
        assert_eq!(sweep.step(&[1]), (Subject::Device, false));
    }
}

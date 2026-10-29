//! Stream and column keys, owned copies of protocol metadata records, the
//! point-in-time snapshots built from them, and the storage class each wire
//! data type decodes to.

use crate::proto::data as wire;
use crate::proto::data::DataType;
use crate::proto::packet::Packet as WirePacket;
use crate::proto::DeviceRoute;
use crate::proto::{ColumnId, StreamId};
use crate::tio;
use crate::tio::packet::{EncodeError, Packet};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// A stream within one connection or parser root.
///
/// The route identifies the device relative to that enclosing source. Two
/// independent connections can therefore have the same `StreamKey`, so an
/// application combining sources must pair it with its own source identity.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct StreamKey {
    /// Hops from the enclosing source down to the device, root-to-leaf.
    pub route: DeviceRoute,
    /// The stream's id on that device, 1 through 127.
    pub stream_id: StreamId,
}

impl StreamKey {
    /// Creates a key naming one stream on one device.
    pub fn new(route: DeviceRoute, stream_id: StreamId) -> Self {
        Self { route, stream_id }
    }
}

impl std::fmt::Display for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]:{}", self.route, self.stream_id)
    }
}

/// A column within one routed stream.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct ColumnKey {
    /// Hops from the enclosing source down to the device, root-to-leaf.
    pub route: DeviceRoute,
    /// The stream's id on that device, 1 through 127.
    pub stream_id: StreamId,
    /// The column's position within the stream's packed sample.
    pub column_id: ColumnId,
}

impl ColumnKey {
    /// Creates a key naming one column of one stream on one device.
    pub fn new(route: DeviceRoute, stream_id: StreamId, column_id: ColumnId) -> Self {
        Self {
            route,
            stream_id,
            column_id,
        }
    }

    /// Returns the key of the stream this column belongs to.
    pub fn stream_key(&self) -> StreamKey {
        StreamKey {
            route: self.route,
            stream_id: self.stream_id,
        }
    }
}

impl std::fmt::Display for ColumnKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]:{}/{}", self.route, self.stream_id, self.column_id)
    }
}

macro_rules! metadata_record {
    ($record:ident, $kind:ident) => {
        /// One retained metadata descriptor, holding the bare wire record
        /// without the `[type][flags]` header of a METADATA packet. Equality
        /// is byte equality, so a field a newer device adds still counts as a
        /// change.
        #[derive(Clone, PartialEq, Eq)]
        pub(crate) struct $record(Arc<[u8]>);

        impl $record {
            /// Copies `record` if it parses as this descriptor, or returns
            /// `None`.
            pub(crate) fn new(record: &[u8]) -> Option<Self> {
                wire::$kind::parse(record)?;
                Some(Self(Arc::from(record)))
            }

            /// Encodes `fields` as a retained record. Returns `None` if the
            /// record does not fit in a packet.
            #[allow(dead_code)]
            pub(crate) fn encode(fields: wire::$kind<'_>) -> Option<Self> {
                let record = wire::Metadata::$kind(fields);
                let mut buf = [0u8; WirePacket::MAX_PAYLOAD];
                let (_, len) = record.write_record(buf.get_mut(..record.record_len())?)?;
                Self::new(&buf[..len])
            }

            pub(crate) fn get(&self) -> wire::$kind<'_> {
                wire::$kind::parse(&self.0).expect("record parsed when retained")
            }

            /// Encodes the descriptor as an UPDATE broadcast to `routing`,
            /// carrying the record exactly as retained.
            pub(crate) fn update(&self, routing: DeviceRoute) -> Result<Packet, EncodeError> {
                Packet::metadata_record(
                    wire::MetadataType::$kind.into(),
                    wire::MetadataFlags::UPDATE,
                    &self.0,
                    routing,
                )
            }
        }

        impl fmt::Debug for $record {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

metadata_record!(DeviceRecord, Device);
metadata_record!(StreamRecord, Stream);
metadata_record!(SegmentRecord, Segment);
metadata_record!(ColumnRecord, Column);

impl SegmentRecord {
    /// Returns the sample rate in hertz after decimation,
    /// `sampling_rate / decimation`, or `None` if either is zero.
    pub(crate) fn effective_rate(&self) -> Option<f64> {
        let segment = self.get();
        (segment.sampling_rate != 0 && segment.decimation != 0)
            .then(|| f64::from(segment.sampling_rate) / f64::from(segment.decimation))
    }
}

/// A request for metadata one route still lacks, as selectors for a
/// `dev.metadata` call that the caller makes.
#[derive(Debug, Clone)]
pub struct MetadataQuery {
    /// The device to address the `dev.metadata` call to.
    pub route: DeviceRoute,
    /// Records this route still lacks, capped at what one request may carry.
    ///
    /// Empty when only the device record is missing. An empty argument
    /// selects the device-chosen bootstrap prefix.
    pub selectors: Vec<wire::MetadataSelector>,
    pub(super) generation: u32,
}

impl MetadataQuery {
    /// Returns the `dev.metadata` argument for these selectors. An empty
    /// argument selects the device-chosen bootstrap prefix.
    pub fn args(&self) -> Vec<u8> {
        self.selectors
            .iter()
            .flat_map(|selector| selector.encode())
            .collect()
    }
}

/// Point-in-time metadata for one routed stream. It holds the stream's
/// identity, its device and stream records, its current segment, and its
/// columns.
///
/// It is the owned counterpart of the borrowed protocol views a
/// [`SampleBatch`](super::SampleBatch) returns. It holds no sample arrays,
/// clones by pointer, and outlives the batch it came from.
#[derive(Debug, Clone)]
pub struct StreamMetadataSnapshot(Arc<StreamMetadataInner>);

#[derive(Debug)]
struct StreamMetadataInner {
    key: StreamKey,
    device: DeviceRecord,
    stream: StreamRecord,
    segment: SegmentRecord,
    columns: Vec<ColumnRecord>,
}

impl StreamMetadataSnapshot {
    pub(super) fn new(
        key: StreamKey,
        device: DeviceRecord,
        stream: StreamRecord,
        segment: SegmentRecord,
        columns: Vec<ColumnRecord>,
    ) -> StreamMetadataSnapshot {
        StreamMetadataSnapshot(Arc::new(StreamMetadataInner {
            key,
            device,
            stream,
            segment,
            columns,
        }))
    }

    /// Returns the route half of [`Self::stream_key`].
    pub fn route(&self) -> DeviceRoute {
        self.0.key.route
    }

    /// Returns the route and stream id together, the identity a
    /// [`SampleBatch`](super::SampleBatch) carries.
    pub fn stream_key(&self) -> StreamKey {
        self.0.key
    }

    /// Returns the device descriptor, borrowed from the retained wire record.
    pub fn device(&self) -> wire::Device<'_> {
        self.0.device.get()
    }

    /// Returns the stream descriptor, borrowed from the retained wire record.
    pub fn stream(&self) -> wire::Stream<'_> {
        self.0.stream.get()
    }

    /// Returns the current segment's descriptor, borrowed from its retained
    /// record.
    pub fn segment(&self) -> wire::Segment<'_> {
        self.0.segment.get()
    }

    /// Returns the stream's columns in index order.
    pub fn columns(&self) -> impl ExactSizeIterator<Item = wire::Column<'_>> + '_ {
        self.0.columns.iter().map(ColumnRecord::get)
    }

    /// Returns the column whose index is `id`, or `None` if the schema has no
    /// such column.
    pub fn column(&self, id: ColumnId) -> Option<wire::Column<'_>> {
        self.columns().find(|column| column.index == id)
    }

    /// Returns the column records in index order, as retained.
    pub(super) fn column_records(&self) -> &[ColumnRecord] {
        &self.0.columns
    }

    /// Encodes the retained descriptors as byte-faithful UPDATE packets, in the
    /// order device, stream, segment, then columns.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if a record is too large for a packet.
    pub fn metadata_packets(&self) -> Result<Vec<tio::Packet>, EncodeError> {
        std::iter::once(self.0.device.update(self.route()))
            .chain(self.stream_packets())
            .collect()
    }

    /// Encodes the stream's own descriptors, without the device record it
    /// shares with its peers.
    fn stream_packets(&self) -> impl Iterator<Item = Result<tio::Packet, EncodeError>> + '_ {
        let routing = self.route();
        [
            self.0.stream.update(routing),
            self.0.segment.update(routing),
        ]
        .into_iter()
        .chain(
            self.0
                .columns
                .iter()
                .map(move |column| column.update(routing)),
        )
    }
}

/// Point-in-time metadata for one device and all advertised streams.
///
/// The parser learns these records incrementally. A snapshot is available only
/// after every advertised stream has a stream, current segment, and columns.
#[derive(Debug, Clone)]
pub struct DeviceMetadataSnapshot(Arc<DeviceMetadataInner>);

#[derive(Debug)]
struct DeviceMetadataInner {
    route: DeviceRoute,
    device: DeviceRecord,
    streams: HashMap<StreamId, StreamMetadataSnapshot>,
}

impl DeviceMetadataSnapshot {
    pub(super) fn new(
        route: DeviceRoute,
        device: DeviceRecord,
        streams: HashMap<StreamId, StreamMetadataSnapshot>,
    ) -> DeviceMetadataSnapshot {
        DeviceMetadataSnapshot(Arc::new(DeviceMetadataInner {
            route,
            device,
            streams,
        }))
    }

    /// Returns the device descriptor, borrowed from the retained wire record.
    pub fn device(&self) -> wire::Device<'_> {
        self.0.device.get()
    }

    /// Returns one stream's snapshot, or `None` if the device advertises no
    /// such id.
    pub fn stream(&self, stream_id: StreamId) -> Option<&StreamMetadataSnapshot> {
        self.0.streams.get(&stream_id)
    }

    /// Returns every advertised stream paired with its id, in no particular
    /// order.
    pub fn streams(
        &self,
    ) -> impl ExactSizeIterator<Item = (StreamId, &StreamMetadataSnapshot)> + '_ {
        self.0.streams.iter().map(|(&id, stream)| (id, stream))
    }

    /// Encodes every retained descriptor as a byte-faithful UPDATE packet, with
    /// the device record once ahead of its streams.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if a record is too large for a packet.
    pub fn metadata_packets(&self) -> Result<Vec<tio::Packet>, EncodeError> {
        std::iter::once(self.0.device.update(self.0.route))
            .chain(
                self.0
                    .streams
                    .values()
                    .flat_map(StreamMetadataSnapshot::stream_packets),
            )
            .collect()
    }
}

#[cfg(test)]
pub(crate) fn buffer_type(data_type: DataType) -> BufferType {
    decoded_buffer_type(data_type).unwrap_or(BufferType::Float)
}

/// Returns the storage class a column of `data_type` decodes to, or `None` for
/// an unknown wire type.
pub(crate) fn decoded_buffer_type(data_type: DataType) -> Option<BufferType> {
    Some(match data_type {
        DataType::F32 | DataType::F64 => BufferType::Float,
        DataType::I8 | DataType::I16 | DataType::I24 | DataType::I32 | DataType::I64 => {
            BufferType::Int
        }
        DataType::U8 | DataType::U16 | DataType::U24 | DataType::U32 | DataType::U64 => {
            BufferType::UInt
        }
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferType {
    Float,
    Int,
    UInt,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;
    use crate::proto::SessionId;

    /// A segment's rate is its sampling rate over its decimation, and a zero
    /// in either gives no rate at all.
    #[test]
    fn a_segment_rate_is_its_sampling_rate_over_its_decimation() {
        let rate = |sampling_rate, decimation| {
            SegmentRecord::encode(wire::Segment {
                sampling_rate,
                decimation,
                ..fixtures::segment(1)
            })
            .unwrap()
            .effective_rate()
        };
        assert_eq!(rate(1000, 4), Some(250.0));
        assert_eq!(rate(1000, 0), None);
        assert_eq!(rate(0, 4), None);
    }

    fn route(value: &str) -> DeviceRoute {
        value.parse().unwrap()
    }

    #[test]
    fn a_retained_record_is_relayed_byte_for_byte() {
        let fields = wire::Device {
            session: SessionId::new(42),
            n_streams: 1,
            name: "device",
            serial: "serial",
            firmware: "firmware",
        };
        let record = DeviceRecord::encode(fields).expect("a short record fits");

        assert_eq!(record.get(), fields);
        assert_eq!(
            record.update(route("/1")).unwrap().as_bytes(),
            Packet::metadata(
                wire::Metadata::Device(fields),
                wire::MetadataFlags::UPDATE,
                route("/1"),
            )
            .unwrap()
            .as_bytes()
        );
    }

    #[test]
    fn a_record_too_long_for_a_payload_cannot_be_retained() {
        let long = "n".repeat(200);
        assert!(DeviceRecord::encode(wire::Device {
            session: SessionId::new(42),
            n_streams: 1,
            name: &long,
            serial: &long,
            firmware: &long,
        })
        .is_none());
    }
}

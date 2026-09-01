//! Private owned backing for borrowed protocol metadata, the point-in-time
//! snapshots published from it, and the decoded storage class chosen for each
//! wire data type.

use super::sample::{SampleBatch, StreamKey};
use crate::tio;
use crate::tio::proto::{DataType, DeviceRoute, EncodeError, Packet};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use twinleaf_proto::data as wire;
use twinleaf_proto::{ColumnId, StreamId, MAX_PAYLOAD_SIZE};

macro_rules! metadata_record {
    ($record:ident, $kind:ident) => {
        /// One retained metadata descriptor: the bare wire record, without the
        /// `[type][flags]` header a METADATA packet puts in front of it.
        /// Equality is byte equality, so a field a newer device adds still
        /// counts as a change.
        #[derive(Clone, PartialEq, Eq)]
        pub(crate) struct $record(Arc<[u8]>);

        impl $record {
            /// Retain a copy of `record` if it parses as this descriptor.
            pub(crate) fn new(record: &[u8]) -> Option<Self> {
                wire::$kind::parse(record)?;
                Some(Self(Arc::from(record)))
            }

            /// Retain a record this host built rather than received.
            #[allow(dead_code)]
            pub(crate) fn encode(fields: wire::$kind<'_>) -> Option<Self> {
                let record = wire::Metadata::$kind(fields);
                let mut buf = [0u8; MAX_PAYLOAD_SIZE];
                let (_, len) = record.write_record(buf.get_mut(..record.record_len())?)?;
                Self::new(&buf[..len])
            }

            pub(crate) fn get(&self) -> wire::$kind<'_> {
                wire::$kind::parse(&self.0).expect("record parsed when retained")
            }

            /// This descriptor as an UPDATE broadcast, carrying the record
            /// exactly as retained.
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

/// A request for metadata one route still lacks, in selectors rather than
/// packets: the caller owns the RPC that answers it.
#[derive(Debug, Clone)]
pub struct MetadataQuery {
    /// The device to address the `dev.metadata` call to.
    pub route: DeviceRoute,
    /// Records this route still lacks, capped at what one request may carry.
    ///
    /// Empty when only the device record is missing, which selects the
    /// device-chosen bootstrap prefix instead.
    pub selectors: Vec<wire::MetadataSelector>,
    pub(super) generation: u32,
}

impl MetadataQuery {
    /// The `dev.metadata` argument for these selectors. Empty selects the
    /// device-chosen bootstrap prefix.
    pub fn args(&self) -> Vec<u8> {
        self.selectors
            .iter()
            .flat_map(|selector| selector.encode())
            .collect()
    }
}

/// Point-in-time metadata for one routed stream: its identity, the device and
/// stream that describe it, its current segment, and its columns.
///
/// This is the owned counterpart to the borrowed protocol views a
/// [`SampleBatch`] returns. It holds no sample arrays and clones by pointer, so
/// applications can keep it after dropping the batch it came from.
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

    /// The route half of [`Self::stream_key`].
    pub fn route(&self) -> DeviceRoute {
        self.0.key.route
    }

    /// Route and stream id together, the identity a [`SampleBatch`] carries.
    pub fn stream_key(&self) -> StreamKey {
        self.0.key
    }

    /// The device descriptor, borrowed from the retained wire record.
    pub fn device(&self) -> wire::Device<'_> {
        self.0.device.get()
    }

    /// The stream descriptor, borrowed from the retained wire record.
    pub fn stream(&self) -> wire::Stream<'_> {
        self.0.stream.get()
    }

    /// The current segment's descriptor, borrowed from its retained record.
    pub fn segment(&self) -> wire::Segment<'_> {
        self.0.segment.get()
    }

    /// The stream's columns in index order.
    pub fn columns(&self) -> impl ExactSizeIterator<Item = wire::Column<'_>> + '_ {
        self.0.columns.iter().map(ColumnRecord::get)
    }

    /// The column whose index is `id`, or `None` if the schema has no such column.
    pub fn column(&self, id: ColumnId) -> Option<wire::Column<'_>> {
        self.columns().find(|column| column.index == id)
    }

    /// Whether `batch` has this routed stream identity and exact column
    /// schema. Device and segment changes do not change a stream schema.
    pub fn matches_schema(&self, batch: &SampleBatch) -> bool {
        self.0.key == batch.stream_key()
            && self.0.columns.len() == batch.schema().len()
            && self
                .0
                .columns
                .iter()
                .zip(batch.schema())
                .all(|(expected, actual)| expected == actual.record())
    }

    /// Encode the retained descriptors as byte-faithful UPDATE packets, ordered
    /// device, stream, segment, then columns.
    pub fn metadata_packets(&self) -> Result<Vec<tio::Packet>, EncodeError> {
        std::iter::once(self.0.device.update(self.route()))
            .chain(self.stream_packets())
            .collect()
    }

    /// The stream's own descriptors, without the device record it shares with
    /// its peers.
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

    /// The device descriptor, borrowed from the retained wire record.
    pub fn device(&self) -> wire::Device<'_> {
        self.0.device.get()
    }

    /// One stream's snapshot, or `None` if the device advertises no such id.
    pub fn stream(&self, stream_id: StreamId) -> Option<&StreamMetadataSnapshot> {
        self.0.streams.get(&stream_id)
    }

    /// Every advertised stream paired with its id, in no particular order.
    pub fn streams(
        &self,
    ) -> impl ExactSizeIterator<Item = (StreamId, &StreamMetadataSnapshot)> + '_ {
        self.0.streams.iter().map(|(&id, stream)| (id, stream))
    }

    /// Encode every retained descriptor as a byte-faithful UPDATE packet, the
    /// device record once ahead of its streams.
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

/// The storage class for a decoded column, or `None` for an unknown wire type.
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
    use twinleaf_proto::SessionId;

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

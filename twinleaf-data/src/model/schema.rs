//! Stream and column keys, owned copies of protocol metadata records, the
//! point-in-time snapshots built from them, and the storage class each wire
//! data type decodes to.

use crate::proto::data as wire;
use crate::proto::packet::{write_routing, Packet as WirePacket, PacketError};
use crate::proto::DeviceRoute;
use crate::proto::{ColumnId, StreamId};
use std::collections::BTreeMap;
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

#[cfg(test)]
thread_local! {
    static RECORD_PARSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Returns how many records this thread has checked to retain, so tests can
/// bound the checks a packet costs.
#[cfg(test)]
pub(crate) fn record_parses() -> u64 {
    RECORD_PARSES.with(std::cell::Cell::get)
}

macro_rules! metadata_record {
    ($record:ident, $kind:ident, $view:ident) => {
        /// One retained metadata descriptor, holding the bare wire record
        /// without the `[type][flags]` header of a METADATA packet. It is
        /// checked once, when retained, and read in place after that.
        /// Equality is byte equality, so a field a newer device adds still
        /// counts as a change.
        #[derive(Clone, PartialEq, Eq)]
        pub(crate) struct $record(Arc<[u8]>);

        impl $record {
            /// Copies `record` if it parses as this descriptor. A record
            /// whose only fault is text that is not UTF-8 is retained with
            /// that text repaired, as [`wire::repair_record_text`] repairs
            /// it in the room [`wire::repaired_record_room`] gives.
            /// Returns `None` for any other record that does not parse.
            pub(crate) fn new(record: &[u8]) -> Option<Self> {
                #[cfg(test)]
                RECORD_PARSES.with(|parses| parses.set(parses.get() + 1));
                if wire::$view::new_checked(record).is_some() {
                    return Some(Self(Arc::from(record)));
                }
                let kind = wire::MetadataType::$kind.into();
                let mut repaired = vec![0; wire::repaired_record_room(record.len())];
                let len = wire::repair_record_text(kind, record, &mut repaired)?;
                wire::$view::new_checked(&repaired[..len])?;
                Some(Self(Arc::from(&repaired[..len])))
            }

            /// Encodes `fields` as a retained record. Returns `None` if the
            /// record does not fit in a packet.
            #[cfg(any(test, feature = "test-util"))]
            pub(crate) fn encode(fields: wire::$kind<'_>) -> Option<Self> {
                let record = wire::Metadata::$kind(fields);
                let mut buf = [0u8; WirePacket::MAX_PAYLOAD];
                let (_, len) = record
                    .write_record(buf.get_mut(..record.record_len())?)
                    .ok()?;
                Self::new(&buf[..len])
            }

            /// Returns a view that reads the record's fields in place.
            pub(crate) fn get(&self) -> wire::$view<'_> {
                wire::$view::new_unchecked(&self.0)
            }

            /// Encodes the descriptor as the bytes of an UPDATE broadcast to
            /// `routing`, carrying the record exactly as retained. Returns
            /// [`PacketError::PayloadTooBig`] if the record does not fit a
            /// packet.
            pub(crate) fn update(&self, routing: DeviceRoute) -> Result<Vec<u8>, PacketError> {
                let mut buf = [0u8; WirePacket::MAX_SIZE];
                let len = wire::write_metadata_record(
                    &mut buf,
                    wire::MetadataType::$kind.into(),
                    wire::MetadataFlags::UPDATE,
                    &self.0,
                )
                .map_err(|_| PacketError::PayloadTooBig)?;
                let len = write_routing(&mut buf, len, routing)
                    .expect("any payload and the longest route fit a packet");
                Ok(buf[..len].to_vec())
            }
        }

        impl fmt::Debug for $record {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

metadata_record!(DeviceRecord, Device, DeviceView);
metadata_record!(StreamRecord, Stream, StreamView);
metadata_record!(SegmentRecord, Segment, SegmentView);
metadata_record!(ColumnRecord, Column, ColumnView);

/// One retained metadata descriptor of any kind.
#[derive(Clone)]
pub(crate) enum Record {
    Device(DeviceRecord),
    Stream(StreamRecord),
    Segment(SegmentRecord),
    Column(ColumnRecord),
}

impl Record {
    /// Copies `record` if it parses as a descriptor of `kind`, as the
    /// descriptor's own `new` does. Returns `None` for a record that does not
    /// parse and for a kind this build does not know.
    pub(crate) fn new(kind: wire::MetadataType, record: &[u8]) -> Option<Record> {
        match kind {
            wire::MetadataType::Device => DeviceRecord::new(record).map(Record::Device),
            wire::MetadataType::Stream => StreamRecord::new(record).map(Record::Stream),
            wire::MetadataType::Segment => SegmentRecord::new(record).map(Record::Segment),
            wire::MetadataType::Column => ColumnRecord::new(record).map(Record::Column),
            wire::MetadataType::Unknown(_) => None,
        }
    }

    /// Encodes the descriptor as the bytes of an UPDATE broadcast to
    /// `routing`, as the descriptor's own `update` does.
    fn update(&self, routing: DeviceRoute) -> Result<Vec<u8>, PacketError> {
        match self {
            Record::Device(record) => record.update(routing),
            Record::Stream(record) => record.update(routing),
            Record::Segment(record) => record.update(routing),
            Record::Column(record) => record.update(routing),
        }
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
    /// The route's metadata generation when the query was formed.
    pub(crate) generation: u32,
}

impl MetadataQuery {
    /// Returns the route's metadata generation when the query was formed,
    /// which the parser's [`metadata_generation`] is told against.
    ///
    /// [`metadata_generation`]: crate::PacketParser::metadata_generation
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Returns the `dev.metadata` argument for these selectors. An empty
    /// argument selects the device-chosen bootstrap prefix.
    pub fn args(&self) -> Vec<u8> {
        self.selectors
            .iter()
            .flat_map(|selector| selector.encode())
            .collect()
    }
}

/// The layout of one routed stream for one run, its stream record and its
/// column records in index order.
///
/// The parser builds it once the stream's columns are complete, and every
/// batch, run and snapshot of the run shares it by [`Arc`]. Two schemas are
/// equal when they are the same allocation or their records are equal byte
/// for byte.
#[derive(Debug)]
pub struct StreamSchema {
    key: StreamKey,
    stream: StreamRecord,
    columns: Vec<ColumnRecord>,
}

impl StreamSchema {
    /// Creates the schema of `key` from its stream record and its columns in
    /// index order.
    ///
    /// # Panics
    ///
    /// Panics if a record describes a stream other than `key`'s, or if a
    /// column is out of index order.
    pub(crate) fn new(
        key: StreamKey,
        stream: StreamRecord,
        columns: Vec<ColumnRecord>,
    ) -> StreamSchema {
        assert_eq!(
            key.stream_id,
            stream.get().stream_id(),
            "stream key must match metadata"
        );
        for (position, column) in columns.iter().enumerate() {
            let column = column.get();
            assert_eq!(
                key.stream_id,
                column.stream_id(),
                "column must match stream"
            );
            assert_eq!(column.index().index(), position, "columns in index order");
        }
        StreamSchema {
            key,
            stream,
            columns,
        }
    }

    /// Returns the routed stream this schema describes.
    pub fn stream_key(&self) -> StreamKey {
        self.key
    }

    /// Returns the stream descriptor, borrowed from the retained wire record.
    pub fn stream(&self) -> wire::StreamView<'_> {
        self.stream.get()
    }

    /// Returns the stream's columns in index order.
    pub fn columns(&self) -> impl ExactSizeIterator<Item = wire::ColumnView<'_>> + '_ {
        self.columns.iter().map(ColumnRecord::get)
    }

    /// Returns the column whose index is `id`, or `None` if the stream has no
    /// such column.
    pub fn column(&self, id: ColumnId) -> Option<wire::ColumnView<'_>> {
        self.columns.get(id.index()).map(ColumnRecord::get)
    }

    /// Returns `true` if `stream` and `columns` are the records this schema
    /// was built from.
    pub(crate) fn describes(&self, stream: &StreamRecord, columns: &[ColumnRecord]) -> bool {
        self.stream == *stream && self.columns == columns
    }
}

impl PartialEq for StreamSchema {
    fn eq(&self, other: &StreamSchema) -> bool {
        std::ptr::eq(self, other)
            || (self.key == other.key && self.describes(&other.stream, &other.columns))
    }
}

impl Eq for StreamSchema {}

/// Point-in-time metadata for one routed stream. It holds the stream's
/// schema, the record of the device that sent it, and its current segment.
///
/// A [`SampleBatch`](crate::SampleBatch), a [`Run`](crate::Run) and a
/// [`StreamSummary`](crate::StreamSummary) each carry one, and reach the
/// protocol views of their records through it. It holds no sample arrays,
/// clones by pointer, and outlives the batch it came from.
#[derive(Debug, Clone)]
pub struct StreamMetadataSnapshot {
    schema: Arc<StreamSchema>,
    device: DeviceRecord,
    segment: SegmentRecord,
}

impl StreamMetadataSnapshot {
    /// Creates a snapshot of `schema`'s stream as `device` sent it in
    /// `segment`.
    ///
    /// # Panics
    ///
    /// Panics if `segment` describes a stream other than the schema's.
    pub(crate) fn new(
        schema: Arc<StreamSchema>,
        device: DeviceRecord,
        segment: SegmentRecord,
    ) -> StreamMetadataSnapshot {
        assert_eq!(
            schema.key.stream_id,
            segment.get().stream_id(),
            "segment must match stream"
        );
        StreamMetadataSnapshot {
            schema,
            device,
            segment,
        }
    }

    /// Returns the route half of [`Self::stream_key`].
    pub fn route(&self) -> DeviceRoute {
        self.schema.key.route
    }

    /// Returns the route and stream id together, the identity a
    /// [`SampleBatch`](crate::SampleBatch) carries.
    pub fn stream_key(&self) -> StreamKey {
        self.schema.key
    }

    /// Returns the stream's schema, shared with every batch of its run.
    pub fn schema(&self) -> &Arc<StreamSchema> {
        &self.schema
    }

    /// Returns the device descriptor, borrowed from the retained wire record.
    pub fn device(&self) -> wire::DeviceView<'_> {
        self.device.get()
    }

    /// Returns the current segment's descriptor, borrowed from its retained
    /// record. The record may be unpopulated, which
    /// [`is_populated`](wire::SegmentView::is_populated) tells. Then its
    /// times and time reference describe nothing.
    pub fn segment(&self) -> wire::SegmentView<'_> {
        self.segment.get()
    }

    /// Returns the segment's sample rate in hertz after decimation, or zero
    /// for a segment whose rate or decimation is zero. The parser emits no
    /// rows from such a segment. The rate of an unpopulated record describes
    /// no rows.
    pub fn rate_hz(&self) -> f64 {
        self.segment.get().rate_hz().unwrap_or(0.0)
    }

    pub(crate) fn segment_record(&self) -> &SegmentRecord {
        &self.segment
    }

    /// Encodes the retained descriptors as byte-faithful UPDATE packets
    /// addressed to `routing`, in the order device, stream, segment, then
    /// columns. Each item holds the bytes of one packet.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::PayloadTooBig`] if a record is too large for a
    /// packet.
    pub fn metadata_packets(&self, routing: DeviceRoute) -> Result<Vec<Vec<u8>>, PacketError> {
        std::iter::once(Record::Device(self.device.clone()))
            .chain(self.records())
            .map(|record| record.update(routing))
            .collect()
    }

    /// Returns the stream's own descriptors, without the device record it
    /// shares with its peers, in the order stream, segment, then columns.
    fn records(&self) -> impl Iterator<Item = Record> + '_ {
        [
            Record::Stream(self.schema.stream.clone()),
            Record::Segment(self.segment.clone()),
        ]
        .into_iter()
        .chain(self.schema.columns.iter().cloned().map(Record::Column))
    }
}

/// Point-in-time metadata for one device and all advertised streams.
///
/// The parser learns these records incrementally. A snapshot is available only
/// after every advertised stream has a stream record, columns, and a record of
/// its current segment slot. That record may be unpopulated.
#[derive(Debug, Clone)]
pub struct DeviceMetadataSnapshot(Arc<DeviceMetadataInner>);

#[derive(Debug)]
struct DeviceMetadataInner {
    route: DeviceRoute,
    device: DeviceRecord,
    streams: BTreeMap<StreamId, StreamMetadataSnapshot>,
}

impl DeviceMetadataSnapshot {
    pub(crate) fn new(
        route: DeviceRoute,
        device: DeviceRecord,
        streams: BTreeMap<StreamId, StreamMetadataSnapshot>,
    ) -> DeviceMetadataSnapshot {
        DeviceMetadataSnapshot(Arc::new(DeviceMetadataInner {
            route,
            device,
            streams,
        }))
    }

    /// Returns the device descriptor, borrowed from the retained wire record.
    pub fn device(&self) -> wire::DeviceView<'_> {
        self.0.device.get()
    }

    /// Returns one stream's snapshot, or `None` if the device advertises no
    /// such id.
    pub fn stream(&self, stream_id: StreamId) -> Option<&StreamMetadataSnapshot> {
        self.0.streams.get(&stream_id)
    }

    /// Returns every advertised stream paired with its id, in order of id.
    pub fn streams(
        &self,
    ) -> impl ExactSizeIterator<Item = (StreamId, &StreamMetadataSnapshot)> + '_ {
        self.0.streams.iter().map(|(&id, stream)| (id, stream))
    }

    /// Returns the route of the device the snapshot describes.
    pub fn route(&self) -> DeviceRoute {
        self.0.route
    }

    /// Encodes every retained descriptor as a byte-faithful UPDATE packet
    /// addressed to `routing`, with the device record once ahead of its
    /// streams, which go in order of id. Each item holds the bytes of one
    /// packet.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::PayloadTooBig`] if a record is too large for a
    /// packet.
    pub fn metadata_packets(&self, routing: DeviceRoute) -> Result<Vec<Vec<u8>>, PacketError> {
        self.records()
            .map(|record| record.update(routing))
            .collect()
    }

    /// Returns every retained descriptor, the device record once ahead of
    /// its streams, which go in order of id.
    pub(crate) fn records(&self) -> impl Iterator<Item = Record> + '_ {
        std::iter::once(Record::Device(self.0.device.clone())).chain(
            self.0
                .streams
                .values()
                .flat_map(StreamMetadataSnapshot::records),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;
    use crate::fixtures::packet;
    use crate::proto::packet::Checked;
    use crate::proto::SessionId;

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
            protocol: 0,
        };
        let record = DeviceRecord::encode(fields).expect("a short record fits");

        assert_eq!(record.get().fields(), fields);
        assert_eq!(
            record.update(route("/1")).unwrap(),
            packet::metadata(
                wire::Metadata::Device(fields),
                wire::MetadataFlags::UPDATE,
                route("/1"),
            )
        );
    }

    /// The bare record `fields` writes, with byte `at` of the strings set to
    /// 0xFF.
    fn with_a_bad_byte(fields: wire::Metadata<'_>, at: usize) -> Vec<u8> {
        let mut buf = vec![0; fields.record_len()];
        let (_, len) = fields.write_record(&mut buf).unwrap();
        buf.truncate(len);
        let head = usize::from(buf[0]);
        buf[head + at] = 0xFF;
        buf
    }

    /// Each kind of record keeps a record with one string that is not
    /// UTF-8, with that string repaired and the rest as sent.
    #[test]
    fn a_record_whose_text_is_not_utf8_is_retained_repaired() {
        let device = DeviceRecord::new(&with_a_bad_byte(
            wire::Metadata::Device(fixtures::device()),
            "test-device".len(),
        ))
        .expect("a repaired device record");
        assert_eq!(
            (
                device.get().name(),
                device.get().serial(),
                device.get().firmware()
            ),
            ("test-device", "\u{FFFD}N123", "fw")
        );

        let stream = StreamRecord::new(&with_a_bad_byte(
            wire::Metadata::Stream(fixtures::stream(1)),
            0,
        ))
        .expect("a repaired stream record");
        assert_eq!(stream.get().name(), "\u{FFFD}est-stream");
        assert_eq!(stream.get().buf_samples(), 128);

        let segment = SegmentRecord::new(&with_a_bad_byte(
            wire::Metadata::Segment(fixtures::segment(1)),
            4,
        ))
        .expect("a repaired segment record");
        assert_eq!(segment.get().timeref_serial(), "cloc\u{FFFD}");
        assert_eq!(segment.get().timeref_session(), SessionId::new(7));

        let column = ColumnRecord::new(&with_a_bad_byte(
            wire::Metadata::Column(wire::Column {
                units: "nT",
                ..fixtures::column(1, 0, wire::DataType::F32)
            }),
            "col_0".len() + 1,
        ))
        .expect("a repaired column record");
        assert_eq!(
            (
                column.get().name(),
                column.get().units(),
                column.get().index()
            ),
            ("col_0", "n\u{FFFD}", ColumnId::new(0))
        );
    }

    /// A repaired record goes out as it is retained, and comes back the
    /// same bytes.
    #[test]
    fn a_repaired_record_is_relayed_byte_for_byte() {
        let record = ColumnRecord::new(&with_a_bad_byte(
            wire::Metadata::Column(fixtures::column(1, 0, wire::DataType::F32)),
            0,
        ))
        .expect("a repaired column record");
        let sent = record.update(route("/1")).unwrap();
        let received = Checked::exact(&sent).unwrap();

        assert_eq!(received.as_bytes(), &sent[..]);
        assert_eq!(
            received.payload_bytes()[wire::METADATA_HEADER_SIZE..],
            record.0[..]
        );
    }
}

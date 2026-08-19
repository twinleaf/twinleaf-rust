//! Seekable and indexed access to immutable TIO log files.

use super::parser::PacketParser;
use super::sample::{Boundary, BoundaryClass, SampleBatch};
use super::state::{PacketError, PacketEvent, ParseState, ValidatedRows};
use crate::tio::proto::identifiers::StreamKey;
use crate::tio::proto::meta::{ColumnMetadata, DeviceMetadata, SegmentMetadata, StreamMetadata};
use crate::tio::{self, Packet};
use bytes::{Buf, Bytes};
use memmap2::Mmap;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

const INDEX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const PROGRESS_BYTES: usize = 1024 * 1024;

/// Immutable memory-mapped TIO log.
///
/// Packet payloads share the mapping instead of copying their bytes. The input
/// file must therefore not be modified or truncated while the log is open.
/// Each call to [`LogFile::packets`] creates an independent packet iterator.
pub struct LogFile {
    data: Bytes,
}

/// Sequential zero-copy traversal over the packets in a [`LogFile`].
pub struct PacketIter {
    remaining: Bytes,
    position: usize,
    failed: bool,
}

/// Wire or semantic failure at a byte offset in a log.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("could not decode packet at byte offset {offset}: {source}")]
    Packet {
        offset: usize,
        #[source]
        source: tio::proto::DecodeError,
    },
    #[error("invalid data at byte offset {offset}: {source}")]
    Data {
        offset: usize,
        #[source]
        source: PacketError,
    },
}

impl LogError {
    pub fn offset(&self) -> usize {
        match self {
            Self::Packet { offset, .. } | Self::Data { offset, .. } => *offset,
        }
    }
}

impl PacketIter {
    fn new(data: Bytes, position: usize) -> Self {
        Self {
            remaining: data,
            position,
            failed: false,
        }
    }

    /// Byte offset immediately after the last packet returned.
    pub fn position(&self) -> usize {
        self.position
    }
}

impl Iterator for PacketIter {
    type Item = Result<Packet, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.remaining.is_empty() {
            return None;
        }

        let offset = self.position;
        match Packet::deserialize_bytes(&self.remaining) {
            Ok((packet, len)) => {
                self.remaining.advance(len);
                self.position += len;
                Some(Ok(packet))
            }
            Err(source) => {
                self.failed = true;
                Some(Err(LogError::Packet { offset, source }))
            }
        }
    }
}

const BOUNDARY_CLASSES: usize = 5;

fn class_index(class: BoundaryClass) -> usize {
    match class {
        BoundaryClass::Seamless => 0,
        BoundaryClass::Startup => 1,
        BoundaryClass::DataLoss => 2,
        BoundaryClass::Reconfig => 3,
        BoundaryClass::Anomaly => 4,
    }
}

/// Aggregate information about one run of a stream: the rows between two
/// non-continuous boundaries, over which schema and timing stay fixed.
#[derive(Debug)]
pub struct StreamSummary {
    run: u32,
    opened_by: Option<BoundaryClass>,
    metadata: Arc<StreamMetadata>,
    segment: Arc<SegmentMetadata>,
    columns: Vec<Arc<ColumnMetadata>>,
    sample_count: u64,
    first_timestamp: Option<f64>,
    last_timestamp: Option<f64>,
}

impl StreamSummary {
    /// Per-stream run ordinal this summary describes.
    pub fn run(&self) -> u32 {
        self.run
    }

    /// Class of the boundary that started this run.
    pub fn opened_by(&self) -> Option<BoundaryClass> {
        self.opened_by
    }

    pub fn metadata(&self) -> &StreamMetadata {
        &self.metadata
    }

    pub fn segment(&self) -> &SegmentMetadata {
        &self.segment
    }

    pub fn columns(&self) -> &[Arc<ColumnMetadata>] {
        &self.columns
    }

    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    pub fn first_timestamp(&self) -> Option<f64> {
        self.first_timestamp
    }

    pub fn last_timestamp(&self) -> Option<f64> {
        self.last_timestamp
    }

    pub fn rate_hz(&self) -> f64 {
        f64::from(self.segment.sampling_rate) / f64::from(self.segment.decimation.max(1))
    }
}

/// File-level metadata and row counts discovered without decoding sample values.
#[derive(Debug, Default)]
pub struct LogSummary {
    bytes_scanned: usize,
    packet_count: u64,
    devices: BTreeMap<tio::proto::DeviceRoute, Arc<DeviceMetadata>>,
    streams: BTreeMap<StreamKey, Vec<StreamSummary>>,
    boundaries: [u64; BOUNDARY_CLASSES],
    error: Option<LogError>,
}

impl LogSummary {
    pub fn bytes_scanned(&self) -> usize {
        self.bytes_scanned
    }

    pub fn packet_count(&self) -> u64 {
        self.packet_count
    }

    pub fn devices(&self) -> &BTreeMap<tio::proto::DeviceRoute, Arc<DeviceMetadata>> {
        &self.devices
    }

    /// Runs of each stream, in the order they were scanned.
    pub fn streams(&self) -> &BTreeMap<StreamKey, Vec<StreamSummary>> {
        &self.streams
    }

    /// Boundaries of one class seen anywhere in the log.
    pub fn boundaries(&self, class: BoundaryClass) -> u64 {
        self.boundaries[class_index(class)]
    }

    pub fn error(&self) -> Option<&LogError> {
        self.error.as_ref()
    }

    fn observe_rows(
        &mut self,
        route: tio::proto::DeviceRoute,
        stream_id: u8,
        rows: &ValidatedRows<'_>,
    ) {
        self.devices
            .entry(route)
            .or_insert_with(|| rows.device.clone());

        if let Some(boundary) = &rows.boundary {
            self.boundaries[class_index(boundary.class())] += 1;
        }

        let runs = self
            .streams
            .entry(StreamKey::new(route, stream_id))
            .or_default();
        if runs
            .last()
            .is_none_or(|last| last.run < rows.generations.stream)
        {
            runs.push(StreamSummary {
                run: rows.generations.stream,
                opened_by: rows.boundary.as_ref().map(Boundary::class),
                metadata: rows.stream.clone(),
                segment: rows.segment.clone(),
                columns: rows.columns.to_vec(),
                sample_count: 0,
                first_timestamp: None,
                last_timestamp: None,
            });
        }
        let stream = runs.last_mut().expect("a run was just opened if empty");
        stream.sample_count += rows.row_count as u64;

        let first_n = rows.first_sample_n + 1;
        let last_n = rows.last_sample_n + 1;
        let first_timestamp = rows.segment.time_at(first_n);
        let last_timestamp = rows.segment.time_at(last_n);
        stream.first_timestamp = Some(
            stream
                .first_timestamp
                .map_or(first_timestamp, |prior| prior.min(first_timestamp)),
        );
        stream.last_timestamp = Some(
            stream
                .last_timestamp
                .map_or(last_timestamp, |prior| prior.max(last_timestamp)),
        );
    }
}

struct IndexedChunk {
    bytes: Range<usize>,
    state: ParseState,
}

/// Compact first-pass result used to summarize and decode a log.
///
/// The index stores parser checkpoints roughly every 16 MiB, rather than one
/// descriptor per packet. This keeps its memory use bounded for logs containing
/// millions of small packets and lets each range decode from the metadata and
/// continuity state in effect at its start.
pub struct LogIndex {
    data: Bytes,
    summary: LogSummary,
    chunks: Vec<IndexedChunk>,
}

impl LogIndex {
    pub fn summary(&self) -> &LogSummary {
        &self.summary
    }

    /// Decode indexed byte ranges in file order.
    ///
    /// Chunk boundaries may end a batch early but never introduce a data
    /// boundary. Sample values and ordering within each stream are unchanged.
    pub fn batches(
        &self,
        batch_rows: usize,
    ) -> impl Iterator<Item = Result<SampleBatch, LogError>> + '_ {
        assert!(batch_rows > 0, "batch row target must be nonzero");
        IndexedBatchIter {
            data: &self.data,
            chunks: self.chunks.iter(),
            batch_rows,
            ready: VecDeque::new(),
            failed: false,
        }
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

struct IndexedBatchIter<'a> {
    data: &'a Bytes,
    chunks: std::slice::Iter<'a, IndexedChunk>,
    batch_rows: usize,
    ready: VecDeque<SampleBatch>,
    failed: bool,
}

impl Iterator for IndexedBatchIter<'_> {
    type Item = Result<SampleBatch, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(batch) = self.ready.pop_front() {
                return Some(Ok(batch));
            }
            if self.failed {
                return None;
            }
            let chunk = self.chunks.next()?;
            match decode_chunk(self.data, chunk, self.batch_rows) {
                Ok(batches) => self.ready.extend(batches),
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

fn decode_chunk(
    data: &Bytes,
    chunk: &IndexedChunk,
    batch_rows: usize,
) -> Result<Vec<SampleBatch>, LogError> {
    let mut packets = PacketIter::new(data.slice(chunk.bytes.clone()), chunk.bytes.start);
    let mut parser = PacketParser::from_state(chunk.state.clone()).with_batch_rows(batch_rows);
    let mut batches = Vec::new();

    loop {
        let packet_offset = packets.position();
        let Some(packet) = packets.next() else {
            break;
        };
        let packet = packet?;
        parser
            .push_packet(&packet)
            .map_err(|source| LogError::Data {
                offset: packet_offset,
                source,
            })?;
        while let Some(batch) = parser.pop_batch() {
            batches.push(batch);
        }
    }
    batches.extend(parser.finish());
    Ok(batches)
}

impl LogFile {
    /// Memory-map a log for zero-copy packet access.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: callers must keep log files immutable while the mapping is
        // open, as documented on `LogFile`.
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Bytes::from_owner(mmap);
        Ok(Self { data })
    }

    /// Total byte length of the mapped log.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the mapped log contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterate over packets from the beginning of the log without copying
    /// variable-length stream payloads.
    pub fn packets(&self) -> PacketIter {
        PacketIter::new(self.data.clone(), 0)
    }

    /// Scan packet structure, metadata, boundaries, and row counts without
    /// decoding sample values.
    pub fn scan(&self, root_route: tio::proto::DeviceRoute, ignore_session: bool) -> LogIndex {
        self.scan_with_progress(root_route, ignore_session, |_| {})
    }

    /// Scan the log and periodically report the byte position reached.
    pub fn scan_with_progress(
        &self,
        root_route: tio::proto::DeviceRoute,
        ignore_session: bool,
        mut progress: impl FnMut(usize),
    ) -> LogIndex {
        let mut packets = self.packets();
        let mut state = ParseState::new(root_route, ignore_session);
        let mut summary = LogSummary::default();
        let mut chunks = Vec::new();
        let mut chunk_start = 0;
        let mut chunk_state = state.clone();
        let mut next_progress = PROGRESS_BYTES;
        let mut scanned = 0;

        loop {
            let packet_offset = packets.position();
            let packet = match packets.next() {
                Some(Ok(packet)) => packet,
                Some(Err(error)) => {
                    summary.error = Some(error);
                    break;
                }
                None => break,
            };
            let event = match state.apply_packet(&packet) {
                Ok(event) => event,
                Err(source) => {
                    summary.error = Some(LogError::Data {
                        offset: packet_offset,
                        source,
                    });
                    break;
                }
            };
            summary.packet_count += 1;
            if let PacketEvent::Rows {
                route,
                stream_id,
                rows,
            } = event
            {
                summary.observe_rows(route, stream_id, &rows);
            }

            let position = packets.position();
            scanned = position;
            if position >= next_progress {
                progress(position);
                next_progress = position.saturating_add(PROGRESS_BYTES);
            }
            if position - chunk_start >= INDEX_CHUNK_BYTES {
                chunks.push(IndexedChunk {
                    bytes: chunk_start..position,
                    state: chunk_state,
                });
                chunk_start = position;
                chunk_state = state.clone();
            }
        }

        if chunk_start < scanned {
            chunks.push(IndexedChunk {
                bytes: chunk_start..scanned,
                state: chunk_state,
            });
        }
        summary.bytes_scanned = scanned;
        progress(scanned);

        LogIndex {
            data: self.data.clone(),
            summary,
            chunks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Generations, StreamDataError};
    use crate::tio::proto::meta::{
        ColumnMetadata, DeviceMetadata, MetadataContent, MetadataEpoch, MetadataFilter,
        MetadataPayload, SegmentMetadata, StreamMetadata,
    };
    use crate::tio::proto::{DataType, DeviceRoute, HeartbeatPayload, Payload, StreamDataPayload};

    fn encoded_log(packets: impl IntoIterator<Item = Packet>) -> Bytes {
        let mut encoded = Vec::new();
        for packet in packets {
            encoded.extend(packet.serialize().unwrap());
        }
        Bytes::from(encoded)
    }

    fn metadata_packet(content: MetadataContent) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content,
                flags: 0,
                unknown_fixed: Vec::new(),
                unknown_varlen: Vec::new(),
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    #[test]
    fn packet_iterators_are_independent() {
        let data = encoded_log([
            Packet {
                payload: Payload::Heartbeat(HeartbeatPayload::Session(1)),
                routing: DeviceRoute::root(),
                ttl: 0,
            },
            Packet {
                payload: Payload::Heartbeat(HeartbeatPayload::Session(2)),
                routing: DeviceRoute::root(),
                ttl: 0,
            },
        ]);
        let log = LogFile { data };
        let mut first = log.packets();
        let second = log.packets();

        assert_eq!(first.position(), 0);
        assert_eq!(second.position(), 0);
        assert!(first.next().unwrap().is_ok());
        assert!(first.position() > 0);
        assert_eq!(second.position(), 0);
        assert_eq!(first.count(), 1);
        assert_eq!(second.count(), 2);
    }

    #[test]
    fn packet_iterator_yields_a_wire_error_once() {
        let log = LogFile {
            data: Bytes::from_static(&[0]),
        };
        let mut packets = log.packets();

        assert!(matches!(
            packets.next(),
            Some(Err(LogError::Packet { offset: 0, .. }))
        ));
        assert!(packets.next().is_none());
        assert_eq!(packets.position(), 0);
    }

    fn segment_metadata(sampling_rate: u32) -> SegmentMetadata {
        SegmentMetadata {
            stream_id: 1,
            segment_id: 0,
            flags: 0,
            time_ref_epoch: MetadataEpoch::Unix,
            time_ref_serial: "clock".to_string(),
            time_ref_session_id: 7,
            start_time: 0,
            sampling_rate,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: MetadataFilter::Unfiltered,
        }
    }

    /// One device with one 1 Hz stream of a single `f32` column.
    fn schema_packets() -> Vec<Packet> {
        [
            MetadataContent::Device(DeviceMetadata {
                serial_number: "SN123".to_string(),
                firmware_hash: "fw".to_string(),
                n_streams: 1,
                session_id: 42,
                name: "test-device".to_string(),
            }),
            MetadataContent::Stream(StreamMetadata {
                stream_id: 1,
                name: "test-stream".to_string(),
                n_columns: 1,
                n_segments: 1,
                sample_size: 4,
                buf_samples: 128,
            }),
            MetadataContent::Segment(segment_metadata(1)),
            MetadataContent::Column(ColumnMetadata {
                stream_id: 1,
                index: 0,
                data_type: DataType::Float32,
                name: "value".to_string(),
                units: String::new(),
                description: String::new(),
            }),
        ]
        .into_iter()
        .map(metadata_packet)
        .collect()
    }

    fn data_packet(first_sample_n: u32, segment_id: u8) -> Packet {
        Packet {
            payload: Payload::StreamData(StreamDataPayload {
                stream_id: 1,
                first_sample_n,
                segment_id,
                data: Bytes::from_static(&[0; 4]),
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    #[test]
    fn scan_reports_semantically_invalid_stream_data() {
        let mut packets = schema_packets();
        let valid_prefix_len = encoded_log(packets.clone()).len();
        packets.push(data_packet(0, 1));
        let log = LogFile {
            data: encoded_log(packets),
        };

        let index = log.scan(DeviceRoute::root(), true);
        assert_eq!(index.summary().bytes_scanned(), valid_prefix_len);
        assert!(matches!(
            index.summary().error(),
            Some(LogError::Data {
                offset,
                source: PacketError::Stream {
                    source: StreamDataError::SegmentOutOfRange { .. },
                    ..
                },
            }) if *offset == valid_prefix_len
        ));
    }

    #[test]
    fn a_mid_log_rate_change_summarizes_each_run_separately() {
        let mut packets = schema_packets();
        packets.extend([data_packet(0, 0), data_packet(1, 0)]);
        packets.push(metadata_packet(MetadataContent::Segment(segment_metadata(
            10,
        ))));
        packets.extend([data_packet(2, 0), data_packet(3, 0)]);
        let log = LogFile {
            data: encoded_log(packets),
        };

        let summary = log.scan(DeviceRoute::root(), true);
        let summary = summary.summary();
        let runs = &summary.streams()[&StreamKey::new(DeviceRoute::root(), 1)];

        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].run(), 1);
        assert_eq!(runs[0].opened_by(), Some(BoundaryClass::Startup));
        assert_eq!(runs[0].rate_hz(), 1.0);
        assert_eq!(runs[0].sample_count(), 2);
        assert_eq!(runs[0].first_timestamp(), Some(1.0));
        assert_eq!(runs[0].last_timestamp(), Some(2.0));
        assert_eq!(runs[1].run(), 2);
        assert_eq!(runs[1].opened_by(), Some(BoundaryClass::Reconfig));
        assert_eq!(runs[1].rate_hz(), 10.0);
        assert_eq!(runs[1].sample_count(), 2);
        assert!((runs[1].first_timestamp().unwrap() - 0.3).abs() < 1e-9);
        assert!((runs[1].last_timestamp().unwrap() - 0.4).abs() < 1e-9);
        assert_eq!(summary.boundaries(BoundaryClass::Reconfig), 1);
        assert_eq!(summary.boundaries(BoundaryClass::Anomaly), 0);
    }

    #[test]
    fn a_backward_time_jump_counts_as_an_anomaly() {
        let mut packets = schema_packets();
        packets.extend([data_packet(0, 0), data_packet(1, 0), data_packet(0, 0)]);
        let log = LogFile {
            data: encoded_log(packets),
        };

        let summary = log.scan(DeviceRoute::root(), true);
        let summary = summary.summary();
        let runs = &summary.streams()[&StreamKey::new(DeviceRoute::root(), 1)];

        assert_eq!(summary.boundaries(BoundaryClass::Anomaly), 1);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[1].opened_by(), Some(BoundaryClass::Anomaly));
    }

    #[test]
    fn indexed_batches_carry_the_generations_of_a_linear_parse() {
        // Padding the log past a checkpoint forces the decode to resume from a
        // cloned parser state rather than from the beginning of the log.
        let filler = Packet {
            payload: Payload::RpcReply(crate::tio::proto::RpcReplyPayload {
                id: 1,
                reply: vec![0; 400],
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        };
        let mut packets = schema_packets();
        packets.push(data_packet(0, 0));
        packets.extend(std::iter::repeat_n(filler, INDEX_CHUNK_BYTES / 400 + 1));
        // Contiguous, then a gap wide enough to be data loss.
        packets.extend([data_packet(1, 0), data_packet(10, 0), data_packet(11, 0)]);

        let log = LogFile {
            data: encoded_log(packets.clone()),
        };
        let index = log.scan(DeviceRoute::root(), true);
        assert!(index.chunk_count() > 1, "the log must span two checkpoints");
        let indexed: Vec<Generations> = index
            .batches(1)
            .map(|batch| batch.expect("valid log").generations())
            .collect();

        let mut parser = PacketParser::new(DeviceRoute::root(), true).with_batch_rows(1);
        let mut linear = Vec::new();
        for packet in &packets {
            parser.push_packet(packet).expect("valid log");
            while let Some(batch) = parser.pop_batch() {
                linear.push(batch.generations());
            }
        }

        assert_eq!(indexed, linear);
        assert_eq!(
            linear,
            [
                Generations {
                    stream: 1,
                    device: 0,
                    global: 0
                },
                Generations {
                    stream: 1,
                    device: 0,
                    global: 0
                },
                Generations {
                    stream: 2,
                    device: 1,
                    global: 1
                },
                Generations {
                    stream: 2,
                    device: 1,
                    global: 1
                },
            ]
        );
    }
}

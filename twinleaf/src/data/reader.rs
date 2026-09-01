//! Seekable and indexed access to immutable TIO log files.

use super::parser::{PacketParser, ParserCheckpoint};
use super::sample::StreamKey;
use super::sample::{sample_time, BoundaryClass, BoundaryReason, SampleBatch};
use super::state::{PacketError, ScannedRows};
use super::{ColumnRecord, DeviceRecord, SegmentRecord, StreamRecord};
use crate::tio::{self, Packet};
use bytes::{Buf, Bytes};
use memmap2::Mmap;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io;
use std::ops::Range;
use std::path::Path;
use twinleaf_proto::data as wire;

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
        match Packet::from_wire_prefix(&self.remaining) {
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
    metadata: StreamRecord,
    segment: SegmentRecord,
    columns: Vec<ColumnRecord>,
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

    pub fn metadata(&self) -> wire::Stream<'_> {
        self.metadata.get()
    }

    pub fn segment(&self) -> wire::Segment<'_> {
        self.segment.get()
    }

    pub fn columns(&self) -> impl ExactSizeIterator<Item = wire::Column<'_>> + '_ {
        self.columns.iter().map(ColumnRecord::get)
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
        let segment = self.segment();
        f64::from(segment.sampling_rate) / f64::from(segment.decimation.max(1))
    }
}

/// File-level metadata and row counts discovered without decoding sample values.
#[derive(Debug, Default)]
pub struct LogSummary {
    bytes_scanned: usize,
    packet_count: u64,
    devices: BTreeMap<tio::proto::DeviceRoute, DeviceRecord>,
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

    pub fn devices(
        &self,
    ) -> impl ExactSizeIterator<Item = (tio::proto::DeviceRoute, wire::Device<'_>)> + '_ {
        self.devices
            .iter()
            .map(|(&route, device)| (route, device.get()))
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

    fn observe_rows(&mut self, rows: &ScannedRows<'_>) {
        self.devices
            .entry(rows.stream_key().route)
            .or_insert_with(|| rows.device().clone());

        if let Some(boundary) = rows.boundary() {
            self.boundaries[class_index(boundary.class())] += 1;
        }

        let runs = self.streams.entry(rows.stream_key()).or_default();
        if runs
            .last()
            .is_none_or(|last| last.run < rows.generations().stream)
        {
            runs.push(StreamSummary {
                run: rows.generations().stream,
                opened_by: rows.boundary().map(BoundaryReason::class),
                metadata: rows.stream().clone(),
                segment: rows.segment().clone(),
                columns: rows.columns().to_vec(),
                sample_count: 0,
                first_timestamp: None,
                last_timestamp: None,
            });
        }
        let stream = runs.last_mut().expect("a run was just opened if empty");
        stream.sample_count += rows.row_count() as u64;

        let (first_n, last_n) = rows.sample_number_bounds();
        let first_timestamp = sample_time(rows.segment().get(), first_n.value() + 1);
        let last_timestamp = sample_time(rows.segment().get(), last_n.value() + 1);
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
    state: ParserCheckpoint,
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
    let mut parser = PacketParser::replay_from(&chunk.state).with_batch_rows(batch_rows);
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
        let mut parser = PacketParser::new(root_route, ignore_session);
        let mut summary = LogSummary::default();
        let mut chunks = Vec::new();
        let mut chunk_start = 0;
        let mut chunk_state = parser.checkpoint();
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
            let validated = match parser.scan_packet(&packet) {
                Ok(validated) => validated,
                Err(source) => {
                    summary.error = Some(LogError::Data {
                        offset: packet_offset,
                        source,
                    });
                    break;
                }
            };
            summary.packet_count += 1;
            if let Some(rows) = validated {
                summary.observe_rows(&rows);
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
                chunk_state = parser.checkpoint();
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
    use crate::data::fixtures::{column, device, segment, stream};
    use crate::data::{Generations, StreamDataError};
    use crate::tio::proto::{DataType, DeviceRoute};

    fn encoded_log(packets: impl IntoIterator<Item = Packet>) -> Bytes {
        let mut encoded = Vec::new();
        for packet in packets {
            encoded.extend_from_slice(packet.as_bytes());
        }
        Bytes::from(encoded)
    }

    fn metadata_packet(record: wire::Metadata<'_>) -> Packet {
        Packet::metadata(record, wire::MetadataFlags::default(), DeviceRoute::root())
            .expect("a valid metadata record")
    }

    #[test]
    fn packet_iterators_are_independent() {
        let data = encoded_log([
            Packet::heartbeat_session(1, DeviceRoute::root()),
            Packet::heartbeat_session(2, DeviceRoute::root()),
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

    /// One device with one 1 Hz stream of a single `f32` column.
    fn schema_packets() -> Vec<Packet> {
        vec![
            metadata_packet(wire::Metadata::Device(device())),
            metadata_packet(wire::Metadata::Stream(stream(1))),
            metadata_packet(wire::Metadata::Segment(segment(1))),
            metadata_packet(wire::Metadata::Column(column(1, 0, DataType::F32))),
        ]
    }

    fn data_packet(first_sample_n: u32, segment_id: u8) -> Packet {
        Packet::samples(1, segment_id, first_sample_n, &[0; 4], DeviceRoute::root())
            .expect("valid samples")
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
        packets.push(metadata_packet(wire::Metadata::Segment(wire::Segment {
            sampling_rate: 10,
            ..segment(1)
        })));
        packets.extend([data_packet(2, 0), data_packet(3, 0)]);
        let log = LogFile {
            data: encoded_log(packets),
        };

        let summary = log.scan(DeviceRoute::root(), true);
        let summary = summary.summary();
        let runs = &summary.streams()
            [&StreamKey::new(DeviceRoute::root(), twinleaf_proto::StreamId::new(1))];

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
        let runs = &summary.streams()
            [&StreamKey::new(DeviceRoute::root(), twinleaf_proto::StreamId::new(1))];

        assert_eq!(summary.boundaries(BoundaryClass::Anomaly), 1);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[1].opened_by(), Some(BoundaryClass::Anomaly));
    }

    #[test]
    fn indexed_batches_carry_the_generations_of_a_linear_parse() {
        // Padding the log past a checkpoint forces the decode to resume from a
        // cloned parser state rather than from the beginning of the log.
        let filler = Packet::rpc_reply(1, &[0; 400], DeviceRoute::root()).unwrap();
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

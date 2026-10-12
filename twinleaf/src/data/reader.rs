//! Seekable and indexed access to immutable TIO log files.
//!
//! [`LogFile`] maps a log and iterates its packets. [`LogFile::scan`] builds a
//! [`LogIndex`], which summarizes the log and decodes it in batches.

use super::metadata::{ColumnRecord, DeviceRecord, SegmentRecord, StreamKey, StreamRecord};
use super::parser::{PacketParser, ParserCheckpoint};
use super::sample::{sample_time, BoundaryClass, BoundaryReason, SampleBatch};
use super::state::{PacketError, ScannedRows};
use crate::proto::data as wire;
use crate::tio::{self, Packet};
use bytes::{Buf, Bytes};
use memmap2::Mmap;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io;
use std::ops::Range;
use std::path::Path;

const INDEX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const PROGRESS_BYTES: usize = 1024 * 1024;

/// An immutable, memory-mapped TIO log.
///
/// Packet payloads share the mapping and copy no bytes. The file must not be
/// modified or truncated while the log, its index, or its packets are alive.
/// Each call to [`LogFile::packets`] creates an independent packet iterator.
///
/// # Examples
///
/// Scan a log, list its runs, then decode it in batches.
///
/// ```
/// use twinleaf::data::LogFile;
/// # use std::io::Write;
/// # use twinleaf::proto::data::{self as wire, DataType};
/// # use twinleaf::proto::sync::Epoch;
/// # use twinleaf::tio::Packet;
/// # use twinleaf::{ColumnId, DeviceRoute, SegmentId, SessionId, StreamId};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let (id, route) = (StreamId::new(1), DeviceRoute::root());
/// # let mut file = tempfile::NamedTempFile::new()?;
/// # for record in [
/// #     wire::Metadata::Device(wire::Device { session: SessionId::new(1), n_streams: 1, name: "sensor", serial: "S1", firmware: "1" }),
/// #     wire::Metadata::Stream(wire::Stream { stream_id: id, n_columns: 1, n_segments: 1, sample_size: 4, buf_samples: 128, name: "field" }),
/// #     wire::Metadata::Segment(wire::Segment { stream_id: id, segment_id: SegmentId::new(0), flags: wire::SegmentFlags::default(), epoch: Epoch::UNIX, timeref_serial: "", timeref_session: SessionId::new(1), start_time: 0, sampling_rate: 10, decimation: 1, filter_cutoff: 0.0, filter_type: wire::FilterType::NONE }),
/// #     wire::Metadata::Column(wire::Column { stream_id: id, index: ColumnId::new(0), data_type: DataType::F32, name: "x", units: "nT", description: "" }),
/// # ] {
/// #     file.write_all(Packet::metadata(record, wire::MetadataFlags::UPDATE, route)?.as_bytes())?;
/// # }
/// # let data: Vec<u8> = [1.0f32, 2.0, 3.0].iter().flat_map(|x| x.to_le_bytes()).collect();
/// # file.write_all(Packet::samples(1, 0, 0, &data, route)?.as_bytes())?;
/// # let path = file.path();
/// let log = LogFile::open(path)?;
/// let index = log.scan(false);
/// if let Some(error) = index.summary().error() {
///     eprintln!("the log stops early: {error}");
/// }
/// for (stream, runs) in index.summary().streams() {
///     for run in runs {
///         let (n, rate) = (run.sample_count(), run.rate_hz());
///         println!("{stream} run {}: {n} samples at {rate} Hz", run.run());
///     }
/// }
///
/// let mut rows = 0;
/// for batch in index.batches(4096) {
///     rows += batch?.len();
/// }
/// assert_eq!(rows, 3);
/// # Ok(())
/// # }
/// ```
pub struct LogFile {
    data: Bytes,
}

impl std::fmt::Debug for LogFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFile")
            .field("len", &self.data.len())
            .finish_non_exhaustive()
    }
}

/// An iterator over the packets of a [`LogFile`] in file order, copying no
/// payload bytes. It yields the first packet that fails to decode as an error
/// and then ends.
pub struct PacketIter {
    remaining: Bytes,
    position: usize,
    failed: bool,
}

impl std::fmt::Debug for PacketIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketIter")
            .field("position", &self.position)
            .field("remaining", &self.remaining.len())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

/// Wire or semantic failure at a byte offset in a log.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LogError {
    /// The bytes at that offset are not a well-formed packet.
    #[error("could not decode packet at byte offset {offset}: {source}")]
    Packet {
        /// Byte offset at which the undecodable packet begins.
        offset: usize,
        /// Which framing or header check rejected the bytes.
        #[source]
        source: tio::packet::DecodeError,
    },
    /// A well-formed packet contradicts the state built from earlier ones.
    #[error("invalid data at byte offset {offset}: {source}")]
    Data {
        /// Byte offset at which the rejected packet begins.
        offset: usize,
        /// Why the packet could not be applied to the data state.
        #[source]
        source: PacketError,
    },
}

impl LogError {
    /// Returns the byte offset at which the failing packet begins.
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

    /// Returns the byte offset just after the last packet returned.
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

/// Aggregate information about one run of a stream. A run is the rows
/// between two non-continuous boundaries, and its schema and timing stay
/// fixed.
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
    /// Returns the per-stream run ordinal this summary describes.
    pub fn run(&self) -> u32 {
        self.run
    }

    /// Returns the class of the boundary that started this run, or `None` if
    /// its first rows carried no boundary.
    pub fn opened_by(&self) -> Option<BoundaryClass> {
        self.opened_by
    }

    /// Returns the stream record in effect for the whole run.
    pub fn metadata(&self) -> wire::Stream<'_> {
        self.metadata.get()
    }

    /// Returns the segment record the run began in.
    pub fn segment(&self) -> wire::Segment<'_> {
        self.segment.get()
    }

    /// Returns every column of a sample, in schema order.
    pub fn columns(&self) -> impl ExactSizeIterator<Item = wire::Column<'_>> + '_ {
        self.columns.iter().map(ColumnRecord::get)
    }

    /// Returns the number of rows in this run alone.
    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Returns the end time of the run's earliest sample, in seconds after its
    /// epoch, or `None` if the run counted no rows.
    pub fn first_timestamp(&self) -> Option<f64> {
        self.first_timestamp
    }

    /// Returns the end time of the run's latest sample, in seconds after its
    /// epoch, or `None` if the run counted no rows.
    pub fn last_timestamp(&self) -> Option<f64> {
        self.last_timestamp
    }

    /// Returns the output rate in hertz after decimation, or zero for a
    /// segment whose rate or decimation is zero. The parser emits no rows from
    /// such a segment.
    pub fn rate_hz(&self) -> f64 {
        self.segment.effective_rate().unwrap_or(0.0)
    }
}

/// File-level metadata and row counts discovered without decoding sample values.
#[derive(Debug, Default)]
pub struct LogSummary {
    bytes_scanned: usize,
    packet_count: u64,
    devices: BTreeMap<crate::proto::DeviceRoute, DeviceRecord>,
    streams: BTreeMap<StreamKey, Vec<StreamSummary>>,
    /// Boundaries seen of each class, indexed by its declaration order.
    boundaries: [u64; BoundaryClass::ALL.len()],
    error: Option<LogError>,
}

impl LogSummary {
    /// Returns the bytes consumed before the scan stopped, excluding any
    /// failing packet.
    pub fn bytes_scanned(&self) -> usize {
        self.bytes_scanned
    }

    /// Returns the number of packets of every type accepted before the scan
    /// stopped.
    pub fn packet_count(&self) -> u64 {
        self.packet_count
    }

    /// Returns the devices that produced rows, in route order.
    pub fn devices(
        &self,
    ) -> impl ExactSizeIterator<Item = (crate::proto::DeviceRoute, wire::Device<'_>)> + '_ {
        self.devices
            .iter()
            .map(|(&route, device)| (route, device.get()))
    }

    /// Returns the runs of each stream, in the order they were scanned.
    pub fn streams(&self) -> &BTreeMap<StreamKey, Vec<StreamSummary>> {
        &self.streams
    }

    /// Returns the number of boundaries of `class` seen anywhere in the log.
    pub fn boundaries(&self, class: BoundaryClass) -> u64 {
        self.boundaries[class as usize]
    }

    /// Returns the failure that ended the scan early, or `None` if the log
    /// scanned to its end.
    pub fn error(&self) -> Option<&LogError> {
        self.error.as_ref()
    }

    fn observe_rows(&mut self, rows: &ScannedRows<'_>) {
        self.devices
            .entry(rows.stream_key().route)
            .or_insert_with(|| rows.device().clone());

        if let Some(boundary) = rows.boundary() {
            self.boundaries[boundary.class() as usize] += 1;
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

/// The result of a first pass over a log. It summarizes the log and decodes
/// it in batches.
///
/// The index stores a parser checkpoint about every 16 MiB of log. Each range
/// between checkpoints decodes from the metadata and continuity state in
/// effect at its start.
pub struct LogIndex {
    data: Bytes,
    summary: LogSummary,
    chunks: Vec<IndexedChunk>,
}

impl std::fmt::Debug for LogIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogIndex")
            .field("len", &self.data.len())
            .field("summary", &self.summary)
            .field("chunks", &self.chunks.len())
            .finish_non_exhaustive()
    }
}

impl LogIndex {
    /// Returns what the indexing scan found, including any error that ended it
    /// early.
    pub fn summary(&self) -> &LogSummary {
        &self.summary
    }

    /// Decodes the indexed ranges in file order, in batches of about
    /// `batch_rows` rows.
    ///
    /// The end of a range may end a batch early but adds no data boundary.
    /// Sample values and their order within each stream are the same as in a
    /// sequential decode. The iterator yields the error that stops decoding
    /// and then ends.
    ///
    /// # Panics
    ///
    /// Panics if `batch_rows` is zero.
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

    /// Returns the number of indexed ranges, one per checkpoint.
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
    /// Memory-maps the log at `path`. The file must stay unmodified while the
    /// log, its index, or its packets are alive.
    ///
    /// # Errors
    ///
    /// Returns the I/O error if the file cannot be opened or mapped.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: callers must keep log files immutable while the mapping is
        // open, as documented on `LogFile`.
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Bytes::from_owner(mmap);
        Ok(Self { data })
    }

    /// Returns the length of the mapped log in bytes.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the mapped log contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns an iterator over the log's packets from the beginning, copying
    /// no payload bytes.
    pub fn packets(&self) -> PacketIter {
        PacketIter::new(self.data.clone(), 0)
    }

    /// Scans the log's packets, metadata, boundaries, and row counts without
    /// decoding sample values. `ignore_session` is passed to
    /// [`PacketParser::new`]. A packet that fails ends the scan, and
    /// [`LogSummary::error`] reports it.
    pub fn scan(&self, ignore_session: bool) -> LogIndex {
        self.scan_with_progress(ignore_session, |_| {})
    }

    /// Scans the log as [`scan`](Self::scan) does, and reports the byte
    /// position reached to `progress` about every MiB and once at the end.
    pub fn scan_with_progress(
        &self,
        ignore_session: bool,
        mut progress: impl FnMut(usize),
    ) -> LogIndex {
        let mut packets = self.packets();
        let mut parser = PacketParser::new(ignore_session);
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
    use crate::data::sample::Generations;
    use crate::data::state::StreamDataError;
    use crate::proto::data::DataType;
    use crate::proto::DeviceRoute;

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

        let index = log.scan(true);
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

        let summary = log.scan(true);
        let summary = summary.summary();
        let runs = &summary.streams()
            [&StreamKey::new(DeviceRoute::root(), crate::proto::StreamId::new(1))];

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

        let summary = log.scan(true);
        let summary = summary.summary();
        let runs = &summary.streams()
            [&StreamKey::new(DeviceRoute::root(), crate::proto::StreamId::new(1))];

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
        let index = log.scan(true);
        assert!(index.chunk_count() > 1, "the log must span two checkpoints");
        let indexed: Vec<Generations> = index
            .batches(1)
            .map(|batch| batch.expect("valid log").generations())
            .collect();

        let mut parser = PacketParser::new(true).with_batch_rows(1);
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

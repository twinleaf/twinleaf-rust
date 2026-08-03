//! Seekable and indexed access to immutable TIO log files.

use super::parser::PacketParser;
use super::sample::{BoundaryReason, SampleBatch};
use super::state::{PacketEvent, ParseState, ValidatedRows};
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

/// Sequential packet reader backed by an immutable memory-mapped log.
///
/// Packet payloads share the mapping instead of copying their bytes. The input
/// file must therefore not be modified or truncated while the reader is open.
/// [`LogReader::scan`] builds a compact first-pass index without moving the
/// low-level packet cursor.
pub struct LogReader {
    data: Bytes,
    remaining: Bytes,
}

/// Failure that ended an index scan before the end of the file.
#[derive(Debug, thiserror::Error)]
#[error("could not parse packet at byte offset {offset}: {source}")]
pub struct LogScanError {
    offset: usize,
    #[source]
    source: tio::proto::Error,
}

impl LogScanError {
    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn packet_error(&self) -> &tio::proto::Error {
        &self.source
    }
}

/// Aggregate information about one stream discovered during the first pass.
#[derive(Debug)]
pub struct StreamSummary {
    metadata: Arc<StreamMetadata>,
    segment: Arc<SegmentMetadata>,
    columns: Vec<Arc<ColumnMetadata>>,
    sample_count: u64,
    first_timestamp: Option<f64>,
    last_timestamp: Option<f64>,
}

impl StreamSummary {
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
    streams: BTreeMap<StreamKey, StreamSummary>,
    session_changes: u64,
    segment_changes: u64,
    error: Option<LogScanError>,
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

    pub fn streams(&self) -> &BTreeMap<StreamKey, StreamSummary> {
        &self.streams
    }

    pub fn session_changes(&self) -> u64 {
        self.session_changes
    }

    pub fn segment_changes(&self) -> u64 {
        self.segment_changes
    }

    pub fn error(&self) -> Option<&LogScanError> {
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

        let stream = self
            .streams
            .entry(StreamKey::new(route, stream_id))
            .or_insert_with(|| StreamSummary {
                metadata: rows.stream.clone(),
                segment: rows.segment.clone(),
                columns: rows.columns.to_vec(),
                sample_count: 0,
                first_timestamp: None,
                last_timestamp: None,
            });
        stream.sample_count += rows.row_count as u64;

        let first_n = rows.first_sample_n.wrapping_add(1);
        let last_n = rows
            .first_sample_n
            .wrapping_add(rows.row_count.saturating_sub(1) as u32)
            .wrapping_add(1);
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

        if let Some(boundary) = &rows.boundary {
            match boundary.reason {
                BoundaryReason::SessionChanged { .. } => self.session_changes += 1,
                BoundaryReason::SegmentChanged { .. } => self.segment_changes += 1,
                _ => {}
            }
        }
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
    ) -> impl Iterator<Item = Result<SampleBatch, tio::proto::Error>> + '_ {
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
    type Item = Result<SampleBatch, tio::proto::Error>;

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
) -> Result<Vec<SampleBatch>, tio::proto::Error> {
    let mut remaining = data.slice(chunk.bytes.clone());
    let mut parser = PacketParser::from_state(chunk.state.clone()).with_batch_rows(batch_rows);
    let mut batches = Vec::new();

    while !remaining.is_empty() {
        let (packet, len) = Packet::deserialize_bytes(&remaining)?;
        remaining.advance(len);
        parser.push_packet(&packet);
        while let Some(batch) = parser.next_batch() {
            batches.push(batch);
        }
    }
    batches.extend(parser.finish());
    Ok(batches)
}

impl LogReader {
    /// Memory-map a log for zero-copy packet access.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: callers must keep log files immutable while a reader is open,
        // as documented on `LogReader`.
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Bytes::from_owner(mmap);
        let remaining = data.clone();
        Ok(Self { data, remaining })
    }

    /// Total byte length of the mapped log.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the mapped log contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Byte offset immediately after the last packet returned by
    /// [`LogReader::next_packet`].
    pub fn position(&self) -> usize {
        self.data.len() - self.remaining.len()
    }

    /// Reset the low-level packet cursor. Scanning and indexed batches do not
    /// use or modify this cursor.
    pub fn rewind(&mut self) {
        self.remaining = self.data.clone();
    }

    /// Decode the next packet without copying its variable-length payload.
    pub fn next_packet(&mut self) -> Result<Option<Packet>, tio::proto::Error> {
        if self.remaining.is_empty() {
            return Ok(None);
        }
        let (packet, len) = Packet::deserialize_bytes(&self.remaining)?;
        self.remaining.advance(len);
        Ok(Some(packet))
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
        let mut remaining = self.data.clone();
        let mut state = ParseState::new(root_route, ignore_session);
        let mut summary = LogSummary::default();
        let mut chunks = Vec::new();
        let mut chunk_start = 0;
        let mut chunk_state = state.clone();
        let mut next_progress = PROGRESS_BYTES;

        while !remaining.is_empty() {
            let packet_offset = self.data.len() - remaining.len();
            let (packet, len) = match Packet::deserialize_bytes(&remaining) {
                Ok(decoded) => decoded,
                Err(source) => {
                    summary.error = Some(LogScanError {
                        offset: packet_offset,
                        source,
                    });
                    break;
                }
            };
            remaining.advance(len);
            summary.packet_count += 1;

            if let PacketEvent::Rows {
                route,
                stream_id,
                rows,
            } = state.apply_packet(&packet)
            {
                summary.observe_rows(route, stream_id, &rows);
            }

            let position = self.data.len() - remaining.len();
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

        let scanned = self.data.len() - remaining.len();
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

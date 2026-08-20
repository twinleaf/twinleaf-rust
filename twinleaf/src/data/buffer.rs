//! Bounded retention of the newest samples of every live stream.
//!
//! [`Buffer`] keeps one [`Run`] per stream, and a run retains its rows as
//! frozen [`SampleBatch`]es: incoming batches are merged into chunks by a
//! [`BatchCoalescer`], and retention drops the oldest rows by slicing the
//! front chunk. Reads hand back slices of the retained chunks and copy at
//! most the still-accumulating rows they ask for.

use crate::data::{BatchCoalescer, Generations, SampleBatch};
use crate::tio::proto::identifiers::{ColumnId, ColumnKey, SampleNumber, StreamKey};
use crate::tio::proto::meta::{ColumnMetadata, SegmentMetadata, StreamMetadata};

use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    sync::Arc,
    time::Instant,
};

/// The newest samples of every stream that has delivered data, each capped at
/// `capacity` rows.
pub struct Buffer {
    capacity: usize,
    runs: HashMap<StreamKey, Run>,
}

impl Buffer {
    pub fn new(capacity: usize) -> Buffer {
        Buffer {
            capacity,
            runs: HashMap::new(),
        }
    }

    /// Retain `batch`'s rows, starting a new run when the parser stamped a new
    /// per-stream generation. Empty batches are ignored.
    pub fn process_batch(&mut self, batch: &SampleBatch) {
        if batch.is_empty() {
            return;
        }
        let stream_key = StreamKey::new(batch.route(), batch.stream().stream_id);
        match self.runs.get_mut(&stream_key) {
            Some(run) if run.generations.stream == batch.generations().stream => run.append(batch),
            _ => {
                self.runs.insert(stream_key, Run::new(batch, self.capacity));
            }
        }
    }

    pub fn get_run(&self, stream_key: &StreamKey) -> Option<&Run> {
        self.runs.get(stream_key)
    }

    /// Iterate the streams currently holding a run, for callers sizing UI
    /// state (e.g. column widths) across all streams.
    pub fn stream_keys(&self) -> impl Iterator<Item = &StreamKey> {
        self.runs.keys()
    }

    /// The stream's newest retained sample as a one-row batch: its metadata
    /// plus every column's last value, cheap enough to build once per render
    /// frame. `None` if the stream has no run or the run retains no rows.
    pub fn latest_row(&self, stream_key: &StreamKey) -> Option<SampleBatch> {
        self.runs.get(stream_key)?.last_row()
    }

    /// A column's metadata as of its stream's newest schema.
    pub fn column_metadata(&self, col: &ColumnKey) -> Option<Arc<ColumnMetadata>> {
        self.runs
            .get(&col.stream_key())?
            .column_metadata(col.column_id)
    }
}

/// One stream's samples since the parser last split its run, oldest row first.
///
/// Rows are appended into a [`BatchCoalescer`], which merges the small live
/// batches into chunks of about `capacity / 16` rows; the rows it still holds
/// are the run's tail, readable without completing them.
pub struct Run {
    /// The continuity generations stamped on every batch of this run.
    generations: Generations,
    stream: Arc<StreamMetadata>,
    segment: Arc<SegmentMetadata>,
    /// Column metadata in schema order, constant within a run.
    columns: Vec<Arc<ColumnMetadata>>,
    /// Completed chunks in row order; the coalescer holds the rows after them.
    chunks: VecDeque<SampleBatch>,
    coalescer: BatchCoalescer,
    /// Exclusive logical position of the next row appended to this run.
    next_row: u64,
    /// Retained rows, the tail included.
    rows: usize,
    capacity: usize,
    last_seen: Instant,
}

impl Run {
    fn new(batch: &SampleBatch, capacity: usize) -> Run {
        let mut run = Run {
            generations: batch.generations(),
            stream: batch.stream().clone(),
            segment: batch.segment().clone(),
            columns: batch
                .schema()
                .iter()
                .map(|column| column.metadata().clone())
                .collect(),
            chunks: VecDeque::new(),
            coalescer: BatchCoalescer::new(Some((capacity / 16).clamp(256, 65_536))),
            next_row: 0,
            rows: 0,
            capacity,
            last_seen: Instant::now(),
        };
        run.append(batch);
        run
    }

    fn append(&mut self, batch: &SampleBatch) {
        debug_assert_eq!(
            batch.schema().len(),
            self.columns.len(),
            "the parser guarantees a constant schema within a run"
        );
        self.coalescer.push_batch(batch);
        self.take_completed_chunks();

        self.stream = batch.stream().clone();
        self.segment = batch.segment().clone();
        self.next_row += batch.len() as u64;
        self.rows += batch.len();
        self.last_seen = Instant::now();
        self.evict();
    }

    fn take_completed_chunks(&mut self) {
        while let Some(chunk) = self.coalescer.next_completed_batch() {
            self.chunks.push_back(chunk);
        }
    }

    /// Drop the oldest rows down to `capacity`, slicing the front chunk so
    /// retention stays row-exact.
    fn evict(&mut self) {
        let Some(mut excess) = self.rows.checked_sub(self.capacity) else {
            return;
        };
        if self.rows - self.coalescer.buffered_len() < excess {
            // The rows to drop reach into the tail, so freeze it first.
            self.coalescer.finish_buffered_batch();
            self.take_completed_chunks();
        }
        while excess > 0 {
            let front = self.chunks.front_mut().expect("chunks cover the excess");
            let dropped = excess.min(front.len());
            if dropped == front.len() {
                self.chunks.pop_front();
            } else {
                *front = front.slice(dropped..front.len());
            }
            self.rows -= dropped;
            excess -= dropped;
        }
    }

    /// The retained rows within `rows`, as contiguous spans in row order.
    pub(crate) fn row_spans(&self, rows: Range<u64>) -> Vec<SampleBatch> {
        let retained = self.retained_rows();
        let (first, last) = (rows.start.max(retained.start), rows.end.min(retained.end));
        if first >= last {
            return Vec::new();
        }
        let start = (first - retained.start) as usize;
        let end = (last - retained.start) as usize;

        let mut spans = Vec::new();
        let mut offset = 0;
        for batch in &self.chunks {
            let from = start.max(offset);
            let to = end.min(offset + batch.len());
            if from < to {
                spans.push(batch.slice(from - offset..to - offset));
            }
            offset += batch.len();
        }
        if let Some(tail) = self.coalescer.tail() {
            let (from, to) = (start.max(offset), end.min(offset + tail.sample_numbers.len()));
            if from < to {
                spans.push(tail.freeze_rows(from - offset..to - offset));
            }
        }
        spans
    }

    /// The newest retained row, as a one-row batch.
    fn last_row(&self) -> Option<SampleBatch> {
        if let Some(tail) = self.coalescer.tail() {
            let rows = tail.sample_numbers.len();
            return Some(tail.freeze_rows(rows - 1..rows));
        }
        let chunk = self.chunks.back()?;
        Some(chunk.slice(chunk.len() - 1..chunk.len()))
    }

    fn column_metadata(&self, column_id: ColumnId) -> Option<Arc<ColumnMetadata>> {
        self.columns
            .iter()
            .find(|column| column.index == column_id)
            .cloned()
    }

    pub fn generations(&self) -> Generations {
        self.generations
    }

    /// Wall-clock time this run last received a batch, refreshed on every
    /// [`Buffer::process_batch`] update. Drives staleness display for callers
    /// like the monitor TUI.
    pub fn last_seen(&self) -> Instant {
        self.last_seen
    }

    pub fn stream(&self) -> &Arc<StreamMetadata> {
        &self.stream
    }

    /// The run's newest segment metadata (sampling rate, decimation, ...),
    /// e.g. for callers that need the raw ints rather than
    /// [`Self::effective_rate`].
    pub fn segment(&self) -> &Arc<SegmentMetadata> {
        &self.segment
    }

    /// Samples per second after decimation.
    pub fn effective_rate(&self) -> f64 {
        self.segment.sampling_rate as f64 / self.segment.decimation as f64
    }

    /// End-of-sample time of the newest retained row.
    pub fn last_timestamp(&self) -> Option<f64> {
        self.last_row().map(|row| row.timestamps()[0])
    }

    pub fn last_sample_number(&self) -> Option<SampleNumber> {
        self.last_row().map(|row| row.sample_numbers()[0])
    }

    /// The logical positions of the rows still retained, counted from the
    /// run's first row.
    pub fn retained_rows(&self) -> Range<u64> {
        self.next_row - self.rows as u64..self.next_row
    }
}

use crate::data::{ColumnData, SampleBatch};
use crate::tio::proto::identifiers::*;
use crate::tio::proto::meta::MetadataEpoch;
use crate::tio::proto::{BufferType, ColumnMetadata, SegmentMetadata, StreamMetadata};

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

pub type RunId = u64;

#[derive(Debug, Clone)]
pub enum ColumnVec {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
}

impl ColumnVec {
    /// Empty vec of the variant selected by a column's resolved buffer type. The
    /// one place the `data_type -> buffer_type -> variant` choice lives on the
    /// write path, mirroring [`ColumnBuffer::new`].
    pub fn empty_for(bt: BufferType) -> Self {
        Self::with_capacity_for(bt, 0)
    }

    /// Empty vec of the selected variant with room for `capacity` decoded values.
    pub fn with_capacity_for(bt: BufferType, capacity: usize) -> Self {
        match bt {
            BufferType::Float => Self::F64(Vec::with_capacity(capacity)),
            BufferType::Int => Self::I64(Vec::with_capacity(capacity)),
            BufferType::UInt => Self::U64(Vec::with_capacity(capacity)),
        }
    }

    /// Append one decoded cell, widening `Int` into a `Float` column and dropping
    /// variant mismatches (same tolerance as [`ColumnBuffer::extend`]).
    pub fn push_data(&mut self, v: &ColumnData) {
        match (self, v) {
            (Self::F64(d), ColumnData::Float(x)) => d.push(*x),
            (Self::F64(d), ColumnData::Int(x)) => d.push(*x as f64),
            (Self::I64(d), ColumnData::Int(x)) => d.push(*x),
            (Self::U64(d), ColumnData::UInt(x)) => d.push(*x),
            _ => {}
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::F64(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::U64(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read one cell as a scalar [`ColumnData`]; `Unknown` when out of bounds.
    pub fn get(&self, i: usize) -> ColumnData {
        match self {
            Self::F64(v) => v
                .get(i)
                .map_or(ColumnData::Unknown, |x| ColumnData::Float(*x)),
            Self::I64(v) => v
                .get(i)
                .map_or(ColumnData::Unknown, |x| ColumnData::Int(*x)),
            Self::U64(v) => v
                .get(i)
                .map_or(ColumnData::Unknown, |x| ColumnData::UInt(*x)),
        }
    }
}

/// Sub-slice a logical sequence stored as two contiguous halves (`a` then `b`,
/// e.g. the two slices of a `VecDeque`) at `[start, start + count)`.
///
/// This is the ONE place the ring-seam math lives. Returns up to two borrowed
/// slices whose concatenation is the requested range. The second slice is empty
/// when the range lies entirely within a single half.
///
/// Precondition: `start + count <= a.len() + b.len()`; callers guarantee this by
/// keeping `count <= total len`.
pub fn clip<'a, T>(a: &'a [T], b: &'a [T], start: usize, count: usize) -> (&'a [T], &'a [T]) {
    let alen = a.len();
    if start >= alen {
        let s = start - alen;
        (&b[s..s + count], &[])
    } else if start + count <= alen {
        (&a[start..start + count], &[])
    } else {
        let head = &a[start..];
        (head, &b[..count - head.len()])
    }
}

/// Clamp `(start, count)` to a sequence of length `len` so the range stays in
/// bounds (`start <= len` and `start + count <= len`).
///
/// Reads derive `(start, count)` from the stream's timestamp deque, but a column
/// buffer can be shorter than that deque: `RunBuffer::push_batch` creates column
/// buffers lazily, so a column first seen on a later sample stays permanently
/// shorter. Clamping keeps `view()`/`get_range` graceful (a short batch that then
/// hits the `InsufficientData` length check) instead of letting `clip` panic out
/// of bounds, matching the old `skip().take()` truncation.
fn clamp_range(len: usize, start: usize, count: usize) -> (usize, usize) {
    let start = start.min(len);
    let count = count.min(len - start);
    (start, count)
}

/// Borrowed twin of [`ColumnVec`]. Each variant carries the two halves of the
/// underlying ring buffer (`VecDeque::as_slices`), already clipped to a window.
#[derive(Debug, Clone, Copy)]
pub enum ColumnView<'a> {
    F64(&'a [f64], &'a [f64]),
    I64(&'a [i64], &'a [i64]),
    U64(&'a [u64], &'a [u64]),
}

impl ColumnView<'_> {
    pub fn len(&self) -> usize {
        match self {
            Self::F64(a, b) => a.len() + b.len(),
            Self::I64(a, b) => a.len() + b.len(),
            Self::U64(a, b) => a.len() + b.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_owned(&self) -> ColumnVec {
        match self {
            Self::F64(a, b) => {
                let mut v = Vec::with_capacity(a.len() + b.len());
                v.extend_from_slice(a);
                v.extend_from_slice(b);
                ColumnVec::F64(v)
            }
            Self::I64(a, b) => {
                let mut v = Vec::with_capacity(a.len() + b.len());
                v.extend_from_slice(a);
                v.extend_from_slice(b);
                ColumnVec::I64(v)
            }
            Self::U64(a, b) => {
                let mut v = Vec::with_capacity(a.len() + b.len());
                v.extend_from_slice(a);
                v.extend_from_slice(b);
                ColumnVec::U64(v)
            }
        }
    }
}

/// Borrowing read of a single column's most-recent samples, returned by
/// [`Buffer::column_window_last_n`]. Holds borrows into the ring buffer; reduce
/// (e.g. decimate) before the borrow ends rather than storing it.
pub struct ColumnWindow<'a> {
    pub run_id: RunId,
    pub effective_rate: f64,
    pub timestamps: (&'a [f64], &'a [f64]),
    pub values: ColumnView<'a>,
    pub column_metadata: Arc<ColumnMetadata>,
}

/// Owned snapshot of a stream's newest sample: metadata Arcs plus the last
/// value of each column, cheap enough to build per render frame. O(columns).
pub struct LatestRow {
    pub stream: Arc<StreamMetadata>,
    pub segment: Arc<SegmentMetadata>,
    pub last_seen: Instant,
    /// Ordered by column id.
    pub columns: Vec<(Arc<ColumnMetadata>, ColumnData)>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("no active run for stream {stream_key:?}")]
    NoActiveRun { stream_key: StreamKey },
    #[error("column {column_id:?} not found in stream {stream_key:?}")]
    ColumnNotFound {
        stream_key: StreamKey,
        column_id: ColumnId,
    },
    #[error("cursor invalidated for stream {stream_key:?}: cursor at run {cursor_run:?}, current run is {current_run:?}")]
    CursorInvalidated {
        stream_key: StreamKey,
        cursor_run: RunId,
        current_run: RunId,
    },
    #[error("cursor out of buffer for stream {stream_key:?}: at sample {cursor_sample:?}, earliest available is {earliest_available:?}")]
    CursorOutOfBuffer {
        stream_key: StreamKey,
        cursor_sample: SampleNumber,
        earliest_available: SampleNumber,
    },
}

#[derive(Debug)]
enum ColumnBuffer {
    F64 {
        metadata: Arc<ColumnMetadata>,
        data: VecDeque<f64>,
    },
    I64 {
        metadata: Arc<ColumnMetadata>,
        data: VecDeque<i64>,
    },
    U64 {
        metadata: Arc<ColumnMetadata>,
        data: VecDeque<u64>,
    },
}

impl ColumnBuffer {
    fn new(metadata: Arc<ColumnMetadata>, capacity: usize) -> Self {
        let alloc = capacity.min(65_536);
        match metadata.data_type.buffer_type() {
            BufferType::Float => Self::F64 {
                metadata,
                data: VecDeque::with_capacity(alloc),
            },
            BufferType::Int => Self::I64 {
                metadata,
                data: VecDeque::with_capacity(alloc),
            },
            BufferType::UInt => Self::U64 {
                metadata,
                data: VecDeque::with_capacity(alloc),
            },
        }
    }

    fn metadata(&self) -> &Arc<ColumnMetadata> {
        match self {
            Self::F64 { metadata, .. }
            | Self::I64 { metadata, .. }
            | Self::U64 { metadata, .. } => metadata,
        }
    }

    fn extend(&mut self, values: &ColumnVec) {
        match (self, values) {
            (Self::F64 { data, .. }, ColumnVec::F64(v)) => data.extend(v.iter().copied()),
            (Self::F64 { data, .. }, ColumnVec::I64(v)) => data.extend(v.iter().map(|&x| x as f64)),
            (Self::I64 { data, .. }, ColumnVec::I64(v)) => data.extend(v.iter().copied()),
            (Self::U64 { data, .. }, ColumnVec::U64(v)) => data.extend(v.iter().copied()),
            _ => {}
        }
    }

    fn pop_front(&mut self) {
        match self {
            Self::F64 { data, .. } => {
                data.pop_front();
            }
            Self::I64 { data, .. } => {
                data.pop_front();
            }
            Self::U64 { data, .. } => {
                data.pop_front();
            }
        }
    }

    fn view(&self, start: usize, count: usize) -> ColumnView<'_> {
        match self {
            Self::F64 { data, .. } => {
                let (a, b) = data.as_slices();
                let (start, count) = clamp_range(a.len() + b.len(), start, count);
                let (p, q) = clip(a, b, start, count);
                ColumnView::F64(p, q)
            }
            Self::I64 { data, .. } => {
                let (a, b) = data.as_slices();
                let (start, count) = clamp_range(a.len() + b.len(), start, count);
                let (p, q) = clip(a, b, start, count);
                ColumnView::I64(p, q)
            }
            Self::U64 { data, .. } => {
                let (a, b) = data.as_slices();
                let (start, count) = clamp_range(a.len() + b.len(), start, count);
                let (p, q) = clip(a, b, start, count);
                ColumnView::U64(p, q)
            }
        }
    }

    /// The most recent cell as an owned scalar, or `None` if the column is empty.
    /// A lighter-weight alternative to `view` for callers that only need the
    /// newest value (e.g. [`Buffer::latest_row`]).
    fn last_data(&self) -> Option<ColumnData> {
        match self {
            Self::F64 { data, .. } => data.back().map(|&x| ColumnData::Float(x)),
            Self::I64 { data, .. } => data.back().map(|&x| ColumnData::Int(x)),
            Self::U64 { data, .. } => data.back().map(|&x| ColumnData::UInt(x)),
        }
    }
}

struct RunBuffer {
    stream_metadata: Arc<StreamMetadata>,
    segment_metadata: Arc<SegmentMetadata>,
    sample_numbers: VecDeque<SampleNumber>,
    timestamps: VecDeque<f64>,
    columns: HashMap<ColumnId, ColumnBuffer>,
    capacity: usize,
}

impl RunBuffer {
    fn new(batch: &SampleBatch, capacity: usize) -> Self {
        let alloc = capacity.min(65_536);
        Self {
            stream_metadata: batch.stream.clone(),
            segment_metadata: batch.segment.clone(),
            sample_numbers: VecDeque::with_capacity(alloc),
            timestamps: VecDeque::with_capacity(alloc),
            columns: HashMap::new(),
            capacity,
        }
    }

    fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    fn push_batch(&mut self, batch: &SampleBatch) {
        self.sample_numbers
            .extend(batch.sample_numbers.iter().copied());
        self.timestamps.extend(
            batch
                .sample_numbers
                .iter()
                .map(|&n| batch.segment.time_at(n + 1)),
        );
        self.segment_metadata = batch.segment.clone();

        for col in &batch.columns {
            self.columns
                .entry(col.index)
                .or_insert_with(|| ColumnBuffer::new(col.metadata.clone(), self.capacity))
                .extend(&col.values);
        }
    }

    fn pop_front(&mut self) {
        self.sample_numbers.pop_front();
        self.timestamps.pop_front();
        for col in self.columns.values_mut() {
            col.pop_front();
        }
    }

    fn sample_number_wraps(&self) -> bool {
        match (self.sample_numbers.front(), self.sample_numbers.back()) {
            (Some(first), Some(last)) => first > last,
            _ => false,
        }
    }

    fn find_start_after_sample(&self, sample_number: SampleNumber) -> Option<usize> {
        if self.sample_numbers.is_empty() {
            return None;
        }

        if !self.sample_number_wraps() {
            let start = self
                .sample_numbers
                .partition_point(|&sn| sn <= sample_number);
            if start == 0 || self.sample_numbers.get(start - 1).copied()? != sample_number {
                return None;
            }
            return Some(start);
        }

        self.sample_numbers
            .iter()
            .rposition(|&sn| sn == sample_number)
            .map(|idx| idx + 1)
    }
}

pub struct ActiveRun {
    pub run_id: RunId,
    pub session_id: SessionId,
    pub segment_id: SegmentId,
    pub effective_rate: f64,
    pub time_ref_epoch: MetadataEpoch,
    pub last_sample_number: SampleNumber,
    pub last_timestamp: f64,
    /// Wall-clock time this run last received a batch, refreshed on every
    /// [`Buffer::process_batch`] update. Drives staleness display for callers
    /// like the monitor TUI (see [`LatestRow::last_seen`]).
    pub last_seen: Instant,
    buffer: RunBuffer,
}

impl ActiveRun {
    /// The run's current segment metadata (sampling rate, decimation, ...),
    /// e.g. for callers that need the raw ints rather than [`Self::effective_rate`].
    pub fn segment(&self) -> &Arc<SegmentMetadata> {
        &self.buffer.segment_metadata
    }

    fn new(run_id: RunId, batch: &SampleBatch, last_n: SampleNumber, capacity: usize) -> Self {
        let segment = &batch.segment;
        Self {
            run_id,
            session_id: batch.device.session_id,
            segment_id: segment.segment_id,
            effective_rate: segment.sampling_rate as f64 / segment.decimation as f64,
            time_ref_epoch: segment.time_ref_epoch.clone(),
            last_sample_number: last_n,
            last_timestamp: segment.time_at(last_n + 1),
            last_seen: Instant::now(),
            buffer: RunBuffer::new(batch, capacity),
        }
    }
}

pub struct Buffer {
    capacity: usize,
    active_runs: HashMap<StreamKey, ActiveRun>,
    next_run_id: RunId,
}

impl Buffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            active_runs: HashMap::new(),
            next_run_id: 0,
        }
    }

    pub fn process_batch(&mut self, batch: &SampleBatch, stream_key: StreamKey) {
        let Some(last_n) = batch.last_sample() else {
            return;
        };
        let needs_new_run = !batch.is_continuous() || !self.active_runs.contains_key(&stream_key);

        if needs_new_run {
            let new_run_id = self.next_run_id;
            self.next_run_id += 1;
            self.active_runs.insert(
                stream_key,
                ActiveRun::new(new_run_id, batch, last_n, self.capacity),
            );
        }

        let active = self.active_runs.get_mut(&stream_key).unwrap();
        active.buffer.push_batch(batch);
        active.last_sample_number = last_n;
        active.last_timestamp = batch.segment.time_at(last_n + 1);
        active.last_seen = Instant::now();
        active.segment_id = batch.segment.segment_id;

        while active.buffer.len() > self.capacity {
            active.buffer.pop_front();
        }
    }

    pub fn get_run(&self, stream_key: &StreamKey) -> Option<&ActiveRun> {
        self.active_runs.get(stream_key)
    }

    /// Iterate the streams currently holding an active run, for callers sizing
    /// UI state (e.g. column widths) across all streams.
    pub fn stream_keys(&self) -> impl Iterator<Item = &StreamKey> {
        self.active_runs.keys()
    }

    /// Owned snapshot of a stream's newest sample (metadata plus each column's
    /// last value), cheap enough to build once per render frame. `None` if the
    /// stream has no active run or the run has no samples yet.
    pub fn latest_row(&self, stream_key: &StreamKey) -> Option<LatestRow> {
        let run = self.active_runs.get(stream_key)?;
        let buf = &run.buffer;
        if buf.timestamps.is_empty() {
            return None;
        }
        let mut columns: Vec<(ColumnId, Arc<ColumnMetadata>, ColumnData)> = buf
            .columns
            .iter()
            .filter_map(|(&id, col)| {
                col.last_data()
                    .map(|data| (id, col.metadata().clone(), data))
            })
            .collect();
        columns.sort_by_key(|(id, _, _)| *id);

        Some(LatestRow {
            stream: buf.stream_metadata.clone(),
            segment: buf.segment_metadata.clone(),
            last_seen: run.last_seen,
            columns: columns.into_iter().map(|(_, m, d)| (m, d)).collect(),
        })
    }

    /// Borrowing read of the most recent `n` samples of a single column.
    ///
    /// Returns borrows directly into the ring buffer (no copy). The caller must
    /// reduce the window before the borrow ends; do not store the
    /// [`ColumnWindow`] long term.
    pub fn column_window_last_n(&self, col: &ColumnKey, n: usize) -> Option<ColumnWindow<'_>> {
        let run = self.active_runs.get(&col.stream_key())?;
        let buf = &run.buffer;
        let len = buf.timestamps.len();
        if len == 0 {
            return None;
        }
        let count = n.min(len);
        let start = len - count;
        let col_buf = buf.columns.get(&col.column_id)?;
        let (ta, tb) = buf.timestamps.as_slices();
        let timestamps = clip(ta, tb, start, count);
        let values = col_buf.view(start, count);
        Some(ColumnWindow {
            run_id: run.run_id,
            effective_rate: run.effective_rate,
            timestamps,
            values,
            column_metadata: col_buf.metadata().clone(),
        })
    }

    /// Borrowing read of a single column over a wall-clock time range
    /// `[start_time, end_time]` (inclusive). Returns borrows into the ring (no
    /// copy); reduce before the borrow ends. `None` if the stream/column is
    /// absent or no samples fall in the range. Single-column and
    /// lossy-friendly: it imposes no cross-stream alignment, so multiple
    /// channels can each be read over the SAME window and then overlaid on a
    /// shared time axis.
    pub fn column_window_time_range(
        &self,
        col: &ColumnKey,
        start_time: f64,
        end_time: f64,
    ) -> Option<ColumnWindow<'_>> {
        let (start_time, end_time) = if start_time <= end_time {
            (start_time, end_time)
        } else {
            (end_time, start_time)
        };
        let run = self.active_runs.get(&col.stream_key())?;
        let buf = &run.buffer;
        if buf.timestamps.is_empty() {
            return None;
        }
        let start = buf.timestamps.partition_point(|&t| t < start_time);
        let end = buf.timestamps.partition_point(|&t| t <= end_time);
        if start >= end {
            return None;
        }
        let count = end - start;
        let col_buf = buf.columns.get(&col.column_id)?;
        let (ta, tb) = buf.timestamps.as_slices();
        let timestamps = clip(ta, tb, start, count);
        let values = col_buf.view(start, count);
        Some(ColumnWindow {
            run_id: run.run_id,
            effective_rate: run.effective_rate,
            timestamps,
            values,
            column_metadata: col_buf.metadata().clone(),
        })
    }

    /// Borrowing read of a single column's samples strictly after `after`
    /// within run `run_id`, for driving incremental consumers (see
    /// [`crate::data::DerivedColumn`]). Returns borrows into the ring (no
    /// copy); reduce before the borrow ends.
    ///
    /// `Ok(None)` means the cursor is caught up: no samples past `after` have
    /// arrived yet. The two error cases are distinguished so a caller can
    /// tell a run restart from stale retention apart:
    /// - [`ReadError::NoActiveRun`]: the stream has no active run (or the
    ///   buffer holds no streams at all).
    /// - [`ReadError::CursorInvalidated`]: the stream's active run is not
    ///   `run_id` (it restarted since the cursor was taken).
    /// - [`ReadError::ColumnNotFound`]: the run exists but this column has
    ///   never been seen on it.
    /// - [`ReadError::CursorOutOfBuffer`]: `after` is not in the run's
    ///   sample-number sequence, i.e. it has aged out of the ring (or is
    ///   otherwise not a sample this run ever produced).
    pub fn column_window_after(
        &self,
        col: &ColumnKey,
        run_id: RunId,
        after: SampleNumber,
    ) -> Result<Option<ColumnWindow<'_>>, ReadError> {
        let stream_key = col.stream_key();
        let run = self
            .active_runs
            .get(&stream_key)
            .ok_or(ReadError::NoActiveRun { stream_key })?;
        if run.run_id != run_id {
            return Err(ReadError::CursorInvalidated {
                stream_key,
                cursor_run: run_id,
                current_run: run.run_id,
            });
        }
        let buf = &run.buffer;
        let col_buf = buf
            .columns
            .get(&col.column_id)
            .ok_or(ReadError::ColumnNotFound {
                stream_key,
                column_id: col.column_id,
            })?;
        let Some(start) = buf.find_start_after_sample(after) else {
            let earliest_available = buf.sample_numbers.front().copied().unwrap_or(after);
            return Err(ReadError::CursorOutOfBuffer {
                stream_key,
                cursor_sample: after,
                earliest_available,
            });
        };
        let count = buf.len() - start;
        if count == 0 {
            return Ok(None);
        }
        let (ta, tb) = buf.timestamps.as_slices();
        let timestamps = clip(ta, tb, start, count);
        let values = col_buf.view(start, count);
        Ok(Some(ColumnWindow {
            run_id: run.run_id,
            effective_rate: run.effective_rate,
            timestamps,
            values,
            column_metadata: col_buf.metadata().clone(),
        }))
    }
}

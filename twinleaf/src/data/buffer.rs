use crate::data::{ColumnData, SampleBatch};
use crate::tio::proto::identifiers::*;
use crate::tio::proto::meta::MetadataEpoch;
use crate::tio::proto::{BufferType, ColumnMetadata, DeviceRoute, SegmentMetadata, StreamMetadata};

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

pub type RunId = u64;

/// A caller-held bookmark into a single stream's run: the run it was read
/// from, plus the last sample number consumed. Reads keyed on a stale
/// `run_id` (a new run started) or a `last_sample_number` that has aged out
/// of the ring both surface as [`ReadError::CursorInvalidated`] /
/// [`ReadError::CursorOutOfBuffer`], never a silent gap or replay.
#[derive(Debug, Clone, Copy)]
pub struct CursorPosition {
    pub run_id: RunId,
    pub last_sample_number: SampleNumber,
}

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

#[derive(Debug, Clone)]
pub struct AlignedWindow {
    pub sample_numbers: HashMap<StreamKey, Vec<SampleNumber>>,
    pub timestamps: Vec<f64>,
    pub columns: HashMap<ColumnKey, ColumnVec>,
    pub stream_metadata: HashMap<StreamKey, Arc<StreamMetadata>>,
    pub segment_metadata: HashMap<StreamKey, Arc<SegmentMetadata>>,
    pub column_metadata: HashMap<ColumnKey, Arc<ColumnMetadata>>,
    pub session_ids: HashMap<StreamKey, SessionId>,
    pub run_ids: HashMap<StreamKey, RunId>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("no columns requested")]
    NoColumnsRequested,
    #[error("no cursor for stream {stream_key:?}")]
    NoCursorForStream { stream_key: StreamKey },
    #[error("no active run for stream {stream_key:?}")]
    NoActiveRun { stream_key: StreamKey },
    #[error(
        "insufficient data for stream {stream_key:?}: requested {requested}, available {available}"
    )]
    InsufficientData {
        stream_key: StreamKey,
        requested: usize,
        available: usize,
    },
    #[error("no data in time range [{requested_start}, {requested_end}]")]
    NoDataInTimeRange {
        requested_start: f64,
        requested_end: f64,
    },
    #[error("requested range [{requested_start}, {requested_end}] exceeds retention window [{available_start}, {available_end}]")]
    RequestedRangeExceedsRetention {
        requested_start: f64,
        requested_end: f64,
        available_start: f64,
        available_end: f64,
    },
    #[error("column {column_id:?} not found in stream {stream_key:?}")]
    ColumnNotFound {
        stream_key: StreamKey,
        column_id: ColumnId,
    },
    #[error("sampling rate mismatch across streams {streams:?} at rates {rates:?}")]
    SamplingRateMismatch {
        streams: Vec<StreamKey>,
        rates: Vec<f64>,
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

    fn get_range(&self, start: usize, count: usize) -> ColumnVec {
        self.view(start, count).to_owned()
    }
}

struct RunBuffer {
    run_id: RunId,
    session_id: SessionId,
    stream_metadata: Arc<StreamMetadata>,
    segment_metadata: Arc<SegmentMetadata>,
    sample_numbers: VecDeque<SampleNumber>,
    timestamps: VecDeque<f64>,
    columns: HashMap<ColumnId, ColumnBuffer>,
    capacity: usize,
}

impl RunBuffer {
    fn new(run_id: RunId, batch: &SampleBatch, capacity: usize) -> Self {
        let alloc = capacity.min(65_536);
        Self {
            run_id,
            session_id: batch.device.session_id,
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

    fn timestamps_range(&self, start: usize, count: usize) -> Vec<f64> {
        let (a, b) = self.timestamps.as_slices();
        let (p, q) = clip(a, b, start, count);
        let mut v = Vec::with_capacity(p.len() + q.len());
        v.extend_from_slice(p);
        v.extend_from_slice(q);
        v
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
    buffer: RunBuffer,
}

impl ActiveRun {
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
            buffer: RunBuffer::new(run_id, batch, capacity),
        }
    }
}

pub struct Buffer {
    capacity: usize,
    active_runs: HashMap<StreamKey, ActiveRun>,
    next_run_id: RunId,
}

enum AlignmentMode<'a> {
    LastN(usize),
    FromCursors {
        cursors: &'a HashMap<StreamKey, CursorPosition>,
        n: usize,
    },
    CommonTail,
    TimeRange {
        start: f64,
        end: f64,
    },
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
                stream_key.clone(),
                ActiveRun::new(new_run_id, batch, last_n, self.capacity),
            );
        }

        let active = self.active_runs.get_mut(&stream_key).unwrap();
        active.buffer.push_batch(batch);
        active.last_sample_number = last_n;
        active.last_timestamp = batch.segment.time_at(last_n + 1);
        active.segment_id = batch.segment.segment_id;

        while active.buffer.len() > self.capacity {
            active.buffer.pop_front();
        }
    }

    pub fn get_run(&self, stream_key: &StreamKey) -> Option<&ActiveRun> {
        self.active_runs.get(stream_key)
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
    /// absent or no samples fall in the range. Unlike [`Self::read_aligned_time_range`]
    /// this is single-column and lossy-friendly: it imposes no cross-stream
    /// alignment, so multiple channels can each be read over the SAME window and
    /// then overlaid on a shared time axis.
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
            .ok_or(ReadError::NoActiveRun {
                stream_key: stream_key.clone(),
            })?;
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
                stream_key: stream_key.clone(),
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

    pub fn read_aligned_window(
        &self,
        columns: &[ColumnKey],
        n: usize,
    ) -> Result<AlignedWindow, ReadError> {
        let by_stream = self.prepare_stream_selection(columns)?;
        let (slices, timestamps) =
            self.compute_aligned_slices(&by_stream, AlignmentMode::LastN(n))?;
        self.build_window_from_slices(&by_stream, &slices, timestamps)
    }

    pub fn read_from_cursor(
        &self,
        columns: &[ColumnKey],
        cursors: &HashMap<StreamKey, CursorPosition>,
        n: usize,
    ) -> Result<AlignedWindow, ReadError> {
        let by_stream = self.prepare_stream_selection(columns)?;
        let (slices, timestamps) =
            self.compute_aligned_slices(&by_stream, AlignmentMode::FromCursors { cursors, n })?;
        self.build_window_from_slices(&by_stream, &slices, timestamps)
    }

    pub fn read_aligned_tail(&self, columns: &[ColumnKey]) -> Result<AlignedWindow, ReadError> {
        let by_stream = self.prepare_stream_selection(columns)?;
        let (slices, timestamps) =
            self.compute_aligned_slices(&by_stream, AlignmentMode::CommonTail)?;
        self.build_window_from_slices(&by_stream, &slices, timestamps)
    }

    pub fn read_aligned_time_range(
        &self,
        columns: &[ColumnKey],
        start_time: f64,
        end_time: f64,
    ) -> Result<AlignedWindow, ReadError> {
        let by_stream = self.prepare_stream_selection(columns)?;
        let (slices, timestamps) = self.compute_aligned_slices(
            &by_stream,
            AlignmentMode::TimeRange {
                start: start_time,
                end: end_time,
            },
        )?;
        self.build_window_from_slices(&by_stream, &slices, timestamps)
            .map_err(|err| match err {
                ReadError::InsufficientData { .. } => ReadError::NoDataInTimeRange {
                    requested_start: start_time.min(end_time),
                    requested_end: start_time.max(end_time),
                },
                other => other,
            })
    }

    fn compute_aligned_slices(
        &self,
        by_stream: &HashMap<StreamKey, Vec<ColumnId>>,
        mode: AlignmentMode<'_>,
    ) -> Result<(HashMap<StreamKey, (usize, usize)>, Vec<f64>), ReadError> {
        match mode {
            AlignmentMode::LastN(n) => {
                let ref_key = Self::reference_stream_key(by_stream);
                let ref_buf = self.active_buffer(ref_key)?;
                let available = ref_buf.len();
                if available == 0 {
                    return Err(ReadError::InsufficientData {
                        stream_key: ref_key.clone(),
                        requested: n,
                        available: 0,
                    });
                }
                let count = n.min(available);
                let start = available.saturating_sub(count);
                let timestamps = ref_buf.timestamps_range(start, count);
                let slices = by_stream
                    .keys()
                    .map(|k| (k.clone(), (start, count)))
                    .collect();
                Ok((slices, timestamps))
            }

            AlignmentMode::FromCursors { cursors, n } => {
                let mut start = 0;
                let mut reference_key: Option<StreamKey> = None;

                for stream_key in by_stream.keys() {
                    let active = self.active_run(stream_key)?;
                    let cursor = cursors
                        .get(stream_key)
                        .ok_or(ReadError::NoCursorForStream {
                            stream_key: stream_key.clone(),
                        })?;
                    if cursor.run_id != active.run_id {
                        return Err(ReadError::CursorInvalidated {
                            stream_key: stream_key.clone(),
                            cursor_run: cursor.run_id,
                            current_run: active.run_id,
                        });
                    }
                    let buf = &active.buffer;
                    if buf.sample_numbers.is_empty() {
                        return Err(ReadError::InsufficientData {
                            stream_key: stream_key.clone(),
                            requested: n,
                            available: 0,
                        });
                    }
                    let s = buf
                        .find_start_after_sample(cursor.last_sample_number)
                        .ok_or(ReadError::CursorOutOfBuffer {
                            stream_key: stream_key.clone(),
                            cursor_sample: cursor.last_sample_number,
                            earliest_available: *buf.sample_numbers.front().unwrap(),
                        })?;
                    if s + n > buf.len() {
                        return Err(ReadError::InsufficientData {
                            stream_key: stream_key.clone(),
                            requested: n,
                            available: buf.len().saturating_sub(s),
                        });
                    }
                    if reference_key.is_none() {
                        start = s;
                        reference_key = Some(stream_key.clone());
                    }
                }

                let ref_key = reference_key.unwrap();
                let ref_buf = self.active_buffer(&ref_key)?;
                let timestamps = ref_buf.timestamps_range(start, n);
                let slices = by_stream.keys().map(|k| (k.clone(), (start, n))).collect();
                Ok((slices, timestamps))
            }

            AlignmentMode::CommonTail => {
                let mut global_start = f64::MIN;
                let mut global_end = f64::MAX;

                for stream_key in by_stream.keys() {
                    let buf = self.active_buffer(stream_key)?;
                    if buf.timestamps.is_empty() {
                        return Err(ReadError::InsufficientData {
                            stream_key: stream_key.clone(),
                            requested: 0,
                            available: 0,
                        });
                    }
                    let first = *buf.timestamps.front().unwrap();
                    let last = *buf.timestamps.back().unwrap();
                    global_start = global_start.max(first);
                    global_end = global_end.min(last);
                }

                if global_start >= global_end {
                    return Err(ReadError::InsufficientData {
                        stream_key: StreamKey::new(DeviceRoute::root(), 0),
                        requested: 0,
                        available: 0,
                    });
                }

                let ref_key = Self::reference_stream_key(by_stream);
                let ref_buf = self.active_buffer(ref_key)?;
                let start = ref_buf
                    .timestamps
                    .iter()
                    .position(|&t| t >= global_start)
                    .unwrap_or(0);
                let end = ref_buf
                    .timestamps
                    .iter()
                    .rposition(|&t| t <= global_end)
                    .unwrap_or(ref_buf.len().saturating_sub(1));
                let count = end.saturating_sub(start) + 1;
                let timestamps = ref_buf.timestamps_range(start, count);
                let slices = by_stream
                    .keys()
                    .map(|k| (k.clone(), (start, count)))
                    .collect();
                Ok((slices, timestamps))
            }

            AlignmentMode::TimeRange {
                start: start_time,
                end: end_time,
            } => {
                let (requested_start, requested_end) = normalize_time_bounds(start_time, end_time);

                let (available_start, available_end) = self
                    .aligned_retained_time_bounds(by_stream)?
                    .ok_or(ReadError::NoDataInTimeRange {
                        requested_start,
                        requested_end,
                    })?;

                if requested_start < available_start || requested_end > available_end {
                    return Err(ReadError::RequestedRangeExceedsRetention {
                        requested_start,
                        requested_end,
                        available_start,
                        available_end,
                    });
                }

                let ref_key = Self::reference_stream_key(by_stream);
                let ref_buf = self.active_buffer(ref_key)?;
                let ref_start = ref_buf.timestamps.partition_point(|&t| t < requested_start);
                let ref_end = ref_buf.timestamps.partition_point(|&t| t <= requested_end);
                if ref_start >= ref_end {
                    return Err(ReadError::NoDataInTimeRange {
                        requested_start,
                        requested_end,
                    });
                }

                let ref_count = ref_end - ref_start;
                let timestamps = ref_buf.timestamps_range(ref_start, ref_count);
                if timestamps.is_empty() {
                    return Err(ReadError::NoDataInTimeRange {
                        requested_start,
                        requested_end,
                    });
                }

                let mut slices = HashMap::new();
                slices.insert(ref_key.clone(), (ref_start, ref_count));

                for stream_key in by_stream.keys() {
                    if stream_key == ref_key {
                        continue;
                    }
                    let buf = self.active_buffer(stream_key)?;
                    let s = buf.timestamps.partition_point(|&t| t < requested_start);
                    let e = buf.timestamps.partition_point(|&t| t <= requested_end);
                    if s >= e {
                        return Err(ReadError::NoDataInTimeRange {
                            requested_start,
                            requested_end,
                        });
                    }
                    let count = e - s;
                    if count != ref_count {
                        return Err(ReadError::NoDataInTimeRange {
                            requested_start,
                            requested_end,
                        });
                    }
                    let stream_timestamps = buf.timestamps.iter().skip(s).take(count).copied();
                    if !timestamps_match_iter(&timestamps, stream_timestamps) {
                        return Err(ReadError::NoDataInTimeRange {
                            requested_start,
                            requested_end,
                        });
                    }
                    slices.insert(stream_key.clone(), (s, count));
                }

                Ok((slices, timestamps))
            }
        }
    }

    fn aligned_retained_time_bounds(
        &self,
        by_stream: &HashMap<StreamKey, Vec<ColumnId>>,
    ) -> Result<Option<(f64, f64)>, ReadError> {
        let mut global_start = f64::MIN;
        let mut global_end = f64::MAX;

        for stream_key in by_stream.keys() {
            let buf = self.active_buffer(stream_key)?;
            let (Some(&first), Some(&last)) = (buf.timestamps.front(), buf.timestamps.back())
            else {
                return Ok(None);
            };

            global_start = global_start.max(first);
            global_end = global_end.min(last);
        }

        if global_start > global_end {
            return Ok(None);
        }

        Ok(Some((global_start, global_end)))
    }

    fn build_window_from_slices(
        &self,
        by_stream: &HashMap<StreamKey, Vec<ColumnId>>,
        slices: &HashMap<StreamKey, (usize, usize)>,
        timestamps: Vec<f64>,
    ) -> Result<AlignedWindow, ReadError> {
        let expected_len = timestamps.len();
        let mut sample_numbers = HashMap::new();
        let mut columns = HashMap::new();
        let mut stream_metadata = HashMap::new();
        let mut segment_metadata = HashMap::new();
        let mut column_metadata = HashMap::new();
        let mut session_ids = HashMap::new();
        let mut run_ids = HashMap::new();

        for (stream_key, col_ids) in by_stream {
            let buf = self.active_buffer(stream_key)?;
            let (start, count) =
                slices
                    .get(stream_key)
                    .copied()
                    .ok_or(ReadError::InsufficientData {
                        stream_key: stream_key.clone(),
                        requested: expected_len,
                        available: 0,
                    })?;

            let stream_sample_numbers: Vec<_> = buf
                .sample_numbers
                .iter()
                .skip(start)
                .take(count)
                .copied()
                .collect();
            if stream_sample_numbers.len() != expected_len {
                return Err(ReadError::InsufficientData {
                    stream_key: stream_key.clone(),
                    requested: expected_len,
                    available: stream_sample_numbers.len(),
                });
            }
            sample_numbers.insert(stream_key.clone(), stream_sample_numbers);

            stream_metadata.insert(stream_key.clone(), buf.stream_metadata.clone());
            segment_metadata.insert(stream_key.clone(), buf.segment_metadata.clone());
            session_ids.insert(stream_key.clone(), buf.session_id);
            run_ids.insert(stream_key.clone(), buf.run_id);

            for &col_id in col_ids {
                let col_buf = buf.columns.get(&col_id).ok_or(ReadError::ColumnNotFound {
                    stream_key: stream_key.clone(),
                    column_id: col_id,
                })?;

                let key = ColumnKey::new(stream_key.route.clone(), stream_key.stream_id, col_id);
                let batch = col_buf.get_range(start, count);
                if batch.len() != expected_len {
                    return Err(ReadError::InsufficientData {
                        stream_key: stream_key.clone(),
                        requested: expected_len,
                        available: batch.len(),
                    });
                }
                columns.insert(key.clone(), batch);
                column_metadata.insert(key, col_buf.metadata().clone());
            }
        }

        Ok(AlignedWindow {
            sample_numbers,
            timestamps,
            columns,
            stream_metadata,
            segment_metadata,
            column_metadata,
            session_ids,
            run_ids,
        })
    }

    fn prepare_stream_selection(
        &self,
        columns: &[ColumnKey],
    ) -> Result<HashMap<StreamKey, Vec<ColumnId>>, ReadError> {
        if columns.is_empty() {
            return Err(ReadError::NoColumnsRequested);
        }
        let by_stream = group_columns_by_stream(columns);
        self.validate_rates(&by_stream)?;
        Ok(by_stream)
    }

    fn validate_rates(
        &self,
        by_stream: &HashMap<StreamKey, Vec<ColumnId>>,
    ) -> Result<(), ReadError> {
        let mut rate: Option<f64> = None;
        let mut rates = Vec::new();

        for stream_key in by_stream.keys() {
            let active = self
                .active_runs
                .get(stream_key)
                .ok_or(ReadError::NoActiveRun {
                    stream_key: stream_key.clone(),
                })?;

            let r = active.effective_rate;
            rates.push(r);

            if let Some(first_rate) = rate {
                if (r - first_rate).abs() > 0.001 {
                    return Err(ReadError::SamplingRateMismatch {
                        streams: by_stream.keys().cloned().collect(),
                        rates,
                    });
                }
            } else {
                rate = Some(r);
            }
        }

        Ok(())
    }

    fn active_run(&self, stream_key: &StreamKey) -> Result<&ActiveRun, ReadError> {
        self.active_runs
            .get(stream_key)
            .ok_or(ReadError::NoActiveRun {
                stream_key: stream_key.clone(),
            })
    }

    fn active_buffer(&self, stream_key: &StreamKey) -> Result<&RunBuffer, ReadError> {
        Ok(&self.active_run(stream_key)?.buffer)
    }

    fn reference_stream_key<'a>(by_stream: &'a HashMap<StreamKey, Vec<ColumnId>>) -> &'a StreamKey {
        by_stream
            .keys()
            .min()
            .expect("reference stream requires at least one stream")
    }
}

fn group_columns_by_stream(columns: &[ColumnKey]) -> HashMap<StreamKey, Vec<ColumnId>> {
    let mut by_stream: HashMap<StreamKey, Vec<ColumnId>> = HashMap::new();
    for col in columns {
        by_stream
            .entry(col.stream_key())
            .or_default()
            .push(col.column_id);
    }
    by_stream
}

fn normalize_time_bounds(start_time: f64, end_time: f64) -> (f64, f64) {
    if start_time <= end_time {
        (start_time, end_time)
    } else {
        (end_time, start_time)
    }
}

fn timestamps_match_iter<I>(reference: &[f64], candidate: I) -> bool
where
    I: Iterator<Item = f64>,
{
    reference
        .iter()
        .copied()
        .zip(candidate)
        .all(|(a, b)| (a - b).abs() <= 1e-9)
}

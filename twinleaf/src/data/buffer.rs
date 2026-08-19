use crate::data::{ColumnData, Generations, SampleBatch};
use crate::tio::proto::identifiers::*;
use crate::tio::proto::meta::MetadataEpoch;
use crate::tio::proto::{BufferType, ColumnMetadata, SegmentMetadata, StreamMetadata};

use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    sync::Arc,
    time::Instant,
};

#[derive(Debug, Clone)]
pub enum ColumnVec {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
}

impl ColumnVec {
    /// Empty vec of the variant selected by a column's resolved buffer type.
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

    /// The buffer type whose columns land in this variant.
    pub fn buffer_type(&self) -> BufferType {
        match self {
            Self::F64(_) => BufferType::Float,
            Self::I64(_) => BufferType::Int,
            Self::U64(_) => BufferType::UInt,
        }
    }

    /// Append one decoded cell, widening `Int` into a `Float` column.
    ///
    /// Panics on any other variant mismatch: the cell was decoded for a
    /// different column type and dropping it would desync this column from
    /// its batch's rows.
    pub fn push_data(&mut self, v: &ColumnData) {
        match (self, v) {
            (Self::F64(d), ColumnData::Float(x)) => d.push(*x),
            (Self::F64(d), ColumnData::Int(x)) => d.push(*x as f64),
            (Self::I64(d), ColumnData::Int(x)) => d.push(*x),
            (Self::U64(d), ColumnData::UInt(x)) => d.push(*x),
            (this, v) => panic!("cannot push {v:?} into a {:?} column", this.buffer_type()),
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
    pub generations: Generations,
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
            (Self::I64 { data, .. }, ColumnVec::I64(v)) => data.extend(v.iter().copied()),
            (Self::U64 { data, .. }, ColumnVec::U64(v)) => data.extend(v.iter().copied()),
            _ => panic!("column value type changed within a run"),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::F64 { data, .. } => data.len(),
            Self::I64 { data, .. } => data.len(),
            Self::U64 { data, .. } => data.len(),
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
                let (p, q) = clip(a, b, start, count);
                ColumnView::F64(p, q)
            }
            Self::I64 { data, .. } => {
                let (a, b) = data.as_slices();
                let (p, q) = clip(a, b, start, count);
                ColumnView::I64(p, q)
            }
            Self::U64 { data, .. } => {
                let (a, b) = data.as_slices();
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
    /// Exclusive logical position of the next row appended to this run.
    next_row: u64,
    timestamps: VecDeque<f64>,
    columns: HashMap<ColumnId, ColumnBuffer>,
    capacity: usize,
}

impl RunBuffer {
    fn new(batch: &SampleBatch, capacity: usize) -> Self {
        let alloc = capacity.min(65_536);
        let columns: HashMap<_, _> = batch
            .schema()
            .iter()
            .map(|column| {
                (
                    column.index(),
                    ColumnBuffer::new(column.metadata().clone(), alloc),
                )
            })
            .collect();
        debug_assert_eq!(
            columns.len(),
            batch.schema().len(),
            "batch construction guarantees unique column ids"
        );

        let mut buffer = Self {
            stream_metadata: batch.stream().clone(),
            segment_metadata: batch.segment().clone(),
            next_row: 0,
            timestamps: VecDeque::with_capacity(alloc),
            columns,
            capacity,
        };
        buffer.append(batch);
        buffer
    }

    fn len(&self) -> usize {
        self.timestamps.len()
    }

    fn retained_rows(&self) -> Range<u64> {
        self.next_row - self.len() as u64..self.next_row
    }

    fn schema_matches(&self, batch: &SampleBatch) -> bool {
        batch.schema().len() == self.columns.len()
            && batch.schema().iter().all(|incoming| {
                self.columns
                    .get(&incoming.index())
                    .is_some_and(|stored| stored.metadata().as_ref() == incoming.metadata().as_ref())
            })
    }

    fn append(&mut self, batch: &SampleBatch) {
        debug_assert!(
            self.schema_matches(batch),
            "the parser guarantees a constant schema within a run"
        );
        self.timestamps.extend(
            batch
                .sample_numbers()
                .iter()
                .map(|&n| batch.segment().time_at(n + 1)),
        );
        self.segment_metadata = batch.segment().clone();

        for col in batch.schema() {
            self.columns
                .get_mut(&col.index())
                .expect("schema checked above")
                .extend(col.values());
        }
        self.next_row += batch.len() as u64;

        while self.len() > self.capacity {
            self.timestamps.pop_front();
            for col in self.columns.values_mut() {
                col.pop_front();
            }
        }

        debug_assert!(
            self.columns
                .values()
                .all(|column| column.len() == self.timestamps.len()),
            "all columns must cover the retained row range"
        );
    }
}

pub struct ActiveRun {
    /// The continuity generations stamped on every batch of this run.
    pub generations: Generations,
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

    fn new(batch: &SampleBatch, capacity: usize) -> Self {
        let segment = &batch.segment();
        let last_n = batch.last_sample().expect("active runs require samples");
        Self {
            generations: batch.generations(),
            session_id: batch.device().session_id,
            segment_id: segment.segment_id,
            effective_rate: segment.sampling_rate as f64 / segment.decimation as f64,
            time_ref_epoch: segment.time_ref_epoch.clone(),
            last_sample_number: last_n,
            last_timestamp: segment.time_at(last_n + 1),
            last_seen: Instant::now(),
            buffer: RunBuffer::new(batch, capacity),
        }
    }

    fn append(&mut self, batch: &SampleBatch) {
        let last_n = batch.last_sample().expect("active runs require samples");
        self.buffer.append(batch);
        self.last_sample_number = last_n;
        self.last_timestamp = batch.segment().time_at(last_n + 1);
        self.last_seen = Instant::now();
        self.segment_id = batch.segment().segment_id;
    }

    pub(crate) fn retained_rows(&self) -> Range<u64> {
        self.buffer.retained_rows()
    }

    pub(crate) fn column_window(
        &self,
        column_id: ColumnId,
        rows: Range<u64>,
    ) -> Option<ColumnWindow<'_>> {
        let retained = self.retained_rows();
        if rows.start < retained.start || rows.end > retained.end || rows.start > rows.end {
            return None;
        }

        let start = usize::try_from(rows.start - retained.start).ok()?;
        let count = usize::try_from(rows.end - rows.start).ok()?;
        let col_buf = self.buffer.columns.get(&column_id)?;
        let (ta, tb) = self.buffer.timestamps.as_slices();
        Some(ColumnWindow {
            generations: self.generations,
            effective_rate: self.effective_rate,
            timestamps: clip(ta, tb, start, count),
            values: col_buf.view(start, count),
            column_metadata: col_buf.metadata().clone(),
        })
    }
}

pub struct Buffer {
    capacity: usize,
    active_runs: HashMap<StreamKey, ActiveRun>,
}

impl Buffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            active_runs: HashMap::new(),
        }
    }

    pub fn process_batch(&mut self, batch: &SampleBatch, stream_key: StreamKey) {
        if batch.is_empty() {
            return;
        }
        let needs_new_run = self
            .active_runs
            .get(&stream_key)
            .is_none_or(|run| run.generations.stream != batch.generations().stream);

        if needs_new_run {
            self.active_runs
                .insert(stream_key, ActiveRun::new(batch, self.capacity));
        } else {
            self.active_runs
                .get_mut(&stream_key)
                .expect("active run checked above")
                .append(batch);
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
        let retained = run.retained_rows();
        if retained.is_empty() {
            return None;
        }
        let start = retained.end.saturating_sub(n as u64).max(retained.start);
        run.column_window(col.column_id, start..retained.end)
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
        let retained_start = run.retained_rows().start;
        run.column_window(
            col.column_id,
            retained_start + start as u64..retained_start + end as u64,
        )
    }
}

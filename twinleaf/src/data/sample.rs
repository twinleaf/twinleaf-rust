use crate::tio;

use std::ops::{Deref, Range};
use std::sync::Arc;
use tio::proto::identifiers::{ColumnId, SampleNumber, SegmentId, SessionId, TimeRefSessionId};
use tio::proto::meta::{ColumnMetadata, DeviceMetadata, SegmentMetadata, StreamMetadata};
use tio::proto::{BufferType, DeviceRoute};

#[derive(Debug, Clone)]
pub enum ColumnData {
    Int(i64),
    UInt(u64),
    Float(f64),
    Unknown,
}

impl ColumnData {
    pub fn try_as_f64(&self) -> Option<f64> {
        match *self {
            ColumnData::Int(i) => Some(i as f64),
            ColumnData::UInt(u) => Some(u as f64),
            ColumnData::Float(f) => Some(f),
            ColumnData::Unknown => None,
        }
    }

    pub fn from_le_bytes(data: &[u8], data_type: tio::proto::DataType) -> ColumnData {
        use tio::proto::DataType;
        match data_type {
            DataType::Int8 => ColumnData::Int(i8::from_le_bytes([data[0]]).into()),
            DataType::UInt8 => ColumnData::UInt(data[0].into()),
            DataType::Int16 => ColumnData::Int(i16::from_le_bytes([data[0], data[1]]).into()),
            DataType::UInt16 => ColumnData::UInt(u16::from_le_bytes([data[0], data[1]]).into()),
            DataType::Int24 => {
                let sign = if data[2] & 0x80 == 0 { 0 } else { 0xff };
                ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], sign]).into())
            }
            DataType::UInt24 => {
                ColumnData::UInt(u32::from_le_bytes([data[0], data[1], data[2], 0]).into())
            }
            DataType::Int32 => {
                ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], data[3]]).into())
            }
            DataType::UInt32 => {
                ColumnData::UInt(u32::from_le_bytes([data[0], data[1], data[2], data[3]]).into())
            }
            DataType::Int64 => ColumnData::Int(i64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])),
            DataType::UInt64 => ColumnData::UInt(u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])),
            DataType::Float32 => {
                ColumnData::Float(f32::from_le_bytes([data[0], data[1], data[2], data[3]]).into())
            }
            DataType::Float64 => ColumnData::Float(f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])),
            DataType::Unknown(_) => ColumnData::Unknown,
        }
    }
}

impl std::fmt::Display for ColumnData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ColumnData::Int(x) => write!(f, "{}", x),
            ColumnData::UInt(x) => write!(f, "{}", x),
            ColumnData::Float(x) => write!(f, "{}", x),
            ColumnData::Unknown => write!(f, "?"),
        }
    }
}

/// A frozen `Vec<T>`, shared by [`Arc`] and narrowed to a row range. Cloning
/// and [`ScalarBuffer::slice`] copy no values.
#[derive(Debug)]
pub struct ScalarBuffer<T> {
    data: Arc<Vec<T>>,
    range: Range<usize>,
}

impl<T> ScalarBuffer<T> {
    /// Sub-slice `range`, relative to this slice's own start.
    pub fn slice(&self, range: Range<usize>) -> ScalarBuffer<T> {
        assert!(
            range.start <= range.end && range.end <= self.range.len(),
            "slice range out of bounds"
        );
        let start = self.range.start + range.start;
        ScalarBuffer {
            data: self.data.clone(),
            range: start..start + range.len(),
        }
    }
}

impl<T> From<Vec<T>> for ScalarBuffer<T> {
    fn from(values: Vec<T>) -> ScalarBuffer<T> {
        ScalarBuffer {
            range: 0..values.len(),
            data: Arc::new(values),
        }
    }
}

impl<T> Clone for ScalarBuffer<T> {
    fn clone(&self) -> ScalarBuffer<T> {
        ScalarBuffer {
            data: self.data.clone(),
            range: self.range.clone(),
        }
    }
}

impl<T> Deref for ScalarBuffer<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.data[self.range.clone()]
    }
}

/// One column's decoded values while they are still being accumulated; freeze
/// into a [`ColumnArray`] to share them.
#[derive(Debug, Clone)]
pub enum ColumnBuilder {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
}

impl ColumnBuilder {
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

    /// Copy a frozen column's values onto the end of this one. Panics on a
    /// variant mismatch: the two columns describe different data.
    pub fn extend_from(&mut self, values: &ColumnArray) {
        match (self, values) {
            (Self::F64(d), ColumnArray::F64(v)) => d.extend_from_slice(v),
            (Self::I64(d), ColumnArray::I64(v)) => d.extend_from_slice(v),
            (Self::U64(d), ColumnArray::U64(v)) => d.extend_from_slice(v),
            (this, v) => panic!(
                "cannot extend a {:?} column from a {:?} one",
                this.buffer_type(),
                v.buffer_type()
            ),
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
                .map_or(ColumnData::Unknown, |&x| ColumnData::Float(x)),
            Self::I64(v) => v
                .get(i)
                .map_or(ColumnData::Unknown, |&x| ColumnData::Int(x)),
            Self::U64(v) => v
                .get(i)
                .map_or(ColumnData::Unknown, |&x| ColumnData::UInt(x)),
        }
    }

    /// Freeze a copy of `rows` only, leaving this column accumulating.
    pub(crate) fn freeze_rows(&self, rows: Range<usize>) -> ColumnArray {
        match self {
            Self::F64(v) => ColumnArray::F64(v[rows].to_vec().into()),
            Self::I64(v) => ColumnArray::I64(v[rows].to_vec().into()),
            Self::U64(v) => ColumnArray::U64(v[rows].to_vec().into()),
        }
    }
}

/// Frozen twin of [`ColumnBuilder`]: one column's decoded values, shared and
/// sliceable. Derefs to a plain slice within each variant.
#[derive(Debug, Clone)]
pub enum ColumnArray {
    F64(ScalarBuffer<f64>),
    I64(ScalarBuffer<i64>),
    U64(ScalarBuffer<u64>),
}

impl ColumnArray {
    /// The buffer type whose columns land in this variant.
    pub fn buffer_type(&self) -> BufferType {
        match self {
            Self::F64(_) => BufferType::Float,
            Self::I64(_) => BufferType::Int,
            Self::U64(_) => BufferType::UInt,
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
                .map_or(ColumnData::Unknown, |&x| ColumnData::Float(x)),
            Self::I64(v) => v
                .get(i)
                .map_or(ColumnData::Unknown, |&x| ColumnData::Int(x)),
            Self::U64(v) => v
                .get(i)
                .map_or(ColumnData::Unknown, |&x| ColumnData::UInt(x)),
        }
    }

    /// Narrow to `rows` without copying values.
    pub fn slice(&self, rows: Range<usize>) -> ColumnArray {
        match self {
            Self::F64(v) => Self::F64(v.slice(rows)),
            Self::I64(v) => Self::I64(v.slice(rows)),
            Self::U64(v) => Self::U64(v.slice(rows)),
        }
    }
}

impl From<ColumnBuilder> for ColumnArray {
    fn from(values: ColumnBuilder) -> ColumnArray {
        match values {
            ColumnBuilder::F64(v) => ColumnArray::F64(v.into()),
            ColumnBuilder::I64(v) => ColumnArray::I64(v.into()),
            ColumnBuilder::U64(v) => ColumnArray::U64(v.into()),
        }
    }
}

/// An immutable batch of samples in columnar (Structure-of-Arrays) form:
/// metadata is held once, and each column's decoded values live in a shared
/// [`ColumnArray`]. Build one incrementally with [`SampleBatchBuilder`].
#[derive(Debug, Clone)]
pub struct SampleBatch {
    route: DeviceRoute,
    /// At most one boundary per batch, anchored at its first row.
    boundary: Option<Boundary>,
    generations: Generations,
    sample_numbers: ScalarBuffer<SampleNumber>,
    /// Each row's end-of-sample time, materialized at construction.
    timestamps: ScalarBuffer<f64>,
    /// Columns in index order.
    columns: Vec<Series>,
    segment: Arc<SegmentMetadata>,
    stream: Arc<StreamMetadata>,
    device: Arc<DeviceMetadata>,
}

#[derive(Debug, Clone)]
pub struct Series {
    index: ColumnId,
    metadata: Arc<ColumnMetadata>,
    values: ColumnArray,
}

impl Series {
    pub fn new(index: ColumnId, metadata: Arc<ColumnMetadata>, values: ColumnBuilder) -> Series {
        let values = ColumnArray::from(values);
        assert_eq!(
            values.buffer_type(),
            metadata.data_type.buffer_type(),
            "column values must use the variant selected by their metadata"
        );
        Series {
            index,
            metadata,
            values,
        }
    }

    pub fn index(&self) -> ColumnId {
        self.index
    }

    pub fn metadata(&self) -> &Arc<ColumnMetadata> {
        &self.metadata
    }

    pub fn values(&self) -> &ColumnArray {
        &self.values
    }

    fn slice(&self, rows: Range<usize>) -> Series {
        Series {
            index: self.index,
            metadata: self.metadata.clone(),
            values: self.values.slice(rows),
        }
    }
}

/// Each row's end-of-sample time, the timestamp convention throughout the crate.
fn timestamps_for(segment: &SegmentMetadata, sample_numbers: &[SampleNumber]) -> ScalarBuffer<f64> {
    sample_numbers
        .iter()
        .map(|&n| segment.time_at(n + 1))
        .collect::<Vec<_>>()
        .into()
}

/// Rows that can enter a [`crate::data::BatchCoalescer`]: they describe the
/// batch they would form and can append themselves onto an accumulating builder.
pub(crate) trait RowSource {
    fn len(&self) -> usize;
    fn boundary(&self) -> Option<&Boundary>;
    fn generations(&self) -> Generations;
    fn segment(&self) -> &Arc<SegmentMetadata>;
    /// An empty builder shaped like these rows: their metadata, boundary and
    /// generations, with room for `capacity` rows.
    fn start_builder(&self, capacity: usize) -> SampleBatchBuilder;
    /// Copy every row onto the end of `tail`. Appending never checks
    /// compatibility; callers must only merge rows that share the tail's
    /// metadata and generations.
    fn append_to(&self, tail: &mut SampleBatchBuilder);
}

/// Accumulates decoded rows into a [`SampleBatch`], one row or one batch at a
/// time.
pub(crate) struct SampleBatchBuilder {
    pub(crate) route: DeviceRoute,
    pub(crate) boundary: Option<Boundary>,
    pub(crate) generations: Generations,
    pub(crate) sample_numbers: Vec<SampleNumber>,
    /// Each row's end-of-sample time, carried alongside its sample number.
    pub(crate) timestamps: Vec<f64>,
    /// One accumulating buffer per decodable column, in schema order.
    pub(crate) columns: Vec<(Arc<ColumnMetadata>, ColumnBuilder)>,
    pub(crate) segment: Arc<SegmentMetadata>,
    pub(crate) stream: Arc<StreamMetadata>,
    pub(crate) device: Arc<DeviceMetadata>,
}

impl RowSource for SampleBatch {
    fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    fn boundary(&self) -> Option<&Boundary> {
        self.boundary.as_ref()
    }

    fn generations(&self) -> Generations {
        self.generations
    }

    fn segment(&self) -> &Arc<SegmentMetadata> {
        &self.segment
    }

    fn start_builder(&self, capacity: usize) -> SampleBatchBuilder {
        SampleBatchBuilder {
            route: self.route,
            boundary: self.boundary.clone(),
            generations: self.generations,
            sample_numbers: Vec::with_capacity(capacity),
            timestamps: Vec::with_capacity(capacity),
            columns: self
                .columns
                .iter()
                .map(|column| {
                    (
                        column.metadata.clone(),
                        ColumnBuilder::with_capacity_for(column.values.buffer_type(), capacity),
                    )
                })
                .collect(),
            segment: self.segment.clone(),
            stream: self.stream.clone(),
            device: self.device.clone(),
        }
    }

    fn append_to(&self, tail: &mut SampleBatchBuilder) {
        assert_eq!(
            tail.columns.len(),
            self.columns.len(),
            "appended rows must have one value per column"
        );
        tail.sample_numbers.extend_from_slice(&self.sample_numbers);
        tail.timestamps.extend_from_slice(&self.timestamps);
        for ((_, values), column) in tail.columns.iter_mut().zip(&self.columns) {
            values.extend_from(&column.values);
        }
    }
}

impl SampleBatchBuilder {
    /// Append one decoded row: `cells` must yield exactly one value per
    /// column, in schema order.
    pub(crate) fn push_row(
        &mut self,
        n: SampleNumber,
        cells: impl IntoIterator<Item = ColumnData>,
    ) {
        self.sample_numbers.push(n);
        self.timestamps.push(self.segment.time_at(n + 1));
        let mut cells = cells.into_iter();
        for (_, values) in &mut self.columns {
            values.push_data(&cells.next().expect("one cell per column"));
        }
        debug_assert!(cells.next().is_none(), "one cell per column");
    }

    /// Freeze a copy of `rows` only, leaving the builder accumulating. The
    /// boundary is anchored at the first row, as in [`SampleBatch::slice`].
    pub(crate) fn freeze_rows(&self, rows: Range<usize>) -> SampleBatch {
        SampleBatch {
            route: self.route,
            boundary: (rows.start == 0).then(|| self.boundary.clone()).flatten(),
            generations: self.generations,
            sample_numbers: self.sample_numbers[rows.clone()].to_vec().into(),
            timestamps: self.timestamps[rows.clone()].to_vec().into(),
            columns: self
                .columns
                .iter()
                .map(|(metadata, values)| Series {
                    index: metadata.index,
                    metadata: metadata.clone(),
                    values: values.freeze_rows(rows.clone()),
                })
                .collect(),
            segment: self.segment.clone(),
            stream: self.stream.clone(),
            device: self.device.clone(),
        }
    }

    /// Freeze the accumulated rows into a shareable batch.
    pub(crate) fn finish(self) -> SampleBatch {
        SampleBatch {
            route: self.route,
            boundary: self.boundary,
            generations: self.generations,
            timestamps: self.timestamps.into(),
            sample_numbers: self.sample_numbers.into(),
            columns: self
                .columns
                .into_iter()
                .map(|(metadata, values)| Series {
                    index: metadata.index,
                    metadata,
                    values: values.into(),
                })
                .collect(),
            segment: self.segment,
            stream: self.stream,
            device: self.device,
        }
    }
}

impl SampleBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        route: DeviceRoute,
        boundary: Option<Boundary>,
        generations: Generations,
        sample_numbers: Vec<SampleNumber>,
        columns: Vec<Series>,
        segment: Arc<SegmentMetadata>,
        stream: Arc<StreamMetadata>,
        device: Arc<DeviceMetadata>,
    ) -> SampleBatch {
        assert!(
            columns
                .iter()
                .all(|c| c.values.len() == sample_numbers.len()),
            "every column must hold exactly one value per sample"
        );
        assert!(
            columns
                .iter()
                .enumerate()
                .all(|(i, c)| columns[..i].iter().all(|prior| prior.index != c.index)),
            "column ids must be unique within a batch"
        );
        SampleBatch {
            route,
            boundary,
            generations,
            timestamps: timestamps_for(&segment, &sample_numbers),
            sample_numbers: sample_numbers.into(),
            columns,
            segment,
            stream,
            device,
        }
    }

    /// A view of `rows` that shares this batch's buffers instead of copying
    /// them. The boundary is anchored at the first row, so it survives only a
    /// slice that starts there.
    pub fn slice(&self, rows: Range<usize>) -> SampleBatch {
        SampleBatch {
            route: self.route,
            boundary: (rows.start == 0).then(|| self.boundary.clone()).flatten(),
            generations: self.generations,
            sample_numbers: self.sample_numbers.slice(rows.clone()),
            timestamps: self.timestamps.slice(rows.clone()),
            columns: self.columns.iter().map(|c| c.slice(rows.clone())).collect(),
            segment: self.segment.clone(),
            stream: self.stream.clone(),
            device: self.device.clone(),
        }
    }

    pub fn route(&self) -> DeviceRoute {
        self.route
    }

    /// The batch's boundary, anchored at its first row.
    pub fn boundary(&self) -> Option<&Boundary> {
        self.boundary.as_ref()
    }

    /// The continuity generations this batch's rows belong to.
    pub fn generations(&self) -> Generations {
        self.generations
    }

    pub fn sample_numbers(&self) -> &[SampleNumber] {
        &self.sample_numbers
    }

    /// Each row's end-of-sample time, in row order.
    pub fn timestamps(&self) -> &[f64] {
        &self.timestamps
    }

    pub fn segment(&self) -> &Arc<SegmentMetadata> {
        &self.segment
    }

    pub fn stream(&self) -> &Arc<StreamMetadata> {
        &self.stream
    }

    pub fn device(&self) -> &Arc<DeviceMetadata> {
        &self.device
    }

    pub fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sample_numbers.is_empty()
    }

    pub fn first_sample(&self) -> Option<SampleNumber> {
        self.sample_numbers.first().copied()
    }

    pub fn last_sample(&self) -> Option<SampleNumber> {
        self.sample_numbers.last().copied()
    }

    /// The batch's columns, in index order. The blessed way to ask schema
    /// questions (names, types, units) without touching row values.
    pub fn schema(&self) -> &[Series] {
        &self.columns
    }

    pub fn column(&self, id: ColumnId) -> Option<&Series> {
        self.columns.iter().find(|c| c.index == id)
    }

    pub fn row(&self, row: usize) -> Option<SampleRow<'_>> {
        (row < self.len()).then_some(SampleRow { batch: self, row })
    }

    pub fn iter(&self) -> impl Iterator<Item = SampleRow<'_>> {
        (0..self.len()).map(move |row| SampleRow { batch: self, row })
    }

    /// True unless the boundary marks a break in continuity.
    pub fn is_continuous(&self) -> bool {
        self.boundary.as_ref().is_none_or(|b| b.is_continuous())
    }

    /// True unless the boundary marks a non-monotonic break.
    pub fn is_monotonic(&self) -> bool {
        self.boundary.as_ref().is_none_or(|b| b.is_monotonic())
    }

    /// True only when the boundary is the stream's first sample.
    pub fn is_initial(&self) -> bool {
        self.boundary.as_ref().is_some_and(|b| b.is_initial())
    }
}

/// Borrowing view of one row of a [`SampleBatch`].
#[derive(Clone, Copy)]
pub struct SampleRow<'a> {
    batch: &'a SampleBatch,
    row: usize,
}

impl<'a> SampleRow<'a> {
    pub fn n(&self) -> SampleNumber {
        self.batch.sample_numbers[self.row]
    }
    pub fn stream(&self) -> &'a Arc<StreamMetadata> {
        &self.batch.stream
    }
    pub fn segment(&self) -> &'a Arc<SegmentMetadata> {
        &self.batch.segment
    }
    pub fn device(&self) -> &'a Arc<DeviceMetadata> {
        &self.batch.device
    }
    pub fn timestamp_begin(&self) -> f64 {
        self.batch.segment.time_at(self.n())
    }
    pub fn timestamp_end(&self) -> f64 {
        self.batch.timestamps[self.row]
    }

    /// The row's column values in schema order.
    pub fn values(&self) -> impl Iterator<Item = ColumnData> + '_ {
        let row = self.row;
        self.batch.columns.iter().map(move |c| c.values.get(row))
    }

    /// The row's value for a single column, by index.
    pub fn value(&self, id: ColumnId) -> Option<ColumnData> {
        self.batch.column(id).map(|c| c.values.get(self.row))
    }
}

impl std::fmt::Display for SampleRow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SAMPLE({}:{}:{}) {:.6}",
            self.batch.device.session_id,
            self.batch.stream.stream_id,
            self.batch.segment.segment_id,
            self.timestamp_end()
        )?;
        for (series, value) in self.batch.schema().iter().zip(self.values()) {
            write!(f, " {}: {}", series.metadata.name, value)?;
        }
        write!(f, " [#{}]", self.n())
    }
}

/// Parser-assigned continuity generations, constant within a batch.
///
/// The parser stamps these where arrival order is authoritative, so consumers
/// can compare identity instead of re-deriving it from boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Generations {
    /// Per-stream run ordinal: bumps at every non-continuous boundary.
    pub stream: u32,
    /// Per-device generation: bumps at any of the device's streams' non-continuous,
    /// non-[`BoundaryReason::Initial`] boundaries.
    pub device: u32,
    /// Whole-parse generation: as `device`, across all routed devices.
    pub global: u32,
}

#[derive(Debug, Clone)]
pub struct Boundary {
    pub reason: BoundaryReason,
}

/// What a boundary says about the data around it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryClass {
    /// Deliberate, time-continuous segment rollover.
    Seamless,
    /// First data from a stream.
    Startup,
    /// Samples missing but the timeline is trustworthy.
    DataLoss,
    /// Session, time reference, rate, or segment reconfiguration.
    Reconfig,
    /// Timeline inconsistency. Current firmware never corrects a segment's
    /// time reference in place — sync changes always restart acquisition into
    /// a new segment — so these indicate wire corruption, a firmware clock
    /// bug, or a device predating that behavior.
    Anomaly,
}

#[derive(Debug, Clone)]
pub enum BoundaryReason {
    /// First sample from this stream
    Initial,
    /// Device session changed
    SessionChanged { old: SessionId, new: SessionId },
    /// Time reference epoch changed
    TimeRefSessionChanged {
        old: TimeRefSessionId,
        new: TimeRefSessionId,
    },
    /// Time jumped backward unexpectedly
    TimeBackward { gap_seconds: f64 },
    /// Time jumped forward unexpectedly with no gap in sample numbers, e.g. a
    /// segment's start time was corrected in place
    TimeForward { gap_seconds: f64 },
    /// Sampling rate changed
    RateChanged { old_rate: f64, new_rate: f64 },

    /// Segment rolled over (continuous, but new segment)
    SegmentRollover {
        old_id: SegmentId,
        new_id: SegmentId,
    },
    /// Segment changed unexpectedly (not a natural rollover)
    SegmentChanged {
        old_id: SegmentId,
        new_id: SegmentId,
    },
    /// Samples were lost (gap in sequence)
    SamplesLost {
        expected: SampleNumber,
        received: SampleNumber,
    },
}

impl Boundary {
    /// Only boundary where continuity is preserved
    pub fn is_continuous(&self) -> bool {
        matches!(self.reason, BoundaryReason::SegmentRollover { .. })
    }

    /// Boundaries where there may be a gap but time is still monotonic
    pub fn is_monotonic(&self) -> bool {
        matches!(
            self.reason,
            BoundaryReason::SamplesLost { .. }
                | BoundaryReason::RateChanged { .. }
                | BoundaryReason::SegmentRollover { .. }
                | BoundaryReason::SegmentChanged { .. }
                | BoundaryReason::TimeForward { .. }
        )
    }

    pub fn is_initial(&self) -> bool {
        matches!(self.reason, BoundaryReason::Initial)
    }

    /// How this boundary should be interpreted, independent of continuity and
    /// monotonicity.
    pub fn class(&self) -> BoundaryClass {
        match self.reason {
            BoundaryReason::SegmentRollover { .. } => BoundaryClass::Seamless,
            BoundaryReason::Initial => BoundaryClass::Startup,
            BoundaryReason::SamplesLost { .. } => BoundaryClass::DataLoss,
            BoundaryReason::SessionChanged { .. }
            | BoundaryReason::TimeRefSessionChanged { .. }
            | BoundaryReason::RateChanged { .. }
            | BoundaryReason::SegmentChanged { .. } => BoundaryClass::Reconfig,
            BoundaryReason::TimeForward { .. } | BoundaryReason::TimeBackward { .. } => {
                BoundaryClass::Anomaly
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tio::proto::meta::{MetadataEpoch, MetadataFilter};
    use tio::proto::DataType;

    #[test]
    fn signed_int24_values_are_sign_extended() {
        for (bytes, expected) in [
            ([0x00, 0x00, 0x00], 0),
            ([0xff, 0xff, 0x7f], 8_388_607),
            ([0xff, 0xff, 0xff], -1),
            ([0x00, 0x00, 0x80], -8_388_608),
        ] {
            let ColumnData::Int(value) = ColumnData::from_le_bytes(&bytes, DataType::Int24) else {
                panic!("expected an integer value");
            };
            assert_eq!(value, expected);
        }
    }

    /// A four-row float batch numbered 0..4, sampled at 4 Hz, with an initial
    /// boundary anchored at its first row.
    fn batch() -> SampleBatch {
        let segment = Arc::new(SegmentMetadata {
            stream_id: 1,
            segment_id: 0,
            flags: 0,
            time_ref_epoch: MetadataEpoch::Unix,
            time_ref_serial: "clock".to_string(),
            time_ref_session_id: 7,
            start_time: 0,
            sampling_rate: 4,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: MetadataFilter::Unfiltered,
        });
        let column = Arc::new(ColumnMetadata {
            stream_id: 1,
            index: 0,
            data_type: DataType::Float64,
            name: "col_0".to_string(),
            units: String::new(),
            description: String::new(),
        });
        let mut builder = SampleBatchBuilder {
            route: DeviceRoute::root(),
            boundary: Some(Boundary {
                reason: BoundaryReason::Initial,
            }),
            generations: Generations {
                stream: 1,
                device: 0,
                global: 0,
            },
            sample_numbers: Vec::new(),
            timestamps: Vec::new(),
            columns: vec![(column, ColumnBuilder::empty_for(BufferType::Float))],
            segment,
            stream: Arc::new(StreamMetadata {
                stream_id: 1,
                name: "test-stream".to_string(),
                n_columns: 1,
                n_segments: 1,
                sample_size: 8,
                buf_samples: 128,
            }),
            device: Arc::new(DeviceMetadata {
                serial_number: "SN123".to_string(),
                firmware_hash: "fw".to_string(),
                n_streams: 1,
                session_id: 42,
                name: "test-device".to_string(),
            }),
        };
        for n in 0..4 {
            builder.push_row(n, [ColumnData::Float(f64::from(n) * 10.0)]);
        }
        builder.finish()
    }

    #[test]
    fn finishing_preserves_values_and_materializes_end_of_sample_times() {
        let batch = batch();
        assert_eq!(batch.sample_numbers(), [0, 1, 2, 3]);
        match batch.schema()[0].values() {
            ColumnArray::F64(v) => assert_eq!(&v[..], [0.0, 10.0, 20.0, 30.0]),
            other => panic!("expected an f64 column, got {other:?}"),
        }
        let expected: Vec<f64> = batch
            .sample_numbers()
            .iter()
            .map(|&n| batch.segment().time_at(n + 1))
            .collect();
        assert_eq!(batch.timestamps(), expected);
        assert_eq!(batch.timestamps(), [0.25, 0.5, 0.75, 1.0]);
        assert_eq!(batch.row(2).unwrap().timestamp_end(), 0.75);
    }

    #[test]
    fn slicing_shares_the_underlying_buffers() {
        let batch = batch();
        let tail = batch.slice(1..3);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail.sample_numbers(), [1, 2]);
        assert_eq!(tail.timestamps(), [0.5, 0.75]);
        match tail.schema()[0].values() {
            ColumnArray::F64(v) => assert_eq!(&v[..], [10.0, 20.0]),
            other => panic!("expected an f64 column, got {other:?}"),
        }
        // Nothing was copied: the slice's rows are the originals in place.
        assert!(std::ptr::eq(&batch.timestamps()[1], &tail.timestamps()[0]));
        let (ColumnArray::F64(whole), ColumnArray::F64(part)) =
            (batch.schema()[0].values(), tail.schema()[0].values())
        else {
            panic!("expected f64 columns");
        };
        assert!(std::ptr::eq(&whole[1], &part[0]));
    }

    #[test]
    fn a_boundary_survives_only_a_slice_that_starts_at_its_row() {
        let batch = batch();
        assert!(batch.slice(0..2).is_initial());
        assert!(batch.slice(1..4).boundary().is_none());
    }

    #[test]
    #[should_panic(expected = "slice range out of bounds")]
    fn slicing_past_the_end_panics() {
        batch().slice(2..5);
    }
}

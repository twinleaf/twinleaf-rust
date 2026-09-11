//! The decoded sample model: routed stream and column identity, immutable
//! columnar batches, row views, and parser-reported continuity boundaries.

use super::metadata::{
    BufferType, ColumnRecord, DeviceRecord, SegmentRecord, StreamMetadataSnapshot, StreamRecord,
};
use crate::proto::data as wire;
use crate::proto::DeviceRoute;
use crate::proto::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};
use crate::tio;
use std::ops::{Deref, Range};
use std::sync::Arc;

/// A stream within one connection or parser root.
///
/// The route identifies the device relative to that enclosing source. Two
/// independent connections can therefore have the same `StreamKey`; an
/// application combining sources must pair it with its own source identity.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct StreamKey {
    /// Hops from the enclosing source down to the device, root-to-leaf.
    pub route: DeviceRoute,
    /// The stream's id on that device, 1 through 127.
    pub stream_id: StreamId,
}

impl StreamKey {
    /// A key naming one stream on one device.
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
    /// A key naming one column of one stream on one device.
    pub fn new(route: DeviceRoute, stream_id: StreamId, column_id: ColumnId) -> Self {
        Self {
            route,
            stream_id,
            column_id,
        }
    }

    /// The stream this column belongs to.
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

/// A segment's map from sample number to time, read once while constructing a
/// batch so timestamping every row does not repeatedly divide its rate.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SampleClock {
    start_time: f64,
    period: f64,
}

impl SampleClock {
    pub(crate) fn of(segment: wire::Segment<'_>) -> Self {
        Self {
            start_time: f64::from(segment.start_time),
            period: f64::from(segment.decimation) / f64::from(segment.sampling_rate),
        }
    }

    pub(crate) fn time_at(&self, n: u32) -> f64 {
        self.start_time + self.period * f64::from(n)
    }
}

pub(crate) fn sample_time(segment: wire::Segment<'_>, n: u32) -> f64 {
    SampleClock::of(segment).time_at(n)
}

/// One decoded scalar, in the storage class its column decodes to.
#[derive(Debug, Clone)]
pub enum ColumnData {
    /// Signed integer, sign-extended from its narrower wire width.
    Int(i64),
    /// Unsigned integer, zero-extended from its narrower wire width.
    UInt(u64),
    /// Floating point, widened from `f32` where the column is 32-bit.
    Float(f64),
    /// Cell whose wire data type this build cannot decode.
    Unknown,
}

impl ColumnData {
    /// `None` only for `Unknown`; wide integers lose precision in the cast.
    pub fn try_as_f64(&self) -> Option<f64> {
        match *self {
            ColumnData::Int(i) => Some(i as f64),
            ColumnData::UInt(u) => Some(u as f64),
            ColumnData::Float(f) => Some(f),
            ColumnData::Unknown => None,
        }
    }

    pub(super) fn from_le_bytes(
        data: &[u8],
        data_type: crate::proto::data::DataType,
    ) -> ColumnData {
        use crate::proto::data::DataType;
        match data_type {
            DataType::I8 => ColumnData::Int(i8::from_le_bytes([data[0]]).into()),
            DataType::U8 => ColumnData::UInt(data[0].into()),
            DataType::I16 => ColumnData::Int(i16::from_le_bytes([data[0], data[1]]).into()),
            DataType::U16 => ColumnData::UInt(u16::from_le_bytes([data[0], data[1]]).into()),
            DataType::I24 => {
                let sign = if data[2] & 0x80 == 0 { 0 } else { 0xff };
                ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], sign]).into())
            }
            DataType::U24 => {
                ColumnData::UInt(u32::from_le_bytes([data[0], data[1], data[2], 0]).into())
            }
            DataType::I32 => {
                ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], data[3]]).into())
            }
            DataType::U32 => {
                ColumnData::UInt(u32::from_le_bytes([data[0], data[1], data[2], data[3]]).into())
            }
            DataType::I64 => ColumnData::Int(i64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])),
            DataType::U64 => ColumnData::UInt(u64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])),
            DataType::F32 => {
                ColumnData::Float(f32::from_le_bytes([data[0], data[1], data[2], data[3]]).into())
            }
            DataType::F64 => ColumnData::Float(f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ])),
            _ => ColumnData::Unknown,
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

/// One column's decoded values while they are still being accumulated.
#[derive(Debug, Clone)]
enum ColumnBuilder {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
}

impl ColumnBuilder {
    /// Empty vec of the selected variant with room for `capacity` decoded values.
    fn with_capacity_for(bt: BufferType, capacity: usize) -> Self {
        match bt {
            BufferType::Float => Self::F64(Vec::with_capacity(capacity)),
            BufferType::Int => Self::I64(Vec::with_capacity(capacity)),
            BufferType::UInt => Self::U64(Vec::with_capacity(capacity)),
        }
    }

    /// The buffer type whose columns land in this variant.
    fn buffer_type(&self) -> BufferType {
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
    fn push_data(&mut self, v: &ColumnData) {
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
    fn extend_from(&mut self, values: &ColumnArray) {
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

    /// Freeze a copy of `rows` only, leaving this column accumulating.
    fn freeze_rows(&self, rows: Range<usize>) -> ColumnArray {
        match self {
            Self::F64(v) => ColumnArray::F64(v[rows].to_vec().into()),
            Self::I64(v) => ColumnArray::I64(v[rows].to_vec().into()),
            Self::U64(v) => ColumnArray::U64(v[rows].to_vec().into()),
        }
    }
}

/// One frozen column's decoded values, shared and sliceable. Derefs to a plain
/// slice within each variant.
#[derive(Debug, Clone)]
pub enum ColumnArray {
    /// Float column; signed integer cells are widened into one.
    F64(ScalarBuffer<f64>),
    /// Signed integer column, whatever its narrower wire width.
    I64(ScalarBuffer<i64>),
    /// Unsigned integer column, whatever its narrower wire width.
    U64(ScalarBuffer<u64>),
}

impl ColumnArray {
    /// The buffer type whose columns land in this variant.
    pub(crate) fn buffer_type(&self) -> BufferType {
        match self {
            Self::F64(_) => BufferType::Float,
            Self::I64(_) => BufferType::Int,
            Self::U64(_) => BufferType::UInt,
        }
    }

    /// Values in this column, one per row of its batch.
    pub fn len(&self) -> usize {
        match self {
            Self::F64(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::U64(v) => v.len(),
        }
    }

    /// True when the column holds no values.
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
/// [`ColumnArray`]. Instances are produced by packet parsers and can be cheaply
/// narrowed with [`SampleBatch::slice`].
#[derive(Debug, Clone)]
pub(super) struct BatchContext {
    key: StreamKey,
    /// At most one boundary per batch, anchored at its first row.
    boundary: Option<BoundaryReason>,
    generations: Generations,
    segment: SegmentRecord,
    stream: StreamRecord,
    device: DeviceRecord,
}

impl BatchContext {
    pub(super) fn new(
        key: StreamKey,
        boundary: Option<BoundaryReason>,
        generations: Generations,
        segment: SegmentRecord,
        stream: StreamRecord,
        device: DeviceRecord,
    ) -> Self {
        assert_eq!(
            key.stream_id,
            stream.get().stream_id,
            "stream key must match metadata"
        );
        assert_eq!(
            key.stream_id,
            segment.get().stream_id,
            "segment must match stream"
        );
        Self {
            key,
            boundary,
            generations,
            segment,
            stream,
            device,
        }
    }
}

/// Rows of one stream from one device, held column-major.
///
/// Immutable and cloned by pointer. Metadata accessors return borrowed
/// protocol views of the records the parser retained, so reading them
/// copies nothing and stays byte-faithful to what the instrument sent.
#[derive(Debug, Clone)]
pub struct SampleBatch {
    context: BatchContext,
    sample_numbers: ScalarBuffer<SampleNumber>,
    /// Each row's end-of-sample time, materialized at construction.
    timestamps: ScalarBuffer<f64>,
    /// Columns in index order.
    columns: Vec<Series>,
}

/// One column of a batch: its metadata and its decoded values.
#[derive(Debug, Clone)]
pub struct Series {
    metadata: ColumnRecord,
    values: ColumnArray,
}

impl Series {
    /// The column's position within the stream's packed sample.
    pub fn index(&self) -> ColumnId {
        self.metadata.get().index
    }

    /// Borrowed view of the column record the parser retained.
    pub fn metadata(&self) -> wire::Column<'_> {
        self.metadata.get()
    }

    pub(crate) fn record(&self) -> &ColumnRecord {
        &self.metadata
    }

    /// The column's decoded values, in row order.
    pub fn values(&self) -> &ColumnArray {
        &self.values
    }

    fn slice(&self, rows: Range<usize>) -> Series {
        Series {
            metadata: self.metadata.clone(),
            values: self.values.slice(rows),
        }
    }
}

/// Rows that can enter a batch coalescer: they describe the
/// batch they would form and can append themselves onto an accumulating builder.
pub(super) trait RowSource {
    fn len(&self) -> usize;
    fn boundary(&self) -> Option<&BoundaryReason>;
    fn generations(&self) -> Generations;
    fn segment_record(&self) -> &SegmentRecord;
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
pub(super) struct SampleBatchBuilder {
    context: BatchContext,
    sample_numbers: Vec<SampleNumber>,
    /// Each row's end-of-sample time, carried alongside its sample number.
    timestamps: Vec<f64>,
    /// The segment's clock, read once because `push_row` runs per row.
    clock: SampleClock,
    /// One accumulating buffer per decodable column, in schema order.
    columns: Vec<(ColumnRecord, ColumnBuilder)>,
}

impl RowSource for SampleBatch {
    fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    fn boundary(&self) -> Option<&BoundaryReason> {
        self.context.boundary.as_ref()
    }

    fn generations(&self) -> Generations {
        self.context.generations
    }

    fn segment_record(&self) -> &SegmentRecord {
        &self.context.segment
    }

    fn start_builder(&self, capacity: usize) -> SampleBatchBuilder {
        SampleBatchBuilder::new(
            self.context.clone(),
            self.columns
                .iter()
                .map(|column| (column.metadata.clone(), column.values.buffer_type())),
            capacity,
        )
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
    pub(super) fn new(
        context: BatchContext,
        columns: impl IntoIterator<Item = (ColumnRecord, BufferType)>,
        capacity: usize,
    ) -> Self {
        let key = context.key;
        let columns = columns
            .into_iter()
            .map(|(metadata, buffer_type)| {
                assert_eq!(
                    key.stream_id,
                    metadata.get().stream_id,
                    "column must match stream"
                );
                (
                    metadata,
                    ColumnBuilder::with_capacity_for(buffer_type, capacity),
                )
            })
            .collect();
        Self {
            clock: SampleClock::of(context.segment.get()),
            context,
            sample_numbers: Vec::with_capacity(capacity),
            timestamps: Vec::with_capacity(capacity),
            columns,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    pub(super) fn generations(&self) -> Generations {
        self.context.generations
    }

    pub(super) fn accepts(&self, rows: &impl RowSource) -> bool {
        self.context.generations == rows.generations()
            && self.context.segment == *rows.segment_record()
    }

    /// Append one decoded row: `cells` must yield exactly one value per
    /// column, in schema order.
    pub(super) fn push_row(
        &mut self,
        n: SampleNumber,
        cells: impl IntoIterator<Item = ColumnData>,
    ) {
        self.sample_numbers.push(n);
        self.timestamps.push(self.clock.time_at(n.value() + 1));
        let mut cells = cells.into_iter();
        for (_, values) in &mut self.columns {
            values.push_data(&cells.next().expect("one cell per column"));
        }
        debug_assert!(cells.next().is_none(), "one cell per column");
    }

    /// Freeze a copy of `rows` only, leaving the builder accumulating. The
    /// boundary is anchored at the first row, as in [`SampleBatch::slice`].
    pub(super) fn freeze_rows(&self, rows: Range<usize>) -> SampleBatch {
        let mut context = self.context.clone();
        if rows.start != 0 {
            context.boundary = None;
        }
        SampleBatch {
            context,
            sample_numbers: self.sample_numbers[rows.clone()].to_vec().into(),
            timestamps: self.timestamps[rows.clone()].to_vec().into(),
            columns: self
                .columns
                .iter()
                .map(|(metadata, values)| Series {
                    metadata: metadata.clone(),
                    values: values.freeze_rows(rows.clone()),
                })
                .collect(),
        }
    }

    /// Freeze the accumulated rows into a shareable batch.
    pub(super) fn finish(self) -> SampleBatch {
        SampleBatch {
            context: self.context,
            timestamps: self.timestamps.into(),
            sample_numbers: self.sample_numbers.into(),
            columns: self
                .columns
                .into_iter()
                .map(|(metadata, values)| Series {
                    metadata,
                    values: values.into(),
                })
                .collect(),
        }
    }
}

impl SampleBatch {
    /// A view of `rows` that shares this batch's buffers instead of copying
    /// them. The boundary is anchored at the first row, so it survives only a
    /// slice that starts there.
    pub fn slice(&self, rows: Range<usize>) -> SampleBatch {
        let mut context = self.context.clone();
        if rows.start != 0 {
            context.boundary = None;
        }
        SampleBatch {
            context,
            sample_numbers: self.sample_numbers.slice(rows.clone()),
            timestamps: self.timestamps.slice(rows.clone()),
            columns: self.columns.iter().map(|c| c.slice(rows.clone())).collect(),
        }
    }

    /// Hops to the device that produced these rows, root-to-leaf.
    pub fn route(&self) -> DeviceRoute {
        self.context.key.route
    }

    /// Which routed stream these rows came from.
    pub fn stream_key(&self) -> StreamKey {
        self.context.key
    }

    /// The batch's boundary, anchored at its first row.
    pub fn boundary(&self) -> Option<&BoundaryReason> {
        self.context.boundary.as_ref()
    }

    /// The continuity generations this batch's rows belong to.
    pub fn generations(&self) -> Generations {
        self.context.generations
    }

    /// Each row's counter within its segment, in row order.
    pub fn sample_numbers(&self) -> &[SampleNumber] {
        &self.sample_numbers
    }

    /// Each row's end-of-sample time, in row order.
    pub fn timestamps(&self) -> &[f64] {
        &self.timestamps
    }

    /// Borrowed view of the segment these rows were sampled in.
    pub fn segment(&self) -> wire::Segment<'_> {
        self.context.segment.get()
    }

    /// Borrowed view of the stream record these rows belong to.
    pub fn stream(&self) -> wire::Stream<'_> {
        self.context.stream.get()
    }

    /// Borrowed view of the record of the device that sent these rows.
    pub fn device(&self) -> wire::Device<'_> {
        self.context.device.get()
    }

    /// Retain this batch's metadata without retaining its sample arrays.
    pub fn metadata(&self) -> StreamMetadataSnapshot {
        StreamMetadataSnapshot::new(
            self.context.key,
            self.context.device.clone(),
            self.context.stream.clone(),
            self.context.segment.clone(),
            self.columns
                .iter()
                .map(|series| series.metadata.clone())
                .collect(),
        )
    }

    pub(crate) fn records(&self) -> (&DeviceRecord, &StreamRecord, &SegmentRecord) {
        (
            &self.context.device,
            &self.context.stream,
            &self.context.segment,
        )
    }

    /// Encode a complete, byte-faithful metadata snapshot for this batch. See
    /// [`StreamMetadataSnapshot::metadata_packets`].
    pub fn metadata_packets(&self) -> Result<Vec<tio::Packet>, tio::packet::EncodeError> {
        self.metadata().metadata_packets()
    }

    /// Rows in this batch, matching every column's length.
    pub fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    /// True when the batch carries no rows.
    pub fn is_empty(&self) -> bool {
        self.sample_numbers.is_empty()
    }

    /// First row's sample number; `None` when the batch is empty.
    pub fn first_sample(&self) -> Option<SampleNumber> {
        self.sample_numbers.first().copied()
    }

    /// Last row's sample number; `None` when the batch is empty.
    pub fn last_sample(&self) -> Option<SampleNumber> {
        self.sample_numbers.last().copied()
    }

    /// The batch's columns, in index order. The blessed way to ask schema
    /// questions (names, types, units) without touching row values.
    pub fn schema(&self) -> &[Series] {
        &self.columns
    }

    /// One column by its schema index; `None` when the schema has no such column.
    pub fn column(&self, id: ColumnId) -> Option<&Series> {
        self.columns.iter().find(|c| c.index() == id)
    }

    /// A borrowing view of one row; `None` past the last row.
    pub fn row(&self, row: usize) -> Option<SampleRow<'_>> {
        (row < self.len()).then_some(SampleRow { batch: self, row })
    }

    /// Borrowing views of every row, in row order.
    pub fn iter(&self) -> impl Iterator<Item = SampleRow<'_>> {
        (0..self.len()).map(move |row| SampleRow { batch: self, row })
    }

    /// True unless the boundary marks a break in continuity.
    pub fn is_continuous(&self) -> bool {
        self.context
            .boundary
            .as_ref()
            .is_none_or(|b| b.is_continuous())
    }

    /// True unless the boundary marks a non-monotonic break.
    pub fn is_monotonic(&self) -> bool {
        self.context
            .boundary
            .as_ref()
            .is_none_or(|b| b.is_monotonic())
    }

    /// True only when the boundary is the stream's first sample.
    pub fn is_initial(&self) -> bool {
        self.context
            .boundary
            .as_ref()
            .is_some_and(|b| b.is_initial())
    }
}

/// Borrowing view of one row of a [`SampleBatch`].
#[derive(Clone, Copy)]
pub struct SampleRow<'a> {
    batch: &'a SampleBatch,
    row: usize,
}

impl<'a> SampleRow<'a> {
    /// The row's counter within its segment.
    pub fn n(&self) -> SampleNumber {
        self.batch.sample_numbers[self.row]
    }
    /// Borrowed view of the stream record this row belongs to.
    pub fn stream(&self) -> wire::Stream<'a> {
        self.batch.context.stream.get()
    }
    /// Borrowed view of the segment this row was sampled in.
    pub fn segment(&self) -> wire::Segment<'a> {
        self.batch.context.segment.get()
    }
    /// Borrowed view of the record of the device that sent this row.
    pub fn device(&self) -> wire::Device<'a> {
        self.batch.context.device.get()
    }
    /// Start of the sample's interval, in seconds after the segment epoch.
    pub fn timestamp_begin(&self) -> f64 {
        sample_time(self.segment(), self.n().value())
    }
    /// End of the sample's interval, the time the batch materialized.
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
            self.device().session.value(),
            self.stream().stream_id,
            self.segment().segment_id,
            self.timestamp_end()
        )?;
        for (series, value) in self.batch.schema().iter().zip(self.values()) {
            write!(f, " {}: {}", series.metadata().name, value)?;
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

/// What a boundary says about the data around it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryClass {
    /// The stream continues without a gap.
    Seamless,
    /// The first data observed for a stream.
    Startup,
    /// Samples are missing, but time remains monotonic.
    DataLoss,
    /// The session, time reference, rate, or segment changed.
    Reconfig,
    /// The timeline is inconsistent, possibly from corrupted data or a device clock error.
    Anomaly,
}

/// Why a stream started or changed.
///
/// Attached to the first batch affected. Use:
///
/// - [`class`](Self::class) to categorize the cause.
/// - [`is_continuous`](Self::is_continuous) to check for an unbroken timeline.
/// - [`is_monotonic`](Self::is_monotonic) to check that time did not move backward.
///
/// | Reason                  | [`class`](Self::class) | [`is_continuous`](Self::is_continuous) | [`is_monotonic`](Self::is_monotonic) |
/// |-------------------------|------------|-----|-----|
/// | `SegmentRollover`       | `Seamless` | yes | yes |
/// | `Initial`               | `Startup`  | no  | no  |
/// | `SamplesLost`           | `DataLoss` | no  | yes |
/// | `SessionChanged`        | `Reconfig` | no  | no  |
/// | `TimeRefSessionChanged` | `Reconfig` | no  | no  |
/// | `RateChanged`           | `Reconfig` | no  | yes |
/// | `SegmentChanged`        | `Reconfig` | no  | yes |
/// | `TimeForward`           | `Anomaly`  | no  | yes |
/// | `TimeBackward`          | `Anomaly`  | no  | no  |
///
/// Continuous boundaries are always monotonic. The class describes the cause,
/// not its effect on the timeline.
#[derive(Debug, Clone)]
pub enum BoundaryReason {
    /// First sample from this stream
    Initial,
    /// Device session changed
    SessionChanged {
        /// Session the preceding rows carried.
        old: SessionId,
        /// Session the device reports now.
        new: SessionId,
    },
    /// Time reference epoch changed
    TimeRefSessionChanged {
        /// Time reference session the preceding rows carried.
        old: SessionId,
        /// Time reference session the segment names now.
        new: SessionId,
    },
    /// Time jumped backward unexpectedly
    TimeBackward {
        /// How far time went back, in seconds, as a positive magnitude.
        gap_seconds: f64,
    },
    /// Time jumped forward unexpectedly with no gap in sample numbers, e.g. a
    /// segment's start time was corrected in place
    TimeForward {
        /// How far time skipped ahead, in seconds.
        gap_seconds: f64,
    },
    /// Sampling rate changed
    RateChanged {
        /// Rate the preceding rows were sampled at, in Hz after decimation.
        old_rate: f64,
        /// Rate now in force, in Hz after decimation.
        new_rate: f64,
    },

    /// Segment rolled over (continuous, but new segment)
    SegmentRollover {
        /// Segment that ended.
        old_id: SegmentId,
        /// Segment the rows continue into.
        new_id: SegmentId,
    },
    /// Segment changed unexpectedly (not a natural rollover)
    SegmentChanged {
        /// Segment the stream had been in.
        old_id: SegmentId,
        /// Segment the rows arrived in.
        new_id: SegmentId,
    },
    /// Samples were lost (gap in sequence)
    SamplesLost {
        /// Sample number the first row should have carried.
        expected: SampleNumber,
        /// Sample number the first row actually carried.
        received: SampleNumber,
    },
}

impl BoundaryReason {
    /// Only boundary where continuity is preserved, and always monotonic too.
    pub fn is_continuous(&self) -> bool {
        matches!(self, BoundaryReason::SegmentRollover { .. })
    }

    /// Boundaries where there may be a gap but time is still monotonic.
    ///
    /// A superset of [`is_continuous`](Self::is_continuous).
    pub fn is_monotonic(&self) -> bool {
        matches!(
            self,
            BoundaryReason::SamplesLost { .. }
                | BoundaryReason::RateChanged { .. }
                | BoundaryReason::SegmentRollover { .. }
                | BoundaryReason::SegmentChanged { .. }
                | BoundaryReason::TimeForward { .. }
        )
    }

    /// True only for the first data a stream has ever delivered.
    pub fn is_initial(&self) -> bool {
        matches!(self, BoundaryReason::Initial)
    }

    /// How this boundary should be interpreted, independent of continuity and
    /// monotonicity.
    pub fn class(&self) -> BoundaryClass {
        match self {
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
    use crate::data::fixtures::{column, device, segment, stream};
    use crate::proto::data::DataType;

    #[test]
    fn signed_int24_values_are_sign_extended() {
        for (bytes, expected) in [
            ([0x00, 0x00, 0x00], 0),
            ([0xff, 0xff, 0x7f], 8_388_607),
            ([0xff, 0xff, 0xff], -1),
            ([0x00, 0x00, 0x80], -8_388_608),
        ] {
            let ColumnData::Int(value) = ColumnData::from_le_bytes(&bytes, DataType::I24) else {
                panic!("expected an integer value");
            };
            assert_eq!(value, expected);
        }
    }

    /// A four-row float batch numbered 0..4, sampled at 4 Hz, with an initial
    /// boundary anchored at its first row.
    fn batch() -> SampleBatch {
        let mut builder = SampleBatchBuilder::new(
            BatchContext::new(
                StreamKey::new(DeviceRoute::root(), StreamId::new(1)),
                Some(BoundaryReason::Initial),
                Generations {
                    stream: 1,
                    device: 0,
                    global: 0,
                },
                SegmentRecord::encode(wire::Segment {
                    sampling_rate: 4,
                    ..segment(1)
                })
                .unwrap(),
                StreamRecord::encode(wire::Stream {
                    sample_size: 8,
                    ..stream(1)
                })
                .unwrap(),
                DeviceRecord::encode(device()).unwrap(),
            ),
            [(
                ColumnRecord::encode(column(1, 0, DataType::F64)).unwrap(),
                BufferType::Float,
            )],
            4,
        );
        for n in 0..4 {
            builder.push_row(
                SampleNumber::new(n),
                [ColumnData::Float(f64::from(n) * 10.0)],
            );
        }
        builder.finish()
    }

    #[test]
    fn owned_metadata_matches_only_the_same_routed_schema() {
        let original = batch();
        let metadata = original.metadata();
        assert!(metadata.matches_schema(&original));
        assert_eq!(metadata.device(), original.device());
        assert_eq!(metadata.stream(), original.stream());
        assert_eq!(metadata.segment(), original.segment());
        assert_eq!(
            metadata.columns().collect::<Vec<_>>(),
            vec![original.schema()[0].metadata()]
        );

        let mut another_stream = original.clone();
        another_stream.context.key.stream_id = StreamId::new(2);
        assert!(!metadata.matches_schema(&another_stream));

        let mut changed_schema = original.clone();
        let column = changed_schema.columns[0].metadata.get();
        changed_schema.columns[0].metadata = ColumnRecord::encode(wire::Column {
            name: "renamed",
            ..column
        })
        .unwrap();
        assert!(!metadata.matches_schema(&changed_schema));
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
            .map(|&n| sample_time(batch.segment(), n.value() + 1))
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

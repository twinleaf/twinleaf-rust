//! The decoded sample model.
//!
//! [`SampleBatch`] holds immutable columnar rows, [`Series`] and
//! [`ColumnArray`] hold its columns, and [`SampleRow`] views one row.
//! [`BoundaryReason`] and [`Generations`] describe the continuity the parser
//! reports.

use super::metadata::{
    BufferType, ColumnRecord, DeviceRecord, SegmentRecord, StreamKey, StreamMetadataSnapshot,
    StreamRecord,
};
use crate::proto::data as wire;
use crate::proto::DeviceRoute;
use crate::proto::{ColumnId, SampleNumber, SegmentId, SessionId};
use crate::tio;
use std::ops::{Deref, Range};
use std::sync::Arc;

/// A segment's map from sample number to time, read once per batch.
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
    /// Returns the value as an `f64`, or `None` for `Unknown`. Integers larger
    /// than 2^53 in magnitude lose precision.
    pub fn try_as_f64(&self) -> Option<f64> {
        match *self {
            ColumnData::Int(i) => Some(i as f64),
            ColumnData::UInt(u) => Some(u as f64),
            ColumnData::Float(f) => Some(f),
            ColumnData::Unknown => None,
        }
    }

    /// Decodes the value of `data_type` at the front of `data`, or returns
    /// [`ColumnData::Unknown`] for a type this build cannot decode.
    ///
    /// # Panics
    ///
    /// Panics if `data` is shorter than the type's size.
    pub(crate) fn from_le_bytes(
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
    /// Returns the sub-slice `range`, relative to this slice's own start,
    /// without copying values.
    ///
    /// # Panics
    ///
    /// Panics if `range` starts after it ends or ends past this slice's
    /// length.
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
    /// Creates an empty column of the variant for `bt`, with room for
    /// `capacity` values.
    fn with_capacity_for(bt: BufferType, capacity: usize) -> Self {
        match bt {
            BufferType::Float => Self::F64(Vec::with_capacity(capacity)),
            BufferType::Int => Self::I64(Vec::with_capacity(capacity)),
            BufferType::UInt => Self::U64(Vec::with_capacity(capacity)),
        }
    }

    /// Returns the buffer type whose columns land in this variant.
    fn buffer_type(&self) -> BufferType {
        match self {
            Self::F64(_) => BufferType::Float,
            Self::I64(_) => BufferType::Int,
            Self::U64(_) => BufferType::UInt,
        }
    }

    /// Appends one decoded cell, widening `Int` into a `Float` column.
    ///
    /// # Panics
    ///
    /// Panics if the cell's variant does not match the column in any other
    /// way.
    fn push_data(&mut self, v: &ColumnData) {
        match (self, v) {
            (Self::F64(d), ColumnData::Float(x)) => d.push(*x),
            (Self::F64(d), ColumnData::Int(x)) => d.push(*x as f64),
            (Self::I64(d), ColumnData::Int(x)) => d.push(*x),
            (Self::U64(d), ColumnData::UInt(x)) => d.push(*x),
            // Dropping a cell decoded for another column type would desync this
            // column from its batch's rows.
            (this, v) => panic!("cannot push {v:?} into a {:?} column", this.buffer_type()),
        }
    }

    /// Appends a frozen column's values to this one.
    ///
    /// # Panics
    ///
    /// Panics if the two columns hold different variants.
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

    /// Returns a frozen copy of `rows`, leaving this column accumulating.
    ///
    /// # Panics
    ///
    /// Panics if `rows` is out of bounds.
    fn freeze_rows(&self, rows: Range<usize>) -> ColumnArray {
        match self {
            Self::F64(v) => ColumnArray::F64(v[rows].to_vec().into()),
            Self::I64(v) => ColumnArray::I64(v[rows].to_vec().into()),
            Self::U64(v) => ColumnArray::U64(v[rows].to_vec().into()),
        }
    }
}

/// One frozen column's decoded values, shared and sliceable. Each variant's
/// buffer derefs to a plain slice.
#[derive(Debug, Clone)]
pub enum ColumnArray {
    /// Float column. Signed integer cells are widened into one.
    F64(ScalarBuffer<f64>),
    /// Signed integer column, whatever its narrower wire width.
    I64(ScalarBuffer<i64>),
    /// Unsigned integer column, whatever its narrower wire width.
    U64(ScalarBuffer<u64>),
}

impl ColumnArray {
    /// Returns the buffer type whose columns land in this variant.
    pub(crate) fn buffer_type(&self) -> BufferType {
        match self {
            Self::F64(_) => BufferType::Float,
            Self::I64(_) => BufferType::Int,
            Self::U64(_) => BufferType::UInt,
        }
    }

    /// Returns the number of values, one per row of its batch.
    pub fn len(&self) -> usize {
        match self {
            Self::F64(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::U64(v) => v.len(),
        }
    }

    /// Returns `true` if the column holds no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the cell at row `i` as a [`ColumnData`], or `Unknown` if `i` is
    /// out of bounds.
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

    /// Returns the values in `rows` without copying them.
    ///
    /// # Panics
    ///
    /// Panics if `rows` starts after it ends or ends past the column's length.
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

/// The identity, boundary, generations, and metadata records that every row
/// of a batch shares.
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
    /// Creates the context for a batch of `key`'s rows.
    ///
    /// # Panics
    ///
    /// Panics if `stream` or `segment` describes a stream other than `key`'s.
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
/// A batch is immutable and clones by pointer. Metadata accessors return
/// borrowed protocol views of the records the parser retained. They copy
/// nothing and match the bytes the instrument sent.
#[derive(Debug, Clone)]
pub struct SampleBatch {
    context: BatchContext,
    sample_numbers: ScalarBuffer<SampleNumber>,
    /// Each row's end-of-sample time, materialized at construction.
    timestamps: ScalarBuffer<f64>,
    /// Columns in index order.
    columns: Vec<Series>,
}

/// One column of a batch, with its metadata and its decoded values.
#[derive(Debug, Clone)]
pub struct Series {
    metadata: ColumnRecord,
    values: ColumnArray,
}

impl Series {
    /// Returns the column's position within the stream's packed sample.
    pub fn index(&self) -> ColumnId {
        self.metadata.get().index
    }

    /// Returns a borrowed view of the column record the parser retained.
    pub fn metadata(&self) -> wire::Column<'_> {
        self.metadata.get()
    }

    pub(crate) fn record(&self) -> &ColumnRecord {
        &self.metadata
    }

    /// Returns the column's decoded values in row order.
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

/// Rows that can enter a batch coalescer. They describe the batch they would
/// form and append themselves onto an accumulating builder.
pub(super) trait RowSource {
    fn len(&self) -> usize;
    fn boundary(&self) -> Option<&BoundaryReason>;
    fn generations(&self) -> Generations;
    fn segment_record(&self) -> &SegmentRecord;
    /// Returns an empty builder with these rows' metadata, boundary and
    /// generations, and room for `capacity` rows.
    fn start_builder(&self, capacity: usize) -> SampleBatchBuilder;
    /// Copies every row onto the end of `tail` without checking
    /// compatibility. Callers merge only rows that share the tail's metadata
    /// and generations.
    fn append_to(&self, tail: &mut SampleBatchBuilder);
}

/// Accumulates decoded rows into a [`SampleBatch`], one row or one batch at a
/// time.
pub(super) struct SampleBatchBuilder {
    context: BatchContext,
    sample_numbers: Vec<SampleNumber>,
    /// Each row's end-of-sample time, carried alongside its sample number.
    timestamps: Vec<f64>,
    /// The segment's clock, read when the builder is created.
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
    /// Creates a builder for rows that share `context` and have `columns`,
    /// with room for `capacity` rows.
    ///
    /// # Panics
    ///
    /// Panics if a column describes a stream other than the context's.
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

    /// Appends one decoded row. `cells` yields one value per column, in schema
    /// order.
    ///
    /// # Panics
    ///
    /// Panics if `cells` yields too few values or a value its column cannot
    /// hold. Debug builds also panic if it yields too many.
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

    /// Returns a frozen copy of `rows`, leaving the builder accumulating. The
    /// boundary is anchored at the first row, as in [`SampleBatch::slice`].
    ///
    /// # Panics
    ///
    /// Panics if `rows` is out of bounds.
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

    /// Freezes the accumulated rows into a shareable batch.
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
    /// Returns a view of `rows` that shares this batch's buffers. The boundary
    /// is anchored at the first row and survives only in a slice that starts
    /// there.
    ///
    /// # Panics
    ///
    /// Panics if `rows` starts after it ends or ends past the last row.
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

    /// Returns the route of the device that produced these rows,
    /// root-to-leaf.
    pub fn route(&self) -> DeviceRoute {
        self.context.key.route
    }

    /// Returns the routed stream these rows came from.
    pub fn stream_key(&self) -> StreamKey {
        self.context.key
    }

    /// Returns the batch's boundary, anchored at its first row, or `None` if
    /// the batch continues its run.
    pub fn boundary(&self) -> Option<&BoundaryReason> {
        self.context.boundary.as_ref()
    }

    /// Returns the continuity generations this batch's rows belong to.
    pub fn generations(&self) -> Generations {
        self.context.generations
    }

    /// Returns each row's sample number within its segment, in row order.
    pub fn sample_numbers(&self) -> &[SampleNumber] {
        &self.sample_numbers
    }

    /// Returns each row's end-of-sample time in seconds after the segment's
    /// epoch, in row order.
    pub fn timestamps(&self) -> &[f64] {
        &self.timestamps
    }

    /// Returns a borrowed view of the segment these rows were sampled in.
    pub fn segment(&self) -> wire::Segment<'_> {
        self.context.segment.get()
    }

    /// Returns a borrowed view of the stream record these rows belong to.
    pub fn stream(&self) -> wire::Stream<'_> {
        self.context.stream.get()
    }

    /// Returns a borrowed view of the record of the device that sent these
    /// rows.
    pub fn device(&self) -> wire::Device<'_> {
        self.context.device.get()
    }

    /// Returns this batch's metadata as a snapshot that holds no sample
    /// arrays.
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

    /// Encodes this batch's metadata as byte-faithful UPDATE packets, as
    /// [`StreamMetadataSnapshot::metadata_packets`] does.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`](tio::packet::EncodeError) if a record is too
    /// large for a packet.
    pub fn metadata_packets(&self) -> Result<Vec<tio::Packet>, tio::packet::EncodeError> {
        self.metadata().metadata_packets()
    }

    /// Returns the number of rows, which every column's length matches.
    pub fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    /// Returns `true` if the batch carries no rows.
    pub fn is_empty(&self) -> bool {
        self.sample_numbers.is_empty()
    }

    /// Returns the first row's sample number, or `None` if the batch is empty.
    pub fn first_sample(&self) -> Option<SampleNumber> {
        self.sample_numbers.first().copied()
    }

    /// Returns the last row's sample number, or `None` if the batch is empty.
    pub fn last_sample(&self) -> Option<SampleNumber> {
        self.sample_numbers.last().copied()
    }

    /// Returns the batch's columns in index order. Their metadata gives each
    /// column's name, type and units.
    pub fn schema(&self) -> &[Series] {
        &self.columns
    }

    /// Returns the column whose index is `id`, or `None` if the schema has no
    /// such column.
    pub fn column(&self, id: ColumnId) -> Option<&Series> {
        self.columns.iter().find(|c| c.index() == id)
    }

    /// Returns a borrowing view of row `row`, or `None` past the last row.
    pub fn row(&self, row: usize) -> Option<SampleRow<'_>> {
        (row < self.len()).then_some(SampleRow { batch: self, row })
    }

    /// Returns borrowing views of every row, in row order.
    pub fn iter(&self) -> impl Iterator<Item = SampleRow<'_>> {
        (0..self.len()).map(move |row| SampleRow { batch: self, row })
    }

    /// Returns `true` unless the batch's boundary breaks continuity.
    pub fn is_continuous(&self) -> bool {
        self.context
            .boundary
            .as_ref()
            .is_none_or(|b| b.is_continuous())
    }

    /// Returns `true` unless the batch's boundary is a non-monotonic break.
    pub fn is_monotonic(&self) -> bool {
        self.context
            .boundary
            .as_ref()
            .is_none_or(|b| b.is_monotonic())
    }

    /// Returns `true` if the batch's boundary marks the stream's first sample.
    pub fn is_initial(&self) -> bool {
        self.context
            .boundary
            .as_ref()
            .is_some_and(|b| b.is_initial())
    }
}

/// A borrowing view of one row of a [`SampleBatch`].
#[derive(Clone, Copy)]
pub struct SampleRow<'a> {
    batch: &'a SampleBatch,
    row: usize,
}

impl<'a> SampleRow<'a> {
    /// Returns the row's sample number within its segment.
    pub fn n(&self) -> SampleNumber {
        self.batch.sample_numbers[self.row]
    }
    /// Returns a borrowed view of the stream record this row belongs to.
    pub fn stream(&self) -> wire::Stream<'a> {
        self.batch.context.stream.get()
    }
    /// Returns a borrowed view of the segment this row was sampled in.
    pub fn segment(&self) -> wire::Segment<'a> {
        self.batch.context.segment.get()
    }
    /// Returns a borrowed view of the record of the device that sent this
    /// row.
    pub fn device(&self) -> wire::Device<'a> {
        self.batch.context.device.get()
    }
    /// Returns the start of the sample's interval, in seconds after the
    /// segment's epoch.
    pub fn timestamp_begin(&self) -> f64 {
        sample_time(self.segment(), self.n().value())
    }
    /// Returns the end of the sample's interval, in seconds after the
    /// segment's epoch. It is the row's entry in [`SampleBatch::timestamps`].
    pub fn timestamp_end(&self) -> f64 {
        self.batch.timestamps[self.row]
    }

    /// Returns the row's column values in schema order.
    pub fn values(&self) -> impl Iterator<Item = ColumnData> + '_ {
        let row = self.row;
        self.batch.columns.iter().map(move |c| c.values.get(row))
    }

    /// Returns the row's value in the column whose index is `id`, or `None`
    /// if the schema has no such column.
    pub fn value(&self, id: ColumnId) -> Option<ColumnData> {
        self.batch.column(id).map(|c| c.values.get(self.row))
    }
}

impl std::fmt::Debug for SampleRow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SampleRow")
            .field("row", &self.row)
            .field("n", &self.n())
            .field("timestamp_end", &self.timestamp_end())
            .field("values", &self.values().collect::<Vec<_>>())
            .finish_non_exhaustive()
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
/// The parser stamps them in arrival order. Comparing them tells runs apart
/// without re-reading boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Generations {
    /// Per-stream run ordinal. It bumps at every non-continuous boundary.
    pub stream: u32,
    /// Per-device generation. It bumps at any non-continuous boundary on the
    /// device's streams other than [`BoundaryReason::Initial`], and when the
    /// device's state resets.
    pub device: u32,
    /// Whole-parse generation. It bumps as `device` does, across all routed
    /// devices.
    pub global: u32,
}

/// What a boundary says about the data around it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryClass {
    /// The first data observed for a stream.
    Startup,
    /// The stream continues without a gap.
    Seamless,
    /// Samples are missing, but time remains monotonic.
    DataLoss,
    /// The session, time reference, rate, or segment changed.
    Reconfig,
    /// The timeline is inconsistent, possibly from corrupted data or a device clock error.
    Anomaly,
}

impl BoundaryClass {
    /// Every class in declaration order, which is a stream's start and then
    /// the rest from the least disruptive to the most.
    pub const ALL: [BoundaryClass; 5] = [
        BoundaryClass::Startup,
        BoundaryClass::Seamless,
        BoundaryClass::DataLoss,
        BoundaryClass::Reconfig,
        BoundaryClass::Anomaly,
    ];
}

/// The class in lowercase words, such as `data loss`.
impl std::fmt::Display for BoundaryClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(match self {
            BoundaryClass::Startup => "startup",
            BoundaryClass::Seamless => "seamless",
            BoundaryClass::DataLoss => "data loss",
            BoundaryClass::Reconfig => "reconfig",
            BoundaryClass::Anomaly => "anomaly",
        })
    }
}

/// Why a stream started or changed.
///
/// Attached to the first batch affected. These methods classify it.
///
/// - [`class`](Self::class) categorizes the cause.
/// - [`is_continuous`](Self::is_continuous) checks for an unbroken timeline.
/// - [`is_monotonic`](Self::is_monotonic) checks that time did not move backward.
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
#[non_exhaustive]
pub enum BoundaryReason {
    /// The stream's first sample.
    Initial,
    /// The device session changed.
    SessionChanged {
        /// Session the preceding rows carried.
        old: SessionId,
        /// Session the device reports now.
        new: SessionId,
    },
    /// The time reference epoch changed.
    TimeRefSessionChanged {
        /// Time reference session the preceding rows carried.
        old: SessionId,
        /// Time reference session the segment names now.
        new: SessionId,
    },
    /// Time jumped backward unexpectedly.
    TimeBackward {
        /// How far time went back, in seconds, as a positive magnitude.
        gap_seconds: f64,
    },
    /// Time jumped forward with no gap in sample numbers, for example after a
    /// segment's start time was corrected in place.
    TimeForward {
        /// How far time skipped ahead, in seconds.
        gap_seconds: f64,
    },
    /// The sampling rate changed.
    RateChanged {
        /// Rate the preceding rows were sampled at, in Hz after decimation.
        old_rate: f64,
        /// Rate now in force, in Hz after decimation.
        new_rate: f64,
    },

    /// The segment rolled over. The timeline continues in a new segment.
    SegmentRollover {
        /// Segment that ended.
        old_id: SegmentId,
        /// Segment the rows continue into.
        new_id: SegmentId,
    },
    /// The segment changed other than by a rollover.
    SegmentChanged {
        /// Segment the stream had been in.
        old_id: SegmentId,
        /// Segment the rows arrived in.
        new_id: SegmentId,
    },
    /// Samples were lost, leaving a gap in sample numbers.
    SamplesLost {
        /// Sample number the first row should have carried.
        expected: SampleNumber,
        /// Sample number the first row actually carried.
        received: SampleNumber,
    },
}

impl BoundaryReason {
    /// Returns `true` only for a segment rollover, the one boundary that
    /// preserves continuity. A continuous boundary is also monotonic.
    pub fn is_continuous(&self) -> bool {
        matches!(self, BoundaryReason::SegmentRollover { .. })
    }

    /// Returns `true` if time stays monotonic across the boundary, though rows
    /// may be missing.
    ///
    /// Every [`is_continuous`](Self::is_continuous) boundary is also
    /// monotonic.
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

    /// Returns `true` only for the first data a stream has ever delivered.
    pub fn is_initial(&self) -> bool {
        matches!(self, BoundaryReason::Initial)
    }

    /// Returns the class of the boundary's cause, independent of continuity
    /// and monotonicity.
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

impl StreamMetadataSnapshot {
    /// Returns `true` if `batch` has this snapshot's routed stream identity and
    /// exact column schema. Device and segment changes do not change a stream
    /// schema.
    pub fn matches_schema(&self, batch: &SampleBatch) -> bool {
        let columns = self.column_records();
        self.stream_key() == batch.stream_key()
            && columns.len() == batch.schema().len()
            && columns
                .iter()
                .zip(batch.schema())
                .all(|(expected, actual)| expected == actual.record())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures::{column, device, segment, stream};
    use crate::proto::data::DataType;
    use crate::proto::StreamId;

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

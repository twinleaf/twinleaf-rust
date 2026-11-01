//! The decoded sample model.
//!
//! [`SampleBatch`] holds immutable columnar rows, one [`ColumnArray`] per
//! column of its [`StreamSchema`], and [`SampleRow`] views one row.

use super::continuity::{Boundary, Generations};
use super::metadata::{SegmentRecord, StreamKey, StreamMetadataSnapshot, StreamSchema};
use crate::proto::data as wire;
use crate::proto::DeviceRoute;
use crate::proto::{ColumnId, SampleNumber};
use std::ops::{Deref, Range};
use std::sync::Arc;

/// A segment's map from sample number to time, read once per batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SampleClock {
    start_time: f64,
    period: f64,
}

impl SampleClock {
    pub(crate) fn of(segment: wire::SegmentView<'_>) -> Self {
        Self {
            start_time: f64::from(segment.start_time()),
            period: f64::from(segment.decimation()) / f64::from(segment.sampling_rate()),
        }
    }

    pub(crate) fn time_at(&self, n: u32) -> f64 {
        self.start_time + self.period * f64::from(n)
    }
}

pub(crate) fn sample_time(segment: wire::SegmentView<'_>, n: u32) -> f64 {
    SampleClock::of(segment).time_at(n)
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

/// Calls `$then!` with every native column type, each as its variant name
/// and Rust type. The arrays, builders, cells and their decoders are all
/// generated from this one list.
macro_rules! native_types {
    ($then:ident) => {
        $then! {
            I8 i8, U8 u8, I16 i16, U16 u16, I32 i32,
            U32 u32, I64 i64, U64 u64, F32 f32, F64 f64,
        }
    };
}
pub(crate) use native_types;

macro_rules! define_columns {
    ($($variant:ident $native:ty),* $(,)?) => {
        /// One decoded value, in its column's native type.
        #[derive(Debug, Clone, Copy, PartialEq)]
        pub enum ColumnData {
            $(
                #[doc = concat!("A `", stringify!($native), "` value.")]
                $variant($native),
            )*
            /// A value whose wire type this build cannot decode.
            Unknown,
        }

        impl ColumnData {
            /// Returns the value as an `f64`, or `None` for `Unknown`. 64-bit
            /// integers larger than 2^53 in magnitude lose precision.
            pub fn try_as_f64(&self) -> Option<f64> {
                match *self {
                    $(Self::$variant(value) => Some(value as f64),)*
                    Self::Unknown => None,
                }
            }
        }

        impl std::fmt::Display for ColumnData {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match *self {
                    $(Self::$variant(value) => write!(f, "{value}"),)*
                    Self::Unknown => write!(f, "?"),
                }
            }
        }

        /// One column's values while they are still being accumulated, in the
        /// column's native type.
        #[derive(Debug, Clone)]
        pub(crate) enum ColumnBuilder {
            $($variant(Vec<$native>),)*
        }

        impl ColumnBuilder {
            /// Returns an empty column for values of `data_type`, with room
            /// for `capacity` of them, or `None` for a type this build cannot
            /// decode.
            pub(crate) fn for_type(data_type: wire::DataType, capacity: usize) -> Option<Self> {
                Some(match data_type {
                    $(wire::DataType::$variant => Self::$variant(Vec::with_capacity(capacity)),)*
                    _ => return None,
                })
            }

            /// Appends one cell, converted to the column's type. Tests build
            /// rows from cells of any type.
            ///
            /// # Panics
            ///
            /// Panics if the cell is `Unknown`.
            #[cfg(test)]
            fn push_data(&mut self, cell: &ColumnData) {
                let value = cell.try_as_f64().expect("a known cell");
                match self {
                    $(Self::$variant(values) => values.push(value as $native),)*
                }
            }

            /// Appends a frozen column's values to this one.
            ///
            /// # Panics
            ///
            /// Panics if the two columns hold different types.
            fn extend_from(&mut self, values: &ColumnArray) {
                match (self, values) {
                    $((Self::$variant(own), ColumnArray::$variant(more)) => own.extend_from_slice(more),)*
                    (own, more) => panic!(
                        "cannot extend a {} column from a {} one",
                        ColumnArray::from(own.clone()).data_type(),
                        more.data_type()
                    ),
                }
            }

            /// Returns a frozen copy of `rows`, leaving this column
            /// accumulating.
            ///
            /// # Panics
            ///
            /// Panics if `rows` is out of bounds.
            fn freeze_rows(&self, rows: Range<usize>) -> ColumnArray {
                match self {
                    $(Self::$variant(values) => ColumnArray::$variant(values[rows].to_vec().into()),)*
                }
            }
        }

        /// One frozen column's values in its native type, shared and
        /// sliceable. Each variant's buffer derefs to a plain slice.
        #[derive(Debug, Clone)]
        pub enum ColumnArray {
            $(
                #[doc = concat!("A column of `", stringify!($native), "` values.")]
                $variant(ScalarBuffer<$native>),
            )*
        }

        impl ColumnArray {
            /// Returns the wire type a column of this type decodes from.
            pub fn data_type(&self) -> wire::DataType {
                match self {
                    $(Self::$variant(_) => wire::DataType::$variant,)*
                }
            }

            /// Returns the number of values, one per row of its batch.
            pub fn len(&self) -> usize {
                match self {
                    $(Self::$variant(values) => values.len(),)*
                }
            }

            /// Returns `true` if the column holds no values.
            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }

            /// Returns the cell at row `i`, or `Unknown` if `i` is out of
            /// bounds.
            pub fn get(&self, i: usize) -> ColumnData {
                match self {
                    $(Self::$variant(values) => values.get(i).map_or(ColumnData::Unknown, |&value| ColumnData::$variant(value)),)*
                }
            }

            /// Returns the values in `rows` without copying them.
            ///
            /// # Panics
            ///
            /// Panics if `rows` starts after it ends or ends past the
            /// column's length.
            pub fn slice(&self, rows: Range<usize>) -> ColumnArray {
                match self {
                    $(Self::$variant(values) => Self::$variant(values.slice(rows)),)*
                }
            }

            /// Returns the values as `f64`, borrowed from an `f64` column and
            /// converted from any other. 64-bit integers larger than 2^53 in
            /// magnitude lose precision.
            pub fn to_f64(&self) -> std::borrow::Cow<'_, [f64]> {
                if let Self::F64(values) = self {
                    return std::borrow::Cow::Borrowed(values);
                }
                match self {
                    $(Self::$variant(values) => values.iter().map(|&value| value as f64).collect(),)*
                }
            }
        }

        impl From<ColumnBuilder> for ColumnArray {
            fn from(values: ColumnBuilder) -> ColumnArray {
                match values {
                    $(ColumnBuilder::$variant(values) => ColumnArray::$variant(values.into()),)*
                }
            }
        }
    };
}

native_types!(define_columns);

/// The metadata, boundary and generations that every row of a batch shares.
#[derive(Debug, Clone)]
pub(super) struct BatchContext {
    pub(super) metadata: StreamMetadataSnapshot,
    /// At most one boundary per batch, anchored at its first row.
    pub(super) boundary: Option<Boundary>,
    pub(super) generations: Generations,
}

/// Rows of one stream from one device, held column-major.
///
/// A batch is its stream's [`StreamSchema`] plus one [`ColumnArray`] per
/// schema column. Column `i` of the schema
/// describes the values of column `i` of the batch. A batch is immutable and
/// clones by pointer. Metadata accessors return borrowed protocol views of
/// the records the parser retained. They copy nothing and match the bytes the
/// instrument sent.
#[derive(Debug, Clone)]
pub struct SampleBatch {
    context: BatchContext,
    sample_numbers: ScalarBuffer<SampleNumber>,
    /// Each row's end-of-sample time, materialized at construction.
    timestamps: ScalarBuffer<f64>,
    /// One array per schema column, in index order.
    columns: Vec<ColumnArray>,
}

/// Rows that can enter a batch coalescer. They describe the batch they would
/// form and append themselves onto an accumulating builder.
pub(crate) trait RowSource {
    fn len(&self) -> usize;
    fn boundary(&self) -> Option<&Boundary>;
    fn generations(&self) -> Generations;
    fn schema(&self) -> &Arc<StreamSchema>;
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
pub(crate) struct SampleBatchBuilder {
    context: BatchContext,
    sample_numbers: Vec<SampleNumber>,
    /// Each row's end-of-sample time, carried alongside its sample number.
    timestamps: Vec<f64>,
    /// The segment's clock, read when the builder is created.
    clock: SampleClock,
    /// One accumulating buffer per schema column, in index order.
    columns: Vec<ColumnBuilder>,
}

impl RowSource for SampleBatch {
    fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    fn boundary(&self) -> Option<&Boundary> {
        self.context.boundary.as_ref()
    }

    fn generations(&self) -> Generations {
        self.context.generations
    }

    fn schema(&self) -> &Arc<StreamSchema> {
        self.context.metadata.schema()
    }

    fn segment_record(&self) -> &SegmentRecord {
        self.context.metadata.segment_record()
    }

    fn start_builder(&self, capacity: usize) -> SampleBatchBuilder {
        SampleBatchBuilder::new(self.context.clone(), capacity)
    }

    fn append_to(&self, tail: &mut SampleBatchBuilder) {
        tail.sample_numbers.extend_from_slice(&self.sample_numbers);
        tail.timestamps.extend_from_slice(&self.timestamps);
        for (values, more) in tail.columns.iter_mut().zip(&self.columns) {
            values.extend_from(more);
        }
    }
}

impl SampleBatchBuilder {
    /// Creates a builder for rows that share `context`, with an empty column
    /// of each schema column's type and room for `capacity` rows.
    ///
    /// # Panics
    ///
    /// Panics if the schema has a column of a type this build cannot decode.
    pub(super) fn new(context: BatchContext, capacity: usize) -> Self {
        let columns = context
            .metadata
            .schema()
            .columns()
            .map(|column| {
                ColumnBuilder::for_type(column.data_type(), capacity)
                    .expect("a stream with a column this build cannot decode is refused")
            })
            .collect();
        Self {
            clock: SampleClock::of(context.metadata.segment()),
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
            && self.context.metadata.segment_record() == rows.segment_record()
            && self.context.metadata.schema() == rows.schema()
    }

    /// Appends one decoded row. `cells` yields one value per column, in schema
    /// order.
    ///
    /// # Panics
    ///
    /// Panics if `cells` yields too few values or a value its column cannot
    /// hold. Debug builds also panic if it yields too many.
    #[cfg(test)]
    pub(super) fn push_row(
        &mut self,
        n: SampleNumber,
        cells: impl IntoIterator<Item = ColumnData>,
    ) {
        self.sample_numbers.push(n);
        self.timestamps.push(self.clock.time_at(n.value() + 1));
        let mut cells = cells.into_iter();
        for values in &mut self.columns {
            values.push_data(&cells.next().expect("one cell per column"));
        }
        debug_assert!(cells.next().is_none(), "one cell per column");
    }

    /// Appends `rows` samples numbered from `first`, with their timestamps,
    /// and returns every column, in schema order, for the caller to append
    /// exactly `rows` values to each.
    pub(super) fn push_samples(
        &mut self,
        first: SampleNumber,
        rows: usize,
    ) -> impl Iterator<Item = &mut ColumnBuilder> {
        for row in 0..rows {
            let n = SampleNumber::new(first.value() + row as u32);
            self.sample_numbers.push(n);
            self.timestamps.push(self.clock.time_at(n.value() + 1));
        }
        self.columns.iter_mut()
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
        SampleBatch::new(
            context,
            self.sample_numbers[rows.clone()].to_vec().into(),
            self.timestamps[rows.clone()].to_vec().into(),
            self.columns
                .iter()
                .map(|values| values.freeze_rows(rows.clone()))
                .collect(),
        )
    }

    /// Freezes the accumulated rows into a shareable batch.
    pub(super) fn finish(self) -> SampleBatch {
        SampleBatch::new(
            self.context,
            self.sample_numbers.into(),
            self.timestamps.into(),
            self.columns.into_iter().map(ColumnArray::from).collect(),
        )
    }
}

impl SampleBatch {
    /// Creates a batch of `columns`, one per column of the context's schema.
    ///
    /// # Panics
    ///
    /// Panics unless there is one array per schema column, each of its
    /// column's type and with one value per row.
    fn new(
        context: BatchContext,
        sample_numbers: ScalarBuffer<SampleNumber>,
        timestamps: ScalarBuffer<f64>,
        columns: Vec<ColumnArray>,
    ) -> SampleBatch {
        let schema = context.metadata.schema();
        assert_eq!(
            columns.len(),
            schema.columns().len(),
            "one array per schema column"
        );
        for (column, values) in schema.columns().zip(&columns) {
            assert_eq!(
                column.data_type(),
                values.data_type(),
                "an array of its column's type"
            );
            assert_eq!(values.len(), sample_numbers.len(), "one value per row");
        }
        assert_eq!(
            timestamps.len(),
            sample_numbers.len(),
            "one timestamp per row"
        );
        SampleBatch {
            context,
            sample_numbers,
            timestamps,
            columns,
        }
    }

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
        self.stream_key().route
    }

    /// Returns the routed stream these rows came from.
    pub fn stream_key(&self) -> StreamKey {
        self.context.metadata.stream_key()
    }

    /// Returns the batch's boundary, anchored at its first row, or `None` if
    /// nothing its receiver can detect changed or was lost since that
    /// receiver's previous batch of the stream.
    pub fn boundary(&self) -> Option<&Boundary> {
        self.context.boundary.as_ref()
    }

    /// Returns this batch with `boundary` in place of its own, and the same
    /// rows, metadata and generations.
    pub(crate) fn with_boundary(mut self, boundary: Option<Boundary>) -> SampleBatch {
        self.context.boundary = boundary;
        self
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

    /// Returns this batch's metadata, the device and segment of its rows with
    /// the schema of its run. Its accessors return borrowed views of the
    /// records the parser retained, and cloning it copies no records.
    pub fn metadata(&self) -> &StreamMetadataSnapshot {
        &self.context.metadata
    }

    /// Returns the number of rows, which every column's length matches.
    pub fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    /// Returns `true` if the batch carries no rows.
    pub fn is_empty(&self) -> bool {
        self.sample_numbers.is_empty()
    }

    /// Returns the schema shared by every batch of this batch's run. Its
    /// columns give each column's name, type and units.
    pub fn schema(&self) -> &Arc<StreamSchema> {
        self.context.metadata.schema()
    }

    /// Returns one array per schema column, in index order.
    pub fn columns(&self) -> &[ColumnArray] {
        &self.columns
    }

    /// Returns the values of the column whose index is `id`, or `None` if
    /// the schema has no such column.
    pub fn column(&self, id: ColumnId) -> Option<&ColumnArray> {
        self.columns.get(id.index())
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

    /// Returns `true` if the batch's boundary is [`Change::Initial`](crate::data::Change::Initial).
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
    pub fn sample_number(&self) -> SampleNumber {
        self.batch.sample_numbers[self.row]
    }
    /// Returns the start of the sample's interval, in seconds after the
    /// segment's epoch.
    pub fn timestamp_begin(&self) -> f64 {
        let segment = self.batch.metadata().segment();
        sample_time(segment, self.sample_number().value())
    }
    /// Returns the end of the sample's interval, in seconds after the
    /// segment's epoch. It is the row's entry in [`SampleBatch::timestamps`].
    pub fn timestamp_end(&self) -> f64 {
        self.batch.timestamps[self.row]
    }

    /// Returns the row's column values in schema order.
    pub fn values(&self) -> impl Iterator<Item = ColumnData> + '_ {
        let row = self.row;
        self.batch.columns.iter().map(move |values| values.get(row))
    }

    /// Returns the row's value in the column whose index is `id`, or `None`
    /// if the schema has no such column.
    pub fn value(&self, id: ColumnId) -> Option<ColumnData> {
        self.batch.column(id).map(|values| values.get(self.row))
    }
}

impl std::fmt::Debug for SampleRow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SampleRow")
            .field("row", &self.row)
            .field("sample_number", &self.sample_number())
            .field("timestamp_end", &self.timestamp_end())
            .field("values", &self.values().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for SampleRow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metadata = self.batch.metadata();
        write!(
            f,
            "SAMPLE({}:{}:{}) {:.6}",
            metadata.device().session().value(),
            metadata.stream_key().stream_id,
            metadata.segment().segment_id(),
            self.timestamp_end()
        )?;
        for (column, value) in self.batch.schema().columns().zip(self.values()) {
            write!(f, " {}: {}", column.name(), value)?;
        }
        write!(f, " [#{}]", self.sample_number())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::continuity::Change;
    use crate::data::fixtures::{self, column, device, segment, stream};
    use crate::proto::data::DataType;

    fn metadata(stream_id: u8, column_name: &str) -> StreamMetadataSnapshot {
        fixtures::metadata(
            DeviceRoute::root(),
            device(),
            wire::Stream {
                sample_size: 8,
                ..stream(stream_id)
            },
            wire::Segment {
                sampling_rate: 4,
                ..segment(stream_id)
            },
            &[wire::Column {
                name: column_name,
                ..column(stream_id, 0, DataType::F64)
            }],
        )
    }

    /// A four-row float batch numbered 0..4, sampled at 4 Hz, with an initial
    /// boundary anchored at its first row.
    fn batch() -> SampleBatch {
        let mut builder = SampleBatchBuilder::new(
            BatchContext {
                metadata: metadata(1, "col_0"),
                boundary: Some(Boundary::of(Change::Initial)),
                generations: Generations {
                    stream: 1,
                    device: 0,
                    global: 0,
                },
            },
            4,
        );
        for n in 0..4 {
            builder.push_row(SampleNumber::new(n), [ColumnData::F64(f64::from(n) * 10.0)]);
        }
        builder.finish()
    }

    #[test]
    fn a_schema_equals_only_the_same_routed_records() {
        let original = batch();
        let schema = original.schema();
        assert!(Arc::ptr_eq(schema, original.clone().schema()));
        assert_eq!(schema, metadata(1, "col_0").schema(), "equal bytes");
        assert_ne!(schema, metadata(2, "col_0").schema(), "another stream");
        assert_ne!(schema, metadata(1, "renamed").schema(), "a changed column");
        assert_eq!(
            schema.columns().collect::<Vec<_>>(),
            [schema.column(ColumnId::new(0)).unwrap()]
        );
    }

    #[test]
    #[should_panic(expected = "an array of its column's type")]
    fn a_batch_refuses_an_array_of_another_type() {
        let batch = batch();
        SampleBatch::new(
            batch.context.clone(),
            batch.sample_numbers.clone(),
            batch.timestamps.clone(),
            vec![ColumnArray::F32(vec![0.0; 4].into())],
        );
    }

    #[test]
    fn finishing_preserves_values_and_materializes_end_of_sample_times() {
        let batch = batch();
        assert_eq!(batch.sample_numbers(), [0, 1, 2, 3]);
        match &batch.columns()[0] {
            ColumnArray::F64(v) => assert_eq!(&v[..], [0.0, 10.0, 20.0, 30.0]),
            other => panic!("expected an f64 column, got {other:?}"),
        }
        let expected: Vec<f64> = batch
            .sample_numbers()
            .iter()
            .map(|&n| sample_time(batch.metadata().segment(), n.value() + 1))
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
        match &tail.columns()[0] {
            ColumnArray::F64(v) => assert_eq!(&v[..], [10.0, 20.0]),
            other => panic!("expected an f64 column, got {other:?}"),
        }
        // Nothing was copied: the slice's rows are the originals in place.
        assert!(std::ptr::eq(&batch.timestamps()[1], &tail.timestamps()[0]));
        let (ColumnArray::F64(whole), ColumnArray::F64(part)) =
            (&batch.columns()[0], &tail.columns()[0])
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

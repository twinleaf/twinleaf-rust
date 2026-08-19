use crate::tio;

use crate::data::ColumnVec;
use std::sync::Arc;
use tio::proto::identifiers::{ColumnId, SampleNumber, SegmentId, SessionId, TimeRefSessionId};
use tio::proto::meta::{ColumnMetadata, DeviceMetadata, SegmentMetadata, StreamMetadata};
use tio::proto::DeviceRoute;

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
                ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], 0]).into())
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

/// Samples in columnar (Structure-of-Arrays) form: metadata is held once, and
/// each column's decoded values live in a contiguous [`ColumnVec`].
#[derive(Debug, Clone)]
pub struct SampleBatch {
    route: DeviceRoute,
    /// At most one boundary per batch, anchored at its first row.
    boundary: Option<Boundary>,
    generations: Generations,
    sample_numbers: Vec<SampleNumber>,
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
    values: ColumnVec,
}

impl Series {
    pub fn new(index: ColumnId, metadata: Arc<ColumnMetadata>, values: ColumnVec) -> Series {
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

    pub fn values(&self) -> &ColumnVec {
        &self.values
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
            sample_numbers,
            columns,
            segment,
            stream,
            device,
        }
    }

    /// Append one decoded row: `cells` must yield exactly one value per
    /// column, in schema order.
    pub(crate) fn push_row(
        &mut self,
        n: SampleNumber,
        cells: impl IntoIterator<Item = ColumnData>,
    ) {
        self.sample_numbers.push(n);
        let mut cells = cells.into_iter();
        for series in &mut self.columns {
            let cell = cells.next().expect("one cell per column");
            series.values.push_data(&cell);
        }
        debug_assert!(cells.next().is_none(), "one cell per column");
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
        self.batch.segment.time_at(self.n() + 1)
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

//! Buffer
//! Stores `Sample`s, emits data according to a `Reader` or `OverflowPolicy`.
//!
//! Assumptions:
//! 1. Time stamps of `Sample`s are exactly aligned
//! 2. Sample rates of columns sharing the same name are the same
//! 3. Sample numbers are independent between `Device`s

use crate::data::{util, ColumnData, Sample, Boundary, BoundaryReason};
use crate::device::CursorPosition;
use crate::tio::proto::identifiers::*;
use crate::tio::proto::meta::MetadataEpoch;
use crate::tio::proto::{BufferType, ColumnMetadata, DeviceRoute, SegmentMetadata, StreamMetadata};

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

pub type RunId = u64;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OverflowPolicy {
    DropOldest,
    Flush,
}

#[derive(Debug, Clone)]
pub enum ColumnBatch {
    F64(Vec<f64>),
    I64(Vec<i64>),
    U64(Vec<u64>),
}

impl ColumnBatch {
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
}

#[derive(Debug, Clone)]
pub struct DataSlice {
    pub stream_key: StreamKey,
    pub run_id: RunId,
    pub session_id: SessionId,
    pub segment_id: SegmentId,
    pub sample_numbers: Vec<SampleNumber>,
    pub timestamps: Vec<f64>,
    pub columns: HashMap<ColumnId, ColumnBatch>,
    pub stream_metadata: Arc<StreamMetadata>,
    pub segment_metadata: Arc<SegmentMetadata>,
    pub column_metadata: HashMap<ColumnId, Arc<ColumnMetadata>>,
}

impl DataSlice {
    pub fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sample_numbers.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct AlignedWindow {
    pub sample_numbers: Vec<SampleNumber>,
    pub timestamps: Vec<f64>,
    pub columns: HashMap<ColumnKey, ColumnBatch>,
    pub stream_metadata: HashMap<StreamKey, Arc<StreamMetadata>>,
    pub segment_metadata: HashMap<StreamKey, Arc<SegmentMetadata>>,
    pub column_metadata: HashMap<ColumnKey, Arc<ColumnMetadata>>,
    pub session_ids: HashMap<StreamKey, SessionId>,
    pub run_ids: HashMap<StreamKey, RunId>,
}

/// Events emitted by the buffer.
pub enum BufferEvent {
    /// A new run started due to a discontinuity.
    RunChanged {
        stream_key: StreamKey,
        old_run_id: Option<RunId>,
        new_run_id: RunId,
        boundary: Boundary,
    },

    /// A chunk of data is ready (Flush overflow policy).
    DataChunk {
        slice: DataSlice,
        is_first_chunk: bool,
    },
}

#[derive(Debug)]
pub enum ReadError {
    NoColumnsRequested,
    NoCursorForStream {
        stream_key: StreamKey,
    },
    NoActiveRun {
        stream_key: StreamKey,
    },
    InsufficientData {
        stream_key: StreamKey,
        requested: usize,
        available: usize,
    },
    ColumnNotFound {
        stream_key: StreamKey,
        column_id: ColumnId,
    },
    SampleNumberMismatch {
        streams: Vec<StreamKey>,
        reason: String,
    },
    SamplingRateMismatch {
        streams: Vec<StreamKey>,
        rates: Vec<f64>,
    },
    CursorInvalidated {
        stream_key: StreamKey,
        cursor_run: RunId,
        current_run: RunId,
    },
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

    fn push(&mut self, value: ColumnData) {
        match (self, value) {
            (Self::F64 { data, .. }, ColumnData::Float(v)) => data.push_back(v),
            (Self::F64 { data, .. }, ColumnData::Int(v)) => data.push_back(v as f64),
            (Self::I64 { data, .. }, ColumnData::Int(v)) => data.push_back(v),
            (Self::U64 { data, .. }, ColumnData::UInt(v)) => data.push_back(v),
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

    fn drain(&mut self) -> ColumnBatch {
        match self {
            Self::F64 { data, .. } => ColumnBatch::F64(data.drain(..).collect()),
            Self::I64 { data, .. } => ColumnBatch::I64(data.drain(..).collect()),
            Self::U64 { data, .. } => ColumnBatch::U64(data.drain(..).collect()),
        }
    }

    fn get_range(&self, start: usize, count: usize) -> ColumnBatch {
        match self {
            Self::F64 { data, .. } => {
                ColumnBatch::F64(data.iter().skip(start).take(count).copied().collect())
            }
            Self::I64 { data, .. } => {
                ColumnBatch::I64(data.iter().skip(start).take(count).copied().collect())
            }
            Self::U64 { data, .. } => {
                ColumnBatch::U64(data.iter().skip(start).take(count).copied().collect())
            }
        }
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
    fn new(run_id: RunId, sample: &Sample, capacity: usize) -> Self {
        let alloc = capacity.min(65_536);
        Self {
            run_id,
            session_id: sample.device.session_id,
            stream_metadata: sample.stream.clone(),
            segment_metadata: sample.segment.clone(),
            sample_numbers: VecDeque::with_capacity(alloc),
            timestamps: VecDeque::with_capacity(alloc),
            columns: HashMap::new(),
            capacity,
        }
    }

    fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    fn push(&mut self, sample: &Sample) {
        self.sample_numbers.push_back(sample.n);
        self.timestamps.push_back(sample.timestamp_end());
        self.segment_metadata = sample.segment.clone();

        for col in &sample.columns {
            self.columns
                .entry(col.desc.index)
                .or_insert_with(|| ColumnBuffer::new(col.desc.clone(), self.capacity))
                .push(col.value.clone());
        }
    }

    fn pop_front(&mut self) {
        self.sample_numbers.pop_front();
        self.timestamps.pop_front();
        for col in self.columns.values_mut() {
            col.pop_front();
        }
    }

    fn drain_to_slice(&mut self, stream_key: &StreamKey) -> Option<DataSlice> {
        if self.sample_numbers.is_empty() {
            return None;
        }

        Some(DataSlice {
            stream_key: stream_key.clone(),
            run_id: self.run_id,
            session_id: self.session_id,
            segment_id: self.segment_metadata.segment_id,
            sample_numbers: self.sample_numbers.drain(..).collect(),
            timestamps: self.timestamps.drain(..).collect(),
            column_metadata: self
                .columns
                .iter()
                .map(|(&id, buf)| (id, buf.metadata().clone()))
                .collect(),
            columns: self
                .columns
                .iter_mut()
                .map(|(&id, buf)| (id, buf.drain()))
                .collect(),
            stream_metadata: self.stream_metadata.clone(),
            segment_metadata: self.segment_metadata.clone(),
        })
    }

    fn get_slice(
        &self,
        stream_key: &StreamKey,
        start: usize,
        count: usize,
        column_ids: &[ColumnId],
    ) -> Result<DataSlice, ReadError> {
        let mut columns = HashMap::new();
        for &col_id in column_ids {
            let buf = self.columns.get(&col_id).ok_or(ReadError::ColumnNotFound {
                stream_key: stream_key.clone(),
                column_id: col_id,
            })?;
            columns.insert(col_id, buf.get_range(start, count));
        }

        Ok(DataSlice {
            stream_key: stream_key.clone(),
            run_id: self.run_id,
            session_id: self.session_id,
            segment_id: self.segment_metadata.segment_id,
            sample_numbers: self
                .sample_numbers
                .iter()
                .skip(start)
                .take(count)
                .copied()
                .collect(),
            timestamps: self
                .timestamps
                .iter()
                .skip(start)
                .take(count)
                .copied()
                .collect(),
            columns,
            column_metadata: column_ids
                .iter()
                .filter_map(|&id| self.columns.get(&id).map(|b| (id, b.metadata().clone())))
                .collect(),
            stream_metadata: self.stream_metadata.clone(),
            segment_metadata: self.segment_metadata.clone(),
        })
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
    has_emitted_chunk: bool,
}

impl ActiveRun {
    fn new(run_id: RunId, sample: &Sample, capacity: usize) -> Self {
        let segment = &sample.segment;
        Self {
            run_id,
            session_id: sample.device.session_id,
            segment_id: segment.segment_id,
            effective_rate: segment.sampling_rate as f64 / segment.decimation as f64,
            time_ref_epoch: segment.time_ref_epoch.clone(),
            last_sample_number: sample.n,
            last_timestamp: sample.timestamp_end(),
            buffer: RunBuffer::new(run_id, sample, capacity),
            has_emitted_chunk: false,
        }
    }

    fn drain_chunk(&mut self, stream_key: &StreamKey) -> Option<(DataSlice, bool)> {
        let is_first = !self.has_emitted_chunk;
        self.buffer.drain_to_slice(stream_key).map(|slice| {
            self.has_emitted_chunk = true;
            (slice, is_first)
        })
    }
}

pub struct Buffer {
    capacity: usize,
    overflow_policy: OverflowPolicy,
    active_runs: HashMap<StreamKey, ActiveRun>,
    next_run_id: RunId,
    event_tx: crossbeam::channel::Sender<BufferEvent>,
}

impl Buffer {
    pub fn new(
        event_tx: crossbeam::channel::Sender<BufferEvent>,
        capacity: usize,
        overflow_policy: OverflowPolicy,
    ) -> Self {
        Self {
            capacity,
            overflow_policy,
            active_runs: HashMap::new(),
            next_run_id: 0,
            event_tx,
        }
    }

    fn emit(&self, event: BufferEvent) {
        let _ = self.event_tx.try_send(event);
    }

    pub fn process_sample(&mut self, sample: Sample, stream_key: StreamKey) {

        let needs_new_run = !sample.is_continuous() || !self.active_runs.contains_key(&stream_key);

        if needs_new_run {
            if self.overflow_policy == OverflowPolicy::Flush {
                self.flush_run(&stream_key);
            }

            let old_run_id = self.active_runs.get(&stream_key).map(|r| r.run_id);
            let new_run_id = self.next_run_id;
            self.next_run_id += 1;

            let boundary = sample.boundary.clone().unwrap_or(Boundary {
                reason: BoundaryReason::Initial,
                prior: None,
            });

            self.emit(BufferEvent::RunChanged {
                stream_key: stream_key.clone(),
                old_run_id,
                new_run_id,
                boundary,
            });

            self.active_runs.insert(
                stream_key.clone(),
                ActiveRun::new(new_run_id, &sample, self.capacity),
            );
        }

        let active = self.active_runs.get_mut(&stream_key).unwrap();
        active.buffer.push(&sample);
        active.last_sample_number = sample.n;
        active.last_timestamp = sample.timestamp_end();
        active.segment_id = sample.segment.segment_id;

        if active.buffer.len() > self.capacity {
            match self.overflow_policy {
                OverflowPolicy::DropOldest => {
                    active.buffer.pop_front();
                }
                OverflowPolicy::Flush => {
                    if let Some((slice, is_first)) = active.drain_chunk(&stream_key) {
                        self.emit(BufferEvent::DataChunk { slice, is_first_chunk: is_first });
                    }
                }
            }
        }
    }

    fn flush_run(&mut self, stream_key: &StreamKey) {
        if let Some(active) = self.active_runs.get_mut(stream_key) {
            if let Some((slice, is_first)) = active.drain_chunk(stream_key) {
                self.emit(BufferEvent::DataChunk { slice, is_first_chunk: is_first });
            }
        }
    }

    pub fn flush_all(&mut self) {
        let keys: Vec<_> = self.active_runs.keys().cloned().collect();
        for key in keys {
            self.flush_run(&key);
        }
    }

    pub fn get_run(&self, stream_key: &StreamKey) -> Option<&ActiveRun> {
        self.active_runs.get(stream_key)
    }

    pub fn read_aligned_window(
        &self,
        columns: &[ColumnKey],
        n: usize,
    ) -> Result<AlignedWindow, ReadError> {
        if columns.is_empty() {
            return Err(ReadError::NoColumnsRequested);
        }

        let mut by_stream: HashMap<StreamKey, Vec<ColumnId>> = HashMap::new();
        for col in columns {
            by_stream
                .entry(col.stream_key())
                .or_default()
                .push(col.column_id);
        }

        let mut slices = Vec::new();
        for (stream_key, col_ids) in &by_stream {
            let active = self
                .active_runs
                .get(stream_key)
                .ok_or(ReadError::NoActiveRun {
                    stream_key: stream_key.clone(),
                })?;

            let available = active.buffer.len();
            if available == 0 {
                return Err(ReadError::InsufficientData {
                    stream_key: stream_key.clone(),
                    requested: n,
                    available: 0,
                });
            }

            let count = n.min(available);
            let start = available.saturating_sub(count);
            slices.push(active.buffer.get_slice(stream_key, start, count, col_ids)?);
        }

        util::merge_slices(slices)
    }

    pub fn read_from_cursor(
        &self,
        columns: &[ColumnKey],
        cursors: &HashMap<StreamKey, CursorPosition>,
        n: usize,
    ) -> Result<AlignedWindow, ReadError> {
        if columns.is_empty() {
            return Err(ReadError::NoColumnsRequested);
        }

        let mut by_stream: HashMap<StreamKey, Vec<ColumnId>> = HashMap::new();
        for col in columns {
            by_stream
                .entry(col.stream_key())
                .or_default()
                .push(col.column_id);
        }

        let mut slices = Vec::new();
        for (stream_key, col_ids) in &by_stream {
            let active = self
                .active_runs
                .get(stream_key)
                .ok_or(ReadError::NoActiveRun {
                    stream_key: stream_key.clone(),
                })?;

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

            let earliest = *buf.sample_numbers.front().unwrap();
            if cursor.last_sample_number < earliest {
                return Err(ReadError::CursorOutOfBuffer {
                    stream_key: stream_key.clone(),
                    cursor_sample: cursor.last_sample_number,
                    earliest_available: earliest,
                });
            }

            let start = buf
                .sample_numbers
                .binary_search(&cursor.last_sample_number)
                .unwrap_or_else(|i| i);
            if start + n > buf.len() {
                return Err(ReadError::InsufficientData {
                    stream_key: stream_key.clone(),
                    requested: n,
                    available: buf.len().saturating_sub(start),
                });
            }

            slices.push(buf.get_slice(stream_key, start, n, col_ids)?);
        }

        util::merge_slices(slices)
    }

    pub fn read_aligned_tail(&self, columns: &[ColumnKey]) -> Result<AlignedWindow, ReadError> {
        if columns.is_empty() {
            return Err(ReadError::NoColumnsRequested);
        }

        let mut by_stream: HashMap<StreamKey, Vec<ColumnId>> = HashMap::new();
        for col in columns {
            by_stream
                .entry(col.stream_key())
                .or_default()
                .push(col.column_id);
        }

        let mut global_start = f64::MIN;
        let mut global_end = f64::MAX;
        let mut rate = 0.0;

        for stream_key in by_stream.keys() {
            let active = self
                .active_runs
                .get(stream_key)
                .ok_or(ReadError::NoActiveRun {
                    stream_key: stream_key.clone(),
                })?;

            if rate != 0.0 && (active.effective_rate - rate).abs() > 0.001 {
                return Err(ReadError::SamplingRateMismatch {
                    streams: by_stream.keys().cloned().collect(),
                    rates: vec![rate, active.effective_rate],
                });
            }
            rate = active.effective_rate;

            let buf = &active.buffer;
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

        let mut slices = Vec::new();
        for (stream_key, col_ids) in &by_stream {
            let buf = &self.active_runs.get(stream_key).unwrap().buffer;

            let start = buf
                .timestamps
                .iter()
                .position(|&t| t >= global_start)
                .unwrap_or(0);
            let end = buf
                .timestamps
                .iter()
                .rposition(|&t| t <= global_end)
                .unwrap_or(buf.len().saturating_sub(1));
            let count = end.saturating_sub(start) + 1;

            if count > 0 {
                slices.push(buf.get_slice(stream_key, start, count, col_ids)?);
            }
        }

        util::merge_slices(slices)
    }
}

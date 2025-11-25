//! Buffer
//! Stores `Sample`s, emits data according to a `Reader` or `OverflowPolicy`.
//!
//! Assumptions:
//! 1. Time stamps of `Sample`s are exactly aligned
//! 2. Sample rates of columns sharing the same name are the same
//! 3. Sample numbers are independent between `Device`s

use crate::data::{ColumnData, Sample, util};
use crate::device::CursorPosition;
use crate::tio::proto::identifiers::*;
use crate::tio::proto::{
    BufferType, DeviceRoute, 
    StreamMetadata, SegmentMetadata, ColumnMetadata
};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

/// Defines how the Buffer handles data when it reaches capacity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OverflowPolicy {
    /// Ring Buffer Mode: Drops the oldest sample to make room for the new one.
    /// Use this for real-time monitoring (Oscilloscopes) where only recent history matters.
    DropOldest,

    /// Streaming Mode: Flushes the current buffer contents as a `DataChunk` event,
    /// then clears the buffer to make room. 
    /// Use this for logging to disk (HDF5) to keep memory usage constant.
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

/// Multiple streams aligned by sample number.
/// Columns keyed globally since multiple streams are merged.
#[derive(Debug, Clone)]
pub struct AlignedWindow {
    pub sample_numbers: Vec<SampleNumber>,
    pub timestamps: Vec<f64>,
    pub columns: HashMap<ColumnKey, ColumnBatch>,
    pub stream_metadata: HashMap<StreamKey, Arc<StreamMetadata>>,
    pub segment_metadata: HashMap<StreamKey, Arc<SegmentMetadata>>,
    pub column_metadata: HashMap<ColumnKey, Arc<ColumnMetadata>>,
    pub session_ids: HashMap<StreamKey, SessionId>,
}

pub struct ActiveSegment {
    pub session_id: SessionId,
    pub segment_id: SegmentId,
    pub buffer: SegmentBuffer,

    pub last_sample_number: SampleNumber,
    pub last_timestamp: f64,
    
    pub has_emitted_chunk: bool,
}

// Insertion-time events (things that happened in the sample stream)
pub enum BufferEvent {
    Samples(Vec<(Sample, DeviceRoute)>),
    MetadataChanged(DeviceRoute),
    SegmentChanged(DeviceRoute),
    RouteDiscovered(DeviceRoute),
    SessionChanged {
        route: DeviceRoute,
        stream_id: StreamId,
        old_id: SessionId,
        new_id: SessionId,
    },
    SamplesSkipped {
        route: DeviceRoute,
        stream_id: StreamId,
        session_id: SessionId,
        expected: SampleNumber,
        received: SampleNumber,
        count: u32,
    },
    SamplesBackward {
        route: DeviceRoute,
        stream_id: StreamId,
        session_id: SessionId,
        previous: SampleNumber,
        current: SampleNumber,
    },
    DataChunk {
        slice: DataSlice,
        is_first_chunk: bool,
    },
}

// Read-time errors (problems when trying to access buffer data)
#[derive(Debug)]
pub enum ReadError {
    NoColumnsRequested,
    NoCursorForStream {
        stream_key: StreamKey,
    },
    NoActiveSegment {
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
    SegmentMismatch {
        column_specs: Vec<ColumnKey>,
        segments: Vec<(SessionId, SegmentId)>,
    },
    SegmentChanged {
        stream_key: StreamKey,
        cursor_segment: (SessionId, SegmentId),
        current_segment: (SessionId, SegmentId),
    },
    CursorOutOfBuffer {
        stream_key: StreamKey,
        cursor_sample: SampleNumber,
        earliest_available: SampleNumber,
    },
}

pub struct SegmentBuffer {
    pub session_id: SessionId,
    pub stream_metadata: Arc<StreamMetadata>, 
    pub segment_metadata: Arc<SegmentMetadata>,
    pub sample_numbers: VecDeque<SampleNumber>,
    pub columns: HashMap<ColumnId, ColumnBuffer>,
    pub capacity: usize,
}

#[derive(Debug)]
pub enum ColumnBuffer {
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
    pub fn metadata(&self) -> &Arc<ColumnMetadata> {
        match self {
            Self::F64 { metadata, .. } => metadata,
            Self::I64 { metadata, .. } => metadata,
            Self::U64 { metadata, .. } => metadata,
        }
    }

    pub fn push(&mut self, value: ColumnData) {
        match (self, value) {
            (Self::F64 { data, .. }, ColumnData::Float(v)) => data.push_back(v),
            (Self::F64 { data, .. }, ColumnData::Int(v)) => data.push_back(v as f64),
            
            (Self::I64 { data, .. }, ColumnData::Int(v)) => data.push_back(v),
            (Self::U64 { data, .. }, ColumnData::UInt(v)) => data.push_back(v),
            
            (s, v) => eprintln!("Type mismatch: Buffer is {:?} but got {:?}", s, v),
        }
    }

    pub fn pop_front(&mut self) {
        match self {
            Self::F64 { data, .. } => { data.pop_front(); },
            Self::I64 { data, .. } => { data.pop_front(); },
            Self::U64 { data, .. } => { data.pop_front(); },
        }
    }

    pub fn drain(&mut self) -> ColumnBatch {
        match self {
            Self::F64 { data, .. } => ColumnBatch::F64(data.drain(..).collect()),
            Self::I64 { data, .. } => ColumnBatch::I64(data.drain(..).collect()),
            Self::U64 { data, .. } => ColumnBatch::U64(data.drain(..).collect()),
        }
    }

    pub fn get_range(&self, start_idx: usize, count: usize) -> ColumnBatch {
        match self {
            Self::F64 { data, .. } => {
                ColumnBatch::F64(data.iter().skip(start_idx).take(count).copied().collect())
            }
            Self::I64 { data, .. } => {
                ColumnBatch::I64(data.iter().skip(start_idx).take(count).copied().collect())
            }
            Self::U64 { data, .. } => {
                ColumnBatch::U64(data.iter().skip(start_idx).take(count).copied().collect())
            }
        }
    }
}

impl SegmentBuffer {
    fn new(
        session_id: SessionId, 
        stream_metadata: Arc<StreamMetadata>, 
        segment_metadata: Arc<SegmentMetadata>, 
        capacity: usize
    ) -> Self {
        let initial_alloc = std::cmp::min(capacity, 65_536);
        Self {
            session_id,
            stream_metadata,
            segment_metadata,
            sample_numbers: VecDeque::with_capacity(initial_alloc),
            columns: HashMap::new(),
            capacity,
        }
    }

    fn len(&self) -> usize {
        self.sample_numbers.len()
    }

    fn push_sample(&mut self, sample: Sample) {
        self.sample_numbers.push_back(sample.n);

        for column in sample.columns {
            let col_index = column.desc.index;

            let col_buffer = self.columns
                .entry(col_index)
                .or_insert_with(|| {
                    let meta = column.desc.clone();
                    let initial_alloc = std::cmp::min(self.capacity, 65_536);
                    
                    match meta.data_type.buffer_type() {
                        BufferType::Float => ColumnBuffer::F64 { 
                            metadata: meta, 
                            data: VecDeque::with_capacity(initial_alloc) 
                        },
                        BufferType::Int => ColumnBuffer::I64 { 
                            metadata: meta, 
                            data: VecDeque::with_capacity(initial_alloc) 
                        },
                        BufferType::UInt => ColumnBuffer::U64 { 
                            metadata: meta, 
                            data: VecDeque::with_capacity(initial_alloc) 
                        },
                    }
                });
            col_buffer.push(column.value);
        }
    }

    fn pop_front(&mut self) {
        self.sample_numbers.pop_front();
        for col_buffer in self.columns.values_mut() {
            col_buffer.pop_front();
        }
    }

    fn compute_timestamps(&self, sample_numbers: &[SampleNumber]) -> Vec<f64> {
        let rate = (self.segment_metadata.sampling_rate / self.segment_metadata.decimation) as f64;
        let start_time = self.segment_metadata.start_time as f64;
        sample_numbers.iter()
            .map(|&n| start_time + (n as f64) / rate)
            .collect()
    }

    fn collect_column_metadata(&self, column_ids: &[ColumnId]) -> HashMap<ColumnId, Arc<ColumnMetadata>> {
        column_ids.iter()
            .filter_map(|&id| {
                self.columns.get(&id).map(|buf| (id, buf.metadata().clone()))
            })
            .collect()
    }

    fn drain_to_slice(&mut self, stream_key: &StreamKey) -> Option<DataSlice> {
        if self.sample_numbers.is_empty() {
            return None;
        }

        let sample_numbers: Vec<_> = self.sample_numbers.drain(..).collect();
        let timestamps = self.compute_timestamps(&sample_numbers);

        let mut columns = HashMap::new();
        let mut column_metadata = HashMap::new();
        
        for (id, buf) in &mut self.columns {
            column_metadata.insert(*id, buf.metadata().clone());
            columns.insert(*id, buf.drain());
        }

        Some(DataSlice {
            stream_key: stream_key.clone(),
            session_id: self.session_id,
            segment_id: self.segment_metadata.segment_id,
            sample_numbers,
            timestamps,
            columns,
            stream_metadata: self.stream_metadata.clone(),
            segment_metadata: self.segment_metadata.clone(),
            column_metadata,
        })
    }

    fn get_latest_n(
        &self,
        stream_key: &StreamKey,
        n: usize,
        column_ids: &[ColumnId],
    ) -> Result<DataSlice, ReadError> {
        let available = self.sample_numbers.len();

        if available == 0 {
            return Err(ReadError::InsufficientData {
                stream_key: stream_key.clone(),
                requested: n,
                available: 0,
            });
        }

        let n = n.min(available);
        let start_idx = available.saturating_sub(n);

        let sample_numbers: Vec<_> = self
            .sample_numbers
            .iter()
            .skip(start_idx)
            .take(n)
            .copied()
            .collect();

        let timestamps = self.compute_timestamps(&sample_numbers);

        let mut columns = HashMap::new();
        for &col_id in column_ids {
            let col_buffer = self.columns.get(&col_id)
                .ok_or(ReadError::ColumnNotFound {
                    stream_key: stream_key.clone(),
                    column_id: col_id,
                })?;
            columns.insert(col_id, col_buffer.get_range(start_idx, n));
        }

        let column_metadata = self.collect_column_metadata(column_ids);

        Ok(DataSlice {
            stream_key: stream_key.clone(),
            session_id: self.session_id,
            segment_id: self.segment_metadata.segment_id,
            sample_numbers,
            timestamps,
            columns,
            stream_metadata: self.stream_metadata.clone(),
            segment_metadata: self.segment_metadata.clone(),
            column_metadata,
        })
    }

    fn get_range(
        &self,
        stream_key: &StreamKey,
        start_sample: SampleNumber,
        count: usize,
        column_ids: &[ColumnId],
    ) -> Result<DataSlice, ReadError> {
        if self.sample_numbers.is_empty() {
            return Err(ReadError::InsufficientData {
                stream_key: stream_key.clone(),
                requested: count,
                available: 0,
            });
        }

        let earliest_sample = self.sample_numbers.front().copied().unwrap();
        if start_sample < earliest_sample {
            return Err(ReadError::CursorOutOfBuffer {
                stream_key: stream_key.clone(),
                cursor_sample: start_sample,
                earliest_available: earliest_sample,
            });
        }

        let start_idx = match self.sample_numbers.binary_search(&start_sample) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        if start_idx + count > self.sample_numbers.len() {
            let available = self.sample_numbers.len().saturating_sub(start_idx);
            return Err(ReadError::InsufficientData {
                stream_key: stream_key.clone(),
                requested: count,
                available,
            });
        }

        let sample_numbers: Vec<_> = self
            .sample_numbers
            .iter()
            .skip(start_idx)
            .take(count)
            .copied()
            .collect();

        let timestamps = self.compute_timestamps(&sample_numbers);

        let mut columns = HashMap::new();
        for &col_id in column_ids {
            let col_buffer = self.columns.get(&col_id)
                .ok_or(ReadError::ColumnNotFound {
                    stream_key: stream_key.clone(),
                    column_id: col_id,
                })?;
            columns.insert(col_id, col_buffer.get_range(start_idx, count));
        }

        let column_metadata = self.collect_column_metadata(column_ids);

        Ok(DataSlice {
            stream_key: stream_key.clone(),
            session_id: self.session_id,
            segment_id: self.segment_metadata.segment_id,
            sample_numbers,
            timestamps,
            columns,
            stream_metadata: self.stream_metadata.clone(),
            segment_metadata: self.segment_metadata.clone(),
            column_metadata,
        })
    }
}

pub struct Buffer {
    routes_seen: HashSet<DeviceRoute>,
    pub active_segments: HashMap<StreamKey, ActiveSegment>,
    event_tx: crossbeam::channel::Sender<BufferEvent>,
    pub forward_samples: bool,
    pub overflow_policy: OverflowPolicy,
    capacity: usize,
}

impl Buffer {
    pub fn new(
        event_tx: crossbeam::channel::Sender<BufferEvent>,
        capacity: usize,
        forward_samples: bool,
        overflow_policy: OverflowPolicy,
    ) -> Self {
        Buffer {
            routes_seen: HashSet::new(),
            active_segments: HashMap::new(),
            event_tx,
            forward_samples,
            overflow_policy,
            capacity,
        }
    }

    pub fn process_sample(&mut self, sample: Sample, route: DeviceRoute) {
        if self.routes_seen.insert(route.clone()) {
            let _ = self
                .event_tx
                .try_send(BufferEvent::RouteDiscovered(route.clone()));
        }
        if sample.meta_changed {
            let _ = self
                .event_tx
                .try_send(BufferEvent::MetadataChanged(route.clone()));
        }
        if sample.segment_changed {
            let _ = self
                .event_tx
                .try_send(BufferEvent::SegmentChanged(route.clone()));
        }
        if self.forward_samples {
            let _ = self.event_tx.try_send(BufferEvent::Samples(vec![(sample.clone(), route.clone())]));
        }

        let stream_id = sample.stream.stream_id;
        let session_id = sample.device.session_id;
        let segment_id = sample.segment.segment_id;
        let stream_key = StreamKey::new(route.clone(), stream_id);

        let needs_new_segment = match self.active_segments.get(&stream_key) {
            None => true,
            Some(active) => active.session_id != session_id || active.segment_id != segment_id,
        };

        if needs_new_segment {
            self.handle_segment_change(sample, route, stream_key);
        } else {
            self.check_continuity_and_push(sample, route, stream_key);
        }
    }

    fn handle_segment_change(&mut self, sample: Sample, route: DeviceRoute, stream_key: StreamKey) {
        let session_id = sample.device.session_id;
        let segment_id = sample.segment.segment_id;
        let stream_id = sample.stream.stream_id;

        if self.overflow_policy == OverflowPolicy::Flush {
            self.flush_active_segment(&stream_key);
        }

        if let Some(active) = self.active_segments.get(&stream_key) {
            if active.session_id != session_id {
                let _ = self.event_tx.try_send(BufferEvent::SessionChanged {
                    route: route.clone(),
                    stream_id,
                    old_id: active.session_id,
                    new_id: session_id,
                });
            }
        }

        let new_segment = ActiveSegment {
            session_id,
            segment_id,
            buffer: SegmentBuffer::new(
                session_id, 
                sample.stream.clone(), 
                sample.segment.clone(), 
                self.capacity
            ),
            last_sample_number: sample.n,
            last_timestamp: sample.timestamp_end(),
            has_emitted_chunk: false,
        };

        self.active_segments.insert(stream_key.clone(), new_segment);

        let active = self.active_segments.get_mut(&stream_key).unwrap();
        active.buffer.push_sample(sample);
    }

    fn check_continuity_and_push(
        &mut self,
        sample: Sample,
        route: DeviceRoute,
        stream_key: StreamKey,
    ) {
        let stream_id = sample.stream.stream_id;
        let session_id = sample.device.session_id;

        let active = self.active_segments.get_mut(&stream_key).unwrap();

        let last_n = active.last_sample_number;

        // Sample number continuity
        if sample.n < last_n {
            let _ = self.event_tx.try_send(BufferEvent::SamplesBackward {
                route: route.clone(),
                stream_id,
                session_id,
                previous: last_n,
                current: sample.n,
            });
        } else if sample.n > last_n + 1 {
            let skipped_count = sample.n.saturating_sub(last_n + 1);
            let _ = self.event_tx.try_send(BufferEvent::SamplesSkipped {
                route: route.clone(),
                stream_id,
                session_id,
                expected: last_n + 1,
                received: sample.n,
                count: skipped_count,
            });
        }

        // Update tracking
        active.last_sample_number = sample.n;
        active.last_timestamp = sample.timestamp_end();

        // Push to buffer
        active.buffer.push_sample(sample);

        if active.buffer.len() > self.capacity {
            match self.overflow_policy {
                OverflowPolicy::DropOldest => {
                    active.buffer.pop_front();
                },
                OverflowPolicy::Flush => {
                    self.flush_active_segment(&stream_key);
                }
            }
        }
    }

    fn flush_active_segment(&mut self, stream_key: &StreamKey) {
        if let Some(active) = self.active_segments.get_mut(stream_key) {
            let is_first = !active.has_emitted_chunk;
            
            if let Some(slice) = active.buffer.drain_to_slice(stream_key) {
                active.has_emitted_chunk = true;
                let _ = self.event_tx.try_send(BufferEvent::DataChunk {
                    slice,
                    is_first_chunk: is_first,
                });
            }
        }
    }

    pub fn flush_all(&mut self) {
        let keys: Vec<StreamKey> = self.active_segments.keys().cloned().collect();
        for key in keys {
            self.flush_active_segment(&key);
        }
    }

    pub fn read_aligned_window(
        &self,
        columns: &[ColumnKey],
        n_samples: usize,
    ) -> Result<AlignedWindow, ReadError> {
        if columns.is_empty() {
            return Err(ReadError::NoColumnsRequested);
        }

        let mut by_stream: HashMap<StreamKey, Vec<&ColumnKey>> = HashMap::new();
        for col_spec in columns {
            by_stream
                .entry(col_spec.stream_key())
                .or_default()
                .push(col_spec);
        }

        let mut slices = Vec::new();

        for (stream_key, col_specs) in &by_stream {
            let active = self.active_segments.get(stream_key)
                .ok_or_else(|| ReadError::NoActiveSegment {
                    stream_key: stream_key.clone(),
                })?;

            let column_ids: Vec<_> = col_specs.iter().map(|cs| cs.column_id).collect();
            
            let slice = active.buffer.get_latest_n(stream_key, n_samples, &column_ids)?;
            slices.push(slice);
        }

        util::merge_slices(slices)
    }

    pub fn read_from_cursor(
        &self,
        columns: &[ColumnKey],
        cursors: &HashMap<StreamKey, CursorPosition>,
        n_samples: usize,
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

        for (stream_key, column_ids) in &by_stream {
            let active = self
                .active_segments
                .get(stream_key)
                .ok_or(ReadError::NoActiveSegment {
                    stream_key: stream_key.clone(),
                })?;

            let cursor = cursors
                .get(stream_key)
                .ok_or(ReadError::NoCursorForStream {
                    stream_key: stream_key.clone(),
                })?;

            // Validate cursor matches current segment
            if cursor.session_id != active.session_id || cursor.segment_id != active.segment_id {
                return Err(ReadError::SegmentChanged {
                    stream_key: stream_key.clone(),
                    cursor_segment: (cursor.session_id, cursor.segment_id),
                    current_segment: (active.session_id, active.segment_id),
                });
            }

            let slice = active.buffer.get_range(
                stream_key,
                cursor.last_sample_number,
                n_samples,
                column_ids,
            )?;
            
            slices.push(slice);
        }

        util::merge_slices(slices)
    }

    pub fn read_aligned_tail(
        &self,
        columns: &[ColumnKey],
    ) -> Result<AlignedWindow, ReadError> {
        if columns.is_empty() {
            return Err(ReadError::NoColumnsRequested);
        }

        let mut by_stream: HashMap<StreamKey, Vec<ColumnId>> = HashMap::new();
        for col in columns {
            by_stream.entry(col.stream_key()).or_default().push(col.column_id);
        }

        let mut global_start = f64::MIN;
        let mut global_end = f64::MAX;
        let mut rate = 0.0;

        // First pass: find overlapping time window
        for stream_key in by_stream.keys() {
            let seg = self.active_segments.get(stream_key)
                .ok_or(ReadError::NoActiveSegment { stream_key: stream_key.clone() })?;
            
            let meta = &seg.buffer.segment_metadata;
            let r = (meta.sampling_rate as f64) / (meta.decimation as f64);
            
            if rate != 0.0 && (r - rate).abs() > 0.001 {
                return Err(ReadError::SamplingRateMismatch {
                    streams: by_stream.keys().cloned().collect(),
                    rates: vec![rate, r],
                });
            }
            rate = r;

            let samples = &seg.buffer.sample_numbers;
            if samples.is_empty() {
                return Err(ReadError::InsufficientData {
                    stream_key: stream_key.clone(),
                    requested: 0,
                    available: 0,
                });
            }

            // Find the start of the contiguous tail
            let last_idx = samples.len() - 1;
            let last_val = samples[last_idx];
            let target_offset = (last_val as i64) - (last_idx as i64);
            let mut low = 0;
            let mut high = last_idx;

            while low < high {
                let mid = low + (high - low) / 2;
                let mid_offset = (samples[mid] as i64) - (mid as i64);
                if mid_offset < target_offset {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            
            let tail_start_time = meta.start_time as f64 + (samples[low] as f64 / rate);
            let tail_end_time = meta.start_time as f64 + (last_val as f64 / rate);

            if tail_start_time > global_start { global_start = tail_start_time; }
            if tail_end_time < global_end { global_end = tail_end_time; }
        }

        if global_start >= global_end {
            return Err(ReadError::InsufficientData {
                stream_key: StreamKey::new(DeviceRoute::root(), 0),
                requested: 0,
                available: 0,
            });
        }

        let samples_to_read = ((global_end - global_start) * rate + 0.5) as usize;
        let mut slices = Vec::new();

        for (stream_key, column_ids) in &by_stream {
            let seg = self.active_segments.get(stream_key).unwrap();
            let start_time = seg.buffer.segment_metadata.start_time as f64;
            
            let start_n = ((global_start - start_time) * rate + 0.5) as u32;

            let slice = seg.buffer.get_range(stream_key, start_n, samples_to_read, column_ids)?;
            slices.push(slice);
        }

        util::merge_slices(slices)
    }
}
//! Buffer
//! Stores `Sample`s, emits data according to a `Reader`.
//!
//! Assumptions:
//! 1. Time stamps of `Sample`s are exactly aligned
//! 2. Sample rates of columns sharing the same name are the same
//! 3. Sample numbers are independent between `Device`s

use crate::{
    data::{ColumnData, Sample},
    device::{
        util, ColumnId, ColumnSpec, CursorPosition, SampleNumber, SegmentId, SessionId,
        StreamId, StreamKey,
    },
    tio::{
        proto::{
            meta::{ColumnMetadata, SegmentMetadata},
            DeviceRoute,
        },
    },
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

pub struct AlignedWindow {
    pub sample_numbers: Vec<SampleNumber>,
    pub timestamps: Vec<f64>,
    pub columns: HashMap<ColumnSpec, Vec<ColumnData>>,
    pub segment_metadata: HashMap<StreamKey, Arc<SegmentMetadata>>,
    pub session_ids: HashMap<StreamKey, SessionId>,
}

pub struct SegmentWindow {
    pub sample_numbers: Vec<SampleNumber>,
    pub timestamps: Vec<f64>,
    pub columns: HashMap<ColumnId, Vec<ColumnData>>,
}

pub struct ActiveSegment {
    pub session_id: SessionId,
    pub segment_id: SegmentId,
    pub buffer: SegmentBuffer,

    pub last_sample_number: SampleNumber,
    pub last_timestamp: f64,
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
        // ADD POLICY TO INTERPOLATE SAMPLE
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
        column_specs: Vec<ColumnSpec>,
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
    pub segment_metadata: Arc<SegmentMetadata>,
    pub sample_numbers: VecDeque<SampleNumber>,
    pub columns: HashMap<ColumnId, ColumnBuffer>,
    pub capacity: usize,
}

pub struct ColumnBuffer {
    pub metadata: Arc<ColumnMetadata>,
    pub data: VecDeque<ColumnData>,
}

impl SegmentBuffer {
    fn new(session_id: SessionId, segment_metadata: Arc<SegmentMetadata>, capacity: usize) -> Self {
        Self {
            session_id,
            segment_metadata,
            sample_numbers: VecDeque::with_capacity(capacity),
            columns: HashMap::new(),
            capacity,
        }
    }

    fn push_sample(&mut self, sample: Sample) {
        self.sample_numbers.push_back(sample.n);

        for column in sample.columns {
            let col_buffer =
                self.columns
                    .entry(column.desc.index)
                    .or_insert_with(|| ColumnBuffer {
                        metadata: column.desc.clone(),
                        data: VecDeque::with_capacity(self.capacity),
                    });
            col_buffer.data.push_back(column.value);
        }

        // Enforce capacity
        if self.sample_numbers.len() > self.capacity {
            self.sample_numbers.pop_front();
            for col_buffer in self.columns.values_mut() {
                col_buffer.data.pop_front();
            }
        }
    }

    fn get_latest_n(&self, n: usize, column_ids: &[ColumnId]) -> Result<SegmentWindow, ReadError> {
        let available = self.sample_numbers.len();

        if available == 0 {
            return Err(ReadError::InsufficientData {
                stream_key: (DeviceRoute::root(), 0), // Will be overwritten by caller
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

        let rate = (self.segment_metadata.sampling_rate / self.segment_metadata.decimation) as f64;
        let start_time = self.segment_metadata.start_time as f64;
        let timestamps: Vec<_> = sample_numbers
            .iter()
            .map(|&sample_n| start_time + (sample_n as f64) / rate)
            .collect();

        let mut columns_data = HashMap::new();
        for &col_id in column_ids {
            if let Some(col_buffer) = self.columns.get(&col_id) {
                let data: Vec<_> = col_buffer
                    .data
                    .iter()
                    .skip(start_idx)
                    .take(n)
                    .cloned()
                    .collect();
                columns_data.insert(col_id, data);
            } else {
                return Err(ReadError::ColumnNotFound {
                    stream_key: (DeviceRoute::root(), 0), // Will be overwritten by caller
                    column_id: col_id,
                });
            }
        }

        Ok(SegmentWindow {
            sample_numbers,
            timestamps,
            columns: columns_data,
        })
    }

    fn get_range(
        &self,
        start_sample: SampleNumber,
        count: usize,
        column_ids: &[ColumnId],
    ) -> Result<SegmentWindow, ReadError> {
        let start_idx = self
            .sample_numbers
            .iter()
            .position(|&n| n == start_sample)
            .ok_or(ReadError::CursorOutOfBuffer {
                stream_key: (DeviceRoute::root(), 0),
                cursor_sample: start_sample,
                earliest_available: self.sample_numbers.front().copied().unwrap_or(0),
            })?;

        let available_after = self.sample_numbers.len().saturating_sub(start_idx + 1);
        if available_after < count {
            return Err(ReadError::InsufficientData {
                stream_key: (DeviceRoute::root(), 0),
                requested: count,
                available: available_after,
            });
        }

        let read_start = start_idx + 1;

        let sample_numbers: Vec<_> = self
            .sample_numbers
            .iter()
            .skip(read_start)
            .take(count)
            .copied()
            .collect();

        let rate = (self.segment_metadata.sampling_rate / self.segment_metadata.decimation) as f64;
        let start_time = self.segment_metadata.start_time as f64;
        let timestamps: Vec<_> = sample_numbers
            .iter()
            .map(|&n| start_time + (n as f64) / rate)
            .collect();

        let mut columns_data = HashMap::new();
        for &col_id in column_ids {
            let col_buffer = self.columns.get(&col_id).ok_or(ReadError::ColumnNotFound {
                stream_key: (DeviceRoute::root(), 0),
                column_id: col_id,
            })?;

            let data: Vec<_> = col_buffer
                .data
                .iter()
                .skip(read_start)
                .take(count)
                .cloned()
                .collect();
            columns_data.insert(col_id, data);
        }

        Ok(SegmentWindow {
            sample_numbers,
            timestamps,
            columns: columns_data,
        })
    }
}
pub struct Buffer {
    routes_seen: HashSet<DeviceRoute>,
    pub active_segments: HashMap<StreamKey, ActiveSegment>,
    event_tx: crossbeam::channel::Sender<BufferEvent>,
    pub forward_samples: bool,
    capacity: usize,
}

impl Buffer {
    pub fn new(
        event_tx: crossbeam::channel::Sender<BufferEvent>,
        capacity: usize,
        forward_samples: bool,
    ) -> Self {
        Buffer {
            routes_seen: HashSet::new(),
            active_segments: HashMap::new(),
            event_tx,
            forward_samples,
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
        let stream_key = (route.clone(), stream_id);

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
            buffer: SegmentBuffer::new(session_id, sample.segment.clone(), self.capacity), // PASS session_id
            last_sample_number: sample.n,
            last_timestamp: sample.timestamp_end(),
        };

        self.active_segments.insert(stream_key.clone(), new_segment);

        self.active_segments
            .get_mut(&stream_key)
            .unwrap()
            .buffer
            .push_sample(sample);
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
    }

    pub fn read_aligned_window(
        &self,
        columns: &[ColumnSpec],
        n_samples: usize,
    ) -> Result<AlignedWindow, ReadError> {
        if columns.is_empty() {
            return Err(ReadError::NoColumnsRequested);
        }

        // Group columns by stream
        let mut by_stream: HashMap<StreamKey, Vec<&ColumnSpec>> = HashMap::new();
        for col_spec in columns {
            by_stream
                .entry(col_spec.stream_key())
                .or_default()
                .push(col_spec);
        }

        // Fetch window for each stream
        let mut stream_windows = HashMap::new();

        for (stream_key, col_specs) in &by_stream {
            let active =
                self.active_segments
                    .get(stream_key)
                    .ok_or_else(|| ReadError::NoActiveSegment {
                        stream_key: stream_key.clone(),
                    })?;

            let column_ids: Vec<_> = col_specs.iter().map(|cs| cs.column_id).collect();
            let window = active
                .buffer
                .get_latest_n(n_samples, &column_ids)
                .map_err(|e| Self::contextualize_error(e, stream_key))?;

            stream_windows.insert(stream_key.clone(), (window, active));
        }

        if stream_windows.len() > 1 {
            util::validate_sampling_rates(&stream_windows)?;
            util::validate_stream_alignment(&stream_windows)?;
        }

        util::merge_windows(stream_windows, by_stream)
    }

    pub fn read_from_cursor(
        &self,
        columns: &[ColumnSpec],
        cursors: &HashMap<StreamKey, CursorPosition>,
        n_samples: usize,
    ) -> Result<AlignedWindow, ReadError> {
        if columns.is_empty() {
            return Err(ReadError::NoColumnsRequested);
        }

        let stream_key = columns[0].stream_key();
        let active = self
            .active_segments
            .get(&stream_key)
            .ok_or(ReadError::NoActiveSegment {
                stream_key: stream_key.clone(),
            })?;

        let cursor = cursors
            .get(&stream_key)
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

        let column_ids: Vec<_> = columns.iter().map(|c| c.column_id).collect();
        let window = active
            .buffer
            .get_range(cursor.last_sample_number, n_samples, &column_ids)
            .map_err(|e| Self::contextualize_error(e, &stream_key))?;

        // Map columns directly using the requested column specs
        let columns_map: HashMap<ColumnSpec, Vec<ColumnData>> = columns
            .iter()
            .filter_map(|col_spec| {
                window
                    .columns
                    .get(&col_spec.column_id)
                    .map(|data| (col_spec.clone(), data.clone()))
            })
            .collect();

        Ok(AlignedWindow {
            sample_numbers: window.sample_numbers,
            timestamps: window.timestamps,
            columns: columns_map,
            segment_metadata: [(stream_key.clone(), active.buffer.segment_metadata.clone())].into(),
            session_ids: [(stream_key, active.session_id)].into(),
        })
    }

    fn contextualize_error(err: ReadError, stream_key: &StreamKey) -> ReadError {
        match err {
            ReadError::InsufficientData {
                requested,
                available,
                ..
            } => ReadError::InsufficientData {
                stream_key: stream_key.clone(),
                requested,
                available,
            },
            ReadError::ColumnNotFound { column_id, .. } => ReadError::ColumnNotFound {
                stream_key: stream_key.clone(),
                column_id,
            },
            other => other,
        }
    }
}

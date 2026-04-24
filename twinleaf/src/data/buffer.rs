use crate::data::{ColumnData, CursorPosition, Sample};
use crate::tio::proto::identifiers::*;
use crate::tio::proto::meta::MetadataEpoch;
use crate::tio::proto::{BufferType, ColumnMetadata, DeviceRoute, SegmentMetadata, StreamMetadata};

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

pub type RunId = u64;

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
pub struct AlignedWindow {
    pub sample_numbers: HashMap<StreamKey, Vec<SampleNumber>>,
    pub timestamps: Vec<f64>,
    pub columns: HashMap<ColumnKey, ColumnBatch>,
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
        self.timestamps
            .iter()
            .skip(start)
            .take(count)
            .copied()
            .collect()
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

    pub fn process_sample(&mut self, sample: Sample, stream_key: StreamKey) {
        let needs_new_run = !sample.is_continuous() || !self.active_runs.contains_key(&stream_key);

        if needs_new_run {
            let new_run_id = self.next_run_id;
            self.next_run_id += 1;
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
            active.buffer.pop_front();
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

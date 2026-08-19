//! Ordered metadata, schema, and continuity state for routed TIO packets.
//!
//! This module validates stream payloads and describes their rows without
//! materializing sample values. Packet consumers decide whether to decode the
//! validated bytes immediately or retain only their metadata and boundaries.

use super::sample::{Boundary, BoundaryReason, Generations};
use crate::tio;
use proto::meta::MetadataType;
use proto::route::RouteError;
use proto::DeviceRoute;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tio::proto;
use tio::proto::identifiers::MAX_SAMPLE_NUMBER;
use tio::proto::meta::{
    ColumnMetadata, DeviceMetadata, MetadataContent, SegmentMetadata, StreamMetadata,
};

const MAX_METADATA_PER_RPC: usize = 16;
const META_RPC_ID: u16 = 7855;

#[derive(Debug, Clone)]
struct MetadataRequest {
    mtype: MetadataType,
    stream_id: u8,
    index: u8,
}

impl MetadataRequest {
    fn device() -> Self {
        Self {
            mtype: MetadataType::Device,
            stream_id: 0,
            index: 0,
        }
    }
    fn stream(stream_id: u8) -> Self {
        Self {
            mtype: MetadataType::Stream,
            stream_id,
            index: 0,
        }
    }
    fn segment(index: u8, stream_id: u8) -> Self {
        Self {
            mtype: MetadataType::Segment,
            stream_id,
            index,
        }
    }
    fn column(index: u8, stream_id: u8) -> Self {
        Self {
            mtype: MetadataType::Column,
            stream_id,
            index,
        }
    }
}

fn encode_metadata_request(requests: &[MetadataRequest]) -> Vec<u8> {
    assert!(requests.len() <= MAX_METADATA_PER_RPC);
    let mut encoded = Vec::with_capacity(requests.len() * 3);
    for request in requests {
        encoded.push(request.mtype.clone().into());
        encoded.push(request.stream_id);
        encoded.push(request.index);
    }
    encoded
}

/// Pack missing metadata descriptors into `dev.metadata` RPC requests.
fn build_metadata_request_packets(requests: &[MetadataRequest]) -> Vec<tio::Packet> {
    let mut packets = Vec::new();
    let mut remaining = requests;
    while !remaining.is_empty() {
        let request_count = if remaining.len() > MAX_METADATA_PER_RPC {
            MAX_METADATA_PER_RPC
        } else if remaining.len() == 1 {
            // If we don't know anything about the device, send zero
            // arguments to get an automatic reply fitting as many things
            // as possible at the beginning, to bootstrap the process
            // more efficiently
            if let MetadataType::Device = remaining[0].mtype {
                remaining = &remaining[1..];
                0
            } else {
                1
            }
        } else {
            remaining.len()
        };
        packets.push(tio::Packet::rpc_request(
            "dev.metadata",
            &encode_metadata_request(&remaining[..request_count]),
            META_RPC_ID,
            DeviceRoute::root(),
        ));
        remaining = &remaining[request_count..];
    }
    packets
}

/// Decode the concatenated metadata records returned by `dev.metadata`.
fn decode_metadata_reply(reply: &[u8]) -> Vec<MetadataContent> {
    use tio::proto::meta;
    let mut decoded = Vec::new();
    let mut remaining = reply;
    while remaining.len() >= 2 {
        let metadata_type = MetadataType::from(remaining[0]);
        let record_len = usize::from(remaining[1]);
        let Some(record) = remaining.get(2..2 + record_len) else {
            break;
        };
        remaining = &remaining[2 + record_len..];
        let content = match metadata_type {
            MetadataType::Device => meta::DeviceMetadata::deserialize(record)
                .ok()
                .map(|(metadata, _, _)| MetadataContent::Device(metadata)),
            MetadataType::Stream => meta::StreamMetadata::deserialize(record)
                .ok()
                .map(|(metadata, _, _)| MetadataContent::Stream(metadata)),
            MetadataType::Segment => meta::SegmentMetadata::deserialize(record)
                .ok()
                .map(|(metadata, _, _)| MetadataContent::Segment(metadata)),
            MetadataType::Column => meta::ColumnMetadata::deserialize(record)
                .ok()
                .map(|(metadata, _, _)| MetadataContent::Column(metadata)),
            _ => None,
        };
        if let Some(content) = content {
            decoded.push(content);
        }
    }
    decoded
}

/// Point-in-time metadata needed to interpret one stream's sample rows.
#[derive(Debug, Clone)]
pub struct StreamMetadataSnapshot {
    pub stream: Arc<StreamMetadata>,
    pub segment: Arc<SegmentMetadata>,
    pub columns: Vec<Arc<ColumnMetadata>>,
}

/// Point-in-time metadata for one device and all advertised streams.
///
/// The parser learns these records incrementally. A snapshot is available only
/// after every advertised stream has a stream, current segment, and columns.
#[derive(Debug, Clone)]
pub struct DeviceMetadataSnapshot {
    pub device: Arc<DeviceMetadata>,
    pub streams: HashMap<u8, StreamMetadataSnapshot>,
}

/// Why an otherwise well-formed packet cannot be applied to the data state.
#[derive(Debug, thiserror::Error)]
pub enum PacketError {
    #[error("packet route {packet_route} cannot be resolved below {root_route}: {source}")]
    Route {
        root_route: DeviceRoute,
        packet_route: DeviceRoute,
        #[source]
        source: RouteError,
    },
    #[error("invalid data for {route} stream {stream_id}: {source}")]
    Stream {
        route: DeviceRoute,
        stream_id: u8,
        #[source]
        source: StreamDataError,
    },
}

/// A stream-data payload contradicts the metadata needed to interpret it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamDataError {
    #[error("stream advertises invalid segment count {count}")]
    InvalidSegmentCount { count: usize },
    #[error("segment {segment_id} is outside the advertised ring of {segment_count}")]
    SegmentOutOfRange {
        segment_id: u8,
        segment_count: usize,
    },
    #[error("received {actual} columns for a schema containing {expected}")]
    ColumnCount { expected: usize, actual: usize },
    #[error("column data occupies {column_bytes} bytes, exceeding sample size {sample_size}")]
    ColumnsExceedSample {
        column_bytes: usize,
        sample_size: usize,
    },
    #[error("stream advertises a zero-byte sample")]
    ZeroSampleSize,
    #[error("payload length {payload_len} is not a multiple of sample size {sample_size}")]
    MisalignedPayload {
        payload_len: usize,
        sample_size: usize,
    },
    #[error("stream-data payload contains no rows")]
    EmptyPayload,
    #[error("{row_count} rows beginning at sample {first_sample_n} exceed the sample counter")]
    SampleNumberOverflow {
        first_sample_n: u32,
        row_count: usize,
    },
    #[error("segment has invalid sampling rate {sampling_rate} and decimation {decimation}")]
    InvalidRate { sampling_rate: u32, decimation: u32 },
}

/// Result of validating stream data against the metadata learned so far.
enum RowState<'a> {
    WaitingForMetadata,
    Validated(ValidatedRows<'a>),
}

/// Metadata and continuity state for one stream on one device.
#[derive(Debug, Clone)]
struct StreamState {
    metadata: Option<Arc<StreamMetadata>>,
    segments: HashMap<u8, Arc<SegmentMetadata>>,
    columns: Vec<Arc<ColumnMetadata>>,

    stream_id: u8,
    current_segment_id: u8,

    // State tracking for boundary detection
    established: bool,
    run: u32,
    last_segment_id: u8,
    last_sample_number: u32,
    last_timestamp: f64,
    last_session_id: u32,
    last_time_ref_session_id: u32,
    effective_rate: f64,
}

impl StreamState {
    fn new(stream_id: u8) -> Self {
        Self {
            metadata: None,
            segments: HashMap::new(),
            columns: Vec::new(),
            stream_id,
            current_segment_id: 0,
            established: false,
            run: 0,
            last_segment_id: 0,
            last_sample_number: 0,
            last_timestamp: 0.0,
            last_session_id: 0,
            last_time_ref_session_id: 0,
            effective_rate: 0.0,
        }
    }

    /// Describe the metadata still needed before this stream can be decoded.
    fn missing_metadata(&self) -> Vec<MetadataRequest> {
        let mut missing = Vec::new();
        match self.metadata.as_ref() {
            Some(metadata) => {
                for index in self.columns.len()..metadata.n_columns {
                    missing.push(MetadataRequest::column(index as u8, self.stream_id))
                }
            }
            None => {
                missing.push(MetadataRequest::stream(self.stream_id));
            }
        }
        if !self.segments.contains_key(&self.current_segment_id) {
            missing.push(MetadataRequest::segment(
                self.current_segment_id,
                self.stream_id,
            ));
        }
        missing
    }

    fn detect_boundary(
        &self,
        first_sample_n: u32,
        first_timestamp: f64,
        device: &DeviceMetadata,
        segment: &Arc<SegmentMetadata>,
        new_rate: f64,
        is_segment_rollover: bool,
    ) -> Option<Boundary> {
        if !self.established {
            return Some(Boundary {
                reason: BoundaryReason::Initial,
            });
        }

        if device.session_id != self.last_session_id {
            return Some(Boundary {
                reason: BoundaryReason::SessionChanged {
                    old: self.last_session_id,
                    new: device.session_id,
                },
            });
        }

        if segment.time_ref_session_id != self.last_time_ref_session_id {
            return Some(Boundary {
                reason: BoundaryReason::TimeRefSessionChanged {
                    old: self.last_time_ref_session_id,
                    new: segment.time_ref_session_id,
                },
            });
        }

        if (new_rate - self.effective_rate).abs() > 1e-9 {
            return Some(Boundary {
                reason: BoundaryReason::RateChanged {
                    old_rate: self.effective_rate,
                    new_rate,
                },
            });
        }

        let half_period = 0.5 / new_rate;
        let time_gap = first_timestamp - self.last_timestamp;

        if time_gap < -half_period {
            return Some(Boundary {
                reason: BoundaryReason::TimeBackward {
                    gap_seconds: -time_gap,
                },
            });
        }

        if segment.segment_id != self.last_segment_id {
            // A benign rollover is time-continuous; a stream restart leaves a
            // forward gap. Backward jumps already returned above.
            let time_continuous = time_gap < half_period;
            return Some(Boundary {
                reason: if is_segment_rollover && time_continuous {
                    BoundaryReason::SegmentRollover {
                        old_id: self.last_segment_id,
                        new_id: segment.segment_id,
                    }
                } else {
                    BoundaryReason::SegmentChanged {
                        old_id: self.last_segment_id,
                        new_id: segment.segment_id,
                    }
                },
            });
        }

        let expected_sample = self.last_sample_number + 1;
        if first_sample_n != expected_sample {
            if time_gap.abs() > half_period {
                return Some(Boundary {
                    reason: BoundaryReason::SamplesLost {
                        expected: expected_sample,
                        received: first_sample_n,
                    },
                });
            }
        } else if time_gap > half_period {
            // Sample numbers are contiguous, so the segment's metadata (e.g. start_time)
            // was corrected in place, and must not pass silently as a continuous batch.
            return Some(Boundary {
                reason: BoundaryReason::TimeForward {
                    gap_seconds: time_gap,
                },
            });
        }

        None
    }

    /// Validate one encoded payload and update continuity state.
    ///
    /// Missing metadata is a normal state; contradictions in complete metadata
    /// or the encoded payload are errors.
    fn validate_rows<'a>(
        &'a mut self,
        data: &'a tio::proto::StreamDataPayload,
        device_metadata: Arc<DeviceMetadata>,
        device_generation: &mut u32,
        global_generation: &mut u32,
    ) -> Result<RowState<'a>, StreamDataError> {
        let Some(stream_metadata) = self.metadata.as_ref().cloned() else {
            return Ok(RowState::WaitingForMetadata);
        };

        // Segment ids index a fixed ring, so a wire value outside it is corrupt.
        let n_segments = u8::try_from(stream_metadata.n_segments).map_err(|_| {
            StreamDataError::InvalidSegmentCount {
                count: stream_metadata.n_segments,
            }
        })?;
        if n_segments == 0 {
            return Err(StreamDataError::InvalidSegmentCount { count: 0 });
        }
        if data.segment_id >= n_segments {
            return Err(StreamDataError::SegmentOutOfRange {
                segment_id: data.segment_id,
                segment_count: stream_metadata.n_segments,
            });
        }
        self.current_segment_id = data.segment_id;

        match self.columns.len().cmp(&stream_metadata.n_columns) {
            Ordering::Less => return Ok(RowState::WaitingForMetadata),
            Ordering::Greater => {
                return Err(StreamDataError::ColumnCount {
                    expected: stream_metadata.n_columns,
                    actual: self.columns.len(),
                });
            }
            Ordering::Equal => {}
        }

        let expected_sample_size: usize = self
            .columns
            .iter()
            .map(|column| column.data_type.size())
            .sum();
        if expected_sample_size > stream_metadata.sample_size {
            return Err(StreamDataError::ColumnsExceedSample {
                column_bytes: expected_sample_size,
                sample_size: stream_metadata.sample_size,
            });
        }
        if stream_metadata.sample_size == 0 {
            return Err(StreamDataError::ZeroSampleSize);
        }
        if !data.data.len().is_multiple_of(stream_metadata.sample_size) {
            return Err(StreamDataError::MisalignedPayload {
                payload_len: data.data.len(),
                sample_size: stream_metadata.sample_size,
            });
        }

        let row_count = data.data.len() / stream_metadata.sample_size;
        if row_count == 0 {
            return Err(StreamDataError::EmptyPayload);
        }
        let sample_span =
            u32::try_from(row_count - 1).map_err(|_| StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first_sample_n,
                row_count,
            })?;
        let last_sample_n = data.first_sample_n.checked_add(sample_span).ok_or(
            StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first_sample_n,
                row_count,
            },
        )?;
        if last_sample_n > MAX_SAMPLE_NUMBER {
            return Err(StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first_sample_n,
                row_count,
            });
        }

        let next_sample = self.last_sample_number + 1;
        let next_segment = self.last_segment_id.wrapping_add(1) % n_segments;

        // A cached entry for a reused segment id can be a stale survivor from a
        // previous trip around the segment ring. If its timestamp for this data
        // is behind where we already are, evict it so we fall back to synthesis
        // below and re-request the real metadata instead of rewinding time.
        if data.segment_id != self.last_segment_id {
            if let Some(seg) = self.segments.get(&data.segment_id) {
                if self.established && seg.decimation != 0 && seg.sampling_rate != 0 {
                    let half_period =
                        0.5 * f64::from(seg.decimation) / f64::from(seg.sampling_rate);
                    if seg.time_at(data.first_sample_n) < self.last_timestamp - half_period {
                        self.segments.remove(&data.segment_id);
                    }
                }
            }
        }

        let (segment, is_segment_rollover) = match (
            self.segments.get(&data.segment_id).cloned(),
            self.segments.get(&self.last_segment_id).cloned(),
        ) {
            (Some(seg), _) => {
                let is_segment_rollover =
                    data.segment_id == next_segment && data.first_sample_n == 0;
                (seg, is_segment_rollover)
            }
            // Synthesize from the previous segment only for a forced rollover;
            // otherwise wait for `missing_metadata` to request the real segment.
            (None, Some(prev)) => {
                // Segments roll on whole seconds of the undecimated clock, on
                // which output sample n sits at sample n * decimation.
                let seconds_at = |sample: u64| {
                    sample * u64::from(prev.decimation) / u64::from(prev.sampling_rate)
                };
                let next_sample = u64::from(next_sample);
                let forced_rollover = self.established
                    && data.first_sample_n == 0
                    && prev.sampling_rate != 0
                    && prev.decimation != 0
                    && seconds_at(next_sample) > seconds_at(next_sample - 1)
                    && data.segment_id == next_segment;
                if !forced_rollover {
                    return Ok(RowState::WaitingForMetadata);
                }
                let mut new_seg = (*prev).clone();
                new_seg.segment_id = data.segment_id;
                new_seg.start_time += seconds_at(next_sample) as u32;
                (Arc::new(new_seg), true)
            }
            (None, None) => return Ok(RowState::WaitingForMetadata),
        };

        if segment.decimation == 0 || segment.sampling_rate == 0 {
            return Err(StreamDataError::InvalidRate {
                sampling_rate: segment.sampling_rate,
                decimation: segment.decimation,
            });
        }
        let new_rate = segment.sampling_rate as f64 / segment.decimation as f64;

        let period = 1.0 / new_rate;
        let first_timestamp =
            f64::from(segment.start_time) + period * f64::from(data.first_sample_n);

        let boundary = self.detect_boundary(
            data.first_sample_n,
            first_timestamp,
            &device_metadata,
            &segment,
            new_rate,
            is_segment_rollover,
        );

        if let Some(boundary) = boundary.as_ref().filter(|b| !b.is_continuous()) {
            self.run += 1;
            // Streams start up independently, so their first data must not bump
            // the generations their peers share.
            if !boundary.is_initial() {
                *device_generation += 1;
                *global_generation += 1;
            }
        }

        self.last_sample_number = last_sample_n;
        self.last_timestamp = segment.time_at(last_sample_n + 1);
        self.last_session_id = device_metadata.session_id;
        self.last_time_ref_session_id = segment.time_ref_session_id;
        self.last_segment_id = segment.segment_id;
        self.effective_rate = new_rate;
        self.established = true;

        let sample_size = stream_metadata.sample_size;
        Ok(RowState::Validated(ValidatedRows {
            boundary,
            generations: Generations {
                stream: self.run,
                device: *device_generation,
                global: *global_generation,
            },
            segment,
            stream: stream_metadata,
            device: device_metadata,
            first_sample_n: data.first_sample_n,
            last_sample_n,
            row_count,
            sample_size,
            encoded: &data.data,
            columns: &self.columns,
        }))
    }

    fn invalidate_metadata(&mut self) {
        self.metadata = None;
        self.segments.clear();
        self.columns.clear();
    }

    fn metadata_snapshot(&self) -> Option<StreamMetadataSnapshot> {
        if !self.missing_metadata().is_empty() {
            return None;
        }
        Some(StreamMetadataSnapshot {
            stream: self.metadata.as_ref()?.clone(),
            segment: self.segments.get(&self.current_segment_id)?.clone(),
            columns: self.columns.clone(),
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataSource {
    /// Metadata returned because this parser requested it.
    Reply,
    /// An unsolicited update whose segment becomes the stream's current one.
    Update,
}

/// Metadata and stream state for one absolute device route.
#[derive(Clone)]
struct DeviceState {
    metadata: Option<Arc<DeviceMetadata>>,
    streams: HashMap<u8, StreamState>,
    ignore_session: bool,
    metadata_rpcs_in_flight: usize,
    generation: u32,
}

impl DeviceState {
    fn new(ignore_session: bool) -> Self {
        Self {
            metadata: None,
            streams: HashMap::new(),
            ignore_session,
            metadata_rpcs_in_flight: 0,
            generation: 0,
        }
    }

    fn stream_mut(&mut self, stream_id: u8) -> &mut StreamState {
        self.streams
            .entry(stream_id)
            .or_insert_with(|| StreamState::new(stream_id))
    }

    fn forget_all_metadata(&mut self) {
        self.metadata = None;
        self.streams.clear();
        self.metadata_rpcs_in_flight = 0;
    }

    fn accepts_stream(&mut self, stream_id: u8) -> bool {
        if self
            .metadata
            .as_ref()
            .is_some_and(|device| usize::from(stream_id) > device.n_streams)
        {
            // An impossible stream id means our device description is stale or
            // corrupt. Bootstrap the route again instead of retaining it.
            self.forget_all_metadata();
            false
        } else {
            true
        }
    }

    /// Merge one metadata record into this route's decoding state.
    fn apply_metadata(&mut self, metadata: &MetadataContent, source: MetadataSource) {
        match metadata {
            MetadataContent::Device(incoming) => {
                if let Some(current) = &self.metadata {
                    if current.serial_number != incoming.serial_number {
                        self.streams.clear();
                    } else if (current.session_id != incoming.session_id)
                        || (current.firmware_hash != incoming.firmware_hash)
                    {
                        for stream in self.streams.values_mut() {
                            stream.invalidate_metadata();
                        }
                    }
                }
                self.metadata = Some(Arc::new(incoming.clone()));
            }
            MetadataContent::Stream(incoming) => {
                if !self.accepts_stream(incoming.stream_id) {
                    return;
                }
                let stream = self.stream_mut(incoming.stream_id);
                if let Some(current) = &stream.metadata {
                    if current.as_ref() != incoming {
                        // This should never happen: stream metadata is constant.
                        self.forget_all_metadata();
                    }
                } else {
                    stream.metadata = Some(Arc::new(incoming.clone()));
                }
            }
            MetadataContent::Segment(incoming) => {
                if !self.accepts_stream(incoming.stream_id) {
                    return;
                }
                let stream = self.stream_mut(incoming.stream_id);
                stream
                    .segments
                    .insert(incoming.segment_id, Arc::new(incoming.clone()));
                if source == MetadataSource::Update {
                    stream.current_segment_id = incoming.segment_id;
                }
            }
            MetadataContent::Column(incoming) => {
                if !self.accepts_stream(incoming.stream_id) {
                    return;
                }
                let stream = self.stream_mut(incoming.stream_id);
                if incoming.index < stream.columns.len() {
                    if stream.columns[incoming.index].as_ref() != incoming {
                        // This should never happen: columns are constant.
                        self.forget_all_metadata();
                    }
                } else if incoming.index == stream.columns.len() {
                    stream.columns.push(Arc::new(incoming.clone()));
                }
            }
            _ => {}
        }
    }

    /// Apply packets that update decoding state but do not contain sample rows.
    fn apply_control_payload(&mut self, payload: &tio::proto::Payload) {
        match payload {
            tio::proto::Payload::RpcReply(reply) if reply.id == META_RPC_ID => {
                self.metadata_rpcs_in_flight = self.metadata_rpcs_in_flight.saturating_sub(1);
                for metadata in decode_metadata_reply(&reply.reply) {
                    self.apply_metadata(&metadata, MetadataSource::Reply);
                }
            }
            tio::proto::Payload::Metadata(update) => {
                self.apply_metadata(&update.content, MetadataSource::Update)
            }
            tio::proto::Payload::Heartbeat(tio::proto::HeartbeatPayload::Session(session_id)) => {
                if let Some(device) = &self.metadata {
                    if device.session_id != *session_id && !self.ignore_session {
                        for stream in self.streams.values_mut() {
                            stream.invalidate_metadata();
                        }
                        self.metadata = None;
                        self.metadata_rpcs_in_flight = 0;
                    }
                }
            }
            tio::proto::Payload::RpcError(error) if error.id == META_RPC_ID => {
                self.metadata_rpcs_in_flight = self.metadata_rpcs_in_flight.saturating_sub(1);
            }
            _ => {}
        }
    }

    /// Validate one stream-data packet using metadata for this route.
    fn validate_stream_data<'a>(
        &'a mut self,
        data: &'a tio::proto::StreamDataPayload,
        global_generation: &mut u32,
    ) -> Result<RowState<'a>, StreamDataError> {
        let Some(device_metadata) = self.metadata.as_ref().cloned() else {
            return Ok(RowState::WaitingForMetadata);
        };
        if !self.accepts_stream(data.stream_id) {
            return Ok(RowState::WaitingForMetadata);
        }

        self.streams
            .entry(data.stream_id)
            .or_insert_with(|| StreamState::new(data.stream_id))
            .validate_rows(
                data,
                device_metadata,
                &mut self.generation,
                global_generation,
            )
    }

    fn take_metadata_requests(&mut self) -> Vec<tio::Packet> {
        if self.metadata_rpcs_in_flight != 0 {
            return Vec::new();
        }
        let requests = self.metadata_request_packets();
        self.metadata_rpcs_in_flight = requests.len();
        requests
    }

    /// Build requests for every piece of metadata this route still lacks.
    fn metadata_request_packets(&self) -> Vec<tio::Packet> {
        let mut missing = Vec::new();
        match self.metadata.as_ref() {
            Some(device) => {
                for stream_id in 1..=device.n_streams as u8 {
                    if let Some(stream) = self.streams.get(&stream_id) {
                        missing.extend(stream.missing_metadata());
                    } else {
                        missing.push(MetadataRequest::stream(stream_id));
                    }
                }
            }
            None => missing.push(MetadataRequest::device()),
        }
        build_metadata_request_packets(&missing)
    }

    fn metadata_snapshot(&self) -> Option<DeviceMetadataSnapshot> {
        if !self.metadata_request_packets().is_empty() {
            return None;
        }
        let device = self.metadata.as_ref()?.clone();
        let mut streams = HashMap::with_capacity(device.n_streams);
        for stream_id in 1..=device.n_streams as u8 {
            streams.insert(
                stream_id,
                self.streams.get(&stream_id)?.metadata_snapshot()?,
            );
        }
        Some(DeviceMetadataSnapshot { device, streams })
    }
}

/// Result of applying one packet to the ordered metadata and continuity state.
pub(super) enum PacketEvent<'a> {
    Applied,
    WaitingForMetadata,
    Reset,
    Rows {
        route: DeviceRoute,
        stream_id: u8,
        rows: ValidatedRows<'a>,
    },
}

/// Route-aware state machine shared by incremental and seekable packet readers.
#[derive(Clone)]
pub(super) struct ParseState {
    root_route: DeviceRoute,
    ignore_session: bool,
    devices: HashMap<DeviceRoute, DeviceState>,
    global_generation: u32,
}

impl ParseState {
    pub(super) fn new(root_route: DeviceRoute, ignore_session: bool) -> Self {
        Self {
            root_route,
            ignore_session,
            devices: HashMap::new(),
            global_generation: 0,
        }
    }

    fn device_mut(&mut self, route: DeviceRoute) -> &mut DeviceState {
        let ignore_session = self.ignore_session;
        self.devices
            .entry(route)
            .or_insert_with(|| DeviceState::new(ignore_session))
    }

    pub(super) fn apply_packet<'a>(
        &'a mut self,
        packet: &'a tio::Packet,
    ) -> Result<PacketEvent<'a>, PacketError> {
        if let proto::Payload::ProxyStatus(status) = &packet.payload {
            if matches!(status.0, proto::ProxyStatus::SensorDisconnected) {
                self.reset();
                return Ok(PacketEvent::Reset);
            }
            return Ok(PacketEvent::Applied);
        }

        let route = self
            .root_route
            .absolute_route(&packet.routing)
            .map_err(|source| PacketError::Route {
                root_route: self.root_route,
                packet_route: packet.routing,
                source,
            })?;

        match &packet.payload {
            proto::Payload::StreamData(data) => {
                let ignore_session = self.ignore_session;
                let device = self
                    .devices
                    .entry(route)
                    .or_insert_with(|| DeviceState::new(ignore_session));
                let state = device
                    .validate_stream_data(data, &mut self.global_generation)
                    .map_err(|source| PacketError::Stream {
                        route,
                        stream_id: data.stream_id,
                        source,
                    })?;
                Ok(match state {
                    RowState::WaitingForMetadata => PacketEvent::WaitingForMetadata,
                    RowState::Validated(rows) => PacketEvent::Rows {
                        route,
                        stream_id: data.stream_id,
                        rows,
                    },
                })
            }
            payload => {
                self.device_mut(route).apply_control_payload(payload);
                Ok(PacketEvent::Applied)
            }
        }
    }

    pub(super) fn reset(&mut self) {
        self.devices.clear();
    }

    pub(super) fn take_requests(&mut self) -> Vec<tio::Packet> {
        let routes: Vec<_> = self.devices.keys().copied().collect();
        let mut out = Vec::new();
        for route in routes {
            out.extend(self.take_requests_for(route));
        }
        out
    }

    pub(super) fn take_requests_for(&mut self, route: DeviceRoute) -> Vec<tio::Packet> {
        let Ok(relative) = self.root_route.relative_route(&route) else {
            return Vec::new();
        };
        self.device_mut(route)
            .take_metadata_requests()
            .into_iter()
            .map(|mut request| {
                request.routing = relative;
                request
            })
            .collect()
    }

    pub(super) fn metadata(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        self.devices.get(&route)?.metadata_snapshot()
    }

    pub(super) fn routes(&self) -> Vec<DeviceRoute> {
        self.devices.keys().copied().collect()
    }
}

/// Stream bytes validated against the metadata and continuity state in effect
/// at their position in the packet sequence.
pub(super) struct ValidatedRows<'a> {
    pub(super) boundary: Option<Boundary>,
    pub(super) generations: Generations,
    pub(super) segment: Arc<SegmentMetadata>,
    pub(super) stream: Arc<StreamMetadata>,
    pub(super) device: Arc<DeviceMetadata>,
    pub(super) first_sample_n: u32,
    pub(super) last_sample_n: u32,
    pub(super) row_count: usize,
    pub(super) sample_size: usize,
    pub(super) encoded: &'a [u8],
    pub(super) columns: &'a [Arc<ColumnMetadata>],
}

/// One column of a decoded batch: where its bytes start within a sample, the
/// buffer its values land in, and its metadata.
pub(super) struct DecodableColumn<'a> {
    pub(super) offset: usize,
    pub(super) buffer_type: proto::BufferType,
    pub(super) metadata: &'a Arc<ColumnMetadata>,
}

impl ValidatedRows<'_> {
    /// The columns a batch can hold, in schema order. Columns whose wire type
    /// this build cannot decode are absent, so every batch stays rectangular.
    pub(super) fn decodable_columns(&self) -> impl Iterator<Item = DecodableColumn<'_>> {
        self.columns
            .iter()
            .scan(0usize, |offset, metadata| {
                let at = *offset;
                *offset += metadata.data_type.size();
                Some((at, metadata))
            })
            .filter_map(|(offset, metadata)| {
                Some(DecodableColumn {
                    offset,
                    buffer_type: metadata.data_type.decoded_buffer_type()?,
                    metadata,
                })
            })
    }
}

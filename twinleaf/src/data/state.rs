//! Ordered metadata, schema, and continuity state for routed TIO packets.
//!
//! This module validates stream payloads and describes their rows without
//! materializing sample values. Packet consumers decide whether to decode the
//! validated bytes immediately or retain only their metadata and boundaries.

use super::metadata::{
    decoded_buffer_type, DeviceMetadataSnapshot, MetadataQuery, StreamMetadataSnapshot,
};
use super::sample::{
    sample_time, BatchContext, BoundaryReason, ColumnData, Generations, RowSource,
    SampleBatchBuilder, StreamKey,
};
use super::{BufferType, ColumnRecord, DeviceRecord, MetadataType};
use super::{SegmentRecord, StreamRecord};
use crate::tio;
use proto::route::RouteError;
use proto::DeviceRoute;
use std::cmp::Ordering;
use std::collections::HashMap;
use tio::proto;
use tio::proto::MAX_SAMPLE_NUMBER;
use twinleaf_proto::data as wire;
use twinleaf_proto::heartbeat::Heartbeat;
use twinleaf_proto::{SampleNumber, SegmentId, SessionId, StreamId};

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
        stream_id: StreamId,
        #[source]
        source: StreamDataError,
    },
}

/// A stream-data payload contradicts the metadata needed to interpret it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamDataError {
    #[error("stream advertises a zero-length segment ring")]
    ZeroSegmentCount,
    #[error("segment {segment_id} is outside the advertised ring of {segment_count}")]
    SegmentOutOfRange {
        segment_id: SegmentId,
        segment_count: u8,
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
        first_sample_n: SampleNumber,
        row_count: usize,
    },
    #[error("segment has invalid sampling rate {sampling_rate} and decimation {decimation}")]
    InvalidRate { sampling_rate: u32, decimation: u32 },
}

/// Result of validating stream data against the metadata learned so far.
enum RowState<'a> {
    WaitingForMetadata,
    Validated(ScannedRows<'a>),
}

/// Metadata and continuity state for one stream on one device.
#[derive(Debug, Clone)]
struct StreamState {
    metadata: Option<StreamRecord>,
    segments: Vec<Option<SegmentRecord>>,
    columns: Vec<ColumnRecord>,

    stream_id: StreamId,
    current_segment_id: SegmentId,

    // Accepted sample history survives metadata invalidation so the next
    // batch can report SessionChanged.
    established: bool,
    run: u32,
    last_segment_id: SegmentId,
    last_sample_number: SampleNumber,
    last_timestamp: f64,
    last_session_id: SessionId,
    last_time_ref_session_id: SessionId,
    effective_rate: f64,
}

impl StreamState {
    fn new(stream_id: StreamId) -> Self {
        Self {
            metadata: None,
            segments: Vec::new(),
            columns: Vec::new(),
            stream_id,
            current_segment_id: SegmentId::new(0),
            established: false,
            run: 0,
            last_segment_id: SegmentId::new(0),
            last_sample_number: SampleNumber::new(0),
            last_timestamp: 0.0,
            last_session_id: SessionId::new(0),
            last_time_ref_session_id: SessionId::new(0),
            effective_rate: 0.0,
        }
    }

    fn segment(&self, segment_id: SegmentId) -> Option<&SegmentRecord> {
        self.segments.get(usize::from(segment_id.value()))?.as_ref()
    }

    fn set_segment(&mut self, segment_id: SegmentId, segment: SegmentRecord) {
        let index = usize::from(segment_id.value());
        if self.segments.len() <= index {
            self.segments.resize(index + 1, None);
        }
        self.segments[index] = Some(segment);
    }

    /// Describe the metadata still needed before this stream can be decoded.
    fn missing_metadata(&self) -> Vec<wire::MetadataSelector> {
        let mut missing = Vec::new();
        match self.metadata.as_ref() {
            Some(metadata) => {
                for index in self.columns.len()..usize::from(metadata.get().n_columns) {
                    missing.push(wire::MetadataSelector::column(
                        self.stream_id.value(),
                        index as u8,
                    ))
                }
            }
            None => {
                missing.push(wire::MetadataSelector::stream(self.stream_id.value()));
            }
        }
        if self.segment(self.current_segment_id).is_none() {
            missing.push(wire::MetadataSelector::segment(
                self.stream_id.value(),
                self.current_segment_id.value(),
            ));
        }
        missing
    }

    fn detect_boundary(
        &self,
        first_sample_n: SampleNumber,
        first_timestamp: f64,
        device: wire::Device<'_>,
        segment: wire::Segment<'_>,
        new_rate: f64,
        is_segment_rollover: bool,
    ) -> Option<BoundaryReason> {
        if !self.established {
            return Some(BoundaryReason::Initial);
        }

        if device.session != self.last_session_id {
            return Some(BoundaryReason::SessionChanged {
                old: self.last_session_id,
                new: device.session,
            });
        }

        if segment.timeref_session != self.last_time_ref_session_id {
            return Some(BoundaryReason::TimeRefSessionChanged {
                old: self.last_time_ref_session_id,
                new: segment.timeref_session,
            });
        }

        if (new_rate - self.effective_rate).abs() > 1e-9 {
            return Some(BoundaryReason::RateChanged {
                old_rate: self.effective_rate,
                new_rate,
            });
        }

        let half_period = 0.5 / new_rate;
        let time_gap = first_timestamp - self.last_timestamp;

        if time_gap < -half_period {
            return Some(BoundaryReason::TimeBackward {
                gap_seconds: -time_gap,
            });
        }

        if segment.segment_id != self.last_segment_id {
            // A benign rollover is time-continuous; a stream restart leaves a
            // forward gap. Backward jumps already returned above.
            let time_continuous = time_gap < half_period;
            return Some(if is_segment_rollover && time_continuous {
                BoundaryReason::SegmentRollover {
                    old_id: self.last_segment_id,
                    new_id: segment.segment_id,
                }
            } else {
                BoundaryReason::SegmentChanged {
                    old_id: self.last_segment_id,
                    new_id: segment.segment_id,
                }
            });
        }

        let expected_sample = SampleNumber::new(self.last_sample_number.value() + 1);
        if first_sample_n != expected_sample {
            if time_gap.abs() > half_period {
                return Some(BoundaryReason::SamplesLost {
                    expected: expected_sample,
                    received: first_sample_n,
                });
            }
        } else if time_gap > half_period {
            // Sample numbers are contiguous, so the segment's metadata (e.g. start_time)
            // was corrected in place, and must not pass silently as a continuous batch.
            return Some(BoundaryReason::TimeForward {
                gap_seconds: time_gap,
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
        data: wire::Samples<'a>,
        route: DeviceRoute,
        device_metadata: DeviceRecord,
        device_generation: &mut u32,
        global_generation: &mut u32,
        metadata_revision: &mut u32,
    ) -> Result<RowState<'a>, StreamDataError> {
        let Some(stream_metadata) = self.metadata.clone() else {
            return Ok(RowState::WaitingForMetadata);
        };
        let stream = stream_metadata.get();

        // Segment ids index a fixed ring, so a wire value outside it is corrupt.
        if stream.n_segments == 0 {
            return Err(StreamDataError::ZeroSegmentCount);
        }
        if data.segment_id.value() >= stream.n_segments {
            return Err(StreamDataError::SegmentOutOfRange {
                segment_id: data.segment_id,
                segment_count: stream.n_segments,
            });
        }
        if self.current_segment_id != data.segment_id {
            self.current_segment_id = data.segment_id;
            *metadata_revision = metadata_revision.wrapping_add(1);
        }

        let n_columns = usize::from(stream.n_columns);
        match self.columns.len().cmp(&n_columns) {
            Ordering::Less => return Ok(RowState::WaitingForMetadata),
            Ordering::Greater => {
                return Err(StreamDataError::ColumnCount {
                    expected: n_columns,
                    actual: self.columns.len(),
                });
            }
            Ordering::Equal => {}
        }

        let sample_size = usize::from(stream.sample_size);
        let expected_sample_size: usize = self
            .columns
            .iter()
            .map(|column| column.get().data_type.size())
            .sum();
        if expected_sample_size > sample_size {
            return Err(StreamDataError::ColumnsExceedSample {
                column_bytes: expected_sample_size,
                sample_size,
            });
        }
        if sample_size == 0 {
            return Err(StreamDataError::ZeroSampleSize);
        }
        if !data.data.len().is_multiple_of(sample_size) {
            return Err(StreamDataError::MisalignedPayload {
                payload_len: data.data.len(),
                sample_size,
            });
        }

        let row_count = data.data.len() / sample_size;
        if row_count == 0 {
            return Err(StreamDataError::EmptyPayload);
        }
        let sample_span =
            u32::try_from(row_count - 1).map_err(|_| StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first,
                row_count,
            })?;
        let last_sample_n = SampleNumber::new(data.first.value().checked_add(sample_span).ok_or(
            StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first,
                row_count,
            },
        )?);
        if last_sample_n.value() > MAX_SAMPLE_NUMBER {
            return Err(StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first,
                row_count,
            });
        }

        let next_sample = self.last_sample_number.value() + 1;
        let next_segment =
            SegmentId::new(self.last_segment_id.value().wrapping_add(1) % stream.n_segments);

        // Infer stale ring entries from backward timestamps within the same
        // device and clock sessions. Comparing across sessions would repeatedly
        // evict valid metadata after a reboot.
        if data.segment_id != self.last_segment_id
            && self.established
            && device_metadata.get().session == self.last_session_id
        {
            let stale = self.segment(data.segment_id).is_some_and(|record| {
                let seg = record.get();
                seg.timeref_session == self.last_time_ref_session_id
                    && seg.decimation != 0
                    && seg.sampling_rate != 0
                    && {
                        let half_period =
                            0.5 * f64::from(seg.decimation) / f64::from(seg.sampling_rate);
                        sample_time(seg, data.first.value()) < self.last_timestamp - half_period
                    }
            });
            if stale {
                self.segments[usize::from(data.segment_id.value())] = None;
            }
        }

        let (segment_record, is_segment_rollover) = match (
            self.segment(data.segment_id).cloned(),
            self.segment(self.last_segment_id).cloned(),
        ) {
            (Some(seg), _) => {
                let is_segment_rollover =
                    data.segment_id == next_segment && data.first.value() == 0;
                (seg, is_segment_rollover)
            }
            // Infer rollover metadata assuming unchanged settings; configuration
            // changes can also match this pattern. The batch uses the inferred
            // descriptor while discovery requests the real one.
            (None, Some(previous)) => {
                let mut rolled = previous.get();
                // Segments roll on whole seconds of the undecimated clock, on
                // which output sample n sits at sample n * decimation.
                let seconds_at = |sample: u64| {
                    sample * u64::from(rolled.decimation) / u64::from(rolled.sampling_rate)
                };
                let next_sample = u64::from(next_sample);
                let forced_rollover = self.established
                    && data.first.value() == 0
                    && rolled.sampling_rate != 0
                    && rolled.decimation != 0
                    && seconds_at(next_sample) > seconds_at(next_sample - 1)
                    && data.segment_id == next_segment;
                if !forced_rollover {
                    return Ok(RowState::WaitingForMetadata);
                }
                rolled.start_time += seconds_at(next_sample) as u32;
                rolled.segment_id = data.segment_id;
                let rolled = SegmentRecord::encode(rolled)
                    .expect("a re-timed copy of a retained record is the same length");
                (rolled, true)
            }
            (None, None) => return Ok(RowState::WaitingForMetadata),
        };
        let segment = segment_record.get();

        if segment.decimation == 0 || segment.sampling_rate == 0 {
            return Err(StreamDataError::InvalidRate {
                sampling_rate: segment.sampling_rate,
                decimation: segment.decimation,
            });
        }
        let new_rate = segment.sampling_rate as f64 / segment.decimation as f64;

        let boundary = self.detect_boundary(
            data.first,
            sample_time(segment, data.first.value()),
            device_metadata.get(),
            segment,
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
        self.last_timestamp = sample_time(segment, last_sample_n.value() + 1);
        self.last_session_id = device_metadata.get().session;
        self.last_time_ref_session_id = segment.timeref_session;
        self.last_segment_id = segment.segment_id;
        self.effective_rate = new_rate;
        self.established = true;

        let key = StreamKey::new(route, stream.stream_id);
        Ok(RowState::Validated(ScannedRows {
            key,
            boundary,
            generations: Generations {
                stream: self.run,
                device: *device_generation,
                global: *global_generation,
            },
            segment: segment_record,
            stream: stream_metadata,
            device: device_metadata,
            first_sample_n: data.first,
            last_sample_n,
            row_count,
            sample_size,
            encoded: data.data,
            columns: &self.columns,
        }))
    }

    fn invalidate_metadata(&mut self) {
        self.metadata = None;
        self.segments.clear();
        self.columns.clear();
    }

    /// Forget metadata and end the current run, keeping the run counter so the
    /// rebuilt stream cannot restamp a run number it has already used.
    fn reset(&mut self) {
        self.invalidate_metadata();
        self.established = false;
    }

    fn metadata_snapshot(
        &self,
        key: StreamKey,
        device: DeviceRecord,
    ) -> Option<StreamMetadataSnapshot> {
        if !self.missing_metadata().is_empty() {
            return None;
        }
        Some(StreamMetadataSnapshot::new(
            key,
            device,
            self.metadata.as_ref()?.clone(),
            self.segment(self.current_segment_id)?.clone(),
            self.columns.clone(),
        ))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataSource {
    /// A reply may describe an explicitly requested older ring segment.
    Reply,
    /// Broadcasts describe the current output segment.
    Broadcast,
}

/// Metadata and stream state for one absolute device route.
#[derive(Clone)]
struct DeviceState {
    metadata: Option<DeviceRecord>,
    streams: Vec<Option<StreamState>>,
    ignore_session: bool,
    query_in_flight: bool,
    metadata_generation: u32,
    /// Bumps whenever an applied record may change what a complete snapshot of
    /// this route would contain.
    metadata_revision: u32,
    generation: u32,
}

impl DeviceState {
    fn new(ignore_session: bool) -> Self {
        Self {
            metadata: None,
            streams: Vec::new(),
            ignore_session,
            query_in_flight: false,
            metadata_generation: 0,
            metadata_revision: 0,
            generation: 0,
        }
    }

    fn revise_metadata(&mut self) {
        self.metadata_revision = self.metadata_revision.wrapping_add(1);
    }

    fn stream_mut(&mut self, stream_id: StreamId) -> &mut StreamState {
        let index = usize::from(stream_id.value());
        if self.streams.len() <= index {
            self.streams.resize_with(index + 1, || None);
        }
        self.streams[index].get_or_insert_with(|| StreamState::new(stream_id))
    }

    /// Bootstrap this route again after metadata contradicted the state used
    /// to decode earlier rows. Preserve each stream's run counter so rebuilt
    /// metadata cannot reuse an existing run identity.
    fn forget_all_metadata(&mut self, global_generation: &mut u32) {
        self.reset();
        *global_generation += 1;
    }

    /// As [`StreamState::reset`] across every stream, opening a new device
    /// generation for the break they all share.
    fn reset(&mut self) {
        self.metadata = None;
        self.rediscover_metadata();
        self.generation += 1;
        for stream in self.streams.iter_mut().flatten() {
            stream.reset();
        }
    }

    /// Open a new metadata generation: no query minted before this point can
    /// still apply, and discovery is armed again.
    fn rediscover_metadata(&mut self) {
        self.metadata_generation = self.metadata_generation.wrapping_add(1);
        self.query_in_flight = false;
    }

    fn accepts_stream(&mut self, stream_id: StreamId, global_generation: &mut u32) -> bool {
        if self
            .metadata
            .as_ref()
            .is_some_and(|device| stream_id.value() > device.get().n_streams)
        {
            // An impossible stream id means our device description is stale or
            // corrupt. Bootstrap the route again instead of retaining it.
            self.forget_all_metadata(global_generation);
            false
        } else {
            true
        }
    }

    /// Merge one metadata record into this route's decoding state. A record
    /// that does not parse as the type it claims is ignored.
    fn apply_metadata(
        &mut self,
        kind: MetadataType,
        record: &[u8],
        source: MetadataSource,
        global_generation: &mut u32,
    ) {
        match kind {
            MetadataType::Device => {
                let Some(incoming) = DeviceRecord::new(record) else {
                    return;
                };
                if self.metadata.as_ref() != Some(&incoming) {
                    self.revise_metadata();
                }
                if let Some(previous) = self.metadata.take() {
                    let (old, new) = (previous.get(), incoming.get());
                    if old.serial != new.serial {
                        self.forget_all_metadata(global_generation);
                    } else if old.session != new.session {
                        // Keeping continuity state lets the first rows under the
                        // new metadata report a specific SessionChanged boundary.
                        for stream in self.streams.iter_mut().flatten() {
                            stream.invalidate_metadata();
                        }
                        self.rediscover_metadata();
                    } else if old.firmware != new.firmware {
                        // A firmware change can alter a schema without changing
                        // the session or timeline, so it must explicitly end all
                        // current runs rather than relying on boundary detection.
                        self.forget_all_metadata(global_generation);
                    }
                }
                self.metadata = Some(incoming);
            }
            MetadataType::Stream => {
                let Some(incoming) = StreamRecord::new(record) else {
                    return;
                };
                let stream_id = incoming.get().stream_id;
                if !self.accepts_stream(stream_id, global_generation) {
                    return;
                }
                let stream = self.stream_mut(stream_id);
                let (contradicted, learned) = match &stream.metadata {
                    Some(current) => (*current != incoming, false),
                    None => {
                        stream.metadata = Some(incoming);
                        (false, true)
                    }
                };
                if contradicted {
                    // This should never happen: stream metadata is constant.
                    self.forget_all_metadata(global_generation);
                } else if learned {
                    self.revise_metadata();
                }
            }
            MetadataType::Segment => {
                let Some(incoming) = SegmentRecord::new(record) else {
                    return;
                };
                let segment = incoming.get();
                let (stream_id, segment_id) = (segment.stream_id, segment.segment_id);
                if !self.accepts_stream(stream_id, global_generation) {
                    return;
                }
                let stream = self.stream_mut(stream_id);
                let learned = stream.segment(segment_id) != Some(&incoming);
                stream.set_segment(segment_id, incoming);
                let switched =
                    source == MetadataSource::Broadcast && stream.current_segment_id != segment_id;
                if switched {
                    stream.current_segment_id = segment_id;
                }
                if learned || switched {
                    self.revise_metadata();
                }
            }
            MetadataType::Column => {
                let Some(incoming) = ColumnRecord::new(record) else {
                    return;
                };
                let column = incoming.get();
                let (stream_id, index) = (column.stream_id, column.index.index());
                if !self.accepts_stream(stream_id, global_generation) {
                    return;
                }
                let stream = self.stream_mut(stream_id);
                let (contradicted, learned) = match stream.columns.get(index) {
                    Some(current) => (*current != incoming, false),
                    None => {
                        let appended = index == stream.columns.len();
                        if appended {
                            stream.columns.push(incoming);
                        }
                        (false, appended)
                    }
                };
                if contradicted {
                    // This should never happen: columns are constant.
                    self.forget_all_metadata(global_generation);
                } else if learned {
                    self.revise_metadata();
                }
            }
            MetadataType::Unknown(_) => {}
        }
    }

    /// Apply packets that update decoding state but do not contain sample rows.
    fn apply_control_packet(&mut self, packet: &tio::Packet, global_generation: &mut u32) {
        match packet.payload() {
            proto::Payload::Metadata(..) => {
                let (kind, _, record) = wire::split_metadata(packet.payload_bytes())
                    .expect("a metadata payload was framed when the packet was validated");
                self.apply_metadata(
                    kind.into(),
                    record,
                    MetadataSource::Broadcast,
                    global_generation,
                );
            }
            proto::Payload::Heartbeat(Heartbeat::Session(session)) => {
                if let Some(device) = &self.metadata {
                    if device.get().session != session && !self.ignore_session {
                        for stream in self.streams.iter_mut().flatten() {
                            stream.invalidate_metadata();
                        }
                        self.metadata = None;
                        self.rediscover_metadata();
                    }
                }
            }
            _ => {}
        }
    }

    /// Validate one stream-data packet using metadata for this route.
    fn validate_stream_data<'a>(
        &'a mut self,
        data: wire::Samples<'a>,
        route: DeviceRoute,
        global_generation: &mut u32,
    ) -> Result<RowState<'a>, StreamDataError> {
        let Some(device_metadata) = self.metadata.clone() else {
            return Ok(RowState::WaitingForMetadata);
        };
        if !self.accepts_stream(data.stream_id, global_generation) {
            return Ok(RowState::WaitingForMetadata);
        }

        let index = usize::from(data.stream_id.value());
        if self.streams.len() <= index {
            self.streams.resize_with(index + 1, || None);
        }
        self.streams[index]
            .get_or_insert_with(|| StreamState::new(data.stream_id))
            .validate_rows(
                data,
                route,
                device_metadata,
                &mut self.generation,
                global_generation,
                &mut self.metadata_revision,
            )
    }

    /// One query for as much of the missing metadata as a request may carry,
    /// while none is outstanding for this route.
    fn take_metadata_query(&mut self, route: DeviceRoute) -> Option<MetadataQuery> {
        if self.query_in_flight {
            return None;
        }
        let missing = self.missing_metadata();
        let selectors = match missing.as_slice() {
            [] => return None,
            [only] if only.mtype == MetadataType::Device => Vec::new(),
            missing => missing[..missing.len().min(wire::MAX_METADATA_SELECTORS)].to_vec(),
        };
        self.query_in_flight = true;
        Some(MetadataQuery {
            route,
            selectors,
            generation: self.metadata_generation,
        })
    }

    /// False if `query` names a metadata generation this route has already
    /// left, including one it left by completing that same query.
    fn complete_metadata_query(&mut self, query: &MetadataQuery) -> bool {
        if query.generation != self.metadata_generation {
            return false;
        }
        self.rediscover_metadata();
        true
    }

    fn apply_metadata_reply(
        &mut self,
        query: MetadataQuery,
        reply: &[u8],
        global_generation: &mut u32,
    ) {
        if !self.complete_metadata_query(&query) {
            return;
        }
        match wire::MetadataReply::parse(reply) {
            Some(records) => {
                for (kind, record) in records {
                    self.apply_metadata(kind, record, MetadataSource::Reply, global_generation);
                }
            }
            None => log::warn!("dropping a dev.metadata reply with broken framing"),
        }
    }

    fn fail_metadata_query(&mut self, query: MetadataQuery) -> bool {
        self.complete_metadata_query(&query)
    }

    /// Every piece of metadata this route still lacks.
    fn missing_metadata(&self) -> Vec<wire::MetadataSelector> {
        match self.metadata.as_ref() {
            Some(device) => (1..=device.get().n_streams)
                .flat_map(|stream_id| {
                    match self
                        .streams
                        .get(usize::from(stream_id))
                        .and_then(Option::as_ref)
                    {
                        Some(stream) => stream.missing_metadata(),
                        None => vec![wire::MetadataSelector::stream(stream_id)],
                    }
                })
                .collect(),
            None => vec![wire::MetadataSelector::device()],
        }
    }

    fn metadata_snapshot(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        if !self.missing_metadata().is_empty() {
            return None;
        }
        let device = self.metadata.as_ref()?.clone();
        let streams = (1..=device.get().n_streams)
            .map(|stream_id| {
                let id = StreamId::new(stream_id);
                let stream = self
                    .streams
                    .get(usize::from(stream_id))?
                    .as_ref()?
                    .metadata_snapshot(StreamKey::new(route, id), device.clone())?;
                Some((id, stream))
            })
            .collect::<Option<HashMap<_, _>>>()?;
        Some(DeviceMetadataSnapshot::new(route, device, streams))
    }
}

/// Result of applying one packet to the ordered metadata and continuity state.
pub(super) enum PacketEvent<'a> {
    Applied,
    WaitingForMetadata,
    Reset,
    Rows(ScannedRows<'a>),
}

/// Route-aware state machine shared by incremental and seekable packet readers.
#[derive(Clone)]
pub(super) struct ParseState {
    root_route: DeviceRoute,
    ignore_session: bool,
    devices: Vec<(DeviceRoute, DeviceState)>,
    global_generation: u32,
}

impl ParseState {
    pub(super) fn new(root_route: DeviceRoute, ignore_session: bool) -> Self {
        Self {
            root_route,
            ignore_session,
            devices: Vec::new(),
            global_generation: 0,
        }
    }

    fn device_index(&mut self, route: DeviceRoute) -> usize {
        match self.devices.iter().position(|(known, _)| *known == route) {
            Some(index) => index,
            None => {
                self.devices
                    .push((route, DeviceState::new(self.ignore_session)));
                self.devices.len() - 1
            }
        }
    }

    fn device_mut(&mut self, route: DeviceRoute) -> &mut DeviceState {
        let index = self.device_index(route);
        &mut self.devices[index].1
    }

    pub(super) fn apply_packet<'a>(
        &'a mut self,
        packet: &'a tio::Packet,
    ) -> Result<PacketEvent<'a>, PacketError> {
        let packet_route = packet.route();
        let route = self
            .root_route
            .absolute_route(&packet_route)
            .map_err(|source| PacketError::Route {
                root_route: self.root_route,
                packet_route,
                source,
            })?;

        if let proto::Payload::ProxyStatus(status) = packet.payload() {
            if matches!(status, proto::ProxyStatus::SensorDisconnected) {
                self.reset_subtree(route);
                return Ok(PacketEvent::Reset);
            }
            return Ok(PacketEvent::Applied);
        }

        match packet.payload() {
            proto::Payload::Samples(data) => {
                let index = self.device_index(route);
                let state = self.devices[index]
                    .1
                    .validate_stream_data(data, route, &mut self.global_generation)
                    .map_err(|source| PacketError::Stream {
                        route,
                        stream_id: data.stream_id,
                        source,
                    })?;
                Ok(match state {
                    RowState::WaitingForMetadata => PacketEvent::WaitingForMetadata,
                    RowState::Validated(rows) => PacketEvent::Rows(rows),
                })
            }
            _ => {
                let index = self.device_index(route);
                self.devices[index]
                    .1
                    .apply_control_packet(packet, &mut self.global_generation);
                Ok(PacketEvent::Applied)
            }
        }
    }

    /// Forget the metadata of every route at and below `subtree` and end its
    /// runs. The generation counters survive, so no stamp from before the
    /// reset can be reused after it.
    pub(super) fn reset_subtree(&mut self, subtree: DeviceRoute) {
        self.global_generation += 1;
        for (route, device) in &mut self.devices {
            if route.starts_with(&subtree) {
                device.reset();
            }
        }
    }

    pub(super) fn take_metadata_queries(&mut self) -> Vec<MetadataQuery> {
        self.devices
            .iter_mut()
            .filter_map(|(route, device)| device.take_metadata_query(*route))
            .collect()
    }

    pub(super) fn take_metadata_queries_for(&mut self, route: DeviceRoute) -> Vec<MetadataQuery> {
        if self.root_route.relative_route(&route).is_err() {
            return Vec::new();
        }
        self.device_mut(route)
            .take_metadata_query(route)
            .into_iter()
            .collect()
    }

    pub(super) fn apply_metadata_reply(&mut self, query: MetadataQuery, reply: &[u8]) {
        let index = self.device_index(query.route);
        self.devices[index]
            .1
            .apply_metadata_reply(query, reply, &mut self.global_generation);
    }

    /// False if the query was already stale, so its failure says nothing about
    /// the metadata the route is discovering now.
    pub(super) fn fail_metadata_query(&mut self, query: MetadataQuery) -> bool {
        let index = self.device_index(query.route);
        self.devices[index].1.fail_metadata_query(query)
    }

    pub(super) fn metadata(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        self.devices
            .iter()
            .find(|(known, _)| *known == route)?
            .1
            .metadata_snapshot(route)
    }

    pub(super) fn metadata_revision(&self, route: DeviceRoute) -> Option<u32> {
        self.devices
            .iter()
            .find(|(known, _)| *known == route)
            .map(|(_, device)| device.metadata_revision)
    }

    pub(super) fn routes(&self) -> Vec<DeviceRoute> {
        self.devices.iter().map(|(route, _)| *route).collect()
    }
}

/// Stream bytes validated against the metadata and continuity state in effect
/// at their position in the packet sequence, described without decoding them.
pub(crate) struct ScannedRows<'a> {
    key: StreamKey,
    boundary: Option<BoundaryReason>,
    generations: Generations,
    segment: SegmentRecord,
    stream: StreamRecord,
    device: DeviceRecord,
    first_sample_n: SampleNumber,
    last_sample_n: SampleNumber,
    row_count: usize,
    sample_size: usize,
    encoded: &'a [u8],
    columns: &'a [ColumnRecord],
}

/// One column of a decoded batch: where its bytes start within a sample, the
/// buffer its values land in, and its metadata.
struct DecodableColumn<'a> {
    offset: usize,
    buffer_type: BufferType,
    metadata: &'a ColumnRecord,
}

impl ScannedRows<'_> {
    pub(crate) fn stream_key(&self) -> StreamKey {
        self.key
    }

    pub(crate) fn boundary(&self) -> Option<&BoundaryReason> {
        self.boundary.as_ref()
    }

    pub(crate) fn generations(&self) -> Generations {
        self.generations
    }

    pub(crate) fn segment(&self) -> &SegmentRecord {
        &self.segment
    }

    pub(crate) fn stream(&self) -> &StreamRecord {
        &self.stream
    }

    pub(crate) fn device(&self) -> &DeviceRecord {
        &self.device
    }

    pub(crate) fn columns(&self) -> &[ColumnRecord] {
        self.columns
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn sample_number_bounds(&self) -> (SampleNumber, SampleNumber) {
        (self.first_sample_n, self.last_sample_n)
    }

    /// The columns a batch can hold, in schema order. Columns whose wire type
    /// this build cannot decode are absent, so every batch stays rectangular.
    fn decodable_columns(&self) -> impl Iterator<Item = DecodableColumn<'_>> {
        self.columns
            .iter()
            .scan(0usize, |offset, metadata| {
                let at = *offset;
                *offset += metadata.get().data_type.size();
                Some((at, metadata))
            })
            .filter_map(|(offset, metadata)| {
                Some(DecodableColumn {
                    offset,
                    buffer_type: decoded_buffer_type(metadata.get().data_type)?,
                    metadata,
                })
            })
    }
}

impl RowSource for ScannedRows<'_> {
    fn len(&self) -> usize {
        self.row_count
    }

    fn boundary(&self) -> Option<&BoundaryReason> {
        self.boundary()
    }

    fn generations(&self) -> Generations {
        self.generations()
    }

    fn segment_record(&self) -> &SegmentRecord {
        &self.segment
    }

    fn start_builder(&self, capacity: usize) -> SampleBatchBuilder {
        SampleBatchBuilder::new(
            BatchContext::new(
                self.key,
                self.boundary.clone(),
                self.generations,
                self.segment.clone(),
                self.stream.clone(),
                self.device.clone(),
            ),
            self.decodable_columns()
                .map(|column| (column.metadata.clone(), column.buffer_type)),
            capacity,
        )
    }

    fn append_to(&self, tail: &mut SampleBatchBuilder) {
        for row in 0..self.row_count {
            let start = row * self.sample_size;
            let raw = &self.encoded[start..start + self.sample_size];
            tail.push_row(
                SampleNumber::new(self.first_sample_n.value() + row as u32),
                self.decodable_columns().map(|column| {
                    ColumnData::from_le_bytes(
                        &raw[column.offset..],
                        column.metadata.get().data_type,
                    )
                }),
            );
        }
    }
}

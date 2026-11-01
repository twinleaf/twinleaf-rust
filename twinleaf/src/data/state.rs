//! Ordered metadata, schema, and continuity state for routed TIO packets.
//!
//! This module validates stream payloads and describes their rows without
//! materializing sample values. Packet consumers decide whether to decode the
//! validated bytes immediately or retain only their metadata and boundaries.

use super::continuity::{BoundaryReason, Generations};
use super::decode::{decode_samples, decodes};
use super::metadata::{
    rate_hz, ColumnRecord, DeviceMetadataSnapshot, DeviceRecord, MetadataQuery, SegmentRecord,
    StreamKey, StreamMetadataSnapshot, StreamRecord, StreamSchema,
};
use super::sample::{sample_time, BatchContext, RowSource, SampleBatchBuilder};
use crate::proto::data as wire;
use crate::proto::data::MAX_SAMPLE_NUMBER;
use crate::proto::heartbeat::Heartbeat;
use crate::proto::DeviceRoute;
use crate::proto::{SampleNumber, SegmentId, SessionId, StreamId};
use crate::tio;
use crate::tio::packet;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

/// Why a well-formed packet cannot be applied to the data state.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DataError {
    /// A stream-data payload contradicts the metadata retained for its stream.
    ///
    /// The rows are dropped, but the retained metadata survives for later
    /// packets.
    #[error("invalid data for {route} stream {stream_id}: {source}")]
    Stream {
        /// Absolute route of the device that sent the data.
        route: DeviceRoute,
        /// Stream the payload belongs to, within that device.
        stream_id: StreamId,
        /// Which part of the payload or schema was contradictory.
        #[source]
        source: StreamDataError,
    },
}

/// A stream-data payload contradicts the metadata needed to interpret it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StreamDataError {
    /// The stream record advertises an empty segment ring, so no segment id is
    /// valid.
    #[error("stream advertises a zero-length segment ring")]
    ZeroSegmentCount,
    /// The data packet names a segment the stream record cannot describe.
    #[error("segment {segment_id} is outside the advertised ring of {segment_count}")]
    SegmentOutOfRange {
        /// Segment id carried by the data packet.
        segment_id: SegmentId,
        /// Ring size from the stream record. Valid ids fall below it.
        segment_count: u8,
    },
    /// More column records are retained than the stream record now admits.
    ///
    /// Too few is not an error. The parser waits for the rest to arrive.
    #[error("received {actual} columns for a schema containing {expected}")]
    ColumnCount {
        /// Column count the stream record advertises.
        expected: usize,
        /// Column records retained for the stream.
        actual: usize,
    },
    /// The packed column types do not fit the stream's declared sample stride.
    ///
    /// A stride with room to spare is accepted. Only overflow is rejected.
    #[error("column data occupies {column_bytes} bytes, exceeding sample size {sample_size}")]
    ColumnsExceedSample {
        /// Packed size of every column type in the schema.
        column_bytes: usize,
        /// Per-sample stride the stream record declares.
        sample_size: usize,
    },
    /// The stream record's stride leaves the payload's row count undefined.
    #[error("stream advertises a zero-byte sample")]
    ZeroSampleSize,
    /// The payload ends mid-sample, as in a truncated or corrupt packet.
    #[error("payload length {payload_len} is not a multiple of sample size {sample_size}")]
    MisalignedPayload {
        /// Byte length of the encoded sample payload.
        payload_len: usize,
        /// Stride the payload length must be a multiple of.
        sample_size: usize,
    },
    /// The sender emitted a data packet carrying no rows to apply.
    #[error("stream-data payload contains no rows")]
    EmptyPayload,
    /// The packet's rows run past the 24-bit wrap of the sample counter.
    #[error("{row_count} rows beginning at sample {first_sample_n} exceed the sample counter")]
    SampleNumberOverflow {
        /// Sample number of the packet's first row.
        first_sample_n: SampleNumber,
        /// Rows the payload decodes to, starting at `first_sample_n`.
        row_count: usize,
    },
    /// The segment's timebase is undefined, so no row can be timestamped.
    #[error("segment has invalid sampling rate {sampling_rate} and decimation {decimation}")]
    InvalidRate {
        /// Samples per second before decimation, from the segment record.
        sampling_rate: u32,
        /// Divisor on the sampling rate. The two give the output rate.
        decimation: u32,
    },
}

/// Metadata and continuity state for one stream on one device.
#[derive(Debug, Clone)]
struct StreamTracker {
    metadata: Option<StreamRecord>,
    segments: Vec<Option<SegmentRecord>>,
    columns: Vec<ColumnRecord>,
    /// Built from the stream record and the columns once every column has
    /// arrived, and dropped whenever either changes.
    schema: Option<Arc<StreamSchema>>,

    key: StreamKey,
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
    rate_hz: f64,
}

impl StreamTracker {
    fn new(key: StreamKey) -> Self {
        Self {
            metadata: None,
            segments: Vec::new(),
            columns: Vec::new(),
            schema: None,
            key,
            current_segment_id: SegmentId::new(0),
            established: false,
            run: 0,
            last_segment_id: SegmentId::new(0),
            last_sample_number: SampleNumber::new(0),
            last_timestamp: 0.0,
            last_session_id: SessionId::new(0),
            last_time_ref_session_id: SessionId::new(0),
            rate_hz: 0.0,
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

    /// Learns the stream record of a stream that had none.
    fn learn_stream(&mut self, stream: StreamRecord) {
        self.metadata = Some(stream);
        self.update_schema();
    }

    /// Appends the column record whose index is the number of columns known.
    fn append_column(&mut self, column: ColumnRecord) {
        self.columns.push(column);
        self.update_schema();
    }

    /// Builds the schema once the stream record and all of its columns are
    /// known, and drops it otherwise.
    fn update_schema(&mut self) {
        self.schema = match &self.metadata {
            Some(stream) if self.columns.len() == usize::from(stream.get().n_columns()) => {
                Some(Arc::new(StreamSchema::new(
                    self.key,
                    stream.clone(),
                    self.columns.clone(),
                )))
            }
            _ => None,
        };
    }

    /// Returns selectors for the metadata this stream still needs before it
    /// can be decoded.
    fn missing_metadata(&self) -> Vec<wire::MetadataSelector> {
        let stream_id = self.key.stream_id.value();
        let mut missing = Vec::new();
        match self.metadata.as_ref() {
            Some(metadata) => {
                for index in self.columns.len()..usize::from(metadata.get().n_columns()) {
                    missing.push(wire::MetadataSelector::column(stream_id, index as u8))
                }
            }
            None => {
                missing.push(wire::MetadataSelector::stream(stream_id));
            }
        }
        if self.segment(self.current_segment_id).is_none() {
            missing.push(wire::MetadataSelector::segment(
                stream_id,
                self.current_segment_id.value(),
            ));
        }
        missing
    }

    fn detect_boundary(
        &self,
        first_sample_n: SampleNumber,
        first_timestamp: f64,
        device: wire::DeviceView<'_>,
        segment: wire::SegmentView<'_>,
        new_rate: f64,
        is_segment_rollover: bool,
    ) -> Option<BoundaryReason> {
        if !self.established {
            return Some(BoundaryReason::Initial);
        }

        if device.session() != self.last_session_id {
            return Some(BoundaryReason::SessionChanged {
                old: self.last_session_id,
                new: device.session(),
            });
        }

        if segment.timeref_session() != self.last_time_ref_session_id {
            return Some(BoundaryReason::TimeRefSessionChanged {
                old: self.last_time_ref_session_id,
                new: segment.timeref_session(),
            });
        }

        if (new_rate - self.rate_hz).abs() > 1e-9 {
            return Some(BoundaryReason::RateChanged {
                old_rate: self.rate_hz,
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

        if segment.segment_id() != self.last_segment_id {
            // A benign rollover is time-continuous; a stream restart leaves a
            // forward gap. Backward jumps already returned above.
            let time_continuous = time_gap < half_period;
            return Some(if is_segment_rollover && time_continuous {
                BoundaryReason::SegmentRollover {
                    old_id: self.last_segment_id,
                    new_id: segment.segment_id(),
                }
            } else {
                BoundaryReason::SegmentChanged {
                    old_id: self.last_segment_id,
                    new_id: segment.segment_id(),
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

    /// Validates one encoded payload and updates continuity state.
    ///
    /// Missing metadata yields `PacketEvent::WaitingForMetadata`. A payload that
    /// contradicts complete metadata is an error.
    fn validate_rows<'a>(
        &'a mut self,
        data: wire::Samples<'a>,
        device_metadata: DeviceRecord,
        device_generation: &mut u32,
        global_generation: &mut u32,
        metadata_revision: &mut u32,
    ) -> Result<PacketEvent<'a>, StreamDataError> {
        let Some(stream_metadata) = self.metadata.clone() else {
            return Ok(PacketEvent::WaitingForMetadata);
        };
        let stream = stream_metadata.get();
        let n_segments = stream.n_segments();
        let device = device_metadata.get();

        // Segment ids index a fixed ring, so a wire value outside it is corrupt.
        if n_segments == 0 {
            return Err(StreamDataError::ZeroSegmentCount);
        }
        if data.segment_id.value() >= n_segments {
            return Err(StreamDataError::SegmentOutOfRange {
                segment_id: data.segment_id,
                segment_count: n_segments,
            });
        }
        // The current segment is the one the last broadcast record or rows
        // named, so a segment other than the last rows' is announced when
        // the device broadcast its record since them.
        let announced = self.current_segment_id == data.segment_id;
        // A stream moving onto a segment in one device session finds a stale
        // ring entry when the record is left from an earlier lap of the ring.
        // In the stream's clock, that record's timestamps run backward. A
        // record of another clock is not comparable, and describes the
        // segment only when announced, as the device sends a segment's record
        // before its first sample. A new session clears the ring, so every
        // record after a reboot is the new session's.
        if data.segment_id != self.last_segment_id
            && self.established
            && device.session() == self.last_session_id
        {
            let stale = self.segment(data.segment_id).is_some_and(|record| {
                let seg = record.get();
                if seg.timeref_session() != self.last_time_ref_session_id {
                    return !announced;
                }
                seg.decimation() != 0 && seg.sampling_rate() != 0 && {
                    let half_period =
                        0.5 * f64::from(seg.decimation()) / f64::from(seg.sampling_rate());
                    sample_time(seg, data.first.value()) < self.last_timestamp - half_period
                }
            });
            if stale {
                self.segments[usize::from(data.segment_id.value())] = None;
            }
        }
        if !announced {
            self.current_segment_id = data.segment_id;
            *metadata_revision = metadata_revision.wrapping_add(1);
        }

        let n_columns = usize::from(stream.n_columns());
        match self.columns.len().cmp(&n_columns) {
            Ordering::Less => return Ok(PacketEvent::WaitingForMetadata),
            Ordering::Greater => {
                return Err(StreamDataError::ColumnCount {
                    expected: n_columns,
                    actual: self.columns.len(),
                });
            }
            Ordering::Equal => {}
        }
        // A stream with a column this build cannot decode is refused whole,
        // so every batch has one array per column of its stream.
        if !self
            .columns
            .iter()
            .all(|column| decodes(column.get().data_type()))
        {
            return Ok(PacketEvent::Unsupported);
        }

        let sample_size = usize::from(stream.sample_size());
        let expected_sample_size: usize = self
            .columns
            .iter()
            .map(|column| column.get().data_type().size())
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
            SegmentId::new(self.last_segment_id.value().wrapping_add(1) % n_segments);

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
                let mut rolled = previous.get().fields();
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
                    return Ok(PacketEvent::WaitingForMetadata);
                }
                rolled.start_time += seconds_at(next_sample) as u32;
                rolled.segment_id = data.segment_id;
                let rolled = SegmentRecord::encode(rolled)
                    .expect("a re-timed copy of a retained record is the same length");
                (rolled, true)
            }
            (None, None) => return Ok(PacketEvent::WaitingForMetadata),
        };
        let segment = segment_record.get();

        let Some(new_rate) = rate_hz(&segment) else {
            return Err(StreamDataError::InvalidRate {
                sampling_rate: segment.sampling_rate(),
                decimation: segment.decimation(),
            });
        };

        let boundary = self.detect_boundary(
            data.first,
            sample_time(segment, data.first.value()),
            device,
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
        self.last_session_id = device.session();
        self.last_time_ref_session_id = segment.timeref_session();
        self.last_segment_id = segment.segment_id();
        self.rate_hz = new_rate;
        self.established = true;

        let schema = self
            .schema
            .as_ref()
            .expect("a stream with every column has a schema");
        debug_assert!(schema.describes(&stream_metadata, &self.columns));
        Ok(PacketEvent::Rows(ScannedRows {
            schema,
            device: device_metadata,
            segment: segment_record,
            boundary,
            generations: Generations {
                stream: self.run,
                device: *device_generation,
                global: *global_generation,
            },
            first_sample_n: data.first,
            last_sample_n,
            row_count,
            encoded: data.data,
        }))
    }

    fn invalidate_metadata(&mut self) {
        self.metadata = None;
        self.segments.clear();
        self.columns.clear();
        self.schema = None;
    }

    /// Forgets metadata and ends the current run. The run counter is kept, and
    /// the rebuilt stream numbers its runs after it.
    fn reset(&mut self) {
        self.invalidate_metadata();
        self.established = false;
    }

    fn metadata_snapshot(&self, device: DeviceRecord) -> Option<StreamMetadataSnapshot> {
        Some(StreamMetadataSnapshot::new(
            self.schema.clone()?,
            device,
            self.segment(self.current_segment_id)?.clone(),
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
    route: DeviceRoute,
    metadata: Option<DeviceRecord>,
    streams: Vec<Option<StreamTracker>>,
    ignore_session: bool,
    query_in_flight: bool,
    metadata_generation: u32,
    /// Bumps whenever an applied record may change what a complete snapshot of
    /// this route would contain.
    metadata_revision: u32,
    generation: u32,
}

impl DeviceState {
    fn new(route: DeviceRoute, ignore_session: bool) -> Self {
        Self {
            route,
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

    fn stream_mut(&mut self, stream_id: StreamId) -> &mut StreamTracker {
        let index = usize::from(stream_id.value());
        if self.streams.len() <= index {
            self.streams.resize_with(index + 1, || None);
        }
        let key = StreamKey::new(self.route, stream_id);
        self.streams[index].get_or_insert_with(|| StreamTracker::new(key))
    }

    /// Bootstraps this route again after metadata contradicted the state used
    /// to decode earlier rows. Each stream keeps its run counter, and the
    /// global generation bumps.
    fn forget_all_metadata(&mut self, global_generation: &mut u32) {
        self.reset();
        *global_generation += 1;
    }

    /// Resets every stream as [`StreamTracker::reset`] does, and opens a new
    /// device generation.
    fn reset(&mut self) {
        self.metadata = None;
        self.rediscover_metadata();
        self.generation += 1;
        for stream in self.streams.iter_mut().flatten() {
            stream.reset();
        }
    }

    /// Opens a new metadata generation and arms discovery again. Queries
    /// minted before this point no longer apply.
    fn rediscover_metadata(&mut self) {
        self.metadata_generation = self.metadata_generation.wrapping_add(1);
        self.query_in_flight = false;
    }

    fn accepts_stream(&mut self, stream_id: StreamId, global_generation: &mut u32) -> bool {
        if self
            .metadata
            .as_ref()
            .is_some_and(|device| stream_id.value() > device.get().n_streams())
        {
            // An impossible stream id means our device description is stale or
            // corrupt. Bootstrap the route again instead of retaining it.
            self.forget_all_metadata(global_generation);
            false
        } else {
            true
        }
    }

    /// Merges one metadata record into this route's decoding state. A record
    /// that does not parse as the type it claims is ignored.
    fn apply_metadata(
        &mut self,
        kind: wire::MetadataType,
        record: &[u8],
        source: MetadataSource,
        global_generation: &mut u32,
    ) {
        match kind {
            wire::MetadataType::Device => {
                let Some(incoming) = DeviceRecord::new(record) else {
                    return;
                };
                if self.metadata.as_ref() != Some(&incoming) {
                    self.revise_metadata();
                }
                if let Some(previous) = self.metadata.take() {
                    let (old, new) = (previous.get(), incoming.get());
                    if old.serial() != new.serial() {
                        self.forget_all_metadata(global_generation);
                    } else if old.session() != new.session() {
                        // Keeping continuity state lets the first rows under the
                        // new metadata report a specific SessionChanged boundary.
                        for stream in self.streams.iter_mut().flatten() {
                            stream.invalidate_metadata();
                        }
                        self.rediscover_metadata();
                    } else if old.firmware() != new.firmware() {
                        // A firmware change can alter a schema without changing
                        // the session or timeline, so it must explicitly end all
                        // current runs rather than relying on boundary detection.
                        self.forget_all_metadata(global_generation);
                    }
                }
                self.metadata = Some(incoming);
            }
            wire::MetadataType::Stream => {
                let Some(incoming) = StreamRecord::new(record) else {
                    return;
                };
                let stream_id = incoming.get().stream_id();
                if !self.accepts_stream(stream_id, global_generation) {
                    return;
                }
                let stream = self.stream_mut(stream_id);
                let (contradicted, learned) = match &stream.metadata {
                    Some(current) => (*current != incoming, false),
                    None => {
                        stream.learn_stream(incoming);
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
            wire::MetadataType::Segment => {
                let Some(incoming) = SegmentRecord::new(record) else {
                    return;
                };
                let segment = incoming.get();
                let (stream_id, segment_id) = (segment.stream_id(), segment.segment_id());
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
            wire::MetadataType::Column => {
                let Some(incoming) = ColumnRecord::new(record) else {
                    return;
                };
                let column = incoming.get();
                let (stream_id, index) = (column.stream_id(), column.index().index());
                if !self.accepts_stream(stream_id, global_generation) {
                    return;
                }
                let stream = self.stream_mut(stream_id);
                let (contradicted, learned) = match stream.columns.get(index) {
                    Some(current) => (*current != incoming, false),
                    None => {
                        let appended = index == stream.columns.len();
                        if appended {
                            if !decodes(column.data_type()) {
                                log::warn!(
                                    "stream {stream_id} column {index} has data type {}, which this build cannot decode, so the stream is refused",
                                    column.data_type()
                                );
                            }
                            stream.append_column(incoming);
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
            wire::MetadataType::Unknown(_) => {}
        }
    }

    /// Applies a packet that updates decoding state but carries no sample rows.
    fn apply_control_packet(&mut self, packet: &tio::Packet, global_generation: &mut u32) {
        match packet.payload() {
            packet::Payload::Metadata(..) => {
                let (kind, _, record) = wire::split_metadata(packet.payload_bytes())
                    .expect("a metadata payload was framed when the packet was validated");
                self.apply_metadata(
                    kind.into(),
                    record,
                    MetadataSource::Broadcast,
                    global_generation,
                );
            }
            packet::Payload::Heartbeat(Heartbeat::Session { session, .. }) => {
                if let Some(device) = &self.metadata {
                    if device.get().session() != session && !self.ignore_session {
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

    /// Validates one stream-data packet against this route's metadata.
    fn validate_stream_data<'a>(
        &'a mut self,
        data: wire::Samples<'a>,
        global_generation: &mut u32,
    ) -> Result<PacketEvent<'a>, StreamDataError> {
        let Some(device_metadata) = self.metadata.clone() else {
            return Ok(PacketEvent::WaitingForMetadata);
        };
        if !self.accepts_stream(data.stream_id, global_generation) {
            return Ok(PacketEvent::WaitingForMetadata);
        }

        let index = usize::from(data.stream_id.value());
        if self.streams.len() <= index {
            self.streams.resize_with(index + 1, || None);
        }
        let key = StreamKey::new(self.route, data.stream_id);
        self.streams[index]
            .get_or_insert_with(|| StreamTracker::new(key))
            .validate_rows(
                data,
                device_metadata,
                &mut self.generation,
                global_generation,
                &mut self.metadata_revision,
            )
    }

    /// Returns one query for as much of the missing metadata as a request may
    /// carry, or `None` while a query is outstanding or nothing is missing.
    fn take_metadata_query(&mut self) -> Option<MetadataQuery> {
        if self.query_in_flight {
            return None;
        }
        let missing = self.missing_metadata();
        let selectors = match missing.as_slice() {
            [] => return None,
            [only] if only.mtype == wire::MetadataType::Device => Vec::new(),
            missing => missing[..missing.len().min(wire::MAX_METADATA_SELECTORS)].to_vec(),
        };
        self.query_in_flight = true;
        Some(MetadataQuery {
            route: self.route,
            selectors,
            generation: self.metadata_generation,
        })
    }

    /// Completes `query` and opens a new metadata generation. Returns `false`
    /// if the route has already left the query's generation, including by
    /// completing that same query.
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

    /// Returns selectors for every piece of metadata this route still lacks.
    fn missing_metadata(&self) -> Vec<wire::MetadataSelector> {
        match self.metadata.as_ref() {
            Some(device) => (1..=device.get().n_streams())
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

    fn metadata_snapshot(&self) -> Option<DeviceMetadataSnapshot> {
        if !self.missing_metadata().is_empty() {
            return None;
        }
        let device = self.metadata.as_ref()?.clone();
        let streams = (1..=device.get().n_streams())
            .map(|stream_id| {
                let stream = self
                    .streams
                    .get(usize::from(stream_id))?
                    .as_ref()?
                    .metadata_snapshot(device.clone())?;
                Some((StreamId::new(stream_id), stream))
            })
            .collect::<Option<HashMap<_, _>>>()?;
        Some(DeviceMetadataSnapshot::new(self.route, device, streams))
    }
}

/// Result of applying one packet to the ordered metadata and continuity state.
pub(super) enum PacketEvent<'a> {
    Applied,
    WaitingForMetadata,
    /// The stream has a column of a type this build cannot decode.
    Unsupported,
    Reset,
    Rows(ScannedRows<'a>),
}

/// Route-aware state machine shared by incremental and seekable packet readers.
#[derive(Clone)]
pub(super) struct ParseState {
    ignore_session: bool,
    devices: Vec<DeviceState>,
    global_generation: u32,
}

impl ParseState {
    pub(super) fn new(ignore_session: bool) -> Self {
        Self {
            ignore_session,
            devices: Vec::new(),
            global_generation: 0,
        }
    }

    fn device_index(&mut self, route: DeviceRoute) -> usize {
        match self.devices.iter().position(|device| device.route == route) {
            Some(index) => index,
            None => {
                self.devices
                    .push(DeviceState::new(route, self.ignore_session));
                self.devices.len() - 1
            }
        }
    }

    fn device(&self, route: DeviceRoute) -> Option<&DeviceState> {
        self.devices.iter().find(|device| device.route == route)
    }

    pub(super) fn apply_packet<'a>(
        &'a mut self,
        packet: &'a tio::Packet,
    ) -> Result<PacketEvent<'a>, DataError> {
        let route = packet.route();

        if let Some(status) = packet.link_status() {
            if matches!(
                status,
                packet::LinkStatus::SensorDisconnected | packet::LinkStatus::Stopped
            ) {
                self.reset(|device| device.starts_with(&route));
                return Ok(PacketEvent::Reset);
            }
            return Ok(PacketEvent::Applied);
        }

        match packet.payload() {
            packet::Payload::Samples(data) => {
                let index = self.device_index(route);
                self.devices[index]
                    .validate_stream_data(data, &mut self.global_generation)
                    .map_err(|source| DataError::Stream {
                        route,
                        stream_id: data.stream_id,
                        source,
                    })
            }
            _ => {
                let index = self.device_index(route);
                self.devices[index].apply_control_packet(packet, &mut self.global_generation);
                Ok(PacketEvent::Applied)
            }
        }
    }

    /// Forgets the metadata of every route at and below `subtree` and ends its
    /// runs. The generation counters keep counting, and no stamp from before
    /// the reset recurs after it.
    pub(super) fn reset(&mut self, routes: impl Fn(&DeviceRoute) -> bool) {
        self.global_generation += 1;
        for device in &mut self.devices {
            if routes(&device.route) {
                device.reset();
            }
        }
    }

    pub(super) fn take_metadata_queries_for(&mut self, route: DeviceRoute) -> Vec<MetadataQuery> {
        let index = self.device_index(route);
        self.devices[index]
            .take_metadata_query()
            .into_iter()
            .collect()
    }

    pub(super) fn apply_metadata_reply(&mut self, query: MetadataQuery, reply: &[u8]) {
        let index = self.device_index(query.route);
        self.devices[index].apply_metadata_reply(query, reply, &mut self.global_generation);
    }

    /// Gives up on `query`. Returns `false` if the query was already stale.
    pub(super) fn fail_metadata_query(&mut self, query: MetadataQuery) -> bool {
        let index = self.device_index(query.route);
        self.devices[index].fail_metadata_query(query)
    }

    pub(super) fn metadata(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        self.device(route)?.metadata_snapshot()
    }

    pub(super) fn metadata_revision(&self, route: DeviceRoute) -> Option<u32> {
        self.device(route).map(|device| device.metadata_revision)
    }

    pub(super) fn routes(&self) -> Vec<DeviceRoute> {
        self.devices.iter().map(|device| device.route).collect()
    }
}

/// Stream bytes validated against the metadata and continuity state in effect
/// at their position in the packet sequence, described without decoding them.
pub(crate) struct ScannedRows<'a> {
    schema: &'a Arc<StreamSchema>,
    device: DeviceRecord,
    segment: SegmentRecord,
    boundary: Option<BoundaryReason>,
    generations: Generations,
    first_sample_n: SampleNumber,
    last_sample_n: SampleNumber,
    row_count: usize,
    encoded: &'a [u8],
}

impl ScannedRows<'_> {
    pub(crate) fn stream_key(&self) -> StreamKey {
        self.schema.stream_key()
    }

    pub(crate) fn device(&self) -> &DeviceRecord {
        &self.device
    }

    /// Returns the metadata these rows carry, as a batch of them would.
    pub(crate) fn metadata(&self) -> StreamMetadataSnapshot {
        StreamMetadataSnapshot::new(
            self.schema.clone(),
            self.device.clone(),
            self.segment.clone(),
        )
    }

    pub(crate) fn sample_number_bounds(&self) -> (SampleNumber, SampleNumber) {
        (self.first_sample_n, self.last_sample_n)
    }
}

impl RowSource for ScannedRows<'_> {
    fn len(&self) -> usize {
        self.row_count
    }

    fn boundary(&self) -> Option<&BoundaryReason> {
        self.boundary.as_ref()
    }

    fn generations(&self) -> Generations {
        self.generations
    }

    fn schema(&self) -> &Arc<StreamSchema> {
        self.schema
    }

    fn segment_record(&self) -> &SegmentRecord {
        &self.segment
    }

    fn start_builder(&self, capacity: usize) -> SampleBatchBuilder {
        SampleBatchBuilder::new(
            BatchContext {
                metadata: self.metadata(),
                boundary: self.boundary.clone(),
                generations: self.generations,
            },
            capacity,
        )
    }

    fn append_to(&self, tail: &mut SampleBatchBuilder) {
        let columns = tail.push_samples(self.first_sample_n, self.row_count);
        decode_samples(self.schema, self.encoded, self.row_count, columns);
    }
}

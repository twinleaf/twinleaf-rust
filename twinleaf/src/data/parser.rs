//! Incremental conversion from routed TIO packets to columnar sample batches.
//!
//! [`PacketParser`] applies packets in arrival order. Shared metadata and
//! continuity state validates each stream payload, after which this module
//! decodes its rows and accumulates them into [`SampleBatch`] values.
//!
//! Stream data can arrive before its metadata. Such packets produce no rows;
//! [`PacketParser::take_requests`] returns the metadata RPCs needed to decode
//! subsequent packets.

use super::sample::{ColumnData, SampleBatch, Series};
use super::state::{DeviceMetadataSnapshot, PacketError, PacketEvent, ParseState, ValidatedRows};
use crate::data::ColumnVec;
use crate::tio::{self, proto};
use proto::identifiers::StreamKey;
use proto::DeviceRoute;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

impl ValidatedRows<'_> {
    fn can_append_to(&self, batch: &SampleBatch) -> bool {
        (Arc::ptr_eq(batch.segment(), &self.segment)
            || batch.segment().as_ref() == self.segment.as_ref())
            && batch.schema().len() == self.decodable_columns().count()
            && batch
                .schema()
                .iter()
                .zip(self.decodable_columns())
                .all(|(series, column)| {
                    Arc::ptr_eq(series.metadata(), column.metadata)
                        || series.metadata().as_ref() == column.metadata.as_ref()
                })
    }

    fn decode_into(&self, batch: &mut SampleBatch) {
        for row in 0..self.row_count {
            let start = row * self.sample_size;
            let raw = &self.encoded[start..start + self.sample_size];
            batch.push_row(
                self.first_sample_n + row as u32,
                self.decodable_columns().map(|column| {
                    ColumnData::from_le_bytes(&raw[column.offset..], column.metadata.data_type)
                }),
            );
        }
    }

    fn append_to(&self, batch: &mut SampleBatch) {
        debug_assert!(self.can_append_to(batch));
        debug_assert_eq!(batch.generations(), self.generations);
        self.decode_into(batch);
    }

    fn into_batch(self, route: DeviceRoute, capacity: usize) -> SampleBatch {
        let columns: Vec<_> = self
            .decodable_columns()
            .map(|column| {
                Series::new(
                    column.metadata.index,
                    column.metadata.clone(),
                    ColumnVec::with_capacity_for(column.buffer_type, capacity),
                )
            })
            .collect();
        let mut batch = SampleBatch::new(
            route,
            self.boundary.clone(),
            self.generations,
            Vec::with_capacity(capacity),
            columns,
            self.segment.clone(),
            self.stream.clone(),
            self.device.clone(),
        );
        self.decode_into(&mut batch);
        batch
    }
}

fn flush_pending_batches(
    pending: &mut HashMap<StreamKey, SampleBatch>,
    ready: &mut VecDeque<SampleBatch>,
) {
    let mut keys: Vec<_> = pending.keys().copied().collect();
    keys.sort_unstable();
    for key in keys {
        ready.push_back(pending.remove(&key).unwrap());
    }
}

/// Decode validated rows and either append, hold, or emit the resulting batch.
fn queue_rows(
    route: DeviceRoute,
    stream_id: u8,
    input: ValidatedRows<'_>,
    target_rows: Option<usize>,
    pending: &mut HashMap<StreamKey, SampleBatch>,
    ready: &mut VecDeque<SampleBatch>,
) -> usize {
    let rows = input.row_count;
    let key = StreamKey::new(route, stream_id);
    // A boundary ends its own stream's batch and emits immediately. Seamless
    // and startup boundaries leave other streams accumulating, but a pending
    // batch must never straddle a bump of the shared generations: emit everything
    // from the older generation first, preserving arrival order at the bump.
    let starts_boundary = input.boundary.is_some();
    if pending
        .values()
        .any(|b| b.generations().global != input.generations.global)
    {
        flush_pending_batches(pending, ready);
    }

    match pending.entry(key) {
        Entry::Occupied(mut entry) if !starts_boundary && input.can_append_to(entry.get()) => {
            input.append_to(entry.get_mut());
            if target_rows.is_none() || entry.get().len() >= target_rows.expect("checked above") {
                ready.push_back(entry.remove());
            }
        }
        Entry::Occupied(entry) => {
            ready.push_back(entry.remove());
            let capacity = target_rows.unwrap_or(rows).max(rows);
            let batch = input.into_batch(route, capacity);
            if starts_boundary || target_rows.is_none() || batch.len() >= capacity {
                ready.push_back(batch);
            } else {
                pending.insert(key, batch);
            }
        }
        Entry::Vacant(entry) => {
            let capacity = target_rows.unwrap_or(rows).max(rows);
            let batch = input.into_batch(route, capacity);
            if starts_boundary || target_rows.is_none() || batch.len() >= capacity {
                ready.push_back(batch);
            } else {
                entry.insert(batch);
            }
        }
    }

    rows
}

/// Incremental, route-aware parser for TIO packets.
///
/// The caller owns packet I/O and supplies packets in arrival order. By
/// default each decodable stream-data packet emits one [`SampleBatch`].
/// [`PacketParser::with_batch_rows`] instead accumulates compatible packets for
/// bulk consumers. Boundaries and schema changes always end the current batch.
pub struct PacketParser {
    state: ParseState,
    batch_target_rows: Option<usize>,
    pending_batches: HashMap<StreamKey, SampleBatch>,
    ready_batches: VecDeque<SampleBatch>,
}

/// State transition produced by pushing one packet into a [`PacketParser`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketOutcome {
    /// A control packet was applied without producing sample rows.
    Applied,
    /// Stream data is valid so far but cannot be decoded until metadata arrives.
    WaitingForMetadata,
    /// A disconnect reset metadata and continuity state.
    Reset,
    /// The packet contained this many validated sample rows.
    Rows(usize),
}

impl PacketOutcome {
    /// Number of sample rows validated by this transition.
    pub fn row_count(self) -> usize {
        match self {
            Self::Rows(count) => count,
            _ => 0,
        }
    }
}

impl PacketParser {
    pub(super) fn from_state(state: ParseState) -> Self {
        Self {
            state,
            batch_target_rows: None,
            pending_batches: HashMap::new(),
            ready_batches: VecDeque::new(),
        }
    }

    /// Create a parser for packets whose routes are relative to `root_route`.
    ///
    /// When `ignore_session` is false, a heartbeat session change invalidates
    /// cached metadata and starts metadata discovery again.
    pub fn new(root_route: DeviceRoute, ignore_session: bool) -> Self {
        Self::from_state(ParseState::new(root_route, ignore_session))
    }

    /// Accumulate approximately `rows` decoded samples per stream before
    /// emitting a batch. Boundaries and schema changes always end a batch.
    pub fn with_batch_rows(mut self, rows: usize) -> Self {
        assert!(rows > 0, "batch row target must be nonzero");
        self.batch_target_rows = Some(rows);
        self
    }

    /// Ingest one port-relative packet and advance the decoding state.
    ///
    /// Missing metadata is reported as [`PacketOutcome::WaitingForMetadata`];
    /// call [`PacketParser::take_requests`] to obtain the corresponding RPCs.
    /// Invalid stream data returns [`PacketError`]. Completed batches are
    /// available through [`PacketParser::pop_batch`].
    pub fn push_packet(&mut self, packet: &tio::Packet) -> Result<PacketOutcome, PacketError> {
        Ok(match self.state.apply_packet(packet)? {
            PacketEvent::Applied => PacketOutcome::Applied,
            PacketEvent::WaitingForMetadata => PacketOutcome::WaitingForMetadata,
            PacketEvent::Reset => {
                self.flush();
                PacketOutcome::Reset
            }
            PacketEvent::Rows {
                route,
                stream_id,
                rows,
            } => PacketOutcome::Rows(queue_rows(
                route,
                stream_id,
                rows,
                self.batch_target_rows,
                &mut self.pending_batches,
                &mut self.ready_batches,
            )),
        })
    }

    /// Pop the oldest completed batch currently buffered.
    pub fn pop_batch(&mut self) -> Option<SampleBatch> {
        self.ready_batches.pop_front()
    }

    /// End every partially accumulated stream batch without resetting metadata.
    pub fn flush(&mut self) {
        flush_pending_batches(&mut self.pending_batches, &mut self.ready_batches);
    }

    /// Drain batches that are already ready without flushing partial batches.
    pub fn drain_batches(&mut self) -> Vec<SampleBatch> {
        self.ready_batches.drain(..).collect()
    }

    /// Flush accumulated samples and return every completed batch.
    pub fn finish(mut self) -> Vec<SampleBatch> {
        self.flush();
        self.drain_batches()
    }

    /// Forget device metadata. Any decoded rows already accumulated remain
    /// available through `pop_batch`.
    pub fn reset(&mut self) {
        self.flush();
        self.state.reset();
    }

    /// Pending metadata requests across all routes. Each packet's routing is
    /// stamped relative to this parser's root and can be sent directly on the
    /// corresponding port.
    pub fn take_requests(&mut self) -> Vec<tio::Packet> {
        self.state.take_requests()
    }

    /// Ensure a route has parser state and take its pending metadata requests.
    pub fn take_requests_for(&mut self, route: DeviceRoute) -> Vec<tio::Packet> {
        self.state.take_requests_for(route)
    }

    /// Return complete metadata for an absolute route once it is available.
    pub fn metadata(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        self.state.metadata(route)
    }

    /// Absolute routes for which this parser has observed state.
    pub fn routes(&self) -> Vec<DeviceRoute> {
        self.state.routes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Boundary, BoundaryClass, ColumnVec, Generations, StreamDataError};
    use bytes::Bytes;
    use proto::identifiers::MAX_SAMPLE_NUMBER;
    use proto::meta::{
        ColumnMetadata, DeviceMetadata, MetadataContent, MetadataEpoch, MetadataFilter,
        MetadataPayload, SegmentMetadata, StreamMetadata,
    };
    use proto::{DataType, Payload, StreamDataPayload};

    const STREAM_ID: u8 = 1;

    fn metadata_packet(content: MetadataContent) -> tio::Packet {
        tio::Packet {
            payload: Payload::Metadata(MetadataPayload {
                content,
                flags: 0,
                unknown_fixed: Vec::new(),
                unknown_varlen: Vec::new(),
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    /// Announce a device with one stream whose columns have `column_types`, then
    /// return a parser holding that metadata.
    fn parser_with_schema(column_types: &[DataType], sample_size: usize) -> PacketParser {
        parser_with_clock(column_types, sample_size, 1, 1, 1)
    }

    /// As [`parser_with_schema`], with an explicit segment ring and clock. Only
    /// segment 0 is announced, so later segments must be synthesized.
    fn parser_with_clock(
        column_types: &[DataType],
        sample_size: usize,
        n_segments: usize,
        sampling_rate: u32,
        decimation: u32,
    ) -> PacketParser {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        for content in [
            MetadataContent::Device(DeviceMetadata {
                serial_number: "SN123".to_string(),
                firmware_hash: "fw".to_string(),
                n_streams: 1,
                session_id: 42,
                name: "test-device".to_string(),
            }),
            MetadataContent::Stream(StreamMetadata {
                stream_id: STREAM_ID,
                name: "test-stream".to_string(),
                n_columns: column_types.len(),
                n_segments,
                sample_size,
                buf_samples: 128,
            }),
            MetadataContent::Segment(SegmentMetadata {
                stream_id: STREAM_ID,
                segment_id: 0,
                flags: 0,
                time_ref_epoch: MetadataEpoch::Unix,
                time_ref_serial: "clock".to_string(),
                time_ref_session_id: 7,
                start_time: 0,
                sampling_rate,
                decimation,
                filter_cutoff: 0.0,
                filter_type: MetadataFilter::Unfiltered,
            }),
        ] {
            parser
                .push_packet(&metadata_packet(content))
                .expect("valid metadata");
        }
        for (index, data_type) in column_types.iter().enumerate() {
            parser
                .push_packet(&metadata_packet(MetadataContent::Column(ColumnMetadata {
                    stream_id: STREAM_ID,
                    index,
                    data_type: *data_type,
                    name: format!("col_{index}"),
                    units: String::new(),
                    description: String::new(),
                })))
                .expect("valid column metadata");
        }
        parser
    }

    /// Announce a device with `n_streams` single-`Float32`-column streams,
    /// numbered from 1.
    fn parser_with_streams(n_streams: usize) -> PacketParser {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        parser
            .push_packet(&metadata_packet(MetadataContent::Device(DeviceMetadata {
                serial_number: "SN123".to_string(),
                firmware_hash: "fw".to_string(),
                n_streams,
                session_id: 42,
                name: "test-device".to_string(),
            })))
            .expect("valid metadata");
        for stream_id in 1..=n_streams as u8 {
            for content in [
                MetadataContent::Stream(StreamMetadata {
                    stream_id,
                    name: format!("stream-{stream_id}"),
                    n_columns: 1,
                    n_segments: 1,
                    sample_size: 4,
                    buf_samples: 128,
                }),
                MetadataContent::Segment(SegmentMetadata {
                    stream_id,
                    segment_id: 0,
                    flags: 0,
                    time_ref_epoch: MetadataEpoch::Unix,
                    time_ref_serial: "clock".to_string(),
                    time_ref_session_id: 7,
                    start_time: 0,
                    sampling_rate: 1,
                    decimation: 1,
                    filter_cutoff: 0.0,
                    filter_type: MetadataFilter::Unfiltered,
                }),
                MetadataContent::Column(ColumnMetadata {
                    stream_id,
                    index: 0,
                    data_type: DataType::Float32,
                    name: "col_0".to_string(),
                    units: String::new(),
                    description: String::new(),
                }),
            ] {
                parser
                    .push_packet(&metadata_packet(content))
                    .expect("valid metadata");
            }
        }
        parser
    }

    fn stream_data_packet_for(stream_id: u8, first_sample_n: u32, rows: usize) -> tio::Packet {
        tio::Packet {
            payload: Payload::StreamData(StreamDataPayload {
                stream_id,
                first_sample_n,
                segment_id: 0,
                data: Bytes::from(vec![0; 4 * rows]),
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    fn stream_data_packet_in_segment(
        segment_id: u8,
        first_sample_n: u32,
        data: Vec<u8>,
    ) -> tio::Packet {
        tio::Packet {
            payload: Payload::StreamData(StreamDataPayload {
                stream_id: STREAM_ID,
                first_sample_n,
                segment_id,
                data: Bytes::from(data),
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    fn stream_data_packet(first_sample_n: u32, data: Vec<u8>) -> tio::Packet {
        stream_data_packet_in_segment(0, first_sample_n, data)
    }

    #[test]
    fn metadata_requests_rearm_only_after_a_terminal_response() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);

        let first = parser.take_requests_for(DeviceRoute::root());
        assert_eq!(first.len(), 1);
        let request_id = match &first[0].payload {
            Payload::RpcRequest(request) => request.id,
            other => panic!("expected metadata RPC request, got {other:?}"),
        };
        assert!(parser.take_requests_for(DeviceRoute::root()).is_empty());

        parser
            .push_packet(&tio::Packet {
                payload: Payload::RpcReply(proto::RpcReplyPayload {
                    id: request_id,
                    reply: Vec::new(),
                }),
                routing: DeviceRoute::root(),
                ttl: 0,
            })
            .unwrap();
        assert_eq!(parser.take_requests_for(DeviceRoute::root()).len(), 1);
        assert!(parser.take_requests_for(DeviceRoute::root()).is_empty());

        parser
            .push_packet(&tio::Packet::rpc_error(
                request_id,
                proto::RpcErrorCode::Timeout,
                DeviceRoute::root(),
            ))
            .unwrap();
        assert_eq!(parser.take_requests_for(DeviceRoute::root()).len(), 1);
    }

    #[test]
    fn unknown_column_type_is_absent_from_the_batch() {
        // A newer firmware reports a type this build predates, between two
        // columns it understands.
        let unknown = DataType::Unknown(0x35);
        assert_eq!(unknown.size(), 3);
        let mut parser = parser_with_schema(&[DataType::Float32, unknown, DataType::Int16], 9);

        let mut data = Vec::new();
        for (value, raw) in [(1.0f32, -3i16), (2.0f32, -4i16)] {
            data.extend_from_slice(&value.to_le_bytes());
            data.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
            data.extend_from_slice(&raw.to_le_bytes());
        }
        assert_eq!(
            parser.push_packet(&stream_data_packet(0, data)).unwrap(),
            PacketOutcome::Rows(2)
        );
        let batch = parser.pop_batch().expect("known columns still decode");

        assert_eq!(batch.len(), 2);
        let names: Vec<&str> = batch
            .schema()
            .iter()
            .map(|series| series.metadata().name.as_str())
            .collect();
        assert_eq!(names, ["col_0", "col_2"]);
        match (&batch.schema()[0].values(), &batch.schema()[1].values()) {
            (ColumnVec::F64(floats), ColumnVec::I64(ints)) => {
                assert_eq!(floats, &[1.0, 2.0]);
                assert_eq!(ints, &[-3, -4]);
            }
            other => panic!("unexpected column buffers: {other:?}"),
        }
    }

    #[test]
    fn segment_id_outside_the_ring_is_rejected() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4);
        // The stream advertises a single segment, so any other id is corrupt and
        // must not become the state the next packet is validated against.
        assert!(matches!(
            parser.push_packet(&stream_data_packet_in_segment(255, 0, vec![0; 4])),
            Err(PacketError::Stream {
                source: StreamDataError::SegmentOutOfRange { .. },
                ..
            })
        ));
        assert_eq!(
            parser
                .push_packet(&stream_data_packet(0, vec![0; 4]))
                .unwrap(),
            PacketOutcome::Rows(1)
        );
        assert!(parser.pop_batch().is_some());
    }

    #[test]
    fn rows_past_the_24_bit_sample_field_are_rejected() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4);

        assert!(matches!(
            parser.push_packet(&stream_data_packet(MAX_SAMPLE_NUMBER, vec![0; 8])),
            Err(PacketError::Stream {
                source: StreamDataError::SampleNumberOverflow { .. },
                ..
            })
        ));

        parser
            .push_packet(&stream_data_packet(MAX_SAMPLE_NUMBER, vec![0; 4]))
            .expect("the largest representable sample itself is valid");
        let batch = parser.pop_batch().expect("a batch is ready");
        assert_eq!(batch.sample_numbers(), [MAX_SAMPLE_NUMBER]);
    }

    #[test]
    fn all_columns_unknown_yields_rows_without_columns() {
        let mut parser = parser_with_schema(&[DataType::Unknown(0x35)], 3);
        parser
            .push_packet(&stream_data_packet(7, vec![0; 6]))
            .expect("rows are still counted");
        let batch = parser.pop_batch().expect("a batch is ready");
        assert!(batch.schema().is_empty());
        assert_eq!(batch.sample_numbers(), vec![7, 8]);
    }

    /// Drive a forced rollover: `output_rows` samples of segment 0, then the
    /// first sample of segment 1 whose metadata has not arrived.
    fn rollover_batch(sampling_rate: u32, decimation: u32, output_rows: usize) -> SampleBatch {
        let mut parser = parser_with_clock(&[DataType::Float32], 4, 2, sampling_rate, decimation);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4 * output_rows]))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");
        parser
            .push_packet(&stream_data_packet_in_segment(1, 0, vec![0; 4]))
            .expect("valid rows");
        parser.pop_batch().expect("the rollover batch")
    }

    #[test]
    fn forced_rollover_follows_the_undecimated_clock() {
        // 10 Hz sampled, decimated by 3: the device switches at undecimated
        // sample 20 (t = 2 s), which follows the 7th output sample.
        let batch = rollover_batch(10, 3, 7);
        assert_eq!(batch.segment().start_time, 2);
        assert!(matches!(
            batch.boundary().map(Boundary::class),
            Some(BoundaryClass::Seamless)
        ));
    }

    #[test]
    fn forced_rollover_is_unchanged_when_decimation_divides_the_rate() {
        // 10 Hz sampled, decimated by 2: the switch at undecimated sample 10
        // (t = 1 s) is also the 5th output sample.
        let batch = rollover_batch(10, 2, 5);
        assert_eq!(batch.segment().start_time, 1);
        assert!(matches!(
            batch.boundary().map(Boundary::class),
            Some(BoundaryClass::Seamless)
        ));
    }

    #[test]
    fn a_rollover_continues_the_current_run() {
        let batch = rollover_batch(10, 2, 5);
        assert_eq!(
            batch.generations(),
            Generations {
                stream: 1,
                device: 0,
                global: 0
            }
        );
    }

    #[test]
    fn startup_opens_a_run_without_disturbing_the_shared_generations() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4);
        let mut generations = Vec::new();
        // Contiguous samples, then a gap the timeline confirms as data loss.
        for first_sample_n in [0, 1, 10] {
            parser
                .push_packet(&stream_data_packet(first_sample_n, vec![0; 4]))
                .expect("valid rows");
            generations.push(parser.pop_batch().expect("a batch is ready").generations());
        }
        assert_eq!(
            generations,
            [
                Generations {
                    stream: 1,
                    device: 0,
                    global: 0
                },
                Generations {
                    stream: 1,
                    device: 0,
                    global: 0
                },
                Generations {
                    stream: 2,
                    device: 1,
                    global: 1
                },
            ]
        );
    }

    #[test]
    fn generations_are_constant_within_an_accumulated_batch() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4).with_batch_rows(4);
        for first_sample_n in [0, 2, 4] {
            parser
                .push_packet(&stream_data_packet(first_sample_n, vec![0; 8]))
                .expect("valid rows");
        }
        // The boundary ends a batch, so the last two packets are the ones that
        // accumulate together.
        parser.pop_batch().expect("the boundary batch");
        let batch = parser.pop_batch().expect("two packets share one batch");
        assert_eq!(batch.len(), 4);
        assert_eq!(
            batch.generations(),
            Generations {
                stream: 1,
                device: 0,
                global: 0
            }
        );
    }

    #[test]
    fn pending_batches_never_straddle_a_shared_generation_bump() {
        let mut parser = parser_with_streams(2).with_batch_rows(4);
        parser
            .push_packet(&stream_data_packet_for(2, 0, 2))
            .expect("valid rows");
        assert_eq!(parser.drain_batches().len(), 1);
        parser
            .push_packet(&stream_data_packet_for(2, 2, 2))
            .expect("valid rows");
        assert!(parser.pop_batch().is_none(), "stream 2 holds half a batch");

        // Stream 1's startup boundary leaves the shared generations alone, so
        // stream 2 keeps accumulating.
        parser
            .push_packet(&stream_data_packet_for(1, 0, 2))
            .expect("valid rows");
        let initial = parser.pop_batch().expect("the initial batch");
        assert_eq!(initial.stream().stream_id, 1);
        assert!(parser.pop_batch().is_none(), "stream 2 still accumulates");

        // A gap on stream 1 bumps the shared generations: stream 2's held rows
        // emit first with their older stamps, then the boundary batch.
        parser
            .push_packet(&stream_data_packet_for(1, 10, 2))
            .expect("valid rows");
        let held = parser.pop_batch().expect("the flushed pre-bump batch");
        assert_eq!(held.stream().stream_id, 2);
        assert_eq!(
            held.generations(),
            Generations {
                stream: 1,
                device: 0,
                global: 0
            }
        );
        let boundary_batch = parser.pop_batch().expect("the boundary batch");
        assert_eq!(boundary_batch.stream().stream_id, 1);
        assert_eq!(
            boundary_batch.generations(),
            Generations {
                stream: 2,
                device: 1,
                global: 1
            }
        );
        assert!(parser.finish().is_empty());
    }

    #[test]
    fn missing_metadata_is_a_state_not_an_error() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        assert_eq!(
            parser
                .push_packet(&stream_data_packet(0, vec![0; 4]))
                .unwrap(),
            PacketOutcome::WaitingForMetadata
        );
        assert!(parser.pop_batch().is_none());
        assert_eq!(parser.take_requests().len(), 1);
    }
}

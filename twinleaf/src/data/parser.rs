//! Incremental conversion from routed TIO packets to columnar sample batches.
//!
//! [`PacketParser`] applies packets in arrival order. Shared metadata and
//! continuity state validates each stream payload, after which this module
//! decodes its rows and accumulates them into [`SampleBatch`] values.
//!
//! Stream data can arrive before its metadata. Such packets produce no rows;
//! [`PacketParser::take_requests`] returns the metadata RPCs needed to decode
//! subsequent packets.

use super::coalesce::BatchCoalescer;
use super::sample::SampleBatch;
use super::state::{DeviceMetadataSnapshot, PacketError, PacketEvent, ParseState, ValidatedRows};
use crate::tio::{self, proto};
use proto::identifiers::StreamKey;
use proto::DeviceRoute;
use std::collections::{HashMap, VecDeque};

fn drain_completed(coalescer: &mut BatchCoalescer, ready: &mut VecDeque<SampleBatch>) {
    while let Some(batch) = coalescer.next_completed_batch() {
        ready.push_back(batch);
    }
}

fn flush_pending_batches(
    pending: &mut HashMap<StreamKey, BatchCoalescer>,
    ready: &mut VecDeque<SampleBatch>,
) {
    let mut keys: Vec<_> = pending.keys().copied().collect();
    keys.sort_unstable();
    for key in keys {
        let coalescer = pending.get_mut(&key).expect("key came from the map");
        coalescer.finish_buffered_batch();
        drain_completed(coalescer, ready);
    }
}

/// Decode validated rows into their stream's coalescer and collect whatever it
/// completes.
fn queue_rows(
    input: ValidatedRows<'_>,
    target_rows: Option<usize>,
    pending: &mut HashMap<StreamKey, BatchCoalescer>,
    ready: &mut VecDeque<SampleBatch>,
) -> usize {
    let rows = input.row_count();
    let key = input.stream_key();
    let global_generation = input.generations().global;
    // A stream's own continuity is the coalescer's business; the parser only
    // enforces the rule spanning streams: a pending batch must never straddle a
    // bump of the shared generations, so everything from the older generation emits
    // first, preserving arrival order at the bump.
    if pending.values().any(|c| {
        c.buffered_generations()
            .is_some_and(|e| e.global != global_generation)
    }) {
        flush_pending_batches(pending, ready);
    }

    let coalescer = pending
        .entry(key)
        .or_insert_with(|| BatchCoalescer::new(target_rows));
    coalescer.push(&input);
    drain_completed(coalescer, ready);

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
    pending_batches: HashMap<StreamKey, BatchCoalescer>,
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
            PacketEvent::Rows(rows) => PacketOutcome::Rows(queue_rows(
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
    use crate::data::{
        Boundary, BoundaryClass, BoundaryReason, Buffer, ColumnArray, Generations, StreamDataError,
    };
    use bytes::Bytes;
    use proto::identifiers::{ColumnKey, StreamKey, MAX_SAMPLE_NUMBER};
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
        announce_schema(
            &mut parser,
            column_types,
            sample_size,
            n_segments,
            sampling_rate,
            decimation,
        );
        parser
    }

    /// Push one device's metadata records into an existing parser, as the
    /// device re-announces them after a reconnect.
    fn announce_schema(
        parser: &mut PacketParser,
        column_types: &[DataType],
        sample_size: usize,
        n_segments: usize,
        sampling_rate: u32,
        decimation: u32,
    ) {
        announce_schema_for(
            parser,
            DeviceMetadata {
                serial_number: "SN123".to_string(),
                firmware_hash: "fw".to_string(),
                n_streams: 1,
                session_id: 42,
                name: "test-device".to_string(),
            },
            column_types,
            sample_size,
            n_segments,
            sampling_rate,
            decimation,
        );
    }

    fn announce_schema_for(
        parser: &mut PacketParser,
        device: DeviceMetadata,
        column_types: &[DataType],
        sample_size: usize,
        n_segments: usize,
        sampling_rate: u32,
        decimation: u32,
    ) {
        for content in [
            MetadataContent::Device(device),
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
        match (batch.schema()[0].values(), batch.schema()[1].values()) {
            (ColumnArray::F64(floats), ColumnArray::I64(ints)) => {
                assert_eq!(&floats[..], [1.0, 2.0]);
                assert_eq!(&ints[..], [-3, -4]);
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
    fn a_reconnect_cannot_restamp_the_generations_it_used_before() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the first batch").generations();

        parser
            .push_packet(&tio::Packet {
                payload: Payload::ProxyStatus(proto::ProxyStatusPayload(
                    proto::ProxyStatus::SensorDisconnected,
                )),
                routing: DeviceRoute::root(),
                ttl: 0,
            })
            .expect("a disconnect resets the parser");

        // The device comes back and re-announces exactly the same schema.
        announce_schema(&mut parser, &[DataType::Float32], 4, 1, 1, 1);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let after = parser.pop_batch().expect("the first batch after reconnect");

        assert_eq!(
            before,
            Generations {
                stream: 1,
                device: 0,
                global: 0
            }
        );
        assert_eq!(
            after.generations(),
            Generations {
                stream: 2,
                device: 1,
                global: 1
            }
        );
        assert!(after.is_initial(), "the reconnect opens a new run");
    }

    #[test]
    fn a_replacement_device_cannot_join_the_previous_devices_buffer_run() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4);
        let mut buffer = Buffer::new(128);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the first device's batch");
        let before_generations = before.generations();
        buffer.process_batch(&before);

        announce_schema_for(
            &mut parser,
            DeviceMetadata {
                serial_number: "SN456".to_string(),
                firmware_hash: "replacement-fw".to_string(),
                n_streams: 1,
                session_id: 42,
                name: "replacement-device".to_string(),
            },
            &[DataType::Float32],
            4,
            1,
            1,
            1,
        );
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid replacement rows");
        let after = parser.pop_batch().expect("the replacement device's batch");

        assert_eq!(
            after.generations(),
            Generations {
                stream: before_generations.stream + 1,
                device: before_generations.device + 1,
                global: before_generations.global + 1,
            }
        );
        assert!(after.is_initial(), "a replacement device opens a new run");
        buffer.process_batch(&after);

        let key = StreamKey::new(DeviceRoute::root(), STREAM_ID);
        let run = buffer.get_run(&key).expect("the replacement run");
        assert_eq!(run.retained_rows(), 0..1);
        assert_eq!(
            buffer
                .latest_row(&key)
                .expect("the replacement row")
                .device()
                .serial_number,
            "SN456"
        );
    }

    #[test]
    fn firmware_change_without_a_session_change_opens_a_new_run() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the old firmware's batch");

        announce_schema_for(
            &mut parser,
            DeviceMetadata {
                serial_number: "SN123".to_string(),
                firmware_hash: "new-fw".to_string(),
                n_streams: 1,
                session_id: 42,
                name: "test-device".to_string(),
            },
            &[DataType::Float32],
            4,
            1,
            1,
            1,
        );
        parser
            .push_packet(&stream_data_packet(1, vec![0; 4]))
            .expect("valid rows under the new firmware");
        let after = parser.pop_batch().expect("the new firmware's batch");

        assert_eq!(after.generations().stream, before.generations().stream + 1);
        assert_eq!(after.generations().device, before.generations().device + 1);
        assert_eq!(after.generations().global, before.generations().global + 1);
        assert!(after.is_initial(), "the firmware schema starts a new run");
    }

    #[test]
    fn session_change_keeps_its_specific_boundary_and_opens_a_new_run() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the old session's batch");

        announce_schema_for(
            &mut parser,
            DeviceMetadata {
                serial_number: "SN123".to_string(),
                firmware_hash: "fw".to_string(),
                n_streams: 1,
                session_id: 43,
                name: "test-device".to_string(),
            },
            &[DataType::Float32],
            4,
            1,
            1,
            1,
        );
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows in the new session");
        let after = parser.pop_batch().expect("the new session's batch");

        assert_eq!(after.generations().stream, before.generations().stream + 1);
        assert_eq!(after.generations().device, before.generations().device + 1);
        assert_eq!(after.generations().global, before.generations().global + 1);
        assert!(matches!(
            after.boundary().map(|boundary| &boundary.reason),
            Some(BoundaryReason::SessionChanged { old: 42, new: 43 })
        ));
    }

    #[test]
    fn changed_schema_cannot_reuse_the_previous_run_identity() {
        let mut parser = parser_with_schema(&[DataType::Float32], 4);
        let mut buffer = Buffer::new(128);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the old schema's batch");
        let before_generations = before.generations();
        buffer.process_batch(&before);

        // Contradicting an already-used column invalidates the route. The
        // device then re-announces the complete replacement schema.
        parser
            .push_packet(&metadata_packet(MetadataContent::Column(ColumnMetadata {
                stream_id: STREAM_ID,
                index: 0,
                data_type: DataType::Float64,
                name: "wide".to_string(),
                units: String::new(),
                description: String::new(),
            })))
            .expect("the changed column triggers rediscovery");
        announce_schema(&mut parser, &[DataType::Float64], 8, 1, 1, 1);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 8]))
            .expect("valid rows under the new schema");
        let after = parser.pop_batch().expect("the new schema's batch");

        assert_eq!(after.generations().stream, before_generations.stream + 1);
        assert_eq!(after.generations().device, before_generations.device + 1);
        assert_eq!(after.generations().global, before_generations.global + 1);
        buffer.process_batch(&after);
        let key = StreamKey::new(DeviceRoute::root(), STREAM_ID);
        let run = buffer.get_run(&key).expect("the new schema's run");
        assert_eq!(run.retained_rows(), 0..1);
        assert_eq!(run.stream().sample_size, 8);
        assert_eq!(
            buffer
                .column_metadata(&ColumnKey::new(DeviceRoute::root(), STREAM_ID, 0))
                .expect("the new column")
                .data_type,
            DataType::Float64
        );
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

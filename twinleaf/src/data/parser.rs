//! Incremental conversion from routed TIO packets to columnar sample batches.
//!
//! [`PacketParser`] applies packets in arrival order. Shared metadata and
//! continuity state validates each stream payload, after which this module
//! decodes its rows and accumulates them into [`SampleBatch`] values.

use super::coalesce::BatchCoalescer;
use super::metadata::{DeviceMetadataSnapshot, MetadataQuery};
use super::sample::{SampleBatch, StreamKey};
use super::state::{PacketError, PacketEvent, ParseState, ScannedRows};
use crate::proto::DeviceRoute;
use crate::tio;
use std::collections::{HashMap, VecDeque};

/// Incremental, route-aware parser for TIO packets.
///
/// The caller owns packet I/O and supplies packets in arrival order. By
/// default each decodable stream-data packet emits one [`SampleBatch`].
/// [`PacketParser::with_batch_rows`] instead accumulates compatible packets for
/// bulk consumers. Boundaries and schema changes always end the current batch.
///
/// Stream data can arrive before the metadata describing it. Such packets
/// produce no rows; [`PacketParser::take_metadata_queries`] returns the
/// metadata the caller must fetch before subsequent packets decode.
pub struct PacketParser {
    state: ParseState,
    batcher: Batcher,
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

/// A parser's decoding state at one point in a packet sequence.
///
/// Taken with [`PacketParser::checkpoint`] and resumed with
/// [`PacketParser::replay_from`], so a long log can be re-read from the middle
/// without replaying everything before it.
#[derive(Clone)]
pub(crate) struct ParserCheckpoint(ParseState);

impl PacketParser {
    fn from_state(state: ParseState) -> Self {
        Self {
            state,
            batcher: Batcher::new(),
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
        self.batcher.target_rows = Some(rows);
        self
    }

    /// Ingest one port-relative packet and advance the decoding state.
    ///
    /// Missing metadata is reported as [`PacketOutcome::WaitingForMetadata`];
    /// call [`PacketParser::take_metadata_queries`] to learn what to fetch.
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
            PacketEvent::Rows(rows) => PacketOutcome::Rows(self.batcher.queue(rows)),
        })
    }

    /// Ingest one packet, validating it without decoding sample values.
    ///
    /// The counterpart to [`push_packet`](Self::push_packet) for a scanning
    /// pass that only summarizes a log; no batch is ever produced.
    pub(crate) fn scan_packet<'a>(
        &'a mut self,
        packet: &'a tio::Packet,
    ) -> Result<Option<ScannedRows<'a>>, PacketError> {
        Ok(match self.state.apply_packet(packet)? {
            PacketEvent::Rows(rows) => Some(rows),
            PacketEvent::Applied | PacketEvent::WaitingForMetadata | PacketEvent::Reset => None,
        })
    }

    /// Capture the decoding state reached so far.
    pub(crate) fn checkpoint(&self) -> ParserCheckpoint {
        ParserCheckpoint(self.state.clone())
    }

    /// Start a parser from a captured state, as if it had read every packet
    /// that preceded the checkpoint.
    pub(crate) fn replay_from(checkpoint: &ParserCheckpoint) -> Self {
        Self::from_state(checkpoint.0.clone())
    }

    /// Pop the oldest completed batch currently buffered.
    pub fn pop_batch(&mut self) -> Option<SampleBatch> {
        self.batcher.ready.pop_front()
    }

    /// End every partially accumulated stream batch without resetting metadata.
    pub fn flush(&mut self) {
        self.batcher.flush();
    }

    /// Drain batches that are already ready without flushing partial batches.
    pub fn drain_batches(&mut self) -> Vec<SampleBatch> {
        self.batcher.ready.drain(..).collect()
    }

    /// Flush accumulated samples and return every completed batch.
    pub fn finish(mut self) -> Vec<SampleBatch> {
        self.flush();
        self.drain_batches()
    }

    /// Forget device metadata. Any decoded rows already accumulated remain
    /// available through `pop_batch`.
    pub fn reset(&mut self) {
        self.reset_subtree(DeviceRoute::root());
    }

    /// Forget device metadata at and below the absolute route `subtree`. Every
    /// partial batch ends — the generation bump would flush them on the next
    /// packet anyway — but only the subtree's routes rediscover.
    pub fn reset_subtree(&mut self, subtree: DeviceRoute) {
        self.flush();
        self.state.reset_subtree(subtree);
    }

    /// Metadata still missing, as one query per route that lacks any. The
    /// caller answers each with [`apply_metadata_reply`](Self::apply_metadata_reply)
    /// or [`fail_metadata_query`](Self::fail_metadata_query).
    pub fn take_metadata_queries(&mut self) -> Vec<MetadataQuery> {
        self.state.take_metadata_queries()
    }

    /// Ensure a route has parser state and take its pending metadata query.
    pub fn take_metadata_queries_for(&mut self, route: DeviceRoute) -> Vec<MetadataQuery> {
        self.state.take_metadata_queries_for(route)
    }

    /// Apply a `dev.metadata` reply. A reply carrying fewer records than the
    /// query selected leaves the rest missing, for the next query to ask for.
    pub fn apply_metadata_reply(&mut self, query: MetadataQuery, reply: &[u8]) {
        self.state.apply_metadata_reply(query, reply)
    }

    /// Give up on a query, arming discovery for its route again. False if the
    /// query was already stale, having been overtaken by a reset or a new
    /// session.
    pub fn fail_metadata_query(&mut self, query: MetadataQuery) -> bool {
        self.state.fail_metadata_query(query)
    }

    /// Return complete metadata for an absolute route once it is available.
    pub fn metadata(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        self.state.metadata(route)
    }

    /// The route's current metadata revision, without building a snapshot.
    pub(crate) fn metadata_revision(&self, route: DeviceRoute) -> Option<u32> {
        self.state.metadata_revision(route)
    }

    /// Absolute routes for which this parser has observed state.
    pub fn routes(&self) -> Vec<DeviceRoute> {
        self.state.routes()
    }
}

/// The batching half of the parser, deliberately outside [`ParserCheckpoint`]:
/// a replay starts accumulating fresh.
struct Batcher {
    target_rows: Option<usize>,
    pending: HashMap<StreamKey, BatchCoalescer>,
    ready: VecDeque<SampleBatch>,
}

impl Batcher {
    fn new() -> Self {
        Self {
            target_rows: None,
            pending: HashMap::new(),
            ready: VecDeque::new(),
        }
    }

    /// Decode validated rows into their stream's coalescer and collect whatever
    /// it completes.
    fn queue(&mut self, input: ScannedRows<'_>) -> usize {
        let rows = input.row_count();
        let key = input.stream_key();
        let global_generation = input.generations().global;
        // A stream's own continuity is the coalescer's business; the parser only
        // enforces the rule spanning streams: a pending batch must never straddle a
        // bump of the shared generations, so everything from the older generation emits
        // first, preserving arrival order at the bump.
        if self.pending.values().any(|c| {
            c.buffered_generations()
                .is_some_and(|e| e.global != global_generation)
        }) {
            self.flush();
        }

        let target_rows = self.target_rows;
        self.pending
            .entry(key)
            .or_insert_with(|| BatchCoalescer::new(target_rows))
            .push(&input);
        self.drain(key);

        rows
    }

    /// End every partially accumulated stream batch, in key order.
    fn flush(&mut self) {
        let mut keys: Vec<_> = self.pending.keys().copied().collect();
        keys.sort_unstable();
        for key in keys {
            self.pending
                .get_mut(&key)
                .expect("key came from the map")
                .finish_buffered_batch();
            self.drain(key);
        }
    }

    fn drain(&mut self, key: StreamKey) {
        let Some(coalescer) = self.pending.get_mut(&key) else {
            return;
        };
        while let Some(batch) = coalescer.next_completed_batch() {
            self.ready.push_back(batch);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::buffer::Buffer;
    use crate::data::fixtures;
    use crate::data::sample::{BoundaryClass, BoundaryReason, ColumnArray, ColumnKey, Generations};
    use crate::data::state::StreamDataError;
    use crate::proto::data as wire;
    use crate::proto::data::{DataType, MAX_SAMPLE_NUMBER};
    use crate::tio::packet::ProxyStatus;

    const STREAM_ID: u8 = 1;

    fn metadata_packet(record: wire::Metadata<'_>) -> tio::Packet {
        tio::Packet::metadata(record, wire::MetadataFlags::default(), DeviceRoute::root())
            .expect("a valid metadata record")
    }

    fn announce(parser: &mut PacketParser, record: wire::Metadata<'_>) {
        parser
            .push_packet(&metadata_packet(record))
            .expect("valid metadata");
    }

    /// Announce a device with one stream whose columns have `column_types`, then
    /// return a parser holding that metadata.
    fn parser_with_schema(column_types: &[DataType], sample_size: u16) -> PacketParser {
        parser_with_clock(column_types, sample_size, 1, 1, 1)
    }

    /// As [`parser_with_schema`], with an explicit segment ring and clock. Only
    /// segment 0 is announced, so later segments must be synthesized.
    fn parser_with_clock(
        column_types: &[DataType],
        sample_size: u16,
        n_segments: u8,
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
        sample_size: u16,
        n_segments: u8,
        sampling_rate: u32,
        decimation: u32,
    ) {
        announce_schema_for(
            parser,
            fixtures::device(),
            column_types,
            sample_size,
            n_segments,
            sampling_rate,
            decimation,
        );
    }

    fn announce_schema_for(
        parser: &mut PacketParser,
        device: wire::Device<'_>,
        column_types: &[DataType],
        sample_size: u16,
        n_segments: u8,
        sampling_rate: u32,
        decimation: u32,
    ) {
        announce(parser, wire::Metadata::Device(device));
        announce(
            parser,
            wire::Metadata::Stream(wire::Stream {
                n_columns: column_types.len() as u8,
                n_segments,
                sample_size,
                ..fixtures::stream(STREAM_ID)
            }),
        );
        announce(
            parser,
            wire::Metadata::Segment(wire::Segment {
                sampling_rate,
                decimation,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        for (index, data_type) in column_types.iter().enumerate() {
            announce(
                parser,
                wire::Metadata::Column(wire::Column {
                    name: &format!("col_{index}"),
                    ..fixtures::column(STREAM_ID, index as u8, *data_type)
                }),
            );
        }
    }

    /// Announce a device with `n_streams` single-`Float32`-column streams,
    /// numbered from 1.
    fn parser_with_streams(n_streams: u8) -> PacketParser {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        announce(
            &mut parser,
            wire::Metadata::Device(wire::Device {
                n_streams,
                ..fixtures::device()
            }),
        );
        for stream_id in 1..=n_streams {
            announce(
                &mut parser,
                wire::Metadata::Stream(wire::Stream {
                    name: &format!("stream-{stream_id}"),
                    ..fixtures::stream(stream_id)
                }),
            );
            announce(
                &mut parser,
                wire::Metadata::Segment(fixtures::segment(stream_id)),
            );
            announce(
                &mut parser,
                wire::Metadata::Column(fixtures::column(stream_id, 0, DataType::F32)),
            );
        }
        parser
    }

    fn stream_data_packet_for(stream_id: u8, first_sample_n: u32, rows: usize) -> tio::Packet {
        tio::Packet::samples(
            stream_id,
            0,
            first_sample_n,
            &vec![0; 4 * rows],
            DeviceRoute::root(),
        )
        .expect("valid samples")
    }

    fn stream_data_packet_in_segment(
        segment_id: u8,
        first_sample_n: u32,
        data: Vec<u8>,
    ) -> tio::Packet {
        tio::Packet::samples(
            STREAM_ID,
            segment_id,
            first_sample_n,
            &data,
            DeviceRoute::root(),
        )
        .expect("valid samples")
    }

    fn stream_data_packet(first_sample_n: u32, data: Vec<u8>) -> tio::Packet {
        stream_data_packet_in_segment(0, first_sample_n, data)
    }

    /// A data packet can switch the current segment without any metadata
    /// record changing; the snapshot revision must move with it.
    #[test]
    fn a_data_packet_switching_segments_revises_the_metadata() {
        let route = DeviceRoute::root();
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 1, 1);
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                ..fixtures::segment(STREAM_ID)
            }),
        );
        let before = parser.metadata_revision(route);

        parser
            .push_packet(&stream_data_packet_in_segment(0, 0, vec![0; 4]))
            .expect("valid samples");

        assert_ne!(parser.metadata_revision(route), before);
        let snapshot = parser.metadata(route).expect("complete metadata");
        let stream = snapshot
            .stream(crate::proto::StreamId::new(STREAM_ID))
            .expect("the described stream");
        assert_eq!(stream.segment().segment_id.value(), 0);
    }

    /// One `dev.metadata` reply carrying `records`, in wire framing.
    fn metadata_reply(records: &[wire::Metadata<'_>]) -> Vec<u8> {
        let mut reply = vec![0u8; wire::MAX_METADATA_REPLY_SIZE];
        let mut written = 0;
        for record in records {
            written += record
                .write_reply_frame(&mut reply[written..])
                .expect("the fixtures fit one reply");
        }
        reply.truncate(written);
        reply
    }

    /// Take the query for the root route, asserting there is exactly one.
    fn take_query(parser: &mut PacketParser) -> MetadataQuery {
        let mut queries = parser.take_metadata_queries_for(DeviceRoute::root());
        assert_eq!(
            queries.len(),
            1,
            "one query per route with anything missing"
        );
        queries.pop().expect("the query")
    }

    #[test]
    fn a_query_stays_outstanding_until_it_completes() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        let query = take_query(&mut parser);
        assert!(
            query.selectors.is_empty(),
            "an unknown device bootstraps with a device-chosen prefix"
        );
        assert!(parser
            .take_metadata_queries_for(DeviceRoute::root())
            .is_empty());

        parser.fail_metadata_query(query);
        assert_eq!(
            take_query(&mut parser).selectors,
            Vec::new(),
            "a failure arms discovery again"
        );
    }

    #[test]
    fn a_completed_query_cannot_complete_twice() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        let query = take_query(&mut parser);

        parser.apply_metadata_reply(
            query.clone(),
            &metadata_reply(&[wire::Metadata::Device(fixtures::device())]),
        );
        let next = take_query(&mut parser);
        assert_eq!(next.selectors, [wire::MetadataSelector::stream(STREAM_ID)]);

        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[wire::Metadata::Device(fixtures::device())]),
        );
        assert!(
            parser
                .take_metadata_queries_for(DeviceRoute::root())
                .is_empty(),
            "a stale completion does not free the outstanding query's slot"
        );
        parser.fail_metadata_query(next);
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::stream(STREAM_ID)],
            "only the live query re-arms discovery"
        );
    }

    #[test]
    fn a_reply_from_before_a_reset_cannot_apply_after_it() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        let query = take_query(&mut parser);
        parser.reset();

        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[wire::Metadata::Device(fixtures::device())]),
        );
        assert!(
            parser.metadata(DeviceRoute::root()).is_none(),
            "the stale reply's records were not applied"
        );
        assert!(
            take_query(&mut parser).selectors.is_empty(),
            "the route is still bootstrapping"
        );
    }

    #[test]
    fn a_reply_from_before_a_session_change_cannot_apply_after_it() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        let bootstrap = take_query(&mut parser);
        parser.apply_metadata_reply(
            bootstrap,
            &metadata_reply(&[wire::Metadata::Device(fixtures::device())]),
        );
        let query = take_query(&mut parser);
        assert_eq!(query.selectors, [wire::MetadataSelector::stream(STREAM_ID)]);

        announce(
            &mut parser,
            wire::Metadata::Device(wire::Device {
                session: crate::proto::SessionId::new(43),
                ..fixtures::device()
            }),
        );
        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[
                wire::Metadata::Stream(fixtures::stream(STREAM_ID)),
                wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
                wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
            ]),
        );
        assert!(parser.metadata(DeviceRoute::root()).is_none());
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::stream(STREAM_ID)],
            "the new session starts its stream metadata over"
        );
    }

    #[test]
    fn a_capacity_limited_reply_leaves_the_rest_to_ask_for() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        let query = take_query(&mut parser);

        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[
                wire::Metadata::Device(fixtures::device()),
                wire::Metadata::Stream(fixtures::stream(STREAM_ID)),
                wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
            ]),
        );

        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::column(STREAM_ID, 0)],
            "the record the reply stopped short of is still missing"
        );
    }

    #[test]
    fn a_query_carries_at_most_one_requests_worth_of_selectors() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        let query = take_query(&mut parser);
        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[wire::Metadata::Device(wire::Device {
                n_streams: 40,
                ..fixtures::device()
            })]),
        );

        let query = take_query(&mut parser);
        assert_eq!(query.selectors.len(), wire::MAX_METADATA_SELECTORS);
        assert_eq!(
            query.args().len(),
            wire::MAX_METADATA_SELECTORS * wire::MetadataSelector::SIZE
        );
    }

    #[test]
    fn a_malformed_reply_arms_discovery_again() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        let query = take_query(&mut parser);

        parser.apply_metadata_reply(query, &[wire::MetadataType::Device.into(), 200]);
        assert!(parser.metadata(DeviceRoute::root()).is_none());
        assert!(take_query(&mut parser).selectors.is_empty());
    }

    #[test]
    fn unknown_column_type_is_absent_from_the_batch() {
        // A newer firmware reports a type this build predates, between two
        // columns it understands.
        let unknown = DataType::new(0x35);
        assert_eq!(unknown.size(), 3);
        let mut parser = parser_with_schema(&[DataType::F32, unknown, DataType::I16], 9);

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
            .map(|series| series.metadata().name)
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
        let mut parser = parser_with_schema(&[DataType::F32], 4);
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
        let mut parser = parser_with_schema(&[DataType::F32], 4);

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
        let mut parser = parser_with_schema(&[DataType::new(0x35)], 3);
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
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, sampling_rate, decimation);
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
            batch.boundary().map(BoundaryReason::class),
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
            batch.boundary().map(BoundaryReason::class),
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
        let mut parser = parser_with_schema(&[DataType::F32], 4);
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
        let mut parser = parser_with_schema(&[DataType::F32], 4).with_batch_rows(4);
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
        assert_eq!(initial.stream().stream_id, crate::proto::StreamId::new(1));
        assert!(parser.pop_batch().is_none(), "stream 2 still accumulates");

        // A gap on stream 1 bumps the shared generations: stream 2's held rows
        // emit first with their older stamps, then the boundary batch.
        parser
            .push_packet(&stream_data_packet_for(1, 10, 2))
            .expect("valid rows");
        let held = parser.pop_batch().expect("the flushed pre-bump batch");
        assert_eq!(held.stream().stream_id, crate::proto::StreamId::new(2));
        assert_eq!(
            held.generations(),
            Generations {
                stream: 1,
                device: 0,
                global: 0
            }
        );
        let boundary_batch = parser.pop_batch().expect("the boundary batch");
        assert_eq!(
            boundary_batch.stream().stream_id,
            crate::proto::StreamId::new(1)
        );
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
        let mut parser = parser_with_schema(&[DataType::F32], 4);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the first batch").generations();

        parser
            .push_packet(&tio::Packet::proxy_status(ProxyStatus::SensorDisconnected))
            .expect("a disconnect resets the parser");

        // The device comes back and re-announces exactly the same schema.
        announce_schema(&mut parser, &[DataType::F32], 4, 1, 1, 1);
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

    /// A routed disconnect — one mount bouncing behind `tio proxy --mount`, as
    /// a recorded log replays it — resets only the subtree at its route.
    #[test]
    fn a_routed_disconnect_resets_only_its_subtree() {
        let mut parser = PacketParser::new(DeviceRoute::root(), true);
        let steady: DeviceRoute = "/1".parse().unwrap();
        let bounced: DeviceRoute = "/2".parse().unwrap();
        for route in [steady, bounced] {
            for record in [
                wire::Metadata::Device(fixtures::device()),
                wire::Metadata::Stream(fixtures::stream(STREAM_ID)),
                wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
                wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
            ] {
                parser
                    .push_packet(&metadata_packet(record).with_route(route))
                    .expect("valid metadata");
            }
        }

        parser
            .push_packet(
                &tio::Packet::proxy_status(ProxyStatus::SensorDisconnected).with_route(bounced),
            )
            .expect("a routed disconnect applies");

        assert!(
            parser.metadata(steady).is_some(),
            "the steady mount kept its description"
        );
        assert!(
            parser.metadata(bounced).is_none(),
            "the bounced mount rediscovers"
        );
    }

    #[test]
    fn a_replacement_device_cannot_join_the_previous_devices_buffer_run() {
        let mut parser = parser_with_schema(&[DataType::F32], 4);
        let mut buffer = Buffer::new(128);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the first device's batch");
        let before_generations = before.generations();
        buffer.process_batch(&before);

        announce_schema_for(
            &mut parser,
            wire::Device {
                name: "replacement-device",
                serial: "SN456",
                firmware: "replacement-fw",
                ..fixtures::device()
            },
            &[DataType::F32],
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

        let key = StreamKey::new(DeviceRoute::root(), crate::proto::StreamId::new(STREAM_ID));
        let run = buffer.get_run(&key).expect("the replacement run");
        assert_eq!(run.retained_rows(), 0..1);
        assert_eq!(
            buffer
                .latest_row(&key)
                .expect("the replacement row")
                .device()
                .serial,
            "SN456"
        );
    }

    #[test]
    fn firmware_change_without_a_session_change_opens_a_new_run() {
        let mut parser = parser_with_schema(&[DataType::F32], 4);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the old firmware's batch");

        announce_schema_for(
            &mut parser,
            wire::Device {
                firmware: "new-fw",
                ..fixtures::device()
            },
            &[DataType::F32],
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
        let mut parser = parser_with_schema(&[DataType::F32], 4);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the old session's batch");

        announce_schema_for(
            &mut parser,
            wire::Device {
                session: crate::proto::SessionId::new(43),
                ..fixtures::device()
            },
            &[DataType::F32],
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
            after.boundary(),
            Some(BoundaryReason::SessionChanged { old, new })
                if old.value() == 42 && new.value() == 43
        ));
    }

    #[test]
    fn changed_schema_cannot_reuse_the_previous_run_identity() {
        let mut parser = parser_with_schema(&[DataType::F32], 4);
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
            .push_packet(&metadata_packet(wire::Metadata::Column(wire::Column {
                name: "wide",
                ..fixtures::column(STREAM_ID, 0, DataType::F64)
            })))
            .expect("the changed column triggers rediscovery");
        announce_schema(&mut parser, &[DataType::F64], 8, 1, 1, 1);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 8]))
            .expect("valid rows under the new schema");
        let after = parser.pop_batch().expect("the new schema's batch");

        assert_eq!(after.generations().stream, before_generations.stream + 1);
        assert_eq!(after.generations().device, before_generations.device + 1);
        assert_eq!(after.generations().global, before_generations.global + 1);
        buffer.process_batch(&after);
        let key = StreamKey::new(DeviceRoute::root(), crate::proto::StreamId::new(STREAM_ID));
        let run = buffer.get_run(&key).expect("the new schema's run");
        assert_eq!(run.retained_rows(), 0..1);
        assert_eq!(run.stream().sample_size, 8);
        assert_eq!(
            buffer
                .column_metadata(&ColumnKey::new(
                    DeviceRoute::root(),
                    crate::proto::StreamId::new(STREAM_ID),
                    crate::proto::ColumnId::new(0),
                ))
                .expect("the new column")
                .data_type,
            DataType::F64
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
        assert_eq!(parser.take_metadata_queries().len(), 1);
    }
}

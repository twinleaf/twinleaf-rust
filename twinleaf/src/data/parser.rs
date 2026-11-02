//! Incremental conversion from routed TIO packets to columnar sample batches.
//!
//! [`PacketParser`] applies packets in arrival order. Shared metadata and
//! continuity state validates each stream payload, after which this module
//! decodes its rows and accumulates them into [`SampleBatch`] values.

use super::coalesce::BatchCoalescer;
use super::metadata::{DeviceMetadataSnapshot, MetadataQuery, StreamKey};
use super::sample::{RowSource, SampleBatch};
use super::state::{DataError, PacketEvent, ParseState, ScannedRows};
use crate::proto::DeviceRoute;
use crate::rand::HashMap;
use crate::tio;
use std::collections::VecDeque;

/// Incremental, route-aware parser for TIO packets.
///
/// The caller owns packet I/O and supplies packets in arrival order. By
/// default each decodable stream-data packet emits one [`SampleBatch`].
/// [`PacketParser::with_batch_rows`] accumulates compatible packets into
/// larger batches for bulk consumers. Boundaries and schema changes always end
/// the current batch.
///
/// Stream data can arrive before the metadata describing it. Such packets
/// produce no rows, and [`PacketParser::take_metadata_queries_for`] returns
/// the metadata the caller must fetch before later packets decode.
///
/// The parser decodes a segment's rows only with that segment's record,
/// which the device sends before the segment's first sample. Rows that
/// arrive without it are lost, and the first batch of the stream once the
/// record is known reports them as a [`Loss`](super::Loss), unless a change
/// other than a rollover stands for them. A record the parser holds counts
/// only if the device announced its segment since the stream's last rows,
/// by a broadcast, a proxy metadata update or a bootstrap reply.
///
/// A device announces `dev.running` turning off as it hands a stop or
/// restart a host asked for to its platform, and otherwise at a refresh
/// after it stops acquiring, so ahead of the record of any segment a later
/// start begins. Rows of a segment the device announced after such a stop
/// come as a [`Change::Restarted`](super::Change::Restarted), and rows of
/// the stopped acquisition that arrive after the stop come as before. A
/// new segment that starts before the last rows end comes as a backward
/// step in time.
///
/// Three kinds of restart reach the parser with no stop announced, and come as a
/// rollover with a loss of the sample periods between the acquisitions.
///
/// - A restart whose announcement was lost, in a gap on the link or in a
///   client's inlet.
/// - A stop and start no host asked for, with no refresh between them,
///   which leave `dev.running` where it was.
/// - Every restart of deprecated firmware, which has no `dev.running`, as
///   [`RpcRegistry::is_deprecated_firmware`](crate::device::rpc::RpcRegistry::is_deprecated_firmware)
///   tells.
///
/// # Examples
///
/// Decode a recorded log packet by packet.
///
/// ```
/// use twinleaf::data::{LogFile, PacketParser};
/// # use std::io::Write;
/// # use twinleaf::proto::data::{self as wire, DataType};
/// # use twinleaf::proto::sync::Epoch;
/// # use twinleaf::tio::Packet;
/// # use twinleaf::{ColumnId, DeviceRoute, SegmentId, SessionId, StreamId};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let (id, route) = (StreamId::new(1), DeviceRoute::root());
/// # let mut file = tempfile::NamedTempFile::new()?;
/// # for record in [
/// #     wire::Metadata::Device(wire::Device { session: SessionId::new(1), n_streams: 1, name: "sensor", serial: "S1", firmware: "1" }),
/// #     wire::Metadata::Stream(wire::Stream { stream_id: id, n_columns: 1, n_segments: 1, sample_size: 4, buf_samples: 128, name: "field" }),
/// #     wire::Metadata::Segment(wire::Segment { stream_id: id, segment_id: SegmentId::new(0), flags: wire::SegmentFlags::default(), epoch: Epoch::UNIX, timeref_serial: "", timeref_session: SessionId::new(1), start_time: 0, sampling_rate: 10, decimation: 1, filter_cutoff: 0.0, filter_type: wire::FilterType::NONE }),
/// #     wire::Metadata::Column(wire::Column { stream_id: id, index: ColumnId::new(0), data_type: DataType::F32, name: "x", units: "nT", description: "" }),
/// # ] {
/// #     file.write_all(Packet::metadata(record, wire::MetadataFlags::UPDATE, route)?.as_bytes())?;
/// # }
/// # let data: Vec<u8> = [1.0f32, 2.0, 3.0].iter().flat_map(|x| x.to_le_bytes()).collect();
/// # file.write_all(Packet::samples(1, 0, 0, &data, route)?.as_bytes())?;
/// # let path = file.path();
/// let mut parser = PacketParser::new(false);
/// let mut rows = 0;
/// for packet in LogFile::open(path)?.packets() {
///     parser.push_packet(&packet?)?;
///     while let Some(batch) = parser.pop_batch() {
///         println!("{}: {} rows", batch.stream_key(), batch.len());
///         rows += batch.len();
///     }
/// }
/// for batch in parser.finish() {
///     rows += batch.len();
/// }
/// assert_eq!(rows, 3);
/// # Ok(())
/// # }
/// ```
pub struct PacketParser {
    state: ParseState,
    batcher: Batcher,
}

impl std::fmt::Debug for PacketParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketParser")
            .field("routes", &self.routes())
            .field("batch_rows", &self.batcher.target_rows)
            .field("ready", &self.batcher.ready.len())
            .finish_non_exhaustive()
    }
}

/// State transition produced by pushing one packet into a [`PacketParser`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketOutcome {
    /// A control packet was applied without producing sample rows.
    Applied,
    /// Stream data is valid so far but cannot be decoded until metadata arrives.
    WaitingForMetadata,
    /// The stream has a column of a type this build cannot decode, so its
    /// rows are refused.
    Unsupported,
    /// A disconnect dropped the metadata of the devices behind it, which the
    /// parser learns again. Their streams keep their last rows, so rows
    /// after the disconnect report what was lost, or a new session, against
    /// them. A stream starts over as
    /// [`Change::Initial`](crate::data::Change::Initial) says, as when
    /// another device answers, and rows in another segment are told as
    /// [`Change::Restarted`](crate::data::Change::Restarted) says.
    Reset,
    /// The packet contained this many validated sample rows.
    Rows(usize),
}

impl PacketOutcome {
    /// Returns the number of sample rows the packet carried, or zero for any
    /// other outcome.
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
/// [`PacketParser::replay_from`] to decode a log from the middle.
#[derive(Clone)]
pub(crate) struct ParserCheckpoint(ParseState);

impl PacketParser {
    fn from_state(state: ParseState) -> Self {
        Self {
            state,
            batcher: Batcher::new(),
        }
    }

    /// Creates a parser for packets whose routes are absolute.
    ///
    /// If `ignore_session` is `false`, a heartbeat that reports a new session
    /// discards the device's metadata and starts discovery again.
    pub fn new(ignore_session: bool) -> Self {
        Self::from_state(ParseState::new(ignore_session))
    }

    /// Accumulates about `rows` decoded samples per stream before emitting a
    /// batch. Boundaries and schema changes always end a batch.
    ///
    /// # Panics
    ///
    /// Panics if `rows` is zero.
    pub fn with_batch_rows(mut self, rows: usize) -> Self {
        assert!(rows > 0, "batch row target must be nonzero");
        self.batcher.target_rows = Some(rows);
        self
    }

    /// Applies one port-relative packet and advances the decoding state.
    ///
    /// Missing metadata is reported as [`PacketOutcome::WaitingForMetadata`],
    /// and [`PacketParser::take_metadata_queries_for`] says what to fetch.
    /// Completed batches are available through [`PacketParser::pop_batch`].
    ///
    /// # Errors
    ///
    /// Returns a [`DataError`] if stream data contradicts the metadata
    /// retained for its stream. The packet's rows are dropped, and the
    /// metadata stays for later packets.
    pub fn push_packet(&mut self, packet: &tio::Packet) -> Result<PacketOutcome, DataError> {
        Ok(match self.state.apply_packet(packet)? {
            PacketEvent::Applied => PacketOutcome::Applied,
            PacketEvent::WaitingForMetadata => PacketOutcome::WaitingForMetadata,
            PacketEvent::Unsupported => PacketOutcome::Unsupported,
            PacketEvent::Reset => {
                self.flush();
                PacketOutcome::Reset
            }
            PacketEvent::Rows(rows) => PacketOutcome::Rows(self.batcher.queue(rows)),
        })
    }

    /// Applies one packet and validates its rows without decoding sample
    /// values.
    ///
    /// The scanning counterpart of [`push_packet`](Self::push_packet). It
    /// produces no batches and returns the same errors.
    pub(crate) fn scan_packet<'a>(
        &'a mut self,
        packet: &'a tio::Packet,
    ) -> Result<Option<ScannedRows<'a>>, DataError> {
        Ok(match self.state.apply_packet(packet)? {
            PacketEvent::Rows(rows) => Some(rows),
            PacketEvent::Applied
            | PacketEvent::WaitingForMetadata
            | PacketEvent::Unsupported
            | PacketEvent::Reset => None,
        })
    }

    /// Captures the decoding state reached so far.
    pub(crate) fn checkpoint(&self) -> ParserCheckpoint {
        ParserCheckpoint(self.state.clone())
    }

    /// Creates a parser from a captured state, as if it had read every packet
    /// before the checkpoint.
    pub(crate) fn replay_from(checkpoint: &ParserCheckpoint) -> Self {
        Self::from_state(checkpoint.0.clone())
    }

    /// Removes and returns the oldest completed batch.
    pub fn pop_batch(&mut self) -> Option<SampleBatch> {
        self.batcher.ready.pop_front()
    }

    /// Ends every partially accumulated batch without resetting metadata.
    /// [`pop_batch`](Self::pop_batch) then returns them.
    pub fn flush(&mut self) {
        self.batcher.flush();
    }

    /// Flushes accumulated samples and returns every completed batch.
    pub fn finish(mut self) -> Vec<SampleBatch> {
        self.flush();
        self.batcher.ready.into()
    }

    /// Forgets device metadata at and below the absolute route `subtree`.
    /// Every partial batch ends, and only the subtree's routes discover their
    /// metadata again.
    pub fn reset_subtree(&mut self, subtree: DeviceRoute) {
        self.flush();
        self.state.reset(|route| route.starts_with(&subtree));
    }

    /// Drops the device metadata of the absolute route `route` alone to learn
    /// it again, as a disconnect does for its subtree. Each stream keeps its
    /// last rows, so the rows after report what was lost, or a new session,
    /// against them, as [`PacketOutcome::Reset`] says.
    pub(crate) fn relearn_route(&mut self, route: DeviceRoute) {
        self.flush();
        self.state.relearn(|device| *device == route);
    }

    /// Takes the pending metadata query for `route`, creating the route's
    /// state if it has none. The list is empty while a query is outstanding
    /// or when nothing is missing. The caller answers each query with
    /// [`apply_metadata_reply`](Self::apply_metadata_reply) or
    /// [`fail_metadata_query`](Self::fail_metadata_query).
    pub fn take_metadata_queries_for(&mut self, route: DeviceRoute) -> Vec<MetadataQuery> {
        self.state.take_metadata_queries_for(route)
    }

    /// Applies the `dev.metadata` reply to `query`. Records the reply leaves
    /// out stay missing, and the next query asks for them. A reply to a stale
    /// query is ignored.
    pub fn apply_metadata_reply(&mut self, query: MetadataQuery, reply: &[u8]) {
        self.state.apply_metadata_reply(query, reply)
    }

    /// Gives up on `query` and arms discovery for its route again. Returns
    /// `false` if a reset or a new session had already made the query stale.
    pub fn fail_metadata_query(&mut self, query: MetadataQuery) -> bool {
        self.state.fail_metadata_query(query)
    }

    /// Returns the complete metadata for an absolute route, or `None` until
    /// all of it has arrived.
    pub fn metadata(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        self.state.metadata(route)
    }

    /// Returns the route's current metadata revision without building a
    /// snapshot.
    pub(crate) fn metadata_revision(&self, route: DeviceRoute) -> Option<u32> {
        self.state.metadata_revision(route)
    }

    /// Returns the route's complete metadata and its revision, or `None` if
    /// the revision is still `seen` or the metadata is incomplete.
    pub(crate) fn revised_metadata(
        &self,
        route: DeviceRoute,
        seen: Option<u32>,
    ) -> Option<(u32, DeviceMetadataSnapshot)> {
        let revision = self.metadata_revision(route)?;
        if seen == Some(revision) {
            return None;
        }
        Some((revision, self.metadata(route)?))
    }

    /// Returns the absolute routes this parser holds state for.
    pub fn routes(&self) -> Vec<DeviceRoute> {
        self.state.routes()
    }
}

/// The batching half of the parser. It is not part of a [`ParserCheckpoint`],
/// and a replayed parser starts with no partial batches.
struct Batcher {
    target_rows: Option<usize>,
    pending: HashMap<StreamKey, BatchCoalescer>,
    ready: VecDeque<SampleBatch>,
}

impl Batcher {
    fn new() -> Self {
        Self {
            target_rows: None,
            pending: HashMap::default(),
            ready: VecDeque::new(),
        }
    }

    /// Decodes validated rows into their stream's coalescer and collects the
    /// batches it completes. Returns the number of rows.
    fn queue(&mut self, input: ScannedRows<'_>) -> usize {
        let rows = input.len();
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

    /// Ends every partially accumulated batch, in stream key order.
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
    use crate::data::continuity::{Boundary, Change, Generations, Loss};
    use crate::data::fixtures;
    use crate::data::sample::ColumnData;
    use crate::data::state::StreamDataError;
    use crate::proto::data as wire;
    use crate::proto::data::{DataType, MAX_SAMPLE_NUMBER};
    use crate::proto::SampleNumber;
    use crate::tio::packet::LinkStatus;

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
    /// segment 0 is announced.
    fn parser_with_clock(
        column_types: &[DataType],
        sample_size: u16,
        n_segments: u8,
        sampling_rate: u32,
        decimation: u32,
    ) -> PacketParser {
        let mut parser = PacketParser::new(true);
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
        let mut parser = PacketParser::new(true);
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

    /// A data packet in a segment the device did not announce switches the
    /// current segment without any metadata record arriving, so the snapshot
    /// revision moves, and the snapshot waits for that segment's record.
    #[test]
    fn a_data_packet_switching_segments_revises_the_metadata() {
        let route = DeviceRoute::root();
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 1, 1);
        parser
            .push_packet(&stream_data_packet_in_segment(0, 0, vec![0; 4]))
            .expect("valid samples");
        assert!(parser.metadata(route).is_some());
        let before = parser.metadata_revision(route);

        parser
            .push_packet(&stream_data_packet_in_segment(1, 0, vec![0; 4]))
            .expect("valid samples");

        assert_ne!(parser.metadata_revision(route), before);
        assert!(parser.metadata(route).is_none(), "segment 1 has no record");
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 1)]
        );
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

    /// A bootstrap reply holds the device's current segments, so it
    /// announces them, and rows in a segment other than 0 decode at once.
    #[test]
    fn a_bootstrap_reply_announces_the_current_segment() {
        let mut parser = PacketParser::new(true);
        let query = take_query(&mut parser);
        assert!(query.selectors.is_empty(), "a bootstrap query");
        let segment = wire::Segment {
            segment_id: crate::proto::SegmentId::new(2),
            ..fixtures::segment(STREAM_ID)
        };
        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[
                wire::Metadata::Device(fixtures::device()),
                wire::Metadata::Stream(wire::Stream {
                    n_segments: 4,
                    sample_size: 4,
                    ..fixtures::stream(STREAM_ID)
                }),
                wire::Metadata::Segment(segment),
                wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
            ]),
        );

        let batches: Vec<SampleBatch> = [5, 6]
            .map(|first| {
                parser
                    .push_packet(&stream_data_packet_in_segment(2, first, vec![0; 4]))
                    .expect("valid rows");
                parser.pop_batch().expect("rows that decode at once")
            })
            .into();
        assert!(batches[0].is_initial());
        assert_eq!(batches[1].boundary(), None, "nothing lost");
        assert!(parser
            .take_metadata_queries_for(DeviceRoute::root())
            .is_empty());
    }

    #[test]
    fn a_query_stays_outstanding_until_it_completes() {
        let mut parser = PacketParser::new(true);
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
        let mut parser = PacketParser::new(true);
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
        let mut parser = PacketParser::new(true);
        let query = take_query(&mut parser);
        parser.reset_subtree(DeviceRoute::root());

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
        let mut parser = PacketParser::new(true);
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
        let mut parser = PacketParser::new(true);
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
        let mut parser = PacketParser::new(true);
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
        let mut parser = PacketParser::new(true);
        let query = take_query(&mut parser);

        parser.apply_metadata_reply(query, &[wire::MetadataType::Device.into(), 200]);
        assert!(parser.metadata(DeviceRoute::root()).is_none());
        assert!(take_query(&mut parser).selectors.is_empty());
    }

    #[test]
    fn a_stream_with_a_column_type_this_build_cannot_decode_is_refused() {
        // A 24-bit column, or a type from newer firmware, between two columns
        // this build decodes.
        for unknown in [DataType::I24, DataType::new(0x35)] {
            assert_eq!(unknown.size(), 3);
            let mut parser = parser_with_schema(&[DataType::F32, unknown, DataType::I16], 9);
            assert_eq!(
                parser
                    .push_packet(&stream_data_packet(0, vec![0; 18]))
                    .unwrap(),
                PacketOutcome::Unsupported
            );
            assert!(parser.pop_batch().is_none());
        }
    }

    #[test]
    fn segment_id_outside_the_ring_is_rejected() {
        let mut parser = parser_with_schema(&[DataType::F32], 4);
        // The stream advertises a single segment, so any other id is corrupt and
        // must not become the state the next packet is validated against.
        assert!(matches!(
            parser.push_packet(&stream_data_packet_in_segment(255, 0, vec![0; 4])),
            Err(DataError::Stream {
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
            Err(DataError::Stream {
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

    /// Rows of a segment whose record the parser lacks wait for it, and the
    /// parser asks for that record.
    #[test]
    fn rows_without_their_record_wait_for_it() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 10, 2);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4 * 5]))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");
        assert_eq!(
            parser
                .push_packet(&stream_data_packet_in_segment(1, 0, vec![0; 4]))
                .expect("valid rows"),
            PacketOutcome::WaitingForMetadata
        );
        assert!(parser.pop_batch().is_none());
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 1)]
        );
    }

    /// Rows numbered back from the last rows, in their segment, with no
    /// record of it since, are of a new acquisition in that slot, as after
    /// a reboot of a child behind a hub. They wait for the slot's record,
    /// and rows after it decode with it, not with the record before.
    #[test]
    fn rows_numbered_back_in_their_segment_wait_for_its_new_record() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 1, 1);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4 * 5]))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");

        for first in [2, 4] {
            assert_eq!(
                parser
                    .push_packet(&stream_data_packet(first, vec![0; 4]))
                    .expect("valid rows"),
                PacketOutcome::WaitingForMetadata,
                "the rows at {first}"
            );
        }
        assert!(parser.pop_batch().is_none());
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 0)]
        );

        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                start_time: 100,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        parser
            .push_packet(&stream_data_packet(5, vec![0; 4]))
            .expect("valid rows");
        let after = parser.pop_batch().expect("the new acquisition's rows");
        assert_eq!(
            after.timestamps(),
            [106.0],
            "the end of sample 5 from 100 s"
        );
    }

    /// A stream's first rows in a segment the device did not announce wait
    /// for that segment's record, as the record the parser holds is left
    /// from an earlier lap of the ring.
    #[test]
    fn a_streams_first_rows_after_a_lap_wait_for_their_record() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
        let segment = |id: u8, start_time: u32| {
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(id),
                start_time,
                ..fixtures::segment(STREAM_ID)
            })
        };
        // Every row of a lap of the ring is lost, and the device is back in
        // segment 0, 4 s after it first opened it.
        for id in 1..4 {
            announce(&mut parser, segment(id, u32::from(id)));
        }
        assert_eq!(
            parser
                .push_packet(&stream_data_packet(0, vec![0; 4]))
                .expect("valid rows"),
            PacketOutcome::WaitingForMetadata
        );
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 0)]
        );
        announce(&mut parser, segment(0, 4));
        parser
            .push_packet(&stream_data_packet(1, vec![0; 4]))
            .expect("valid rows");

        let batch = parser.pop_batch().expect("the stream's first batch");
        assert!(batch.is_initial());
        assert_eq!(batch.timestamps(), [6.0], "row 1 ends 2 s into the segment");
    }

    /// A rollover whose record comes first continues the current run.
    #[test]
    fn a_rollover_continues_the_current_run() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 10, 2);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4 * 5]))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                start_time: 1,
                sampling_rate: 10,
                decimation: 2,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        parser
            .push_packet(&stream_data_packet_in_segment(1, 0, vec![0; 4]))
            .expect("valid rows");
        let batch = parser.pop_batch().expect("the rollover batch");
        assert_eq!(
            batch.boundary(),
            Some(&Boundary::of(Change::SegmentRollover {
                old_id: crate::proto::SegmentId::new(0),
                new_id: crate::proto::SegmentId::new(1),
            }))
        );
        assert_eq!(
            batch.generations(),
            Generations {
                stream: 1,
                device: 0,
                global: 0
            }
        );
    }

    /// The boundary of `outputs` rows of segment 0, at `sampling_rate`
    /// decimated by `decimation`, then row `first` of segment `next`, which
    /// starts `seconds` after segment 0.
    fn boundary_across(
        (sampling_rate, decimation): (u32, u32),
        outputs: usize,
        (next, seconds, first): (u8, u32, u32),
    ) -> Option<Boundary> {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 4, sampling_rate, decimation);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4 * outputs]))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(next),
                start_time: seconds,
                sampling_rate,
                decimation,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        parser
            .push_packet(&stream_data_packet_in_segment(next, first, vec![0; 4]))
            .expect("valid rows");
        parser
            .pop_batch()
            .expect("the batch after")
            .boundary()
            .cloned()
    }

    fn rollover(old: u8, new: u8) -> Option<Change> {
        Some(Change::SegmentRollover {
            old_id: crate::proto::SegmentId::new(old),
            new_id: crate::proto::SegmentId::new(new),
        })
    }

    /// A ring that came round to the last rows' slot holds a segment that
    /// starts later, which is a rollover onto the same id.
    #[test]
    fn a_ring_that_came_round_rolls_over_onto_the_same_slot() {
        // 4 segments of one row at 1 Hz, all after the first lost.
        assert_eq!(
            boundary_across((1, 1), 1, (0, 4, 0)),
            Some(Boundary {
                change: rollover(0, 0),
                loss: Some(Loss {
                    samples: 3,
                    expected: Some(SampleNumber::new(1)),
                    dropped_batches: 0,
                }),
            })
        );
    }

    /// A device announces only the segment it is publishing, so the parser
    /// takes rows of a segment after the record of another one for rows of a
    /// later lap of the ring. It drops the segment's record, waits for the
    /// record again, and reports the rows after it as a rollover onto the
    /// same slot, with the rows it waited through lost.
    #[test]
    fn rows_after_a_broadcast_of_another_segment_wait_for_their_record_and_roll_over() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 1, 1);
        parser
            .push_packet(&stream_data_packet(0, vec![0; 4]))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                start_time: 1,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        parser
            .push_packet(&stream_data_packet(1, vec![0; 4]))
            .expect("valid rows");
        assert!(
            parser.pop_batch().is_none(),
            "the rows wait for their record"
        );
        let query = take_query(&mut parser);
        assert_eq!(
            query.selectors,
            vec![wire::MetadataSelector::segment(STREAM_ID, 0)]
        );
        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[wire::Metadata::Segment(fixtures::segment(STREAM_ID))]),
        );
        parser
            .push_packet(&stream_data_packet(2, vec![0; 4]))
            .expect("valid rows");

        let batch = parser.pop_batch().expect("the rows after the reply");
        assert_eq!(
            batch.boundary().cloned(),
            Some(Boundary {
                change: rollover(0, 0),
                loss: Some(Loss {
                    samples: 1,
                    expected: Some(SampleNumber::new(1)),
                    dropped_batches: 0,
                }),
            })
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
        assert_eq!(std::iter::from_fn(|| parser.pop_batch()).count(), 1);
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
        assert_eq!(
            initial.schema().stream().stream_id(),
            crate::proto::StreamId::new(1)
        );
        assert!(parser.pop_batch().is_none(), "stream 2 still accumulates");

        // A gap on stream 1 bumps the shared generations: stream 2's held rows
        // emit first with their older stamps, then the boundary batch.
        parser
            .push_packet(&stream_data_packet_for(1, 10, 2))
            .expect("valid rows");
        let held = parser.pop_batch().expect("the flushed pre-bump batch");
        assert_eq!(
            held.schema().stream().stream_id(),
            crate::proto::StreamId::new(2)
        );
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
            boundary_batch.schema().stream().stream_id(),
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
            .push_packet(&tio::Packet::legacy_status(LinkStatus::SensorDisconnected))
            .expect("the disconnect");

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
        assert!(
            matches!(
                after
                    .boundary()
                    .and_then(|boundary| boundary.change.as_ref()),
                Some(Change::TimeBackward { .. })
            ),
            "rows that go back in the same session step back in time: {:?}",
            after.boundary()
        );
    }

    /// A routed disconnect, one mount bouncing behind `tio proxy --mount` as a
    /// recorded log replays it, has only the subtree at its route learn its
    /// metadata again. A log from a current proxy holds it as a link packet,
    /// and one from an old proxy as a legacy status.
    #[test]
    fn a_routed_disconnect_relearns_only_its_subtree() {
        let forms: [fn(LinkStatus) -> tio::Packet; 2] =
            [tio::Packet::link, tio::Packet::legacy_status];
        for form in forms {
            let mut parser = PacketParser::new(true);
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

            let marker = form(LinkStatus::SensorDisconnected).with_route(bounced);
            parser
                .push_packet(&marker)
                .expect("a routed disconnect applies");

            assert!(
                parser.metadata(steady).is_some(),
                "{:?}: the steady mount kept its description",
                marker.ptype()
            );
            assert!(
                parser.metadata(bounced).is_none(),
                "{:?}: the bounced mount rediscovers",
                marker.ptype()
            );
        }
    }

    /// A routed stop has its subtree learn its metadata again exactly as a
    /// routed disconnect does, and leaves every other subtree alone.
    #[test]
    fn a_routed_stop_relearns_only_its_subtree() {
        let mut parser = PacketParser::new(true);
        let steady: DeviceRoute = "/1".parse().unwrap();
        let stopped: DeviceRoute = "/2".parse().unwrap();
        for route in [steady, stopped] {
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
            .push_packet(&tio::Packet::link(LinkStatus::Stopped).with_route(stopped))
            .expect("a routed stop applies");

        assert!(
            parser.metadata(steady).is_some(),
            "the steady mount kept its description"
        );
        assert!(
            parser.metadata(stopped).is_none(),
            "the stopped mount learns its description again"
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
        let run = buffer.run(&key).expect("the replacement run");
        assert_eq!(run.retained_rows(), 0..1);
        assert_eq!(
            buffer
                .latest_row(&key)
                .expect("the replacement row")
                .metadata()
                .device()
                .serial(),
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
            after.boundary().and_then(|b| b.change.as_ref()),
            Some(Change::SessionChanged { old, new })
                if old.value() == 42 && new.value() == 43
        ));
    }

    #[test]
    fn reboot_from_a_nonzero_segment_does_not_rediscover_metadata_per_sample() {
        let route = DeviceRoute::root();
        let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                start_time: 1000,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        parser
            .push_packet(&stream_data_packet_in_segment(1, 0, vec![0; 4]))
            .unwrap();
        let before = parser.pop_batch().unwrap();
        announce_schema_for(
            &mut parser,
            wire::Device {
                session: crate::proto::SessionId::new(43),
                ..fixtures::device()
            },
            &[DataType::F32],
            4,
            4,
            1,
            1,
        );
        let revision = parser.metadata_revision(route);
        for first in 0..10 {
            parser
                .push_packet(&stream_data_packet(first, vec![0; 4]))
                .unwrap();
            let batch = parser
                .pop_batch()
                .expect("rebooted samples must decode immediately");
            if first == 0 {
                assert!(matches!(
                    batch.boundary().and_then(|b| b.change.as_ref()),
                    Some(Change::SessionChanged { .. })
                ));
                assert_eq!(batch.generations().stream, before.generations().stream + 1);
            }
            assert!(parser
                .take_metadata_queries_for(DeviceRoute::root())
                .is_empty());
            assert_eq!(parser.metadata_revision(route), revision);
        }
    }

    #[test]
    fn a_broadcast_selects_a_nonzero_segment_without_samples() {
        let route = DeviceRoute::root();
        let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                ..fixtures::segment(STREAM_ID)
            }),
        );
        assert_eq!(
            parser
                .metadata(route)
                .unwrap()
                .stream(crate::proto::StreamId::new(STREAM_ID))
                .unwrap()
                .segment()
                .segment_id()
                .value(),
            1
        );
    }

    #[test]
    fn a_stale_ring_entry_in_the_same_clock_is_still_evicted() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                ..fixtures::segment(STREAM_ID)
            }),
        );
        // Segment 0 is announced again, so its rows decode, and segment 1's
        // record is left from before.
        announce(
            &mut parser,
            wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
        );
        parser
            .push_packet(&stream_data_packet(1000, vec![0; 4]))
            .unwrap();
        parser.pop_batch().unwrap();
        parser
            .push_packet(&stream_data_packet_in_segment(1, 1, vec![0; 4]))
            .unwrap();
        assert!(parser.pop_batch().is_none());
        let query = take_query(&mut parser);
        assert_eq!(
            query.selectors,
            vec![wire::MetadataSelector::segment(STREAM_ID, 1)]
        );
        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                start_time: 1000,
                ..fixtures::segment(STREAM_ID)
            })]),
        );
        parser
            .push_packet(&stream_data_packet_in_segment(1, 1, vec![0; 4]))
            .unwrap();
        assert!(parser.pop_batch().is_some());
    }

    #[test]
    fn a_metadata_reply_does_not_override_the_broadcast_current_segment() {
        let route = DeviceRoute::root();
        let mut parser = PacketParser::new(false);
        let query = take_query(&mut parser);
        announce_schema(&mut parser, &[DataType::F32], 4, 4, 1, 1);
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                ..fixtures::segment(STREAM_ID)
            }),
        );
        parser.apply_metadata_reply(
            query,
            &metadata_reply(&[wire::Metadata::Segment(fixtures::segment(STREAM_ID))]),
        );
        assert_eq!(
            parser
                .metadata(route)
                .unwrap()
                .stream(crate::proto::StreamId::new(STREAM_ID))
                .unwrap()
                .segment()
                .segment_id()
                .value(),
            1
        );
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
        let run = buffer.run(&key).expect("the new schema's run");
        assert_eq!(run.retained_rows(), 0..1);
        let schema = run.metadata().schema();
        assert_eq!(schema.stream().sample_size(), 8);
        assert_eq!(
            schema
                .column(crate::proto::ColumnId::new(0))
                .expect("the new column")
                .data_type(),
            DataType::F64
        );
    }

    #[test]
    fn missing_metadata_is_a_state_not_an_error() {
        let mut parser = PacketParser::new(true);
        assert_eq!(
            parser
                .push_packet(&stream_data_packet(0, vec![0; 4]))
                .unwrap(),
            PacketOutcome::WaitingForMetadata
        );
        assert!(parser.pop_batch().is_none());
        assert_eq!(
            parser.take_metadata_queries_for(DeviceRoute::root()).len(),
            1
        );
    }

    /// Each column of a batch as its decoded values, by column name.
    type Decoded = Vec<(String, Vec<f64>)>;

    fn decoded(batch: &SampleBatch) -> Decoded {
        batch
            .schema()
            .columns()
            .zip(batch.columns())
            .map(|(column, values)| (column.name().to_string(), values.to_f64().into_owned()))
            .collect()
    }

    fn named(columns: &[(&str, &[f64])]) -> Decoded {
        columns
            .iter()
            .map(|(name, values)| (name.to_string(), values.to_vec()))
            .collect()
    }

    /// Once a stream's metadata is known, decoding a packet into a batch and
    /// reading the batch's metadata parse no retained record. A record is
    /// checked when it arrives and read in place after that. Every batch
    /// shares the one schema of its run, so no batch allocates metadata.
    #[test]
    fn a_steady_packet_parses_no_metadata_record() {
        let types = [DataType::F32, DataType::I16, DataType::U8];
        let mut parser = parser_with_schema(&types, 7);
        let rows = 4;
        parser
            .push_packet(&stream_data_packet(0, vec![0; 7 * rows]))
            .expect("valid rows");
        let first = parser.pop_batch().expect("the first batch");

        let before = crate::data::metadata::record_parses();
        for packet in 1..=10 {
            let first_sample = packet * rows as u32;
            parser
                .push_packet(&stream_data_packet(first_sample, vec![0; 7 * rows]))
                .expect("valid rows");
            let batch = parser.pop_batch().expect("a batch per packet");
            assert_eq!(batch.schema().columns().len(), types.len());
            assert!(std::sync::Arc::ptr_eq(batch.schema(), first.schema()));
        }
        assert_eq!(crate::data::metadata::record_parses() - before, 0);
    }

    /// One `f32` and one `i16` column, six bytes a sample.
    fn first_layout_sample(value: f32, count: i16) -> Vec<u8> {
        let mut sample = value.to_le_bytes().to_vec();
        sample.extend_from_slice(&count.to_le_bytes());
        sample
    }

    /// A reboot can bring firmware whose stream has another layout. The rows
    /// after it decode at the new offsets and types, not the old ones.
    #[test]
    fn rows_after_a_reboot_decode_with_the_new_layout() {
        let mut parser = parser_with_schema(&[DataType::F32, DataType::I16], 6);
        parser
            .push_packet(&stream_data_packet(0, first_layout_sample(1.5, -7)))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the first session's batch");
        assert_eq!(
            decoded(&before),
            named(&[("col_0", &[1.5]), ("col_1", &[-7.0])])
        );

        announce_schema_for(
            &mut parser,
            wire::Device {
                session: crate::proto::SessionId::new(43),
                ..fixtures::device()
            },
            &[DataType::I16, DataType::F64, DataType::U8],
            11,
            1,
            1,
            1,
        );
        let mut sample = (-2i16).to_le_bytes().to_vec();
        sample.extend_from_slice(&2.25f64.to_le_bytes());
        sample.push(200);
        parser
            .push_packet(&stream_data_packet(0, sample))
            .expect("valid rows in the new session");
        let after = parser.pop_batch().expect("the new session's batch");

        assert!(matches!(
            after.boundary().and_then(|b| b.change.as_ref()),
            Some(Change::SessionChanged { .. })
        ));
        assert_eq!(
            decoded(&after),
            named(&[("col_0", &[-2.0]), ("col_1", &[2.25]), ("col_2", &[200.0])])
        );
    }

    /// A device lost and found again may be another build of it. Its layout
    /// is learned anew, even with the same session, and its rows start the
    /// stream over, a break its peers share.
    #[test]
    fn rows_after_a_disconnect_decode_with_the_reannounced_layout() {
        let mut parser = parser_with_schema(&[DataType::F32, DataType::I16], 6);
        parser
            .push_packet(&stream_data_packet(0, first_layout_sample(1.5, -7)))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the first batch").generations();

        parser
            .push_packet(
                &tio::Packet::link(LinkStatus::SensorDisconnected).with_route(DeviceRoute::root()),
            )
            .expect("the disconnect");
        announce_schema(&mut parser, &[DataType::U16, DataType::F32], 6, 1, 1, 1);
        let mut sample = 513u16.to_le_bytes().to_vec();
        sample.extend_from_slice(&(-0.5f32).to_le_bytes());
        parser
            .push_packet(&stream_data_packet(0, sample))
            .expect("valid rows under the new layout");
        let after = parser.pop_batch().expect("the batch after the reconnect");

        assert!(after.is_initial());
        assert_eq!(after.generations().device, before.device + 1);
        assert_eq!(
            decoded(&after),
            named(&[("col_0", &[513.0]), ("col_1", &[-0.5])])
        );
    }

    /// A column record that contradicts a learned one resets the route, and
    /// the replacement is laid out from scratch.
    #[test]
    fn a_contradicted_column_is_laid_out_again() {
        let mut parser = parser_with_schema(&[DataType::F32, DataType::F32], 8);
        let mut sample = 1.0f32.to_le_bytes().to_vec();
        sample.extend_from_slice(&2.0f32.to_le_bytes());
        parser
            .push_packet(&stream_data_packet(0, sample))
            .expect("valid rows");
        parser.pop_batch().expect("the old layout's batch");

        parser
            .push_packet(&metadata_packet(wire::Metadata::Column(wire::Column {
                name: "col_0",
                ..fixtures::column(STREAM_ID, 0, DataType::F64)
            })))
            .expect("the changed column triggers rediscovery");
        announce_schema(&mut parser, &[DataType::F64, DataType::F32], 12, 1, 1, 1);
        let mut sample = 3.5f64.to_le_bytes().to_vec();
        sample.extend_from_slice(&4.0f32.to_le_bytes());
        parser
            .push_packet(&stream_data_packet(0, sample))
            .expect("valid rows under the new layout");
        let after = parser.pop_batch().expect("the new layout's batch");

        assert_eq!(
            decoded(&after),
            named(&[("col_0", &[3.5]), ("col_1", &[4.0])])
        );
    }

    /// Decimation lives in the segment, not the columns. A new segment with
    /// another rate retimes the rows but leaves their layout alone.
    #[test]
    fn a_decimation_change_retimes_rows_without_relaying_them() {
        let mut parser = parser_with_clock(&[DataType::I32], 4, 2, 1000, 1);
        let samples = |values: &[i32]| -> Vec<u8> {
            values
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect()
        };
        parser
            .push_packet(&stream_data_packet(0, samples(&[10, 11])))
            .expect("valid rows at the full rate");
        let before = parser.pop_batch().expect("the full rate batch");
        assert_eq!(decoded(&before), named(&[("col_0", &[10.0, 11.0])]));

        parser
            .push_packet(&metadata_packet(wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                sampling_rate: 1000,
                decimation: 10,
                ..fixtures::segment(STREAM_ID)
            })))
            .expect("the decimated segment");
        parser
            .push_packet(&stream_data_packet_in_segment(1, 0, samples(&[-20, -21])))
            .expect("valid rows at the decimated rate");
        let after = parser.pop_batch().expect("the decimated batch");

        assert!(matches!(
            after.boundary().and_then(|b| b.change.as_ref()),
            Some(Change::RateChanged { .. })
        ));
        assert_eq!(decoded(&after), named(&[("col_0", &[-20.0, -21.0])]));
        let step = after.timestamps()[1] - after.timestamps()[0];
        assert!(
            (step - 0.01).abs() < 1e-9,
            "rows are 10 ms apart, got {step}"
        );
    }

    /// Setting announcements, such as the `rpc.hash` that `dev.priv` sends,
    /// and heartbeats of the same session change no stream metadata.
    #[test]
    fn announcements_and_heartbeats_keep_the_layout() {
        let mut parser = parser_with_schema(&[DataType::F32, DataType::I16], 6);
        parser
            .push_packet(&stream_data_packet(0, first_layout_sample(1.5, -7)))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");

        let mut raw = [0u8; 64];
        let len = crate::proto::settings::Setting {
            name: b"rpc.hash",
            seq: Some(1),
            reply: &0x1234_5678u32.to_le_bytes(),
        }
        .write(&mut raw)
        .expect("the announcement fits");
        let announcement = tio::Packet::from_slice_prefix(&raw[..len])
            .expect("a valid packet")
            .0
            .with_route(DeviceRoute::root());
        parser
            .push_packet(&announcement)
            .expect("an announcement applies");
        parser
            .push_packet(&tio::Packet::heartbeat_session(
                fixtures::device().session.value(),
                DeviceRoute::root(),
            ))
            .expect("a heartbeat applies");
        parser
            .push_packet(&stream_data_packet(1, first_layout_sample(2.5, 8)))
            .expect("valid rows");
        let after = parser.pop_batch().expect("the next batch");

        assert!(after.boundary().is_none(), "{:?}", after.boundary());
        assert_eq!(
            decoded(&after),
            named(&[("col_0", &[2.5]), ("col_1", &[8.0])])
        );
    }

    /// A log can interleave devices whose streams share an id but not a
    /// layout. Each route decodes with its own.
    #[test]
    fn interleaved_devices_decode_with_their_own_layouts() {
        let mut parser = PacketParser::new(true);
        let narrow: DeviceRoute = "/0".parse().unwrap();
        let wide: DeviceRoute = "/1".parse().unwrap();
        for (route, serial, types, sample_size) in [
            (narrow, "NARROW", [DataType::I8, DataType::U16], 3u16),
            (wide, "WIDE", [DataType::F64, DataType::I32], 12),
        ] {
            let mut records = vec![
                wire::Metadata::Device(wire::Device {
                    serial,
                    ..fixtures::device()
                }),
                wire::Metadata::Stream(wire::Stream {
                    n_columns: 2,
                    sample_size,
                    ..fixtures::stream(STREAM_ID)
                }),
                wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
            ];
            records.extend(types.iter().enumerate().map(|(index, data_type)| {
                wire::Metadata::Column(fixtures::column(STREAM_ID, index as u8, *data_type))
            }));
            for record in records {
                parser
                    .push_packet(&metadata_packet(record).with_route(route))
                    .expect("valid metadata");
            }
        }

        let narrow_sample = |count: i8, level: u16| -> Vec<u8> {
            let mut sample = count.to_le_bytes().to_vec();
            sample.extend_from_slice(&level.to_le_bytes());
            sample
        };
        let wide_sample = |value: f64, count: i32| -> Vec<u8> {
            let mut sample = value.to_le_bytes().to_vec();
            sample.extend_from_slice(&count.to_le_bytes());
            sample
        };
        let packets = [
            (narrow, 0, narrow_sample(-1, 300)),
            (wide, 0, wide_sample(0.25, -100_000)),
            (narrow, 1, narrow_sample(-2, 301)),
            (wide, 1, wide_sample(0.5, -100_001)),
        ];
        let mut values: HashMap<DeviceRoute, Vec<Decoded>> = HashMap::default();
        for (route, first, data) in packets {
            let packet =
                tio::Packet::samples(STREAM_ID, 0, first, &data, route).expect("valid samples");
            parser.push_packet(&packet).expect("valid rows");
            let batch = parser.pop_batch().expect("a batch per packet");
            assert_eq!(batch.stream_key().route, route);
            values.entry(route).or_default().push(decoded(&batch));
        }

        let column =
            |values: [f64; 2]| -> Vec<Vec<f64>> { values.map(|value| vec![value]).to_vec() };
        let by_column = |route: DeviceRoute, index: usize| -> Vec<Vec<f64>> {
            values[&route]
                .iter()
                .map(|batch| batch[index].1.clone())
                .collect()
        };
        assert_eq!(by_column(narrow, 0), column([-1.0, -2.0]));
        assert_eq!(by_column(narrow, 1), column([300.0, 301.0]));
        assert_eq!(by_column(wide, 0), column([0.25, 0.5]));
        assert_eq!(by_column(wide, 1), column([-100_000.0, -100_001.0]));
    }

    /// Whatever its column types and whatever padding ends its samples, a
    /// stream decodes each column at the offset the types before it add up
    /// to, in the column's own type.
    #[test]
    fn every_layout_decodes_each_column_at_its_offset() {
        use proptest::prelude::*;
        use proptest::test_runner::TestRunner;

        let types = prop::sample::select(vec![
            DataType::I8,
            DataType::U8,
            DataType::I16,
            DataType::U16,
            DataType::I32,
            DataType::U32,
            DataType::I64,
            DataType::U64,
            DataType::F32,
            DataType::F64,
        ]);
        let case = (
            prop::collection::vec(types, 1..7),
            0usize..4,
            1usize..5,
            prop::collection::vec(any::<u8>(), 256),
        );
        TestRunner::new(ProptestConfig {
            source_file: Some(file!()),
            ..ProptestConfig::default()
        })
        .run(&case, |(types, padding, rows, bytes)| {
            let sample_size = types
                .iter()
                .map(|data_type| data_type.size())
                .sum::<usize>()
                + padding;
            let mut parser = parser_with_schema(&types, sample_size as u16);
            let data = bytes[..rows * sample_size].to_vec();
            parser
                .push_packet(&stream_data_packet(0, data.clone()))
                .expect("valid rows");
            let batch = parser.pop_batch().expect("a batch");

            let mut expected = Vec::new();
            let mut offset = 0;
            for (index, data_type) in types.iter().enumerate() {
                let cells: Vec<ColumnData> = (0..rows)
                    .map(|row| {
                        ColumnData::from_le_bytes(&data[row * sample_size + offset..], *data_type)
                    })
                    .collect();
                expected.push((format!("col_{index}"), cells));
                offset += data_type.size();
            }

            prop_assert_eq!(batch.columns().len(), expected.len());
            let columns = batch.schema().columns().zip(batch.columns());
            for ((column, values), (name, cells)) in columns.zip(&expected) {
                prop_assert_eq!(column.name(), name.as_str());
                prop_assert_eq!(values.data_type(), column.data_type());
                for (row, cell) in cells.iter().enumerate() {
                    // Floats compare by bits, so a NaN matches itself.
                    let same = match (values.get(row), cell) {
                        (ColumnData::F32(value), ColumnData::F32(cell)) => {
                            value.to_bits() == cell.to_bits()
                        }
                        (ColumnData::F64(value), ColumnData::F64(cell)) => {
                            value.to_bits() == cell.to_bits()
                        }
                        (value, cell) => value == *cell,
                    };
                    prop_assert!(same, "{name} row {row}: {cell:?}");
                }
            }
            Ok(())
        })
        .expect("every layout decodes at its offsets");
    }
}

//! Incremental conversion from routed TIO packets to columnar sample batches.
//!
//! [`PacketParser`] applies packets in arrival order. Shared metadata and
//! continuity state validates each stream payload, after which this module
//! decodes its rows and accumulates them into [`SampleBatch`] values.

use crate::decode::state::{DataError, PacketEvent, ParseState, ScannedRows};
use crate::model::continuity::LossSite;
use crate::model::sample::{RowSource, SampleBatch};
use crate::model::schema::{DeviceMetadataSnapshot, MetadataQuery, Record, StreamKey};
use crate::proto::packet::{Checked, Payload};
use crate::proto::proxy::ProxyRx;
use crate::proto::{DeviceRoute, StreamId};
use crate::store::coalesce::BatchCoalescer;
use std::collections::HashMap;
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
/// record is known reports them as a [`Loss`](crate::Loss), unless a change
/// other than a rollover stands for them. A record the parser holds counts
/// only if the device announced its segment since the stream's last rows,
/// by a broadcast, a proxy metadata update or a bootstrap reply.
///
/// For a device that [marks its starts and
/// stops](crate::proto::data::DeviceView::marks_starts_and_stops), rows of a
/// segment a start opened, or of one after a close, come as a
/// [`Change::Restarted`](crate::Change::Restarted) with no loss, a reboot
/// among them, and a new session changes nothing of a stream's state, as rows
/// of the last session may still come after it. A new segment that starts
/// before the last rows end comes as a backward step in time. A device whose
/// record declares no revision, the deprecated C firmware, marks neither, so
/// its restarts come as a rollover with a loss of the sample periods between
/// the acquisitions, and its reboots as a
/// [`Change::SessionChanged`](crate::Change::SessionChanged).
///
/// # Examples
///
/// Decode a recorded log packet by packet.
///
/// ```
/// use twinleaf_data::{LogFile, PacketParser};
/// # use std::io::Write;
/// # use twinleaf_data::proto::data::{self as wire, DataType};
/// # use twinleaf_data::proto::sync::Epoch;
/// # use twinleaf_data::proto::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let (id, mut buf) = (StreamId::new(1), [0u8; 512]);
/// # let mut file = tempfile::NamedTempFile::new()?;
/// # for record in [
/// #     wire::Metadata::Device(wire::Device { session: SessionId::new(1), n_streams: 1, name: "sensor", serial: "S1", firmware: "1", protocol: 0 }),
/// #     wire::Metadata::Stream(wire::Stream { stream_id: id, n_columns: 1, n_segments: 1, sample_size: 4, buf_samples: 128, name: "field" }),
/// #     wire::Metadata::Segment(wire::Segment { stream_id: id, segment_id: SegmentId::new(0), flags: wire::SegmentFlags::VALID, epoch: Epoch::UNIX, timeref_serial: "", timeref_session: SessionId::new(1), start_time: 0, sampling_rate: 10, decimation: 1, filter_cutoff: 0.0, filter_type: wire::FilterType::NONE }),
/// #     wire::Metadata::Column(wire::Column { stream_id: id, index: ColumnId::new(0), data_type: DataType::F32, name: "x", units: "nT", description: "" }),
/// # ] {
/// #     let len = record.write(wire::MetadataFlags::UPDATE, &mut buf)?;
/// #     file.write_all(&buf[..len])?;
/// # }
/// # let data: Vec<u8> = [1.0f32, 2.0, 3.0].iter().flat_map(|x| x.to_le_bytes()).collect();
/// # let samples = wire::Samples { stream_id: id, segment_id: SegmentId::new(0), first: SampleNumber::new(0), data: &data };
/// # let len = samples.write(&mut buf)?;
/// # file.write_all(&buf[..len])?;
/// # let path = file.path();
/// let log = LogFile::open(path)?;
/// let mut parser = PacketParser::new(false);
/// let mut rows = 0;
/// for packet in log.packets() {
///     let (_offset, packet) = packet?;
///     parser.push_packet(packet)?;
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
    /// [`Change::Initial`](crate::Change::Initial) says, as when
    /// another device answers, and rows in another segment are told as
    /// [`Change::Restarted`](crate::Change::Restarted) says.
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
    pub fn push_packet<'a>(
        &mut self,
        packet: impl Into<Checked<'a>>,
    ) -> Result<PacketOutcome, DataError> {
        let Self { state, batcher } = self;
        Ok(match apply(state, batcher, packet.into())? {
            PacketEvent::Applied => PacketOutcome::Applied,
            PacketEvent::WaitingForMetadata => PacketOutcome::WaitingForMetadata,
            PacketEvent::Unsupported => PacketOutcome::Unsupported,
            PacketEvent::Reset => PacketOutcome::Reset,
            PacketEvent::Rows(rows) => PacketOutcome::Rows(batcher.queue(rows)),
        })
    }

    /// Applies one packet and validates its rows without decoding sample
    /// values.
    ///
    /// The scanning counterpart of [`push_packet`](Self::push_packet). It
    /// produces no batches and returns the same errors.
    pub(crate) fn scan_packet<'a>(
        &'a mut self,
        packet: Checked<'a>,
    ) -> Result<Option<ScannedRows<'a>>, DataError> {
        Ok(match apply(&mut self.state, &mut self.batcher, packet)? {
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

    /// Tells the parser that packets received for it were dropped before
    /// they reached it, as by a receiver whose queue had no room for them.
    /// The rows each stream loses before its next rows then count as
    /// dropped by the receiver, in
    /// [`Loss::dropped_samples`](crate::Loss::dropped_samples).
    pub fn note_dropped_packets(&mut self) {
        self.state.blame(LossSite::Receiver, |_| true);
    }

    /// Drops the device metadata of the absolute route `route` alone to learn
    /// it again, as a disconnect does for its subtree. Each stream keeps its
    /// last rows, so the rows after report what was lost, or a new session,
    /// against them, as [`PacketOutcome::Reset`] says.
    pub fn relearn_route(&mut self, route: DeviceRoute) {
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

    /// Merges `record` into the state of the absolute route `route` as the
    /// device's own broadcast of it would.
    pub(crate) fn learn(&mut self, route: DeviceRoute, record: Record) {
        self.state.learn(route, record);
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

    /// Returns the route's metadata generation. It moves on when a query
    /// completes or fails and when the route relearns or resets.
    pub fn metadata_generation(&self, route: DeviceRoute) -> Option<u32> {
        self.state.metadata_generation(route)
    }

    /// Returns the route's current metadata revision without building a
    /// snapshot.
    #[cfg(test)]
    fn metadata_revision(&self, route: DeviceRoute) -> Option<u32> {
        self.state.metadata_revision(route)
    }

    /// Returns the complete metadata of the absolute route `route` if it was
    /// revised since this call last returned it. The flag says this call
    /// returned none since the parser last forgot the route.
    pub fn revised_metadata(
        &mut self,
        route: DeviceRoute,
    ) -> Option<(bool, DeviceMetadataSnapshot)> {
        self.state.revised_metadata(route)
    }

    /// Returns the absolute routes this parser holds state for.
    pub fn routes(&self) -> Vec<DeviceRoute> {
        self.state.routes()
    }
}

/// Applies `packet` to `state`. A proxy's marker packet becomes a note. A
/// link status of a disconnect or a stop makes the routes of the subtree its
/// route names learn their metadata again, blames the link for what their
/// streams lose and ends every partial batch. Any other status changes
/// nothing. A count of dropped packets blames the proxy for what its stream
/// loses. Every other packet goes to the state machine.
fn apply<'a>(
    state: &'a mut ParseState,
    batcher: &mut Batcher,
    packet: Checked<'a>,
) -> Result<PacketEvent<'a>, DataError> {
    let route = packet.route();
    let payload = packet.payload();
    if let Some(status) = payload.link_status() {
        if !status.forgets_device() {
            return Ok(PacketEvent::Applied);
        }
        state.relearn(|device| device.starts_with(&route));
        state.blame(LossSite::Link, |device| device.starts_with(&route));
        batcher.flush();
        return Ok(PacketEvent::Reset);
    }
    match payload {
        Payload::ProxyRx(ProxyRx::Dropped { stream_id, .. }) => {
            note_proxy_dropped(state, route, StreamId::new(stream_id));
            Ok(PacketEvent::Applied)
        }
        _ => state.apply_packet(route, payload, packet.payload_bytes()),
    }
}

/// Tells `state` that the proxy dropped packets of stream `stream_id` of
/// `route`. The rows the stream loses before its next rows count as dropped
/// by the proxy.
fn note_proxy_dropped(state: &mut ParseState, route: DeviceRoute, stream_id: StreamId) {
    state.blame_stream(route, stream_id, LossSite::Proxy);
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
            pending: HashMap::new(),
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
    use crate::decode::state::StreamDataError;
    use crate::fixtures;
    use crate::fixtures::{checked, packet};
    use crate::model::continuity::{Boundary, Change, Generations, Loss};
    use crate::model::sample::ColumnData;
    use crate::proto::data as wire;
    use crate::proto::data::{DataType, MAX_SAMPLE_NUMBER};
    use crate::proto::proxy::LinkStatus;
    use crate::proto::sync::Epoch;
    use crate::proto::{SampleNumber, SessionId};
    use crate::store::buffer::Buffer;

    const STREAM_ID: u8 = 1;

    thread_local! {
        /// The warnings this thread logs while [`warnings_during`] runs.
        static WARNINGS: std::cell::RefCell<Option<Vec<String>>> =
            const { std::cell::RefCell::new(None) };
    }

    /// Keeps each warning a thread logs while that thread captures them.
    struct Capture;

    impl log::Log for Capture {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            metadata.level() <= log::Level::Warn
        }

        fn log(&self, record: &log::Record<'_>) {
            if !self.enabled(record.metadata()) {
                return;
            }
            WARNINGS.with(|warnings| {
                if let Some(warnings) = warnings.borrow_mut().as_mut() {
                    warnings.push(record.args().to_string());
                }
            });
        }

        fn flush(&self) {}
    }

    /// Runs `run` and returns the warnings it logged on this thread, in order.
    fn warnings_during(run: impl FnOnce()) -> Vec<String> {
        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            if log::set_logger(&Capture).is_ok() {
                log::set_max_level(log::LevelFilter::Warn);
            }
        });
        WARNINGS.with(|warnings| *warnings.borrow_mut() = Some(Vec::new()));
        run();
        WARNINGS.with(|warnings| warnings.borrow_mut().take().unwrap_or_default())
    }

    fn metadata_packet(record: wire::Metadata<'_>) -> Vec<u8> {
        metadata_packet_at(record, DeviceRoute::root())
    }

    fn metadata_packet_at(record: wire::Metadata<'_>, route: DeviceRoute) -> Vec<u8> {
        packet::metadata(record, wire::MetadataFlags::default(), route)
    }

    fn announce(parser: &mut PacketParser, record: wire::Metadata<'_>) {
        parser
            .push_packet(checked(&metadata_packet(record)))
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

    /// Announce a device with `n_streams` streams of one `DataType::F32`
    /// column each, numbered from 1.
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

    fn stream_data_packet_for(stream_id: u8, first_sample_n: u32, rows: usize) -> Vec<u8> {
        packet::samples(
            stream_id,
            0,
            first_sample_n,
            &vec![0; 4 * rows],
            DeviceRoute::root(),
        )
    }

    fn stream_data_packet_in_segment(
        segment_id: u8,
        first_sample_n: u32,
        data: Vec<u8>,
    ) -> Vec<u8> {
        packet::samples(
            STREAM_ID,
            segment_id,
            first_sample_n,
            &data,
            DeviceRoute::root(),
        )
    }

    fn stream_data_packet(first_sample_n: u32, data: Vec<u8>) -> Vec<u8> {
        stream_data_packet_in_segment(0, first_sample_n, data)
    }

    /// Each revision of a route's metadata is handed out once, the first
    /// flagged as such, and again as the first after the route is forgotten.
    #[test]
    fn a_revision_is_handed_out_once() {
        let route = DeviceRoute::root();
        let mut parser = parser_with_clock(&[DataType::F32], 4, 1, 1, 1);
        let handed =
            |parser: &mut PacketParser| parser.revised_metadata(route).map(|(first, _)| first);
        assert_eq!(handed(&mut parser), Some(true));
        announce_schema(&mut parser, &[DataType::F32], 4, 1, 1, 1);
        assert_eq!(handed(&mut parser), None, "the same records again");
        let renamed = wire::Device {
            name: "renamed",
            ..fixtures::device()
        };
        announce(&mut parser, wire::Metadata::Device(renamed));
        assert_eq!(handed(&mut parser), Some(false));
        parser.reset_subtree(route);
        announce_schema(&mut parser, &[DataType::F32], 4, 1, 1, 1);
        assert_eq!(handed(&mut parser), Some(true));
    }

    /// A data packet in a segment the device did not announce switches the
    /// current segment without any metadata record arriving, so the snapshot
    /// revision moves, and the snapshot waits for that segment's record.
    #[test]
    fn a_data_packet_switching_segments_revises_the_metadata() {
        let route = DeviceRoute::root();
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 1, 1);
        parser
            .push_packet(checked(&stream_data_packet_in_segment(0, 0, vec![0; 4])))
            .expect("valid samples");
        assert!(parser.metadata(route).is_some());
        let before = parser.metadata_revision(route);

        parser
            .push_packet(checked(&stream_data_packet_in_segment(1, 0, vec![0; 4])))
            .expect("valid samples");

        assert_ne!(parser.metadata_revision(route), before);
        assert!(parser.metadata(route).is_none(), "segment 1 has no record");
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 1)]
        );
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
        let mut parser = parser_bootstrapped_with(wire::Segment {
            segment_id: crate::proto::SegmentId::new(2),
            ..fixtures::segment(STREAM_ID)
        });

        let batches: Vec<SampleBatch> = [5, 6]
            .map(|first| {
                parser
                    .push_packet(checked(&stream_data_packet_in_segment(
                        2,
                        first,
                        vec![0; 4],
                    )))
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

    /// A record of slot 0 of the stream that holds nothing, with a start
    /// time and time reference no populated record of the slot has.
    fn placeholder() -> wire::Segment<'static> {
        wire::Segment {
            flags: wire::SegmentFlags::default(),
            start_time: 100,
            timeref_session: SessionId::new(9),
            ..fixtures::segment(STREAM_ID)
        }
    }

    /// The populated record of slot 0 of the stream, from 0 s.
    fn populated() -> wire::Segment<'static> {
        wire::Segment {
            flags: wire::SegmentFlags::VALID | wire::SegmentFlags::ACTIVE,
            ..fixtures::segment(STREAM_ID)
        }
    }

    /// A parser that took a bootstrap reply in which the stream, of four
    /// slots, has `segment` in its current slot.
    fn parser_bootstrapped_with(segment: wire::Segment<'_>) -> PacketParser {
        let mut parser = PacketParser::new(true);
        let query = take_query(&mut parser);
        assert!(query.selectors.is_empty(), "a bootstrap query");
        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[
                wire::Metadata::Device(fixtures::device()),
                wire::Metadata::Stream(wire::Stream {
                    n_segments: 4,
                    ..fixtures::stream(STREAM_ID)
                }),
                wire::Metadata::Segment(segment),
                wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
            ]),
        );
        parser
    }

    /// Rows of a slot that holds a placeholder wait for the slot's record,
    /// and the parser asks for it. Rows after it decode with its times.
    #[test]
    fn rows_do_not_decode_against_a_placeholder() {
        let mut parser = parser_bootstrapped_with(placeholder());
        assert!(parser.metadata(DeviceRoute::root()).is_some());
        assert!(parser
            .take_metadata_queries_for(DeviceRoute::root())
            .is_empty());

        assert_eq!(
            parser
                .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
                .expect("valid rows"),
            PacketOutcome::WaitingForMetadata
        );
        assert!(parser.pop_batch().is_none());
        let query = take_query(&mut parser);
        assert_eq!(
            query.selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 0)]
        );

        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(populated())]),
        );
        parser
            .push_packet(checked(&stream_data_packet(1, vec![0; 4])))
            .expect("valid rows");
        let batch = parser.pop_batch().expect("rows under the populated record");
        assert_eq!(batch.timestamps(), [2.0], "the end of sample 1 from 0 s");
    }

    /// A placeholder a reply brings for a slot that holds a populated record
    /// leaves the record in place, and the rows after it go on under it.
    #[test]
    fn a_placeholder_does_not_replace_a_populated_record() {
        let mut parser = PacketParser::new(true);
        announce(
            &mut parser,
            wire::Metadata::Device(wire::Device {
                n_streams: 2,
                ..fixtures::device()
            }),
        );
        announce(&mut parser, wire::Metadata::Stream(fixtures::stream(1)));
        announce(&mut parser, wire::Metadata::Segment(populated()));
        announce(
            &mut parser,
            wire::Metadata::Column(fixtures::column(1, 0, DataType::F32)),
        );
        announce(&mut parser, wire::Metadata::Stream(fixtures::stream(2)));
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4 * 5])))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");

        let query = take_query(&mut parser);
        assert!(!query.selectors.is_empty(), "a query that names records");
        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(placeholder())]),
        );
        parser
            .push_packet(checked(&stream_data_packet(5, vec![0; 4])))
            .expect("valid rows");
        let after = parser.pop_batch().expect("the rows after");
        assert_eq!(after.timestamps(), [6.0], "the end of sample 5 from 0 s");
        assert_eq!(after.boundary(), None);
    }

    /// A placeholder a device broadcasts for the slot of its last rows does
    /// not close the segment, so the rows after it are no restart.
    #[test]
    fn a_broadcast_placeholder_is_not_a_closing_record() {
        let mut parser = PacketParser::new(true);
        let device = wire::Device {
            protocol: 1,
            ..fixtures::device()
        };
        announce_schema_for(&mut parser, device, &[DataType::F32], 4, 1, 1, 1);
        announce(&mut parser, wire::Metadata::Segment(populated()));
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4 * 5])))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");

        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                timeref_session: SessionId::new(7),
                ..placeholder()
            }),
        );
        parser
            .push_packet(checked(&stream_data_packet(5, vec![0; 4])))
            .expect("valid rows");
        let after = parser.pop_batch().expect("the rows after");
        assert_eq!(after.boundary(), None);
    }

    /// A placeholder for any slot names that slot current while the current
    /// slot is empty, as a proxy's snapshot of a stream that has not
    /// started does, and that completes the device's metadata.
    #[test]
    fn a_broadcast_placeholder_names_an_empty_current_slot() {
        let mut parser = PacketParser::new(true);
        announce(&mut parser, wire::Metadata::Device(fixtures::device()));
        announce(
            &mut parser,
            wire::Metadata::Stream(wire::Stream {
                n_segments: 4,
                ..fixtures::stream(STREAM_ID)
            }),
        );
        announce(
            &mut parser,
            wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
        );

        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(2),
                ..placeholder()
            }),
        );
        let queries = parser.take_metadata_queries_for(DeviceRoute::root());
        let asked: Vec<_> = queries.into_iter().map(|query| query.selectors).collect();
        assert_eq!(asked, Vec::<Vec<wire::MetadataSelector>>::new());
        assert!(parser.metadata(DeviceRoute::root()).is_some());
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
            &fixtures::metadata_reply(&[wire::Metadata::Device(fixtures::device())]),
        );
        let next = take_query(&mut parser);
        assert_eq!(next.selectors, [wire::MetadataSelector::stream(STREAM_ID)]);

        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Device(fixtures::device())]),
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
            &fixtures::metadata_reply(&[wire::Metadata::Device(fixtures::device())]),
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
            &fixtures::metadata_reply(&[wire::Metadata::Device(fixtures::device())]),
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
            &fixtures::metadata_reply(&[
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
            &fixtures::metadata_reply(&[
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
            &fixtures::metadata_reply(&[wire::Metadata::Device(wire::Device {
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
                    .push_packet(checked(&stream_data_packet(0, vec![0; 18])))
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
            parser.push_packet(checked(&stream_data_packet_in_segment(255, 0, vec![0; 4]))),
            Err(DataError::Stream {
                source: StreamDataError::SegmentOutOfRange { .. },
                ..
            })
        ));
        assert_eq!(
            parser
                .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
                .unwrap(),
            PacketOutcome::Rows(1)
        );
        assert!(parser.pop_batch().is_some());
    }

    #[test]
    fn rows_past_the_24_bit_sample_field_are_rejected() {
        let mut parser = parser_with_schema(&[DataType::F32], 4);

        assert!(matches!(
            parser.push_packet(checked(&stream_data_packet(MAX_SAMPLE_NUMBER, vec![0; 8]))),
            Err(DataError::Stream {
                source: StreamDataError::SampleNumberOverflow { .. },
                ..
            })
        ));

        parser
            .push_packet(checked(&stream_data_packet(MAX_SAMPLE_NUMBER, vec![0; 4])))
            .expect("the largest representable sample itself is valid");
        let batch = parser.pop_batch().expect("a batch is ready");
        assert_eq!(batch.sample_numbers(), [MAX_SAMPLE_NUMBER]);
    }

    /// Rows numbered back from the last rows, in their segment, with no
    /// record of it since, are of a new acquisition in that slot whose
    /// record the parser missed, after a reboot or a full lap of the ring.
    /// They wait for the slot's record, and rows after it decode with it,
    /// not with the record before.
    #[test]
    fn rows_numbered_back_in_their_segment_wait_for_its_new_record() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 1, 1);
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4 * 5])))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");

        for first in [2, 4] {
            assert_eq!(
                parser
                    .push_packet(checked(&stream_data_packet(first, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(5, vec![0; 4])))
            .expect("valid rows");
        let after = parser.pop_batch().expect("the new acquisition's rows");
        assert_eq!(
            after.timestamps(),
            [106.0],
            "the end of sample 5 from 100 s"
        );
    }

    /// A parser holding 30 rows of slot 0 of a device that declares a
    /// revision, where a start opened the slot's segment at 10 s, after the
    /// device sent that segment's record again unchanged.
    fn parser_after_a_resent_record() -> PacketParser {
        let mut parser = PacketParser::new(false);
        let device = wire::Device {
            protocol: 1,
            ..fixtures::device()
        };
        announce_schema_for(&mut parser, device, &[DataType::F32], 4, 2, 1, 1);
        let opened = wire::Metadata::Segment(wire::Segment {
            start_time: 10,
            flags: wire::SegmentFlags::VALID
                | wire::SegmentFlags::ACTIVE
                | wire::SegmentFlags::START,
            ..fixtures::segment(STREAM_ID)
        });
        announce(&mut parser, opened);
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4 * 30])))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");
        announce(&mut parser, opened);
        parser
    }

    /// The record a device sends again of the segment it is in does not
    /// renew the slot. Rows of a new acquisition in the slot, in a new
    /// session, wait for the record that opens it, and decode with it.
    #[test]
    fn rows_of_a_new_acquisition_wait_for_its_record_after_the_last_one_came_again() {
        let mut parser = parser_in_a_new_session();
        assert_eq!(
            parser
                .push_packet(checked(&stream_data_packet(17, vec![0; 4 * 3])))
                .expect("valid rows"),
            PacketOutcome::WaitingForMetadata
        );
        assert!(
            parser.pop_batch().is_none(),
            "the rows wait for their record"
        );
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 0)]
        );

        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                start_time: 50,
                flags: wire::SegmentFlags::VALID
                    | wire::SegmentFlags::ACTIVE
                    | wire::SegmentFlags::START,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        parser
            .push_packet(checked(&stream_data_packet(20, vec![0; 4])))
            .expect("valid rows");
        let after = parser.pop_batch().expect("the new acquisition's rows");
        assert_eq!(after.timestamps(), [71.0], "the end of sample 20 from 50 s");
        assert!(after.boundary().is_some());
    }

    /// The parser of [`parser_after_a_resent_record`] after the device's
    /// heartbeat and record of a new session.
    fn parser_in_a_new_session() -> PacketParser {
        let mut parser = parser_after_a_resent_record();
        parser
            .push_packet(checked(&packet::heartbeat_session(43, DeviceRoute::root())))
            .unwrap();
        announce(
            &mut parser,
            wire::Metadata::Device(wire::Device {
                session: crate::proto::SessionId::new(43),
                protocol: 1,
                ..fixtures::device()
            }),
        );
        parser
    }

    /// The last rows' record sent again after the device's new session, as
    /// a proxy that still holds it sends it, does not renew the slot. Rows
    /// numbered back in the slot wait for the record that opens their
    /// acquisition.
    #[test]
    fn the_last_record_sent_again_in_a_new_session_does_not_renew_its_slot() {
        let mut parser = parser_in_a_new_session();
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                start_time: 10,
                flags: wire::SegmentFlags::VALID
                    | wire::SegmentFlags::ACTIVE
                    | wire::SegmentFlags::START,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        assert_eq!(
            parser
                .push_packet(checked(&stream_data_packet(5, vec![0; 4])))
                .expect("valid rows"),
            PacketOutcome::WaitingForMetadata
        );
        assert!(
            parser.pop_batch().is_none(),
            "the rows wait for their record"
        );
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 0)]
        );
    }

    /// A record of the last rows' slot with their start second, on the time
    /// reference of the device's new session, opens a new acquisition. Its
    /// rows decode with a boundary.
    #[test]
    fn a_restart_on_the_devices_own_clock_opens_a_new_acquisition() {
        let mut parser = parser_in_a_new_session();
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                start_time: 10,
                timeref_session: crate::proto::SessionId::new(43),
                flags: wire::SegmentFlags::VALID
                    | wire::SegmentFlags::ACTIVE
                    | wire::SegmentFlags::START,
                ..fixtures::segment(STREAM_ID)
            }),
        );
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4 * 3])))
            .expect("valid rows");
        let after = parser.pop_batch().expect("the new acquisition's rows");
        assert!(after.boundary().is_some());
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
                .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
                .expect("valid rows"),
            PacketOutcome::WaitingForMetadata
        );
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 0)]
        );
        announce(&mut parser, segment(0, 4));
        parser
            .push_packet(checked(&stream_data_packet(1, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4 * 5])))
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
            .push_packet(checked(&stream_data_packet_in_segment(1, 0, vec![0; 4])))
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

    /// The boundary of `outputs` rows of slot 0 in `parser`, then row `first`
    /// of the segment `next`.
    fn boundary_across(
        mut parser: PacketParser,
        outputs: usize,
        next: wire::Segment<'_>,
        first: u32,
    ) -> Option<Boundary> {
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4 * outputs])))
            .expect("valid rows");
        parser.pop_batch().expect("the first batch");
        announce(&mut parser, wire::Metadata::Segment(next));
        parser
            .push_packet(checked(&stream_data_packet_in_segment(
                next.segment_id.value(),
                first,
                vec![0; 4],
            )))
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
        let next = wire::Segment {
            segment_id: crate::proto::SegmentId::new(0),
            start_time: 4,
            sampling_rate: 1,
            decimation: 1,
            ..fixtures::segment(STREAM_ID)
        };
        assert_eq!(
            boundary_across(parser_with_clock(&[DataType::F32], 4, 4, 1, 1), 1, next, 0),
            Some(Boundary {
                change: rollover(0, 0),
                loss: Some(Loss {
                    samples: 3,
                    expected: Some(SampleNumber::new(1)),
                    dropped_batches: 0,
                    dropped_samples: 0,
                    link_samples: 0,
                    proxy_samples: 0,
                }),
            })
        );
    }

    /// A segment a start opened, on a device that declares a revision, comes as a restart with no
    /// loss, where a switch or any segment of a device that declares none comes as a rollover
    /// with the periods between lost.
    #[test]
    fn a_declared_start_is_a_restart_and_anything_else_a_rollover() {
        let boundary = |protocol: u8, flags: wire::SegmentFlags| {
            let mut parser = PacketParser::new(true);
            let device = wire::Device {
                protocol,
                ..fixtures::device()
            };
            announce_schema_for(&mut parser, device, &[DataType::F32], 4, 4, 1, 1);
            parser
                .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
                .unwrap();
            parser.pop_batch().expect("the first batch");
            let next = wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                start_time: 4,
                flags,
                ..fixtures::segment(STREAM_ID)
            };
            announce(&mut parser, wire::Metadata::Segment(next));
            parser
                .push_packet(checked(&stream_data_packet_in_segment(1, 0, vec![0; 4])))
                .unwrap();
            parser
                .pop_batch()
                .expect("the batch after")
                .boundary()
                .cloned()
        };
        let open = wire::SegmentFlags::VALID | wire::SegmentFlags::ACTIVE;
        let lost = Some(Loss {
            samples: 3,
            expected: Some(SampleNumber::new(1)),
            dropped_batches: 0,
            dropped_samples: 0,
            link_samples: 0,
            proxy_samples: 0,
        });
        let restarted = Change::Restarted {
            old_id: crate::proto::SegmentId::new(0),
            new_id: crate::proto::SegmentId::new(1),
        };
        assert_eq!(
            boundary(1, open | wire::SegmentFlags::START),
            Some(Boundary::of(restarted))
        );
        for (protocol, flags) in [(1, open), (0, open | wire::SegmentFlags::START)] {
            assert_eq!(
                boundary(protocol, flags),
                Some(Boundary {
                    change: rollover(0, 1),
                    loss: lost.clone(),
                }),
                "revision {protocol} with {flags:?}"
            );
        }
    }

    /// The boundary of five rows of slot 0 under `before`, then row 0 of slot 1 under `after`.
    /// Both are records a switch opens, at 10 rows a second.
    fn boundary_between(before: wire::Segment<'_>, after: wire::Segment<'_>) -> Option<Boundary> {
        let open = wire::SegmentFlags::VALID | wire::SegmentFlags::ACTIVE;
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 10, 1);
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(0),
                flags: open,
                ..before
            }),
        );
        let after = wire::Segment {
            segment_id: crate::proto::SegmentId::new(1),
            flags: open,
            ..after
        };
        boundary_across(parser, 5, after, 0)
    }

    /// A segment on the time reference of session 7 of `clock`, in `epoch`, from `start_time`.
    fn on_reference(epoch: Epoch, start_time: u32) -> wire::Segment<'static> {
        wire::Segment {
            epoch,
            start_time,
            sampling_rate: 10,
            decimation: 1,
            ..fixtures::segment(STREAM_ID)
        }
    }

    fn reference_changed(old: u32, new: u32) -> Option<Boundary> {
        Some(Boundary::of(Change::TimeRefSessionChanged {
            old: SessionId::new(old),
            new: SessionId::new(new),
        }))
    }

    /// A next segment whose epoch alone differs, forward, is on another time reference.
    #[test]
    fn an_epoch_change_forward_is_a_time_reference_change() {
        assert_eq!(
            boundary_between(
                on_reference(Epoch::SYSTIME, 5000),
                on_reference(Epoch::UNIX, 1_700_000_000)
            ),
            reference_changed(7, 7)
        );
    }

    /// A next segment whose epoch alone differs, backward, is on another time reference.
    #[test]
    fn an_epoch_change_backward_is_a_time_reference_change() {
        assert_eq!(
            boundary_between(
                on_reference(Epoch::UNIX, 1_700_000_000),
                on_reference(Epoch::SYSTIME, 5000)
            ),
            reference_changed(7, 7)
        );
    }

    /// A next segment whose serial alone differs is on another time reference.
    #[test]
    fn a_serial_change_is_a_time_reference_change() {
        assert_eq!(
            boundary_between(
                on_reference(Epoch::SYSTIME, 5000),
                wire::Segment {
                    timeref_serial: "other",
                    ..on_reference(Epoch::SYSTIME, 5003)
                }
            ),
            reference_changed(7, 7)
        );
    }

    /// A next segment whose session alone differs is on another time reference, with both
    /// sessions named.
    #[test]
    fn a_session_change_is_a_time_reference_change() {
        assert_eq!(
            boundary_between(
                on_reference(Epoch::SYSTIME, 5000),
                wire::Segment {
                    timeref_session: SessionId::new(8),
                    ..on_reference(Epoch::SYSTIME, 5003)
                }
            ),
            reference_changed(7, 8)
        );
    }

    /// Rows a device that declares no revision sends after its link came back go on from the last
    /// rows in their segment, with the rows between lost, and come as a restart in another.
    #[test]
    fn rows_after_a_lost_link_go_on_in_their_segment_and_restart_in_another() {
        let boundary = |segment_id: u8, start_time: u32, first: u32| {
            let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
            parser
                .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
                .unwrap();
            parser.pop_batch().expect("the first batch");
            let lost = packet::link(LinkStatus::SensorDisconnected, DeviceRoute::root());
            parser.push_packet(checked(&lost)).unwrap();
            announce_schema(&mut parser, &[DataType::F32], 4, 4, 1, 1);
            announce(
                &mut parser,
                wire::Metadata::Segment(wire::Segment {
                    segment_id: crate::proto::SegmentId::new(segment_id),
                    start_time,
                    ..fixtures::segment(STREAM_ID)
                }),
            );
            parser
                .push_packet(checked(&stream_data_packet_in_segment(
                    segment_id,
                    first,
                    vec![0; 4],
                )))
                .unwrap();
            parser
                .pop_batch()
                .expect("the batch after")
                .boundary()
                .cloned()
        };
        assert_eq!(
            boundary(0, 0, 3),
            Some(Boundary {
                change: None,
                loss: Some(Loss {
                    samples: 2,
                    expected: Some(SampleNumber::new(1)),
                    dropped_batches: 0,
                    dropped_samples: 0,
                    link_samples: 2,
                    proxy_samples: 0,
                }),
            })
        );
        assert_eq!(
            boundary(1, 4, 0),
            Some(Boundary::of(Change::Restarted {
                old_id: crate::proto::SegmentId::new(0),
                new_id: crate::proto::SegmentId::new(1),
            }))
        );
    }

    /// The rows a stream loses are counted where the parser was told they
    /// went since the stream's last rows. A lost link explains more than a
    /// receiver that dropped packets, that more than a proxy that dropped
    /// packets of the stream, and with nothing told the device skipped them.
    /// What a proxy dropped of another stream explains nothing, and what was
    /// told is forgotten at the stream's next rows.
    #[test]
    fn a_loss_is_counted_where_the_parser_was_told_the_rows_went() {
        #[derive(Clone, Copy, Debug)]
        enum Told {
            Link,
            Receiver,
            Proxy(u8),
        }
        // The rows lost before the next batch, as those the receiver
        // dropped, those the link lost, those the proxy dropped and those
        // skipped.
        let lost = |told: &[Told]| {
            let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
            parser
                .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
                .unwrap();
            for told in told {
                match *told {
                    Told::Link => {
                        let lost =
                            packet::link(LinkStatus::SensorDisconnected, DeviceRoute::root());
                        parser.push_packet(checked(&lost)).unwrap();
                        announce_schema(&mut parser, &[DataType::F32], 4, 4, 1, 1);
                        announce(
                            &mut parser,
                            wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
                        );
                    }
                    Told::Receiver => parser.note_dropped_packets(),
                    Told::Proxy(stream) => {
                        let dropped = packet::dropped(stream, 2, DeviceRoute::root());
                        parser.push_packet(checked(&dropped)).unwrap();
                    }
                }
            }
            for first in [3, 6] {
                parser
                    .push_packet(checked(&stream_data_packet(first, vec![0; 4])))
                    .unwrap();
            }
            parser.flush();
            let losses: Vec<_> = std::iter::from_fn(|| parser.pop_batch())
                .filter_map(|batch| batch.boundary().and_then(|b| b.loss.clone()))
                .map(|loss| {
                    assert_eq!(loss.samples, 2, "{told:?}");
                    (
                        loss.dropped_samples,
                        loss.link_samples,
                        loss.proxy_samples,
                        loss.skipped_samples(),
                    )
                })
                .collect();
            assert_eq!(losses.len(), 2, "{told:?}");
            assert_eq!(losses[1], (0, 0, 0, 2), "{told:?} is forgotten");
            losses[0]
        };
        assert_eq!(lost(&[]), (0, 0, 0, 2));
        assert_eq!(lost(&[Told::Proxy(STREAM_ID)]), (0, 0, 2, 0));
        assert_eq!(lost(&[Told::Proxy(STREAM_ID + 1)]), (0, 0, 0, 2));
        assert_eq!(lost(&[Told::Receiver]), (2, 0, 0, 0));
        assert_eq!(
            lost(&[Told::Receiver, Told::Proxy(STREAM_ID)]),
            (2, 0, 0, 0)
        );
        assert_eq!(lost(&[Told::Receiver, Told::Link]), (0, 2, 0, 0));
        assert_eq!(lost(&[Told::Link, Told::Proxy(STREAM_ID)]), (0, 2, 0, 0));
    }

    /// A parser told of the packets a proxy dropped by the proxy's marker
    /// packet gives the same batches with the same boundaries as one told by
    /// a call.
    #[test]
    fn a_marker_packet_and_a_call_tell_the_parser_alike() {
        // The sample numbers, generations and boundary of each batch, with
        // the parser told by packet or by call between two runs of rows.
        let batches = |by_packet: bool| {
            let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
            parser
                .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
                .unwrap();
            parser
                .push_packet(checked(&stream_data_packet(1, vec![0; 4])))
                .unwrap();
            let root = DeviceRoute::root();
            if by_packet {
                let dropped = packet::dropped(STREAM_ID, 2, root);
                let outcome = parser.push_packet(checked(&dropped)).unwrap();
                assert_eq!(outcome, PacketOutcome::Applied);
            } else {
                note_proxy_dropped(&mut parser.state, root, StreamId::new(STREAM_ID));
            }
            for first in [4, 5] {
                parser
                    .push_packet(checked(&stream_data_packet(first, vec![0; 4])))
                    .unwrap();
            }
            parser
                .finish()
                .iter()
                .map(|batch| {
                    (
                        batch.sample_numbers().to_vec(),
                        batch.generations(),
                        batch.boundary().cloned(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let by_packet = batches(true);
        assert!(
            by_packet.iter().any(|(_, _, boundary)| boundary.is_some()),
            "the rows after carry a boundary"
        );
        assert_eq!(by_packet, batches(false));
    }

    /// A device that declares a revision may send its next session's heartbeat and record ahead
    /// of its last session's rows, which still decode under the records they came after.
    #[test]
    fn a_declared_devices_new_session_leaves_its_streams_as_they_were() {
        let mut parser = PacketParser::new(false);
        let device = |session: u32| wire::Device {
            session: crate::proto::SessionId::new(session),
            protocol: 1,
            ..fixtures::device()
        };
        announce_schema_for(&mut parser, device(42), &[DataType::F32], 4, 4, 1, 1);
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
            .unwrap();
        parser.pop_batch().expect("the first batch");
        parser
            .push_packet(checked(&packet::heartbeat_session(43, DeviceRoute::root())))
            .unwrap();
        announce(&mut parser, wire::Metadata::Device(device(43)));
        parser
            .push_packet(checked(&stream_data_packet(1, vec![0; 4])))
            .unwrap();
        let late = parser.pop_batch().expect("the last session's rows decode");
        assert_eq!(late.boundary(), None);
        assert!(parser
            .take_metadata_queries_for(DeviceRoute::root())
            .is_empty());
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
            &fixtures::metadata_reply(&[wire::Metadata::Segment(wire::Segment {
                start_time: 2,
                ..fixtures::segment(STREAM_ID)
            })]),
        );
        parser
            .push_packet(checked(&stream_data_packet(1, vec![0; 4])))
            .expect("valid rows");

        let batch = parser.pop_batch().expect("the rows after the reply");
        assert_eq!(
            batch.boundary().cloned(),
            Some(Boundary {
                change: rollover(0, 0),
                loss: Some(Loss {
                    samples: 2,
                    expected: Some(SampleNumber::new(1)),
                    dropped_batches: 0,
                    dropped_samples: 0,
                    link_samples: 0,
                    proxy_samples: 0,
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
                .push_packet(checked(&stream_data_packet(first_sample_n, vec![0; 4])))
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
                .push_packet(checked(&stream_data_packet(first_sample_n, vec![0; 8])))
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

    /// Rows or a record of a stream above the device's stream count are
    /// refused alone. The streams the device counts keep decoding, and the
    /// route asks for nothing.
    #[test]
    fn a_stream_above_the_stream_count_is_refused_alone() {
        let route = DeviceRoute::root();
        let above = [
            stream_data_packet_for(3, 0, 1),
            packet::metadata(
                wire::Metadata::Segment(fixtures::segment(3)),
                wire::MetadataFlags::UPDATE,
                route,
            ),
        ];
        for packet in above {
            let mut parser = parser_with_streams(2);
            assert!(parser.metadata(route).is_some());
            parser
                .push_packet(checked(&packet))
                .expect("a refusal is no error");
            assert!(parser.metadata(route).is_some(), "the metadata is kept");
            for stream_id in [1, 2] {
                assert!(matches!(
                    parser.push_packet(checked(&stream_data_packet_for(stream_id, 0, 1))),
                    Ok(PacketOutcome::Rows(1))
                ));
            }
            assert!(
                parser.take_metadata_queries_for(route).is_empty(),
                "no bootstrap query"
            );
        }
    }

    /// A record the parser drops is logged with its route, its kind and why.
    /// The same drop again, as a device that broadcasts the record each
    /// second makes it, is a warning only the first time. A column ahead of
    /// its turn is no fault and gives no warning.
    #[test]
    fn a_dropped_metadata_record_is_logged() {
        let route = DeviceRoute::from_hops(&[3]).expect("a route");
        let mut parser = PacketParser::new(true);
        let query = parser
            .take_metadata_queries_for(route)
            .pop()
            .expect("a bootstrap query");
        let mut reply = fixtures::metadata_reply(&[
            wire::Metadata::Device(wire::Device {
                n_streams: 2,
                ..fixtures::device()
            }),
            wire::Metadata::Segment(fixtures::segment(3)),
        ]);
        // A column record whose stream id is 0, which does not parse.
        reply.extend_from_slice(&[wire::MetadataType::Column.into(), 7, 7, 0, 0, 0x42, 0, 0, 0]);
        let warnings = warnings_during(|| {
            parser.apply_metadata_reply(query, &reply);
            for _ in 0..2 {
                for record in [
                    wire::Metadata::Segment(fixtures::segment(3)),
                    wire::Metadata::Column(fixtures::column(1, 2, DataType::F32)),
                ] {
                    let broadcast = packet::metadata(record, wire::MetadataFlags::UPDATE, route);
                    parser
                        .push_packet(checked(&broadcast))
                        .expect("a drop is no error");
                }
            }
        });
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(
            warnings.iter().all(|warning| warning.contains("/3")),
            "{warnings:?}"
        );
        assert!(
            warnings[0].contains("Segment") && warnings[0].contains("stream 3"),
            "{warnings:?}"
        );
        assert!(
            warnings[1].contains("Column") && warnings[1].contains("does not parse"),
            "{warnings:?}"
        );
    }

    /// The bare column record of stream 1, index 0 and type f32 whose units
    /// are `B0 43`, a degree sign without its lead byte, then `C`.
    fn column_with_bad_units() -> Vec<u8> {
        let head = [7, 1, 0, DataType::F32.value(), 4, 2, 0];
        [&head[..], b"temp", &[0xB0, b'C']].concat()
    }

    /// The units of column 0 of stream 1 in the metadata the parser holds
    /// for `route`.
    fn units(parser: &PacketParser, route: DeviceRoute) -> String {
        let metadata = parser.metadata(route).expect("complete metadata");
        let stream = metadata
            .stream(crate::proto::StreamId::new(STREAM_ID))
            .expect("stream 1");
        let column = stream.schema().column(crate::proto::ColumnId::new(0));
        column.expect("column 0").units().to_owned()
    }

    /// A `dev.metadata` reply whose column has units that are not UTF-8
    /// completes the stream, and the parser asks for nothing more.
    #[test]
    fn a_reply_column_whose_units_are_not_utf8_completes_the_stream() {
        let route = DeviceRoute::root();
        let mut parser = PacketParser::new(true);
        let query = take_query(&mut parser);
        let mut reply = fixtures::metadata_reply(&[
            wire::Metadata::Device(fixtures::device()),
            wire::Metadata::Stream(fixtures::stream(STREAM_ID)),
            wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
        ]);
        let column = column_with_bad_units();
        reply.extend_from_slice(&[wire::MetadataType::Column.into(), column.len() as u8]);
        reply.extend_from_slice(&column);
        parser.apply_metadata_reply(query, &reply);

        assert_eq!(units(&parser, route), "\u{FFFD}C");
        assert!(parser.take_metadata_queries_for(route).is_empty());
    }

    /// A broadcast column whose units are not UTF-8 is applied repaired.
    #[test]
    fn a_broadcast_column_whose_units_are_not_utf8_is_applied() {
        let route = DeviceRoute::root();
        let mut parser = PacketParser::new(true);
        for record in [
            wire::Metadata::Device(fixtures::device()),
            wire::Metadata::Stream(fixtures::stream(STREAM_ID)),
            wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
        ] {
            announce(&mut parser, record);
        }
        let kind = wire::MetadataType::Column.into();
        let mut raw = [0u8; 64];
        let flags = wire::MetadataFlags::UPDATE;
        let len = wire::write_metadata_record(&mut raw, kind, flags, &column_with_bad_units())
            .expect("the record fits");
        parser
            .push_packet(checked(&raw[..len]))
            .expect("a metadata record");

        assert_eq!(units(&parser, route), "\u{FFFD}C");
    }

    /// A record kept with text that is not UTF-8 is a warning that names
    /// the route, the record and the field, the first time only.
    #[test]
    fn a_record_whose_text_is_not_utf8_is_logged_once() {
        let route = DeviceRoute::from_hops(&[3]).expect("a route");
        let mut parser = PacketParser::new(true);
        let column = column_with_bad_units();
        let warnings = warnings_during(|| {
            for _ in 0..2 {
                let query = parser
                    .take_metadata_queries_for(route)
                    .pop()
                    .expect("a query");
                let mut reply =
                    fixtures::metadata_reply(&[wire::Metadata::Device(fixtures::device())]);
                reply.extend_from_slice(&[wire::MetadataType::Column.into(), column.len() as u8]);
                reply.extend_from_slice(&column);
                parser.apply_metadata_reply(query, &reply);
            }
        });
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            ["/3", "Column", "stream 1 column 0", "U+FFFD in its units"]
                .iter()
                .all(|part| warnings[0].contains(part)),
            "{warnings:?}"
        );
    }

    /// The warnings a parser at `/3` logs for a bootstrap reply that holds
    /// the device record and then `column`, a bare column record.
    fn warnings_for_reply_column(column: &[u8]) -> Vec<String> {
        let route = DeviceRoute::from_hops(&[3]).expect("a route");
        let mut parser = PacketParser::new(true);
        warnings_during(|| {
            let query = parser
                .take_metadata_queries_for(route)
                .pop()
                .expect("a query");
            let mut reply = fixtures::metadata_reply(&[wire::Metadata::Device(fixtures::device())]);
            reply.extend_from_slice(&[wire::MetadataType::Column.into(), column.len() as u8]);
            reply.extend_from_slice(column);
            parser.apply_metadata_reply(query, &reply);
        })
    }

    /// A device may send U+FFFD as valid UTF-8, so the warning says what
    /// the text holds and not that its bytes were not UTF-8.
    #[test]
    fn a_record_that_sends_a_replacement_character_is_not_said_to_be_invalid() {
        let units = "\u{FFFD}".as_bytes();
        let head = [7, 1, 0, DataType::F32.value(), 4, units.len() as u8, 0];
        let warnings = warnings_for_reply_column(&[&head[..], b"temp", units].concat());
        assert!(
            warnings
                .iter()
                .all(|warning| !warning.contains("not UTF-8")),
            "{warnings:?}"
        );
    }

    /// A record whose text holds U+FFFD and that is then dropped logs the
    /// drop and nothing else.
    #[test]
    fn a_dropped_record_whose_text_holds_a_replacement_character_logs_only_the_drop() {
        let mut column = column_with_bad_units();
        column[1] = 99; // a stream above the device's count of 1
        let warnings = warnings_for_reply_column(&column);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("dropped"), "{warnings:?}");
    }

    #[test]
    fn pending_batches_never_straddle_a_shared_generation_bump() {
        let mut parser = parser_with_streams(2).with_batch_rows(4);
        parser
            .push_packet(checked(&stream_data_packet_for(2, 0, 2)))
            .expect("valid rows");
        assert_eq!(std::iter::from_fn(|| parser.pop_batch()).count(), 1);
        parser
            .push_packet(checked(&stream_data_packet_for(2, 2, 2)))
            .expect("valid rows");
        assert!(parser.pop_batch().is_none(), "stream 2 holds half a batch");

        // Stream 1's startup boundary leaves the shared generations alone, so
        // stream 2 keeps accumulating.
        parser
            .push_packet(checked(&stream_data_packet_for(1, 0, 2)))
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
            .push_packet(checked(&stream_data_packet_for(1, 10, 2)))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the first batch").generations();

        parser
            .push_packet(checked(&packet::legacy_status(
                LinkStatus::SensorDisconnected,
                DeviceRoute::root(),
            )))
            .expect("the disconnect");

        // The device comes back and re-announces exactly the same schema.
        announce_schema(&mut parser, &[DataType::F32], 4, 1, 1, 1);
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
        let forms: [fn(LinkStatus, DeviceRoute) -> Vec<u8>; 2] =
            [packet::link, packet::legacy_status];
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
                        .push_packet(checked(&metadata_packet_at(record, route)))
                        .expect("valid metadata");
                }
            }

            let marker = form(LinkStatus::SensorDisconnected, bounced);
            parser
                .push_packet(checked(&marker))
                .expect("a routed disconnect applies");

            assert!(
                parser.metadata(steady).is_some(),
                "{:?}: the steady mount kept its description",
                checked(&marker).header().ptype
            );
            assert!(
                parser.metadata(bounced).is_none(),
                "{:?}: the bounced mount rediscovers",
                checked(&marker).header().ptype
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
                    .push_packet(checked(&metadata_packet_at(record, route)))
                    .expect("valid metadata");
            }
        }

        parser
            .push_packet(checked(&packet::link(LinkStatus::Stopped, stopped)))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(1, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet_in_segment(1, 0, vec![0; 4])))
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
                .push_packet(checked(&stream_data_packet(first, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(1000, vec![0; 4])))
            .unwrap();
        parser.pop_batch().unwrap();
        parser
            .push_packet(checked(&stream_data_packet_in_segment(1, 1, vec![0; 4])))
            .unwrap();
        assert!(parser.pop_batch().is_none());
        let query = take_query(&mut parser);
        assert_eq!(
            query.selectors,
            vec![wire::MetadataSelector::segment(STREAM_ID, 1)]
        );
        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                start_time: 1000,
                ..fixtures::segment(STREAM_ID)
            })]),
        );
        parser
            .push_packet(checked(&stream_data_packet_in_segment(1, 1, vec![0; 4])))
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
            &fixtures::metadata_reply(&[wire::Metadata::Segment(fixtures::segment(STREAM_ID))]),
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

    /// Pushes `rows` rows of slot `segment_id` of the stream, from sample
    /// `first`.
    fn push_rows(
        parser: &mut PacketParser,
        segment_id: u8,
        first: u32,
        rows: usize,
    ) -> PacketOutcome {
        parser
            .push_packet(checked(&stream_data_packet_in_segment(
                segment_id,
                first,
                vec![0; 4 * rows],
            )))
            .expect("valid rows")
    }

    /// The segment record of the stream in the parser's snapshot, as its
    /// slot, its start time and whether it is populated.
    fn snapshot_segment(parser: &PacketParser) -> Option<(u8, u32, bool)> {
        let metadata = parser.metadata(DeviceRoute::root())?;
        let segment = metadata.stream(StreamId::new(STREAM_ID))?.segment();
        Some((
            segment.segment_id().value(),
            segment.start_time(),
            segment.is_populated(),
        ))
    }

    /// A populated record of slot `segment_id` of the stream from
    /// `start_time`.
    fn segment_at(segment_id: u8, start_time: u32) -> wire::Segment<'static> {
        wire::Segment {
            segment_id: crate::proto::SegmentId::new(segment_id),
            start_time,
            ..fixtures::segment(STREAM_ID)
        }
    }

    /// A parser that took five rows of slot 0 of the stream, of four slots,
    /// under [`populated`], on a device of revision `protocol` whose second
    /// stream it knows only by its stream record, and the query that asks
    /// for the rest of that stream.
    fn parser_asking_for_another_stream(protocol: u8) -> (PacketParser, MetadataQuery) {
        let mut parser = PacketParser::new(true);
        announce(
            &mut parser,
            wire::Metadata::Device(wire::Device {
                n_streams: 2,
                protocol,
                ..fixtures::device()
            }),
        );
        announce(
            &mut parser,
            wire::Metadata::Stream(wire::Stream {
                n_segments: 4,
                ..fixtures::stream(STREAM_ID)
            }),
        );
        announce(&mut parser, wire::Metadata::Segment(populated()));
        announce(
            &mut parser,
            wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
        );
        announce(&mut parser, wire::Metadata::Stream(fixtures::stream(2)));
        assert_eq!(push_rows(&mut parser, 0, 0, 5), PacketOutcome::Rows(5));
        parser.pop_batch().expect("the first batch");
        let query = take_query(&mut parser);
        assert!(!query.selectors.is_empty(), "a query that names records");
        (parser, query)
    }

    /// An unpopulated broadcast names its slot expected while the expected
    /// slot is unknown, and the slot holds that record. Rows of the slot
    /// wait, and the parser asks for its record.
    #[test]
    fn an_unpopulated_record_names_its_slot_expected_and_its_rows_wait() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
        announce(&mut parser, wire::Metadata::Segment(segment_at(1, 1)));
        assert_eq!(
            push_rows(&mut parser, 2, 0, 1),
            PacketOutcome::WaitingForMetadata
        );
        announce(&mut parser, wire::Metadata::Segment(placeholder()));

        assert_eq!(snapshot_segment(&parser), Some((0, 100, false)));
        assert_eq!(
            push_rows(&mut parser, 0, 0, 1),
            PacketOutcome::WaitingForMetadata
        );
        assert!(parser.pop_batch().is_none());
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 0)]
        );
    }

    /// An unpopulated record of another slot in a reply is dropped, and the
    /// parser asks again for the record of the slot the rows are in.
    #[test]
    fn an_unpopulated_reply_about_another_slot_is_dropped() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
        assert_eq!(
            push_rows(&mut parser, 2, 0, 1),
            PacketOutcome::WaitingForMetadata
        );
        let query = take_query(&mut parser);
        assert_eq!(
            query.selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 2)]
        );
        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(1),
                ..placeholder()
            })]),
        );

        assert_eq!(snapshot_segment(&parser), None);
        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 2)]
        );
    }

    /// A populated record of another slot than the expected one, in a reply,
    /// is dropped and leaves no mark. One a start opened leaves rows of
    /// another segment a rollover. One of another segment in the last rows'
    /// slot leaves rows numbered back waiting after their own record comes
    /// again.
    #[test]
    fn a_record_about_another_slot_in_a_reply_leaves_no_mark() {
        let (mut parser, query) = parser_asking_for_another_stream(1);
        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(wire::Segment {
                flags: wire::SegmentFlags::VALID
                    | wire::SegmentFlags::ACTIVE
                    | wire::SegmentFlags::START,
                ..segment_at(2, 3)
            })]),
        );
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                flags: wire::SegmentFlags::VALID | wire::SegmentFlags::ACTIVE,
                ..segment_at(1, 5)
            }),
        );
        assert_eq!(push_rows(&mut parser, 1, 0, 1), PacketOutcome::Rows(1));
        assert_eq!(
            parser.pop_batch().expect("the rows after").boundary(),
            Some(&Boundary {
                change: rollover(0, 1),
                loss: None,
            })
        );

        let (mut parser, query) = parser_asking_for_another_stream(0);
        announce(&mut parser, wire::Metadata::Segment(segment_at(1, 1)));
        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(segment_at(0, 50))]),
        );
        announce(&mut parser, wire::Metadata::Segment(populated()));
        assert_eq!(
            push_rows(&mut parser, 0, 2, 1),
            PacketOutcome::WaitingForMetadata
        );
        assert!(parser.pop_batch().is_none());
    }

    /// A record of another slot than the expected one that the parser drops
    /// leaves the metadata revision as it was, populated in a reply or
    /// unpopulated in a broadcast.
    #[test]
    fn a_record_dropped_for_another_slot_leaves_the_revision() {
        let route = DeviceRoute::root();
        let (mut parser, query) = parser_asking_for_another_stream(0);
        let before = parser.metadata_revision(route);
        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(segment_at(2, 3))]),
        );
        assert_eq!(parser.metadata_revision(route), before);

        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                segment_id: crate::proto::SegmentId::new(3),
                ..placeholder()
            }),
        );
        assert_eq!(parser.metadata_revision(route), before);
    }

    /// A populated record of the expected slot in a reply, while the slot
    /// holds a record, is dropped. The rows after decode with the held one.
    #[test]
    fn a_reply_about_the_expected_slot_keeps_its_held_record() {
        let route = DeviceRoute::root();
        let (mut parser, query) = parser_asking_for_another_stream(0);
        let before = parser.metadata_revision(route);
        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(segment_at(0, 50))]),
        );

        assert_eq!(parser.metadata_revision(route), before);
        assert_eq!(push_rows(&mut parser, 0, 5, 1), PacketOutcome::Rows(1));
        let after = parser.pop_batch().expect("the rows after");
        assert_eq!(after.timestamps(), [6.0], "the end of sample 5 from 0 s");
        assert_eq!(after.boundary(), None);
    }

    /// A reply that brings a populated record for a current slot the device
    /// said holds nothing fills the slot, and rows of it decode.
    #[test]
    fn a_reply_fills_a_current_slot_the_device_said_holds_nothing() {
        let mut parser = PacketParser::new(true);
        announce(
            &mut parser,
            wire::Metadata::Device(wire::Device {
                n_streams: 2,
                ..fixtures::device()
            }),
        );
        announce(
            &mut parser,
            wire::Metadata::Stream(wire::Stream {
                n_segments: 4,
                ..fixtures::stream(STREAM_ID)
            }),
        );
        announce(&mut parser, wire::Metadata::Segment(placeholder()));
        announce(
            &mut parser,
            wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
        );
        announce(&mut parser, wire::Metadata::Stream(fixtures::stream(2)));
        let query = take_query(&mut parser);
        assert!(!query.selectors.is_empty(), "a query that names records");

        parser.apply_metadata_reply(
            query,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(populated())]),
        );
        assert_eq!(push_rows(&mut parser, 0, 0, 1), PacketOutcome::Rows(1));
        let batch = parser.pop_batch().expect("rows under the reply's record");
        assert_eq!(batch.timestamps(), [1.0], "the end of sample 0 from 0 s");
    }

    /// A broadcast record of the last rows' segment with other bytes
    /// replaces the record the slot holds. The snapshot and the rows after
    /// carry it.
    #[test]
    fn a_broadcast_of_the_same_segment_with_other_bytes_replaces_its_record() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 1, 1, 1);
        assert_eq!(push_rows(&mut parser, 0, 0, 5), PacketOutcome::Rows(5));
        parser.pop_batch().expect("the first batch");
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                filter_cutoff: 3.0,
                ..fixtures::segment(STREAM_ID)
            }),
        );

        let snapshot = parser.metadata(DeviceRoute::root()).expect("complete");
        let stream = snapshot
            .stream(StreamId::new(STREAM_ID))
            .expect("the stream");
        assert_eq!(stream.segment().filter_cutoff(), 3.0);
        assert_eq!(push_rows(&mut parser, 0, 5, 1), PacketOutcome::Rows(1));
        let after = parser.pop_batch().expect("the rows after");
        assert_eq!(after.boundary(), None);
        assert_eq!(after.metadata().segment().filter_cutoff(), 3.0);
    }

    /// The new-record mark another segment's record of the last rows' slot
    /// sets stays set when the last rows' own record comes again, so rows
    /// numbered back decode with it as a step back in time.
    #[test]
    fn the_new_record_mark_stays_set_until_rows_are_accepted() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 1, 1);
        assert_eq!(push_rows(&mut parser, 0, 0, 5), PacketOutcome::Rows(5));
        parser.pop_batch().expect("the first batch");
        announce(&mut parser, wire::Metadata::Segment(segment_at(0, 50)));
        announce(
            &mut parser,
            wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
        );

        assert_eq!(push_rows(&mut parser, 0, 2, 1), PacketOutcome::Rows(1));
        assert!(matches!(
            parser
                .pop_batch()
                .expect("the rows numbered back")
                .boundary()
                .and_then(|boundary| boundary.change.as_ref()),
            Some(Change::TimeBackward { .. })
        ));
    }

    /// A closing record opens a restart for the rows of the next segment
    /// only when the device broadcast it. One in a reply does not.
    #[test]
    fn a_closing_record_counts_only_from_a_broadcast() {
        let change = |broadcast: bool| {
            let mut parser = PacketParser::new(true);
            let device = wire::Device {
                protocol: 1,
                ..fixtures::device()
            };
            announce_schema_for(&mut parser, device, &[DataType::F32], 4, 4, 1, 1);
            announce(&mut parser, wire::Metadata::Segment(populated()));
            assert_eq!(push_rows(&mut parser, 0, 0, 5), PacketOutcome::Rows(5));
            parser.pop_batch().expect("the first batch");
            // Rows numbered back leave the slot empty, so a reply may fill it.
            assert_eq!(
                push_rows(&mut parser, 0, 2, 1),
                PacketOutcome::WaitingForMetadata
            );
            let query = take_query(&mut parser);
            let closed = wire::Metadata::Segment(fixtures::segment(STREAM_ID));
            if broadcast {
                announce(&mut parser, closed);
            } else {
                parser.apply_metadata_reply(query, &fixtures::metadata_reply(&[closed]));
            }
            announce(
                &mut parser,
                wire::Metadata::Segment(wire::Segment {
                    flags: wire::SegmentFlags::VALID | wire::SegmentFlags::ACTIVE,
                    ..segment_at(1, 5)
                }),
            );
            assert_eq!(push_rows(&mut parser, 1, 0, 1), PacketOutcome::Rows(1));
            parser
                .pop_batch()
                .expect("the next segment's rows")
                .boundary()
                .and_then(|boundary| boundary.change.clone())
        };
        assert_eq!(
            change(true),
            Some(Change::Restarted {
                old_id: crate::proto::SegmentId::new(0),
                new_id: crate::proto::SegmentId::new(1),
            })
        );
        assert_eq!(change(false), rollover(0, 1));
    }

    /// A reset forgets the stream's records but keeps its current slot, and
    /// the parser asks for that slot's record.
    #[test]
    fn a_reset_keeps_the_slot_it_asks_for() {
        let mut parser = parser_with_clock(&[DataType::F32], 4, 4, 1, 1);
        announce(&mut parser, wire::Metadata::Segment(segment_at(1, 1)));
        parser.reset_subtree(DeviceRoute::root());
        announce(&mut parser, wire::Metadata::Device(fixtures::device()));
        announce(
            &mut parser,
            wire::Metadata::Stream(wire::Stream {
                n_segments: 4,
                ..fixtures::stream(STREAM_ID)
            }),
        );
        announce(
            &mut parser,
            wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
        );

        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 1)]
        );
    }

    /// Rows numbered back in the last rows' slot revise the metadata, also
    /// when earlier rows numbered back already left the slot empty.
    #[test]
    fn rows_numbered_back_revise_the_metadata_while_their_slot_is_empty() {
        let route = DeviceRoute::root();
        let mut parser = parser_with_clock(&[DataType::F32], 4, 2, 1, 1);
        assert_eq!(push_rows(&mut parser, 0, 0, 5), PacketOutcome::Rows(5));
        parser.pop_batch().expect("the first batch");
        assert_eq!(
            push_rows(&mut parser, 0, 2, 1),
            PacketOutcome::WaitingForMetadata
        );
        let before = parser.metadata_revision(route);

        assert_eq!(
            push_rows(&mut parser, 0, 4, 1),
            PacketOutcome::WaitingForMetadata
        );
        assert_ne!(parser.metadata_revision(route), before);
    }

    /// Rows that arrive before their stream's record leave the current slot
    /// as it was.
    #[test]
    fn rows_before_the_stream_record_leave_the_current_slot() {
        let mut parser = PacketParser::new(true);
        announce(&mut parser, wire::Metadata::Device(fixtures::device()));
        assert_eq!(
            push_rows(&mut parser, 2, 0, 1),
            PacketOutcome::WaitingForMetadata
        );
        announce(
            &mut parser,
            wire::Metadata::Stream(wire::Stream {
                n_segments: 4,
                ..fixtures::stream(STREAM_ID)
            }),
        );
        announce(
            &mut parser,
            wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
        );

        assert_eq!(
            take_query(&mut parser).selectors,
            [wire::MetadataSelector::segment(STREAM_ID, 0)]
        );
    }

    /// Rows that wait for a column record name their slot current first.
    #[test]
    fn rows_that_wait_for_a_column_name_their_slot_current() {
        let mut parser = PacketParser::new(true);
        announce(&mut parser, wire::Metadata::Device(fixtures::device()));
        announce(
            &mut parser,
            wire::Metadata::Stream(wire::Stream {
                n_columns: 2,
                n_segments: 2,
                sample_size: 8,
                ..fixtures::stream(STREAM_ID)
            }),
        );
        announce(
            &mut parser,
            wire::Metadata::Segment(fixtures::segment(STREAM_ID)),
        );
        announce(
            &mut parser,
            wire::Metadata::Column(fixtures::column(STREAM_ID, 0, DataType::F32)),
        );
        assert_eq!(
            parser
                .push_packet(checked(&stream_data_packet_in_segment(1, 0, vec![0; 8])))
                .expect("valid rows"),
            PacketOutcome::WaitingForMetadata
        );

        assert_eq!(
            take_query(&mut parser).selectors,
            [
                wire::MetadataSelector::column(STREAM_ID, 1),
                wire::MetadataSelector::segment(STREAM_ID, 1),
            ]
        );
    }

    /// A parser whose bootstrap query is still out, that then took the
    /// device's broadcasts, on a device of revision `protocol` whose stream
    /// has four slots, and five rows of slot 0 under [`populated`].
    fn parser_with_a_late_bootstrap(protocol: u8) -> (PacketParser, MetadataQuery) {
        let mut parser = PacketParser::new(true);
        let bootstrap = take_query(&mut parser);
        assert!(bootstrap.selectors.is_empty(), "a bootstrap query");
        let device = wire::Device {
            protocol,
            ..fixtures::device()
        };
        announce_schema_for(&mut parser, device, &[DataType::F32], 4, 4, 1, 1);
        announce(&mut parser, wire::Metadata::Segment(populated()));
        assert_eq!(push_rows(&mut parser, 0, 0, 5), PacketOutcome::Rows(5));
        parser.pop_batch().expect("the first batch");
        (parser, bootstrap)
    }

    /// A late bootstrap record of another slot than the expected one, one a
    /// start opened, is dropped and leaves no mark. Rows of another segment
    /// after it are a rollover.
    #[test]
    fn a_late_bootstrap_record_about_another_slot_leaves_no_mark() {
        let (mut parser, bootstrap) = parser_with_a_late_bootstrap(1);
        parser.apply_metadata_reply(
            bootstrap,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(wire::Segment {
                flags: wire::SegmentFlags::VALID
                    | wire::SegmentFlags::ACTIVE
                    | wire::SegmentFlags::START,
                ..segment_at(2, 3)
            })]),
        );
        announce(
            &mut parser,
            wire::Metadata::Segment(wire::Segment {
                flags: wire::SegmentFlags::VALID | wire::SegmentFlags::ACTIVE,
                ..segment_at(1, 5)
            }),
        );
        assert_eq!(push_rows(&mut parser, 1, 0, 1), PacketOutcome::Rows(1));
        assert_eq!(
            parser.pop_batch().expect("the rows after").boundary(),
            Some(&Boundary {
                change: rollover(0, 1),
                loss: None,
            })
        );
    }

    /// A late bootstrap record of the expected slot, while the slot holds a
    /// record, is dropped. The rows after decode with the held one.
    #[test]
    fn a_late_bootstrap_record_about_the_expected_slot_keeps_its_held_record() {
        let route = DeviceRoute::root();
        let (mut parser, bootstrap) = parser_with_a_late_bootstrap(0);
        let before = parser.metadata_revision(route);
        parser.apply_metadata_reply(
            bootstrap,
            &fixtures::metadata_reply(&[wire::Metadata::Segment(segment_at(0, 50))]),
        );

        assert_eq!(parser.metadata_revision(route), before);
        assert_eq!(push_rows(&mut parser, 0, 5, 1), PacketOutcome::Rows(1));
        let after = parser.pop_batch().expect("the rows after");
        assert_eq!(after.timestamps(), [6.0], "the end of sample 5 from 0 s");
        assert_eq!(after.boundary(), None);
    }

    #[test]
    fn changed_schema_cannot_reuse_the_previous_run_identity() {
        let mut parser = parser_with_schema(&[DataType::F32], 4);
        let mut buffer = Buffer::new(128);
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the old schema's batch");
        let before_generations = before.generations();
        buffer.process_batch(&before);

        // Contradicting an already-used column invalidates the route. The
        // device then re-announces the complete replacement schema.
        parser
            .push_packet(checked(&metadata_packet(wire::Metadata::Column(
                wire::Column {
                    name: "wide",
                    ..fixtures::column(STREAM_ID, 0, DataType::F64)
                },
            ))))
            .expect("the changed column triggers rediscovery");
        announce_schema(&mut parser, &[DataType::F64], 8, 1, 1, 1);
        parser
            .push_packet(checked(&stream_data_packet(0, vec![0; 8])))
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
                .push_packet(checked(&stream_data_packet(0, vec![0; 4])))
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
            .push_packet(checked(&stream_data_packet(0, vec![0; 7 * rows])))
            .expect("valid rows");
        let first = parser.pop_batch().expect("the first batch");

        let before = crate::model::schema::record_parses();
        for packet in 1..=10 {
            let first_sample = packet * rows as u32;
            parser
                .push_packet(checked(&stream_data_packet(
                    first_sample,
                    vec![0; 7 * rows],
                )))
                .expect("valid rows");
            let batch = parser.pop_batch().expect("a batch per packet");
            assert_eq!(batch.schema().columns().len(), types.len());
            assert!(std::sync::Arc::ptr_eq(batch.schema(), first.schema()));
        }
        assert_eq!(crate::model::schema::record_parses() - before, 0);
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
            .push_packet(checked(&stream_data_packet(
                0,
                first_layout_sample(1.5, -7),
            )))
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
            .push_packet(checked(&stream_data_packet(0, sample)))
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
            .push_packet(checked(&stream_data_packet(
                0,
                first_layout_sample(1.5, -7),
            )))
            .expect("valid rows");
        let before = parser.pop_batch().expect("the first batch").generations();

        parser
            .push_packet(checked(&packet::link(
                LinkStatus::SensorDisconnected,
                DeviceRoute::root(),
            )))
            .expect("the disconnect");
        announce_schema(&mut parser, &[DataType::U16, DataType::F32], 6, 1, 1, 1);
        let mut sample = 513u16.to_le_bytes().to_vec();
        sample.extend_from_slice(&(-0.5f32).to_le_bytes());
        parser
            .push_packet(checked(&stream_data_packet(0, sample)))
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
            .push_packet(checked(&stream_data_packet(0, sample)))
            .expect("valid rows");
        parser.pop_batch().expect("the old layout's batch");

        parser
            .push_packet(checked(&metadata_packet(wire::Metadata::Column(
                wire::Column {
                    name: "col_0",
                    ..fixtures::column(STREAM_ID, 0, DataType::F64)
                },
            ))))
            .expect("the changed column triggers rediscovery");
        announce_schema(&mut parser, &[DataType::F64, DataType::F32], 12, 1, 1, 1);
        let mut sample = 3.5f64.to_le_bytes().to_vec();
        sample.extend_from_slice(&4.0f32.to_le_bytes());
        parser
            .push_packet(checked(&stream_data_packet(0, sample)))
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
            .push_packet(checked(&stream_data_packet(0, samples(&[10, 11]))))
            .expect("valid rows at the full rate");
        let before = parser.pop_batch().expect("the full rate batch");
        assert_eq!(decoded(&before), named(&[("col_0", &[10.0, 11.0])]));

        parser
            .push_packet(checked(&metadata_packet(wire::Metadata::Segment(
                wire::Segment {
                    segment_id: crate::proto::SegmentId::new(1),
                    sampling_rate: 1000,
                    decimation: 10,
                    ..fixtures::segment(STREAM_ID)
                },
            ))))
            .expect("the decimated segment");
        parser
            .push_packet(checked(&stream_data_packet_in_segment(
                1,
                0,
                samples(&[-20, -21]),
            )))
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
            .push_packet(checked(&stream_data_packet(
                0,
                first_layout_sample(1.5, -7),
            )))
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
        parser
            .push_packet(checked(&raw[..len]))
            .expect("an announcement applies");
        parser
            .push_packet(checked(&packet::heartbeat_session(
                fixtures::device().session.value(),
                DeviceRoute::root(),
            )))
            .expect("a heartbeat applies");
        parser
            .push_packet(checked(&stream_data_packet(1, first_layout_sample(2.5, 8))))
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
                    .push_packet(checked(&metadata_packet_at(record, route)))
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
        let mut values: HashMap<DeviceRoute, Vec<Decoded>> = HashMap::new();
        for (route, first, data) in packets {
            let samples = packet::samples(STREAM_ID, 0, first, &data, route);
            parser.push_packet(checked(&samples)).expect("valid rows");
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
                .push_packet(checked(&stream_data_packet(0, data.clone())))
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

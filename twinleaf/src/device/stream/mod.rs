//! The stream pump, one task per connection.
//!
//! The pump drains the session's inlet and keeps metadata discovery going. It
//! sends packets, batches, and events to owned [`Receiver`]s, each filtered
//! to the subtree its view covers.

mod event;
mod metadata;
mod subscription;

pub use event::{DeviceEvent, Event, LinkEvent, NamedRoute};
pub(crate) use metadata::{Completed, MetadataSchedule};
pub(crate) use subscription::Scope;
pub use subscription::{Receiver, RecvError};

use super::session::{Handle, Inlet, RawCallResult};
use crate::data::{DeviceMetadataSnapshot, MetadataQuery, PacketParser, SampleBatch};
use crate::proto::DeviceRoute;
use crate::rt::{sleep_until_some, Supervisor};
use crate::tio;
use crate::tio::packet;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::{self, Future};
use std::pin::Pin;
use std::sync::atomic::{self, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Poll;
use subscription::Sink;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// How many batches a subscriber's queue holds before the pump drops more.
const BATCH_QUEUE_LEN: usize = 1024;

/// How many events a subscriber's queue holds. Only an abandoned receiver
/// fills it.
const EVENT_QUEUE_LEN: usize = 4096;

/// How many packets a raw tap's queue holds, enough for a recorder's disk
/// stall.
const PACKET_QUEUE_LEN: usize = 8192;

/// Returns the RPC table hash from an `rpc.hash` setting, a little-endian
/// `u32`, or `None` for any other setting.
fn rpc_hash(setting: &crate::proto::settings::Setting<'_>) -> Option<u32> {
    if setting.name != b"rpc.hash" {
        return None;
    }
    let value = setting.reply.get(..4)?;
    Some(u32::from_le_bytes(value.try_into().ok()?))
}

/// The pump's state. It holds the parser, the known routes and their
/// metadata revisions, the latest link status per subtree, and the batches
/// and events waiting to go out.
struct StreamState {
    parser: PacketParser,
    known_routes: HashSet<DeviceRoute>,
    metadata_seen: HashMap<DeviceRoute, u32>,
    /// Latest status per subtree, replayed root-first to late subscribers. A new
    /// status supersedes every entry under it.
    link_state: HashMap<DeviceRoute, packet::LinkStatus>,
    batches: VecDeque<SampleBatch>,
    events: VecDeque<Event>,
}

impl StreamState {
    fn new() -> StreamState {
        StreamState {
            parser: PacketParser::new(false),
            known_routes: HashSet::new(),
            metadata_seen: HashMap::new(),
            link_state: HashMap::new(),
            batches: VecDeque::new(),
            events: VecDeque::new(),
        }
    }

    fn pop_batch(&mut self) -> Option<SampleBatch> {
        self.batches.pop_front()
    }

    fn pop_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    fn device_event(&mut self, route: DeviceRoute, event: DeviceEvent) {
        self.events.push_back(Event::Device { route, event });
    }

    /// Queues [`DeviceEvent::Metadata`] when a route's complete metadata is a
    /// revision not yet published.
    fn publish_metadata(&mut self, route: DeviceRoute) {
        let seen = self.metadata_seen.get(&route).copied();
        let Some((revision, snapshot)) = self.parser.revised_metadata(route, seen) else {
            return;
        };
        self.metadata_seen.insert(route, revision);
        self.device_event(route, DeviceEvent::Metadata(snapshot));
    }

    /// Returns every route with complete metadata, with its latest snapshot.
    fn latest_metadata(&self) -> impl Iterator<Item = (DeviceRoute, DeviceMetadataSnapshot)> + '_ {
        self.metadata_seen
            .keys()
            .filter_map(|route| Some((*route, self.parser.metadata(*route)?)))
    }

    /// Brings a new event subscriber up to date with the routes heard, each
    /// subtree's link status, and the latest metadata. Filters them as live
    /// events are. Returns `false` once the subscriber is gone.
    fn replay(&self, sink: &mut Sink<Event>) -> bool {
        self.known_routes.iter().all(|route| {
            sink.offer(Scope::point(*route), || Event::RouteDiscovered {
                route: *route,
            })
        }) && {
            let mut link: Vec<_> = self.link_state.iter().collect();
            link.sort_by_key(|(subtree, _)| subtree.len());
            link.into_iter().all(|(subtree, status)| {
                sink.offer(Scope::subtree(*subtree), || Event::Link {
                    subtree: *subtree,
                    event: LinkEvent::Status(*status),
                })
            })
        } && self.latest_metadata().all(|(route, snapshot)| {
            sink.offer(Scope::point(route), || Event::Device {
                route,
                event: DeviceEvent::Metadata(snapshot),
            })
        })
    }

    /// Clears the metadata of every route in `subtree`. The next session
    /// there discovers it again.
    fn forget_metadata(&mut self, subtree: DeviceRoute) {
        self.metadata_seen
            .retain(|route, _| !route.starts_with(&subtree));
        self.parser.reset_subtree(subtree);
    }

    /// Reports an inlet overflow without resetting the parser. The parser
    /// reports the gaps, and the next heartbeat catches a missed session
    /// change.
    fn input_overrun(&mut self) {
        self.events.push_back(Event::Link {
            subtree: DeviceRoute::root(),
            event: LinkEvent::InputOverrun,
        });
    }

    /// Applies a link's status to the subtree it concerns, the root or one
    /// mount of what the proxy serves.
    fn apply_status(&mut self, subtree: DeviceRoute, status: packet::LinkStatus) {
        self.link_state
            .retain(|root, _| !root.starts_with(&subtree));
        self.link_state.insert(subtree, status);
        self.events.push_back(Event::Link {
            subtree,
            event: LinkEvent::Status(status),
        });
        match status {
            packet::LinkStatus::SensorDisconnected | packet::LinkStatus::Stopped => {
                self.forget_metadata(subtree)
            }
            packet::LinkStatus::SensorReconnected => {
                let refresh: Vec<_> = self
                    .known_routes
                    .iter()
                    .filter(|route| route.starts_with(&subtree))
                    .map(|route| Event::Device {
                        route: *route,
                        event: DeviceEvent::NewHash(None),
                    })
                    .collect();
                self.events.extend(refresh);
            }
            packet::LinkStatus::FailedToConnect
            | packet::LinkStatus::FailedToReconnect
            | packet::LinkStatus::Unknown(_) => {}
        }
    }

    /// Takes one packet. Link statuses, new routes, and control packets are
    /// handled for every route. Heartbeat and setting events and sample
    /// decoding happen only when `covered`.
    fn process_packet(&mut self, pkt: &tio::Packet, covered: bool) {
        if let Some(status) = pkt.link_status() {
            return self.apply_status(pkt.route(), status);
        }
        let route = pkt.route();
        if self.known_routes.insert(route) {
            self.events.push_back(Event::RouteDiscovered { route });
        }

        match pkt.payload() {
            packet::Payload::Heartbeat(beat) => {
                if covered {
                    self.device_event(
                        route,
                        DeviceEvent::Heartbeat {
                            session_id: beat.session(),
                        },
                    );
                }
            }
            packet::Payload::Setting(setting) if covered => {
                if let Some(hash) = rpc_hash(&setting) {
                    self.device_event(route, DeviceEvent::NewHash(Some(hash)));
                }
                if let Some(name) = setting.name_str() {
                    self.device_event(
                        route,
                        DeviceEvent::Setting {
                            name: name.to_string(),
                            reply: setting.reply.to_vec(),
                        },
                    );
                }
            }
            packet::Payload::Samples(_) if !covered => return,
            _ => {}
        }

        if let Err(error) = self.parser.push_packet(pkt) {
            log::warn!("dropping invalid stream packet: {error}");
        }
        while let Some(batch) = self.parser.pop_batch() {
            self.batches.push_back(batch);
        }
        self.publish_metadata(route);
    }
}

/// A subscription a view has minted, on its way to the pump's fan-out lists.
enum PumpSink {
    Batches(Sink<SampleBatch>),
    Events(Sink<Event>),
    Packets(Sink<tio::Packet>),
}

/// The connection's one stream, shared by every view. Its pump starts at the
/// first subscription and owns the session's inlet, the parser, and
/// discovery.
pub(super) struct Stream {
    root: Handle,
    pump: OnceLock<mpsc::UnboundedSender<PumpSink>>,
}

impl Stream {
    /// Creates the stream of the session behind `root`, with no pump yet.
    pub(super) fn new(root: Handle) -> Stream {
        Stream {
            root,
            pump: OnceLock::new(),
        }
    }

    /// Subscribes to the sample batches in `scope`.
    pub(super) fn batches(self: &Arc<Stream>, scope: Scope) -> Receiver<SampleBatch> {
        self.mint(scope, BATCH_QUEUE_LEN, PumpSink::Batches)
    }

    /// Subscribes to the events in `scope`.
    pub(super) fn events(self: &Arc<Stream>, scope: Scope) -> Receiver<Event> {
        self.mint(scope, EVENT_QUEUE_LEN, PumpSink::Events)
    }

    /// Subscribes to the packets in `scope`.
    pub(super) fn packets(self: &Arc<Stream>, scope: Scope) -> Receiver<tio::Packet> {
        self.mint(scope, PACKET_QUEUE_LEN, PumpSink::Packets)
    }

    /// Hands the pump one more sink, starting the pump if this is the first.
    /// A sink the pump does not take is dropped, and its receiver reports
    /// that the stream ended.
    fn mint<T>(
        self: &Arc<Stream>,
        scope: Scope,
        capacity: usize,
        wrap: fn(Sink<T>) -> PumpSink,
    ) -> Receiver<T> {
        let (sink, receiver) = Sink::new(capacity, scope);
        let _ = self.pump().send(wrap(sink));
        receiver.tied_to(self)
    }

    /// Returns the sender of the pump's new sinks, starting the pump on the
    /// first call. Only one pump takes the session's inlet.
    fn pump(&self) -> &mpsc::UnboundedSender<PumpSink> {
        self.pump.get_or_init(|| Pump::start(&self.root))
    }
}

/// The pump itself. It owns the session's inlet, the parser, and the
/// metadata queries in flight, and runs whether or not anything receives
/// from it.
struct Pump {
    inlet: Inlet,
    commands: mpsc::UnboundedReceiver<PumpSink>,
    state: StreamState,
    schedule: MetadataSchedule,
    batches: Vec<Sink<SampleBatch>>,
    events: Vec<Sink<Event>>,
    packets: Vec<Sink<tio::Packet>>,
}

impl Pump {
    /// Takes the session's inlet and starts the task that drains it.
    fn start(root: &Handle) -> mpsc::UnboundedSender<PumpSink> {
        let (commands, received) = mpsc::unbounded_channel();
        let pump = Pump::new(root, root.session().take_inlet(), received);
        Supervisor::new().spawn("twinleaf stream", pump.run());
        commands
    }

    fn new(root: &Handle, inlet: Inlet, commands: mpsc::UnboundedReceiver<PumpSink>) -> Pump {
        Pump {
            state: StreamState::new(),
            schedule: MetadataSchedule::new(root.clone()),
            inlet,
            commands,
            batches: Vec::new(),
            events: Vec::new(),
            packets: Vec::new(),
        }
    }

    /// Runs until the session ends or the stream is dropped. Drains and
    /// discards packets while no sink is live.
    async fn run(mut self) {
        loop {
            let retry = self.schedule.next_retry();
            // A sink takes the packets that arrive after it was made, so new
            // sinks, replies and pruning go ahead of the inlet.
            let running = tokio::select! {
                biased;
                sink = self.commands.recv() => match sink {
                    Some(sink) => {
                        self.add_sink(sink);
                        true
                    }
                    None => false,
                },
                Ok(reply) = self.schedule.replies().recv_async() => {
                    self.take_metadata_replies(reply);
                    true
                }
                () = pruned(&mut self.batches, &mut self.events, &mut self.packets) => true,
                received = self.inlet.receiver().recv_async() => self.take_input(received),
                () = sleep_until_some(retry) => true,
            };
            self.publish();
            if !running {
                return;
            }
            self.submit_metadata_queries();
        }
    }

    fn add_sink(&mut self, sink: PumpSink) {
        match sink {
            PumpSink::Batches(sink) => self.batches.push(sink),
            PumpSink::Events(mut sink) => {
                if self.state.replay(&mut sink) {
                    self.events.push(sink);
                }
            }
            PumpSink::Packets(sink) => self.packets.push(sink),
        }
    }

    /// Returns `true` if any batch or event subscriber covers `route`. Raw
    /// taps do not count.
    fn covers(&self, route: DeviceRoute) -> bool {
        self.batches
            .iter()
            .map(|sink| sink.scope)
            .chain(self.events.iter().map(|sink| sink.scope))
            .any(|scope| scope.covers(route))
    }

    /// Takes `received` from the inlet, and what else the inlet held then.
    /// Returns `false` once the session has ended and its last packet is
    /// taken.
    fn take_input(&mut self, received: Result<tio::Packet, flume::RecvError>) -> bool {
        if self.inlet.overran() {
            self.state.input_overrun();
        }
        let Ok(packet) = received else {
            // The session recorded why it ended before the inlet
            // disconnected. This fence orders that record ahead of the
            // sinks dropping, for receivers that ask after.
            atomic::fence(Ordering::Acquire);
            return false;
        };
        self.take(&packet);
        for _ in 0..self.inlet.receiver().len() {
            let Ok(packet) = self.inlet.receiver().try_recv() else {
                break;
            };
            self.take(&packet);
        }
        true
    }

    /// Takes one packet from the inlet.
    fn take(&mut self, packet: &tio::Packet) {
        self.tap(packet);
        if let Some(packet::LinkStatus::SensorDisconnected | packet::LinkStatus::Stopped) =
            packet.link_status()
        {
            self.schedule.forget(packet.route());
        }
        let covered = self.covers(packet.route());
        self.state.process_packet(packet, covered);
    }

    /// Offers a packet to the raw taps, unparsed and with its absolute route.
    /// A status reaches every tap its subtree overlaps.
    fn tap(&mut self, packet: &tio::Packet) {
        if self.packets.is_empty() {
            return;
        }
        let scope = match packet.link_status() {
            Some(_) => Scope::subtree(packet.route()),
            None => Scope::point(packet.route()),
        };
        self.packets
            .retain_mut(|sink| sink.offer(scope, || packet.clone()));
    }

    /// Takes `first` and every other finished `dev.metadata` call. Publishes
    /// the metadata each completed, and reports the routes whose firmware
    /// has no `dev.metadata`.
    fn take_metadata_replies(&mut self, first: (MetadataQuery, RawCallResult)) {
        let mut reply = Some(first);
        while let Some((query, result)) = reply
            .take()
            .or_else(|| self.schedule.replies().try_recv().ok())
        {
            let route = query.route;
            match self
                .schedule
                .complete(&mut self.state.parser, query, result)
            {
                Completed::Applied => self.state.publish_metadata(route),
                Completed::Unsupported => self
                    .state
                    .device_event(route, DeviceEvent::MetadataUnavailable),
                Completed::BackedOff => {}
            }
        }
    }

    /// Asks each covered route that is due for whatever metadata the parser
    /// still wants.
    fn submit_metadata_queries(&mut self) {
        let due = self
            .schedule
            .due(self.state.known_routes.iter().copied(), Instant::now());
        for route in due {
            if self.covers(route) {
                self.schedule.submit(&mut self.state.parser, route);
            }
        }
    }

    fn publish(&mut self) {
        while let Some(batch) = self.state.pop_batch() {
            let scope = Scope::point(batch.route());
            self.batches
                .retain_mut(|sink| sink.offer(scope, || batch.clone()));
        }
        while let Some(event) = self.state.pop_event() {
            let scope = event.scope();
            self.events
                .retain_mut(|sink| sink.offer(scope, || event.clone()));
        }
        self.batches.retain_mut(Sink::report_lag);
        self.events.retain_mut(Sink::report_lag);
        self.packets.retain_mut(Sink::report_lag);
    }
}

/// Waits until a subscriber drops its receiver, then drops the sink of each
/// subscriber that has.
async fn pruned(
    batches: &mut Vec<Sink<SampleBatch>>,
    events: &mut Vec<Sink<Event>>,
    packets: &mut Vec<Sink<tio::Packet>>,
) {
    future::poll_fn(|cx| {
        let held = batches.len() + events.len() + packets.len();
        // A lifeline resolves only once its receiver is gone, and its sink
        // is dropped before it is polled again.
        batches.retain_mut(|sink| Pin::new(&mut sink.alive).poll(cx).is_pending());
        events.retain_mut(|sink| Pin::new(&mut sink.alive).poll(cx).is_pending());
        packets.retain_mut(|sink| Pin::new(&mut sink.alive).poll(cx).is_pending());
        match batches.len() + events.len() + packets.len() < held {
            true => Poll::Ready(()),
            false => Poll::Pending,
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::session::{Session, TestProxy};
    use crate::proto::data as wire;
    use crate::proto::data::DataType;
    use crate::proto::rpc as wire_rpc;
    use crate::proto::sync::Epoch;
    use crate::proto::SessionId as WireSessionId;
    use crate::tio::packet::Packet;
    use crate::tio::transport;
    use std::time::{Duration, Instant};

    /// The whole tree, as a root tree view covers it.
    fn everything() -> Scope {
        Scope {
            route: DeviceRoute::root(),
            depth: DeviceRoute::MAX_HOPS,
        }
    }

    fn exactly(route: &str) -> Scope {
        Scope {
            route: route.parse().expect("a valid route"),
            depth: 0,
        }
    }

    /// A pump serving `sinks` over a session whose socket is a pair of
    /// channels: the subscription lane, the session's far end, and the root
    /// handle on the session.
    fn pump_with(sinks: Vec<PumpSink>) -> (mpsc::UnboundedSender<PumpSink>, TestProxy, Handle) {
        let (session, proxy) = Session::test_pair();
        let root = Handle::root(session);
        let subscriptions = start_pump(&root, root.session().take_inlet(), sinks);
        (subscriptions, proxy, root)
    }

    /// Start a pump on `inlet` serving `sinks`, and return its subscription
    /// lane.
    fn start_pump(
        root: &Handle,
        inlet: Inlet,
        sinks: Vec<PumpSink>,
    ) -> mpsc::UnboundedSender<PumpSink> {
        let (subscriptions, received) = mpsc::unbounded_channel();
        let mut pump = Pump::new(root, inlet, received);
        for sink in sinks {
            pump.add_sink(sink);
        }
        Supervisor::new().spawn("twinleaf stream", pump.run());
        subscriptions
    }

    fn pump(first: PumpSink) -> (mpsc::UnboundedSender<PumpSink>, TestProxy, Handle) {
        pump_with(vec![first])
    }

    /// One more subscription on a pump that is already running.
    fn subscribe<T>(
        subscriptions: &mpsc::UnboundedSender<PumpSink>,
        scope: Scope,
        wrap: fn(Sink<T>) -> PumpSink,
    ) -> Receiver<T> {
        let (sink, receiver) = Sink::new(EVENT_QUEUE_LEN, scope);
        subscriptions
            .send(wrap(sink))
            .expect("the pump takes the subscription");
        receiver
    }

    /// A stream over a session whose socket is a pair of channels, and the
    /// session's far end.
    fn stream() -> (Arc<Stream>, TestProxy) {
        let (session, proxy) = Session::test_pair();
        (Arc::new(Stream::new(Handle::root(session))), proxy)
    }

    /// Whether the session wrote anything within `within`.
    fn wrote_within(proxy: &TestProxy, within: Duration) -> bool {
        proxy.sent.recv_timeout(within).is_ok()
    }

    fn samples(first: u32, route: DeviceRoute) -> Packet {
        Packet::samples(1, 0, first, &[0; 4], route).expect("valid samples")
    }

    /// One SETTING announcement, as a device sends it.
    fn announcement(name: &str, reply: &[u8], route: DeviceRoute) -> Packet {
        let mut raw = [0u8; 64];
        let len = crate::proto::settings::Setting {
            name: name.as_bytes(),
            flags: 0,
            reply,
        }
        .write(&mut raw)
        .expect("the announcement fits");
        Packet::from_slice_prefix(&raw[..len])
            .expect("a valid packet")
            .0
            .with_route(route)
    }

    /// The four records that let the parser decode one f32 stream, as the
    /// device in `session` describes them.
    fn metadata_records(session: u32) -> [wire::Metadata<'static>; 4] {
        [
            wire::Metadata::Device(wire::Device {
                session: WireSessionId::new(session),
                n_streams: 1,
                name: "d",
                serial: "s",
                firmware: "f",
            }),
            wire::Metadata::Stream(wire::Stream {
                stream_id: crate::proto::StreamId::new(1),
                n_columns: 1,
                n_segments: 2,
                sample_size: 4,
                buf_samples: 128,
                name: "stream",
            }),
            wire::Metadata::Segment(segment_record(0)),
            wire::Metadata::Column(wire::Column {
                stream_id: crate::proto::StreamId::new(1),
                index: crate::proto::ColumnId::new(0),
                data_type: DataType::F32,
                name: "col",
                units: "",
                description: "",
            }),
        ]
    }

    /// The stream's segment `segment_id`, starting one second per segment in.
    fn segment_record(segment_id: u8) -> wire::Segment<'static> {
        wire::Segment {
            stream_id: crate::proto::StreamId::new(1),
            segment_id: crate::proto::SegmentId::new(segment_id),
            flags: wire::SegmentFlags::default(),
            epoch: Epoch::UNIX,
            timeref_serial: "clock",
            timeref_session: WireSessionId::new(7),
            start_time: u32::from(segment_id),
            sampling_rate: 1,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: wire::FilterType::NONE,
        }
    }

    fn broadcast(proxy: &TestProxy, record: wire::Metadata<'_>, route: DeviceRoute) {
        proxy.send(
            Packet::metadata(record, wire::MetadataFlags::UPDATE, route).expect("one record fits"),
        );
    }

    fn describe(proxy: &TestProxy, route: DeviceRoute, session: u32) {
        for record in metadata_records(session) {
            broadcast(proxy, record, route);
        }
    }

    /// The current segment of the one stream a snapshot describes.
    fn current_segment(snapshot: &DeviceMetadataSnapshot) -> u8 {
        snapshot
            .stream(crate::proto::StreamId::new(1))
            .expect("the described stream")
            .segment()
            .segment_id
            .value()
    }

    /// The snapshot an event carries, if it carries one.
    fn snapshot(event: &Event) -> Option<&DeviceMetadataSnapshot> {
        match event {
            Event::Device {
                event: DeviceEvent::Metadata(snapshot),
                ..
            } => Some(snapshot),
            _ => None,
        }
    }

    /// The `dev.metadata` call the pump submitted: its route, its args, and
    /// its id.
    fn metadata_call(proxy: &TestProxy) -> (DeviceRoute, Vec<u8>, u16) {
        let (route, name, args, id) = proxy.call();
        assert_eq!(name, wire::METADATA_RPC_METHOD);
        (route, args, id)
    }

    /// Take events until one matches, or panic rather than block forever.
    fn wait_for(events: &Receiver<Event>, wanted: impl Fn(&Event) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while let Ok(event) = events.recv_deadline(deadline) {
            if wanted(&event) {
                return;
            }
        }
        panic!("the pump never published the event the test waited for");
    }

    /// One `dev.metadata` reply frame describing a one-stream device.
    fn device_reply() -> Vec<u8> {
        let mut record = [0u8; 64];
        let len = metadata_records(1)[0]
            .write_reply_frame(&mut record)
            .expect("one record fits");
        record[..len].to_vec()
    }

    #[test]
    fn a_paused_consumer_never_stalls_the_pump_and_learns_it_lagged() {
        let (sink, batches) = Sink::new(2, everything());
        let (marks, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_subscriptions, proxy, handle) =
            pump_with(vec![PumpSink::Batches(sink), PumpSink::Events(marks)]);
        let root = DeviceRoute::root();
        describe(&proxy, root, 1);

        // Nothing is receiving, and the subscriber has room for two batches:
        // a pump that only ran while a consumer pulled would stop there.
        let mut sample_number = 0..;
        for first in sample_number.by_ref().take(4096) {
            proxy.send(samples(first, root));
        }
        proxy.send(announcement("flood.end", b"", root));

        // An RPC issued while the pump is buried in samples still completes.
        let pending = handle.submit(root, "dev.name", b"");
        let (route, id) = loop {
            let (route, name, _, id) = proxy.call();
            if name == "dev.name" {
                break (route, id);
            }
        };
        proxy.reply(id, b"ASM", route);
        assert_eq!(
            pending
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            b"ASM"
        );

        // The consumer stays paused until the pump has taken every sample,
        // which it has once it publishes the announcement sent after them.
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Device {
                    event: DeviceEvent::Setting { name, .. },
                    ..
                } if name == "flood.end"
            )
        });
        let mut lagged = 0u64;
        for first in sample_number.take(100) {
            proxy.send(samples(first, root));
            match batches.recv_timeout(Duration::from_secs(1)) {
                Ok(_) => {}
                Err(RecvError::Lagged(skipped)) => {
                    lagged += skipped;
                    break;
                }
                Err(_) => break,
            }
        }
        assert!(lagged > 0, "the consumer is told how much it missed");
    }

    #[test]
    fn metadata_discovery_completes_over_independent_calls() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        let child: DeviceRoute = "/1".parse().unwrap();
        proxy.send(samples(0, DeviceRoute::root()));
        proxy.send(samples(0, child));

        // Both routes bootstrap at once; answering them out of order must still
        // reach the right parser state.
        let first = metadata_call(&proxy);
        let second = metadata_call(&proxy);
        assert_ne!(first.0, second.0, "one query per route");
        assert!(first.1.is_empty() && second.1.is_empty(), "both bootstrap");
        proxy.reply(second.2, &device_reply(), second.0);
        proxy.reply(first.2, &device_reply(), first.0);

        let wanted: Vec<Vec<u8>> = (0..2).map(|_| metadata_call(&proxy).1).collect();
        assert!(
            wanted
                .iter()
                .all(|args| *args == wire::MetadataSelector::stream(1).encode()),
            "each reply reached its own route, which now wants only its stream"
        );
        drop(events);
    }

    /// A segment UPDATE replaces the current segment of a route that never
    /// stopped being complete, so completeness alone would miss the change.
    #[test]
    fn a_live_segment_rollover_republishes_metadata_and_replays_the_latest() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (handle, proxy, _root) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        describe(&proxy, root, 1);
        wait_for(&events, |event| {
            snapshot(event).is_some_and(|snapshot| current_segment(snapshot) == 0)
        });

        // Firmware announces the current output segment. Event subscribers
        // must see it even when no sample packet follows.
        broadcast(&proxy, wire::Metadata::Segment(segment_record(1)), root);
        wait_for(&events, |event| {
            snapshot(event).is_some_and(|snapshot| current_segment(snapshot) == 1)
        });

        let late = subscribe(&handle, everything(), PumpSink::Events);
        wait_for(&late, |event| {
            snapshot(event).is_some_and(|snapshot| current_segment(snapshot) == 1)
        });
    }

    /// A session change makes the route rediscover. Subscribers must be told
    /// what it found instead of keeping what the old session said.
    #[test]
    fn a_new_session_republishes_metadata_once_the_route_rediscovers() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        describe(&proxy, root, 1);
        wait_for(&events, |event| {
            snapshot(event).is_some_and(|snapshot| snapshot.device().session.value() == 1)
        });

        proxy.send(Packet::heartbeat_session(2, root));
        describe(&proxy, root, 2);
        wait_for(&events, |event| {
            snapshot(event).is_some_and(|snapshot| snapshot.device().session.value() == 2)
        });
    }

    /// Devices re-broadcast their metadata. Saying again what a subscriber
    /// already has is not news.
    #[test]
    fn re_broadcasting_identical_metadata_does_not_republish_it() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        describe(&proxy, root, 1);
        wait_for(&events, |event| snapshot(event).is_some());

        describe(&proxy, root, 1);
        let deadline = Instant::now() + Duration::from_millis(500);
        while let Ok(event) = events.recv_deadline(deadline) {
            assert!(
                snapshot(&event).is_none(),
                "identical metadata was republished"
            );
        }
    }

    #[test]
    fn a_route_without_dev_metadata_is_asked_once() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        proxy.send(samples(0, DeviceRoute::root()));

        let (route, _, id) = metadata_call(&proxy);
        proxy.refuse(id, wire_rpc::RpcError::NotFound, route);

        let deadline = Instant::now() + Duration::from_secs(5);
        let reported = std::iter::from_fn(|| events.recv_deadline(deadline).ok()).any(|event| {
            matches!(
                event,
                Event::Device {
                    event: DeviceEvent::MetadataUnavailable,
                    ..
                }
            )
        });
        assert!(reported, "the refusal is reported, not retried forever");
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "discovery does not re-arm for a route with no dev.metadata"
        );
        drop(proxy);
    }

    #[test]
    fn a_disconnect_terminates_every_receiver() {
        let (sink, batches) = Sink::new(BATCH_QUEUE_LEN, everything());
        let (handle, proxy, _root) = pump(PumpSink::Batches(sink));
        let events = subscribe(&handle, everything(), PumpSink::Events);

        // The socket closes, which ends the session for good.
        drop(proxy);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match batches.recv_deadline(deadline) {
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Disconnected) => break,
                Err(RecvError::Stopped) => panic!("a closed socket is no stop"),
                Err(RecvError::Timeout) => panic!("the batches outlived the session"),
            }
        }
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(5)),
            Err(RecvError::Disconnected)
        ));
    }

    #[test]
    fn an_inlet_overrun_is_reported_and_the_stream_goes_on() {
        let (session, proxy) = Session::test_pair();
        let handle = Handle::root(session);
        let inlet = handle.session().take_inlet();
        let overflow = inlet.overflow_marker();
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let _subscriptions = start_pump(&handle, inlet, vec![PumpSink::Events(sink)]);
        let root = DeviceRoute::root();
        proxy.send(samples(0, root));
        let (route, _, id) = metadata_call(&proxy);
        proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Device {
                    event: DeviceEvent::MetadataUnavailable,
                    ..
                }
            )
        });

        // The session dropped what the inlet had no room for, and the next
        // packet wakes the pump to say so.
        overflow();
        proxy.send(samples(1, root));
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Link {
                    event: LinkEvent::InputOverrun,
                    ..
                }
            )
        });

        proxy.send(samples(2, root));
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "the overrun did not forget that this route has no dev.metadata"
        );
        let child: DeviceRoute = "/1".parse().unwrap();
        proxy.send(samples(0, child));
        wait_for(
            &events,
            |event| matches!(event, Event::RouteDiscovered { route } if *route == child),
        );
    }

    /// A stop at the root is the session's last packet. The pump publishes
    /// it, then ends, though the socket is still open. Every receiver says
    /// the proxy stopped, even one that carries no statuses and one that
    /// subscribed after.
    #[test]
    fn a_stop_at_the_root_is_published_then_ends_the_stream() {
        let (stream, proxy) = stream();
        let events = stream.events(everything());
        let batches = stream.batches(everything());

        proxy.send(Packet::link(packet::LinkStatus::Stopped));

        wait_for(&events, |event| {
            matches!(
                event,
                Event::Link {
                    subtree,
                    event: LinkEvent::Status(packet::LinkStatus::Stopped),
                } if subtree.is_empty()
            )
        });
        let wait = Duration::from_secs(5);
        assert_eq!(events.recv_timeout(wait).err(), Some(RecvError::Stopped));
        assert_eq!(batches.recv_timeout(wait).err(), Some(RecvError::Stopped));
        let late = stream.batches(everything());
        assert_eq!(late.recv_timeout(wait).err(), Some(RecvError::Stopped));
        drop(proxy);
    }

    /// The status saying why the link failed may be the last thing the
    /// session queues, so the pump must publish what the inlet still holds
    /// before ending.
    #[test]
    fn the_last_packets_a_session_queued_are_still_published() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        proxy.send(Packet::link(packet::LinkStatus::FailedToReconnect));
        drop(proxy);

        wait_for(&events, |event| {
            matches!(
                event,
                Event::Link {
                    event: LinkEvent::Status(packet::LinkStatus::FailedToReconnect),
                    ..
                }
            )
        });
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(5)),
            Err(RecvError::Disconnected)
        ));
    }

    /// The pump ends once nothing can reach it any more: no view left to
    /// subscribe, and no receiver left holding the stream open.
    #[test]
    fn dropping_the_last_view_and_receiver_ends_the_pump() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (handle, proxy, root) = pump(PumpSink::Events(sink));

        drop((handle, events, root));

        // A silent device gives the pump nothing else to wake on, so it has to
        // wake on the subscription lane itself going away. Once the pump is
        // gone nothing holds the session, which closes its socket.
        assert!(matches!(
            proxy.sent.recv_timeout(Duration::from_secs(5)),
            Err(flume::RecvTimeoutError::Disconnected)
        ));
    }

    /// A refusal answering a query the reset already overtook belongs to the
    /// session that ended, and must not condemn the route discovering now.
    #[test]
    fn a_refusal_from_before_a_reset_does_not_condemn_the_route() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        proxy.send(samples(0, root));
        let (stale_route, _, stale) = metadata_call(&proxy);

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Link {
                    event: LinkEvent::Status(packet::LinkStatus::SensorDisconnected),
                    ..
                }
            )
        });
        proxy.refuse(stale, wire_rpc::RpcError::NotFound, stale_route);

        let child: DeviceRoute = "/1".parse().unwrap();
        proxy.send(samples(0, child));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match events
                .recv_deadline(deadline)
                .expect("the pump keeps going")
            {
                Event::RouteDiscovered { route } if route == child => break,
                Event::Device {
                    event: DeviceEvent::MetadataUnavailable,
                    ..
                } => panic!("the stale refusal condemned a route that had moved on"),
                Event::RouteDiscovered { .. } | Event::Link { .. } | Event::Device { .. } => {}
            }
        }
    }

    /// Link facts belong to the transport, so a device view hears them. Another
    /// device's facts are not its business.
    #[test]
    fn a_device_view_hears_link_facts_but_not_another_routes_device_facts() {
        let (wide, _all) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (narrow, device) = Sink::new(EVENT_QUEUE_LEN, exactly("/1"));
        let (_handle, proxy, _root) =
            pump_with(vec![PumpSink::Events(wide), PumpSink::Events(narrow)]);
        let root = DeviceRoute::root();
        let child: DeviceRoute = "/1".parse().unwrap();

        // The wide view covers the root, so the root's device facts are
        // produced; only the filter keeps them from the device view.
        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        proxy.send(Packet::heartbeat(root));
        proxy.send(Packet::heartbeat(child));

        let mut heard_status = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match device
                .recv_deadline(deadline)
                .expect("the device view keeps receiving")
            {
                Event::Link {
                    event: LinkEvent::Status(packet::LinkStatus::SensorDisconnected),
                    ..
                } => heard_status = true,
                Event::Link { .. } => {}
                Event::RouteDiscovered { route } => {
                    assert_ne!(route, root, "the device view heard another route's fact")
                }
                Event::Device { route, event } => {
                    assert_ne!(route, root, "the device view heard another route's fact");
                    if matches!(event, DeviceEvent::Heartbeat { .. }) {
                        break;
                    }
                }
            }
        }
        assert!(heard_status, "the device view heard the link's own status");
    }

    /// A status carries a fabricated route, since `FailedToConnect` means
    /// nothing is there, so it must never mint a device. A proxy that predates
    /// `PROXY_RX` sends it as a legacy status.
    #[test]
    fn a_status_packet_discovers_no_route() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        proxy.send(Packet::legacy_status(packet::LinkStatus::FailedToConnect));
        proxy.send(Packet::heartbeat(root));

        // Events publish in arrival order: a status that discovered its route
        // would announce it before the link fact, not after.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut heard_status = false;
        loop {
            match events.recv_deadline(deadline).expect("the pump publishes") {
                Event::Link {
                    event: LinkEvent::Status(packet::LinkStatus::FailedToConnect),
                    ..
                } => heard_status = true,
                Event::RouteDiscovered { route } => {
                    assert_eq!(route, root, "only the heartbeat's route exists");
                    assert!(heard_status, "the status discovered its own route");
                    break;
                }
                _ => {}
            }
        }
    }

    /// Behind `tio proxy --mount`, a status arrives on its mount's prefix: one
    /// sensor bouncing redescribes its own subtree and no one else's.
    #[test]
    fn a_mounts_disconnect_resets_only_its_own_subtree() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        let steady: DeviceRoute = "/1".parse().unwrap();
        let bounced: DeviceRoute = "/2".parse().unwrap();
        describe(&proxy, steady, 1);
        describe(&proxy, bounced, 1);
        wait_for(
            &events,
            |event| matches!(event, Event::Device { route, event: DeviceEvent::Metadata(_) } if *route == bounced),
        );

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected).with_route(bounced));
        wait_for(
            &events,
            |event| matches!(event, Event::Link { subtree, event: LinkEvent::Status(packet::LinkStatus::SensorDisconnected) } if *subtree == bounced),
        );

        // Both re-broadcast; only the bounced mount's description is news.
        describe(&proxy, steady, 1);
        describe(&proxy, bounced, 1);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Event::Device {
                route,
                event: DeviceEvent::Metadata(_),
            } = events.recv_deadline(deadline).expect("the pump publishes")
            {
                assert_ne!(route, steady, "the steady mount was reset too");
                if route == bounced {
                    break;
                }
            }
        }

        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected).with_route(bounced));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Event::Device {
                route,
                event: DeviceEvent::NewHash(None),
            } = events.recv_deadline(deadline).expect("the pump publishes")
            {
                assert_ne!(route, steady, "the steady mount was asked to refresh");
                if route == bounced {
                    break;
                }
            }
        }
    }

    /// A mount that stopped on purpose ends its session as a disconnected one
    /// does: its own subtree redescribes and no one else's does.
    #[test]
    fn a_mounts_stop_resets_only_its_own_subtree() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        let steady: DeviceRoute = "/1".parse().unwrap();
        let stopped: DeviceRoute = "/2".parse().unwrap();
        describe(&proxy, steady, 1);
        describe(&proxy, stopped, 1);
        wait_for(
            &events,
            |event| matches!(event, Event::Device { route, event: DeviceEvent::Metadata(_) } if *route == stopped),
        );

        proxy.send(Packet::link(packet::LinkStatus::Stopped).with_route(stopped));
        wait_for(
            &events,
            |event| matches!(event, Event::Link { subtree, event: LinkEvent::Status(packet::LinkStatus::Stopped) } if *subtree == stopped),
        );

        // Both re-broadcast, and only the stopped mount's description is news.
        describe(&proxy, steady, 1);
        describe(&proxy, stopped, 1);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Event::Device {
                route,
                event: DeviceEvent::Metadata(_),
            } = events.recv_deadline(deadline).expect("the pump publishes")
            {
                assert_ne!(route, steady, "the steady mount was reset too");
                if route == stopped {
                    break;
                }
            }
        }
    }

    /// A proxy announces a mount's status as a link packet, and one that
    /// predates `PROXY_RX` announces it as a legacy status. The pump hears the
    /// same event from either.
    #[test]
    fn a_link_packet_is_the_same_event_as_a_legacy_status() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Events(sink));
        let mount: DeviceRoute = "/2".parse().unwrap();
        let heard = |status: Packet| {
            proxy.send(status.with_route(mount));
            match events
                .recv_timeout(Duration::from_secs(5))
                .expect("the pump publishes")
            {
                Event::Link {
                    subtree,
                    event: LinkEvent::Status(status),
                } => (subtree, status),
                other => panic!("expected the link's status, got {other:?}"),
            }
        };

        let legacy = heard(Packet::legacy_status(
            packet::LinkStatus::SensorDisconnected,
        ));
        let link = heard(Packet::link(packet::LinkStatus::SensorDisconnected));

        assert_eq!(legacy, (mount, packet::LinkStatus::SensorDisconnected));
        assert_eq!(link, legacy);
    }

    /// The affected subtree contains the view, not the other way around: a
    /// view pinned below a mount still hears that mount's status, and a view
    /// on another mount hears nothing.
    #[test]
    fn a_view_below_the_affected_subtree_hears_its_status() {
        let (a, below) = Sink::new(EVENT_QUEUE_LEN, exactly("/1/0"));
        let (b, elsewhere) = Sink::new(EVENT_QUEUE_LEN, exactly("/2"));
        let (_handle, proxy, _root) = pump_with(vec![PumpSink::Events(a), PumpSink::Events(b)]);
        let mount: DeviceRoute = "/1".parse().unwrap();
        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected).with_route(mount));

        wait_for(
            &below,
            |event| matches!(event, Event::Link { subtree, event: LinkEvent::Status(packet::LinkStatus::SensorReconnected) } if *subtree == mount),
        );
        assert!(
            matches!(
                elsewhere.recv_timeout(Duration::from_millis(200)),
                Err(RecvError::Timeout)
            ),
            "another mount's status is not this view's business"
        );
    }

    /// A raw tap sees the wire, absolutely routed and filtered to its scope,
    /// and nothing the engine itself needs to run. Status filters as the
    /// subtree at its route: it is a recorded stream's reset marker.
    #[test]
    fn a_packet_tap_is_filtered_absolute_and_passive() {
        let (wide, taps) = Sink::new(PACKET_QUEUE_LEN, everything());
        let (narrow, scoped) = Sink::new(PACKET_QUEUE_LEN, exactly("/1"));
        let (_handle, proxy, _root) =
            pump_with(vec![PumpSink::Packets(wide), PumpSink::Packets(narrow)]);
        let root = DeviceRoute::root();
        let child: DeviceRoute = "/1".parse().unwrap();

        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected));
        proxy.send(samples(0, root));
        proxy.send(samples(7, child));

        let status = taps.recv_timeout(Duration::from_secs(5)).expect("a packet");
        assert!(status.link_status().is_some());
        let first = taps.recv_timeout(Duration::from_secs(5)).expect("a packet");
        assert_eq!(first.route(), root);
        assert!(matches!(first.payload(), packet::Payload::Samples(_)));
        assert_eq!(
            taps.recv_timeout(Duration::from_secs(5))
                .expect("a packet")
                .route(),
            child
        );

        let status = scoped.recv_timeout(Duration::from_secs(5)).expect("packet");
        assert!(
            status.link_status().is_some(),
            "the whole link's status reaches a tap scoped inside it"
        );
        let only = scoped.recv_timeout(Duration::from_secs(5)).expect("packet");
        assert_eq!(only.route(), child, "a tap only sees what its scope covers");
        assert!(matches!(
            scoped.recv_timeout(Duration::from_millis(200)),
            Err(RecvError::Timeout)
        ));
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "a raw tap decodes nothing and discovers nothing"
        );
    }

    /// Link state is a level, not an edge: a subscriber that joins during an
    /// outage learns what it did not watch happen, and converges with one
    /// that watched.
    #[test]
    fn a_late_subscriber_replays_the_link_state_it_did_not_watch() {
        let (sink, early) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (handle, proxy, _root) = pump(PumpSink::Events(sink));
        let mount: DeviceRoute = "/2".parse().unwrap();
        proxy.send(samples(0, mount));
        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected).with_route(mount));
        wait_for(&early, |event| {
            matches!(
                event,
                Event::Link {
                    event: LinkEvent::Status(packet::LinkStatus::SensorDisconnected),
                    ..
                }
            )
        });

        let late = subscribe(&handle, everything(), PumpSink::Events);
        let first = late.recv_timeout(Duration::from_secs(5)).expect("replay");
        assert!(matches!(first, Event::RouteDiscovered { route } if route == mount));
        let second = late.recv_timeout(Duration::from_secs(5)).expect("replay");
        assert!(
            matches!(second, Event::Link { subtree, event: LinkEvent::Status(packet::LinkStatus::SensorDisconnected) } if subtree == mount)
        );

        // A view pinned below the mount has no business with the route fact,
        // but its own link state still reaches it.
        let pinned = subscribe(&handle, exactly("/2/1"), PumpSink::Events);
        assert!(matches!(
            pinned.recv_timeout(Duration::from_secs(5)).expect("replay"),
            Event::Link { subtree, event: LinkEvent::Status(packet::LinkStatus::SensorDisconnected) } if subtree == mount
        ));
    }

    /// A status supersedes the stored state of every subtree it covers, so a
    /// stale narrower fact never replays past the broader one that ended it.
    #[test]
    fn a_broader_status_supersedes_the_narrower_state_it_covers() {
        let (sink, early) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (handle, proxy, _root) = pump(PumpSink::Events(sink));
        let mount: DeviceRoute = "/2".parse().unwrap();
        proxy.send(samples(0, mount));
        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected).with_route(mount));
        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected));
        wait_for(&early, |event| {
            matches!(
                event,
                Event::Link {
                    event: LinkEvent::Status(packet::LinkStatus::SensorReconnected),
                    ..
                }
            )
        });

        let late = subscribe(&handle, everything(), PumpSink::Events);
        let deadline = Instant::now() + Duration::from_millis(500);
        let replayed: Vec<Event> =
            std::iter::from_fn(|| late.recv_deadline(deadline).ok()).collect();
        assert!(replayed.iter().any(|event| matches!(
            event,
            Event::Link { subtree, event: LinkEvent::Status(packet::LinkStatus::SensorReconnected) } if *subtree == DeviceRoute::root()
        )));
        assert!(
            !replayed.iter().any(|event| matches!(
                event,
                Event::Link {
                    event: LinkEvent::Status(packet::LinkStatus::SensorDisconnected),
                    ..
                }
            )),
            "the mount's stale disconnect outlived the reconnect that covered it"
        );
    }

    #[test]
    fn a_route_no_view_covers_is_neither_decoded_nor_asked_for_metadata() {
        let (sink, device) = Sink::new(EVENT_QUEUE_LEN, exactly("/1"));
        let (handle, proxy, _root) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        let elsewhere: DeviceRoute = "/2".parse().unwrap();
        describe(&proxy, root, 1);
        for first in 0..2 {
            proxy.send(samples(first, root));
        }
        proxy.send(samples(0, elsewhere));

        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "a route outside every view is never asked for metadata"
        );

        // The new view covers everything, so the route nothing covered
        // bootstraps — which the pump only does once the sink is in its lists.
        let batches = subscribe(&handle, everything(), PumpSink::Batches);
        let (route, args, _) = metadata_call(&proxy);
        assert_eq!(route, elsewhere);
        assert!(args.is_empty(), "the newly covered route bootstraps");

        proxy.send(samples(2, root));
        let batch = batches
            .recv_timeout(Duration::from_secs(5))
            .expect("a covered route decodes from the metadata it announced");
        assert_eq!(batch.sample_numbers(), [2]);
        assert!(
            batch.is_initial(),
            "the samples that arrived uncovered were never decoded"
        );
        drop(device);
    }

    #[test]
    fn overlapping_views_share_one_inlet_and_one_discovery() {
        let (stream, proxy) = stream();
        let child: DeviceRoute = "/1".parse().unwrap();
        let events = stream.events(everything());
        let batches = stream.batches(exactly("/1"));

        proxy.send(samples(0, DeviceRoute::root()));
        proxy.send(samples(0, child));

        let first = metadata_call(&proxy);
        let second = metadata_call(&proxy);
        assert_ne!(first.0, second.0, "one bootstrap per route, not per view");
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "the second view started no second discovery"
        );
        drop((events, batches));
    }

    /// A receiver is the subscription: it keeps the pump running on its own,
    /// so a caller need not hold the view it was minted from.
    #[test]
    fn a_receiver_keeps_the_pump_running_without_its_view() {
        let (stream, proxy) = stream();
        let events = stream.events(everything());
        drop(stream);

        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Device {
                    event: DeviceEvent::Heartbeat { .. },
                    ..
                }
            )
        });
    }

    /// A channel consults a deadline only once it runs dry, so a lane that
    /// stays saturated would deliver items past the caller's bound forever.
    #[test]
    fn a_saturated_subscription_still_honours_an_elapsed_deadline() {
        let overrun = || Event::Link {
            subtree: DeviceRoute::root(),
            event: LinkEvent::InputOverrun,
        };
        let (mut sink, receiver) = Sink::new(2, everything());
        assert!(sink.send(overrun()));
        assert!(sink.send(overrun()));

        assert!(matches!(
            receiver.recv_deadline(Instant::now()),
            Err(RecvError::Timeout)
        ));
        assert!(
            receiver.recv().is_ok(),
            "the bound reports the time, it does not shed the queue"
        );
    }

    /// A stream whose session already ended hands back receivers that say
    /// so, rather than ones that wait on a pump that will never run. With no
    /// stop heard, that is only that the stream ended.
    #[test]
    fn subscribing_to_an_ended_session_yields_a_disconnected_receiver() {
        let (stream, proxy) = stream();
        drop(proxy);

        assert!(matches!(
            stream
                .events(everything())
                .recv_timeout(Duration::from_secs(5)),
            Err(RecvError::Disconnected)
        ));
        assert!(matches!(
            stream
                .batches(everything())
                .recv_timeout(Duration::from_secs(5)),
            Err(RecvError::Disconnected)
        ));
    }

    /// Every announcement reaches the views covering the device that sent it.
    #[test]
    fn an_announcement_becomes_an_event_on_its_own_route() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_subscriptions, proxy, _root) = pump(PumpSink::Events(sink));
        let route: DeviceRoute = "/1".parse().expect("a valid route");
        proxy.send(announcement("test.amplitude", &[1, 2, 3, 4], route));

        wait_for(&events, |event| {
            matches!(
                event,
                Event::Device {
                    route: from,
                    event: DeviceEvent::Setting { name, reply },
                } if *from == route && name == "test.amplitude" && reply == &[1, 2, 3, 4]
            )
        });
    }

    /// The table hash travels as an announcement, so it is heard twice: as the
    /// refresh signal, and as the announcement it is.
    #[test]
    fn the_table_hash_is_both_a_new_hash_and_an_announcement() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_subscriptions, proxy, _root) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        proxy.send(announcement("rpc.hash", &7u32.to_le_bytes(), root));

        wait_for(&events, |event| {
            matches!(
                event,
                Event::Device {
                    event: DeviceEvent::NewHash(Some(7)),
                    ..
                }
            )
        });
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Device {
                    event: DeviceEvent::Setting { name, .. },
                    ..
                } if name == "rpc.hash"
            )
        });
    }

    /// A receiver dropped while the device is silent is pruned at once. A
    /// route only it covered, heard afterwards, is not asked for metadata
    /// until a live receiver covers it.
    #[tokio::test(start_paused = true)]
    async fn a_receiver_dropped_while_the_stream_is_silent_is_pruned() {
        let tasks = Supervisor::current();
        let (session, proxy) = Session::test_pair_on(&tasks, transport::DEFAULT_RX_CHANNEL_SIZE);
        let root = Handle::root(session);
        let (subscriptions, commands) = mpsc::unbounded_channel();
        let pump = Pump::new(&root, root.session().take_inlet(), commands);
        tasks.spawn("twinleaf stream", pump.run());
        let child: DeviceRoute = "/1".parse().unwrap();
        let batches = subscribe(&subscriptions, exactly("/1"), PumpSink::Batches);

        drop(batches);
        tokio::time::sleep(Duration::from_millis(1)).await;
        proxy.send(samples(0, child));
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(
            proxy.sent.is_empty(),
            "a route only a dropped receiver covered was asked for metadata"
        );
        let _covering = subscribe(&subscriptions, exactly("/1"), PumpSink::Batches);
        let query = proxy.sent.recv_async().await.unwrap();
        assert_eq!(query.route(), child);
    }

    /// A route whose metadata query failed is asked again when its backoff
    /// ends on the clock the pump sleeps on, and not before.
    #[tokio::test(start_paused = true)]
    async fn a_failed_metadata_query_is_asked_again_when_its_backoff_ends() {
        let tasks = Supervisor::current();
        let (session, proxy) = Session::test_pair_on(&tasks, transport::DEFAULT_RX_CHANNEL_SIZE);
        let root = Handle::root(session);
        let (subscriptions, commands) = mpsc::unbounded_channel();
        let pump = Pump::new(&root, root.session().take_inlet(), commands);
        tasks.spawn("twinleaf stream", pump.run());
        let _events = subscribe(&subscriptions, everything(), PumpSink::Events);
        proxy.send(samples(0, DeviceRoute::root()));

        let asked = proxy.sent.recv_async().await.unwrap();
        let packet::Payload::RpcRequest(request) = asked.payload() else {
            panic!("expected a metadata query, got {:?}", asked.payload());
        };
        proxy.refuse(request.id.value(), wire_rpc::RpcError::Busy, asked.route());
        let refused = tokio::time::Instant::now();

        let again = proxy.sent.recv_async().await.unwrap();
        assert_eq!(again.route(), asked.route());
        assert_eq!(refused.elapsed(), metadata::RETRY_FIRST);
    }
}

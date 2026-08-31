//! The stream pump: one private thread per connection that drains its data
//! port, keeps metadata discovery going, and fans packets, batches and events
//! out to owned receivers, each filtered to the subtree its view covers.

mod event;
mod subscription;

pub use event::{DeviceEvent, Event, LinkEvent, NamedRoute, TreeEvent};
pub use subscription::{Receiver, RecvError};
pub(crate) use subscription::Scope;
use subscription::Sink;

use crate::data::{DeviceMetadataSnapshot, MetadataQuery, PacketParser, SampleBatch};
use crate::device::RpcMethod;
use crate::tio;
use crate::tio::proto::{self, DeviceRoute};
use crate::tio::proxy;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel;
use twinleaf_proto::data as wire;
use twinleaf_proto::rpc as wire_rpc;

/// Batches a subscriber may fall behind by before the pump sheds them.
const BATCH_QUEUE_LEN: usize = 1024;

/// Events a subscriber may fall behind by. Events are rare next to samples, so
/// this is deep enough that only an abandoned receiver reaches it.
const EVENT_QUEUE_LEN: usize = 4096;

/// Packets a raw tap may fall behind by. A tap sees every packet its scope
/// covers, so this is deep enough to ride out a recorder's disk stall.
const PACKET_QUEUE_LEN: usize = 8192;

/// How long a first subscription keeps asking for the pump's port while the
/// proxy's command lane is congested, and how often it asks. A pump replacing a
/// port it lost keeps asking for as long as the worker lives, so it has no
/// budget: congestion is momentary, and only a stopped worker is terminal.
const REGISTER_BUDGET: Duration = Duration::from_secs(2);
const REGISTER_POLL: Duration = Duration::from_millis(50);

/// Wait before the first metadata retry, and the ceiling it doubles up to.
const METADATA_RETRY_FIRST: Duration = Duration::from_millis(250);
const METADATA_RETRY_MAX: Duration = Duration::from_secs(8);

/// The one setting a host reads as more than opaque bytes: its value is the
/// device's RPC table hash, a u32le.
fn rpc_hash(setting: &twinleaf_proto::settings::Setting<'_>) -> Option<u32> {
    if setting.name != b"rpc.hash" {
        return None;
    }
    let value = setting.reply.get(..4)?;
    Some(u32::from_le_bytes(value.try_into().ok()?))
}

/// Why a route is not being asked for metadata right now.
enum Discovery {
    /// The last query failed; ask again once `until` passes.
    Backoff { until: Instant, delay: Duration },
    /// A backed-off route that has come due: eligible again, and remembering
    /// the delay the next failure doubles.
    Due { delay: Duration },
    /// The device answered `NotFound`: its firmware has no `dev.metadata`.
    Unsupported,
}

/// Everything the connection's stream needs to stay live: the parser, the
/// routes known and the metadata revision last seen for each, and where
/// discovery stands.
struct StreamState {
    parser: PacketParser,
    known_routes: HashSet<DeviceRoute>,
    metadata_seen: HashMap<DeviceRoute, u32>,
    discovery: HashMap<DeviceRoute, Discovery>,
    batches: VecDeque<SampleBatch>,
    events: VecDeque<Event>,
}

impl StreamState {
    fn new() -> StreamState {
        StreamState {
            parser: PacketParser::new(DeviceRoute::root(), false),
            known_routes: HashSet::new(),
            metadata_seen: HashMap::new(),
            discovery: HashMap::new(),
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

    /// Emit [`DeviceEvent::Metadata`] whenever a route's complete metadata is a
    /// revision not yet seen. Every transition to completeness bumps the
    /// revision, so an unseen one is the only moment a snapshot can be news.
    fn publish_metadata(&mut self, route: DeviceRoute) {
        let Some(revision) = self.parser.metadata_revision(route) else {
            return;
        };
        if self.metadata_seen.insert(route, revision) == Some(revision) {
            return;
        }
        let Some(snapshot) = self.parser.metadata(route) else {
            return;
        };
        self.device_event(route, DeviceEvent::Metadata(snapshot));
    }

    /// Every route with complete metadata, at its latest snapshot.
    fn latest_metadata(&self) -> impl Iterator<Item = (DeviceRoute, DeviceMetadataSnapshot)> + '_ {
        self.metadata_seen
            .keys()
            .filter_map(|route| Some((*route, self.parser.metadata(*route)?)))
    }

    /// Bring a new event subscriber up to date with what the pump already
    /// knows, filtered exactly as live events are.
    fn replay(&self, sink: &mut Sink<Event>) -> bool {
        self.known_routes.iter().all(|route| {
            sink.offer(Scope::point(*route), || Event::Tree {
                route: *route,
                event: TreeEvent::RouteDiscovered,
            })
        }) && self.latest_metadata().all(|(route, snapshot)| {
            sink.offer(Scope::point(route), || Event::Device {
                route,
                event: DeviceEvent::Metadata(snapshot),
            })
        })
    }

    /// Routes whose metadata may be asked for now, expiring the backoffs that
    /// bring them due so no elapsed deadline is left to wake the pump again.
    fn take_due_routes(&mut self, now: Instant) -> Vec<DeviceRoute> {
        let due: Vec<_> = self
            .known_routes
            .iter()
            .copied()
            .filter(|route| match self.discovery.get(route) {
                None | Some(Discovery::Due { .. }) => true,
                Some(Discovery::Backoff { until, .. }) => *until <= now,
                Some(Discovery::Unsupported) => false,
            })
            .collect();
        for route in &due {
            if let Some(Discovery::Backoff { delay, .. }) = self.discovery.get(route) {
                self.discovery
                    .insert(*route, Discovery::Due { delay: *delay });
            }
        }
        due
    }

    /// The earliest moment a backed-off route wants to be asked again.
    fn next_retry(&self) -> Option<Instant> {
        self.discovery
            .values()
            .filter_map(|state| match state {
                Discovery::Backoff { until, .. } => Some(*until),
                Discovery::Due { .. } | Discovery::Unsupported => None,
            })
            .min()
    }

    fn apply_metadata_reply(&mut self, query: MetadataQuery, reply: &[u8]) {
        let route = query.route;
        self.parser.apply_metadata_reply(query, reply);
        self.discovery.remove(&route);
        self.publish_metadata(route);
    }

    /// Give up on a query and hold its route off for a growing delay. A query
    /// a reset or a new session already overtook says nothing about what the
    /// route is discovering now, so it holds nothing off.
    fn back_off(&mut self, query: MetadataQuery) {
        let route = query.route;
        if !self.parser.fail_metadata_query(query) {
            return;
        }
        let delay = match self.discovery.get(&route) {
            None => METADATA_RETRY_FIRST,
            Some(Discovery::Backoff { delay, .. }) | Some(Discovery::Due { delay }) => {
                (*delay * 2).min(METADATA_RETRY_MAX)
            }
            Some(Discovery::Unsupported) => return,
        };
        self.discovery.insert(
            route,
            Discovery::Backoff {
                until: Instant::now() + delay,
                delay,
            },
        );
    }

    /// Stop asking a route whose firmware does not implement `dev.metadata`.
    fn give_up(&mut self, query: MetadataQuery) {
        let route = query.route;
        if !self.parser.fail_metadata_query(query) {
            return;
        }
        if matches!(
            self.discovery.insert(route, Discovery::Unsupported),
            Some(Discovery::Unsupported)
        ) {
            return;
        }
        log::warn!("{route} has no dev.metadata; its streams cannot be decoded");
        self.device_event(route, DeviceEvent::MetadataUnavailable);
    }

    /// Forget what the current session taught us about `subtree`, so the next
    /// session there rediscovers.
    fn forget_metadata(&mut self, subtree: DeviceRoute) {
        self.metadata_seen
            .retain(|route, _| subtree.relative_route(route).is_err());
        self.discovery
            .retain(|route, _| subtree.relative_route(route).is_err());
        self.parser.reset_subtree(subtree);
    }

    /// The inlet overflowed. Only the loss itself is news: the packets that
    /// vanished leave sample-number gaps the parser already reports, a missed
    /// segment update self-heals on the next id mismatch, and a missed session
    /// change is caught by the next heartbeat. Resetting here would turn
    /// sustained overload into an endless rediscovery storm.
    fn input_overrun(&mut self) {
        self.events.push_back(Event::Link {
            subtree: DeviceRoute::root(),
            event: LinkEvent::InputOverrun,
        });
    }

    /// Apply a link's status to the subtree it concerns: everything on a
    /// direct connection, one mount behind a `tio proxy --mount` fan-in, which
    /// rewrites the status onto its mount prefix.
    fn apply_status(&mut self, subtree: DeviceRoute, status: proto::ProxyStatus) {
        self.events.push_back(Event::Link {
            subtree,
            event: LinkEvent::Status(status),
        });
        match status {
            proto::ProxyStatus::SensorDisconnected => self.forget_metadata(subtree),
            proto::ProxyStatus::SensorReconnected => {
                let refresh: Vec<_> = self
                    .known_routes
                    .iter()
                    .filter(|route| subtree.relative_route(route).is_ok())
                    .map(|route| Event::Device {
                        route: *route,
                        event: DeviceEvent::NewHash(None),
                    })
                    .collect();
                self.events.extend(refresh);
            }
            proto::ProxyStatus::FailedToConnect
            | proto::ProxyStatus::FailedToReconnect
            | proto::ProxyStatus::Unknown(_) => {}
        }
    }

    /// Take one packet. Route identity reaches every view, and every control
    /// packet reaches the parser whatever its route — a session change missed
    /// on an uncovered route would leave a stale snapshot to replay as fresh.
    /// Only sample decoding waits on coverage. A status proves nothing about
    /// its route — `FailedToConnect` means the device is not there — so it is
    /// applied before any of that.
    fn process_packet(&mut self, pkt: &tio::Packet, covered: bool) {
        if let proto::Payload::ProxyStatus(status) = pkt.payload() {
            return self.apply_status(pkt.route(), status);
        }
        let route = pkt.route();
        if self.known_routes.insert(route) {
            self.events.push_back(Event::Tree {
                route,
                event: TreeEvent::RouteDiscovered,
            });
        }

        match pkt.payload() {
            proto::Payload::RpcUpdate(method) => {
                if covered {
                    self.device_event(
                        route,
                        DeviceEvent::RpcInvalidated(RpcMethod::from_wire(method)),
                    );
                }
                return;
            }
            proto::Payload::Heartbeat(beat) => {
                if covered {
                    self.device_event(
                        route,
                        DeviceEvent::Heartbeat {
                            session_id: beat.session(),
                        },
                    );
                }
            }
            proto::Payload::Setting(setting) => {
                if let (true, Some(hash)) = (covered, rpc_hash(&setting)) {
                    self.device_event(route, DeviceEvent::NewHash(Some(hash)));
                }
            }
            proto::Payload::Samples(_) if !covered => return,
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

/// The connection's one stream: a pump thread started at the first
/// subscription and running until the last view of the connection is gone.
/// Every view minted from the connection subscribes here, so they share a
/// port, a parser, and one discovery.
pub(super) struct Stream {
    root: proxy::RpcEndpoint,
    pump: OnceLock<channel::Sender<PumpSink>>,
}

impl Stream {
    pub(super) fn new(root: proxy::RpcEndpoint) -> Stream {
        Stream {
            root,
            pump: OnceLock::new(),
        }
    }

    pub(super) fn batches(self: &Arc<Stream>, scope: Scope) -> Receiver<SampleBatch> {
        self.mint(scope, BATCH_QUEUE_LEN, PumpSink::Batches)
    }

    pub(super) fn events(self: &Arc<Stream>, scope: Scope) -> Receiver<Event> {
        self.mint(scope, EVENT_QUEUE_LEN, PumpSink::Events)
    }

    pub(super) fn packets(self: &Arc<Stream>, scope: Scope) -> Receiver<tio::Packet> {
        self.mint(scope, PACKET_QUEUE_LEN, PumpSink::Packets)
    }

    /// Hand the pump one more sink, starting it if this is the first. A sink
    /// no pump takes is dropped here, so its receiver reports `Disconnected`
    /// on the first receive instead of waiting on a stream nothing feeds.
    fn mint<T>(
        self: &Arc<Stream>,
        scope: Scope,
        capacity: usize,
        wrap: fn(Sink<T>) -> PumpSink,
    ) -> Receiver<T> {
        let (sink, receiver) = Sink::new(capacity, scope);
        if let Some(pump) = self.pump() {
            let _ = pump.send(wrap(sink));
        }
        receiver.tied_to(self)
    }

    /// The running pump's command lane, started by the first subscription that
    /// finds none. A start that failed memoizes nothing: only a stopped proxy
    /// worker is terminal, and every later subscription tries again.
    fn pump(&self) -> Option<&channel::Sender<PumpSink>> {
        if let Some(pump) = self.pump.get() {
            return Some(pump);
        }
        let started = Pump::start(&self.root)?;
        // Two first subscriptions can race here; the pump whose lane loses the
        // cell finds it disconnected and stops.
        Some(self.pump.get_or_init(|| started))
    }
}

/// The pump itself: it owns the data port, the parser, and the metadata queries
/// in flight, and runs whether or not anything is receiving from it.
struct Pump {
    root: proxy::RpcEndpoint,
    data: proxy::Port,
    commands: channel::Receiver<PumpSink>,
    commands_open: bool,
    state: StreamState,
    metadata_calls: Vec<(MetadataQuery, channel::Receiver<proxy::RawCallResult>)>,
    batches: Vec<Sink<SampleBatch>>,
    events: Vec<Sink<Event>>,
    packets: Vec<Sink<tio::Packet>>,
}

impl Pump {
    /// Open the connection's one data port and start the thread that drains
    /// it, or `None` when no port could be had: there is nothing to serve, and
    /// nothing is remembered.
    fn start(root: &proxy::RpcEndpoint) -> Option<channel::Sender<PumpSink>> {
        let data = Pump::register(root)?;
        let (commands, received) = channel::unbounded();
        let pump = Pump {
            state: StreamState::new(),
            root: root.clone(),
            data,
            commands: received,
            commands_open: true,
            metadata_calls: Vec::new(),
            batches: Vec::new(),
            events: Vec::new(),
            packets: Vec::new(),
        };
        thread::Builder::new()
            .name("twinleaf-stream".into())
            .spawn(move || pump.run())
            .expect("failed to spawn the stream pump thread");
        Some(commands)
    }

    /// Ask the worker for the data port, waiting out a command lane that is
    /// merely congested: it is shared with every RPC, so a busy instant says
    /// nothing about whether the connection has a stream to give.
    fn register(root: &proxy::RpcEndpoint) -> Option<proxy::Port> {
        let deadline = Instant::now() + REGISTER_BUDGET;
        loop {
            let error = match root.open_port(true, true) {
                Ok(data) => return Some(data),
                Err(proxy::PortError::ProxyBusy) if Instant::now() < deadline => {
                    thread::sleep(REGISTER_POLL);
                    continue;
                }
                Err(error) => error,
            };
            log::warn!("the stream pump could not open its port: {error}");
            return None;
        }
    }

    /// The pump outlives every subscription: it ends only when the link does,
    /// or when the last view that could subscribe is gone. With no live sinks
    /// it drains its port and discards, which coverage gating makes cheap.
    fn run(mut self) {
        loop {
            self.drain_commands();
            let connected = self.drain_input() && self.link_alive();
            self.publish();
            if !connected || !self.commands_open {
                return;
            }
            self.submit_metadata_queries();
            self.wait();
        }
    }

    fn drain_commands(&mut self) {
        while self.commands_open {
            match self.commands.try_recv() {
                Ok(sink) => self.add_sink(sink),
                Err(channel::TryRecvError::Empty) => return,
                Err(channel::TryRecvError::Disconnected) => self.commands_open = false,
            }
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

    /// True if any live view decodes `route`; only those routes are decoded and
    /// asked for metadata. Raw taps are passive — they neither decode nor
    /// discover — so their scopes are not counted here.
    fn covers(&self, route: DeviceRoute) -> bool {
        self.batches
            .iter()
            .map(|sink| sink.scope)
            .chain(self.events.iter().map(|sink| sink.scope))
            .any(|scope| scope.covers(route))
    }

    /// Take what the port had queued on entry and no more, so a device that
    /// keeps writing cannot starve new subscriptions or publishing.
    ///
    /// False once the proxy link is gone.
    fn drain_input(&mut self) -> bool {
        self.drain_metadata_replies();
        for _ in 0..self.data.receiver().len().max(1) {
            match self.data.try_recv() {
                Ok(packet) => {
                    self.tap(&packet);
                    let covered = self.covers(packet.route());
                    self.state.process_packet(&packet, covered);
                }
                Err(proxy::RecvError::WouldBlock) => return true,
                Err(proxy::RecvError::ProxyDisconnected) => return self.reopen(),
            }
        }
        true
    }

    /// Offer a packet to the raw taps, before anything parses it and with the
    /// absolute route it arrived with. RPC invalidations are the engine's own
    /// input, not device data, so they stop here; a status concerns the whole
    /// subtree at its route and is a recorded stream's own reset marker, so
    /// every tap that subtree touches hears it.
    fn tap(&mut self, packet: &tio::Packet) {
        if self.packets.is_empty() || matches!(packet.payload(), proto::Payload::RpcUpdate(_)) {
            return;
        }
        let scope = match packet.payload() {
            proto::Payload::ProxyStatus(_) => Scope::subtree(packet.route()),
            _ => Scope::point(packet.route()),
        };
        self.packets
            .retain_mut(|sink| sink.offer(scope, || packet.clone()));
    }

    /// False once the worker has stopped and its port has run dry, so the last
    /// packets it queued — the status saying why it stopped among them — are
    /// still published.
    fn link_alive(&self) -> bool {
        !self.data.receiver().is_empty() || self.worker_alive()
    }

    /// False once the proxy worker has stopped. A port it never adopted keeps
    /// its own channel alive, so only the worker's lifeline reports this.
    fn worker_alive(&self) -> bool {
        !matches!(
            self.root.worker_alive().try_recv(),
            Err(channel::TryRecvError::Disconnected)
        )
    }

    /// The port died. While the worker lives, the drop was this client falling
    /// behind — loss to report rather than an end — so registration is retried
    /// for as long as the worker is there to answer it. A congested command
    /// lane is never terminal; a stopped worker, and nothing left to subscribe,
    /// are.
    fn reopen(&mut self) -> bool {
        loop {
            if !self.worker_alive() {
                return false;
            }
            match self.root.open_port(true, true) {
                Ok(data) => {
                    self.data = data;
                    self.state.input_overrun();
                    return true;
                }
                Err(proxy::PortError::ProxyBusy) => {}
                Err(proxy::PortError::FailedNewClientSetup)
                | Err(proxy::PortError::RpcTimeoutTooShort)
                | Err(proxy::PortError::RpcTimeoutTooLong) => return false,
            }
            self.drain_commands();
            if !self.commands_open {
                return false;
            }
            thread::sleep(REGISTER_POLL);
        }
    }

    fn drain_metadata_replies(&mut self) {
        let mut waiting = Vec::with_capacity(self.metadata_calls.len());
        for (query, pending) in std::mem::take(&mut self.metadata_calls) {
            match pending.try_recv() {
                Ok(result) => self.complete_metadata(query, result),
                Err(channel::TryRecvError::Empty) => waiting.push((query, pending)),
                Err(channel::TryRecvError::Disconnected) => {
                    self.complete_metadata(query, Err(proxy::RawCallError::ProxyClosed))
                }
            }
        }
        self.metadata_calls = waiting;
    }

    /// Apply a finished `dev.metadata` call, or decide when to ask again.
    fn complete_metadata(&mut self, query: MetadataQuery, result: proxy::RawCallResult) {
        match result {
            Ok(reply) => self.state.apply_metadata_reply(query, &reply),
            Err(proxy::RawCallError::Device {
                error: wire_rpc::RpcError::NotFound,
                ..
            }) => self.state.give_up(query),
            Err(proxy::RawCallError::Device { .. })
            | Err(proxy::RawCallError::InvalidRoute(_))
            | Err(proxy::RawCallError::RequestNotSubmitted)
            | Err(proxy::RawCallError::Timeout)
            | Err(proxy::RawCallError::DeviceDisconnected)
            | Err(proxy::RawCallError::ProxyClosed) => self.state.back_off(query),
        }
    }

    /// Ask each covered route that is due for whatever metadata the parser
    /// still wants.
    fn submit_metadata_queries(&mut self) {
        let due: Vec<_> = self
            .state
            .take_due_routes(Instant::now())
            .into_iter()
            .filter(|route| self.covers(*route))
            .collect();
        for route in due {
            for query in self.state.parser.take_metadata_queries_for(route) {
                match self
                    .root
                    .submit(route, wire::METADATA_RPC_METHOD, &query.args())
                {
                    Ok(pending) => self.metadata_calls.push((query, pending)),
                    Err(error) => self.complete_metadata(query, Err(error)),
                }
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
        self.batches
            .retain_mut(|sink| sink.report_lag() && sink.is_live());
        self.events
            .retain_mut(|sink| sink.report_lag() && sink.is_live());
        self.packets
            .retain_mut(|sink| sink.report_lag() && sink.is_live());
    }

    /// Sleep until something the pump cares about happens.
    fn wait(&self) {
        let mut select = channel::Select::new();
        select.recv(self.data.receiver());
        for (_, pending) in &self.metadata_calls {
            select.recv(pending);
        }
        for sink in &self.batches {
            select.recv(&sink.alive);
        }
        for sink in &self.events {
            select.recv(&sink.alive);
        }
        for sink in &self.packets {
            select.recv(&sink.alive);
        }
        if self.commands_open {
            select.recv(&self.commands);
        }
        select.recv(self.root.worker_alive());
        match self.state.next_retry() {
            Some(deadline) => {
                let _ = select.ready_deadline(deadline);
            }
            None => {
                select.ready();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tio::proto::{DataType, Packet};
    use crate::tio::proxy_core::ProxyCommand;
    use twinleaf_proto::rpc::Method;
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::SessionId as WireSessionId;

    /// The whole tree, as a root tree view covers it.
    fn everything() -> Scope {
        Scope {
            route: DeviceRoute::root(),
            depth: twinleaf_proto::MAX_ROUTING_SIZE,
        }
    }

    fn exactly(route: &str) -> Scope {
        Scope {
            route: route.parse().expect("a valid route"),
            depth: 0,
        }
    }

    /// A pump serving `sinks` over a port with no proxy behind it: the lane new
    /// subscriptions take, the far end that delivers packets to it, the far end
    /// that answers its RPCs, and the worker lifeline the test holds until it
    /// wants the proxy to look gone.
    fn pump_with(
        sinks: Vec<PumpSink>,
    ) -> (
        channel::Sender<PumpSink>,
        channel::Sender<Packet>,
        channel::Receiver<ProxyCommand>,
        proxy::RpcEndpoint,
        channel::Sender<()>,
    ) {
        let (endpoint, commands, worker) =
            proxy::RpcEndpoint::test_pair(DeviceRoute::root(), twinleaf_proto::MAX_ROUTING_SIZE);
        let (data, _sent, deliver) = proxy::Port::test_pair();
        let (subscriptions, received) = channel::unbounded();
        let mut pump = Pump {
            state: StreamState::new(),
            root: endpoint.clone(),
            data,
            commands: received,
            commands_open: true,
            metadata_calls: Vec::new(),
            batches: Vec::new(),
            events: Vec::new(),
            packets: Vec::new(),
        };
        for sink in sinks {
            pump.add_sink(sink);
        }
        thread::spawn(move || pump.run());
        (subscriptions, deliver, commands, endpoint, worker)
    }

    fn pump(
        first: PumpSink,
    ) -> (
        channel::Sender<PumpSink>,
        channel::Sender<Packet>,
        channel::Receiver<ProxyCommand>,
        proxy::RpcEndpoint,
        channel::Sender<()>,
    ) {
        pump_with(vec![first])
    }

    /// One more subscription on a pump that is already running.
    fn subscribe<T>(
        subscriptions: &channel::Sender<PumpSink>,
        scope: Scope,
        wrap: fn(Sink<T>) -> PumpSink,
    ) -> Receiver<T> {
        let (sink, receiver) = Sink::new(EVENT_QUEUE_LEN, scope);
        subscriptions
            .send(wrap(sink))
            .expect("the pump takes the subscription");
        receiver
    }

    /// A stream with no proxy behind it: the far end receiving the port it
    /// opens and the RPCs it submits, and the worker's lifeline.
    fn stream() -> (
        Arc<Stream>,
        proxy::RpcEndpoint,
        channel::Receiver<ProxyCommand>,
        channel::Sender<()>,
    ) {
        let (endpoint, commands, worker) =
            proxy::RpcEndpoint::test_pair(DeviceRoute::root(), twinleaf_proto::MAX_ROUTING_SIZE);
        (
            Arc::new(Stream::new(endpoint.clone())),
            endpoint,
            commands,
            worker,
        )
    }

    /// The far end delivering packets to the port the pump has just opened.
    fn opened_port(commands: &channel::Receiver<ProxyCommand>) -> channel::Sender<Packet> {
        loop {
            match commands
                .recv_timeout(Duration::from_secs(5))
                .expect("the pump opens a port")
            {
                ProxyCommand::OpenPort { client } => return client.test_channels().0,
                ProxyCommand::Call { .. } => {}
            }
        }
    }

    fn samples(first: u32, route: DeviceRoute) -> Packet {
        Packet::samples(1, 0, first, &[0; 4], route).expect("valid samples")
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
                stream_id: twinleaf_proto::StreamId::new(1),
                n_columns: 1,
                n_segments: 2,
                sample_size: 4,
                buf_samples: 128,
                name: "stream",
            }),
            wire::Metadata::Segment(segment_record(0)),
            wire::Metadata::Column(wire::Column {
                stream_id: twinleaf_proto::StreamId::new(1),
                index: twinleaf_proto::ColumnId::new(0),
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
            stream_id: twinleaf_proto::StreamId::new(1),
            segment_id: twinleaf_proto::SegmentId::new(segment_id),
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

    fn broadcast(
        deliver: &channel::Sender<Packet>,
        record: wire::Metadata<'_>,
        route: DeviceRoute,
    ) {
        deliver
            .send(
                Packet::metadata(record, wire::MetadataFlags::UPDATE, route)
                    .expect("one record fits"),
            )
            .expect("the pump is listening");
    }

    fn describe(deliver: &channel::Sender<Packet>, route: DeviceRoute, session: u32) {
        for record in metadata_records(session) {
            broadcast(deliver, record, route);
        }
    }

    /// The current segment of the one stream a snapshot describes.
    fn current_segment(snapshot: &DeviceMetadataSnapshot) -> u8 {
        snapshot
            .stream(twinleaf_proto::StreamId::new(1))
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

    /// The `dev.metadata` call the pump submitted: its route, its args, and the
    /// one-shot that answers it.
    fn metadata_call(
        commands: &channel::Receiver<ProxyCommand>,
    ) -> (DeviceRoute, Vec<u8>, channel::Sender<proxy::RawCallResult>) {
        let ProxyCommand::Call {
            request, result, ..
        } = commands
            .recv_timeout(Duration::from_secs(5))
            .expect("the pump submits the query")
        else {
            panic!("expected a direct RPC command");
        };
        let route = request.route();
        let proto::Payload::RpcRequest(rpc) = request.payload() else {
            panic!("expected an RPC request");
        };
        assert_eq!(
            rpc.method,
            Method::ByName(wire::METADATA_RPC_METHOD.as_bytes())
        );
        assert_eq!(
            rpc.id.value(),
            0,
            "the proxy, not the pump, allocates request ids"
        );
        (route, rpc.args.to_vec(), result)
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
        let (_handle, deliver, commands, endpoint, _worker) = pump(PumpSink::Batches(sink));
        let root = DeviceRoute::root();
        describe(&deliver, root, 1);

        // Nothing is receiving, and the port's own queue is far smaller than
        // this: a pump that only ran while a consumer pulled would block here.
        let mut sample_number = 0..;
        for first in sample_number.by_ref().take(4096) {
            deliver
                .send_timeout(samples(first, root), Duration::from_secs(5))
                .expect("the pump keeps draining while nothing receives");
        }

        // An RPC issued while the pump is buried in samples still completes.
        let pending = endpoint
            .submit(root, "dev.name", b"")
            .expect("the endpoint accepts the call");
        let result = loop {
            let ProxyCommand::Call {
                request, result, ..
            } = commands
                .recv_timeout(Duration::from_secs(5))
                .expect("the call reaches the proxy")
            else {
                panic!("expected a direct RPC command");
            };
            let proto::Payload::RpcRequest(rpc) = request.payload() else {
                panic!("expected an RPC request");
            };
            if rpc.method == Method::ByName(b"dev.name") {
                break result;
            }
        };
        result.send(Ok(b"ASM".to_vec())).unwrap();
        assert_eq!(
            pending
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            b"ASM"
        );

        let mut lagged = 0u64;
        for first in sample_number.take(100) {
            let _ = deliver.send_timeout(samples(first, root), Duration::from_secs(5));
            match batches.recv_timeout(Duration::from_secs(1)) {
                Ok(_) => {}
                Err(RecvError::Lagged(skipped)) => {
                    lagged += skipped;
                    break;
                }
                Err(RecvError::Timeout) | Err(RecvError::Disconnected) => break,
            }
        }
        assert!(lagged > 0, "the consumer is told how much it missed");
    }

    #[test]
    fn metadata_discovery_completes_over_independent_calls() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, deliver, commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let child: DeviceRoute = "/1".parse().unwrap();
        deliver.send(samples(0, DeviceRoute::root())).unwrap();
        deliver.send(samples(0, child)).unwrap();

        // Both routes bootstrap at once; answering them out of order must still
        // reach the right parser state.
        let first = metadata_call(&commands);
        let second = metadata_call(&commands);
        assert_ne!(first.0, second.0, "one query per route");
        assert!(first.1.is_empty() && second.1.is_empty(), "both bootstrap");
        second.2.send(Ok(device_reply())).unwrap();
        first.2.send(Ok(device_reply())).unwrap();

        let wanted: Vec<Vec<u8>> = (0..2).map(|_| metadata_call(&commands).1).collect();
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
        let (handle, deliver, _commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        describe(&deliver, root, 1);
        wait_for(&events, |event| {
            snapshot(event).is_some_and(|snapshot| current_segment(snapshot) == 0)
        });

        // Firmware announces the current output segment. Event subscribers
        // must see it even when no sample packet follows.
        broadcast(&deliver, wire::Metadata::Segment(segment_record(1)), root);
        wait_for(&events, |event| {
            snapshot(event).is_some_and(|snapshot| current_segment(snapshot) == 1)
        });

        let late = subscribe(&handle, everything(), PumpSink::Events);
        wait_for(&late, |event| {
            snapshot(event).is_some_and(|snapshot| current_segment(snapshot) == 1)
        });
    }

    /// A session change makes the route rediscover; subscribers must be told
    /// what it found instead of keeping what the old session said.
    #[test]
    fn a_new_session_republishes_metadata_once_the_route_rediscovers() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, deliver, _commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        describe(&deliver, root, 1);
        wait_for(&events, |event| {
            snapshot(event).is_some_and(|snapshot| snapshot.device().session.value() == 1)
        });

        deliver.send(Packet::heartbeat_session(2, root)).unwrap();
        describe(&deliver, root, 2);
        wait_for(&events, |event| {
            snapshot(event).is_some_and(|snapshot| snapshot.device().session.value() == 2)
        });
    }

    /// Devices re-broadcast their metadata; saying again what a subscriber
    /// already has is not news.
    #[test]
    fn re_broadcasting_identical_metadata_does_not_republish_it() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, deliver, _commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        describe(&deliver, root, 1);
        wait_for(&events, |event| snapshot(event).is_some());

        describe(&deliver, root, 1);
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
        let (_handle, deliver, commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        deliver.send(samples(0, DeviceRoute::root())).unwrap();

        let (_, _, result) = metadata_call(&commands);
        result
            .send(Err(proxy::RawCallError::Device {
                error: wire_rpc::RpcError::NotFound,
                message: Vec::new(),
            }))
            .unwrap();

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
            commands.recv_timeout(Duration::from_millis(500)).is_err(),
            "discovery does not re-arm for a route with no dev.metadata"
        );
        drop(deliver);
    }

    #[test]
    fn a_disconnect_terminates_every_receiver() {
        let (sink, batches) = Sink::new(BATCH_QUEUE_LEN, everything());
        let (handle, deliver, commands, _endpoint, worker) = pump(PumpSink::Batches(sink));
        let events = subscribe(&handle, everything(), PumpSink::Events);
        // The worker itself is gone, so no port replaces the one that dies.
        drop((worker, commands));
        drop(deliver);

        while batches.recv().is_ok() {}
        assert!(matches!(batches.recv(), Err(RecvError::Disconnected)));
        assert!(matches!(events.recv(), Err(RecvError::Disconnected)));
    }

    #[test]
    fn an_inlet_overrun_is_reported_and_the_stream_goes_on() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, deliver, commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        deliver.send(samples(0, root)).unwrap();
        let (_, _, result) = metadata_call(&commands);
        result
            .send(Err(proxy::RawCallError::Device {
                error: wire_rpc::RpcError::NotFound,
                message: Vec::new(),
            }))
            .unwrap();
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Device {
                    event: DeviceEvent::MetadataUnavailable,
                    ..
                }
            )
        });

        // The proxy dropped this client for falling behind, but still serves.
        drop(deliver);
        let deliver = opened_port(&commands);
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Link {
                    event: LinkEvent::InputOverrun,
                    ..
                }
            )
        });

        deliver.send(samples(1, root)).unwrap();
        assert!(
            commands.recv_timeout(Duration::from_millis(500)).is_err(),
            "the overrun did not forget that this route has no dev.metadata"
        );
        let child: DeviceRoute = "/1".parse().unwrap();
        deliver.send(samples(0, child)).unwrap();
        wait_for(
            &events,
            |event| matches!(event, Event::Tree { route, event: TreeEvent::RouteDiscovered } if *route == child),
        );
    }

    /// A port the worker never adopted keeps its own channel alive, so nothing
    /// about it would ever say the proxy stopped.
    #[test]
    fn a_stopped_worker_ends_the_stream_even_with_a_stranded_port() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, deliver, commands, _endpoint, worker) = pump(PumpSink::Events(sink));
        drop(worker);

        assert!(matches!(events.recv(), Err(RecvError::Disconnected)));
        drop((deliver, commands));
    }

    /// The status saying why the proxy stopped is the last thing it queues, so
    /// the pump must publish what the port still holds before ending.
    #[test]
    fn the_last_packets_a_stopping_worker_queued_are_still_published() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, deliver, commands, _endpoint, worker) = pump(PumpSink::Events(sink));
        deliver
            .send(Packet::proxy_status(proto::ProxyStatus::FailedToReconnect))
            .unwrap();
        drop((worker, commands, deliver));

        wait_for(&events, |event| {
            matches!(
                event,
                Event::Link {
                    event: LinkEvent::Status(proto::ProxyStatus::FailedToReconnect),
                    ..
                }
            )
        });
        assert!(matches!(events.recv(), Err(RecvError::Disconnected)));
    }

    /// The pump ends once nothing can reach it any more: no view left to
    /// subscribe, and no receiver left holding the stream open.
    #[test]
    fn dropping_the_last_view_and_receiver_ends_the_pump() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (handle, deliver, commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        drop((handle, events));

        // A silent device gives the pump nothing else to wake on, so it has to
        // wake on the subscription lane itself going away.
        for _ in 0..500 {
            if deliver
                .send(Packet::heartbeat(DeviceRoute::root()))
                .is_err()
            {
                drop(commands);
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("the pump outlived every view of its connection");
    }

    /// An elapsed deadline that stayed in the map would make every `wait`
    /// return at once, spinning for as long as the route stayed uncovered.
    #[test]
    fn a_route_that_comes_due_stops_asking_the_pump_to_wake() {
        let mut state = StreamState::new();
        let route = DeviceRoute::root();
        state.known_routes.insert(route);
        state.discovery.insert(
            route,
            Discovery::Backoff {
                until: Instant::now() - Duration::from_millis(1),
                delay: METADATA_RETRY_FIRST,
            },
        );

        assert_eq!(state.take_due_routes(Instant::now()), [route]);
        assert!(
            state.next_retry().is_none(),
            "an elapsed deadline never wakes the pump again"
        );
        let query = state
            .parser
            .take_metadata_queries_for(route)
            .pop()
            .expect("the route is still bootstrapping");
        state.back_off(query);
        assert!(
            matches!(state.discovery.get(&route), Some(Discovery::Backoff { delay, .. }) if *delay == METADATA_RETRY_FIRST * 2),
            "coming due keeps the delay the next failure doubles"
        );
    }

    /// A refusal answering a query the reset already overtook belongs to the
    /// session that ended, and must not condemn the route discovering now.
    #[test]
    fn a_refusal_from_before_a_reset_does_not_condemn_the_route() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, deliver, commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        deliver.send(samples(0, root)).unwrap();
        let (_, _, stale) = metadata_call(&commands);

        deliver
            .send(Packet::proxy_status(proto::ProxyStatus::SensorDisconnected))
            .unwrap();
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Link {
                    event: LinkEvent::Status(proto::ProxyStatus::SensorDisconnected),
                    ..
                }
            )
        });
        stale
            .send(Err(proxy::RawCallError::Device {
                error: wire_rpc::RpcError::NotFound,
                message: Vec::new(),
            }))
            .unwrap();
        while !stale.is_empty() {
            thread::sleep(Duration::from_millis(1));
        }

        let child: DeviceRoute = "/1".parse().unwrap();
        deliver.send(samples(0, child)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match events
                .recv_deadline(deadline)
                .expect("the pump keeps going")
            {
                Event::Tree {
                    route,
                    event: TreeEvent::RouteDiscovered,
                } if route == child => break,
                Event::Device {
                    event: DeviceEvent::MetadataUnavailable,
                    ..
                } => panic!("the stale refusal condemned a route that had moved on"),
                Event::Tree { .. } | Event::Link { .. } | Event::Device { .. } => {}
            }
        }
    }

    /// Link facts belong to the transport, so a device view hears them; another
    /// device's facts are not its business.
    #[test]
    fn a_device_view_hears_link_facts_but_not_another_routes_device_facts() {
        let (wide, _all) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (narrow, device) = Sink::new(EVENT_QUEUE_LEN, exactly("/1"));
        let (_handle, deliver, _commands, _endpoint, _worker) =
            pump_with(vec![PumpSink::Events(wide), PumpSink::Events(narrow)]);
        let root = DeviceRoute::root();
        let child: DeviceRoute = "/1".parse().unwrap();

        // The wide view covers the root, so the root's device facts are
        // produced; only the filter keeps them from the device view.
        deliver
            .send(Packet::proxy_status(proto::ProxyStatus::SensorDisconnected))
            .unwrap();
        deliver.send(Packet::heartbeat(root)).unwrap();
        deliver.send(Packet::heartbeat(child)).unwrap();

        let mut heard_status = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match device
                .recv_deadline(deadline)
                .expect("the device view keeps receiving")
            {
                Event::Link {
                    event: LinkEvent::Status(proto::ProxyStatus::SensorDisconnected),
                    ..
                } => heard_status = true,
                Event::Link { .. } => {}
                Event::Tree { route, .. } => {
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

    /// A status carries a fabricated route — `FailedToConnect` means nothing
    /// is there — so it must never mint a device.
    #[test]
    fn a_status_packet_discovers_no_route() {
        let (sink, events) = Sink::new(EVENT_QUEUE_LEN, everything());
        let (_handle, deliver, _commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        deliver
            .send(Packet::proxy_status(proto::ProxyStatus::FailedToConnect))
            .unwrap();
        deliver.send(Packet::heartbeat(root)).unwrap();

        // Events publish in arrival order: a status that discovered its route
        // would announce it before the link fact, not after.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut heard_status = false;
        loop {
            match events.recv_deadline(deadline).expect("the pump publishes") {
                Event::Link {
                    event: LinkEvent::Status(proto::ProxyStatus::FailedToConnect),
                    ..
                } => heard_status = true,
                Event::Tree {
                    route,
                    event: TreeEvent::RouteDiscovered,
                } => {
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
        let (_handle, deliver, _commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let steady: DeviceRoute = "/1".parse().unwrap();
        let bounced: DeviceRoute = "/2".parse().unwrap();
        describe(&deliver, steady, 1);
        describe(&deliver, bounced, 1);
        wait_for(&events, |event| {
            matches!(event, Event::Device { route, event: DeviceEvent::Metadata(_) } if *route == bounced)
        });

        deliver
            .send(
                Packet::proxy_status(proto::ProxyStatus::SensorDisconnected).with_route(bounced),
            )
            .unwrap();
        wait_for(&events, |event| {
            matches!(event, Event::Link { subtree, event: LinkEvent::Status(proto::ProxyStatus::SensorDisconnected) } if *subtree == bounced)
        });

        // Both re-broadcast; only the bounced mount's description is news.
        describe(&deliver, steady, 1);
        describe(&deliver, bounced, 1);
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

        deliver
            .send(Packet::proxy_status(proto::ProxyStatus::SensorReconnected).with_route(bounced))
            .unwrap();
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

    /// The affected subtree contains the view, not the other way around: a
    /// view pinned below a mount still hears that mount's status, and a view
    /// on another mount hears nothing.
    #[test]
    fn a_view_below_the_affected_subtree_hears_its_status() {
        let (a, below) = Sink::new(EVENT_QUEUE_LEN, exactly("/1/0"));
        let (b, elsewhere) = Sink::new(EVENT_QUEUE_LEN, exactly("/2"));
        let (_handle, deliver, _commands, _endpoint, _worker) =
            pump_with(vec![PumpSink::Events(a), PumpSink::Events(b)]);
        let mount: DeviceRoute = "/1".parse().unwrap();
        deliver
            .send(Packet::proxy_status(proto::ProxyStatus::SensorReconnected).with_route(mount))
            .unwrap();

        wait_for(&below, |event| {
            matches!(event, Event::Link { subtree, event: LinkEvent::Status(proto::ProxyStatus::SensorReconnected) } if *subtree == mount)
        });
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
        let (_handle, deliver, commands, _endpoint, _worker) =
            pump_with(vec![PumpSink::Packets(wide), PumpSink::Packets(narrow)]);
        let root = DeviceRoute::root();
        let child: DeviceRoute = "/1".parse().unwrap();

        deliver
            .send(Packet::proxy_status(proto::ProxyStatus::SensorReconnected))
            .unwrap();
        deliver.send(samples(0, root)).unwrap();
        deliver.send(samples(7, child)).unwrap();

        let status = taps.recv_timeout(Duration::from_secs(5)).expect("a packet");
        assert!(matches!(status.payload(), proto::Payload::ProxyStatus(_)));
        let first = taps.recv_timeout(Duration::from_secs(5)).expect("a packet");
        assert_eq!(first.route(), root);
        assert!(matches!(first.payload(), proto::Payload::Samples(_)));
        assert_eq!(
            taps.recv_timeout(Duration::from_secs(5))
                .expect("a packet")
                .route(),
            child
        );

        let status = scoped.recv_timeout(Duration::from_secs(5)).expect("packet");
        assert!(
            matches!(status.payload(), proto::Payload::ProxyStatus(_)),
            "the whole link's status reaches a tap scoped inside it"
        );
        let only = scoped.recv_timeout(Duration::from_secs(5)).expect("packet");
        assert_eq!(only.route(), child, "a tap only sees what its scope covers");
        assert!(matches!(
            scoped.recv_timeout(Duration::from_millis(200)),
            Err(RecvError::Timeout)
        ));
        assert!(
            commands.recv_timeout(Duration::from_millis(500)).is_err(),
            "a raw tap decodes nothing and discovers nothing"
        );
    }

    #[test]
    fn a_route_no_view_covers_is_neither_decoded_nor_asked_for_metadata() {
        let (sink, device) = Sink::new(EVENT_QUEUE_LEN, exactly("/1"));
        let (handle, deliver, commands, _endpoint, _worker) = pump(PumpSink::Events(sink));
        let root = DeviceRoute::root();
        let elsewhere: DeviceRoute = "/2".parse().unwrap();
        describe(&deliver, root, 1);
        for first in 0..2 {
            deliver.send(samples(first, root)).unwrap();
        }
        deliver.send(samples(0, elsewhere)).unwrap();

        assert!(
            commands.recv_timeout(Duration::from_millis(500)).is_err(),
            "a route outside every view is never asked for metadata"
        );

        // The new view covers everything, so the route nothing covered
        // bootstraps — which the pump only does once the sink is in its lists.
        let batches = subscribe(&handle, everything(), PumpSink::Batches);
        let (route, args, _result) = metadata_call(&commands);
        assert_eq!(route, elsewhere);
        assert!(args.is_empty(), "the newly covered route bootstraps");

        deliver.send(samples(2, root)).unwrap();
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
    fn overlapping_views_share_one_port_and_one_discovery() {
        let (stream, _endpoint, commands, _worker) = stream();
        let child: DeviceRoute = "/1".parse().unwrap();
        let events = stream.events(everything());
        let deliver = opened_port(&commands);
        let batches = stream.batches(exactly("/1"));

        deliver.send(samples(0, DeviceRoute::root())).unwrap();
        deliver.send(samples(0, child)).unwrap();

        let first = metadata_call(&commands);
        let second = metadata_call(&commands);
        assert_ne!(first.0, second.0, "one bootstrap per route, not per view");
        assert!(
            commands.recv_timeout(Duration::from_millis(500)).is_err(),
            "the second view opened no port and started no second discovery"
        );
        drop((events, batches));
    }

    /// A receiver is the subscription: it keeps the pump running on its own,
    /// so a caller need not hold the view it was minted from.
    #[test]
    fn a_receiver_keeps_the_pump_running_without_its_view() {
        let (stream, _endpoint, commands, _worker) = stream();
        let events = stream.events(everything());
        let deliver = opened_port(&commands);
        drop(stream);

        deliver
            .send(Packet::heartbeat(DeviceRoute::root()))
            .unwrap();
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

    /// The command lane a port registration takes is shared with every RPC, so
    /// a busy instant at the first subscription is congestion, not an answer:
    /// it must leave neither the connection without a pump nor a dead one
    /// remembered in its place.
    #[test]
    fn a_first_subscription_waits_out_a_busy_command_lane() {
        let (stream, endpoint, commands, _worker) = stream();
        let root = DeviceRoute::root();
        let queued: Vec<_> =
            std::iter::from_fn(|| endpoint.submit(root, "dev.name", b"").ok()).collect();
        assert!(!queued.is_empty(), "the lane takes what it can hold");
        assert!(matches!(
            endpoint.open_port(true, true),
            Err(proxy::PortError::ProxyBusy)
        ));

        let subscriber = {
            let stream = Arc::clone(&stream);
            thread::spawn(move || stream.events(everything()))
        };
        // Draining what the lane held is the congestion passing: the retried
        // registration fits, and the pump the connection kept is the one that
        // started.
        let deliver = opened_port(&commands);
        let events = subscriber.join().expect("the subscription completes");
        deliver.send(Packet::heartbeat(root)).unwrap();
        wait_for(&events, |event| {
            matches!(
                event,
                Event::Device {
                    event: DeviceEvent::Heartbeat { .. },
                    ..
                }
            )
        });
        drop(queued);
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

    /// A stream whose proxy is already gone hands back receivers that say so,
    /// rather than ones that wait on a pump that will never run.
    #[test]
    fn subscribing_to_a_dead_proxy_yields_a_disconnected_receiver() {
        let (stream, _endpoint, commands, worker) = stream();
        drop((commands, worker));

        assert!(matches!(
            stream.events(everything()).recv(),
            Err(RecvError::Disconnected)
        ));
        assert!(matches!(
            stream.batches(everything()).recv(),
            Err(RecvError::Disconnected)
        ));
    }
}

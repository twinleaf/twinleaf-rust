//! The stream pump, one task per connection.
//!
//! The pump drains the session's inlet and keeps each device's state. It
//! sends packets, batches and device state to owned [`Receiver`]s, each
//! filtered to the subtree its view covers.
//!
//! A pump whose session the proxy opened follows the state the proxy keeps,
//! presence and announcements included, with a `proxy.watch` of each device
//! it needs. Any other pump asks the devices for their metadata, RPC table
//! and settings itself, and follows their presence and announcements from
//! the packets, as the client inside the proxy does over a port on a link.

mod event;
mod metadata;
mod subscription;

pub(crate) use event::DeviceEvent;
pub use event::NamedRoute;
pub(crate) use metadata::{Completed, MetadataSchedule};
pub(crate) use subscription::{End, Lane, Queued, Scope};
pub use subscription::{Receiver, RecvError};

use super::session::{Handle, Inlet, RawCallResult};
use super::state::{
    self, Learned, MetadataState, Presence, RegistryState, SettingChange, Settings,
};
use crate::data::{MetadataQuery, PacketParser, SampleBatch};
use crate::proto::{DeviceRoute, SessionId};
use crate::proxy::{decode, encode, Part, Update, Watch, WATCH};
use crate::rt::{sleep_until_some, Supervisor};
use crate::tio;
use crate::tio::packet::{self, Packet, ProxyRx};
use crate::tio::transport;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::{self, Future};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use subscription::Sink;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// How many batches a subscriber's queue holds before the pump drops more.
const BATCH_QUEUE_LEN: usize = 1024;

/// How many setting announcements a subscriber's queue holds.
const SETTING_QUEUE_LEN: usize = 4096;

/// How many packets a raw tap's queue holds, enough for a recorder's disk
/// stall.
const PACKET_QUEUE_LEN: usize = 8192;

/// How many packets the inlet holds, as many as a socket's own queue.
const INLET_LEN: usize = transport::DEFAULT_RX_CHANNEL_SIZE;

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
/// metadata revisions and presence, the latest link status per subtree, the
/// devices' workers, and the batches and state changes waiting to go out.
struct StreamState {
    parser: PacketParser,
    known_routes: HashSet<DeviceRoute>,
    metadata_seen: HashMap<DeviceRoute, u32>,
    /// Routes whose firmware has no `dev.metadata`.
    unsupported: HashSet<DeviceRoute>,
    /// Latest status per subtree, and when it arrived, which gives a route
    /// not yet heard its presence. A new status supersedes every entry under
    /// it.
    link_state: HashMap<DeviceRoute, (packet::LinkStatus, std::time::Instant)>,
    /// The presence of each route heard. Any other route is as its link
    /// left it.
    presence: HashMap<DeviceRoute, Presence>,
    /// The worker of each device whose RPC table or settings were asked for.
    workers: HashMap<DeviceRoute, Worker>,
    /// Whether presence and setting changes come from the packets, rather
    /// than from the proxy's watches.
    derives: bool,
    batches: VecDeque<SampleBatch>,
    changes: VecDeque<Change>,
}

/// A device's worker, and the latest it learned. A pump that watches the
/// proxy's state has no thread to send events to, and the watch of the
/// device's table and settings stands in for it.
struct Worker {
    events: Option<flume::Sender<DeviceEvent>>,
    registry: Option<RegistryState>,
    settings: Option<Arc<Settings>>,
}

/// Where the pump's device state comes from.
enum Source {
    /// The pump asks each device for its metadata on this schedule, and its
    /// workers load the table and read the settings. So does the client
    /// inside the proxy, over a port on a link.
    Asks(MetadataSchedule),
    /// The proxy keeps the state, and the pump follows a `proxy.watch` of
    /// each device it needs, by request id.
    Watches(HashMap<u16, Watched>),
}

/// A `proxy.watch` the pump follows, and what has arrived of its next
/// update.
struct Watched {
    watch: Watch,
    joined: Vec<u8>,
    /// The index of the part due next, or `None` once a part went missing,
    /// until the next update begins.
    next: Option<u16>,
}

impl Watched {
    /// Takes the reply `value`, a part of an update. Returns the encoded
    /// update once its last part is in.
    fn join(&mut self, value: &[u8]) -> Option<Vec<u8>> {
        let part: Part = decode(value).ok()?;
        if part.index == 0 {
            self.joined.clear();
            self.next = Some(0);
        }
        if self.next != Some(part.index) {
            self.next = None;
            return None;
        }
        self.joined.extend_from_slice(&part.bytes);
        self.next = part.index.checked_add(1);
        (self.next == Some(part.count)).then(|| std::mem::take(&mut self.joined))
    }
}

/// Asks the proxy over `root` for `watch`, as the next of `watches`, unless
/// it is one of them already.
fn start_watch(root: &Handle, watches: &mut HashMap<u16, Watched>, watch: Watch) {
    if watches.values().any(|watched| watched.watch == watch) {
        return;
    }
    let id = u16::try_from(watches.len() + 1).unwrap_or(u16::MAX);
    match Packet::proxy_request(WATCH, &encode(&watch), id) {
        Ok(request) => root.session().send(request),
        Err(error) => log::warn!("could not watch {}: {error}", watch.route),
    }
    let watched = Watched {
        watch,
        joined: Vec::new(),
        next: None,
    };
    watches.insert(id, watched);
}

/// Device state that changed, waiting to go out.
enum Change {
    Route(DeviceRoute),
    Presence(DeviceRoute),
    Metadata(DeviceRoute),
    Setting(DeviceRoute, SettingChange),
    Registry(DeviceRoute),
    Settings(DeviceRoute),
}

impl Change {
    /// Returns which value of which device changed, the same for two
    /// changes to one value.
    fn key(&self) -> (std::mem::Discriminant<Change>, DeviceRoute) {
        let route = match self {
            Change::Route(route)
            | Change::Presence(route)
            | Change::Metadata(route)
            | Change::Setting(route, _)
            | Change::Registry(route)
            | Change::Settings(route) => *route,
        };
        (std::mem::discriminant(self), route)
    }
}

/// Returns the presence a link's `status` gives the devices it serves, or
/// `None` for a status that says nothing about them.
fn link_presence(status: packet::LinkStatus, since: std::time::Instant) -> Option<Presence> {
    match status {
        packet::LinkStatus::SensorDisconnected
        | packet::LinkStatus::Stopped
        | packet::LinkStatus::FailedToConnect
        | packet::LinkStatus::FailedToReconnect => Some(Presence::Lost { since }),
        packet::LinkStatus::SensorReconnected => Some(Presence::Connecting),
        packet::LinkStatus::Unknown(_) => None,
    }
}

impl StreamState {
    fn new(derives: bool) -> StreamState {
        StreamState {
            parser: PacketParser::new(false),
            known_routes: HashSet::new(),
            metadata_seen: HashMap::new(),
            unsupported: HashSet::new(),
            link_state: HashMap::new(),
            presence: HashMap::new(),
            workers: HashMap::new(),
            derives,
            batches: VecDeque::new(),
            changes: VecDeque::new(),
        }
    }

    fn pop_batch(&mut self) -> Option<SampleBatch> {
        self.batches.pop_front()
    }

    /// Hands a fact about the device at `route` to its worker, if it has one.
    fn device_event(&self, route: DeviceRoute, event: DeviceEvent) {
        if let Some(events) = self.workers.get(&route).and_then(|w| w.events.as_ref()) {
            let _ = events.send(event);
        }
    }

    /// Returns the routes heard within `scope`, sorted.
    fn routes_in(&self, scope: Scope) -> Vec<DeviceRoute> {
        let mut routes: Vec<_> = self
            .known_routes
            .iter()
            .copied()
            .filter(|route| scope.covers(*route))
            .collect();
        routes.sort();
        routes
    }

    /// Returns the presence of the device at `route`. One not heard from is
    /// as the deepest link status over it left it.
    fn presence_of(&self, route: DeviceRoute) -> Presence {
        if let Some(presence) = self.presence.get(&route) {
            return *presence;
        }
        self.link_state
            .iter()
            .filter(|(subtree, _)| route.starts_with(subtree))
            .max_by_key(|(subtree, _)| subtree.len())
            .and_then(|(_, (status, since))| link_presence(*status, *since))
            .unwrap_or(Presence::Connecting)
    }

    fn set_presence(&mut self, route: DeviceRoute, presence: Presence) {
        if self.presence.insert(route, presence) != Some(presence) {
            self.changes.push_back(Change::Presence(route));
        }
    }

    /// Returns how far the metadata of the device at `route` is collected.
    fn metadata_of(&self, route: DeviceRoute) -> MetadataState {
        match self.metadata_seen.contains_key(&route) {
            true => self
                .parser
                .metadata(route)
                .map_or(MetadataState::Pending, MetadataState::Ready),
            false if self.unsupported.contains(&route) => MetadataState::Unsupported,
            false => MetadataState::Pending,
        }
    }

    /// Notes that the firmware at `route` has no `dev.metadata`.
    fn metadata_unavailable(&mut self, route: DeviceRoute) {
        if self.unsupported.insert(route) {
            self.changes.push_back(Change::Metadata(route));
        }
    }

    /// Queues a change of a route's metadata when its complete metadata is a
    /// revision not yet published.
    fn publish_metadata(&mut self, route: DeviceRoute) {
        let seen = self.metadata_seen.get(&route).copied();
        let Some((revision, _)) = self.parser.revised_metadata(route, seen) else {
            return;
        };
        self.metadata_seen.insert(route, revision);
        self.changes.push_back(Change::Metadata(route));
    }

    /// Clears the metadata of every route in `subtree`. The next session
    /// there discovers it again.
    fn forget_metadata(&mut self, subtree: DeviceRoute) {
        let forgotten: Vec<_> = self
            .metadata_seen
            .keys()
            .chain(&self.unsupported)
            .filter(|route| route.starts_with(&subtree))
            .copied()
            .collect();
        self.metadata_seen
            .retain(|route, _| !route.starts_with(&subtree));
        self.unsupported
            .retain(|route| !route.starts_with(&subtree));
        self.parser.reset_subtree(subtree);
        self.changes
            .extend(forgotten.into_iter().map(Change::Metadata));
    }

    /// Applies a link's status to the subtree it concerns, the root or one
    /// mount of what the proxy serves.
    fn apply_status(&mut self, subtree: DeviceRoute, status: packet::LinkStatus) {
        let now = std::time::Instant::now();
        let presence = link_presence(status, now).filter(|_| self.derives);
        if self.derives {
            self.link_state
                .retain(|root, _| !root.starts_with(&subtree));
            self.link_state.insert(subtree, (status, now));
        }
        if let Some(presence) = presence {
            let served: Vec<_> = self
                .presence
                .keys()
                .filter(|route| route.starts_with(&subtree))
                .copied()
                .collect();
            for route in served {
                self.set_presence(route, presence);
            }
        }
        match status {
            packet::LinkStatus::SensorDisconnected | packet::LinkStatus::Stopped => {
                self.forget_metadata(subtree)
            }
            packet::LinkStatus::SensorReconnected => {
                let refresh: Vec<_> = self
                    .known_routes
                    .iter()
                    .filter(|route| route.starts_with(&subtree))
                    .copied()
                    .collect();
                for route in refresh {
                    self.device_event(route, DeviceEvent::NewHash(None));
                }
            }
            packet::LinkStatus::FailedToConnect
            | packet::LinkStatus::FailedToReconnect
            | packet::LinkStatus::Unknown(_) => {}
        }
    }

    /// Takes one packet. Link statuses, new routes, presence, and control
    /// packets are handled for every route. Heartbeats and announcements
    /// reach the device's state, and samples decode, only when `covered`.
    fn process_packet(&mut self, pkt: &tio::Packet, covered: bool) {
        if let Some(status) = pkt.link_status() {
            return self.apply_status(pkt.route(), status);
        }
        let route = pkt.route();
        if self.known_routes.insert(route) {
            self.changes.push_back(Change::Route(route));
        }
        let session = match self.presence.get(&route) {
            Some(Presence::Alive { session }) => *session,
            _ => None,
        };
        let session = match pkt.payload() {
            packet::Payload::Heartbeat(beat) => beat.session().or(session),
            _ => session,
        };
        if self.derives {
            self.set_presence(route, Presence::Alive { session });
        }

        match pkt.payload() {
            packet::Payload::Heartbeat(beat) => {
                if covered {
                    self.device_event(
                        route,
                        DeviceEvent::Heartbeat {
                            session_id: beat.session(),
                            settings: beat.settings(),
                        },
                    );
                }
            }
            packet::Payload::Setting(setting) if covered => {
                if let Some(hash) = rpc_hash(&setting) {
                    self.device_event(route, DeviceEvent::NewHash(Some(hash)));
                }
                if let Some(name) = setting.name_str() {
                    let change = SettingChange {
                        name: name.to_string(),
                        seq: setting.seq,
                        reply: setting.reply.to_vec(),
                    };
                    if self.derives {
                        self.changes
                            .push_back(Change::Setting(route, change.clone()));
                    }
                    self.device_event(
                        route,
                        DeviceEvent::Setting {
                            name: change.name,
                            seq: change.seq,
                            reply: change.reply,
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
    Packets(Sink<tio::Packet>),
    Routes(Sink<Vec<DeviceRoute>>),
    Presence(Sink<Presence>),
    Metadata(Sink<MetadataState>),
    Registry(Sink<RegistryState>),
    Settings(Sink<Arc<Settings>>),
    SettingChanges(Sink<SettingChange>),
}

impl PumpSink {
    /// Ends the subscription saying why.
    fn end(self, end: End) {
        match self {
            PumpSink::Batches(sink) => sink.lane.end(end),
            PumpSink::Packets(sink) => sink.lane.end(end),
            PumpSink::Routes(sink) => sink.lane.end(end),
            PumpSink::Presence(sink) => sink.lane.end(end),
            PumpSink::Metadata(sink) => sink.lane.end(end),
            PumpSink::Registry(sink) => sink.lane.end(end),
            PumpSink::Settings(sink) => sink.lane.end(end),
            PumpSink::SettingChanges(sink) => sink.lane.end(end),
        }
    }
}

/// The pump's fan-out lists, one per kind of subscription.
#[derive(Default)]
struct Sinks {
    batches: Vec<Sink<SampleBatch>>,
    packets: Vec<Sink<tio::Packet>>,
    routes: Vec<Sink<Vec<DeviceRoute>>>,
    presence: Vec<Sink<Presence>>,
    metadata: Vec<Sink<MetadataState>>,
    registry: Vec<Sink<RegistryState>>,
    settings: Vec<Sink<Arc<Settings>>>,
    setting_changes: Vec<Sink<SettingChange>>,
}

/// Calls the generic `$apply` with each list of `$sinks` and then `$args`.
macro_rules! each_list {
    ($sinks:expr, $apply:ident $(, $args:expr)*) => {{
        let sinks = $sinks;
        $apply(&mut sinks.batches $(, $args)*);
        $apply(&mut sinks.packets $(, $args)*);
        $apply(&mut sinks.routes $(, $args)*);
        $apply(&mut sinks.presence $(, $args)*);
        $apply(&mut sinks.metadata $(, $args)*);
        $apply(&mut sinks.registry $(, $args)*);
        $apply(&mut sinks.settings $(, $args)*);
        $apply(&mut sinks.setting_changes $(, $args)*);
    }};
}

impl Sinks {
    /// Returns `true` if a subscription that needs the route decoded covers
    /// `route`. Raw taps and route lists do not.
    fn cover(&self, route: DeviceRoute) -> bool {
        let scopes = self
            .batches
            .iter()
            .map(|sink| sink.scope)
            .chain(self.presence.iter().map(|sink| sink.scope))
            .chain(self.metadata.iter().map(|sink| sink.scope))
            .chain(self.registry.iter().map(|sink| sink.scope))
            .chain(self.settings.iter().map(|sink| sink.scope))
            .chain(self.setting_changes.iter().map(|sink| sink.scope));
        let mut scopes = scopes;
        scopes.any(|scope| scope.covers(route))
    }

    /// Returns how many subscriptions there are.
    fn len(&mut self) -> usize {
        let mut count = 0;
        each_list!(self, count_into, &mut count);
        count
    }

    /// Ends every subscription saying why.
    fn end(&mut self, end: End) {
        each_list!(self, end_all, end);
    }

    /// Sends each subscriber's count of missed items, and drops the
    /// subscriptions whose receivers are gone.
    fn report_lag(&mut self) {
        each_list!(self, report_all);
    }

    /// Drops the subscriptions whose receivers are gone, as their
    /// lifelines report in `cx`.
    fn prune(&mut self, cx: &mut Context<'_>) {
        each_list!(self, prune_all, &mut *cx);
    }
}

fn count_into<T>(list: &[Sink<T>], count: &mut usize) {
    *count += list.len();
}

fn end_all<T>(list: &mut Vec<Sink<T>>, end: End) {
    list.drain(..).for_each(|sink| sink.lane.end(end));
}

fn report_all<T>(list: &mut Vec<Sink<T>>) {
    list.retain_mut(|sink| sink.lane.report_lag());
}

fn prune_all<T>(list: &mut Vec<Sink<T>>, cx: &mut Context<'_>) {
    // A lifeline resolves only once its receiver is gone, and its sink is
    // dropped before it is polled again.
    list.retain_mut(|sink| Pin::new(&mut sink.alive).poll(cx).is_pending());
}

/// Sends `item` to each subscriber in `list` whose scope covers `route`.
fn send_at<T: Clone>(list: &mut Vec<Sink<T>>, route: DeviceRoute, item: impl Fn() -> T) {
    list.retain_mut(|sink| sink.offer(Scope::point(route), &item));
}

/// What the pump does after a turn.
enum Turn {
    Running,
    /// The session or the stream is gone.
    Gone,
    /// The session ended saying why.
    Ended(End),
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

    /// Subscribes to the packets in `scope`.
    pub(super) fn packets(self: &Arc<Stream>, scope: Scope) -> Receiver<tio::Packet> {
        self.mint(scope, PACKET_QUEUE_LEN, PumpSink::Packets)
    }

    /// Subscribes to the sorted list of the routes heard in `scope`.
    pub(super) fn routes(self: &Arc<Stream>, scope: Scope) -> Receiver<Vec<DeviceRoute>> {
        self.mint_latest(scope, PumpSink::Routes)
    }

    /// Subscribes to the presence of the device at `route`.
    pub(super) fn presence(self: &Arc<Stream>, route: DeviceRoute) -> Receiver<Presence> {
        self.mint_latest(Scope::point(route), PumpSink::Presence)
    }

    /// Subscribes to the metadata of the device at `route`.
    pub(super) fn metadata(self: &Arc<Stream>, route: DeviceRoute) -> Receiver<MetadataState> {
        self.mint_latest(Scope::point(route), PumpSink::Metadata)
    }

    /// Subscribes to the RPC table of the device at `route`.
    pub(super) fn rpc_registry(self: &Arc<Stream>, route: DeviceRoute) -> Receiver<RegistryState> {
        self.mint_latest(Scope::point(route), PumpSink::Registry)
    }

    /// Subscribes to the settings of the device at `route`.
    pub(super) fn settings(self: &Arc<Stream>, route: DeviceRoute) -> Receiver<Arc<Settings>> {
        self.mint_latest(Scope::point(route), PumpSink::Settings)
    }

    /// Subscribes to every setting the device at `route` announces.
    pub(super) fn setting_changes(
        self: &Arc<Stream>,
        route: DeviceRoute,
    ) -> Receiver<SettingChange> {
        self.mint(
            Scope::point(route),
            SETTING_QUEUE_LEN,
            PumpSink::SettingChanges,
        )
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
        self.hand(Sink::new(capacity, scope), wrap)
    }

    /// Hands the pump one more sink on a latest-value lane, as
    /// [`mint`](Self::mint) does.
    fn mint_latest<T>(
        self: &Arc<Stream>,
        scope: Scope,
        wrap: fn(Sink<T>) -> PumpSink,
    ) -> Receiver<T> {
        self.hand(Sink::latest(scope), wrap)
    }

    fn hand<T>(
        self: &Arc<Stream>,
        (sink, receiver): (Sink<T>, Receiver<T>),
        wrap: fn(Sink<T>) -> PumpSink,
    ) -> Receiver<T> {
        let _ = self.pump().send(wrap(sink));
        receiver.tied_to(self)
    }

    /// Returns the sender of the pump's new sinks, starting the pump on the
    /// first call. Only one pump takes the session's inlet.
    fn pump(&self) -> &mpsc::UnboundedSender<PumpSink> {
        self.pump.get_or_init(|| Pump::start(&self.root))
    }
}

/// The pump itself. It owns the session's inlet, the parser, the metadata
/// queries in flight or the watches, and the devices' workers, and runs
/// whether or not anything receives from it.
struct Pump {
    root: Handle,
    inlet: Inlet,
    commands: mpsc::UnboundedReceiver<PumpSink>,
    state: StreamState,
    source: Source,
    sinks: Sinks,
    /// What the devices' workers learned.
    learned: mpsc::UnboundedReceiver<(DeviceRoute, Learned)>,
    /// The sender of what they learned, which each worker gets.
    teach: mpsc::UnboundedSender<(DeviceRoute, Learned)>,
}

impl Pump {
    /// Attaches to the session and starts the task that drains its inlet.
    fn start(root: &Handle) -> mpsc::UnboundedSender<PumpSink> {
        let (commands, received) = mpsc::unbounded_channel();
        let pump = Pump::new(root, root.session().attach(INLET_LEN), received);
        Supervisor::new().spawn("twinleaf stream", pump.run());
        commands
    }

    fn new(root: &Handle, inlet: Inlet, commands: mpsc::UnboundedReceiver<PumpSink>) -> Pump {
        let (teach, learned) = mpsc::unbounded_channel();
        let watches = root.session().watches();
        Pump {
            root: root.clone(),
            state: StreamState::new(!watches),
            source: match watches {
                true => Source::Watches(HashMap::new()),
                false => Source::Asks(MetadataSchedule::new(root.clone())),
            },
            inlet,
            commands,
            sinks: Sinks::default(),
            learned,
            teach,
        }
    }

    /// Runs until the session ends or the stream is dropped. Drains and
    /// discards packets while no sink is live. Once the session ended saying
    /// why, ends every sink so, and each new one until the stream drops.
    async fn run(mut self) {
        let end = loop {
            let retry = match &self.source {
                Source::Asks(schedule) => schedule.next_retry(),
                Source::Watches(_) => None,
            };
            // A sink takes the packets that arrive after it was made, so new
            // sinks, replies and pruning go ahead of the inlet.
            let turn = tokio::select! {
                biased;
                sink = self.commands.recv() => match sink {
                    Some(sink) => {
                        self.add_sink(sink);
                        Turn::Running
                    }
                    None => Turn::Gone,
                },
                Ok(reply) = metadata_reply(&self.source) => {
                    self.take_metadata_replies(reply);
                    Turn::Running
                }
                Some((route, learned)) = self.learned.recv() => {
                    self.take_learned(route, learned);
                    Turn::Running
                }
                () = pruned(&mut self.sinks) => Turn::Running,
                received = self.inlet.recv_async() => self.take_input(received),
                () = sleep_until_some(retry) => Turn::Running,
            };
            self.publish();
            match turn {
                Turn::Running => self.discover(),
                Turn::Gone => return,
                Turn::Ended(end) => break end,
            }
        };
        // Every device is lost with the session, which no watch can say once
        // the proxy is gone.
        let since = std::time::Instant::now();
        let routes: Vec<_> = self.sinks.presence.iter().map(|s| s.scope.route).collect();
        for route in routes {
            if !matches!(self.state.presence_of(route), Presence::Lost { .. }) {
                self.state.set_presence(route, Presence::Lost { since });
            }
        }
        self.publish();
        self.sinks.end(end);
        self.state.workers.clear();
        while let Some(sink) = self.commands.recv().await {
            sink.end(end);
        }
    }

    /// Takes a new subscription. A subscription to state receives the
    /// current value first.
    fn add_sink(&mut self, sink: PumpSink) {
        if let (
            PumpSink::Presence(Sink { scope, .. })
            | PumpSink::Metadata(Sink { scope, .. })
            | PumpSink::SettingChanges(Sink { scope, .. }),
            Source::Watches(watches),
        ) = (&sink, &mut self.source)
        {
            let route = scope.route;
            start_watch(
                &self.root,
                watches,
                Watch {
                    route,
                    table: false,
                },
            );
        }
        let state = &self.state;
        match sink {
            PumpSink::Batches(sink) => self.sinks.batches.push(sink),
            PumpSink::Packets(sink) => self.sinks.packets.push(sink),
            PumpSink::Routes(mut sink) => {
                if sink.lane.send(state.routes_in(sink.scope)) {
                    self.sinks.routes.push(sink);
                }
            }
            PumpSink::Presence(mut sink) => {
                if sink.lane.send(state.presence_of(sink.scope.route)) {
                    self.sinks.presence.push(sink);
                }
            }
            PumpSink::Metadata(mut sink) => {
                if sink.lane.send(state.metadata_of(sink.scope.route)) {
                    self.sinks.metadata.push(sink);
                }
            }
            PumpSink::Registry(mut sink) => {
                let worker = self.worker(sink.scope.route);
                if worker
                    .registry
                    .clone()
                    .is_none_or(|registry| sink.lane.send(registry))
                {
                    self.sinks.registry.push(sink);
                }
            }
            PumpSink::Settings(mut sink) => {
                let worker = self.worker(sink.scope.route);
                if worker
                    .settings
                    .clone()
                    .is_none_or(|settings| sink.lane.send(settings))
                {
                    self.sinks.settings.push(sink);
                }
            }
            PumpSink::SettingChanges(sink) => self.sinks.setting_changes.push(sink),
        }
    }

    /// Returns the worker of the device at `route`, starting it if it is
    /// not running, or watching the proxy's table and settings of the device.
    fn worker(&mut self, route: DeviceRoute) -> &mut Worker {
        let (root, teach, source) = (&self.root, &self.teach, &mut self.source);
        self.state.workers.entry(route).or_insert_with(|| {
            let events = match source {
                Source::Asks(_) => Some(state::start_worker(root.clone(), route, teach.clone())),
                Source::Watches(watches) => {
                    start_watch(root, watches, Watch { route, table: true });
                    None
                }
            };
            Worker {
                events,
                registry: None,
                settings: None,
            }
        })
    }

    /// Takes a reply to one of the pump's watches, a part of an update or a
    /// refusal.
    fn take_reply(&mut self, reply: ProxyRx<'_>) {
        let Source::Watches(watches) = &mut self.source else {
            return;
        };
        let (id, value) = match reply {
            ProxyRx::Reply(reply) => (reply.req_id.value(), reply.value),
            ProxyRx::Error(error) => {
                if let Some(watched) = watches.get(&error.req_id.value()) {
                    let message = String::from_utf8_lossy(error.message);
                    log::warn!(
                        "the proxy follows no state of {}: {message}",
                        watched.watch.route
                    );
                }
                return;
            }
            ProxyRx::Link(_) | ProxyRx::Alive | ProxyRx::Unknown(..) => return,
        };
        let Some(watched) = watches.get_mut(&id) else {
            return;
        };
        let Some(joined) = watched.join(value) else {
            return;
        };
        let route = watched.watch.route;
        match decode::<Update>(&joined) {
            Ok(update) => self.take_update(route, update),
            Err(error) => {
                log::warn!("dropping a state update of {route} that does not decode: {error}")
            }
        }
    }

    /// Keeps a value of the state of the device at `route` that the proxy
    /// sent, and queues it to go out. Its metadata reaches the parser as the
    /// device's own records.
    fn take_update(&mut self, route: DeviceRoute, update: Update) {
        match update {
            Update::Metadata(records) => {
                for record in records {
                    let applied = Packet::from_slice_prefix(&record)
                        .map_err(|error| error.to_string())
                        .and_then(|(packet, _)| {
                            let packet = packet.with_route(route);
                            self.state
                                .parser
                                .push_packet(&packet)
                                .map_err(|error| error.to_string())
                        });
                    if let Err(error) = applied {
                        log::warn!("dropping a metadata record of {route}: {error}");
                    }
                }
                self.state.publish_metadata(route);
            }
            Update::Unsupported => self.state.metadata_unavailable(route),
            Update::Connecting => self.state.set_presence(route, Presence::Connecting),
            Update::Alive(session) => {
                let session = session.map(SessionId::new);
                self.state.set_presence(route, Presence::Alive { session });
            }
            Update::Lost(ago) => {
                let now = std::time::Instant::now();
                let since = now.checked_sub(Duration::from_millis(ago)).unwrap_or(now);
                self.state.set_presence(route, Presence::Lost { since });
            }
            Update::Setting { name, seq, reply } => {
                let reply = reply.into();
                let change = SettingChange { name, seq, reply };
                self.state.changes.push_back(Change::Setting(route, change));
            }
            Update::Settings {
                hash,
                session,
                version,
                values,
            } => {
                let worker = self.state.workers.get(&route);
                let registry = worker.and_then(|worker| worker.registry.clone()?.ok());
                let Some(registry) = registry.filter(|registry| registry.hash == hash) else {
                    log::debug!("dropping settings of {route} read with a table not held here");
                    return;
                };
                let values = values.into_iter().map(|(name, value)| (name, value.into()));
                let settings = Settings::new(
                    registry,
                    session.map(SessionId::new),
                    version,
                    values.collect(),
                );
                self.take_learned(route, Learned::Settings(Arc::new(settings)));
            }
            update => {
                if let Some(registry) = update.registry() {
                    self.take_learned(route, Learned::Registry(registry));
                }
            }
        }
    }

    /// Keeps what a device's worker learned and queues it to go out.
    fn take_learned(&mut self, route: DeviceRoute, learned: Learned) {
        let Some(worker) = self.state.workers.get_mut(&route) else {
            return;
        };
        let change = match learned {
            Learned::Registry(registry) => {
                worker.registry = Some(registry);
                Change::Registry(route)
            }
            Learned::Settings(settings) => {
                worker.settings = Some(settings);
                Change::Settings(route)
            }
        };
        self.state.changes.push_back(change);
    }

    /// Returns `true` if a subscription that needs the route decoded covers
    /// `route`, or the route has a worker. Raw taps and route lists do not
    /// count.
    fn covers(&self, route: DeviceRoute) -> bool {
        self.sinks.cover(route) || self.state.workers.contains_key(&route)
    }

    /// Takes `received` from the inlet, and what else the inlet held then,
    /// up to the end.
    fn take_input(&mut self, received: Result<Queued<tio::Packet>, flume::RecvError>) -> Turn {
        let Ok(queued) = received else {
            return Turn::Gone;
        };
        let inlet = self.inlet.clone();
        let held = inlet.try_iter().take(inlet.len());
        let end = std::iter::once(queued)
            .chain(held)
            .find_map(|queued| self.take(queued));
        end.map_or(Turn::Running, Turn::Ended)
    }

    /// Takes one packet from the inlet, or reports the gap the session left.
    /// Returns why the session ended, at its end.
    fn take(&mut self, queued: Queued<tio::Packet>) -> Option<End> {
        let packet = &match queued {
            Queued::Item(packet) => packet,
            // The parser marks the gap in the samples, and the next
            // heartbeat catches a missed session change.
            Queued::Gap(lost) => {
                log::warn!("the stream's inlet overflowed and {lost} packets were lost");
                return None;
            }
            Queued::Ended(end) => return Some(end),
        };
        if let packet::Payload::ProxyRx(reply @ (ProxyRx::Reply(_) | ProxyRx::Error(_))) =
            packet.payload()
        {
            self.take_reply(reply);
            return None;
        }
        self.tap(packet);
        if let (
            Some(packet::LinkStatus::SensorDisconnected | packet::LinkStatus::Stopped),
            Source::Asks(schedule),
        ) = (packet.link_status(), &mut self.source)
        {
            schedule.forget(packet.route());
        }
        let covered = self.covers(packet.route());
        self.state.process_packet(packet, covered);
        if packet.link_status().is_some() {
            self.status_reached_unheard(packet.route());
        }
        None
    }

    /// Sends the presence a link status in `subtree` gave each subscribed
    /// device there that the pump has not heard, whose presence comes from
    /// the status alone.
    fn status_reached_unheard(&mut self, subtree: DeviceRoute) {
        if !self.state.derives {
            return;
        }
        let unheard: Vec<_> = self
            .sinks
            .presence
            .iter()
            .map(|sink| sink.scope.route)
            .filter(|route| route.starts_with(&subtree) && !self.state.presence.contains_key(route))
            .collect();
        self.state
            .changes
            .extend(unheard.into_iter().map(Change::Presence));
    }

    /// Offers a packet to the raw taps, unparsed and with its absolute route.
    /// A status reaches every tap its subtree overlaps.
    fn tap(&mut self, packet: &tio::Packet) {
        if self.sinks.packets.is_empty() {
            return;
        }
        let scope = match packet.link_status() {
            Some(_) => Scope::subtree(packet.route()),
            None => Scope::point(packet.route()),
        };
        self.sinks
            .packets
            .retain_mut(|sink| sink.offer(scope, || packet.clone()));
    }

    /// Takes `first` and every other finished `dev.metadata` call. Publishes
    /// the metadata each completed, and reports the routes whose firmware
    /// has no `dev.metadata`.
    fn take_metadata_replies(&mut self, first: (MetadataQuery, RawCallResult)) {
        let Source::Asks(schedule) = &mut self.source else {
            return;
        };
        let mut reply = Some(first);
        while let Some((query, result)) =
            reply.take().or_else(|| schedule.replies().try_recv().ok())
        {
            let route = query.route;
            match schedule.complete(&mut self.state.parser, query, result) {
                Completed::Applied => self.state.publish_metadata(route),
                Completed::Unsupported => self.state.metadata_unavailable(route),
                Completed::BackedOff => {}
            }
        }
    }

    /// Asks each covered route that is due for whatever metadata the parser
    /// still wants, or watches the metadata of each covered route not yet
    /// watched.
    fn discover(&mut self) {
        let (state, sinks) = (&mut self.state, &self.sinks);
        let covers = |route| sinks.cover(route) || state.workers.contains_key(&route);
        match &mut self.source {
            Source::Asks(schedule) => {
                let due = schedule.due(state.known_routes.iter().copied(), Instant::now());
                let due: Vec<_> = due.into_iter().filter(|route| covers(*route)).collect();
                for route in due {
                    schedule.submit(&mut state.parser, route);
                }
            }
            Source::Watches(watches) => {
                let unwatched: Vec<_> = state
                    .known_routes
                    .iter()
                    .map(|&route| Watch {
                        route,
                        table: false,
                    })
                    .filter(|watch| covers(watch.route))
                    .collect();
                for watch in unwatched {
                    start_watch(&self.root, watches, watch);
                }
            }
        }
    }

    fn publish(&mut self) {
        while let Some(batch) = self.state.pop_batch() {
            let scope = Scope::point(batch.route());
            self.sinks
                .batches
                .retain_mut(|sink| sink.offer(scope, || batch.clone()));
        }
        // Each change sends the value now, so a value that changed twice in
        // one turn goes out once. Every announcement goes out.
        let mut sent = HashSet::new();
        let mut heard = Vec::new();
        while let Some(change) = self.state.changes.pop_front() {
            match change {
                Change::Route(route) => heard.push(route),
                Change::Setting(..) => self.publish_change(change),
                _ => {
                    if sent.insert(change.key()) {
                        self.publish_change(change);
                    }
                }
            }
        }
        let state = &self.state;
        self.sinks.routes.retain_mut(|sink| {
            !heard.iter().any(|route| sink.scope.covers(*route))
                || sink.lane.send(state.routes_in(sink.scope))
        });
        self.sinks.report_lag();
    }

    /// Sends the current value of what `change` names to its subscribers.
    fn publish_change(&mut self, change: Change) {
        let (state, sinks) = (&self.state, &mut self.sinks);
        match change {
            Change::Route(_) => {}
            Change::Presence(route) => {
                send_at(&mut sinks.presence, route, || state.presence_of(route))
            }
            Change::Metadata(route) => {
                send_at(&mut sinks.metadata, route, || state.metadata_of(route))
            }
            Change::Setting(route, change) => {
                send_at(&mut sinks.setting_changes, route, || change.clone())
            }
            Change::Registry(route) => {
                if let Some(registry) = state.workers.get(&route).and_then(|w| w.registry.clone()) {
                    send_at(&mut sinks.registry, route, || registry.clone());
                }
            }
            Change::Settings(route) => {
                if let Some(settings) = state.workers.get(&route).and_then(|w| w.settings.clone()) {
                    send_at(&mut sinks.settings, route, || settings.clone());
                }
            }
        }
    }
}

/// Waits for a finished `dev.metadata` call, or forever when the pump asks
/// for no metadata itself.
async fn metadata_reply(
    source: &Source,
) -> Result<(MetadataQuery, RawCallResult), flume::RecvError> {
    match source {
        Source::Asks(schedule) => schedule.replies().recv_async().await,
        Source::Watches(_) => future::pending().await,
    }
}

/// Waits until a subscriber drops its receiver, then drops the sink of each
/// subscriber that has.
async fn pruned(sinks: &mut Sinks) {
    future::poll_fn(|cx| {
        let held = sinks.len();
        sinks.prune(cx);
        match sinks.len() < held {
            true => Poll::Ready(()),
            false => Poll::Pending,
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::DeviceMetadataSnapshot;
    use crate::device::session::{Session, TestProxy};
    use crate::proto::data as wire;
    use crate::proto::data::DataType;
    use crate::proto::rpc as wire_rpc;
    use crate::proto::sync::Epoch;
    use crate::proto::SessionId as WireSessionId;
    use crate::tio::packet::Packet;
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
        let subscriptions = start_pump(&root, root.session().attach(INLET_LEN), sinks);
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
        hand(subscriptions, Sink::new(SETTING_QUEUE_LEN, scope), wrap)
    }

    /// One more subscription to the state of the device at `route`, on a
    /// pump that is already running.
    fn watch<T>(
        subscriptions: &mpsc::UnboundedSender<PumpSink>,
        route: DeviceRoute,
        wrap: fn(Sink<T>) -> PumpSink,
    ) -> Receiver<T> {
        hand(subscriptions, Sink::latest(Scope::point(route)), wrap)
    }

    fn hand<T>(
        subscriptions: &mpsc::UnboundedSender<PumpSink>,
        (sink, receiver): (Sink<T>, Receiver<T>),
        wrap: fn(Sink<T>) -> PumpSink,
    ) -> Receiver<T> {
        subscriptions
            .send(wrap(sink))
            .expect("the pump takes the subscription");
        receiver
    }

    /// A sink for the state of the device at `route`, and its receiver.
    fn state_of<T>(route: DeviceRoute) -> (Sink<T>, Receiver<T>) {
        Sink::latest(Scope::point(route))
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
    fn announcement(name: &str, seq: Option<u32>, reply: &[u8], route: DeviceRoute) -> Packet {
        let mut raw = [0u8; 64];
        let len = crate::proto::settings::Setting {
            name: name.as_bytes(),
            seq,
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
            .segment_id()
            .value()
    }

    /// The snapshot of metadata that is ready.
    fn snapshot(state: &MetadataState) -> Option<&DeviceMetadataSnapshot> {
        match state {
            MetadataState::Ready(snapshot) => Some(snapshot),
            MetadataState::Pending | MetadataState::Unsupported => None,
        }
    }

    /// The `dev.metadata` call the pump submitted: its route, its args, and
    /// its id.
    fn metadata_call(proxy: &TestProxy) -> (DeviceRoute, Vec<u8>, u16) {
        let (route, name, args, id) = proxy.call();
        assert_eq!(name, wire::METADATA_RPC_METHOD);
        (route, args, id)
    }

    /// Take items until one matches and return it, or panic rather than
    /// block forever.
    fn wait_for<T>(items: &Receiver<T>, wanted: impl Fn(&T) -> bool) -> T {
        let deadline = Instant::now() + Duration::from_secs(5);
        while let Ok(item) = items.recv_deadline(deadline) {
            if wanted(&item) {
                return item;
            }
        }
        panic!("the pump never published what the test waited for");
    }

    /// Whether the device is lost.
    fn lost(presence: &Presence) -> bool {
        matches!(presence, Presence::Lost { .. })
    }

    /// Whether the device is alive.
    fn alive(presence: &Presence) -> bool {
        matches!(presence, Presence::Alive { .. })
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
        let root = DeviceRoute::root();
        let (sink, batches) = Sink::new(2, everything());
        let (marks, changes) = Sink::new(SETTING_QUEUE_LEN, Scope::point(root));
        let (_subscriptions, proxy, handle) = pump_with(vec![
            PumpSink::Batches(sink),
            PumpSink::SettingChanges(marks),
        ]);
        describe(&proxy, root, 1);

        // Nothing is receiving, and the subscriber has room for two batches:
        // a pump that only ran while a consumer pulled would stop there.
        let mut sample_number = 0..;
        for first in sample_number.by_ref().take(4096) {
            proxy.send(samples(first, root));
        }
        proxy.send(announcement("flood.end", Some(1), b"", root));

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
        wait_for(&changes, |change| change.name == "flood.end");
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
        let (sink, batches) = Sink::new(BATCH_QUEUE_LEN, everything());
        let (_handle, proxy, _root) = pump(PumpSink::Batches(sink));
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
        drop(batches);
    }

    /// A segment UPDATE replaces the current segment of a route that never
    /// stopped being complete, so completeness alone would miss the change.
    #[test]
    fn a_live_segment_rollover_republishes_metadata_and_replays_the_latest() {
        let root = DeviceRoute::root();
        let (sink, metadata) = state_of(root);
        let (handle, proxy, _root) = pump(PumpSink::Metadata(sink));
        describe(&proxy, root, 1);
        wait_for(&metadata, |state| {
            snapshot(state).is_some_and(|snapshot| current_segment(snapshot) == 0)
        });

        // Firmware announces the current output segment. Subscribers must
        // see it even when no sample packet follows.
        broadcast(&proxy, wire::Metadata::Segment(segment_record(1)), root);
        wait_for(&metadata, |state| {
            snapshot(state).is_some_and(|snapshot| current_segment(snapshot) == 1)
        });

        let late = watch(&handle, root, PumpSink::Metadata);
        let first = late
            .recv_timeout(Duration::from_secs(5))
            .expect("the state");
        assert_eq!(snapshot(&first).map(current_segment), Some(1));
    }

    /// A session change makes the route rediscover. Subscribers must be told
    /// what it found instead of keeping what the old session said.
    #[test]
    fn a_new_session_republishes_metadata_once_the_route_rediscovers() {
        let root = DeviceRoute::root();
        let (sink, metadata) = state_of(root);
        let (_handle, proxy, _root) = pump(PumpSink::Metadata(sink));
        describe(&proxy, root, 1);
        wait_for(&metadata, |state| {
            snapshot(state).is_some_and(|snapshot| snapshot.device().session().value() == 1)
        });

        proxy.send(Packet::heartbeat_session(2, root));
        describe(&proxy, root, 2);
        wait_for(&metadata, |state| {
            snapshot(state).is_some_and(|snapshot| snapshot.device().session().value() == 2)
        });
    }

    /// Devices re-broadcast their metadata. Saying again what a subscriber
    /// already has is not news.
    #[test]
    fn re_broadcasting_identical_metadata_does_not_republish_it() {
        let root = DeviceRoute::root();
        let (sink, metadata) = state_of(root);
        let (_handle, proxy, _root) = pump(PumpSink::Metadata(sink));
        describe(&proxy, root, 1);
        wait_for(&metadata, |state| snapshot(state).is_some());

        describe(&proxy, root, 1);
        assert!(
            matches!(
                metadata.recv_timeout(Duration::from_millis(500)),
                Err(RecvError::Timeout)
            ),
            "identical metadata was republished"
        );
    }

    #[test]
    fn a_route_without_dev_metadata_is_asked_once() {
        let (sink, metadata) = state_of(DeviceRoute::root());
        let (_handle, proxy, _root) = pump(PumpSink::Metadata(sink));
        proxy.send(samples(0, DeviceRoute::root()));

        let (route, _, id) = metadata_call(&proxy);
        proxy.refuse(id, wire_rpc::RpcError::NotFound, route);

        let deadline = Instant::now() + Duration::from_secs(5);
        let reported = std::iter::from_fn(|| metadata.recv_deadline(deadline).ok())
            .any(|state| matches!(state, MetadataState::Unsupported));
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
        let presence = watch(&handle, DeviceRoute::root(), PumpSink::Presence);

        // The socket closes, which ends the session for good.
        drop(proxy);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match batches.recv_deadline(deadline) {
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Disconnected) => break,
                Err(RecvError::Stopped | RecvError::Closed) => panic!("a closed socket is no stop"),
                Err(RecvError::Timeout) => panic!("the batches outlived the session"),
            }
        }
        let ended = loop {
            match presence.recv_deadline(deadline) {
                Ok(_) => {}
                Err(ended) => break ended,
            }
        };
        assert_eq!(ended, RecvError::Disconnected);
    }

    /// An inlet overrun is only logged, as the samples mark the gap. The
    /// stream goes on and keeps what it knew.
    #[test]
    fn the_stream_goes_on_after_an_inlet_overrun() {
        let (session, proxy) = Session::test_pair();
        let handle = Handle::root(session);
        let (inlet, packets) = flume::unbounded();
        let deliver = |packet| inlet.send(Queued::Item(packet)).unwrap();
        let root = DeviceRoute::root();
        let (sink, metadata) = state_of(root);
        let (all, routes) = Sink::latest(everything());
        let _subscriptions = start_pump(
            &handle,
            packets,
            vec![PumpSink::Metadata(sink), PumpSink::Routes(all)],
        );
        deliver(samples(0, root));
        let (route, _, id) = metadata_call(&proxy);
        proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
        wait_for(&metadata, |state| {
            matches!(state, MetadataState::Unsupported)
        });

        inlet.send(Queued::Gap(1)).unwrap();
        deliver(samples(2, root));
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "the overrun did not forget that this route has no dev.metadata"
        );
        let child: DeviceRoute = "/1".parse().unwrap();
        deliver(samples(0, child));
        wait_for(&routes, |routes| routes.contains(&child));
    }

    /// A stop at the root is the session's last packet. The pump takes it,
    /// then ends, though the socket is still open. Every receiver says the
    /// proxy stopped, even one that carries no statuses and one that
    /// subscribed after.
    #[test]
    fn a_stop_at_the_root_is_published_then_ends_the_stream() {
        let (stream, proxy) = stream();
        let root = DeviceRoute::root();
        let presence = stream.presence(root);
        let batches = stream.batches(everything());
        proxy.send(Packet::heartbeat(root));
        wait_for(&presence, alive);

        proxy.send(Packet::link(packet::LinkStatus::Stopped));

        wait_for(&presence, lost);
        let wait = Duration::from_secs(5);
        assert_eq!(presence.recv_timeout(wait).err(), Some(RecvError::Stopped));
        assert_eq!(batches.recv_timeout(wait).err(), Some(RecvError::Stopped));
        let late = stream.batches(everything());
        assert_eq!(late.recv_timeout(wait).err(), Some(RecvError::Stopped));
        drop(proxy);
    }

    /// A subscriber whose queue is full still hears the stop, after what was
    /// queued.
    #[test]
    fn a_full_queue_still_ends_with_the_stop() {
        let (mut sink, receiver) = Sink::new(2, everything());
        assert!((0..3).all(|item| sink.lane.send(item)));

        sink.lane.end(End::Stopped);

        assert_eq!(receiver.try_recv(), Ok(Some(0)));
        assert_eq!(receiver.try_recv(), Ok(Some(1)));
        assert_eq!(receiver.try_recv(), Err(RecvError::Stopped));
        assert_eq!(receiver.try_recv(), Err(RecvError::Disconnected));
    }

    /// The status saying why the link failed may be the last thing the
    /// session queues, so the pump must publish what the inlet still holds
    /// before ending.
    #[test]
    fn the_last_packets_a_session_queued_are_still_published() {
        let root = DeviceRoute::root();
        let (sink, presence) = state_of(root);
        let (_handle, proxy, _root) = pump(PumpSink::Presence(sink));
        proxy.send(Packet::heartbeat(root));
        proxy.send(Packet::link(packet::LinkStatus::FailedToReconnect));
        drop(proxy);

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut heard = false;
        let ended = loop {
            match presence.recv_deadline(deadline) {
                Ok(now) => heard |= lost(&now),
                Err(ended) => break ended,
            }
        };
        assert!(heard, "the link's last status was not published");
        assert_eq!(ended, RecvError::Disconnected);
    }

    /// The pump ends once nothing can reach it any more: no view left to
    /// subscribe, and no receiver left holding the stream open.
    #[test]
    fn dropping_the_last_view_and_receiver_ends_the_pump() {
        let (sink, batches) = Sink::new(BATCH_QUEUE_LEN, everything());
        let (handle, proxy, root) = pump(PumpSink::Batches(sink));

        drop((handle, batches, root));

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
        let root = DeviceRoute::root();
        let (sink, presence) = state_of(root);
        let (all, routes) = Sink::latest(everything());
        let (handle, proxy, _root) =
            pump_with(vec![PumpSink::Presence(sink), PumpSink::Routes(all)]);
        proxy.send(samples(0, root));
        let (stale_route, _, stale) = metadata_call(&proxy);

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        wait_for(&presence, lost);
        proxy.refuse(stale, wire_rpc::RpcError::NotFound, stale_route);

        // The refusal reaches the pump ahead of the packet sent after it.
        let child: DeviceRoute = "/1".parse().unwrap();
        proxy.send(samples(0, child));
        wait_for(&routes, |routes| routes.contains(&child));
        let metadata = watch(&handle, root, PumpSink::Metadata);
        assert!(
            matches!(
                metadata.recv_timeout(Duration::from_secs(5)),
                Ok(MetadataState::Pending)
            ),
            "the stale refusal condemned a route that had moved on"
        );
    }

    /// A link's status reaches the device views it serves. Another device's
    /// heartbeat is not their business.
    #[test]
    fn a_device_view_follows_its_link_but_not_another_routes_heartbeat() {
        let root = DeviceRoute::root();
        let child: DeviceRoute = "/1".parse().unwrap();
        let (wide, other) = state_of(root);
        let (narrow, device) = state_of(child);
        let (_handle, proxy, _root) =
            pump_with(vec![PumpSink::Presence(wide), PumpSink::Presence(narrow)]);
        proxy.send(Packet::heartbeat(child));
        wait_for(&device, alive);

        // The root's heartbeat changes the root's presence, and only the
        // filter keeps it from the device view.
        proxy.send(Packet::heartbeat(root));
        wait_for(&other, alive);
        assert!(
            matches!(
                device.recv_timeout(Duration::from_millis(200)),
                Err(RecvError::Timeout)
            ),
            "the device view heard another route's heartbeat"
        );

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        let next = device.recv_timeout(Duration::from_secs(5));
        assert!(
            next.as_ref().is_ok_and(lost),
            "the device view follows its link, got {next:?}"
        );
    }

    /// A status carries a fabricated route, since `FailedToConnect` means
    /// nothing is there, so it must never mint a device. A proxy that predates
    /// `PROXY_RX` sends it as a legacy status, at the root.
    #[test]
    fn a_status_packet_discovers_no_route() {
        let (sink, routes) = Sink::latest(everything());
        let (_handle, proxy, _root) = pump(PumpSink::Routes(sink));
        let child: DeviceRoute = "/1".parse().unwrap();
        proxy.send(Packet::legacy_status(packet::LinkStatus::FailedToConnect));
        proxy.send(Packet::heartbeat(child));

        // Packets are taken in arrival order, so the status was taken by the
        // time the heartbeat's route is heard.
        let heard = wait_for(&routes, |routes| !routes.is_empty());
        assert_eq!(heard, [child], "only the heartbeat's route exists");
    }

    /// Behind `tio proxy --mount`, a status arrives on its mount's prefix: one
    /// sensor bouncing redescribes its own subtree and no one else's.
    #[test]
    fn a_mounts_disconnect_resets_only_its_own_subtree() {
        let steady: DeviceRoute = "/1".parse().unwrap();
        let bounced: DeviceRoute = "/2".parse().unwrap();
        let (a, steady_metadata) = state_of(steady);
        let (b, bounced_metadata) = state_of(bounced);
        let (c, steady_presence) = state_of(steady);
        let (d, bounced_presence) = state_of(bounced);
        let (_handle, proxy, _root) = pump_with(vec![
            PumpSink::Metadata(a),
            PumpSink::Metadata(b),
            PumpSink::Presence(c),
            PumpSink::Presence(d),
        ]);
        describe(&proxy, steady, 1);
        describe(&proxy, bounced, 1);
        wait_for(&steady_metadata, |state| snapshot(state).is_some());
        wait_for(&bounced_metadata, |state| snapshot(state).is_some());
        wait_for(&steady_presence, alive);
        wait_for(&bounced_presence, alive);

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected).with_route(bounced));
        wait_for(&bounced_metadata, |state| {
            matches!(state, MetadataState::Pending)
        });
        wait_for(&bounced_presence, lost);

        // Both re-broadcast, and only the bounced mount's description is news.
        describe(&proxy, steady, 1);
        describe(&proxy, bounced, 1);
        wait_for(&bounced_metadata, |state| snapshot(state).is_some());
        assert!(
            matches!(steady_metadata.try_recv(), Ok(None)),
            "the steady mount was reset too"
        );

        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected).with_route(bounced));
        wait_for(&bounced_presence, |presence| {
            *presence == Presence::Connecting
        });
        assert!(
            matches!(steady_presence.try_recv(), Ok(None)),
            "the steady mount reconnected too"
        );
    }

    /// A mount that stopped on purpose ends its session as a disconnected one
    /// does: its own subtree redescribes and no one else's does.
    #[test]
    fn a_mounts_stop_resets_only_its_own_subtree() {
        let steady: DeviceRoute = "/1".parse().unwrap();
        let stopped: DeviceRoute = "/2".parse().unwrap();
        let (a, steady_metadata) = state_of(steady);
        let (b, stopped_metadata) = state_of(stopped);
        let (_handle, proxy, _root) = pump_with(vec![PumpSink::Metadata(a), PumpSink::Metadata(b)]);
        describe(&proxy, steady, 1);
        describe(&proxy, stopped, 1);
        wait_for(&steady_metadata, |state| snapshot(state).is_some());
        wait_for(&stopped_metadata, |state| snapshot(state).is_some());

        proxy.send(Packet::link(packet::LinkStatus::Stopped).with_route(stopped));
        wait_for(&stopped_metadata, |state| {
            matches!(state, MetadataState::Pending)
        });

        // Both re-broadcast, and only the stopped mount's description is news.
        describe(&proxy, steady, 1);
        describe(&proxy, stopped, 1);
        wait_for(&stopped_metadata, |state| snapshot(state).is_some());
        assert!(
            matches!(steady_metadata.try_recv(), Ok(None)),
            "the steady mount was reset too"
        );
    }

    /// A proxy announces a mount's status as a link packet, and one that
    /// predates `PROXY_RX` announces it as a legacy status. The pump takes
    /// either the same way.
    #[test]
    fn a_link_packet_is_the_same_status_as_a_legacy_one() {
        let mount: DeviceRoute = "/2".parse().unwrap();
        let (sink, presence) = state_of(mount);
        let (_handle, proxy, _root) = pump(PumpSink::Presence(sink));
        let heard = |status: Packet| {
            proxy.send(Packet::heartbeat(mount));
            wait_for(&presence, alive);
            proxy.send(status.with_route(mount));
            presence
                .recv_timeout(Duration::from_secs(5))
                .expect("the pump publishes")
        };

        let legacy = heard(Packet::legacy_status(
            packet::LinkStatus::SensorDisconnected,
        ));
        let link = heard(Packet::link(packet::LinkStatus::SensorDisconnected));

        assert!(
            lost(&legacy),
            "the legacy status lost the mount, got {legacy:?}"
        );
        assert!(lost(&link), "the link status lost the mount, got {link:?}");
    }

    /// The affected subtree contains the device, not the other way around: a
    /// device below a mount follows that mount's status, and a device on
    /// another mount does not.
    #[test]
    fn a_device_below_the_affected_subtree_follows_its_status() {
        let pinned: DeviceRoute = "/1/0".parse().unwrap();
        let other: DeviceRoute = "/2".parse().unwrap();
        let (a, below) = state_of(pinned);
        let (b, elsewhere) = state_of(other);
        let (_handle, proxy, _root) = pump_with(vec![PumpSink::Presence(a), PumpSink::Presence(b)]);
        proxy.send(Packet::heartbeat(pinned));
        proxy.send(Packet::heartbeat(other));
        wait_for(&below, alive);
        wait_for(&elsewhere, alive);

        let mount: DeviceRoute = "/1".parse().unwrap();
        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected).with_route(mount));

        wait_for(&below, |presence| *presence == Presence::Connecting);
        assert!(
            matches!(
                elsewhere.recv_timeout(Duration::from_millis(200)),
                Err(RecvError::Timeout)
            ),
            "another mount's status is not this device's business"
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
    fn a_late_subscriber_starts_from_the_link_state_it_did_not_watch() {
        let mount: DeviceRoute = "/2".parse().unwrap();
        let (sink, early) = state_of(mount);
        let (handle, proxy, _root) = pump(PumpSink::Presence(sink));
        proxy.send(samples(0, mount));
        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected).with_route(mount));
        wait_for(&early, lost);

        let late = watch(&handle, mount, PumpSink::Presence);
        let first = late
            .recv_timeout(Duration::from_secs(5))
            .expect("the state");
        assert!(lost(&first), "the late subscriber starts from {first:?}");
        let routes = hand(&handle, Sink::latest(everything()), PumpSink::Routes);
        assert_eq!(routes.recv_timeout(Duration::from_secs(5)), Ok(vec![mount]));

        // A device below the mount was never heard, but its link's state
        // still gives its presence.
        let pinned = watch(&handle, "/2/1".parse().unwrap(), PumpSink::Presence);
        let first = pinned
            .recv_timeout(Duration::from_secs(5))
            .expect("the state");
        assert!(lost(&first), "the pinned subscriber starts from {first:?}");
    }

    /// A status supersedes the stored state of every subtree it covers, so a
    /// stale narrower status never outlives the broader one that ended it.
    #[test]
    fn a_broader_status_supersedes_the_narrower_state_it_covers() {
        let mount: DeviceRoute = "/2".parse().unwrap();
        let (sink, early) = state_of(mount);
        let (handle, proxy, _root) = pump(PumpSink::Presence(sink));
        proxy.send(samples(0, mount));
        wait_for(&early, alive);
        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected).with_route(mount));
        wait_for(&early, lost);
        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected));
        wait_for(&early, |presence| *presence == Presence::Connecting);

        // A device below the mount was never heard, so only the link's state
        // gives its presence.
        let late = watch(&handle, "/2/1".parse().unwrap(), PumpSink::Presence);
        assert_eq!(
            late.recv_timeout(Duration::from_secs(5)),
            Ok(Presence::Connecting),
            "the mount's stale disconnect outlived the reconnect that covered it"
        );
    }

    #[test]
    fn a_route_no_view_covers_is_neither_decoded_nor_asked_for_metadata() {
        let (sink, device) = state_of("/1".parse().unwrap());
        let (handle, proxy, _root) = pump(PumpSink::Metadata(sink));
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
        let metadata = stream.metadata(DeviceRoute::root());
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
        drop((metadata, batches));
    }

    /// A receiver is the subscription: it keeps the pump running on its own,
    /// so a caller need not hold the view it was minted from.
    #[test]
    fn a_receiver_keeps_the_pump_running_without_its_view() {
        let (stream, proxy) = stream();
        let presence = stream.presence(DeviceRoute::root());
        drop(stream);

        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        wait_for(&presence, alive);
    }

    /// A channel consults a deadline only once it runs dry, so a lane that
    /// stays saturated would deliver items past the caller's bound forever.
    #[test]
    fn a_saturated_subscription_still_honours_an_elapsed_deadline() {
        let (mut sink, receiver) = Sink::new(2, everything());
        assert!(sink.lane.send(1));
        assert!(sink.lane.send(2));

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
                .setting_changes(DeviceRoute::root())
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

    #[tokio::test]
    async fn an_async_receive_returns_a_queued_item() {
        let (mut sink, receiver) = Sink::new(1, everything());
        assert!(sink.lane.send(7));

        assert_eq!(receiver.recv_async().await, Ok(7));
    }

    #[tokio::test]
    async fn an_async_receive_waits_for_an_item_another_thread_sends_later() {
        let (mut sink, receiver) = Sink::new(1, everything());
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            sink.lane.send(7)
        });

        assert_eq!(receiver.recv_async().await, Ok(7));
        assert!(sender.join().unwrap());
    }

    /// A latest-value lane keeps only the newest item, so a reader that fell
    /// behind skips to it and is never told it lagged.
    #[test]
    fn a_latest_value_reader_skips_to_the_newest_item() {
        let (mut sink, receiver) = Sink::latest(everything());
        for value in 1..=5 {
            assert!(sink.lane.send(value));
        }

        assert_eq!(receiver.recv(), Ok(5));
        assert_eq!(receiver.try_recv(), Ok(None));

        assert!(sink.lane.send(6));
        assert_eq!(receiver.recv(), Ok(6));
    }

    /// A lane being replaced as fast as another thread can never looks empty
    /// to a reader who has not taken its item, and a reader who takes items
    /// meanwhile ends on the last one sent.
    #[test]
    fn a_latest_value_lane_never_looks_empty_while_it_is_replaced() {
        const SENT: u32 = 100_000;
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(0));
        let sender = std::thread::spawn(move || {
            for value in 1..=SENT {
                sink.lane.send(value);
            }
            sink
        });
        while !sender.is_finished() {
            assert!(!receiver.as_flume().is_empty(), "the lane looked empty");
        }
        let mut sink = sender.join().unwrap();
        assert_eq!(receiver.try_recv(), Ok(Some(SENT)));

        let sender = std::thread::spawn(move || {
            for value in 1..=SENT {
                sink.lane.send(value);
            }
        });
        let mut last = 0;
        while last < SENT {
            let value = receiver.recv().unwrap();
            assert!(value >= last, "{value} came after {last}");
            last = value;
        }
        sender.join().unwrap();
    }

    /// A blocking receive with a deadline times out on an empty lane, and
    /// waits for an item another thread sends while it waits.
    #[test]
    fn a_latest_value_reader_waits_until_a_deadline() {
        let (mut sink, receiver) = Sink::latest(everything());
        assert_eq!(
            receiver.recv_deadline(Instant::now() + Duration::from_millis(20)),
            Err(RecvError::Timeout)
        );

        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            sink.lane.send(7)
        });
        assert_eq!(
            receiver.recv_deadline(Instant::now() + Duration::from_secs(5)),
            Ok(7)
        );
        assert!(sender.join().unwrap());
    }

    #[tokio::test]
    async fn an_async_latest_value_receive_waits_for_the_next_item() {
        let (mut sink, receiver) = Sink::latest(everything());
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            sink.lane.send(7)
        });

        assert_eq!(receiver.recv_async().await, Ok(7));
        assert!(sender.join().unwrap());
    }

    /// The sync API may run inside an application's own tokio runtime, where
    /// tokio's `block_on` would panic. A latest-value receive blocks there as
    /// any other receive does.
    #[test]
    fn a_latest_value_receive_blocks_inside_a_tokio_runtime() {
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(3));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("start a runtime");

        let received = runtime.block_on(async { receiver.recv_timeout(Duration::from_secs(5)) });
        assert_eq!(received, Ok(3));
    }

    /// The end has a place of its own, so the last value is still read
    /// before the reader learns why the stream ended.
    #[test]
    fn a_latest_value_lane_delivers_its_last_item_before_its_end() {
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(1));
        sink.lane.end(End::Closed);

        assert_eq!(receiver.recv(), Ok(1));
        assert_eq!(receiver.recv(), Err(RecvError::Closed));
    }

    /// The lane holds its own end of the queue, which must not hide that the
    /// reader is gone.
    #[test]
    fn a_latest_value_lane_reports_a_dropped_reader() {
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(1));
        drop(receiver);

        assert!(!sink.lane.send(2));
    }

    /// An async receive ends as a blocking one does, so it too says the proxy
    /// stopped rather than only that the stream ended.
    #[tokio::test]
    async fn an_async_receive_ends_as_a_blocking_one_does() {
        let (stream, proxy) = stream();
        let awaited = stream.batches(everything());
        let blocked = stream.batches(everything());

        proxy.send(Packet::link(packet::LinkStatus::Stopped));

        assert_eq!(awaited.recv_async().await.err(), Some(RecvError::Stopped));
        assert_eq!(blocked.recv().err(), Some(RecvError::Stopped));
    }

    /// A device not yet heard takes its presence from its link, and its
    /// subscriber hears the link's status as one heard would.
    #[test]
    fn a_device_not_yet_heard_is_lost_with_its_link() {
        let (stream, proxy) = stream();
        let unheard: DeviceRoute = "/1".parse().expect("a valid route");
        let presence = stream.presence(unheard);
        assert_eq!(presence.recv(), Ok(Presence::Connecting));

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        assert!(matches!(
            presence.recv_timeout(Duration::from_secs(5)),
            Ok(Presence::Lost { .. })
        ));
    }

    /// A device is connecting until it is heard, alive with the session its
    /// heartbeat reports, lost when its link is, and connecting again when the
    /// link reconnects. A late subscriber starts from the presence now.
    #[test]
    fn presence_follows_packets_heartbeats_and_link_statuses() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let presence = stream.presence(route);
        assert_eq!(presence.recv(), Ok(Presence::Connecting));

        proxy.send(Packet::heartbeat_session(7, route));
        assert_eq!(
            presence.recv(),
            Ok(Presence::Alive {
                session: Some(WireSessionId::new(7))
            })
        );

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        assert!(matches!(presence.recv(), Ok(Presence::Lost { .. })));

        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected));
        assert_eq!(presence.recv(), Ok(Presence::Connecting));
        assert_eq!(stream.presence(route).recv(), Ok(Presence::Connecting));
    }

    /// A device's metadata is pending until it is complete, and pending again
    /// once its link drops it.
    #[test]
    fn metadata_state_is_ready_once_complete_and_pending_after_a_disconnect() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let metadata = stream.metadata(route);
        assert!(matches!(metadata.recv(), Ok(MetadataState::Pending)));

        describe(&proxy, route, 1);
        let deadline = Instant::now() + Duration::from_secs(5);
        let ready = std::iter::from_fn(|| metadata.recv_deadline(deadline).ok())
            .find_map(|state| match state {
                MetadataState::Ready(snapshot) => Some(snapshot),
                _ => None,
            })
            .expect("the described device is ready");
        assert_eq!(ready.device().name(), "d");

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        assert!(matches!(
            metadata.recv_timeout(Duration::from_secs(5)),
            Ok(MetadataState::Pending)
        ));
    }

    /// A device whose firmware has no `dev.metadata` is reported as such.
    #[test]
    fn metadata_state_reports_firmware_without_dev_metadata() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let metadata = stream.metadata(route);
        assert!(matches!(metadata.recv(), Ok(MetadataState::Pending)));

        proxy.send(samples(0, route));
        let (route, _, id) = metadata_call(&proxy);
        proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
        assert!(matches!(
            metadata.recv_timeout(Duration::from_secs(5)),
            Ok(MetadataState::Unsupported)
        ));
    }

    /// A route list holds every route heard within its scope, sorted, and
    /// only those.
    #[test]
    fn a_route_list_holds_the_routes_heard_within_its_scope() {
        let (stream, proxy) = stream();
        let below: DeviceRoute = "/1".parse().expect("a valid route");
        let routes = stream.routes(Scope::subtree(below));
        assert_eq!(routes.recv(), Ok(Vec::new()));

        let (second, first, outside) = ("/1/2", "/1/0", "/2");
        for route in [second, outside, first] {
            proxy.send(Packet::heartbeat(route.parse().expect("a valid route")));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        let heard = std::iter::from_fn(|| routes.recv_deadline(deadline).ok())
            .find(|heard| heard.len() == 2)
            .expect("both routes below are heard");
        let expected: Vec<DeviceRoute> = [first, second]
            .iter()
            .map(|route| route.parse().expect("a valid route"))
            .collect();
        assert_eq!(heard, expected);
        assert_eq!(
            stream.routes(everything()).recv().map(|all| all.len()),
            Ok(3)
        );
    }

    /// Setting changes arrive one by one, in the order announced.
    #[test]
    fn setting_changes_arrive_in_order() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let changes = stream.setting_changes(route);
        // The subscription reaches the pump before the announcements do.
        let _ = stream.presence(route).recv();

        proxy.send(announcement("test.amplitude", Some(9), &[1], route));
        proxy.send(announcement("test.amplitude", Some(10), &[2], route));
        let seqs: Vec<_> = (0..2)
            .map(|_| {
                changes
                    .recv_timeout(Duration::from_secs(5))
                    .map(|change| change.seq)
            })
            .collect();
        assert_eq!(seqs, [Ok(Some(9)), Ok(Some(10))]);
    }

    /// The name, hash and session of the table [`answer_table`] serves.
    const TABLE_DEVICE: &str = "state-test-device";
    const TABLE_HASH: u32 = 0x5747_e001;

    /// Answers the calls the session writes as a device with an announced
    /// setting and `settings.version` would, until no call comes for a
    /// while. Returns how many times the table was loaded.
    fn answer_table(proxy: &TestProxy) -> std::thread::JoinHandle<usize> {
        let (sent, deliver) = (proxy.sent.clone(), proxy.deliver.clone());
        std::thread::spawn(move || {
            let proxy = TestProxy { sent, deliver };
            let table = [("test.amplitude", 0xA310u16), ("settings.version", 0x8140)];
            let mut loads = 0;
            while let Ok(packet) = proxy.sent.recv_timeout(Duration::from_secs(2)) {
                let packet::Payload::RpcRequest(request) = packet.payload() else {
                    continue;
                };
                let wire_rpc::Method::ByName(name) = request.method else {
                    continue;
                };
                let (id, route) = (request.id.value(), packet.route());
                let reply = match std::str::from_utf8(name).expect("a UTF-8 name") {
                    "dev.name" => {
                        loads += 1;
                        TABLE_DEVICE.as_bytes().to_vec()
                    }
                    "rpc.hash" => TABLE_HASH.to_le_bytes().to_vec(),
                    "dev.session" => 7u32.to_le_bytes().to_vec(),
                    "rpc.listinfo" if request.args.is_empty() => {
                        (table.len() as u16).to_le_bytes().to_vec()
                    }
                    "rpc.listinfo" => {
                        let index = u16::from_le_bytes([request.args[0], request.args[1]]);
                        let (name, meta) = table[usize::from(index)];
                        let mut reply = meta.to_le_bytes().to_vec();
                        reply.extend_from_slice(name.as_bytes());
                        reply
                    }
                    "settings.version" => 5u32.to_le_bytes().to_vec(),
                    "test.amplitude" => vec![1],
                    _ => {
                        proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
                        continue;
                    }
                };
                proxy.reply(id, &reply, route);
            }
            loads
        })
    }

    /// The table is loaded once for every subscriber, the settings are read
    /// and then kept by each announcement, and a new table hash loads the
    /// table again.
    #[test]
    fn the_table_and_settings_load_once_and_follow_announcements() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let answering = answer_table(&proxy);
        let settings = stream.settings(route);
        let wait = Duration::from_secs(5);

        let first = settings.recv_timeout(wait).expect("the settings are read");
        assert_eq!(first.version(), Some(5));
        assert_eq!(first.get("test.amplitude"), Some(&[1u8][..]));
        assert_eq!(first.get("settings.version"), None);
        let registry = stream
            .rpc_registry(route)
            .recv_timeout(wait)
            .expect("the table is shared")
            .expect("the table loaded");
        assert_eq!(registry.hash, Some(TABLE_HASH));

        proxy.send(announcement("test.amplitude", Some(6), &[2], route));
        let next = settings
            .recv_timeout(wait)
            .expect("the announcement is kept");
        assert_eq!(
            (next.version(), next.get("test.amplitude")),
            (Some(6), Some(&[2u8][..]))
        );

        proxy.send(announcement(
            "rpc.hash",
            Some(7),
            &(TABLE_HASH + 1).to_le_bytes(),
            route,
        ));
        let reread = settings.recv_timeout(wait).expect("the table loads again");
        assert_eq!(reread.version(), Some(5));
        drop((settings, stream));
        assert_eq!(answering.join().expect("the device answers"), 2);
        if let Some(cached) = crate::device::rpc::cache::path(TABLE_DEVICE, TABLE_HASH) {
            let _ = std::fs::remove_file(cached);
        }
    }

    /// Every announcement reaches the subscribers of the device that sent
    /// it, and no other device's.
    #[test]
    fn an_announcement_becomes_a_setting_change_of_its_own_device() {
        let route: DeviceRoute = "/1".parse().expect("a valid route");
        let (sink, changes) = Sink::new(SETTING_QUEUE_LEN, Scope::point(route));
        let (other, elsewhere) = Sink::new(SETTING_QUEUE_LEN, Scope::point(DeviceRoute::root()));
        let (_subscriptions, proxy, _root) = pump_with(vec![
            PumpSink::SettingChanges(sink),
            PumpSink::SettingChanges(other),
        ]);
        proxy.send(announcement(
            "test.amplitude",
            Some(9),
            &[1, 2, 3, 4],
            route,
        ));

        let change = changes
            .recv_timeout(Duration::from_secs(5))
            .expect("the announcement");
        assert_eq!(
            change,
            SettingChange {
                name: "test.amplitude".to_string(),
                seq: Some(9),
                reply: vec![1, 2, 3, 4],
            }
        );
        assert!(
            matches!(
                elsewhere.recv_timeout(Duration::from_millis(200)),
                Err(RecvError::Timeout)
            ),
            "another device's announcement is not this device's change"
        );
    }

    /// The table hash travels as an announcement, so besides asking for a
    /// new table it is a setting change as any other.
    #[test]
    fn the_table_hash_is_also_a_setting_change() {
        let root = DeviceRoute::root();
        let (sink, changes) = Sink::new(SETTING_QUEUE_LEN, Scope::point(root));
        let (_subscriptions, proxy, _root) = pump(PumpSink::SettingChanges(sink));
        proxy.send(announcement("rpc.hash", None, &7u32.to_le_bytes(), root));

        let change = changes
            .recv_timeout(Duration::from_secs(5))
            .expect("the announcement");
        assert_eq!(
            (change.name.as_str(), change.seq, change.reply),
            ("rpc.hash", None, 7u32.to_le_bytes().to_vec())
        );
    }

    /// An update too large for one reply arrives in parts, and is whole once
    /// its last part is in. An update that lost a part is dropped, and the
    /// next one joins whole.
    #[test]
    fn a_watched_update_joins_from_its_parts_and_one_missing_a_part_is_dropped() {
        let entries = (0..100).map(|index| (format!("test.entry{index}"), 0x8140));
        let update = Update::Table {
            hash: Some(1),
            session: None,
            entries: entries.collect(),
        };
        let parts: Vec<Vec<u8>> = crate::proxy::parts(1, &update)
            .iter()
            .map(|packet| match packet.payload() {
                packet::Payload::ProxyRx(ProxyRx::Reply(reply)) => reply.value.to_vec(),
                other => panic!("a part is a reply, not {other:?}"),
            })
            .collect();
        assert!(parts.len() > 2, "the table takes several replies");
        let mut watched = Watched {
            watch: Watch {
                route: DeviceRoute::root(),
                table: true,
            },
            joined: Vec::new(),
            next: None,
        };

        let lost_one = parts.iter().enumerate().filter(|(index, _)| *index != 1);
        let joined: Vec<_> = lost_one
            .filter_map(|(_, part)| watched.join(part))
            .collect();
        assert!(joined.is_empty(), "an update that lost a part is dropped");
        let joined: Vec<_> = parts.iter().filter_map(|part| watched.join(part)).collect();
        assert_eq!(joined.len(), 1, "only the last part completes the update");
        assert_eq!(decode::<Update>(&joined[0]).unwrap(), update);
    }

    /// The watch request the pump wrote next, as its id and what it follows.
    fn watch_request(proxy: &TestProxy) -> (u16, Watch) {
        let request = proxy
            .sent
            .recv_timeout(Duration::from_secs(5))
            .expect("the pump watches the device");
        let packet::Payload::ProxyTx(request) = request.payload() else {
            panic!("expected a watch, got {:?}", request.payload());
        };
        (request.id.value(), decode(request.args).unwrap())
    }

    /// Hands the pump `update` as the proxy's replies to watch `id`.
    fn reply_update(proxy: &TestProxy, id: u16, update: &Update) {
        for part in crate::proxy::parts(id, update) {
            proxy.send(part);
        }
    }

    /// A table receiver taken with a change receiver holds the table by the
    /// time a change arrives, even as the settings that change replaces are
    /// on their way. A reader decodes each change with that table.
    #[test]
    fn a_table_subscribed_with_the_changes_is_held_when_a_change_arrives() {
        let (session, proxy) = Session::test_pair();
        let stream = Arc::new(Stream::new(Handle::root(session.watching())));
        let route = DeviceRoute::root();
        let loaded = stream.rpc_registry(route);
        let (table_watch, _) = watch_request(&proxy);
        let table = Update::Table {
            hash: Some(1),
            session: None,
            entries: vec![("test.gain".to_string(), 0xA742)],
        };
        reply_update(&proxy, table_watch, &table);
        loaded.recv().unwrap().expect("the table loads");

        let changes = stream.setting_changes(route);
        let tables = stream.rpc_registry(route);
        let (device_watch, _) = watch_request(&proxy);
        let announced = Update::Setting {
            name: "test.gain".to_string(),
            seq: Some(2),
            reply: 1.5f32.to_le_bytes().to_vec().into(),
        };
        reply_update(&proxy, device_watch, &announced);
        let values =
            std::collections::BTreeMap::from([("test.gain".to_string(), vec![2u8].into())]);
        let settings = Update::Settings {
            hash: Some(1),
            session: None,
            version: Some(2),
            values,
        };
        reply_update(&proxy, table_watch, &settings);

        assert_eq!(changes.recv().unwrap().seq, Some(2));
        let held = tables.try_recv().unwrap().expect("the table is held");
        assert!(held.unwrap().find("test.gain").is_some());
    }

    /// A pump that watches the proxy takes presence and setting changes from
    /// the watch alone. It watches a device once a presence subscriber asks,
    /// before the device is heard, and a heartbeat or an announcement it
    /// receives changes neither.
    #[test]
    fn a_watching_pump_takes_presence_and_setting_changes_from_the_proxy_alone() {
        let (session, proxy) = Session::test_pair();
        let stream = Arc::new(Stream::new(Handle::root(session.watching())));
        let route = DeviceRoute::root();
        let presence = stream.presence(route);
        let changes = stream.setting_changes(route);
        assert_eq!(presence.recv(), Ok(Presence::Connecting));
        let (id, watch) = watch_request(&proxy);
        assert_eq!(
            watch,
            Watch {
                route,
                table: false
            }
        );

        proxy.send(Packet::heartbeat_session(7, route));
        proxy.send(announcement("test.amplitude", Some(1), &[1], route));
        let quiet = Duration::from_millis(200);
        assert_eq!(presence.recv_timeout(quiet), Err(RecvError::Timeout));
        assert_eq!(changes.recv_timeout(quiet), Err(RecvError::Timeout));

        let announced = Update::Setting {
            name: "test.amplitude".to_string(),
            seq: Some(1),
            reply: vec![1].into(),
        };
        reply_update(&proxy, id, &Update::Alive(Some(7)));
        reply_update(&proxy, id, &announced);
        assert_eq!(
            presence.recv(),
            Ok(Presence::Alive {
                session: Some(WireSessionId::new(7))
            })
        );
        let change = changes.recv().expect("the proxy's announcement");
        assert_eq!(
            (change.name.as_str(), change.reply),
            ("test.amplitude", vec![1])
        );

        reply_update(&proxy, id, &Update::Lost(1000));
        let Ok(Presence::Lost { since }) = presence.recv() else {
            panic!("the proxy's loss of the device");
        };
        assert!(since.elapsed() >= Duration::from_secs(1));
    }

    /// A receiver dropped while the device is silent is pruned at once. A
    /// route only it covered, heard afterwards, is not asked for metadata
    /// until a live receiver covers it.
    #[tokio::test(start_paused = true)]
    async fn a_receiver_dropped_while_the_stream_is_silent_is_pruned() {
        let tasks = Supervisor::current();
        let (session, proxy) = Session::test_pair_on(&tasks);
        let root = Handle::root(session);
        let (subscriptions, commands) = mpsc::unbounded_channel();
        let pump = Pump::new(&root, root.session().attach(INLET_LEN), commands);
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
        let (session, proxy) = Session::test_pair_on(&tasks);
        let root = Handle::root(session);
        let (subscriptions, commands) = mpsc::unbounded_channel();
        let pump = Pump::new(&root, root.session().attach(INLET_LEN), commands);
        tasks.spawn("twinleaf stream", pump.run());
        let _batches = subscribe(&subscriptions, everything(), PumpSink::Batches);
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

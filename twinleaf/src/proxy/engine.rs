//! The engine of one link, the one place its devices' state is computed, and
//! the one reader of its packets.
//!
//! The engine reads every packet of its link through a session over a port
//! of its own, which never fills, so it loses nothing. It follows each
//! device's presence from the packets it hears and the link's statuses, and
//! loses a device silent for [`QUIET`], or one a reconnect left connecting
//! that stays silent as long. It collects the metadata of each device a
//! watch follows. It loads the RPC table and keeps the settings of each
//! device it has heard whose table a watch follows, on a worker thread per
//! device that stops once no watch has followed the table for [`LINGER`].
//! It keeps all of this in the link's [`Store`], which every client's
//! watches follow, for as long as the link runs.
//!
//! The engine then hands each packet, in the order the link delivered it, to
//! each client that mounts its route, through the client's [`Outlet`]. A
//! state update the packet caused reaches the outlet first, so a client
//! hears a record ahead of every sample the device sent after it.

use super::client::ClientError;
use super::link::Link;
use super::metadata::{Completed, MetadataSchedule};
use super::outbox::Outlet;
use super::settings::{CacheAction, DeviceEvent, SettingsCache};
use super::worker::within;
use super::Watch;
use crate::data::{MetadataQuery, PacketParser};
use crate::device::rpc::{PendingReply, RpcRegistry};
use crate::device::session::{Handle, Inlet, RawCallResult, Session};
use crate::device::store::{lock, pruned, Store, CHANGES, METADATA, PRESENCE};
use crate::device::stream::Queued;
use crate::device::{MetadataState, Presence, SettingChange, Settings};
use crate::proto::DeviceRoute;
use crate::rt::{sleep_until_some, Supervisor};
use crate::tio::packet::{self, LinkStatus, Packet};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// How long a device may be silent before the engine loses it. The link
/// loses the device at its root sooner, as it goes quiet.
const QUIET: Duration = Duration::from_secs(3);

/// How long a worker waits after its RPC table failed to load before it
/// loads it again, unless the device announces a new table first. It loads
/// it again only once the device is heard after the wait.
const RELOAD_DELAY: Duration = Duration::from_secs(5);

/// How long a worker keeps a device's table and settings once no watch
/// follows them, so a client that watches again soon finds them current.
#[cfg(not(test))]
const LINGER: Duration = Duration::from_secs(10);
#[cfg(test)]
const LINGER: Duration = Duration::from_millis(500);

/// Runs the engine of `link` until the link ends, with the tasks of its
/// session on `tasks`. Every receiver of the link's store then ends, and
/// every client that mounts the link is let go.
pub(super) async fn run(link: Link, tasks: Supervisor) {
    match link.open_session(&tasks) {
        Ok(session) => serve(link.store(), link.taps(), session).await,
        Err(_) => {
            lock(&link.store()).end_all(None);
            link.taps().end();
        }
    }
}

/// Keeps `store` from what arrives over `session`, and feeds `taps`, until
/// the session ends. Then ends every receiver of the store and lets every
/// client of `taps` go. It attaches to the session at once, so it takes every
/// packet that arrives from then on.
fn serve(
    store: Arc<Mutex<Store>>,
    taps: Arc<Taps>,
    session: Session,
) -> impl std::future::Future<Output = ()> {
    let inlet = session.attach(usize::MAX / 2);
    lock(&store).list(DeviceRoute::root(), []);
    let engine = Engine {
        store: Arc::clone(&store),
        taps: Arc::clone(&taps),
        schedule: MetadataSchedule::new(Handle::root(session.clone())),
        handle: Handle::root(session),
        inlet,
        parser: PacketParser::new(false),
        metadata_seen: HashMap::new(),
        heard: HashMap::new(),
        demand: BTreeSet::new(),
        workers: HashMap::new(),
    };
    async move {
        engine.run().await;
        lock(&store).end_all(None);
        taps.end();
    }
}

/// The clients' mounts of one link, which the link's engine feeds.
pub(super) struct Taps {
    /// The link's URL, which names it when a client is let go.
    url: String,
    mounts: Mutex<Vec<Feed>>,
}

/// One client's mount of a subtree of the link.
struct Feed {
    source: DeviceRoute,
    /// Where the client sees the subtree.
    prefix: DeviceRoute,
    outlet: Arc<Outlet>,
    /// The client's port on the link, which the mount lasts as long as.
    port: mpsc::WeakSender<Packet>,
    /// Whether the last link status the mount heard is `FailedToReconnect`.
    gave_up: bool,
}

impl Taps {
    /// Creates the mounts of the link to `url`, none yet.
    pub(super) fn new(url: &str) -> Arc<Taps> {
        let mounts = Mutex::default();
        let url = url.to_string();
        Arc::new(Taps { url, mounts })
    }

    /// Locks the mounts, and forgets each whose client let go of its port.
    fn lock(&self) -> MutexGuard<'_, Vec<Feed>> {
        let mut mounts = self.mounts.lock().unwrap_or_else(PoisonError::into_inner);
        mounts.retain(|feed| feed.port.strong_count() > 0);
        mounts
    }

    /// Feeds the client of `outlet` the packets of the subtree at `source`,
    /// at the routes it sees under `prefix`, for as long as it holds `port`.
    pub(super) fn add(
        &self,
        source: DeviceRoute,
        prefix: DeviceRoute,
        outlet: &Arc<Outlet>,
        port: &mpsc::Sender<Packet>,
    ) {
        self.lock().push(Feed {
            source,
            prefix,
            outlet: Arc::clone(outlet),
            port: port.downgrade(),
            gave_up: false,
        });
    }

    /// Hands `packet` to each client whose mount it falls in, at the route
    /// the client sees.
    fn feed(&self, packet: &Packet) {
        let status = packet.link_status();
        for feed in self.lock().iter_mut() {
            let Some(below) = within(feed.source, packet) else {
                continue;
            };
            let Ok(route) = feed.prefix.absolute_route(&below) else {
                log::warn!("Dropping a packet whose route {below} exceeds the max depth");
                continue;
            };
            if let Some(status) = status {
                feed.gave_up = status == LinkStatus::FailedToReconnect;
            }
            feed.outlet.push(packet.with_route(route));
        }
    }

    /// Lets each client go, as the link ended. A mount that last heard
    /// `FailedToReconnect` hears that the proxy gave up on the link.
    pub(super) fn end(&self) {
        for feed in self.lock().drain(..) {
            let url = self.url.clone();
            let error = match feed.gave_up {
                true => ClientError::GaveUp(url),
                false => ClientError::LinkEnded(url),
            };
            feed.outlet.end(error);
        }
    }
}

/// The engine's state, besides what it keeps in the store.
struct Engine {
    store: Arc<Mutex<Store>>,
    /// The clients the engine feeds.
    taps: Arc<Taps>,
    handle: Handle,
    inlet: Inlet,
    parser: PacketParser,
    schedule: MetadataSchedule,
    /// The metadata revision of each route last published.
    metadata_seen: HashMap<DeviceRoute, u32>,
    /// When each device alive or connecting was last heard, or set
    /// connecting.
    heard: HashMap<DeviceRoute, Instant>,
    /// What the store's receivers follow, as it last said.
    demand: BTreeSet<Watch>,
    /// The events of each device whose RPC table a receiver follows, which
    /// its worker takes, and since when no receiver has followed it.
    workers: HashMap<DeviceRoute, (flume::Sender<DeviceEvent>, Option<Instant>)>,
}

impl Engine {
    /// Runs until the session over the link's port ends.
    async fn run(mut self) {
        let (store, followed) = (
            Arc::clone(&self.store),
            Arc::clone(&lock(&self.store).followed),
        );
        loop {
            let quiet = self.heard.values().map(|at| *at + QUIET).min();
            let unwatched = self.workers.values().filter_map(|(_, since)| *since);
            let linger = unwatched.min().map(|since| since + LINGER);
            let wake = [self.schedule.next_retry(), quiet, linger];
            let wake = wake.into_iter().flatten().min();
            tokio::select! {
                biased;
                () = followed.notified() => self.follow_demand(),
                () = pruned(|cx| lock(&store).prune_gone(cx)) => self.follow_demand(),
                Ok(reply) = self.schedule.replies().recv_async() => self.take_metadata_replies(reply),
                received = self.inlet.recv_async() => {
                    let Ok(first) = received else { return };
                    let inlet = self.inlet.clone();
                    let held = inlet.try_iter().take(inlet.len());
                    for queued in std::iter::once(first).chain(held) {
                        match queued {
                            Queued::Item(Some(packet)) => {
                                self.take(&packet);
                                self.taps.feed(&packet);
                            }
                            Queued::Item(None) => {}
                            Queued::Gap(lost) => log::error!("the engine lost {lost} packets"),
                            Queued::Ended(_) => return,
                        }
                    }
                }
                () = sleep_until_some(wake) => {
                    self.lose_quiet();
                    self.stop_unwatched();
                }
            }
            self.discover();
        }
    }

    /// Starts a worker for each table a receiver now follows of a device
    /// heard, and notes since when no receiver follows the table of each
    /// other worker.
    fn follow_demand(&mut self) {
        let mut store = lock(&self.store);
        self.demand = store.demand();
        let now = Instant::now();
        for (route, (_, since)) in &mut self.workers {
            match self.demand.contains(&Watch {
                route: *route,
                table: true,
            }) {
                true => *since = None,
                false => *since = since.or(Some(now)),
            }
        }
        let wanted: Vec<_> = self
            .demand
            .iter()
            .filter(|watch| watch.table && store.routes().contains(&watch.route))
            .filter(|watch| !self.workers.contains_key(&watch.route))
            .map(|watch| watch.route)
            .collect();
        drop(store);
        for route in wanted {
            let events = start_worker(self.handle.clone(), route, Arc::clone(&self.store));
            self.workers.insert(route, (events, None));
        }
    }

    /// Stops each worker whose table no receiver has followed for
    /// [`LINGER`], and drops the table and settings it kept, which no one
    /// keeps current after.
    fn stop_unwatched(&mut self) {
        let now = Instant::now();
        // The store stays locked until each stopped worker is let go, and a
        // worker sets nothing in it once it is.
        let mut store = lock(&self.store);
        self.workers.retain(|route, (_, since)| {
            let lingers = since.is_none_or(|since| since + LINGER > now);
            if !lingers {
                store.clear(Watch {
                    route: *route,
                    table: true,
                });
            }
            lingers
        });
    }

    /// Hands a fact about the device at `route` to its worker, if it has one.
    fn tell(&self, route: DeviceRoute, event: DeviceEvent) {
        if let Some((events, _)) = self.workers.get(&route) {
            let _ = events.send(event);
        }
    }

    /// Takes one packet the link delivered into the store.
    fn take(&mut self, packet: &Packet) {
        let route = packet.route();
        if let Some(status) = packet.link_status() {
            return self.apply_status(route, status);
        }
        self.heard.insert(route, Instant::now());
        let mut store = lock(&self.store);
        let new = !store.routes().contains(&route);
        if new {
            store.list(DeviceRoute::root(), [route]);
        }
        let held = match store.value(route, PRESENCE) {
            Some(Presence::Alive { session }) => Some(session),
            _ => None,
        };
        let session = match packet.payload() {
            packet::Payload::Heartbeat(beat) => beat.session().or(held.flatten()),
            _ => held.flatten(),
        };
        if held != Some(session) {
            store.set(route, PRESENCE, Presence::Alive { session });
        }
        drop(store);
        if new {
            self.follow_demand();
        }
        match packet.payload() {
            packet::Payload::Heartbeat(beat) => self.tell(
                route,
                DeviceEvent::Heartbeat {
                    session_id: beat.session(),
                    settings: beat.settings(),
                },
            ),
            packet::Payload::Setting(setting) => {
                if let Some(hash) = rpc_hash(&setting) {
                    self.tell(route, DeviceEvent::NewHash(Some(hash)));
                }
                if let Some(name) = setting.name_str() {
                    let change = SettingChange {
                        name: name.to_string(),
                        seq: setting.seq,
                        reply: setting.reply.to_vec(),
                    };
                    lock(&self.store).set(route, CHANGES, change.clone());
                    self.tell(route, DeviceEvent::Setting(change));
                }
            }
            packet::Payload::Samples(_) => return,
            _ => {}
        }
        if let Err(error) = self.parser.push_packet(packet) {
            log::debug!("the engine dropped an invalid packet: {error}");
        }
        while self.parser.pop_batch().is_some() {}
        self.publish_metadata(route);
    }

    /// Applies a link's status to the subtree it concerns. A device it sets
    /// connecting is lost unless it speaks within [`QUIET`].
    fn apply_status(&mut self, subtree: DeviceRoute, status: LinkStatus) {
        let presence = match status {
            LinkStatus::SensorDisconnected
            | LinkStatus::Stopped
            | LinkStatus::FailedToConnect
            | LinkStatus::FailedToReconnect => Presence::Lost {
                since: std::time::Instant::now(),
            },
            LinkStatus::SensorReconnected => Presence::Connecting,
            LinkStatus::Unknown(_) => return,
        };
        self.heard.retain(|route, _| !route.starts_with(&subtree));
        let forget = matches!(status, LinkStatus::SensorDisconnected | LinkStatus::Stopped);
        if forget {
            self.parser.reset_subtree(subtree);
            self.schedule.forget(subtree);
            self.metadata_seen
                .retain(|route, _| !route.starts_with(&subtree));
        }
        // A device a receiver follows that was not heard yet goes with its
        // link too.
        let mut store = lock(&self.store);
        let followed = self.demand.iter().map(|watch| &watch.route);
        let served: BTreeSet<_> = store
            .routes()
            .iter()
            .chain(followed)
            .filter(|route| route.starts_with(&subtree))
            .copied()
            .collect();
        for route in served {
            // A loss keeps the moment of the first.
            let lost = matches!(store.value(route, PRESENCE), Some(Presence::Lost { .. }));
            if presence == Presence::Connecting || !lost {
                store.set(route, PRESENCE, presence);
            }
            if forget {
                store.set(route, METADATA, MetadataState::Pending);
            }
            if presence == Presence::Connecting {
                self.heard.insert(route, Instant::now());
                self.tell(route, DeviceEvent::NewHash(None));
            }
        }
    }

    /// Loses each device silent for [`QUIET`].
    fn lose_quiet(&mut self) {
        let now = Instant::now();
        let mut store = lock(&self.store);
        self.heard.retain(|route, at| {
            let heard = *at + QUIET > now;
            if !heard {
                let since = std::time::Instant::now();
                store.set(*route, PRESENCE, Presence::Lost { since });
            }
            heard
        });
    }

    /// Publishes the metadata of `route` if it is complete and a revision
    /// not yet published.
    fn publish_metadata(&mut self, route: DeviceRoute) {
        let seen = self.metadata_seen.get(&route).copied();
        let Some((revision, snapshot)) = self.parser.revised_metadata(route, seen) else {
            return;
        };
        self.metadata_seen.insert(route, revision);
        lock(&self.store).set(route, METADATA, MetadataState::Ready(snapshot));
    }

    /// Takes `first` and every other finished `dev.metadata` call.
    fn take_metadata_replies(&mut self, first: (MetadataQuery, RawCallResult)) {
        let mut reply = Some(first);
        while let Some((query, result)) = reply
            .take()
            .or_else(|| self.schedule.replies().try_recv().ok())
        {
            let route = query.route;
            match self.schedule.complete(&mut self.parser, query, result) {
                Completed::Applied => self.publish_metadata(route),
                Completed::Unsupported => {
                    lock(&self.store).set(route, METADATA, MetadataState::Unsupported)
                }
                Completed::BackedOff => {}
            }
        }
    }

    /// Asks each device a receiver follows, heard and due, for whatever
    /// metadata the parser still wants.
    fn discover(&mut self) {
        let demand = &self.demand;
        let followed = self.heard.keys().copied();
        let followed = followed.filter(|route| {
            demand.contains(&Watch {
                route: *route,
                table: false,
            })
        });
        let due = self.schedule.due(followed, Instant::now());
        for route in due {
            self.schedule.submit(&mut self.parser, route);
        }
    }
}

/// Returns the RPC table hash from an `rpc.hash` setting, a little-endian
/// `u32`, or `None` for any other setting.
fn rpc_hash(setting: &crate::proto::settings::Setting<'_>) -> Option<u32> {
    let value = setting
        .reply
        .get(..4)
        .filter(|_| setting.name == b"rpc.hash")?;
    Some(u32::from_le_bytes(value.try_into().ok()?))
}

/// Starts the worker that loads the RPC table of the device at `route`
/// through `handle` and keeps its settings in `store`, and returns the
/// sender of the device's events it follows. It stops once the sender is
/// dropped.
fn start_worker(
    handle: Handle,
    route: DeviceRoute,
    store: Arc<Mutex<Store>>,
) -> flume::Sender<DeviceEvent> {
    let (events, received) = flume::unbounded();
    let worker = Worker {
        handle,
        route,
        events: received,
        store,
    };
    let started = std::thread::Builder::new()
        .name(format!("twinleaf settings {route}"))
        .spawn(move || worker.run());
    if let Err(error) = started {
        log::warn!("could not start the settings worker of {route}: {error}");
    }
    events
}

/// The worker of one device. Its calls block, so it runs on a thread of its
/// own.
struct Worker {
    handle: Handle,
    route: DeviceRoute,
    events: flume::Receiver<DeviceEvent>,
    store: Arc<Mutex<Store>>,
}

impl Worker {
    /// Loads the table and tracks the device until a new table or a new
    /// session calls for loading it again, until the engine lets go.
    fn run(self) {
        while !self.events.is_disconnected() {
            let loaded = RpcRegistry::load_with(|name, arg| self.submit(name, arg));
            let registry = loaded.map(Arc::new);
            {
                let mut store = lock(&self.store);
                if self.events.is_disconnected() {
                    return;
                }
                store.set_registry(self.route, registry.clone());
            }
            let tracked = match registry {
                Ok(registry) => self.track(&registry),
                Err(error) => {
                    log::warn!("could not load the RPC table of {}: {error}", self.route);
                    self.await_new_table()
                }
            };
            if !tracked {
                return;
            }
        }
    }

    /// Keeps the settings of `registry` until it is out of date. Returns
    /// `false` once the engine lets go.
    fn track(&self, registry: &Arc<RpcRegistry>) -> bool {
        let mut cache = SettingsCache::new(registry);
        self.snapshot(registry, &mut cache);
        loop {
            let Ok(event) = self.events.recv() else {
                return false;
            };
            match &event {
                DeviceEvent::NewHash(None) => return true,
                DeviceEvent::NewHash(Some(hash)) if Some(*hash) != registry.hash => return true,
                DeviceEvent::Heartbeat {
                    session_id: Some(session),
                    ..
                } if registry.session.is_some_and(|read_in| read_in != *session) => return true,
                _ => {}
            }
            let before = cache.version();
            match cache.observe(&event) {
                CacheAction::Keep if cache.version() == before => {}
                CacheAction::Keep => self.publish(registry, &cache),
                CacheAction::Snapshot => self.snapshot(registry, &mut cache),
                CacheAction::Reload => return true,
            }
        }
    }

    /// Fills `cache` with a snapshot and publishes the settings it holds
    /// then. A failed snapshot is logged, and the cache asks for another at
    /// the next heartbeat.
    fn snapshot(&self, registry: &Arc<RpcRegistry>, cache: &mut SettingsCache) {
        if self.events.is_disconnected() {
            return;
        }
        if let Err(error) = cache.snapshot_with(|name, arg| self.submit(name, arg)) {
            log::warn!("could not read the settings of {}: {error}", self.route);
        }
        self.publish(registry, cache);
    }

    /// Puts the settings `cache` holds, read with `registry`, in the store,
    /// unless the engine has let go.
    fn publish(&self, registry: &Arc<RpcRegistry>, cache: &SettingsCache) {
        let settings = Settings::new(
            Arc::clone(registry),
            cache.version(),
            cache.values().clone(),
        );
        let mut store = lock(&self.store);
        if !self.events.is_disconnected() {
            store.set_settings(self.route, Arc::new(settings));
        }
    }

    /// Waits for the device to announce a new table, or for the delay
    /// before loading again and then for the device to be heard, so a
    /// device that is not there is not asked again. Returns `false` once the
    /// engine lets go.
    fn await_new_table(&self) -> bool {
        let deadline = std::time::Instant::now() + RELOAD_DELAY;
        loop {
            match self.events.recv_deadline(deadline) {
                Ok(DeviceEvent::NewHash(_)) => return true,
                Ok(_) => {}
                Err(flume::RecvTimeoutError::Timeout) => return self.events.recv().is_ok(),
                Err(flume::RecvTimeoutError::Disconnected) => return false,
            }
        }
    }

    fn submit(&self, name: &str, arg: &[u8]) -> PendingReply {
        PendingReply {
            reply: self.handle.submit(self.route, name, arg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::DeviceMetadataSnapshot;
    use crate::device::fixtures::{
        announcement, broadcast, description, device_reply, samples, segment_record, wait_for,
    };
    use crate::device::session::TestProxy;
    use crate::device::store::{Field, REGISTRY, SETTINGS};
    use crate::device::stream::{Receiver, RecvError, Scope};
    use crate::proto::data as wire;
    use crate::proto::rpc as wire_rpc;
    use crate::proto::SessionId;
    use crate::proxy::client::device_updates;
    use crate::proxy::control::{Joined, Update};
    use crate::tio::packet::{Payload, ProxyRx};
    use std::time::Instant as StdInstant;

    /// Feeds a client the whole link, with a watch of the device at the
    /// root, from the engine whose store is `store`, for as long as the
    /// client holds the port returned.
    fn client_of(store: &Mutex<Store>, taps: &Taps) -> (Arc<Outlet>, mpsc::Sender<Packet>) {
        let (outlet, root) = (Arc::<Outlet>::default(), DeviceRoute::root());
        outlet.watch(1, root, device_updates(store, root));
        let port = mpsc::channel(1).0;
        taps.add(root, root, &outlet, &port);
        (outlet, port)
    }

    /// Takes what `outlet` holds, as a client's task does, pausing between
    /// takes for a while that `seed` picks, until a packet `done` picks has
    /// gone. Returns everything taken, in order.
    fn take_until(outlet: &Arc<Outlet>, seed: u64, done: impl Fn(&Packet) -> bool) -> Vec<Packet> {
        let deadline = StdInstant::now() + Duration::from_secs(20);
        let (mut taken, mut pause) = (Vec::new(), seed);
        loop {
            assert!(
                StdInstant::now() < deadline,
                "the last packet never went out"
            );
            let (queued, _) = outlet.take().expect("the client is kept");
            let finished = queued.iter().any(&done);
            taken.extend(queued);
            if finished {
                return taken;
            }
            pause = (pause * 7 + 3) % 11;
            std::thread::sleep(Duration::from_micros(pause * 200));
        }
    }

    /// A metadata update's place in what a client took, the id and start of
    /// each segment it holds, and its device's session.
    type Renewal = (usize, Vec<(u8, u32)>, u32);

    /// Each metadata update in `taken`, as the client joins them.
    fn segment_updates(taken: &[Packet]) -> Vec<Renewal> {
        let mut joined = Joined::default();
        let mut updates = Vec::new();
        for (place, packet) in taken.iter().enumerate() {
            let Payload::ProxyRx(ProxyRx::Reply(reply)) = packet.payload() else {
                continue;
            };
            let Some(Update::Metadata(Some(records))) = joined.join(reply.value) else {
                continue;
            };
            let (mut segments, mut session) = (Vec::new(), 0);
            for record in records {
                let (record, _) = Packet::from_slice_prefix(&record).unwrap();
                match record.payload() {
                    Payload::Metadata(wire::Metadata::Segment(segment), _) => {
                        segments.push((segment.segment_id.value(), segment.start_time))
                    }
                    Payload::Metadata(wire::Metadata::Device(device), _) => {
                        session = device.session.value()
                    }
                    _ => {}
                }
            }
            updates.push((place, segments, session));
        }
        updates
    }

    /// Whatever the interleaving of segment records and samples, and however
    /// the client's task takes from its outlet meanwhile, the client hears
    /// each record ahead of every sample the device sent after it. A sample
    /// that goes out goes out under the record it was sent under.
    #[test]
    fn a_client_hears_each_record_ahead_of_every_sample_sent_after_it() {
        let root = DeviceRoute::root();
        let done = route("/9");
        for seed in 1..=4u64 {
            let (store, taps, proxy) = engine_with_taps(&Supervisor::new());
            let (outlet, _tap) = client_of(&store, &taps);
            let taking = Arc::clone(&outlet);
            let taker = std::thread::spawn(move || {
                take_until(&taking, seed, |packet| packet.route() == done)
            });
            describe(&proxy, root, 1);
            let mut random = seed;
            let mut next = move || {
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                random >> 60
            };
            let (mut segment, mut start, mut first) = (0, 0, 0);
            let mut sent = HashMap::new();
            for _ in 0..3000 {
                match next() {
                    0 => {
                        (segment, start) = (segment ^ 1, start + 1);
                        proxy.send(broadcast(record(segment, start), root));
                    }
                    1 => proxy.send(broadcast(record(segment, start), root)),
                    _ => {
                        sent.insert(first, (segment, start));
                        proxy.send(Packet::samples(1, segment, first, &[0; 400], root).unwrap());
                        first += 100;
                    }
                }
            }
            proxy.send(Packet::heartbeat(done));
            let taken = taker.join().unwrap();

            let updates = segment_updates(&taken);
            let mut held = HashMap::new();
            let mut updates = updates.into_iter().peekable();
            let mut decoded = 0;
            for (place, packet) in taken.iter().enumerate() {
                while let Some((_, segments, _)) = updates.next_if(|(at, ..)| *at <= place) {
                    held.extend(segments);
                }
                let Payload::Samples(data) = packet.payload() else {
                    continue;
                };
                let (segment, start) = sent[&data.first.value()];
                assert_eq!(
                    held.get(&segment),
                    Some(&start),
                    "seed {seed}: sample {} went out under another record",
                    data.first.value()
                );
                decoded += 1;
            }
            assert!(decoded > 0, "seed {seed}: no sample went out");
        }
    }

    /// The segment record of `segment` that starts at `start`.
    fn record(segment: u8, start: u32) -> wire::Metadata<'static> {
        wire::Metadata::Segment(wire::Segment {
            start_time: start,
            ..segment_record(segment)
        })
    }

    /// A heartbeat of the old session goes out ahead of the metadata of the
    /// new one, and the samples the device sent after it go out after it,
    /// so a client never meets a stale heartbeat behind newer metadata.
    #[test]
    fn an_old_sessions_heartbeat_goes_out_ahead_of_the_new_sessions_metadata() {
        let root = DeviceRoute::root();
        let (store, taps, proxy) = engine_with_taps(&Supervisor::new());
        let (outlet, _tap) = client_of(&store, &taps);
        describe(&proxy, root, 1);
        proxy.send(Packet::heartbeat_session(1, root));
        proxy.send(Packet::heartbeat_session(2, root));
        describe(&proxy, root, 2);
        proxy.send(samples(5, root));

        let is_sample = |packet: &Packet| matches!(packet.payload(), Payload::Samples(_));
        let taken = take_until(&outlet, 0, is_sample);
        let beat = taken.iter().position(|packet| match packet.payload() {
            Payload::Heartbeat(beat) => beat.session().map(SessionId::value) == Some(1),
            _ => false,
        });
        let updates = segment_updates(&taken);
        let renewed = updates.iter().find(|(_, _, session)| *session == 2);
        let sample = taken.iter().position(is_sample);
        let renewed = renewed.map(|(place, ..)| *place);
        assert!(
            beat < renewed && renewed < sample,
            "{beat:?} {renewed:?} {sample:?}"
        );
        assert!(beat.is_some());
    }

    /// An engine over a session whose socket is a pair of channels, with
    /// its tasks on `tasks`. Returns its store and the session's far end,
    /// which plays the link.
    fn engine_on(tasks: &Supervisor) -> (Arc<Mutex<Store>>, TestProxy) {
        let (store, _, proxy) = engine_with_taps(tasks);
        (store, proxy)
    }

    /// An engine as [`engine_on`] starts it, with the mounts it feeds.
    fn engine_with_taps(tasks: &Supervisor) -> (Arc<Mutex<Store>>, Arc<Taps>, TestProxy) {
        let (session, proxy) = Session::test_pair_on(tasks);
        let store = Arc::new(Mutex::new(Store::new(true)));
        let taps = Taps::new("tcp://device");
        let serving = serve(Arc::clone(&store), Arc::clone(&taps), session);
        tasks.spawn("proxy state", serving);
        (store, taps, proxy)
    }

    fn engine() -> (Arc<Mutex<Store>>, TestProxy) {
        engine_on(&Supervisor::new())
    }

    fn follow<T: Clone>(store: &Mutex<Store>, route: DeviceRoute, field: Field<T>) -> Receiver<T> {
        lock(store).follow(route, field)
    }

    fn presence(store: &Mutex<Store>, route: DeviceRoute) -> Receiver<Presence> {
        follow(store, route, PRESENCE)
    }

    fn metadata(store: &Mutex<Store>, route: DeviceRoute) -> Receiver<MetadataState> {
        follow(store, route, METADATA)
    }

    fn route(text: &str) -> DeviceRoute {
        text.parse().expect("a valid route")
    }

    fn describe(proxy: &TestProxy, route: DeviceRoute, session: u32) {
        for packet in description(route, session) {
            proxy.send(packet);
        }
    }

    /// The `dev.metadata` call the engine submitted: its route, its args,
    /// and its id.
    fn metadata_call(proxy: &TestProxy) -> (DeviceRoute, Vec<u8>, u16) {
        let (route, name, args, id) = proxy.call();
        assert_eq!(name, wire::METADATA_RPC_METHOD);
        (route, args, id)
    }

    /// Whether the session wrote anything within `within`.
    fn wrote_within(proxy: &TestProxy, within: Duration) -> bool {
        proxy.sent.recv_timeout(within).is_ok()
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

    fn snapshot(state: &MetadataState) -> Option<&DeviceMetadataSnapshot> {
        match state {
            MetadataState::Ready(snapshot) => Some(snapshot),
            MetadataState::Pending | MetadataState::Unsupported => None,
        }
    }

    fn lost(presence: &Presence) -> bool {
        matches!(presence, Presence::Lost { .. })
    }

    fn alive(presence: &Presence) -> bool {
        matches!(presence, Presence::Alive { .. })
    }

    #[test]
    fn metadata_discovery_completes_over_independent_calls() {
        let (store, proxy) = engine();
        let child = route("/1");
        let _followed = [
            metadata(&store, DeviceRoute::root()),
            metadata(&store, child),
        ];
        proxy.send(samples(0, DeviceRoute::root()));
        proxy.send(samples(0, child));

        // Both routes bootstrap at once, and answering them out of order
        // must still reach the right parser state.
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
    }

    /// A segment UPDATE replaces the current segment of a route that never
    /// stopped being complete, so completeness alone would miss the change.
    #[test]
    fn a_live_segment_rollover_republishes_metadata_and_replays_the_latest() {
        let root = DeviceRoute::root();
        let (store, proxy) = engine();
        let followed = metadata(&store, root);
        describe(&proxy, root, 1);
        wait_for(&followed, |state| {
            snapshot(state).is_some_and(|snapshot| current_segment(snapshot) == 0)
        });

        proxy.send(broadcast(wire::Metadata::Segment(segment_record(1)), root));
        wait_for(&followed, |state| {
            snapshot(state).is_some_and(|snapshot| current_segment(snapshot) == 1)
        });

        let late = metadata(&store, root);
        let first = late
            .recv_timeout(Duration::from_secs(5))
            .expect("the state");
        assert_eq!(snapshot(&first).map(current_segment), Some(1));
    }

    /// A session change makes the route rediscover. Receivers are told what
    /// it found instead of keeping what the old session said.
    #[test]
    fn a_new_session_republishes_metadata_once_the_route_rediscovers() {
        let root = DeviceRoute::root();
        let (store, proxy) = engine();
        let followed = metadata(&store, root);
        describe(&proxy, root, 1);
        wait_for(&followed, |state| {
            snapshot(state).is_some_and(|snapshot| snapshot.device().session().value() == 1)
        });

        proxy.send(Packet::heartbeat_session(2, root));
        describe(&proxy, root, 2);
        wait_for(&followed, |state| {
            snapshot(state).is_some_and(|snapshot| snapshot.device().session().value() == 2)
        });
    }

    /// Devices re-broadcast their metadata. Saying again what a receiver
    /// already has is not news.
    #[test]
    fn re_broadcasting_identical_metadata_does_not_republish_it() {
        let root = DeviceRoute::root();
        let (store, proxy) = engine();
        let followed = metadata(&store, root);
        describe(&proxy, root, 1);
        wait_for(&followed, |state| snapshot(state).is_some());

        describe(&proxy, root, 1);
        assert!(
            matches!(
                followed.recv_timeout(Duration::from_millis(500)),
                Err(RecvError::Timeout)
            ),
            "identical metadata was republished"
        );
    }

    #[test]
    fn a_route_without_dev_metadata_is_asked_once() {
        let (store, proxy) = engine();
        let followed = metadata(&store, DeviceRoute::root());
        proxy.send(samples(0, DeviceRoute::root()));

        let (route, _, id) = metadata_call(&proxy);
        proxy.refuse(id, wire_rpc::RpcError::NotFound, route);

        wait_for(&followed, |state| {
            matches!(state, MetadataState::Unsupported)
        });
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "discovery does not re-arm for a route with no dev.metadata"
        );
    }

    /// A refusal answering a query the reset already overtook belongs to the
    /// session that ended, and must not condemn the route discovering now.
    #[test]
    fn a_refusal_from_before_a_reset_does_not_condemn_the_route() {
        let root = DeviceRoute::root();
        let (store, proxy) = engine();
        let followed = presence(&store, root);
        let routes = lock(&store).follow_routes(Scope::subtree(root));
        proxy.send(samples(0, root));
        let (stale_route, _, stale) = metadata_call(&proxy);

        proxy.send(Packet::link(LinkStatus::SensorDisconnected));
        wait_for(&followed, lost);
        proxy.refuse(stale, wire_rpc::RpcError::NotFound, stale_route);

        // The refusal reaches the engine ahead of the packet sent after it.
        let child = route("/1");
        proxy.send(samples(0, child));
        wait_for(&routes, |routes| routes.contains(&child));
        let late = metadata(&store, root);
        assert!(
            matches!(
                late.recv_timeout(Duration::from_secs(5)),
                Ok(MetadataState::Pending)
            ),
            "the stale refusal condemned a route that had moved on"
        );
    }

    /// A link's status reaches the devices it serves. Another device's
    /// heartbeat is not their business.
    #[test]
    fn a_device_follows_its_link_but_not_another_routes_heartbeat() {
        let (root, child) = (DeviceRoute::root(), route("/1"));
        let (store, proxy) = engine();
        let (other, device) = (presence(&store, root), presence(&store, child));
        proxy.send(Packet::heartbeat(child));
        wait_for(&device, alive);

        proxy.send(Packet::heartbeat(root));
        wait_for(&other, alive);
        assert_eq!(
            device.recv_timeout(Duration::from_millis(200)),
            Err(RecvError::Timeout),
            "the device heard another route's heartbeat"
        );

        proxy.send(Packet::link(LinkStatus::SensorDisconnected));
        let next = device.recv_timeout(Duration::from_secs(5));
        assert!(next.as_ref().is_ok_and(lost), "got {next:?}");
    }

    /// A status carries a fabricated route, since `FailedToConnect` means
    /// nothing is there, so it must never mint a device. An upstream proxy
    /// older than `PROXY_RX` sends it as a legacy status, at the root.
    #[test]
    fn a_status_packet_discovers_no_route() {
        let (store, proxy) = engine();
        let routes = lock(&store).follow_routes(Scope::subtree(DeviceRoute::root()));
        assert_eq!(routes.recv(), Ok(Vec::new()));
        let child = route("/1");
        proxy.send(Packet::legacy_status(LinkStatus::FailedToConnect));
        proxy.send(Packet::link(LinkStatus::FailedToConnect).with_route(route("/2")));
        proxy.send(Packet::heartbeat(child));

        let heard = wait_for(&routes, |routes| !routes.is_empty());
        assert_eq!(heard, [child], "only the heartbeat's route exists");
    }

    /// Behind `tio proxy --mount`, a status arrives on its mount's prefix:
    /// one sensor bouncing redescribes its own subtree and no one else's.
    #[test]
    fn a_mounts_disconnect_resets_only_its_own_subtree() {
        let (steady, bounced) = (route("/1"), route("/2"));
        let (store, proxy) = engine();
        let (steady_metadata, bounced_metadata) =
            (metadata(&store, steady), metadata(&store, bounced));
        let (steady_presence, bounced_presence) =
            (presence(&store, steady), presence(&store, bounced));
        describe(&proxy, steady, 1);
        describe(&proxy, bounced, 1);
        wait_for(&steady_metadata, |state| snapshot(state).is_some());
        wait_for(&bounced_metadata, |state| snapshot(state).is_some());
        wait_for(&steady_presence, alive);
        wait_for(&bounced_presence, alive);

        proxy.send(Packet::link(LinkStatus::SensorDisconnected).with_route(bounced));
        wait_for(&bounced_metadata, |state| {
            matches!(state, MetadataState::Pending)
        });
        wait_for(&bounced_presence, lost);

        describe(&proxy, steady, 1);
        describe(&proxy, bounced, 1);
        wait_for(&bounced_metadata, |state| snapshot(state).is_some());
        assert!(
            matches!(steady_metadata.try_recv(), Ok(None)),
            "the steady mount was reset too"
        );

        proxy.send(Packet::link(LinkStatus::SensorReconnected).with_route(bounced));
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
        let (steady, stopped) = (route("/1"), route("/2"));
        let (store, proxy) = engine();
        let (steady_metadata, stopped_metadata) =
            (metadata(&store, steady), metadata(&store, stopped));
        describe(&proxy, steady, 1);
        describe(&proxy, stopped, 1);
        wait_for(&steady_metadata, |state| snapshot(state).is_some());
        wait_for(&stopped_metadata, |state| snapshot(state).is_some());

        proxy.send(Packet::link(LinkStatus::Stopped).with_route(stopped));
        wait_for(&stopped_metadata, |state| {
            matches!(state, MetadataState::Pending)
        });

        describe(&proxy, steady, 1);
        describe(&proxy, stopped, 1);
        wait_for(&stopped_metadata, |state| snapshot(state).is_some());
        assert!(
            matches!(steady_metadata.try_recv(), Ok(None)),
            "the steady mount was reset too"
        );
    }

    /// An upstream proxy announces a mount's status as a link packet, and
    /// one that predates `PROXY_RX` announces it as a legacy status. The
    /// engine takes either the same way.
    #[test]
    fn a_link_packet_is_the_same_status_as_a_legacy_one() {
        let mount = route("/2");
        let (store, proxy) = engine();
        let followed = presence(&store, mount);
        let heard = |status: Packet| {
            proxy.send(Packet::heartbeat(mount));
            wait_for(&followed, alive);
            proxy.send(status.with_route(mount));
            followed
                .recv_timeout(Duration::from_secs(5))
                .expect("the engine publishes")
        };

        let legacy = heard(Packet::legacy_status(LinkStatus::SensorDisconnected));
        let link = heard(Packet::link(LinkStatus::SensorDisconnected));

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
        let (pinned, other) = (route("/1/0"), route("/2"));
        let (store, proxy) = engine();
        let (below, elsewhere) = (presence(&store, pinned), presence(&store, other));
        proxy.send(Packet::heartbeat(pinned));
        proxy.send(Packet::heartbeat(other));
        wait_for(&below, alive);
        wait_for(&elsewhere, alive);

        proxy.send(Packet::link(LinkStatus::SensorReconnected).with_route(route("/1")));

        wait_for(&below, |presence| *presence == Presence::Connecting);
        assert_eq!(
            elsewhere.recv_timeout(Duration::from_millis(200)),
            Err(RecvError::Timeout),
            "another mount's status is not this device's business"
        );
    }

    /// Link state is a level, not an edge: a receiver that joins during an
    /// outage learns what it did not watch happen.
    #[test]
    fn a_late_receiver_starts_from_the_link_state_it_did_not_watch() {
        let mount = route("/2");
        let (store, proxy) = engine();
        let early = presence(&store, mount);
        proxy.send(samples(0, mount));
        proxy.send(Packet::link(LinkStatus::SensorDisconnected).with_route(mount));
        wait_for(&early, lost);

        let late = presence(&store, mount);
        let first = late
            .recv_timeout(Duration::from_secs(5))
            .expect("the state");
        assert!(lost(&first), "the late receiver starts from {first:?}");
        let routes = lock(&store).follow_routes(Scope::subtree(DeviceRoute::root()));
        assert_eq!(routes.recv_timeout(Duration::from_secs(5)), Ok(vec![mount]));
    }

    /// A device not heard yet, that a receiver follows, is lost with its
    /// link.
    #[test]
    fn a_followed_device_not_yet_heard_is_lost_with_its_link() {
        let (store, proxy) = engine();
        let unheard = presence(&store, route("/1"));
        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        proxy.send(Packet::link(LinkStatus::SensorDisconnected));

        wait_for(&unheard, lost);
    }

    #[test]
    fn a_route_no_receiver_follows_is_not_asked_for_metadata() {
        let (store, proxy) = engine();
        let (root, elsewhere) = (DeviceRoute::root(), route("/2"));
        let _device = metadata(&store, route("/1"));
        describe(&proxy, root, 1);
        proxy.send(samples(0, root));
        proxy.send(samples(0, elsewhere));

        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "a route no receiver follows is never asked for metadata"
        );

        let _followed = presence(&store, elsewhere);
        let (route, args, _) = metadata_call(&proxy);
        assert_eq!(route, elsewhere);
        assert!(args.is_empty(), "the newly followed route bootstraps");
    }

    #[test]
    fn overlapping_receivers_share_one_discovery() {
        let (store, proxy) = engine();
        let child = route("/1");
        let _followed = [
            metadata(&store, DeviceRoute::root()),
            metadata(&store, child),
            metadata(&store, child),
        ];
        let _present = presence(&store, child);
        proxy.send(samples(0, DeviceRoute::root()));
        proxy.send(samples(0, child));

        let first = metadata_call(&proxy);
        let second = metadata_call(&proxy);
        assert_ne!(
            first.0, second.0,
            "one bootstrap per route, not per receiver"
        );
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "the second receiver started no second discovery"
        );
    }

    /// A device is connecting until it is heard, alive with the session its
    /// heartbeat reports, lost when its link is, and connecting again when
    /// the link reconnects. A late receiver starts from the presence now.
    #[test]
    fn presence_follows_packets_heartbeats_and_link_statuses() {
        let (store, proxy) = engine();
        let root = DeviceRoute::root();
        let followed = presence(&store, root);
        assert_eq!(followed.recv(), Ok(Presence::Connecting));
        proxy.send(Packet::heartbeat_session(7, root));
        assert_eq!(
            followed.recv_timeout(Duration::from_secs(5)),
            Ok(Presence::Alive {
                session: Some(crate::proto::SessionId::new(7))
            })
        );

        proxy.send(Packet::link(LinkStatus::SensorDisconnected));
        assert!(matches!(followed.recv(), Ok(Presence::Lost { .. })));

        proxy.send(Packet::link(LinkStatus::SensorReconnected));
        assert_eq!(followed.recv(), Ok(Presence::Connecting));
        assert_eq!(presence(&store, root).recv(), Ok(Presence::Connecting));
    }

    /// A device under a hub that goes quiet while the hub keeps sending is
    /// lost once it has been silent for [`QUIET`], and alive again once it
    /// speaks.
    #[tokio::test(start_paused = true)]
    async fn a_device_silent_while_its_hub_speaks_is_lost() {
        let tasks = Supervisor::current();
        let (store, proxy) = engine_on(&tasks);
        let (hub, device) = (DeviceRoute::root(), route("/1"));
        let followed = presence(&store, device);
        proxy.send(Packet::heartbeat(device));
        while !followed.recv_async().await.as_ref().is_ok_and(alive) {}
        let heard = Instant::now();

        let beating = async {
            loop {
                proxy.send(Packet::heartbeat(hub));
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        };
        tokio::select! {
            () = beating => unreachable!(),
            now = followed.recv_async() => assert!(now.as_ref().is_ok_and(lost), "{now:?}"),
        }
        assert!(heard.elapsed() >= QUIET, "lost after {:?}", heard.elapsed());

        proxy.send(Packet::heartbeat(device));
        while !followed.recv_async().await.as_ref().is_ok_and(alive) {}
    }

    /// A device's metadata is pending until it is complete, and pending
    /// again once its link drops it.
    #[test]
    fn metadata_is_ready_once_complete_and_pending_after_a_disconnect() {
        let (store, proxy) = engine();
        let root = DeviceRoute::root();
        let followed = metadata(&store, root);
        describe(&proxy, root, 1);
        let ready = wait_for(&followed, |state| snapshot(state).is_some());
        assert_eq!(snapshot(&ready).unwrap().device().name(), "d");

        proxy.send(Packet::link(LinkStatus::SensorDisconnected));
        assert!(matches!(
            followed.recv_timeout(Duration::from_secs(5)),
            Ok(MetadataState::Pending)
        ));
    }

    /// A route list holds every route heard within its scope, sorted, and
    /// only those.
    #[test]
    fn a_route_list_holds_the_routes_heard_within_its_scope() {
        let (store, proxy) = engine();
        let routes = lock(&store).follow_routes(Scope::subtree(route("/1")));
        assert_eq!(routes.recv(), Ok(Vec::new()));

        for heard in ["/1/2", "/2", "/1/0"] {
            proxy.send(Packet::heartbeat(route(heard)));
        }
        let heard = wait_for(&routes, |heard| heard.len() == 2);
        assert_eq!(heard, [route("/1/0"), route("/1/2")]);
        let all = lock(&store).follow_routes(Scope::subtree(DeviceRoute::root()));
        assert_eq!(all.recv().map(|all| all.len()), Ok(3));
    }

    /// Setting changes arrive one by one, in the order announced, to the
    /// receivers of the device that announced them and no other's.
    #[test]
    fn setting_changes_arrive_in_order_at_their_own_device() {
        let (store, proxy) = engine();
        let (device, other) = (route("/1"), DeviceRoute::root());
        let changes = lock(&store).follow(device, CHANGES);
        let elsewhere = lock(&store).follow(other, CHANGES);

        proxy.send(announcement("test.amplitude", Some(9), &[1], device));
        proxy.send(announcement("rpc.hash", None, &7u32.to_le_bytes(), device));
        let first = changes.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            first,
            SettingChange {
                name: "test.amplitude".to_string(),
                seq: Some(9),
                reply: vec![1],
            }
        );
        let hash = changes.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!((hash.name.as_str(), hash.seq), ("rpc.hash", None));
        assert_eq!(
            elsewhere.recv_timeout(Duration::from_millis(200)),
            Err(RecvError::Timeout),
            "another device's announcement is not this device's change"
        );
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

    fn forget_table() {
        if let Some(cached) = crate::device::rpc::cache::path(TABLE_DEVICE, TABLE_HASH) {
            let _ = std::fs::remove_file(cached);
        }
    }

    /// The table is loaded once for every receiver, the settings are read
    /// and then kept by each announcement, and a new table hash loads the
    /// table again, which drops the settings read with the old one.
    #[test]
    fn the_table_and_settings_load_once_and_follow_announcements() {
        forget_table();
        let (store, proxy) = engine();
        let root = DeviceRoute::root();
        let answering = answer_table(&proxy);
        proxy.send(Packet::heartbeat(root));
        let settings = follow(&store, root, SETTINGS);
        let wait = Duration::from_secs(5);

        let first = settings.recv_timeout(wait).expect("the settings are read");
        assert_eq!(first.version(), Some(5));
        assert_eq!(first.get("test.amplitude"), Some(&[1u8][..]));
        assert_eq!(first.get("settings.version"), None);
        let registry = follow(&store, root, REGISTRY)
            .recv_timeout(wait)
            .expect("the table is shared")
            .expect("the table loaded");
        assert_eq!(registry.hash, Some(TABLE_HASH));

        proxy.send(announcement("test.amplitude", Some(6), &[2], root));
        let next = settings
            .recv_timeout(wait)
            .expect("the announcement is kept");
        assert_eq!(
            (next.version(), next.get("test.amplitude")),
            (Some(6), Some(&[2u8][..]))
        );

        let tables = follow(&store, root, REGISTRY);
        tables.recv_timeout(wait).unwrap().unwrap();
        proxy.send(announcement(
            "rpc.hash",
            Some(7),
            &(TABLE_HASH + 1).to_le_bytes(),
            root,
        ));
        tables
            .recv_timeout(wait)
            .expect("the table loads again")
            .unwrap();
        let stale = follow(&store, root, SETTINGS);
        if let Ok(Some(read)) = stale.try_recv() {
            assert_eq!(
                read.version(),
                Some(5),
                "settings of the old table were kept"
            );
        }
        let reread = settings
            .recv_timeout(wait)
            .expect("the settings are read again");
        assert_eq!(reread.version(), Some(5));
        drop((settings, stale, tables));
        assert_eq!(answering.join().expect("the device answers"), 2);
        forget_table();
    }

    /// A worker keeps its device's table while a receiver follows it and
    /// for the linger after, so a receiver that follows again soon finds it
    /// at once. Then the worker stops and the table is dropped, so a device
    /// no one asks is never walked again and no one is handed a table no
    /// one keeps current.
    #[test]
    fn a_table_no_receiver_follows_is_dropped_after_its_linger() {
        forget_table();
        let (store, proxy) = engine();
        let root = DeviceRoute::root();
        let answering = answer_table(&proxy);
        proxy.send(Packet::heartbeat(root));
        let registry = follow(&store, root, REGISTRY);
        registry
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        drop(registry);
        let again = follow(&store, root, REGISTRY);
        assert!(
            matches!(again.try_recv(), Ok(Some(Ok(_)))),
            "the table lingers"
        );
        drop(again);

        std::thread::sleep(LINGER * 4);
        assert!(
            lock(&store).value(root, REGISTRY).is_none(),
            "the table outlived its worker"
        );
        proxy.send(Packet::link(LinkStatus::SensorReconnected));
        assert_eq!(answering.join().expect("the device answers"), 1);
        forget_table();
    }

    /// A table that failed to load is loaded again after [`RELOAD_DELAY`]
    /// only once its device is heard, so a worker never asks a device that
    /// is not there again and again.
    #[test]
    fn a_table_that_failed_loads_again_only_once_its_device_is_heard() {
        let (store, proxy) = engine();
        let device = route("/9");
        let registry = follow(&store, device, REGISTRY);
        proxy.send(Packet::heartbeat(device));
        while let Ok(packet) = proxy.sent.recv_timeout(Duration::from_millis(500)) {
            if let packet::Payload::RpcRequest(request) = packet.payload() {
                proxy.refuse(
                    request.id.value(),
                    wire_rpc::RpcError::NotFound,
                    packet.route(),
                );
            }
        }
        let failed = registry
            .recv_timeout(Duration::from_secs(5))
            .expect("the failure");
        assert!(failed.is_err(), "{failed:?}");

        assert!(
            !wrote_within(&proxy, RELOAD_DELAY + Duration::from_secs(1)),
            "a device never heard was asked again"
        );
        proxy.send(Packet::heartbeat(device));
        let (asked, ..) = proxy.call();
        assert_eq!(asked, device);
    }

    /// A table followed of a device not heard starts no worker, so a watch
    /// of a route where nothing is starts no thread and asks nothing, until
    /// the device is heard.
    #[test]
    fn a_table_of_a_device_not_heard_is_not_loaded_until_it_is() {
        let (store, proxy) = engine();
        let device = route("/9");
        let _registry = follow(&store, device, REGISTRY);
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "a worker asked"
        );

        proxy.send(Packet::heartbeat(device));
        let (asked, ..) = proxy.call();
        assert_eq!(asked, device);
    }

    /// A device a reconnect of its link left connecting is lost once it has
    /// been silent for [`QUIET`], rather than connecting for ever.
    #[tokio::test(start_paused = true)]
    async fn a_device_left_connecting_by_a_reconnect_is_lost_when_silent() {
        let tasks = Supervisor::current();
        let (store, proxy) = engine_on(&tasks);
        let device = route("/1");
        let followed = presence(&store, device);
        proxy.send(Packet::heartbeat(device));
        while !followed.recv_async().await.as_ref().is_ok_and(alive) {}

        proxy.send(Packet::link(LinkStatus::SensorReconnected));
        assert_eq!(followed.recv_async().await, Ok(Presence::Connecting));
        let reconnected = Instant::now();
        let now = tokio::time::timeout(QUIET * 2, followed.recv_async()).await;
        assert!(
            now.as_ref().is_ok_and(|now| now.as_ref().is_ok_and(lost)),
            "{now:?}"
        );
        assert!(
            reconnected.elapsed() >= QUIET,
            "lost after {:?}",
            reconnected.elapsed()
        );
    }

    /// The engine follows every packet its link sends, however many, so a
    /// status after a flood of samples still reaches the receivers.
    #[test]
    fn the_engine_loses_none_of_its_input() {
        let (store, proxy) = engine();
        let root = DeviceRoute::root();
        let followed = presence(&store, root);
        let changes = lock(&store).follow(root, CHANGES);
        for first in 0..20_000 {
            proxy.send(samples(first, root));
        }
        for seq in 0..100 {
            proxy.send(announcement("test.amplitude", Some(seq), &[1], root));
        }
        proxy.send(Packet::link(LinkStatus::SensorDisconnected));

        let seqs: Vec<_> = (0..100)
            .map(|_| changes.recv_timeout(Duration::from_secs(5)).unwrap().seq)
            .collect();
        assert_eq!(seqs, (0..100).map(Some).collect::<Vec<_>>());
        wait_for(&followed, lost);
    }

    /// A route whose metadata query failed is asked again when its backoff
    /// ends on the clock the engine sleeps on, and not before.
    #[tokio::test(start_paused = true)]
    async fn a_failed_metadata_query_is_asked_again_when_its_backoff_ends() {
        let tasks = Supervisor::current();
        let (store, proxy) = engine_on(&tasks);
        let _followed = metadata(&store, DeviceRoute::root());
        tokio::time::sleep(Duration::from_millis(1)).await;
        proxy.send(samples(0, DeviceRoute::root()));

        let asked = proxy.sent.recv_async().await.unwrap();
        let packet::Payload::RpcRequest(request) = asked.payload() else {
            panic!("expected a metadata query, got {:?}", asked.payload());
        };
        proxy.refuse(request.id.value(), wire_rpc::RpcError::Busy, asked.route());
        let refused = Instant::now();

        let again = proxy.sent.recv_async().await.unwrap();
        assert_eq!(again.route(), asked.route());
        assert_eq!(refused.elapsed(), super::super::metadata::RETRY_FIRST);
    }

    /// A status supersedes the presence of every device in the subtree it
    /// covers, so a mount's loss never outlives the reconnect of the whole
    /// link that ended it.
    #[test]
    fn a_broader_status_supersedes_the_narrower_state_it_covers() {
        let mount = route("/2");
        let (store, proxy) = engine();
        let early = presence(&store, mount);
        proxy.send(samples(0, mount));
        wait_for(&early, alive);
        proxy.send(Packet::link(LinkStatus::SensorDisconnected).with_route(mount));
        wait_for(&early, lost);

        proxy.send(Packet::link(LinkStatus::SensorReconnected));
        wait_for(&early, |presence| *presence == Presence::Connecting);
        let below = lock(&store).value(route("/2/1"), PRESENCE);
        assert!(
            below.is_none_or(|below| below == Presence::Connecting),
            "{below:?}"
        );
    }

    /// Every receiver ends once the engine does, with its link, rather than
    /// waiting on state no one will send. The status saying why the link
    /// failed may be the last packet the link sends, and it still counts.
    #[test]
    fn the_receivers_end_with_the_engine() {
        let (store, proxy) = engine();
        let followed = presence(&store, DeviceRoute::root());
        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        proxy.send(Packet::link(LinkStatus::FailedToReconnect));
        drop(proxy);

        let deadline = StdInstant::now() + Duration::from_secs(5);
        let mut heard = false;
        let ended = loop {
            match followed.recv_deadline(deadline) {
                Ok(now) => heard |= lost(&now),
                Err(ended) => break ended,
            }
        };
        assert!(heard, "the link's last status was not taken");
        assert_eq!(ended, RecvError::Disconnected);

        let late = presence(&store, DeviceRoute::root());
        let ended = loop {
            if let Err(ended) = late.recv_timeout(Duration::from_secs(5)) {
                break ended;
            }
        };
        assert_eq!(ended, RecvError::Disconnected);
    }
}

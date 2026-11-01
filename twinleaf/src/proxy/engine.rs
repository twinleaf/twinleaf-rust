//! The engine of one link, the one place its devices' state is computed, and
//! the one reader of its device's packets.
//!
//! The engine reads every packet of its link through a [`Session`] over a
//! port of its own, whose queues have no limit, so it loses nothing. It
//! reads through a session, as an app does, so its [`Handle`] can call
//! `dev.metadata`, load the RPC table and read the settings of each device.
//! It follows each device's presence from the packets it hears and the
//! link's statuses, and loses a device silent for [`QUIET`], or one a
//! reconnect left connecting that stays silent as long. A device heard again
//! after it was lost has reconnected, just as one whose link reconnected
//! has, and its table is loaded again. It collects the metadata of each
//! device a watch follows, and publishes it once it is complete. It loads
//! the RPC table and keeps the settings of each device it has heard whose
//! table a watch follows, on a worker task per device. The engine holds each
//! worker's task, which ends once no watch has followed the table for
//! [`LINGER`], and with the engine. It keeps all of this in the link's
//! [`Store`], which the clients' watches follow, for as long as the link
//! runs.
//!
//! The engine also feeds the link's [`Mounts`]. For each packet, in the
//! order the link delivered it, it takes the packet into the store, and
//! then puts the packet on the stream of each client's [`Outbox`] whose
//! mount covers its route. Each time it sets a device's metadata it puts the
//! metadata update on the stream of each client whose watch follows that
//! device, in the same step. So a client hears a metadata update ahead of
//! every sample the device sent after the metadata changed.
//!
//! The engine takes each finished `dev.metadata` call ahead of the packets
//! waiting in its inlet. A reply fills in only metadata the parser still
//! lacks for a device, so it replaces no record that a waiting sample was
//! sent under, and the client's parser decodes each sample with the record
//! of the sample's own segment.
//! Until a device's metadata is complete the client has none for it, and
//! its parser drops the device's samples.
//!
//! Locks nest in one order. The list of mounts comes first, then a mount's
//! metadata watches, and then the store or an outbox. The engine holds no
//! other lock while it holds the store.

use super::client::ClientError;
use super::control::Update;
use super::metadata::{Completed, MetadataSchedule};
use super::outbox::Outbox;
use super::settings::{CacheAction, DeviceEvent, SettingsCache};
use super::Watch;
use crate::chan::lock;
use crate::chan::AbortOnDropHandle;
use crate::data::{MetadataQuery, PacketParser};
use crate::device::rpc::{PendingReply, RpcRegistry};
use crate::device::session::{Handle, Inlet, RawCallResult, Received, Session};
use crate::device::store::{pruned, rpc_hash, Store, METADATA, PRESENCE};
use crate::device::stream::Queued;
use crate::device::{MetadataState, Presence, Settings};
use crate::proto::{DeviceRoute, RouteError};
use crate::rt::{sleep_until_some, Supervisor};
use crate::tio::packet::{self, LinkStatus, Packet};
use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

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

/// Keeps `store` from what arrives over `session`, and feeds `mounts`, until
/// the session ends. Then ends every receiver of the store and lets every
/// client of `mounts` go. It attaches to the session at once, so it takes
/// every packet that arrives from then on. The workers of its devices run
/// on `tasks`.
pub(super) fn serve(
    store: Arc<Mutex<Store>>,
    mounts: Arc<Mounts>,
    session: Session,
    tasks: &Supervisor,
) -> impl std::future::Future<Output = ()> {
    let engine = Engine::new(&store, &mounts, session, tasks);
    async move {
        engine.run().await;
        lock(&store).end_all(None);
        mounts.end();
    }
}

/// The clients' mounts of one link, which the link's engine feeds.
pub(super) struct Mounts {
    /// The link's URL, which names it when a client is let go.
    url: String,
    list: Mutex<MountList>,
}

/// What [`Mounts`] holds behind its lock.
#[derive(Default)]
struct MountList {
    mounts: Vec<Arc<Mount>>,
    /// The last link status the engine took at the root, which says whether
    /// the link has lost its device or given it up.
    status: Option<LinkStatus>,
}

/// One client's mount of a subtree of a link, which the client sees at a
/// prefix of its own. It maps routes between the two, and holds the
/// client's [`Outbox`] and the devices whose metadata the client's watches
/// follow through it.
pub(super) struct Mount {
    /// The subtree of the link's device the mount shows.
    source: DeviceRoute,
    /// Where the client sees the subtree.
    prefix: DeviceRoute,
    outbox: Arc<Outbox>,
    /// The device each watch follows the metadata of here, by the id of its
    /// `proxy.watch`, as its route on the link.
    metadata: Mutex<HashMap<u16, DeviceRoute>>,
}

impl Mount {
    /// Returns the mount of the subtree at `source` that the client of
    /// `outbox` sees at `prefix`.
    pub(super) fn new(
        source: DeviceRoute,
        prefix: DeviceRoute,
        outbox: &Arc<Outbox>,
    ) -> Arc<Mount> {
        Arc::new(Mount {
            source,
            prefix,
            outbox: Arc::clone(outbox),
            metadata: Mutex::default(),
        })
    }

    /// Returns `packet` at the route the client sees, or `None` for a packet
    /// outside the mount or one whose route there would be deeper than a
    /// route can be. A link status whose route is above the subtree concerns
    /// the subtree's root.
    fn shown(&self, packet: &Packet) -> Option<Packet> {
        let route = packet.route();
        let below = match self.source.relative_route(&route) {
            Ok(below) => below,
            Err(_) if packet.link_status().is_some() && self.source.starts_with(&route) => {
                DeviceRoute::root()
            }
            Err(_) => return None,
        };
        let Ok(shown) = self.prefix.absolute_route(&below) else {
            log::warn!("Dropping a packet whose route {below} exceeds the max depth");
            return None;
        };
        Some(packet.with_route(shown))
    }

    /// Returns the route on the link of `route`, a route the client sees, or
    /// `None` if the mount does not show it.
    ///
    /// # Errors
    ///
    /// Returns an error if the route on the link would be deeper than a
    /// route can be.
    pub(super) fn to_device(&self, route: DeviceRoute) -> Option<Result<DeviceRoute, RouteError>> {
        let below = self.prefix.relative_route(&route).ok()?;
        Some(self.source.absolute_route(&below))
    }

    /// Returns the route the client sees of `route`, a route on the link, or
    /// `None` if the mount does not show it.
    pub(super) fn to_client(&self, route: DeviceRoute) -> Option<DeviceRoute> {
        let below = self.source.relative_route(&route).ok()?;
        self.prefix.absolute_route(&below).ok()
    }

    /// Returns the subtree on the link that the client sees at or below
    /// `route`, or `None` if the mount shows nothing there. That is the
    /// route's own subtree for a route the mount shows, and the whole mount
    /// for a route above the mount's prefix.
    pub(super) fn subtree_at(&self, route: DeviceRoute) -> Option<DeviceRoute> {
        match self.to_device(route) {
            Some(device) => device.ok(),
            None if self.prefix.starts_with(&route) => Some(self.source),
            None => None,
        }
    }

    /// Queues `packet` ahead of the client's stream, at the route the client
    /// sees, such as the answer to one of its requests.
    pub(super) fn urgent(&self, packet: &Packet) {
        if let Some(shown) = self.shown(packet) {
            self.outbox.urgent(shown);
        }
    }

    /// Has the link's engine queue the metadata of the device at `route` on
    /// the link for watch `id`, starting with what `store` holds now, in
    /// place of what the watch followed here before. The metadata of a
    /// device the engine has not described is pending. The engine sets the
    /// store before it takes this mount's lock to queue a change, so a
    /// change made while the watch starts comes once or twice, and never not
    /// at all. The client takes a repeat as the same latest value. Samples
    /// queued for the client before the watch starts reach it ahead of this
    /// first update, and its parser drops them for want of metadata, unless
    /// an earlier watch gave it the metadata they were sent under.
    pub(super) fn watch_metadata(&self, id: u16, route: DeviceRoute, store: &Mutex<Store>) {
        let mut metadata = lock(&self.metadata);
        let state = lock(store).value(route, METADATA);
        if let Some(update) = Update::metadata(&state.unwrap_or(MetadataState::Pending)) {
            self.outbox.update(id, &update);
        }
        metadata.insert(id, route);
    }

    /// Stops queuing metadata for watch `id`.
    pub(super) fn unwatch(&self, id: u16) {
        lock(&self.metadata).remove(&id);
    }
}

impl Mounts {
    /// Creates the mounts of the link to `url`, none yet.
    pub(super) fn new(url: &str) -> Arc<Mounts> {
        Arc::new(Mounts {
            url: url.to_string(),
            list: Mutex::default(),
        })
    }

    /// Feeds `mount` the packets of its subtree from now on. A mount added
    /// while the link has lost its device hears so first. The status goes
    /// on the urgent queue, which here keeps device order, as nothing of
    /// this mount is queued on its client's stream yet.
    pub(super) fn add(&self, mount: Arc<Mount>) {
        let mut list = lock(&self.list);
        if list.status == Some(LinkStatus::SensorDisconnected) {
            mount.urgent(&Packet::link(LinkStatus::SensorDisconnected));
        }
        list.mounts.push(mount);
    }

    /// Stops feeding `mount`.
    pub(super) fn remove(&self, mount: &Arc<Mount>) {
        let mut list = lock(&self.list);
        list.mounts.retain(|held| !Arc::ptr_eq(held, mount));
    }

    /// Puts `packet` on the stream of each client whose mount it falls in,
    /// at the route the client sees.
    fn feed(&self, packet: &Packet) {
        let mut list = lock(&self.list);
        let status = packet.link_status();
        if status.is_some() && packet.route().is_empty() {
            list.status = status;
        }
        for mount in &list.mounts {
            if let Some(shown) = mount.shown(packet) {
                mount.outbox.stream(shown);
            }
        }
    }

    /// Puts the update of `state`, the metadata of the device at `route`, on
    /// the stream of each client whose watch follows it.
    fn metadata(&self, route: DeviceRoute, state: &MetadataState) {
        let Some(update) = Update::metadata(state) else {
            return;
        };
        let list = lock(&self.list);
        for mount in &list.mounts {
            let watches = lock(&mount.metadata);
            let following = watches.iter().filter(|(_, followed)| **followed == route);
            for (id, _) in following {
                mount.outbox.update(*id, &update);
            }
        }
    }

    /// Lets each client go, as the link ended. The clients hear that the
    /// proxy gave up on the link if the last status was `FailedToReconnect`.
    fn end(&self) {
        let mut list = lock(&self.list);
        let url = self.url.clone();
        let error = match list.status {
            Some(LinkStatus::FailedToReconnect) => ClientError::GaveUp(url),
            _ => ClientError::LinkEnded(url),
        };
        for mount in list.mounts.drain(..) {
            mount.outbox.end(error.clone());
        }
    }
}

/// The engine's state, besides what it keeps in the store.
struct Engine {
    store: Arc<Mutex<Store>>,
    /// The clients the engine feeds.
    mounts: Arc<Mounts>,
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
    /// The spawner of the workers.
    tasks: Supervisor,
    /// The worker of each device whose RPC table a receiver follows.
    workers: HashMap<DeviceRoute, WorkerHold>,
}

/// The engine's hold on the worker of one device. The worker ends once the
/// hold drops.
struct WorkerHold {
    /// The device's events, which the worker takes.
    events: crate::chan::Sender<DeviceEvent>,
    /// Since when no receiver has followed the device's table.
    unwatched: Option<Instant>,
    /// The worker's task, aborted once the hold drops.
    _task: AbortOnDropHandle<Option<()>>,
}

impl Engine {
    /// Returns the engine of the link that `session` reaches, which keeps
    /// `store` and feeds `mounts`, with its workers on `tasks`. It attaches
    /// to the session at once.
    fn new(
        store: &Arc<Mutex<Store>>,
        mounts: &Arc<Mounts>,
        session: Session,
        tasks: &Supervisor,
    ) -> Engine {
        // An inlet with room for every packet a `usize` can count has no limit.
        let inlet = session.attach(usize::MAX);
        lock(store).list(DeviceRoute::root(), []);
        Engine {
            store: Arc::clone(store),
            mounts: Arc::clone(mounts),
            schedule: MetadataSchedule::new(Handle::root(session.clone())),
            handle: Handle::root(session),
            inlet,
            parser: PacketParser::new(false),
            metadata_seen: HashMap::new(),
            heard: HashMap::new(),
            demand: BTreeSet::new(),
            tasks: tasks.clone(),
            workers: HashMap::new(),
        }
    }

    /// Runs until the session over the link's port ends.
    async fn run(mut self) {
        let store = Arc::clone(&self.store);
        let followed = Arc::clone(&lock(&self.store).followed);
        loop {
            let wake = self.next_wake();
            tokio::select! {
                biased;
                () = followed.notified() => self.follow_demand(),
                () = pruned(|cx| lock(&store).prune_gone(cx)) => self.follow_demand(),
                Ok(reply) = self.schedule.replies().recv_async() => self.take_metadata_replies(reply),
                received = self.inlet.recv_async() => {
                    let Ok(first) = received else { return };
                    if self.take_inlet(first).is_break() {
                        return;
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

    /// Returns the earliest moment the engine has work that no packet
    /// brings, which is a metadata retry of a route it asks, a device going
    /// quiet, or the end of a worker's linger. A retry of a route the engine
    /// does not ask is never due, so it sets no wake.
    fn next_wake(&self) -> Option<Instant> {
        let retry = self.schedule.next_retry(asked(&self.heard, &self.demand));
        let quiet = self.heard.values().map(|at| *at + QUIET).min();
        let unwatched = self.workers.values().filter_map(|hold| hold.unwatched);
        let linger = unwatched.min().map(|since| since + LINGER);
        [retry, quiet, linger].into_iter().flatten().min()
    }

    /// Takes `first` and what else the inlet holds now, in order. Takes each
    /// packet into the store, and then feeds it to the mounts. Breaks once
    /// the session has ended.
    fn take_inlet(&mut self, first: Queued<Received>) -> ControlFlow<()> {
        let inlet = self.inlet.clone();
        let held = inlet.try_iter().take(inlet.len());
        for queued in std::iter::once(first).chain(held) {
            match queued {
                Queued::Item(Received::Packet(packet)) => {
                    self.take(&packet);
                    self.mounts.feed(&packet);
                }
                // The session over the engine's port has no socket to lose.
                Queued::Item(Received::SocketLost) => {}
                Queued::Gap(_) => unreachable!("the engine's inlet has no limit"),
                Queued::Ended(_) => return ControlFlow::Break(()),
            }
        }
        ControlFlow::Continue(())
    }

    /// Starts a worker for each table a receiver now follows of a device
    /// heard, and notes since when no receiver follows the table of each
    /// other worker.
    fn follow_demand(&mut self) {
        let mut store = lock(&self.store);
        self.demand = store.demand();
        let now = Instant::now();
        for (route, hold) in &mut self.workers {
            match self.demand.contains(&Watch {
                route: *route,
                table: true,
            }) {
                true => hold.unwatched = None,
                false => hold.unwatched = hold.unwatched.or(Some(now)),
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
            let hold = start_worker(&self.tasks, self.handle.clone(), route, &self.store);
            self.workers.insert(route, hold);
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
        self.workers.retain(|route, hold| {
            let lingers = hold.unwatched.is_none_or(|since| since + LINGER > now);
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
        if let Some(hold) = self.workers.get(&route) {
            let _ = hold.events.send(event);
        }
    }

    /// Takes one packet the link delivered into the store.
    fn take(&mut self, packet: &Packet) {
        let route = packet.route();
        if let Some(status) = packet.link_status() {
            return self.apply_status(route, status);
        }
        self.heard.insert(route, Instant::now());
        let shared = Arc::clone(&self.store);
        let mut store = lock(&shared);
        let new = !store.routes().contains(&route);
        if new {
            store.list(DeviceRoute::root(), [route]);
        }
        let held = match store.value(route, PRESENCE) {
            Some(Presence::Alive { session }) => Some(session),
            Some(Presence::Lost { .. }) => {
                self.reconnect(&mut store, route);
                None
            }
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
                if let Some(hash) = rpc_hash(setting.name, setting.reply) {
                    self.tell(route, DeviceEvent::NewHash(Some(hash)));
                }
                if let Some(name) = setting.name_str() {
                    let change = lock(&self.store).record_change(
                        route,
                        name.to_string(),
                        setting.seq,
                        setting.reply.to_vec(),
                    );
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
                since: Instant::now().into_std(),
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
        let mut forgotten = Vec::new();
        let shared = Arc::clone(&self.store);
        let mut store = lock(&shared);
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
                forgotten.push(route);
            }
            if presence == Presence::Connecting {
                self.reconnect(&mut store, route);
            }
        }
        drop(store);
        for route in forgotten {
            self.set_metadata(route, MetadataState::Pending);
        }
    }

    /// Takes the device at `route` as reconnected, as its link reconnected
    /// or it was heard again after it was lost. A reconnected device may run
    /// another table, so `store` holds its table out of force until the
    /// worker loads it again, and the engine loses the device unless it
    /// speaks within [`QUIET`].
    fn reconnect(&mut self, store: &mut Store, route: DeviceRoute) {
        self.heard.insert(route, Instant::now());
        store.reconnect(route);
        self.tell(route, DeviceEvent::NewHash(None));
    }

    /// Loses each device silent for [`QUIET`].
    fn lose_quiet(&mut self) {
        let now = Instant::now();
        let mut store = lock(&self.store);
        self.heard.retain(|route, at| {
            let heard = *at + QUIET > now;
            if !heard {
                let since = now.into_std();
                store.set(*route, PRESENCE, Presence::Lost { since });
            }
            heard
        });
    }

    /// Publishes the metadata of `route` if it is complete and a revision
    /// not yet published. Metadata that is not complete is not published, so
    /// a client has none of the device's metadata until then.
    fn publish_metadata(&mut self, route: DeviceRoute) {
        let seen = self.metadata_seen.get(&route).copied();
        let Some((revision, snapshot)) = self.parser.revised_metadata(route, seen) else {
            return;
        };
        self.metadata_seen.insert(route, revision);
        self.set_metadata(route, MetadataState::Ready(snapshot));
    }

    /// Sets the metadata of the device at `route` in the store, and puts its
    /// update on the stream of each client that follows it, ahead of every
    /// packet the engine takes after.
    fn set_metadata(&mut self, route: DeviceRoute, state: MetadataState) {
        lock(&self.store).set(route, METADATA, state.clone());
        self.mounts.metadata(route, &state);
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
                Completed::Unsupported => self.set_metadata(route, MetadataState::Unsupported),
                Completed::BackedOff => {}
            }
        }
    }

    /// Asks each device the engine asks that is due for whatever metadata
    /// the parser still wants.
    fn discover(&mut self) {
        let asked = asked(&self.heard, &self.demand);
        let due = self.schedule.due(asked, Instant::now());
        for route in due {
            self.schedule.submit(&mut self.parser, route);
        }
    }
}

/// Returns each route the engine asks for metadata, given when each device
/// was `heard` and the `demand` of the store's receivers. That is each
/// device heard whose metadata a receiver follows. [`Engine::discover`]
/// asks these routes and [`Engine::next_wake`] wakes for their retries
/// alone. A route that is not asked, as its device was lost or no receiver
/// follows it, keeps its backoff, and sets no wake until it is asked again.
fn asked<'a>(
    heard: &'a HashMap<DeviceRoute, Instant>,
    demand: &'a BTreeSet<Watch>,
) -> impl Iterator<Item = DeviceRoute> + 'a {
    let followed = |route: &DeviceRoute| {
        demand.contains(&Watch {
            route: *route,
            table: false,
        })
    };
    heard.keys().copied().filter(followed)
}

/// Starts the worker that loads the RPC table of the device at `route`
/// through `handle` and keeps its settings in `store`, as a task on `tasks`.
/// Returns the hold on it, which hands it the device's events.
fn start_worker(
    tasks: &Supervisor,
    handle: Handle,
    route: DeviceRoute,
    store: &Arc<Mutex<Store>>,
) -> WorkerHold {
    let (events, received) = crate::chan::unbounded();
    let worker = Worker {
        handle,
        route,
        events: received,
        store: Arc::clone(store),
    };
    let task = tasks.spawn(format_args!("proxy settings {route}"), worker.run());
    WorkerHold {
        events,
        unwatched: None,
        _task: AbortOnDropHandle::new(task),
    }
}

/// The worker of one device. Once the engine lets it go it sets nothing in
/// the store, which it checks under the store's lock, since a step of its
/// task can still be running on another thread then.
struct Worker {
    handle: Handle,
    route: DeviceRoute,
    events: crate::chan::Receiver<DeviceEvent>,
    store: Arc<Mutex<Store>>,
}

impl Worker {
    /// Loads the table and tracks the device until a new table or a new
    /// session calls for loading it again, until the engine lets go.
    async fn run(self) {
        while !self.events.is_disconnected() {
            // A load that started before the device last reconnected may have
            // read the device as it was before, so it puts no table in force.
            let started = lock(&self.store).epoch(self.route);
            let loaded = RpcRegistry::load_with(|name, arg| self.submit(name, arg)).await;
            let registry = loaded.map(Arc::new);
            {
                let mut store = lock(&self.store);
                if self.events.is_disconnected() {
                    return;
                }
                store.set_loaded(self.route, registry.clone(), started);
            }
            let tracked = match registry {
                Ok(registry) => self.track(&registry).await,
                Err(error) => {
                    log::warn!("could not load the RPC table of {}: {error}", self.route);
                    self.await_new_table().await
                }
            };
            if !tracked {
                return;
            }
        }
    }

    /// Keeps the settings of `registry` until it is out of date. Returns
    /// `false` once the engine lets go.
    async fn track(&self, registry: &Arc<RpcRegistry>) -> bool {
        let mut cache = SettingsCache::new(registry);
        self.snapshot(registry, &mut cache).await;
        loop {
            let Ok(event) = self.events.recv_async().await else {
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
                CacheAction::Snapshot => self.snapshot(registry, &mut cache).await,
                CacheAction::Reload => return true,
            }
        }
    }

    /// Fills `cache` with a snapshot and publishes the settings it holds
    /// then. A failed snapshot is logged, and the cache asks for another at
    /// the next heartbeat.
    async fn snapshot(&self, registry: &Arc<RpcRegistry>, cache: &mut SettingsCache) {
        if self.events.is_disconnected() {
            return;
        }
        let taken = cache
            .snapshot_with(|name, arg| self.submit(name, arg))
            .await;
        if let Err(error) = taken {
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
    async fn await_new_table(&self) -> bool {
        let deadline = Instant::now() + RELOAD_DELAY;
        loop {
            match timeout_at(deadline, self.events.recv_async()).await {
                Ok(Ok(DeviceEvent::NewHash(_))) => return true,
                Ok(Ok(_)) => {}
                Ok(Err(crate::chan::RecvError::Disconnected)) => return false,
                Err(_elapsed) => return self.events.recv_async().await.is_ok(),
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
    use crate::device::rpc::RpcValue;
    use crate::device::session::TestProxy;
    use crate::device::store::{Field, CHANGES, REGISTRY, SETTINGS};
    use crate::device::stream::{Receiver, RecvError, Scope};
    use crate::device::RegistryState;
    use crate::proto::data as wire;
    use crate::proto::rpc as wire_rpc;
    use crate::proto::SessionId;
    use crate::proxy::control::Joined;
    use crate::rt::Supervisor;
    use crate::tio::packet::{Payload, ProxyRx};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Instant as StdInstant;

    /// Feeds a client the whole link, with watch 1 following the metadata
    /// of the device at the root, from the engine whose store is `store`.
    fn client_of(store: &Mutex<Store>, mounts: &Mounts) -> Arc<Outbox> {
        let (outbox, root) = (Arc::<Outbox>::default(), DeviceRoute::root());
        let mount = Mount::new(root, root, &outbox);
        mount.watch_metadata(1, root, store);
        mounts.add(mount);
        outbox
    }

    /// Takes what `outbox` holds, as a client's task does, pausing between
    /// takes for a while that `seed` picks, until a packet `done` picks has
    /// gone. Returns everything taken, in order.
    fn take_until(outbox: &Outbox, seed: u64, done: impl Fn(&Packet) -> bool) -> Vec<Packet> {
        let deadline = StdInstant::now() + Duration::from_secs(20);
        let (mut taken, mut pause) = (Vec::new(), seed);
        loop {
            assert!(
                StdInstant::now() < deadline,
                "the last packet never went out"
            );
            let queued = outbox.take().packets;
            outbox.check();
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
    /// the client's task takes from its outbox meanwhile, the client hears
    /// each record ahead of every sample the device sent after it. A sample
    /// that goes out goes out under the record it was sent under.
    #[test]
    fn a_client_hears_each_record_ahead_of_every_sample_sent_after_it() {
        let root = DeviceRoute::root();
        let done = route("/9");
        for seed in 1..=4u64 {
            let (store, mounts, proxy) = engine_with_mounts(&Supervisor::new());
            let outbox = client_of(&store, &mounts);
            let taking = Arc::clone(&outbox);
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

    /// A mount added while its link has lost the device hears so first, at
    /// the root of its subtree, and then hears the device come back along
    /// with the mount that was there.
    #[test]
    fn a_mount_added_while_its_device_is_lost_hears_so_first() {
        let root = DeviceRoute::root();
        let mounts = Mounts::new("tcp://device");
        let (early, late) = (Arc::<Outbox>::default(), Arc::<Outbox>::default());
        mounts.add(Mount::new(root, root, &early));
        mounts.feed(&Packet::heartbeat(root));
        mounts.feed(&Packet::link(LinkStatus::SensorDisconnected));

        mounts.add(Mount::new(route("/1"), route("/2"), &late));
        mounts.feed(&Packet::link(LinkStatus::SensorReconnected));

        let heard = |outbox: &Outbox| -> Vec<_> {
            let taken = outbox.take().packets;
            taken
                .iter()
                .map(|pkt| (pkt.route(), pkt.link_status()))
                .collect()
        };
        let (lost, back) = (
            Some(LinkStatus::SensorDisconnected),
            Some(LinkStatus::SensorReconnected),
        );
        assert_eq!(heard(&early), [(root, None), (root, lost), (root, back)]);
        assert_eq!(heard(&late), [(route("/2"), lost), (route("/2"), back)]);
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
        let (store, mounts, proxy) = engine_with_mounts(&Supervisor::new());
        let outbox = client_of(&store, &mounts);
        describe(&proxy, root, 1);
        proxy.send(Packet::heartbeat_session(1, root));
        proxy.send(Packet::heartbeat_session(2, root));
        describe(&proxy, root, 2);
        proxy.send(samples(5, root));

        let is_sample = |packet: &Packet| matches!(packet.payload(), Payload::Samples(_));
        let taken = take_until(&outbox, 0, is_sample);
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
        let (store, _, proxy) = engine_with_mounts(tasks);
        (store, proxy)
    }

    /// An engine as [`engine_on`] starts it, with the mounts it feeds.
    fn engine_with_mounts(tasks: &Supervisor) -> (Arc<Mutex<Store>>, Arc<Mounts>, TestProxy) {
        let (session, proxy) = Session::test_pair_on(tasks);
        let store = Arc::new(Mutex::new(Store::new(true)));
        let mounts = Mounts::new("tcp://device");
        let serving = serve(Arc::clone(&store), Arc::clone(&mounts), session, tasks);
        tasks.spawn("proxy state", serving);
        (store, mounts, proxy)
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

    /// A device silent for [`QUIET`] is lost then on the paused clock, the
    /// moment its presence gives is that one, and how long ago the proxy
    /// says it was lost follows the paused clock after.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_loss_and_its_moment_follow_the_paused_clock() {
        let tasks = Supervisor::current();
        let (store, proxy) = engine_on(&tasks);
        let device = route("/1");
        let followed = presence(&store, device);
        proxy.send(Packet::heartbeat(device));
        while !followed.recv_async().await.as_ref().is_ok_and(alive) {}
        let heard = Instant::now();

        let now = followed.recv_async().await;
        let Ok(Presence::Lost { since }) = now else {
            panic!("the device was not lost, got {now:?}");
        };
        assert_eq!(heard.elapsed(), QUIET);
        assert_eq!(since, (heard + QUIET).into_std());
        tokio::time::sleep(Duration::from_secs(60)).await;
        let lost = Update::presence(Presence::Lost { since });
        assert_eq!(lost, Update::Presence(Err(Some(60_000))));
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
            (first.name.as_str(), first.seq, first.reply),
            ("test.amplitude", Some(9), vec![1])
        );
        let hash = changes.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!((hash.name.as_str(), hash.seq), ("rpc.hash", None));
        assert_eq!(
            elsewhere.recv_timeout(Duration::from_millis(200)).err(),
            Some(RecvError::Timeout),
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
        answer_tables(proxy, Arc::new(AtomicU32::new(TABLE_HASH)))
    }

    /// Answers as [`answer_table`] does, as a device whose table has the
    /// hash that `hash` holds at each call. A table with an odd hash lists
    /// the announced setting, and one with an even hash hides it.
    fn answer_tables(proxy: &TestProxy, hash: Arc<AtomicU32>) -> std::thread::JoinHandle<usize> {
        let (sent, deliver) = (proxy.sent.clone(), proxy.deliver.clone());
        std::thread::spawn(move || {
            let proxy = TestProxy { sent, deliver };
            let mut loads = 0;
            while let Ok(packet) = proxy.sent.recv_timeout(Duration::from_secs(2)) {
                let asked = answer(&proxy, &packet, hash.load(Ordering::SeqCst));
                loads += usize::from(asked.as_deref() == Some("dev.name"));
            }
            loads
        })
    }

    /// Answers `packet` as the device of [`answer_tables`] does, with the
    /// table of hash `hash`. Returns the name of the RPC it calls, if it is
    /// a call by name.
    fn answer(proxy: &TestProxy, packet: &Packet, hash: u32) -> Option<String> {
        let listed = [("test.amplitude", 0xA310u16), ("settings.version", 0x8140)];
        let table = &listed[usize::from(hash.is_multiple_of(2))..];
        let packet::Payload::RpcRequest(request) = packet.payload() else {
            return None;
        };
        let wire_rpc::Method::ByName(name) = request.method else {
            return None;
        };
        let (id, route) = (request.id.value(), packet.route());
        let name = std::str::from_utf8(name).expect("a UTF-8 name");
        let reply = match name {
            "dev.name" => TABLE_DEVICE.as_bytes().to_vec(),
            "rpc.hash" => hash.to_le_bytes().to_vec(),
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
                return Some(name.to_string());
            }
        };
        proxy.reply(id, &reply, route);
        Some(name.to_string())
    }

    /// A change the device announces after an `rpc.hash` that names another
    /// table carries no table until that table loads, and the new table
    /// after. A change recorded before keeps the old table, and decodes with
    /// it while the store holds a new one that lacks its entry.
    #[test]
    fn a_change_after_a_new_table_carries_the_new_table() {
        const SWITCHED: u32 = 0x5747_e101;
        let forget = || {
            for hash in [SWITCHED, SWITCHED + 1] {
                if let Some(cached) = crate::device::rpc::cache::path(TABLE_DEVICE, hash) {
                    let _ = std::fs::remove_file(cached);
                }
            }
        };
        forget();
        let (store, proxy) = engine();
        let root = DeviceRoute::root();
        let hash = Arc::new(AtomicU32::new(SWITCHED));
        let answering = answer_tables(&proxy, Arc::clone(&hash));
        proxy.send(Packet::heartbeat(root));
        let (tables, changes) = (
            follow(&store, root, REGISTRY),
            follow(&store, root, CHANGES),
        );
        let wait = Duration::from_secs(5);
        let old = tables.recv_timeout(wait).unwrap().expect("the table loads");

        proxy.send(announcement("test.amplitude", Some(6), &[2], root));
        let before = changes.recv_timeout(wait).unwrap();
        hash.store(SWITCHED + 1, Ordering::SeqCst);
        let announced = (SWITCHED + 1).to_le_bytes();
        proxy.send(announcement("rpc.hash", Some(7), &announced, root));
        proxy.send(announcement("test.amplitude", Some(8), &[3], root));
        let new = tables
            .recv_timeout(wait)
            .unwrap()
            .expect("the new table loads");
        proxy.send(announcement("test.amplitude", Some(9), &[4], root));
        let carried: Vec<_> = (0..3)
            .map(|_| changes.recv_timeout(wait).unwrap())
            .map(|change| change.rpc_registry().map(Arc::as_ptr))
            .collect();

        assert_eq!(new.hash, Some(SWITCHED + 1));
        assert!(Arc::ptr_eq(before.rpc_registry().unwrap(), &old));
        assert_eq!(carried, [None, None, Some(Arc::as_ptr(&new))]);
        assert_eq!(before.value().unwrap(), RpcValue::U64(2));
        drop((tables, changes));
        answering.join().expect("the device answers");
        forget();
    }

    /// A device heard again after its link reconnects reports no session
    /// until its next heartbeat, so a setting it announces before then
    /// carries no table, while the store still holds the table read in its
    /// session.
    #[test]
    fn a_change_before_the_first_heartbeat_after_a_reconnect_carries_no_table() {
        forget_table();
        let (store, proxy) = engine();
        let root = DeviceRoute::root();
        let answering = answer_table(&proxy);
        proxy.send(Packet::heartbeat(root));
        let (tables, changes) = (
            follow(&store, root, REGISTRY),
            follow(&store, root, CHANGES),
        );
        let wait = Duration::from_secs(5);
        let table = tables.recv_timeout(wait).unwrap().expect("the table loads");

        proxy.send(announcement("test.amplitude", Some(6), &[2], root));
        let before = changes.recv_timeout(wait).unwrap();
        proxy.send(Packet::link(LinkStatus::SensorReconnected));
        proxy.send(announcement("test.amplitude", Some(7), &[3], root));
        let after = changes.recv_timeout(wait).unwrap();

        assert!(Arc::ptr_eq(before.rpc_registry().unwrap(), &table));
        assert!(after.rpc_registry().is_none());
        drop((tables, changes));
        answering.join().expect("the device answers");
        forget_table();
    }

    /// A table whose load was in flight when the link reconnected is held
    /// once it lands, but is not in force, so a change recorded then carries
    /// no table. The load the reconnect calls for puts its table in force.
    #[test]
    fn a_table_loaded_across_a_reconnect_is_not_in_force() {
        forget_table();
        let (store, proxy) = engine();
        let root = DeviceRoute::root();
        proxy.send(Packet::heartbeat(root));
        let (tables, changes) = (
            follow(&store, root, REGISTRY),
            follow(&store, root, CHANGES),
        );
        let wait = Duration::from_secs(5);
        let deadline = StdInstant::now() + wait;
        let name_of = |packet: &Packet| match packet.payload() {
            packet::Payload::RpcRequest(request) => match request.method {
                wire_rpc::Method::ByName(name) => name.to_vec(),
                wire_rpc::Method::ById(_) => Vec::new(),
            },
            _ => Vec::new(),
        };

        // The load is in flight once the worker asks the device's name.
        let mut asked = Vec::new();
        while asked
            .last()
            .is_none_or(|packet| name_of(packet) != b"dev.name")
        {
            asked.push(proxy.sent.recv_timeout(wait).expect("the worker loads"));
        }
        proxy.send(Packet::link(LinkStatus::SensorReconnected));
        for packet in &asked {
            answer(&proxy, packet, TABLE_HASH);
        }
        let landed = loop {
            assert!(StdInstant::now() < deadline, "the load never landed");
            if let Ok(Some(loaded)) = tables.try_recv() {
                break loaded.expect("the table loads");
            }
            if let Ok(packet) = proxy.sent.recv_timeout(Duration::from_millis(20)) {
                answer(&proxy, &packet, TABLE_HASH);
            }
        };
        proxy.send(announcement("test.amplitude", Some(6), &[2], root));
        let across = changes.recv_timeout(wait).unwrap();

        let answering = answer_table(&proxy);
        let reloaded = tables
            .recv_timeout(wait)
            .unwrap()
            .expect("the table loads again");
        proxy.send(announcement("test.amplitude", Some(7), &[3], root));
        let after = changes.recv_timeout(wait).unwrap();

        assert!(across.rpc_registry().is_none());
        assert!(!Arc::ptr_eq(&landed, &reloaded));
        assert!(Arc::ptr_eq(after.rpc_registry().unwrap(), &reloaded));
        drop((tables, changes));
        answering.join().expect("the device answers");
        forget_table();
    }

    /// A device heard again after it went quiet has reconnected, though its
    /// link reported nothing, as a port of a hub that is plugged in again
    /// does. A change it announces before its table loads again carries no
    /// table, even as its heartbeats report no session, and the table the
    /// reconnect calls for is in force once it loads.
    #[tokio::test(start_paused = true)]
    async fn a_device_heard_again_after_going_quiet_loads_its_table_again() {
        const QUIETED: u32 = 0x5747_e201;
        let forget = || {
            if let Some(cached) = crate::device::rpc::cache::path(TABLE_DEVICE, QUIETED) {
                let _ = std::fs::remove_file(cached);
            }
        };
        forget();
        let tasks = Supervisor::current();
        let (store, proxy) = engine_on(&tasks);
        let device = route("/1");
        let (tables, changes, followed) = (
            follow(&store, device, REGISTRY),
            follow(&store, device, CHANGES),
            presence(&store, device),
        );
        proxy.send(Packet::heartbeat(device));
        let old = answer_until_loaded(&proxy, &tables, QUIETED).await;
        answer_until_idle(&proxy, QUIETED).await;
        proxy.send(announcement("test.amplitude", Some(6), &[2], device));
        let before = changes.recv_async().await.unwrap();

        let quiet = Instant::now();
        while !followed.recv_async().await.as_ref().is_ok_and(lost) {}
        assert!(quiet.elapsed() >= QUIET, "lost after {:?}", quiet.elapsed());
        proxy.send(Packet::heartbeat(device));
        proxy.send(announcement("test.amplitude", Some(7), &[3], device));
        let after_quiet = changes.recv_async().await.unwrap();
        assert!(
            after_quiet.rpc_registry().is_none(),
            "the table read before the device went quiet is in force"
        );
        let reloading = answer_until_loaded(&proxy, &tables, QUIETED);
        let new = tokio::time::timeout(QUIET, reloading)
            .await
            .expect("the table loads again");
        answer_until_idle(&proxy, QUIETED).await;
        proxy.send(announcement("test.amplitude", Some(8), &[4], device));
        let after_reload = changes.recv_async().await.unwrap();

        assert!(Arc::ptr_eq(before.rpc_registry().unwrap(), &old));
        assert!(!Arc::ptr_eq(&old, &new));
        assert!(Arc::ptr_eq(after_reload.rpc_registry().unwrap(), &new));
        forget();
    }

    /// Answers each call the session writes as the device of
    /// [`answer_tables`] with the table of hash `hash` does, until a table
    /// lands in `tables`. Returns the table.
    async fn answer_until_loaded(
        proxy: &TestProxy,
        tables: &Receiver<RegistryState>,
        hash: u32,
    ) -> Arc<RpcRegistry> {
        loop {
            tokio::select! {
                biased;
                table = tables.recv_async() => return table.unwrap().expect("the table loads"),
                packet = proxy.sent.recv_async() => {
                    answer(proxy, &packet.unwrap(), hash);
                }
            }
        }
    }

    /// Answers each call the session writes as [`answer_until_loaded`]
    /// does, until none comes for a while.
    async fn answer_until_idle(proxy: &TestProxy, hash: u32) {
        let idle = Duration::from_millis(100);
        while let Ok(packet) = tokio::time::timeout(idle, proxy.sent.recv_async()).await {
            answer(proxy, &packet.unwrap(), hash);
        }
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
    #[tokio::test(start_paused = true)]
    async fn a_table_no_receiver_follows_is_dropped_after_its_linger() {
        forget_table();
        let tasks = Supervisor::current();
        let (store, proxy) = engine_on(&tasks);
        let root = DeviceRoute::root();
        proxy.send(Packet::heartbeat(root));
        let registry = follow(&store, root, REGISTRY);
        answer_until_loaded(&proxy, &registry, TABLE_HASH).await;
        answer_until_idle(&proxy, TABLE_HASH).await;
        drop(registry);
        let again = follow(&store, root, REGISTRY);
        assert!(
            matches!(again.try_recv(), Ok(Some(Ok(_)))),
            "the table lingers"
        );
        drop(again);

        tokio::time::sleep(LINGER / 2).await;
        assert!(
            lock(&store).value(root, REGISTRY).is_some(),
            "the table was dropped before its linger ended"
        );
        tokio::time::sleep(LINGER).await;
        assert!(
            lock(&store).value(root, REGISTRY).is_none(),
            "the table outlived its worker"
        );
        proxy.send(Packet::link(LinkStatus::SensorReconnected));
        let asked = tokio::time::timeout(RELOAD_DELAY * 2, proxy.sent.recv_async()).await;
        assert!(asked.is_err(), "a stopped worker loaded the table again");
        forget_table();
    }

    /// A table that failed to load is loaded again after [`RELOAD_DELAY`]
    /// only once its device is heard, so a worker never asks a device that
    /// is not there again and again. The device stays alive over the wait
    /// on samples, which its worker does not hear.
    #[tokio::test(start_paused = true)]
    async fn a_table_that_failed_loads_again_only_once_its_device_is_heard() {
        let tasks = Supervisor::current();
        let (store, proxy) = engine_on(&tasks);
        let device = route("/9");
        let registry = follow(&store, device, REGISTRY);
        proxy.send(Packet::heartbeat(device));
        let idle = Duration::from_millis(500);
        while let Ok(packet) = tokio::time::timeout(idle, proxy.sent.recv_async()).await {
            let packet = packet.unwrap();
            if let packet::Payload::RpcRequest(request) = packet.payload() {
                proxy.refuse(
                    request.id.value(),
                    wire_rpc::RpcError::NotFound,
                    packet.route(),
                );
            }
        }
        let failed = registry.recv_async().await.expect("the failure");
        assert!(failed.is_err(), "{failed:?}");

        let sampling = async {
            loop {
                proxy.send(samples(0, device));
                tokio::time::sleep(QUIET / 2).await;
            }
        };
        tokio::select! {
            () = sampling => unreachable!(),
            asked = tokio::time::timeout(RELOAD_DELAY * 2, proxy.sent.recv_async()) => {
                assert!(asked.is_err(), "a device not heard since was asked again");
            }
        }
        proxy.send(Packet::heartbeat(device));
        let asked = proxy.sent.recv_async().await.unwrap();
        assert_eq!(asked.route(), device);
    }

    /// A worker's task ends once the engine lets go of its device, sooner
    /// than the call it waits on, never answered, times out. Every worker's
    /// task ends with the engine. Devices followed and let go again and
    /// again leave no task behind.
    #[tokio::test(start_paused = true)]
    async fn a_workers_task_ends_when_the_engine_lets_go_and_with_the_engine() {
        let (engine_tasks, workers) = (Supervisor::current(), Supervisor::current());
        let (session, proxy) = Session::test_pair_on(&engine_tasks);
        let store = Arc::new(Mutex::new(Store::new(true)));
        let mounts = Mounts::new("tcp://device");
        engine_tasks.spawn(
            "proxy state",
            serve(Arc::clone(&store), mounts, session, &workers),
        );
        let devices = [route("/1"), route("/2"), route("/3")];
        let follow_all = || -> Vec<_> {
            let followed = devices.iter();
            followed
                .map(|device| follow(&store, *device, REGISTRY))
                .collect()
        };

        for _ in 0..3 {
            let followed = follow_all();
            for device in devices {
                proxy.send(Packet::heartbeat(device));
            }
            running(&workers, devices.len()).await;
            drop(followed);
            let released = Instant::now();
            running(&workers, 0).await;
            let took = released.elapsed();
            assert!(
                took < LINGER * 2,
                "the workers ended {took:?} after release"
            );
        }

        let _followed = follow_all();
        running(&workers, devices.len()).await;
        drop(proxy);
        let ended = Instant::now();
        running(&workers, 0).await;
        let took = ended.elapsed();
        assert!(took < LINGER, "the workers ended {took:?} after the engine");
    }

    /// Waits until `count` of the tasks `tasks` spawned run.
    async fn running(tasks: &Supervisor, count: usize) {
        let waited = tokio::time::timeout(Duration::from_secs(60), async {
            while tasks.running() != count {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        assert!(
            waited.await.is_ok(),
            "{} tasks run, not {count}",
            tasks.running()
        );
    }

    /// A table followed of a device not heard starts no worker, so a watch
    /// of a route where nothing is starts no task and asks nothing, until
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

    /// A device whose link fails to reconnect is lost at the moment of the
    /// status on the paused clock.
    #[tokio::test(start_paused = true)]
    async fn a_loss_by_status_is_dated_on_the_paused_clock() {
        let tasks = Supervisor::current();
        let (store, proxy) = engine_on(&tasks);
        let device = route("/1");
        let followed = presence(&store, device);
        proxy.send(Packet::heartbeat(device));
        while !followed.recv_async().await.as_ref().is_ok_and(alive) {}
        tokio::time::sleep(QUIET / 3).await;

        proxy.send(Packet::link(LinkStatus::FailedToReconnect).with_route(device));
        let failed = Instant::now();
        let now = followed.recv_async().await;
        let Ok(Presence::Lost { since }) = now else {
            panic!("the device was not lost, got {now:?}");
        };
        assert_eq!(since, failed.into_std());
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

    /// A route whose metadata query is backing off and that the engine
    /// stops asking, as its device is lost or no receiver follows it, sets
    /// no wake for the retry, so the engine sleeps past the moment it was
    /// due. Asked again, it is asked at once as its backoff has passed, and
    /// its next failure doubles the delay as the schedule says.
    #[tokio::test(start_paused = true)]
    async fn a_retry_the_engine_will_not_make_sets_no_wake() {
        for unfollowed in [false, true] {
            let tasks = Supervisor::current();
            let (session, proxy) = Session::test_pair_on(&tasks);
            let store = Arc::new(Mutex::new(Store::new(true)));
            let mounts = Mounts::new("tcp://device");
            let mut engine = Engine::new(&store, &mounts, session, &tasks);
            let device = route("/1");
            let mut followed = Some(presence(&store, device));
            engine.take(&Packet::heartbeat(device));
            engine.discover();
            refuse_busy(&proxy, &mut engine, device).await;
            let retry = Instant::now() + super::super::metadata::RETRY_FIRST;
            assert_eq!(engine.next_wake(), Some(retry));

            if unfollowed {
                followed = None;
                pruned(|cx| lock(&store).prune_gone(cx)).await;
                engine.follow_demand();
            } else {
                engine.apply_status(device, LinkStatus::FailedToReconnect);
            }
            crate::chan::sleep_until(retry + Duration::from_millis(1)).await;
            let wake = engine.next_wake();
            assert!(
                wake.is_none_or(|wake| wake > Instant::now()),
                "unfollowed {unfollowed}, the engine wakes at once for a retry it will not make"
            );

            if followed.is_none() {
                followed = Some(presence(&store, device));
                engine.follow_demand();
            }
            engine.take(&Packet::heartbeat(device));
            engine.discover();
            let asked_at = Instant::now();
            refuse_busy(&proxy, &mut engine, device).await;
            assert_eq!(
                asked_at,
                Instant::now(),
                "unfollowed {unfollowed}, asked late"
            );
            let doubled = asked_at + super::super::metadata::RETRY_FIRST * 2;
            assert_eq!(engine.next_wake(), Some(doubled), "unfollowed {unfollowed}");
            drop(followed);
        }
    }

    /// Refuses the one `dev.metadata` call to `device` that the engine has
    /// sent as busy, and has the engine take the refusal.
    async fn refuse_busy(proxy: &TestProxy, engine: &mut Engine, device: DeviceRoute) {
        let asking = tokio::time::timeout(Duration::from_secs(1), proxy.sent.recv_async());
        let asked = asking.await.expect("the engine asks").unwrap();
        let packet::Payload::RpcRequest(request) = asked.payload() else {
            panic!("expected a metadata query, got {:?}", asked.payload());
        };
        assert_eq!(asked.route(), device);
        proxy.refuse(request.id.value(), wire_rpc::RpcError::Busy, device);
        let reply = engine.schedule.replies().recv_async().await.unwrap();
        engine.take_metadata_replies(reply);
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

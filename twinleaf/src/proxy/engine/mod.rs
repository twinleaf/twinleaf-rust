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

mod metadata;
mod settings;

use super::outbox::{ClientError, Outbox};
use crate::chan::lock;
use crate::chan::AbortOnDropHandle;
use crate::data::{MetadataQuery, PacketParser};
use crate::proto::{DeviceRoute, RouteError};
use crate::rt::{sleep_until_some, Supervisor};
use crate::session::rpc::{PendingReply, RpcRegistry};
use crate::session::Queued;
use crate::session::{pruned, rpc_hash, Store, METADATA, PRESENCE};
use crate::session::{Handle, Inlet, RawCallResult, Received, Session, Update, Watch};
use crate::session::{MetadataState, Presence, Settings};
use crate::tio::packet::{self, LinkStatus, Packet};
use metadata::{Completed, MetadataSchedule};
use settings::{CacheAction, DeviceEvent, SettingsCache};
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
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
    metadata: Mutex<BTreeMap<u16, DeviceRoute>>,
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
    heard: BTreeMap<DeviceRoute, Instant>,
    /// What the store's receivers follow, as it last said.
    demand: BTreeSet<Watch>,
    /// The spawner of the workers.
    tasks: Supervisor,
    /// The worker of each device whose RPC table a receiver follows.
    workers: BTreeMap<DeviceRoute, WorkerHold>,
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
            heard: BTreeMap::new(),
            demand: BTreeSet::new(),
            tasks: tasks.clone(),
            workers: BTreeMap::new(),
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
    heard: &'a BTreeMap<DeviceRoute, Instant>,
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
                Ok(registry) => {
                    if registry.is_deprecated_firmware() {
                        log::warn!(
                            "the device at {} runs deprecated C firmware, which Twinleaf is \
                             porting to Rust. Its stops and starts read as lost data. Ask \
                             Twinleaf for its Rust firmware.",
                            self.route
                        );
                    }
                    self.track(&registry).await
                }
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
mod tests;

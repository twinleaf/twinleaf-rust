//! The [`Store`] of the state of every device in one tree.
//!
//! It holds each device's latest presence, metadata, RPC table and settings,
//! and the routes heard, and sends each new value to the receivers that
//! follow it. A receiver that follows a value already held gets it at once,
//! on a latest-value lane, so one that falls behind skips to the newest.
//! Setting changes are the one ordered stream, queued in full for each
//! receiver and held by no one.
//!
//! The proxy keeps one store per link, which its engine fills, and each
//! client's watches follow it. A device the proxy has not heard is
//! connecting with its metadata pending. A client keeps one store per
//! connection, which its pump fills from those watches, and holds no value
//! it has not received.

use super::state::{MetadataState, Presence, RegistryState, SettingChange, Settings};
use super::stream::{End, Receiver, Scope, Sink};
use crate::proto::DeviceRoute;
use crate::proxy::Watch;
use std::collections::{BTreeMap, BTreeSet};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use tokio::sync::Notify;

/// How many setting changes a receiver's queue holds.
pub(crate) const CHANGE_QUEUE_LEN: usize = 4096;

/// One value of a device, and the receivers that follow it. A slot that
/// queues holds no value, and queues each one in full for each receiver.
pub(crate) struct Slot<T> {
    value: Option<T>,
    sinks: Vec<Sink<T>>,
    queues: bool,
}

impl<T: Clone> Slot<T> {
    fn new(value: Option<T>, queues: bool) -> Slot<T> {
        let sinks = Vec::new();
        Slot {
            value,
            sinks,
            queues,
        }
    }

    /// Sends `value` to each receiver, and holds it unless the slot queues.
    fn set(&mut self, value: T) {
        self.sinks.retain_mut(|sink| sink.lane.send(value.clone()));
        self.value = (!self.queues).then_some(value);
    }

    /// Returns a new receiver, which gets the value held at once.
    fn follow(&mut self, route: DeviceRoute) -> Receiver<T> {
        let (mut sink, receiver) = match self.queues {
            true => Sink::new(CHANGE_QUEUE_LEN, Scope::point(route)),
            false => Sink::latest(Scope::point(route)),
        };
        if self.value.clone().is_none_or(|value| sink.lane.send(value)) {
            self.sinks.push(sink);
        }
        receiver
    }
}

/// The state of one device.
pub(crate) struct Entry {
    presence: Slot<Presence>,
    metadata: Slot<MetadataState>,
    registry: Slot<RegistryState>,
    settings: Slot<Arc<Settings>>,
    changes: Slot<SettingChange>,
}

/// Evaluates `$body` with `$sinks` bound to the receivers of each slot of
/// `$entry` in turn, into an array of the results.
macro_rules! each_slot {
    ($entry:expr, |$sinks:ident| $body:expr) => {{
        let entry: &mut Entry = $entry;
        [
            {
                let $sinks = &mut entry.presence.sinks;
                $body
            },
            {
                let $sinks = &mut entry.metadata.sinks;
                $body
            },
            {
                let $sinks = &mut entry.registry.sinks;
                $body
            },
            {
                let $sinks = &mut entry.settings.sinks;
                $body
            },
            {
                let $sinks = &mut entry.changes.sinks;
                $body
            },
        ]
    }};
}

/// Picks one value of a device's state.
pub(crate) type Field<T> = fn(&mut Entry) -> &mut Slot<T>;
pub(crate) const PRESENCE: Field<Presence> = |entry| &mut entry.presence;
pub(crate) const METADATA: Field<MetadataState> = |entry| &mut entry.metadata;
pub(crate) const REGISTRY: Field<RegistryState> = |entry| &mut entry.registry;
pub(crate) const SETTINGS: Field<Arc<Settings>> = |entry| &mut entry.settings;
pub(crate) const CHANGES: Field<SettingChange> = |entry| &mut entry.changes;

/// Locks `store`, past a panic of another holder.
pub(crate) fn lock(store: &Mutex<Store>) -> MutexGuard<'_, Store> {
    store.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The state of every device in one tree, and its receivers.
#[derive(Default)]
pub(crate) struct Store {
    devices: BTreeMap<DeviceRoute, Entry>,
    /// The routes heard.
    routes: BTreeSet<DeviceRoute>,
    /// The roots of the subtrees whose routes are all in `routes`.
    listed: BTreeSet<DeviceRoute>,
    route_sinks: Vec<Sink<Vec<DeviceRoute>>>,
    /// Whether the store has ended, and why when it was told.
    ended: Option<Option<End>>,
    /// Wakes whoever fills the store when a receiver starts to follow it.
    followed: Arc<Notify>,
    /// Whether a device not heard yet is connecting with its metadata
    /// pending, as the proxy says, rather than not known yet.
    proxy: bool,
}

impl Store {
    /// Creates the store of the proxy, with `proxy`, or of a client.
    pub(crate) fn new(proxy: bool) -> Store {
        Store {
            proxy,
            ..Store::default()
        }
    }

    /// Returns what wakes once a receiver starts to follow the store.
    pub(crate) fn followed(&self) -> Arc<Notify> {
        Arc::clone(&self.followed)
    }

    fn entry(&mut self, route: DeviceRoute) -> &mut Entry {
        let proxy = self.proxy;
        self.devices.entry(route).or_insert_with(|| Entry {
            presence: Slot::new(proxy.then_some(Presence::Connecting), false),
            metadata: Slot::new(proxy.then_some(MetadataState::Pending), false),
            registry: Slot::new(None, false),
            settings: Slot::new(None, false),
            changes: Slot::new(None, true),
        })
    }

    /// Follows the `field` of the device at `route`. The receiver yields
    /// the value held now, if any, then each new one, and ends at once if
    /// the store has ended.
    pub(crate) fn follow<T: Clone>(&mut self, route: DeviceRoute, field: Field<T>) -> Receiver<T> {
        let (ended, followed) = (self.ended, Arc::clone(&self.followed));
        let slot = field(self.entry(route));
        let receiver = slot.follow(route);
        match ended {
            None => followed.notify_one(),
            Some(end) => end_sinks(&mut slot.sinks, end),
        }
        receiver
    }

    /// Follows the sorted list of the routes heard in `scope`. The receiver
    /// yields it once the list of a subtree that holds `scope` is known.
    pub(crate) fn follow_routes(&mut self, scope: Scope) -> Receiver<Vec<DeviceRoute>> {
        let (mut sink, receiver) = Sink::latest(scope);
        match self.ended {
            Some(end) => end_sinks(&mut vec![sink], end),
            None => {
                self.followed.notify_one();
                if !self.knows(scope) || sink.lane.send(in_scope(&self.routes, scope)) {
                    self.route_sinks.push(sink);
                }
            }
        }
        receiver
    }

    /// Sets the `field` of the device at `route`.
    pub(crate) fn set<T: Clone>(&mut self, route: DeviceRoute, field: Field<T>, value: T) {
        field(self.entry(route)).set(value);
    }

    /// Returns the `field` of the device at `route`, if it holds one.
    pub(crate) fn value<T: Clone>(&mut self, route: DeviceRoute, field: Field<T>) -> Option<T> {
        field(self.devices.get_mut(&route)?).value.clone()
    }

    /// Sets the RPC table of the device at `route`, and drops the settings
    /// read with the table before.
    pub(crate) fn set_registry(&mut self, route: DeviceRoute, registry: RegistryState) {
        let entry = self.entry(route);
        entry.settings.value = None;
        entry.registry.set(registry);
    }

    /// Sets the settings of the device at `route` if they were read with the
    /// table the store holds, and drops them otherwise.
    pub(crate) fn set_settings(&mut self, route: DeviceRoute, settings: Arc<Settings>) {
        let entry = self.entry(route);
        if let Some(Ok(registry)) = &entry.registry.value {
            if Arc::ptr_eq(registry, settings.rpc_registry()) {
                entry.settings.set(settings);
            }
        }
    }

    /// Tells each receiver of the changes of the device at `route` that it
    /// missed `missed` of them.
    pub(crate) fn lag(&mut self, route: DeviceRoute, missed: u64) {
        if let Some(entry) = self.devices.get_mut(&route) {
            entry.changes.sinks.retain_mut(|sink| sink.lane.lag(missed));
        }
    }

    /// Returns the routes heard, sorted.
    pub(crate) fn routes(&self) -> &BTreeSet<DeviceRoute> {
        &self.routes
    }

    /// Adds `routes`, and notes that they are every route heard below
    /// `below`. Sends the new list to each receiver it changes.
    pub(crate) fn list(
        &mut self,
        below: DeviceRoute,
        routes: impl IntoIterator<Item = DeviceRoute>,
    ) {
        let added: Vec<_> = routes
            .into_iter()
            .filter(|route| self.routes.insert(*route))
            .collect();
        let listed = self.listed.insert(below);
        let (all, known) = (&self.routes, &self.listed);
        self.route_sinks.retain_mut(|sink| {
            let scope = sink.scope;
            let changed = (listed && scope.route.starts_with(&below))
                || added.iter().any(|route| scope.covers(*route));
            let held = known.iter().any(|root| scope.route.starts_with(root));
            !(changed && held) || sink.lane.send(in_scope(all, scope))
        });
    }

    /// Returns `true` once the routes of a subtree that holds `scope` are
    /// known.
    fn knows(&self, scope: Scope) -> bool {
        self.listed.iter().any(|root| scope.route.starts_with(root))
    }

    /// Returns the watch of each device a receiver follows. A list of
    /// routes counts as following the device at its root.
    pub(crate) fn demand(&mut self) -> BTreeSet<Watch> {
        let lists = self.route_sinks.iter().map(|sink| Watch {
            route: sink.scope.route,
            table: false,
        });
        let devices = self.devices.iter_mut().flat_map(|(route, entry)| {
            let [presence, metadata, registry, settings, changes] =
                each_slot!(entry, |sinks| !sinks.is_empty());
            [
                (presence || metadata || changes, false),
                (registry || settings, true),
            ]
            .into_iter()
            .filter(|(followed, _)| *followed)
            .map(|(_, table)| Watch {
                route: *route,
                table,
            })
        });
        lists.chain(devices).collect()
    }

    /// Drops each receiver that is gone, as its lifeline reports in `cx`,
    /// and each device no receiver follows that holds nothing or that the
    /// proxy has not heard. Returns `true` if a receiver was gone.
    pub(crate) fn prune_gone(&mut self, cx: &mut Context<'_>) -> bool {
        let mut pruned = prune(&mut self.route_sinks, cx);
        let (proxy, heard) = (self.proxy, &self.routes);
        self.devices.retain(|route, entry| {
            pruned |= each_slot!(entry, |sinks| prune(sinks, cx)).contains(&true);
            let followed = each_slot!(entry, |sinks| !sinks.is_empty()).contains(&true);
            let holds = [
                entry.presence.value.is_some(),
                entry.metadata.value.is_some(),
            ];
            let holds = holds.contains(&true) || entry.registry.value.is_some();
            followed || (proxy && heard.contains(route)) || (!proxy && holds)
        });
        pruned
    }

    /// Ends every receiver of the device at `route`, and of the routes below
    /// it, which no one sends since the proxy refused to watch it.
    pub(crate) fn end_device(&mut self, route: DeviceRoute) {
        self.route_sinks.retain(|sink| sink.scope.route != route);
        if let Some(entry) = self.devices.get_mut(&route) {
            each_slot!(entry, |sinks| sinks.clear());
        }
    }

    /// Drops the values `watch` kept, which no one keeps current once it
    /// stops, so a receiver that follows them later waits for new ones.
    pub(crate) fn clear(&mut self, watch: Watch) {
        if let Some(entry) = self.devices.get_mut(&watch.route) {
            match watch.table {
                true => (entry.registry.value, entry.settings.value) = (None, None),
                false => (entry.presence.value, entry.metadata.value) = (None, None),
            }
        }
        if !watch.table && self.listed.remove(&watch.route) {
            let listed = &self.listed;
            self.routes.retain(|heard| {
                !heard.starts_with(&watch.route)
                    || listed.iter().any(|root| heard.starts_with(root))
            });
        }
    }

    /// Makes every device the store holds lost, as no one can say more of
    /// them. A device lost already keeps the moment it was lost.
    pub(crate) fn mark_lost(&mut self) {
        let since = std::time::Instant::now();
        for entry in self.devices.values_mut() {
            if !matches!(entry.presence.value, Some(Presence::Lost { .. })) {
                entry.presence.set(Presence::Lost { since });
            }
        }
    }

    /// Ends every receiver, saying why if `end` does, and each that follows
    /// later.
    pub(crate) fn end_all(&mut self, end: Option<End>) {
        self.ended = Some(end);
        end_sinks(&mut self.route_sinks, end);
        for entry in self.devices.values_mut() {
            each_slot!(entry, |sinks| end_sinks(sinks, end));
        }
    }
}

/// Ends each of `sinks`, saying why if `end` does.
pub(crate) fn end_sinks<T>(sinks: &mut Vec<Sink<T>>, end: Option<End>) {
    for sink in sinks.drain(..) {
        if let Some(end) = end {
            sink.lane.end(end);
        }
    }
}

/// Drops each of `sinks` whose receiver is gone. Returns `true` if any was.
pub(crate) fn prune<T>(sinks: &mut Vec<Sink<T>>, cx: &mut Context<'_>) -> bool {
    let held = sinks.len();
    // A lifeline resolves only once its receiver is gone, and its sink is
    // dropped before it is polled again.
    sinks.retain_mut(|sink| Pin::new(&mut sink.alive).poll(cx).is_pending());
    sinks.len() < held
}

/// Waits until `prune` drops a receiver that is gone.
pub(crate) async fn pruned(mut prune: impl FnMut(&mut Context<'_>) -> bool) {
    poll_fn(|cx| match prune(cx) {
        true => Poll::Ready(()),
        false => Poll::Pending,
    })
    .await
}

fn in_scope(routes: &BTreeSet<DeviceRoute>, scope: Scope) -> Vec<DeviceRoute> {
    let routes = routes.iter().copied();
    routes.filter(|route| scope.covers(*route)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::rpc::RpcRegistry;
    use crate::device::stream::RecvError;
    use std::time::Duration;

    fn change(seq: u32) -> SettingChange {
        SettingChange {
            name: "test.amplitude".to_string(),
            seq: Some(seq),
            reply: vec![1],
        }
    }

    /// A receiver that follows a value already held gets it at once, and a
    /// receiver that has read nothing while it changed gets only the newest.
    #[test]
    fn a_slow_receiver_gets_the_newest_value() {
        let mut store = Store::new(true);
        let route = DeviceRoute::root();
        let slow = store.follow(route, PRESENCE);
        for _ in 0..1000 {
            store.set(route, PRESENCE, Presence::Connecting);
            let session = Some(crate::proto::SessionId::new(3));
            store.set(route, PRESENCE, Presence::Alive { session });
        }

        let alive = Presence::Alive {
            session: Some(crate::proto::SessionId::new(3)),
        };
        assert_eq!(slow.try_recv(), Ok(Some(alive)));
        assert_eq!(slow.try_recv(), Ok(None));
        let late = store.follow(route, PRESENCE);
        assert_eq!(late.try_recv(), Ok(Some(alive)));
    }

    fn table(hash: u32) -> Arc<RpcRegistry> {
        Arc::new(RpcRegistry::from_entries(Vec::new(), Some(hash), None))
    }

    fn settings_of(table: &Arc<RpcRegistry>) -> Arc<Settings> {
        Arc::new(Settings::new(
            Arc::clone(table),
            Some(1),
            Default::default(),
        ))
    }

    /// A new table drops the settings read with the old one, and settings
    /// read with any table but the one held are never kept, so a receiver
    /// never gets settings paired with another table.
    #[test]
    fn the_store_holds_settings_only_for_its_current_table() {
        let mut store = Store::new(true);
        let route = DeviceRoute::root();
        let (old, new) = (table(1), table(2));
        store.set_registry(route, Ok(Arc::clone(&old)));
        store.set_settings(route, settings_of(&old));

        store.set_registry(route, Ok(Arc::clone(&new)));
        assert!(matches!(store.follow(route, SETTINGS).try_recv(), Ok(None)));
        store.set_settings(route, settings_of(&old));
        assert!(matches!(store.follow(route, SETTINGS).try_recv(), Ok(None)));
        store.set_settings(route, settings_of(&new));
        let held = store.follow(route, SETTINGS).try_recv().unwrap().unwrap();
        assert!(Arc::ptr_eq(held.rpc_registry(), &new));
    }

    /// A device no receiver follows any more leaves the proxy's store unless
    /// the proxy heard it, so following routes where nothing is grows
    /// nothing.
    #[test]
    fn a_device_no_one_follows_and_no_one_heard_is_forgotten() {
        let mut store = Store::new(true);
        let (heard, unheard): (DeviceRoute, DeviceRoute) =
            ("/1".parse().unwrap(), "/2".parse().unwrap());
        store.list(DeviceRoute::root(), [heard]);
        drop([
            store.follow(heard, PRESENCE),
            store.follow(unheard, PRESENCE),
        ]);

        let waker = futures_util::task::noop_waker();
        assert!(store.prune_gone(&mut Context::from_waker(&waker)));

        assert_eq!(store.devices.keys().copied().collect::<Vec<_>>(), [heard]);
    }

    /// A device lost already keeps the moment it was lost when every device
    /// is made lost.
    #[test]
    fn a_device_lost_already_keeps_its_moment() {
        let mut store = Store::new(false);
        let route = DeviceRoute::root();
        let since = std::time::Instant::now() - Duration::from_secs(60);
        store.set(route, PRESENCE, Presence::Lost { since });

        store.mark_lost();

        assert_eq!(store.value(route, PRESENCE), Some(Presence::Lost { since }));
    }

    /// Setting changes are queued in full, and a receiver that overflows its
    /// queue is told how many it missed.
    #[test]
    fn a_setting_change_receiver_that_overflows_learns_it_lagged() {
        let mut store = Store::new(true);
        let route = DeviceRoute::root();
        let changes = store.follow(route, CHANGES);
        let sent = CHANGE_QUEUE_LEN as u32 + 10;
        for seq in 0..sent {
            store.set(route, CHANGES, change(seq));
        }
        for seq in 0..CHANGE_QUEUE_LEN as u32 {
            assert_eq!(
                changes.try_recv().unwrap().map(|change| change.seq),
                Some(Some(seq))
            );
        }

        store.set(route, CHANGES, change(sent));
        assert_eq!(changes.try_recv(), Err(RecvError::Lagged(10)));
        assert_eq!(
            changes.try_recv().unwrap().map(|change| change.seq),
            Some(Some(sent))
        );
    }

    /// A store that ends makes the devices it followed lost first, then ends
    /// every receiver, and each that follows later.
    #[test]
    fn an_ended_store_loses_its_devices_then_ends_every_receiver() {
        let mut store = Store::new(false);
        let route = DeviceRoute::root();
        let presence = store.follow(route, PRESENCE);

        store.mark_lost();
        store.end_all(Some(End::Stopped));

        assert!(matches!(
            presence.try_recv(),
            Ok(Some(Presence::Lost { .. }))
        ));
        assert_eq!(presence.try_recv(), Err(RecvError::Stopped));
        let late = store.follow(route, METADATA);
        assert_eq!(
            late.recv_timeout(Duration::from_secs(1)).err(),
            Some(RecvError::Stopped)
        );
    }

    /// A route list follows only once the routes of a subtree that holds its
    /// scope are known.
    #[test]
    fn a_route_list_waits_until_its_subtree_is_listed() {
        let mut store = Store::new(true);
        let below: DeviceRoute = "/1".parse().unwrap();
        let routes = store.follow_routes(Scope::subtree(below));
        assert!(matches!(routes.try_recv(), Ok(None)));

        store.list(DeviceRoute::root(), ["/2".parse().unwrap()]);
        assert_eq!(routes.try_recv(), Ok(Some(Vec::new())));
        store.list(DeviceRoute::root(), [below]);
        assert_eq!(routes.try_recv(), Ok(Some(vec![below])));
    }
}

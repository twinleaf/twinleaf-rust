//! The [`Store`] of the state of every device in one tree.
//!
//! It holds each device's latest presence, metadata, RPC table and settings,
//! and the routes heard, and sends each new value to the receivers that
//! follow it. A receiver that follows a value already held gets it at once,
//! on a latest-value lane, so one that falls behind skips to the newest.
//! Setting changes are the one ordered stream, queued in full for each
//! receiver and held by no one.
//!
//! Each setting change carries the RPC table in force for its device when
//! the store records it. In the proxy's store a table is in force from its
//! load until the device reconnects, the device reports another session, or
//! the device announces another `rpc.hash`. A device reconnects as its link
//! reconnects, and as it is heard again after it was lost. Only a load that
//! started after the device last reconnected counts. A client's store
//! takes the table the proxy names ahead of the changes recorded under it.
//!
//! The proxy keeps one store per link, which its engine fills, and each
//! client's watches follow it. A device the proxy has not heard is
//! connecting with its metadata pending. A client keeps one store per
//! connection, which its pump fills from those watches, and holds no value
//! it has not received.

use super::control::{Update, Watch, CHANGED, TIMED_OUT, UNANSWERED};
use super::rpc::{CallError, RpcErrorPayload, RpcRegistry, RpcRegistryError};
use super::state::{MetadataState, Presence, RegistryState, SettingChange, Settings};
use super::subscription::{End, Lane, Receiver, Scope, Sink};
use crate::chan::Notify;
use crate::data::PacketParser;
use crate::proto::rpc::RpcError;
use crate::proto::{DeviceRoute, SessionId};
use crate::tio::packet::Packet;
use std::collections::{BTreeMap, BTreeSet};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

/// How many setting changes a receiver's queue holds.
pub(crate) const CHANGE_QUEUE_LEN: usize = 4096;

/// One value of a device, and the receivers that follow it. A slot that
/// queues holds no value, and queues each one in full for each receiver.
pub(crate) struct Slot<T> {
    value: Option<T>,
    sinks: Vec<Sink<Lane<T>>>,
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
    /// The last `rpc.hash` the device announced to the proxy, and the
    /// session it reported then.
    announced: Option<(Option<SessionId>, u32)>,
    /// How many times the device has reconnected.
    epoch: u64,
    /// The epoch the load of the table the store holds started in.
    loaded_in: u64,
    /// The table the proxy names for the changes a client records next.
    named: Option<Arc<RpcRegistry>>,
}

impl Entry {
    /// Returns the table the proxy's store holds in force for the device.
    /// That is the table it holds when its load started after the device
    /// last reconnected, while the device reports the session the
    /// table was read in or reports none, and while the table has the hash
    /// the device last announced in that session.
    fn in_force(&self) -> Option<Arc<RpcRegistry>> {
        let Some(Ok(table)) = &self.registry.value else {
            return None;
        };
        let session = match self.presence.value {
            Some(Presence::Alive { session }) => session,
            _ => None,
        };
        let same_boot = match (table.session, session) {
            (Some(read_in), Some(reported)) => read_in == reported,
            _ => true,
        };
        let same_hash = match self.announced {
            Some((announced_in, hash)) if announced_in == session => table.hash == Some(hash),
            _ => true,
        };
        let current = self.loaded_in == self.epoch;
        (current && same_boot && same_hash).then(|| Arc::clone(table))
    }

    /// Returns the table of this device the store holds with `hash` and
    /// `session`, if any.
    fn table_of(&self, hash: Option<u32>, session: Option<SessionId>) -> Option<Arc<RpcRegistry>> {
        let held = match &self.registry.value {
            Some(Ok(table)) => Some(table),
            _ => None,
        };
        held.into_iter()
            .chain(&self.named)
            .find(|table| (table.hash, table.session) == (hash, session))
            .cloned()
    }
}

/// Returns the RPC table hash an announcement of `name` with `reply` gives,
/// a little endian `u32` from `rpc.hash`, or `None` for any other setting.
pub(crate) fn rpc_hash(name: &[u8], reply: &[u8]) -> Option<u32> {
    let value = reply.get(..4).filter(|_| name == b"rpc.hash")?;
    Some(u32::from_le_bytes(value.try_into().ok()?))
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

/// The state of every device in one tree, and its receivers.
#[derive(Default)]
pub(crate) struct Store {
    devices: BTreeMap<DeviceRoute, Entry>,
    /// The routes heard.
    routes: BTreeSet<DeviceRoute>,
    /// The roots of the subtrees whose routes are all in `routes`.
    listed: BTreeSet<DeviceRoute>,
    route_sinks: Vec<Sink<Lane<Vec<DeviceRoute>>>>,
    /// Whether the store has ended, and why when it was told.
    ended: Option<Option<End>>,
    /// Wakes whoever fills the store when a receiver starts to follow it.
    pub(crate) followed: Arc<Notify>,
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

    fn entry(&mut self, route: DeviceRoute) -> &mut Entry {
        let proxy = self.proxy;
        self.devices.entry(route).or_insert_with(|| Entry {
            presence: Slot::new(proxy.then_some(Presence::Connecting), false),
            metadata: Slot::new(proxy.then_some(MetadataState::Pending), false),
            registry: Slot::new(None, false),
            settings: Slot::new(None, false),
            changes: Slot::new(None, true),
            announced: None,
            epoch: 0,
            loaded_in: 0,
            named: None,
        })
    }

    /// Follows the `field` of the device at `route`. The receiver yields
    /// the value held now, if any, then each new one, and ends at once if
    /// the store has ended.
    pub(crate) fn follow<T: Clone>(&mut self, route: DeviceRoute, field: Field<T>) -> Receiver<T> {
        self.followed.notify_one();
        let ended = self.ended;
        let slot = field(self.entry(route));
        let receiver = slot.follow(route);
        if let Some(end) = ended {
            end_sinks(&mut slot.sinks, end);
        }
        receiver
    }

    /// Follows the sorted list of the routes heard in `scope`. The receiver
    /// yields it once the list of a subtree that holds `scope` is known.
    pub(crate) fn follow_routes(&mut self, scope: Scope) -> Receiver<Vec<DeviceRoute>> {
        let (mut sink, receiver) = Sink::latest(scope);
        self.followed.notify_one();
        if !self.knows(scope) || sink.lane.send(in_scope(&self.routes, scope)) {
            self.route_sinks.push(sink);
        }
        if let Some(end) = self.ended {
            end_sinks(&mut self.route_sinks, end);
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

    /// Sets the RPC table of the device at `route`, as loaded in the current
    /// epoch, and drops the settings read with the table before.
    pub(crate) fn set_registry(&mut self, route: DeviceRoute, registry: RegistryState) {
        let epoch = self.epoch(route);
        self.set_loaded(route, registry, epoch);
    }

    /// Sets the RPC table of the device at `route`, from a load that started
    /// in `started`, which [`epoch`](Self::epoch) gave then, and drops the
    /// settings read with the table before. A table whose load started
    /// before the device last reconnected is held but not in force.
    pub(crate) fn set_loaded(&mut self, route: DeviceRoute, registry: RegistryState, started: u64) {
        let entry = self.entry(route);
        entry.settings.value = None;
        entry.loaded_in = started;
        entry.registry.set(registry);
    }

    /// Returns the epoch of the device at `route`, which counts how many
    /// times it has reconnected. A table load reads it as it starts.
    pub(crate) fn epoch(&mut self, route: DeviceRoute) -> u64 {
        self.entry(route).epoch
    }

    /// Starts a new epoch for the device at `route`, as it reconnected and
    /// may have restarted with another table. The table held is out of force
    /// until a table whose load started after this is set.
    pub(crate) fn reconnect(&mut self, route: DeviceRoute) {
        self.entry(route).epoch += 1;
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

    /// Records a setting the device at `route` announced, the change of
    /// `name` to `reply` counted as `seq`, and sends it to each receiver of
    /// its changes. The change carries the table in force for the device
    /// then. Returns the change.
    ///
    /// In the proxy's store an `rpc.hash` announcement first sets the hash
    /// that the table in force must have, from this change on.
    pub(crate) fn record_change(
        &mut self,
        route: DeviceRoute,
        name: String,
        seq: Option<u32>,
        reply: Vec<u8>,
    ) -> SettingChange {
        let proxy = self.proxy;
        let entry = self.entry(route);
        let table = if proxy {
            if let Some(hash) = rpc_hash(name.as_bytes(), &reply) {
                let session = match entry.presence.value {
                    Some(Presence::Alive { session }) => session,
                    _ => None,
                };
                entry.announced = Some((session, hash));
            }
            entry.in_force()
        } else {
            entry.named.clone()
        };
        let change = SettingChange::new(name, seq, reply, table);
        entry.changes.set(change.clone());
        change
    }

    /// Names `table` as the table the changes of the device at `route` that
    /// a client's store records next were recorded under, or no table. A
    /// table with the hash and session of one the store holds is that one.
    pub(crate) fn name_table(&mut self, route: DeviceRoute, table: Option<RpcRegistry>) {
        let entry = self.entry(route);
        entry.named = table.map(|table| {
            entry
                .table_of(table.hash, table.session)
                .unwrap_or_else(|| Arc::new(table))
        });
    }

    /// Returns the table of the device at `route` the store holds with
    /// `hash` and `session`, either as its table or as the one named for its
    /// changes, if any.
    pub(crate) fn table_of(
        &self,
        route: DeviceRoute,
        hash: Option<u32>,
        session: Option<SessionId>,
    ) -> Option<Arc<RpcRegistry>> {
        self.devices.get(&route)?.table_of(hash, session)
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
    /// routes counts as following the device at its root. In a client's
    /// store a receiver of the changes follows the table too, so that the
    /// proxy loads the table the changes are recorded under.
    pub(crate) fn demand(&mut self) -> BTreeSet<Watch> {
        let lists = self.route_sinks.iter().map(|sink| Watch {
            route: sink.scope.route,
            table: false,
        });
        let client = !self.proxy;
        let devices = self.devices.iter_mut().flat_map(move |(route, entry)| {
            let [presence, metadata, registry, settings, changes] =
                each_slot!(entry, |sinks| !sinks.is_empty());
            [
                (presence || metadata || changes, false),
                (registry || settings || (client && changes), true),
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
                false => {
                    (entry.presence.value, entry.metadata.value) = (None, None);
                    entry.named = None;
                }
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
        let since = tokio::time::Instant::now().into_std();
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

    /// Keeps what `update` says of the device at `route`. Its
    /// metadata records reach `parser` as the device's own, and the
    /// metadata the parser then holds is the value kept, and no metadata
    /// resets any route but `route`. Settings are kept only when they were
    /// read with the table the store holds, and a table with the hash and
    /// session of the one held keeps them. A table with the hash and session
    /// of one the store holds already is that one, so the device's table and
    /// the table its changes carry are one value while they are the same.
    pub(crate) fn apply(&mut self, route: DeviceRoute, update: Update, parser: &mut PacketParser) {
        match update {
            Update::Presence(presence) => {
                let now = Instant::now().into_std();
                let presence = match presence.map(|session| session.map(SessionId::new)) {
                    Ok(session) => Presence::Alive { session },
                    Err(None) => Presence::Connecting,
                    Err(Some(ago)) => {
                        let since = now.checked_sub(Duration::from_millis(ago));
                        Presence::Lost {
                            since: since.unwrap_or(now),
                        }
                    }
                };
                self.set(route, PRESENCE, presence);
            }
            Update::Metadata(None) => self.set(route, METADATA, MetadataState::Unsupported),
            Update::Metadata(Some(records)) if records.is_empty() => {
                parser.relearn_route(route);
                self.set(route, METADATA, MetadataState::Pending);
            }
            Update::Metadata(Some(records)) => {
                for record in records {
                    let applied = Packet::from_slice_prefix(&record)
                        .map_err(|error| error.to_string())
                        .and_then(|(packet, _)| {
                            let packet = packet.with_route(route);
                            parser
                                .push_packet(&packet)
                                .map_err(|error| error.to_string())
                        });
                    if let Err(error) = applied {
                        log::warn!("dropping a metadata record of {route}: {error}");
                    }
                }
                if let Some(snapshot) = parser.metadata(route) {
                    self.set(route, METADATA, MetadataState::Ready(snapshot));
                }
            }
            Update::Table(Ok((hash, session, entries))) => {
                let session = session.map(SessionId::new);
                // The same table again keeps the settings read with it.
                let held = self.value(route, REGISTRY);
                if !held.is_some_and(|held| {
                    held.is_ok_and(|held| (held.hash, held.session) == (hash, session))
                }) {
                    let table = self.table_of(route, hash, session).unwrap_or_else(|| {
                        Arc::new(RpcRegistry::from_entries(entries, hash, session))
                    });
                    self.set_registry(route, Ok(table));
                }
            }
            Update::Table(Err((CHANGED, _))) => {
                self.set_registry(route, Err(RpcRegistryError::TableChanged))
            }
            Update::Table(Err((code, message))) => {
                let error = match code {
                    TIMED_OUT => CallError::Timeout,
                    UNANSWERED => CallError::ResponseLost,
                    code => CallError::DeviceError(RpcErrorPayload {
                        error: RpcError::from(code),
                        extra: message.into(),
                    }),
                };
                self.set_registry(route, Err(RpcRegistryError::DeviceRpcError(error)));
            }
            Update::Settings(hash, version, values) => match self.value(route, REGISTRY) {
                Some(Ok(registry)) if registry.hash == hash => {
                    let values = values.into_iter().map(|(name, value)| (name, value.into()));
                    let settings = Settings::new(registry, version, values.collect());
                    self.set_settings(route, Arc::new(settings));
                }
                _ => log::warn!("dropping settings of {route} read with a table it does not hold"),
            },
            Update::Setting(name, seq, reply) => {
                self.record_change(route, name, seq, reply.into());
            }
            Update::Recorded(table) => {
                let table = table.map(|(hash, session, entries)| {
                    RpcRegistry::from_entries(entries, hash, session.map(SessionId::new))
                });
                self.name_table(route, table);
            }
            Update::Routes(routes) => {
                let routes = routes
                    .iter()
                    .filter_map(|hops| DeviceRoute::from_hops(hops).ok());
                self.list(route, routes);
            }
            Update::Lagged(missed) => self.lag(route, missed),
        }
    }
}

/// Ends each of `sinks`, saying why if `end` does.
pub(crate) fn end_sinks<T>(sinks: &mut Vec<Sink<Lane<T>>>, end: Option<End>) {
    for sink in sinks.drain(..) {
        if let Some(end) = end {
            sink.lane.end(end);
        }
    }
}

/// Drops each of `sinks` whose receiver is gone. Returns `true` if any was.
pub(crate) fn prune<L>(sinks: &mut Vec<Sink<L>>, cx: &mut Context<'_>) -> bool {
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
    use crate::session::rpc::{RpcRegistry, RpcRegistryError, RpcValue};
    use crate::session::state::SettingValueError;
    use crate::session::subscription::RecvError;
    use std::time::Duration;

    fn change(seq: u32) -> SettingChange {
        SettingChange::new("test.amplitude".to_string(), Some(seq), vec![1], None)
    }

    /// The metadata words of an announced read and write `u8` and `i16`.
    const BYTE: u16 = 0xA310;
    const SHORT: u16 = 0xA321;

    /// A table with `hash`, read in `session`, whose one entry
    /// `test.amplitude` has the type of `meta`, or no entry without.
    fn table_in(hash: u32, session: u32, meta: Option<u16>) -> Arc<RpcRegistry> {
        let entries = meta.map(|meta| ("test.amplitude".to_string(), meta));
        let session = Some(SessionId::new(session));
        let table = RpcRegistry::from_entries(entries.into_iter().collect(), Some(hash), session);
        Arc::new(table)
    }

    /// Makes the device at the root alive in `session`.
    fn alive(store: &mut Store, session: u32) {
        let session = Some(SessionId::new(session));
        store.set(DeviceRoute::root(), PRESENCE, Presence::Alive { session });
    }

    /// Records the device at the root announcing `name` as `reply`.
    fn record(store: &mut Store, name: &str, reply: &[u8]) -> SettingChange {
        let route = DeviceRoute::root();
        store.record_change(route, name.to_string(), None, reply.to_vec())
    }

    /// The proxy's store records each change under the table in force for
    /// its device then. None is in force before the first table loads, from
    /// an `rpc.hash` that names another table until that table loads, or
    /// from a new session until its table loads. A change recorded meanwhile
    /// reaches each receiver all the same, with no table.
    #[test]
    fn a_change_carries_the_table_in_force_when_it_is_recorded() {
        let mut store = Store::new(true);
        let route = DeviceRoute::root();
        let changes = store.follow(route, CHANGES);
        let (old, new) = (table_in(1, 7, Some(BYTE)), table_in(2, 7, Some(BYTE)));
        alive(&mut store, 7);

        record(&mut store, "test.amplitude", &[1]);
        store.set_registry(route, Ok(Arc::clone(&old)));
        record(&mut store, "test.amplitude", &[2]);
        record(&mut store, "rpc.hash", &2u32.to_le_bytes());
        record(&mut store, "test.amplitude", &[3]);
        store.set_registry(route, Ok(Arc::clone(&new)));
        record(&mut store, "test.amplitude", &[4]);
        alive(&mut store, 8);
        record(&mut store, "test.amplitude", &[5]);

        let carried: Vec<_> = std::iter::from_fn(|| changes.try_recv().unwrap())
            .map(|change| change.rpc_registry().map(Arc::as_ptr))
            .collect();
        let (old, new) = (Some(Arc::as_ptr(&old)), Some(Arc::as_ptr(&new)));
        assert_eq!(carried, [None, old, None, None, new, None]);
    }

    /// A table is out of force from the moment the device's link
    /// reconnects, whatever session the device reports then, until a table
    /// whose load started after the reconnect is set. One whose load started
    /// before is held but not in force.
    #[test]
    fn a_change_after_a_reconnect_carries_no_table_until_the_table_loads_again() {
        let mut store = Store::new(true);
        let route = DeviceRoute::root();
        alive(&mut store, 7);
        store.set_registry(route, Ok(table_in(1, 7, Some(BYTE))));

        let started = store.epoch(route);
        store.reconnect(route);
        store.set(route, PRESENCE, Presence::Alive { session: None });
        let unheard = record(&mut store, "test.amplitude", &[1]);
        alive(&mut store, 7);
        let beat = record(&mut store, "test.amplitude", &[2]);
        store.set_loaded(route, Ok(table_in(1, 7, Some(BYTE))), started);
        let across = record(&mut store, "test.amplitude", &[3]);
        let reloaded = table_in(1, 7, Some(BYTE));
        let started = store.epoch(route);
        store.set_loaded(route, Ok(Arc::clone(&reloaded)), started);
        let loaded = record(&mut store, "test.amplitude", &[4]);

        assert!(unheard.rpc_registry().is_none());
        assert!(beat.rpc_registry().is_none());
        assert!(across.rpc_registry().is_none());
        assert!(Arc::ptr_eq(loaded.rpc_registry().unwrap(), &reloaded));
    }

    /// A device whose heartbeats carry no session has its table in force
    /// once it loads, though the table was read in a session, and its
    /// changes decode.
    #[test]
    fn a_table_is_in_force_for_a_device_whose_heartbeats_carry_no_session() {
        let mut store = Store::new(true);
        let route = DeviceRoute::root();
        store.set(route, PRESENCE, Presence::Alive { session: None });
        store.set_registry(route, Ok(table_in(1, 7, Some(BYTE))));

        let change = record(&mut store, "test.amplitude", &[5]);

        assert_eq!(change.value().unwrap(), RpcValue::U64(5));
    }

    /// A change decodes with the table it was recorded under, after the
    /// store holds a table that types its entry otherwise or lacks it, and a
    /// change recorded under no table does not decode.
    #[test]
    fn a_change_decodes_with_its_own_table_after_the_store_moves_on() {
        let mut store = Store::new(true);
        let route = DeviceRoute::root();
        alive(&mut store, 7);
        let reply = 0x1234i16.to_le_bytes();

        store.set_registry(route, Ok(table_in(1, 7, Some(BYTE))));
        let byte = record(&mut store, "test.amplitude", &reply);
        store.set_registry(route, Ok(table_in(2, 7, Some(SHORT))));
        let short = record(&mut store, "test.amplitude", &reply);
        store.set_registry(route, Ok(table_in(3, 7, None)));
        let lacking = record(&mut store, "test.amplitude", &reply);
        store.set_registry(route, Err(RpcRegistryError::TableChanged));
        let unloaded = record(&mut store, "test.amplitude", &reply);

        assert_eq!(byte.value().unwrap(), RpcValue::U64(0x34));
        assert_eq!(short.value().unwrap(), RpcValue::I64(0x1234));
        assert!(matches!(lacking.value(), Err(SettingValueError::NoEntry)));
        assert!(matches!(unloaded.value(), Err(SettingValueError::NoTable)));
        assert_eq!(unloaded.reply, reply);
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

    /// Every device made lost is lost at that moment on the paused clock.
    #[tokio::test(start_paused = true)]
    async fn a_device_made_lost_is_dated_on_the_paused_clock() {
        let mut store = Store::new(false);
        let route = DeviceRoute::root();
        store.set(route, PRESENCE, Presence::Connecting);
        tokio::time::sleep(Duration::from_secs(60)).await;

        store.mark_lost();

        let since = tokio::time::Instant::now().into_std();
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
        assert_eq!(changes.try_recv().err(), Some(RecvError::Lagged(10)));
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

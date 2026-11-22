//! The devices the hub's connection sees, and the RPCs the page calls on
//! them.
//!
//! A watcher per device follows what the proxy keeps of it, its presence,
//! metadata, RPC table and announced settings, and sends each to every page
//! as it changes. None of that asks the device anything. As a table loads,
//! the watcher lists the device's capture RPCs, and asks the device once
//! what each is a capture of, and `firmware` reads the device's `dev.desc`
//! once. Each message is the newest value of its kind,
//! as the proxy keeps it, so [`Devices`] keeps the newest of each for a
//! page's snapshot, and sends nothing that changes nothing. `captures`
//! keeps where each capture RPC stands here too, and `firmware` what each
//! device runs and where its upgrade stands. A device as its metadata
//! first names it, or names it anew, and a table that did not load are
//! each a line of the log.
//!
//! A device is named by its route, as `/` or `/0`, which is also how traces
//! name theirs. The routes message lists the devices there are. A device it
//! does not list is in no snapshot and sends no change, and one it lists
//! again sends its newest of each kind again, so a page that heard every
//! change holds what a snapshot would give it.

use crate::captures::is_capture;
use crate::feed::Feed;
use crate::firmware::Firmware;
use crate::logbook::Logbook;
use crate::loss::Losses;
use crate::protocol::{
    Access, CallReply, CaptureInfo, CaptureLabel, Frame, Level, Message, MountInfo, Outcome,
    Presence, ReadReply, Refusal, RpcInfo, TableInfo, TableState,
};
use axum::http::StatusCode;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use twinleaf::device::capture::Capture;
use twinleaf::device::rpc::{
    self, Channel, RpcAccess, RpcDescriptor, RpcMetaExt, RpcRegistry, RpcValueType, RpcValueTypeExt,
};
use twinleaf::device::{LinkState, MetadataState, RegistryState};
use twinleaf::{Connection, Device, DeviceRoute, DeviceTree, RecvError};

/// The kinds of message a device has the newest of, in the order a
/// snapshot holds them.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Kind {
    Device,
    Table,
    Settings,
    /// The capture RPCs it lists.
    Captures,
    /// Where the capture RPC of this name stands.
    Capture(String),
    /// The firmware it runs, and what the catalog holds for it.
    Firmware,
    /// Where its upgrade stands.
    Upgrade,
}

/// The newest of what the watchers sent, and the feed it goes out on.
pub(crate) struct Devices {
    latest: Mutex<Latest>,
    feed: Arc<Feed>,
}

#[derive(Default)]
struct Latest {
    /// The newest routes message, which a snapshot starts with.
    routes: Option<Message>,
    /// The routes it lists.
    listed: BTreeSet<DeviceRoute>,
    /// Each device's newest message of each kind, listed or not.
    devices: BTreeMap<(DeviceRoute, Kind), Message>,
    /// The table load each device's list of captures is of, counted per
    /// device.
    listings: BTreeMap<DeviceRoute, u64>,
}

impl Devices {
    pub(crate) fn new(feed: Arc<Feed>) -> Arc<Devices> {
        Arc::new(Devices {
            latest: Mutex::new(Latest::default()),
            feed,
        })
    }

    /// Lists `routes` as the devices there are, with what the hub mounted,
    /// and sends again the newest of each kind of every device listed anew.
    pub(crate) fn publish_routes(&self, routes: &[DeviceRoute], mounts: Option<Vec<MountInfo>>) {
        let mut latest = self.latest.lock().unwrap();
        let listed: BTreeSet<DeviceRoute> = routes.iter().copied().collect();
        let message = Message::Routes {
            routes: routes.iter().map(|r| r.to_string()).collect(),
            mounts,
        };
        self.feed.message(&message);
        for ((route, _), message) in &latest.devices {
            if listed.contains(route) && !latest.listed.contains(route) {
                self.feed.message(message);
            }
        }
        latest.routes = Some(message);
        latest.listed = listed;
    }

    /// Keeps `message` as the newest of its `kind` for `route`, and sends it
    /// when the routes list the device.
    pub(crate) fn publish(&self, route: DeviceRoute, kind: Kind, message: Message) {
        let mut latest = self.latest.lock().unwrap();
        if latest.listed.contains(&route) {
            self.feed.message(&message);
        }
        latest.devices.insert((route, kind), message);
    }

    /// Lists `captures` as the capture RPCs of the device at `route`, as
    /// its table load `load` has them. A list of an older load than the one
    /// listed is dropped. Where a capture RPC stood that the device no
    /// longer lists goes with it.
    fn publish_captures(&self, route: DeviceRoute, load: u64, captures: Vec<CaptureInfo>) {
        let mut latest = self.latest.lock().unwrap();
        if latest
            .listings
            .get(&route)
            .is_some_and(|&listed| listed > load)
        {
            return;
        }
        latest.listings.insert(route, load);
        latest.devices.retain(|(at, kind), _| match kind {
            Kind::Capture(rpc) if *at == route => captures.iter().any(|listed| listed.rpc == *rpc),
            _ => true,
        });
        let message = Message::Captures {
            route: route.to_string(),
            captures,
        };
        if latest.listed.contains(&route) {
            self.feed.message(&message);
        }
        latest.devices.insert((route, Kind::Captures), message);
    }

    /// The routes, then the newest message of each kind of each device
    /// listed, for a page's snapshot.
    pub(crate) fn snapshot(&self) -> Vec<Frame> {
        let latest = self.latest.lock().unwrap();
        let devices = latest
            .devices
            .iter()
            .filter(|((route, _), _)| latest.listed.contains(route));
        latest
            .routes
            .iter()
            .chain(devices.map(|(_, message)| message))
            .map(Message::frame)
            .collect()
    }

    /// Keeps `label` as what the capture `rpc` of the device at `route` is
    /// of, as a capture read of it says, and lists the device's captures
    /// again when that is news.
    pub(crate) fn label_capture(&self, route: DeviceRoute, rpc: &str, label: CaptureLabel) {
        let mut latest = self.latest.lock().unwrap();
        let key = (route, Kind::Captures);
        let Some(Message::Captures { captures, .. }) = latest.devices.get_mut(&key) else {
            return;
        };
        let Some(info) = captures.iter_mut().find(|info| info.rpc == rpc) else {
            return;
        };
        if info.label.as_ref() == Some(&label) {
            return;
        }
        info.label = Some(label);
        if latest.listed.contains(&route) {
            self.feed.message(&latest.devices[&key]);
        }
    }

    /// The newest message of `kind` of the device at `route`, listed or
    /// not, as a snapshot holds it.
    pub(crate) fn newest(&self, route: DeviceRoute, kind: Kind) -> Option<Message> {
        self.latest
            .lock()
            .unwrap()
            .devices
            .get(&(route, kind))
            .cloned()
    }

    /// The devices the routes list.
    pub(crate) fn listed(&self) -> BTreeSet<DeviceRoute> {
        self.latest.lock().unwrap().listed.clone()
    }

    /// Whether the hub lists `rpc` among the captures of the device at
    /// `route`, as its newest list of them has it.
    pub(crate) fn lists_capture(&self, route: DeviceRoute, rpc: &str) -> bool {
        let latest = self.latest.lock().unwrap();
        match latest.devices.get(&(route, Kind::Captures)) {
            Some(Message::Captures { captures, .. }) => captures.iter().any(|info| info.rpc == rpc),
            _ => false,
        }
    }
}

/// Starts the watchers of every device `connection` sees, on `runtime`, for
/// as long as the connection lasts. `mounts` is what the hub mounted, or
/// `None` for the proxy's default. Each drop of a device's link is counted
/// in `losses`, `log` takes the watchers' lines, and `firmware` reads what a
/// device runs as its table loads.
pub(crate) fn watch(
    connection: &Connection,
    mounts: Option<Vec<MountInfo>>,
    devices: Arc<Devices>,
    losses: Arc<Losses>,
    log: Arc<Logbook>,
    firmware: Arc<Firmware>,
    runtime: &tokio::runtime::Handle,
) {
    let tree = connection.tree(DeviceRoute::root());
    let routes = tree.routes_updates();
    let spawner = runtime.clone();
    runtime.spawn(async move {
        let mut watched = BTreeSet::new();
        loop {
            let seen = match routes.recv_async().await {
                Ok(seen) => seen,
                Err(RecvError::Lagged(_)) => continue,
                Err(_) => return,
            };
            devices.publish_routes(&seen, mounts.clone());
            for route in seen {
                if watched.insert(route) {
                    let device = tree.device(route).expect("a route the tree heard is in it");
                    let (devices, losses) = (devices.clone(), losses.clone());
                    let (log, firmware) = (log.clone(), firmware.clone());
                    spawner.spawn(watch_device(device, route, devices, losses, log, firmware));
                }
            }
        }
    });
}

/// Notes the presence `update` of the device at `route` in `losses`, and
/// returns what the page shows of it. The lines of a drop and a return carry
/// the link's newest URL. The proxy says a new URL ahead of the presence it
/// goes with, so `newer`, what the link lane holds unread, is read into
/// `linked` first.
fn note_presence(
    update: twinleaf::device::Presence,
    route: DeviceRoute,
    newer: impl IntoIterator<Item = LinkState>,
    linked: &mut Option<LinkState>,
    losses: &Losses,
) -> Presence {
    if let Some(newest) = newer.into_iter().last() {
        *linked = Some(newest);
    }
    match update {
        twinleaf::device::Presence::Alive { .. } => {
            losses.link_up(route, linked.as_ref());
            Presence::Alive
        }
        twinleaf::device::Presence::Lost { since, cause } => {
            losses.link_down(route, since, cause, linked.as_ref());
            Presence::Lost
        }
        _ => Presence::Connecting,
    }
}

/// What the page shows of a device besides its RPCs.
#[derive(Clone, PartialEq)]
struct Identity {
    name: Option<String>,
    serial: Option<String>,
    firmware: Option<String>,
    presence: Presence,
}

/// Sends what the proxy keeps of `device` as it changes, until the
/// connection ends, counts each drop of its link in `losses`, and says in
/// `log` what its metadata names it and that a table did not load. Each
/// table that loads has `firmware` read what the device runs.
async fn watch_device(
    device: Device,
    route: DeviceRoute,
    devices: Arc<Devices>,
    losses: Arc<Losses>,
    log: Arc<Logbook>,
    firmware: Arc<Firmware>,
) {
    let presence = device.presence_updates();
    let link = device.link_updates();
    // What the proxy last said of the device's link, whose URL the lines of
    // its drops and returns carry.
    let mut linked = None;
    let metadata = device.metadata_updates();
    let table = device.rpc_registry_updates();
    let settings = device.settings_updates();
    let mut identity = Identity {
        name: None,
        serial: None,
        firmware: None,
        presence: Presence::Connecting,
    };
    // What was last sent. A heartbeat or a repeated update that changes none
    // of it sends nothing.
    let mut sent: Option<Identity> = None;
    let mut sent_settings: Option<BTreeMap<String, serde_json::Value>> = None;
    // How many tables have loaded, which tells the lists of captures apart.
    let mut loads = 0;
    let at = route.to_string();
    loop {
        tokio::select! {
            // The presence is read first. As the connection ends, the store
            // marks the device lost and then ends every lane, so the loss is
            // read before another lane says it ended.
            biased;
            update = presence.recv_async() => match update {
                Ok(update) => {
                    let newer = std::iter::from_fn(|| link.try_recv().ok().flatten());
                    identity.presence = note_presence(update, route, newer, &mut linked, &losses);
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(_) => return,
            },
            update = link.recv_async() => match update {
                Ok(newest) => {
                    linked = Some(newest);
                    continue;
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(_) => return,
            },
            update = metadata.recv_async() => match update {
                Ok(MetadataState::Ready(snapshot)) => {
                    let view = snapshot.device();
                    let named = (
                        Some(view.name().to_string()),
                        Some(view.serial().to_string()),
                        Some(view.firmware().to_string()),
                    );
                    if named != (identity.name.clone(), identity.serial.clone(), identity.firmware.clone()) {
                        let text = format!("Found {}, serial {}, firmware {}", view.name(), view.serial(), view.firmware());
                        log.proxy(Level::Info, Some(route), text, None);
                    }
                    (identity.name, identity.serial, identity.firmware) = named;
                }
                Ok(_) | Err(RecvError::Lagged(_)) => continue,
                Err(_) => return,
            },
            update = table.recv_async() => {
                let update = match update {
                    Ok(update) => update,
                    Err(RecvError::Lagged(_)) => continue,
                    Err(_) => return,
                };
                match &update {
                    Ok(registry) => {
                        loads += 1;
                        tokio::spawn(list_captures(device.clone(), route, registry.clone(), devices.clone(), loads));
                        firmware.read_installed(device.clone(), route);
                    }
                    Err(error) => {
                        log.proxy(Level::Warn, Some(route), format!("The RPC table did not load. {error}"), None);
                    }
                }
                // The values read with the old table go with it, until the next read.
                if sent_settings.replace(BTreeMap::new()).is_some_and(|sent| !sent.is_empty()) {
                    devices.publish(route, Kind::Settings, Message::Settings { route: at.clone(), values: BTreeMap::new() });
                }
                devices.publish(route, Kind::Table, table_message(route, &update));
                continue;
            },
            update = settings.recv_async() => {
                let settings = match update {
                    Ok(settings) => settings,
                    Err(RecvError::Lagged(_)) => continue,
                    Err(_) => return,
                };
                let values = settings_values(settings.rpc_registry(), settings.iter());
                // A repeated announcement of the same values sends nothing.
                if sent_settings.as_ref() != Some(&values) {
                    sent_settings = Some(values.clone());
                    devices.publish(route, Kind::Settings, Message::Settings { route: at.clone(), values });
                }
                continue;
            },
        }
        if sent.as_ref() == Some(&identity) {
            continue;
        }
        sent = Some(identity.clone());
        let message = Message::Device {
            route: at.clone(),
            name: identity.name.clone(),
            serial: identity.serial.clone(),
            firmware: identity.firmware.clone(),
            presence: identity.presence,
        };
        devices.publish(route, Kind::Device, message);
    }
}

/// What every page is told of the RPC table of the device at `route`, as
/// `update` loaded it or says why it did not.
fn table_message(route: DeviceRoute, update: &RegistryState) -> Message {
    let table = match update {
        Ok(registry) => {
            let rpcs = registry
                .iter()
                .filter(|desc| !desc.is_protocol())
                .map(describe)
                .collect();
            TableState::Loaded(TableInfo { rpcs })
        }
        Err(error) => TableState::Failed {
            error: error.to_string(),
        },
    };
    Message::Table {
        route: route.to_string(),
        table,
    }
}

/// The settings values every page is told of, each of `values` by name as
/// its RPC replies, decoded by `table`. A value the table does not decode is
/// left out.
fn settings_values<'a>(
    table: &RpcRegistry,
    values: impl IntoIterator<Item = (&'a str, &'a [u8])>,
) -> BTreeMap<String, serde_json::Value> {
    (values.into_iter())
        .filter_map(|(name, reply)| {
            let value = table.find(name)?.meta.kind().decode_shortest(reply).ok()?;
            Some((name.to_string(), value.to_json()))
        })
        .collect()
}

/// Lists the capture RPCs of `table`, the table `device` loaded as its load
/// `load`, each with what its metadata says a capture of it is of. It asks
/// each for its metadata once, all at once, and takes no capture. A device
/// that does not answer one, or answers what is no metadata, has no label
/// for it.
async fn list_captures(
    device: Device,
    route: DeviceRoute,
    table: Arc<RpcRegistry>,
    devices: Arc<Devices>,
    load: u64,
) {
    let asked: Vec<_> = table
        .iter()
        .filter(|desc| !desc.is_protocol() && is_capture(desc))
        .map(|desc| {
            let metadata = Capture::new(&device, &desc.full_name).metadata();
            (desc.full_name.clone(), metadata)
        })
        .collect();
    let mut captures = Vec::new();
    for (rpc, metadata) in asked {
        let label = metadata.await.ok().map(|metadata| CaptureLabel {
            name: metadata.name,
            units: metadata.units,
            x_name: metadata.x_name,
            x_units: metadata.x_units,
        });
        captures.push(CaptureInfo { rpc, label });
    }
    devices.publish_captures(route, load, captures);
}

/// The page's view of one table entry.
pub(crate) fn describe(desc: &RpcDescriptor) -> RpcInfo {
    let access = match desc.meta.access() {
        RpcAccess::ReadWrite => vec![Access::Read, Access::Write],
        RpcAccess::ReadOnly => vec![Access::Read],
        RpcAccess::WriteOnly => vec![Access::Write],
        RpcAccess::Action => vec![],
    };
    let (class, range, length) = match desc.meta.text_type() {
        RpcValueType::Unit | RpcValueType::Raw { .. } => ("none", None, None),
        RpcValueType::Int { signed, size } => {
            let bits = u32::from(size) * 8 - u32::from(signed);
            let range = (-(i128::from(signed) << bits), (1i128 << bits) - 1);
            ("int", Some(range), None)
        }
        RpcValueType::Float { .. } => ("float", None, None),
        RpcValueType::Bool => ("bool", None, None),
        RpcValueType::String { max_len } => ("string", None, max_len.map(|len| len.get())),
        RpcValueType::Bytes { len } => ("bytes", None, len.map(|len| len.get())),
    };
    let kind = Some(desc.meta.type_str()).filter(|name| !name.is_empty());
    RpcInfo {
        name: desc.full_name.clone(),
        class,
        min: range.map(|(min, _)| min.to_string()),
        max: range.map(|(_, max)| max.to_string()),
        length,
        action: desc.channel() == Some(Channel::Action) || (access.is_empty() && kind.is_none()),
        kind,
        access,
        persistent: desc.meta.is_persistent(),
        readable: desc.is_readable(),
        channel: desc.channel().map(Channel::name),
    }
}

/// The page's view of what the device at `route` made of a call or a read.
fn outcome(route: &str, outcome: rpc::Outcome) -> Outcome {
    let route = route.to_string();
    match outcome {
        rpc::Outcome::Ok(value) => Outcome::Ok {
            route,
            value: value.map_or(serde_json::Value::Null, |value| value.to_json()),
        },
        rpc::Outcome::Error(error) => Outcome::Error {
            route,
            error: error.to_string(),
        },
        rpc::Outcome::Unknown(error) => Outcome::Unknown {
            route,
            error: error.to_string(),
        },
        rpc::Outcome::Refused(why) => Outcome::Refused {
            route,
            error: why.to_string(),
        },
        rpc::Outcome::Skipped => Outcome::Skipped { route },
    }
}

/// Calls `name` with `text`, or with no argument for none, on each of
/// `routes`, as [`DeviceTree::call_each`] does, and returns each device's
/// outcome in the order of `routes`. A route that does not parse refuses
/// the call, and every other route is skipped.
pub(crate) async fn call(
    tree: &DeviceTree,
    name: &str,
    text: Option<&str>,
    routes: &[String],
) -> CallReply {
    let parsed: Vec<Result<DeviceRoute, String>> = routes
        .iter()
        .map(|at| at.parse::<DeviceRoute>().map_err(|error| error.to_string()))
        .collect();
    if parsed.iter().any(Result::is_err) {
        let results = parsed
            .into_iter()
            .zip(routes)
            .map(|(parsed, at)| match parsed {
                Ok(_) => Outcome::Skipped { route: at.clone() },
                Err(error) => Outcome::Refused {
                    route: at.clone(),
                    error,
                },
            })
            .collect();
        return CallReply {
            sent: false,
            results,
        };
    }
    let parsed: Vec<DeviceRoute> = parsed.into_iter().filter_map(Result::ok).collect();
    let each = tree.call_each(name, text, &parsed).await;
    let sent = !each
        .iter()
        .any(|(_, outcome)| matches!(outcome, rpc::Outcome::Refused(_)));
    let results = each
        .into_iter()
        .zip(routes)
        .map(|((_, each), at)| outcome(at, each))
        .collect();
    CallReply { sent, results }
}

/// The device at `route` and its RPC table as the core holds it, waiting
/// for the table as a call does, or the status and the words of why there
/// is none.
pub(crate) async fn table(
    tree: &DeviceTree,
    route: &str,
) -> Result<(DeviceRoute, Arc<RpcRegistry>), (StatusCode, String)> {
    let parsed = (route.parse::<DeviceRoute>())
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("{route}: {error}")))?;
    let device = (tree.device(parsed))
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("{route}: {error}")))?;
    let table = (device.rpc_registry_async().await)
        .map_err(|why| (StatusCode::SERVICE_UNAVAILABLE, why.to_string()))?;
    Ok((parsed, table))
}

/// Reads each of `names` from the device at `route` at once, for the entries
/// no announcement keeps current, by the table the core holds of it, waiting
/// for it as a call does. A name the device cannot read is left out.
pub(crate) async fn read(
    tree: &DeviceTree,
    route: &str,
    names: &[String],
) -> Result<ReadReply, Refusal> {
    let (parsed, table) =
        (table(tree, route).await).map_err(|(status, why)| Refusal::new(status, why))?;
    let pending: Vec<_> = names
        .iter()
        .filter_map(|name| table.find(name).filter(|desc| desc.is_readable()))
        .map(|desc| {
            (
                desc.full_name.clone(),
                desc.meta.kind(),
                tree.submit(parsed, &desc.full_name, &[]),
            )
        })
        .collect();
    let mut values = BTreeMap::new();
    for (name, kind, reply) in pending {
        let each = rpc::Outcome::of_reply(kind, reply.await);
        values.insert(name, outcome(route, each));
    }
    Ok(ReadReply { values })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use twinleaf::device::rpc::RpcValue;

    /// Records what the pages are told of a table that did not load, and
    /// the reply to a call whose answer timed out, which the simulated
    /// device never leaves.
    pub(crate) fn record(recorded: &mut crate::recorded::Recorded) {
        let changed = Err(rpc::RpcRegistryError::TableChanged);
        recorded.frame(&table_message(DeviceRoute::root(), &changed).frame());
        let unknown = outcome("/", rpc::Outcome::Unknown(rpc::CallError::Timeout));
        let results = vec![unknown];
        let reply = CallReply {
            sent: true,
            results,
        };
        recorded.reply("call-unknown", axum::Json(reply));
    }

    /// What the pages are told of the settings the device at `/` of `tree`
    /// announced, with its session, `dev.session`, taken as 1.
    pub(crate) async fn settings(tree: &DeviceTree) -> Message {
        let device = tree.device(DeviceRoute::root()).unwrap();
        let settings = tokio::task::spawn_blocking(move || device.settings());
        let settings = settings.await.unwrap().unwrap();
        let session = 1u32.to_le_bytes();
        let values = (settings.iter()).map(|(name, reply)| match name {
            "dev.session" => (name, &session[..]),
            _ => (name, reply),
        });
        Message::Settings {
            route: DeviceRoute::root().to_string(),
            values: settings_values(settings.rpc_registry(), values),
        }
    }

    /// A loss that comes with a word of the link not read yet is a line of
    /// the drop with that word's URL, and the return's line has it too.
    #[test]
    fn a_loss_with_an_unread_link_carries_that_url() {
        let feed = Feed::new();
        let mut rx = feed.subscribe().1;
        let losses = Losses::new(feed.clone(), Logbook::new(feed));
        let root = DeviceRoute::root();
        let mut linked = None;
        let alive = twinleaf::device::Presence::Alive { session: None };
        note_presence(alive, root, [], &mut linked, &losses);
        let link = LinkState {
            url: "udp://127.0.0.1:1".to_string(),
        };
        let lost = twinleaf::device::Presence::Lost {
            since: std::time::Instant::now(),
            cause: Some(twinleaf::device::LostCause::Quiet),
        };
        let shown = note_presence(lost, root, [link], &mut linked, &losses);
        assert_eq!(shown, Presence::Lost);
        note_presence(alive, root, [], &mut linked, &losses);
        let said = crate::logbook::tests::heard(&mut rx);
        let said: Vec<_> = said.iter().filter(|line| line["type"] == "log").collect();
        assert_eq!(said.len(), 2, "{said:?}");
        assert_eq!(said[0]["loss"]["url"], "udp://127.0.0.1:1");
        assert_eq!(said[0]["loss"]["why"], "quiet");
        assert_eq!(said[1]["loss"]["url"], "udp://127.0.0.1:1");
    }

    /// An integer's least and greatest are the values its type encodes at
    /// each end, and its type encodes none past either end.
    #[test]
    fn an_integers_range_is_what_its_type_encodes() {
        let mut ints = 0;
        for name in RpcValueType::NAMES {
            let kind = RpcValueType::from_name(name).unwrap();
            let meta = rpc::RpcMeta::new(kind, rpc::RpcMetaFlags::READABLE);
            let info = describe(&RpcDescriptor::from_meta(meta.bits(), name.to_string()));
            let (Some(min), Some(max)) = (info.min, info.max) else {
                assert_ne!(info.class, "int", "{name}");
                continue;
            };
            ints += 1;
            let (min, max): (i128, i128) = (min.parse().unwrap(), max.parse().unwrap());
            for (value, takes) in [(min, true), (max, true), (min - 1, false), (max + 1, false)] {
                let encoded = kind.encode_text(&value.to_string());
                assert_eq!(encoded.is_ok(), takes, "{value} as {name}");
            }
        }
        assert_eq!(ints, 8);
    }

    /// The page reads an answer of no value as `null`, and each other
    /// outcome with the route it is of.
    #[test]
    fn an_outcome_is_the_pages_json_of_it() {
        let ok = outcome("/1", rpc::Outcome::Ok(Some(RpcValue::F64(0.5))));
        let value = serde_json::json!({ "state": "ok", "route": "/1", "value": 0.5 });
        assert_eq!(serde_json::to_value(ok).unwrap(), value);
        let none = serde_json::to_value(outcome("/", rpc::Outcome::Ok(None))).unwrap();
        assert_eq!(none["value"], serde_json::Value::Null);
        assert_eq!(
            outcome("/0", rpc::Outcome::Unknown(rpc::CallError::Timeout)),
            Outcome::Unknown {
                route: "/0".into(),
                error: "timed out waiting for the RPC reply".into()
            }
        );
        assert_eq!(
            outcome("/0", rpc::Outcome::Refused(rpc::Refusal::NoSuchRpc)),
            Outcome::Refused {
                route: "/0".into(),
                error: "it has no such RPC".into()
            }
        );
        assert_eq!(
            outcome("/0", rpc::Outcome::Skipped),
            Outcome::Skipped { route: "/0".into() }
        );
    }

    fn device(route: &str, name: &str) -> Message {
        Message::Device {
            route: route.into(),
            name: Some(name.into()),
            serial: None,
            firmware: None,
            presence: Presence::Alive,
        }
    }

    fn types(frames: &[Frame]) -> Vec<String> {
        frames
            .iter()
            .map(|f| match f {
                Frame::Text(text) => {
                    let v: serde_json::Value = serde_json::from_str(text).unwrap();
                    format!(
                        "{} {}",
                        v["type"].as_str().unwrap(),
                        v["route"].as_str().unwrap_or("")
                    )
                }
                Frame::Binary(_) => "binary".into(),
            })
            .collect()
    }

    #[test]
    fn a_snapshot_lists_the_routes_then_only_their_devices() {
        let devices = Devices::new(Feed::new());
        let (a, b): (DeviceRoute, DeviceRoute) = ("/0".parse().unwrap(), "/1".parse().unwrap());
        devices.publish_routes(&[a, b], None);
        devices.publish(a, Kind::Device, device("/0", "x"));
        devices.publish(b, Kind::Device, device("/1", "y"));
        assert_eq!(
            types(&devices.snapshot()),
            ["routes ", "device /0", "device /1"]
        );
        devices.publish_routes(&[a], None);
        assert_eq!(types(&devices.snapshot()), ["routes ", "device /0"]);
        let named = |route| match devices.newest(route, Kind::Device) {
            Some(Message::Device { name, .. }) => name,
            _ => None,
        };
        assert_eq!(named(a).as_deref(), Some("x"));
        assert_eq!(named(b).as_deref(), Some("y"), "kept, though not listed");
        assert!(devices.newest(a, Kind::Table).is_none());
    }

    #[test]
    fn a_device_listed_again_sends_its_newest_again_and_one_not_listed_sends_nothing() {
        let feed = Feed::new();
        let devices = Devices::new(feed.clone());
        let (a, b): (DeviceRoute, DeviceRoute) = ("/0".parse().unwrap(), "/1".parse().unwrap());
        devices.publish_routes(&[a, b], None);
        devices.publish(b, Kind::Device, device("/1", "y"));
        devices.publish_routes(&[a], None);
        let (_, mut rx) = feed.subscribe();
        devices.publish(b, Kind::Device, device("/1", "z"));
        devices.publish_routes(&[a, b], None);
        let heard: Vec<Frame> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|f| (*f).clone())
            .collect();
        assert_eq!(types(&heard), ["routes ", "device /1"]);
    }

    fn capture_state(route: &str, rpc: &str) -> Message {
        Message::Capture {
            route: route.into(),
            rpc: rpc.into(),
            state: crate::protocol::CaptureState::Idle,
            number: 1,
            repeat: None,
        }
    }

    fn listed(rpcs: &[&str]) -> Vec<CaptureInfo> {
        let info = |rpc: &&str| CaptureInfo {
            rpc: rpc.to_string(),
            label: None,
        };
        rpcs.iter().map(info).collect()
    }

    /// A snapshot holds a device's list of captures and where each stands
    /// after its table and settings. A list of an older table load than the
    /// one listed is dropped, and a capture the device no longer lists
    /// stands nowhere.
    #[test]
    fn a_devices_captures_follow_its_newest_table() {
        let devices = Devices::new(Feed::new());
        let root = DeviceRoute::root();
        devices.publish_routes(&[root], None);
        assert!(
            !devices.lists_capture(root, "fid.capture"),
            "nothing is listed yet"
        );
        devices.publish_captures(root, 2, listed(&["fid.capture", "phase.capture"]));
        assert!(devices.lists_capture(root, "fid.capture"));
        assert!(!devices.lists_capture(root, "fid.capture.rate"));
        for rpc in ["phase.capture", "fid.capture"] {
            devices.publish(root, Kind::Capture(rpc.into()), capture_state("/", rpc));
        }
        devices.publish(root, Kind::Device, device("/", "x"));
        assert_eq!(
            types(&devices.snapshot()),
            [
                "routes ",
                "device /",
                "captures /",
                "capture /",
                "capture /"
            ]
        );

        devices.publish_captures(root, 1, listed(&[]));
        assert_eq!(
            types(&devices.snapshot()).len(),
            5,
            "an older list is dropped"
        );

        devices.publish_captures(root, 3, listed(&["phase.capture"]));
        let Frame::Text(last) = devices.snapshot().pop().unwrap() else {
            panic!("a state is text")
        };
        assert!(last.contains("phase.capture"), "{last}");
        assert!(
            !devices.lists_capture(root, "fid.capture"),
            "the newest list has it not"
        );
        assert_eq!(
            types(&devices.snapshot()),
            ["routes ", "device /", "captures /", "capture /"]
        );
    }

    /// A capture read of an RPC labels it in its device's list, which goes
    /// out again only when the label is news.
    #[test]
    fn a_capture_read_labels_its_rpc_in_the_list() {
        let feed = Feed::new();
        let devices = Devices::new(feed.clone());
        let root = DeviceRoute::root();
        devices.publish_routes(&[root], None);
        devices.publish_captures(root, 1, listed(&["fid.capture", "phase.capture"]));
        let (_, mut rx) = feed.subscribe();
        let label = |name: &str| CaptureLabel {
            name: name.into(),
            units: "V".into(),
            x_name: "Time".into(),
            x_units: "s".into(),
        };

        devices.label_capture(root, "fid.capture", label("FID"));
        devices.label_capture(root, "fid.capture", label("FID"));
        devices.label_capture(root, "none.capture", label("None"));
        devices.label_capture("/3".parse().unwrap(), "fid.capture", label("Other"));

        let heard: Vec<Frame> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|frame| (*frame).clone())
            .collect();
        assert_eq!(types(&heard), ["captures /"]);
        let Frame::Text(text) = &heard[0] else {
            panic!("a list is text")
        };
        let list: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(list["captures"][0]["label"]["name"], "FID");
        assert_eq!(list["captures"][1]["label"], serde_json::Value::Null);
        assert_eq!(devices.snapshot().len(), 2, "the routes and the list");

        devices.label_capture(root, "fid.capture", label("Decay"));
        assert_eq!(types(&[(*rx.try_recv().unwrap()).clone()]), ["captures /"]);
    }

    /// A new table after values were sent leaves the device no values, which
    /// a page joining then is sent, until the next read with the new table.
    #[tokio::test]
    async fn a_replaced_table_holds_no_values_until_the_next_read() {
        use crate::firmware::tests::{release, Bench, Fake, OLD_BUILD};
        let catalog = Fake::listing(vec![release("2026-06-08", "000002")]);
        let mut bench = Bench::start(&["--build", OLD_BUILD], catalog, None, &["/"]).await;
        let (_, mut changes) = bench.live.snapshot(0).await.unwrap();
        let page = bench.live.page();
        page.check();
        bench.catalog("/", "checked").await;
        page.upgrade("/", crate::firmware::Wanted::Latest).unwrap();
        let mut told = Vec::new();
        let read = async {
            while !(told.contains(&"table") && told.last() == Some(&"values")) {
                let frame = match changes.recv().await {
                    Ok(frame) => frame,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                };
                let Frame::Text(text) = &*frame else { continue };
                let message: serde_json::Value = serde_json::from_str(text).unwrap();
                match (message["type"].as_str(), message["values"].as_object()) {
                    (Some("table"), _) => told.push("table"),
                    (Some("settings"), Some(none)) if none.is_empty() => told.push("none"),
                    (Some("settings"), _) => told.push("values"),
                    _ => {}
                }
            }
        };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(90), read).await;
        assert!(told.ends_with(&["none", "table", "values"]), "{told:?}");
    }

    #[test]
    fn devices_and_points_share_one_numbering() {
        let feed = Feed::new();
        let devices = Devices::new(feed.clone());
        let (start, mut rx) = feed.subscribe();
        devices.publish_routes(&["/".parse().unwrap()], None);
        feed.binary(|seq| seq.to_le_bytes().to_vec());
        let first = rx.try_recv().unwrap();
        let Frame::Text(text) = &*first else {
            panic!("routes are text")
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(text).unwrap()["seq"],
            start + 1
        );
        assert_eq!(
            *rx.try_recv().unwrap(),
            Frame::Binary((start + 2).to_le_bytes().to_vec())
        );
    }
}

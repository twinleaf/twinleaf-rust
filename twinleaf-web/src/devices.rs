//! The devices the hub's connection sees, and the RPCs the page calls on
//! them.
//!
//! A watcher per device follows what the proxy keeps of it, its presence,
//! metadata, RPC table and announced settings, and sends each to every page
//! as it changes. None of that asks the device anything. As a table loads,
//! the watcher lists the device's capture RPCs, and asks the device once
//! what each is a capture of. Each message is the newest value of its kind,
//! as the proxy keeps it, so [`Devices`] keeps the newest of each for a
//! page's snapshot, and sends nothing that changes nothing. `captures`
//! keeps where each capture RPC stands here too. A device as its metadata
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
use crate::logbook::Logbook;
use crate::loss::Losses;
use crate::protocol::{
    Access, CallReply, CaptureInfo, CaptureLabel, Frame, Level, Message, MountInfo, Outcome,
    Presence, ReadReply, Refusal, RpcInfo, TableInfo, TableState,
};
use axum::http::StatusCode;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use twinleaf::device::capture::Capture;
use twinleaf::device::rpc::{
    CallError, Channel, RpcAccess, RpcDescriptor, RpcMetaExt, RpcRegistry, RpcValueType,
    RpcValueTypeExt,
};
use twinleaf::device::MetadataState;
use twinleaf::{Connection, Device, DeviceRoute, DeviceTree, RecvError};

/// How long a call the page makes waits for each device's reply.
const CALL_TIMEOUT: Duration = Duration::from_secs(3);

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
}

/// The newest of what the watchers sent, and the feed it goes out on.
pub(crate) struct Devices {
    latest: Mutex<Latest>,
    /// Each device's RPC table as last loaded, which a call encodes by.
    tables: Mutex<HashMap<DeviceRoute, Arc<RpcRegistry>>>,
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
            tables: Mutex::new(HashMap::new()),
            feed,
        })
    }

    /// Lists `routes` as the devices there are, with what the hub mounted,
    /// and sends again the newest of each kind of every device listed anew.
    fn publish_routes(&self, routes: &[DeviceRoute], mounts: Option<Vec<MountInfo>>) {
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

    /// The RPC table of the device at `route` as last loaded.
    pub(crate) fn table(&self, route: DeviceRoute) -> Option<Arc<RpcRegistry>> {
        self.tables.lock().unwrap().get(&route).cloned()
    }

    fn set_table(&self, route: DeviceRoute, table: Arc<RpcRegistry>) {
        self.tables.lock().unwrap().insert(route, table);
    }
}

/// Starts the watchers of every device `connection` sees, on `runtime`, for
/// as long as the connection lasts. `mounts` is what the hub mounted, or
/// `None` for the proxy's default. Each drop of a device's link is counted
/// in `losses`, and `log` takes the watchers' lines.
pub(crate) fn watch(
    connection: &Connection,
    mounts: Option<Vec<MountInfo>>,
    devices: Arc<Devices>,
    losses: Arc<Losses>,
    log: Arc<Logbook>,
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
                    spawner.spawn(watch_device(device, route, devices, losses, log.clone()));
                }
            }
        }
    });
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
/// `log` what its metadata names it and that a table did not load.
async fn watch_device(
    device: Device,
    route: DeviceRoute,
    devices: Arc<Devices>,
    losses: Arc<Losses>,
    log: Arc<Logbook>,
) {
    let presence = device.presence_updates();
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
            update = presence.recv_async() => match update {
                Ok(update) => identity.presence = match update {
                    twinleaf::device::Presence::Alive { .. } => {
                        losses.link_up(route);
                        Presence::Alive
                    }
                    twinleaf::device::Presence::Lost { since } => {
                        losses.link_down(route, since);
                        Presence::Lost
                    }
                    _ => Presence::Connecting,
                },
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
                let table = match update {
                    Ok(Ok(registry)) => {
                        devices.set_table(route, registry.clone());
                        loads += 1;
                        tokio::spawn(list_captures(device.clone(), route, registry.clone(), devices.clone(), loads));
                        let rpcs = registry.iter().filter(|desc| shown(desc)).map(describe).collect();
                        TableState::Loaded(TableInfo { rpcs })
                    }
                    Ok(Err(error)) => {
                        log.proxy(Level::Warn, Some(route), format!("The RPC table did not load. {error}"), None);
                        TableState::Failed { error: error.to_string() }
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(_) => return,
                };
                devices.publish(route, Kind::Table, Message::Table { route: at.clone(), table });
                continue;
            },
            update = settings.recv_async() => {
                let settings = match update {
                    Ok(settings) => settings,
                    Err(RecvError::Lagged(_)) => continue,
                    Err(_) => return,
                };
                let table = settings.rpc_registry();
                let values: BTreeMap<String, serde_json::Value> = settings
                    .iter()
                    .filter_map(|(name, reply)| {
                        let value = table.find(name)?.meta.kind().decode_shortest(reply).ok()?;
                        Some((name.to_string(), value.to_json()))
                    })
                    .collect();
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
    let device = device.with_timeout(CALL_TIMEOUT);
    let asked: Vec<_> = table
        .iter()
        .filter(|desc| shown(desc) && is_capture(desc))
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

/// Whether the page lists `desc`. A protocol entry is how the host speaks
/// to the device, which no person calls.
fn shown(desc: &RpcDescriptor) -> bool {
    desc.channel() != Some(Channel::Protocol)
}

/// The page's view of one table entry.
fn describe(desc: &RpcDescriptor) -> RpcInfo {
    let access = match desc.meta.access() {
        RpcAccess::ReadWrite => vec![Access::Read, Access::Write],
        RpcAccess::ReadOnly => vec![Access::Read],
        RpcAccess::WriteOnly => vec![Access::Write],
        RpcAccess::Action => vec![],
    };
    RpcInfo {
        name: desc.full_name.clone(),
        kind: Some(desc.meta.type_str()).filter(|name| !name.is_empty()),
        access,
        persistent: desc.meta.is_persistent(),
        readable: desc.is_readable(),
        channel: desc.channel().map(Channel::name),
    }
}

/// What the device at `route` made of a call, whose reply is of `kind`, or
/// of no value for `None`. A call that timed out or lost its reply may or
/// may not have taken effect, so it is unknown, not failed.
fn outcome(route: &str, kind: Option<RpcValueType>, reply: Result<Vec<u8>, CallError>) -> Outcome {
    let route = route.to_string();
    match reply {
        Ok(bytes) => match kind.map(|kind| kind.decode_shortest(&bytes)) {
            Some(Ok(value)) => Outcome::Ok {
                route,
                value: value.to_json(),
            },
            Some(Err(error)) => Outcome::Error {
                route,
                error: error.to_string(),
            },
            None => Outcome::Ok {
                route,
                value: serde_json::Value::Null,
            },
        },
        Err(error @ (CallError::Timeout | CallError::ResponseLost)) => Outcome::Unknown {
            route,
            error: error.to_string(),
        },
        Err(error) => Outcome::Error {
            route,
            error: error.to_string(),
        },
    }
}

/// One device's part of a call, its argument encoded by its own table.
#[derive(Debug)]
struct Planned {
    at: String,
    route: DeviceRoute,
    /// What the reply decodes as.
    kind: RpcValueType,
    arg: Vec<u8>,
}

/// Encodes `text` as the argument of `name` for each of `routes`, or, when
/// any device cannot take it, an outcome for every route, the devices that
/// cannot refused and the others skipped.
fn plan(
    devices: &Devices,
    name: &str,
    text: &str,
    routes: &[String],
) -> Result<Vec<Planned>, Vec<Outcome>> {
    let encoded: Vec<Result<Planned, String>> = routes
        .iter()
        .map(|at| {
            let route = at
                .parse::<DeviceRoute>()
                .map_err(|error| error.to_string())?;
            let table = devices.table(route).ok_or("its RPC table has not loaded")?;
            let desc = table.find(name).ok_or("it has no such RPC")?;
            let arg = desc
                .meta
                .text_type()
                .encode_text(text)
                .map_err(|error| error.to_string())?;
            Ok(Planned {
                at: at.clone(),
                route,
                kind: desc.meta.kind(),
                arg,
            })
        })
        .collect();
    if encoded.iter().all(Result::is_ok) {
        return Ok(encoded.into_iter().map(Result::unwrap).collect());
    }
    Err(encoded
        .into_iter()
        .zip(routes)
        .map(|(planned, at)| match planned {
            Ok(_) => Outcome::Skipped { route: at.clone() },
            Err(error) => Outcome::Refused {
                route: at.clone(),
                error,
            },
        })
        .collect())
}

/// Calls `name` with `text` on each of `routes` at once, each argument
/// encoded by that device's own table, and returns each device's outcome in
/// the order of `routes`.
///
/// Nothing is sent unless every device takes the text. A device whose
/// table lacks the name, or whose type the text does not fit, refuses the
/// whole call, as a write that reaches only some devices cannot be taken
/// back.
pub(crate) async fn call(
    tree: &DeviceTree,
    devices: &Devices,
    name: &str,
    text: &str,
    routes: &[String],
) -> CallReply {
    let planned = match plan(devices, name, text, routes) {
        Ok(planned) => planned,
        Err(results) => {
            return CallReply {
                sent: false,
                results,
            }
        }
    };
    let tree = tree.with_timeout(CALL_TIMEOUT);
    let pending: Vec<_> = planned
        .into_iter()
        .map(|p| (p.at, p.kind, tree.submit(p.route, name, &p.arg)))
        .collect();
    let mut results = Vec::new();
    for (at, kind, reply) in pending {
        let kind = (kind != RpcValueType::Unit).then_some(kind);
        results.push(outcome(&at, kind, reply.await));
    }
    CallReply {
        sent: true,
        results,
    }
}

/// Reads each of `names` from the device at `route` at once, for the entries
/// no announcement keeps current. A name the device cannot read is left out.
pub(crate) async fn read(
    tree: &DeviceTree,
    devices: &Devices,
    route: &str,
    names: &[String],
) -> Result<ReadReply, Refusal> {
    let parsed = route
        .parse::<DeviceRoute>()
        .map_err(|error| Refusal::new(StatusCode::BAD_REQUEST, format!("{route}: {error}")))?;
    let table = devices.table(parsed).ok_or_else(|| {
        Refusal::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "its RPC table has not loaded",
        )
    })?;
    let tree = tree.with_timeout(CALL_TIMEOUT);
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
        values.insert(name, outcome(route, Some(kind), reply.await));
    }
    Ok(ReadReply { values })
}

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf::device::rpc::{RpcMeta, RpcMetaFlags};

    fn devices(tables: &[(&str, &[(&str, RpcValueType)])]) -> Arc<Devices> {
        let devices = Devices::new(Feed::new());
        let setting = RpcMetaFlags::READABLE | RpcMetaFlags::WRITABLE;
        for (route, entries) in tables {
            let rpcs = entries
                .iter()
                .map(|(name, kind)| {
                    RpcDescriptor::from_meta(
                        RpcMeta::typed(*kind, setting).bits(),
                        name.to_string(),
                    )
                })
                .collect();
            devices.set_table(route.parse().unwrap(), Arc::new(RpcRegistry::new(rpcs)));
        }
        devices
    }

    fn routes(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    const F32: RpcValueType = RpcValueType::Float { size: 4 };

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
        devices.publish_captures(root, 2, listed(&["fid.capture", "phase.capture"]));
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

    #[test]
    fn a_call_every_device_takes_is_planned_for_each() {
        let devices = devices(&[("/0", &[("gain", F32)]), ("/1", &[("gain", F32)])]);
        let planned = plan(&devices, "gain", "2.5", &routes(&["/0", "/1"])).unwrap();
        assert_eq!(
            planned.iter().map(|p| p.at.as_str()).collect::<Vec<_>>(),
            ["/0", "/1"]
        );
        assert_eq!(planned[0].arg, 2.5f32.to_le_bytes());
    }

    #[test]
    fn a_device_that_refuses_a_call_stops_it_for_every_device() {
        let devices = devices(&[("/0", &[("gain", F32)]), ("/1", &[("other", F32)])]);
        let results = plan(&devices, "gain", "2.5", &routes(&["/0", "/1", "/2"])).unwrap_err();
        assert_eq!(
            results,
            [
                Outcome::Skipped { route: "/0".into() },
                Outcome::Refused {
                    route: "/1".into(),
                    error: "it has no such RPC".into()
                },
                Outcome::Refused {
                    route: "/2".into(),
                    error: "its RPC table has not loaded".into()
                },
            ]
        );
    }

    #[test]
    fn a_text_one_type_cannot_hold_refuses_the_call() {
        let devices = devices(&[
            ("/0", &[("gain", F32)]),
            ("/1", &[("gain", RpcValueType::Bool)]),
        ]);
        let results = plan(&devices, "gain", "2.5", &routes(&["/0", "/1"])).unwrap_err();
        assert!(matches!(results[0], Outcome::Skipped { .. }));
        assert!(matches!(results[1], Outcome::Refused { .. }));
    }

    #[test]
    fn a_reply_that_never_came_is_unknown() {
        assert!(matches!(
            outcome("/", Some(F32), Err(CallError::Timeout)),
            Outcome::Unknown { .. }
        ));
        assert_eq!(
            outcome("/", Some(F32), Ok(0.5f32.to_le_bytes().to_vec())),
            Outcome::Ok {
                route: "/".into(),
                value: serde_json::json!(0.5)
            }
        );
    }
}

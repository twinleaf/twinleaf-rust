//! Finding the Twinleaf devices reachable from this host.
//!
//! Devices are found on local serial ports and, with the `mdns` feature, on
//! the network, where they advertise the `_twinleaf._tcp` or `_twinleaf._udp`
//! mDNS service. [`Discovery`] streams devices from both sources as they
//! appear and disappear. [`enumerate_serial`] returns a one-shot list of
//! serial devices, and [`query_name`] reads one device's name.

use super::connection::{Connection, DeviceTree};
use super::rpc::RpcReply;
use super::runtime::ProxyAddress;
use super::session::Session;
use super::stream::{NamedRoute, RecvError};
use crate::proto::DeviceRoute;
use crate::proxy::Purpose;
use crate::rt::{BoxFuture, Supervisor};
#[doc(inline)]
pub use crate::tio::transport::{enumerate_serial, DiscoveredDevice, PortInterface};
use std::collections::{BTreeSet, HashMap};
use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{StreamExt, StreamMap};

/// What a discovery session looks for and how far it probes.
#[derive(Debug, Clone, Copy)]
pub struct DiscoveryConfig {
    /// Include serial ports with unrecognized USB VID/PID. They are reported
    /// but not probed.
    pub include_unknown: bool,
    /// Browse the local network for mDNS-advertised devices.
    pub network: bool,
    /// Keep a probe connection to each recognized device to read its
    /// `dev.name` and follow the subdevices routed behind it.
    pub probe_names: bool,
    /// Prefer the UDP transport when a device advertises both TCP and UDP.
    pub prefer_udp: bool,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            include_unknown: false,
            network: true,
            probe_names: true,
            prefer_udp: false,
        }
    }
}

/// What a discovery session reports as it runs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DiscoveryEvent {
    /// A device appeared. `name` is set for mDNS instances (from the
    /// advertisement) and `None` for serial ports until a probe resolves it.
    Added(DiscoveredDevice),
    /// A device's `dev.name` was resolved after it was added.
    Named {
        /// The device's locator.
        url: String,
        /// Its `dev.name`.
        name: String,
    },
    /// The subdevices alive behind a device. It is reported after the first
    /// check of a probe connection and then whenever they change. Each event
    /// supersedes the previous one for `url`.
    Subdevices {
        /// The device's locator.
        url: String,
        /// The routes behind it, each with its name when known.
        routes: Vec<NamedRoute>,
    },
    /// A previously-added device went away (mDNS goodbye or serial unplug).
    Removed {
        /// The device's locator.
        url: String,
    },
    /// Network browsing could not start, and no network devices are
    /// reported. Serial discovery goes on.
    NetworkUnavailable {
        /// Why browsing could not start.
        reason: String,
    },
}

/// A running discovery session streaming [`DiscoveryEvent`]s from serial ports
/// and, when enabled, the network.
///
/// With [`DiscoveryConfig::probe_names`] set, each recognized device gets a
/// probe task of its own on the crate's runtime. The task keeps one probe
/// connection to the device through the proxy. Dropping the session stops
/// browsing and probing. It blocks until every task of the session has
/// ended, each probe with its connection dropped, for at most five seconds.
#[derive(Debug)]
pub struct Discovery {
    events: flume::Receiver<DiscoveryEvent>,
    /// Ends the session's scan and browse once dropped.
    lifelines: Vec<oneshot::Sender<Infallible>>,
    tasks: Supervisor,
}

impl Discovery {
    /// Starts a discovery session with `config`.
    ///
    /// Lists the serial ports before returning and reports them as the first
    /// events. Lists them again every two seconds on a task of the crate's
    /// runtime. Network browsing runs on another task, and each recognized
    /// device is probed on a task of its own. A failed probe is tried again
    /// after a delay that grows from half a second to four seconds. Each try
    /// may take longer to open than the one before, up to four seconds.
    pub fn start(config: DiscoveryConfig) -> Discovery {
        let include_unknown = config.include_unknown;
        let backend = Backend {
            list: Arc::new(move || enumerate_serial(include_unknown)),
            open: Arc::new(|url, timeout| Box::pin(probe(url, timeout))),
            period: REPROBE_PERIOD,
            retry: RETRY_FIRST,
        };
        Self::start_with(config, backend, &Supervisor::new())
    }

    /// Starts a discovery session as [`start`](Self::start) does, with
    /// `backend` to list ports and open probes, and its tasks on `tasks`.
    fn start_with(config: DiscoveryConfig, backend: Backend, tasks: &Supervisor) -> Discovery {
        let (tx, events) = flume::unbounded();

        let serial = (backend.list)();
        for device in &serial {
            let _ = tx.send(DiscoveryEvent::Added(device.clone()));
        }
        let probing = config.probe_names.then(|| backend.clone());

        let (scanning, alive) = oneshot::channel();
        let probes = Probes::new(probing.clone(), tasks);
        let scan = scan_serial(serial, backend, probes, tx.clone(), alive);
        tasks.spawn("twinleaf discovery serial", scan);

        #[cfg(feature = "mdns")]
        let browsing = config.network.then(|| {
            let (browsing, alive) = oneshot::channel();
            let probes = Probes::new(probing, tasks);
            let browse = browse_network(tx, probes, config.prefer_udp, alive);
            tasks.spawn("twinleaf discovery network", browse);
            browsing
        });
        #[cfg(not(feature = "mdns"))]
        let browsing = {
            let _ = config.network;
            None
        };

        Discovery {
            events,
            lifelines: [Some(scanning), browsing].into_iter().flatten().collect(),
            tasks: tasks.clone(),
        }
    }

    /// Returns the channel of discovery events. Drain it with `try_recv`, or
    /// wait on it with `recv` or `recv_async`.
    pub fn events(&self) -> &flume::Receiver<DiscoveryEvent> {
        &self.events
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        self.lifelines.clear();
        let tasks = self.tasks.clone();
        let ended = Supervisor::new().run("twinleaf discovery stop", async move {
            let waited = tokio::time::timeout(STOP_TIMEOUT, tasks.wait()).await;
            Ok(waited.is_ok())
        });
        if !matches!(ended, Ok(true)) {
            log::warn!("the discovery tasks did not end within {STOP_TIMEOUT:?}");
        }
    }
}

/// Lists the serial ports.
type Lister = Arc<dyn Fn() -> Vec<DiscoveredDevice> + Send + Sync>;

/// Opens a probe connection to a device's URL, waiting at most the time
/// given.
type Opener =
    Arc<dyn Fn(String, Duration) -> BoxFuture<'static, io::Result<Connection>> + Send + Sync>;

/// What a discovery session lists ports and opens probes with, and how often.
#[derive(Clone)]
struct Backend {
    /// Lists the serial ports. It may block, and runs on a blocking thread.
    list: Lister,
    /// Opens each probe connection.
    open: Opener,
    /// How often the serial ports are listed, and each probe connection
    /// checks its device's routes.
    period: Duration,
    /// How long a probe waits before it tries a failed open again. The wait
    /// doubles after each failure in a row, up to [`RETRY_MAX`].
    retry: Duration,
}

/// Opens a probe connection to `url` through the proxy [`Connection::open`]
/// uses. A TCP `url` on this machine is reached directly. A probe does not
/// make the device a choice for `auto`. Fails if the proxy does not open the
/// device within `timeout`.
async fn probe(url: String, timeout: Duration) -> io::Result<Connection> {
    Connection::open_at_async(&ProxyAddress::chosen(), &url, Purpose::Probe, timeout).await
}

/// Connects briefly to `url` and reads its `dev.name`, through the proxy
/// [`Connection::open`] uses. A TCP `url` on this machine is read directly.
/// The probe does not make the device a choice for `auto`.
///
/// Returns `None` if the port is busy, nothing answers at a proxy that
/// `TWINLEAF_PROXY` names, or the device does not answer within about twice
/// `timeout`.
pub fn query_name(url: &str, timeout: Duration) -> Option<String> {
    let proxy = ProxyAddress::chosen();
    let connection = Connection::open_at(&proxy, url, Purpose::Probe, timeout).ok()?;
    root_name(connection, timeout)
}

/// Reads the `dev.name` of `url` as [`query_name`] does, through the proxy at
/// `proxy_url`, such as one an app hosts on a port of its own.
///
/// Ignores `TWINLEAF_PROXY`. Opens even a TCP `url` on this machine through
/// that proxy, as a probe. Does not start a proxy, and returns `None` also
/// when nothing answers at `proxy_url`.
pub fn query_name_via(proxy_url: &str, url: &str, timeout: Duration) -> Option<String> {
    let proxy = ProxyAddress {
        url: proxy_url.to_string(),
        ensure: None,
    };
    let session = Session::open(&proxy, url, Purpose::Probe, timeout).ok()?;
    root_name(Connection::over(session), timeout)
}

/// Reads the `dev.name` of the root device of `connection`, giving up after
/// `timeout`.
fn root_name(connection: Connection, timeout: Duration) -> Option<String> {
    connection
        .device(DeviceRoute::root())
        .with_timeout(timeout)
        .get("dev.name")
        .ok()
}

/// How long a probe's first open may take, and each RPC of a probe.
const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// How long each check of a probe connection listens for routes. Devices
/// send heartbeats at 5 Hz.
const ROUTE_DISCOVERY_WINDOW: Duration = Duration::from_millis(300);

/// How often serial ports are listed, and each probe connection checks its
/// device's routes for subdevices that boot late.
const REPROBE_PERIOD: Duration = Duration::from_secs(2);

/// How long a probe first waits to try a failed open again.
const RETRY_FIRST: Duration = Duration::from_millis(500);

/// The longest a probe waits to try a failed open again, and the longest a
/// later try may take to open.
const RETRY_MAX: Duration = Duration::from_secs(4);

/// How long dropping a [`Discovery`] waits for its tasks to end.
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns `true` if a device found on a serial port is a Twinleaf device,
/// which discovery probes.
fn recognized(device: &DiscoveredDevice) -> bool {
    !matches!(device.interface, PortInterface::Unknown(..))
}

/// Runs [`Discovery`]'s serial scan until the session stops or nothing
/// receives events. Lists the ports every [`Backend::period`] on a blocking
/// thread, and reports each port that appears or goes. Probes each
/// recognized port with `probes`. Starts from the ports in `known`, which are
/// already reported.
async fn scan_serial(
    known: Vec<DiscoveredDevice>,
    backend: Backend,
    mut probes: Probes,
    tx: flume::Sender<DiscoveryEvent>,
    mut alive: oneshot::Receiver<Infallible>,
) {
    let tasks = probes.tasks.clone();
    let mut ports: Vec<String> = Vec::new();
    for device in known {
        if recognized(&device) {
            probes.start(&device.url);
        }
        ports.push(device.url);
    }
    let mut scans = tokio::time::interval_at(Instant::now() + backend.period, backend.period);
    scans.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let current = tokio::select! {
            _ = &mut alive => return,
            event = probes.report() => {
                if tx.send(event).is_err() {
                    return;
                }
                continue;
            }
            _ = scans.tick() => {
                let list = backend.list.clone();
                match tasks.spawn_blocking(move || list()).await {
                    Ok(current) => current,
                    Err(_) => return,
                }
            }
        };
        let gone: Vec<String> = ports
            .extract_if(.., |url| !current.iter().any(|device| &device.url == url))
            .collect();
        for url in gone {
            probes.retire(&url);
            if tx.send(DiscoveryEvent::Removed { url }).is_err() {
                return;
            }
        }
        for device in current {
            if ports.contains(&device.url) {
                continue;
            }
            ports.push(device.url.clone());
            let url = device.url.clone();
            let probe = recognized(&device);
            if tx.send(DiscoveryEvent::Added(device)).is_err() {
                return;
            }
            if probe {
                probes.start(&url);
            }
        }
    }
}

/// The probe tasks one owner runs, by URL, and the events they send it.
struct Probes {
    /// What the probes open with, or `None` when discovery reads no names.
    backend: Option<Backend>,
    /// Runs the owner and its probes.
    tasks: Supervisor,
    /// The events of each current probe, by URL. A probe ends once its
    /// events have no receiver.
    running: StreamMap<String, UnboundedReceiverStream<DiscoveryEvent>>,
}

impl Probes {
    /// Creates an empty set that probes with `backend` on `tasks`, or starts
    /// no probe with `None`. Dropping it ends its probes.
    fn new(backend: Option<Backend>, tasks: &Supervisor) -> Probes {
        Probes {
            backend,
            tasks: tasks.clone(),
            running: StreamMap::new(),
        }
    }

    /// Starts a probe of `url`, unless discovery reads no names. Retires any
    /// probe `url` already had.
    fn start(&mut self, url: &str) {
        let Some(backend) = self.backend.clone() else {
            return;
        };
        let (reports, reported) = mpsc::unbounded_channel();
        let unheard = reports.clone();
        let reporter = Reporter {
            url: url.to_string(),
            reports,
        };
        let probe = async move {
            tokio::select! {
                () = unheard.closed() => {}
                () = run_probe(backend, reporter) => {}
            }
        };
        self.tasks
            .spawn(format_args!("twinleaf probe {url}"), probe);
        let reported = UnboundedReceiverStream::new(reported);
        self.running.insert(url.to_string(), reported);
    }

    /// Retires the probe of `url`, if it has one. Nothing it reports is
    /// forwarded once this returns.
    fn retire(&mut self, url: &str) {
        self.running.remove(url);
    }

    /// Waits for the next report of a current probe, and returns its event.
    /// Never returns while no probe runs.
    async fn report(&mut self) -> DiscoveryEvent {
        match self.running.next().await {
            Some((_, event)) => event,
            None => std::future::pending().await,
        }
    }
}

/// Why a probe stopped watching its connection.
enum Interrupt {
    /// The connection ended. The probe opens another.
    Ended,
    /// Its owner retired it or ended. The probe ends.
    Stopped,
}

/// Where one probe sends its events.
struct Reporter {
    /// The device's URL.
    url: String,
    reports: mpsc::UnboundedSender<DiscoveryEvent>,
}

impl Reporter {
    /// Sends `event` to the probe's owner.
    ///
    /// Returns [`Interrupt::Stopped`] if its owner retired it or ended.
    fn send(&self, event: DiscoveryEvent) -> Result<(), Interrupt> {
        self.reports.send(event).map_err(|_| Interrupt::Stopped)
    }
}

/// Runs one probe until it is retired or its owner ends. Opens a probe
/// connection to the device and watches it. Opens another after a delay when
/// the open fails or the connection ends. Each failed open doubles the delay
/// and the time the next open may take, up to [`RETRY_MAX`].
async fn run_probe(backend: Backend, reporter: Reporter) {
    let url = &reporter.url;
    let mut delay = backend.retry;
    // A link that only probes use closes when its probe gives up, so a
    // device that is slow to answer gets more time at each try.
    let mut patience = PROBE_TIMEOUT;
    loop {
        match (backend.open)(url.clone(), patience).await {
            Ok(connection) => {
                (delay, patience) = (backend.retry, PROBE_TIMEOUT);
                match watch(&connection, backend.period, &reporter).await {
                    Interrupt::Ended => log::debug!("the probe connection to {url} ended"),
                    Interrupt::Stopped => return,
                }
            }
            Err(error) => log::debug!("could not probe {url}: {error}"),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(RETRY_MAX);
        patience = (patience * 2).min(RETRY_MAX);
    }
}

/// Watches a device over its probe `connection`. Checks the routes heard
/// every `period`, and reports the root's name and the subdevices to
/// `reporter` when they change. Returns why it stopped.
async fn watch(connection: &Connection, period: Duration, reporter: &Reporter) -> Interrupt {
    let root = DeviceRoute::root();
    let tree = connection.tree(root).with_timeout(PROBE_TIMEOUT);
    let mut names: HashMap<DeviceRoute, String> = HashMap::new();
    let mut named: Option<String> = None;
    let mut reported: Option<Vec<NamedRoute>> = None;
    loop {
        let mut heard = match heard_routes(connection, ROUTE_DISCOVERY_WINDOW).await {
            Ok(heard) => heard,
            Err(interrupt) => return interrupt,
        };
        // The root is what the connection opened, heard or not.
        heard.insert(root);
        names.retain(|route, _| heard.contains(route));
        for route in &heard {
            if names.contains_key(route) {
                continue;
            }
            if let Some(name) = device_name(&tree, *route).await {
                names.insert(*route, name);
            }
        }

        if let Some(name) = names
            .get(&root)
            .filter(|&name| named.as_ref() != Some(name))
        {
            let event = DiscoveryEvent::Named {
                url: reporter.url.clone(),
                name: name.clone(),
            };
            if let Err(interrupt) = reporter.send(event) {
                return interrupt;
            }
            named = Some(name.clone());
        }
        let routes: Vec<NamedRoute> = heard
            .iter()
            .filter(|route| !route.is_empty())
            .map(|route| NamedRoute {
                route: *route,
                name: names.get(route).cloned(),
            })
            .collect();
        if reported
            .as_ref()
            .is_none_or(|last| !same_routes(last, &routes))
        {
            let event = DiscoveryEvent::Subdevices {
                url: reporter.url.clone(),
                routes: routes.clone(),
            };
            if let Err(interrupt) = reporter.send(event) {
                return interrupt;
            }
            reported = Some(routes);
        }

        tokio::time::sleep(period).await;
    }
}

/// Listens to `connection` for `window` and returns the routes it heard
/// from.
///
/// Returns [`Interrupt::Ended`] if the connection ends first.
async fn heard_routes(
    connection: &Connection,
    window: Duration,
) -> Result<BTreeSet<DeviceRoute>, Interrupt> {
    let packets = connection.packets();
    let deadline = Instant::now() + window;
    let mut heard = BTreeSet::new();
    loop {
        let received = tokio::select! {
            received = packets.recv_async() => received,
            () = tokio::time::sleep_until(deadline) => return Ok(heard),
        };
        match received {
            Ok(packet) if packet.link_status().is_none() => {
                heard.insert(packet.route());
            }
            Ok(_) | Err(RecvError::Lagged(_) | RecvError::Timeout) => {}
            Err(RecvError::Disconnected | RecvError::Stopped) => return Err(Interrupt::Ended),
        }
    }
}

/// Returns the trimmed `dev.name` of the device at `route`, or `None` if it
/// does not answer or its name is empty.
async fn device_name(tree: &DeviceTree, route: DeviceRoute) -> Option<String> {
    let reply = tree.submit(route, "dev.name", &[]).await.ok()?;
    let name = String::decode_reply(&reply).ok()?;
    let name = name.trim();
    (!name.is_empty()).then(|| name.to_string())
}

/// Returns `true` if `a` and `b` list the same routes with the same names.
fn same_routes(a: &[NamedRoute], b: &[NamedRoute]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(a, b)| a.route == b.route && a.name == b.name)
}

/// Service types Twinleaf network devices advertise. TCP is preferred when
/// both appear. Firmware built before 2026-06-15 advertises `_tio`.
#[cfg(feature = "mdns")]
const TWINLEAF_MDNS_SERVICES: [&str; 4] = [
    "_twinleaf._tcp.local.",
    "_twinleaf._udp.local.",
    "_tio._tcp.local.",
    "_tio._udp.local.",
];

/// The records one mDNS instance advertises, keyed by fullname, and the URL
/// announced for it.
#[cfg(feature = "mdns")]
#[derive(Default)]
struct Instance {
    // Keyed by fullname, so one device under several service types stays
    // distinct.
    urls: HashMap<String, String>,
    announced: Option<String>,
}

#[cfg(feature = "mdns")]
impl Instance {
    /// Returns this instance's URL on the `preferred` transport if one is
    /// advertised, or else the first URL in sorted order.
    fn preferred_url(&self, preferred: &str) -> Option<&str> {
        let mut urls: Vec<&str> = self.urls.values().map(String::as_str).collect();
        urls.sort_unstable();
        urls.iter()
            .find(|url| url.starts_with(preferred))
            .or(urls.first())
            .copied()
    }
}

/// Reports that network browsing could not start, as an event and in the
/// log.
#[cfg(feature = "mdns")]
fn network_unavailable(tx: &flume::Sender<DiscoveryEvent>, reason: String) {
    log::warn!("network discovery unavailable: {reason}");
    let _ = tx.send(DiscoveryEvent::NetworkUnavailable { reason });
}

/// Updates the URL announced for `label` from its current records. When it
/// changes, re-announces the instance and moves its probe in `probes` to
/// the new URL.
///
/// Returns an error if nothing receives events.
#[cfg(feature = "mdns")]
fn sync_instance(
    instances: &mut HashMap<String, Instance>,
    label: &str,
    preferred: &str,
    tx: &flume::Sender<DiscoveryEvent>,
    probes: &mut Probes,
) -> Result<(), ()> {
    let Some(instance) = instances.get_mut(label) else {
        return Ok(());
    };
    let wanted = instance.preferred_url(preferred).map(str::to_string);
    if wanted == instance.announced {
        return Ok(());
    }
    if let Some(url) = instance.announced.take() {
        probes.retire(&url);
        tx.send(DiscoveryEvent::Removed { url }).map_err(|_| ())?;
    }
    let Some(url) = wanted else {
        instances.remove(label);
        return Ok(());
    };
    instance.announced = Some(url.clone());
    let device = DiscoveredDevice {
        url: url.clone(),
        interface: PortInterface::Network,
        name: Some(label.to_string()),
    };
    tx.send(DiscoveryEvent::Added(device)).map_err(|_| ())?;
    probes.start(&url);
    Ok(())
}

/// Runs [`Discovery`]'s network browse until the session stops or nothing
/// receives events. Starts browsing on a blocking thread, announces
/// instances as they resolve, move, or leave, and probes each with `probes`.
#[cfg(feature = "mdns")]
async fn browse_network(
    tx: flume::Sender<DiscoveryEvent>,
    mut probes: Probes,
    prefer_udp: bool,
    mut alive: oneshot::Receiver<Infallible>,
) {
    use mdns_sd::{ServiceDaemon, ServiceEvent};

    let tasks = probes.tasks.clone();

    let started = tasks.spawn_blocking(|| {
        let daemon = ServiceDaemon::new().map_err(|error| format!("mDNS daemon: {error}"))?;
        let browser = Browser(daemon);
        let receivers: Vec<_> = TWINLEAF_MDNS_SERVICES
            .iter()
            .filter_map(|service| browser.0.browse(service).ok())
            .collect();
        match receivers.is_empty() {
            true => Err("no mDNS service type could be browsed".to_string()),
            false => Ok((browser, receivers)),
        }
    });
    let (browser, receivers) = match started.await {
        Ok(Ok(browsing)) => browsing,
        Ok(Err(reason)) => return network_unavailable(&tx, reason),
        Err(error) => return network_unavailable(&tx, error.to_string()),
    };
    // The daemon blocks on a full listener, and a probe may wait on the
    // daemon to resolve its device. So a task drains each receiver into an
    // unbounded queue until discovery stops.
    let (found, mut events) = mpsc::unbounded_channel();
    for receiver in receivers {
        let found = found.clone();
        tasks.spawn("twinleaf discovery mdns", async move {
            while let Ok(event) = receiver.recv_async().await {
                if found.send(event).is_err() {
                    break;
                }
            }
        });
    }
    drop(found);

    let mut instances: HashMap<String, Instance> = HashMap::new();
    let preferred = if prefer_udp { "udp" } else { "tcp" };
    loop {
        let event = tokio::select! {
            _ = &mut alive => break,
            event = probes.report() => {
                if tx.send(event).is_err() {
                    break;
                }
                continue;
            }
            Some(event) = events.recv() => event,
        };
        let label = match event {
            ServiceEvent::ServiceResolved(info) => {
                let Some(host) = url_host(&info) else {
                    continue;
                };
                let scheme = if info.ty_domain.contains("._udp.") {
                    "udp"
                } else {
                    "tcp"
                };
                let url = format!("{}://{}:{}", scheme, host, info.get_port());
                let label = instance_label(info.get_fullname());
                instances
                    .entry(label.clone())
                    .or_default()
                    .urls
                    .insert(info.get_fullname().to_string(), url);
                label
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                let label = instance_label(&fullname);
                if let Some(instance) = instances.get_mut(&label) {
                    instance.urls.remove(&fullname);
                }
                label
            }
            _ => continue,
        };
        if sync_instance(&mut instances, &label, preferred, &tx, &mut probes).is_err() {
            break;
        }
    }
    tasks.spawn_blocking(move || drop(browser));
}

/// The mDNS daemon of one discovery session's browse. Dropping it shuts the
/// daemon down, which stops the browsing.
#[cfg(feature = "mdns")]
struct Browser(mdns_sd::ServiceDaemon);

#[cfg(feature = "mdns")]
impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.0.shutdown();
    }
}

/// Returns the instance name in a resolved fullname, without its service
/// type, such as `Twinleaf COMM-ETH R7 (COMM-ETH.00019)`.
#[cfg(feature = "mdns")]
fn instance_label(fullname: &str) -> String {
    for service in TWINLEAF_MDNS_SERVICES {
        if let Some(label) = fullname.strip_suffix(service) {
            return label.trim_end_matches('.').to_string();
        }
    }
    fullname.to_string()
}

/// Returns the host for a discovered device's URL. That is its advertised
/// hostname, or else its lowest routable IPv4 address.
// The hostname stays the same when the device's address changes, and a
// person can read it. Each open of the URL resolves it again.
#[cfg(feature = "mdns")]
fn url_host(info: &mdns_sd::ResolvedService) -> Option<String> {
    let hostname = info.get_hostname().trim_end_matches('.');
    if !hostname.is_empty() {
        return Some(hostname.to_string());
    }

    let mut v4: Vec<_> = info.get_addresses_v4().into_iter().collect();
    v4.sort();
    let ip = v4
        .iter()
        .find(|ip| !ip.is_loopback() && !ip.is_link_local())
        .or_else(|| v4.first())?;
    Some(ip.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::fixtures::{self, FakeDevice, ENVIRONMENT, WAIT};
    use crate::device::session::TestProxy;
    use crate::proto::rpc::Method;
    use crate::proxy::fixtures::{assert_took, PAUSED_WAIT};
    use crate::proxy::Proxy;
    use crate::tio::packet::Payload;
    use crate::tio::Packet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Mutex, MutexGuard, PoisonError};
    use std::thread;
    use std::time::Instant;

    /// Locks `mutex`, even if a thread panicked while holding it. It suits locks
    /// that guard no change a panic could leave half made.
    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A URL no test device listens at, which only a test's own opener
    /// handles.
    const SLOW: &str = "serial:///dev/twinleaf-test-slow";

    /// How often the tests' discovery lists ports and checks routes.
    const PERIOD: Duration = Duration::from_millis(100);

    /// A port at `url` behind `interface`.
    fn port(url: &str, interface: PortInterface) -> DiscoveredDevice {
        DiscoveredDevice {
            url: url.to_string(),
            interface,
            name: None,
        }
    }

    /// A port with a Twinleaf device behind it.
    fn twinleaf(url: &str) -> DiscoveredDevice {
        port(url, PortInterface::STM32)
    }

    /// The ports a test lists, which it may change as discovery runs.
    type Ports = Arc<Mutex<Vec<DiscoveredDevice>>>;

    /// A backend that lists `ports` and opens each probe through `proxy`,
    /// local TCP URLs included.
    fn backend(proxy: &Proxy, ports: &Ports) -> Backend {
        let address = ProxyAddress {
            url: proxy.endpoint(),
            ensure: None,
        };
        let ports = ports.clone();
        Backend {
            list: Arc::new(move || lock(&ports).clone()),
            open: Arc::new(move |url, _| {
                let address = address.clone();
                Box::pin(async move {
                    let opened = Session::open_async(&address, &url, Purpose::Probe, WAIT).await;
                    opened.map(Connection::over)
                })
            }),
            period: PERIOD,
            retry: Duration::from_millis(50),
        }
    }

    /// Starts a discovery whose tasks run on the crate's runtime.
    fn start(config: DiscoveryConfig, backend: Backend) -> Discovery {
        Discovery::start_with(config, backend, &Supervisor::new())
    }

    /// The configuration of a discovery that lists serial ports alone.
    fn serial_only() -> DiscoveryConfig {
        DiscoveryConfig {
            network: false,
            ..DiscoveryConfig::default()
        }
    }

    /// Returns the first event `pick` accepts, skipping others. Panics if
    /// none arrives within [`WAIT`].
    fn expect<T>(discovery: &Discovery, mut pick: impl FnMut(DiscoveryEvent) -> Option<T>) -> T {
        let deadline = Instant::now() + WAIT;
        loop {
            match discovery.events().recv_deadline(deadline) {
                Ok(event) => {
                    if let Some(found) = pick(event) {
                        return found;
                    }
                }
                Err(error) => panic!("no expected event arrived: {error}"),
            }
        }
    }

    /// Returns the name discovery reports for `url`.
    fn expect_named(discovery: &Discovery, url: &str) -> String {
        expect(discovery, |event| match event {
            DiscoveryEvent::Named { url: named, name } if named == url => Some(name),
            _ => None,
        })
    }

    /// Returns the subdevices discovery reports for `url`, by route.
    fn expect_subdevices(discovery: &Discovery, url: &str) -> Vec<(String, Option<String>)> {
        expect(discovery, |event| match event {
            DiscoveryEvent::Subdevices { url: at, routes } if at == url => Some(
                routes
                    .into_iter()
                    .map(|named| (named.route.to_string(), named.name))
                    .collect(),
            ),
            _ => None,
        })
    }

    /// Blocks until `done` holds, polling. Panics if it does not within
    /// `timeout`.
    fn wait_until(timeout: Duration, what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while !done() {
            assert!(Instant::now() < deadline, "{what} within {timeout:?}");
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Each device has a probe task of its own, so a device that is slow to
    /// open delays no other.
    #[test]
    fn devices_are_probed_in_parallel() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();
        let ports = Arc::new(Mutex::new(vec![twinleaf(SLOW), twinleaf(&device.url)]));
        let mut backend = backend(&proxy, &ports);
        let (release, held) = flume::bounded::<()>(0);
        let slow_done = Arc::new(AtomicBool::new(false));
        let through = backend.open.clone();
        let done = slow_done.clone();
        backend.open = Arc::new(move |url, timeout| {
            if url != SLOW {
                return through(url, timeout);
            }
            let (held, done) = (held.clone(), done.clone());
            Box::pin(async move {
                let _ = tokio::time::timeout(WAIT, held.recv_async()).await;
                done.store(true, Ordering::SeqCst);
                Err(io::Error::other("the slow device never opened"))
            })
        });
        let discovery = start(serial_only(), backend);

        assert_eq!(expect_named(&discovery, &device.url), "FAKE");

        assert!(!slow_done.load(Ordering::SeqCst), "the probes ran in turn");
        drop(release);
        drop(discovery);
    }

    /// A port with an unrecognized USB interface is reported but never
    /// opened.
    #[test]
    fn an_unrecognized_port_is_never_opened() {
        let unknown = FakeDevice::start();
        let known = FakeDevice::start();
        let proxy = fixtures::proxy();
        let ports = Arc::new(Mutex::new(vec![
            port(&unknown.url, PortInterface::Unknown(0x1234, 0x5678)),
            twinleaf(&known.url),
        ]));
        let mut backend = backend(&proxy, &ports);
        let opened = Arc::new(Mutex::new(Vec::new()));
        let through = backend.open.clone();
        let record = opened.clone();
        backend.open = Arc::new(move |url, timeout| {
            lock(&record).push(url.clone());
            through(url, timeout)
        });
        let config = DiscoveryConfig {
            include_unknown: true,
            ..serial_only()
        };
        let discovery = start(config, backend);

        expect(&discovery, |event| match event {
            DiscoveryEvent::Added(device) if device.url == unknown.url => Some(()),
            _ => None,
        });
        assert_eq!(expect_named(&discovery, &known.url), "FAKE");
        // Several more listings of the ports.
        thread::sleep(PERIOD * 4);
        drop(discovery);

        assert_eq!(*lock(&opened), std::slice::from_ref(&known.url));
        assert_eq!(unknown.accepted(), 0);
    }

    /// A probe opened by discovery alone closes when discovery stops, and
    /// the proxy closes the device at once.
    #[test]
    fn a_probe_only_link_is_released_when_discovery_stops() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();
        let ports = Arc::new(Mutex::new(vec![twinleaf(&device.url)]));
        let discovery = start(serial_only(), backend(&proxy, &ports));
        assert_eq!(expect_named(&discovery, &device.url), "FAKE");
        assert_eq!(device.connections(), 1);

        drop(discovery);

        // Well within the ten seconds a device a client opened stays open.
        wait_until(Duration::from_secs(2), "the device closed", || {
            device.connections() == 0
        });
        assert!(proxy.clients().is_empty());
    }

    /// A serial scan that ends while discovery runs on closes its probe
    /// connections.
    #[test]
    fn a_scan_that_ends_closes_its_probes() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();
        let ports = Arc::new(Mutex::new(vec![twinleaf(&device.url)]));
        let mut backend = backend(&proxy, &ports);
        let failing = Arc::new(AtomicBool::new(false));
        let (list, fail) = (backend.list.clone(), failing.clone());
        backend.list = Arc::new(move || {
            if fail.load(Ordering::SeqCst) {
                panic!("a test failure to list the ports");
            }
            list()
        });
        let discovery = start(serial_only(), backend);
        assert_eq!(expect_named(&discovery, &device.url), "FAKE");
        assert_eq!(device.connections(), 1);

        failing.store(true, Ordering::SeqCst);

        wait_until(Duration::from_secs(2), "the probe closed", || {
            device.connections() == 0
        });
        drop(discovery);
    }

    /// A port that goes away is reported removed, and its probe connection
    /// closes.
    #[test]
    fn a_port_that_goes_away_is_removed_and_its_probe_closes() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();
        let ports = Arc::new(Mutex::new(vec![twinleaf(&device.url)]));
        let discovery = start(serial_only(), backend(&proxy, &ports));
        assert_eq!(expect_named(&discovery, &device.url), "FAKE");
        assert_eq!(expect_subdevices(&discovery, &device.url), []);

        lock(&ports).clear();

        expect(&discovery, |event| match event {
            DiscoveryEvent::Removed { url } if url == device.url => Some(()),
            _ => None,
        });
        wait_until(Duration::from_secs(2), "the device closed", || {
            device.connections() == 0
        });
        let late = discovery.events().try_recv();
        assert!(late.is_err(), "an event came after the removal: {late:?}");
    }

    /// A probe that fails is tried again until it opens, and each try may
    /// take longer to open than the one before.
    #[test]
    fn a_failed_probe_is_tried_again_with_more_time() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();
        let ports = Arc::new(Mutex::new(vec![twinleaf(&device.url)]));
        let mut backend = backend(&proxy, &ports);
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let through = backend.open.clone();
        let counted = attempts.clone();
        backend.open = Arc::new(move |url, timeout| {
            let mut attempts = lock(&counted);
            attempts.push(timeout);
            if attempts.len() <= 2 {
                return Box::pin(async { Err(io::Error::other("the device is not up yet")) });
            }
            through(url, timeout)
        });
        let discovery = start(serial_only(), backend);

        assert_eq!(expect_named(&discovery, &device.url), "FAKE");

        drop(discovery);
        let first = PROBE_TIMEOUT;
        assert_eq!(*lock(&attempts), [first, first * 2, first * 4]);
    }

    /// The probe connection stays open. A subdevice that boots late is
    /// found over it, and subdevices are reported only when they change.
    #[test]
    fn a_late_subdevice_is_found_over_the_same_connection() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();
        let ports = Arc::new(Mutex::new(vec![twinleaf(&device.url)]));
        let discovery = start(serial_only(), backend(&proxy, &ports));
        assert_eq!(expect_subdevices(&discovery, &device.url), []);
        // Several more checks of the routes, none of them reported.
        thread::sleep(PERIOD * 6);
        while let Ok(event) = discovery.events().try_recv() {
            assert!(
                !matches!(event, DiscoveryEvent::Subdevices { .. }),
                "unchanged subdevices were reported again: {event:?}"
            );
        }

        let subdevice: DeviceRoute = "/1".parse().unwrap();
        let booted = AtomicBool::new(true);
        let found = thread::scope(|scope| {
            scope.spawn(|| {
                while booted.load(Ordering::SeqCst) {
                    device.send(Packet::heartbeat(subdevice));
                    thread::sleep(Duration::from_millis(50));
                }
            });
            let found = expect_subdevices(&discovery, &device.url);
            booted.store(false, Ordering::SeqCst);
            found
        });

        assert_eq!(found, [("/1".to_string(), Some("FAKE".to_string()))]);
        assert_eq!(device.accepted(), 1);
    }

    /// A TCP URL on this machine names what serves the device already, so
    /// the name is read from there, whatever proxy `TWINLEAF_PROXY` names.
    #[test]
    fn a_name_at_a_local_tcp_url_is_read_directly() {
        let _environment = ENVIRONMENT.lock().unwrap_or_else(PoisonError::into_inner);
        let device = FakeDevice::start();
        let closed = fixtures::ClosedPort::new();
        let silent = closed.url();

        std::env::set_var("TWINLEAF_PROXY", &silent);
        let name = query_name(&device.url, WAIT);

        std::env::remove_var("TWINLEAF_PROXY");
        assert_eq!(name.as_deref(), Some("FAKE"));
    }

    /// A name is read through the proxy named, as a probe, so the device is
    /// open there but no choice for `auto`.
    #[test]
    fn query_name_via_reads_a_name_through_that_proxy_as_a_probe() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();

        let name = query_name_via(&proxy.endpoint(), &device.url, WAIT);

        assert_eq!(name.as_deref(), Some("FAKE"));
        assert_eq!(device.accepted(), 1);
        // With no device attached, auto finds the probed one no choice.
        if fixtures::attached() == 0 {
            let auto = fixtures::open_through(&proxy, "auto");
            assert!(auto.is_err(), "a probe made the device a choice for auto");
        }
    }

    /// Nothing answering at the proxy `TWINLEAF_PROXY` names names nothing,
    /// and no proxy is started in its place.
    #[test]
    fn no_name_is_read_where_the_named_proxy_does_not_answer() {
        let _environment = ENVIRONMENT.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("TWINLEAF_TIO", dir.path().join("no-such-tio"));
        std::env::set_var("TWINLEAF_RUNTIME_DIR", dir.path());
        let closed = fixtures::ClosedPort::new();
        let silent = closed.url();
        std::env::set_var("TWINLEAF_PROXY", &silent);

        let name = query_name("tcp://192.0.2.1", Duration::from_millis(500));

        for variable in ["TWINLEAF_TIO", "TWINLEAF_RUNTIME_DIR", "TWINLEAF_PROXY"] {
            std::env::remove_var(variable);
        }
        assert_eq!(name, None);
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "a proxy was spawned, and logs in {}",
            dir.path().display()
        );
    }

    /// A serial port no test opens.
    const FIRST: &str = "serial:///dev/twinleaf-test-first";

    /// Another serial port no test opens.
    const SECOND: &str = "serial:///dev/twinleaf-test-second";

    /// A backend for a paused clock that lists `ports` and opens probes with
    /// `open`, at the period and retry discovery runs with.
    fn paused_backend(ports: &Ports, open: Opener) -> Backend {
        let ports = ports.clone();
        Backend {
            list: Arc::new(move || lock(&ports).clone()),
            open,
            period: REPROBE_PERIOD,
            retry: RETRY_FIRST,
        }
    }

    /// An opener whose every open fails.
    fn never_opens() -> Opener {
        Arc::new(|_, _| Box::pin(async { Err(io::Error::other("the device is not up yet")) }))
    }

    /// Returns the next event of `discovery`. Panics if none arrives within
    /// [`PAUSED_WAIT`].
    async fn next_event(discovery: &Discovery) -> DiscoveryEvent {
        let next = tokio::time::timeout(PAUSED_WAIT, discovery.events().recv_async()).await;
        next.expect("an event arrives").expect("discovery runs")
    }

    /// Returns the method name and id of the next call `proxy` receives.
    /// Panics if none arrives within [`PAUSED_WAIT`].
    async fn next_call(proxy: &TestProxy) -> (String, u16) {
        let next = tokio::time::timeout(PAUSED_WAIT, proxy.sent.recv_async()).await;
        let packet = next.expect("a call arrives").expect("the session runs");
        let Payload::RpcRequest(request) = packet.payload() else {
            panic!("expected an RPC request, got {:?}", packet.payload());
        };
        let Method::ByName(name) = request.method else {
            panic!("a probe calls by name");
        };
        (
            String::from_utf8_lossy(name).into_owned(),
            request.id.value(),
        )
    }

    /// Stops `discovery` and waits for its tasks to end. Unlike its drop, it
    /// does not block the test's runtime.
    async fn stop(mut discovery: Discovery) {
        discovery.lifelines.clear();
        discovery.tasks.wait().await;
    }

    /// The serial ports are listed again every period, and a port that
    /// appears or goes is reported at the listing that sees it.
    #[tokio::test(start_paused = true)]
    async fn the_serial_ports_are_listed_every_period() {
        let ports = Arc::new(Mutex::new(vec![twinleaf(FIRST)]));
        let config = DiscoveryConfig {
            probe_names: false,
            ..serial_only()
        };
        let backend = paused_backend(&ports, never_opens());
        let started = tokio::time::Instant::now();
        let discovery = Discovery::start_with(config, backend, &Supervisor::current());
        let event = next_event(&discovery).await;
        assert!(matches!(&event, DiscoveryEvent::Added(device) if device.url == FIRST));

        lock(&ports).push(twinleaf(SECOND));
        let event = next_event(&discovery).await;
        assert!(matches!(&event, DiscoveryEvent::Added(device) if device.url == SECOND));
        assert_took(started.elapsed(), REPROBE_PERIOD, "the second listing");

        lock(&ports).remove(0);
        let event = next_event(&discovery).await;
        assert!(matches!(&event, DiscoveryEvent::Removed { url } if url == FIRST));
        assert_took(started.elapsed(), REPROBE_PERIOD * 2, "the third listing");
        stop(discovery).await;
    }

    /// A probe that keeps failing waits twice as long before each try, from
    /// the first retry delay up to the longest. Each try may take twice as
    /// long to open, from the probe timeout up to the longest.
    #[tokio::test(start_paused = true)]
    async fn a_failing_probe_backs_off_up_to_the_longest_delay() {
        let ports = Arc::new(Mutex::new(vec![twinleaf(FIRST)]));
        let (tried, tries) = flume::unbounded();
        let open: Opener = Arc::new(move |_, patience| {
            let _ = tried.send((tokio::time::Instant::now(), patience));
            Box::pin(async { Err(io::Error::other("the device is not up yet")) })
        });
        let backend = paused_backend(&ports, open);
        let discovery = Discovery::start_with(serial_only(), backend, &Supervisor::current());

        let ms = Duration::from_millis;
        let expected = [
            (ms(0), PROBE_TIMEOUT),
            (RETRY_FIRST, ms(1000)),
            (ms(1000), ms(2000)),
            (ms(2000), RETRY_MAX),
            (RETRY_MAX, RETRY_MAX),
            (RETRY_MAX, RETRY_MAX),
        ];
        let mut last = None;
        for (index, (delay, patience)) in expected.into_iter().enumerate() {
            let next = tokio::time::timeout(PAUSED_WAIT, tries.recv_async()).await;
            let (at, given) = next.expect("the probe tries again").unwrap();
            if let Some(last) = last {
                assert_took(at - last, delay, format!("the wait before try {index}"));
            }
            assert_eq!(given, patience, "the time try {index} may take");
            last = Some(at);
        }
        stop(discovery).await;
    }

    /// Each check of a probe connection listens for routes for the route
    /// window, then asks the names it lacks. The next check starts a period
    /// after the last one ended.
    #[tokio::test(start_paused = true)]
    async fn each_check_listens_for_the_route_window_then_waits_a_period() {
        let sessions = Supervisor::current();
        let ports = Arc::new(Mutex::new(vec![twinleaf(FIRST)]));
        let (opened, proxies) = flume::unbounded();
        let open: Opener = Arc::new(move |_, _| {
            let (session, proxy) = Session::test_pair_on(&sessions);
            let _ = opened.send(proxy);
            Box::pin(async { Ok(Connection::over(session)) })
        });
        let backend = paused_backend(&ports, open);
        let discovery = Discovery::start_with(serial_only(), backend, &Supervisor::current());
        assert!(matches!(
            next_event(&discovery).await,
            DiscoveryEvent::Added(_)
        ));
        let next = tokio::time::timeout(PAUSED_WAIT, proxies.recv_async()).await;
        let proxy = next.expect("the probe opens").unwrap();
        let started = tokio::time::Instant::now();

        assert_eq!(next_call(&proxy).await.0, "dev.name");
        let window = ROUTE_DISCOVERY_WINDOW;
        assert_took(started.elapsed(), window, "the first check");
        // Unanswered, the name is missing from the first report.
        let event = next_event(&discovery).await;
        assert!(matches!(&event, DiscoveryEvent::Subdevices { routes, .. } if routes.is_empty()));
        assert_took(
            started.elapsed(),
            window + PROBE_TIMEOUT,
            "the first report",
        );

        let (method, id) = next_call(&proxy).await;
        assert_eq!(method, "dev.name");
        let second = window + PROBE_TIMEOUT + REPROBE_PERIOD + window;
        assert_took(started.elapsed(), second, "the second check");
        proxy.reply(id, b"FAKE", DeviceRoute::root());
        let event = next_event(&discovery).await;
        assert!(matches!(&event, DiscoveryEvent::Named { name, .. } if name == "FAKE"));
        stop(discovery).await;
    }
}

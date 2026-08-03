//! Device discovery.
//!
//! Find Twinleaf devices reachable from this host: local serial ports and, with
//! the `mdns` feature, networked devices advertising the `_twinleaf._tcp`/`_udp`
//! mDNS/DNS-SD service. [`Discovery`] is the main entry point — it streams
//! devices as they appear and disappear, drawing from both sources.
//! [`enumerate_serial`] is a synchronous serial-only snapshot for callers that
//! just want a one-shot list.

use crate::device::{DeviceTree, NamedRoute};
use crate::tio::{proto::DeviceRoute, proxy};
use crossbeam::channel;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How a discovered device is reached.
#[derive(Debug, Clone)]
pub enum PortInterface {
    /// Serial device behind an FTDI USB interface.
    FTDI,
    /// Serial device behind an STM32 USB interface.
    STM32,
    /// Networked device discovered via mDNS (`_twinleaf._tcp`/`_twinleaf._udp`).
    Network,
    /// Other serial port, identified by its USB `(vid, pid)`.
    Unknown(u16, u16),
}

/// A device found during discovery.
#[derive(Debug, Clone)]
pub struct DiscoveredDevice {
    pub url: String,
    pub interface: PortInterface,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct DiscoveryConfig {
    /// Include serial ports with unrecognized USB VID/PID.
    pub include_unknown: bool,
    /// Browse the local network for mDNS-advertised devices.
    pub network: bool,
    /// Briefly connect to each device to resolve its `dev.name` and enumerate
    /// the subdevices routed behind it.
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

#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// A device appeared. `name` is set for mDNS instances (from the
    /// advertisement) and `None` for serial ports until a probe resolves it.
    Added(DiscoveredDevice),
    /// A device's `dev.name` was resolved after it was added.
    Named { url: String, name: String },
    /// The subdevices currently alive behind a device, refreshed each probe
    /// pass. Each event supersedes the previous one for `url`.
    Subdevices { url: String, routes: Vec<NamedRoute> },
    /// A previously-added device went away (mDNS goodbye or serial unplug).
    Removed { url: String },
}

/// A running discovery session that streams [`DiscoveryEvent`]s as devices are
/// found on serial ports and, when enabled, the local network. Serial ports are
/// reported immediately; network devices arrive as mDNS resolves them.
///
/// Dropping the handle stops all browsing and name-probing and joins the worker
/// threads, releasing any port held mid-probe — so it is safe to open a device
/// for real right after the handle is dropped.
pub struct Discovery {
    events: channel::Receiver<DiscoveryEvent>,
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl Discovery {
    /// Start a discovery session with the given configuration.
    pub fn start(config: DiscoveryConfig) -> Discovery {
        let (tx, events) = channel::unbounded();
        let stop = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::new();

        // Serial ports are a point-in-time snapshot, reported right away.
        let serial = enumerate_serial(config.include_unknown);
        let serial_urls: Vec<String> = serial.iter().map(|d| d.url.clone()).collect();
        for dev in serial {
            let _ = tx.send(DiscoveryEvent::Added(dev));
        }

        // Probing runs off-thread: serial ports are queued up front, network
        // devices by the mDNS browser as they resolve.
        let (probe_tx, probe_rx) = channel::unbounded::<String>();
        if config.probe_names {
            for url in serial_urls {
                let _ = probe_tx.send(url);
            }
            let tx = tx.clone();
            let stop = stop.clone();
            let include_unknown = config.include_unknown;
            workers.push(std::thread::spawn(move || {
                probe_devices(&probe_rx, include_unknown, &tx, &stop)
            }));
        }

        #[cfg(feature = "mdns")]
        if config.network {
            let tx = tx.clone();
            let stop = stop.clone();
            let probe_tx = config.probe_names.then(|| probe_tx.clone());
            let prefer_udp = config.prefer_udp;
            workers.push(std::thread::spawn(move || {
                browse_network(&tx, probe_tx, prefer_udp, &stop)
            }));
        }
        #[cfg(not(feature = "mdns"))]
        let _ = config.network;

        Discovery {
            events,
            stop,
            workers,
        }
    }

    /// The stream of discovery updates. Drain it with `try_recv`.
    pub fn events(&self) -> &channel::Receiver<DiscoveryEvent> {
        &self.events
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

/// Enumerate Twinleaf devices on local serial ports.
///
/// Matches known Twinleaf USB VID/PIDs (FTDI and STM32 variants). If
/// `include_unknown` is true, other serial ports are also returned with
/// `PortInterface::Unknown(vid, pid)` so callers can surface them as
/// "also found these serial ports" style output.
pub fn enumerate_serial(include_unknown: bool) -> Vec<DiscoveredDevice> {
    #[cfg(not(feature = "serial"))]
    {
        let _ = include_unknown;
        return Vec::new();
    }

    #[cfg(feature = "serial")]
    {
        let mut ports: Vec<DiscoveredDevice> = Vec::new();

        if let Ok(avail_ports) = serialport::available_ports() {
            for p in avail_ports.iter() {
                if let serialport::SerialPortType::UsbPort(info) = &p.port_type {
                    let interface = match (info.vid, info.pid) {
                        (0x0403, 0x6015) => PortInterface::FTDI,
                        (0x0483, 0x5740) => PortInterface::STM32,
                        (vid, pid) => {
                            if !include_unknown {
                                continue;
                            }
                            PortInterface::Unknown(vid, pid)
                        }
                    };
                    #[cfg(target_os = "macos")]
                    if p.port_name.starts_with("/dev/tty.") && !include_unknown {
                        continue;
                    }
                    ports.push(DiscoveredDevice {
                        url: format!("serial://{}", p.port_name),
                        interface,
                        name: None,
                    });
                }
            }
        }

        ports
    }
}

/// Briefly connect to a device and query its `dev.name` RPC.
///
/// Returns `Some(name)` on success, `None` if the port is busy, times out,
/// or the device otherwise fails to respond. Total wall-clock time is
/// bounded by roughly twice `timeout` (reconnect budget + RPC budget).
pub fn query_name(url: &str, timeout: Duration) -> Option<String> {
    let interface = proxy::Interface::new_proxy(url, Some(timeout), None);
    let port = interface
        .new_port(Some(timeout), DeviceRoute::root(), 0, false, false)
        .ok()?;
    port.rpc("dev.name", ()).ok()
}

/// RPC/reconnect budget for each probe connection.
const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// Listen window per probe pass. Heartbeats fire at 5 Hz, so 300 ms sees
/// every live route.
const ROUTE_DISCOVERY_WINDOW: Duration = Duration::from_millis(300);

/// Serial re-scan/re-probe cadence, catching hotplug and late-booting
/// subdevices. Network devices are probed once: reconnecting repeatedly
/// could disrupt a session another host has with them.
const REPROBE_PERIOD: Duration = Duration::from_secs(2);

/// [`Discovery`]'s probe worker: probe each queued URL, then keep re-scanning
/// serial ports every [`REPROBE_PERIOD`]. Bails promptly when `stop` is set.
fn probe_devices(
    urls: &channel::Receiver<String>,
    include_unknown: bool,
    tx: &channel::Sender<DiscoveryEvent>,
    stop: &AtomicBool,
) {
    let mut serial_urls: Vec<String> = Vec::new();
    let mut queue_open = true;
    let mut next_reprobe = Instant::now() + REPROBE_PERIOD;
    while !stop.load(Ordering::Relaxed) {
        if queue_open {
            match urls.recv_timeout(Duration::from_millis(50)) {
                Ok(url) => {
                    if probe_device(&url, tx).is_err() {
                        return;
                    }
                    if url.starts_with("serial://") {
                        serial_urls.push(url);
                    }
                    continue;
                }
                Err(channel::RecvTimeoutError::Timeout) => {}
                Err(channel::RecvTimeoutError::Disconnected) => queue_open = false,
            }
        } else {
            std::thread::sleep(Duration::from_millis(50));
        }

        if Instant::now() >= next_reprobe {
            if reprobe_serial(&mut serial_urls, include_unknown, tx, stop).is_err() {
                return;
            }
            next_reprobe = Instant::now() + REPROBE_PERIOD;
        }
    }
}

/// One serial round: diff the current ports against `known` (emitting
/// `Removed`/`Added`), then probe each. Errors when the event channel closed.
fn reprobe_serial(
    known: &mut Vec<String>,
    include_unknown: bool,
    tx: &channel::Sender<DiscoveryEvent>,
    stop: &AtomicBool,
) -> Result<(), ()> {
    let current = enumerate_serial(include_unknown);
    for url in known.iter().filter(|u| !current.iter().any(|d| &d.url == *u)) {
        tx.send(DiscoveryEvent::Removed { url: url.clone() })
            .map_err(|_| ())?;
    }
    known.retain(|u| current.iter().any(|d| &d.url == u));
    for dev in current {
        if !known.contains(&dev.url) {
            known.push(dev.url.clone());
            tx.send(DiscoveryEvent::Added(dev)).map_err(|_| ())?;
        }
    }
    for url in known.iter() {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        probe_device(url, tx)?;
    }
    Ok(())
}

/// One probe pass over `url`: resolve the root `dev.name` and snapshot the
/// routes alive behind it. Errors when the event channel closed.
fn probe_device(url: &str, tx: &channel::Sender<DiscoveryEvent>) -> Result<(), ()> {
    let interface = proxy::Interface::new_proxy(url, Some(PROBE_TIMEOUT), None);
    let Ok(port) = interface.new_port(
        Some(PROBE_TIMEOUT),
        DeviceRoute::root(),
        usize::MAX,
        true,
        true,
    ) else {
        return Ok(());
    };
    let mut tree = DeviceTree::new(port, DeviceRoute::root());

    let mut routes = Vec::new();
    for named in tree.named_routes(ROUTE_DISCOVERY_WINDOW) {
        if named.route.len() == 0 {
            if let Some(name) = named.name {
                let event = DiscoveryEvent::Named {
                    url: url.to_string(),
                    name,
                };
                tx.send(event).map_err(|_| ())?;
            }
        } else {
            routes.push(named);
        }
    }
    let event = DiscoveryEvent::Subdevices {
        url: url.to_string(),
        routes,
    };
    tx.send(event).map_err(|_| ())
}

/// mDNS/DNS-SD service types Twinleaf network devices advertise. A device may
/// advertise both transports; TCP is preferred when it does. Firmware built
/// before 2026-06-15 advertises `_tio` instead of `_twinleaf`.
#[cfg(feature = "mdns")]
const TWINLEAF_MDNS_SERVICES: [&str; 4] = [
    "_twinleaf._tcp.local.",
    "_twinleaf._udp.local.",
    "_tio._tcp.local.",
    "_tio._udp.local.",
];

/// [`Discovery`]'s network worker: browse `_twinleaf._tcp`/`_twinleaf._udp`
/// continuously, emitting `Added` as instances resolve and `Removed` on goodbye,
/// until `stop` is set. When an instance advertises both transports, the
/// preferred one (TCP unless `prefer_udp`) supersedes the other. Resolved
/// devices are queued on `probe_tx` (when set) so the probe worker can
/// enumerate their subdevices.
#[cfg(feature = "mdns")]
fn browse_network(
    tx: &channel::Sender<DiscoveryEvent>,
    probe_tx: Option<channel::Sender<String>>,
    prefer_udp: bool,
    stop: &AtomicBool,
) {
    use mdns_sd::{ServiceDaemon, ServiceEvent};
    use std::collections::HashMap;

    let daemon = match ServiceDaemon::new() {
        Ok(daemon) => daemon,
        Err(_) => return,
    };
    let receivers: Vec<_> = TWINLEAF_MDNS_SERVICES
        .iter()
        .filter_map(|service| daemon.browse(service).ok())
        .collect();
    if receivers.is_empty() {
        let _ = daemon.shutdown();
        return;
    }

    let mut announced: HashMap<String, String> = HashMap::new();

    while !stop.load(Ordering::Relaxed) {
        let mut idle = true;
        for receiver in &receivers {
            while let Ok(event) = receiver.try_recv() {
                idle = false;
                match event {
                    ServiceEvent::ServiceResolved(info) => {
                        let label = instance_label(info.get_fullname());
                        let preferred = if prefer_udp { "udp" } else { "tcp" };
                        if announced.get(&label).is_some_and(|u| u.starts_with(preferred)) {
                            continue;
                        }
                        let Some(host) = url_host(&info) else {
                            continue;
                        };
                        let scheme = if info.ty_domain.contains("._udp.") {
                            "udp"
                        } else {
                            "tcp"
                        };
                        let url = format!("{}://{}:{}", scheme, host, info.get_port());
                        if announced.get(&label) == Some(&url) {
                            continue;
                        }
                        if let Some(old) = announced.insert(label.clone(), url.clone()) {
                            if tx.send(DiscoveryEvent::Removed { url: old }).is_err() {
                                let _ = daemon.shutdown();
                                return;
                            }
                        }
                        let device = DiscoveredDevice {
                            url: url.clone(),
                            interface: PortInterface::Network,
                            name: Some(label),
                        };
                        if tx.send(DiscoveryEvent::Added(device)).is_err() {
                            let _ = daemon.shutdown();
                            return;
                        }
                        if let Some(probe_tx) = &probe_tx {
                            let _ = probe_tx.send(url);
                        }
                    }
                    ServiceEvent::ServiceRemoved(_, fullname) => {
                        let label = instance_label(&fullname);
                        if let Some(url) = announced.remove(&label) {
                            if tx.send(DiscoveryEvent::Removed { url }).is_err() {
                                let _ = daemon.shutdown();
                                return;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        if idle {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    let _ = daemon.shutdown();
}

/// Strip the trailing service type from a resolved fullname, leaving the
/// human-readable instance name (e.g. `Twinleaf COMM-ETH R7 (COMM-ETH.00019)`).
#[cfg(feature = "mdns")]
fn instance_label(fullname: &str) -> String {
    for service in TWINLEAF_MDNS_SERVICES {
        if let Some(label) = fullname.strip_suffix(service) {
            return label.trim_end_matches('.').to_string();
        }
    }
    fullname.to_string()
}

/// Host part for a discovered device's connect URL. Prefers the advertised
/// `.local.` hostname — stable across IP changes and human-readable, and the OS
/// resolver handles `.local.` via mDNS — falling back to a routable IPv4
/// address (sorted for deterministic output) if no hostname is advertised.
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

//! `tio list` — list connected Twinleaf devices and their subdevices,
//! verifying liveness via heartbeat/event observation.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use twinleaf::device::discovery::{self, PortInterface};
use twinleaf::tio::proto::DeviceRoute;
use twinleaf::tio::proxy;

/// Total time spent listening for heartbeats/data on a freshly opened
/// port. Heartbeats fire at 5 Hz (200 ms), so 300 ms covers at least
/// one guaranteed heartbeat per live route plus routing slack.
const DISCOVERY_WINDOW: Duration = Duration::from_millis(300);

/// RPC timeout for the follow-up `dev.name` query on each discovered route.
const NAME_RPC_TIMEOUT: Duration = Duration::from_millis(200);

struct ListedRoute {
    route: DeviceRoute,
    name: Option<String>,
}

enum ListedStatus {
    Unreachable(String),
    Silent,
    Alive(Vec<ListedRoute>),
}

struct ListedDevice {
    url: String,
    interface: PortInterface,
    status: ListedStatus,
}

pub fn list_devices(all: bool) -> eyre::Result<()> {
    let candidates = discovery::enumerate_serial(all);

    if candidates.is_empty() {
        println!("No Twinleaf devices found.");
        return Ok(());
    }

    let results: Vec<ListedDevice> = std::thread::scope(|s| {
        let handles: Vec<_> = candidates
            .into_iter()
            .map(|dev| s.spawn(move || probe(dev.url, dev.interface)))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("probe thread panicked"))
            .collect()
    });

    render(&results);
    Ok(())
}

fn probe(url: String, interface: PortInterface) -> ListedDevice {
    let ifc = proxy::Interface::new_proxy(&url, Some(Duration::from_millis(500)), None);

    let port = match ifc.subtree_full(DeviceRoute::root()) {
        Ok(p) => p,
        Err(e) => {
            return ListedDevice {
                url,
                interface,
                status: ListedStatus::Unreachable(format!("{}", e)),
            };
        }
    };

    // Collect every route that produces a packet within the discovery window.
    let mut seen: HashSet<DeviceRoute> = HashSet::new();
    let deadline = Instant::now() + DISCOVERY_WINDOW;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match port.receiver().recv_timeout(remaining) {
            Ok(pkt) => {
                seen.insert(pkt.routing);
            }
            Err(_) => break,
        }
    }

    if seen.is_empty() {
        return ListedDevice {
            url,
            interface,
            status: ListedStatus::Silent,
        };
    }

    let mut routes: Vec<DeviceRoute> = seen.into_iter().collect();
    routes.sort();

    let listed: Vec<ListedRoute> = routes
        .into_iter()
        .map(|route| {
            let name = ifc
                .new_port(Some(NAME_RPC_TIMEOUT), route.clone(), 0, false, false)
                .ok()
                .and_then(|p| p.rpc::<(), String>("dev.name", ()).ok());
            ListedRoute { route, name }
        })
        .collect();

    ListedDevice {
        url,
        interface,
        status: ListedStatus::Alive(listed),
    }
}

fn render(devices: &[ListedDevice]) {
    let url_width = devices.iter().map(|d| d.url.len()).max().unwrap_or(0);

    let mut known: Vec<&ListedDevice> = devices
        .iter()
        .filter(|d| !matches!(d.interface, PortInterface::Unknown(..)))
        .collect();
    let unknown: Vec<&ListedDevice> = devices
        .iter()
        .filter(|d| matches!(d.interface, PortInterface::Unknown(..)))
        .collect();

    known.sort_by(|a, b| a.url.cmp(&b.url));

    if !known.is_empty() {
        println!("Twinleaf devices:");
        for dev in &known {
            render_device(dev, url_width);
        }
    }

    if !unknown.is_empty() {
        println!("\nOther serial ports:");
        for dev in &unknown {
            let vp = match dev.interface {
                PortInterface::Unknown(vid, pid) => format!("vid:{:04x} pid:{:04x}", vid, pid),
                _ => String::new(),
            };
            println!("  {}  ({})", dev.url, vp);
        }
    }

    let summary = summary(devices);
    if !summary.is_empty() {
        println!("\n{}", summary);
    }
}

fn render_device(dev: &ListedDevice, url_width: usize) {
    match &dev.status {
        ListedStatus::Unreachable(reason) => {
            println!(
                "  {:<width$}  (unreachable: {})",
                dev.url,
                reason,
                width = url_width
            );
        }
        ListedStatus::Silent => {
            println!("  {:<width$}  (silent)", dev.url, width = url_width);
        }
        ListedStatus::Alive(routes) => {
            let root_name = routes
                .iter()
                .find(|r| r.route.len() == 0)
                .map(|r| r.name.as_deref().unwrap_or("(no name)").to_string())
                .unwrap_or_else(|| "(no root response)".to_string());

            let root_label = format!(
                "{:<width$}  {}",
                dev.url,
                root_name,
                width = url_width
            );
            let tree = build_tree(&DeviceRoute::root(), root_label, routes);
            for line in tree.to_string().lines() {
                println!("  {}", line);
            }
        }
    }
}

fn build_tree(
    parent: &DeviceRoute,
    label: String,
    routes: &[ListedRoute],
) -> termtree::Tree<String> {
    let mut node = termtree::Tree::new(label);
    for r in routes {
        if is_direct_child(parent, &r.route) {
            let child_label = format!(
                "{}  {}",
                r.route,
                r.name.as_deref().unwrap_or("(no name)")
            );
            node.push(build_tree(&r.route, child_label, routes));
        }
    }
    node
}

fn is_direct_child(parent: &DeviceRoute, candidate: &DeviceRoute) -> bool {
    candidate.len() == parent.len() + 1
        && parent
            .iter()
            .zip(candidate.iter())
            .take(parent.len())
            .all(|(a, b)| a == b)
}

fn summary(devices: &[ListedDevice]) -> String {
    let mut alive = 0usize;
    let mut silent = 0usize;
    let mut unreachable = 0usize;
    let mut subdevices = 0usize;

    for dev in devices {
        if matches!(dev.interface, PortInterface::Unknown(..)) {
            continue;
        }
        match &dev.status {
            ListedStatus::Alive(routes) => {
                alive += 1;
                subdevices += routes.iter().filter(|r| r.route.len() > 0).count();
            }
            ListedStatus::Silent => silent += 1,
            ListedStatus::Unreachable(_) => unreachable += 1,
        }
    }

    let mut parts = Vec::new();
    if alive > 0 {
        parts.push(format!(
            "{} device{} alive",
            alive,
            if alive == 1 { "" } else { "s" }
        ));
    }
    if subdevices > 0 {
        parts.push(format!(
            "{} subdevice{}",
            subdevices,
            if subdevices == 1 { "" } else { "s" }
        ));
    }
    if silent > 0 {
        parts.push(format!("{} silent", silent));
    }
    if unreachable > 0 {
        parts.push(format!("{} unreachable", unreachable));
    }
    parts.join(", ")
}

// Called from `tio proxy --enumerate` for backward compatibility; emits a
// deprecation warning and delegates to list_devices.
pub fn list_devices_deprecated(all: bool) -> eyre::Result<()> {
    eprintln!("warning: 'tio proxy --enumerate' is deprecated; use 'tio list' instead");
    list_devices(all)
}

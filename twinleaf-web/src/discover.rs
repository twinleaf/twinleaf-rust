//! Finding the devices this host reaches, for the page's picker, as
//! `tio list` finds them. A scan looks for a while and replies what it found,
//! each device with its name and the subdevices routed behind it.

use crate::protocol::{DiscoverReply, FoundDevice, FoundRoute, Interface};
use std::collections::BTreeMap;
use std::time::Duration;
use twinleaf::device::discovery::{Discovery, DiscoveryConfig, DiscoveryEvent, PortInterface};

/// How long a scan looks.
const SCAN: Duration = Duration::from_secs(3);

/// What a scan knows of one device.
#[derive(Default)]
struct Found {
    interface: Interface,
    name: Option<String>,
    routes: Vec<FoundRoute>,
}

/// How the device is reached, as the picker labels it.
fn interface(interface: &PortInterface) -> Interface {
    match interface {
        PortInterface::FTDI | PortInterface::STM32 => Interface::Usb,
        PortInterface::Network => Interface::Net,
        PortInterface::Unknown(..) => Interface::Serial,
        _ => Interface::Other,
    }
}

/// Looks for devices on the serial ports, and with `network` on the network
/// too, for [`SCAN`], and replies each one found and not gone again.
pub(crate) async fn scan(network: bool) -> DiscoverReply {
    let config = DiscoveryConfig {
        network,
        ..DiscoveryConfig::default()
    };
    let discovery = Discovery::start_async(config).await;
    let mut found: BTreeMap<String, Found> = BTreeMap::new();
    let mut notices = Vec::new();
    let deadline = tokio::time::Instant::now() + SCAN;
    while let Ok(Ok(event)) =
        tokio::time::timeout_at(deadline, discovery.events().recv_async()).await
    {
        match event {
            DiscoveryEvent::Added(device) => {
                let entry = found.entry(device.url).or_default();
                entry.interface = interface(&device.interface);
                entry.name = device.name.or(entry.name.take());
            }
            DiscoveryEvent::Named { url, name } => found.entry(url).or_default().name = Some(name),
            DiscoveryEvent::Subdevices { url, routes } => {
                found.entry(url).or_default().routes = routes
                    .into_iter()
                    .map(|named| FoundRoute {
                        route: named.route.to_string(),
                        name: named.name,
                    })
                    .collect();
            }
            DiscoveryEvent::Removed { url } => {
                found.remove(&url);
            }
            DiscoveryEvent::NetworkUnavailable { reason } => notices.push(reason),
            _ => {}
        }
    }
    discovery.shutdown_async().await;
    let devices = found
        .into_iter()
        .map(|(url, device)| FoundDevice {
            url,
            interface: device.interface,
            name: device.name,
            routes: device.routes,
        })
        .collect();
    DiscoverReply { devices, notices }
}

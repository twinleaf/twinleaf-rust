//! Finding the devices this host reaches, for the page's picker, as
//! `tio list` finds them. A scan looks for a while and replies what it found,
//! each device with its name and the subdevices routed behind it.

use crate::protocol::{DiscoverReply, FoundDevice, FoundRoute};
use std::collections::BTreeMap;
use std::time::Duration;
use twinleaf::device::discovery::{Discovery, DiscoveryConfig, DiscoveryEvent};

/// How long a scan looks.
const SCAN: Duration = Duration::from_secs(3);

/// What a scan knows of one device.
#[derive(Default)]
struct Found {
    interface: Option<&'static str>,
    name: Option<String>,
    routes: Vec<FoundRoute>,
}

/// What a scan heard: each device found and not gone again, by its URL, and
/// why a part of the scan could not run.
#[derive(Default)]
struct Scan {
    found: BTreeMap<String, Found>,
    notices: Vec<String>,
}

impl Scan {
    /// Takes in what discovery says of a device.
    fn heard(&mut self, event: DiscoveryEvent) {
        let found = &mut self.found;
        match event {
            DiscoveryEvent::Added(device) => {
                let entry = found.entry(device.url).or_default();
                entry.interface = Some(device.interface.label());
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
            DiscoveryEvent::NetworkUnavailable { reason } => self.notices.push(reason),
            _ => {}
        }
    }

    /// The reply of what the scan heard.
    fn reply(self) -> DiscoverReply {
        let devices = (self.found.into_iter())
            .map(|(url, device)| FoundDevice {
                url,
                interface: device.interface.unwrap_or("other"),
                name: device.name,
                routes: device.routes,
            })
            .collect();
        DiscoverReply {
            devices,
            notices: self.notices,
        }
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
    let mut scan = Scan::default();
    let deadline = tokio::time::Instant::now() + SCAN;
    while let Ok(Ok(event)) =
        tokio::time::timeout_at(deadline, discovery.events().recv_async()).await
    {
        scan.heard(event);
    }
    discovery.shutdown_async().await;
    scan.reply()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::Json;
    use twinleaf::device::discovery::{DiscoveredDevice, PortInterface};
    use twinleaf::device::NamedRoute;

    /// Records the reply of a scan that found nothing, and of one that found
    /// a device on the network with a subdevice behind it, and a serial port
    /// that went again, and could not browse the network.
    pub(crate) fn record(recorded: &mut crate::recorded::Recorded) {
        recorded.reply("discover-none", Json(Scan::default().reply()));
        let added = |url: &str, interface, name: Option<&str>| {
            DiscoveryEvent::Added(DiscoveredDevice {
                url: url.to_string(),
                interface,
                name: name.map(str::to_string),
            })
        };
        let mut scan = Scan::default();
        for event in [
            added(
                "tcp://sensor.local:7855",
                PortInterface::Network,
                Some("sensor"),
            ),
            added("serial:///dev/ttyUSB0", PortInterface::FTDI, None),
            DiscoveryEvent::Subdevices {
                url: "tcp://sensor.local:7855".to_string(),
                routes: vec![NamedRoute {
                    route: "/0".parse().unwrap(),
                    name: Some("subsensor".to_string()),
                }],
            },
            DiscoveryEvent::Removed {
                url: "serial:///dev/ttyUSB0".to_string(),
            },
            DiscoveryEvent::NetworkUnavailable {
                reason: "mDNS could not start".to_string(),
            },
        ] {
            scan.heard(event);
        }
        recorded.reply("discover", Json(scan.reply()));
    }
}

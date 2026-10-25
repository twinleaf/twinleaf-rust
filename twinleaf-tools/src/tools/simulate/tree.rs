//! The devices of a layout, as the tree their routes make. A device reaches
//! its children through the hub `twinleaf-device` runs on its ports, as
//! firmware does. A route the layout leaves out is a plain router, which
//! passes packets by their hops and says nothing of its own.

use std::collections::BTreeSet;

use twinleaf::proto::packet::{Packet, PacketView};
use twinleaf::proto::route::{pop_hop, push_hop};
use twinleaf::proto::DeviceRoute;
use twinleaf_device::hub::{self, Event, Events, Hub, PortSink};
use twinleaf_device::Sink;

use super::device::SimDevice;

/// Every hop a route may name.
const PORTS: usize = 256;

/// Packets a node sends up, in the order it sent them.
#[derive(Default)]
pub(super) struct Queue(pub(super) Vec<Vec<u8>>);

impl Sink for Queue {
    fn send(&mut self, packet: &[u8]) {
        self.0.push(packet.to_vec());
    }
}

/// Packets a hub sends down, with the port each goes to.
#[derive(Default)]
struct Down(Vec<(u8, Vec<u8>)>);

impl PortSink for Down {
    fn send(&mut self, port: u8, packet: &[u8]) {
        self.0.push((port, packet.to_vec()));
    }
}

/// What a hub notices, which the simulator has no use for.
struct Unheeded;

impl Events<()> for Unheeded {
    fn event(&mut self, _event: Event<'_, ()>) {}
}

/// One route of the tree, with or without a device at it.
pub(super) struct Node {
    device: Option<SimDevice>,
    /// The ports of the device here. A route with no device has none.
    hub: Hub<(), PORTS>,
    children: Vec<(u8, Node)>,
}

impl Node {
    fn passive() -> Self {
        Self {
            device: None,
            hub: Hub::new(),
            children: Vec::new(),
        }
    }

    /// The tree of `devices`, each at its route.
    ///
    /// # Errors
    ///
    /// Returns what is wrong with the layout, a route given twice or a route
    /// reached through a port of a device that has nothing plugged into it.
    pub(super) fn build(devices: Vec<SimDevice>) -> Result<Self, String> {
        let routes: BTreeSet<DeviceRoute> = devices.iter().map(|device| device.route).collect();
        if routes.len() != devices.len() {
            let mut seen = BTreeSet::new();
            let twice = devices.iter().find(|device| !seen.insert(device.route));
            return Err(format!(
                "the layout gives {} twice",
                twice.expect("a repeat exists").route
            ));
        }
        for route in &routes {
            let Some(missing) = missing_parent(&routes, route) else {
                continue;
            };
            if let Some(above) = ancestors(missing).find(|parent| routes.contains(parent)) {
                return Err(format!(
                    "the layout puts {route} behind {missing}, which has no device, on a port \
                     of the device at {above}; a hub reaches nothing through an empty port"
                ));
            }
        }
        let mut root = Self::passive();
        for device in devices {
            let mut node = &mut root;
            for &hop in device.route.as_slice() {
                let at = match node.children.iter().position(|(port, _)| *port == hop) {
                    Some(at) => at,
                    None => {
                        node.children.push((hop, Self::passive()));
                        node.children.len() - 1
                    }
                };
                node = &mut node.children[at].1;
            }
            node.device = Some(device);
        }
        Ok(root)
    }

    /// Every device of the tree, parents before their children.
    pub(super) fn devices(&self) -> Vec<&SimDevice> {
        let mut devices: Vec<&SimDevice> = self.device.iter().collect();
        for (_, child) in &self.children {
            devices.extend(child.devices());
        }
        devices
    }

    /// Hands a packet from the host to the route it names. `buf` holds the
    /// packet at its start.
    pub(super) fn route_down(&mut self, buf: &mut [u8], now_ns: u64, up: &mut Queue) {
        let Ok((view, len)) = PacketView::parse_prefix(buf) else {
            return;
        };
        let Some(&port) = view.routing.last() else {
            if let Some(device) = &mut self.device {
                device.handle(&buf[..len], now_ns, up);
            }
            return;
        };
        let mut down = Down::default();
        match self.device {
            Some(_) => self.hub.handle(
                hub::Input::FromHost(buf),
                now_ns,
                up,
                &mut down,
                &mut Unheeded,
            ),
            None => {
                if let Ok((_, len)) = pop_hop(buf) {
                    down.0.push((port, buf[..len].to_vec()));
                }
            }
        }
        for (port, packet) in down.0 {
            let Some(child) = self.child(port) else {
                continue;
            };
            let mut sent = Queue::default();
            let mut buf = [0u8; Packet::MAX_SIZE];
            buf[..packet.len()].copy_from_slice(&packet);
            child.route_down(&mut buf, now_ns, &mut sent);
            self.lift(port, sent, now_ns, up);
        }
    }

    /// Sends what is due at `now_ns` from every device of the tree, and lets
    /// each hub unplug the children it stopped hearing.
    pub(super) fn tick(&mut self, now_ns: u64, up: &mut Queue) {
        if let Some(device) = &mut self.device {
            device.tick(now_ns, up);
            self.hub.handle(
                hub::Input::Tick,
                now_ns,
                up,
                &mut Down::default(),
                &mut Unheeded,
            );
        }
        for at in 0..self.children.len() {
            let mut sent = Queue::default();
            let (port, child) = &mut self.children[at];
            let port = *port;
            child.tick(now_ns, &mut sent);
            self.lift(port, sent, now_ns, up);
        }
    }

    /// Does `act` to every device of the tree, and sends up what each sent.
    pub(super) fn each(
        &mut self,
        now_ns: u64,
        up: &mut Queue,
        act: &mut dyn FnMut(&mut SimDevice, &mut Queue),
    ) {
        if let Some(device) = &mut self.device {
            act(device, up);
        }
        for at in 0..self.children.len() {
            let mut sent = Queue::default();
            let (port, child) = &mut self.children[at];
            let port = *port;
            child.each(now_ns, &mut sent, act);
            self.lift(port, sent, now_ns, up);
        }
    }

    fn child(&mut self, port: u8) -> Option<&mut Node> {
        self.children
            .iter_mut()
            .find(|(hop, _)| *hop == port)
            .map(|(_, child)| child)
    }

    /// Sends up what the child on `port` sent, wearing that port. It goes
    /// through the hub of the device here, or straight up if there is none.
    fn lift(&mut self, port: u8, sent: Queue, now_ns: u64, up: &mut Queue) {
        for packet in sent.0 {
            let mut buf = [0u8; Packet::MAX_SIZE];
            buf[..packet.len()].copy_from_slice(&packet);
            match self.device {
                Some(_) => self.hub.handle(
                    hub::Input::FromChild {
                        port,
                        packet: &mut buf,
                    },
                    now_ns,
                    up,
                    &mut Down::default(),
                    &mut Unheeded,
                ),
                None => {
                    if let Ok(len) = push_hop(&mut buf, port) {
                        up.send(&buf[..len]);
                    }
                }
            }
        }
    }
}

/// The routes above `route`, nearest first.
fn ancestors(route: DeviceRoute) -> impl Iterator<Item = DeviceRoute> {
    (0..route.as_slice().len())
        .rev()
        .filter_map(move |len| DeviceRoute::from_hops(&route.as_slice()[..len]).ok())
}

/// The nearest route above `route` that `routes` leaves without a device.
pub(super) fn missing_parent(
    routes: &BTreeSet<DeviceRoute>,
    route: &DeviceRoute,
) -> Option<DeviceRoute> {
    ancestors(*route).find(|parent| !routes.contains(parent))
}

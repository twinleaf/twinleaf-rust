//! Stateful access to one device or a routed device tree.

use crate::data::{DeviceMetadataSnapshot, PacketParser, SampleBatch};
use crate::tio;
use crate::tio::proto::DeviceRoute;
use tio::proto::{RpcArgs, RpcReply};
use tio::{proto, proxy};

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Overall budget for [`DeviceTree::get_metadata`] to finish discovery, which
/// can take several `dev.metadata` round trips.
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// Device-level events produced by the live connection engine.
///
/// These events arrive via both direct serial and tio-proxy connections. A
/// direct serial connection closes after `SensorDisconnected`; a tio-proxy TCP
/// connection can remain open, making the status event the disconnection
/// signal shared by both transports.
#[derive(Debug, Clone)]
pub enum DeviceEvent {
    /// Connection status changed.
    ///
    /// A direct serial channel closes after `SensorDisconnected`, while a
    /// tio-proxy connection can stay open. Consumers should therefore handle
    /// this event rather than depending on a later receive error.
    Status(proto::ProxyStatus),
    /// Another client completed an RPC that can invalidate a cached value.
    RpcInvalidated(proto::RpcMethod),
    /// Device heartbeat, including the session id for the standard format.
    Heartbeat {
        session_id: Option<proto::identifiers::SessionId>,
    },
    /// The parser has collected complete metadata for this device.
    MetadataReady(DeviceMetadataSnapshot),
    /// `Some(hash)` comes from a settings packet; `None` requests a refresh
    /// after reconnection.
    NewHash(Option<u32>),
}

/// A discovered route paired with its device's `dev.name` (`None` if the device
/// didn't answer). Returned by [`DeviceTree::named_routes`].
#[derive(Debug, Clone)]
pub struct NamedRoute {
    pub route: DeviceRoute,
    pub name: Option<String>,
}

/// Events from a DeviceTree (multi-device monitoring).
#[derive(Debug, Clone)]
pub enum TreeEvent {
    /// First packet received from this route.
    RouteDiscovered(DeviceRoute),

    /// Event from a specific device.
    Device {
        route: DeviceRoute,
        event: DeviceEvent,
    },
}

#[derive(Debug, Clone)]
pub enum TreeItem {
    Batch(SampleBatch),
    Event(TreeEvent),
}

/// The connection to the proxy closed.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("proxy disconnected")]
pub struct ProxyDisconnected;

pub struct DeviceTree {
    port: proxy::Port,
    root_route: DeviceRoute,
    parser: PacketParser,
    known_routes: HashSet<DeviceRoute>,
    metadata_announced: HashSet<DeviceRoute>,
    batch_queue: VecDeque<SampleBatch>,
    event_queue: VecDeque<TreeEvent>,
}

impl DeviceTree {
    pub fn new(port: proxy::Port, root_route: DeviceRoute) -> DeviceTree {
        DeviceTree {
            port,
            parser: PacketParser::new(root_route, false),
            root_route,
            known_routes: HashSet::new(),
            metadata_announced: HashSet::new(),
            batch_queue: VecDeque::new(),
            event_queue: VecDeque::new(),
        }
    }

    pub fn open(
        proxy: &tio::proxy::Interface,
        route: DeviceRoute,
    ) -> Result<DeviceTree, proxy::PortError> {
        let port = proxy.subtree_full(route)?;
        Ok(Self::new(port, route))
    }

    fn internal_rpcs(&mut self) -> Result<(), ProxyDisconnected> {
        for req in self.parser.take_requests() {
            // Parser requests target routes we received packets from, which
            // are in scope by construction; the only send failure is a dead
            // link.
            self.port.send(req).map_err(|_| ProxyDisconnected)?;
        }
        Ok(())
    }

    fn process_packet(&mut self, pkt: &tio::Packet) {
        let Ok(absolute_route) = self.root_route.absolute_route(&pkt.routing) else {
            log::warn!(
                "dropping packet whose route {} exceeds root scope {}",
                pkt.routing,
                self.root_route
            );
            return;
        };

        if self.known_routes.insert(absolute_route) {
            self.event_queue
                .push_back(TreeEvent::RouteDiscovered(absolute_route));
        }

        match &pkt.payload {
            tio::proto::Payload::ProxyStatus(ps) => {
                self.event_queue.push_back(TreeEvent::Device {
                    route: absolute_route,
                    event: DeviceEvent::Status(ps.0),
                });

                // Forget our metadata on disconnect
                if matches!(ps.0, proto::ProxyStatus::SensorDisconnected) {
                    self.metadata_announced = HashSet::new();
                    self.parser.reset();
                }

                // We might have new hash(es) on reconnect
                if matches!(ps.0, proto::ProxyStatus::SensorReconnected) {
                    for route in self.known_routes.iter() {
                        self.event_queue.push_back(TreeEvent::Device {
                            route: *route,
                            event: DeviceEvent::NewHash(None),
                        });
                    }
                }

                return;
            }
            tio::proto::Payload::RpcUpdate(ru) => {
                self.event_queue.push_back(TreeEvent::Device {
                    route: absolute_route,
                    event: DeviceEvent::RpcInvalidated(ru.0.clone()),
                });
                return;
            }
            tio::proto::Payload::Heartbeat(hb) => {
                let session_id = match hb {
                    tio::proto::HeartbeatPayload::Session(sid) => Some(*sid),
                    tio::proto::HeartbeatPayload::Any(_) => None,
                };
                self.event_queue.push_back(TreeEvent::Device {
                    route: absolute_route,
                    event: DeviceEvent::Heartbeat { session_id },
                });
            }
            tio::proto::Payload::Settings(set) => {
                let hash = match set {
                    tio::proto::SettingsPayload::RpcHash(h) => Some(*h),
                    tio::proto::SettingsPayload::Unknown { .. } => None,
                };
                if let Some(hash) = hash {
                    self.event_queue.push_back(TreeEvent::Device {
                        route: absolute_route,
                        event: DeviceEvent::NewHash(Some(hash)),
                    });
                }
            }
            _ => {}
        }

        if let Some(batch) = self.parser.process_packet(pkt) {
            self.batch_queue.push_back(batch);
        }
        if !self.metadata_announced.contains(&absolute_route) {
            if let Some(full_metadata) = self.parser.metadata(absolute_route) {
                self.metadata_announced.insert(absolute_route);
                self.event_queue.push_back(TreeEvent::Device {
                    route: absolute_route,
                    event: DeviceEvent::MetadataReady(full_metadata),
                });
            }
        }
    }

    /// Collect complete metadata for `route`, waiting up to `METADATA_TIMEOUT`
    /// before returning [`proxy::RpcError::Timeout`].
    pub fn get_metadata(
        &mut self,
        route: DeviceRoute,
    ) -> Result<DeviceMetadataSnapshot, tio::proxy::RpcError> {
        let deadline = Instant::now() + METADATA_TIMEOUT;
        loop {
            if let Some(full_meta) = self.parser.metadata(route) {
                return Ok(full_meta);
            }
            if Instant::now() >= deadline {
                return Err(tio::proxy::RpcError::Timeout);
            }
            for req in self.parser.take_requests_for(route) {
                self.port.send(req)?;
            }
            let pkt = self
                .port
                .recv_deadline(deadline)
                .map_err(|error| match error {
                    proxy::RecvTimeoutError::Timeout => tio::proxy::RpcError::Timeout,
                    proxy::RecvTimeoutError::ProxyDisconnected => {
                        tio::proxy::RpcError::ResponseLost
                    }
                })?;
            self.process_packet(&pkt);
        }
    }

    fn pop_item(&mut self) -> Option<TreeItem> {
        self.batch_queue
            .pop_front()
            .map(TreeItem::Batch)
            .or_else(|| self.event_queue.pop_front().map(TreeItem::Event))
    }

    /// Block until the next item.
    ///
    /// May transmit metadata requests to the proxy as part of receiving.
    pub fn recv(&mut self) -> Result<TreeItem, ProxyDisconnected> {
        loop {
            if let Some(item) = self.pop_item() {
                return Ok(item);
            }
            self.internal_rpcs()?;
            let pkt = self.port.recv().map_err(|_| ProxyDisconnected)?;
            self.process_packet(&pkt);
        }
    }

    /// Like [`recv`](Self::recv), but gives up at `deadline`.
    pub fn recv_deadline(
        &mut self,
        deadline: Instant,
    ) -> Result<TreeItem, proxy::RecvTimeoutError> {
        loop {
            if let Some(item) = self.pop_item() {
                return Ok(item);
            }
            if Instant::now() >= deadline {
                return Err(proxy::RecvTimeoutError::Timeout);
            }
            self.internal_rpcs()
                .map_err(|_| proxy::RecvTimeoutError::ProxyDisconnected)?;
            let pkt = self.port.recv_deadline(deadline)?;
            self.process_packet(&pkt);
        }
    }

    pub fn raw_rpc(
        &mut self,
        route: DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<Vec<u8>, tio::proxy::RpcError> {
        let relative_routing = self
            .root_route
            .relative_route(&route)
            .map_err(|_| tio::proxy::RpcError::InvalidRoute)?;
        let req = proto::Packet::rpc_request(name, arg, 0, relative_routing);
        self.port.send(req)?;

        loop {
            self.internal_rpcs()
                .map_err(|_| tio::proxy::RpcError::ResponseLost)?;
            let pkt = match self.port.recv() {
                Ok(packet) => packet,
                Err(_) => return Err(tio::proxy::RpcError::ResponseLost),
            };

            let Ok(absolute_route) = self.root_route.absolute_route(&pkt.routing) else {
                continue;
            };

            if absolute_route == route {
                match &pkt.payload {
                    // Our own request carries id 0 while metadata requests
                    // use a different id and are consumed by the parser
                    tio::proto::Payload::RpcReply(rep) if rep.id == 0 => {
                        return Ok(rep.reply.clone());
                    }
                    tio::proto::Payload::RpcError(err) if err.id == 0 => {
                        return Err(tio::proxy::RpcError::DeviceError(err.clone()));
                    }
                    _ => {}
                }
            }

            self.process_packet(&pkt);
        }
    }

    pub fn rpc<ReqT: RpcArgs, RepT: RpcReply>(
        &mut self,
        route: DeviceRoute,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, tio::proxy::RpcError> {
        let ret = self.raw_rpc(route, name, &arg.encode_args())?;
        RepT::decode_reply(&ret).map_err(tio::proxy::RpcError::InvalidReply)
    }

    pub fn action(&mut self, route: DeviceRoute, name: &str) -> Result<(), tio::proxy::RpcError> {
        self.rpc(route, name, ())
    }

    pub fn get<T: RpcReply>(
        &mut self,
        route: DeviceRoute,
        name: &str,
    ) -> Result<T, tio::proxy::RpcError> {
        self.rpc(route, name, ())
    }

    /// Passively observe the subtree for `window` and return the routes seen, sorted.
    pub fn discover_routes(&mut self, window: Duration) -> Vec<DeviceRoute> {
        let deadline = Instant::now() + window;
        while Instant::now() < deadline {
            match self.port.recv_deadline(deadline) {
                Ok(pkt) => self.process_packet(&pkt),
                Err(_) => break,
            }
        }
        let mut routes = self.parser.routes();
        routes.sort();
        routes
    }

    /// Discover the subtree's routes (see [`discover_routes`](Self::discover_routes))
    /// and pair each with its `dev.name`. A route that doesn't answer is returned
    /// with `name: None` rather than dropped, so the caller still sees it.
    pub fn named_routes(&mut self, window: Duration) -> Vec<NamedRoute> {
        self.discover_routes(window)
            .into_iter()
            .map(|route| {
                let name = self
                    .get::<String>(route, "dev.name")
                    .ok()
                    .map(|n| n.trim().to_string())
                    .filter(|n| !n.is_empty());
                NamedRoute { route, name }
            })
            .collect()
    }
}

/// Why [`Device::next_batch`] returned without a sample batch.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum BatchError {
    /// Sensor connection lost; the proxy is reconnecting. Recoverable: call
    /// `next_batch` again to wait, and expect a `Boundary` on the first batch
    /// after reconnection.
    #[error("sensor disconnected")]
    SensorDisconnected,
    /// The proxy gave up connecting to the sensor. Terminal.
    #[error("sensor connection failed")]
    ConnectionFailed,
    /// The link to the proxy itself closed. Terminal.
    #[error("proxy disconnected")]
    ProxyClosed,
}

/// A route-free view of one exact device.
///
/// The underlying port has depth zero, so the general [`DeviceTree`] engine can
/// only observe its root route. This wrapper removes route arguments and folds
/// connection-level events into [`next_batch`](Self::next_batch)'s error type;
/// use [`DeviceTree`] directly to observe the full event stream.
pub struct Device {
    tree: DeviceTree,
}

impl Device {
    pub fn new(dev_port: proxy::Port) -> Device {
        Device {
            tree: DeviceTree::new(dev_port, DeviceRoute::root()),
        }
    }

    pub fn open(proxy: &proxy::Interface, route: DeviceRoute) -> Result<Device, proxy::PortError> {
        Ok(Self::new(proxy.device_full(route)?))
    }

    pub fn get_metadata(&mut self) -> Result<DeviceMetadataSnapshot, proxy::RpcError> {
        self.tree.get_metadata(DeviceRoute::root())
    }

    /// Wait for the next sample batch.
    ///
    /// Over tio-proxy a dead sensor does not error the underlying channel, so
    /// the [`BatchError::SensorDisconnected`] result is the only disconnection
    /// signal shared by both serial and proxy transports.
    pub fn next_batch(&mut self) -> Result<SampleBatch, BatchError> {
        loop {
            match self.tree.recv().map_err(|_| BatchError::ProxyClosed)? {
                TreeItem::Batch(batch) => return Ok(batch),
                TreeItem::Event(TreeEvent::Device {
                    event: DeviceEvent::Status(status),
                    ..
                }) => match status {
                    proto::ProxyStatus::SensorDisconnected => {
                        return Err(BatchError::SensorDisconnected)
                    }
                    proto::ProxyStatus::FailedToReconnect | proto::ProxyStatus::FailedToConnect => {
                        return Err(BatchError::ConnectionFailed)
                    }
                    // Reconnects surface as a Boundary on the next batch.
                    _ => {}
                },
                TreeItem::Event(_) => {}
            }
        }
    }

    pub fn raw_rpc(&mut self, name: &str, arg: &[u8]) -> Result<Vec<u8>, proxy::RpcError> {
        self.tree.raw_rpc(DeviceRoute::root(), name, arg)
    }

    pub fn rpc<ReqT: RpcArgs, RepT: RpcReply>(
        &mut self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, proxy::RpcError> {
        self.tree.rpc(DeviceRoute::root(), name, arg)
    }

    pub fn action(&mut self, name: &str) -> Result<(), proxy::RpcError> {
        self.tree.action(DeviceRoute::root(), name)
    }

    pub fn get<T: RpcReply>(&mut self, name: &str) -> Result<T, proxy::RpcError> {
        self.tree.get(DeviceRoute::root(), name)
    }
}

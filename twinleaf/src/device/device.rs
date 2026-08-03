use crate::data::{DeviceMetadataSnapshot, PacketParser, SampleBatch};
use crate::tio;
use proto::DeviceRoute;
use tio::{proto, proxy, util};

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

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
        let port = proxy.subtree_full(route.clone())?;
        Ok(Self::new(port, route))
    }

    fn internal_rpcs(&mut self) -> Result<(), proxy::SendError> {
        for req in self.parser.take_requests() {
            self.port.send(req)?;
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

        if self.known_routes.insert(absolute_route.clone()) {
            self.event_queue
                .push_back(TreeEvent::RouteDiscovered(absolute_route.clone()));
        }

        match &pkt.payload {
            tio::proto::Payload::ProxyStatus(ps) => {
                self.event_queue.push_back(TreeEvent::Device {
                    route: absolute_route.clone(),
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
                            route: route.clone(),
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
                    route: absolute_route.clone(),
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
                        route: absolute_route.clone(),
                        event: DeviceEvent::NewHash(Some(hash)),
                    });
                }
            }
            _ => {}
        }

        if let Some(batch) = self.parser.process_packet(&pkt) {
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

    pub fn get_metadata(
        &mut self,
        route: DeviceRoute,
    ) -> Result<DeviceMetadataSnapshot, tio::proxy::RpcError> {
        loop {
            if let Some(full_meta) = self.parser.metadata(route) {
                return Ok(full_meta);
            }
            for req in self.parser.take_requests_for(route) {
                self.port
                    .send(req)
                    .map_err(tio::proxy::RpcError::SendFailed)?;
            }
            let pkt = self.port.recv().map_err(tio::proxy::RpcError::RecvFailed)?;
            self.process_packet(&pkt);
        }
    }

    pub fn drain(&mut self) -> Result<Vec<SampleBatch>, tio::proxy::RpcError> {
        loop {
            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;
            match self.port.try_recv() {
                Ok(pkt) => {
                    self.process_packet(&pkt);
                }
                Err(proxy::RecvError::WouldBlock) => {
                    break;
                }
                Err(e) => {
                    return Err(tio::proxy::RpcError::RecvFailed(e));
                }
            }
        }

        Ok(self.batch_queue.drain(..).collect())
    }

    fn pop_batch(&mut self) -> Option<SampleBatch> {
        self.batch_queue.pop_front()
    }

    pub fn try_next_event(&mut self) -> Option<TreeEvent> {
        self.event_queue.pop_front()
    }

    pub fn drain_events(&mut self) -> Vec<TreeEvent> {
        self.event_queue.drain(..).collect()
    }

    pub fn next_item(&mut self) -> Result<TreeItem, proxy::RpcError> {
        loop {
            if let Some(parsed) = self.batch_queue.pop_front() {
                return Ok(TreeItem::Batch(parsed));
            }

            if let Some(event) = self.event_queue.pop_front() {
                return Ok(TreeItem::Event(event));
            }

            self.internal_rpcs()?;
            let pkt = self.port.recv()?;
            self.process_packet(&pkt);
        }
    }

    pub fn try_next_item(&mut self) -> Result<Option<TreeItem>, proxy::RpcError> {
        loop {
            if let Some(parsed) = self.batch_queue.pop_front() {
                return Ok(Some(TreeItem::Batch(parsed)));
            }

            if let Some(event) = self.event_queue.pop_front() {
                return Ok(Some(TreeItem::Event(event)));
            }

            self.internal_rpcs()?;
            match self.port.try_recv() {
                Ok(pkt) => self.process_packet(&pkt),
                Err(proxy::RecvError::WouldBlock) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub fn raw_rpc(
        &mut self,
        route: DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<Vec<u8>, tio::proxy::RpcError> {
        let mut req = util::PacketBuilder::make_rpc_request(name, arg, 0, DeviceRoute::root());
        let relative_routing = match self.root_route.relative_route(&route) {
            Ok(r) => r,
            Err(_) => {
                req.routing = route;
                return Err(tio::proxy::RpcError::SendFailed(
                    tio::proxy::SendError::InvalidRoute(req),
                ));
            }
        };

        req.routing = relative_routing;

        if let Err(err) = self.port.send(req) {
            return Err(tio::proxy::RpcError::SendFailed(err));
        }

        loop {
            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;
            let pkt = match self.port.recv() {
                Ok(packet) => packet,
                Err(e) => return Err(tio::proxy::RpcError::RecvFailed(e)),
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
                        return Err(tio::proxy::RpcError::ExecError(err.clone()));
                    }
                    _ => {}
                }
            }

            self.process_packet(&pkt);
        }
    }

    pub fn rpc<ReqT: tio::util::TioRpcRequestable<ReqT>, RepT: tio::util::TioRpcReplyable<RepT>>(
        &mut self,
        route: DeviceRoute,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, tio::proxy::RpcError> {
        let ret = self.raw_rpc(route, name, &arg.to_request())?;
        if let Ok(val) = RepT::from_reply(&ret) {
            Ok(val)
        } else {
            Err(tio::proxy::RpcError::TypeError)
        }
    }

    pub fn action(&mut self, route: DeviceRoute, name: &str) -> Result<(), tio::proxy::RpcError> {
        self.rpc(route, name, ())
    }

    pub fn get<T: tio::util::TioRpcReplyable<T>>(
        &mut self,
        route: DeviceRoute,
        name: &str,
    ) -> Result<T, tio::proxy::RpcError> {
        self.rpc(route, name, ())
    }

    pub fn get_multi(
        &mut self,
        route: DeviceRoute,
        name: &str,
    ) -> Result<Vec<u8>, tio::proxy::RpcError> {
        let mut full_reply = vec![];

        for i in 0u16..=65535u16 {
            match self.raw_rpc(route.clone(), &name, &i.to_le_bytes().to_vec()) {
                Ok(mut rep) => full_reply.append(&mut rep),
                Err(err @ proxy::RpcError::ExecError(_)) => {
                    if let proxy::RpcError::ExecError(payload) = &err {
                        if let tio::proto::RpcErrorCode::InvalidArgs = payload.error {
                            break;
                        }
                    }
                    return Err(err);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(full_reply)
    }

    pub fn get_multi_str(
        &mut self,
        route: DeviceRoute,
        name: &str,
    ) -> Result<String, tio::proxy::RpcError> {
        let reply_bytes = self.get_multi(route, name)?;
        let result_string = String::from_utf8_lossy(&reply_bytes).to_string();
        Ok(result_string)
    }

    pub fn known_routes(&self) -> Vec<DeviceRoute> {
        self.parser.routes()
    }

    /// Passively observe the subtree for `window` and return the routes seen, sorted.
    pub fn discover_routes(&mut self, window: Duration) -> Vec<DeviceRoute> {
        let deadline = Instant::now() + window;
        while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            let received = self.port.receiver().recv_timeout(remaining);
            match received {
                Ok(pkt) => self.process_packet(&pkt),
                Err(_) => break,
            }
        }
        let mut routes = self.known_routes();
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
                    .get::<String>(route.clone(), "dev.name")
                    .ok()
                    .map(|n| n.trim().to_string())
                    .filter(|n| !n.is_empty());
                NamedRoute { route, name }
            })
            .collect()
    }
}

pub enum DeviceItem {
    Batch(SampleBatch),
    Event(DeviceEvent),
}

/// A route-free view of one exact device.
///
/// The underlying port has depth zero, so the general [`DeviceTree`] engine can
/// only observe its root route. This wrapper removes route arguments and hides
/// subtree-discovery events while sharing packet, metadata, and RPC handling.
pub struct Device {
    tree: DeviceTree,
    event_queue: VecDeque<DeviceEvent>,
}

fn scoped_event(event: TreeEvent) -> Option<DeviceEvent> {
    match event {
        TreeEvent::Device { event, .. } => Some(event),
        TreeEvent::RouteDiscovered(_) => None,
    }
}

impl Device {
    pub fn new(dev_port: proxy::Port) -> Device {
        Device {
            tree: DeviceTree::new(dev_port, DeviceRoute::root()),
            event_queue: VecDeque::new(),
        }
    }

    pub fn open(proxy: &proxy::Interface, route: DeviceRoute) -> Result<Device, proxy::PortError> {
        Ok(Self::new(proxy.device_full(route)?))
    }

    fn capture_event(&mut self, event: TreeEvent) {
        if let Some(event) = scoped_event(event) {
            self.event_queue.push_back(event);
        }
    }

    fn capture_queued_events(&mut self) {
        while let Some(event) = self.tree.try_next_event() {
            self.capture_event(event);
        }
    }

    pub fn get_metadata(&mut self) -> Result<DeviceMetadataSnapshot, proxy::RpcError> {
        self.tree.get_metadata(DeviceRoute::root())
    }

    pub fn next(&mut self) -> Result<SampleBatch, proxy::RpcError> {
        loop {
            match self.tree.next_item()? {
                TreeItem::Batch(batch) => return Ok(batch),
                TreeItem::Event(event) => self.capture_event(event),
            }
        }
    }

    pub fn try_next(&mut self) -> Result<Option<SampleBatch>, proxy::RpcError> {
        loop {
            match self.tree.try_next_item()? {
                Some(TreeItem::Batch(batch)) => return Ok(Some(batch)),
                Some(TreeItem::Event(event)) => self.capture_event(event),
                None => return Ok(None),
            }
        }
    }

    pub fn drain(&mut self) -> Result<Vec<SampleBatch>, proxy::RpcError> {
        let batches = self.tree.drain()?;
        self.capture_queued_events();
        Ok(batches)
    }

    pub fn try_next_event(&mut self) -> Option<DeviceEvent> {
        self.capture_queued_events();
        self.event_queue.pop_front()
    }

    pub fn drain_events(&mut self) -> Vec<DeviceEvent> {
        self.capture_queued_events();
        self.event_queue.drain(..).collect()
    }

    pub fn next_item(&mut self) -> Result<DeviceItem, proxy::RpcError> {
        if let Some(batch) = self.tree.pop_batch() {
            return Ok(DeviceItem::Batch(batch));
        }
        if let Some(event) = self.event_queue.pop_front() {
            return Ok(DeviceItem::Event(event));
        }

        loop {
            match self.tree.next_item()? {
                TreeItem::Batch(batch) => return Ok(DeviceItem::Batch(batch)),
                TreeItem::Event(TreeEvent::Device { event, .. }) => {
                    return Ok(DeviceItem::Event(event));
                }
                TreeItem::Event(TreeEvent::RouteDiscovered(_)) => {}
            }
        }
    }

    pub fn try_next_item(&mut self) -> Result<Option<DeviceItem>, proxy::RpcError> {
        if let Some(batch) = self.tree.pop_batch() {
            return Ok(Some(DeviceItem::Batch(batch)));
        }
        if let Some(event) = self.event_queue.pop_front() {
            return Ok(Some(DeviceItem::Event(event)));
        }

        loop {
            match self.tree.try_next_item()? {
                Some(TreeItem::Batch(batch)) => return Ok(Some(DeviceItem::Batch(batch))),
                Some(TreeItem::Event(TreeEvent::Device { event, .. })) => {
                    return Ok(Some(DeviceItem::Event(event)));
                }
                Some(TreeItem::Event(TreeEvent::RouteDiscovered(_))) => {}
                None => return Ok(None),
            }
        }
    }

    pub fn raw_rpc(&mut self, name: &str, arg: &[u8]) -> Result<Vec<u8>, proxy::RpcError> {
        self.tree.raw_rpc(DeviceRoute::root(), name, arg)
    }

    pub fn rpc<ReqT: tio::util::TioRpcRequestable<ReqT>, RepT: tio::util::TioRpcReplyable<RepT>>(
        &mut self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, proxy::RpcError> {
        self.tree.rpc(DeviceRoute::root(), name, arg)
    }

    pub fn action(&mut self, name: &str) -> Result<(), proxy::RpcError> {
        self.tree.action(DeviceRoute::root(), name)
    }

    pub fn get<T: tio::util::TioRpcReplyable<T>>(
        &mut self,
        name: &str,
    ) -> Result<T, proxy::RpcError> {
        self.tree.get(DeviceRoute::root(), name)
    }

    pub fn get_multi(&mut self, name: &str) -> Result<Vec<u8>, proxy::RpcError> {
        self.tree.get_multi(DeviceRoute::root(), name)
    }

    pub fn get_multi_str(&mut self, name: &str) -> Result<String, proxy::RpcError> {
        self.tree.get_multi_str(DeviceRoute::root(), name)
    }
}

//! Stateful access to one device or a routed device tree.

use crate::data::{DeviceMetadataSnapshot, SampleBatch};
use crate::device::stream::{
    DeviceEvent, NamedRoute, PumpHandle, Receiver, RecvTimeoutError, TreeEvent,
};
use crate::device::{CallError, RpcArgs, RpcRegistry, RpcRegistryError, RpcReply};
use crate::tio::proto::DeviceRoute;
use crate::tio::proxy::{self, Connection};

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam::channel;

/// Overall budget for [`DeviceTree::metadata`] to finish discovery, which can
/// take several `dev.metadata` round trips.
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// Why [`DeviceTree::metadata`] returned no metadata.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum MetadataError {
    /// The device's firmware has no `dev.metadata`, so its streams can never be
    /// described. Terminal.
    #[error("the device does not report metadata")]
    Unsupported,
    /// Discovery did not finish within the metadata budget.
    #[error("metadata did not arrive in time")]
    Timeout,
    /// The link to the proxy closed. Terminal.
    #[error("proxy disconnected")]
    Disconnected,
}

/// A reply that has not arrived yet.
///
/// Blocking is just consuming it now with [`wait`](Self::wait); a caller that
/// must keep doing other work polls [`try_get`](Self::try_get) instead. Every
/// pending reply resolves: with the value, or with the device's or the proxy's
/// error (the proxy times outstanding RPCs out).
#[must_use = "the RPC is in flight; wait on the reply or poll it"]
pub struct PendingReply {
    replies: channel::Receiver<proxy::RawCallResult>,
}

impl PendingReply {
    /// Block until the reply resolves.
    pub fn wait(self) -> Result<Vec<u8>, CallError> {
        Ok(self
            .replies
            .recv()
            .unwrap_or(Err(proxy::RawCallError::ProxyClosed))?)
    }

    /// The reply if it has already arrived, `None` while still in flight.
    pub fn try_get(&self) -> Option<Result<Vec<u8>, CallError>> {
        match self.replies.try_recv() {
            Ok(reply) => Some(reply.map_err(CallError::from)),
            Err(channel::TryRecvError::Empty) => None,
            Err(channel::TryRecvError::Disconnected) => Some(Err(CallError::ResponseLost)),
        }
    }
}

impl Connection {
    /// The whole device tree reachable through this connection.
    pub fn tree(&self) -> DeviceTree {
        self.tree_with(DeviceRoute::root(), twinleaf_proto::MAX_ROUTING_SIZE, None)
            .expect("the default RPC timeout is in range")
    }

    /// The subtree rooted at `route`, reaching `depth` levels below it and
    /// giving up on an RPC after `rpc_timeout` instead of the default.
    pub fn tree_with(
        &self,
        route: DeviceRoute,
        depth: usize,
        rpc_timeout: Option<Duration>,
    ) -> Result<DeviceTree, proxy::PortError> {
        Ok(DeviceTree {
            endpoint: self.rpc_endpoint(rpc_timeout, route, depth)?,
            pump: Arc::new(Mutex::new(None)),
        })
    }

    /// The device at `route`. Shorthand for `self.tree().device(route)`.
    pub fn device(&self, route: DeviceRoute) -> Device {
        self.tree().device(route)
    }

    /// The device at `route`, giving up on an RPC after `rpc_timeout`.
    pub fn device_with(
        &self,
        route: DeviceRoute,
        rpc_timeout: Option<Duration>,
    ) -> Result<Device, proxy::PortError> {
        Ok(self.tree_with(route, 0, rpc_timeout)?.device(route))
    }
}

/// A session with a routed device tree.
///
/// RPCs go straight to the proxy worker and block only on their own
/// completion, so nothing else needs to run. [`subscribe`](Self::subscribe) and
/// [`events`](Self::events) start a private pump that drains the tree's data
/// continuously and hands out owned receivers; neither borrows the tree.
/// Cloning is cheap: every clone is the same capability over the same proxy
/// worker and shares the pump.
///
/// ```no_run
/// use twinleaf::{Connection, DeviceRoute};
///
/// let tree = Connection::open("tcp://localhost").tree();
/// let name: String = tree.get(DeviceRoute::root(), "dev.name")?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
pub struct DeviceTree {
    endpoint: proxy::RpcEndpoint,
    pump: Arc<Mutex<Option<PumpHandle>>>,
}

impl DeviceTree {
    /// The device at `route`, the only way to mint a [`Device`].
    pub fn device(&self, route: DeviceRoute) -> Device {
        Device {
            tree: DeviceTree {
                endpoint: self.endpoint.scoped(route, 0),
                pump: Arc::new(Mutex::new(None)),
            },
        }
    }

    /// Subscribe to the subtree's sample batches, each tagged with the route it
    /// came from. The first subscription starts the pump.
    pub fn subscribe(&self) -> Result<Receiver<SampleBatch>, proxy::PortError> {
        self.mint(PumpHandle::batches)
    }

    /// Subscribe to the subtree's connection, metadata and invalidation events.
    /// A new subscriber is first told the routes and metadata already known.
    pub fn events(&self) -> Result<Receiver<TreeEvent>, proxy::PortError> {
        self.mint(PumpHandle::events)
    }

    /// Start the pump if it is not already running, and take a receiver from it.
    fn mint<T>(
        &self,
        subscribe: impl Fn(&PumpHandle) -> Option<Receiver<T>>,
    ) -> Result<Receiver<T>, proxy::PortError> {
        let mut pump = self.pump.lock().expect("the stream pump lock is poisoned");
        if let Some(receiver) = pump.as_ref().and_then(&subscribe) {
            return Ok(receiver);
        }
        let handle = PumpHandle::start(&self.endpoint)?;
        let receiver = subscribe(&handle).ok_or(proxy::PortError::FailedNewClientSetup)?;
        *pump = Some(handle);
        Ok(receiver)
    }

    /// Collect complete metadata for `route`, waiting up to `METADATA_TIMEOUT`.
    pub fn metadata(&self, route: DeviceRoute) -> Result<DeviceMetadataSnapshot, MetadataError> {
        let events = self.events().map_err(|_| MetadataError::Disconnected)?;
        let deadline = Instant::now() + METADATA_TIMEOUT;
        loop {
            let event = match events.recv_deadline(deadline) {
                Ok(event) => event,
                Err(RecvTimeoutError::Lagged(_)) => continue,
                Err(RecvTimeoutError::Timeout) => return Err(MetadataError::Timeout),
                Err(RecvTimeoutError::Disconnected) => return Err(MetadataError::Disconnected),
            };
            let TreeEvent::Device { route: from, event } = event else {
                continue;
            };
            if from != route {
                continue;
            }
            match event {
                DeviceEvent::MetadataReady(snapshot) => return Ok(snapshot),
                DeviceEvent::MetadataUnavailable => return Err(MetadataError::Unsupported),
                DeviceEvent::Status(_)
                | DeviceEvent::RpcInvalidated(_)
                | DeviceEvent::Heartbeat { .. }
                | DeviceEvent::NewHash(_) => {}
            }
        }
    }

    /// Passively observe the subtree for `window` and return the routes seen, sorted.
    pub fn discover_routes(&self, window: Duration) -> Vec<DeviceRoute> {
        let Ok(events) = self.events() else {
            return Vec::new();
        };
        let deadline = Instant::now() + window;
        let mut routes = Vec::new();
        loop {
            match events.recv_deadline(deadline) {
                Ok(TreeEvent::RouteDiscovered(route)) => routes.push(route),
                Ok(TreeEvent::Device { .. }) | Err(RecvTimeoutError::Lagged(_)) => {}
                Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        routes.sort();
        routes
    }

    /// Discover the subtree's routes (see [`discover_routes`](Self::discover_routes))
    /// and pair each with its `dev.name`. A route that doesn't answer is returned
    /// with `name: None` rather than dropped, so the caller still sees it.
    pub fn named_routes(&self, window: Duration) -> Vec<NamedRoute> {
        self.discover_routes(window)
            .into_iter()
            .map(|route| NamedRoute {
                route,
                name: self
                    .get::<String>(route, "dev.name")
                    .ok()
                    .map(|name| name.trim().to_string())
                    .filter(|name| !name.is_empty()),
            })
            .collect()
    }

    /// Issue an RPC without waiting for its reply.
    pub fn submit(
        &self,
        route: DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<PendingReply, CallError> {
        Ok(PendingReply {
            replies: self.endpoint.submit(route, name, arg)?,
        })
    }

    pub fn raw_rpc(
        &self,
        route: DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<Vec<u8>, CallError> {
        self.submit(route, name, arg)?.wait()
    }

    pub fn rpc<ReqT: RpcArgs, RepT: RpcReply>(
        &self,
        route: DeviceRoute,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, CallError> {
        let ret = self.raw_rpc(route, name, &arg.encode_args())?;
        RepT::decode_reply(&ret).map_err(CallError::InvalidReply)
    }

    pub fn action(&self, route: DeviceRoute, name: &str) -> Result<(), CallError> {
        self.rpc(route, name, ())
    }

    pub fn get<T: RpcReply>(&self, route: DeviceRoute, name: &str) -> Result<T, CallError> {
        self.rpc(route, name, ())
    }

    /// The RPCs the device at `route` offers, from the on-disk cache when its
    /// `rpc.hash` still matches, otherwise by walking `rpc.listinfo`.
    pub fn rpc_registry(&self, route: DeviceRoute) -> Result<RpcRegistry, RpcRegistryError> {
        RpcRegistry::load_with(|name, arg| {
            let pending = self.submit(route, name, arg)?;
            Ok(move || pending.wait())
        })
    }
}

/// A route-free view of one exact device, minted by [`DeviceTree::device`].
///
/// An exact, depth-zero [`DeviceTree`] slice: the session can only address
/// its own root route, so this wrapper removes route arguments. Use
/// [`DeviceTree`] directly to address a subtree.
///
/// ```no_run
/// use twinleaf::{Connection, DeviceRoute};
///
/// let device = Connection::open("tcp://localhost").device(DeviceRoute::root());
/// let name: String = device.rpc("dev.name", ())?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
pub struct Device {
    tree: DeviceTree,
}

impl Device {
    fn route(&self) -> DeviceRoute {
        self.tree.endpoint.scope()
    }

    /// Subscribe to this device's sample batches. See [`DeviceTree::subscribe`].
    pub fn subscribe(&self) -> Result<Receiver<SampleBatch>, proxy::PortError> {
        self.tree.subscribe()
    }

    /// Subscribe to this device's events. See [`DeviceTree::events`].
    pub fn events(&self) -> Result<Receiver<TreeEvent>, proxy::PortError> {
        self.tree.events()
    }

    /// Collect this device's metadata. See [`DeviceTree::metadata`].
    pub fn metadata(&self) -> Result<DeviceMetadataSnapshot, MetadataError> {
        self.tree.metadata(self.route())
    }

    /// Issue an RPC without waiting for its reply. See [`DeviceTree::submit`].
    pub fn submit(&self, name: &str, arg: &[u8]) -> Result<PendingReply, CallError> {
        self.tree.submit(self.route(), name, arg)
    }

    pub fn raw_rpc(&self, name: &str, arg: &[u8]) -> Result<Vec<u8>, CallError> {
        self.tree.raw_rpc(self.route(), name, arg)
    }

    pub fn rpc<ReqT: RpcArgs, RepT: RpcReply>(
        &self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, CallError> {
        self.tree.rpc(self.route(), name, arg)
    }

    pub fn action(&self, name: &str) -> Result<(), CallError> {
        self.tree.action(self.route(), name)
    }

    pub fn get<T: RpcReply>(&self, name: &str) -> Result<T, CallError> {
        self.tree.get(self.route(), name)
    }

    /// The RPCs this device offers. See [`DeviceTree::rpc_registry`].
    pub fn rpc_registry(&self) -> Result<RpcRegistry, RpcRegistryError> {
        self.tree.rpc_registry(self.route())
    }

    /// Test-only device with no proxy behind it. Returns the device and the far
    /// end receiving the calls it submits.
    #[cfg(test)]
    pub(crate) fn test_pair() -> (
        Device,
        channel::Receiver<crate::tio::proxy_core::ProxyCommand>,
    ) {
        let (endpoint, commands) = proxy::RpcEndpoint::test_pair(DeviceRoute::root(), 0);
        let tree = DeviceTree {
            endpoint,
            pump: Arc::new(Mutex::new(None)),
        };
        (Device { tree }, commands)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tio::proto;
    use crate::tio::proxy::RawCallError;
    use crate::tio::proxy_core::ProxyCommand;
    use std::thread;
    use twinleaf_proto::rpc as wire_rpc;

    fn test_tree() -> (DeviceTree, channel::Receiver<ProxyCommand>) {
        let (device, commands) = Device::test_pair();
        (device.tree, commands)
    }

    #[test]
    fn a_blocking_rpc_waits_on_its_own_completion() {
        let (tree, commands) = test_tree();
        let responder = thread::spawn(move || {
            let ProxyCommand::Call {
                request, result, ..
            } = commands.recv().unwrap()
            else {
                panic!("expected a direct RPC command");
            };
            let proto::Payload::RpcRequest(request) = request.payload() else {
                panic!("expected an RPC request");
            };
            assert_eq!(request.method, wire_rpc::Method::ByName(b"dev.name"));
            result.send(Ok(b"ASM".to_vec())).unwrap();
        });

        let name: String = tree.get(DeviceRoute::root(), "dev.name").unwrap();
        assert_eq!(name, "ASM");
        responder.join().unwrap();
    }

    #[test]
    fn concurrent_typed_rpcs_complete_out_of_order() {
        let (tree, commands) = test_tree();
        let responder = thread::spawn(move || {
            let in_flight: Vec<_> = (0..2).map(|_| commands.recv().unwrap()).collect();
            for call in in_flight.into_iter().rev() {
                let ProxyCommand::Call {
                    request, result, ..
                } = call
                else {
                    panic!("expected a direct RPC command");
                };
                let proto::Payload::RpcRequest(request) = request.payload() else {
                    panic!("expected an RPC request");
                };
                let wire_rpc::Method::ByName(name) = request.method else {
                    panic!("expected a call by name");
                };
                result.send(Ok(name.to_vec())).unwrap();
            }
        });

        let tree = &tree;
        thread::scope(|calls| {
            for name in ["dev.name", "dev.desc"] {
                calls.spawn(move || {
                    assert_eq!(tree.get::<String>(DeviceRoute::root(), name).unwrap(), name);
                });
            }
        });
        responder.join().unwrap();
    }

    #[test]
    fn raw_call_failures_map_to_typed_call_errors() {
        let (tree, commands) = test_tree();
        let responder = thread::spawn(move || {
            let failures = [
                Some(RawCallError::Device {
                    error: wire_rpc::RpcError::NotFound,
                    message: Vec::new(),
                }),
                Some(RawCallError::DeviceDisconnected),
                None,
            ];
            for failure in failures {
                let ProxyCommand::Call { result, .. } = commands.recv().unwrap() else {
                    panic!("expected a direct RPC command");
                };
                match failure {
                    Some(failure) => result.send(Err(failure)).unwrap(),
                    None => drop(result),
                }
            }
        });

        let refused = tree.raw_rpc(DeviceRoute::root(), "dev.name", b"");
        assert!(matches!(
            refused,
            Err(CallError::DeviceError(payload))
                if matches!(payload.error, wire_rpc::RpcError::NotFound)
        ));
        assert!(matches!(
            tree.raw_rpc(DeviceRoute::root(), "dev.name", b""),
            Err(CallError::DeviceDisconnected)
        ));
        assert!(matches!(
            tree.raw_rpc(DeviceRoute::root(), "dev.name", b""),
            Err(CallError::ResponseLost)
        ));
        responder.join().unwrap();
    }
}

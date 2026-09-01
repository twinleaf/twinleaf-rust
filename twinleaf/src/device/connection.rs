//! Stateful access to one device or a routed device tree.

use crate::data::{DeviceMetadataSnapshot, SampleBatch};
use crate::device::stream::{
    DeviceEvent, Event, NamedRoute, Receiver, RecvError, Scope, Stream, TreeEvent,
};
use crate::device::{CallError, PendingReply, RpcArgs, RpcRegistry, RpcRegistryError, RpcReply};
use crate::tio;
use crate::tio::proto::route::RouteError;
use crate::tio::proto::DeviceRoute;
use crate::tio::proxy;

use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel;

/// Overall budget for [`DeviceTree::metadata`] to finish discovery, which can
/// take several `dev.metadata` round trips.
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// Why [`DeviceTree::metadata`] returned no metadata.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum MetadataError {
    /// The requested route is outside this tree view.
    #[error(transparent)]
    Route(#[from] RouteError),
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

/// A live link to one device tree, over serial, TCP, or UDP.
///
/// Opening one starts background workers that own the transport and proxy and
/// reconnect on their own; the first subscription starts the stream pump every
/// view minted here shares. It is the whole tree at full depth:
/// [`tree`](Connection::tree) names any subtree and [`device`](Connection::device)
/// any single device — neither can fail here — and the three subscriptions are
/// the same here as on any view. Cloning is free; every clone is the same
/// connection.
///
/// # Platform behavior
///
/// On macOS and Windows, the transport and proxy workers ask the OS for
/// latency-critical execution and inhibit idle system sleep while active. These
/// requests are best-effort and require no caller setup.
#[derive(Clone)]
pub struct Connection {
    tree: DeviceTree,
}

impl Connection {
    /// Open a connection to `url` and start driving its transport.
    ///
    /// Accepted transport locators are:
    ///
    /// - `serial://port[:target_bps[:default_bps]]`, with both rates defaulting
    ///   to 115200. The `serial://` prefix may be omitted for `/dev/...` paths
    ///   on Unix and `COM...` ports on Windows. Requires the `serial` feature.
    /// - `tcp://address[:port]`, with `tcp4://` and `tcp6://` variants.
    /// - `udp://address[:port]`, with `udp4://` and `udp6://` variants.
    ///
    /// TCP and UDP use port 7855 when no port is given.
    pub fn open(url: &str) -> Connection {
        Self::open_with(url, None, None)
    }

    /// Open a connection that gives up on a reconnect after `reconnect_timeout`
    /// and reports transport events to `status_queue`.
    ///
    /// See [`open`](Self::open) for accepted transport locators.
    pub fn open_with(
        url: &str,
        reconnect_timeout: Option<Duration>,
        status_queue: Option<channel::Sender<proxy::Event>>,
    ) -> Connection {
        Self::over(&proxy::Connection::open_with(
            url,
            reconnect_timeout,
            status_queue,
        ))
    }

    /// A device view of a link a proxy server already owns.
    ///
    /// A server keeps its own [`tio::proxy`](crate::tio::proxy) plumbing —
    /// ports, the status queue — and asks the device layer its questions over
    /// that transport rather than opening a second one.
    pub fn over(proxy: &proxy::Connection) -> Connection {
        let root = proxy
            .rpc_endpoint(None, DeviceRoute::root(), twinleaf_proto::MAX_ROUTING_SIZE)
            .expect("the default RPC timeout is in range");
        Connection {
            tree: DeviceTree {
                stream: Arc::new(Stream::new(root.clone())),
                endpoint: root,
            },
        }
    }

    /// The tree at and below `route`: the whole tree from the root route.
    /// [`to_depth`](DeviceTree::to_depth) and
    /// [`with_timeout`](DeviceTree::with_timeout) narrow the view further.
    ///
    /// Every route is under the connection's root, so this cannot fail; only
    /// narrowing an already-narrowed view can.
    pub fn tree(&self, route: DeviceRoute) -> DeviceTree {
        DeviceTree {
            endpoint: self
                .tree
                .endpoint
                .subtree(route)
                .expect("every route is under the connection's root"),
            stream: self.tree.stream.clone(),
        }
    }

    /// The device at `route`. Shorthand for `self.tree(route)` narrowed to
    /// exactly that node, with the same infallibility.
    pub fn device(&self, route: DeviceRoute) -> Device {
        self.tree
            .device(route)
            .expect("every route is under the connection's root")
    }

    /// Subscribe to every route's sample batches. See [`DeviceTree::samples`].
    pub fn samples(&self) -> Receiver<SampleBatch> {
        self.tree.samples()
    }

    /// Subscribe to every route's events. See [`DeviceTree::events`].
    pub fn events(&self) -> Receiver<Event> {
        self.tree.events()
    }

    /// Subscribe to the connection's whole packet stream. See
    /// [`DeviceTree::packets`].
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.tree.packets()
    }
}

/// A routed view of a device tree.
///
/// Use this view when the target route varies from one operation to the next;
/// [`Device`] binds one route for repeated operations on the same device. RPCs
/// go straight to the proxy worker and block only on their own completion, so
/// nothing else needs to run. [`samples`](Self::samples),
/// [`events`](Self::events) and [`packets`](Self::packets) take owned
/// receivers from the connection's one stream, filtered to this subtree and
/// carrying absolute routes; each holds the stream it came from, so it keeps
/// delivering once every view is dropped. Cloning is cheap: every clone is the
/// same capability over the same proxy worker.
///
/// ```no_run
/// use twinleaf::{Connection, DeviceRoute};
///
/// let root = DeviceRoute::root();
/// let tree = Connection::open("tcp://localhost").tree(root);
/// let name: String = tree.get(root, "dev.name").expect("read device name");
/// ```
#[derive(Clone)]
pub struct DeviceTree {
    endpoint: proxy::RpcEndpoint,
    stream: Arc<Stream>,
}

impl DeviceTree {
    /// The device at `route`, the only way to mint a [`Device`].
    ///
    /// Fails when `route` lies outside the subtree this view covers; that the
    /// device is there is proven by its first RPC, not here.
    pub fn device(&self, route: DeviceRoute) -> Result<Device, RouteError> {
        Ok(Device {
            tree: DeviceTree {
                endpoint: self.endpoint.scoped(route)?,
                stream: self.stream.clone(),
            },
        })
    }

    /// The same view reaching no deeper than `depth` levels below its root.
    pub fn to_depth(&self, depth: usize) -> DeviceTree {
        DeviceTree {
            endpoint: self.endpoint.to_depth(depth),
            stream: self.stream.clone(),
        }
    }

    /// The same view giving up on its own RPCs after `rpc_timeout` instead of
    /// the default. The connection's shared metadata discovery keeps the
    /// default.
    pub fn with_timeout(&self, rpc_timeout: Duration) -> DeviceTree {
        DeviceTree {
            endpoint: self.endpoint.with_timeout(rpc_timeout),
            stream: self.stream.clone(),
        }
    }

    /// Subscribe to the subtree's sample batches, each tagged with the route it
    /// came from. The first subscription on the connection starts its pump.
    pub fn samples(&self) -> Receiver<SampleBatch> {
        self.stream.batches(Scope::of(&self.endpoint))
    }

    /// Subscribe to the subtree's facts: the link's, the population's, and
    /// each covered device's. A new subscriber is first told the routes and
    /// metadata already known.
    pub fn events(&self) -> Receiver<Event> {
        self.stream.events(Scope::of(&self.endpoint))
    }

    /// Subscribe to the subtree's packets as they arrive on the wire, before
    /// anything parses them. A tap is passive: it neither decodes samples nor
    /// asks a device to describe itself.
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.stream.packets(Scope::of(&self.endpoint))
    }

    /// Collect complete metadata for `route`, waiting up to `METADATA_TIMEOUT`.
    /// A route outside this view is rejected before waiting.
    pub fn metadata(&self, route: DeviceRoute) -> Result<DeviceMetadataSnapshot, MetadataError> {
        self.endpoint.scoped(route)?;
        let events = self.events();
        let deadline = Instant::now() + METADATA_TIMEOUT;
        loop {
            let event = match events.recv_deadline(deadline) {
                Ok(event) => event,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Timeout) => return Err(MetadataError::Timeout),
                Err(RecvError::Disconnected) => return Err(MetadataError::Disconnected),
            };
            match event {
                Event::Device { route: from, event } if from == route => match event {
                    DeviceEvent::Metadata(snapshot) => return Ok(snapshot),
                    DeviceEvent::MetadataUnavailable => return Err(MetadataError::Unsupported),
                    DeviceEvent::Heartbeat { .. }
                    | DeviceEvent::RpcInvalidated(_)
                    | DeviceEvent::NewHash(_) => {}
                },
                Event::Device { .. } | Event::Tree { .. } | Event::Link { .. } => {}
            }
        }
    }

    /// Passively observe the subtree for `window` and return the routes seen, sorted.
    pub fn discover_routes(&self, window: Duration) -> Vec<DeviceRoute> {
        let events = self.events();
        let deadline = Instant::now() + window;
        let mut routes = Vec::new();
        loop {
            match events.recv_deadline(deadline) {
                Ok(Event::Tree {
                    route,
                    event: TreeEvent::RouteDiscovered,
                }) => routes.push(route),
                Ok(Event::Device { .. }) | Ok(Event::Link { .. }) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Timeout) | Err(RecvError::Disconnected) => break,
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

    /// Issue an RPC at `route` without waiting for its reply. An out-of-scope
    /// route returns [`CallError::InvalidRoute`] without submitting a request.
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

    /// Call `name` at `route` with already encoded arguments and return the raw
    /// reply.
    pub fn raw_rpc(
        &self,
        route: DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<Vec<u8>, CallError> {
        self.submit(route, name, arg)?.wait()
    }

    /// Call a typed RPC at `route`, encoding its arguments and decoding its
    /// reply.
    pub fn rpc<ReqT: RpcArgs, RepT: RpcReply>(
        &self,
        route: DeviceRoute,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, CallError> {
        let ret = self.raw_rpc(route, name, &arg.encode_args())?;
        RepT::decode_reply(&ret).map_err(CallError::InvalidReply)
    }

    /// Call a no-argument RPC at `route` that returns no value.
    pub fn action(&self, route: DeviceRoute, name: &str) -> Result<(), CallError> {
        self.rpc(route, name, ())
    }

    /// Call a no-argument RPC at `route` and decode its reply as `T`.
    pub fn get<T: RpcReply>(&self, route: DeviceRoute, name: &str) -> Result<T, CallError> {
        self.rpc(route, name, ())
    }

    /// The RPCs the device at `route` offers, from the on-disk cache when its
    /// `rpc.hash` still matches, otherwise by walking `rpc.listinfo`.
    pub fn rpc_registry(&self, route: DeviceRoute) -> Result<RpcRegistry, RpcRegistryError> {
        RpcRegistry::load_with(|name, arg| self.submit(route, name, arg))
    }
}

/// A route-free view of one exact device, minted by [`DeviceTree::device`].
///
/// An exact, depth-zero [`DeviceTree`] slice. It is cheap to create and does
/// not contact the device; the first operation establishes whether the device
/// exists. Use it when several operations share one route, and use
/// [`DeviceTree`] when the route varies between operations.
///
/// ```no_run
/// use twinleaf::{Connection, DeviceRoute};
///
/// let device = Connection::open("tcp://localhost").device(DeviceRoute::root());
/// let name: String = device.rpc("dev.name", ()).expect("read device name");
/// ```
#[derive(Clone)]
pub struct Device {
    tree: DeviceTree,
}

impl Device {
    fn route(&self) -> DeviceRoute {
        self.tree.endpoint.scope()
    }

    /// Subscribe to this device's sample batches. See [`DeviceTree::samples`].
    pub fn samples(&self) -> Receiver<SampleBatch> {
        self.tree.samples()
    }

    /// Subscribe to this device's facts, and the link's. See
    /// [`DeviceTree::events`].
    pub fn events(&self) -> Receiver<Event> {
        self.tree.events()
    }

    /// Subscribe to this device's packets. See [`DeviceTree::packets`].
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.tree.packets()
    }

    /// Collect this device's metadata. See [`DeviceTree::metadata`].
    pub fn metadata(&self) -> Result<DeviceMetadataSnapshot, MetadataError> {
        self.tree.metadata(self.route())
    }

    /// The same device giving up on its RPCs after `rpc_timeout` instead of
    /// the default.
    pub fn with_timeout(&self, rpc_timeout: Duration) -> Device {
        Device {
            tree: self.tree.with_timeout(rpc_timeout),
        }
    }

    /// Issue an RPC without waiting for its reply.
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

    /// The RPCs this device offers, from the on-disk cache when its
    /// `rpc.hash` still matches, otherwise by walking `rpc.listinfo`.
    pub fn rpc_registry(&self) -> Result<RpcRegistry, RpcRegistryError> {
        self.tree.rpc_registry(self.route())
    }

    /// Test-only device with no proxy behind it. Returns the device, the far
    /// end receiving the calls it submits, and the worker's lifeline.
    #[cfg(test)]
    pub(crate) fn test_pair() -> (
        Device,
        channel::Receiver<crate::tio::proxy_core::ProxyCommand>,
        channel::Sender<()>,
    ) {
        let (endpoint, commands, worker) = proxy::RpcEndpoint::test_pair(DeviceRoute::root(), 0);
        let tree = DeviceTree {
            stream: Arc::new(Stream::new(endpoint.clone())),
            endpoint,
        };
        (Device { tree }, commands, worker)
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

    fn test_tree() -> (
        DeviceTree,
        channel::Receiver<ProxyCommand>,
        channel::Sender<()>,
    ) {
        let (device, commands, worker) = Device::test_pair();
        (device.tree, commands, worker)
    }

    /// A view may not mint one that reaches further than itself: containment is
    /// checked here, existence at the first RPC.
    #[test]
    fn a_device_must_lie_within_the_trees_scope_and_depth() {
        let route = |text: &str| text.parse::<DeviceRoute>().expect("a valid route");
        let (endpoint, _commands, _worker) = proxy::RpcEndpoint::test_pair(route("/1"), 1);
        let tree = DeviceTree {
            stream: Arc::new(Stream::new(endpoint.clone())),
            endpoint,
        };

        assert!(tree.device(route("/1")).is_ok());
        assert!(tree.device(route("/1/2")).is_ok());
        assert!(matches!(
            tree.device(route("/2")),
            Err(RouteError::OutsideSubtree)
        ));
        assert!(matches!(
            tree.device(route("/1/2/3")),
            Err(RouteError::OutsideSubtree)
        ));
    }

    #[test]
    fn metadata_rejects_a_route_outside_the_tree_before_waiting() {
        let route = |text: &str| text.parse::<DeviceRoute>().expect("a valid route");
        let (endpoint, _commands, _worker) = proxy::RpcEndpoint::test_pair(route("/1"), 1);
        let tree = DeviceTree {
            stream: Arc::new(Stream::new(endpoint.clone())),
            endpoint,
        };

        assert!(matches!(
            tree.metadata(route("/2")),
            Err(MetadataError::Route(RouteError::OutsideSubtree))
        ));
    }

    #[test]
    fn a_blocking_rpc_waits_on_its_own_completion() {
        let (tree, commands, _worker) = test_tree();
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
        let (tree, commands, _worker) = test_tree();
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
        let (tree, commands, _worker) = test_tree();
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

//! The [`Connection`] to a proxy and its [`DeviceTree`] and [`Device`] views.

use super::rpc::{
    CallError, PendingReply, RpcArgs, RpcMeta, RpcRegistry, RpcRegistryError, RpcReply,
    SettingsCache,
};
use super::runtime::ProxyAddress;
use super::session::{Handle, Session};
use super::stream::{DeviceEvent, Event, NamedRoute, Receiver, RecvError, Scope, Stream};
use crate::data::{DeviceMetadataSnapshot, SampleBatch};
use crate::proto::DeviceRoute;
use crate::proto::RouteError;
use crate::proxy::Purpose;
use crate::rt::Supervisor;
use crate::tio;
use crate::tio::transport::is_local_tcp;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long [`DeviceTree::metadata`] waits for discovery to finish, across
/// all its `dev.metadata` round trips.
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// How long [`Connection::open`] waits for the proxy to open a device. The
/// proxy itself gives a device 10 s to answer.
const OPEN_TIMEOUT: Duration = Duration::from_secs(15);

/// Why [`DeviceTree::metadata`] returned no metadata.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// The requested route is outside this tree view.
    #[error(transparent)]
    Route(#[from] RouteError),
    /// The device's firmware has no `dev.metadata`. Terminal.
    #[error("the device does not report metadata")]
    Unsupported,
    /// Discovery did not finish within the metadata budget.
    #[error("metadata did not arrive in time")]
    Timeout,
    /// The link to the proxy closed. Terminal.
    #[error("proxy disconnected")]
    Disconnected,
}

/// A live link to one device tree through the machine's
/// [proxy](crate::proxy), which opens the device over serial, TCP, or UDP.
///
/// Opening one starts a task on the crate's internal runtime that owns the
/// connection's socket to the proxy. The connection covers every route, and
/// [`tree`](Connection::tree) and [`device`](Connection::device) cannot
/// fail. Cloning is cheap, and every clone is the same connection.
///
/// On macOS and Windows the connection's I/O requests latency-critical
/// scheduling, and its open socket inhibits idle sleep.
#[derive(Clone, Debug)]
pub struct Connection {
    tree: DeviceTree,
}

impl Connection {
    /// Opens a connection to the default device.
    ///
    /// The default device is what the proxy resolves `auto` to. That is the
    /// default set by whoever hosts the proxy, such as `tio proxy` or a
    /// selection in `tio list`. Failing that, it is the one device the proxy
    /// has open, and then the only Twinleaf serial device attached. Apart from
    /// the target, this is [`open`](Self::open).
    ///
    /// # Errors
    ///
    /// Returns an error if no single device is the default, with a message
    /// that names the candidates. Also fails in every way
    /// [`open`](Self::open) does.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use twinleaf::{Connection, DeviceRoute};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let connection = Connection::connect()?;
    /// let name: String = connection.device(DeviceRoute::root()).get("dev.name")?;
    /// println!("connected to {name}");
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect() -> std::io::Result<Connection> {
        Self::open("auto")
    }

    /// Opens a connection to the device at `url`.
    ///
    /// Devices are shared through the machine's [proxy](crate::proxy) on
    /// `127.0.0.1:7855`, which opens each device once for every process.
    /// When no proxy answers, this starts `tio proxy --background` from
    /// `twinleaf-tools`, and that proxy exits ten seconds after its last
    /// client leaves. When `tio` cannot be started, or is too old to run the
    /// proxy, this process hosts the proxy itself.
    ///
    /// The accepted locators are listed below.
    ///
    /// - `serial://port[:target_bps[:default_bps]]`, with both rates defaulting
    ///   to 115200. The `serial://` prefix may be omitted for `/dev/...` paths
    ///   on Unix and `COM...` ports on Windows. Requires the `serial` feature.
    /// - `tcp://address[:port]`, with `tcp4://` and `tcp6://` variants.
    /// - `udp://address[:port]`, with `udp4://` and `udp6://` variants.
    /// - `auto`, or an empty `url`, for the default device that
    ///   [`connect`](Self::connect) opens.
    ///
    /// TCP and UDP use port 7855 when no port is given. A proxy built with
    /// the `mdns` feature resolves a host name that ends in `.local` over
    /// mDNS. It falls back to the system's resolver when mDNS finds no
    /// address within 1.5 s.
    ///
    /// A TCP URL whose host is this machine, such as `tcp://localhost`,
    /// names a running proxy, of tio 2.0 or older. The connection reaches it
    /// directly, with no handshake, and gets whatever that proxy resolves
    /// `auto` to. No proxy is started for it.
    ///
    /// The `TWINLEAF_PROXY` environment variable, when set and not empty,
    /// names a different proxy as `tcp://host:port`. For example,
    /// `TWINLEAF_PROXY=tcp://127.0.0.1:17855` goes through a
    /// `tio proxy --port 17855` or a proxy an app hosts on that port. A proxy
    /// named this way is not started or embedded.
    ///
    /// `TWINLEAF_TIO` names the `tio` program to start, in place of the one on
    /// the `PATH`. A background proxy logs to `proxy.log` in
    /// `$XDG_RUNTIME_DIR/twinleaf`, or in `twinleaf/runtime` under the local
    /// data directory. `TWINLEAF_RUNTIME_DIR` names another directory.
    ///
    /// # Lifetime
    ///
    /// The connection does not reconnect. A device that drops out does not
    /// end it. The proxy reconnects the device, and the views see each change
    /// as a [`LinkEvent::Status`](crate::LinkEvent::Status) on
    /// [`events`](Self::events).
    ///
    /// The connection ends when its socket to the proxy closes, or when the
    /// proxy stops, as `tio proxy` does at Ctrl-C. Then every subscription
    /// ends and every RPC fails. At a stop the views first see a
    /// [`Stopped`](crate::tio::packet::LinkStatus::Stopped) status, and
    /// subscriptions end with [`RecvError::Stopped`]. Any other end gives
    /// [`RecvError::Disconnected`].
    ///
    /// # Errors
    ///
    /// Returns an error in these cases.
    ///
    /// - The proxy cannot be reached, started, or hosted.
    /// - The proxy refuses to open the device, or does not open it within
    ///   15 s. A refusal carries the proxy's reason, such as the candidates
    ///   an ambiguous `auto` could mean.
    /// - The peer that answers is not a Twinleaf 2.0 proxy, such as a 1.x
    ///   `tio proxy`. The message names the loopback URL that reaches it.
    /// - Nothing answers at the proxy `TWINLEAF_PROXY` names. The message
    ///   names the variable.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use twinleaf::{Connection, DeviceRoute};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let connection = Connection::open("serial:///dev/ttyACM0")?;
    /// let device = connection.device(DeviceRoute::root());
    /// let serial: String = device.get("dev.serial")?;
    /// println!("opened {serial}");
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(url: &str) -> std::io::Result<Connection> {
        Self::open_at(&ProxyAddress::chosen(), url, Purpose::Use, OPEN_TIMEOUT)
    }

    /// Opens `url` through `proxy` for `purpose`, waiting at most
    /// `open_timeout` for the proxy to open it. A TCP URL on this machine is
    /// reached directly.
    pub(crate) fn open_at(
        proxy: &ProxyAddress,
        url: &str,
        purpose: Purpose,
        open_timeout: Duration,
    ) -> std::io::Result<Connection> {
        let (proxy, url) = (proxy.clone(), url.to_string());
        Supervisor::new().run("twinleaf connection open", async move {
            Self::open_at_async(&proxy, &url, purpose, open_timeout).await
        })
    }

    /// Opens `url` as [`open_at`](Self::open_at) does, without blocking.
    /// Must be awaited on a tokio runtime.
    pub(crate) async fn open_at_async(
        proxy: &ProxyAddress,
        url: &str,
        purpose: Purpose,
        open_timeout: Duration,
    ) -> std::io::Result<Connection> {
        let session = match is_local_tcp(url) {
            true => {
                let url = url.to_string();
                let direct = Supervisor::new().spawn_blocking(move || Session::direct(&url));
                direct.await.map_err(std::io::Error::other)??
            }
            false => Session::open_async(proxy, url, purpose, open_timeout).await?,
        };
        Ok(Self::over(session))
    }

    /// Creates a connection over `session`.
    pub(crate) fn over(session: Session) -> Connection {
        let root = Handle::root(session);
        Connection {
            tree: DeviceTree {
                stream: Arc::new(Stream::new(root.clone())),
                handle: root,
            },
        }
    }

    /// Returns the view of `route` and every route below it.
    ///
    /// # Panics
    ///
    /// Never panics. The connection covers every route.
    pub fn tree(&self, route: DeviceRoute) -> DeviceTree {
        DeviceTree {
            handle: self
                .tree
                .handle
                .subtree(route)
                .expect("every route is under the connection's root"),
            stream: self.tree.stream.clone(),
        }
    }

    /// Returns the device at `route`, the view of that one route.
    ///
    /// # Panics
    ///
    /// Never panics. The connection covers every route.
    pub fn device(&self, route: DeviceRoute) -> Device {
        self.tree
            .device(route)
            .expect("every route is under the connection's root")
    }

    /// Subscribes to the sample batches of every route. See
    /// [`DeviceTree::samples`].
    pub fn samples(&self) -> Receiver<SampleBatch> {
        self.tree.samples()
    }

    /// Subscribes to the events of every route. See [`DeviceTree::events`].
    pub fn events(&self) -> Receiver<Event> {
        self.tree.events()
    }

    /// Subscribes to every packet on the connection. See
    /// [`DeviceTree::packets`].
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.tree.packets()
    }
}

/// A routed view of a device tree.
///
/// Its RPCs take a route. A [`Device`] binds one. Subscriptions are filtered
/// to this subtree and carry absolute routes. Cloning is cheap, and every
/// clone is the same view.
///
/// ```no_run
/// use twinleaf::{Connection, DeviceRoute};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let root = DeviceRoute::root();
/// let connection = Connection::connect()?;
/// let tree = connection.tree(root);
/// let name: String = tree.get(root, "dev.name")?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct DeviceTree {
    handle: Handle,
    stream: Arc<Stream>,
}

impl std::fmt::Debug for DeviceTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceTree")
            .field("route", &self.handle.scope())
            .field("depth", &self.handle.depth())
            .finish_non_exhaustive()
    }
}

impl DeviceTree {
    /// Returns the device at `route`. Its first RPC learns whether a device
    /// is there.
    ///
    /// # Errors
    ///
    /// Returns [`RouteError::OutsideSubtree`] if `route` lies outside this
    /// view.
    pub fn device(&self, route: DeviceRoute) -> Result<Device, RouteError> {
        Ok(Device {
            tree: DeviceTree {
                handle: self.handle.scoped(route)?,
                stream: self.stream.clone(),
            },
        })
    }

    /// Returns the same view, reaching no deeper than `depth` levels below
    /// its root. A `depth` beyond the view's own has no effect.
    pub fn to_depth(&self, depth: usize) -> DeviceTree {
        DeviceTree {
            handle: self.handle.to_depth(depth),
            stream: self.stream.clone(),
        }
    }

    /// Returns the same view, with each RPC giving up after `rpc_timeout`.
    ///
    /// The timeout is clamped to between 100 ms and 60 s. The default is 3 s.
    /// Metadata discovery keeps its own timeout.
    pub fn with_timeout(&self, rpc_timeout: Duration) -> DeviceTree {
        DeviceTree {
            handle: self.handle.with_timeout(rpc_timeout),
            stream: self.stream.clone(),
        }
    }

    /// Subscribes to the subtree's sample batches, each tagged with the route
    /// it came from.
    ///
    /// The first subscription on the connection starts its stream task.
    /// The receiver queues up to 1024 batches. Batches that arrive while it
    /// is full are dropped, and their count arrives as
    /// [`RecvError::Lagged`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use twinleaf::{Connection, DeviceRoute, RecvError};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let connection = Connection::connect()?;
    /// let samples = connection.tree(DeviceRoute::root()).samples();
    /// loop {
    ///     let batch = match samples.recv() {
    ///         Ok(batch) => batch,
    ///         Err(RecvError::Lagged(skipped)) => {
    ///             eprintln!("skipped {skipped} batches");
    ///             continue;
    ///         }
    ///         Err(error) => return Err(error.into()),
    ///     };
    ///     for row in batch.iter() {
    ///         let values: Vec<String> = row.values().map(|v| v.to_string()).collect();
    ///         println!("{} {}: {}", batch.route(), batch.stream().name, values.join(" "));
    ///     }
    /// }
    /// # }
    /// ```
    pub fn samples(&self) -> Receiver<SampleBatch> {
        self.stream.batches(Scope::of(&self.handle))
    }

    /// Subscribes to the subtree's events. A new subscriber first receives
    /// the routes, link statuses, and metadata already known.
    pub fn events(&self) -> Receiver<Event> {
        self.stream.events(Scope::of(&self.handle))
    }

    /// Subscribes to the subtree's packets before they are parsed.
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.stream.packets(Scope::of(&self.handle))
    }

    /// Returns the session handle behind this view.
    pub(super) fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Collects complete metadata for `route`, waiting up to 10 s.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::Route`] at once if `route` lies outside this
    /// view. Returns [`MetadataError::Unsupported`] if the device has no
    /// `dev.metadata`, [`MetadataError::Timeout`] if discovery does not
    /// finish in time, and [`MetadataError::Disconnected`] if the connection
    /// ends.
    pub fn metadata(&self, route: DeviceRoute) -> Result<DeviceMetadataSnapshot, MetadataError> {
        self.handle.scoped(route)?;
        let events = self.events();
        let deadline = Instant::now() + METADATA_TIMEOUT;
        loop {
            let event = match events.recv_deadline(deadline) {
                Ok(event) => event,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Timeout) => return Err(MetadataError::Timeout),
                Err(RecvError::Disconnected | RecvError::Stopped) => {
                    return Err(MetadataError::Disconnected)
                }
            };
            match event {
                Event::Device { route: from, event } if from == route => match event {
                    DeviceEvent::Metadata(snapshot) => return Ok(snapshot),
                    DeviceEvent::MetadataUnavailable => return Err(MetadataError::Unsupported),
                    DeviceEvent::Heartbeat { .. }
                    | DeviceEvent::NewHash(_)
                    | DeviceEvent::Setting { .. } => {}
                },
                Event::Device { .. } | Event::RouteDiscovered { .. } | Event::Link { .. } => {}
            }
        }
    }

    /// Listens to the subtree for `window` and returns the routes heard,
    /// sorted. Sends nothing to the devices.
    pub fn discover_routes(&self, window: Duration) -> Vec<DeviceRoute> {
        let events = self.events();
        let deadline = Instant::now() + window;
        let mut routes = Vec::new();
        loop {
            match events.recv_deadline(deadline) {
                Ok(Event::RouteDiscovered { route }) => routes.push(route),
                Ok(Event::Device { .. }) | Ok(Event::Link { .. }) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Timeout | RecvError::Disconnected | RecvError::Stopped) => break,
            }
        }
        routes.sort();
        routes
    }

    /// Discovers the subtree's routes as [`discover_routes`](Self::discover_routes)
    /// does and pairs each with its `dev.name`. The name is `None` when the
    /// device did not answer.
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

    /// Submits an RPC at `route` without waiting for its reply.
    ///
    /// The returned [`PendingReply`] resolves exactly once. A route outside
    /// this view resolves it with [`CallError::InvalidRoute`].
    pub fn submit(&self, route: DeviceRoute, name: &str, arg: &[u8]) -> PendingReply {
        PendingReply {
            reply: self.handle.submit(route, name, arg),
        }
    }

    /// Calls `name` at `route` with encoded arguments and returns the raw
    /// reply. Blocks until the reply arrives or the view's timeout passes.
    ///
    /// # Errors
    ///
    /// Returns a [`CallError`] if `route` lies outside this view, the request
    /// cannot be sent, no reply arrives in time, the connection ends, or the
    /// device refuses the call.
    pub fn raw_rpc(
        &self,
        route: DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<Vec<u8>, CallError> {
        self.submit(route, name, arg).wait()
    }

    /// Calls `name` at `route` with typed arguments and decodes its reply.
    /// Blocks until the reply arrives or the view's timeout passes.
    ///
    /// # Errors
    ///
    /// Returns a [`CallError`] for any failure of [`raw_rpc`](Self::raw_rpc),
    /// or [`CallError::InvalidReply`] if the reply does not decode as `RepT`.
    pub fn rpc<ReqT: RpcArgs, RepT: RpcReply>(
        &self,
        route: DeviceRoute,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, CallError> {
        let ret = self.raw_rpc(route, name, &arg.encode_args())?;
        RepT::decode_reply(&ret).map_err(CallError::InvalidReply)
    }

    /// Calls the no-argument RPC `name` at `route` and expects no value.
    ///
    /// # Errors
    ///
    /// Fails as [`rpc`](Self::rpc) does.
    pub fn action(&self, route: DeviceRoute, name: &str) -> Result<(), CallError> {
        self.rpc(route, name, ())
    }

    /// Calls the no-argument RPC `name` at `route` and decodes its reply as
    /// `T`.
    ///
    /// # Errors
    ///
    /// Fails as [`rpc`](Self::rpc) does.
    pub fn get<T: RpcReply>(&self, route: DeviceRoute, name: &str) -> Result<T, CallError> {
        self.rpc(route, name, ())
    }

    /// Returns the RPCs the device at `route` offers.
    ///
    /// The table comes from the on-disk cache when the device's `rpc.hash`
    /// matches it. On a miss it is read with `rpc.listinfo` and cached. A
    /// cache that cannot be read or written is logged and skipped.
    ///
    /// # Errors
    ///
    /// Returns [`RpcRegistryError::DeviceRpcError`] if a call fails, and
    /// [`RpcRegistryError::TableChanged`] if the table changed during every walk.
    pub fn rpc_registry(&self, route: DeviceRoute) -> Result<RpcRegistry, RpcRegistryError> {
        RpcRegistry::load_with(|name, arg| self.submit(route, name, arg))
    }

    /// Returns the type, access, and flags of the RPC `name` at `route`, as
    /// the device's `rpc.info` reports them. Reads only that one entry.
    ///
    /// # Errors
    ///
    /// Fails as [`rpc`](Self::rpc) does. A device that has no RPC `name`
    /// refuses with [`CallError::DeviceError`].
    pub fn rpc_meta(&self, route: DeviceRoute, name: &str) -> Result<RpcMeta, CallError> {
        self.rpc(route, "rpc.info", name).map(RpcMeta::from_bits)
    }
}

/// One device with its route bound, from [`Connection::device`] or
/// [`DeviceTree::device`].
///
/// Creating one is cheap and does not contact the device. Its first
/// operation learns whether the device exists. Cloning is cheap, and every
/// clone is the same view.
///
/// ```no_run
/// use twinleaf::{Connection, DeviceRoute};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let connection = Connection::connect()?;
/// let device = connection.device(DeviceRoute::root());
/// let name: String = device.get("dev.name")?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Device {
    tree: DeviceTree,
}

impl Device {
    fn route(&self) -> DeviceRoute {
        self.tree.handle.scope()
    }

    /// Subscribes to this device's sample batches. See
    /// [`DeviceTree::samples`].
    pub fn samples(&self) -> Receiver<SampleBatch> {
        self.tree.samples()
    }

    /// Subscribes to this device's events and the events of the links that
    /// serve it. See [`DeviceTree::events`].
    pub fn events(&self) -> Receiver<Event> {
        self.tree.events()
    }

    /// Subscribes to this device's packets. See [`DeviceTree::packets`].
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.tree.packets()
    }

    /// Collects this device's complete metadata, waiting up to 10 s.
    ///
    /// # Errors
    ///
    /// Fails as [`DeviceTree::metadata`] does.
    pub fn metadata(&self) -> Result<DeviceMetadataSnapshot, MetadataError> {
        self.tree.metadata(self.route())
    }

    /// Returns the same device, with each RPC giving up after `rpc_timeout`.
    /// See [`DeviceTree::with_timeout`].
    pub fn with_timeout(&self, rpc_timeout: Duration) -> Device {
        Device {
            tree: self.tree.with_timeout(rpc_timeout),
        }
    }

    /// Submits an RPC without waiting for its reply. See
    /// [`DeviceTree::submit`].
    pub fn submit(&self, name: &str, arg: &[u8]) -> PendingReply {
        self.tree.submit(self.route(), name, arg)
    }

    /// Calls `name` with encoded arguments and returns the raw reply. Blocks
    /// until the reply arrives or the timeout passes.
    ///
    /// # Errors
    ///
    /// Fails as [`DeviceTree::raw_rpc`] does.
    pub fn raw_rpc(&self, name: &str, arg: &[u8]) -> Result<Vec<u8>, CallError> {
        self.tree.raw_rpc(self.route(), name, arg)
    }

    /// Calls `name` with typed arguments and decodes its reply. Blocks until
    /// the reply arrives or the timeout passes.
    ///
    /// The argument is any [`RpcArgs`], such as `()`, a scalar, a string, or
    /// a tuple. The reply type is any [`RpcReply`].
    ///
    /// # Errors
    ///
    /// Returns a [`CallError`] if the request cannot be sent, no reply arrives
    /// in time, the connection ends, the device refuses the call, or the
    /// reply does not decode as `RepT`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use twinleaf::{Connection, DeviceRoute};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let connection = Connection::connect()?;
    /// let device = connection.device(DeviceRoute::root());
    ///
    /// let name: String = device.get("dev.name")?;
    /// let decimation: u32 = device.rpc("aux.data.decimation", 10u32)?;
    /// println!("{name} now decimates by {decimation}");
    /// device.action("dev.stop")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn rpc<ReqT: RpcArgs, RepT: RpcReply>(
        &self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, CallError> {
        self.tree.rpc(self.route(), name, arg)
    }

    /// Calls the no-argument RPC `name` and expects no value.
    ///
    /// # Errors
    ///
    /// Fails as [`rpc`](Self::rpc) does.
    pub fn action(&self, name: &str) -> Result<(), CallError> {
        self.tree.action(self.route(), name)
    }

    /// Calls the no-argument RPC `name` and decodes its reply as `T`.
    ///
    /// # Errors
    ///
    /// Fails as [`rpc`](Self::rpc) does.
    pub fn get<T: RpcReply>(&self, name: &str) -> Result<T, CallError> {
        self.tree.get(self.route(), name)
    }

    /// Returns the RPCs this device offers. See [`DeviceTree::rpc_registry`].
    ///
    /// # Errors
    ///
    /// Fails as [`DeviceTree::rpc_registry`] does.
    pub fn rpc_registry(&self) -> Result<RpcRegistry, RpcRegistryError> {
        self.tree.rpc_registry(self.route())
    }

    /// Returns the type, access, and flags of this device's RPC `name`. See
    /// [`DeviceTree::rpc_meta`].
    ///
    /// # Errors
    ///
    /// Fails as [`DeviceTree::rpc_meta`] does.
    pub fn rpc_meta(&self, name: &str) -> Result<RpcMeta, CallError> {
        self.tree.rpc_meta(self.route(), name)
    }

    /// Re-reads the settings in `cache` if this device's `settings.version`
    /// differs from the count of announcements `cache` has applied.
    ///
    /// Returns `true` if it re-read them. A device without the counter is
    /// not read, and gives `false`. Apply every announcement already received
    /// before calling. An announcement received before the call but applied
    /// after it is counted twice.
    ///
    /// # Errors
    ///
    /// Returns a [`CallError`] if reading `settings.version` or any setting
    /// fails. The cache then keeps its old values and count.
    pub fn resync_settings(&self, cache: &mut SettingsCache) -> Result<bool, CallError> {
        cache.resync_with(|name, arg| self.submit(name, arg))
    }

    /// Returns a test device at the root, over a session whose socket is a
    /// pair of channels, and the session's far end.
    #[cfg(test)]
    pub(crate) fn test_pair() -> (Device, super::session::TestProxy) {
        let (session, proxy) = Session::test_pair();
        let connection = Connection::over(session);
        (connection.device(DeviceRoute::root()), proxy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::fixtures::{self, open_through, FakeDevice, WAIT};
    use crate::device::rpc::RpcArgs;
    use crate::device::session::TestProxy;
    use crate::device::LinkEvent;
    use crate::proto::rpc as wire_rpc;
    use crate::proxy::{call, Proxy};
    use crate::tio::packet::LinkStatus;
    use std::thread;

    fn test_tree() -> (DeviceTree, TestProxy) {
        let (device, proxy) = Device::test_pair();
        (device.tree, proxy)
    }

    /// A tree at `route`, reaching `depth` below it, over a session whose
    /// socket is a pair of channels.
    fn tree_at(route: DeviceRoute, depth: usize) -> (DeviceTree, TestProxy) {
        let (session, proxy) = Session::test_pair();
        let tree = Connection::over(session).tree(route).to_depth(depth);
        (tree, proxy)
    }

    /// A view may not mint one that reaches further than itself: containment is
    /// checked here, existence at the first RPC.
    #[test]
    fn a_device_must_lie_within_the_trees_scope_and_depth() {
        let route = |text: &str| text.parse::<DeviceRoute>().expect("a valid route");
        let (tree, _proxy) = tree_at(route("/1"), 1);

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
        let (tree, _proxy) = tree_at(route("/1"), 1);

        assert!(matches!(
            tree.metadata(route("/2")),
            Err(MetadataError::Route(RouteError::OutsideSubtree))
        ));
    }

    #[test]
    fn a_blocking_rpc_waits_on_its_own_completion() {
        let (tree, proxy) = test_tree();
        let responder = thread::spawn(move || {
            let (route, name, _, id) = proxy.call();
            assert_eq!(name, "dev.name");
            proxy.reply(id, b"ASM", route);
            proxy
        });

        let name: String = tree.get(DeviceRoute::root(), "dev.name").unwrap();
        assert_eq!(name, "ASM");
        responder.join().unwrap();
    }

    #[test]
    fn concurrent_typed_rpcs_complete_out_of_order() {
        let (tree, proxy) = test_tree();
        let responder = thread::spawn(move || {
            let in_flight: Vec<_> = (0..2).map(|_| proxy.call()).collect();
            for (route, name, _, id) in in_flight.into_iter().rev() {
                proxy.reply(id, name.as_bytes(), route);
            }
            proxy
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
        let (tree, proxy) = test_tree();
        let tree = tree.with_timeout(Duration::from_millis(150));
        let responder = thread::spawn(move || {
            let (route, _, _, id) = proxy.call();
            proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
            // The second call goes unanswered, and the third is lost with
            // the socket.
            proxy.call();
            proxy.call();
        });

        let refused = tree.raw_rpc(DeviceRoute::root(), "dev.name", b"");
        assert!(matches!(
            refused,
            Err(CallError::DeviceError(payload))
                if matches!(payload.error, wire_rpc::RpcError::NotFound)
        ));
        assert!(matches!(
            tree.raw_rpc(DeviceRoute::root(), "dev.name", b""),
            Err(CallError::Timeout)
        ));
        let lost = tree.raw_rpc(DeviceRoute::root(), "dev.name", b"");
        assert!(matches!(lost, Err(CallError::ResponseLost)), "{lost:?}");
        responder.join().unwrap();
    }

    /// An RPC's metadata is what `rpc.info` answers for its name, and a
    /// device that does not know the name refuses it.
    #[test]
    fn rpc_meta_asks_rpc_info_by_name() {
        let (device, proxy) = Device::test_pair();
        let responder = thread::spawn(move || {
            let (route, method, arg, id) = proxy.call();
            assert_eq!(
                (method.as_str(), arg.as_slice()),
                ("rpc.info", &b"test.enable"[..])
            );
            proxy.reply(id, &0x8b10u16.to_le_bytes(), route);
            let (route, _, _, id) = proxy.call();
            proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
        });

        let meta = device.rpc_meta("test.enable").unwrap();
        assert_eq!(meta, wire_rpc::RpcMeta::from_bits(0x8b10));
        assert!(matches!(
            device.rpc_meta("no.such.rpc"),
            Err(CallError::DeviceError(_))
        ));
        responder.join().unwrap();
    }

    fn never() -> crate::rt::BoxFuture<'static, std::io::Result<()>> {
        panic!("a local TCP URL went looking for the proxy")
    }

    /// The name of the root device of `connection`.
    fn root_name(connection: &Connection) -> String {
        connection
            .device(DeviceRoute::root())
            .get("dev.name")
            .unwrap()
    }

    /// Whether this process opened `target` through `proxy`.
    fn opened(proxy: &Proxy, target: &str) -> bool {
        proxy
            .clients()
            .iter()
            .any(|client| client.own_process && client.target.as_deref() == Some(target))
    }

    /// A connection the proxy opened its device for reaches the device.
    #[test]
    fn a_connection_reaches_its_device_through_the_proxy() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();

        let connection = open_through(&proxy, &device.url).unwrap();

        assert_eq!(root_name(&connection), "FAKE");
        assert!(opened(&proxy, &device.url));
    }

    /// The call's own timeout decides how long a device may take, so a reply
    /// within it arrives through the proxy however long it took.
    #[test]
    fn a_slow_reply_within_the_calls_timeout_arrives() {
        let device = FakeDevice::start();
        device.delay_replies(Duration::from_millis(2500));
        let proxy = fixtures::proxy();

        let connection = open_through(&proxy, &device.url).unwrap();

        assert_eq!(root_name(&connection), "FAKE");
    }

    /// A TCP URL on this machine names a proxy already, of tio 2.0 or older,
    /// so it is connected to as given.
    #[test]
    fn a_local_tcp_url_connects_directly() {
        let device = FakeDevice::start();

        let machine = ProxyAddress {
            url: "tcp://192.0.2.1:9".to_string(),
            ensure: Some(never),
        };

        let connection = Connection::open_at(&machine, &device.url, Purpose::Use, WAIT).unwrap();

        assert_eq!(root_name(&connection), "FAKE");
    }

    /// An `auto` that could mean several devices fails with the proxy's own
    /// words, which name each candidate.
    #[test]
    fn an_ambiguous_auto_fails_with_the_proxys_message() {
        let devices = [FakeDevice::start(), FakeDevice::start()];
        let proxy = fixtures::proxy();
        let _connections: Vec<Connection> = devices
            .iter()
            .map(|device| open_through(&proxy, &device.url).unwrap())
            .collect();
        if fixtures::attached() == 1 {
            eprintln!("skipped: the one device attached makes auto unambiguous");
            return;
        }
        let resolved = call(&proxy.endpoint(), "proxy.open", &"auto".encode_args(), WAIT);
        let message = resolved.expect_err("the proxy resolved auto").to_string();

        for target in ["auto", ""] {
            let opened = open_through(&proxy, target);
            let error = opened.expect_err("an ambiguous auto opens nothing");
            assert_eq!(error.to_string(), message);
        }
        for device in &devices {
            assert!(message.contains(&device.url), "{message}");
        }
    }

    /// A connection whose proxy stops hears the stop, then ends with every
    /// subscription saying so, and never starts a proxy in its place.
    #[test]
    fn a_connection_ends_when_its_proxy_stops() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy_serving(&device.url);
        let connection = open_through(&proxy, "auto").unwrap();
        let events = connection.events();
        assert_eq!(root_name(&connection), "FAKE");

        proxy.shutdown();

        let deadline = Instant::now() + WAIT;
        let mut stopped = false;
        loop {
            match events.recv_deadline(deadline) {
                Ok(Event::Link {
                    event: LinkEvent::Status(LinkStatus::Stopped),
                    ..
                }) => stopped = true,
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Stopped) => break,
                Err(RecvError::Disconnected) => panic!("the events ended with no stop"),
                Err(RecvError::Timeout) => panic!("the connection outlived its proxy"),
            }
        }
        assert!(stopped, "the stop never reached the events");
        assert!(matches!(
            connection
                .device(DeviceRoute::root())
                .get::<String>("dev.name"),
            Err(CallError::ResponseLost)
        ));
    }

    /// A sample stream carries no statuses, yet through a proxy that stops it
    /// ends saying so.
    #[test]
    fn a_sample_stream_through_a_proxy_that_stops_says_so() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy_serving(&device.url);
        let connection = open_through(&proxy, "auto").unwrap();
        let samples = connection.tree(DeviceRoute::root()).samples();
        assert_eq!(root_name(&connection), "FAKE");

        proxy.shutdown();

        let deadline = Instant::now() + WAIT;
        let ended = loop {
            match samples.recv_deadline(deadline) {
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(error) => break error,
            }
        };
        assert_eq!(ended, RecvError::Stopped);
        assert_eq!(ended.to_string(), "the proxy stopped");
    }

    /// Point the variables a proxy is found or started by at `dir`, where
    /// no `tio` can be started and where a proxy started anyway would log.
    fn no_tio_in(dir: &std::path::Path) {
        std::env::set_var("TWINLEAF_TIO", dir.join("no-such-tio"));
        std::env::set_var("TWINLEAF_RUNTIME_DIR", dir);
    }

    /// Clear the variables these tests set.
    fn clear_variables() {
        for variable in ["TWINLEAF_TIO", "TWINLEAF_RUNTIME_DIR", "TWINLEAF_PROXY"] {
            std::env::remove_var(variable);
        }
    }

    /// Fail when a proxy was started, and logs in `dir`.
    fn assert_none_started(dir: &std::path::Path) {
        assert!(
            std::fs::read_dir(dir).unwrap().next().is_none(),
            "a proxy was spawned, and logs in {}",
            dir.display()
        );
    }

    /// `TWINLEAF_PROXY` names the proxy a connection goes through, here a
    /// proxy on a port of its own, which opens what the connection asks for.
    #[test]
    fn twinleaf_proxy_names_the_proxy_a_connection_goes_through() {
        let _environment = fixtures::ENVIRONMENT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let device = FakeDevice::start();
        let proxy = fixtures::proxy_serving(&device.url);
        let dir = tempfile::tempdir().unwrap();
        no_tio_in(dir.path());

        std::env::set_var("TWINLEAF_PROXY", proxy.endpoint());
        let named = Connection::open("auto");

        clear_variables();
        assert_eq!(root_name(&named.unwrap()), "FAKE");
        assert!(opened(&proxy, "default"));
        assert_none_started(dir.path());
    }

    /// A connection through a proxy that `TWINLEAF_PROXY` names and that does
    /// not answer fails at once, saying what named it, and never starts or
    /// embeds one in its place.
    #[test]
    fn a_connection_through_a_proxy_that_does_not_answer_starts_none() {
        let _environment = fixtures::ENVIRONMENT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        no_tio_in(dir.path());
        let closed = fixtures::ClosedPort::new();
        let (port, proxy) = (closed.port(), closed.url());

        std::env::set_var("TWINLEAF_PROXY", &proxy);
        let started = Instant::now();
        let opened = Connection::open("auto");

        clear_variables();
        let Err(error) = opened else {
            panic!("a connection opened through {proxy}, where nothing answers");
        };
        assert!(started.elapsed() < WAIT, "it took {:?}", started.elapsed());
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionRefused);
        assert_eq!(
            error.to_string(),
            format!("no proxy answers at {proxy} (TWINLEAF_PROXY)")
        );
        assert!(
            std::net::TcpStream::connect(("127.0.0.1", port)).is_err(),
            "a proxy was started at {proxy}"
        );
        assert_none_started(dir.path());
    }
}

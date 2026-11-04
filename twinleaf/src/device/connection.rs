//! The [`Connection`] to a proxy and its [`DeviceTree`] and [`Device`] views.

use super::runtime::chosen_proxy;
use super::stream::Stream;
use crate::data::{DeviceMetadataSnapshot, SampleBatch};
use crate::proto::DeviceRoute;
use crate::proto::RouteError;
use crate::rt::Supervisor;
use crate::session::rpc::{CallError, PendingReply, RpcMeta, RpcRegistry, RpcRegistryError};
use crate::session::{Field, CHANGES, METADATA, PRESENCE, REGISTRY, SETTINGS};
use crate::session::{Handle, Session};
use crate::session::{Layout, ProxyAddress, Purpose, Receiver, RecvError, Scope};
use crate::session::{MetadataState, Presence, RegistryState, SettingChange, Settings};
use crate::tio;
use crate::tio::rpc::{RpcArgs, RpcReply};
use crate::tio::transport::is_local_tcp;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long [`Device::metadata`] waits for discovery to finish, across all
/// its `dev.metadata` round trips, and [`Device::presence`] and
/// [`DeviceTree::routes`] wait for the proxy's first word.
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// How long [`Device::rpc_registry`] and [`Device::settings`] wait for the
/// table to load and the settings to be read.
const TABLE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`Connection::open`] waits for the proxy to open a device. The
/// proxy itself gives a device 10 s to answer.
const OPEN_TIMEOUT: Duration = Duration::from_secs(15);

/// Why [`Device::metadata`] returned no metadata.
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
    /// `twinleaf-tools`, and that proxy exits five seconds after its last
    /// client leaves. When `tio` cannot be started, or is too old to run the
    /// proxy, this process hosts the proxy itself. A sandboxed macOS app
    /// hosts the proxy itself without starting `tio`.
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
    /// A TCP URL whose host is this machine, such as `tcp://localhost:17855`,
    /// names a running proxy of this build. The connection goes through that
    /// proxy and opens what it resolves `auto` to. No proxy is started for
    /// it.
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
    /// The connection's tasks run on the crate's own runtime. This may be
    /// called from inside an application's tokio runtime, and blocks that
    /// thread until the proxy has opened the device or the open fails.
    ///
    /// # Lifetime
    ///
    /// A device that drops out does not end the connection. The proxy
    /// reconnects the device, and each change shows in the device's
    /// [`presence`](Device::presence).
    ///
    /// A socket to the proxy that closes without a stop, as when the proxy
    /// crashes, does not end it either. Every device is
    /// [`Lost`](Presence::Lost) and every RPC fails with
    /// [`CallError::ResponseLost`] until the connection is open again, and
    /// the raw [`packets`](DeviceTree::packets) carry a
    /// [`SensorDisconnected`](crate::tio::packet::LinkStatus::SensorDisconnected)
    /// status at the root. It opens the target that proxy resolved the first
    /// time again at the same address, never starting a proxy, and watches
    /// each device again, and the samples after begin a new run. A
    /// connection that cannot open again within 30 s ends.
    ///
    /// The connection ends when the proxy stops, as `tio proxy` does at
    /// Ctrl-C, and when the proxy gives a device up and closes the socket.
    /// Then every subscription ends and every RPC fails. At a stop the raw
    /// [`packets`](DeviceTree::packets) first carry a
    /// [`Stopped`](crate::tio::packet::LinkStatus::Stopped) status, and
    /// subscriptions end with [`RecvError::Stopped`]. After
    /// [`close`](Self::close) they end with [`RecvError::Closed`]. Any other
    /// end gives [`RecvError::Disconnected`].
    ///
    /// # Errors
    ///
    /// Returns an error in these cases.
    ///
    /// - The proxy cannot be reached, started, or hosted.
    /// - The proxy refuses to open the device, or does not open it within
    ///   15 s. A refusal carries the proxy's reason, such as the candidates
    ///   an ambiguous `auto` could mean.
    /// - The peer that answers gives no introduction to the hello, as a
    ///   `tio proxy` from an older build gives none. This build cannot reach
    ///   such a proxy.
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
        Self::open_at(&chosen_proxy(), url, Purpose::Use, OPEN_TIMEOUT)
    }

    /// Opens a connection that sees `layout` as one tree, each mount's
    /// subtree at its prefix. A client so names its own combination of
    /// devices, where [`connect`](Self::connect) takes the host's default.
    ///
    /// # Errors
    ///
    /// Returns an [`InvalidInput`](std::io::ErrorKind::InvalidInput) error if
    /// a mount's URL is longer than 255 bytes or the layout has more than 255
    /// mounts. Also fails with the proxy's message when it refuses the
    /// layout, such as one whose prefixes overlap, and in every way
    /// [`open`](Self::open) does.
    pub fn open_layout(layout: &Layout) -> std::io::Result<Connection> {
        fits_one_request(layout)?;
        Self::mount_at(&chosen_proxy(), layout, OPEN_TIMEOUT)
    }

    /// Makes `layout` the default of the machine's background proxy, the
    /// tree every client that names no device opens, until the returned
    /// connection closes. The connection sees the tree it holds, as one that
    /// opened it does. Starts the proxy when none runs.
    ///
    /// # Errors
    ///
    /// Fails as [`open_layout`](Self::open_layout) does, and with the proxy's
    /// message when a terminal or an app hosts it, or another client already
    /// holds its default.
    pub fn hold_default(layout: &Layout) -> std::io::Result<Connection> {
        fits_one_request(layout)?;
        Self::hold_default_at(&chosen_proxy(), layout, OPEN_TIMEOUT)
    }

    /// Holds `layout` as the default of `proxy`, waiting at most
    /// `open_timeout` for its answer.
    pub(crate) fn hold_default_at(
        proxy: &ProxyAddress,
        layout: &Layout,
        open_timeout: Duration,
    ) -> std::io::Result<Connection> {
        let (proxy, layout) = (proxy.clone(), layout.clone());
        let session = Supervisor::new().run("twinleaf connection default", async move {
            Session::hold_default_async(&proxy, &layout, open_timeout).await
        })?;
        Ok(Self::over(session))
    }

    /// Opens a connection through `proxy` that sees `layout`, waiting at most
    /// `open_timeout` for the proxy to open it.
    pub(crate) fn mount_at(
        proxy: &ProxyAddress,
        layout: &Layout,
        open_timeout: Duration,
    ) -> std::io::Result<Connection> {
        let (proxy, layout) = (proxy.clone(), layout.clone());
        let session = Supervisor::new().run("twinleaf connection mount", async move {
            Session::mount_async(&proxy, &layout, open_timeout).await
        })?;
        Ok(Self::over(session))
    }

    /// Closes the connection for every clone, view and subscription, however
    /// many remain. Subscriptions then end with [`RecvError::Closed`], and
    /// RPCs fail. Returns at once, and the socket closes soon after.
    ///
    /// Dropping the last handle on the connection closes it too. Close it to
    /// end it at a known moment, as an app does before it opens another
    /// layout.
    pub fn close(&self) {
        self.tree.handle.session().close();
    }

    /// Opens `url` through `proxy` for `purpose`, waiting at most
    /// `open_timeout` for the proxy to open it. A TCP URL on this machine
    /// names the proxy to open `auto` through instead.
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
                let named = ProxyAddress {
                    url: url.to_string(),
                    ensure: None,
                };
                let opened = Session::open_async(&named, "auto", Purpose::Use, open_timeout).await;
                opened.map_err(|error| match error.kind() {
                    std::io::ErrorKind::ConnectionRefused => {
                        std::io::Error::new(error.kind(), format!("no proxy answers at {url}"))
                    }
                    _ => error,
                })?
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

    /// Subscribes to every packet on the connection. See
    /// [`DeviceTree::packets`].
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.tree.packets()
    }
}

/// Returns an [`InvalidInput`](std::io::ErrorKind::InvalidInput) error if a
/// mount's URL is longer than 255 bytes or `layout` has more than 255 mounts.
fn fits_one_request(layout: &Layout) -> std::io::Result<()> {
    let too_long = layout
        .mounts
        .iter()
        .any(|mount| mount.url.len() > u8::MAX.into());
    match too_long || layout.mounts.len() > u8::MAX.into() {
        true => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("the layout {layout} does not fit one request"),
        )),
        false => Ok(()),
    }
}

/// Returns why an RPC table or settings were not received, as `error`
/// says.
fn unreceived(error: RecvError) -> RpcRegistryError {
    RpcRegistryError::DeviceRpcError(match error {
        RecvError::Timeout => CallError::Timeout,
        _ => CallError::ResponseLost,
    })
}

/// A discovered route and its device's `dev.name`, from
/// [`DeviceTree::named_routes`].
#[derive(Debug, Clone)]
pub struct NamedRoute {
    /// The route discovered.
    pub route: DeviceRoute,
    /// Its `dev.name`, when the device answered.
    pub name: Option<String>,
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
    /// Returns the subtree this view covers, as its stream subscribes to it.
    fn scope(&self) -> Scope {
        Scope {
            route: self.handle.scope(),
            depth: self.handle.depth(),
        }
    }

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
    /// [`RecvError::Lagged`]. The next batch of each stream that lost
    /// batches carries a [`Loss`](crate::data::Loss).
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
    ///         println!("{} {}: {}", batch.route(), batch.schema().stream().name(), values.join(" "));
    ///     }
    /// }
    /// # }
    /// ```
    pub fn samples(&self) -> Receiver<SampleBatch> {
        self.stream.batches(self.scope())
    }

    /// Subscribes to the subtree's packets before they are parsed.
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.stream.packets(self.scope())
    }

    /// Returns the routes the proxy has heard in the subtree so far, sorted,
    /// waiting up to 10 s for its answer. Sends nothing to the devices.
    pub fn routes(&self) -> Vec<DeviceRoute> {
        self.routes_updates()
            .recv_timeout(METADATA_TIMEOUT)
            .unwrap_or_default()
    }

    /// Subscribes to the routes the proxy has heard in the subtree. The
    /// receiver yields the sorted list of them once the proxy says it, then
    /// the whole list again each time a new route is heard. A reader that
    /// falls behind skips to the newest list.
    pub fn routes_updates(&self) -> Receiver<Vec<DeviceRoute>> {
        self.stream.routes(self.scope())
    }

    /// Collects complete metadata for `route`, waiting up to 10 s. See
    /// [`Device::metadata`].
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::Route`] at once if `route` lies outside this
    /// view. Otherwise fails as [`Device::metadata`] does.
    pub fn metadata(&self, route: DeviceRoute) -> Result<DeviceMetadataSnapshot, MetadataError> {
        self.device(route)?.metadata()
    }

    /// Listens to the subtree for `window` and returns the routes heard by
    /// then, sorted. Sends nothing to the devices.
    pub fn discover_routes(&self, window: Duration) -> Vec<DeviceRoute> {
        let updates = self.routes_updates();
        let deadline = Instant::now() + window;
        let mut routes = Vec::new();
        loop {
            match updates.recv_deadline(deadline) {
                Ok(heard) => routes = heard,
                Err(RecvError::Lagged(_)) => {}
                Err(
                    RecvError::Timeout
                    | RecvError::Disconnected
                    | RecvError::Stopped
                    | RecvError::Closed,
                ) => return routes,
            }
        }
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

    /// Returns the RPCs the device at `route` offers. See
    /// [`Device::rpc_registry`].
    ///
    /// # Errors
    ///
    /// Returns [`RpcRegistryError::DeviceRpcError`] with
    /// [`CallError::InvalidRoute`] if `route` lies outside this view.
    /// Otherwise fails as [`Device::rpc_registry`] does.
    pub fn rpc_registry(&self, route: DeviceRoute) -> Result<Arc<RpcRegistry>, RpcRegistryError> {
        let device = self.device(route).map_err(CallError::InvalidRoute)?;
        device.rpc_registry()
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

    /// Subscribes to this device's packets. See [`DeviceTree::packets`].
    pub fn packets(&self) -> Receiver<tio::Packet> {
        self.tree.packets()
    }

    /// Returns whether this device is there now, as the proxy says, waiting
    /// up to 10 s for its answer.
    ///
    /// The proxy follows each device from its packets and the statuses of
    /// the link that serves it, and every client watches what it found. A
    /// device not heard from yet, or one the proxy does not answer for in
    /// time, is [`Presence::Connecting`]. A device silent for 3 s is lost.
    /// Once the connection has ended, every device is [`Presence::Lost`].
    pub fn presence(&self) -> Presence {
        match self.presence_updates().recv_timeout(METADATA_TIMEOUT) {
            Ok(presence) => presence,
            Err(RecvError::Timeout) => Presence::Connecting,
            Err(_) => Presence::Lost {
                since: Instant::now(),
            },
        }
    }

    /// Subscribes to whether this device is there. The receiver yields the
    /// presence once the proxy says it, then each change. A heartbeat
    /// changes it only when it reports a new session. A reader that falls
    /// behind skips to the newest.
    pub fn presence_updates(&self) -> Receiver<Presence> {
        self.state(PRESENCE)
    }

    /// Collects this device's complete metadata, waiting up to 10 s.
    ///
    /// The proxy asks the device once, and every caller of every client of
    /// the proxy shares what it collected.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::Unsupported`] if the device has no
    /// `dev.metadata`, [`MetadataError::Timeout`] if discovery does not
    /// finish in time, and [`MetadataError::Disconnected`] if the connection
    /// ends.
    pub fn metadata(&self) -> Result<DeviceMetadataSnapshot, MetadataError> {
        let updates = self.metadata_updates();
        let deadline = Instant::now() + METADATA_TIMEOUT;
        loop {
            match updates.recv_deadline(deadline) {
                Ok(MetadataState::Ready(snapshot)) => return Ok(snapshot),
                Ok(MetadataState::Unsupported) => return Err(MetadataError::Unsupported),
                Ok(MetadataState::Pending) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Timeout) => return Err(MetadataError::Timeout),
                Err(RecvError::Disconnected | RecvError::Stopped | RecvError::Closed) => {
                    return Err(MetadataError::Disconnected)
                }
            }
        }
    }

    /// Subscribes to this device's metadata. The receiver yields how far it
    /// is collected now, then each change. Subscribing starts discovery. A
    /// reader that falls behind skips to the newest.
    pub fn metadata_updates(&self) -> Receiver<MetadataState> {
        self.state(METADATA)
    }

    /// Subscribes to the `field` of this device's state.
    fn state<T: Clone>(&self, field: Field<T>) -> Receiver<T> {
        self.tree.stream.state(self.route(), field)
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

    /// Returns the RPCs this device offers, waiting up to 30 s until the
    /// table is loaded.
    ///
    /// The proxy loads the table once, and every caller of every client of
    /// the proxy shares it. It comes from the on-disk cache when the device's `rpc.hash` matches it.
    /// On a miss it is read with `rpc.listinfo` and cached. A cache that
    /// cannot be read or written is logged and skipped. The table is loaded
    /// again when the device announces a new `rpc.hash`, starts a new
    /// session, or its link reconnects.
    ///
    /// # Errors
    ///
    /// Returns [`RpcRegistryError::DeviceRpcError`] if a call fails, with
    /// [`CallError::Timeout`] if the table is not loaded in time, and
    /// [`RpcRegistryError::TableChanged`] if the table changed during every
    /// walk. The proxy loads it again later.
    pub fn rpc_registry(&self) -> Result<Arc<RpcRegistry>, RpcRegistryError> {
        let registry = self.rpc_registry_updates().recv_timeout(TABLE_TIMEOUT);
        registry.map_err(unreceived)?
    }

    /// Subscribes to this device's RPC table. The receiver yields the table,
    /// or why it could not be loaded, once it is known, then each new one.
    /// Subscribing starts loading it. A reader that falls behind skips to the
    /// newest.
    pub fn rpc_registry_updates(&self) -> Receiver<RegistryState> {
        self.state(REGISTRY)
    }

    /// Returns the values of this device's announced settings, waiting up
    /// to 30 s until the RPC table is loaded and the values are read.
    ///
    /// The proxy reads them once and keeps them by the device's
    /// announcements, and every caller of every client of the proxy shares
    /// them. See [`Settings`].
    ///
    /// # Errors
    ///
    /// Fails as [`rpc_registry`](Self::rpc_registry) does, as the values
    /// cannot be read without the table.
    pub fn settings(&self) -> Result<Arc<Settings>, RpcRegistryError> {
        let (settings, deadline) = (self.settings_updates(), Instant::now() + TABLE_TIMEOUT);
        let registry = self.rpc_registry_updates().recv_deadline(deadline);
        registry.map_err(unreceived)??;
        settings.recv_deadline(deadline).map_err(unreceived)
    }

    /// Subscribes to the values of this device's announced settings. The
    /// receiver yields them once they are read, then again after each
    /// change. Subscribing starts loading the RPC table and reading them. A
    /// reader that falls behind skips to the newest values.
    pub fn settings_updates(&self) -> Receiver<Arc<Settings>> {
        self.state(SETTINGS)
    }

    /// Subscribes to every setting this device announces from now on, in
    /// the order it announces them. Each carries the RPC table it was
    /// recorded under, which decodes its value. Subscribing follows the
    /// table too, so the proxy loads the table and reads every setting once,
    /// as for [`settings_updates`](Self::settings_updates). The receiver
    /// queues up to 4096 of them, and one
    /// that falls behind gets [`RecvError::Lagged`]. Read
    /// [`settings`](Self::settings) then to catch up.
    pub fn setting_changes(&self) -> Receiver<SettingChange> {
        self.state(CHANGES)
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

    /// Returns a test device at the root, over a session whose socket is a
    /// pair of channels, and the session's far end.
    #[cfg(test)]
    pub(crate) fn test_pair() -> (Device, crate::session::TestProxy) {
        let (session, proxy) = Session::test_pair();
        let connection = Connection::over(session);
        (connection.device(DeviceRoute::root()), proxy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::tests::fixtures::{announcement, broadcast, metadata_records, samples};
    use crate::device::tests::fixtures::{self, open_through};
    use crate::proto::rpc as wire_rpc;
    use crate::proxy::Proxy;
    use crate::session::tests::wait_for;
    use crate::session::TestProxy;
    use crate::tio::tests::device::FakeDevice;
    use crate::tio::tests::ClosedPort;
    use crate::tio::tests::WAIT;
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

    /// A view may not mint one that reaches further than itself. Containment
    /// is checked here, and existence at the first RPC.
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

    /// A refusal, a call nothing answers, and a call lost with the socket
    /// each fail as their own error. Only the unanswered call has a short
    /// timeout, so the refusal and the loss never wait on it.
    #[test]
    fn raw_call_failures_map_to_typed_call_errors() {
        let (tree, proxy) = test_tree();
        let (patient, hasty) = (
            tree.with_timeout(WAIT * 2),
            tree.with_timeout(Duration::from_millis(150)),
        );
        let responder = thread::spawn(move || {
            let (route, _, _, id) = proxy.call();
            proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
            // The second call goes unanswered, and the third is lost with
            // the socket.
            proxy.call();
            proxy.call();
        });

        let refused = patient.raw_rpc(DeviceRoute::root(), "dev.name", b"");
        assert!(matches!(
            refused,
            Err(CallError::DeviceError(payload))
                if matches!(payload.error, wire_rpc::RpcError::NotFound)
        ));
        assert!(matches!(
            hasty.raw_rpc(DeviceRoute::root(), "dev.name", b""),
            Err(CallError::Timeout)
        ));
        let lost = patient.raw_rpc(DeviceRoute::root(), "dev.name", b"");
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

    /// The call's own timeout decides how long a device may take, so a reply
    /// within it arrives through the proxy however long it took. The call
    /// waits twice [`WAIT`], so a slow test thread never runs it out.
    #[test]
    fn a_slow_reply_within_the_calls_timeout_arrives() {
        let device = FakeDevice::start();
        device.delay_replies(Duration::from_millis(2500));
        let proxy = fixtures::proxy();

        let connection = open_through(&proxy, &device.url).unwrap();

        let root = connection
            .device(DeviceRoute::root())
            .with_timeout(WAIT * 2);
        assert_eq!(root.get::<String>("dev.name").unwrap(), "FAKE");
    }

    /// Closing one clone ends every subscription and view of the connection,
    /// though they outlive the clone, and the proxy lets the client go.
    #[test]
    fn a_close_ends_every_handle_on_the_connection() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();
        let connection = open_through(&proxy, &device.url).unwrap();
        let view = connection.device(DeviceRoute::root());
        let presence = view.presence_updates();
        assert_eq!(root_name(&connection), "FAKE");

        connection.clone().close();
        drop(connection);

        let deadline = Instant::now() + WAIT;
        let ended = loop {
            match presence.recv_deadline(deadline) {
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(ended) => break ended,
            }
        };
        assert_eq!(ended, RecvError::Closed);
        assert!(view.get::<String>("dev.name").is_err());
        while !proxy.clients().is_empty() {
            assert!(
                Instant::now() < deadline,
                "the proxy kept the closed client"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The routes of a tree come from the proxy, each as the client sees it
    /// through the mount that shows it.
    #[test]
    fn the_routes_through_a_mount_come_from_the_proxy() {
        let device = FakeDevice::start();
        let proxy = fixtures::proxy();
        let prefix: DeviceRoute = "/3".parse().unwrap();
        let mounted = Layout {
            mounts: vec![crate::session::Mount {
                url: device.url.clone(),
                source: DeviceRoute::root(),
                prefix,
            }],
        };
        let address = ProxyAddress {
            url: proxy.endpoint(),
            ensure: None,
        };
        let connection = Connection::mount_at(&address, &mounted, WAIT).unwrap();

        let routes = connection.tree(DeviceRoute::root()).routes_updates();
        let heard = wait_for(&routes, |routes| !routes.is_empty());
        assert_eq!(heard, [prefix]);
        assert_eq!(connection.tree(prefix).routes(), [prefix]);
    }

    /// A proxy whose link is another proxy keeps the state of the device
    /// behind it as of any device. Its clients share one discovery, one walk
    /// and one snapshot, which reach the device through the other proxy.
    #[test]
    fn clients_of_a_proxy_behind_a_proxy_share_its_state() {
        let device = FakeDevice::start();
        let upstream = fixtures::proxy_serving(&device.url);
        let downstream = fixtures::proxy();
        for _ in 0..2 {
            let connection = open_through(&downstream, &upstream.endpoint()).unwrap();
            let view = connection.device(DeviceRoute::root());
            let metadata = view.metadata().expect("the proxy collects the metadata");
            assert_eq!(metadata.device().name(), "FAKE");
            let settings = view.settings().expect("the proxy reads the settings");
            assert_eq!(settings.get("test.amplitude"), Some(&[1u8][..]));
        }

        let asked =
            ["dev.metadata", "rpc.listinfo", "test.amplitude"].map(|name| device.calls(name));
        assert_eq!(asked, [1, 1 + 2, 1]);
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
        let _environment = crate::chan::lock(&fixtures::ENVIRONMENT);
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
        let _environment = crate::chan::lock(&fixtures::ENVIRONMENT);
        let dir = tempfile::tempdir().unwrap();
        no_tio_in(dir.path());
        let closed = ClosedPort::new();
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

    /// Set in the process that counts the tasks left, so that no other test
    /// runs tasks beside the count.
    const COUNTING: &str = "TWINLEAF_COUNT_TASKS";

    /// No task is left once a connection and its proxy are dropped, in
    /// either order. The count runs in a process of its own.
    #[test]
    fn a_dropped_connection_and_proxy_leave_no_task() {
        let counted = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "device::connection::tests::tasks_left",
                "--test-threads=1",
            ])
            .env(COUNTING, "1")
            .status()
            .unwrap();
        assert!(counted.success());
    }

    /// Counts the tasks left after each order of drops, when run alone.
    #[test]
    fn tasks_left() {
        if std::env::var_os(COUNTING).is_none() {
            return;
        }
        let device = FakeDevice::start();
        for proxy_first in [false, true] {
            let proxy = fixtures::proxy();
            let connection = open_through(&proxy, &device.url).unwrap();
            let samples = connection.samples();
            assert_eq!(root_name(&connection), "FAKE");
            match proxy_first {
                true => drop((proxy, connection)),
                false => drop((connection, proxy)),
            }
            drop(samples);

            let deadline = Instant::now() + WAIT;
            while crate::rt::handle().metrics().num_alive_tasks() > 0 && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            let left = crate::rt::handle().metrics().num_alive_tasks();
            assert_eq!(
                left, 0,
                "tasks left with the proxy dropped first: {proxy_first}"
            );
        }
    }

    mod scope {
        //! Properties of the packets a view delivers.
        //!
        //! # Observations
        //!
        //! The packets each of several views' [`DeviceTree::packets`]
        //! receivers yields, in order, while the proxy sends packets from
        //! routes of 0 to 3 hops, inside the views and out of them. The views
        //! share one connection.
        //!
        //! # Sources
        //!
        //! The [`device`](crate::device) module doc, where a connection's
        //! scope is every route on the link and a tree at `/1` covers `/1`
        //! and its descendants. The doc of [`DeviceTree`], whose
        //! subscriptions are filtered to its subtree and carry absolute
        //! routes. The doc of [`DeviceTree::to_depth`], which reaches no
        //! deeper than `depth` levels below the view's root, and on which a
        //! depth beyond the view's own has no effect. The doc of
        //! [`DeviceTree::packets`].
        //!
        //! # Model
        //!
        //! A view at route `s` reaching `d` levels covers a route `r` when
        //! the hops of `s` begin the hops of `r` and `r` has at most `d` hops
        //! more. A view never narrowed reaches every depth, and a view
        //! narrowed several times reaches the least depth it was narrowed to.
        //! The model reads the hops alone, not the crate's own route checks.
        //!
        //! # Laws
        //!
        //! - V1, model. Each view delivers each packet from a route it covers,
        //!   in the order the proxy sent them, with its absolute route, and
        //!   nothing from a route it does not cover, whatever the other views
        //!   on the connection cover. Source, the module doc and the docs of
        //!   `DeviceTree`, `to_depth` and `packets`. It rules out a view that
        //!   loses what lies two or more hops below its root, one that ignores
        //!   the depth it was narrowed to or keeps the last depth rather than
        //!   the least, one that leaks a sibling's packets of any type, and a
        //!   connection that delivers a packet only to the first view that
        //!   covers it.
        //!
        //! # Generator obligations
        //!
        //! One to three views on one connection, at routes of 0 to 2 hops
        //! mostly and up to the eight a route holds, each never narrowed or
        //! narrowed up to three times to 0 to 8 levels, eight among them.
        //! Packets of samples, heartbeats with and without a session,
        //! metadata records, setting announcements and RPC requests and
        //! replies. Their routes are a view's root followed by more hops, or
        //! any route, mostly a few hops and one time in four as many as a
        //! route holds. Hops are 0 to 2 mostly, so views overlap and siblings
        //! and prefixes collide often, and sometimes 254 or 255.
        //!
        //! # Exclusions
        //!
        //! Decoded batches from [`DeviceTree::samples`], which take the same
        //! scope and also need the proxy's metadata of each device. Link
        //! statuses, which reach every view their subtree overlaps and which
        //! the stream tests cover. RPC replies from a route a view covers,
        //! which the session takes as the answers to its own calls, so a
        //! reply is sent only from a route no view covers.
        //!
        //! # Examples that stay
        //!
        //! None.

        use super::*;
        use crate::tio::packet::{Packet, Payload};
        use proptest::prelude::*;

        /// The sample number of the packet that closes view `i`'s part of a
        /// case is `LAST + i`. No generated packet numbers a sample so high.
        const LAST: u32 = 999_000;

        /// A view, its root's hops and each depth it was narrowed to.
        #[derive(Debug, Clone)]
        struct View {
            scope: Vec<u8>,
            narrowed: Vec<usize>,
        }

        impl View {
            /// Whether the view covers `route`, from their hops alone.
            fn covers(&self, route: &[u8]) -> bool {
                let below = route.len().wrapping_sub(self.scope.len());
                let depth = self.narrowed.iter().min();
                route.starts_with(&self.scope) && depth.is_none_or(|&depth| below <= depth)
            }
        }

        fn route_of(hops: &[u8]) -> DeviceRoute {
            DeviceRoute::from_hops(hops).unwrap()
        }

        /// A hop, mostly 0 to 2 so that routes collide, sometimes the
        /// largest hop values.
        fn hop() -> impl Strategy<Value = u8> {
            prop_oneof![8 => 0u8..3, 1 => Just(254u8), 1 => Just(u8::MAX)]
        }

        /// Up to `most` hops, a few most of the time, and all `most` of
        /// them one time in four.
        fn hops_up_to(most: usize) -> impl Strategy<Value = Vec<u8>> {
            prop_oneof![
                3 => prop::collection::vec(hop(), 0..=most.min(3)),
                1 => prop::collection::vec(hop(), most..=most),
            ]
        }

        fn view() -> impl Strategy<Value = View> {
            let most = DeviceRoute::MAX_HOPS;
            (
                prop_oneof![3 => prop::collection::vec(hop(), 0..=2), 1 => hops_up_to(most)],
                prop::collection::vec(
                    prop_oneof![3 => 0usize..=4, 1 => 5usize..=most, 1 => Just(most)],
                    0..=3,
                ),
            )
                .prop_map(|(scope, narrowed)| View { scope, narrowed })
        }

        /// The hops of a sent packet's route, below one of `views` or
        /// anywhere, never more than a route holds.
        fn hops(views: Vec<View>) -> impl Strategy<Value = Vec<u8>> {
            let most = DeviceRoute::MAX_HOPS;
            let below =
                (any::<prop::sample::Index>(), hops_up_to(most)).prop_map(move |(view, suffix)| {
                    let mut hops = views[view.index(views.len())].scope.clone();
                    hops.extend(suffix);
                    hops.truncate(most);
                    hops
                });
            prop_oneof![below, hops_up_to(most)]
        }

        /// The packet of kind `kind` numbered `n`, from the route of `hops`.
        fn packet(kind: u8, n: u32, hops: &[u8]) -> Packet {
            let route = route_of(hops);
            match kind {
                0 => samples(n, route),
                1 => Packet::heartbeat(route),
                2 => Packet::heartbeat_session(n, route),
                3 => broadcast(metadata_records(n)[n as usize % 4], route),
                4 => announcement("test.amplitude", Some(n), &[1], route),
                5 => Packet::rpc_request("dev.name", &[], n as u16, route).unwrap(),
                _ => Packet::rpc_reply(n as u16, b"FAKE", route).unwrap(),
            }
        }

        fn case() -> impl Strategy<Value = (Vec<View>, Vec<(u8, Vec<u8>)>)> {
            prop::collection::vec(view(), 1..=3).prop_flat_map(|views| {
                let sent = (0u8..7, hops(views.clone()));
                (Just(views), prop::collection::vec(sent, 1..=16))
            })
        }

        /// Whether `packet` closes a view's part of a case.
        fn closing(packet: &Packet) -> Option<u32> {
            match packet.payload() {
                Payload::Samples(samples) if samples.first.value() >= LAST => {
                    Some(samples.first.value() - LAST)
                }
                _ => None,
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(48))]

            /// V1. Each view delivers the packets of exactly the routes it
            /// covers, two and three hops deep among them, beside other views.
            #[test]
            fn each_view_delivers_the_packets_of_exactly_the_routes_it_covers(
                (views, sent) in case(),
            ) {
                let (session, proxy) = Session::test_pair();
                let connection = Connection::over(session);
                let receivers: Vec<_> = views
                    .iter()
                    .map(|view| {
                        let tree = connection.tree(route_of(&view.scope));
                        let tree = view
                            .narrowed
                            .iter()
                            .fold(tree, |tree, &depth| tree.to_depth(depth));
                        tree.packets()
                    })
                    .collect();

                let mut packets = Vec::new();
                for (n, (kind, hops)) in sent.iter().enumerate() {
                    let covered = views.iter().any(|view| view.covers(hops));
                    if *kind == 6 && covered {
                        continue;
                    }
                    packets.push((hops.clone(), packet(*kind, n as u32, hops)));
                }
                for (_, packet) in &packets {
                    proxy.send(packet.clone());
                }
                for (i, view) in views.iter().enumerate() {
                    proxy.send(samples(LAST + i as u32, route_of(&view.scope)));
                }

                for (i, (view, receiver)) in views.iter().zip(&receivers).enumerate() {
                    let mut delivered = Vec::new();
                    loop {
                        let packet = receiver.recv_timeout(WAIT).expect("the closing packet");
                        match closing(&packet) {
                            Some(closes) if closes == i as u32 => break,
                            Some(_) => {}
                            None => delivered.push(packet.as_bytes().to_vec()),
                        }
                    }
                    let expected: Vec<Vec<u8>> = packets
                        .iter()
                        .filter(|(hops, _)| view.covers(hops))
                        .map(|(_, packet)| packet.as_bytes().to_vec())
                        .collect();
                    prop_assert_eq!(delivered, expected, "view {:?}", view);
                }
            }
        }
    }
}

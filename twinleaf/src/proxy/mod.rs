//! The proxy, which opens each device once and shares it with any number of
//! clients.
//!
//! A [`Proxy`] listens on TCP port 7855 of `127.0.0.1` and `::1` by default.
//! [`HostOptions`] sets the port and the interfaces. `tio proxy` hosts one in
//! a terminal, an app may embed one, and a tool may start one in the
//! background. [`stop`] stops a background proxy.
//!
//! The proxy closes a device that a client opened once no client has used it
//! for ten seconds. It closes a device that only probes have used as soon as
//! the last probe leaves. It reconnects a device that drops out, and the
//! clients using it stay connected. Each hears the link status
//! [`SensorDisconnected`](crate::tio::packet::LinkStatus::SensorDisconnected)
//! at the mount's prefix, then
//! [`SensorReconnected`](crate::tio::packet::LinkStatus::SensorReconnected)
//! once the device answers again. A client that opens the device while it is
//! out hears the first at once. If [`HostOptions::reconnect_timeout`] passes
//! first, each client hears
//! [`FailedToReconnect`](crate::tio::packet::LinkStatus::FailedToReconnect)
//! and is disconnected. A proxy that stops sends every client
//! [`Stopped`](crate::tio::packet::LinkStatus::Stopped) at the root and
//! closes each connection. A client must not reconnect after that status.
//!
//! # Control requests
//!
//! A client connects over TCP and speaks the packet protocol, plus control
//! requests to the proxy itself. A control request is an RPC request in a
//! `PROXY_TX` packet, and the proxy answers it in a `PROXY_RX` packet.
//! Arguments and replies are encoded as
//! [`RpcArgs`](crate::tio::rpc::RpcArgs) encodes them. The methods are
//! these.
//!
//! - `proxy.hello` takes the client's introduction and replies with the
//!   proxy's own. An introduction is the `u32` pid, the `u8` protocol
//!   version, the `twinleaf` version as three `u16`s for major, minor and
//!   patch, and then the name. The protocol version is 1. A client refuses a
//!   proxy whose protocol is older than its own.
//! - `proxy.open` takes a target and replies what it resolved to, a device
//!   URL or `default`. A connection opens once.
//! - `proxy.probe` takes `(String url)` and opens that device for a client
//!   that only looks it over, as discovery does to read its name. A device
//!   that only probes have opened is not a choice for `auto`, and closes as
//!   soon as the last probe leaves. A probe of a device a client opened
//!   shares it.
//! - `proxy.mount` takes a [`Layout`] and opens it as one tree, each mount
//!   at its prefix, in place of `proxy.open`. A layout is a `u8` count of
//!   mounts, then each mount's URL, source route and prefix route, each
//!   after a `u8` length. Each mount's device becomes a choice for `auto`.
//!   A layout that mounts nothing, or whose prefixes overlap, is refused.
//! - `proxy.stop` takes nothing and replies nothing, then stops the proxy.
//!   Only a proxy with [`HostOptions::stoppable`] set accepts it.
//!
//! A target is one of these.
//!
//! - A device URL opens that device at the root.
//! - `default` opens the host's default [`Layout`].
//! - `auto` opens the host's default layout if it set one. Without one, it
//!   opens the one open device that has answered and that a `proxy.open` has
//!   opened, idle or not. Without such a device, it opens the one Twinleaf
//!   serial device attached. If none of these is single, the refusal lists
//!   the candidates.
//!
//! A client whose first packet within 250 ms of connecting is not a control
//! request is a legacy client. It is opened to `auto` at once and hears link
//! statuses in the legacy form, type 64. It is disconnected if `auto` opens
//! nothing.
//!
//! # Choosing a proxy
//!
//! A [`Connection`](crate::Connection) goes through the proxy at
//! `tcp://127.0.0.1:7855`, and starts one if none answers there. The
//! `TWINLEAF_PROXY` environment variable names another proxy as
//! `tcp://host:port`, such as `tcp://127.0.0.1:17855` for a
//! `tio proxy --port 17855`. A proxy named this way is not started, and a
//! connection to it fails if nothing answers there.
//!
//! # Sandboxed apps
//!
//! In a sandboxed macOS app, the library hosts the proxy in the app's process
//! instead of starting `tio`. The app needs these entitlements.
//!
//! - `com.apple.security.network.client` to connect to the proxy and to
//!   network devices.
//! - `com.apple.security.network.server` to host the proxy.
//! - `com.apple.security.device.serial` for serial devices.
//!
//! On macOS 15 and later, reaching network devices and discovering them asks
//! the user for local network access. The app needs an
//! `NSLocalNetworkUsageDescription` in its `Info.plist` for that. Loopback
//! traffic to the proxy does not need this access.
//!
//! An app may host the proxy itself at startup with [`Proxy::host`]. Before
//! it quits, [`Proxy::clients`] shows who else uses the proxy.
//!
//! Other sandboxes, such as Flatpak, Snap and packaged Windows apps, also
//! need permission for the local network and for serial devices. Where an
//! app's network is isolated from the host, its proxy serves only that app.
//!
//! # Logging
//!
//! The proxy logs through the `log` crate. Each device's events are logged
//! under the target `proxy::<url>`. The device's own log packets are logged
//! under `device::<route>` at the level the device gave them. The traffic
//! [`HostOptions::dump`] asks for is logged at info level.

mod auto;
mod client;
mod control;
mod link;
mod server;
mod worker;

pub(crate) use control::{call, executable_name, handshake, Purpose, CLIENT_KEEPALIVE, MOUNT};

use crate::proto::{DeviceRoute, TCP_DEFAULT_PORT};
use crate::rt::Supervisor;
use crate::tio::transport::{CLOSE_TIMEOUT, CONNECT_TIMEOUT};
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// How long to wait for the handshake on a loopback address. A running proxy
/// completes it at once.
// A listening socket completes the handshake in the kernel before it
// accepts. A closed port can hang instead of refusing, as on WSL2 with
// mirrored networking, or take about 2 s to refuse, as on Windows.
const LOOPBACK_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// How long [`stop`] waits for the proxy to reply.
const STOP_TIMEOUT: Duration = Duration::from_secs(3);

/// How long a stopping proxy waits for its clients and links, each of which
/// closes its transport within [`CLOSE_TIMEOUT`].
pub(super) const LEAVE_TIMEOUT: Duration = CLOSE_TIMEOUT.saturating_add(Duration::from_millis(500));

/// How long a proxy keeps a device that a client opened with no client
/// using it, and a background proxy keeps running with no client at all.
const IDLE: Duration = Duration::from_secs(10);

/// The longest a client's RPC waits for its device's reply. The proxy holds
/// a request this long, and a session's call may ask for no longer.
pub(crate) const MAX_RPC_TIMEOUT: Duration = Duration::from_secs(60);

/// The variable that names the proxy to go through, in place of the
/// machine's own.
pub(crate) const PROXY_VARIABLE: &str = "TWINLEAF_PROXY";

/// Returns the URL of the proxy `TWINLEAF_PROXY` names, if it is set and not
/// empty. A proxy named this way is not started.
pub(crate) fn named() -> Option<String> {
    named_by(std::env::var(PROXY_VARIABLE).ok())
}

/// Returns what [`named`] returns when `TWINLEAF_PROXY` has the value
/// `variable`.
fn named_by(variable: Option<String>) -> Option<String> {
    variable.filter(|url| !url.is_empty())
}

/// Returns the URL at which a client on this machine reaches the proxy on
/// `port`, such as `tcp://127.0.0.1:7855`.
pub(crate) fn endpoint_at(port: u16) -> String {
    format!("tcp://127.0.0.1:{port}")
}

/// Returns `true` if anything answers a connection to `port` of
/// `127.0.0.1`. A connect that times out is tried once more for longer, and
/// a refusal is final. Blocks the calling thread.
pub(crate) fn answers(port: u16) -> bool {
    let probing = async move { Ok(answers_async(port).await) };
    matches!(Supervisor::new().run("proxy probe", probing), Ok(true))
}

/// Returns `true` if anything answers a connection to `port` of
/// `127.0.0.1`, without blocking. A connect that times out is tried once
/// more for longer, and a refusal is final. Must be awaited on a tokio
/// runtime.
pub(crate) async fn answers_async(port: u16) -> bool {
    answers_on_retry(|| tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))).await
}

/// Returns `true` if `connect` completes within [`LOOPBACK_CONNECT_TIMEOUT`],
/// or on a second try within [`CONNECT_TIMEOUT`] after a timeout.
async fn answers_on_retry<F, T>(connect: impl Fn() -> F) -> bool
where
    F: Future<Output = io::Result<T>>,
{
    match tokio::time::timeout(LOOPBACK_CONNECT_TIMEOUT, connect()).await {
        Ok(Ok(_)) => true,
        Ok(Err(_refused)) => false,
        Err(_elapsed) => matches!(
            tokio::time::timeout(CONNECT_TIMEOUT, connect()).await,
            Ok(Ok(_))
        ),
    }
}

/// Where a proxy accepts clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Listen {
    /// This machine only, on `127.0.0.1`, and on `::1` too when the host has
    /// IPv6.
    Loopback,
    /// Every interface. Other machines can connect too.
    AllInterfaces,
}

/// One device subtree and where a client sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    /// The device's URL.
    pub url: String,
    /// The subtree of the device that is mounted.
    pub source: DeviceRoute,
    /// The route at which a client sees that subtree.
    pub prefix: DeviceRoute,
}

/// Mounts a client sees together, as one tree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Layout {
    /// Each mounted subtree.
    pub mounts: Vec<Mount>,
}

/// Shows the device's URL, the subtree when it is not the whole device, and
/// where the client sees it, as `serial:///dev/ttyACM0 /2 at /1`.
impl fmt::Display for Mount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.source.is_empty() {
            write!(formatter, "{} at {}", self.url, self.prefix)
        } else {
            write!(formatter, "{} {} at {}", self.url, self.source, self.prefix)
        }
    }
}

/// Shows each mount, separated by commas.
impl fmt::Display for Layout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mounts: Vec<String> = self.mounts.iter().map(Mount::to_string).collect();
        formatter.write_str(&mounts.join(", "))
    }
}

impl Layout {
    /// Creates a layout of the whole device at `url`, seen at the root.
    fn single(url: &str) -> Layout {
        Layout {
            mounts: vec![Mount {
                url: url.to_string(),
                source: DeviceRoute::root(),
                prefix: DeviceRoute::root(),
            }],
        }
    }
}

/// The traffic a proxy logs at info level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DumpFlags {
    /// Packets between clients and devices, other than the kinds below.
    pub traffic: bool,
    /// Sample data.
    pub data: bool,
    /// Metadata.
    pub meta: bool,
    /// Heartbeats.
    pub heartbeats: bool,
}

/// How a proxy is hosted.
#[derive(Debug, Clone)]
pub struct HostOptions {
    /// Where to accept clients.
    pub listen: Listen,
    /// The TCP port to listen on. Port 0 picks a free port and skips the
    /// check for a proxy already running.
    pub port: u16,
    /// Whether to stop once no client has been connected for ten seconds.
    pub idle_exit: bool,
    /// Whether a client may stop the proxy with `proxy.stop`. Only a
    /// background proxy allows it.
    pub stoppable: bool,
    /// The layout `default` opens, and `auto` prefers to any device.
    pub default: Option<Layout>,
    /// Who hosts the proxy, for logs.
    pub name: String,
    /// How long a link keeps trying to reconnect to a device it lost, or
    /// `None` to try forever. A link that gives up disconnects its clients.
    pub reconnect_timeout: Option<Duration>,
    /// Whether to disconnect a client that falls behind. When `false`, the
    /// proxy drops the packets the client cannot take.
    pub kick_slow: bool,
    /// The traffic to log.
    pub dump: DumpFlags,
}

/// The options a tool hosts the machine's proxy with. The proxy listens on
/// loopback at the default port and runs until it is shut down. No client
/// may stop it, it has no default layout, and its name is this executable's.
/// It reconnects forever, drops what a slow client cannot take, and logs no
/// traffic.
impl Default for HostOptions {
    fn default() -> Self {
        HostOptions {
            listen: Listen::Loopback,
            port: TCP_DEFAULT_PORT,
            idle_exit: false,
            stoppable: false,
            default: None,
            name: executable_name(),
            reconnect_timeout: None,
            kick_slow: false,
            dump: DumpFlags::default(),
        }
    }
}

/// A client connected to a proxy, as [`Proxy::clients`] lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientInfo {
    /// The proxy's number for the connection.
    pub id: u64,
    /// Where the client connects from.
    pub peer: SocketAddr,
    /// The process id the client gave in `proxy.hello`.
    pub pid: Option<u32>,
    /// The name the client gave in `proxy.hello`.
    pub name: Option<String>,
    /// The version of the `twinleaf` crate the client gave in `proxy.hello`,
    /// such as `2.0.0`.
    pub version: Option<String>,
    /// What the client opened, a device URL or `default`.
    pub target: Option<String>,
    /// Whether the client is this process, by the pid it gave.
    pub own_process: bool,
}

/// A running proxy. It is shut down when dropped.
pub struct Proxy {
    /// The server's mailbox, which keeps the proxy running.
    server: mpsc::UnboundedSender<server::Notice>,
    /// The receiver that disconnects once the proxy has stopped.
    stopped: flume::Receiver<()>,
    /// The client table, as the server publishes it.
    clients: watch::Receiver<Vec<ClientInfo>>,
    default: watch::Sender<Option<Layout>>,
    endpoint: String,
    #[cfg(test)]
    links: link::Links,
}

/// Stops the background proxy at `proxy`, a `tcp://host:port`. With `None`
/// it stops the proxy [`Connection::open`](crate::Connection::open) goes
/// through. Only a proxy with [`HostOptions::stoppable`] set stops this way,
/// such as the one a connection starts in the background. Waits at most
/// three seconds for the reply.
///
/// # Errors
///
/// Returns an error if no proxy is running there, if the proxy there does
/// not accept `proxy.stop`, or if what answers there is not a Twinleaf 2.0
/// proxy.
///
/// # Examples
///
/// ```no_run
/// use twinleaf::proxy;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // The background proxy that tools go through.
/// proxy::stop(None)?;
/// // A background proxy on a port of its own.
/// proxy::stop(Some("tcp://127.0.0.1:17855"))?;
/// # Ok(())
/// # }
/// ```
pub fn stop(proxy: Option<&str>) -> io::Result<()> {
    let url = match proxy {
        Some(url) => url.to_string(),
        None => named().unwrap_or_else(|| endpoint_at(TCP_DEFAULT_PORT)),
    };
    call(&url, control::STOP, &[], STOP_TIMEOUT).map(drop)
}

impl Proxy {
    /// Starts a proxy with `options`. Returns once it accepts clients.
    ///
    /// # Errors
    ///
    /// Returns an [`io::ErrorKind::AddrInUse`] error if `options.port` is
    /// nonzero and anything already answers on that port of `127.0.0.1`.
    /// Nothing is bound then. The message says whether a Twinleaf proxy
    /// already runs there. Also returns any error from binding the port or
    /// listening on it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use twinleaf::proxy::{HostOptions, Layout, Mount, Proxy};
    /// use twinleaf::DeviceRoute;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let proxy = Proxy::host(HostOptions {
    ///     default: Some(Layout {
    ///         mounts: vec![Mount {
    ///             url: "serial:///dev/ttyACM0".to_string(),
    ///             source: DeviceRoute::root(),
    ///             prefix: DeviceRoute::root(),
    ///         }],
    ///     }),
    ///     ..HostOptions::default()
    /// })?;
    /// println!("Serving at {}", proxy.endpoint());
    /// proxy.wait();
    /// # Ok(())
    /// # }
    /// ```
    pub fn host(options: HostOptions) -> io::Result<Proxy> {
        server::host(options)
    }

    /// Returns the URL a client on this machine connects to, such as
    /// `tcp://127.0.0.1:7855`.
    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    /// Replaces the layout `default` opens and `auto` prefers, or clears it
    /// with `None`. Clients that already opened one keep it.
    pub fn set_default(&self, layout: Option<Layout>) {
        self.default.send_replace(layout);
    }

    /// Returns the clients connected now, in the order they connected. A
    /// proxy that has stopped has none.
    pub fn clients(&self) -> Vec<ClientInfo> {
        match self.clients.has_changed() {
            Ok(_) => self.clients.borrow().clone(),
            Err(_stopped) => Vec::new(),
        }
    }

    /// Blocks until the proxy has stopped, by `proxy.stop`, by idling out, or
    /// by [`shutdown`](Self::shutdown).
    pub fn wait(&self) {
        let _ = self.stopped.recv();
    }

    /// Stops the proxy and blocks until it has stopped. Each client hears the
    /// status [`Stopped`](crate::tio::packet::LinkStatus::Stopped), then its
    /// connection closes. The proxy stops accepting clients and closes every
    /// device. A stop takes about 1.5 s at most, and then ends what still runs.
    /// Calling it again does nothing.
    pub fn shutdown(&self) {
        let _ = self.server.send(server::Notice::Stop);
        self.wait();
    }
}

impl fmt::Debug for Proxy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Proxy")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
pub(crate) use control::{client_name, Introduction};

#[cfg(test)]
pub(crate) mod fixtures;

#[cfg(test)]
mod tests {
    use super::fixtures::{options, FakeDevice, UNREACHABLE, WAIT};
    use super::*;
    use crate::proto::rpc::RpcError;
    use crate::proto::PacketType;
    use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
    use crate::tio::rpc::{RpcArgs, RpcReply};
    use crate::tio::transport;
    use std::net::{Ipv4Addr, Ipv6Addr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    /// The `proxy.hello` of a client named `caller` with pid 4242.
    fn caller_hello() -> Vec<u8> {
        Introduction {
            pid: 4242,
            ..Introduction::of_this_process("caller")
        }
        .encode_args()
    }

    /// The introduction a proxy hosted by this process under `name` replies
    /// to `proxy.hello` with.
    fn hosted_by(name: &str) -> Introduction {
        Introduction::of_this_process(name)
    }

    /// Decodes the reply to `proxy.hello`.
    fn host_of(reply: io::Result<Vec<u8>>) -> Introduction {
        Introduction::decode_reply(&reply.unwrap()).unwrap()
    }

    /// A client speaking raw packets to a proxy.
    struct TestClient {
        port: transport::Port,
        rx: flume::Receiver<transport::ReceiveResult>,
    }

    impl TestClient {
        fn connect(endpoint: &str) -> TestClient {
            let (sender, rx) = flume::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
            let port = transport::Port::new(endpoint, sender);
            TestClient {
                port: port.unwrap(),
                rx,
            }
        }

        /// A client over `stream`, which a test may also write to as it is.
        fn over(stream: TcpStream) -> TestClient {
            let (sender, rx) = flume::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
            let port = transport::Port::from_tcp_stream(
                stream,
                sender,
                transport::DEFAULT_TX_CHANNEL_SIZE,
            );
            TestClient {
                port: port.unwrap(),
                rx,
            }
        }

        fn send(&self, pkt: Packet) {
            self.port.try_send(pkt).unwrap();
        }

        /// The first packet `pick` accepts, skipping any other.
        fn expect<T>(&self, mut pick: impl FnMut(&Packet) -> Option<T>) -> T {
            let deadline = Instant::now() + WAIT;
            loop {
                match self.rx.recv_deadline(deadline) {
                    Ok(Ok(transport::TransportEvent::Packet(pkt))) => {
                        if let Some(found) = pick(&pkt) {
                            return found;
                        }
                    }
                    Ok(Ok(event)) => panic!("unexpected transport event: {event:?}"),
                    Ok(Err(error)) => panic!("the client could not receive: {error}"),
                    Err(flume::RecvTimeoutError::Timeout) => {
                        panic!("nothing expected arrived within {WAIT:?}")
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => {
                        panic!("the proxy closed the connection")
                    }
                }
            }
        }

        /// Wait for the client to hear that the proxy stopped, then for its
        /// connection to close.
        fn expect_stopped(&self, legacy: bool) {
            self.expect(|pkt| {
                let status = match pkt.payload() {
                    Payload::ProxyRx(ProxyRx::Link(status)) if !legacy => status,
                    // A legacy client knows no stop, so it hears that
                    // reconnecting failed.
                    Payload::LegacyStatus(status) if legacy => match status {
                        LinkStatus::FailedToReconnect => LinkStatus::Stopped,
                        status => status,
                    },
                    _ => return None,
                };
                assert_eq!(status, LinkStatus::Stopped);
                assert_eq!(pkt.route(), DeviceRoute::root());
                Some(())
            });
            self.expect_closed();
        }

        /// Wait for the proxy to close the connection, skipping whatever
        /// arrives first.
        fn expect_closed(&self) {
            let deadline = Instant::now() + WAIT;
            loop {
                match self.rx.recv_deadline(deadline) {
                    Ok(_) => {}
                    Err(flume::RecvTimeoutError::Disconnected) => return,
                    Err(flume::RecvTimeoutError::Timeout) => {
                        panic!("the connection stayed open")
                    }
                }
            }
        }

        /// Make a control request and wait for the proxy's answer.
        fn control(
            &self,
            method: &str,
            args: &[u8],
            id: u16,
        ) -> Result<Vec<u8>, (RpcError, String)> {
            self.send(Packet::proxy_request(method, args, id).unwrap());
            self.expect(|pkt| match pkt.payload() {
                Payload::ProxyRx(ProxyRx::Reply(reply)) if reply.req_id.value() == id => {
                    Some(Ok(reply.value.to_vec()))
                }
                Payload::ProxyRx(ProxyRx::Error(error)) if error.req_id.value() == id => {
                    Some(Err((
                        RpcError::from(error.code),
                        String::from_utf8_lossy(error.message).into_owned(),
                    )))
                }
                _ => None,
            })
        }

        fn hello(&self, pid: u32, name: &str) {
            let client = Introduction {
                pid,
                ..Introduction::of_this_process(name)
            };
            let reply = self.control("proxy.hello", &client.encode_args(), 1);
            let host = Introduction::decode_reply(&reply.unwrap()).unwrap();
            assert_eq!(host.protocol, 1);
        }

        fn open(&self, target: &str) -> Result<String, (RpcError, String)> {
            self.control("proxy.open", &target.encode_args(), 2)
                .map(|reply| String::decode_reply(&reply).unwrap())
        }

        fn probe(&self, url: &str) -> Result<String, (RpcError, String)> {
            self.control("proxy.probe", &url.encode_args(), 2)
                .map(|reply| String::decode_reply(&reply).unwrap())
        }

        /// Ask the root device its name.
        fn dev_name(&self, id: u16) -> String {
            self.dev_name_at(DeviceRoute::root(), id)
        }

        /// Ask the device at `route` its name.
        fn dev_name_at(&self, route: DeviceRoute, id: u16) -> String {
            let request = Packet::rpc_request("dev.name", &[], id, route);
            self.send(request.unwrap());
            self.expect(|pkt| match pkt.payload() {
                Payload::RpcReply(reply) if reply.req_id.value() == id => {
                    assert_eq!(pkt.route(), route, "the reply came from elsewhere");
                    Some(String::from_utf8_lossy(reply.value).into_owned())
                }
                _ => None,
            })
        }
    }

    /// `TWINLEAF_PROXY` names a proxy, and an empty variable names none.
    #[test]
    fn the_variable_names_a_proxy_unless_it_is_empty() {
        let url = "tcp://127.0.0.1:2".to_string();
        assert_eq!(named_by(Some(url.clone())), Some(url));
        assert_eq!(named_by(Some(String::new())), None);
        assert_eq!(named_by(None), None);
    }

    #[test]
    fn a_listening_port_answers() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();

        assert!(answers(listener.local_addr().unwrap().port()));
    }

    #[cfg(not(windows))]
    #[test]
    fn a_refused_port_does_not_answer_at_once() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let started = Instant::now();

        assert!(!answers(port));
        let took = started.elapsed();
        assert!(took < Duration::from_secs(1), "it took {took:?}");
    }

    /// A connect the machine stalls past the first try is given longer on a
    /// second, which a running proxy then answers.
    #[tokio::test(start_paused = true)]
    async fn a_connect_that_times_out_once_answers_on_its_second_try() {
        let tries = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();

        let answered = answers_on_retry(|| {
            let stalled = tries.fetch_add(1, Ordering::Relaxed) == 0;
            async move {
                if stalled {
                    std::future::pending::<()>().await;
                }
                io::Result::Ok(())
            }
        })
        .await;

        assert!(answered);
        assert_eq!(tries.into_inner(), 2);
        assert_eq!(started.elapsed(), LOOPBACK_CONNECT_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_connect_is_not_tried_again() {
        let tries = AtomicUsize::new(0);

        let answered = answers_on_retry(|| {
            tries.fetch_add(1, Ordering::Relaxed);
            async { io::Result::<()>::Err(io::ErrorKind::ConnectionRefused.into()) }
        })
        .await;

        assert!(!answered);
        assert_eq!(tries.into_inner(), 1);
    }

    #[test]
    fn a_proxy_can_be_shared_between_threads() {
        fn shareable<T: Send + Sync>() {}
        shareable::<Proxy>();
    }

    #[test]
    fn two_control_clients_share_one_link() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let first = TestClient::connect(&proxy.endpoint());
        let second = TestClient::connect(&proxy.endpoint());

        for (client, name) in [(&first, "first"), (&second, "second")] {
            client.hello(4242, name);
            assert_eq!(client.open(&device.url), Ok(device.url.clone()));
        }

        assert_eq!(first.dev_name(7), "FAKE");
        assert_eq!(second.dev_name(7), "FAKE");
        assert_eq!(device.accepted(), 1);
        assert_eq!(first.open(&device.url).unwrap_err().0, RpcError::State);
    }

    #[test]
    fn a_client_that_opened_nothing_hears_nothing() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let opener = TestClient::connect(&proxy.endpoint());
        opener.hello(4242, "opener");
        opener.open(&device.url).unwrap();
        let idle = TestClient::connect(&proxy.endpoint());
        idle.hello(4243, "idle");

        let request = Packet::rpc_request("dev.name", &[], 7, DeviceRoute::root());
        idle.send(request.unwrap());

        // The device answers and beats every 300 ms, but none of it is the
        // idle client's.
        assert!(idle.rx.recv_timeout(Duration::from_millis(700)).is_err());
        assert_eq!(opener.dev_name(8), "FAKE");
    }

    #[test]
    fn a_device_that_cannot_be_reached_is_refused() {
        let url = UNREACHABLE.to_string();
        let proxy = Proxy::host(options()).unwrap();
        let client = TestClient::connect(&proxy.endpoint());
        client.hello(4242, "client");

        let (code, message) = client.open(&url).unwrap_err();

        assert_eq!(code, RpcError::Load);
        assert!(message.contains(&url), "{message}");
    }

    /// A link that fails to open is tried afresh by the next open, and
    /// forgotten once its task ends, however often it is tried, so none of
    /// its tasks lingers.
    #[test]
    fn a_device_that_cannot_be_reached_leaves_no_link_open() {
        let url = UNREACHABLE.to_string();
        let proxy = Proxy::host(options()).unwrap();
        let client = TestClient::connect(&proxy.endpoint());
        client.hello(4242, "client");

        for _ in 0..10 {
            assert_eq!(client.open(&url).unwrap_err().0, RpcError::Load);
        }

        let deadline = Instant::now() + WAIT;
        while !proxy.links.urls().is_empty() {
            assert!(
                Instant::now() < deadline,
                "a link that failed to open stayed open"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            proxy.links.wait_finished(WAIT),
            "a link that failed to open was never closed"
        );
    }

    /// A device that resets its connection is the proxy's to reconnect. The
    /// client using it stays attached through it on the port it holds, hears
    /// the device drop out and come back, and is answered again.
    #[test]
    fn a_device_that_resets_is_reconnected_while_its_clients_stay_attached() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let client = TestClient::connect(&proxy.endpoint());
        client.hello(4242, "client");
        client.open(&device.url).unwrap();
        assert_eq!(client.dev_name(7), "FAKE");

        device.reset_connections();
        // The device resets once a request it has not read waits for it.
        let deadline = Instant::now() + WAIT;
        while device.resets() == 0 {
            assert!(Instant::now() < deadline, "the device never reset");
            let request = Packet::rpc_request("dev.name", &[], 8, DeviceRoute::root());
            client.send(request.unwrap());
            std::thread::sleep(Duration::from_millis(20));
        }

        for expected in [
            LinkStatus::SensorDisconnected,
            LinkStatus::SensorReconnected,
        ] {
            client.expect(|pkt| match pkt.payload() {
                Payload::ProxyRx(ProxyRx::Link(status)) => {
                    assert_eq!(status, expected);
                    assert_eq!(pkt.route(), DeviceRoute::root());
                    Some(())
                }
                _ => None,
            });
        }
        assert_eq!(client.dev_name(9), "FAKE");
        assert_eq!(device.accepted(), 2);
        assert_eq!(proxy.links.urls(), [device.url.as_str()]);
        assert_eq!(proxy.clients().len(), 1);
    }

    /// A device that never comes back is given up once the reconnect timeout
    /// passes. Each client using it hears so in its own form and is then
    /// disconnected, and its link is closed and forgotten.
    #[test]
    fn the_clients_of_a_device_given_up_hear_so_and_are_disconnected() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(HostOptions {
            reconnect_timeout: Some(Duration::from_millis(200)),
            ..options()
        })
        .unwrap();
        let control = TestClient::connect(&proxy.endpoint());
        control.hello(4242, "control");
        control.open(&device.url).unwrap();
        let legacy = TestClient::connect(&proxy.endpoint());
        assert_eq!(legacy.dev_name(7), "FAKE");

        device.die();

        control.expect(|pkt| match pkt.payload() {
            Payload::ProxyRx(ProxyRx::Link(LinkStatus::SensorDisconnected)) => None,
            Payload::ProxyRx(ProxyRx::Link(status)) => {
                assert_eq!(status, LinkStatus::FailedToReconnect);
                assert_eq!(pkt.route(), DeviceRoute::root());
                Some(())
            }
            Payload::LegacyStatus(status) => panic!("a control client was sent {status:?}"),
            _ => None,
        });
        legacy.expect(|pkt| match pkt.payload() {
            Payload::LegacyStatus(LinkStatus::SensorDisconnected) => None,
            Payload::LegacyStatus(status) => {
                assert_eq!(pkt.ptype(), PacketType::PROXY_STATUS);
                assert_eq!(status, LinkStatus::FailedToReconnect);
                Some(())
            }
            Payload::ProxyRx(rx) => panic!("a legacy client was sent {rx:?}"),
            _ => None,
        });
        control.expect_closed();
        legacy.expect_closed();
        let deadline = Instant::now() + WAIT;
        while !proxy.links.urls().is_empty() || !proxy.clients().is_empty() {
            assert!(Instant::now() < deadline, "the link given up stayed open");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            proxy.links.wait_finished(WAIT),
            "the link given up was never closed"
        );
    }

    /// A second proxy on a port in use binds nothing, and says whether a
    /// proxy or something else holds the port.
    #[test]
    fn a_second_proxy_on_a_port_in_use_binds_nothing_and_says_what_holds_it() {
        let first = Proxy::host(options()).unwrap();
        let endpoint = first.endpoint();
        let port = endpoint.rsplit(':').next().unwrap().parse().unwrap();

        let refused = Proxy::host(HostOptions { port, ..options() }).unwrap_err();

        assert_eq!(refused.kind(), io::ErrorKind::AddrInUse);
        assert_eq!(
            refused.to_string(),
            format!(
                "a Twinleaf proxy already runs at {endpoint}, hosted by {}",
                hosted_by("a test")
            )
        );

        let device = FakeDevice::start();
        let port = device.url.rsplit(':').next().unwrap().parse().unwrap();
        let refused = Proxy::host(HostOptions { port, ..options() }).unwrap_err();

        assert_eq!(refused.kind(), io::ErrorKind::AddrInUse);
        assert_eq!(
            refused.to_string(),
            format!(
                "tcp://127.0.0.1:{port} is not a Twinleaf 2.0 proxy; \
                 reach it with -r tcp://localhost:{port}"
            )
        );
    }

    /// A client whose `localhost` resolves to `::1` first finds the proxy
    /// too, until it stops.
    #[test]
    fn a_proxy_listens_on_both_loopback_addresses() {
        if TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).is_err() {
            eprintln!("skipped: this host cannot bind ::1");
            return;
        }
        let proxy = Proxy::host(options()).unwrap();
        let endpoint = proxy.endpoint();
        let port: u16 = endpoint.rsplit(':').next().unwrap().parse().unwrap();
        assert_eq!(endpoint, format!("tcp://127.0.0.1:{port}"));
        let hello = caller_hello();

        for endpoint in [endpoint, format!("tcp://[::1]:{port}")] {
            let reply = call(&endpoint, "proxy.hello", &hello, WAIT);
            assert_eq!(host_of(reply), hosted_by("a test"), "{endpoint}");
        }

        proxy.shutdown();
        let addresses: [SocketAddr; 2] = [
            (Ipv4Addr::LOCALHOST, port).into(),
            (Ipv6Addr::LOCALHOST, port).into(),
        ];
        for address in addresses {
            let connected = TcpStream::connect_timeout(&address, WAIT);
            assert!(connected.is_err(), "{address} still answers");
        }
    }

    #[test]
    fn call_hears_a_proxy_and_knows_a_legacy_peer_is_not_one() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let hello = caller_hello();

        assert_eq!(
            host_of(call(&proxy.endpoint(), "proxy.hello", &hello, WAIT)),
            hosted_by("a test")
        );
        let refused = call(&proxy.endpoint(), "proxy.reboot", &[], WAIT).unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::Other);
        assert_eq!(refused.to_string(), "the proxy has no method proxy.reboot");
        let legacy = call(
            &device.url,
            "proxy.hello",
            &hello,
            Duration::from_millis(800),
        );
        let legacy = legacy.unwrap_err();
        let port = device.url.rsplit(':').next().unwrap();
        assert_eq!(legacy.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            legacy.to_string(),
            format!(
                "{} is not a Twinleaf 2.0 proxy; reach it with -r tcp://localhost:{port}",
                device.url
            )
        );
    }

    /// A peer that never answers is known not to be a proxy once a hello
    /// would have been answered, not at the end of a call allowed to wait
    /// for a device.
    #[test]
    fn a_silent_peer_is_not_a_proxy_long_before_the_call_times_out() {
        let device = FakeDevice::start();
        let started = Instant::now();

        let result = call(&device.url, "proxy.open", b"auto", Duration::from_secs(15));

        let kind = result.as_ref().map_err(io::Error::kind);
        assert_eq!(kind, Err(io::ErrorKind::InvalidData), "{result:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    /// Both `auto` and `default` open the host's layout, mounted where it
    /// says, until the host clears it.
    #[test]
    fn auto_and_default_open_the_hosts_layout() {
        let device = FakeDevice::start();
        let prefix: DeviceRoute = "/3".parse().unwrap();
        let layout = Layout {
            mounts: vec![Mount {
                url: device.url.clone(),
                source: DeviceRoute::root(),
                prefix,
            }],
        };
        let proxy = Proxy::host(HostOptions {
            default: Some(layout),
            ..options()
        })
        .unwrap();

        for target in ["auto", "default"] {
            let client = TestClient::connect(&proxy.endpoint());
            client.hello(4242, target);
            assert_eq!(client.open(target), Ok("default".to_string()));
            assert_eq!(client.dev_name_at(prefix, 7), "FAKE");
        }

        proxy.set_default(None);
        let client = TestClient::connect(&proxy.endpoint());
        client.hello(4242, "late");
        assert_eq!(client.open("default").unwrap_err().0, RpcError::Invalid);
        assert_eq!(client.open("auto"), Ok(device.url.clone()));
    }

    /// A client mounts devices of its own choosing, each where it says, and
    /// the table shows what it mounted. A layout whose prefixes overlap is
    /// refused.
    #[test]
    fn a_client_mounts_the_layout_it_names() {
        let (first, second) = (FakeDevice::start(), FakeDevice::start());
        let proxy = Proxy::host(options()).unwrap();
        let at = |prefix: &str| prefix.parse::<DeviceRoute>().unwrap();
        let layout = Layout {
            mounts: [(&first, "/1"), (&second, "/2")]
                .map(|(device, prefix)| Mount {
                    url: device.url.clone(),
                    source: DeviceRoute::root(),
                    prefix: at(prefix),
                })
                .to_vec(),
        };
        let client = TestClient::connect(&proxy.endpoint());
        client.hello(4242, "mounting");

        let reply = client.control("proxy.mount", &layout.encode_args(), 2);

        assert_eq!(
            reply.map(|reply| String::decode_reply(&reply).unwrap()),
            Ok(layout.to_string())
        );
        assert_eq!(client.dev_name_at(at("/1"), 7), "FAKE");
        assert_eq!(client.dev_name_at(at("/2"), 8), "FAKE");
        let targets: Vec<_> = proxy
            .clients()
            .into_iter()
            .map(|client| client.target)
            .collect();
        assert_eq!(targets, [Some(layout.to_string())]);

        let overlapping = Layout {
            mounts: vec![
                layout.mounts[0].clone(),
                Mount {
                    prefix: at("/1/2"),
                    ..layout.mounts[1].clone()
                },
            ],
        };
        let other = TestClient::connect(&proxy.endpoint());
        other.hello(4242, "overlapping");
        let refused = other.control("proxy.mount", &overlapping.encode_args(), 2);
        assert_eq!(refused.unwrap_err().0, RpcError::Invalid);
    }

    /// A link still waiting for its device to answer is no candidate for
    /// `auto`, so it leaves the one that answered the only choice.
    #[test]
    fn auto_counts_only_the_links_that_answered() {
        let device = FakeDevice::start();
        // It takes connections but never says anything.
        let silent = TcpListener::bind("127.0.0.1:0").unwrap();
        let silent_url = format!("tcp://{}", silent.local_addr().unwrap());
        let proxy = Proxy::host(options()).unwrap();
        let answered = TestClient::connect(&proxy.endpoint());
        answered.hello(4242, "answered");
        answered.open(&device.url).unwrap();
        let waiting = TestClient::connect(&proxy.endpoint());
        waiting.hello(4243, "waiting");
        let open = Packet::proxy_request("proxy.open", &silent_url.encode_args(), 2);
        waiting.send(open.unwrap());
        let deadline = Instant::now() + WAIT;
        while proxy.links.urls().len() < 2 {
            assert!(Instant::now() < deadline, "the silent link never opened");
            std::thread::sleep(Duration::from_millis(10));
        }

        let legacy = TestClient::connect(&proxy.endpoint());
        assert_eq!(legacy.dev_name(7), "FAKE");
        let auto = TestClient::connect(&proxy.endpoint());
        auto.hello(4244, "auto");
        assert_eq!(auto.open("auto"), Ok(device.url.clone()));
    }

    /// `auto` shares the link it chose as it is. A replug that moved the
    /// device to another port leaves the link's URL naming another device, or
    /// none, so the URL must not be looked up again.
    #[test]
    fn auto_shares_the_link_it_chose_though_its_url_no_longer_finds_it() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let opener = TestClient::connect(&proxy.endpoint());
        opener.hello(4242, "opener");
        opener.open(&device.url).unwrap();

        proxy.links.rekey(&device.url, "usb:moved");

        let auto = TestClient::connect(&proxy.endpoint());
        auto.hello(4243, "auto");
        assert_eq!(auto.open("auto"), Ok(device.url.clone()));
        assert_eq!(auto.dev_name(7), "FAKE");
        assert_eq!(device.accepted(), 1);
        assert_eq!(proxy.links.users(), 2);
    }

    /// A probe reaches its device as an open does, but a device only probes
    /// have opened is nobody's choice for `auto`, until a client opens it.
    #[test]
    fn a_probed_device_is_no_choice_for_auto_until_it_is_opened() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let prober = TestClient::connect(&proxy.endpoint());
        prober.hello(4242, "prober");
        assert_eq!(prober.probe(&device.url), Ok(device.url.clone()));
        assert_eq!(prober.dev_name(7), "FAKE");

        // With one device attached, auto would open it rather than refuse.
        if auto::attached().len() != 1 {
            let auto = TestClient::connect(&proxy.endpoint());
            auto.hello(4243, "auto");
            let (code, message) = auto.open("auto").unwrap_err();
            assert_eq!(code, RpcError::Invalid);
            assert!(
                message.contains("in use") && !message.contains(&device.url),
                "{message}"
            );
        }

        let opener = TestClient::connect(&proxy.endpoint());
        opener.hello(4244, "opener");
        assert_eq!(opener.open(&device.url), Ok(device.url.clone()));
        let auto = TestClient::connect(&proxy.endpoint());
        auto.hello(4245, "auto");
        assert_eq!(auto.open("auto"), Ok(device.url.clone()));
        assert_eq!(device.accepted(), 1);
    }

    /// A device that only a probe used closes as soon as the probe leaves.
    /// A device a client opened stays open after the client leaves.
    #[test]
    fn a_probed_device_closes_once_the_probe_leaves_but_an_opened_one_stays() {
        let probed = FakeDevice::start();
        let opened = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let prober = TestClient::connect(&proxy.endpoint());
        prober.hello(4242, "prober");
        assert_eq!(prober.probe(&probed.url), Ok(probed.url.clone()));
        let opener = TestClient::connect(&proxy.endpoint());
        opener.hello(4243, "opener");
        assert_eq!(opener.open(&opened.url), Ok(opened.url.clone()));
        assert_eq!(proxy.links.urls().len(), 2);

        drop(prober);
        drop(opener);

        // Well within the ten seconds an opened device stays open.
        let deadline = Instant::now() + WAIT;
        while proxy.links.urls() != [opened.url.clone()] {
            let urls = proxy.links.urls();
            assert!(Instant::now() < deadline, "the links stayed {urls:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(proxy.links.urls(), std::slice::from_ref(&opened.url));
    }

    /// A client that hangs up while its open or probe waits for the device
    /// is let go at once, rather than when the device would have had to
    /// answer. Its task ends, and its use of the link with it.
    #[test]
    fn a_client_that_hangs_up_mid_wait_is_let_go() {
        // It takes connections but never says anything.
        let silent = TcpListener::bind("127.0.0.1:0").unwrap();
        let silent_url = format!("tcp://{}", silent.local_addr().unwrap());
        let proxy = Proxy::host(options()).unwrap();

        for method in ["proxy.open", "proxy.probe"] {
            let leaving = TestClient::connect(&proxy.endpoint());
            leaving.hello(4242, "leaving");
            let open = Packet::proxy_request(method, &silent_url.encode_args(), 2);
            leaving.send(open.unwrap());
            let deadline = Instant::now() + WAIT;
            while proxy.links.users() == 0 {
                assert!(Instant::now() < deadline, "{method} never began to wait");
                std::thread::sleep(Duration::from_millis(10));
            }

            drop(leaving);

            let deadline = Instant::now() + Duration::from_millis(1500);
            while !proxy.clients().is_empty() || proxy.links.users() > 0 {
                assert!(
                    Instant::now() < deadline,
                    "{method} still waits for a client that hung up"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    /// A packet that does not decode is skipped as the transport skips it,
    /// whether it comes before the client's first control request or after
    /// its open. The client stays connected, and is answered after it.
    #[test]
    fn a_packet_that_does_not_decode_is_skipped_and_the_client_stays() {
        use std::io::Write;

        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let port: u16 = proxy
            .endpoint()
            .rsplit(':')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let connect = || {
            let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).unwrap();
            (stream.try_clone().unwrap(), TestClient::over(stream))
        };
        // An RPC request too short for its header, and a type 64 packet
        // with no status in it.
        let undecodable = |raw: &mut TcpStream| {
            raw.write_all(&[2, 0, 1, 0, 0]).unwrap();
            raw.write_all(&[64, 0, 0, 0]).unwrap();
        };

        let (mut early_raw, early) = connect();
        undecodable(&mut early_raw);
        early.hello(4242, "early");
        early.open(&device.url).unwrap();
        assert_eq!(early.dev_name(7), "FAKE");

        let (mut late_raw, late) = connect();
        late.hello(4243, "late");
        late.open(&device.url).unwrap();
        undecodable(&mut late_raw);
        assert_eq!(late.dev_name(7), "FAKE");

        assert_eq!(proxy.clients().len(), 2);
    }

    /// A client whose first packet is not a control request, or that sends
    /// nothing at first, is served `auto` at once.
    #[test]
    fn a_legacy_client_is_bound_to_auto() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let opener = TestClient::connect(&proxy.endpoint());
        opener.hello(4242, "opener");
        opener.open(&device.url).unwrap();

        let talker = TestClient::connect(&proxy.endpoint());
        assert_eq!(talker.dev_name(7), "FAKE");

        let silent = TestClient::connect(&proxy.endpoint());
        silent.expect(|pkt| matches!(pkt.payload(), Payload::Heartbeat(_)).then_some(()));
        assert_eq!(device.accepted(), 1);
    }

    #[test]
    fn each_client_hears_link_statuses_in_its_own_form() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let control = TestClient::connect(&proxy.endpoint());
        control.hello(4242, "control");
        control.open(&device.url).unwrap();
        let legacy = TestClient::connect(&proxy.endpoint());
        assert_eq!(legacy.dev_name(7), "FAKE");

        device.drop_connections();

        legacy.expect(|pkt| match pkt.payload() {
            Payload::LegacyStatus(status) => {
                assert_eq!(pkt.ptype(), PacketType::PROXY_STATUS);
                assert_eq!(pkt.route(), DeviceRoute::root());
                assert_eq!(status, LinkStatus::SensorDisconnected);
                Some(())
            }
            Payload::ProxyRx(rx) => panic!("a legacy client was sent {rx:?}"),
            _ => None,
        });
        control.expect(|pkt| match pkt.payload() {
            Payload::ProxyRx(ProxyRx::Link(status)) => {
                assert_eq!(pkt.route(), DeviceRoute::root());
                assert_eq!(status, LinkStatus::SensorDisconnected);
                Some(())
            }
            Payload::LegacyStatus(status) => panic!("a control client was sent {status:?}"),
            _ => None,
        });
    }

    /// Type 64 is also `USER`, which a device and an app may send. Such a
    /// packet is no status of the proxy's, so it passes untouched both
    /// ways, but for its route.
    #[test]
    fn a_user_packet_passes_through_untouched() {
        let device = FakeDevice::start();
        let prefix: DeviceRoute = "/3".parse().unwrap();
        let layout = Layout {
            mounts: vec![Mount {
                url: device.url.clone(),
                source: DeviceRoute::root(),
                prefix,
            }],
        };
        let proxy = Proxy::host(HostOptions {
            default: Some(layout),
            ..options()
        })
        .unwrap();
        let control = TestClient::connect(&proxy.endpoint());
        control.hello(4242, "control");
        control.open("default").unwrap();
        let legacy = TestClient::connect(&proxy.endpoint());
        assert_eq!(legacy.dev_name_at(prefix, 7), "FAKE");
        let (user, _) = Packet::from_slice_prefix(&[64, 0, 3, 0, 4, 0xAA, 0xBB]).unwrap();

        device.send(user.clone());

        for client in [&control, &legacy] {
            client.expect(|pkt| match pkt.payload() {
                Payload::ProxyRx(rx) => panic!("the USER packet arrived as {rx:?}"),
                _ if pkt.ptype() == PacketType::PROXY_STATUS => {
                    assert_eq!(pkt.as_bytes(), user.with_route(prefix).as_bytes());
                    Some(())
                }
                _ => None,
            });
        }

        for client in [&control, &legacy] {
            client.send(user.with_route(prefix));
        }

        for pkt in device.expect_received(PacketType::PROXY_STATUS, 2) {
            assert_eq!(pkt.as_bytes(), user.as_bytes());
        }
    }

    /// A legacy client cannot be told why, so it is disconnected.
    #[test]
    fn a_legacy_client_whose_auto_cannot_open_is_closed() {
        let url = UNREACHABLE.to_string();
        let proxy = Proxy::host(HostOptions {
            default: Some(Layout::single(&url)),
            ..options()
        })
        .unwrap();

        let legacy = TestClient::connect(&proxy.endpoint());

        assert!(matches!(
            legacy.rx.recv_timeout(WAIT),
            Err(flume::RecvTimeoutError::Disconnected)
        ));
    }

    /// Whether `proxy` stops within the time a test waits, waited for on
    /// another thread so that a proxy that never stops fails the test.
    fn stops_in_time(proxy: Arc<Proxy>) -> bool {
        let (done, stopped) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            proxy.wait();
            let _ = done.send(());
        });
        stopped.recv_timeout(WAIT).is_ok()
    }

    /// Whether `proxy` shuts down within the time a test waits, shut down
    /// on another thread so that a proxy that never stops fails the test.
    fn shuts_down_in_time(proxy: Arc<Proxy>) -> bool {
        let (done, stopped) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            proxy.shutdown();
            let _ = done.send(());
        });
        stopped.recv_timeout(WAIT).is_ok()
    }

    /// A client's task that panics leaves the proxy serving the other
    /// clients, the table without it, and the proxy able to stop.
    #[test]
    fn a_panic_in_a_client_task_wedges_nothing() {
        let device = FakeDevice::start();
        let proxy = Arc::new(Proxy::host(options()).unwrap());
        let first = TestClient::connect(&proxy.endpoint());
        first.hello(4242, "first");
        let panicking = TestClient::connect(&proxy.endpoint());
        panicking.hello(4243, "panicking");
        assert_eq!(panicking.open(&device.url), Ok(device.url.clone()));

        panicking.send(Packet::proxy_request("proxy.test.panic", &[], 3).unwrap());

        panicking.expect_closed();
        let second = TestClient::connect(&proxy.endpoint());
        second.hello(4244, "second");
        assert_eq!(second.open(&device.url), Ok(device.url.clone()));
        assert_eq!(second.dev_name(7), "FAKE");
        let deadline = Instant::now() + WAIT;
        while proxy.clients().len() > 2 {
            assert!(
                Instant::now() < deadline,
                "the client that panicked stayed listed"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let names: Vec<_> = proxy
            .clients()
            .into_iter()
            .map(|client| client.name)
            .collect();
        assert_eq!(
            names,
            [Some("first".to_string()), Some("second".to_string())]
        );
        assert_eq!(proxy.links.users(), 1);

        assert!(shuts_down_in_time(proxy));
        first.expect_stopped(false);
        second.expect_stopped(false);
    }

    /// Hosts a proxy, and connects a client that opened `device` and one
    /// that opened nothing. Returns the proxy and both clients.
    fn hosted_with_clients(device: &FakeDevice) -> (Arc<Proxy>, TestClient, TestClient) {
        let proxy = Arc::new(Proxy::host(options()).unwrap());
        let user = TestClient::connect(&proxy.endpoint());
        user.hello(4242, "user");
        assert_eq!(user.open(&device.url), Ok(device.url.clone()));
        let watcher = TestClient::connect(&proxy.endpoint());
        watcher.hello(4243, "watcher");
        (proxy, user, watcher)
    }

    /// Asserts that `proxy` has stopped accepting clients and let go of
    /// `device`.
    fn assert_let_go(proxy: &Proxy, device: &FakeDevice) {
        let endpoint = proxy.endpoint();
        let connected = TcpStream::connect(endpoint.trim_start_matches("tcp://"));
        assert!(connected.is_err(), "the proxy still accepts clients");
        let deadline = Instant::now() + WAIT;
        while device.connections() > 0 {
            assert!(Instant::now() < deadline, "the device stayed open");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// A panic in the registry of links stops the proxy. A client hears the
    /// stop, waiters return, and the device is let go.
    #[test]
    fn a_panic_in_a_core_task_stops_the_proxy_cleanly() {
        let device = FakeDevice::start();
        let (proxy, user, watcher) = hosted_with_clients(&device);

        proxy.links.panic();

        watcher.expect_stopped(false);
        // Its link ended with the registry, before or after the stop.
        user.expect_closed();
        assert!(stops_in_time(proxy.clone()));
        assert_let_go(&proxy, &device);
    }

    /// A panic in the server stops the proxy. Its clients end with it, and
    /// waiters return.
    #[test]
    fn a_panic_in_the_server_stops_the_proxy() {
        let device = FakeDevice::start();
        let (proxy, user, watcher) = hosted_with_clients(&device);

        let _ = proxy.server.send(server::Notice::Panic);

        watcher.expect_closed();
        user.expect_closed();
        assert!(stops_in_time(proxy.clone()));
        assert_let_go(&proxy, &device);
        assert!(proxy.clients().is_empty(), "the table outlived the server");
    }

    #[test]
    fn shutdown_tells_every_client_it_stopped() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let control = TestClient::connect(&proxy.endpoint());
        control.hello(4242, "control");
        control.open(&device.url).unwrap();
        let legacy = TestClient::connect(&proxy.endpoint());
        assert_eq!(legacy.dev_name(7), "FAKE");

        proxy.shutdown();

        control.expect_stopped(false);
        legacy.expect_stopped(true);
        let deadline = Instant::now() + WAIT;
        while !proxy.clients().is_empty() {
            assert!(Instant::now() < deadline, "a client stayed listed");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(proxy.links.wait_finished(WAIT), "a link outlived the proxy");
        proxy.shutdown();
        proxy.wait();
    }

    /// A client still sending as the proxy stops hears the stop all the
    /// same. Its connection closes only once what was queued for it is
    /// written, however its own packets race the close.
    #[test]
    fn a_client_sending_as_the_proxy_stops_still_hears_the_stop() {
        use std::io::Write;

        let proxy = Proxy::host(options()).unwrap();
        let stream = TcpStream::connect(proxy.endpoint().trim_start_matches("tcp://")).unwrap();
        let mut raw = stream.try_clone().unwrap();
        let client = TestClient::over(stream);
        client.hello(4242, "sending");
        let beat = Packet::heartbeat(DeviceRoute::root());
        let sending = std::thread::spawn(move || while raw.write_all(beat.as_bytes()).is_ok() {});

        proxy.shutdown();

        client.expect_stopped(false);
        sending.join().unwrap();
    }

    /// A proxy hosted again on the port of one that just stopped binds it,
    /// though the connections its clients closed still wait out their time
    /// on the proxy's side.
    #[test]
    fn a_proxy_hosted_again_binds_the_port_its_stopped_clients_left() {
        let stopped = Proxy::host(options()).unwrap();
        let endpoint = stopped.endpoint();
        let clients: Vec<TestClient> = (0..4)
            .map(|n| {
                let client = TestClient::connect(&endpoint);
                client.hello(4242, &format!("client {n}"));
                client
            })
            .collect();
        stopped.shutdown();
        for client in &clients {
            client.expect_stopped(false);
        }
        drop(clients);

        let port = endpoint.rsplit(':').next().unwrap().parse().unwrap();
        let again = Proxy::host(HostOptions { port, ..options() });

        assert!(again.is_ok(), "{:?}", again.err());
    }

    /// A client whose open still waits for its device when the proxy stops
    /// hears the stop, and never that its open was refused, which would blame
    /// its target.
    #[test]
    fn an_open_still_waiting_hears_the_stop_and_no_refusal() {
        // It takes connections but never says anything.
        let silent = TcpListener::bind("127.0.0.1:0").unwrap();
        let silent_url = format!("tcp://{}", silent.local_addr().unwrap());
        let proxy = Arc::new(Proxy::host(options()).unwrap());
        let waiting = TestClient::connect(&proxy.endpoint());
        waiting.hello(4242, "waiting");
        let open = Packet::proxy_request("proxy.open", &silent_url.encode_args(), 2);
        waiting.send(open.unwrap());
        let deadline = Instant::now() + WAIT;
        while proxy.links.urls().is_empty() {
            assert!(Instant::now() < deadline, "the silent link never opened");
            std::thread::sleep(Duration::from_millis(10));
        }

        assert!(shuts_down_in_time(proxy));

        waiting.expect(|pkt| match pkt.payload() {
            Payload::ProxyRx(ProxyRx::Error(error)) => panic!(
                "the open was refused: {}",
                String::from_utf8_lossy(error.message)
            ),
            Payload::ProxyRx(ProxyRx::Link(status)) => {
                assert_eq!(status, LinkStatus::Stopped);
                assert_eq!(pkt.route(), DeviceRoute::root());
                Some(())
            }
            _ => None,
        });
        waiting.expect_closed();
    }

    /// A stop that the host refuses names the host, and a stop nothing
    /// answers names the address.
    #[test]
    fn proxy_stop_is_accepted_only_when_the_host_is_stoppable() {
        let kept = Proxy::host(options()).unwrap();
        let refused = stop(Some(&kept.endpoint())).unwrap_err();
        assert_eq!(
            refused.to_string(),
            format!(
                "the proxy at {} is hosted by a test and stops only with it",
                kept.endpoint()
            )
        );
        let hello = caller_hello();
        assert_eq!(
            host_of(call(&kept.endpoint(), "proxy.hello", &hello, WAIT)),
            hosted_by("a test")
        );

        let stoppable = Proxy::host(HostOptions {
            stoppable: true,
            ..options()
        })
        .unwrap();
        let watcher = TestClient::connect(&stoppable.endpoint());
        watcher.hello(4242, "watcher");

        stop(Some(&stoppable.endpoint())).unwrap();

        watcher.expect_stopped(false);
        let endpoint = stoppable.endpoint();
        assert!(stops_in_time(Arc::new(stoppable)));
        assert_eq!(
            stop(Some(&endpoint)).unwrap_err().to_string(),
            format!("no proxy is running at {endpoint}")
        );
    }

    /// A hello is answered with the host's introduction, however the proxy
    /// is hosted.
    #[test]
    fn hello_replies_with_the_hosts_introduction() {
        let hello = caller_hello();

        for stoppable in [false, true] {
            let proxy = Proxy::host(HostOptions {
                stoppable,
                ..options()
            })
            .unwrap();

            let reply = call(&proxy.endpoint(), "proxy.hello", &hello, WAIT);

            assert_eq!(host_of(reply), hosted_by("a test"));
        }
    }

    #[test]
    fn clients_lists_who_said_hello_and_what_they_opened() {
        let device = FakeDevice::start();
        let proxy = Proxy::host(options()).unwrap();
        let other = TestClient::connect(&proxy.endpoint());
        other.hello(4242, "other");
        other.open(&device.url).unwrap();
        let mine = TestClient::connect(&proxy.endpoint());
        mine.hello(std::process::id(), "mine");

        let clients = proxy.clients();

        let summary: Vec<_> = clients
            .iter()
            .map(|client| {
                (
                    client.pid,
                    client.name.as_deref(),
                    client.version.as_deref(),
                    client.target.as_deref(),
                    client.own_process,
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                (
                    Some(4242),
                    Some("other"),
                    Some(env!("CARGO_PKG_VERSION")),
                    Some(device.url.as_str()),
                    false
                ),
                (
                    Some(std::process::id()),
                    Some("mine"),
                    Some(env!("CARGO_PKG_VERSION")),
                    None,
                    true
                ),
            ]
        );
        assert!(clients[0].id < clients[1].id);
        assert!(clients.iter().all(|client| client.peer.ip().is_loopback()));

        // A client leaves the table once its connection closes.
        drop(other);
        let deadline = Instant::now() + WAIT;
        while proxy.clients().len() > 1 {
            assert!(Instant::now() < deadline, "the closed client stayed listed");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(proxy.clients()[0].name.as_deref(), Some("mine"));
    }
}

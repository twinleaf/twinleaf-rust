//! The proxy, which opens each device once and shares it with any number of
//! clients.
//!
//! A [`Proxy`] listens on TCP port 7855 of `127.0.0.1` and `::1` by default.
//! [`HostOptions`] sets the port and the interfaces. `tio proxy` hosts one in
//! a terminal, an app may embed one, and a tool may start one in the
//! background. [`stop`] stops a background proxy.
//!
//! The proxy keeps open each device its default layout mounts for as long as
//! it runs. It closes any other device that a client opened once no client has
//! used it for five seconds. It closes a device that only probes have used as soon as
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
//! [`Stopped`](crate::tio::packet::LinkStatus::Stopped) at the root, behind
//! everything already queued for the client, and closes each connection once
//! that is written or one second has passed, the `CLOSE_TIMEOUT` of its
//! transport. A client must not reconnect after that status. The status can
//! fail to arrive, when that second runs out or the proxy crashes. A
//! client's session then takes the closed socket as a lost one, and opens
//! its target again through the same address. It never starts a proxy to do
//! so.
//!
//! A client that falls behind loses samples, the oldest first, and nothing
//! else.
//!
//! # When the proxy lets a client go
//!
//! The proxy closes a client's connection, and logs why, in these cases, and
//! when it cannot set up a new connection's transport at all.
//!
//! - The client hung up, or a write to it stalled for longer than the
//!   transport's drain timeout of 3 s, `DRAIN_TIMEOUT`, as when the client
//!   stops reading. It is logged as `HungUp`.
//! - The client sent nothing for longer than the 5 s quiet time of the
//!   proxy's keepalive, `PROXY_KEEPALIVE`. It is logged as `HungUp`.
//! - The packets other than samples queued for the client passed 8 MiB,
//!   `HARD_LIMIT`, as a flood of them outran a client that still reads. It
//!   is logged as `TooSlow`.
//! - The link to a device the client opened ended, as `LinkEnded`, or gave
//!   the device up after `FailedToReconnect`, as `GaveUp`.
//! - A packet the client sent found its mount's port full, or would reach
//!   deeper on the device than a route can. It is logged as `ForwardFailed`.
//! - The proxy stopped. It is logged as `Stopped`.
//!
//! # How a packet reaches a client
//!
//! Each link to a device has one worker, which owns the device's port. The
//! worker hands every packet the device sends, in order, to the link's
//! engine, through a queue with no limit, and the engine reads it through a
//! session whose inlet has no limit either. The engine keeps the link's state
//! from each packet, and then puts the packet on the stream of the outbox of
//! each client whose mount covers its route. As it sets a device's metadata,
//! it puts the metadata update on the stream of each client that follows the
//! device, so the update reaches the client ahead of every sample sent after
//! the change.
//!
//! A client's outbox holds that stream and an urgent queue, for the answers
//! to the client's requests and the other updates of its watches. It drops
//! only samples, the oldest first, past 1 MiB of them, `SAMPLE_BUDGET`, and
//! lets the client go past 8 MiB of anything else. The client's task takes
//! the whole urgent queue and then the oldest 64 KiB of the stream,
//! `TAKE_LIMIT`, at a time, and writes them into the socket's queue of 64
//! packets, `SOCKET_QUEUE`, from which the transport writes them to the
//! connection. So an answer waits behind at most 64 KiB of the stream and
//! the rest of one update, then the 64 packets the socket holds.
//!
//! What a client sends goes from its connection into a queue of 32768
//! packets, or as many as `TWINLEAF_PROXY_INTERFACE_TX_BUFSIZE` asks for
//! above that. The client's task forwards each packet to the port of the
//! mount its route falls in, a queue as deep, and a packet that finds that
//! port full lets the client go. The worker writes each packet to the
//! device, and hands the answer to a client's request back to that client's
//! mount, which queues it on the urgent queue.
//!
//! On the client, the session reads the socket into its stream pump's
//! inlet, which holds 32768 packets. A pump that falls that far behind loses
//! what does not fit, and the parser marks the gap in the samples. The pump
//! hands each sample batch to each receiver's queue of 1024 batches, and
//! each packet to each packet receiver's queue of 8192 packets. A receiver
//! that falls behind loses what does not fit in its queue, and hears
//! [`RecvError::Lagged`](crate::RecvError::Lagged) with the count. The next
//! batch of each stream that lost batches carries a
//! [`Loss`](crate::data::Loss). The
//! state the proxy sends goes into the connection's store, whose presence,
//! metadata, RPC table and settings hold only their latest value, so a slow
//! reader sees the newest and misses none it needs. Announced settings
//! queue in order, and a reader that falls behind them hears how many it
//! missed. So a sample can be lost in the proxy's outbox, in the pump's
//! inlet, and in a receiver's queue, and each place counts what it lost.
//!
//! # Control requests
//!
//! A client connects over TCP and speaks the packet protocol, plus control
//! requests to the proxy itself. A control request is an RPC request in a
//! `PROXY_TX` packet, and the proxy answers it in a `PROXY_RX` packet. Its
//! arguments and its reply are each one CBOR item. A struct is the array of
//! its fields in order, and a route is the byte string of its hops. The
//! methods are these.
//!
//! - `proxy.hello` takes the client's introduction and replies with the
//!   proxy's own. An introduction is the pid, the `twinleaf` version as an
//!   array of major, minor and patch, and the name. A peer that sends no
//!   control reply within two seconds, as a proxy from an older build of
//!   `twinleaf` does, or that answers with no introduction, is no proxy the
//!   client can use, and the client says to update it.
//! - `proxy.open` takes a target as a text string and replies what it
//!   resolved to, a device URL or `default`. A connection opens once.
//! - `proxy.probe` takes a device URL and opens that device for a client
//!   that only looks it over, as discovery does to read its name. A device
//!   that only probes have opened is not a choice for `auto`, and closes as
//!   soon as the last probe leaves. A probe of a device a client opened
//!   shares it.
//! - `proxy.mount` takes a [`Layout`] and opens it as one tree, each mount
//!   at its prefix, in place of `proxy.open`. A layout is the array of its
//!   mounts, and a mount is its URL, source route and prefix route. Each
//!   mount's device becomes a choice for `auto`. A layout that mounts
//!   nothing, or whose prefixes overlap, is refused.
//! - `proxy.default` takes a [`Layout`] and replies nothing. The layout is
//!   the default until the connection that sent it closes, when the default
//!   it replaced returns. Only a proxy with [`HostOptions::stoppable`] set
//!   accepts it, from one connection at a time.
//! - `proxy.stop` takes nothing and replies nothing, then stops the proxy.
//!   Only a proxy with [`HostOptions::stoppable`] set accepts it.
//! - `proxy.unwatch` takes the id of a `proxy.watch` request and replies
//!   nothing. The proxy stops sending that watch's updates. Parts already
//!   queued for the client still go out.
//! - `proxy.watch` takes the route of a device the connection opened, as
//!   the client sees it, and whether to follow the device's RPC table and
//!   settings rather than its presence, metadata and announcements. It
//!   replies with the state now, and again at each change, all under the
//!   request's id. Each reply is one part of an update, the array of the
//!   part's index, the count of parts and its bytes, and the bytes of every
//!   part in order are one CBOR item. A metadata update reaches the client
//!   ahead of every sample the device sent after the metadata changed. Any
//!   other update may overtake the device's packets, and keeps its order
//!   among the watch's updates of its kind.
//!   An update is the device's presence, which is connecting, alive in a
//!   session, or lost some milliseconds before the update went out, one
//!   setting it announced, its complete metadata as its own metadata
//!   packets, that it has no `dev.metadata`, its RPC table as the hash, the
//!   session and each entry's name and metadata word, why the table did not
//!   load, or the values of its announced settings with the table's hash,
//!   their session and their version. The proxy asks each device for this
//!   state once, for every client that watches it. A client asks the device
//!   nothing of it, and takes none of it from the packets it receives.
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
//! A client reaches no device until a control request opens a target, and
//! it hears each link status as a `PROXY_RX` link packet. A client that
//! sends no control request opens nothing.
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
mod engine;
mod link;
mod outbox;
mod server;
mod table;
mod worker;

pub(crate) use control::{
    call, encode, executable_name, handshake, Joined, Purpose, Update, Watch, AUTO, CHANGED,
    DEFAULT_LAYOUT, MAX_RPC_TIMEOUT, MOUNT, PROXY_VARIABLE, TIMED_OUT, UNANSWERED, UNWATCH, WATCH,
};
#[cfg(test)]
pub(crate) use control::{decode, parts, CLIENT_KEEPALIVE};
pub use control::{Layout, Mount};
pub(crate) use server::host;
pub use server::Proxy;

use crate::proto::TCP_DEFAULT_PORT;
use crate::tio::transport::{CLOSE_TIMEOUT, CONNECT_TIMEOUT};
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

/// How long to wait for the handshake on a loopback address. A running proxy
/// completes it at once.
// A listening socket completes the handshake in the kernel before it
// accepts. A closed port can hang instead of refusing, as on WSL2 with
// mirrored networking. Windows resends an unanswered handshake before it
// refuses, and a loopback connect is told not to resend it there.
const LOOPBACK_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// How long [`stop`] waits for the proxy to reply.
const STOP_TIMEOUT: Duration = Duration::from_secs(3);

/// How long a stopping proxy waits for its clients and links, each of which
/// closes its transport within [`CLOSE_TIMEOUT`].
pub(super) const LEAVE_TIMEOUT: Duration = CLOSE_TIMEOUT.saturating_add(Duration::from_millis(500));

/// How long a proxy keeps a device that a client opened with no client using
/// it, unless its default layout mounts the device, and how long a background
/// proxy keeps running with no client at all.
const IDLE: Duration = Duration::from_secs(5);

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
#[cfg(test)]
pub(crate) fn answers(port: u16) -> bool {
    let probing = async move { Ok(answers_async(port).await) };
    matches!(
        crate::rt::Supervisor::new().run("proxy probe", probing),
        Ok(true)
    )
}

/// Returns `true` if anything answers a connection to `port` of
/// `127.0.0.1`, without blocking. A connect that times out is tried once
/// more for longer, and a refusal is final. Must be awaited on a tokio
/// runtime.
pub(crate) async fn answers_async(port: u16) -> bool {
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    answers_on_retry(|| crate::net::connect(address)).await
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
    /// Whether to stop once no client has been connected for five seconds.
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
    /// The traffic to log.
    pub dump: DumpFlags,
}

/// The options a tool hosts the machine's proxy with. The proxy listens on
/// loopback at the default port and runs until it is shut down. No client
/// may stop it, it has no default layout, and its name is this executable's.
/// It reconnects forever and logs no traffic.
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

#[cfg(test)]
pub(crate) use control::{client_name, Introduction};

#[cfg(test)]
pub(crate) mod tests;

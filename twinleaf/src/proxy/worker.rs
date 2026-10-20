//! The link worker, a task that owns one device's port and shares it among
//! many ports of clients. [`new`] creates it, and it reports its state in a
//! [`Readiness`].
//!
//! The worker reopens a device whose port closes, fails with an I/O error,
//! or goes quiet. A device that has answered goes quiet after 1 s without a
//! packet, or 3 s over TCP. The worker follows a serial device that a replug
//! moved to another port. Its ports stay open meanwhile. Each hears the
//! device drop out as
//! [`SensorDisconnected`](crate::tio::packet::LinkStatus::SensorDisconnected)
//! and come back as
//! [`SensorReconnected`](crate::tio::packet::LinkStatus::SensorReconnected).
//! A port that joins while the device is out hears the first at once. A
//! reopened port counts only once the device answers on it. One the device
//! never answered on is a failed attempt, and the ports hear nothing of it.
//! Attempts run beside the ports' traffic and are tried each second.
//!
//! The worker stops when its first open fails, when its command sender
//! drops, or when a reconnect gives up after the reconnect timeout. Its ports
//! hear a reconnect that gives up as
//! [`FailedToReconnect`](crate::tio::packet::LinkStatus::FailedToReconnect).
//! Its task ends once the device's port has let go of the device. An open in
//! flight on a blocking thread runs to its end first. A TCP open in flight is
//! dropped.
//!
//! The worker negotiates the serial rate and fans what arrives out to its
//! ports, each restricted to a subtree. It renumbers the RPCs the ports make
//! and gives each reply back its original id. A request the device does not
//! answer in time, or that a lost or restarted device leaves, is answered
//! with an RPC error. Control packets a port sends go to the proxy and never
//! reach the device.
//!
//! The worker logs its device's events under the target `proxy::<url>`, the
//! device's own log packets under `device::<route>` at their own level, and
//! the traffic [`DumpFlags`] asks for.

use super::{DumpFlags, MAX_RPC_TIMEOUT};
use crate::proto::heartbeat::Heartbeat;
use crate::proto::log::LogLevel;
use crate::proto::rpc::RpcError;
use crate::proto::{DeviceRoute, PacketType};
use crate::rt::{finish, sleep_until_some, BoxFuture, Supervisor};
use crate::tio::packet::{self, Packet};
use crate::tio::transport;
use crate::tio::transport::Port as HardwarePort;
use crate::tio::transport::{ReceiveResult, TransportEvent};

use futures_util::{FutureExt, StreamExt};
use std::fs;
use std::future::Future;
use std::io;
use std::ops::ControlFlow;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use std::collections::{BTreeMap, HashMap, HashSet};

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_stream::{wrappers::ReceiverStream, StreamMap, StreamNotifyClose};

/// Whether a link's device has answered. The worker sets it, and the
/// proxy's link table reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) enum Readiness {
    /// Nothing has arrived yet.
    #[default]
    Pending,
    /// The device sent its first packet.
    Ready,
    /// The first open failed, for this reason.
    Failed(String),
}

/// The two channels of a port before the worker adopts it.
#[derive(Debug)]
pub(super) struct Subscriber {
    sender: SubscriberSender,
    rx: mpsc::Receiver<Packet>,
}

/// Delivers packets to one port, with routes relative to its subtree.
#[derive(Debug)]
struct SubscriberSender {
    tx: mpsc::Sender<Packet>,
    scope: DeviceRoute,
}

/// What a link asks of its worker.
pub(super) enum Command {
    /// A port for the worker to adopt.
    OpenPort(Subscriber),
}

impl Subscriber {
    /// Creates a port on the subtree at `scope`. The worker delivers to it on
    /// `tx` and reads what it sends from `rx`.
    pub(super) fn new(
        tx: mpsc::Sender<Packet>,
        rx: mpsc::Receiver<Packet>,
        scope: DeviceRoute,
    ) -> Subscriber {
        Subscriber {
            sender: SubscriberSender { tx, scope },
            rx,
        }
    }
}

impl SubscriberSender {
    /// Delivers `pkt` to the port with its route relative to the port's
    /// subtree. Skips a packet outside the subtree. A link status whose route
    /// is above the subtree reaches the port at its root. Returns `false` if
    /// the port's queue is full or the port has closed.
    fn try_send(&self, pkt: &Packet) -> bool {
        let route = match self.scope.relative_route(&pkt.route()) {
            Ok(below) => below,
            Err(_) if pkt.link_status().is_some() && self.scope.starts_with(&pkt.route()) => {
                DeviceRoute::root()
            }
            Err(_) => return true,
        };
        self.tx.try_send(pkt.with_route(route)).is_ok()
    }
}

/// What opens a device's port. It takes the sender the port delivers to,
/// and returns a future of the open port.
pub(super) struct Opener {
    opens: Box<dyn Fn(flume::Sender<ReceiveResult>) -> Opening + Send + Sync>,
    /// Whether the worker may drop an open in flight when it stops.
    droppable: bool,
}

/// A future of an open port.
type Opening = BoxFuture<'static, io::Result<HardwarePort>>;

impl Opener {
    /// Returns an opener that opens with `open`. The worker lets an open in
    /// flight run to its end before it stops.
    pub(super) fn new(
        open: impl Fn(flume::Sender<ReceiveResult>) -> Opening + Send + Sync + 'static,
    ) -> Opener {
        Opener {
            opens: Box::new(open),
            droppable: false,
        }
    }

    /// Returns an opener that opens with `open`, whose open in flight the
    /// worker drops when it stops. Dropping the open must leave nothing open.
    pub(super) fn droppable(
        open: impl Fn(flume::Sender<ReceiveResult>) -> Opening + Send + Sync + 'static,
    ) -> Opener {
        Opener {
            opens: Box::new(open),
            droppable: true,
        }
    }

    /// Returns a future that opens a port delivering to `results`.
    pub(super) fn open(&self, results: flume::Sender<ReceiveResult>) -> Opening {
        (self.opens)(results)
    }
}

/// How many commands may wait for a worker.
const COMMAND_LANE: usize = 64;

/// Creates the worker of the device at `url`, which `open` opens. A lost
/// device is retried for `reconnect_timeout`, or forever with `None`. The
/// worker logs the traffic `dump` asks for. It stops once every command
/// sender has dropped. Returns the command sender, the receiver of the
/// worker's readiness, and the future that runs the worker as its link's task.
pub(super) fn new(
    url: &str,
    reconnect_timeout: Option<Duration>,
    dump: DumpFlags,
    open: Opener,
) -> (
    mpsc::Sender<Command>,
    watch::Receiver<Readiness>,
    impl Future<Output = ()> + Send + 'static,
) {
    let (commands, command_queue) = mpsc::channel(COMMAND_LANE);
    let (readiness, watched) = watch::channel(Readiness::Pending);
    let worker = Worker::new(
        url.to_string(),
        reconnect_timeout,
        command_queue,
        dump,
        readiness,
        open,
    );
    (commands, watched, worker.run())
}

/// Returns the opener of the device at `url`. A TCP device connects as a
/// future, which the worker drops if it stops meanwhile. Any other device
/// opens in one blocking step on `tasks`. If that port does not open, the
/// step looks for a serial device that a replug moved to another port, and
/// opens that.
pub(super) fn opener(tasks: &Supervisor, url: &str) -> Opener {
    let tasks = tasks.clone();
    let url = url.to_string();
    if HardwarePort::opens_as_future(&url) {
        return Opener::droppable(move |results| {
            let (url, tasks) = (url.clone(), tasks.clone());
            Box::pin(async move { HardwarePort::open(&url, results, &tasks).await })
        });
    }
    // The key of a serial device, taken on the first attempt while the
    // device is likely present.
    let identity = Arc::new(OnceLock::new());
    Opener::new(move |results| {
        let (url, identity, on) = (url.clone(), identity.clone(), tasks.clone());
        let opening = tasks.spawn_blocking(move || {
            let key: &Option<String> = identity.get_or_init(|| self::identity(&url));
            match HardwarePort::new_on(&url, results.clone(), &on) {
                Ok(port) => Ok(port),
                Err(error) => match relocate(&url, key.as_deref()) {
                    Some(moved) => HardwarePort::new_on(&moved, results, &on),
                    None => Err(error),
                },
            }
        });
        Box::pin(async move { opening.await.map_err(io::Error::other)? })
    })
}

/// A state of the rate autonegotiation.
#[derive(Debug, Clone)]
enum RateChange {
    DoNothing,
    WaitingForSession,
    QueryDeviceRate,
    WaitingDeviceRate,
    SetDeviceRate,
    WaitingNewRate,
    RateChanged,
    GaveUp,
}

/// A device's port and the state of its rate negotiation.
struct Device {
    port: HardwarePort,
    /// What the port receives.
    results: flume::r#async::RecvStream<'static, ReceiveResult>,
    rate_change_state: RateChange,
    last_rx: Instant,
    seen_since_connect: bool,
    last_session: Option<u32>,
    restarted: bool,
}

impl Device {
    /// Creates a device reached through `port`, whose results arrive on
    /// `results`.
    fn new(port: HardwarePort, results: flume::Receiver<ReceiveResult>) -> Device {
        // Kickstart rate autonegotiation only if the port supports
        // changing rates and the target rate differs from the default.
        let mut rate_change_state = RateChange::DoNothing;
        if let Some(rates) = port.rate_info() {
            if rates.target_bps != rates.default_bps {
                rate_change_state = RateChange::WaitingForSession;
            }
        }
        Device {
            port,
            results: results.into_stream(),
            rate_change_state,
            last_rx: Instant::now(),
            seen_since_connect: false,
            last_session: None,
            restarted: false,
        }
    }

    /// Returns `true` if the device's rate is not negotiated.
    fn has_static_rate(&self) -> bool {
        matches!(self.rate_change_state, RateChange::DoNothing)
    }

    /// Returns `true` while the rate negotiation runs. It stays `true` after
    /// the higher rate is negotiated, while the new rate is on trial.
    fn needs_autonegotiation(&self) -> bool {
        !matches!(
            self.rate_change_state,
            RateChange::DoNothing | RateChange::GaveUp
        )
    }

    /// Returns `false` while the device's rate is changing. Packets for the
    /// device are held back until it returns `true`.
    fn safe_to_forward(&self) -> bool {
        !matches!(
            self.rate_change_state,
            RateChange::SetDeviceRate | RateChange::WaitingNewRate
        )
    }

    /// Returns how long the device may stay silent before the watchdog tears
    /// it down.
    fn liveness_timeout(&self) -> Duration {
        match self.port.kind() {
            transport::TransportKind::Tcp => LIVENESS_TIMEOUT_TCP,
            transport::TransportKind::Serial | transport::TransportKind::Udp => LIVENESS_TIMEOUT,
        }
    }

    /// Returns when the device counts as lost if it stays silent, or `None`
    /// before it has answered.
    fn liveness_deadline(&self) -> Option<Instant> {
        self.seen_since_connect
            .then(|| self.last_rx + self.liveness_timeout())
    }

    /// Returns when the port goes back to its default rate if nothing
    /// arrives at the new rate, or `None` while no new rate is on trial.
    fn rate_trial_deadline(&self) -> Option<Instant> {
        matches!(self.rate_change_state, RateChange::RateChanged).then(|| self.last_rx + RATE_TRIAL)
    }

    /// Returns the device's rate information. Panics if the port's rate is
    /// not settable.
    fn rates(&self) -> transport::RateInfo {
        self.port
            .rate_info()
            .expect("Rates requested for unsupported device")
    }

    /// Notes `result`, which the port received. Packets and text refresh
    /// the device's liveness. A heartbeat of the root device moves the rate
    /// negotiation on, and marks the device restarted when its session
    /// changes.
    fn received(&mut self, result: &ReceiveResult) {
        // Any received packet (text included) refreshes liveness, for every
        // device — the watchdog needs a timestamp regardless of rate state.
        match result {
            Ok(TransportEvent::Packet(_) | TransportEvent::Text(_)) => {
                self.last_rx = Instant::now();
            }
            _ => {}
        }
        if !self.has_static_rate() {
            if let Ok(TransportEvent::Packet(pkt)) = result {
                if let packet::Payload::Heartbeat(Heartbeat::Session(session)) = pkt.payload() {
                    let session = session.value();
                    if pkt.route().is_empty() {
                        // This is a heartbeat for the root sensor
                        let old_session = self.last_session.replace(session);
                        if let RateChange::WaitingForSession = self.rate_change_state {
                            self.rate_change_state = RateChange::QueryDeviceRate;
                        } else if (self.last_session != old_session) && old_session.is_some() {
                            // It has restarted, restart autonegotiation if needed.
                            self.rate_change_state = match self.rate_change_state {
                                RateChange::DoNothing => RateChange::DoNothing,
                                RateChange::WaitingForSession => RateChange::WaitingForSession, // never happens
                                _ => RateChange::QueryDeviceRate,
                            };
                            self.restarted = true;
                        }
                    }
                }
            }
        }
    }
}

enum RpcTarget {
    Port { port: u64, original_id: u16 },
    Internal(u16),
}

enum RpcOrigin {
    Port(u64),
    Internal,
}

#[derive(Debug, Clone, Copy)]
enum RpcFailure {
    Timeout,
    DeviceDisconnected,
    ProxyClosed,
}

impl RpcFailure {
    fn wire_error(self) -> RpcError {
        match self {
            Self::Timeout => RpcError::Timeout,
            Self::DeviceDisconnected | Self::ProxyClosed => RpcError::Undefined,
        }
    }
}

struct RpcMapEntry {
    target: RpcTarget,
    route: DeviceRoute,
    timeout: Instant,
}

/// The state of one device's link. [`new`] creates it, with the future that
/// runs it as the link's task.
struct Worker {
    url: String,
    reconnect_timeout: Option<Duration>,
    command_queue: mpsc::Receiver<Command>,
    /// The target the worker logs under, `proxy::<url>`.
    log_target: String,
    /// The traffic to log.
    dump: DumpFlags,
    /// The worker's readiness, which the link watches.
    readiness: watch::Sender<Readiness>,
    /// What opens the device's port.
    open: Opener,

    device: Option<Device>,
    /// The attempt in flight to reopen the device.
    attempt: Option<BoxFuture<'static, io::Result<Device>>>,

    ever_connected: bool,
    /// When reconnecting gives up, counted from when the device was lost. It
    /// is cleared once the device answers again. `None` while the device is
    /// not being reconnected, or if it is retried forever.
    reconnect_deadline: Option<Instant>,
    /// When the next attempt to reopen the device is due.
    next_attempt: Instant,

    /// The number to give the next port.
    next_port: u64,
    subscribers: HashMap<u64, SubscriberSender>,
    /// Packet sources report their port ID and notify when the port closes.
    inputs: StreamMap<u64, StreamNotifyClose<ReceiverStream<Packet>>>,
    /// The ports to drop before the next wait.
    leaving: HashSet<u64>,

    next_rpc_id: u16,
    rpc_map: HashMap<u16, RpcMapEntry>,
    rpc_timeouts: BTreeMap<Instant, HashSet<u16>>,
}

/// How long a device that has already sent a packet may stay silent before
/// its port is torn down and reopened.
const LIVENESS_TIMEOUT: Duration = Duration::from_millis(1000);

/// How long a TCP device that has already sent a packet may stay silent. It
/// allows one RTO plus slack.
const LIVENESS_TIMEOUT_TCP: Duration = Duration::from_millis(3000);

/// How long the worker waits after an attempt to reopen a device fails, or
/// opens a port the device never answers on, before the next attempt.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

/// How long a port at a new rate may go without receiving before it goes
/// back to its default rate.
const RATE_TRIAL: Duration = Duration::from_millis(1000);

static QUERY_RATE_RPC_ID: u16 = 0x101;
static SET_RATE_RPC_ID: u16 = 0x102;

impl Worker {
    /// Creates the worker of the device at `url`, which `open` opens. It
    /// adopts the ports `command_queue` brings, reports its state in
    /// `readiness`, and stops once every command sender has dropped.
    fn new(
        url: String,
        reconnect_timeout: Option<Duration>,
        command_queue: mpsc::Receiver<Command>,
        dump: DumpFlags,
        readiness: watch::Sender<Readiness>,
        open: Opener,
    ) -> Worker {
        Worker {
            log_target: format!("proxy::{url}"),
            url,
            reconnect_timeout,
            command_queue,
            dump,
            readiness,
            open,
            device: None,
            attempt: None,
            ever_connected: false,
            reconnect_deadline: None,
            next_attempt: Instant::now(),
            // Start from port 1, as 0 is reserved for internal RPCs.
            next_port: 1,
            subscribers: HashMap::new(),
            leaving: HashSet::new(),
            inputs: StreamMap::new(),
            next_rpc_id: 0,
            rpc_map: HashMap::new(),
            rpc_timeouts: BTreeMap::new(),
        }
    }

    /// Returns a future that opens the device's port.
    fn open_device(&self) -> BoxFuture<'static, io::Result<Device>> {
        let (sender, results) = flume::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
        let opening = self.open.open(sender);
        Box::pin(async move { Ok(Device::new(opening.await?, results)) })
    }

    /// Drops the port after it closed, failed, or went quiet. The ports
    /// stay open, and the worker reopens the device until the reconnect
    /// timeout passes. If the device had answered on the port, every port
    /// hears that it was lost. If not, the ports hear nothing, and the next
    /// attempt is put off as after a failed open.
    fn lose_device(&mut self) {
        let Some(device) = self.device.take() else {
            return;
        };
        let now = Instant::now();
        if self.reconnect_deadline.is_none() {
            self.reconnect_deadline = self.reconnect_timeout.map(|timeout| now + timeout);
        }
        if device.seen_since_connect {
            log::warn!(target: &self.log_target, "Sensor disconnected");
            self.broadcast_status(packet::LinkStatus::SensorDisconnected);
        } else {
            log::debug!("lost {} before it answered", self.url);
            self.next_attempt = now + RECONNECT_INTERVAL;
        }
    }

    /// Marks `port` for the worker to drop before its next wait, and logs it
    /// once.
    fn drop_subscriber(&mut self, port: u64) {
        if self.leaving.insert(port) {
            log::trace!(target: &self.log_target, "Port {port} terminated");
        }
    }

    /// Adopts a new port. A port that joins while the device is lost hears
    /// so first, as the others did, and then hears it come back with them.
    fn adopt(&mut self, subscriber: Subscriber) {
        let Subscriber { sender, rx } = subscriber;
        let port = self.next_port;
        self.next_port += 1;
        let answering = self
            .device
            .as_ref()
            .is_some_and(|device| device.seen_since_connect);
        if self.ever_connected && !answering {
            let lost = Packet::link(packet::LinkStatus::SensorDisconnected);
            sender.try_send(&lost);
        }
        self.inputs
            .insert(port, StreamNotifyClose::new(ReceiverStream::new(rx)));
        self.subscribers.insert(port, sender);
        log::trace!(target: &self.log_target, "Port {port} opened");
    }

    fn rpc_restore(&mut self, wire_id: u16, route: &DeviceRoute) -> Option<RpcMapEntry> {
        let remap = match self.rpc_map.remove(&wire_id) {
            None => {
                return None;
            }
            Some(r) => r,
        };
        if remap.route != *route {
            self.rpc_map.insert(wire_id, remap);
            return None;
        }
        if let Some(ids) = self.rpc_timeouts.get_mut(&remap.timeout) {
            ids.remove(&wire_id);
            if ids.is_empty() {
                self.rpc_timeouts.remove(&remap.timeout);
            }
        } else {
            log::warn!("Failed to find RPC timeout in map");
        }
        Some(remap)
    }

    fn next_wire_rpc_id(&mut self) -> Option<u16> {
        for _ in 0..=u16::MAX {
            let wire_id = self.next_rpc_id;
            self.next_rpc_id = self.next_rpc_id.wrapping_add(1);
            if !self.rpc_map.contains_key(&wire_id) {
                return Some(wire_id);
            }
        }
        None
    }

    /// Forwards a packet from port `port` to the device. Drops a control
    /// packet, and answers a request the device cannot be sent with an RPC
    /// error.
    async fn port_packet(&mut self, port: u64, pkt: Packet) {
        // A control packet talks to the proxy itself, never to a device.
        // Type 64 is also USER, which apps may send, so it is forwarded like
        // any other packet.
        if matches!(pkt.ptype(), PacketType::PROXY_TX | PacketType::PROXY_RX) {
            log::debug!("dropping a control packet from port {port}: {pkt:?}");
            return;
        }
        let reply_target = match pkt.payload() {
            packet::Payload::RpcRequest(req) => Some((pkt.route(), req.id.value())),
            _ => None,
        };
        if let Err(error) = self.forward_to_device(pkt, RpcOrigin::Port(port)).await {
            let (route, id) = reply_target.expect("only RPC requests produce forwarding errors");
            let subscriber = self
                .subscribers
                .get(&port)
                .expect("the port that sent is open");
            if !subscriber.try_send(&Packet::rpc_error(id, error, route)) {
                log::trace!(target: &self.log_target, "Port {port} fell behind");
                self.drop_subscriber(port);
            }
        }
    }

    // RPC failures are returned so the caller can notify the requesting port.
    async fn forward_to_device(
        &mut self,
        mut pkt: Packet,
        origin: RpcOrigin,
    ) -> Result<(), RpcError> {
        let mut mapped: Option<(u16, Instant)> = None;
        let from_port = match &origin {
            RpcOrigin::Port(port) => Some(*port),
            RpcOrigin::Internal => None,
        };
        // Decide from the borrowed view before rewriting the packet's bytes.
        let request = match pkt.payload() {
            packet::Payload::RpcRequest(req) => Some(req.id.value()),
            _ => None,
        };
        if let Some(original_id) = request {
            let Some(wire_id) = self.next_wire_rpc_id() else {
                return Err(RpcError::NoBufs);
            };
            let (target, timeout) = match origin {
                RpcOrigin::Port(port) => (
                    RpcTarget::Port { port, original_id },
                    // The port's own deadline comes first. This one only
                    // frees the wire id of a request never answered.
                    Instant::now() + MAX_RPC_TIMEOUT,
                ),
                RpcOrigin::Internal => (
                    RpcTarget::Internal(original_id),
                    Instant::now() + Duration::from_secs(1),
                ),
            };
            self.rpc_map.insert(
                wire_id,
                RpcMapEntry {
                    target,
                    route: pkt.route(),
                    timeout,
                },
            );
            if let Some(port) = from_port {
                log::trace!(
                    target: &self.log_target,
                    "RPC {original_id} of port {port} goes out as {wire_id}"
                );
            }
            pkt = pkt.with_rpc_id(wire_id);
            mapped = Some((wire_id, timeout));
        } else if from_port.is_none() {
            return Err(RpcError::Malformed);
        }
        if let Some(dev) = &self.device {
            if let Ok(()) = dev.port.send_async(pkt).await {
                if let Some((rpc_id, timeout)) = mapped {
                    self.rpc_timeouts.entry(timeout).or_default().insert(rpc_id);
                }
                return Ok(());
            }
        }
        // If we got here, the packet was not sent. avoid erroring out since if
        // there is something wrong with the device we'll notice in the main
        // loop soon but remove the rpc from the map and send back an error to
        // the port.
        if let Some((rpc_id, _)) = mapped {
            self.rpc_map
                .remove(&rpc_id)
                .expect("Unexpected missing timeout set");
            Err(RpcError::Undefined)
        } else {
            Ok(())
        }
    }

    fn broadcast_status(&self, status: packet::LinkStatus) {
        let pkt = Packet::link(status);
        for subscriber in self.subscribers.values() {
            subscriber.try_send(&pkt);
        }
    }

    /// Handles everything the device's port has received so far, without
    /// waiting.
    async fn receive_device(&mut self) {
        while let Some(result) = self
            .device
            .as_mut()
            .and_then(|device| device.results.next().now_or_never().flatten())
        {
            self.device_received(result).await;
        }
    }

    /// Handles `result`, which the device's port received. An I/O error or
    /// a disconnect loses the device.
    async fn device_received(&mut self, result: ReceiveResult) {
        let Some(device) = self.device.as_mut() else {
            return;
        };
        device.received(&result);
        match result {
            Ok(TransportEvent::Packet(pkt)) => {
                // First packet since (re)connect: the device is live, announce it.
                let first_packet = !std::mem::replace(&mut device.seen_since_connect, true);
                if first_packet {
                    self.reconnect_deadline = None;
                    if self.ever_connected {
                        log::info!(target: &self.log_target, "Sensor reconnected");
                        self.broadcast_status(packet::LinkStatus::SensorReconnected);
                    } else {
                        // Initial connect needs no wire status broadcast.
                        self.ever_connected = true;
                        log::trace!(target: &self.log_target, "Sensor connected");
                        self.readiness.send_replace(Readiness::Ready);
                    }
                }
                log_device_packet(&pkt, self.dump);
                self.dispatch_device_packet(pkt).await;
            }
            Ok(TransportEvent::Text(text)) => {
                log::info!(target: &self.log_target, "Text: {text}");
            }
            Ok(TransportEvent::RejectedFrame(perror)) => {
                log::debug!(target: &self.log_target, "Protocol error: {perror:?}");
            }
            // A port that failed is lost as one that closed is, and the
            // device is reopened.
            Err(err) => {
                log::debug!("lost {}: {err}", self.url);
                self.lose_device();
            }
        }
    }

    async fn dispatch_device_packet(&mut self, pkt: Packet) {
        let route = pkt.route();
        let Some(wire_id) = (match pkt.payload() {
            packet::Payload::RpcReply(reply) => Some(reply.req_id.value()),
            packet::Payload::RpcError(error) => Some(error.req_id.value()),
            _ => None,
        }) else {
            let mut to_drop = Vec::new();
            for (port, subscriber) in self.subscribers.iter() {
                if !subscriber.try_send(&pkt) {
                    log::trace!(target: &self.log_target, "Port {port} fell behind");
                    to_drop.push(*port);
                }
            }
            for port in to_drop {
                self.drop_subscriber(port);
            }
            return;
        };

        let Some(remap) = self.rpc_restore(wire_id, &route) else {
            log::trace!(target: &self.log_target, "No request waits for reply {wire_id}");
            return;
        };
        match remap.target {
            RpcTarget::Internal(original_id) => match pkt.payload() {
                packet::Payload::RpcReply(reply) => {
                    self.internal_rpc_reply(original_id, reply.value).await
                }
                packet::Payload::RpcError(error) => {
                    self.internal_rpc_error(RpcError::from(error.code))
                }
                _ => unreachable!(),
            },
            RpcTarget::Port { port, original_id } => {
                let Some(subscriber) = self.subscribers.get(&port) else {
                    log::trace!(
                        target: &self.log_target,
                        "Port {port} left before reply {wire_id}"
                    );
                    return;
                };
                log::trace!(
                    target: &self.log_target,
                    "Reply {wire_id} goes to port {port} as {original_id}"
                );
                let restored = pkt.with_rpc_id(original_id);
                if !subscriber.try_send(&restored) {
                    log::trace!(target: &self.log_target, "Port {port} fell behind");
                    self.drop_subscriber(port);
                }
            }
        }
    }

    /// Fails every request whose timeout has come by `until`, or every
    /// request with `None`. Each port gets an RPC error for `failure` in
    /// place of its reply. It times requests out, and fails those still
    /// waiting when the device disconnects or restarts.
    fn dispatch_rpc_errors(&mut self, failure: RpcFailure, until: Option<Instant>) {
        let mut to_remove = Vec::new();
        let mut failed = Vec::new();
        let mut to_drop = Vec::new();
        for (timeout, rpc_ids) in self.rpc_timeouts.iter() {
            if let Some(timeout_bound) = until {
                if *timeout > timeout_bound {
                    break;
                }
            }
            to_remove.push(*timeout);
            for rpc_id in rpc_ids {
                log::trace!(target: &self.log_target, "Request {rpc_id} failed: {failure:?}");
                let remap = self
                    .rpc_map
                    .remove(rpc_id)
                    .expect("RPC ID from timeout missing in main map");
                failed.push(remap);
            }
        }
        for timeout in to_remove {
            self.rpc_timeouts.remove(&timeout);
        }
        for remap in failed {
            match remap.target {
                RpcTarget::Port { port, original_id } => {
                    let Some(subscriber) = self.subscribers.get(&port) else {
                        continue;
                    };
                    if !subscriber.try_send(&Packet::rpc_error(
                        original_id,
                        failure.wire_error(),
                        remap.route,
                    )) {
                        to_drop.push(port);
                        log::debug!("Failed to send generated RPC error to port {port}");
                    }
                }
                RpcTarget::Internal(_) => {
                    self.internal_rpc_error(failure.wire_error());
                }
            }
        }
        for port in to_drop {
            self.drop_subscriber(port);
        }
    }

    /// Fails every request whose timeout has come.
    fn process_rpc_timeouts(&mut self) {
        self.dispatch_rpc_errors(RpcFailure::Timeout, Some(Instant::now()));
    }

    async fn send_internal_rpc(
        &mut self,
        name: &str,
        arg: &[u8],
        id: u16,
        route: DeviceRoute,
    ) -> Result<(), RpcError> {
        let pkt = Packet::rpc_request(name, arg, id, route).map_err(|_| RpcError::ArgsSize)?;
        self.forward_to_device(pkt, RpcOrigin::Internal).await
    }

    /// Handles a reply to an RPC the worker issued itself. Panics if `id` is
    /// not one of the worker's own request ids.
    async fn internal_rpc_reply(&mut self, id: u16, value: &[u8]) {
        fn get_rate_vars(worker: &Worker) -> Option<(RateChange, u32)> {
            if let Some(dev) = worker.device.as_ref() {
                dev.port
                    .rate_info()
                    .map(|rate_info| (dev.rate_change_state.clone(), rate_info.target_bps))
            } else {
                None
            }
        }

        if id == QUERY_RATE_RPC_ID {
            if let Some((RateChange::WaitingDeviceRate, target)) = get_rate_vars(self) {
                let next_state = if let Ok(raw) = <[u8; 4]>::try_from(value) {
                    let value = u32::from_le_bytes(raw);
                    let error = (((target as f64) - (value as f64)) / (value as f64)).abs();
                    if value == 0 || error > 0.015 {
                        log::trace!(
                            target: &self.log_target,
                            "The device offers {value} bps for {target}, so the rate stays"
                        );
                        RateChange::GaveUp
                    } else {
                        log::trace!(target: &self.log_target, "The device supports {value} bps");
                        RateChange::SetDeviceRate
                    }
                } else {
                    log::trace!(target: &self.log_target, "The device's rate reply did not decode");
                    RateChange::GaveUp
                };
                self.device.as_mut().expect("").rate_change_state = next_state;
                return;
            }
        } else if id == SET_RATE_RPC_ID {
            if let Some((RateChange::WaitingNewRate, target)) = get_rate_vars(self) {
                log::trace!(target: &self.log_target, "Setting the port to {target} bps");
                let dev = self.device.as_mut().expect("");
                let next_state = match dev.port.set_rate(target).await {
                    Ok(_) => RateChange::RateChanged,
                    Err(error) => {
                        log::trace!(target: &self.log_target, "The port refused the rate: {error}");
                        RateChange::GaveUp
                    }
                };
                dev.rate_change_state = next_state;
                return;
            }
        } else {
            // Note: internal RPCs still get remapped with all other RPCs,
            // so this ID does not come from the device itself, but from the
            // proxy remapping, and it should never be an unexpected value.
            panic!("Unexpected reply ID to internal RPC: {}", id)
        }

        log::debug!(
            "Unexpected internal rpc reply 0x{:x} in state {:?}",
            id,
            get_rate_vars(self)
        );
    }

    fn internal_rpc_error(&mut self, error: RpcError) {
        // We could handle this better, but just keep the device to the default speed until the port is reset
        log::trace!(target: &self.log_target, "The rate change failed: {error}");
        if let Some(dev) = self.device.as_mut() {
            dev.rate_change_state = RateChange::GaveUp;
        }
    }

    async fn autonegotiation(&mut self) {
        // When this is called, device will be Some, and it does not change
        // from any of the called methods, except where a failed port is lost
        // and this returns at once
        fn device(worker: &mut Worker) -> &mut Device {
            worker
                .device
                .as_mut()
                .expect("No device but in autonegotiation")
        }
        let next_state = match device(self).rate_change_state.clone() {
            RateChange::QueryDeviceRate => {
                let target = device(self).rates().target_bps;
                if let Err(rpc_error) = self
                    .send_internal_rpc(
                        "dev.port.rate.near",
                        &target.to_le_bytes(),
                        QUERY_RATE_RPC_ID,
                        DeviceRoute::root(),
                    )
                    .await
                {
                    log::trace!(target: &self.log_target, "Could not ask for a rate: {rpc_error}");
                    RateChange::GaveUp
                } else {
                    log::trace!(target: &self.log_target, "Asked the device for {target} bps");
                    RateChange::WaitingDeviceRate
                }
            }
            RateChange::SetDeviceRate => {
                if self.rpc_map.is_empty() {
                    let target = device(self).rates().target_bps;
                    if let Err(rpc_error) = self
                        .send_internal_rpc(
                            "dev.port.rate",
                            &target.to_le_bytes(),
                            SET_RATE_RPC_ID,
                            DeviceRoute::root(),
                        )
                        .await
                    {
                        log::trace!(
                            target: &self.log_target,
                            "Could not set the rate: {rpc_error}"
                        );
                        RateChange::GaveUp
                    } else {
                        log::trace!(
                            target: &self.log_target,
                            "Told the device to use {target} bps"
                        );
                        RateChange::WaitingNewRate
                    }
                } else {
                    log::trace!(target: &self.log_target, "Waiting for requests to finish");
                    RateChange::SetDeviceRate
                }
            }
            RateChange::RateChanged => {
                let trial_over = device(self)
                    .rate_trial_deadline()
                    .is_some_and(|deadline| Instant::now() >= deadline);
                if trial_over {
                    let dev = device(self);
                    let default_bps = dev.rates().default_bps;
                    if let Err(error) = dev.port.set_rate(default_bps).await {
                        log::debug!(
                            target: &self.log_target,
                            "The port could not go back to {default_bps} bps: {error}"
                        );
                        // The worker opens the device again, at the default rate.
                        self.lose_device();
                        return;
                    }
                    log::trace!(
                        target: &self.log_target,
                        "Nothing arrived at the new rate, so the port is back at {default_bps} bps"
                    );
                    RateChange::GaveUp
                } else {
                    RateChange::RateChanged
                }
            }
            // In any other case, do nothing
            current_state => current_state,
        };
        device(self).rate_change_state = next_state;
    }

    fn cancel_active_rpcs(&mut self) {
        self.dispatch_rpc_errors(RpcFailure::DeviceDisconnected, None);
    }

    /// Returns the earliest deadline the worker keeps. It is the first RPC
    /// timeout, the reconnect deadline, when the device counts as lost, or
    /// when the next attempt to reopen it is due.
    fn next_deadline(&self) -> Option<Instant> {
        let reopen = self.device.is_none() && self.attempt.is_none();
        [
            self.rpc_timeouts.keys().next().copied(),
            self.reconnect_deadline,
            self.device.as_ref().and_then(Device::liveness_deadline),
            self.device.as_ref().and_then(Device::rate_trial_deadline),
            reopen.then_some(self.next_attempt),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Opens the device, then runs the worker until it stops, and shuts it
    /// down. A stop during an open that the opener lets the worker drop
    /// shuts the worker down at once.
    async fn run(mut self) {
        let mut opening = self.open_device();
        let opened = match self.open.droppable {
            true => loop {
                tokio::select! {
                    opened = &mut opening => break opened,
                    command = self.command_queue.recv() => match command {
                        Some(Command::OpenPort(subscriber)) => self.adopt(subscriber),
                        None => {
                            log::trace!(target: &self.log_target, "The link closed while it opened");
                            self.shutdown().await;
                            return;
                        }
                    },
                }
            },
            false => opening.await,
        };
        match opened {
            Ok(device) => self.device = Some(device),
            Err(error) => {
                log::debug!(target: &self.log_target, "Failed to connect: {error}");
                let failed = Readiness::Failed("failed to connect".to_string());
                self.readiness.send_replace(failed);
                self.shutdown().await;
                return;
            }
        }
        while self.turn().await.is_continue() {}
        self.shutdown().await;
    }

    /// Runs one turn of the worker. Handles what the device has sent, acts
    /// on each deadline that has come, and takes the step of the rate
    /// negotiation that is due. Then waits for the next deadline or a
    /// channel, and handles it. Returns [`ControlFlow::Break`] once the
    /// worker stops.
    async fn turn(&mut self) -> ControlFlow<()> {
        // What arrived before a deadline counts before the deadline is
        // checked.
        self.receive_device().await;
        let now = Instant::now();
        self.process_rpc_timeouts();
        if self.device.is_none() {
            self.cancel_active_rpcs();
            if self.attempt.is_none() && now >= self.next_attempt {
                self.attempt = Some(self.open_device());
            }
        }
        // Giving up is the only way a lost device ends the worker. Only the
        // device answering clears the deadline, so a port that opens again
        // but never answers is given up too.
        if self
            .reconnect_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            log::error!(
                target: &self.log_target,
                "Stopping reconnection attempts due to timeout"
            );
            self.broadcast_status(packet::LinkStatus::FailedToReconnect);
            return ControlFlow::Break(());
        }
        let liveness = self.device.as_ref().and_then(Device::liveness_deadline);
        if liveness.is_some_and(|deadline| now >= deadline) {
            self.lose_device();
            return ControlFlow::Continue(());
        }
        let restarted = self
            .device
            .as_mut()
            .is_some_and(|device| std::mem::take(&mut device.restarted));
        if restarted {
            log::trace!(target: &self.log_target, "The root device restarted");
            self.cancel_active_rpcs();
        }
        for port in self.leaving.drain() {
            self.subscribers.remove(&port);
            self.inputs.remove(&port);
        }
        // A step falls due when what the device sent was just handled, or
        // when the trial of a new rate runs out, which `next_deadline` wakes
        // for. A step that is not due does nothing.
        if self
            .device
            .as_ref()
            .is_some_and(Device::needs_autonegotiation)
        {
            self.autonegotiation().await;
        }
        let wake = self.next_deadline();

        // The ports' packets wait in their queues while the device's rate
        // changes. Without a device, forwarding answers requests with errors.
        let forwarding = self.device.as_ref().is_none_or(Device::safe_to_forward);
        let connected = self.device.is_some();
        tokio::select! {
            command = self.command_queue.recv() => match command {
                Some(Command::OpenPort(subscriber)) => self.adopt(subscriber),
                None => {
                    log::trace!(target: &self.log_target, "The link closed");
                    return ControlFlow::Break(());
                }
            },
            Some((port, sent)) = self.inputs.next(), if forwarding && !self.inputs.is_empty() => {
                match sent {
                    Some(pkt) => {
                        let scope = self.subscribers[&port].scope;
                        let absolute = scope.absolute_route(&pkt.route())
                            .expect("Port validates scoped routes before enqueueing them");
                        self.port_packet(port, pkt.with_route(absolute)).await;
                    }
                    None => self.drop_subscriber(port),
                }
            }
            received = async { self.device.as_mut().expect("a device is present").results.next().await }, if connected => {
                let received = received.unwrap_or(Err(transport::RecvError::Disconnected));
                self.device_received(received).await;
            }
            opened = finish(&mut self.attempt) => match opened {
                Ok(device) => self.device = Some(device),
                Err(error) => {
                    log::debug!("failed to reopen {}: {error}", self.url);
                    self.next_attempt = Instant::now() + RECONNECT_INTERVAL;
                }
            },
            () = sleep_until_some(wake) => {}
        }
        ControlFlow::Continue(())
    }

    /// Refuses new ports and fails every request still waiting. Waits for an
    /// attempt in flight to reopen the device, or drops it if the opener
    /// allows. Closes the device's port and waits until it has let go of the
    /// device, for at most [`transport::CLOSE_TIMEOUT`]. Then drops the ports.
    async fn shutdown(&mut self) {
        self.command_queue.close();
        self.dispatch_rpc_errors(RpcFailure::ProxyClosed, None);
        // An open on a blocking thread cannot be stopped, and the device it
        // opens must be closed before the next worker opens it.
        if let Some(attempt) = self.attempt.take() {
            if !self.open.droppable {
                self.device = attempt.await.ok();
            }
        }
        if let Some(device) = self.device.take() {
            if !device.port.close(transport::CLOSE_TIMEOUT).await {
                log::warn!(
                    target: &self.log_target,
                    "The transport did not let go of the device within {:?}",
                    transport::CLOSE_TIMEOUT
                );
            }
        }
        self.inputs.clear();
        self.subscribers.clear();
    }
}

/// Logs a packet from the device if `dump` asks for its kind. Logs a device's
/// log packet under `device::<route>` at the device's level.
fn log_device_packet(pkt: &Packet, dump: DumpFlags) {
    let routing = pkt.route();
    let payload = pkt.payload();
    let dump = match payload {
        packet::Payload::Heartbeat(_) => dump.heartbeats,
        packet::Payload::Metadata(..) => dump.meta,
        packet::Payload::Samples(_) => dump.data,
        _ => dump.traffic,
    };
    if dump {
        log::info!("Packet from {} -- {:?}", routing, payload);
    }
    if let packet::Payload::Log(message) = payload {
        // Map the device-reported level onto the log crate's level
        // so the logger filter and prefix reflect it.
        let level = match message.level {
            LogLevel::CRITICAL | LogLevel::ERROR => log::Level::Error,
            LogLevel::WARNING => log::Level::Warn,
            LogLevel::DEBUG => log::Level::Debug,
            _ => log::Level::Info,
        };
        let text = String::from_utf8_lossy(message.message);
        log::log!(target: &format!("device::{routing}"), level, "{text}");
    }
}

fn is_serial(url: &str) -> bool {
    url.starts_with("serial://") || url.starts_with("/dev/") || url.starts_with("COM")
}

fn key_from(identity: &str) -> String {
    identity
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn serial_locator(url: &str) -> (&str, &str) {
    let locator = url.strip_prefix("serial://").unwrap_or(url);
    let path = locator.split(':').next().unwrap_or_default();
    (path, &locator[path.len()..])
}

/// Returns the key the proxy files a device's link under. A network URL is
/// its own key. A serial port is keyed by its USB serial number if it
/// reports one, else by its path. The baud rate is ignored.
pub(super) fn device_key(url: &str) -> String {
    if !is_serial(url) {
        return key_from(url);
    }
    let (path, _) = serial_locator(url);
    let path = match path.strip_prefix("/dev/tty.") {
        Some(rest) if cfg!(target_os = "macos") => format!("/dev/cu.{rest}"),
        _ => path.to_string(),
    };
    key_from(&usb_identity(&path).unwrap_or_else(|| {
        let canonical = fs::canonicalize(&path).unwrap_or_else(|_| path.into());
        format!("path:{}", canonical.display())
    }))
}

/// Returns the key of a serial `url`, or `None` for a network transport.
fn identity(url: &str) -> Option<String> {
    is_serial(url).then(|| device_key(url))
}

/// Returns the URL of the device with `identity` if a replug moved it off
/// `url`.
fn relocate(url: &str, identity: Option<&str>) -> Option<String> {
    let key = identity?;
    let (_, baud) = serial_locator(url);
    transport::enumerate_serial(true)
        .into_iter()
        .map(|device| device.url)
        .filter(|candidate| {
            !(cfg!(target_os = "macos") && candidate.starts_with("serial:///dev/tty."))
        })
        .find(|candidate| device_key(candidate) == key)
        .map(|candidate| format!("{candidate}{baud}"))
        .filter(|moved| moved != url)
}

/// Returns `usb:<vid>:<pid>:<serial>` for the port at `path`. Returns `None`
/// if the port reports no serial number or several ports share it.
#[cfg(feature = "serial")]
fn usb_identity(path: &str) -> Option<String> {
    let ports = serialport::available_ports().ok()?;
    let usb = |port: &serialport::SerialPortInfo| match &port.port_type {
        serialport::SerialPortType::UsbPort(info) => info
            .serial_number
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| format!("usb:{:04x}:{:04x}:{s}", info.vid, info.pid)),
        _ => None,
    };
    let identity = usb(ports.iter().find(|p| p.port_name == path)?)?;
    let sharing = ports
        .iter()
        .filter(|p| !(cfg!(target_os = "macos") && p.port_name.starts_with("/dev/tty.")))
        .filter(|p| usb(p).as_ref() == Some(&identity))
        .count();
    (sharing == 1).then_some(identity)
}

#[cfg(not(feature = "serial"))]
fn usb_identity(_path: &str) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::fixtures::{assert_took, UNREACHABLE, WAIT};
    use crate::rt;
    use crate::tio::transport::{FakeEnd, RateInfo, TransportKind};
    use std::future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    /// An opener that never opens anything.
    fn never() -> Opener {
        Opener::new(|_| Box::pin(future::pending()))
    }

    fn idle_worker() -> Worker {
        let (_commands, command_queue) = mpsc::channel(1);
        let (readiness, _) = watch::channel(Readiness::Pending);
        let dump = DumpFlags::default();
        Worker::new(String::new(), None, command_queue, dump, readiness, never())
    }

    /// The depth of the queue a test port is delivered to.
    const DELIVERED: usize = 64;

    /// A port at `scope`, and what it is delivered.
    fn subscriber(scope: DeviceRoute) -> (SubscriberSender, mpsc::Receiver<Packet>) {
        let (to_port, delivered) = mpsc::channel(DELIVERED);
        (SubscriberSender { tx: to_port, scope }, delivered)
    }

    fn insert_rpc(
        worker: &mut Worker,
        wire_id: u16,
        port: u64,
        original_id: u16,
        route: DeviceRoute,
        timeout: Instant,
    ) {
        worker.rpc_map.insert(
            wire_id,
            RpcMapEntry {
                target: RpcTarget::Port { port, original_id },
                route,
                timeout,
            },
        );
        worker
            .rpc_timeouts
            .entry(timeout)
            .or_default()
            .insert(wire_id);
    }

    #[test]
    fn keys_ignore_baud_and_stay_filesystem_safe() {
        assert_eq!(device_key("tcp://192.0.2.1:7855"), "tcp---192.0.2.1-7855");
        assert_eq!(
            device_key("serial:///dev/twinleaf-test"),
            device_key("serial:///dev/twinleaf-test:115200:115200")
        );
        assert_eq!(
            device_key("serial:///dev/twinleaf-none:9600"),
            key_from("path:/dev/twinleaf-none")
        );
        assert!(key_from("mount:usb/1=/1+tcp://h:1=/2")
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-'));
    }

    /// Waits for a worker's `task` to end, and checks that it did not panic.
    fn join(task: JoinHandle<Option<()>>) {
        let (ended, outcome) = flume::bounded(1);
        rt::handle().spawn(async move {
            let _ = ended.send(task.await);
        });
        let outcome = outcome
            .recv_timeout(WAIT)
            .expect("the worker never stopped");
        assert!(matches!(outcome, Ok(Some(()))), "the worker panicked");
    }

    /// A control packet talks to the proxy, so neither control type a port
    /// sends reaches the device. Type 64 is also `USER`, which apps may send,
    /// so it still does.
    #[test]
    fn a_ports_control_packets_never_reach_the_device() {
        use std::io::Read;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("tcp://{}", listener.local_addr().unwrap());
        let dump = DumpFlags::default();
        let tasks = Supervisor::new();
        let open = opener(&tasks, &url);
        let (commands, _readiness, run) = new(&url, None, dump, open);
        let worker = tasks.spawn("a test worker", run);
        let (mut device, _) = listener.accept().unwrap();

        let (to_port, _delivered) = mpsc::channel(DELIVERED);
        let (from_port, rx) = mpsc::channel(4);
        commands
            .try_send(Command::OpenPort(Subscriber::new(
                to_port,
                rx,
                DeviceRoute::root(),
            )))
            .unwrap();
        let user = Packet::legacy_status(packet::LinkStatus::SensorReconnected);
        for pkt in [
            Packet::proxy_request("proxy.open", b"auto", 1).unwrap(),
            Packet::proxy_reply(1, b"").unwrap(),
            Packet::link(packet::LinkStatus::SensorReconnected),
            user.clone(),
        ] {
            from_port.try_send(pkt).unwrap();
        }

        // The port's packets are forwarded in order, so a control packet
        // that got through would arrive ahead of the USER packet.
        let mut first = vec![0; user.as_bytes().len()];
        device
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        device.read_exact(&mut first).unwrap();
        assert_eq!(
            first,
            user.as_bytes(),
            "a control packet reached the device"
        );
        device
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        assert!(
            device.read(&mut [0; 1]).is_err(),
            "the device was sent more"
        );
        drop((commands, from_port));
        join(worker);
    }

    /// A status names a subtree, so a scoped port must receive it in its own
    /// coordinates, never in the proxy's, and never for a disjoint mount. The
    /// link packet and the legacy status are clamped alike.
    #[test]
    fn a_status_is_clamped_into_the_ports_subtree_or_skipped() {
        let forms: [fn(packet::LinkStatus) -> Packet; 2] = [Packet::link, Packet::legacy_status];
        for form in forms {
            let status = |route: &str| {
                form(packet::LinkStatus::SensorDisconnected).with_route(route.parse().unwrap())
            };
            let ptype = status("/").ptype();
            let delivered_routes = |mut delivered: mpsc::Receiver<Packet>| -> Vec<DeviceRoute> {
                std::iter::from_fn(|| delivered.try_recv().ok())
                    .map(|pkt| {
                        assert_eq!(pkt.ptype(), ptype, "the status keeps its form");
                        pkt.route()
                    })
                    .collect()
            };

            let (port, delivered) = subscriber("/1".parse().unwrap());
            assert!(port.try_send(&status("/")));
            assert!(port.try_send(&status("/1/3")));
            assert!(port.try_send(&status("/2")));
            assert_eq!(
                delivered_routes(delivered),
                [DeviceRoute::root(), "/3".parse().unwrap()],
                "{ptype:?}: clamped into port coordinates, the disjoint mount skipped"
            );
        }
    }

    #[test]
    fn rpc_replies_restore_independently_and_out_of_order() {
        let mut worker = idle_worker();
        let first_route: DeviceRoute = "/1".parse().unwrap();
        let second_route: DeviceRoute = "/2".parse().unwrap();
        let timeout = Instant::now() + Duration::from_secs(3);
        insert_rpc(&mut worker, 20, 3, 100, first_route, timeout);
        insert_rpc(&mut worker, 21, 4, 101, second_route, timeout);

        let remap = worker.rpc_restore(21, &second_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                port: 4,
                original_id: 101
            }
        ));
        let remap = worker.rpc_restore(20, &first_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                port: 3,
                original_id: 100
            }
        ));
        assert!(worker.rpc_map.is_empty());
        assert!(worker.rpc_timeouts.is_empty());
    }

    #[test]
    fn rpc_reply_from_the_wrong_route_does_not_consume_the_request() {
        let mut worker = idle_worker();
        let requested_route: DeviceRoute = "/1".parse().unwrap();
        let wrong_route: DeviceRoute = "/2".parse().unwrap();
        let timeout = Instant::now() + Duration::from_secs(3);
        insert_rpc(&mut worker, 20, 3, 100, requested_route, timeout);

        assert!(worker.rpc_restore(20, &wrong_route).is_none());
        assert!(worker.rpc_map.contains_key(&20));
        assert!(worker.rpc_timeouts[&timeout].contains(&20));
        let remap = worker.rpc_restore(20, &requested_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                port: 3,
                original_id: 100
            }
        ));
    }

    #[test]
    fn rpc_timeout_is_delivered_with_the_ports_request_id() {
        let mut worker = idle_worker();

        let (to_port, mut delivered) = mpsc::channel(1);
        let (_from_port, rx) = mpsc::channel(1);
        let port = 1;
        worker.adopt(Subscriber::new(to_port, rx, DeviceRoute::root()));

        let wire_id = 12;
        let original_id = 7855;
        let timeout = Instant::now() - Duration::from_millis(1);
        insert_rpc(
            &mut worker,
            wire_id,
            port,
            original_id,
            DeviceRoute::root(),
            timeout,
        );

        worker.process_rpc_timeouts();

        let packet = delivered.try_recv().unwrap();
        let packet::Payload::RpcError(error) = packet.payload() else {
            panic!("expected an RPC timeout packet");
        };
        assert_eq!(error.req_id.value(), original_id);
        assert!(matches!(RpcError::from(error.code), RpcError::Timeout));
        assert!(worker.rpc_map.is_empty());
        assert!(worker.rpc_timeouts.is_empty());
    }

    /// A device that goes away answers nobody, so every port with a request
    /// waiting hears that it failed, under the id it asked with, and nothing
    /// is replayed.
    #[test]
    fn a_disconnect_fails_every_ports_request_without_replaying_it() {
        let mut worker = idle_worker();
        let (first, first_delivered) = subscriber(DeviceRoute::root());
        let (second, second_delivered) = subscriber(DeviceRoute::root());
        worker.subscribers.insert(1, first);
        worker.subscribers.insert(2, second);
        let deadline = Instant::now() + Duration::from_secs(3);
        insert_rpc(&mut worker, 20, 1, 100, DeviceRoute::root(), deadline);
        insert_rpc(&mut worker, 21, 2, 200, DeviceRoute::root(), deadline);

        worker.cancel_active_rpcs();

        for (mut delivered, id) in [(first_delivered, 100), (second_delivered, 200)] {
            let failed = delivered.try_recv().unwrap();
            let packet::Payload::RpcError(error) = failed.payload() else {
                panic!("expected an RPC error, got {:?}", failed.payload());
            };
            assert_eq!(error.req_id.value(), id);
            assert_eq!(RpcError::from(error.code), RpcError::Undefined);
            assert!(delivered.try_recv().is_err(), "a request was failed twice");
        }
        assert!(worker.rpc_map.is_empty());
        assert!(worker.rpc_timeouts.is_empty());
    }

    /// Returns an opener whose attempt number `n`, counted from 0, runs
    /// `attempt(n, results)`.
    fn scripted<F>(
        attempt: impl Fn(usize, flume::Sender<ReceiveResult>) -> F + Send + Sync + 'static,
    ) -> Opener
    where
        F: Future<Output = io::Result<HardwarePort>> + Send + 'static,
    {
        let attempts = AtomicUsize::new(0);
        Opener::new(move |results| {
            let number = attempts.fetch_add(1, Ordering::Relaxed);
            Box::pin(attempt(number, results))
        })
    }

    /// Opens a fake port of `kind` that delivers to `results`, and hands its
    /// end to the test through `ends`.
    fn fake(
        kind: TransportKind,
        results: flume::Sender<ReceiveResult>,
        ends: &mpsc::UnboundedSender<FakeEnd>,
    ) -> io::Result<HardwarePort> {
        let (port, end) = HardwarePort::fake(kind, None, results);
        let _ = ends.send(end);
        Ok(port)
    }

    /// Returns an opener of fake ports of `kind`, and the receiver of their
    /// ends.
    fn fakes(kind: TransportKind) -> (Opener, mpsc::UnboundedReceiver<FakeEnd>) {
        let (ends, opened) = mpsc::unbounded_channel();
        let open = scripted(move |_, results| future::ready(fake(kind, results, &ends)));
        (open, opened)
    }

    /// Waits for the opener to open the next fake port, and returns its end.
    async fn opened(ends: &mut mpsc::UnboundedReceiver<FakeEnd>) -> FakeEnd {
        let opened = timeout(WAIT, ends.recv()).await;
        let opened = opened.expect("the worker opened nothing");
        opened.expect("the worker stopped")
    }

    /// A worker running as a task on the test's runtime, with one port on the
    /// whole tree.
    struct Running {
        commands: mpsc::Sender<Command>,
        readiness: watch::Receiver<Readiness>,
        /// Sends what the port sends to the device.
        to_device: mpsc::Sender<Packet>,
        /// What the port is delivered.
        port: mpsc::Receiver<Packet>,
        task: JoinHandle<Option<()>>,
    }

    impl Running {
        /// Runs a worker that opens its device with `open`, and gives up a
        /// lost device after `reconnect_timeout`. The port is in place before
        /// the worker starts, so it hears everything.
        fn start(open: Opener, reconnect_timeout: Option<Duration>) -> Running {
            let (commands, command_queue) = mpsc::channel(4);
            let (readiness, watched) = watch::channel(Readiness::Pending);
            let mut worker = Worker::new(
                "fake".to_string(),
                reconnect_timeout,
                command_queue,
                DumpFlags::default(),
                readiness,
                open,
            );
            let (to_port, port) = mpsc::channel(DELIVERED);
            let (to_device, rx) = mpsc::channel(4);
            worker.adopt(Subscriber::new(to_port, rx, DeviceRoute::root()));
            let task = Supervisor::current().spawn("a test worker", worker.run());
            Running {
                commands,
                readiness: watched,
                to_device,
                port,
                task,
            }
        }

        /// Waits until the worker's readiness is no longer `from`, and
        /// returns it.
        async fn settled(&mut self, from: Readiness) -> Readiness {
            let changed = self.readiness.wait_for(|readiness| *readiness != from);
            let settled = timeout(WAIT, changed).await;
            let settled = settled.unwrap_or_else(|_| panic!("the worker stayed {from:?}"));
            settled.expect("the worker kept its readiness").clone()
        }

        /// Closes the link, and waits for the worker to stop.
        async fn stop(self) {
            drop(self.commands);
            ended(self.task).await;
        }
    }

    /// Waits for a worker's `task` to end, and checks that it did not panic.
    async fn ended(task: JoinHandle<Option<()>>) {
        let ended = timeout(WAIT, task).await.expect("the worker never stopped");
        assert!(matches!(ended, Ok(Some(()))), "the worker panicked");
    }

    /// Waits for the next packet delivered to `port`. Lets the other tasks
    /// run before it lets time pass, a millisecond at a time.
    async fn next(port: &mut mpsc::Receiver<Packet>) -> Packet {
        let deadline = Instant::now() + WAIT;
        for polls in 0.. {
            if let Ok(pkt) = port.try_recv() {
                return pkt;
            }
            assert!(Instant::now() < deadline, "nothing was delivered");
            if polls < 10 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        unreachable!()
    }

    /// Waits for the next `count` packets delivered to `port`, and returns
    /// each one's link status or packet type.
    async fn delivered(port: &mut mpsc::Receiver<Packet>, count: usize) -> Vec<String> {
        let mut seen = Vec::new();
        for _ in 0..count {
            let pkt = next(port).await;
            seen.push(match pkt.link_status() {
                Some(status) => format!("{status:?}"),
                None => format!("{:?}", pkt.ptype()),
            });
        }
        seen
    }

    /// Returns the id and error of `pkt`, an RPC error.
    fn rpc_error(pkt: &Packet) -> (u16, RpcError) {
        let packet::Payload::RpcError(error) = pkt.payload() else {
            panic!("expected an RPC error, got {:?}", pkt.payload());
        };
        (error.req_id.value(), RpcError::from(error.code))
    }

    fn heartbeat() -> Packet {
        Packet::heartbeat(DeviceRoute::root())
    }

    /// The type of [`heartbeat`], as [`delivered`] gives it.
    fn beat() -> String {
        format!("{:?}", heartbeat().ptype())
    }

    /// A closed input drains its queued packets and releases its port.
    /// Other inputs retain their scopes and keep forwarding both ways.
    #[tokio::test(start_paused = true)]
    async fn a_closed_input_drains_without_disrupting_other_scoped_ports() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let mut worker = Running::start(open, None);
        let mut device = opened(&mut ends).await;
        device.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

        let (first_output, mut first_port) = mpsc::channel(DELIVERED);
        let (first_input, first_rx) = mpsc::channel(4);
        let (second_output, mut second_port) = mpsc::channel(DELIVERED);
        let (second_input, second_rx) = mpsc::channel(4);
        for (tx, rx, scope) in [
            (first_output, first_rx, "/1"),
            (second_output, second_rx, "/2"),
        ] {
            worker
                .commands
                .send(Command::OpenPort(Subscriber::new(
                    tx,
                    rx,
                    scope.parse().unwrap(),
                )))
                .await
                .unwrap();
        }
        first_input.send(heartbeat()).await.unwrap();
        first_input
            .send(heartbeat().with_route("/3".parse().unwrap()))
            .await
            .unwrap();
        drop(first_input);
        second_input
            .send(heartbeat().with_route("/4".parse().unwrap()))
            .await
            .unwrap();

        let mut routes = Vec::new();
        for _ in 0..3 {
            let sent = timeout(WAIT, device.recv()).await.unwrap().unwrap();
            routes.push(sent.route().to_string());
        }
        routes.sort();
        assert_eq!(routes, ["/1", "/1/3", "/2/4"]);
        assert!(timeout(WAIT, first_port.recv()).await.unwrap().is_none());

        second_input
            .send(heartbeat().with_route("/5".parse().unwrap()))
            .await
            .unwrap();
        let sent = timeout(WAIT, device.recv()).await.unwrap().unwrap();
        assert_eq!(sent.route(), "/2/5".parse().unwrap());
        device
            .send(&heartbeat().with_route("/2/6".parse().unwrap()))
            .await;
        assert_eq!(next(&mut second_port).await.route(), "/6".parse().unwrap());
        worker.stop().await;
    }

    /// A port that fails with an I/O error loses the device as one that
    /// closes does, so the worker reconnects instead of stopping. The port it
    /// had stays, and hears the device drop out, come back, and send again.
    /// The link is ready from the first packet on.
    #[tokio::test(start_paused = true)]
    async fn an_io_error_is_a_disconnect_and_the_worker_reconnects() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let mut worker = Running::start(open, None);
        let mut first = opened(&mut ends).await;
        first.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

        first.fail(io::ErrorKind::ConnectionReset);

        let mut second = opened(&mut ends).await;
        second.send(&heartbeat()).await;
        assert_eq!(
            delivered(&mut worker.port, 4).await,
            [
                beat(),
                "SensorDisconnected".to_string(),
                "SensorReconnected".to_string(),
                beat(),
            ]
        );
        assert!(!worker.task.is_finished());
        assert_eq!(*worker.readiness.borrow(), Readiness::Ready);
        worker.stop().await;
    }

    /// A port that joins while the device is lost hears so at once, though
    /// the device's port is open again, and then hears the device come back
    /// along with the port that was there.
    #[tokio::test(start_paused = true)]
    async fn a_port_that_joins_while_the_device_is_lost_hears_so() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let mut worker = Running::start(open, None);
        let mut first = opened(&mut ends).await;
        first.send(&heartbeat()).await;
        first.fail(io::ErrorKind::ConnectionReset);
        let mut second = opened(&mut ends).await;
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );

        let (to_port, mut joined) = mpsc::channel(DELIVERED);
        let (_to_device, rx) = mpsc::channel(1);
        let subscriber = Subscriber::new(to_port, rx, DeviceRoute::root());
        let opening = worker.commands.send(Command::OpenPort(subscriber));
        opening.await.unwrap();
        assert_eq!(delivered(&mut joined, 1).await, ["SensorDisconnected"]);
        second.send(&heartbeat()).await;

        for port in [&mut worker.port, &mut joined] {
            assert_eq!(
                delivered(port, 2).await,
                ["SensorReconnected".to_string(), beat()]
            );
        }
        worker.stop().await;
    }

    /// A device lost for good ends the worker once the reconnect timeout
    /// passes, and its port hears that reconnecting failed before it closes.
    /// The link's readiness stays as it was.
    #[tokio::test(start_paused = true)]
    async fn a_device_that_never_comes_back_ends_the_worker_with_failed_to_reconnect() {
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |attempt, results| {
            future::ready(match attempt {
                0 => fake(TransportKind::Tcp, results, &ends),
                _ => Err(io::ErrorKind::ConnectionRefused.into()),
            })
        });
        let reconnect_timeout = Duration::from_millis(200);
        let mut worker = Running::start(open, Some(reconnect_timeout));
        let mut first = opened(&mut opened_ends).await;
        first.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

        first.fail(io::ErrorKind::ConnectionReset);
        let lost = Instant::now();

        ended(worker.task).await;
        assert_took(lost.elapsed(), reconnect_timeout, "giving up");
        assert_eq!(*worker.readiness.borrow(), Readiness::Ready);
        assert_eq!(
            delivered(&mut worker.port, 3).await[1..],
            ["SensorDisconnected", "FailedToReconnect"]
        );
        assert!(matches!(
            worker.port.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
    }

    /// A port lost before its device answers on it is a failed attempt. The
    /// ports hear nothing more of it, the next attempt waits the reconnect
    /// interval, and the reconnect timeout still ends the worker however
    /// readily the port opens.
    #[tokio::test(start_paused = true)]
    async fn a_transport_lost_before_its_device_answers_is_a_failed_attempt() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let mut worker = Running::start(open, Some(Duration::from_millis(200)));
        let mut first = opened(&mut ends).await;
        first.send(&heartbeat()).await;
        assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

        first.fail(io::ErrorKind::ConnectionReset);

        // Each attempt opens a port, which closes before the device says
        // anything. The ends stop once the worker has stopped.
        let mut attempts = 0;
        while let Some(end) = timeout(WAIT, ends.recv()).await.expect("never gave up") {
            attempts += 1;
            assert_eq!(attempts, 1, "an attempt came before the interval");
            drop(end);
        }
        assert_eq!(attempts, 1);
        assert_eq!(
            delivered(&mut worker.port, 3).await[1..],
            ["SensorDisconnected", "FailedToReconnect"],
            "a failed attempt was reported to the port"
        );
        ended(worker.task).await;
        assert_eq!(*worker.readiness.borrow(), Readiness::Ready);
    }

    /// A device that answers again clears the reconnect timeout, so the
    /// worker keeps it once the timeout has passed.
    #[tokio::test(start_paused = true)]
    async fn a_device_that_answers_again_outlasts_the_reconnect_timeout() {
        let (open, mut ends) = fakes(TransportKind::Tcp);
        let reconnect_timeout = Duration::from_millis(200);
        let mut worker = Running::start(open, Some(reconnect_timeout));
        let mut first = opened(&mut ends).await;
        first.send(&heartbeat()).await;
        first.fail(io::ErrorKind::ConnectionReset);
        let mut second = opened(&mut ends).await;
        second.send(&heartbeat()).await;
        assert_eq!(
            delivered(&mut worker.port, 3).await[1..],
            ["SensorDisconnected", "SensorReconnected"]
        );

        // Well within how long a TCP device may stay silent.
        tokio::time::sleep(2 * reconnect_timeout).await;

        assert!(!worker.task.is_finished(), "the worker gave up");
        assert_eq!(*worker.readiness.borrow(), Readiness::Ready);
        worker.stop().await;
    }

    /// A device that has answered is lost once it stays silent for a second,
    /// or three over TCP. Its port is opened again at once.
    #[tokio::test(start_paused = true)]
    async fn a_silent_device_is_lost_after_a_second_or_three_over_tcp() {
        for (kind, silence) in [
            (TransportKind::Serial, LIVENESS_TIMEOUT),
            (TransportKind::Udp, LIVENESS_TIMEOUT),
            (TransportKind::Tcp, LIVENESS_TIMEOUT_TCP),
        ] {
            let (open, mut ends) = fakes(kind);
            let mut worker = Running::start(open, None);
            let mut first = opened(&mut ends).await;
            first.send(&heartbeat()).await;
            let answered = Instant::now();

            let _second = opened(&mut ends).await;

            assert_took(answered.elapsed(), silence, kind);
            assert_eq!(
                delivered(&mut worker.port, 2).await,
                [beat(), "SensorDisconnected".to_string()]
            );
            worker.stop().await;
        }
    }

    /// While an attempt to reopen the device hangs, a port that joins still
    /// hears at once that the device is lost. A port's request fails at once,
    /// as does the request the device took before it was lost.
    #[tokio::test(start_paused = true)]
    async fn a_hung_reopen_delays_neither_a_new_port_nor_a_ports_rpc_errors() {
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |attempt, results| {
            let opened = (attempt == 0).then(|| fake(TransportKind::Tcp, results, &ends));
            async move {
                match opened {
                    Some(opened) => opened,
                    None => future::pending().await,
                }
            }
        });
        let mut worker = Running::start(open, None);
        let mut device = opened(&mut opened_ends).await;
        device.send(&heartbeat()).await;
        let request = |id| Packet::rpc_request("dev.name", &[], id, DeviceRoute::root()).unwrap();
        worker.to_device.send(request(7)).await.unwrap();
        let taken = timeout(WAIT, device.recv()).await.unwrap().unwrap();
        assert!(matches!(taken.payload(), packet::Payload::RpcRequest(_)));

        device.fail(io::ErrorKind::ConnectionReset);
        let lost = Instant::now();

        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        assert_eq!(
            rpc_error(&next(&mut worker.port).await),
            (7, RpcError::Undefined)
        );
        let (to_port, mut joined) = mpsc::channel(DELIVERED);
        let (to_device, rx) = mpsc::channel(1);
        let subscriber = Subscriber::new(to_port, rx, DeviceRoute::root());
        worker
            .commands
            .try_send(Command::OpenPort(subscriber))
            .unwrap();
        assert_eq!(delivered(&mut joined, 1).await, ["SensorDisconnected"]);
        to_device.try_send(request(8)).unwrap();
        assert_eq!(
            rpc_error(&next(&mut joined).await),
            (8, RpcError::Undefined)
        );
        assert_eq!(
            lost.elapsed(),
            Duration::ZERO,
            "a port waited for the reopen"
        );
    }

    /// A worker that stops while an attempt to reopen the device is in
    /// flight waits for the attempt, however long it takes. It closes the
    /// port the attempt opened before it ends, and drops its ports last.
    #[tokio::test(start_paused = true)]
    async fn shutdown_waits_for_a_reopen_in_flight() {
        let gate = Arc::new(Notify::new());
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = {
            let gate = gate.clone();
            scripted(move |attempt, results| {
                let (gate, ends) = (gate.clone(), ends.clone());
                async move {
                    if attempt > 0 {
                        gate.notified().await;
                    }
                    fake(TransportKind::Tcp, results, &ends)
                }
            })
        };
        let mut worker = Running::start(open, None);
        let mut first = opened(&mut opened_ends).await;
        first.send(&heartbeat()).await;
        first.fail(io::ErrorKind::ConnectionReset);
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        let Running {
            commands,
            mut port,
            task,
            ..
        } = worker;

        drop(commands);
        tokio::time::sleep(WAIT).await;

        assert!(!task.is_finished(), "the worker left an attempt in flight");
        assert!(matches!(port.try_recv(), Err(TryRecvError::Empty)));
        gate.notify_one();
        let reopened = opened(&mut opened_ends).await;
        ended(task).await;
        assert!(
            reopened.released(),
            "the reopened port still holds the device"
        );
        assert!(matches!(port.try_recv(), Err(TryRecvError::Disconnected)));
    }

    /// Returns a droppable opener whose first open is a fake TCP port, handed
    /// to the test through `ends`, if `first_opens`. Every other open never
    /// ends, and the receiver it returns disconnects once the open is
    /// dropped.
    fn droppable_hang(
        first_opens: bool,
        ends: mpsc::UnboundedSender<FakeEnd>,
    ) -> (Opener, tokio::sync::oneshot::Receiver<()>) {
        let (held, dropped) = tokio::sync::oneshot::channel();
        let held = std::sync::Mutex::new(Some(held));
        let attempts = AtomicUsize::new(0);
        let open = Opener::droppable(move |results| -> Opening {
            if first_opens && attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                return Box::pin(future::ready(fake(TransportKind::Tcp, results, &ends)));
            }
            let held = held.lock().unwrap().take();
            Box::pin(async move {
                let _held = held;
                future::pending().await
            })
        });
        (open, dropped)
    }

    /// A worker whose opener lets it drop an open in flight, as a TCP opener
    /// does, ends at once when its link closes during the first open. The
    /// open is dropped, and the link never became ready.
    #[tokio::test(start_paused = true)]
    async fn a_link_closed_during_a_droppable_first_open_ends_at_once() {
        let (ends, _opened_ends) = mpsc::unbounded_channel();
        let (open, mut dropped) = droppable_hang(false, ends);
        let Running {
            commands,
            readiness,
            task,
            ..
        } = Running::start(open, None);
        tokio::time::sleep(WAIT).await;

        drop(commands);
        let closed = Instant::now();

        ended(task).await;
        assert_eq!(
            closed.elapsed(),
            Duration::ZERO,
            "the worker waited for the open"
        );
        assert_eq!(*readiness.borrow(), Readiness::Pending);
        assert!(dropped.try_recv().is_err(), "the open in flight was kept");
    }

    /// A worker whose opener lets it drop an open in flight ends at once when
    /// its link closes while it reopens the device, and drops the open.
    #[tokio::test(start_paused = true)]
    async fn a_link_closed_during_a_droppable_reopen_ends_at_once() {
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let (open, mut dropped) = droppable_hang(true, ends);
        let mut worker = Running::start(open, None);
        let mut first = opened(&mut opened_ends).await;
        first.send(&heartbeat()).await;
        first.fail(io::ErrorKind::ConnectionReset);
        assert_eq!(
            delivered(&mut worker.port, 2).await,
            [beat(), "SensorDisconnected".to_string()]
        );
        tokio::time::sleep(WAIT).await;
        assert!(
            dropped
                .try_recv()
                .is_err_and(|error| error == tokio::sync::oneshot::error::TryRecvError::Empty),
            "no reopen is in flight"
        );

        drop(worker.commands);
        let closed = Instant::now();

        ended(worker.task).await;
        assert_eq!(
            closed.elapsed(),
            Duration::ZERO,
            "the worker waited for the reopen"
        );
        assert!(
            dropped
                .try_recv()
                .is_err_and(|error| error == tokio::sync::oneshot::error::TryRecvError::Closed),
            "the reopen in flight was kept"
        );
    }

    /// The rate negotiation gives up as soon as the device turns the target
    /// rate down, and then wakes the worker no more.
    #[tokio::test(start_paused = true)]
    async fn the_rate_negotiation_gives_up_at_once_and_wakes_no_more() {
        let rates = RateInfo {
            default_bps: 115200,
            target_bps: 230400,
        };
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |_, results| {
            let kind = TransportKind::Serial;
            let (port, end) = HardwarePort::fake(kind, Some(rates.clone()), results);
            let _ = ends.send(end);
            future::ready(Ok(port))
        });
        let (_commands, command_queue) = mpsc::channel(1);
        let (readiness, _readiness) = watch::channel(Readiness::Pending);
        let dump = DumpFlags::default();
        let mut worker = Worker::new(
            "fake".to_string(),
            None,
            command_queue,
            dump,
            readiness,
            open,
        );
        worker.device = Some(worker.open_device().await.unwrap());
        // The device starts a session, and offers its default rate when it
        // is asked for the target.
        let mut device = opened(&mut opened_ends).await;
        tokio::spawn(async move {
            device
                .send(&Packet::heartbeat_session(1, DeviceRoute::root()))
                .await;
            while let Some(pkt) = device.recv().await {
                let reply = match pkt.payload() {
                    packet::Payload::RpcRequest(request) => {
                        let rate = 115200u32.to_le_bytes();
                        Packet::rpc_reply(request.id.value(), &rate, pkt.route()).ok()
                    }
                    _ => None,
                };
                if let Some(reply) = reply {
                    device.send(&reply).await;
                }
            }
        });
        let began = Instant::now();
        let gave_up = |worker: &Worker| {
            let device = worker.device.as_ref().expect("the device stays");
            matches!(device.rate_change_state, RateChange::GaveUp)
        };

        while !gave_up(&worker) {
            let turned = timeout(WAIT, worker.turn()).await;
            assert!(turned.is_ok(), "the negotiation stalled");
        }
        assert_eq!(began.elapsed(), Duration::ZERO, "the negotiation waited");

        let turned = timeout(LIVENESS_TIMEOUT / 2, worker.turn()).await;
        assert!(turned.is_err(), "the negotiation still wakes the worker");
    }

    /// Each step of the rate negotiation goes out as soon as what makes it
    /// due arrives. The device's first session heartbeat brings the query
    /// for the target rate, and the device's answer brings the switch, with
    /// no time passing in between.
    #[tokio::test(start_paused = true)]
    async fn each_step_of_the_rate_negotiation_goes_out_at_once() {
        use crate::proto::rpc::Method;

        let rates = RateInfo {
            default_bps: 115200,
            target_bps: 230400,
        };
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |_, results| {
            let kind = TransportKind::Serial;
            let (port, end) = HardwarePort::fake(kind, Some(rates.clone()), results);
            let _ = ends.send(end);
            future::ready(Ok(port))
        });
        let _worker = Running::start(open, None);
        let mut device = opened(&mut opened_ends).await;
        let began = Instant::now();

        device
            .send(&Packet::heartbeat_session(1, DeviceRoute::root()))
            .await;
        let mut asked = Vec::new();
        while asked.len() < 2 {
            let pkt = timeout(WAIT, device.recv()).await.unwrap().unwrap();
            let packet::Payload::RpcRequest(request) = pkt.payload() else {
                continue;
            };
            let Method::ByName(name) = request.method else {
                continue;
            };
            asked.push((String::from_utf8_lossy(name).into_owned(), began.elapsed()));
            let rate = 230400u32.to_le_bytes();
            let reply = Packet::rpc_reply(request.id.value(), &rate, pkt.route()).unwrap();
            device.send(&reply).await;
        }

        let at_once = |name: &str| (name.to_string(), Duration::ZERO);
        assert_eq!(
            asked,
            [at_once("dev.port.rate.near"), at_once("dev.port.rate")]
        );
    }

    /// A port that fails as it goes back to the default rate loses the
    /// device, and the worker runs on to open it again.
    #[tokio::test(start_paused = true)]
    async fn a_port_that_cannot_go_back_to_the_default_rate_loses_the_device() {
        let rates = RateInfo {
            default_bps: 115200,
            target_bps: 230400,
        };
        let (ends, mut opened_ends) = mpsc::unbounded_channel();
        let open = scripted(move |_, results| {
            let kind = TransportKind::Serial;
            let (port, end) = HardwarePort::fake(kind, Some(rates.clone()), results);
            let _ = ends.send(end);
            future::ready(Ok(port))
        });
        let (_commands, command_queue) = mpsc::channel(1);
        let (readiness, _readiness) = watch::channel(Readiness::Pending);
        let dump = DumpFlags::default();
        let mut worker = Worker::new(
            "fake".to_string(),
            None,
            command_queue,
            dump,
            readiness,
            open,
        );
        worker.device = Some(worker.open_device().await.unwrap());
        worker.device.as_mut().unwrap().rate_change_state = RateChange::RateChanged;

        // The device is unplugged, so its port ends, and nothing arrives at
        // the new rate.
        drop(opened(&mut opened_ends).await);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        worker.autonegotiation().await;

        assert!(worker.device.is_none(), "the worker kept a failed port");
    }

    /// A port that opens again is no reconnect until its device answers on
    /// it. So a silent TCP peer, or a UDP device, whose port always opens,
    /// still ends the worker once the reconnect timeout passes.
    #[tokio::test]
    async fn a_device_that_never_answers_again_ends_the_worker_with_failed_to_reconnect() {
        // Neither ever answers, and the TCP one never even accepts.
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        for url in [
            format!("tcp://{}", tcp.local_addr().unwrap()),
            format!("udp://{}", udp.local_addr().unwrap()),
        ] {
            // The device answers on a fake port at first, and is reopened at
            // `url`.
            let (ends, mut opened_ends) = mpsc::unbounded_channel();
            let reopen = opener(&Supervisor::current(), &url);
            let open = scripted(
                move |attempt, results| -> BoxFuture<'static, io::Result<HardwarePort>> {
                    match attempt {
                        0 => Box::pin(future::ready(fake(TransportKind::Tcp, results, &ends))),
                        _ => reopen.open(results),
                    }
                },
            );
            let mut worker = Running::start(open, Some(Duration::from_millis(200)));
            let mut first = opened(&mut opened_ends).await;
            first.send(&heartbeat()).await;
            assert_eq!(worker.settled(Readiness::Pending).await, Readiness::Ready);

            first.fail(io::ErrorKind::ConnectionReset);

            assert_eq!(
                delivered(&mut worker.port, 3).await[1..],
                ["SensorDisconnected", "FailedToReconnect"],
                "{url}"
            );
            ended(worker.task).await;
        }
    }

    /// A worker whose first open fails stops, and the link learns why.
    #[tokio::test]
    async fn a_first_open_that_fails_ends_the_worker_and_says_so() {
        let (_commands, command_queue) = mpsc::channel(1);
        let (readiness, watched) = watch::channel(Readiness::Pending);
        let dump = DumpFlags::default();
        let open = opener(&Supervisor::current(), UNREACHABLE);

        Worker::new(
            UNREACHABLE.to_string(),
            None,
            command_queue,
            dump,
            readiness,
            open,
        )
        .run()
        .await;

        assert_eq!(
            *watched.borrow(),
            Readiness::Failed("failed to connect".to_string())
        );
    }
}

//! Wire and server plumbing: the [`Connection`] and the packet [`Port`]s that
//! multiplex one transport.
//!
//! A proxy thread owns the hardware port and fans it out to any number of
//! `Port`s, each restricted to a subtree and a traffic class, negotiating the
//! serial rate on the way. This is what a proxy *server* serves its clients
//! with; applications reach devices through [`Connection`](crate::Connection),
//! whose views hand out packets, events and samples already filtered to what
//! they cover.

use super::proto::route::RouteError;
use super::proto::{self, DeviceRoute, Packet, ProxyStatus};
use super::proxy_core::{ProxyClient, ProxyCommand, ProxyCore};
use super::transport;
use twinleaf_proto::rpc as wire_rpc;

use std::env;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel;

pub const DEFAULT_URL: &str = "tcp://localhost";

/// Status event that ProxyCore sent back to an optional user specified channel
#[derive(Debug)]
pub enum Event {
    /// First packet received from the device, not merely a transport opened.
    SensorConnected,
    /// The transport was torn down, either by an I/O failure or by the liveness
    /// watchdog after the device went quiet for `LIVENESS_TIMEOUT`.
    SensorDisconnected,
    /// First packet received after a disconnect.
    SensorReconnected,
    FailedToConnect,
    FailedToReconnect,
    Exiting,
    Text(String),
    ProtocolError(proto::DecodeError),
    FatalError(transport::RecvError),
    NewClient(u64),
    RpcRemap((u64, u16), u16),
    RpcRestore(u16, (u64, u16)),
    RpcRestoreNotFound(u16),
    RpcClientNotFound(u64),
    RpcTimeout(u16),
    RpcCancel(u16),
    ClientSendFailed(u64),
    ClientTerminated(u64),
    RootDeviceRestarted,
    AutoRateGaveUp,
    AutoRateQueried(u32),
    AutoRateRpcError(wire_rpc::RpcError),
    AutoRateRpcInvalid,
    AutoRateIncompatible(u32),
    AutoRateCompatible(u32),
    AutoRateWait,
    AutoRateSet(u32),
    SetRate(u32),
    SetRateFailed,
    NoData,
}

impl From<ProxyStatus> for super::proxy::Event {
    fn from(status: ProxyStatus) -> Self {
        match status {
            ProxyStatus::SensorDisconnected => Event::SensorDisconnected,
            ProxyStatus::SensorReconnected => Event::SensorReconnected,
            ProxyStatus::FailedToReconnect => Event::FailedToReconnect,
            ProxyStatus::FailedToConnect => Event::FailedToConnect,
            ProxyStatus::Unknown(_) => Event::SensorDisconnected,
        }
    }
}

/// A port which communicates with a proxy via `crossbeam::channel`s
pub struct Port {
    tx: channel::Sender<Packet>,
    rx: channel::Receiver<Packet>,
    depth: usize,
    scope: DeviceRoute,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SendError {
    #[error("channel full")]
    WouldBlock(Packet),
    #[error("proxy disconnected")]
    ProxyDisconnected(Packet),
    #[error("route exceeds port scope")]
    InvalidRoute(Packet),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum RecvError {
    #[error("no packet available")]
    WouldBlock,
    #[error("proxy disconnected")]
    ProxyDisconnected,
}

/// Error returned by a receive operation with a time bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecvTimeoutError {
    #[error("timed out waiting for a packet")]
    Timeout,
    #[error("proxy disconnected")]
    ProxyDisconnected,
}

impl Port {
    /// Test-only port wired directly to channel pairs, with no proxy behind
    /// it. Returns the port, the far end receiving what the port sends, and
    /// the far end for delivering packets to the port.
    #[cfg(test)]
    pub(crate) fn test_pair() -> (Port, channel::Receiver<Packet>, channel::Sender<Packet>) {
        let (tx, sent) = channel::bounded(256);
        let (deliver, rx) = channel::bounded(256);
        (
            Port {
                tx,
                rx,
                depth: 0,
                scope: DeviceRoute::root(),
            },
            sent,
            deliver,
        )
    }

    /// Sends a TIO packet to this port synchronously. This call will
    /// block if the port is backed up.
    #[allow(clippy::result_large_err)] // Preserve ownership when submission fails.
    pub fn send(&self, packet: Packet) -> Result<(), SendError> {
        let route = packet.route();
        if route.len() > self.depth || self.scope.absolute_route(&route).is_err() {
            return Err(SendError::InvalidRoute(packet));
        }
        match self.tx.send(packet) {
            Ok(()) => Ok(()),
            Err(se) => Err(SendError::ProxyDisconnected(se.into_inner())),
        }
    }

    /// Attempts to send a TIO packet to this port without blocking.
    #[allow(clippy::result_large_err)] // Preserve ownership when submission fails.
    pub fn try_send(&self, packet: Packet) -> Result<(), SendError> {
        let route = packet.route();
        if route.len() > self.depth || self.scope.absolute_route(&route).is_err() {
            return Err(SendError::InvalidRoute(packet));
        }
        match self.tx.try_send(packet) {
            Ok(()) => Ok(()),
            Err(crossbeam::channel::TrySendError::Full(pkt)) => Err(SendError::WouldBlock(pkt)),
            Err(crossbeam::channel::TrySendError::Disconnected(pkt)) => {
                Err(SendError::ProxyDisconnected(pkt))
            }
        }
    }

    /// Waits for a packet to be available, and returns it.
    pub fn recv(&self) -> Result<Packet, RecvError> {
        match self.rx.recv() {
            Ok(pkt) => Ok(pkt),
            Err(crossbeam::channel::RecvError) => Err(RecvError::ProxyDisconnected),
        }
    }

    /// Waits up to `timeout` for a packet to be available.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Packet, RecvTimeoutError> {
        match self.rx.recv_timeout(timeout) {
            Ok(pkt) => Ok(pkt),
            Err(channel::RecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
            Err(channel::RecvTimeoutError::Disconnected) => {
                Err(RecvTimeoutError::ProxyDisconnected)
            }
        }
    }

    /// Waits until `deadline` for a packet to be available.
    ///
    /// An absolute deadline can be reused across a loop without extending the
    /// caller's overall time budget after unrelated packets are processed.
    pub fn recv_deadline(&self, deadline: Instant) -> Result<Packet, RecvTimeoutError> {
        match self.rx.recv_deadline(deadline) {
            Ok(pkt) => Ok(pkt),
            Err(channel::RecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
            Err(channel::RecvTimeoutError::Disconnected) => {
                Err(RecvTimeoutError::ProxyDisconnected)
            }
        }
    }

    /// Returns a packet if available, otherwise it doesn't stop.
    pub fn try_recv(&self) -> Result<Packet, RecvError> {
        match self.rx.try_recv() {
            Ok(pkt) => Ok(pkt),
            Err(crossbeam::channel::TryRecvError::Empty) => Err(RecvError::WouldBlock),
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                Err(RecvError::ProxyDisconnected)
            }
        }
    }

    /// To use `crossbeam::channel::select!`.
    pub fn receiver(&self) -> &crossbeam::channel::Receiver<Packet> {
        &self.rx
    }

    /// Iterate over packets (until disconnect or break out).
    pub fn iter(&self) -> crossbeam::channel::Iter<'_, Packet> {
        self.rx.iter()
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum PortError {
    #[error("RPC timeout too short")]
    RpcTimeoutTooShort,
    #[error("RPC timeout too long")]
    RpcTimeoutTooLong,
    /// The worker's command lane is full. Transient: it is still running, and
    /// the same request may succeed once it drains.
    #[error("the proxy is busy")]
    ProxyBusy,
    /// The proxy worker has stopped. Terminal.
    #[error("failed to set up new proxy client")]
    FailedNewClientSetup,
}

/// The RPC budget a caller gets without asking, and the range it may ask for.
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_millis(3000);
const MIN_RPC_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_RPC_TIMEOUT: Duration = Duration::from_secs(60);

fn checked_rpc_timeout(timeout: Option<Duration>) -> Result<Duration, PortError> {
    let timeout = timeout.unwrap_or(DEFAULT_RPC_TIMEOUT);
    if timeout < MIN_RPC_TIMEOUT {
        return Err(PortError::RpcTimeoutTooShort);
    }
    if timeout > MAX_RPC_TIMEOUT {
        return Err(PortError::RpcTimeoutTooLong);
    }
    Ok(timeout)
}

/// An owned low-level RPC result delivered directly by the proxy worker.
pub(crate) type RawCallResult = Result<Vec<u8>, RawCallError>;

/// What a direct call does with its outcome, exactly once. It runs on the
/// proxy thread, so it must hand the outcome off without blocking.
pub(crate) type Completion = Box<dyn FnOnce(RawCallResult) + Send>;

/// Why a raw RPC submitted directly to the proxy did not return a value.
///
/// This deliberately contains no device-layer decoding or registry concepts.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum RawCallError {
    #[error("RPC route exceeds endpoint scope: {0}")]
    InvalidRoute(#[source] RouteError),
    #[error("RPC request was not submitted to the proxy")]
    RequestNotSubmitted,
    #[error("RPC timed out in the proxy")]
    Timeout,
    #[error("device disconnected while the RPC was in flight")]
    DeviceDisconnected,
    #[error("proxy worker stopped while the RPC was in flight")]
    ProxyClosed,
    #[error("device returned RPC error {error}")]
    Device {
        error: wire_rpc::RpcError,
        message: Vec<u8>,
    },
}

/// Cloneable access to the one proxy worker behind a [`Connection`].
#[derive(Clone)]
pub(crate) struct ProxyHandle {
    commands: channel::Sender<ProxyCommand>,
    /// Disconnects when the worker stops. A queued command outlives the
    /// worker's receiver, so a stranded port never disconnects on its own.
    worker_alive: channel::Receiver<()>,
    client_rx_channel_size: usize,
    client_tx_channel_size: usize,
}

impl ProxyHandle {
    /// Queue a port for the worker to adopt, never waiting on a worker that may
    /// be blocked acquiring a transport that never opens.
    fn register_port(&self, client: ProxyClient) -> Result<(), PortError> {
        match self.commands.try_send(ProxyCommand::OpenPort { client }) {
            Ok(()) => Ok(()),
            Err(channel::TrySendError::Full(_)) => Err(PortError::ProxyBusy),
            Err(channel::TrySendError::Disconnected(_)) => Err(PortError::FailedNewClientSetup),
        }
    }

    fn open_port(
        &self,
        rpc_timeout: Duration,
        scope: DeviceRoute,
        depth: usize,
        forward_data: bool,
        forward_nonrpc: bool,
    ) -> Result<Port, PortError> {
        let (client_to_proxy_sender, proxy_from_client_receiver) =
            channel::bounded::<Packet>(self.client_tx_channel_size);
        let (proxy_to_client_sender, client_from_proxy_receiver) =
            channel::bounded::<Packet>(self.client_rx_channel_size);
        self.register_port(ProxyClient::new(
            proxy_to_client_sender,
            proxy_from_client_receiver,
            rpc_timeout,
            scope,
            depth,
            forward_data,
            forward_nonrpc,
        ))?;
        Ok(Port {
            tx: client_to_proxy_sender,
            rx: client_from_proxy_receiver,
            depth,
            scope,
        })
    }
}

/// Scoped raw-RPC capability. The proxy worker owns request IDs and progress.
#[derive(Clone)]
pub(crate) struct RpcEndpoint {
    proxy: ProxyHandle,
    scope: DeviceRoute,
    depth: usize,
    timeout: Duration,
}

impl RpcEndpoint {
    pub(crate) fn scope(&self) -> DeviceRoute {
        self.scope
    }

    pub(crate) fn depth(&self) -> usize {
        self.depth
    }

    /// Disconnects when the proxy worker stops.
    pub(crate) fn worker_alive(&self) -> &channel::Receiver<()> {
        &self.proxy.worker_alive
    }

    /// Submit a call and receive its outcome on a oneshot. See
    /// [`submit_with`](Self::submit_with) to run something else with it.
    pub(crate) fn submit(
        &self,
        route: DeviceRoute,
        name: &str,
        args: &[u8],
    ) -> oneshot::Receiver<RawCallResult> {
        let (resolve, reply) = oneshot::channel();
        self.submit_with(
            route,
            name,
            args,
            Box::new(move |outcome| {
                let _ = resolve.send(outcome);
            }),
        );
        reply
    }

    /// Submit a call; `complete` runs exactly once with its outcome, here if
    /// the request cannot leave, otherwise on the proxy thread.
    pub(crate) fn submit_with(
        &self,
        route: DeviceRoute,
        name: &str,
        args: &[u8],
        complete: Completion,
    ) {
        let relative = match self.scope.relative_route(&route) {
            Ok(relative) => relative,
            Err(error) => return complete(Err(RawCallError::InvalidRoute(error))),
        };
        if relative.len() > self.depth {
            return complete(Err(RawCallError::InvalidRoute(RouteError::OutsideSubtree)));
        }
        let Ok(request) = Packet::rpc_request(name, args, 0, route) else {
            return complete(Err(RawCallError::RequestNotSubmitted));
        };
        let command = ProxyCommand::Call {
            request,
            timeout: self.timeout,
            complete,
        };
        let (refused, error) = match self.commands().try_send(command) {
            Ok(()) => return,
            Err(channel::TrySendError::Full(command)) => {
                (command, RawCallError::RequestNotSubmitted)
            }
            Err(channel::TrySendError::Disconnected(command)) => {
                (command, RawCallError::ProxyClosed)
            }
        };
        let ProxyCommand::Call { complete, .. } = refused else {
            unreachable!("the refused command is the call just built");
        };
        complete(Err(error));
    }

    fn commands(&self) -> &channel::Sender<ProxyCommand> {
        &self.proxy.commands
    }

    /// The same capability narrowed to exactly `scope`, which this endpoint
    /// must already cover: no view may widen its own authority.
    pub(crate) fn scoped(&self, scope: DeviceRoute) -> Result<RpcEndpoint, RouteError> {
        Ok(RpcEndpoint {
            depth: 0,
            ..self.subtree(scope)?
        })
    }

    /// The same capability narrowed to the subtree at `scope`, keeping the
    /// depth that remains below it.
    pub(crate) fn subtree(&self, scope: DeviceRoute) -> Result<RpcEndpoint, RouteError> {
        let below = self.scope.relative_route(&scope)?.len();
        let Some(depth) = self.depth.checked_sub(below) else {
            return Err(RouteError::OutsideSubtree);
        };
        Ok(RpcEndpoint {
            proxy: self.proxy.clone(),
            scope,
            depth,
            timeout: self.timeout,
        })
    }

    /// The same capability reaching no deeper than `depth`. Narrowing only:
    /// a view may not reach past what it was given.
    pub(crate) fn to_depth(&self, depth: usize) -> RpcEndpoint {
        RpcEndpoint {
            depth: depth.min(self.depth),
            ..self.clone()
        }
    }

    /// The same capability on a different RPC budget, clamped to the range the
    /// proxy accepts: a view is never worth failing over one.
    pub(crate) fn with_timeout(&self, timeout: Duration) -> RpcEndpoint {
        RpcEndpoint {
            timeout: timeout.clamp(MIN_RPC_TIMEOUT, MAX_RPC_TIMEOUT),
            ..self.clone()
        }
    }

    pub(crate) fn open_port(
        &self,
        forward_data: bool,
        forward_nonrpc: bool,
    ) -> Result<Port, PortError> {
        self.proxy.open_port(
            self.timeout,
            self.scope,
            self.depth,
            forward_data,
            forward_nonrpc,
        )
    }

    /// Test-only endpoint with no proxy behind it. Returns the endpoint, the
    /// far end receiving the commands it submits, and the lifeline the worker
    /// would hold: dropping it is the worker stopping.
    #[cfg(test)]
    pub(crate) fn test_pair(
        scope: DeviceRoute,
        depth: usize,
    ) -> (
        RpcEndpoint,
        channel::Receiver<ProxyCommand>,
        channel::Sender<()>,
    ) {
        let (commands, receiver) = channel::bounded(16);
        let (alive, worker_alive) = channel::bounded(0);
        (
            RpcEndpoint {
                proxy: ProxyHandle {
                    commands,
                    worker_alive,
                    client_rx_channel_size: 4,
                    client_tx_channel_size: 4,
                },
                scope,
                depth,
                timeout: Duration::from_secs(3),
            },
            receiver,
            alive,
        )
    }
}

/// The transport half of a link to one device tree, over serial, TCP, or UDP.
///
/// Opening one starts the I/O thread that owns the transport and reconnects on
/// its own. This is the half a proxy server runs on;
/// [`Connection`](crate::Connection) is what an application opens.
pub struct Connection {
    proxy: ProxyHandle,
}

impl Connection {
    /// Open a connection to `url` and start driving its transport.
    pub fn open(url: &str) -> Connection {
        Self::open_with(url, None, None)
    }

    /// Open a connection that gives up on a reconnect after `reconnect_timeout`
    /// and reports transport events to `status_queue`.
    pub fn open_with(
        url: &str,
        reconnect_timeout: Option<Duration>,
        status_queue: Option<channel::Sender<Event>>,
    ) -> Connection {
        let (command_sender, command_receiver) = channel::bounded::<ProxyCommand>(64);
        let (status_sender, only_clients) = {
            if let Some(status_sender) = status_queue {
                (status_sender, false)
            } else {
                let (sender, _receiver) = channel::bounded::<Event>(1);
                (sender, true)
            }
        };
        let url_string = url.to_string();
        let (alive, worker_alive) = channel::bounded(0);
        thread::spawn(move || {
            #[cfg(target_os = "windows")]
            let _priority = super::os::windows_helpers::ActivityGuard::latency_critical()
                .map_err(|e| log::warn!("proxy core: failed to raise thread priority: {e}"))
                .ok();

            #[cfg(target_os = "macos")]
            let _activity =
                super::os::macos_helpers::ActivityGuard::latency_critical("Twinleaf proxy core");

            let mut proxy = ProxyCore::new(
                url_string,
                reconnect_timeout,
                command_receiver,
                status_sender,
                only_clients,
                alive,
            );
            proxy.run();
        });
        Connection {
            proxy: ProxyHandle {
                commands: command_sender,
                worker_alive,
                client_rx_channel_size: client_rx_channel_size(),
                client_tx_channel_size: client_tx_channel_size(),
            },
        }
    }

    pub(crate) fn rpc_endpoint(
        &self,
        rpc_timeout: Option<Duration>,
        scope: DeviceRoute,
        depth: usize,
    ) -> Result<RpcEndpoint, PortError> {
        Ok(RpcEndpoint {
            proxy: self.proxy.clone(),
            scope,
            depth,
            timeout: checked_rpc_timeout(rpc_timeout)?,
        })
    }
}

/// Open a raw packet port on `connection`, filtered to `depth` levels under
/// `scope`: one client of the proxy mux, which is what a server hands out.
/// Applications tap the wire with
/// [`DeviceTree::packets`](crate::DeviceTree::packets) instead.
pub fn open_port(
    connection: &Connection,
    rpc_timeout: Option<Duration>,
    scope: DeviceRoute,
    depth: usize,
    forward_data: bool,
    forward_nonrpc: bool,
) -> Result<Port, PortError> {
    let rpc_timeout = checked_rpc_timeout(rpc_timeout)?;
    connection
        .proxy
        .open_port(rpc_timeout, scope, depth, forward_data, forward_nonrpc)
}

/// Depth of the queue the proxy fills for each of its ports.
pub fn client_rx_channel_size() -> usize {
    let min_size = transport::DEFAULT_RX_CHANNEL_SIZE;
    if let Ok(req) = env::var("TWINLEAF_PROXY_INTERFACE_RX_BUFSIZE") {
        std::cmp::max(req.parse().unwrap_or(0), min_size)
    } else {
        min_size
    }
}

/// Depth of the queue each port fills for the proxy.
pub fn client_tx_channel_size() -> usize {
    let min_size = transport::DEFAULT_TX_CHANNEL_SIZE;
    if let Ok(req) = env::var("TWINLEAF_PROXY_INTERFACE_TX_BUFSIZE") {
        std::cmp::max(req.parse().unwrap_or(0), min_size)
    } else {
        min_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_port() -> (Port, channel::Sender<Packet>) {
        let (client_tx, _proxy_rx) = channel::bounded(1);
        let (proxy_tx, client_rx) = channel::bounded(1);
        let port = Port {
            tx: client_tx,
            rx: client_rx,
            depth: usize::MAX,
            scope: DeviceRoute::root(),
        };
        (port, proxy_tx)
    }

    #[test]
    fn recv_deadline_returns_a_queued_packet() {
        let (port, proxy_tx) = test_port();
        proxy_tx
            .send(Packet::heartbeat(DeviceRoute::root()))
            .unwrap();

        let packet = port
            .recv_deadline(Instant::now() + Duration::from_millis(100))
            .unwrap();

        assert!(matches!(packet.payload(), proto::Payload::Heartbeat(_)));
    }

    #[test]
    fn recv_deadline_reports_timeout() {
        let (port, _proxy_tx) = test_port();

        assert!(matches!(
            port.recv_deadline(Instant::now() + Duration::from_millis(5)),
            Err(RecvTimeoutError::Timeout)
        ));
    }

    #[test]
    fn recv_deadline_reports_disconnect() {
        let (port, proxy_tx) = test_port();
        drop(proxy_tx);

        assert!(matches!(
            port.recv_deadline(Instant::now() + Duration::from_millis(100)),
            Err(RecvTimeoutError::ProxyDisconnected)
        ));
    }

    #[test]
    fn endpoint_submits_an_absolute_routed_call_without_a_caller_id() {
        let scope: DeviceRoute = "/1".parse().unwrap();
        let route: DeviceRoute = "/1/2".parse().unwrap();
        let (endpoint, commands, _worker) = RpcEndpoint::test_pair(scope, 1);

        let _pending = endpoint.submit(route, "dev.name", b"arg");
        let ProxyCommand::Call {
            request, timeout, ..
        } = commands.recv().unwrap()
        else {
            panic!("expected a direct RPC command");
        };

        assert_eq!(request.route(), route);
        assert_eq!(timeout, Duration::from_secs(3));
        let proto::Payload::RpcRequest(request) = request.payload() else {
            panic!("expected an RPC request");
        };
        assert_eq!(request.id.value(), 0);
        assert_eq!(request.args, b"arg");
        assert!(matches!(
            request.method,
            wire_rpc::Method::ByName(name) if name == b"dev.name"
        ));
    }

    /// A worker stuck acquiring a transport it will never open must not hold up
    /// the caller: `tio dump -r tcp://<unreachable>` has to reach its deadline.
    #[test]
    fn opening_a_port_does_not_wait_for_a_worker_that_never_answers() {
        let (endpoint, commands, _worker) = RpcEndpoint::test_pair(DeviceRoute::root(), 0);

        let _port = endpoint.open_port(true, true).expect("the port is queued");

        assert!(matches!(
            commands.try_recv(),
            Ok(ProxyCommand::OpenPort { .. })
        ));
    }

    #[test]
    fn opening_a_port_fails_once_the_worker_is_gone() {
        let (endpoint, commands, _worker) = RpcEndpoint::test_pair(DeviceRoute::root(), 0);
        drop(commands);

        assert!(matches!(
            endpoint.open_port(true, true),
            Err(PortError::FailedNewClientSetup)
        ));
    }

    /// A congested command lane says nothing about whether the worker lives,
    /// so it must never be reported as the worker being gone.
    #[test]
    fn a_full_command_lane_is_distinct_from_a_stopped_worker() {
        let (endpoint, commands, worker) = RpcEndpoint::test_pair(DeviceRoute::root(), 0);
        while endpoint.open_port(true, true).is_ok() {}

        assert!(matches!(
            endpoint.open_port(true, true),
            Err(PortError::ProxyBusy)
        ));
        drop(worker);
        drop(commands);
        assert!(matches!(
            endpoint.open_port(true, true),
            Err(PortError::FailedNewClientSetup)
        ));
    }

    #[test]
    fn endpoint_rejects_routes_outside_its_scope_or_depth() {
        let scope: DeviceRoute = "/1".parse().unwrap();
        let (endpoint, commands, _worker) = RpcEndpoint::test_pair(scope, 1);

        let outside: DeviceRoute = "/2".parse().unwrap();
        assert!(matches!(
            endpoint.submit(outside, "dev.name", b"").recv(),
            Ok(Err(RawCallError::InvalidRoute(RouteError::OutsideSubtree)))
        ));
        let too_deep: DeviceRoute = "/1/2/3".parse().unwrap();
        assert!(matches!(
            endpoint.submit(too_deep, "dev.name", b"").recv(),
            Ok(Err(RawCallError::InvalidRoute(RouteError::OutsideSubtree)))
        ));
        assert!(commands.is_empty());
    }
}

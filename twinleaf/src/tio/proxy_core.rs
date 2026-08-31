use super::proto::{self, DeviceRoute, Packet, PacketType, RpcMethod};
use super::proxy::{Event, RawCallError, RawCallResult};
use super::transport;
use super::transport::Port as HardwarePort;
use super::transport::{ReceiveResult, RecvError};
use twinleaf_proto::heartbeat::Heartbeat;
use twinleaf_proto::rpc::RpcError;

use std::io;
use std::time::{Duration, Instant};

use std::collections::{BTreeMap, HashMap, HashSet};

use crossbeam::channel;

struct StatusQueue {
    dest: channel::Sender<Event>,
    only_new_client: bool,
}

impl StatusQueue {
    fn send(&self, event: Event) {
        if match &event {
            Event::NewClient(_) => true,
            _ => !self.only_new_client,
        } {
            match self.dest.try_send(event) {
                Ok(()) | Err(channel::TrySendError::Disconnected(_)) => {}
                Err(channel::TrySendError::Full(event)) => {
                    log::warn!("dropping proxy status event because its queue is full: {event:?}");
                }
            }
        }
    }
}

/// Internal proxy state per client
#[derive(Debug)]
pub(crate) struct ProxyClient {
    /// Used to send packets to the client
    tx: channel::Sender<Packet>,

    /// Used to receive packets from the client
    rx: channel::Receiver<Packet>,

    /// Configurable (per-client) timeout for RPCs
    rpc_timeout: Duration,

    /// Restrict traffic to devices in the device tree at or under this node.
    /// Addresses are stripped of this common prefix on receive, and augmented
    /// with it on transmit.
    scope: DeviceRoute,

    /// Restrict traffic to devices at most this deep under the scope root.
    depth: usize,

    /// Forward sample data.
    forward_data: bool,

    /// Forward packets that are not sample data nor RPC-related.
    forward_nonrpc: bool,
}

pub(crate) enum ProxyCommand {
    OpenPort {
        client: ProxyClient,
    },
    Call {
        request: Packet,
        timeout: Duration,
        result: channel::Sender<RawCallResult>,
    },
}

impl ProxyClient {
    pub fn new(
        tx: channel::Sender<Packet>,
        rx: channel::Receiver<Packet>,
        rpc_timeout: Duration,
        scope: DeviceRoute,
        depth: usize,
        forward_data: bool,
        forward_nonrpc: bool,
    ) -> ProxyClient {
        ProxyClient {
            tx,
            rx,
            rpc_timeout,
            scope,
            depth,
            forward_data,
            forward_nonrpc,
        }
    }

    fn try_send(&self, pkt: &Packet) -> bool {
        // ProxyStatus should be route-agnostic
        if pkt.ptype() == PacketType::PROXY_STATUS {
            return self.tx.try_send(pkt.clone()).is_ok();
        }

        let scoped_route = if let Ok(r) = self.scope.relative_route(&pkt.route()) {
            if r.len() <= self.depth {
                r
            } else {
                return true;
            }
        } else {
            return true;
        };
        if !match pkt.ptype() {
            PacketType::RPC_REQ | PacketType::RPC_REP | PacketType::RPC_ERROR => true,
            _ if pkt.is_data() => self.forward_data,
            _ => self.forward_nonrpc,
        } {
            return true;
        }
        self.tx.try_send(pkt.with_route(scoped_route)).is_ok()
    }

    fn recv(&self) -> Result<Packet, channel::TryRecvError> {
        let pkt = self.rx.try_recv()?;
        let absolute = self
            .scope
            .absolute_route(&pkt.route())
            .expect("Port validates scoped routes before enqueueing them");
        Ok(pkt.with_route(absolute))
    }
}

/// States for the rate autonegotiation state machine
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

struct ProxyDevice {
    tio_port: HardwarePort,
    rx_channel: channel::Receiver<ReceiveResult>,
    rate_change_state: RateChange,
    last_rx: Instant,
    seen_since_connect: bool,
    last_session: Option<u32>,
    restarted: bool,
    rpc_meta: HashMap<String, u16>,
    pending_broadcasts: Vec<(String, DeviceRoute, Option<u64>)>,
    pending_lookup: Option<(String, DeviceRoute)>,
}

impl ProxyDevice {
    /// True if this device does not have a settable data rate.
    fn has_static_rate(&self) -> bool {
        matches!(self.rate_change_state, RateChange::DoNothing)
    }

    /// True if this device needs to run the periodic rate negotiation task.
    /// Note that this is true even after the higher rate has been negotiated,
    /// to deal with reverting back to the default rate after some time goes
    /// by without seeing data.
    fn needs_autonegotiation(&self) -> bool {
        !matches!(
            self.rate_change_state,
            RateChange::DoNothing | RateChange::GaveUp
        )
    }

    /// True if it's safe to forward packets to the device due to rate
    /// negotiation concerns. Specifically, packets might be lost around
    /// when the rate transitions, so we hold back on forwarding traffic then.
    fn safe_to_forward(&self) -> bool {
        !matches!(
            self.rate_change_state,
            RateChange::SetDeviceRate | RateChange::WaitingNewRate
        )
    }

    /// How long this device may stay silent before the watchdog tears it down.
    fn liveness_timeout(&self) -> Duration {
        match self.tio_port.kind() {
            transport::TransportKind::Tcp => LIVENESS_TIMEOUT_TCP,
            transport::TransportKind::Serial | transport::TransportKind::Udp => LIVENESS_TIMEOUT,
        }
    }

    /// Convenience method to get the rate information for this device,
    /// when already known it has settable data rate.
    fn rates(&self) -> transport::RateInfo {
        self.tio_port
            .rate_info()
            .expect("Rates requested for unsupported device")
    }

    fn try_recv(
        &mut self,
        status_queue: &StatusQueue,
    ) -> Result<ReceiveResult, crossbeam::channel::TryRecvError> {
        let res = self.rx_channel.try_recv()?;
        // Any received packet (text included) refreshes liveness, for every
        // device — the watchdog needs a timestamp regardless of rate state.
        match &res {
            Ok(_) | Err(RecvError::Text(_)) => {
                self.last_rx = Instant::now();
            }
            _ => {}
        }
        if !self.has_static_rate() {
            if let Ok(pkt) = &res {
                if let proto::Payload::Heartbeat(Heartbeat::Session(session)) = pkt.payload() {
                    let session = session.value();
                    if pkt.route().is_empty() {
                        // This is a heartbeat for the root sensor
                        let old_session = self.last_session.replace(session);
                        if let RateChange::WaitingForSession = self.rate_change_state {
                            self.rate_change_state = RateChange::QueryDeviceRate;
                        } else if (self.last_session != old_session) && old_session.is_some() {
                            status_queue.send(Event::RootDeviceRestarted);
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
        Ok(res)
    }
}

enum RpcTarget {
    Port { client_id: u64, original_id: u16 },
    Direct(channel::Sender<RawCallResult>),
    Internal(u16),
}

enum RpcOrigin {
    Port(u64),
    Direct {
        result: channel::Sender<RawCallResult>,
        timeout: Duration,
    },
    Internal,
}

#[derive(Clone, Copy)]
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

    fn direct_error(self) -> RawCallError {
        match self {
            Self::Timeout => RawCallError::Timeout,
            Self::DeviceDisconnected => RawCallError::DeviceDisconnected,
            Self::ProxyClosed => RawCallError::ProxyClosed,
        }
    }
}

struct RpcMapEntry {
    target: RpcTarget,
    route: DeviceRoute,
    timeout: Instant,
    has_arg: bool,
    method: RpcMethod,
}

pub(crate) struct ProxyCore {
    url: String,
    reconnect_timeout: Option<Duration>,
    command_queue: channel::Receiver<ProxyCommand>,
    status_queue: StatusQueue,

    device: Option<ProxyDevice>,

    ever_connected: bool,

    /// Id to assign to the next client, 64 bits.
    /// It is realistic to assume that it will never wrap around.
    next_client_id: u64,
    clients: HashMap<u64, ProxyClient>,
    clients_to_drop: HashSet<u64>,

    next_rpc_id: u16,
    rpc_map: HashMap<u16, RpcMapEntry>,
    rpc_timeouts: BTreeMap<Instant, HashSet<u16>>,
}

/// How long a device that has already sent a packet may stay silent before
/// its transport is torn down and reconnected.
const LIVENESS_TIMEOUT: Duration = Duration::from_millis(1000);

/// Allow one RTO plus slack
const LIVENESS_TIMEOUT_TCP: Duration = Duration::from_millis(3000);

/// Extra sleep so the mainloop wakes just after liveness expires, not just before.
const LIVENESS_WAKE_SLACK: Duration = Duration::from_millis(1);

/// Polling interval while retrying to reopen a device.
const RECONNECT_POLL_INTERVAL: Duration = Duration::from_secs(1);

static QUERY_RATE_RPC_ID: u16 = 0x101;
static SET_RATE_RPC_ID: u16 = 0x102;
static RPC_INFO_LOOKUP_ID: u16 = 0x103;

impl ProxyCore {
    pub fn new(
        url: String,
        reconnect_timeout: Option<Duration>,
        command_queue: channel::Receiver<ProxyCommand>,
        status_queue: channel::Sender<Event>,
        notify_new_client_only: bool,
    ) -> ProxyCore {
        ProxyCore {
            url,
            reconnect_timeout,
            command_queue,
            status_queue: StatusQueue {
                dest: status_queue,
                only_new_client: notify_new_client_only,
            },
            device: None,
            ever_connected: false,
            // Start from client 1, as 0 is reserved for internal RPCs.
            next_client_id: 1,
            clients: HashMap::new(),
            clients_to_drop: HashSet::new(),
            next_rpc_id: 0,
            rpc_map: HashMap::new(),
            rpc_timeouts: BTreeMap::new(),
        }
    }

    fn try_setup_device(&mut self) -> io::Result<()> {
        if self.device.is_some() {
            return Ok(());
        }
        let (port_rx_send, port_rx) = HardwarePort::rx_channel();
        let port = HardwarePort::new(&self.url, HardwarePort::rx_to_channel(port_rx_send))?;
        // Kickstart rate autonegotiation only if the port supports
        // changing rates and the target rate differs from the default.
        let mut rate_change_state = RateChange::DoNothing;
        if let Some(rates) = port.rate_info() {
            if rates.target_bps != rates.default_bps {
                rate_change_state = RateChange::WaitingForSession;
            }
        }
        self.device = Some(ProxyDevice {
            tio_port: port,
            rx_channel: port_rx,
            rate_change_state,
            last_rx: Instant::now(),
            seen_since_connect: false,
            last_session: None,
            restarted: false,
            rpc_meta: HashMap::new(),
            pending_broadcasts: Vec::new(),
            pending_lookup: None,
        });
        Ok(())
    }

    /// Clients get dropped as part of the main loop. This function adds a
    /// client to drop to a set to be processed later, and if its ID was not
    /// already in the set, send a status event.
    fn drop_client(&mut self, client_id: u64) {
        if self.clients_to_drop.insert(client_id) {
            self.status_queue.send(Event::ClientTerminated(client_id));
        }
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

    // RPC failures are returned so the caller can notify the requesting client.
    fn forward_to_device(&mut self, mut pkt: Packet, origin: RpcOrigin) -> Result<(), RpcError> {
        let mut rpc_mapped_id: Option<u16> = None;
        let mut timeout = Instant::now();
        let port_client = match &origin {
            RpcOrigin::Port(client_id) => Some(*client_id),
            RpcOrigin::Direct { .. } | RpcOrigin::Internal => None,
        };
        // Decide from the borrowed view before rewriting the packet's bytes.
        let request = match pkt.payload() {
            proto::Payload::RpcRequest(req) => Some((
                req.id.value(),
                RpcMethod::from_wire(req.method),
                !req.args.is_empty(),
            )),
            _ => None,
        };
        if let Some((client_rpc_id, method, has_arg)) = request {
            let wire_id = self.next_wire_rpc_id().ok_or(RpcError::NoBufs)?;
            let target = match origin {
                RpcOrigin::Port(client_id) => {
                    timeout += self
                        .clients
                        .get(&client_id)
                        .expect("Invalid client when forwarding RPC")
                        .rpc_timeout;
                    RpcTarget::Port {
                        client_id,
                        original_id: client_rpc_id,
                    }
                }
                RpcOrigin::Direct {
                    result,
                    timeout: call_timeout,
                } => {
                    timeout += call_timeout;
                    RpcTarget::Direct(result)
                }
                RpcOrigin::Internal => {
                    timeout += Duration::from_secs(1);
                    RpcTarget::Internal(client_rpc_id)
                }
            };
            self.rpc_map.insert(
                wire_id,
                RpcMapEntry {
                    target,
                    route: pkt.route(),
                    timeout,
                    method,
                    has_arg,
                },
            );
            if let Some(client_id) = port_client {
                self.status_queue
                    .send(Event::RpcRemap((client_id, client_rpc_id), wire_id));
            }
            pkt = pkt.with_rpc_id(wire_id);
            rpc_mapped_id = Some(wire_id);
        } else if port_client.is_none() {
            return Err(RpcError::Malformed);
        }
        if let Some(dev) = &self.device {
            if let Ok(()) = dev.tio_port.send(pkt) {
                if let Some(rpc_id) = rpc_mapped_id {
                    self.rpc_timeouts.entry(timeout).or_default().insert(rpc_id);
                }
                return Ok(());
            }
        }
        // If we got here, the packet was not sent. avoid erroring out since if
        // there is something wrong with the device we'll notice in the main
        // loop soon but remove the rpc from the map and send back an error to
        // the client.
        if let Some(rpc_id) = rpc_mapped_id {
            self.rpc_map
                .remove(&rpc_id)
                .expect("Unexpected missing timeout set");
            Err(RpcError::Undefined)
        } else {
            Ok(())
        }
    }

    fn broadcast_status(&self, status: proto::ProxyStatus) {
        let pkt = Packet::proxy_status(status);
        for client in self.clients.values() {
            client.try_send(&pkt);
        }
    }

    fn broadcast_rpc_update(
        &self,
        method: &RpcMethod,
        route: &DeviceRoute,
        exclude_client: Option<u64>,
    ) {
        let Ok(pkt) = method.update_packet(*route) else {
            log::warn!("Dropping RPC update for a method name that does not fit a packet");
            return;
        };
        for (client_id, client) in self.clients.iter() {
            if Some(*client_id) != exclude_client {
                let _ = client.tx.try_send(pkt.clone());
            }
        }
    }

    fn invalidate_after_write(
        &mut self,
        method: &RpcMethod,
        route: DeviceRoute,
        exclude_client: Option<u64>,
    ) {
        let RpcMethod::Name(name) = method else {
            return;
        };
        let should_broadcast = self
            .device
            .as_ref()
            .and_then(|dev| dev.rpc_meta.get(name))
            .map(|&meta| {
                let readable = (meta & 0x0100) != 0;
                let writable = (meta & 0x0200) != 0;
                readable && writable
            });

        match should_broadcast {
            Some(true) => self.broadcast_rpc_update(method, &route, exclude_client),
            Some(false) => {}
            None => {
                let should_send = if let Some(dev) = self.device.as_mut() {
                    dev.pending_broadcasts
                        .push((name.clone(), route, exclude_client));
                    if dev.pending_lookup.is_none() {
                        dev.pending_lookup = Some((name.clone(), route));
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if should_send {
                    let _ = self.send_internal_rpc(
                        "rpc.info",
                        name.as_bytes(),
                        RPC_INFO_LOOKUP_ID,
                        route,
                    );
                }
            }
        }
    }

    fn dispatch_device_packet(&mut self, pkt: Packet) {
        let route = pkt.route();
        let Some(wire_id) = (match pkt.payload() {
            proto::Payload::RpcReply(reply) => Some(reply.req_id.value()),
            proto::Payload::RpcError(error) => Some(error.req_id.value()),
            _ => None,
        }) else {
            let mut to_drop = Vec::new();
            for (client_id, client) in self.clients.iter() {
                if !client.try_send(&pkt) {
                    self.status_queue.send(Event::ClientSendFailed(*client_id));
                    to_drop.push(*client_id);
                }
            }
            for client_id in to_drop {
                self.drop_client(client_id);
            }
            return;
        };

        let Some(remap) = self.rpc_restore(wire_id, &route) else {
            self.status_queue.send(Event::RpcRestoreNotFound(wire_id));
            return;
        };
        let RpcMapEntry {
            target,
            method,
            has_arg,
            ..
        } = remap;

        if let RpcTarget::Internal(original_id) = &target {
            match pkt.payload() {
                proto::Payload::RpcReply(reply) => {
                    self.internal_rpc_reply(*original_id, reply.value)
                }
                proto::Payload::RpcError(error) => {
                    self.internal_rpc_error(*original_id, RpcError::from(error.code))
                }
                _ => unreachable!(),
            }
            return;
        }

        let exclude_client = match &target {
            RpcTarget::Port { client_id, .. } => Some(*client_id),
            RpcTarget::Direct(_) => None,
            RpcTarget::Internal(_) => unreachable!(),
        };
        if has_arg && pkt.ptype() == PacketType::RPC_REP {
            self.invalidate_after_write(&method, route, exclude_client);
        }

        match target {
            RpcTarget::Port {
                client_id,
                original_id,
            } => {
                let Some(client) = self.clients.get(&client_id) else {
                    self.status_queue.send(Event::RpcClientNotFound(client_id));
                    return;
                };
                self.status_queue
                    .send(Event::RpcRestore(wire_id, (client_id, original_id)));
                let restored = pkt.with_rpc_id(original_id);
                if !client.try_send(&restored) {
                    self.status_queue.send(Event::ClientSendFailed(client_id));
                    self.drop_client(client_id);
                }
            }
            RpcTarget::Direct(result) => {
                let reply = match pkt.payload() {
                    proto::Payload::RpcReply(reply) => Ok(reply.value.to_vec()),
                    proto::Payload::RpcError(error) => Err(RawCallError::Device {
                        error: RpcError::from(error.code),
                        message: error.message.to_vec(),
                    }),
                    _ => unreachable!(),
                };
                let _ = result.send(reply);
            }
            RpcTarget::Internal(_) => unreachable!(),
        }
    }

    /// Synthesize an RPC error packet with the given code and send it back to
    /// all clients that have an RPC with timeout < `until` (all RPCs if None).
    /// Used to generate RPC timeouts, or to notify a client that it will never
    /// get a reply when the device disconnects or restarts.
    fn dispatch_rpc_errors(&mut self, failure: RpcFailure, until: Option<Instant>) {
        let mut to_remove = Vec::new();
        let mut failed = Vec::new();
        let mut to_drop = Vec::new();
        for (timeout, rpc_ids) in self.rpc_timeouts.iter() {
            if let Some(timeout_bound) = until {
                if *timeout >= timeout_bound {
                    break;
                }
            }
            to_remove.push(*timeout);
            for rpc_id in rpc_ids {
                self.status_queue
                    .send(if matches!(failure, RpcFailure::Timeout) {
                        Event::RpcTimeout(*rpc_id)
                    } else {
                        Event::RpcCancel(*rpc_id)
                    });
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
                RpcTarget::Port {
                    client_id,
                    original_id,
                } => {
                    let Some(client) = self.clients.get(&client_id) else {
                        continue;
                    };
                    if !client.try_send(&Packet::rpc_error(
                        original_id,
                        failure.wire_error(),
                        remap.route,
                    )) {
                        to_drop.push(client_id);
                        log::debug!("Failed to send generated RPC error to client {client_id}");
                    }
                }
                RpcTarget::Direct(result) => {
                    let _ = result.send(Err(failure.direct_error()));
                }
                RpcTarget::Internal(original_id) => {
                    self.internal_rpc_error(original_id, failure.wire_error());
                }
            }
        }
        for client_id in to_drop {
            self.drop_client(client_id);
        }
    }

    fn process_rpc_timeouts(&mut self) -> Duration {
        let now = Instant::now();
        self.dispatch_rpc_errors(RpcFailure::Timeout, Some(now));
        if let Some(timeout) = self.rpc_timeouts.keys().next() {
            timeout.saturating_duration_since(now) + Duration::from_millis(1)
        } else {
            Duration::from_secs(60)
        }
    }

    fn send_internal_rpc(
        &mut self,
        name: &str,
        arg: &[u8],
        id: u16,
        route: DeviceRoute,
    ) -> Result<(), RpcError> {
        let pkt = Packet::rpc_request(name, arg, id, route).map_err(|_| RpcError::ArgsSize)?;
        self.forward_to_device(pkt, RpcOrigin::Internal)
    }

    /// Process a reply to an RPC issued by the ProxyCore.
    fn internal_rpc_reply(&mut self, id: u16, value: &[u8]) {
        fn get_rate_vars(proxy: &ProxyCore) -> Option<(RateChange, u32)> {
            if let Some(dev) = proxy.device.as_ref() {
                dev.tio_port
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
                    if value == 0 {
                        self.status_queue.send(Event::AutoRateIncompatible(0));
                        self.status_queue.send(Event::AutoRateGaveUp);
                        RateChange::GaveUp
                    } else {
                        let error = (((target as f64) - (value as f64)) / (value as f64)).abs();
                        if error > 0.015 {
                            self.status_queue.send(Event::AutoRateIncompatible(value));
                            self.status_queue.send(Event::AutoRateGaveUp);
                            RateChange::GaveUp
                        } else {
                            self.status_queue.send(Event::AutoRateCompatible(value));
                            RateChange::SetDeviceRate
                        }
                    }
                } else {
                    self.status_queue.send(Event::AutoRateRpcInvalid);
                    RateChange::GaveUp
                };
                self.device.as_mut().expect("").rate_change_state = next_state;
                return;
            }
        } else if id == SET_RATE_RPC_ID {
            if let Some((RateChange::WaitingNewRate, target)) = get_rate_vars(self) {
                self.status_queue.send(Event::SetRate(target));
                let next_state = match self.device.as_ref().expect("").tio_port.set_rate(target) {
                    Ok(_) => RateChange::RateChanged,
                    Err(_) => {
                        self.status_queue.send(Event::AutoRateGaveUp);
                        RateChange::GaveUp
                    }
                };
                self.device.as_mut().expect("").rate_change_state = next_state;
                return;
            }
        } else if id == RPC_INFO_LOOKUP_ID {
            let broadcast_info = self.device.as_mut().and_then(|dev| {
                let (name, _) = dev.pending_lookup.take()?;
                if value.len() < 2 {
                    return None;
                }

                let meta = u16::from_le_bytes([value[0], value[1]]);
                dev.rpc_meta.insert(name.clone(), meta);

                let readable = (meta & 0x0100) != 0;
                let writable = (meta & 0x0200) != 0;

                let pending = std::mem::take(&mut dev.pending_broadcasts);
                let (matching, remaining): (Vec<_>, Vec<_>) =
                    pending.into_iter().partition(|(n, _, _)| *n == name);
                dev.pending_broadcasts = remaining;

                let next = dev
                    .pending_broadcasts
                    .first()
                    .map(|(n, r, _)| (n.clone(), *r));

                if let Some((next_name, next_route)) = &next {
                    dev.pending_lookup = Some((next_name.clone(), *next_route));
                }

                Some((matching, readable && writable, next))
            });

            if let Some((matching, should_broadcast, next_lookup)) = broadcast_info {
                if should_broadcast {
                    for (name, route, exclude_client) in matching {
                        self.broadcast_rpc_update(&RpcMethod::Name(name), &route, exclude_client);
                    }
                }

                if let Some((next_name, next_route)) = next_lookup {
                    let _ = self.send_internal_rpc(
                        "rpc.info",
                        next_name.as_bytes(),
                        RPC_INFO_LOOKUP_ID,
                        next_route,
                    );
                }
            }
            return;
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

    fn internal_rpc_error(&mut self, id: u16, error: RpcError) {
        if id == RPC_INFO_LOOKUP_ID {
            if let Some(dev) = self.device.as_mut() {
                // Clear current lookup
                let failed_name = dev.pending_lookup.take().map(|(n, _)| n);

                // Remove any pending broadcasts for the failed lookup
                if let Some(name) = failed_name {
                    dev.pending_broadcasts.retain(|(n, _, _)| *n != name);
                }

                // Start next lookup if there are more pending
                if let Some((next_name, next_route, _)) = dev.pending_broadcasts.first().cloned() {
                    dev.pending_lookup = Some((next_name.clone(), next_route));
                    let _ = self.send_internal_rpc(
                        "rpc.info",
                        next_name.as_bytes(),
                        RPC_INFO_LOOKUP_ID,
                        next_route,
                    );
                }
            }
            return;
        }

        // We could handle this better, but just keep the device to the default speed until the port is reset
        self.status_queue.send(Event::AutoRateRpcError(error));
        if let Some(dev) = self.device.as_mut() {
            dev.rate_change_state = RateChange::GaveUp;
            self.status_queue.send(Event::AutoRateGaveUp);
        }
    }

    fn autonegotiation(&mut self) {
        // When this is called, device will be Some, and it does not change
        // from any of the called methods
        fn device(proxy: &mut ProxyCore) -> &mut ProxyDevice {
            proxy
                .device
                .as_mut()
                .expect("No device but in autonegotiation")
        }
        let next_state = match device(self).rate_change_state.clone() {
            RateChange::QueryDeviceRate => {
                let target = device(self).rates().target_bps;
                if let Err(rpc_error) = self.send_internal_rpc(
                    "dev.port.rate.near",
                    &target.to_le_bytes(),
                    QUERY_RATE_RPC_ID,
                    DeviceRoute::root(),
                ) {
                    self.status_queue.send(Event::AutoRateRpcError(rpc_error));
                    RateChange::GaveUp
                } else {
                    self.status_queue.send(Event::AutoRateQueried(target));
                    RateChange::WaitingDeviceRate
                }
            }
            RateChange::SetDeviceRate => {
                if self.rpc_map.is_empty() {
                    let target = device(self).rates().target_bps;
                    if let Err(rpc_error) = self.send_internal_rpc(
                        "dev.port.rate",
                        &target.to_le_bytes(),
                        SET_RATE_RPC_ID,
                        DeviceRoute::root(),
                    ) {
                        self.status_queue.send(Event::AutoRateRpcError(rpc_error));
                        RateChange::GaveUp
                    } else {
                        self.status_queue.send(Event::AutoRateSet(target));
                        RateChange::WaitingNewRate
                    }
                } else {
                    self.status_queue.send(Event::AutoRateWait);
                    RateChange::SetDeviceRate
                }
            }
            RateChange::RateChanged => {
                let last_rx_delta = device(self).last_rx.elapsed();
                if last_rx_delta > Duration::from_millis(1000) {
                    self.status_queue.send(Event::NoData);
                    let dev = device(self);
                    let default_bps = dev.rates().default_bps;
                    dev.tio_port
                        .set_rate(default_bps)
                        .expect("Failed to set default port rate");
                    self.status_queue.send(Event::SetRate(default_bps));
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

    pub(crate) fn run(&mut self) {
        use channel::TryRecvError;

        if let Err(error) = self.try_setup_device() {
            log::debug!("failed to open {}: {error}", self.url);
            self.status_queue.send(Event::FailedToConnect);
            self.broadcast_status(proto::ProxyStatus::FailedToConnect);
            return;
        }
        let mut device_timeout = Instant::now();

        'mainloop: loop {
            let mut timeout = self.process_rpc_timeouts();

            if self.device.is_none() {
                self.cancel_active_rpcs();
                if let Err(error) = self.try_setup_device() {
                    log::debug!("failed to reopen {}: {error}", self.url);
                    if Instant::now() > device_timeout {
                        self.status_queue.send(Event::FailedToReconnect);
                        self.broadcast_status(proto::ProxyStatus::FailedToReconnect);
                        break;
                    }
                    timeout = std::cmp::min(timeout, RECONNECT_POLL_INTERVAL);
                }
            }

            let liveness_expired = self
                .device
                .as_ref()
                .map(|dev| dev.seen_since_connect && dev.last_rx.elapsed() > dev.liveness_timeout())
                .unwrap_or(false);
            if liveness_expired {
                self.device = None;
                device_timeout =
                    Instant::now() + self.reconnect_timeout.unwrap_or(Duration::from_secs(0));
                self.status_queue.send(Event::SensorDisconnected);
                self.broadcast_status(proto::ProxyStatus::SensorDisconnected);
                continue;
            }

            let (safe_to_forward, needs_autonegotiation, restarted) =
                if let Some(dev) = &mut self.device {
                    // Wake in time to run the watchdog even if the device goes silent.
                    if dev.seen_since_connect {
                        let until_stale = (dev.last_rx + dev.liveness_timeout())
                            .saturating_duration_since(Instant::now());
                        timeout = std::cmp::min(timeout, until_stale + LIVENESS_WAKE_SLACK);
                    }
                    (
                        dev.safe_to_forward(),
                        if dev.needs_autonegotiation() {
                            timeout = std::cmp::min(timeout, Duration::from_millis(200));
                            true
                        } else {
                            false
                        },
                        if dev.restarted {
                            dev.restarted = false;
                            true
                        } else {
                            false
                        },
                    )
                } else {
                    // If no device, forwarding will send RPC errors, which we want.
                    (true, false, false)
                };

            if needs_autonegotiation {
                self.autonegotiation();
            }
            if restarted {
                self.cancel_active_rpcs();
            }
            // Drop dead clients right before populating the Select object.
            for client_id in self.clients_to_drop.drain() {
                drop(self.clients.remove(&client_id));
            }
            let mut sel = channel::Select::new();
            let mut ids: Vec<u64> = Vec::new();
            if safe_to_forward {
                // Ignore data from clients if in the process of autonegotiation,
                // as the packet might get lost. Once the process finishes, we
                // their queue will be processed.
                for (id, client) in self.clients.iter() {
                    sel.recv(&client.rx);
                    ids.push(*id);
                }
            }

            let command_index = if safe_to_forward {
                Some(sel.recv(&self.command_queue))
            } else {
                None
            };
            let device_index = self
                .device
                .as_ref()
                .map(|device| sel.recv(&device.rx_channel));

            let index = match sel.ready_timeout(timeout) {
                Ok(index) => index,
                Err(channel::ReadyTimeoutError) => continue,
            };

            if index < ids.len() {
                // data from a client to send to the port
                let client_id = ids[index];
                let mut packets = vec![];
                {
                    let client = self
                        .clients
                        .get(&client_id)
                        .expect("invalid client from Select");
                    loop {
                        // Looking up the client for every packet is not very efficient,
                        // but the packet rate client->device is very low that in
                        // practice this will rarely loop more than once
                        match client.recv() {
                            Ok(pkt) => {
                                packets.push(pkt);
                            }
                            Err(TryRecvError::Empty) => {
                                break;
                            }
                            Err(TryRecvError::Disconnected) => {
                                // On disconnect, just break out of the receive loop,
                                // but still forward any received packets: it could be
                                // an RPC which the client doesn't care about but
                                // we should still forward it to the device if possible.
                                self.drop_client(client_id);
                                break;
                            }
                        }
                    }
                }

                // Forward all packets from clients to the device. If there are
                // RPC requests which cannot be sent, a synthetic RPC error
                // will be returned to send back.
                let mut rpc_errors = vec![];
                for pkt in packets {
                    let reply_target = match pkt.payload() {
                        proto::Payload::RpcRequest(req) => Some((pkt.route(), req.id.value())),
                        _ => None,
                    };
                    if let Err(error) = self.forward_to_device(pkt, RpcOrigin::Port(client_id)) {
                        let (route, id) =
                            reply_target.expect("only RPC requests produce forwarding errors");
                        rpc_errors.push(Packet::rpc_error(id, error, route));
                    }
                }

                // Send back eventual RPC errors to the client
                if !rpc_errors.is_empty() {
                    // Looking up again is not ideal, but this is a vanishingly
                    // rare condition, so just do it to make the borrow checker
                    // happy without usafe code or additional indirection.
                    let client = self
                        .clients
                        .get(&client_id)
                        .expect("invalid client from Select");
                    let mut failed = false;
                    for pkt in rpc_errors {
                        if !client.try_send(&pkt) {
                            failed = true;
                            break;
                        }
                    }
                    if failed {
                        self.status_queue.send(Event::ClientSendFailed(client_id));
                        self.drop_client(client_id);
                    }
                }
            } else if Some(index) == command_index {
                // New ports and direct RPC calls share one bounded command lane.
                loop {
                    match self.command_queue.try_recv() {
                        Ok(ProxyCommand::OpenPort { client }) => {
                            let client_id = self.next_client_id;
                            self.next_client_id += 1;
                            self.clients.insert(client_id, client);
                            self.status_queue.send(Event::NewClient(client_id));
                        }
                        Ok(ProxyCommand::Call {
                            request,
                            timeout,
                            result,
                        }) => {
                            let completion = result.clone();
                            if let Err(error) = self
                                .forward_to_device(request, RpcOrigin::Direct { result, timeout })
                            {
                                let error = if matches!(error, RpcError::Undefined) {
                                    RawCallError::DeviceDisconnected
                                } else {
                                    RawCallError::Device {
                                        error,
                                        message: Vec::new(),
                                    }
                                };
                                let _ = completion.send(Err(error));
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            self.status_queue.send(Event::Exiting);
                            break 'mainloop;
                        }
                    }
                }
            } else if Some(index) == device_index {
                // data from the device
                while let Some(device) = self.device.as_mut() {
                    match device.try_recv(&self.status_queue) {
                        Ok(Ok(pkt)) => {
                            // First packet since (re)connect: the device is live, announce it.
                            let first_packet =
                                !std::mem::replace(&mut device.seen_since_connect, true);
                            if first_packet {
                                if self.ever_connected {
                                    self.status_queue.send(Event::SensorReconnected);
                                    self.broadcast_status(proto::ProxyStatus::SensorReconnected);
                                } else {
                                    // Initial connect needs no wire status broadcast.
                                    self.ever_connected = true;
                                    self.status_queue.send(Event::SensorConnected);
                                }
                            }
                            self.dispatch_device_packet(pkt);
                        }
                        // Got a RecvError
                        Ok(Err(err)) => {
                            match err {
                                RecvError::Text(text) => {
                                    self.status_queue.send(Event::Text(text));
                                }
                                RecvError::Protocol(perror) => {
                                    self.status_queue.send(Event::ProtocolError(perror));
                                }
                                // All other errors are treated as fatal.
                                err => {
                                    self.status_queue.send(Event::FatalError(err));
                                    break 'mainloop;
                                }
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            self.device = None;
                            device_timeout = Instant::now()
                                + match self.reconnect_timeout {
                                    Some(t) => t,
                                    None => Duration::from_secs(0),
                                };
                            self.status_queue.send(Event::SensorDisconnected);
                            self.broadcast_status(proto::ProxyStatus::SensorDisconnected);
                            break;
                        }
                    }
                }
            } else {
                unreachable!("ready channel was not registered with the proxy select loop");
            }
        }

        self.dispatch_rpc_errors(RpcFailure::ProxyClosed, None);
        while let Ok(command) = self.command_queue.try_recv() {
            if let ProxyCommand::Call { result, .. } = command {
                let _ = result.send(Err(RawCallError::ProxyClosed));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_core() -> ProxyCore {
        let (_new_client_tx, new_client_rx) = channel::bounded(1);
        let (status_tx, _status_rx) = channel::bounded(16);
        ProxyCore::new(String::new(), None, new_client_rx, status_tx, false)
    }

    fn insert_rpc(
        core: &mut ProxyCore,
        wire_id: u16,
        client_id: u64,
        client_request_id: u16,
        route: DeviceRoute,
        timeout: Instant,
    ) {
        core.rpc_map.insert(
            wire_id,
            RpcMapEntry {
                target: RpcTarget::Port {
                    client_id,
                    original_id: client_request_id,
                },
                route,
                timeout,
                has_arg: false,
                method: RpcMethod::Name("dev.name".into()),
            },
        );
        core.rpc_timeouts
            .entry(timeout)
            .or_default()
            .insert(wire_id);
    }

    fn insert_direct_rpc(
        core: &mut ProxyCore,
        wire_id: u16,
        route: DeviceRoute,
        timeout: Instant,
    ) -> channel::Receiver<RawCallResult> {
        let (result, receiver) = channel::bounded(1);
        core.rpc_map.insert(
            wire_id,
            RpcMapEntry {
                target: RpcTarget::Direct(result),
                route,
                timeout,
                has_arg: false,
                method: RpcMethod::Name("dev.name".into()),
            },
        );
        core.rpc_timeouts
            .entry(timeout)
            .or_default()
            .insert(wire_id);
        receiver
    }

    #[test]
    fn direct_rpcs_complete_independently_and_out_of_order() {
        let mut core = test_core();
        let first_route: DeviceRoute = "/1".parse().unwrap();
        let second_route: DeviceRoute = "/2".parse().unwrap();
        let timeout = Instant::now() + Duration::from_secs(3);
        let first = insert_direct_rpc(&mut core, 20, first_route, timeout);
        let second = insert_direct_rpc(&mut core, 21, second_route, timeout);

        core.dispatch_device_packet(Packet::rpc_reply(21, b"second", second_route).unwrap());
        core.dispatch_device_packet(Packet::rpc_reply(20, b"first", first_route).unwrap());

        assert_eq!(first.recv().unwrap().unwrap(), b"first");
        assert_eq!(second.recv().unwrap().unwrap(), b"second");
        assert!(core.rpc_map.is_empty());
        assert!(core.rpc_timeouts.is_empty());
    }

    #[test]
    fn a_wire_timeout_is_distinct_from_a_local_direct_call_timeout() {
        let mut core = test_core();
        let route = DeviceRoute::root();
        let deadline = Instant::now() + Duration::from_secs(3);
        let wire_error = insert_direct_rpc(&mut core, 20, route, deadline);
        core.dispatch_device_packet(Packet::rpc_error(20, RpcError::Timeout, route));
        assert!(matches!(
            wire_error.recv().unwrap(),
            Err(RawCallError::Device {
                error: RpcError::Timeout,
                ..
            })
        ));

        let expired = Instant::now() - Duration::from_millis(1);
        let local_timeout = insert_direct_rpc(&mut core, 21, route, expired);
        core.process_rpc_timeouts();
        assert!(matches!(
            local_timeout.recv().unwrap(),
            Err(RawCallError::Timeout)
        ));
    }

    #[test]
    fn disconnect_resolves_every_direct_call_without_replaying_it() {
        let mut core = test_core();
        let deadline = Instant::now() + Duration::from_secs(3);
        let first = insert_direct_rpc(&mut core, 20, DeviceRoute::root(), deadline);
        let second = insert_direct_rpc(&mut core, 21, DeviceRoute::root(), deadline);

        core.cancel_active_rpcs();

        assert!(matches!(
            first.recv().unwrap(),
            Err(RawCallError::DeviceDisconnected)
        ));
        assert!(matches!(
            second.recv().unwrap(),
            Err(RawCallError::DeviceDisconnected)
        ));
        assert!(core.rpc_map.is_empty());
        assert!(core.rpc_timeouts.is_empty());
    }

    #[test]
    fn a_full_status_observer_cannot_kill_rpc_progress() {
        let (status, events) = channel::bounded(1);
        let queue = StatusQueue {
            dest: status,
            only_new_client: false,
        };
        queue.send(Event::SensorConnected);
        queue.send(Event::SensorDisconnected);
        assert!(matches!(events.recv().unwrap(), Event::SensorConnected));
    }

    #[test]
    fn rpc_replies_restore_independently_and_out_of_order() {
        let mut core = test_core();
        let first_route: DeviceRoute = "/1".parse().unwrap();
        let second_route: DeviceRoute = "/2".parse().unwrap();
        let timeout = Instant::now() + Duration::from_secs(3);
        insert_rpc(&mut core, 20, 3, 100, first_route, timeout);
        insert_rpc(&mut core, 21, 4, 101, second_route, timeout);

        let remap = core.rpc_restore(21, &second_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                client_id: 4,
                original_id: 101
            }
        ));
        assert!(!remap.has_arg);
        assert!(matches!(remap.method, RpcMethod::Name(name) if name == "dev.name"));
        let remap = core.rpc_restore(20, &first_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                client_id: 3,
                original_id: 100
            }
        ));
        assert!(!remap.has_arg);
        assert!(matches!(remap.method, RpcMethod::Name(name) if name == "dev.name"));
        assert!(core.rpc_map.is_empty());
        assert!(core.rpc_timeouts.is_empty());
    }

    #[test]
    fn rpc_reply_from_the_wrong_route_does_not_consume_the_request() {
        let mut core = test_core();
        let requested_route: DeviceRoute = "/1".parse().unwrap();
        let wrong_route: DeviceRoute = "/2".parse().unwrap();
        let timeout = Instant::now() + Duration::from_secs(3);
        insert_rpc(&mut core, 20, 3, 100, requested_route, timeout);

        assert!(core.rpc_restore(20, &wrong_route).is_none());
        assert!(core.rpc_map.contains_key(&20));
        assert!(core.rpc_timeouts[&timeout].contains(&20));
        let remap = core.rpc_restore(20, &requested_route).unwrap();
        assert!(matches!(
            remap.target,
            RpcTarget::Port {
                client_id: 3,
                original_id: 100
            }
        ));
        assert!(!remap.has_arg);
        assert!(matches!(remap.method, RpcMethod::Name(name) if name == "dev.name"));
    }

    #[test]
    fn rpc_timeout_is_delivered_with_the_client_request_id() {
        let mut core = test_core();

        let (to_client_tx, to_client_rx) = channel::bounded(1);
        let (_from_client_tx, from_client_rx) = channel::bounded(1);
        let client_id = 1;
        core.clients.insert(
            client_id,
            ProxyClient::new(
                to_client_tx,
                from_client_rx,
                Duration::from_secs(3),
                DeviceRoute::root(),
                usize::MAX,
                true,
                true,
            ),
        );

        let wire_id = 12;
        let client_request_id = 7855;
        let timeout = Instant::now() - Duration::from_millis(1);
        insert_rpc(
            &mut core,
            wire_id,
            client_id,
            client_request_id,
            DeviceRoute::root(),
            timeout,
        );

        core.process_rpc_timeouts();

        let packet = to_client_rx.try_recv().unwrap();
        let proto::Payload::RpcError(error) = packet.payload() else {
            panic!("expected an RPC timeout packet");
        };
        assert_eq!(error.req_id.value(), client_request_id);
        assert!(matches!(RpcError::from(error.code), RpcError::Timeout));
        assert!(core.rpc_map.is_empty());
        assert!(core.rpc_timeouts.is_empty());
    }
}

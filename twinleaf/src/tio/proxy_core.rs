use super::port;
use super::port::Port as HardwarePort;
use super::port::RecvError;
use super::proto::{self, DeviceRoute, Packet};
use super::proxy::Event;
use super::util;
use super::util::TioRpcReplyable;

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
            self.dest
                .try_send(event)
                .expect("Failed to send event to proxy status queue");
        }
    }
}

/// Internal proxy state per client
pub struct ProxyClient {
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

    fn send(&self, pkt: &Packet) -> Result<(), channel::TrySendError<Packet>> {
        let scoped_route = if let Ok(r) = self.scope.relative_route(&pkt.routing) {
            if r.len() <= self.depth {
                r
            } else {
                return Ok(());
            }
        } else {
            return Ok(());
        };
        if !match pkt.payload {
            proto::Payload::RpcRequest(_)
            | proto::Payload::RpcReply(_)
            | proto::Payload::RpcError(_) => true,
            proto::Payload::LegacyStreamData(_) | proto::Payload::StreamData(_) => {
                self.forward_data
            }
            _ => self.forward_nonrpc,
        } {
            return Ok(());
        }
        self.tx.try_send(Packet {
            payload: pkt.payload.clone(),
            routing: scoped_route,
            ttl: pkt.ttl,
        })
    }

    fn recv(&self) -> Result<Packet, channel::TryRecvError> {
        let mut pkt = self.rx.try_recv()?;
        pkt.routing = self.scope.absolute_route(&pkt.routing);
        Ok(pkt)
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
    rx_channel: channel::Receiver<Result<Packet, RecvError>>,
    rate_change_state: RateChange,
    last_rx: Instant,
    last_session: Option<u32>,
    restarted: bool,
}

impl ProxyDevice {
    /// True if this device does not have a settable data rate.
    fn has_static_rate(&self) -> bool {
        match self.rate_change_state {
            RateChange::DoNothing => true,
            _ => false,
        }
    }

    /// True if this device needs to run the periodic rate negotiation task.
    /// Note that this is true even after the higher rate has been negotiated,
    /// to deal with reverting back to the default rate after some time goes
    /// by without seeing data.
    fn needs_autonegotiation(&self) -> bool {
        match self.rate_change_state {
            RateChange::DoNothing | RateChange::GaveUp => false,
            _ => true,
        }
    }

    /// True if it's safe to forward packets to the device due to rate
    /// negotiation concerns. Specifically, packets might be lost around
    /// when the rate transitions, so we hold back on forwarding traffic then.
    fn safe_to_forward(&self) -> bool {
        match self.rate_change_state {
            RateChange::SetDeviceRate | RateChange::WaitingNewRate => false,
            _ => true,
        }
    }

    /// Convenience method to get the rate information for this device,
    /// when already known it has settable data rate.
    fn rates(&self) -> port::RateInfo {
        return self
            .tio_port
            .rate_info()
            .expect("Rates requested for unsupported device");
    }

    fn try_recv(
        &mut self,
        status_queue: &StatusQueue,
    ) -> Result<Result<Packet, RecvError>, crossbeam::channel::TryRecvError> {
        if self.has_static_rate() {
            self.rx_channel.try_recv()
        } else {
            match self.rx_channel.try_recv() {
                Ok(res) => {
                    self.last_rx = match &res {
                        Ok(pkt) => {
                            if let proto::Payload::Heartbeat(proto::HeartbeatPayload::Session(
                                session,
                            )) = pkt.payload
                            {
                                if pkt.routing.len() == 0 {
                                    // This is a heartbeat for the root sensor
                                    let old_session = self.last_session.replace(session);
                                    if let RateChange::WaitingForSession = self.rate_change_state {
                                        self.rate_change_state = RateChange::QueryDeviceRate;
                                    } else if (self.last_session != old_session)
                                        && old_session.is_some()
                                    {
                                        status_queue.send(Event::RootDeviceRestarted);
                                        // It has restarted, restart autonegotiation if needed.
                                        self.rate_change_state = match self.rate_change_state {
                                            RateChange::DoNothing => RateChange::DoNothing,
                                            RateChange::WaitingForSession => {
                                                RateChange::WaitingForSession
                                            } // never happens
                                            _ => RateChange::QueryDeviceRate,
                                        };
                                        self.restarted = true;
                                    }
                                }
                            }
                            Instant::now()
                        }
                        // Text means we are still getting data. Other protocol errors could mean we are getting
                        // garbled bytes from running at the wrong rate
                        Err(RecvError::Protocol(proto::Error::Text(_))) => Instant::now(),
                        _ => self.last_rx,
                    };
                    Ok(res)
                }
                err => err,
            }
        }
    }
}

struct RpcMapEntry {
    id: u16,
    client: u64,
    route: DeviceRoute,
    timeout: Instant,
    method: proto::RpcMethod,
}

pub struct ProxyCore {
    url: String,
    reconnect_timeout: Option<Duration>,
    new_client_queue: channel::Receiver<ProxyClient>,
    status_queue: StatusQueue,

    device: Option<ProxyDevice>,

    /// Id to assign to the next client, 64 bits.
    /// It is realistic to assume that it will never wrap around.
    next_client_id: u64,
    clients: HashMap<u64, ProxyClient>,
    clients_to_drop: HashSet<u64>,

    next_rpc_id: u16,
    rpc_map: HashMap<u16, RpcMapEntry>,
    rpc_timeouts: BTreeMap<Instant, HashSet<u16>>,
}

static QUERY_RATE_RPC_ID: u16 = 0x101;
static SET_RATE_RPC_ID: u16 = 0x102;

impl ProxyCore {
    pub fn new(
        url: String,
        reconnect_timeout: Option<Duration>,
        new_client_queue: channel::Receiver<ProxyClient>,
        status_queue: channel::Sender<Event>,
        notify_new_client_only: bool,
    ) -> ProxyCore {
        ProxyCore {
            url: url,
            reconnect_timeout: reconnect_timeout,
            new_client_queue: new_client_queue,
            status_queue: StatusQueue {
                dest: status_queue,
                only_new_client: notify_new_client_only,
            },
            device: None,
            // Start from client 1, as 0 is reserved for internal RPCs.
            next_client_id: 1,
            clients: HashMap::new(),
            clients_to_drop: HashSet::new(),
            next_rpc_id: 0,
            rpc_map: HashMap::new(),
            rpc_timeouts: BTreeMap::new(),
        }
    }

    fn try_setup_device(&mut self) -> bool {
        if self.device.is_some() {
            return true;
        }
        let (port_rx_send, port_rx) = HardwarePort::rx_channel();
        let port = match HardwarePort::new(&self.url, HardwarePort::rx_to_channel(port_rx_send)) {
            Ok(p) => p,
            Err(_) => {
                return false;
            }
        };
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
            rate_change_state: rate_change_state,
            last_rx: Instant::now(),
            last_session: None,
            restarted: false,
        });
        true
    }

    /// Clients get dropped as part of the main loop. This function adds a
    /// client to drop to a set to be processed later, and if its ID was not
    /// already in the set, send a status event.
    fn drop_client(&mut self, client_id: u64) {
        if self.clients_to_drop.insert(client_id) {
            self.status_queue.send(Event::ClientTerminated(client_id));
        }
    }

    fn rpc_restore(
        &mut self,
        wire_id: u16,
        route: &DeviceRoute,
    ) -> Option<(u64, u16, proto::RpcMethod)> {
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
            if ids.len() == 0 {
                self.rpc_timeouts.remove(&remap.timeout);
            }
        } else {
            #[cfg(debug_assertions)]
            eprintln!("Failed to find RPC timeout in map");
        }
        Some((remap.client, remap.id, remap.method))
    }

    // Ok: successful. Err: packet should be sent back to client
    fn forward_to_device(&mut self, mut pkt: Packet, client_id: u64) -> Result<(), Packet> {
        let mut rpc_mapped_id: Option<u16> = None;
        let mut timeout = Instant::now();
        if let proto::Payload::RpcRequest(req) = &mut pkt.payload {
            let wire_id = self.next_rpc_id;
            // Always increment even if it fails, on the slim chance it hits an open spot
            // next time.
            self.next_rpc_id = self.next_rpc_id.wrapping_add(1);
            if self.rpc_map.contains_key(&wire_id) {
                return Err(util::PacketBuilder::new(pkt.routing)
                    .rpc_error(req.id, proto::RpcErrorCode::OutOfMemory));
            }
            timeout += if client_id != 0 {
                self.clients
                    .get(&client_id)
                    .expect("Invalid client when forwarding RPC")
                    .rpc_timeout
            } else {
                // Timeout internal RPCs after 1 second
                Duration::from_secs(1)
            };
            self.rpc_map.insert(
                wire_id,
                RpcMapEntry {
                    id: req.id,
                    client: client_id,
                    route: pkt.routing.clone(),
                    timeout: timeout,
                    method: req.method.clone(),
                },
            );
            self.status_queue
                .send(Event::RpcRemap((client_id, req.id), wire_id));
            req.id = wire_id;
            rpc_mapped_id = Some(wire_id);
        }
        if let Some(dev) = &self.device {
            if let Ok(()) = dev.tio_port.send(pkt) {
                if let Some(rpc_id) = rpc_mapped_id {
                    if !self.rpc_timeouts.contains_key(&timeout) {
                        self.rpc_timeouts.insert(timeout, HashSet::new());
                    }
                    let timeout_ids = self
                        .rpc_timeouts
                        .get_mut(&timeout)
                        .expect("Unexpected missing timeout set");
                    timeout_ids.insert(rpc_id);
                }
                return Ok(());
            }
        }
        // If we got here, the packet was not sent. avoid erroring out since if
        // there is something wrong with the device we'll notice in the main
        // loop soon but remove the rpc from the map and send back an error to
        // the client.
        if let Some(rpc_id) = rpc_mapped_id {
            let remap = self
                .rpc_map
                .remove(&rpc_id)
                .expect("Unexpected missing timeout set");
            return Err(util::PacketBuilder::new(remap.route)
                .rpc_error(remap.id, proto::RpcErrorCode::Undefined));
        } else {
            Ok(())
        }
    }

    fn broadcast_status(&self, status: proto::ProxyStatus) {
        let pkt = Packet {
            payload: proto::Payload::ProxyEvent(proto::ProxyEventPayload::Status(status)),
            routing: DeviceRoute::root(),
            ttl: 0,
        };
        for (_client_id, client) in self.clients.iter() {
            if let Err(_) = client.send(&pkt) {
                // Don't drop clients here. Let the normal error handling do it
            }
        }
    }

    fn broadcast_rpc_update(
        &self,
        method: &proto::RpcMethod,
        route: &DeviceRoute,
        exclude_client: u64,
    ) {
        let pkt = Packet {
            payload: proto::Payload::ProxyEvent(proto::ProxyEventPayload::RpcUpdate(
                method.clone(),
            )),
            routing: route.clone(),
            ttl: 0,
        };
        for (client_id, client) in self.clients.iter() {
            if *client_id != exclude_client {
                let _ = client.tx.try_send(pkt.clone());
            }
        }
    }

    /// Synthesize an RPC error packet with the given code and send it back to
    /// all clients that have an RPC with timeout < `until` (all RPCs if None).
    /// Used to generate RPC timeouts, or to notify a client that it will never
    /// get a reply when the device disconnects or restarts.
    fn dispatch_rpc_errors(&mut self, error: proto::RpcErrorCode, until: Option<Instant>) {
        let mut to_remove = Vec::new();
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
                    .send(if let proto::RpcErrorCode::Timeout = error {
                        Event::RpcTimeout(*rpc_id)
                    } else {
                        Event::RpcCancel(*rpc_id)
                    });
                let remap = self
                    .rpc_map
                    .remove(&rpc_id)
                    .expect("RPC ID from timeout missing in main map");
                let client = if let Some(c) = self.clients.get(&remap.client) {
                    c
                } else {
                    // Client is gone.
                    continue;
                };
                if let Err(_) = client.send(&util::PacketBuilder::make_rpc_error(
                    remap.id,
                    error.clone(),
                    remap.route,
                )) {
                    to_drop.push(remap.client);
                    // This can happen without a problem per se, if e.g. a client
                    // issues an RPC which will time out, and disconnects before
                    // said timeout occurs, so only say something in debug mode.
                    #[cfg(debug_assertions)]
                    eprintln!(
                        "Failed to send generated RPC error to client {:?}",
                        remap.client
                    );
                }
            }
        }
        for timeout in to_remove {
            self.rpc_timeouts.remove(&timeout);
        }
        for client_id in to_drop {
            self.drop_client(client_id);
        }
    }

    fn process_rpc_timeouts(&mut self) -> Duration {
        let now = Instant::now();
        self.dispatch_rpc_errors(proto::RpcErrorCode::Timeout, Some(now));
        if let Some(timeout) = self.rpc_timeouts.keys().next() {
            timeout.saturating_duration_since(now) + Duration::from_millis(1)
        } else {
            Duration::from_secs(60)
        }
    }

    fn send_internal_rpc(&mut self, pkt: Packet) -> Result<(), proto::RpcErrorCode> {
        if let Err(epkt) = self.forward_to_device(pkt, 0) {
            if let proto::Payload::RpcError(rpc_err) = epkt.payload {
                Err(rpc_err.error)
            } else {
                panic!(
                    "Unexpected error packet from sending interal RPC: {:?}",
                    epkt
                );
            }
        } else {
            Ok(())
        }
    }

    /// Process a reply to an RPC issued by the ProxyCore.
    fn internal_rpc_reply(&mut self, rep: &proto::RpcReplyPayload) {
        fn get_rate_vars(proxy: &ProxyCore) -> Option<(RateChange, u32)> {
            if let Some(dev) = proxy.device.as_ref() {
                if let Some(rate_info) = dev.tio_port.rate_info() {
                    Some((dev.rate_change_state.clone(), rate_info.target_bps))
                } else {
                    None
                }
            } else {
                None
            }
        }

        if rep.id == QUERY_RATE_RPC_ID {
            if let Some((RateChange::WaitingDeviceRate, target)) = get_rate_vars(self) {
                let next_state = if let Ok(value) = u32::from_reply(&rep.reply) {
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
        } else if rep.id == SET_RATE_RPC_ID {
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
        } else {
            // Note: internal RPCs still get remapped with all other RPCs,
            // so this ID does not come from the device itself, but from the
            // proxy remapping, and it should never be an unexpected value.
            panic!("Unexpected reply ID to internal RPC: {}", rep.id)
        }

        #[cfg(debug_assertions)]
        eprintln!(
            "Unexpected internal rpc reply 0x{:x} in state {:?}",
            rep.id,
            get_rate_vars(self)
        );
    }

    fn internal_rpc_error(&mut self, err: &proto::RpcErrorPayload) {
        // We could handle this better, but just keep the device to the default speed until the port is reset
        self.status_queue
            .send(Event::AutoRateRpcError(err.error.clone()));
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
                if let Err(rpc_error) =
                    self.send_internal_rpc(util::PacketBuilder::make_rpc_request(
                        "dev.port.rate.near",
                        &target.to_le_bytes(),
                        QUERY_RATE_RPC_ID,
                        DeviceRoute::root(),
                    ))
                {
                    self.status_queue.send(Event::AutoRateRpcError(rpc_error));
                    RateChange::GaveUp
                } else {
                    self.status_queue.send(Event::AutoRateQueried(target));
                    RateChange::WaitingDeviceRate
                }
            }
            RateChange::SetDeviceRate => {
                if self.rpc_map.len() == 0 {
                    let target = device(self).rates().target_bps;
                    if let Err(rpc_error) =
                        self.send_internal_rpc(util::PacketBuilder::make_rpc_request(
                            "dev.port.rate",
                            &target.to_le_bytes(),
                            SET_RATE_RPC_ID,
                            DeviceRoute::root(),
                        ))
                    {
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
        self.dispatch_rpc_errors(proto::RpcErrorCode::Undefined, None);
    }

    pub fn run(&mut self) {
        use channel::TryRecvError;

        if !self.try_setup_device() {
            self.status_queue.send(Event::FailedToConnect);
            self.broadcast_status(proto::ProxyStatus::FailedToConnect);
            return;
        } else {
            self.status_queue.send(Event::SensorConnected);
            self.broadcast_status(proto::ProxyStatus::SensorReconnected);
        }
        let mut device_timeout = Instant::now();

        'mainloop: loop {
            let mut timeout = self.process_rpc_timeouts();

            if self.device.is_none() {
                self.cancel_active_rpcs();
                if !self.try_setup_device() {
                    if Instant::now() > device_timeout {
                        self.status_queue.send(Event::FailedToReconnect);
                        self.broadcast_status(proto::ProxyStatus::FailedToReconnect);
                        break;
                    }
                    timeout = std::cmp::min(timeout, Duration::from_secs(1));
                } else {
                    self.status_queue.send(Event::SensorReconnected);
                    self.broadcast_status(proto::ProxyStatus::SensorReconnected);
                }
            }

            let (safe_to_forward, needs_autonegotiation, restarted) =
                if let Some(dev) = &mut self.device {
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

            sel.recv(&self.new_client_queue);
            if let Some(device) = &self.device {
                sel.recv(&device.rx_channel);
            }

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
                    if let Err(rpkt) = self.forward_to_device(pkt, client_id) {
                        rpc_errors.push(rpkt);
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
                        if let Err(_) = client.send(&pkt) {
                            failed = true;
                            break;
                        }
                    }
                    if failed {
                        self.status_queue.send(Event::ClientSendFailed(client_id));
                        self.drop_client(client_id);
                    }
                }
            } else if index == ids.len() {
                // new proxy client
                loop {
                    match self.new_client_queue.try_recv() {
                        Ok(client) => {
                            self.status_queue
                                .send(Event::NewClient(self.next_client_id));
                            self.clients.insert(self.next_client_id, client);
                            self.next_client_id += 1;
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
            } else {
                // data from the device
                loop {
                    // This should always be true, but still check.
                    let device = if let Some(x) = self.device.as_mut() {
                        x
                    } else {
                        break;
                    };
                    match device.try_recv(&self.status_queue) {
                        Ok(Ok(mut pkt)) => {
                            // In general, packets get forwarded to all clients,
                            // except for RPCs which are directed only to the
                            // client which placed the request.
                            if let Some(wire_id) = match &pkt.payload {
                                proto::Payload::RpcReply(rep) => Some(rep.id),
                                proto::Payload::RpcError(err) => Some(err.id),
                                _ => None,
                            } {
                                // Remap RPC reply or error ID to client + ID
                                let (client, client_id, original_id, method) =
                                    if let Some((client_id, rpc_id, method)) =
                                        self.rpc_restore(wire_id, &pkt.routing)
                                    {
                                        if client_id == 0 {
                                            // internal reply
                                            (None, 0, rpc_id, method)
                                        } else if let Some(client) = self.clients.get(&client_id) {
                                            self.status_queue.send(Event::RpcRestore(
                                                wire_id,
                                                (client_id, rpc_id),
                                            ));
                                            (Some(client), client_id, rpc_id, method)
                                        } else {
                                            // If we cannot find the client which originally sent the
                                            // request, just drop the packet and send an event.
                                            self.status_queue
                                                .send(Event::RpcClientNotFound(client_id));
                                            continue;
                                        }
                                    } else {
                                        self.status_queue.send(Event::RpcRestoreNotFound(wire_id));
                                        continue;
                                    };
                                // Restore original ID, and process internal RPCs.
                                match &mut pkt.payload {
                                    proto::Payload::RpcReply(rep) => {
                                        rep.id = original_id;
                                        if client_id == 0 {
                                            self.internal_rpc_reply(rep);
                                            continue;
                                        }
                                        self.broadcast_rpc_update(&method, &pkt.routing, client_id);
                                    }
                                    proto::Payload::RpcError(err) => {
                                        err.id = original_id;
                                        if client_id == 0 {
                                            self.internal_rpc_error(err);
                                            continue;
                                        }
                                    }
                                    _ => {
                                        // should never happen due to outer match
                                        panic!("unexpected payload")
                                    }
                                }
                                // Forward with correct request id to the requestor
                                if let Err(_) = client.expect("unexpected client").send(&pkt) {
                                    self.status_queue.send(Event::ClientSendFailed(client_id));
                                    self.drop_client(client_id);
                                }
                            } else {
                                let mut to_drop = vec![];
                                for (client_id, client) in self.clients.iter() {
                                    if let Err(_) = client.send(&pkt) {
                                        self.status_queue.send(Event::ClientSendFailed(*client_id));
                                        to_drop.push(*client_id);
                                    }
                                }
                                for client_id in to_drop {
                                    self.drop_client(client_id);
                                }
                            }
                        }
                        // Got a RecvError
                        Ok(Err(err)) => {
                            match err {
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
            }
        }
    }
}

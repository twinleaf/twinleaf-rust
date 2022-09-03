mod port;
pub mod proto;

pub use port::{Port, RecvError, SendError};
pub use proto::{DeviceRoute, Packet};

use std::io;
use std::thread;
use std::time::{Duration, Instant};

use std::collections::{BTreeMap, HashMap, HashSet};

use crossbeam::channel;

// Status event that is sent back to an optional user specified channel
#[derive(Debug)]
pub enum TioProxyEvent {
    SensorConnected,
    SensorDisconnected,
    FailedToConnect,
    Exiting,
    NewClient(u64),
    RpcRemap((u64, u16), u16),
    RpcRestore(u16, (u64, u16)),
    RpcTimeout(u16),
}

// Internal proxy state per client
struct TioProxyClient {
    tx: channel::Sender<Packet>,
    rx: channel::Receiver<Packet>,
    rpc_timeout: Duration,
    scope: DeviceRoute,
    forward_data: bool,
    forward_nonrpc: bool,
}

impl TioProxyClient {
    fn send(&self, pkt: &Packet) -> Result<(), channel::TrySendError<Packet>> {
        let scoped_route = if let Ok(r) = self.scope.relative_route(&pkt.routing) {
            r
        } else {
            return Ok(());
        };
        if !match pkt.payload {
            proto::Payload::RpcRequest(_)
            | proto::Payload::RpcReply(_)
            | proto::Payload::RpcError(_) => true,
            proto::Payload::StreamData(_) => self.forward_data,
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

struct RpcMapEntry {
    id: u16,
    client: u64,
    route: DeviceRoute,
    timeout: Instant,
}

/// States for the rate autonegotiation state machine
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
    tio_port: Port,
    rx_channel: channel::Receiver<Result<Packet, RecvError>>,
    rate_change_state: RateChange,
    last_rx: Instant,
    last_session: u32, // TODO: handle heartbeats without session??
}

impl ProxyDevice {
    fn has_static_rate(&self) -> bool {
        match self.rate_change_state {
            RateChange::DoNothing => true,
            _ => false,
        }
    }

    fn needs_autonegotiation(&self) -> bool {
        match self.rate_change_state {
            RateChange::DoNothing | RateChange::GaveUp => false,
            _ => true,
        }
    }

    fn try_recv(&mut self) -> Result<Result<Packet, RecvError>, crossbeam::TryRecvError> {
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
                                    if let RateChange::WaitingForSession = self.rate_change_state {
                                        self.rate_change_state = RateChange::QueryDeviceRate;
                                    } else if session != self.last_session {
                                        // It has restarted, restart autonegotiation if needed.
                                        if let RateChange::GaveUp = self.rate_change_state {
                                            println!("Detected device restart. Attempting rate negotiation.");
                                            self.rate_change_state = RateChange::QueryDeviceRate;
                                        }
                                    }
                                    self.last_session = session;
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

struct TioProxy {
    url: String,
    reconnect_timeout: Option<Duration>,
    new_client_queue: channel::Receiver<TioProxyClient>,
    status_queue: Option<channel::Sender<TioProxyEvent>>,

    device: Option<ProxyDevice>,

    /// Id to assign to the next client, 64 bits.
    /// It is realistic to assume that it will never wrap around.
    next_client_id: u64,
    clients: HashMap<u64, TioProxyClient>,

    next_rpc_id: u16,
    rpc_map: HashMap<u16, RpcMapEntry>,
    rpc_timeouts: BTreeMap<Instant, HashSet<u16>>,
}

impl TioProxy {
    fn new(
        url: String,
        reconnect_timeout: Option<Duration>,
        new_client_queue: channel::Receiver<TioProxyClient>,
        status_queue: Option<channel::Sender<TioProxyEvent>>,
    ) -> TioProxy {
        TioProxy {
            url: url,
            reconnect_timeout: reconnect_timeout,
            new_client_queue: new_client_queue,
            status_queue: status_queue,
            device: None,
            // Start from client 1, as 0 is reserved for internal RPCs.
            next_client_id: 1,
            clients: HashMap::new(),
            next_rpc_id: 0,
            rpc_map: HashMap::new(),
            rpc_timeouts: BTreeMap::new(),
        }
    }

    fn try_setup_device(&mut self) -> bool {
        if self.device.is_some() {
            return true;
        }
        let (port_rx_send, port_rx) = Port::rx_channel();
        let port = match Port::from_url(&self.url, Port::rx_to_channel(port_rx_send)) {
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
            last_session: 0,
        });
        true
    }

    fn rpc_restore(&mut self, wire_id: u16) -> Option<(u16, u64)> {
        let remap = match self.rpc_map.remove(&wire_id) {
            None => {
                return None;
            }
            Some(r) => r,
        };
        let ids = self.rpc_timeouts.get_mut(&remap.timeout).unwrap();
        ids.remove(&wire_id);
        if ids.len() == 0 {
            self.rpc_timeouts.remove(&remap.timeout);
        }
        self.send_event(TioProxyEvent::RpcRestore(wire_id, (remap.client, remap.id)));
        Some((remap.id, remap.client))
    }

    // Ok: successful. Err: packet should be sent back to client
    fn forward_to_device(&mut self, mut pkt: Packet, client_id: u64) -> Result<(), Packet> {
        let mut rpc_mapped_id: Option<u16> = None;
        let mut timeout = Instant::now();
        if let proto::Payload::RpcRequest(req) = &mut pkt.payload {
            let wire_id = self.next_rpc_id;
            // Always increment even if it fails, on the slim chance it hits an open spot
            // next time.
            self.next_rpc_id += 1;
            if self.rpc_map.contains_key(&wire_id) {
                let mut fail = Packet::make_rpc_error(req.id, proto::RpcErrorCode::NoBufs);
                fail.routing = pkt.routing;
                return Err(fail);
            }
            timeout += if client_id != 0 {
                self.clients.get(&client_id).unwrap().rpc_timeout
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
                },
            );
            self.send_event(TioProxyEvent::RpcRemap((client_id, req.id), wire_id));
            req.id = wire_id;
            rpc_mapped_id = Some(wire_id);
        }
        if let Some(dev) = &self.device {
            if let Ok(()) = dev.tio_port.send(pkt) {
                if let Some(rpc_id) = rpc_mapped_id {
                    if !self.rpc_timeouts.contains_key(&timeout) {
                        self.rpc_timeouts.insert(timeout, HashSet::new());
                    }
                    let timeout_ids = self.rpc_timeouts.get_mut(&timeout).unwrap();
                    timeout_ids.insert(rpc_id);
                }
                return Ok(());
            }
        }
        // If we got here, the packet was not sent. avoid erroring out since if there is something wrong with the device we'll notice in the main loop soon
        // but remove the rpc from the map and send back an error to the client.
        if let Some(rpc_id) = rpc_mapped_id {
            let remap = self.rpc_map.remove(&rpc_id).unwrap();
            let mut fail = Packet::make_rpc_error(remap.id, proto::RpcErrorCode::Undefined);
            fail.routing = remap.route;
            Err(fail)
        } else {
            Ok(())
        }
    }

    fn dispatch_rpc_timeouts(&mut self, until: Instant, error: proto::RpcErrorCode) {
        let mut to_remove = Vec::new();
        for (timeout, rpc_ids) in self.rpc_timeouts.iter() {
            if *timeout >= until {
                break;
            }
            to_remove.push(*timeout);
            for rpc_id in rpc_ids {
                self.send_event(TioProxyEvent::RpcTimeout(*rpc_id));
                let remap = self.rpc_map.remove(&rpc_id).unwrap();
                let client = if let Some(c) = self.clients.get(&remap.client) {
                    c
                } else {
                    // Client is gone, do nothing.
                    // TODO: maybe inform via status channel
                    continue;
                };
                let mut pkt = Packet::make_rpc_error(remap.id, error.clone());
                pkt.routing = remap.route;
                client.send(&pkt).unwrap(); // TODO
            }
        }
        for timeout in to_remove {
            self.rpc_timeouts.remove(&timeout);
        }
    }

    fn process_rpc_timeouts(&mut self) -> Duration {
        let now = Instant::now();
        self.dispatch_rpc_timeouts(now, proto::RpcErrorCode::Timeout);
        if let Some(timeout) = self.rpc_timeouts.keys().next() {
            timeout.saturating_duration_since(now) + Duration::from_millis(1)
        } else {
            Duration::from_secs(60)
        }
    }

    fn send_internal_rpc(&mut self, pkt: Packet) -> Result<(), Packet> {
        self.forward_to_device(pkt, 0)
    }

    fn internal_rpc_reply(&mut self, rep: &proto::RpcReplyPayload) {
        //println!("GOT INTERNAL REPLY: {:?}", rep);
        // TODO: better handling. now we just assume it's 4 bytes
        let value = u32::from_le_bytes(rep.reply[0..4].try_into().unwrap());
        if let Some(dev) = self.device.as_ref() {
            let target = dev.tio_port.rate_info().unwrap().target_bps;
            let new_state = match dev.rate_change_state {
                RateChange::WaitingDeviceRate => {
                    if value == 0 {
                        println!("RATE NOT SUPPORTED BY DEVICE");
                        RateChange::GaveUp
                    } else {
                        let error = (((target as f64) - (value as f64)) / (value as f64)).abs();
                        if error > 0.015 {
                            println!("RATE WOULD BE OFF BY >1.5% {} {}", target, value);
                            RateChange::GaveUp
                        } else {
                            // TODO: wait for RPCs completed??
                            println!("DEVICE RATE WILL BE SET TO: {}", value);
                            RateChange::SetDeviceRate
                        }
                    }
                }
                RateChange::WaitingNewRate => {
                    println!("SETTING NEW RATE: {}", target);
                    match dev.tio_port.set_rate(target) {
                        Ok(_) => RateChange::RateChanged,
                        Err(_) => {
                            println!("Failed to set rate, reverting to default");
                            RateChange::GaveUp
                        }
                    }
                }
                _ => {
                    panic!("unexpected internal rpc reply");
                }
            };
            self.device.as_mut().unwrap().rate_change_state = new_state; // TODO: rework this mess
        }
    }

    fn internal_rpc_error(&mut self, err: &proto::RpcErrorPayload) {
        println!("GOT INTERNAL ERROR: {:?}", err);
        // We could handle this better, but just set the device to the default speed until the port is reset
        println!("Keeping port at default rate");
        if let Some(dev) = self.device.as_mut() {
            dev.rate_change_state = RateChange::GaveUp;
        }
    }

    fn autonegotiation(&mut self) {
        // when this is called, device will be Some, and it does not change from any of the called methods
        match self.device.as_ref().unwrap().rate_change_state {
            RateChange::QueryDeviceRate => {
                let target = self
                    .device
                    .as_ref()
                    .unwrap()
                    .tio_port
                    .rate_info()
                    .unwrap()
                    .target_bps;
                self.send_internal_rpc(proto::Packet::rpc(
                    "dev.port.rate.near".to_string(),
                    &target.to_le_bytes(),
                ))
                .unwrap(); // TODO
                self.device.as_mut().unwrap().rate_change_state = RateChange::WaitingDeviceRate;
            }
            RateChange::SetDeviceRate => {
                if self.rpc_map.len() == 0 {
                    let target = self
                        .device
                        .as_ref()
                        .unwrap()
                        .tio_port
                        .rate_info()
                        .unwrap()
                        .target_bps;
                    println!("SETTING PORT RATE TO {}", target);
                    self.send_internal_rpc(proto::Packet::rpc(
                        "dev.port.rate".to_string(),
                        &target.to_le_bytes(),
                    ))
                    .unwrap(); //TODO
                    self.device.as_mut().unwrap().rate_change_state = RateChange::WaitingNewRate;
                } else {
                    println!("active RPCs, waiting to rate change");
                }
            }
            RateChange::RateChanged => {
                let last_rx_delta = self.device.as_ref().unwrap().last_rx.elapsed();
                if last_rx_delta > Duration::from_millis(1000) {
                    println!(
                        "Did not receive sensor data in one second. Reverting to default rate."
                    );
                    let dev = self.device.as_mut().unwrap();
                    dev.tio_port
                        .set_rate(dev.tio_port.rate_info().unwrap().default_bps)
                        .unwrap();
                    dev.rate_change_state = RateChange::GaveUp;
                }
            }
            // In any other case, do nothing
            _ => {}
        }
    }

    fn cancel_active_rpcs(&mut self) {
        self.dispatch_rpc_timeouts(
            Instant::now() + Duration::from_secs(1000),
            proto::RpcErrorCode::Undefined,
        );
    }

    fn send_event(&self, event: TioProxyEvent) {
        if self.status_queue.is_some() {
            self.status_queue.as_ref().unwrap().send(event).unwrap(); // TODO
        }
    }

    fn run(&mut self) {
        use channel::TryRecvError;

        if !self.try_setup_device() {
            self.send_event(TioProxyEvent::FailedToConnect);
            return;
        } else {
            self.send_event(TioProxyEvent::SensorConnected);
        }
        let mut device_timeout = Instant::now();

        let mut clients_to_drop: HashSet<u64> = HashSet::new();

        'mainloop: loop {
            let mut timeout = self.process_rpc_timeouts();
            if self.device.is_none() {
                self.cancel_active_rpcs();
                if !self.try_setup_device() {
                    if Instant::now() > device_timeout {
                        self.send_event(TioProxyEvent::FailedToConnect);
                        break;
                    }
                    timeout = std::cmp::min(timeout, Duration::from_secs(1));
                } else {
                    self.send_event(TioProxyEvent::SensorConnected);
                }
            }
            if let Some(dev) = &self.device {
                // If there is some device connected and it supports it,
                // do autonegotiation upkeep.
                if dev.needs_autonegotiation() {
                    self.autonegotiation();
                    timeout = std::cmp::min(timeout, Duration::from_millis(200));
                }
            }
            for client_id in clients_to_drop.drain() {
                drop(self.clients.remove(&client_id));
            }
            let mut sel = channel::Select::new();
            let mut ids: Vec<u64> = Vec::new();
            for (id, client) in self.clients.iter() {
                sel.recv(&client.rx);
                ids.push(*id);
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
                loop {
                    match self.clients.get(&client_id).unwrap().recv() {
                        Ok(pkt) => {
                            if let Err(rpkt) = self.forward_to_device(pkt, client_id) {
                                // TODO: error handling. not much we can do here but inform the status queue
                                self.clients.get(&client_id).unwrap().send(&rpkt).unwrap();
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            clients_to_drop.insert(client_id);
                            break;
                        }
                    }
                }
            } else if index == ids.len() {
                // new proxy client
                loop {
                    match self.new_client_queue.try_recv() {
                        Ok(client) => {
                            self.send_event(TioProxyEvent::NewClient(self.next_client_id));
                            self.clients.insert(self.next_client_id, client);
                            self.next_client_id += 1;
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            self.send_event(TioProxyEvent::Exiting);
                            break 'mainloop;
                        }
                    }
                }
            } else {
                // data from the device
                loop {
                    //println!("PP: devdata");
                    match self.device.as_mut().unwrap().try_recv() {
                        Ok(Ok(mut pkt)) => {
                            match &mut pkt.payload {
                                proto::Payload::RpcReply(rep) => {
                                    let (original_id, client) =
                                        if let Some((o, c)) = self.rpc_restore(rep.id) {
                                            if let Some(client) = self.clients.get(&c) {
                                                // TODO: say something here or in the else branch
                                                (o, client)
                                            } else {
                                                if c == 0 {
                                                    // internal reply
                                                    rep.id = o;
                                                    self.internal_rpc_reply(&rep);
                                                }
                                                continue;
                                            }
                                        } else {
                                            // TODO: say something
                                            continue;
                                        };
                                    rep.id = original_id;
                                    client.send(&pkt).unwrap(); // TODO
                                }
                                // TODO: find a good way to avoid duplication
                                proto::Payload::RpcError(err) => {
                                    let (original_id, client) =
                                        if let Some((o, c)) = self.rpc_restore(err.id) {
                                            if let Some(client) = self.clients.get(&c) {
                                                // TODO: say something here or in the else branch
                                                (o, client)
                                            } else {
                                                if c == 0 {
                                                    // internal error
                                                    err.id = o;
                                                    self.internal_rpc_error(&err);
                                                }
                                                continue;
                                            }
                                        } else {
                                            // TODO: say something
                                            continue;
                                        };
                                    err.id = original_id;
                                    client.send(&pkt).unwrap(); // TODO
                                }
                                _ => {
                                    for (_, client) in self.clients.iter() {
                                        // TODO: check for failure
                                        client.send(&pkt).unwrap();
                                    }
                                }
                            }
                        }
                        // Got a RecvError
                        Ok(Err(err)) => {
                            match err {
                                // TODO: complete list of non-fatal errors
                                RecvError::Protocol(proto::Error::Text(txt)) => {
                                    println!("PP: TEXT: {}", txt); //TODO: send event
                                }
                                RecvError::Protocol(perror) => {
                                    println!("PP: protocol error: {:?}", perror);
                                    //TODO: send event
                                }
                                // All other errors are treated as fatal.
                                _ => {
                                    println!("PP: porterror {:?}", err); //TODO: send event
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
                            self.send_event(TioProxyEvent::SensorDisconnected);
                            break;
                        }
                    }
                }
            }
        }
    }
}

pub struct TioProxyPort {
    new_client_queue: channel::Sender<TioProxyClient>,
}

impl TioProxyPort {
    pub fn new(
        url: &str,
        reconnect_timeout: Option<Duration>,
        status_queue: Option<channel::Sender<TioProxyEvent>>,
    ) -> TioProxyPort {
        let (sender, receiver) = channel::bounded::<TioProxyClient>(5);
        let url_string = url.to_string();
        thread::spawn(move || {
            let mut proxy = TioProxy::new(url_string, reconnect_timeout, receiver, status_queue);
            proxy.run();
        });
        TioProxyPort {
            new_client_queue: sender,
        }
    }

    pub fn new_port(
        &self,
        rpc_timeout: Option<Duration>,
        scope: DeviceRoute,
        forward_data: bool,
        forward_nonrpc: bool,
    ) -> io::Result<(channel::Sender<Packet>, channel::Receiver<Packet>)> {
        let default_rpc_timeout = Duration::from_millis(2000);
        let rpc_timeout = rpc_timeout.unwrap_or(default_rpc_timeout);
        if rpc_timeout < Duration::from_millis(100) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rpc timeout too short",
            ));
        }
        if rpc_timeout > Duration::from_secs(60) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rpc timeout too long",
            ));
        }

        let (client_to_proxy_sender, proxy_from_client_receiver) = channel::bounded::<Packet>(32);
        let (proxy_to_client_sender, client_from_proxy_receiver) = channel::bounded::<Packet>(32);
        if let Err(_) = self.new_client_queue.send(TioProxyClient {
            tx: proxy_to_client_sender,
            rx: proxy_from_client_receiver,
            rpc_timeout: rpc_timeout,
            scope: scope,
            forward_data: forward_data,
            forward_nonrpc: forward_nonrpc,
        }) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "failed to send client to thread",
            ));
        }
        Ok((client_to_proxy_sender, client_from_proxy_receiver))
    }

    pub fn port(&self) -> io::Result<(channel::Sender<Packet>, channel::Receiver<Packet>)> {
        self.new_port(None, DeviceRoute::root(), true, true)
    }

    pub fn scoped_port(
        &self,
        root: DeviceRoute,
    ) -> io::Result<(channel::Sender<Packet>, channel::Receiver<Packet>)> {
        self.new_port(None, root, true, true)
    }
}

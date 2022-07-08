mod port;
pub mod proto;

pub use port::{Port, RecvError, SendError};
pub use proto::Packet;

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
}

// Internal proxy state per client
struct TioProxyClient {
    tx: channel::Sender<Packet>,
    rx: channel::Receiver<Packet>,
    rpc_timeout: Duration,
    scope: Vec<u8>,
    forward_data: bool,
    forward_nonrpc: bool,
}

impl TioProxyClient {
    fn send(&self, pkt: &Packet) -> Result<(), channel::TrySendError<Packet>> {
        // TODO: encapsulate this stuff in a routing type
        if pkt.routing.len() < self.scope.len() {
            return Ok(());
        }
        if pkt.routing[0..self.scope.len()] != self.scope {
            return Ok(());
        }
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
            routing: pkt.routing[self.scope.len()..].to_vec(),
            ttl: pkt.ttl,
        })
    }

    fn recv(&self) -> Result<Packet, channel::TryRecvError> {
        let mut pkt = self.rx.try_recv()?;
        if self.scope.len() != 0 {
            let mut routing = self.scope.clone();
            routing.extend_from_slice(&pkt.routing[..]);
            pkt.routing = routing;
        }
        Ok(pkt)
    }
}

struct RpcMapEntry {
    id: u16,
    client: u64,
    route: Vec<u8>,
    timeout: Instant,
}

struct ProxyDevice {
    tio_port: Port,
    rx_channel: channel::Receiver<Result<Packet, RecvError>>,
}

struct TioProxy {
    url: String,
    reconnect_timeout: Option<Duration>,
    new_client_queue: channel::Receiver<TioProxyClient>,
    status_queue: Option<channel::Sender<TioProxyEvent>>,

    device: Option<ProxyDevice>,

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
            next_client_id: 0,
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
        if let Some(rates) = port.rate_info() {
            println!(
                "RATES: {} {} {}",
                rates.default_bps, rates.min_bps, rates.max_bps
            );
        }
        self.device = Some(ProxyDevice {
            tio_port: port,
            rx_channel: port_rx,
        });
        true
    }

    fn rpc_remap(&mut self, wire_id: u16) -> Option<(u16, &TioProxyClient)> {
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
        if let Some(client) = self.clients.get(&remap.client) {
            Some((remap.id, client))
        } else {
            None
        }
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
            timeout += self.clients.get(&client_id).unwrap().rpc_timeout;
            self.rpc_map.insert(
                wire_id,
                RpcMapEntry {
                    id: req.id,
                    client: client_id,
                    route: pkt.routing.clone(),
                    timeout: timeout,
                },
            );
            println!("RPC ID MAPPED: {} -> {}", req.id, wire_id);
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
                let remap = self.rpc_map.remove(&rpc_id).unwrap();
                let client = if let Some(c) = self.clients.get(&remap.client) {
                    c
                } else {
                    // Client is gone, do nothing.
                    // TODO: maybe inform via status channel
                    continue;
                };
                client.send(&Packet::make_rpc_error(remap.id, error.clone()));
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

    fn cancel_active_rpcs(&mut self) {
        self.dispatch_rpc_timeouts(
            Instant::now() + Duration::from_secs(1000),
            proto::RpcErrorCode::Undefined,
        );
    }

    fn run(&mut self) {
        use channel::TryRecvError;

        if !self.try_setup_device() {
            return;
        }
        let mut device_timeout = Instant::now();

        let mut clients_to_drop: HashSet<u64> = HashSet::new();

        'mainloop: loop {
            let mut timeout = self.process_rpc_timeouts();
            if self.device.is_none() {
                self.cancel_active_rpcs();
                if !self.try_setup_device() {
                    if Instant::now() > device_timeout {
                        break;
                    }
                    timeout = std::cmp::min(timeout, Duration::from_secs(1));
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
                                self.clients.get(&client_id).unwrap().send(&rpkt);
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
                    println!("PP: newclient");
                    match self.new_client_queue.try_recv() {
                        Ok(client) => {
                            println!("PP: new client {}", self.next_client_id);
                            self.clients.insert(self.next_client_id, client);
                            self.next_client_id += 1;
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            // channel closed.
                            println!("PP: channelclosed");
                            break 'mainloop;
                        }
                    }
                }
            } else {
                // data from the device
                loop {
                    //println!("PP: devdata");
                    match self.device.as_ref().unwrap().rx_channel.try_recv() {
                        Ok(Ok(mut pkt)) => {
                            match &mut pkt.payload {
                                proto::Payload::RpcReply(rep) => {
                                    let (original_id, client) =
                                        if let Some((o, c)) = self.rpc_remap(rep.id) {
                                            (o, c)
                                        } else {
                                            // TODO: say something
                                            continue;
                                        };
                                    rep.id = original_id;
                                    client.send(&pkt);
                                }
                                // TODO: find a good way to avoid duplication
                                proto::Payload::RpcError(err) => {
                                    let (original_id, client) =
                                        if let Some((o, c)) = self.rpc_remap(err.id) {
                                            (o, c)
                                        } else {
                                            // TODO: say something
                                            continue;
                                        };
                                    err.id = original_id;
                                    client.send(&pkt);
                                }
                                _ => {
                                    for (_, client) in self.clients.iter() {
                                        // TODO: check for failure
                                        client.send(&pkt);
                                    }
                                }
                            }
                        }
                        Ok(Err(_)) => {
                            // TODO: not all error should be fatal
                            println!("PP: porterror");
                            break 'mainloop;
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
                            break;
                            //break 'mainloop;
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
        scope: Vec<u8>,
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
        if let Err(e) = self.new_client_queue.send(TioProxyClient {
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
        self.new_port(None, vec![], true, true)
    }

    pub fn scoped_port() {}

    pub fn direct_port() {}
}

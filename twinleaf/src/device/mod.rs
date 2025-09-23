use super::data::{DeviceDataParser, DeviceFullMetadata, Sample};
use super::tio;
use crossbeam::channel::Select;
use proto::DeviceRoute;
use tio::{proto, proxy, util};

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::time::{Duration, Instant};

pub enum Live {}
pub enum Frozen {}

#[derive(Debug)]
pub enum BuildError {
    Port(proxy::PortError),
    Recv(proxy::RecvError),
    Rpc(proxy::RpcError),
    MissingDiscoveryDeadline,
    NoDevicesDiscovered,
}
impl From<proxy::PortError> for BuildError { fn from(e: proxy::PortError) -> Self { Self::Port(e) } }
impl From<proxy::RecvError> for BuildError { fn from(e: proxy::RecvError) -> Self { Self::Recv(e) } }
impl From<proxy::RpcError> for BuildError { fn from(e: proxy::RpcError) -> Self { Self::Rpc(e) } }

pub struct Device {
    dev_port: proxy::Port,
    parser: DeviceDataParser,
    n_reqs: usize,
    sample_queue: VecDeque<Sample>,
}

impl Device {
    pub fn new(dev_port: proxy::Port) -> Device {
        Device {
            dev_port,
            parser: DeviceDataParser::new(false),
            n_reqs: 0,
            sample_queue: VecDeque::new(),
        }
    }

    pub fn open(proxy: &proxy::Interface, route: DeviceRoute) -> Result<Device, proxy::PortError> {
        let port = proxy.device_full(route)?;
        Ok(Self::new(port))
    }

    pub fn route(&self) -> DeviceRoute {
        self.dev_port.scope()
    }

    fn internal_rpcs(&mut self) -> Result<(), proxy::SendError> {
        if self.n_reqs == 0 {
            let reqs = self.parser.requests();
            for req in reqs {
                self.dev_port.send(req)?;
                self.n_reqs += 1;
            }
        }
        Ok(())
    }

    fn process_packet(&mut self, pkt: tio::Packet) -> Option<tio::Packet> {
        match &pkt.payload {
            proto::Payload::RpcReply(rep) => {
                if rep.id == 7855 { self.n_reqs -= 1 } else { return Some(pkt); }
            }
            proto::Payload::RpcError(err) => {
                if err.id == 7855 { self.n_reqs -= 1 } else { return Some(pkt); }
            }
            _ => {}
        }
        self.sample_queue
            .append(&mut VecDeque::from(self.parser.process_packet(&pkt)));
        None
    }

    pub fn get_metadata(&mut self) -> Result<DeviceFullMetadata, proxy::RpcError> {
        loop {
            if self.n_reqs == 0 {
                match self.parser.get_metadata() {
                    Ok(full_meta) => return Ok(full_meta),
                    Err(reqs) => {
                        for req in reqs {
                            self.dev_port.send(req).map_err(proxy::RpcError::SendFailed)?;
                            self.n_reqs += 1;
                        }
                    }
                }
            }
            let pkt = self.dev_port.recv().map_err(proxy::RpcError::RecvFailed)?;
            self.process_packet(pkt);
        }
    }

    pub fn next(&mut self) -> Result<Sample, proxy::RpcError> {
        loop {
            if let Some(s) = self.sample_queue.pop_front() { return Ok(s); }
            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;
            let pkt = self.dev_port.recv().map_err(proxy::RpcError::RecvFailed)?;
            self.process_packet(pkt);
        }
    }

    pub fn try_next(&mut self) -> Result<Option<Sample>, proxy::RpcError> {
        loop {
            if let Some(s) = self.sample_queue.pop_front() { return Ok(Some(s)); }
            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;
            let pkt = match self.dev_port.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => return Ok(None),
                Err(e) => return Err(proxy::RpcError::RecvFailed(e)),
            };
            self.process_packet(pkt);
        }
    }

    pub fn drain(&mut self) -> Result<Vec<Sample>, proxy::RpcError> {
        loop {
            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;
            match self.dev_port.try_recv() {
                Ok(pkt) => { self.process_packet(pkt); }
                Err(proxy::RecvError::WouldBlock) => break,
                Err(e) => return Err(proxy::RpcError::RecvFailed(e)),
            }
        }
        Ok(self.sample_queue.drain(..).collect())
    }

    pub fn select_recv<'a>(&'a self, sel: &mut crossbeam::channel::Select<'a>) -> usize {
        self.dev_port.select_recv(sel)
    }

    pub fn raw_rpc(&mut self, name: &str, arg: &[u8]) -> Result<Vec<u8>, proxy::RpcError> {
        if let Err(err) = self.dev_port.send(util::PacketBuilder::make_rpc_request(
            name, arg, 0, DeviceRoute::root(),
        )) {
            return Err(proxy::RpcError::SendFailed(err));
        }
        loop {
            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;
            let pkt = self.dev_port.recv().map_err(proxy::RpcError::RecvFailed)?;
            if let Some(pkt) = self.process_packet(pkt) {
                match pkt.payload {
                    proto::Payload::RpcReply(rep) => return Ok(rep.reply),
                    proto::Payload::RpcError(err) => return Err(proxy::RpcError::ExecError(err)),
                    _ => unreachable!("process_packet returned a non-RPC packet to raw_rpc"),
                }
            }
        }
    }

    pub fn rpc<ReqT: util::TioRpcRequestable<ReqT>, RepT: util::TioRpcReplyable<RepT>>(
        &mut self, name: &str, arg: ReqT,
    ) -> Result<RepT, proxy::RpcError> {
        let ret = self.raw_rpc(name, &arg.to_request())?;
        if let Ok(val) = RepT::from_reply(&ret) { Ok(val) } else { Err(proxy::RpcError::TypeError) }
    }

    pub fn action(&mut self, name: &str) -> Result<(), proxy::RpcError> { self.rpc(name, ()) }

    pub fn get<T: util::TioRpcReplyable<T>>(&mut self, name: &str) -> Result<T, proxy::RpcError> {
        self.rpc(name, ())
    }

    pub fn get_multi(&mut self, name: &str) -> Result<Vec<u8>, proxy::RpcError> {
        let mut full_reply = vec![];
        for i in 0u16..=65535u16 {
            match self.raw_rpc(&name, &i.to_le_bytes().to_vec()) {
                Ok(mut rep) => full_reply.append(&mut rep),
                Err(err @ proxy::RpcError::ExecError(_)) => {
                    if let proxy::RpcError::ExecError(payload) = &err {
                        if let proto::RpcErrorCode::InvalidArgs = payload.error { break; }
                    }
                    return Err(err);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(full_reply)
    }

    pub fn get_multi_str(&mut self, name: &str) -> Result<String, proxy::RpcError> {
        let reply_bytes = self.get_multi(name)?;
        Ok(String::from_utf8_lossy(&reply_bytes).to_string())
    }
}

pub struct DeviceTree<M = Live> {
    proxy: proxy::Interface,
    probe: Option<proxy::Port>, // Some for Live, None for Frozen
    devices: Vec<Device>,
    route_map: HashMap<DeviceRoute, usize>,
    rr_next: usize,
    _mode: PhantomData<M>,
}

impl DeviceTree<Live> {
    pub fn open(proxy: proxy::Interface, route: DeviceRoute) -> Result<Self, proxy::PortError> {
        let probe = proxy.subtree_full(route)?;
        Ok(DeviceTree {
            proxy,
            probe: Some(probe),
            devices: vec![],
            route_map: HashMap::new(),
            rr_next: 0,
            _mode: PhantomData,
        })
    }

    fn probe_mut(&mut self) -> &mut proxy::Port {
        self.probe.as_mut().expect("Live tree must have a probe")
    }

    fn get_or_create(&mut self, route: &DeviceRoute) -> Result<Option<&mut Device>, proxy::RecvError> {
        if let Some(&idx) = self.route_map.get(route) { return Ok(Some(&mut self.devices[idx])); }
        match Device::open(&self.proxy, route.clone()) {
            Ok(dev) => {
                let idx = self.devices.len();
                self.route_map.insert(route.clone(), idx);
                self.devices.push(dev);
                Ok(self.devices.last_mut())
            }
            Err(e @ proxy::PortError::FailedNewClientSetup)
            | Err(e @ proxy::PortError::RpcTimeoutTooShort)
            | Err(e @ proxy::PortError::RpcTimeoutTooLong) => {
                eprintln!("device open failed for {route:?}: {:?}", e);
                Ok(None)
            }
        }
    }

    pub fn discover_with_watchdog(
        &mut self,
        total: Option<Duration>,
        quiet: Option<Duration>,
    ) -> Result<(), proxy::RecvError> {
        let start = Instant::now();
        let mut last_new = Instant::now();
        loop {
            if let Some(b) = total { if start.elapsed() >= b { break; } }
            if let Some(q) = quiet { if last_new.elapsed() >= q { break; } }
            match self.probe_mut().try_recv() {
                Ok(pkt) => { self.get_or_create(&pkt.routing)?; last_new = Instant::now(); }
                Err(proxy::RecvError::WouldBlock) => std::thread::yield_now(),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn prefetch_metadata(&mut self) -> Result<(), proxy::RpcError> {
        for dev in self.devices.iter_mut() {
            let _ = dev.get_metadata()?;
        }
        Ok(())
    }

    pub fn next(&mut self) -> Result<(Sample, DeviceRoute), proxy::RpcError> {
        loop {
            let mut sel = Select::new();
            self.probe.as_ref().unwrap().select_recv(&mut sel);
            for dev in &self.devices { dev.select_recv(&mut sel); }
            let idx = sel.ready();
            if idx == 0 {
                let pkt = self.probe_mut().recv().map_err(proxy::RpcError::RecvFailed)?;
                self.get_or_create(&pkt.routing).map_err(proxy::RpcError::RecvFailed)?;
                continue;
            } else {
                let dev = &mut self.devices[idx - 1];
                let s = dev.next()?;
                return Ok((s, dev.route()));
            }
        }
    }

    pub fn try_next(&mut self) -> Result<Option<(Sample, DeviceRoute)>, proxy::RpcError> {
        loop {
            match self.probe_mut().try_recv() {
                Ok(pkt) => { self.get_or_create(&pkt.routing).map_err(proxy::RpcError::RecvFailed)?; continue; }
                Err(proxy::RecvError::WouldBlock) => break,
                Err(e) => return Err(proxy::RpcError::RecvFailed(e)),
            }
        }
        if self.devices.is_empty() { return Ok(None); }
        let mut sel = Select::new();
        for dev in &self.devices { dev.select_recv(&mut sel); }
        let idx = match sel.try_ready() { Ok(i) => i, Err(_) => return Ok(None) };
        let dev = &mut self.devices[idx];
        match dev.try_next()? {
            Some(s) => Ok(Some((s, dev.route()))),
            None => Ok(None),
        }
    }

    pub fn drain(&mut self) -> Result<Vec<(Sample, DeviceRoute)>, proxy::RpcError> {
        let mut out = Vec::new();
        loop {
            let mut sel = Select::new();
            self.probe.as_ref().unwrap().select_recv(&mut sel);
            for dev in &self.devices { dev.select_recv(&mut sel); }
            let idx = match sel.try_ready() { Ok(i) => i, Err(_) => break };
            if idx == 0 {
                while let Ok(pkt) = self.probe_mut().try_recv() {
                    self.get_or_create(&pkt.routing).map_err(proxy::RpcError::RecvFailed)?;
                }
            } else {
                let dev = &mut self.devices[idx - 1];
                let route = dev.route();
                for s in dev.drain()? { out.push((s, route.clone())); }
            }
        }
        Ok(out)
    }

    pub fn freeze(self) -> DeviceTree<Frozen> {
        DeviceTree::<Frozen> {
            proxy: self.proxy,
            probe: None,
            devices: self.devices,
            route_map: self.route_map,
            rr_next: 0,
            _mode: PhantomData,
        }
    }
}

impl DeviceTree<Frozen> {
    fn select_rotated<'a>(&'a self, sel: &mut Select<'a>) -> usize {
        let n = self.devices.len();
        for k in 0..n {
            let idx = (self.rr_next + k) % n;
            self.devices[idx].select_recv(sel);
        }
        n
    }

    pub fn next(&mut self) -> Result<(Sample, DeviceRoute), proxy::RpcError> {
        let n = self.devices.len();
        let mut sel = Select::new();
        self.select_rotated(&mut sel);
        let ready = sel.ready();
        let actual = (self.rr_next + ready) % n;
        let dev = &mut self.devices[actual];
        let s = dev.next()?;
        let route = dev.route();
        self.rr_next = (actual + 1) % n;
        Ok((s, route))
    }

    pub fn try_next(&mut self) -> Result<Option<(Sample, DeviceRoute)>, proxy::RpcError> {
        let n = self.devices.len();
        let mut sel = Select::new();
        self.select_rotated(&mut sel);
        let ready = match sel.try_ready() { Ok(i) => i, Err(_) => return Ok(None) };
        let actual = (self.rr_next + ready) % n;
        let dev = &mut self.devices[actual];
        match dev.try_next()? {
            Some(s) => {
                let route = dev.route();
                self.rr_next = (actual + 1) % n;
                Ok(Some((s, route)))
            }
            None => Ok(None),
        }
    }

    pub fn drain(&mut self) -> Result<Vec<(Sample, DeviceRoute)>, proxy::RpcError> {
        let mut out = Vec::new();
        let n = self.devices.len();
        for k in 0..n {
            let idx = (self.rr_next + k) % n;
            let dev = &mut self.devices[idx];
            let route = dev.route();
            for s in dev.drain()? {
                out.push((s, route.clone()));
            }
        }
        self.rr_next = (self.rr_next + 1) % n.max(1);
        Ok(out)
    }

    pub fn devices(&self) -> &[Device] { &self.devices }

    pub fn sort_by_route(&mut self) {
        self.devices.sort_by_key(|d| format!("{:?}", d.route()));
        self.route_map.clear();
        for (i, d) in self.devices.iter().enumerate() {
            self.route_map.insert(d.route(), i);
        }
    }

    pub fn sort_by_device_name(&mut self) -> Result<(), proxy::RpcError> {
        self.sort_by_meta(|m| m.device.name.clone())
    }

    pub fn sort_by_serial_number(&mut self) -> Result<(), proxy::RpcError> {
        self.sort_by_meta(|m| m.device.serial_number.clone())
    }

    fn sort_by_meta<F>(&mut self, f: F) -> Result<(), proxy::RpcError>
    where
        F: Fn(&DeviceFullMetadata) -> String
    {
        let mut keys: Vec<(usize, String, String)> = Vec::with_capacity(self.devices.len());
        for (i, dev) in self.devices.iter_mut().enumerate() {
            let meta = dev.get_metadata()?;
            let primary = f(&meta);
            let route_str = format!("{:?}", dev.route());
            keys.push((i, primary, route_str));
        }

        keys.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.2.cmp(&b.2)));

        let old = std::mem::take(&mut self.devices);
        let mut pool: Vec<Option<Device>> = old.into_iter().map(Some).collect();
        self.devices = Vec::with_capacity(pool.len());
        for (idx, _, _) in keys {
            self.devices.push(pool[idx].take().unwrap());
        }

        self.route_map.clear();
        for (i, d) in self.devices.iter().enumerate() {
            self.route_map.insert(d.route(), i);
        }
        Ok(())
    }
}

type FrozenSortFn = fn(&mut DeviceTree<Frozen>) -> Result<(), proxy::RpcError>;
pub struct DeviceTreeBuilder<M = Live> {
    proxy: proxy::Interface,
    route: DeviceRoute,
    discover_for: Option<Duration>,
    watchdog_for: Option<Duration>,
    prefetch_metadata: bool,
    sorter: Option<FrozenSortFn>,
    _mode: PhantomData<M>,
}

impl<M> DeviceTreeBuilder<M> {
    pub fn discover_for(mut self, d: Duration) -> Self { self.discover_for = Some(d); self }
    pub fn watchdog_for(mut self, d: Duration) -> Self { self.watchdog_for = Some(d); self }
    pub fn prefetch_metadata(mut self) -> Self { self.prefetch_metadata = true; self }
}

impl DeviceTreeBuilder<Live> {
    pub fn new(proxy: proxy::Interface, route: DeviceRoute) -> Self {
        Self {
            proxy, route,
            discover_for: None,
            watchdog_for: None,
            prefetch_metadata: false,
            sorter: None,
            _mode: PhantomData,
        }
    }

    pub fn freeze(self) -> DeviceTreeBuilder<Frozen> {
        DeviceTreeBuilder::<Frozen> {
            proxy: self.proxy,
            route: self.route,
            discover_for: self.discover_for,
            watchdog_for: self.watchdog_for,
            prefetch_metadata: self.prefetch_metadata,
            sorter: None,
            _mode: PhantomData,
        }
    }

    pub fn build(self) -> Result<DeviceTree<Live>, BuildError> {
        let mut tree = DeviceTree::<Live>::open(self.proxy, self.route)?;
        if self.discover_for.is_some() || self.watchdog_for.is_some() {
            tree.discover_with_watchdog(self.discover_for, self.watchdog_for)?;
        }
        if self.prefetch_metadata {
            tree.prefetch_metadata()?;
        }
        Ok(tree)
    }
}

impl DeviceTreeBuilder<Frozen> {
    pub fn sort_by_route(mut self) -> Self {
        self.sorter = Some(|t: &mut DeviceTree<Frozen>| { t.sort_by_route(); Ok(()) });
        self
    }
    pub fn sort_by_device_name(mut self) -> Self {
        self.sorter = Some(|t: &mut DeviceTree<Frozen>| t.sort_by_device_name());
        self
    }
    pub fn sort_by_serial_number(mut self) -> Self {
        self.sorter = Some(|t: &mut DeviceTree<Frozen>| t.sort_by_serial_number());
        self
    }

    pub fn build(self) -> Result<DeviceTree<Frozen>, BuildError> {
        if self.discover_for.is_none() && self.watchdog_for.is_none() {
            return Err(BuildError::MissingDiscoveryDeadline);
        }

        let live = DeviceTreeBuilder::<Live> {
            proxy: self.proxy,
            route: self.route,
            discover_for: self.discover_for,
            watchdog_for: self.watchdog_for,
            prefetch_metadata: self.prefetch_metadata,
            sorter: None,
            _mode: PhantomData,
        }.build()?;

        let mut frozen = live.freeze(); 
        if let Some(sort) = self.sorter {
            sort(&mut frozen)?;
        }
        Ok(frozen)
    }
}
use super::data::{DeviceDataParser, DeviceFullMetadata, Sample};
use super::tio;
use crossbeam::channel::Select;
use proto::DeviceRoute;
use tio::{proto, proxy, util};

use std::collections::{HashMap, VecDeque};

pub struct Device {
    dev_port: proxy::Port,
    parser: DeviceDataParser,
    n_reqs: usize,
    sample_queue: VecDeque<Sample>,
}

impl Device {
    pub fn new(dev_port: proxy::Port) -> Device {
        Device {
            dev_port: dev_port,
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
                if rep.id == 7855 {
                    self.n_reqs -= 1
                } else {
                    return Some(pkt);
                }
            }
            proto::Payload::RpcError(err) => {
                if err.id == 7855 {
                    self.n_reqs -= 1
                } else {
                    return Some(pkt);
                }
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
                            self.dev_port
                                .send(req)
                                .map_err(proxy::RpcError::SendFailed)?;
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
            if !self.sample_queue.is_empty() {
                return Ok(self.sample_queue.pop_front().unwrap());
            }

            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;

            let pkt = self.dev_port.recv().map_err(proxy::RpcError::RecvFailed)?;
            self.process_packet(pkt);
        }
    }

    pub fn try_next(&mut self) -> Result<Option<Sample>, proxy::RpcError> {
        loop {
            if !self.sample_queue.is_empty() {
                return Ok(self.sample_queue.pop_front());
            }

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
                Ok(pkt) => {
                    self.process_packet(pkt);
                }
                Err(proxy::RecvError::WouldBlock) => {
                    break;
                }
                Err(e) => {
                    return Err(proxy::RpcError::RecvFailed(e));
                }
            }
        }

        Ok(self.sample_queue.drain(0..).collect())
    }

    pub fn select_recv<'a>(&'a self, sel: &mut crossbeam::channel::Select<'a>) -> usize {
        self.dev_port.select_recv(sel)
    }

    pub fn raw_rpc(&mut self, name: &str, arg: &[u8]) -> Result<Vec<u8>, proxy::RpcError> {
        if let Err(err) = self.dev_port.send(util::PacketBuilder::make_rpc_request(
            name,
            arg,
            0,
            DeviceRoute::root(),
        )) {
            return Err(proxy::RpcError::SendFailed(err));
        }
        loop {
            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;
            let pkt = match self.dev_port.recv() {
                Ok(packet) => packet,
                Err(e) => return Err(proxy::RpcError::RecvFailed(e)),
            };

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
        &mut self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, proxy::RpcError> {
        let ret = self.raw_rpc(name, &arg.to_request())?;
        if let Ok(val) = RepT::from_reply(&ret) {
            Ok(val)
        } else {
            Err(proxy::RpcError::TypeError)
        }
    }

    /// Action: rpc with no argument which returns nothing
    pub fn action(&mut self, name: &str) -> Result<(), proxy::RpcError> {
        self.rpc(name, ())
    }

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
                        if let proto::RpcErrorCode::InvalidArgs = payload.error {
                            break;
                        }
                    }
                    return Err(err);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(full_reply)
    }

    pub fn get_multi_str(&mut self, name: &str) -> Result<String, proxy::RpcError> {
        let reply_bytes = self.get_multi(name)?;
        let result_string = String::from_utf8_lossy(&reply_bytes).to_string();
        Ok(result_string)
    }
}

pub struct DeviceTree {
    proxy: proxy::Interface,
    probe: proxy::Port,
    devices: Vec<Device>,
    route_map: std::collections::HashMap<DeviceRoute, usize>,
}

impl DeviceTree {
    pub fn open(
        proxy: proxy::Interface,
        route: DeviceRoute,
    ) -> Result<DeviceTree, proxy::PortError> {
        let probe = proxy.subtree_full(route)?;
        Ok(DeviceTree {
            proxy,
            probe,
            devices: vec![],
            route_map: std::collections::HashMap::new(),
        })
    }

    fn get_or_create(
        &mut self,
        route: &DeviceRoute,
    ) -> Result<Option<&mut Device>, proxy::RecvError> {
        if let Some(&idx) = self.route_map.get(route) {
            return Ok(Some(&mut self.devices[idx]));
        }
        match Device::open(&self.proxy, route.clone()) {
            Ok(dev) => {
                let idx = self.devices.len();
                self.route_map.insert(route.clone(), idx);
                self.devices.push(dev);
                Ok(self.devices.last_mut())
            }
            Err(e @ proxy::PortError::FailedNewClientSetup) => {
                eprintln!("device open failed for {route:?}: {:?}", e);
                Ok(None)
            }
            Err(e @ proxy::PortError::RpcTimeoutTooShort)
            | Err(e @ proxy::PortError::RpcTimeoutTooLong) => {
                eprintln!("device open failed for {route:?}: {:?}", e);
                Ok(None)
            }
        }
    }

    pub fn discover_for(&mut self, timeout: std::time::Duration) -> Result<(), proxy::RecvError> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            match self.probe.try_recv() {
                Ok(pkt) => {
                    self.get_or_create(&pkt.routing)?;
                }
                Err(proxy::RecvError::WouldBlock) => std::thread::yield_now(),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn get_all_metadata(
        &mut self,
    ) -> Result<HashMap<DeviceRoute, DeviceFullMetadata>, proxy::RpcError> {
        if self.devices.is_empty() {
            eprintln!("device tree: get_all_metadata called with no devices discovered");
        }

        let mut out = HashMap::new();

        for dev in self.devices.iter_mut() {
            let meta = dev.get_metadata()?;
            out.insert(dev.route(), meta);
        }

        Ok(out)
    }

    pub fn next(&mut self) -> Result<(Sample, DeviceRoute), proxy::RpcError> {
        loop {
            let mut sel = Select::new();
            self.probe.select_recv(&mut sel);
            for dev in &self.devices {
                dev.select_recv(&mut sel);
            }

            let idx = sel.ready();

            if idx == 0 {
                // discover, then continue waiting for a sample.
                let pkt = self.probe.recv().map_err(proxy::RpcError::RecvFailed)?;
                self.get_or_create(&pkt.routing)
                    .map_err(proxy::RpcError::RecvFailed)?;
                continue;
            } else {
                // Device fired: return one sample from that device.
                let dev = &mut self.devices[idx - 1];
                if let Ok(s) = dev.next() {
                    return Ok((s, dev.route()));
                }
            }
        }
    }

    pub fn try_next(&mut self) -> Result<Option<(Sample, DeviceRoute)>, proxy::RpcError> {
        loop {
            match self.probe.try_recv() {
                Ok(pkt) => {
                    self.get_or_create(&pkt.routing)
                        .map_err(proxy::RpcError::RecvFailed)?;
                    continue;
                }
                Err(proxy::RecvError::WouldBlock) => break,
                Err(e) => return Err(proxy::RpcError::RecvFailed(e)),
            }
        }

        if self.devices.is_empty() {
            return Ok(None);
        }

        let mut sel = crossbeam::channel::Select::new();
        for dev in &self.devices {
            dev.select_recv(&mut sel);
        }

        let idx = match sel.try_ready() {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };

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
            self.probe.select_recv(&mut sel);
            for dev in &self.devices {
                dev.select_recv(&mut sel);
            }

            let idx = match sel.try_ready() {
                Ok(i) => i,
                Err(_) => break,
            };

            if idx == 0 {
                while let Ok(pkt) = self.probe.try_recv() {
                    self.get_or_create(&pkt.routing)
                        .map_err(proxy::RpcError::RecvFailed)?;
                }
            } else {
                let dev = &mut self.devices[idx - 1];
                let route = dev.route();
                for s in dev.drain()? {
                    out.push((s, route.clone()));
                }
            }
        }
        Ok(out)
    }
}

use super::data::{DeviceDataParser, DeviceFullMetadata, Sample};
use super::tio;
use proto::DeviceRoute;
use tio::{proto, proxy, util};

use std::collections::VecDeque;

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

    pub fn open(
        proxy: &tio::proxy::Interface,
        route: DeviceRoute,
    ) -> Result<Device, proxy::PortError> {
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
            tio::proto::Payload::RpcReply(rep) => {
                if rep.id == 7855 {
                    self.n_reqs -= 1
                } else {
                    return Some(pkt);
                }
            }
            tio::proto::Payload::RpcError(err) => {
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

    pub fn get_metadata(&mut self) -> Result<DeviceFullMetadata, tio::proxy::RpcError> {
        loop {
            if self.n_reqs == 0 {
                match self.parser.get_metadata() {
                    Ok(full_meta) => return Ok(full_meta),
                    Err(reqs) => {
                        for req in reqs {
                            self.dev_port
                                .send(req)
                                .map_err(tio::proxy::RpcError::SendFailed)?;
                            self.n_reqs += 1;
                        }
                    }
                }
            }
            let pkt = self
                .dev_port
                .recv()
                .map_err(tio::proxy::RpcError::RecvFailed)?;
            self.process_packet(pkt);
        }
    }

    pub fn next(&mut self) -> Result<Sample, tio::proxy::RpcError> {
        loop {
            if !self.sample_queue.is_empty() {
                return Ok(self.sample_queue.pop_front().unwrap());
            }

            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;

            let pkt = self
                .dev_port
                .recv()
                .map_err(tio::proxy::RpcError::RecvFailed)?;
            self.process_packet(pkt);
        }
    }

    pub fn try_next(&mut self) -> Result<Option<Sample>, tio::proxy::RpcError> {
        loop {
            if !self.sample_queue.is_empty() {
                return Ok(self.sample_queue.pop_front());
            }

            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;

            let pkt = match self.dev_port.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => return Ok(None),
                Err(e) => return Err(tio::proxy::RpcError::RecvFailed(e)),
            };
            self.process_packet(pkt);
        }
    }

    pub fn drain(&mut self) -> Result<Vec<Sample>, tio::proxy::RpcError> {
        loop {
            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;
            match self.dev_port.try_recv() {
                Ok(pkt) => {
                    self.process_packet(pkt);
                }
                Err(proxy::RecvError::WouldBlock) => {
                    break;
                }
                Err(e) => {
                    return Err(tio::proxy::RpcError::RecvFailed(e));
                }
            }
        }

        Ok(self.sample_queue.drain(0..).collect())
    }

    pub fn select_recv<'a>(&'a self, sel: &mut crossbeam::channel::Select<'a>) -> usize {
        self.dev_port.select_recv(sel)
    }

    pub fn raw_rpc(&mut self, name: &str, arg: &[u8]) -> Result<Vec<u8>, tio::proxy::RpcError> {
        if let Err(err) = self.dev_port.send(util::PacketBuilder::make_rpc_request(
            name,
            arg,
            0,
            DeviceRoute::root(),
        )) {
            return Err(tio::proxy::RpcError::SendFailed(err));
        }
        loop {
            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;
            let pkt = match self.dev_port.recv() {
                Ok(packet) => packet,
                Err(e) => return Err(tio::proxy::RpcError::RecvFailed(e)),
            };

            if let Some(pkt) = self.process_packet(pkt) {
                match pkt.payload {
                    tio::proto::Payload::RpcReply(rep) => return Ok(rep.reply),
                    tio::proto::Payload::RpcError(err) => {
                        return Err(tio::proxy::RpcError::ExecError(err))
                    }
                    _ => unreachable!("process_packet returned a non-RPC packet to raw_rpc"),
                }
            }
        }
    }

    pub fn rpc<ReqT: tio::util::TioRpcRequestable<ReqT>, RepT: tio::util::TioRpcReplyable<RepT>>(
        &mut self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, tio::proxy::RpcError> {
        let ret = self.raw_rpc(name, &arg.to_request())?;
        if let Ok(val) = RepT::from_reply(&ret) {
            Ok(val)
        } else {
            Err(tio::proxy::RpcError::TypeError)
        }
    }

    /// Action: rpc with no argument which returns nothing
    pub fn action(&mut self, name: &str) -> Result<(), tio::proxy::RpcError> {
        self.rpc(name, ())
    }

    pub fn get<T: tio::util::TioRpcReplyable<T>>(
        &mut self,
        name: &str,
    ) -> Result<T, tio::proxy::RpcError> {
        self.rpc(name, ())
    }

    pub fn get_multi(&mut self, name: &str) -> Result<Vec<u8>, tio::proxy::RpcError> {
        let mut full_reply = vec![];

        for i in 0u16..=65535u16 {
            match self.raw_rpc(&name, &i.to_le_bytes().to_vec()) {
                Ok(mut rep) => full_reply.append(&mut rep),
                Err(err @ proxy::RpcError::ExecError(_)) => {
                    if let proxy::RpcError::ExecError(payload) = &err {
                        if let tio::proto::RpcErrorCode::InvalidArgs = payload.error {
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

    pub fn get_multi_str(&mut self, name: &str) -> Result<String, tio::proxy::RpcError> {
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
        proxy: tio::proxy::Interface,
        route: DeviceRoute,
    ) -> Result<DeviceTree, proxy::PortError> {
        let probe = proxy.subtree_probe(route)?;
        Ok(DeviceTree {
            proxy,
            probe,
            devices: vec![],
            route_map: std::collections::HashMap::new(),
        })
    }

    fn get_or_create(&mut self, route: &DeviceRoute) -> Result<(), proxy::PortError> {
        if self.route_map.contains_key(route) {
            //Ok(self.devices[*self.route_map.get(route).unwrap()].clone())
        } else {
            self.devices.push(Device::open(&self.proxy, route.clone())?);
            self.route_map.insert(route.clone(), self.devices.len() - 1);
            //Ok(self.devices.last().unwrap().clone())
        }
        Ok(())
    }

    pub fn drain_next(&mut self, wait: bool) -> Result<Vec<(Sample, DeviceRoute)>, ()> {
        loop {
            let mut sel = crossbeam::channel::Select::new();
            self.probe.select_recv(&mut sel);
            for dev in self.devices.iter() {
                dev.select_recv(&mut sel);
            }
            let index = if wait {
                sel.ready()
            } else {
                if let Ok(index) = sel.try_ready() {
                    index
                } else {
                    break;
                }
            };

            if index == 0 {
                if let Ok(pkt) = self.probe.try_recv() {
                    self.get_or_create(&pkt.routing).map_err(|_| {})?;
                }
            } else {
                let dev = &mut self.devices[index - 1];
                let samples = dev.drain().map_err(|_| {})?;
                return Ok(samples
                    .iter()
                    .map(|sample| (sample.clone(), dev.route()))
                    .collect());
            }
            if wait {
                break;
            }
        }
        return Ok(vec![]);
    }

    pub fn drain(&mut self, wait: bool) -> Result<Vec<(Sample, DeviceRoute)>, ()> {
        let mut ret = vec![];
        loop {
            let mut chunk = if ret.is_empty() && wait {
                self.drain_next(true)?
            } else {
                if let Ok(samples) = self.drain_next(false) {
                    samples
                } else {
                    break;
                }
            };
            if chunk.is_empty() {
                break;
            }
            ret.append(&mut chunk);
        }
        Ok(ret)
    }
}

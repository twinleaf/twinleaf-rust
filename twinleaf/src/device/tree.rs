use crate::data::{DeviceDataParser, DeviceFullMetadata, Sample};
use crate::tio;
use proto::DeviceRoute;
use tio::{proto, proxy, util};

use std::collections::{HashMap, VecDeque};

pub struct DeviceTree {
    port: proxy::Port,
    root_route: DeviceRoute,
    parsers: HashMap<DeviceRoute, DeviceDataParser>,
    n_reqs: HashMap<DeviceRoute, usize>,
    sample_queue: VecDeque<(Sample, DeviceRoute)>,
    status_queue: VecDeque<(proto::ProxyEventPayload, DeviceRoute)>,
}

impl DeviceTree {
    pub fn new(port: proxy::Port, root_route: DeviceRoute) -> DeviceTree {
        DeviceTree {
            port,
            root_route,
            parsers: HashMap::new(),
            n_reqs: HashMap::new(),
            sample_queue: VecDeque::new(),
            status_queue: VecDeque::new(),
        }
    }

    pub fn open(
        proxy: &tio::proxy::Interface,
        route: DeviceRoute,
    ) -> Result<DeviceTree, proxy::PortError> {
        let port = proxy.subtree_full(route.clone())?;
        Ok(Self::new(port, route))
    }

    fn get_or_create_parser(&mut self, route: &DeviceRoute) -> &mut DeviceDataParser {
        self.parsers
            .entry(route.clone())
            .or_insert_with(|| DeviceDataParser::new(false))
    }

    fn internal_rpcs(&mut self) -> Result<(), proxy::SendError> {
        let routes: Vec<DeviceRoute> = self.parsers.keys().cloned().collect();

        for route in routes {
            let n_reqs = self.n_reqs.get(&route).copied().unwrap_or(0);

            if n_reqs == 0 {
                let parser = self.parsers.get_mut(&route).unwrap();
                let reqs = parser.requests();

                for mut req in reqs {
                    let rel = self
                        .root_route
                        .relative_route(&route)
                        .expect("parser routes must be under root_route");
                    req.routing = rel;

                    self.port.send(req)?;
                    *self.n_reqs.entry(route.clone()).or_insert(0) += 1;
                }
            }
        }

        Ok(())
    }

    fn process_packet(&mut self, pkt: &tio::Packet) {
        let absolute_route = self.root_route.absolute_route(&pkt.routing);

        match &pkt.payload {
            tio::proto::Payload::ProxyEvent(pe) => {
                self.status_queue.push_back((pe.clone(), absolute_route));
                return;
            }
            tio::proto::Payload::RpcReply(rep) => {
                if rep.id == 7855 {
                    if let Some(count) = self.n_reqs.get_mut(&absolute_route) {
                        *count = count.saturating_sub(1);
                    }
                }
            }
            tio::proto::Payload::RpcError(err) => {
                if err.id == 7855 {
                    if let Some(count) = self.n_reqs.get_mut(&absolute_route) {
                        *count = count.saturating_sub(1);
                    }
                }
            }
            _ => {}
        }

        let parser = self.get_or_create_parser(&absolute_route);
        let samples: Vec<Sample> = parser.process_packet(&pkt);

        for sample in samples {
            self.sample_queue
                .push_back((sample, absolute_route.clone()));
        }
    }

    pub fn get_metadata(
        &mut self,
        route: DeviceRoute,
    ) -> Result<DeviceFullMetadata, tio::proxy::RpcError> {
        loop {
            let n_reqs = self.n_reqs.get(&route).copied().unwrap_or(0);

            if n_reqs == 0 {
                let parser = self.get_or_create_parser(&route);
                match parser.get_metadata() {
                    Ok(full_meta) => {
                        return Ok(full_meta);
                    }
                    Err(reqs) => {
                        for mut req in reqs {
                            req.routing = route.clone();
                            self.port
                                .send(req)
                                .map_err(tio::proxy::RpcError::SendFailed)?;
                            *self.n_reqs.entry(route.clone()).or_insert(0) += 1;
                        }
                    }
                }
            }
            let pkt = self.port.recv().map_err(tio::proxy::RpcError::RecvFailed)?;
            self.process_packet(&pkt);
        }
    }

    pub fn next(&mut self) -> Result<(Sample, DeviceRoute), tio::proxy::RpcError> {
        loop {
            if let Some(sample) = self.sample_queue.pop_front() {
                return Ok(sample);
            }
            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;

            let pkt = self.port.recv().map_err(tio::proxy::RpcError::RecvFailed)?;

            self.process_packet(&pkt);
        }
    }

    pub fn try_next(&mut self) -> Result<Option<(Sample, DeviceRoute)>, tio::proxy::RpcError> {
        loop {
            if let Some(sample) = self.sample_queue.pop_front() {
                return Ok(Some(sample));
            }
            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;

            let pkt = match self.port.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => return Ok(None),
                Err(e) => return Err(tio::proxy::RpcError::RecvFailed(e)),
            };

            self.process_packet(&pkt);
        }
    }

    pub fn drain(&mut self) -> Result<Vec<(Sample, DeviceRoute)>, tio::proxy::RpcError> {
        loop {
            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;
            match self.port.try_recv() {
                Ok(pkt) => {
                    self.process_packet(&pkt);
                }
                Err(proxy::RecvError::WouldBlock) => {
                    break;
                }
                Err(e) => {
                    return Err(tio::proxy::RpcError::RecvFailed(e));
                }
            }
        }

        Ok(self.sample_queue.drain(..).collect())
    }

    pub fn try_next_status(&mut self) -> Option<(proto::ProxyEventPayload, DeviceRoute)> {
        self.status_queue.pop_front()
    }

    pub fn drain_status(&mut self) -> Vec<(proto::ProxyEventPayload, DeviceRoute)> {
        self.status_queue.drain(..).collect()
    }

    pub fn raw_rpc(
        &mut self,
        route: DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<Vec<u8>, tio::proxy::RpcError> {
        let mut req = util::PacketBuilder::make_rpc_request(name, arg, 0, DeviceRoute::root());
        let relative_routing = match self.root_route.relative_route(&route) {
            Ok(r) => r,
            Err(_) => {
                req.routing = route;
                return Err(tio::proxy::RpcError::SendFailed(
                    tio::proxy::SendError::InvalidRoute(req),
                ));
            }
        };

        req.routing = relative_routing;

        if let Err(err) = self.port.send(req) {
            return Err(tio::proxy::RpcError::SendFailed(err));
        }

        loop {
            self.internal_rpcs()
                .map_err(tio::proxy::RpcError::SendFailed)?;
            let pkt = match self.port.recv() {
                Ok(packet) => packet,
                Err(e) => return Err(tio::proxy::RpcError::RecvFailed(e)),
            };

            let absolute_route = self.root_route.absolute_route(&pkt.routing);

            if absolute_route == route {
                match &pkt.payload {
                    tio::proto::Payload::RpcReply(rep) if rep.id != 7855 => {
                        return Ok(rep.reply.clone());
                    }
                    tio::proto::Payload::RpcError(err) if err.id != 7855 => {
                        return Err(tio::proxy::RpcError::ExecError(err.clone()));
                    }
                    _ => {}
                }
            }

            self.process_packet(&pkt);
        }
    }

    pub fn rpc<ReqT: tio::util::TioRpcRequestable<ReqT>, RepT: tio::util::TioRpcReplyable<RepT>>(
        &mut self,
        route: DeviceRoute,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, tio::proxy::RpcError> {
        let ret = self.raw_rpc(route, name, &arg.to_request())?;
        if let Ok(val) = RepT::from_reply(&ret) {
            Ok(val)
        } else {
            Err(tio::proxy::RpcError::TypeError)
        }
    }

    pub fn action(&mut self, route: DeviceRoute, name: &str) -> Result<(), tio::proxy::RpcError> {
        self.rpc(route, name, ())
    }

    pub fn get<T: tio::util::TioRpcReplyable<T>>(
        &mut self,
        route: DeviceRoute,
        name: &str,
    ) -> Result<T, tio::proxy::RpcError> {
        self.rpc(route, name, ())
    }

    pub fn get_multi(
        &mut self,
        route: DeviceRoute,
        name: &str,
    ) -> Result<Vec<u8>, tio::proxy::RpcError> {
        let mut full_reply = vec![];

        for i in 0u16..=65535u16 {
            match self.raw_rpc(route.clone(), &name, &i.to_le_bytes().to_vec()) {
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

    pub fn get_multi_str(
        &mut self,
        route: DeviceRoute,
        name: &str,
    ) -> Result<String, tio::proxy::RpcError> {
        let reply_bytes = self.get_multi(route, name)?;
        let result_string = String::from_utf8_lossy(&reply_bytes).to_string();
        Ok(result_string)
    }

    pub fn known_routes(&self) -> Vec<DeviceRoute> {
        self.parsers.keys().cloned().collect()
    }
}

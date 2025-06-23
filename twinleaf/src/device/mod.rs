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

    pub fn open(proxy: &tio::proxy::Interface, route: DeviceRoute) -> Device {
        let port = proxy.device_full(route).unwrap(); // TODO
        Self::new(port)
    }

    fn internal_rpcs(&mut self) {
        if self.n_reqs == 0 {
            let reqs = self.parser.requests();
            for req in reqs {
                self.dev_port.send(req).unwrap();
                self.n_reqs += 1;
            }
        }
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

    pub fn get_metadata(&mut self) -> DeviceFullMetadata {
        loop {
            if self.n_reqs == 0 {
                match self.parser.get_metadata() {
                    Ok(full_meta) => return full_meta,
                    Err(reqs) => {
                        for req in reqs {
                            self.dev_port.send(req).unwrap();
                            self.n_reqs += 1;
                        }
                    }
                }
            }
            let pkt = self.dev_port.recv().unwrap();
            self.process_packet(pkt);
        }
    }

    pub fn next(&mut self) -> Sample {
        loop {
            if !self.sample_queue.is_empty() {
                return self.sample_queue.pop_front().unwrap();
            }

            self.internal_rpcs();
            let pkt = self.dev_port.recv().expect("no packet in blocking recv");
            self.process_packet(pkt);
        }
    }

    pub fn try_next(&mut self) -> Option<Sample> {
        loop {
            if !self.sample_queue.is_empty() {
                return self.sample_queue.pop_front();
            }

            self.internal_rpcs();
            self.process_packet(match self.dev_port.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => {
                    return None;
                }
                _ => {
                    panic!("receive error");
                }
            });
        }
    }

    pub fn drain(&mut self) -> Vec<Sample> {
        loop {
            self.internal_rpcs();
            self.process_packet(match self.dev_port.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => {
                    break;
                }
                _ => {
                    panic!("receive error");
                }
            });
        }

        self.sample_queue.drain(0..).collect()
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
            self.internal_rpcs();
            let pkt = self.dev_port.recv().expect("no packet in blocking recv");
            if let Some(pkt) = self.process_packet(pkt) {
                match pkt.payload {
                    tio::proto::Payload::RpcReply(rep) => return Ok(rep.reply),
                    tio::proto::Payload::RpcError(err) => {
                        return Err(tio::proxy::RpcError::ExecError(err))
                    }
                    _ => panic!("unexpected"),
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

    pub fn get_multi(&mut self, name: &str) -> Vec<u8> {
        let mut full_reply = vec![];

        for i in 0u16..=65535u16 {
            match self.raw_rpc(&name, &i.to_le_bytes().to_vec()) {
                Ok(mut rep) => full_reply.append(&mut rep),
                Err(proxy::RpcError::ExecError(err)) => {
                    if let tio::proto::RpcErrorCode::InvalidArgs = err.error {
                        break;
                    } else {
                        panic!("RPC error");
                    }
                }
                _ => {
                    panic!("RPC error")
                }
            }
        }

        full_reply
    }

    pub fn get_multi_str(&mut self, name: &str) -> String {
        String::from_utf8_lossy(&self.get_multi(name)).to_string()
    }
}

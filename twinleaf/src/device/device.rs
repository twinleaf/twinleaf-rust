use crate::data::{DeviceDataParser, DeviceFullMetadata, Sample};
use crate::tio;
use proto::DeviceRoute;
use tio::{proto, proxy, util};

use std::collections::VecDeque;

/// Device-level events (transport/connection layer).
///
/// These events arrive via both direct serial and tio-proxy connections.
///
/// **Important distinction:**
/// - **Direct serial**: After `Status(SensorDisconnected)`, the channel closes
///   and `next()` returns `Err(ProxyDisconnected)`.
/// - **Via tio-proxy**: After `Status(SensorDisconnected)`, the TCP connection
///   to tio-proxy remains open. `next()` will block indefinitely and
///   `try_next()` will return `None`. The event is your only signal that
///   the sensor is gone.
///
/// For robust handling across both connection types, always check for
/// `Status(SensorDisconnected)` rather than relying on channel errors.
#[derive(Debug, Clone)]
pub enum DeviceEvent {
    /// Connection status changed.
    ///
    /// - `SensorDisconnected`: Sensor connection lost. Through tio-proxy,
    ///   this is your only notification - the channel won't error out.
    /// - `SensorReconnected`: Connection restored, samples will resume.
    ///   The first sample after reconnect will have a `Boundary`.
    /// - `FailedToReconnect`: Gave up reconnection attempts (direct only).
    /// - `FailedToConnect`: Initial connection failed (direct only).
    Status(proto::ProxyStatus),

    /// An RPC with arguments completed from another client.
    /// If you're caching RPC values, invalidate this one.
    RpcInvalidated(proto::RpcMethod),
    /// Heartbeat received from device.
    ///
    /// Heartbeats are sent periodically to indicate
    /// the device is alive. The session_id changes when the device restarts.
    Heartbeat {
        /// Session ID if this is a standard session heartbeat.
        /// None for non-standard/legacy heartbeat formats.
        session_id: Option<proto::identifiers::SessionId>,
    },

    MetadataReady(DeviceFullMetadata),

    NewHash(u32),
}

pub enum DeviceItem {
    Sample(Sample),
    Event(DeviceEvent),
}

pub struct Device {
    dev_port: proxy::Port,
    parser: DeviceDataParser,
    n_reqs: usize,
    metadata_announced: bool,
    sample_queue: VecDeque<Sample>,
    event_queue: VecDeque<DeviceEvent>,
}

impl Device {
    pub fn new(dev_port: proxy::Port) -> Device {
        Device {
            dev_port: dev_port,
            parser: DeviceDataParser::new(false),
            n_reqs: 0,
            metadata_announced: false,
            sample_queue: VecDeque::new(),
            event_queue: VecDeque::new(),
        }
    }

    pub fn open(proxy: &proxy::Interface, route: DeviceRoute) -> Result<Device, proxy::PortError> {
        let port = proxy.device_full(route)?;
        Ok(Self::new(port))
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

    fn process_packet(&mut self, pkt: &tio::Packet) {
        match &pkt.payload {
            tio::proto::Payload::ProxyStatus(ps) => {
                self.event_queue.push_back(DeviceEvent::Status(ps.0));
                return;
            }
            tio::proto::Payload::RpcUpdate(ru) => {
                self.event_queue
                    .push_back(DeviceEvent::RpcInvalidated(ru.0.clone()));
                return;
            }
            tio::proto::Payload::Heartbeat(hb) => {
                let session_id = match hb {
                    tio::proto::HeartbeatPayload::Session(sid) => Some(*sid),
                    tio::proto::HeartbeatPayload::Any(_) => None,
                };
                self.event_queue
                    .push_back(DeviceEvent::Heartbeat { session_id });
            }
            tio::proto::Payload::Settings(set) => {
                match set.name.as_str() {
                    "rpc.hash" => {
                        let hash = u32::from_le_bytes(set.reply.clone().try_into().unwrap());
                        self.event_queue.push_back(DeviceEvent::NewHash(hash));
                    },
                    _ => {},
                }
            }
            tio::proto::Payload::RpcReply(rep) => {
                if rep.id == 7855 {
                    self.n_reqs -= 1
                }
            }
            tio::proto::Payload::RpcError(err) => {
                if err.id == 7855 {
                    self.n_reqs -= 1
                }
            }
            _ => {}
        }
        self.sample_queue
            .append(&mut VecDeque::from(self.parser.process_packet(&pkt)));

        if !self.metadata_announced {
            if let Ok(full_metadata) = self.parser.get_metadata() {
                self.metadata_announced = true;
                self.event_queue
                    .push_back(DeviceEvent::MetadataReady(full_metadata));
            }
        }
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
            self.process_packet(&pkt);
        }
    }

    pub fn next(&mut self) -> Result<Sample, proxy::RpcError> {
        loop {
            if let Some(sample) = self.sample_queue.pop_front() {
                return Ok(sample);
            }

            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;

            let pkt = self.dev_port.recv().map_err(proxy::RpcError::RecvFailed)?;
            self.process_packet(&pkt);
        }
    }

    pub fn try_next(&mut self) -> Result<Option<Sample>, proxy::RpcError> {
        loop {
            if let Some(sample) = self.sample_queue.pop_front() {
                return Ok(Some(sample));
            }

            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;

            let pkt = match self.dev_port.try_recv() {
                Ok(pkt) => pkt,
                Err(proxy::RecvError::WouldBlock) => return Ok(None),
                Err(e) => return Err(proxy::RpcError::RecvFailed(e)),
            };
            self.process_packet(&pkt);
        }
    }

    pub fn drain(&mut self) -> Result<Vec<Sample>, proxy::RpcError> {
        loop {
            self.internal_rpcs().map_err(proxy::RpcError::SendFailed)?;
            match self.dev_port.try_recv() {
                Ok(pkt) => {
                    self.process_packet(&pkt);
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

    pub fn try_next_event(&mut self) -> Option<DeviceEvent> {
        self.event_queue.pop_front()
    }

    pub fn drain_events(&mut self) -> Vec<DeviceEvent> {
        self.event_queue.drain(..).collect()
    }

    pub fn next_item(&mut self) -> Result<DeviceItem, proxy::RpcError> {
        loop {
            if let Some(sample) = self.sample_queue.pop_front() {
                return Ok(DeviceItem::Sample(sample));
            }
            if let Some(event) = self.event_queue.pop_front() {
                return Ok(DeviceItem::Event(event));
            }

            self.internal_rpcs()?;
            let pkt = self.dev_port.recv()?;
            self.process_packet(&pkt);
        }
    }

    pub fn try_next_item(&mut self) -> Result<Option<DeviceItem>, proxy::RpcError> {
        loop {
            if let Some(sample) = self.sample_queue.pop_front() {
                return Ok(Some(DeviceItem::Sample(sample)));
            }

            if let Some(event) = self.event_queue.pop_front() {
                return Ok(Some(DeviceItem::Event(event)));
            }

            self.internal_rpcs()?;
            match self.dev_port.try_recv() {
                Ok(pkt) => self.process_packet(&pkt),
                Err(proxy::RecvError::WouldBlock) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
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

            self.process_packet(&pkt);

            match pkt.payload {
                tio::proto::Payload::RpcReply(rep) if rep.id != 7855 => {
                    return Ok(rep.reply);
                }
                tio::proto::Payload::RpcError(err) if err.id != 7855 => {
                    return Err(proxy::RpcError::ExecError(err));
                }
                _ => {}
            }
        }
    }

    pub fn rpc<ReqT: tio::util::TioRpcRequestable<ReqT>, RepT: tio::util::TioRpcReplyable<RepT>>(
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

    pub fn action(&mut self, name: &str) -> Result<(), proxy::RpcError> {
        self.rpc(name, ())
    }

    pub fn get<T: tio::util::TioRpcReplyable<T>>(
        &mut self,
        name: &str,
    ) -> Result<T, proxy::RpcError> {
        self.rpc(name, ())
    }

    pub fn get_multi(&mut self, name: &str) -> Result<Vec<u8>, proxy::RpcError> {
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

    pub fn get_multi_str(&mut self, name: &str) -> Result<String, proxy::RpcError> {
        let reply_bytes = self.get_multi(name)?;
        let result_string = String::from_utf8_lossy(&reply_bytes).to_string();
        Ok(result_string)
    }
}

#[derive(Debug, Clone)]
pub struct GenericPayload {
    pub packet_type: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum LogLevel {
    CRITICAL = 0,
    ERROR = 1,
    WARNING = 2,
    INFO = 3,
    DEBUG = 4,
}

#[derive(Debug, Clone)]
pub struct LogMessagePayload {
    pub data: u32,
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum RpcMethod {
    Id(u16),
    Name(String),
}

#[derive(Debug, Clone)]
pub struct RpcRequestPayload {
    pub id: u16,
    pub method: RpcMethod,
    pub arg: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RpcReplyPayload {
    pub id: u16,
    pub reply: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum RpcErrorCode {
    NoError,
    Undefined,
    NotFound,
    Timeout,
    NoBufs,
    Unknown(u16),
}

#[derive(Debug, Clone)]
pub struct RpcErrorPayload {
    pub id: u16,
    pub error: RpcErrorCode,
    pub extra: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum HeartbeatPayload {
    Session(u32),
    Any(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct StreamDataPayload {
    pub stream_id: u8,
    pub sample_n: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum Payload {
    LogMessage(LogMessagePayload),
    RpcRequest(RpcRequestPayload),
    RpcReply(RpcReplyPayload),
    RpcError(RpcErrorPayload),
    Heartbeat(HeartbeatPayload),
    StreamData(StreamDataPayload),
    Unknown(GenericPayload),
}

#[derive(Debug, Clone)]
pub struct DeviceRoute {
    route: Vec<u8>,
}

impl DeviceRoute {
    pub fn root() -> DeviceRoute {
        DeviceRoute { route: vec![] }
    }

    pub fn from_bytes(bytes: &[u8]) -> DeviceRoute {
        let mut ret = DeviceRoute {
            route: bytes.to_vec(),
        };
        ret.route.reverse();
        ret
    }

    pub fn from_str(route_str: &str) -> Result<DeviceRoute, ()> {
        let mut ret = DeviceRoute::root();
        let stripped = match route_str.strip_prefix("/") {
            Some(s) => s,
            None => route_str,
        };
        if stripped.len() > 0 {
            for segment in stripped.split('/') {
                match segment.parse() {
                    Ok(n) => {
                        ret.route.push(n);
                    }
                    _ => {
                        return Err(());
                    }
                }
            }
        }
        Ok(ret)
    }

    pub fn len(&self) -> usize {
        self.route.len()
    }

    pub fn iter(&self) -> std::slice::Iter<u8> {
        self.route.iter()
    }

    pub fn serialize(&self, mut rest_of_packet: Vec<u8>) -> Vec<u8> {
        for hop in self.route.iter().rev() {
            rest_of_packet.push(*hop);
        }
        rest_of_packet
    }

    // Returns the relative route from this to other_route (which is absolute.
    // Error if other route is not in the subtree rooted by this route.
    pub fn relative_route(&self, other_route: &DeviceRoute) -> Result<DeviceRoute, ()> {
        if (self.len() <= other_route.len()) && (self.route == other_route.route[0..self.len()]) {
            Ok(DeviceRoute {
                route: other_route.route[self.len()..].to_vec(),
            })
        } else {
            Err(())
        }
    }

    pub fn absolute_route(&self, other_route: &DeviceRoute) -> DeviceRoute {
        let mut route = self.route.clone();
        route.extend_from_slice(&other_route.route[..]);
        DeviceRoute { route }
    }
}

use std::fmt::{Display, Formatter};

impl Display for DeviceRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        if self.route.len() == 0 {
            write!(f, "/").unwrap();
        } else {
            for segment in &self.route {
                write!(f, "/{}", segment).unwrap();
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Packet {
    pub payload: Payload,
    pub routing: DeviceRoute,
    pub ttl: u8,
}

pub static MAX_PACKET_LEN: usize = 512;

#[derive(Debug)]
pub enum Error {
    NeedMore,
    Text(String),
    CRC32(Vec<u8>),
    PacketTooBig(Vec<u8>),
    PacketTooSmall(Vec<u8>),
    InvalidPacketType(Vec<u8>),
    PayloadTooBig(Vec<u8>),
    RoutingTooBig(Vec<u8>),
    PayloadTooSmall(Vec<u8>),
}

impl Packet {
    pub fn deserialize(raw: &[u8]) -> Result<(Packet, usize), Error> {
        if raw.len() < 1 {
            return Err(Error::NeedMore);
        }
        let packet_type = raw[0];
        if let 9 | 10 | 13 = packet_type {
            return Err(Error::InvalidPacketType(raw.to_vec()));
        }
        if raw.len() < 2 {
            return Err(Error::NeedMore);
        }
        let ttl = raw[1] >> 4;
        let routing_len = (raw[1] & 0xF) as usize;
        if routing_len > 8 {
            return Err(Error::RoutingTooBig(raw.to_vec()));
        }
        if raw.len() < 4 {
            return Err(Error::NeedMore);
        }
        let payload_len = u16::from_le_bytes(raw[2..4].try_into().unwrap()) as usize;
        let routing_start = 4 + payload_len;
        let packet_len = routing_start + routing_len;
        if packet_len > 512 {
            return Err(Error::PayloadTooBig(raw.to_vec()));
        }
        if raw.len() < packet_len {
            return Err(Error::NeedMore);
        }
        let payload = match packet_type {
            1 => {
                if routing_start < 9 {
                    return Err(Error::PayloadTooSmall(raw.to_vec()));
                }
                Payload::LogMessage(LogMessagePayload {
                    data: u32::from_le_bytes(raw[4..8].try_into().unwrap()),
                    level: match raw[8] {
                        0 => LogLevel::CRITICAL,
                        1 => LogLevel::ERROR,
                        2 => LogLevel::WARNING,
                        3 => LogLevel::INFO,
                        _ => LogLevel::DEBUG,
                    },
                    message: String::from_utf8_lossy(&raw[9..routing_start]).to_string(),
                })
            }
            2 => {
                if routing_start < 8 {
                    return Err(Error::PayloadTooSmall(raw.to_vec()));
                }
                let mut arg_start: usize = 8;
                let method = u16::from_le_bytes(raw[6..8].try_into().unwrap());
                let method = if (method & 0x8000) != 0 {
                    arg_start += (method & 0x7FFF) as usize;
                    RpcMethod::Name(String::from_utf8_lossy(&raw[8..arg_start]).to_string())
                } else {
                    RpcMethod::Id(method)
                };
                Payload::RpcRequest(RpcRequestPayload {
                    id: u16::from_le_bytes(raw[4..6].try_into().unwrap()),
                    method: method,
                    arg: raw[arg_start..routing_start].to_vec(),
                })
            }
            3 => {
                if routing_start < 6 {
                    return Err(Error::PayloadTooSmall(raw.to_vec()));
                }
                Payload::RpcReply(RpcReplyPayload {
                    id: u16::from_le_bytes(raw[4..6].try_into().unwrap()),
                    reply: raw[6..routing_start].to_vec(),
                })
            }
            4 => {
                if routing_start < 8 {
                    return Err(Error::PayloadTooSmall(raw.to_vec()));
                }
                Payload::RpcError(RpcErrorPayload {
                    id: u16::from_le_bytes(raw[4..6].try_into().unwrap()),
                    error: match u16::from_le_bytes(raw[6..8].try_into().unwrap()) {
                        0 => RpcErrorCode::NoError,
                        1 => RpcErrorCode::Undefined,
                        2 => RpcErrorCode::NotFound,
                        8 => RpcErrorCode::Timeout,
                        16 => RpcErrorCode::NoBufs,
                        code => RpcErrorCode::Unknown(code),
                    },
                    extra: raw[8..routing_start].to_vec(),
                })
            }
            5 => {
                let payload = raw[4..routing_start].to_vec();
                if payload.len() == 4 {
                    let session = u32::from_le_bytes(payload[..].try_into().unwrap());
                    Payload::Heartbeat(HeartbeatPayload::Session(session))
                } else {
                    Payload::Heartbeat(HeartbeatPayload::Any(payload))
                }
            }
            ptype if ptype >= 128 => {
                if routing_start < 9 {
                    return Err(Error::PayloadTooSmall(raw.to_vec()));
                }
                Payload::StreamData(StreamDataPayload {
                    stream_id: ptype - 128,
                    sample_n: u32::from_le_bytes(raw[4..8].try_into().unwrap()),
                    data: raw[8..routing_start].to_vec(),
                })
            }
            ptype => Payload::Unknown(GenericPayload {
                packet_type: ptype,
                payload: raw[4..routing_start].to_vec(),
            }),
        };

        Ok((
            Packet {
                payload: payload,
                routing: DeviceRoute::from_bytes(&raw[routing_start..packet_len]),
                ttl: ttl,
            },
            packet_len,
        ))
    }

    fn prepare_header(&self, packet_type: u8, payload_len: usize) -> Vec<u8> {
        vec![
            packet_type,
            (self.ttl & 0xF) << 4 | (self.routing.len() as u8),
            (payload_len & 0xFF) as u8,
            ((payload_len & 0xFF00) >> 8) as u8,
        ]
    }

    pub fn serialize(&self) -> Vec<u8> {
        match &self.payload {
            Payload::LogMessage(log) => {
                let mut ret = self.prepare_header(1, 5 + log.message.len());
                ret.extend(log.data.to_le_bytes());
                ret.push(match log.level {
                    LogLevel::CRITICAL => 0,
                    LogLevel::ERROR => 1,
                    LogLevel::WARNING => 2,
                    LogLevel::INFO => 3,
                    LogLevel::DEBUG => 4,
                });
                ret.extend(log.message.as_bytes());
                self.routing.serialize(ret)
            }
            Payload::RpcRequest(req) => {
                let mut ret = self.prepare_header(
                    2,
                    4 + req.arg.len()
                        + if let RpcMethod::Name(name) = &req.method {
                            name.len()
                        } else {
                            0
                        },
                );
                ret.extend(req.id.to_le_bytes());
                match &req.method {
                    RpcMethod::Id(id) => {
                        ret.extend(id.to_le_bytes());
                    }
                    RpcMethod::Name(name) => {
                        ret.extend(((name.len() | 0x8000) as u16).to_le_bytes());
                        ret.extend(name.as_bytes());
                    }
                }
                ret.extend(&req.arg);
                self.routing.serialize(ret)
            }
            Payload::RpcReply(rep) => {
                let mut ret = self.prepare_header(3, 2 + rep.reply.len());
                ret.extend(rep.id.to_le_bytes());
                ret.extend(&rep.reply);
                self.routing.serialize(ret)
            }
            Payload::RpcError(err) => {
                let mut ret = self.prepare_header(4, 4 + err.extra.len());
                ret.extend(err.id.to_le_bytes());
                let code: u16 = match err.error {
                    RpcErrorCode::NoError => 0,
                    RpcErrorCode::Undefined => 1,
                    RpcErrorCode::NotFound => 2,
                    RpcErrorCode::Timeout => 8,
                    RpcErrorCode::NoBufs => 16,
                    RpcErrorCode::Unknown(code) => code,
                };
                ret.extend(code.to_le_bytes());
                ret.extend(&err.extra);
                self.routing.serialize(ret)
            }
            Payload::Heartbeat(payload) => {
                let raw_payload = match payload {
                    HeartbeatPayload::Session(session) => session.to_le_bytes().to_vec(),
                    HeartbeatPayload::Any(raw) => raw.clone(),
                };
                let mut ret = self.prepare_header(5, raw_payload.len());
                ret.extend(raw_payload);
                self.routing.serialize(ret)
            }
            Payload::StreamData(sample) => {
                let mut ret = self.prepare_header(128 + sample.stream_id, 4 + sample.data.len());
                ret.extend(sample.sample_n.to_le_bytes());
                ret.extend(&sample.data);
                self.routing.serialize(ret)
            }
            Payload::Unknown(payload) => {
                let mut ret = self.prepare_header(payload.packet_type, payload.payload.len());
                ret.extend(&payload.payload);
                self.routing.serialize(ret)
            } //            _ => {
              //                vec![]
              //            }
        }
    }

    pub fn rpc(name: String, arg: &[u8]) -> Packet {
        Packet {
            payload: Payload::RpcRequest(RpcRequestPayload {
                id: 0,
                method: RpcMethod::Name(name),
                arg: arg.to_vec(),
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    pub fn make_hb(payload: Option<Vec<u8>>) -> Packet {
        Packet {
            payload: Payload::Heartbeat(HeartbeatPayload::Any(match payload {
                Some(v) => v,
                None => {
                    vec![]
                }
            })),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    pub fn make_rpc_error(id: u16, error: RpcErrorCode) -> Packet {
        Packet {
            payload: Payload::RpcError(RpcErrorPayload {
                id: id,
                error: error,
                extra: vec![],
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }
}

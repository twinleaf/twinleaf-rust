#[derive(Debug)]
pub struct GenericPayload {
    pub packet_type: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum RpcMethod {
    Id(u16),
    Name(String),
}

#[derive(Debug)]
pub struct RpcRequestPayload {
    pub id: u16,
    pub method: RpcMethod,
    pub arg: Vec<u8>,
}

#[derive(Debug)]
pub struct RpcReplyPayload {
    pub id: u16,
    pub reply: Vec<u8>,
}

#[derive(Debug)]
pub enum RpcErrorCode {
    NoError,
    Undefined,
    NotFound,
    Unknown(u16),
}

#[derive(Debug)]
pub struct RpcErrorPayload {
    pub id: u16,
    pub error: RpcErrorCode,
    pub extra: Vec<u8>,
}

#[derive(Debug)]
pub enum HeartbeatPayload {
    Session(u32),
    Any(Vec<u8>),
}

#[derive(Debug)]
pub struct StreamDataPayload {
    pub stream_id: u8,
    pub sample_n: u32,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub enum Payload {
    RpcRequest(RpcRequestPayload),
    RpcReply(RpcReplyPayload),
    RpcError(RpcErrorPayload),
    Heartbeat(HeartbeatPayload),
    StreamData(StreamDataPayload),
    Unknown(GenericPayload),
}

#[derive(Debug)]
pub struct Packet {
    pub payload: Payload,
    pub routing: Vec<u8>,
    pub ttl: u8,
}

#[derive(Debug)]
pub enum Error {
    NeedMore,
    InvalidPacketType,
    PayloadTooBig,
    RoutingTooBig,
    PayloadTooSmall,
}

impl Packet {
    pub fn deserialize(raw: &[u8]) -> Result<(Packet, usize), Error> {
        if raw.len() < 1 {
            return Err(Error::NeedMore);
        }
        let packet_type = raw[0];
        if let 9 | 10 | 13 = packet_type {
            return Err(Error::InvalidPacketType);
        }
        if raw.len() < 2 {
            return Err(Error::NeedMore);
        }
        let ttl = raw[1] >> 4;
        let routing_len = (raw[1] & 0xF) as usize;
        if routing_len > 8 {
            return Err(Error::RoutingTooBig);
        }
        if raw.len() < 4 {
            return Err(Error::NeedMore);
        }
        let payload_len = u16::from_le_bytes(raw[2..4].try_into().unwrap()) as usize;
        let routing_start = 4 + payload_len;
        let packet_len = routing_start + routing_len;
        if packet_len > 512 {
            return Err(Error::PayloadTooBig);
        }
        if raw.len() < packet_len {
            return Err(Error::NeedMore);
        }
        let payload = match packet_type {
            3 => {
                if routing_start < 6 {
                    return Err(Error::PayloadTooSmall);
                }
                Payload::RpcReply(RpcReplyPayload {
                    id: u16::from_le_bytes(raw[4..6].try_into().unwrap()),
                    reply: raw[6..routing_start].to_vec(),
                })
            }
            ptype if ptype >= 128 => {
                if routing_start < 9 {
                    return Err(Error::PayloadTooSmall);
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
                routing: raw[routing_start..packet_len].to_vec(),
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
            Payload::RpcRequest(req) => {
                let mut ret = self.prepare_header(
                    2,
                    4 + if let RpcMethod::Name(name) = &req.method {
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
                ret
            }
            Payload::RpcReply(rep) => {
                let mut ret = self.prepare_header(3, 2 + rep.reply.len());
                ret.extend(rep.id.to_le_bytes());
                ret.extend(&rep.reply);
                ret
            }
            Payload::Heartbeat(payload) => {
                // TODO: payload
                vec![5u8, 0, 0, 0]
            }
            Payload::StreamData(sample) => {
                let mut ret = self.prepare_header(128 + sample.stream_id, 4 + sample.data.len());
                ret.extend(sample.sample_n.to_le_bytes());
                ret.extend(&sample.data);
                ret
            }
            Payload::Unknown(payload) => {
                let mut ret = self.prepare_header(payload.packet_type, payload.payload.len());
                ret.extend(&payload.payload);
                ret
            }
            _ => {
                vec![]
            }
        }
    }

    pub fn rpc(name: String, arg: &[u8]) -> Packet {
        Packet {
            payload: Payload::RpcRequest(RpcRequestPayload {
                id: 0,
                method: RpcMethod::Name(name),
                arg: arg.to_vec(),
            }),
            routing: vec![],
            ttl: 0,
        }
    }

    pub fn make_hb(payload: Option<Vec<u8>>) -> Packet {
        // TODO
        Packet {
            payload: Payload::Heartbeat(HeartbeatPayload::Any(vec![])),
            routing: vec![],
            ttl: 0,
        }
    }
}

pub struct UnknownPayload {
    pub msg_id: u8,
    pub payload: Vec<u8>,
}

pub struct StreamPayload {
    pub stream_id: u8,
    pub sample_n: u32,
    pub payload: Vec<u8>,
}

pub enum RpcMethod {
    Id(u16),
    Name(String),
}

pub struct RpcReqPayload {
    pub id: u16,
    pub method: RpcMethod,
    pub arg: Vec<u8>,
}

pub struct RpcRepPayload {
    pub id: u16,
    pub reply: Vec<u8>,
}

pub enum TioData {
    Unknown(UnknownPayload),
    RpcRequest(RpcReqPayload),
    RpcReply(RpcRepPayload),
    Stream(StreamPayload),
}

pub struct TioPacket {
    pub payload: TioData,
    pub routing: Vec<u8>,
}

#[derive(Debug)]
pub enum TioError {
    TooSmall,
    // TODO: other errors
    Invalid,
}

impl TioPacket {
    pub fn deserialize(raw: &[u8]) -> Result<TioPacket, TioError> {
        if raw.len() < 4 {
            return Err(TioError::TooSmall);
        }
        let payload_len = u16::from_le_bytes(raw[2..4].try_into().expect("")) as usize;
        let routing_len = (raw[1] & 0xF) as usize;
        let routing_start = 4 + payload_len;
        let packet_len = routing_start + routing_len;
        // TODO: verify fields
        if raw.len() < packet_len {
            return Err(TioError::TooSmall);
        }
        let routing = &raw[routing_start..packet_len];
        match raw[0] {
            3 => Ok(TioPacket {
                payload: TioData::RpcReply(RpcRepPayload {
                    id: u16::from_le_bytes(raw[2..4].try_into().expect("")),
                    reply: raw[6..routing_start].to_vec(),
                }),
                routing: routing.to_vec(),
            }),
            id if id >= 128 => Ok(TioPacket {
                payload: TioData::Stream(StreamPayload {
                    stream_id: id - 128,
                    sample_n: u32::from_le_bytes(raw[4..8].try_into().expect("")),
                    payload: raw[8..routing_start].to_vec(),
                }),
                routing: routing.to_vec(),
            }),
            id => Ok(TioPacket {
                payload: TioData::Unknown(UnknownPayload {
                    msg_id: id,
                    payload: raw[4..routing_start].to_vec(),
                }),
                routing: routing.to_vec(),
            }),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        if let TioData::RpcRequest(req) = &self.payload {
            let mut ret = vec![2u8];
            ret.push(0); // TODO: routing
            let plen = 4
                + req.arg.len()
                + if let RpcMethod::Name(name) = &req.method {
                    name.len()
                } else {
                    0
                };
            ret.extend((plen as u16).to_le_bytes());
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
        } else {
            vec![]
        }
    }

    pub fn make_rpc(name: String, arg: &[u8]) -> TioPacket {
        TioPacket {
            payload: TioData::RpcRequest(RpcReqPayload {
                id: 0,
                method: RpcMethod::Name(name),
                arg: arg.to_vec(),
            }),
            routing: vec![],
        }
    }

    /*
    fn set_routing(mut self, routing: &[u8]) -> TioPacket {
        let _ = routing; // TODO
        self
    }
    */
}

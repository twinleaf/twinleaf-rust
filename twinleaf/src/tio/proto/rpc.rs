use super::{too_small, Error, TioPktHdr, TioPktType, TIO_PACKET_MAX_PAYLOAD_SIZE};
use num_enum::{FromPrimitive, IntoPrimitive};

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

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum RpcErrorCode {
    NoError = 0,
    Undefined = 1,
    NotFound = 2,
    MalformedRequest = 3,
    WrongSizeArgs = 4,
    InvalidArgs = 5,
    ReadOnly = 6,
    WriteOnly = 7,
    Timeout = 8,
    Busy = 9,
    WrongDeviceState = 10,
    LoadFailed = 11,
    LoadRpcFailed = 12,
    SaveFailed = 13,
    SaveWriteFailed = 14,
    Internal = 15,
    OutOfMemory = 16,
    OutOfRange = 17,
    #[num_enum(catch_all)]
    Unknown(u16),
}

#[derive(Debug, Clone)]
pub struct RpcErrorPayload {
    pub id: u16,
    pub error: RpcErrorCode,
    pub extra: Vec<u8>,
}

impl RpcRequestPayload {
    pub fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<RpcRequestPayload, Error> {
        if raw.len() < 4 {
            return Err(too_small(full_data));
        }
        let id = u16::from_le_bytes([raw[0], raw[1]]);
        let method = u16::from_le_bytes([raw[2], raw[3]]);
        let (method, arg_start) = if (method & 0x8000) != 0 {
            let arg_start = (method & 0x7FFF) as usize + 4;
            if arg_start > TIO_PACKET_MAX_PAYLOAD_SIZE {
                return Err(Error::InvalidPayload(full_data.to_vec()));
            }
            if raw.len() < arg_start {
                return Err(too_small(full_data));
            }
            (
                RpcMethod::Name(String::from_utf8_lossy(&raw[4..arg_start]).to_string()),
                arg_start,
            )
        } else {
            (RpcMethod::Id(method), 4)
        };
        Ok(RpcRequestPayload {
            id: id,
            method: method,
            arg: raw[arg_start..].to_vec(),
        })
    }
    pub fn serialize(&self) -> Result<Vec<u8>, ()> {
        let method_name_len = if let RpcMethod::Name(method_name) = &self.method {
            method_name.as_bytes().len() as u16
        } else {
            0
        };
        let payload_size = 4 + (method_name_len as usize) + self.arg.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::RpcReq, 0, payload_size as u16);
        ret.extend(self.id.to_le_bytes());
        match &self.method {
            RpcMethod::Id(method) => {
                ret.extend(method.to_le_bytes());
            }
            RpcMethod::Name(method) => {
                ret.extend((method_name_len | 0x8000).to_le_bytes());
                ret.extend(method.as_bytes())
            }
        }
        ret.extend_from_slice(&self.arg);
        Ok(ret)
    }
}

impl RpcReplyPayload {
    pub fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<RpcReplyPayload, Error> {
        if raw.len() < 2 {
            return Err(too_small(full_data));
        }
        let id = u16::from_le_bytes([raw[0], raw[1]]);
        Ok(RpcReplyPayload {
            id: id,
            reply: raw[2..].to_vec(),
        })
    }
    pub fn serialize(&self) -> Result<Vec<u8>, ()> {
        let payload_size = 2 + self.reply.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::RpcRep, 0, payload_size as u16);
        ret.extend(self.id.to_le_bytes());
        ret.extend_from_slice(&self.reply);
        Ok(ret)
    }
}

impl RpcErrorPayload {
    pub fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<RpcErrorPayload, Error> {
        if raw.len() < 4 {
            return Err(too_small(full_data));
        }
        Ok(RpcErrorPayload {
            id: u16::from_le_bytes([raw[0], raw[1]]),
            error: RpcErrorCode::from(u16::from_le_bytes([raw[2], raw[3]])),
            extra: raw[4..].to_vec(),
        })
    }
    pub fn serialize(&self) -> Result<Vec<u8>, ()> {
        let payload_size = 4 + self.extra.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::RpcError, 0, payload_size as u16);
        ret.extend(self.id.to_le_bytes());
        ret.extend(u16::from(self.error).to_le_bytes());
        ret.extend_from_slice(&self.extra);
        Ok(ret)
    }
}

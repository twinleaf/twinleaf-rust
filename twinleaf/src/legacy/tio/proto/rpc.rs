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

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[repr(u16)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum RpcErrorCode {
    #[error("no error")]
    NoError = 0,
    #[error("undefined error")]
    Undefined = 1,
    #[error("RPC not found")]
    NotFound = 2,
    #[error("malformed request")]
    MalformedRequest = 3,
    #[error("wrong size args")]
    WrongSizeArgs = 4,
    #[error("invalid arguments")]
    InvalidArgs = 5,
    #[error("read-only")]
    ReadOnly = 6,
    #[error("write-only")]
    WriteOnly = 7,
    #[error("timeout")]
    Timeout = 8,
    #[error("device busy")]
    Busy = 9,
    #[error("wrong device state")]
    WrongDeviceState = 10,
    #[error("load failed")]
    LoadFailed = 11,
    #[error("load RPC failed")]
    LoadRpcFailed = 12,
    #[error("save failed")]
    SaveFailed = 13,
    #[error("save write failed")]
    SaveWriteFailed = 14,
    #[error("internal error")]
    Internal = 15,
    #[error("out of memory")]
    OutOfMemory = 16,
    #[error("out of range")]
    OutOfRange = 17,
    #[num_enum(catch_all)]
    #[error("unknown error code {0}")]
    Unknown(u16),
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{error}")]
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

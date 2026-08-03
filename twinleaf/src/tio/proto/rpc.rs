use super::{DecodeError, EncodeError};
use num_enum::{FromPrimitive, IntoPrimitive};

mod meta;
mod typed;
mod value;

pub use meta::{RpcAccess, RpcMeta, RpcMetaFlags, RpcStringLen, RpcValueType};
pub use typed::{RpcArgs, RpcDecodeError, RpcReply, RpcReplyFixedSize};
pub use value::{RpcValue, RpcValueDecodeError, RpcValueEncodeError};

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
    pub fn deserialize(raw: &[u8]) -> Result<RpcRequestPayload, DecodeError> {
        if raw.len() < 4 {
            return Err(DecodeError::PayloadTooShort {
                expected: 4,
                actual: raw.len(),
            });
        }
        let id = u16::from_le_bytes([raw[0], raw[1]]);
        let method = u16::from_le_bytes([raw[2], raw[3]]);
        let (method, arg_start) = if (method & 0x8000) != 0 {
            let arg_start = (method & 0x7FFF) as usize + 4;
            if raw.len() < arg_start {
                return Err(DecodeError::PayloadTooShort {
                    expected: arg_start,
                    actual: raw.len(),
                });
            }
            (
                RpcMethod::Name(String::from_utf8_lossy(&raw[4..arg_start]).to_string()),
                arg_start,
            )
        } else {
            (RpcMethod::Id(method), 4)
        };
        Ok(RpcRequestPayload {
            id,
            method,
            arg: raw[arg_start..].to_vec(),
        })
    }
    pub(super) fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        let method_name_len = if let RpcMethod::Name(method_name) = &self.method {
            method_name.len() as u16
        } else {
            0
        };
        output.extend(self.id.to_le_bytes());
        match &self.method {
            RpcMethod::Id(method) => {
                output.extend(method.to_le_bytes());
            }
            RpcMethod::Name(method) => {
                output.extend((method_name_len | 0x8000).to_le_bytes());
                output.extend(method.as_bytes());
            }
        }
        output.extend_from_slice(&self.arg);
        Ok(())
    }
}

impl RpcReplyPayload {
    pub fn deserialize(raw: &[u8]) -> Result<RpcReplyPayload, DecodeError> {
        if raw.len() < 2 {
            return Err(DecodeError::PayloadTooShort {
                expected: 2,
                actual: raw.len(),
            });
        }
        let id = u16::from_le_bytes([raw[0], raw[1]]);
        Ok(RpcReplyPayload {
            id,
            reply: raw[2..].to_vec(),
        })
    }
    pub(super) fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        output.extend(self.id.to_le_bytes());
        output.extend_from_slice(&self.reply);
        Ok(())
    }
}

impl RpcErrorPayload {
    pub fn deserialize(raw: &[u8]) -> Result<RpcErrorPayload, DecodeError> {
        if raw.len() < 4 {
            return Err(DecodeError::PayloadTooShort {
                expected: 4,
                actual: raw.len(),
            });
        }
        Ok(RpcErrorPayload {
            id: u16::from_le_bytes([raw[0], raw[1]]),
            error: RpcErrorCode::from(u16::from_le_bytes([raw[2], raw[3]])),
            extra: raw[4..].to_vec(),
        })
    }
    pub(super) fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        output.extend(self.id.to_le_bytes());
        output.extend(u16::from(self.error).to_le_bytes());
        output.extend_from_slice(&self.extra);
        Ok(())
    }
}

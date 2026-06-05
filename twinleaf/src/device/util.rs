use crate::device::rpc::{
    DecodeError, EncodeError, RpcDescriptor, RpcMeta, RpcValue, RpcValueType,
};
use crate::tio::proxy;

pub fn load_rpc_specs(device: &proxy::Port) -> Result<Vec<RpcDescriptor>, proxy::RpcError> {
    let nrpcs: u16 = device.get("rpc.listinfo")?;
    let mut specs = Vec::with_capacity(nrpcs as usize);

    for id in 0..nrpcs {
        let (meta, name): (u16, String) = device.rpc("rpc.listinfo", id)?;
        specs.push(RpcDescriptor::from_meta(meta, name));
    }

    Ok(specs)
}

/// When `meta` is `None` (rpc.info failed, or the RPC isn't in any registry)
/// or when the parsed kind is `Raw` (meta == 0, common for hidden RPCs),
/// fall back to `String` so a typed arg still gets sent as bytes.
pub fn resolve_arg_type(meta: Option<u16>, _name: &str) -> RpcValueType {
    let kind = meta
        .map(|m| RpcMeta::from_bits(m).kind())
        .unwrap_or(RpcValueType::String { max_len: None });
    match kind {
        RpcValueType::Raw { .. } => RpcValueType::String { max_len: None },
        other => other,
    }
}

pub fn rpc_encode_arg(input: &str, kind: &RpcValueType) -> Result<Vec<u8>, EncodeError> {
    match kind {
        RpcValueType::Unit => {
            if input.is_empty() {
                Ok(Vec::new())
            } else {
                Err(EncodeError::NotEncodableForKind("unit"))
            }
        }

        RpcValueType::String { max_len } => {
            if let Some(max) = max_len {
                if input.len() > *max as usize {
                    return Err(EncodeError::StringTooLong {
                        max: *max,
                        actual: input.len(),
                    });
                }
            }
            Ok(input.as_bytes().to_vec())
        }

        RpcValueType::Int {
            signed: false,
            size,
        } => match *size {
            1 => Ok(input.parse::<u8>()?.to_le_bytes().to_vec()),
            2 => Ok(input.parse::<u16>()?.to_le_bytes().to_vec()),
            4 => Ok(input.parse::<u32>()?.to_le_bytes().to_vec()),
            8 => Ok(input.parse::<u64>()?.to_le_bytes().to_vec()),
            s => Err(EncodeError::UnsupportedIntSize(s)),
        },

        RpcValueType::Int { signed: true, size } => match *size {
            1 => Ok(input.parse::<i8>()?.to_le_bytes().to_vec()),
            2 => Ok(input.parse::<i16>()?.to_le_bytes().to_vec()),
            4 => Ok(input.parse::<i32>()?.to_le_bytes().to_vec()),
            8 => Ok(input.parse::<i64>()?.to_le_bytes().to_vec()),
            s => Err(EncodeError::UnsupportedIntSize(s)),
        },

        RpcValueType::Float { size } => match *size {
            4 => Ok(input.parse::<f32>()?.to_le_bytes().to_vec()),
            8 => Ok(input.parse::<f64>()?.to_le_bytes().to_vec()),
            s => Err(EncodeError::UnsupportedFloatSize(s)),
        },

        RpcValueType::Raw { .. } => Ok(Vec::new()),
    }
}

pub fn rpc_decode_reply(reply: &[u8], kind: &RpcValueType) -> Result<RpcValue, DecodeError> {
    match kind {
        RpcValueType::Unit => Ok(RpcValue::Unit),

        RpcValueType::String { .. } => {
            match std::str::from_utf8(reply) {
                Ok(s) => Ok(RpcValue::Str(s.to_owned())),
                Err(_) => Ok(RpcValue::Bytes(reply.to_vec())), // keep bytes if not UTF-8
            }
        }

        RpcValueType::Int {
            signed: false,
            size,
        } => match *size {
            1 => {
                if reply.len() < 1 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 1,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::U64(u8::from_le_bytes([reply[0]]) as u64))
            }
            2 => {
                if reply.len() < 2 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 2,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::U64(
                    u16::from_le_bytes(reply[0..2].try_into().unwrap()) as u64,
                ))
            }
            4 => {
                if reply.len() < 4 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 4,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::U64(
                    u32::from_le_bytes(reply[0..4].try_into().unwrap()) as u64,
                ))
            }
            8 => {
                if reply.len() < 8 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 8,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::U64(u64::from_le_bytes(
                    reply[0..8].try_into().unwrap(),
                )))
            }
            s => Err(DecodeError::UnsupportedIntSize(s)),
        },

        RpcValueType::Int { signed: true, size } => match *size {
            1 => {
                if reply.len() < 1 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 1,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::I64(i8::from_le_bytes([reply[0]]) as i64))
            }
            2 => {
                if reply.len() < 2 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 2,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::I64(
                    i16::from_le_bytes(reply[0..2].try_into().unwrap()) as i64,
                ))
            }
            4 => {
                if reply.len() < 4 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 4,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::I64(
                    i32::from_le_bytes(reply[0..4].try_into().unwrap()) as i64,
                ))
            }
            8 => {
                if reply.len() < 8 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 8,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::I64(i64::from_le_bytes(
                    reply[0..8].try_into().unwrap(),
                )))
            }
            s => Err(DecodeError::UnsupportedIntSize(s)),
        },

        RpcValueType::Float { size } => match *size {
            4 => {
                if reply.len() < 4 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 4,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::F64(
                    f32::from_le_bytes(reply[0..4].try_into().unwrap()) as f64,
                ))
            }
            8 => {
                if reply.len() < 8 {
                    return Err(DecodeError::InsufficientBytes {
                        expected: 8,
                        got: reply.len(),
                    });
                }
                Ok(RpcValue::F64(f64::from_le_bytes(
                    reply[0..8].try_into().unwrap(),
                )))
            }
            s => Err(DecodeError::UnsupportedFloatSize(s)),
        },

        RpcValueType::Raw { .. } => Ok(RpcValue::Bytes(reply.to_vec())),
    }
}

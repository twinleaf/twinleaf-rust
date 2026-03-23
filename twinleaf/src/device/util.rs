use crate::device::rpc::{DecodeError, EncodeError, RpcDescriptor, RpcValue, RpcValueType};
use crate::tio::proxy;

pub fn load_rpc_specs(device: &proxy::Port) -> Result<Vec<RpcDescriptor>, proxy::RpcError> {
    let nrpcs: u16 = device.get("rpc.listinfo")?;
    let mut specs = Vec::with_capacity(nrpcs as usize);

    for id in 0..nrpcs {
        let (meta, name): (u16, String) = device.rpc("rpc.listinfo", id)?;
        specs.push(parse_rpc_spec(meta, name));
    }

    Ok(specs)
}

pub fn parse_rpc_spec(meta: u16, name: String) -> RpcDescriptor {
    let data_type = meta & 0x000F; // low 4 bits
    let data_size = ((meta >> 4) & 0x000F) as u8; // next 4 bits

    let readable = (meta & 0x0100) != 0;
    let writable = (meta & 0x0200) != 0;
    let persistent = (meta & 0x0400) != 0;
    let unknown = meta == 0;

    let data_kind = if unknown {
        RpcValueType::Raw { meta }
    } else {
        match data_type {
            0 => {
                // unsigned integer
                match data_size {
                    0 => RpcValueType::Unit,
                    1 | 2 | 4 | 8 => RpcValueType::Int {
                        signed: false,
                        size: data_size,
                    },
                    _ => RpcValueType::Raw { meta },
                }
            }
            1 => {
                // signed integer
                match data_size {
                    0 => RpcValueType::Unit,
                    1 | 2 | 4 | 8 => RpcValueType::Int {
                        signed: true,
                        size: data_size,
                    },
                    _ => RpcValueType::Raw { meta },
                }
            }
            2 => {
                // float
                match data_size {
                    4 | 8 => RpcValueType::Float { size: data_size },
                    0 => RpcValueType::Unit,
                    _ => RpcValueType::Raw { meta },
                }
            }
            3 => {
                // string; optional length from size field if nonzero
                let max_len = if data_size == 0 {
                    None
                } else {
                    Some(data_size as u16)
                };
                RpcValueType::String { max_len }
            }
            _ => RpcValueType::Raw { meta },
        }
    };

    let segments = name.split('.').map(|s| s.to_string()).collect();

    RpcDescriptor {
        full_name: name,
        segments,
        data_kind,
        readable,
        writable,
        persistent,
        meta_raw: meta,
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

pub fn format_rpc_value_for_cli(v: &RpcValue, kind: &RpcValueType) -> String {
    match (v, kind) {
        (RpcValue::Unit, _) => "OK".to_string(),

        (RpcValue::Str(s), _) => format!("\"{}\" {:?}", s, s.as_bytes()),

        (RpcValue::U64(n), RpcValueType::Int { signed: false, .. }) => format!("{}", n),
        (RpcValue::I64(n), RpcValueType::Int { signed: true, .. }) => format!("{}", n),
        (RpcValue::F64(x), RpcValueType::Float { .. }) => format!("{}", x),

        (RpcValue::Bytes(b), _) => format!("{:?}", b),
        (other, _) => format!("{:?}", other),
    }
}

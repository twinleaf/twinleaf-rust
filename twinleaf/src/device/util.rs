use crate::device::rpc_registry::{DecodeError, EncodeError, RpcDataKind, RpcMeta, RpcValue};
use crate::tio::proxy;

pub fn load_rpc_specs(device: &proxy::Port) -> Result<Vec<RpcMeta>, proxy::RpcError> {
    let nrpcs: u16 = device.get("rpc.listinfo")?;
    let mut specs = Vec::with_capacity(nrpcs as usize);

    for id in 0..nrpcs {
        let (meta, name): (u16, String) = device.rpc("rpc.listinfo", id)?;
        specs.push(parse_rpc_spec(meta, name));
    }

    Ok(specs)
}

pub fn parse_rpc_spec(meta: u16, name: String) -> RpcMeta {
    let data_type = meta & 0x000F;         // low 4 bits
    let data_size = ((meta >> 4) & 0x000F) as u8; // next 4 bits

    let readable   = (meta & 0x0100) != 0;
    let writable   = (meta & 0x0200) != 0;
    let persistent = (meta & 0x0400) != 0;
    let unknown    = meta == 0;

    let data_kind = if unknown {
        RpcDataKind::Raw { meta }
    } else {
        match data_type {
            0 => {
                // unsigned integer
                match data_size {
                    0 => RpcDataKind::Unit,
                    1 | 2 | 4 | 8 => RpcDataKind::Int {
                        signed: false,
                        size: data_size,
                    },
                    _ => RpcDataKind::Raw { meta },
                }
            }
            1 => {
                // signed integer
                match data_size {
                    0 => RpcDataKind::Unit,
                    1 | 2 | 4 | 8 => RpcDataKind::Int {
                        signed: true,
                        size: data_size,
                    },
                    _ => RpcDataKind::Raw { meta },
                }
            }
            2 => {
                // float
                match data_size {
                    4 | 8 => RpcDataKind::Float { size: data_size },
                    0 => RpcDataKind::Unit,
                    _ => RpcDataKind::Raw { meta },
                }
            }
            3 => {
                // string; optional length from size field if nonzero
                let max_len = if data_size == 0 {
                    None
                } else {
                    Some(data_size as u16)
                };
                RpcDataKind::String { max_len }
            }
            _ => RpcDataKind::Raw { meta },
        }
    };

    let segments = name.split('.').map(|s| s.to_string()).collect();

    RpcMeta {
        full_name: name,
        segments,
        data_kind,
        readable,
        writable,
        persistent,
        meta_raw: meta,
    }
}


pub fn rpc_encode_arg(input: &str, kind: &RpcDataKind) -> Result<Vec<u8>, EncodeError> {
    match kind {
        RpcDataKind::Unit => {
            if input.is_empty() { Ok(Vec::new()) }
            else { Err(EncodeError::NotEncodableForKind("unit")) }
        }

        RpcDataKind::String { max_len } => {
            if let Some(max) = max_len {
                if input.len() > *max as usize {
                    return Err(EncodeError::StringTooLong { max: *max, actual: input.len() });
                }
            }
            Ok(input.as_bytes().to_vec())
        }

        RpcDataKind::Int { signed: false, size } => match *size {
            1 => Ok(input.parse::<u8>()?.to_le_bytes().to_vec()),
            2 => Ok(input.parse::<u16>()?.to_le_bytes().to_vec()),
            4 => Ok(input.parse::<u32>()?.to_le_bytes().to_vec()),
            8 => Ok(input.parse::<u64>()?.to_le_bytes().to_vec()),
            s => Err(EncodeError::UnsupportedIntSize(s)),
        },

        RpcDataKind::Int { signed: true, size } => match *size {
            1 => Ok(input.parse::<i8>()?.to_le_bytes().to_vec()),
            2 => Ok(input.parse::<i16>()?.to_le_bytes().to_vec()),
            4 => Ok(input.parse::<i32>()?.to_le_bytes().to_vec()),
            8 => Ok(input.parse::<i64>()?.to_le_bytes().to_vec()),
            s => Err(EncodeError::UnsupportedIntSize(s)),
        },

        RpcDataKind::Float { size } => match *size {
            4 => Ok(input.parse::<f32>()?.to_le_bytes().to_vec()),
            8 => Ok(input.parse::<f64>()?.to_le_bytes().to_vec()),
            s => Err(EncodeError::UnsupportedFloatSize(s)),
        },

        RpcDataKind::Raw { .. } => {
            Ok(Vec::new())
        }
    }
}

pub fn rpc_decode_reply(reply: &[u8], kind: &RpcDataKind) -> Result<RpcValue, DecodeError> {
    match kind {
        RpcDataKind::Unit => Ok(RpcValue::Unit),

        RpcDataKind::String { .. } => {
            match std::str::from_utf8(reply) {
                Ok(s) => Ok(RpcValue::Str(s.to_owned())),
                Err(_) => Ok(RpcValue::Bytes(reply.to_vec())), // keep bytes if not UTF-8
            }
        }

        RpcDataKind::Int { signed: false, size } => match *size {
            1 => {
                if reply.len() < 1 { return Err(DecodeError::InsufficientBytes { expected: 1, got: reply.len() }); }
                Ok(RpcValue::U64(u8::from_le_bytes([reply[0]]) as u64))
            }
            2 => {
                if reply.len() < 2 { return Err(DecodeError::InsufficientBytes { expected: 2, got: reply.len() }); }
                Ok(RpcValue::U64(u16::from_le_bytes(reply[0..2].try_into().unwrap()) as u64))
            }
            4 => {
                if reply.len() < 4 { return Err(DecodeError::InsufficientBytes { expected: 4, got: reply.len() }); }
                Ok(RpcValue::U64(u32::from_le_bytes(reply[0..4].try_into().unwrap()) as u64))
            }
            8 => {
                if reply.len() < 8 { return Err(DecodeError::InsufficientBytes { expected: 8, got: reply.len() }); }
                Ok(RpcValue::U64(u64::from_le_bytes(reply[0..8].try_into().unwrap())))
            }
            s => Err(DecodeError::UnsupportedIntSize(s)),
        },

        RpcDataKind::Int { signed: true, size } => match *size {
            1 => {
                if reply.len() < 1 { return Err(DecodeError::InsufficientBytes { expected: 1, got: reply.len() }); }
                Ok(RpcValue::I64(i8::from_le_bytes([reply[0]]) as i64))
            }
            2 => {
                if reply.len() < 2 { return Err(DecodeError::InsufficientBytes { expected: 2, got: reply.len() }); }
                Ok(RpcValue::I64(i16::from_le_bytes(reply[0..2].try_into().unwrap()) as i64))
            }
            4 => {
                if reply.len() < 4 { return Err(DecodeError::InsufficientBytes { expected: 4, got: reply.len() }); }
                Ok(RpcValue::I64(i32::from_le_bytes(reply[0..4].try_into().unwrap()) as i64))
            }
            8 => {
                if reply.len() < 8 { return Err(DecodeError::InsufficientBytes { expected: 8, got: reply.len() }); }
                Ok(RpcValue::I64(i64::from_le_bytes(reply[0..8].try_into().unwrap())))
            }
            s => Err(DecodeError::UnsupportedIntSize(s)),
        },

        RpcDataKind::Float { size } => match *size {
            4 => {
                if reply.len() < 4 { return Err(DecodeError::InsufficientBytes { expected: 4, got: reply.len() }); }
                Ok(RpcValue::F64(f32::from_le_bytes(reply[0..4].try_into().unwrap()) as f64))
            }
            8 => {
                if reply.len() < 8 { return Err(DecodeError::InsufficientBytes { expected: 8, got: reply.len() }); }
                Ok(RpcValue::F64(f64::from_le_bytes(reply[0..8].try_into().unwrap())))
            }
            s => Err(DecodeError::UnsupportedFloatSize(s)),
        },

        RpcDataKind::Raw { .. } => Ok(RpcValue::Bytes(reply.to_vec())),
    }
}

pub fn format_rpc_value_for_cli(v: &RpcValue, kind: &RpcDataKind) -> String {
    match (v, kind) {
        (RpcValue::Unit, _) => "OK".to_string(),

        (RpcValue::Str(s), _) => format!("\"{}\" {:?}", s, s.as_bytes()),

        (RpcValue::U64(n), RpcDataKind::Int { signed: false, .. }) => format!("{}", n),
        (RpcValue::I64(n), RpcDataKind::Int { signed: true, .. })  => format!("{}", n),
        (RpcValue::F64(x),  RpcDataKind::Float { .. })              => format!("{}", x),

        (RpcValue::Bytes(b), _) => format!("{:?}", b),
        (other, _) => format!("{:?}", other),
    }
}
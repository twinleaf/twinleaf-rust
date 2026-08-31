//! Dynamic RPC values, for hosts that learn an RPC's type from its metadata
//! at run time rather than from a Rust type at compile time.

use twinleaf_proto::rpc::{RpcAccess, RpcMeta, RpcMetaFlags, RpcValueType};

#[derive(Debug, Clone, PartialEq)]
pub enum RpcValue {
    Unit,
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
}

impl std::fmt::Display for RpcValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcValue::Unit => Ok(()),
            RpcValue::U64(n) => write!(f, "{}", n),
            RpcValue::I64(n) => write!(f, "{}", n),
            RpcValue::F64(x) => write!(f, "{}", x),
            RpcValue::Str(s) => write!(f, "{}", s),
            RpcValue::Bytes(b) => {
                for byte in b {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
        }
    }
}

/// Host-side codecs between the shared value-type tag and owned [`RpcValue`]s.
pub trait RpcValueTypeExt {
    fn encode(self, value: &RpcValue) -> Result<Vec<u8>, RpcValueEncodeError>;
    fn decode(self, bytes: &[u8]) -> Result<RpcValue, RpcValueDecodeError>;
}

impl RpcValueTypeExt for RpcValueType {
    fn encode(self, value: &RpcValue) -> Result<Vec<u8>, RpcValueEncodeError> {
        match (self, value) {
            (RpcValueType::Unit, RpcValue::Unit) => Ok(Vec::new()),
            (
                target @ RpcValueType::Int {
                    signed: false,
                    size,
                },
                RpcValue::U64(value),
            ) => encode_unsigned(*value, size, target),
            (target @ RpcValueType::Int { signed: true, size }, RpcValue::I64(value)) => {
                encode_signed(*value, size, target)
            }
            (RpcValueType::Float { size: 4 }, RpcValue::F64(value)) => {
                Ok((*value as f32).to_le_bytes().to_vec())
            }
            (RpcValueType::Float { size: 8 }, RpcValue::F64(value)) => {
                Ok(value.to_le_bytes().to_vec())
            }
            (RpcValueType::Float { size }, RpcValue::F64(_)) => {
                Err(RpcValueEncodeError::UnsupportedFloatSize(size))
            }
            (RpcValueType::String { max_len }, RpcValue::Str(value)) => {
                if let Some(max) = max_len {
                    if value.len() > usize::from(max.get()) {
                        return Err(RpcValueEncodeError::StringTooLong {
                            max: max.get(),
                            actual: value.len(),
                        });
                    }
                }
                Ok(value.as_bytes().to_vec())
            }
            (RpcValueType::Raw { .. }, RpcValue::Bytes(value)) => Ok(value.clone()),
            (expected, actual) => Err(RpcValueEncodeError::TypeMismatch {
                expected,
                actual: actual.kind_name(),
            }),
        }
    }

    fn decode(self, bytes: &[u8]) -> Result<RpcValue, RpcValueDecodeError> {
        match self {
            RpcValueType::Unit => Ok(RpcValue::Unit),
            RpcValueType::Int {
                signed: false,
                size,
            } => decode_unsigned(bytes, size),
            RpcValueType::Int { signed: true, size } => decode_signed(bytes, size),
            RpcValueType::Float { size: 4 } => {
                let raw = take_array::<4>(bytes)?;
                Ok(RpcValue::F64(f32::from_le_bytes(raw).into()))
            }
            RpcValueType::Float { size: 8 } => {
                let raw = take_array::<8>(bytes)?;
                Ok(RpcValue::F64(f64::from_le_bytes(raw)))
            }
            RpcValueType::Float { size } => Err(RpcValueDecodeError::UnsupportedFloatSize(size)),
            RpcValueType::String { .. } => match std::str::from_utf8(bytes) {
                Ok(value) => Ok(RpcValue::Str(value.to_owned())),
                Err(_) => Ok(RpcValue::Bytes(bytes.to_vec())),
            },
            RpcValueType::Raw { .. } => Ok(RpcValue::Bytes(bytes.to_vec())),
        }
    }
}

impl RpcValue {
    fn kind_name(&self) -> &'static str {
        match self {
            RpcValue::Unit => "unit",
            RpcValue::U64(_) => "unsigned integer",
            RpcValue::I64(_) => "signed integer",
            RpcValue::F64(_) => "float",
            RpcValue::Str(_) => "string",
            RpcValue::Bytes(_) => "bytes",
        }
    }
}

fn encode_unsigned(
    value: u64,
    size: u8,
    target: RpcValueType,
) -> Result<Vec<u8>, RpcValueEncodeError> {
    let out_of_range = || RpcValueEncodeError::OutOfRange {
        value: value.to_string(),
        target,
    };
    match size {
        1 => u8::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .map_err(|_| out_of_range()),
        2 => u16::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .map_err(|_| out_of_range()),
        4 => u32::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .map_err(|_| out_of_range()),
        8 => Ok(value.to_le_bytes().to_vec()),
        _ => Err(RpcValueEncodeError::UnsupportedIntegerSize(size)),
    }
}

fn encode_signed(
    value: i64,
    size: u8,
    target: RpcValueType,
) -> Result<Vec<u8>, RpcValueEncodeError> {
    let out_of_range = || RpcValueEncodeError::OutOfRange {
        value: value.to_string(),
        target,
    };
    match size {
        1 => i8::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .map_err(|_| out_of_range()),
        2 => i16::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .map_err(|_| out_of_range()),
        4 => i32::try_from(value)
            .map(|value| value.to_le_bytes().to_vec())
            .map_err(|_| out_of_range()),
        8 => Ok(value.to_le_bytes().to_vec()),
        _ => Err(RpcValueEncodeError::UnsupportedIntegerSize(size)),
    }
}

fn take_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], RpcValueDecodeError> {
    if bytes.len() < N {
        return Err(RpcValueDecodeError::InsufficientBytes {
            expected: N,
            actual: bytes.len(),
        });
    }
    Ok(bytes[..N]
        .try_into()
        .expect("slice length was checked before conversion"))
}

fn decode_unsigned(bytes: &[u8], size: u8) -> Result<RpcValue, RpcValueDecodeError> {
    let value = match size {
        1 => u8::from_le_bytes(take_array::<1>(bytes)?) as u64,
        2 => u16::from_le_bytes(take_array::<2>(bytes)?) as u64,
        4 => u32::from_le_bytes(take_array::<4>(bytes)?) as u64,
        8 => u64::from_le_bytes(take_array::<8>(bytes)?),
        _ => return Err(RpcValueDecodeError::UnsupportedIntegerSize(size)),
    };
    Ok(RpcValue::U64(value))
}

fn decode_signed(bytes: &[u8], size: u8) -> Result<RpcValue, RpcValueDecodeError> {
    let value = match size {
        1 => i8::from_le_bytes(take_array::<1>(bytes)?) as i64,
        2 => i16::from_le_bytes(take_array::<2>(bytes)?) as i64,
        4 => i32::from_le_bytes(take_array::<4>(bytes)?) as i64,
        8 => i64::from_le_bytes(take_array::<8>(bytes)?),
        _ => return Err(RpcValueDecodeError::UnsupportedIntegerSize(size)),
    };
    Ok(RpcValue::I64(value))
}

#[derive(Debug, thiserror::Error)]
pub enum RpcValueEncodeError {
    #[error("cannot encode {actual} as {expected:?}")]
    TypeMismatch {
        expected: RpcValueType,
        actual: &'static str,
    },
    #[error("value {value} is out of range for {target:?}")]
    OutOfRange { value: String, target: RpcValueType },
    #[error("string too long ({actual} bytes, max {max})")]
    StringTooLong { max: u8, actual: usize },
    #[error("unsupported integer size: {0} bytes")]
    UnsupportedIntegerSize(u8),
    #[error("unsupported float size: {0} bytes")]
    UnsupportedFloatSize(u8),
}

#[derive(Debug, thiserror::Error)]
pub enum RpcValueDecodeError {
    #[error("expected {expected} bytes, got {actual}")]
    InsufficientBytes { expected: usize, actual: usize },
    #[error("unsupported integer size: {0} bytes")]
    UnsupportedIntegerSize(u8),
    #[error("unsupported float size: {0} bytes")]
    UnsupportedFloatSize(u8),
}

/// Host-side display helpers over the shared metadata word.
pub trait RpcMetaExt {
    fn perm_str(&self) -> String;
    fn type_str(&self) -> String;
}

impl RpcMetaExt for RpcMeta {
    fn perm_str(&self) -> String {
        if self.is_unknown() {
            return "???".to_string();
        }
        let (r, w) = match self.access() {
            RpcAccess::ReadWrite => ("R", "W"),
            RpcAccess::ReadOnly => ("R", "-"),
            RpcAccess::WriteOnly => ("-", "W"),
            RpcAccess::Action => ("-", "-"),
        };
        let p = if self.is_persistent() { "P" } else { "-" };
        format!("{r}{w}{p}")
    }

    fn type_str(&self) -> String {
        let flags = self.flags();
        if flags.contains(RpcMetaFlags::CAPTURE) {
            return "capture".to_string();
        }
        if flags.contains(RpcMetaFlags::BOOL) {
            return "bool".to_string();
        }
        match self.kind() {
            RpcValueType::Unit => String::new(),
            RpcValueType::Int { signed, size } => {
                let bits = (size as usize) * 8;
                if signed {
                    format!("i{bits}")
                } else {
                    format!("u{bits}")
                }
            }
            RpcValueType::Float { size } => format!("f{}", (size as usize) * 8),
            RpcValueType::String { max_len } => match max_len {
                Some(n) => format!("string<{n}>"),
                None => "string".to_string(),
            },
            RpcValueType::Raw { .. } => String::new(),
        }
    }
}

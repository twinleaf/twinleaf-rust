#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcValueType {
    Unit,
    Int { signed: bool, size: u8 },
    Float { size: u8 },
    String { max_len: Option<u16> },
    Raw { meta: u16 },
}

impl RpcValueType {
    const TYPE_UINT: u8 = 0;
    const TYPE_INT: u8 = 1;
    const TYPE_FLOAT: u8 = 2;
    const TYPE_STRING: u8 = 3;

    pub const fn from_low_byte(byte: u8) -> Option<Self> {
        let data_type = byte & 0x0F;
        let data_size = (byte >> 4) & 0x0F;
        let kind = match data_type {
            Self::TYPE_UINT => match data_size {
                0 => RpcValueType::Unit,
                1 | 2 | 4 | 8 => RpcValueType::Int {
                    signed: false,
                    size: data_size,
                },
                _ => return None,
            },
            Self::TYPE_INT => match data_size {
                0 => RpcValueType::Unit,
                1 | 2 | 4 | 8 => RpcValueType::Int {
                    signed: true,
                    size: data_size,
                },
                _ => return None,
            },
            Self::TYPE_FLOAT => match data_size {
                4 | 8 => RpcValueType::Float { size: data_size },
                0 => RpcValueType::Unit,
                _ => return None,
            },
            Self::TYPE_STRING => RpcValueType::String {
                max_len: if data_size == 0 {
                    None
                } else {
                    Some(data_size as u16)
                },
            },
            _ => return None,
        };
        Some(kind)
    }

    pub const fn low_byte(self) -> u8 {
        match self {
            RpcValueType::Unit => 0,
            RpcValueType::Int {
                signed: false,
                size,
            } => (size << 4) | Self::TYPE_UINT,
            RpcValueType::Int { signed: true, size } => (size << 4) | Self::TYPE_INT,
            RpcValueType::Float { size } => (size << 4) | Self::TYPE_FLOAT,
            RpcValueType::String { max_len } => {
                let n = match max_len {
                    Some(n) => n,
                    None => 0,
                };
                (((n & 0x0F) as u8) << 4) | Self::TYPE_STRING
            }
            RpcValueType::Raw { meta } => (meta & 0x00FF) as u8,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("invalid integer: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("invalid float: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("string too long ({actual} bytes, max {max})")]
    StringTooLong { max: u16, actual: usize },
    #[error("unsupported integer size: {0} bytes")]
    UnsupportedIntSize(u8),
    #[error("unsupported float size: {0} bytes")]
    UnsupportedFloatSize(u8),
    #[error("value not encodable for kind '{0}'")]
    NotEncodableForKind(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("expected {expected} bytes, got {got}")]
    InsufficientBytes { expected: usize, got: usize },
    #[error("invalid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("unsupported integer size: {0} bytes")]
    UnsupportedIntSize(u8),
    #[error("unsupported float size: {0} bytes")]
    UnsupportedFloatSize(u8),
}

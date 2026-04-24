#[derive(Debug, Clone)]
pub enum RpcValue {
    Unit,
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub enum RpcValueType {
    Unit,
    Int { signed: bool, size: u8 },
    Float { size: u8 },
    String { max_len: Option<u16> },
    Raw { meta: u16 },
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

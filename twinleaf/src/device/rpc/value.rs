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

#[derive(Debug)]
pub enum EncodeError {
    ParseInt(std::num::ParseIntError),
    ParseFloat(std::num::ParseFloatError),
    StringTooLong { max: u16, actual: usize },
    UnsupportedIntSize(u8),
    UnsupportedFloatSize(u8),
    NotEncodableForKind(&'static str),
}

impl From<std::num::ParseIntError> for EncodeError {
    fn from(e: std::num::ParseIntError) -> Self {
        EncodeError::ParseInt(e)
    }
}
impl From<std::num::ParseFloatError> for EncodeError {
    fn from(e: std::num::ParseFloatError) -> Self {
        EncodeError::ParseFloat(e)
    }
}

#[derive(Debug)]
pub enum DecodeError {
    InsufficientBytes { expected: usize, got: usize },
    Utf8(std::str::Utf8Error),
    UnsupportedIntSize(u8),
    UnsupportedFloatSize(u8),
}

impl From<std::str::Utf8Error> for DecodeError {
    fn from(e: std::str::Utf8Error) -> Self {
        DecodeError::Utf8(e)
    }
}

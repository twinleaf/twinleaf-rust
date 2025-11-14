use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub enum RpcValue {
    Unit,
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
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
    fn from(e: std::num::ParseIntError) -> Self { EncodeError::ParseInt(e) }
}
impl From<std::num::ParseFloatError> for EncodeError {
    fn from(e: std::num::ParseFloatError) -> Self { EncodeError::ParseFloat(e) }
}

#[derive(Debug)]
pub enum DecodeError {
    InsufficientBytes { expected: usize, got: usize },
    Utf8(std::str::Utf8Error),
    UnsupportedIntSize(u8),
    UnsupportedFloatSize(u8),
}

impl From<std::str::Utf8Error> for DecodeError {
    fn from(e: std::str::Utf8Error) -> Self { DecodeError::Utf8(e) }
}

#[derive(Debug, Clone)]
pub enum RpcDataKind {
    Unit,
    Int { signed: bool, size: u8 },
    Float { size: u8 },
    String { max_len: Option<u16> },
    Raw { meta: u16 },
}


#[derive(Debug, Clone)]
pub struct RpcMeta {
    pub full_name: String,      // "dev.port.rate.min"
    pub segments: Vec<String>,  // ["dev", "port", "rate", "min"]
    pub data_kind: RpcDataKind,
    pub readable: bool,
    pub writable: bool,
    pub persistent: bool,
    pub meta_raw: u16,
}

impl RpcMeta {
    pub fn is_unknown(&self) -> bool {
        self.meta_raw == 0
    }

    pub fn perm_str(&self) -> String {
        if self.is_unknown() {
            "???".to_string()
        } else {
            format!(
                "{}{}{}",
                if self.readable   { "R" } else { "-" },
                if self.writable   { "W" } else { "-" },
                if self.persistent { "P" } else { "-" },
            )
        }
    }

    pub fn type_str(&self) -> String {
        match self.data_kind {
            RpcDataKind::Unit => "".to_string(),
            RpcDataKind::Int { signed, size } => {
                let bits = (size as usize) * 8;
                if signed {
                    format!("i{bits}")
                } else {
                    format!("u{bits}")
                }
            }
            RpcDataKind::Float { size } => {
                let bits = (size as usize) * 8;
                format!("f{bits}")
            }
            RpcDataKind::String { max_len } => {
                if let Some(n) = max_len {
                    format!("string<{n}>")
                } else {
                    "string".to_string()
                }
            }
            RpcDataKind::Raw { .. } => "".to_string(),
        }
    }

    pub fn size_bytes(&self) -> Option<usize> {
        match self.data_kind {
            RpcDataKind::Unit => Some(0),
            RpcDataKind::Int { size, .. } => Some(size as usize),
            RpcDataKind::Float { size } => Some(size as usize),
            RpcDataKind::String { .. } => None,
            RpcDataKind::Raw { .. } => None,
        }
    }
}


#[derive(Default)]
struct RpcNode {
    children: BTreeMap<String, RpcNode>,
    rpc: Option<RpcMeta>,
}

fn build_tree(specs: &[RpcMeta]) -> RpcNode {
    let mut root = RpcNode::default();
    for spec in specs {
        let mut node = &mut root;
        for seg in &spec.segments {
            node = node.children.entry(seg.clone()).or_default();
        }
        node.rpc = Some(spec.clone());
    }
    root
}

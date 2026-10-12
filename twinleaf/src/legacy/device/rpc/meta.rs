use super::flags::{RpcAccess, RpcMetaFlags};
use super::value::RpcValueType;

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct RpcMeta(u16);

impl RpcMeta {
    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    pub const fn new(kind: RpcValueType, flags: RpcMetaFlags) -> Self {
        Self(flags.bits() | kind.low_byte() as u16)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn is_unknown(self) -> bool {
        self.0 == 0
    }

    pub const fn flags(self) -> RpcMetaFlags {
        RpcMetaFlags::from_meta(self.0)
    }

    pub const fn access(self) -> RpcAccess {
        self.flags().access()
    }

    pub const fn is_persistent(self) -> bool {
        self.flags().is_persistent()
    }

    pub fn kind(self) -> RpcValueType {
        if self.is_unknown() || self.flags().contains(RpcMetaFlags::CAPTURE) {
            return RpcValueType::Raw { meta: self.0 };
        }
        RpcValueType::from_low_byte(self.0 as u8).unwrap_or(RpcValueType::Raw { meta: self.0 })
    }

    pub fn size_bytes(self) -> Option<usize> {
        match self.kind() {
            RpcValueType::Unit => Some(0),
            RpcValueType::Int { size, .. } => Some(size as usize),
            RpcValueType::Float { size } => Some(size as usize),
            RpcValueType::String { .. } | RpcValueType::Raw { .. } => None,
        }
    }

    pub fn perm_str(self) -> String {
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

    pub fn type_str(self) -> String {
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

impl From<u16> for RpcMeta {
    fn from(bits: u16) -> Self {
        Self::from_bits(bits)
    }
}

impl From<RpcMeta> for u16 {
    fn from(meta: RpcMeta) -> u16 {
        meta.bits()
    }
}

impl std::fmt::Debug for RpcMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcMeta")
            .field("bits", &format_args!("{:#06x}", self.0))
            .field("access", &self.access())
            .field("persistent", &self.is_persistent())
            .field("kind", &self.kind())
            .finish()
    }
}

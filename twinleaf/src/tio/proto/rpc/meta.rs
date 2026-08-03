/// Bounded string length, as encoded in the metadata size nibble.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RpcStringLen(u8);

impl RpcStringLen {
    pub const MAX: u8 = 0x0F;

    /// Returns `None` for lengths which cannot be encoded, zero included:
    /// an unbounded string is `RpcValueType::String { max_len: None }`.
    pub const fn new(len: u8) -> Option<Self> {
        if len == 0 || len > Self::MAX {
            None
        } else {
            Some(Self(len))
        }
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

impl std::fmt::Display for RpcStringLen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcValueType {
    Unit,
    Int { signed: bool, size: u8 },
    Float { size: u8 },
    String { max_len: Option<RpcStringLen> },
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
                max_len: RpcStringLen::new(data_size),
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
                    Some(n) => n.get(),
                    None => 0,
                };
                (n << 4) | Self::TYPE_STRING
            }
            RpcValueType::Raw { meta } => (meta & 0x00FF) as u8,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RpcAccess {
    ReadWrite,
    ReadOnly,
    WriteOnly,
    Action,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct RpcMetaFlags(u16);

impl RpcMetaFlags {
    pub const READABLE: Self = Self(0x0100);
    pub const WRITABLE: Self = Self(0x0200);
    pub const PERSISTENT: Self = Self(0x0400);
    pub const BOOL: Self = Self(0x0800);
    pub const CAPTURE: Self = Self(0x1000);

    const MASK: u16 = 0xFF00;

    pub const fn from_meta(meta: u16) -> Self {
        Self(meta & Self::MASK)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn access(self) -> RpcAccess {
        match (self.contains(Self::READABLE), self.contains(Self::WRITABLE)) {
            (true, true) => RpcAccess::ReadWrite,
            (true, false) => RpcAccess::ReadOnly,
            (false, true) => RpcAccess::WriteOnly,
            (false, false) => RpcAccess::Action,
        }
    }

    pub const fn is_persistent(self) -> bool {
        self.contains(Self::PERSISTENT)
    }
}

impl std::ops::BitOr for RpcMetaFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        self.union(rhs)
    }
}

impl std::fmt::Debug for RpcMetaFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (flag, name) in [
            (Self::READABLE, "READABLE"),
            (Self::WRITABLE, "WRITABLE"),
            (Self::PERSISTENT, "PERSISTENT"),
            (Self::BOOL, "BOOL"),
            (Self::CAPTURE, "CAPTURE"),
        ] {
            if self.contains(flag) {
                write!(f, "{}{name}", if first { "" } else { " | " })?;
                first = false;
            }
        }
        if first {
            write!(f, "(none)")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct RpcMeta(u16);

impl RpcMeta {
    const TYPED: u16 = 0x8000;

    pub const ANY: Self = Self(0);
    pub const ACTION: Self = Self(Self::TYPED);

    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    pub const fn new(kind: RpcValueType, flags: RpcMetaFlags) -> Self {
        Self(Self::TYPED | flags.bits() | kind.low_byte() as u16)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn to_le_bytes(self) -> [u8; 2] {
        self.0.to_le_bytes()
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

    pub const fn kind(self) -> RpcValueType {
        if self.is_unknown() || self.flags().contains(RpcMetaFlags::CAPTURE) {
            return RpcValueType::Raw { meta: self.0 };
        }
        match RpcValueType::from_low_byte(self.0 as u8) {
            Some(kind) => kind,
            None => RpcValueType::Raw { meta: self.0 },
        }
    }

    pub const fn size_bytes(self) -> Option<usize> {
        match self.kind() {
            RpcValueType::Unit => Some(0),
            RpcValueType::Int { size, .. } | RpcValueType::Float { size } => Some(size as usize),
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

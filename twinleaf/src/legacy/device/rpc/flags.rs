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

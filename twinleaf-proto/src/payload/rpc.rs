//! RPC packets, which carry a request, a reply, or an error.
//!
//! A request payload is `{ id: u16le, method: u16le }`, then the method name
//! if `method & 0x8000` is set, then the argument bytes. The name is
//! `method & 0x7FFF` bytes long and not NUL-terminated.
//!
//! A reply payload is `{ req_id: u16le }` followed by the return value.
//!
//! An error payload is `{ req_id: u16le, code: u16le }` followed by an
//! optional message.
//!
//! [`Request`], [`Reply`], and [`ErrorReply`] parse the payloads, and the
//! `write_*` functions build them. [`RpcMeta`] decodes the `rpc.info` word.

use crate::packet::{Header, Packet, PacketType};
use crate::{RpcMethodId, RpcRequestId};

/// Method word bit marking a request by name, 0x8000.
pub const REQUEST_BY_NAME: u16 = 0x8000;
/// Method word bits holding the name length, 0x7FFF.
pub const NAMELEN_MASK: u16 = 0x7FFF;

/// Size of the `[request id u16le]` header before a reply's value.
pub const REPLY_HEADER_SIZE: usize = 2;

/// Error codes (`TL_RPC_ERROR_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum RpcError {
    /// No error, 0.
    #[error("no error")]
    None,
    /// Undefined error, 1.
    #[error("undefined error")]
    Undefined,
    /// RPC not found, 2.
    #[error("RPC not found")]
    NotFound,
    /// Malformed request, 3.
    #[error("malformed request")]
    Malformed,
    /// Wrong argument size, 4.
    #[error("wrong size args")]
    ArgsSize,
    /// Invalid arguments, 5.
    #[error("invalid arguments")]
    Invalid,
    /// Read-only, 6.
    #[error("read-only")]
    ReadOnly,
    /// Write-only, 7.
    #[error("write-only")]
    WriteOnly,
    /// Timeout, 8.
    #[error("timeout")]
    Timeout,
    /// Device busy, 9.
    #[error("device busy")]
    Busy,
    /// Wrong device state, 10.
    #[error("wrong device state")]
    State,
    /// Load failed, 11.
    #[error("load failed")]
    Load,
    /// Load RPC failed, 12.
    #[error("load RPC failed")]
    LoadRpc,
    /// Save failed, 13.
    #[error("save failed")]
    Save,
    /// Save write failed, 14.
    #[error("save write failed")]
    SaveWrite,
    /// Internal error, 15.
    #[error("internal error")]
    Internal,
    /// Out of memory, 16.
    #[error("out of memory")]
    NoBufs,
    /// Out of range, 17.
    #[error("out of range")]
    Range,
    /// Any other code. Codes from 18 up are defined per RPC.
    #[error("unknown error code {0}")]
    Unknown(u16),
}

impl RpcError {
    /// Returns the error code.
    pub const fn value(self) -> u16 {
        match self {
            Self::None => 0,
            Self::Undefined => 1,
            Self::NotFound => 2,
            Self::Malformed => 3,
            Self::ArgsSize => 4,
            Self::Invalid => 5,
            Self::ReadOnly => 6,
            Self::WriteOnly => 7,
            Self::Timeout => 8,
            Self::Busy => 9,
            Self::State => 10,
            Self::Load => 11,
            Self::LoadRpc => 12,
            Self::Save => 13,
            Self::SaveWrite => 14,
            Self::Internal => 15,
            Self::NoBufs => 16,
            Self::Range => 17,
            Self::Unknown(code) => code,
        }
    }
}

impl From<u16> for RpcError {
    fn from(code: u16) -> Self {
        match code {
            0 => Self::None,
            1 => Self::Undefined,
            2 => Self::NotFound,
            3 => Self::Malformed,
            4 => Self::ArgsSize,
            5 => Self::Invalid,
            6 => Self::ReadOnly,
            7 => Self::WriteOnly,
            8 => Self::Timeout,
            9 => Self::Busy,
            10 => Self::State,
            11 => Self::Load,
            12 => Self::LoadRpc,
            13 => Self::Save,
            14 => Self::SaveWrite,
            15 => Self::Internal,
            16 => Self::NoBufs,
            17 => Self::Range,
            code => Self::Unknown(code),
        }
    }
}

impl From<RpcError> for u16 {
    fn from(error: RpcError) -> Self {
        error.value()
    }
}

/// How a request names its method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method<'a> {
    /// By numeric id.
    ById(RpcMethodId),
    /// By name, not NUL-terminated.
    ByName(&'a [u8]),
}

/// A request, as it travels in an `RPC_REQ` packet.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    /// Request id, returned with the reply.
    pub id: RpcRequestId,
    /// Method called.
    pub method: Method<'a>,
    /// Argument bytes.
    pub args: &'a [u8],
}

impl<'a> Request<'a> {
    /// Parses an RPC request from a packet payload, without the packet header.
    /// Returns `None` if the payload is truncated.
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        if payload.len() < 4 {
            return None;
        }
        let id = RpcRequestId::from_le_bytes([payload[0], payload[1]]);
        let method_id = u16::from_le_bytes([payload[2], payload[3]]);
        let rest = &payload[4..];
        if method_id & REQUEST_BY_NAME != 0 {
            let name_len = (method_id & NAMELEN_MASK) as usize;
            if rest.len() < name_len {
                return None;
            }
            Some(Self {
                id,
                method: Method::ByName(&rest[..name_len]),
                args: &rest[name_len..],
            })
        } else {
            Some(Self {
                id,
                method: Method::ById(RpcMethodId::new(method_id)),
                args: rest,
            })
        }
    }
}

/// Writes a full `RPC_REQ` packet with no routing into `buf`. Returns its
/// length, or `None` if it does not fit.
pub fn write_request(
    buf: &mut [u8],
    id: RpcRequestId,
    method: Method<'_>,
    args: &[u8],
) -> Option<usize> {
    let payload_len = request_payload_len(method, args)?;
    let total = Header::SIZE + payload_len;
    if buf.len() < total || payload_len > Packet::MAX_PAYLOAD {
        return None;
    }
    let hdr = Header::new(PacketType::RPC_REQ, payload_len as u16);
    hdr.write(buf.first_chunk_mut()?);
    write_request_payload(&mut buf[Header::SIZE..total], id, method, args)?;
    Some(total)
}

/// Returns the payload length of a request, or `None` if the method name is
/// longer than [`NAMELEN_MASK`] bytes.
pub fn request_payload_len(method: Method<'_>, args: &[u8]) -> Option<usize> {
    let name_len = match method {
        Method::ById(_) => 0,
        Method::ByName(name) => name.len(),
    };
    if name_len > NAMELEN_MASK as usize {
        return None;
    }
    Some(4 + name_len + args.len())
}

/// Writes the request payload alone, without a packet header, into `out`.
/// Returns its length, or `None` if it does not fit.
pub fn write_request_payload(
    out: &mut [u8],
    id: RpcRequestId,
    method: Method<'_>,
    args: &[u8],
) -> Option<usize> {
    let len = request_payload_len(method, args)?;
    if out.len() < len {
        return None;
    }
    let (method_id, name) = match method {
        Method::ById(id) => (id.value(), &[][..]),
        Method::ByName(name) => (REQUEST_BY_NAME | name.len() as u16, name),
    };
    out[0..2].copy_from_slice(&id.to_le_bytes());
    out[2..4].copy_from_slice(&method_id.to_le_bytes());
    out[4..4 + name.len()].copy_from_slice(name);
    out[4 + name.len()..len].copy_from_slice(args);
    Some(len)
}

/// Overwrites the request id at the start of a request, reply, or error
/// payload. Returns `false` if the payload is under two bytes.
pub fn set_req_id(payload: &mut [u8], id: RpcRequestId) -> bool {
    let Some(field) = payload.get_mut(..2) else {
        return false;
    };
    field.copy_from_slice(&id.to_le_bytes());
    true
}

/// A successful answer, with the id of its request and the returned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reply<'a> {
    /// Id of the request answered.
    pub req_id: RpcRequestId,
    /// Return value bytes.
    pub value: &'a [u8],
}

impl<'a> Reply<'a> {
    /// Parses an RPC reply from a packet payload, without the packet header.
    /// Returns `None` if the payload is under two bytes.
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        let (req_id, value) = payload.split_at_checked(2)?;
        Some(Self {
            req_id: RpcRequestId::from_le_bytes([req_id[0], req_id[1]]),
            value,
        })
    }
}

/// Writes a full `RPC_REP` packet with no routing into `buf`. Returns its
/// length, or `None` if `buf` is too small or the value is longer than a
/// packet carries.
pub fn write_reply(buf: &mut [u8], req_id: RpcRequestId, value: &[u8]) -> Option<usize> {
    let payload_len = 2 + value.len();
    let total = Header::SIZE + payload_len;
    if buf.len() < total || payload_len > Packet::MAX_PAYLOAD {
        return None;
    }
    let hdr = Header::new(PacketType::RPC_REP, payload_len as u16);
    hdr.write(buf.first_chunk_mut()?);
    write_reply_payload(&mut buf[Header::SIZE..total], req_id, value)?;
    Some(total)
}

/// Writes the reply payload alone, without a packet header, into `out`.
/// Returns its length, or `None` if it does not fit.
pub fn write_reply_payload(out: &mut [u8], req_id: RpcRequestId, value: &[u8]) -> Option<usize> {
    let len = 2 + value.len();
    if out.len() < len {
        return None;
    }
    out[0..2].copy_from_slice(&req_id.to_le_bytes());
    out[2..len].copy_from_slice(value);
    Some(len)
}

/// A refused request, with its raw error code and any message that followed
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorReply<'a> {
    /// Id of the request answered.
    pub req_id: RpcRequestId,
    /// Error code, see [`RpcError`].
    pub code: u16,
    /// Optional message bytes.
    pub message: &'a [u8],
}

impl<'a> ErrorReply<'a> {
    /// Parses an RPC error from a packet payload, without the packet header.
    /// Returns `None` if the payload is under four bytes.
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        let (head, message) = payload.split_at_checked(4)?;
        Some(Self {
            req_id: RpcRequestId::from_le_bytes([head[0], head[1]]),
            code: u16::from_le_bytes([head[2], head[3]]),
            message,
        })
    }

    /// Returns the error this reply names.
    pub fn error(&self) -> RpcError {
        RpcError::from(self.code)
    }
}

/// Writes a full `RPC_ERROR` packet with no routing and no message into
/// `buf`. Returns its length, or `None` if `buf` is too small.
pub fn write_error(buf: &mut [u8], req_id: RpcRequestId, code: RpcError) -> Option<usize> {
    write_error_with(buf, req_id, code, &[])
}

/// Writes a full `RPC_ERROR` packet with no routing into `buf`, with
/// `message` after its code. Returns its length, or `None` if `buf` is too
/// small or the message is longer than a packet carries.
pub fn write_error_with(
    buf: &mut [u8],
    req_id: RpcRequestId,
    code: RpcError,
    message: &[u8],
) -> Option<usize> {
    let payload_len = 4 + message.len();
    let total = Header::SIZE + payload_len;
    if buf.len() < total || payload_len > Packet::MAX_PAYLOAD {
        return None;
    }
    let hdr = Header::new(PacketType::RPC_ERROR, payload_len as u16);
    hdr.write(buf.first_chunk_mut()?);
    write_error_payload(&mut buf[Header::SIZE..total], req_id, code.value(), message)?;
    Some(total)
}

/// Writes the error payload alone, without a packet header, into `out`.
/// Returns its length, or `None` if it does not fit. `code` is raw and may be
/// a code with no [`RpcError`] name.
pub fn write_error_payload(
    out: &mut [u8],
    req_id: RpcRequestId,
    code: u16,
    message: &[u8],
) -> Option<usize> {
    let len = 4 + message.len();
    if out.len() < len {
        return None;
    }
    out[0..2].copy_from_slice(&req_id.to_le_bytes());
    out[2..4].copy_from_slice(&code.to_le_bytes());
    out[4..len].copy_from_slice(message);
    Some(len)
}

/// A reply or an error, told apart by the packet type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer<'a> {
    /// A reply.
    Reply(Reply<'a>),
    /// An error.
    Error(ErrorReply<'a>),
}

impl<'a> Answer<'a> {
    /// Parses a payload as the answer its packet type `ptype` names. Returns
    /// `None` for any other packet type or a payload too short to name a
    /// request.
    pub fn parse(ptype: PacketType, payload: &'a [u8]) -> Option<Self> {
        match ptype {
            PacketType::RPC_REP => Reply::parse(payload).map(Self::Reply),
            PacketType::RPC_ERROR => ErrorReply::parse(payload).map(Self::Error),
            _ => None,
        }
    }

    /// Returns the id of the request answered.
    pub fn req_id(&self) -> RpcRequestId {
        match self {
            Self::Reply(reply) => reply.req_id,
            Self::Error(error) => error.req_id,
        }
    }
}

/// Access/behavior flags in the 16-bit legacy `rpc.info` value.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct RpcMetaFlags(u16);

impl core::fmt::Debug for RpcMetaFlags {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut first = true;
        for (flag, name) in [
            (Self::READABLE, "READABLE"),
            (Self::WRITABLE, "WRITABLE"),
            (Self::PERSISTENT, "PERSISTENT"),
            (Self::CAPTURE, "CAPTURE"),
            (Self::ANNOUNCED, "ANNOUNCED"),
            (Self::PRIVILEGED, "PRIVILEGED"),
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

impl RpcMetaFlags {
    /// Readable, 0x0100.
    pub const READABLE: Self = Self(0x0100);
    /// Writable, 0x0200.
    pub const WRITABLE: Self = Self(0x0200);
    /// Saved across reboots, 0x0400.
    pub const PERSISTENT: Self = Self(0x0400);
    /// Capture, an array of the element type read in blocks, 0x1000.
    pub const CAPTURE: Self = Self(0x1000);
    /// Every change is announced, so a host may cache the value, 0x2000.
    pub const ANNOUNCED: Self = Self(0x2000);
    /// Callable only by a session `dev.priv` has unlocked, 0x4000.
    pub const PRIVILEGED: Self = Self(0x4000);

    // Preserve unknown flag bits when decoding.
    const MASK: u16 = 0xff00;

    /// Extracts the flags from an `rpc.info` word.
    pub const fn from_meta(meta: u16) -> Self {
        Self(meta & Self::MASK)
    }

    /// Returns the raw bits.
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Returns `true` if every bit of `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns the flags set in either `self` or `other`.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns the access implied by the readable and writable flags.
    pub const fn access(self) -> RpcAccess {
        match (self.contains(Self::READABLE), self.contains(Self::WRITABLE)) {
            (true, true) => RpcAccess::ReadWrite,
            (true, false) => RpcAccess::ReadOnly,
            (false, true) => RpcAccess::WriteOnly,
            (false, false) => RpcAccess::Action,
        }
    }

    /// Returns `true` if the value is saved across reboots.
    pub const fn is_persistent(self) -> bool {
        self.contains(Self::PERSISTENT)
    }
}

impl core::ops::BitOr for RpcMetaFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

/// Access of a method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcAccess {
    /// Readable and writable.
    ReadWrite,
    /// Readable only.
    ReadOnly,
    /// Writable only.
    WriteOnly,
    /// Neither, an action.
    Action,
}

/// A length of 1 to 15 bytes, as the metadata size nibble encodes it, which
/// bounds a string and sizes a byte array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct RpcStringLen(u8);

impl RpcStringLen {
    /// Longest bounded length, 15.
    pub const MAX: u8 = 0x0f;

    /// Creates a bounded string length. Returns `None` for 0 and for lengths
    /// above 15. An unbounded string is `RpcValueType::String { max_len: None }`.
    pub const fn new(len: u8) -> Option<Self> {
        if len == 0 || len > Self::MAX {
            None
        } else {
            Some(Self(len))
        }
    }

    /// Returns the length.
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl core::fmt::Display for RpcStringLen {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

/// Value kind encoded in `rpc.info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcValueType {
    /// No value.
    Unit,
    /// Integer.
    Int {
        /// Whether the integer is signed.
        signed: bool,
        /// Size in bytes, 1, 2, 4, or 8.
        size: u8,
    },
    /// Float.
    Float {
        /// Size in bytes, 4 or 8.
        size: u8,
    },
    /// String.
    String {
        /// Longest value in bytes, or `None` if unbounded.
        max_len: Option<RpcStringLen>,
    },
    /// Boolean, one byte, nonzero for true.
    Bool,
    /// Raw bytes.
    Bytes {
        /// The exact length in bytes, or `None` if it varies.
        len: Option<RpcStringLen>,
    },
    /// Any other encoding.
    Raw {
        /// The raw `rpc.info` word.
        meta: u16,
    },
}

impl RpcValueType {
    const TYPE_UINT: u8 = 0;
    const TYPE_INT: u8 = 1;
    const TYPE_FLOAT: u8 = 2;
    const TYPE_STRING: u8 = 3;
    const TYPE_BOOL: u8 = 4;
    const TYPE_BYTES: u8 = 5;

    /// Decodes the value type in the low byte of an `rpc.info` word. Returns
    /// `None` for an undefined encoding.
    pub const fn from_low_byte(byte: u8) -> Option<Self> {
        let data_type = byte & 0x0f;
        let data_size = (byte >> 4) & 0x0f;
        match data_type {
            Self::TYPE_UINT => match data_size {
                0 => Some(Self::Unit),
                1 | 2 | 4 | 8 => Some(Self::Int {
                    signed: false,
                    size: data_size,
                }),
                _ => None,
            },
            Self::TYPE_INT => match data_size {
                0 => Some(Self::Unit),
                1 | 2 | 4 | 8 => Some(Self::Int {
                    signed: true,
                    size: data_size,
                }),
                _ => None,
            },
            Self::TYPE_FLOAT => match data_size {
                0 => Some(Self::Unit),
                4 | 8 => Some(Self::Float { size: data_size }),
                _ => None,
            },
            Self::TYPE_STRING => Some(Self::String {
                max_len: RpcStringLen::new(data_size),
            }),
            Self::TYPE_BOOL => match data_size {
                0 => Some(Self::Unit),
                1 => Some(Self::Bool),
                _ => None,
            },
            Self::TYPE_BYTES => Some(Self::Bytes {
                len: RpcStringLen::new(data_size),
            }),
            _ => None,
        }
    }

    /// Returns the low byte of an `rpc.info` word for this type.
    pub const fn low_byte(self) -> u8 {
        match self {
            Self::Unit => 0,
            Self::Int {
                signed: false,
                size,
            } => (size << 4) | Self::TYPE_UINT,
            Self::Int { signed: true, size } => (size << 4) | Self::TYPE_INT,
            Self::Float { size } => (size << 4) | Self::TYPE_FLOAT,
            Self::String { max_len } => match max_len {
                Some(size) => (size.get() << 4) | Self::TYPE_STRING,
                None => Self::TYPE_STRING,
            },
            Self::Bool => (1 << 4) | Self::TYPE_BOOL,
            Self::Bytes { len } => match len {
                Some(size) => (size.get() << 4) | Self::TYPE_BYTES,
                None => Self::TYPE_BYTES,
            },
            Self::Raw { meta } => (meta & 0x00ff) as u8,
        }
    }
}

/// The 16-bit `rpc.info` word, with flags in the high byte and the value type
/// in the low byte.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct RpcMeta(u16);

impl core::fmt::Debug for RpcMeta {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RpcMeta")
            .field("bits", &format_args!("{:#06x}", self.0))
            .field("access", &self.access())
            .field("persistent", &self.is_persistent())
            .field("kind", &self.kind())
            .finish()
    }
}

impl RpcMeta {
    const TYPED: u16 = 0x8000;
    /// The retired bool flag older firmware sets on a `u8`, read but never written.
    const LEGACY_BOOL: u16 = 0x0800;

    /// Untyped metadata, for a variable-size or unspecified value.
    pub const ANY: Self = Self(0);
    /// Void action RPC, with neither the readable nor the writable flag.
    pub const ACTION: Self = Self(Self::TYPED);

    /// Creates a word from its bits.
    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    /// Creates typed metadata, including the legacy type marker. Same as
    /// [`typed`](Self::typed).
    pub const fn new(kind: RpcValueType, flags: RpcMetaFlags) -> Self {
        Self::typed(kind, flags)
    }

    /// Creates the word for a value of type `kind` with `flags`.
    pub const fn typed(kind: RpcValueType, flags: RpcMetaFlags) -> Self {
        Self(Self::TYPED | kind.low_byte() as u16 | flags.bits())
    }

    /// Creates the word for an unsigned integer of `size` bytes.
    ///
    /// # Panics
    ///
    /// Panics unless `size` is 1, 2, 4, or 8. In a `const` context this is a
    /// compile error.
    pub const fn fixed_uint(size: u8, flags: RpcMetaFlags) -> Self {
        assert!(matches!(size, 1 | 2 | 4 | 8), "unsupported RPC uint size");
        Self::typed(
            RpcValueType::Int {
                signed: false,
                size,
            },
            flags,
        )
    }

    /// Creates the word for an unbounded string.
    pub const fn string(flags: RpcMetaFlags) -> Self {
        Self::typed(RpcValueType::String { max_len: None }, flags)
    }

    /// Returns the raw word.
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Returns the two wire bytes, little endian.
    pub const fn to_le_bytes(self) -> [u8; 2] {
        self.0.to_le_bytes()
    }

    /// Returns the flag bits.
    pub const fn flags(self) -> RpcMetaFlags {
        RpcMetaFlags::from_meta(self.0)
    }

    /// Returns `true` if the word is zero, an untyped method.
    pub const fn is_unknown(self) -> bool {
        self.0 == 0
    }

    /// Returns the access of the method.
    pub const fn access(self) -> RpcAccess {
        self.flags().access()
    }

    /// Returns `true` if the value is saved across reboots.
    pub const fn is_persistent(self) -> bool {
        self.flags().is_persistent()
    }

    /// Returns the value type. An untyped word, a capture, or an undefined
    /// encoding is [`RpcValueType::Raw`], and a `u8` with the retired bool
    /// flag is [`RpcValueType::Bool`].
    pub const fn kind(self) -> RpcValueType {
        if self.is_unknown() || self.flags().contains(RpcMetaFlags::CAPTURE) {
            return RpcValueType::Raw { meta: self.0 };
        }
        match RpcValueType::from_low_byte(self.0 as u8) {
            Some(RpcValueType::Int {
                signed: false,
                size: 1,
            }) if self.0 & Self::LEGACY_BOOL != 0 => RpcValueType::Bool,
            Some(kind) => kind,
            None => RpcValueType::Raw { meta: self.0 },
        }
    }

    /// Returns the fixed value size in bytes, or `None` for strings and raw
    /// encodings.
    pub const fn size_bytes(self) -> Option<usize> {
        match self.kind() {
            RpcValueType::Unit => Some(0),
            RpcValueType::Int { size, .. } | RpcValueType::Float { size } => Some(size as usize),
            RpcValueType::Bool => Some(1),
            RpcValueType::Bytes { len: Some(len) } => Some(len.get() as usize),
            RpcValueType::Bytes { len: None }
            | RpcValueType::String { .. }
            | RpcValueType::Raw { .. } => None,
        }
    }
}

impl From<u16> for RpcMeta {
    fn from(bits: u16) -> Self {
        Self::from_bits(bits)
    }
}

impl From<RpcMeta> for u16 {
    fn from(meta: RpcMeta) -> Self {
        meta.bits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::strategy::{buffer, bytes, bytes_to, half, len_to, slack, write_over_any};
    use proptest::prelude::*;

    /// Size of the `{ id, method }` head of a request.
    const REQUEST_HEAD: usize = 4;
    /// Size of the `{ req_id, code }` head of an error.
    const ERROR_HEAD: usize = 4;

    /// How a request names its method, owning the name.
    #[derive(Debug, Clone)]
    enum Named {
        Id(u16),
        Name(Vec<u8>),
    }

    impl Named {
        fn method(&self) -> Method<'_> {
            match self {
                Self::Id(id) => Method::ById(RpcMethodId::new(*id)),
                Self::Name(name) => Method::ByName(name),
            }
        }

        fn name_len(&self) -> usize {
            match self {
                Self::Id(_) => 0,
                Self::Name(name) => name.len(),
            }
        }
    }

    /// A method id of 0, the largest, or any.
    fn method_id() -> impl Strategy<Value = u16> {
        prop_oneof![Just(0), Just(RpcMethodId::MAX), 0..=RpcMethodId::MAX]
    }

    /// A method by id, or by a name whose length [`len_to`] draws up to
    /// `max_name`.
    fn named(max_name: usize) -> impl Strategy<Value = Named> {
        prop_oneof![
            method_id().prop_map(Named::Id),
            bytes_to(max_name).prop_map(Named::Name),
        ]
    }

    /// A request id of 0, the largest, or any.
    fn req_id() -> impl Strategy<Value = RpcRequestId> {
        half().prop_map(RpcRequestId::new)
    }

    /// An error code, a third of them the codes [`RpcError`] names and the
    /// first it does not.
    fn code() -> impl Strategy<Value = u16> {
        prop_oneof![2 => half(), 1 => 0..=18u16]
    }

    /// The head of a request payload and its body. The head names a method
    /// by id, or by a name of 0, 1, the longest a packet carries, the longest
    /// the method word measures, or any length. The body holds the name, a
    /// byte short of it, or a few bytes over.
    fn request_payload() -> impl Strategy<Value = Vec<u8>> {
        let word = prop_oneof![
            method_id(),
            prop_oneof![
                Just(0),
                Just(1),
                Just(Packet::MAX_PAYLOAD - REQUEST_HEAD),
                Just(NAMELEN_MASK as usize),
                0..=40usize,
            ]
            .prop_map(|len| REQUEST_BY_NAME | len as u16),
        ];
        let head = (half(), word, prop_oneof![Just(-1), Just(0), 1..=8isize]);
        prop_oneof![
            1 => (0..REQUEST_HEAD).prop_flat_map(bytes),
            4 => head.prop_flat_map(|(id, word, over)| {
                let name_len = match word & REQUEST_BY_NAME {
                    0 => 0,
                    _ => (word & NAMELEN_MASK) as usize,
                };
                let body = name_len.checked_add_signed(over).unwrap_or(0);
                bytes(body).prop_map(move |body| {
                    [&id.to_le_bytes()[..], &word.to_le_bytes(), &body].concat()
                })
            }),
        ]
    }

    /// A payload of 0 to 5 bytes, around the heads of a reply and an error,
    /// or of any length up to the largest.
    fn answer_payload() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            (0..=5usize).prop_flat_map(bytes),
            bytes_to(Packet::MAX_PAYLOAD)
        ]
    }

    proptest! {
        /// A request that fits a packet writes an `RPC_REQ` packet of exactly
        /// its length with no routing, whose payload parses back to it. The
        /// payload alone is written the same. Both are the same whatever the
        /// buffer held before.
        #[test]
        fn a_request_decodes_as_it_was_encoded(
            id in req_id(),
            (named, args) in named(Packet::MAX_PAYLOAD - REQUEST_HEAD).prop_flat_map(|named| {
                let room = Packet::MAX_PAYLOAD - REQUEST_HEAD - named.name_len();
                (Just(named), bytes_to(room))
            }),
        ) {
            let method = named.method();
            let payload_len = REQUEST_HEAD + named.name_len() + args.len();
            prop_assert_eq!(request_payload_len(method, &args), Some(payload_len));
            let (len, buf) =
                write_over_any(Header::SIZE + payload_len, |buf| write_request(buf, id, method, &args))?;
            prop_assert_eq!(len, Some(buf.len()));
            let header = Header::parse(buf[..Header::SIZE].try_into().unwrap()).unwrap();
            prop_assert_eq!(header, Header::new(PacketType::RPC_REQ, payload_len as u16));
            let request = Request::parse(&buf[Header::SIZE..]).unwrap();
            prop_assert_eq!(request.id, id);
            prop_assert_eq!(request.method, method);
            prop_assert_eq!(request.args, &args[..]);
            let (len, payload) =
                write_over_any(payload_len, |out| write_request_payload(out, id, method, &args))?;
            prop_assert_eq!(len, Some(payload_len));
            prop_assert_eq!(&payload[..], &buf[Header::SIZE..]);
        }

        /// A request payload parses when it holds the four-byte head and the
        /// name the method word measures. Writing what was parsed gives back
        /// the same payload, whatever the buffer held before.
        #[test]
        fn any_request_payload_parses_when_it_holds_its_name(payload in request_payload()) {
            let Some(request) = Request::parse(&payload) else {
                let holds = payload.len() >= REQUEST_HEAD && {
                    let word = u16::from_le_bytes([payload[2], payload[3]]);
                    word & REQUEST_BY_NAME == 0
                        || payload.len() >= REQUEST_HEAD + (word & NAMELEN_MASK) as usize
                };
                prop_assert!(!holds);
                return Ok(());
            };
            prop_assert!(payload.len() >= REQUEST_HEAD);
            let (len, written) = write_over_any(payload.len(), |out| {
                write_request_payload(out, request.id, request.method, request.args)
            })?;
            prop_assert_eq!(len, Some(payload.len()));
            prop_assert_eq!(written, payload);
        }

        /// A request is written when its name is at most [`NAMELEN_MASK`]
        /// bytes, its payload at most [`Packet::MAX_PAYLOAD`] bytes, and the
        /// buffer holds the packet. The payload alone is written when the name
        /// is at most [`NAMELEN_MASK`] bytes and the buffer holds the payload.
        #[test]
        fn a_request_is_written_only_when_it_fits(
            (name_len, args_len) in prop_oneof![
                len_to(Packet::MAX_PAYLOAD - REQUEST_HEAD + 1),
                Just(NAMELEN_MASK as usize),
                Just(NAMELEN_MASK as usize + 1),
            ]
            .prop_flat_map(|name_len| {
                let room = Packet::MAX_PAYLOAD.saturating_sub(REQUEST_HEAD + name_len);
                (Just(name_len), prop_oneof![Just(room), Just(room + 1), 0..=room + 1])
            }),
            by_id in any::<bool>(),
            slack in slack(),
        ) {
            let name = vec![b'n'; name_len];
            let args = vec![0u8; args_len];
            let (method, name_len) = match by_id {
                true => (Method::ById(RpcMethodId::new(RpcMethodId::MAX)), 0),
                false => (Method::ByName(&name), name_len),
            };
            let id = RpcRequestId::new(1);
            let payload_len = REQUEST_HEAD + name_len + args_len;
            let named = name_len <= NAMELEN_MASK as usize;
            prop_assert_eq!(request_payload_len(method, &args), named.then_some(payload_len));

            let mut buf = buffer(Header::SIZE + payload_len, slack);
            let fits = named && payload_len <= Packet::MAX_PAYLOAD
                && buf.len() >= Header::SIZE + payload_len;
            prop_assert_eq!(
                write_request(&mut buf, id, method, &args),
                fits.then_some(Header::SIZE + payload_len)
            );
            let mut out = buffer(payload_len, slack);
            let fits = named && out.len() >= payload_len;
            prop_assert_eq!(
                write_request_payload(&mut out, id, method, &args),
                fits.then_some(payload_len)
            );
        }

        /// A reply that fits a packet writes an `RPC_REP` packet of exactly
        /// its length with no routing, whose payload parses back to it as a
        /// reply and as the answer its packet type names. The payload alone is
        /// written the same. Both are the same whatever the buffer held
        /// before.
        #[test]
        fn a_reply_decodes_as_it_was_encoded(
            req_id in req_id(),
            value in bytes_to(Packet::MAX_PAYLOAD - REPLY_HEADER_SIZE),
        ) {
            let payload_len = REPLY_HEADER_SIZE + value.len();
            let (len, buf) =
                write_over_any(Header::SIZE + payload_len, |buf| write_reply(buf, req_id, &value))?;
            prop_assert_eq!(len, Some(buf.len()));
            let header = Header::parse(buf[..Header::SIZE].try_into().unwrap()).unwrap();
            prop_assert_eq!(header, Header::new(PacketType::RPC_REP, payload_len as u16));
            let reply = Reply { req_id, value: &value };
            prop_assert_eq!(Reply::parse(&buf[Header::SIZE..]), Some(reply));
            let answer = Answer::parse(header.ptype, &buf[Header::SIZE..]);
            prop_assert_eq!(answer, Some(Answer::Reply(reply)));
            prop_assert_eq!(answer.unwrap().req_id(), req_id);
            let (len, payload) =
                write_over_any(payload_len, |out| write_reply_payload(out, req_id, &value))?;
            prop_assert_eq!(len, Some(payload_len));
            prop_assert_eq!(&payload[..], &buf[Header::SIZE..]);
        }

        /// An error with any code and a message that fits a packet writes an
        /// `RPC_ERROR` packet of exactly its length with no routing, whose
        /// payload parses back to it as an error and as the answer its packet
        /// type names. The payload alone is written the same, and an error
        /// without a message is written the same with or without one given.
        /// Each is the same whatever the buffer held before.
        #[test]
        fn an_error_decodes_as_it_was_encoded(
            req_id in req_id(),
            code in code(),
            message in bytes_to(Packet::MAX_PAYLOAD - ERROR_HEAD),
        ) {
            let error = RpcError::from(code);
            prop_assert_eq!(error.value(), code);
            prop_assert_eq!(u16::from(error), code);
            let payload_len = ERROR_HEAD + message.len();
            let (len, buf) = write_over_any(Header::SIZE + payload_len, |buf| {
                write_error_with(buf, req_id, error, &message)
            })?;
            prop_assert_eq!(len, Some(buf.len()));
            let header = Header::parse(buf[..Header::SIZE].try_into().unwrap()).unwrap();
            prop_assert_eq!(header, Header::new(PacketType::RPC_ERROR, payload_len as u16));
            let parsed = ErrorReply { req_id, code, message: &message };
            prop_assert_eq!(ErrorReply::parse(&buf[Header::SIZE..]), Some(parsed));
            let answer = Answer::parse(header.ptype, &buf[Header::SIZE..]);
            prop_assert_eq!(answer, Some(Answer::Error(parsed)));
            prop_assert_eq!(answer.unwrap().req_id(), req_id);
            let (len, payload) =
                write_over_any(payload_len, |out| write_error_payload(out, req_id, code, &message))?;
            prop_assert_eq!(len, Some(payload_len));
            prop_assert_eq!(&payload[..], &buf[Header::SIZE..]);

            let (len, bare) =
                write_over_any(Header::SIZE + ERROR_HEAD, |buf| write_error(buf, req_id, error))?;
            prop_assert_eq!(len, Some(bare.len()));
            let mut with_none = [0u8; Header::SIZE + ERROR_HEAD];
            write_error_with(&mut with_none, req_id, error, &[]).unwrap();
            prop_assert_eq!(&bare[..], &with_none[..]);
        }

        /// A reply or an error is written when its payload is at most
        /// [`Packet::MAX_PAYLOAD`] bytes and the buffer holds the packet. The
        /// payload alone is written when the buffer holds it.
        #[test]
        fn an_answer_is_written_only_when_it_fits(
            len in prop_oneof![
                len_to(Packet::MAX_PAYLOAD),
                (Packet::MAX_PAYLOAD - ERROR_HEAD - 1..=Packet::MAX_PAYLOAD - 1),
            ],
            slack in slack(),
        ) {
            let id = RpcRequestId::new(1);
            let bytes = vec![0u8; len];
            for (head, packet, payload) in [
                (
                    REPLY_HEADER_SIZE,
                    write_reply(&mut buffer(Header::SIZE + REPLY_HEADER_SIZE + len, slack), id, &bytes),
                    write_reply_payload(&mut buffer(REPLY_HEADER_SIZE + len, slack), id, &bytes),
                ),
                (
                    ERROR_HEAD,
                    write_error_with(
                        &mut buffer(Header::SIZE + ERROR_HEAD + len, slack),
                        id,
                        RpcError::Range,
                        &bytes,
                    ),
                    write_error_payload(&mut buffer(ERROR_HEAD + len, slack), id, 17, &bytes),
                ),
            ] {
                let payload_len = head + len;
                let room = slack >= 0;
                prop_assert_eq!(
                    packet,
                    (room && payload_len <= Packet::MAX_PAYLOAD).then_some(Header::SIZE + payload_len)
                );
                prop_assert_eq!(payload, room.then_some(payload_len));
            }
            prop_assert_eq!(
                write_error(&mut buffer(Header::SIZE + ERROR_HEAD, slack), id, RpcError::Range),
                (slack >= 0).then_some(Header::SIZE + ERROR_HEAD)
            );
        }

        /// A reply payload parses when it holds the two-byte request id, as
        /// that id and the rest, and an error payload when it holds the id and
        /// the code, as those and the rest. Both are little endian. Writing
        /// what was parsed gives back the same payload, whatever the buffer
        /// held before. A packet of type 3 is a reply, one of type 4 an
        /// error, and one of any other type no answer.
        #[test]
        fn any_answer_payload_parses_when_it_holds_its_head(
            payload in answer_payload(),
            ptype in prop_oneof![Just(3), Just(4), any::<u8>()]
                .prop_filter_map("a packet type", PacketType::try_new),
        ) {
            let half = |at: usize| u16::from(payload[at]) | u16::from(payload[at + 1]) << 8;
            let reply = (payload.len() >= 2).then(|| Reply {
                req_id: RpcRequestId::new(half(0)),
                value: &payload[2..],
            });
            let error = (payload.len() >= 4).then(|| ErrorReply {
                req_id: RpcRequestId::new(half(0)),
                code: half(2),
                message: &payload[4..],
            });
            prop_assert_eq!(Reply::parse(&payload), reply);
            prop_assert_eq!(ErrorReply::parse(&payload), error);
            if let Some(reply) = reply {
                let (len, written) = write_over_any(payload.len(), |out| {
                    write_reply_payload(out, reply.req_id, reply.value)
                })?;
                prop_assert_eq!(len, Some(payload.len()));
                prop_assert_eq!(&written, &payload);
            }
            if let Some(error) = error {
                let (len, written) = write_over_any(payload.len(), |out| {
                    write_error_payload(out, error.req_id, error.code, error.message)
                })?;
                prop_assert_eq!(len, Some(payload.len()));
                prop_assert_eq!(&written, &payload);
            }
            let answer = Answer::parse(ptype, &payload);
            if ptype.value() == 3 {
                prop_assert_eq!(answer, reply.map(Answer::Reply));
            } else if ptype.value() == 4 {
                prop_assert_eq!(answer, error.map(Answer::Error));
            } else {
                prop_assert_eq!(answer, None);
            }
        }

        /// Replacing the request id of a payload of two bytes or more changes
        /// its first two bytes to the id and leaves the rest. A shorter
        /// payload is left as it is and refused.
        #[test]
        fn a_request_id_is_replaced_without_moving_anything_else(
            payload in answer_payload(),
            id in req_id(),
        ) {
            let mut replaced = payload.clone();
            prop_assert_eq!(set_req_id(&mut replaced, id), payload.len() >= 2);
            if payload.len() >= 2 {
                prop_assert_eq!(&replaced[..2], &id.to_le_bytes());
                prop_assert_eq!(&replaced[2..], &payload[2..]);
            } else {
                prop_assert_eq!(replaced, payload);
            }
        }
    }

    #[test]
    fn reply_layout() {
        let mut buf = [0u8; 32];
        let n = write_reply(&mut buf, RpcRequestId::new(0x1234), b"ok").unwrap();
        assert_eq!(&buf[..n], &[3, 0, 4, 0, 0x34, 0x12, b'o', b'k']);
        let n = write_error(&mut buf, RpcRequestId::new(1), RpcError::NotFound).unwrap();
        assert_eq!(&buf[..n], &[4, 0, 4, 0, 1, 0, 2, 0]);
        let n = write_error_with(&mut buf, RpcRequestId::new(1), RpcError::Range, b"no").unwrap();
        assert_eq!(&buf[..n], &[4, 0, 6, 0, 1, 0, 17, 0, b'n', b'o']);
    }

    /// The request layout on the wire: header, id, `BY_NAME | len`, the
    /// name, then the argument, and no routing.
    #[test]
    fn request_layout() {
        let mut buf = [0u8; 32];
        let n = write_request(
            &mut buf,
            RpcRequestId::new(0x1234),
            Method::ByName(b"dev.name"),
            b"x",
        )
        .unwrap();
        assert_eq!(
            &buf[..n],
            &[
                2, 0, 13, 0, // RPC_REQ, no routing, 13-byte payload
                0x34, 0x12, // request id
                8, 0x80, // BY_NAME | 8
                b'd', b'e', b'v', b'.', b'n', b'a', b'm', b'e', b'x',
            ]
        );

        let n = write_request(
            &mut buf,
            RpcRequestId::new(1),
            Method::ById(RpcMethodId::new(4)),
            &[],
        )
        .unwrap();
        assert_eq!(&buf[..n], &[2, 0, 4, 0, 1, 0, 4, 0]);
    }

    /// A code this crate has no name for still reaches the caller: everything
    /// from `TL_RPC_ERROR_USER` = 18 up is defined per RPC.
    #[test]
    fn an_unnamed_error_code_survives_parsing() {
        let payload = [7, 0, 20, 0, b'w', b'h', b'y'];
        let error = ErrorReply::parse(&payload).unwrap();
        assert_eq!(error.code, 20);
        assert_eq!(error.error(), RpcError::Unknown(20));
        assert_eq!(error.message, b"why");
    }

    /// Codes are the wire values, and survive a round trip whether or not
    /// this crate names them.
    #[test]
    fn error_codes_match_the_wire_values() {
        let codes = [
            (0, RpcError::None),
            (1, RpcError::Undefined),
            (2, RpcError::NotFound),
            (3, RpcError::Malformed),
            (4, RpcError::ArgsSize),
            (5, RpcError::Invalid),
            (6, RpcError::ReadOnly),
            (7, RpcError::WriteOnly),
            (8, RpcError::Timeout),
            (9, RpcError::Busy),
            (10, RpcError::State),
            (11, RpcError::Load),
            (12, RpcError::LoadRpc),
            (13, RpcError::Save),
            (14, RpcError::SaveWrite),
            (15, RpcError::Internal),
            (16, RpcError::NoBufs),
            (17, RpcError::Range),
            // TL_RPC_ERROR_USER and above are defined per RPC.
            (18, RpcError::Unknown(18)),
            (0xFFFF, RpcError::Unknown(0xFFFF)),
        ];
        for (code, error) in codes {
            assert_eq!(RpcError::from(code), error);
            assert_eq!(error.value(), code);
            assert_eq!(u16::from(error), code);
        }
    }

    #[test]
    fn metadata_matches_the_wire_encoding() {
        assert_eq!(
            RpcMeta::fixed_uint(2, RpcMetaFlags::READABLE).bits(),
            0x8120
        );
        assert_eq!(RpcMeta::string(RpcMetaFlags::READABLE).bits(), 0x8103);
        assert_eq!(RpcMeta::ACTION.bits(), 0x8000);
        assert_eq!(RpcMeta::ANY.bits(), 0);
        assert_eq!(
            RpcMeta::from_bits(0x8120).kind(),
            RpcValueType::Int {
                signed: false,
                size: 2
            }
        );
        assert_eq!(RpcMeta::from_bits(0x8120).access(), RpcAccess::ReadOnly);
        assert_eq!(RpcMeta::from_bits(0x8120).size_bytes(), Some(2));
    }

    /// A byte array is type 5 with its exact length in the size nibble, or
    /// zero for a length that varies.
    #[test]
    fn bytes_carry_their_length_or_none() {
        let uid = RpcValueType::Bytes {
            len: RpcStringLen::new(12),
        };
        let meta = RpcMeta::typed(uid, RpcMetaFlags::READABLE);
        assert_eq!(meta.bits(), 0x81c5);
        assert_eq!(meta.kind(), uid);
        assert_eq!(meta.size_bytes(), Some(12));
        let any = RpcMeta::from_bits(0x8105);
        assert_eq!(any.kind(), RpcValueType::Bytes { len: None });
        assert_eq!(any.size_bytes(), None);
    }

    /// A bool is its own type code; the retired `0x0800` flag on a `u8` still
    /// reads as one, and on anything else is ignored.
    #[test]
    fn a_bool_is_a_type_and_the_retired_flag_still_decodes() {
        let flag = RpcMeta::typed(RpcValueType::Bool, RpcMetaFlags::READABLE);
        assert_eq!(flag.bits(), 0x8114);
        assert_eq!(flag.kind(), RpcValueType::Bool);
        assert_eq!(flag.size_bytes(), Some(1));
        assert_eq!(RpcMeta::from_bits(0x8b10).kind(), RpcValueType::Bool);
        assert_eq!(
            RpcMeta::from_bits(0x8b20).kind(),
            RpcValueType::Int {
                signed: false,
                size: 2
            }
        );
        assert_eq!(
            RpcMeta::from_bits(0x8114 | 0x2000 | 0x4000).flags(),
            RpcMetaFlags::READABLE
                | RpcMetaFlags::ANNOUNCED
                | RpcMetaFlags::PRIVILEGED
                | RpcMetaFlags::from_meta(0x8000)
        );
    }
}

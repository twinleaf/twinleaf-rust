//! RPC request/reply/error wire formats
//!
//! Request payload: `{ id: u16le, method: u16le }` then, if `method & 0x8000`,
//! a method-name string of `method & 0x7FFF` bytes (not NUL-terminated),
//! then the argument bytes.
//! Reply payload: `{ req_id: u16le }` then the return value bytes.
//! Error payload: `{ req_id: u16le, code: u16le }` then an optional message.
//!
//! Both directions live here: a device answers requests, and a hub *asks* them
//! of its own children. [`write_request`] and [`Answer`] are that second half.
//!
//! [`parse_update`] and [`write_update`] handle the proxy's RPC_UPDATE packet,
//! which names a method without calling it. A device's own announcement that a
//! setting changed is a different packet, in [`crate::settings`].

use crate::packet::{Header, PacketType};
use crate::{RpcMethodId, RpcRequestId, HEADER_SIZE};

pub const REQUEST_BY_NAME: u16 = 0x8000;
pub const NAMELEN_MASK: u16 = 0x7FFF;

/// `[request id u16le]` preceding a reply's value.
pub const REPLY_HEADER_SIZE: usize = 2;

/// Every error code an RPC can answer with (`TL_RPC_ERROR_*`).
///
/// [`Self::Unknown`] is part of the protocol, not a decoding fallback:
/// `TL_RPC_ERROR_USER` = 18 reserves every code from 18 up for
/// per-RPC definitions this crate cannot name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum RpcError {
    #[error("no error")]
    None,
    #[error("undefined error")]
    Undefined,
    #[error("RPC not found")]
    NotFound,
    #[error("malformed request")]
    Malformed,
    #[error("wrong size args")]
    ArgsSize,
    #[error("invalid arguments")]
    Invalid,
    #[error("read-only")]
    ReadOnly,
    #[error("write-only")]
    WriteOnly,
    #[error("timeout")]
    Timeout,
    #[error("device busy")]
    Busy,
    #[error("wrong device state")]
    State,
    #[error("load failed")]
    Load,
    #[error("load RPC failed")]
    LoadRpc,
    #[error("save failed")]
    Save,
    #[error("save write failed")]
    SaveWrite,
    #[error("internal error")]
    Internal,
    #[error("out of memory")]
    NoBufs,
    #[error("out of range")]
    Range,
    #[error("unknown error code {0}")]
    Unknown(u16),
}

impl RpcError {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method<'a> {
    ById(RpcMethodId),
    ByName(&'a [u8]),
}

#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub id: RpcRequestId,
    pub method: Method<'a>,
    pub args: &'a [u8],
}

impl<'a> Request<'a> {
    /// Parse an RPC request from a packet payload (header excluded).
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

/// Serialize a full request packet (header included) into `buf`; returns
/// length. `None` if `buf` is too small or the payload overflows a packet.
///
/// The packet carries no routing — `routing_size_and_ttl` is zero — leaving
/// addressing to the caller: a hub pushes the hops it wants afterwards, or
/// hands the packet straight to the child port that is the only hop.
pub fn write_request(
    buf: &mut [u8],
    id: RpcRequestId,
    method: Method<'_>,
    args: &[u8],
) -> Option<usize> {
    let payload_len = request_payload_len(method, args)?;
    let total = HEADER_SIZE + payload_len;
    if buf.len() < total || payload_len > crate::MAX_PAYLOAD_SIZE {
        return None;
    }
    let hdr = Header::new(PacketType::RPC_REQ, payload_len as u16);
    hdr.write((&mut buf[..HEADER_SIZE]).try_into().unwrap());
    write_request_payload(&mut buf[HEADER_SIZE..total], id, method, args)?;
    Some(total)
}

/// Payload length of a request; `None` when the name overflows its field.
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

/// Write just the request payload (no header) into `out`; returns its length.
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

/// Overwrite the request id an RPC payload starts with, returning whether
/// there was room for one.
///
/// Requests, replies, and errors all lead with the id, so a hub that remaps
/// the ids it forwards downstream and restores them on the way back edits both
/// directions through this one writer. Nothing else in the payload moves.
pub fn set_req_id(payload: &mut [u8], id: RpcRequestId) -> bool {
    let Some(field) = payload.get_mut(..2) else {
        return false;
    };
    field.copy_from_slice(&id.to_le_bytes());
    true
}

/// A successful answer: the request it belongs to and the returned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reply<'a> {
    pub req_id: RpcRequestId,
    pub value: &'a [u8],
}

impl<'a> Reply<'a> {
    /// Parse an RPC reply from a packet payload (header excluded).
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        let (req_id, value) = payload.split_at_checked(2)?;
        Some(Self {
            req_id: RpcRequestId::from_le_bytes([req_id[0], req_id[1]]),
            value,
        })
    }
}

/// Serialize a full reply packet (header included) into `buf`; returns length.
/// Returns None if `buf` is too small.
pub fn write_reply(buf: &mut [u8], req_id: RpcRequestId, value: &[u8]) -> Option<usize> {
    let payload_len = 2 + value.len();
    let total = HEADER_SIZE + payload_len;
    if buf.len() < total || payload_len > u16::MAX as usize {
        return None;
    }
    let hdr = Header::new(PacketType::RPC_REP, payload_len as u16);
    hdr.write((&mut buf[..HEADER_SIZE]).try_into().unwrap());
    write_reply_payload(&mut buf[HEADER_SIZE..total], req_id, value)?;
    Some(total)
}

/// Write just the reply payload (no header) into `out`; returns its length.
pub fn write_reply_payload(out: &mut [u8], req_id: RpcRequestId, value: &[u8]) -> Option<usize> {
    let len = 2 + value.len();
    if out.len() < len {
        return None;
    }
    out[0..2].copy_from_slice(&req_id.to_le_bytes());
    out[2..len].copy_from_slice(value);
    Some(len)
}

/// A refused request: the code, kept raw, and whatever message followed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorReply<'a> {
    pub req_id: RpcRequestId,
    pub code: u16,
    pub message: &'a [u8],
}

impl<'a> ErrorReply<'a> {
    /// Parse an RPC error from a packet payload (header excluded).
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        let (head, message) = payload.split_at_checked(4)?;
        Some(Self {
            req_id: RpcRequestId::from_le_bytes([head[0], head[1]]),
            code: u16::from_le_bytes([head[2], head[3]]),
            message,
        })
    }

    /// The error this reply names.
    pub fn error(&self) -> RpcError {
        RpcError::from(self.code)
    }
}

/// Serialize a full error packet (header included) into `buf`; returns length.
pub fn write_error(buf: &mut [u8], req_id: RpcRequestId, code: RpcError) -> Option<usize> {
    let total = HEADER_SIZE + 4;
    if buf.len() < total {
        return None;
    }
    let hdr = Header::new(PacketType::RPC_ERROR, 4);
    hdr.write((&mut buf[..HEADER_SIZE]).try_into().unwrap());
    write_error_payload(&mut buf[HEADER_SIZE..total], req_id, code.value(), &[])?;
    Some(total)
}

/// Write just the error payload (no header) into `out`; returns its length.
/// The code is raw so a hub can forward codes this crate has no name for.
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

/// Either kind of answer to a request, told apart by the packet type.
///
/// A client waiting on one request cares only which request answered and
/// whether it succeeded; both layouts start with the same `req_id`, which is
/// what lets a hub read one field off either packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer<'a> {
    Reply(Reply<'a>),
    Error(ErrorReply<'a>),
}

impl<'a> Answer<'a> {
    /// Parse a payload as the answer its packet type says it is. `None` for
    /// any other packet type, or a payload too short to name a request.
    pub fn parse(ptype: PacketType, payload: &'a [u8]) -> Option<Self> {
        match ptype {
            PacketType::RPC_REP => Reply::parse(payload).map(Self::Reply),
            PacketType::RPC_ERROR => ErrorReply::parse(payload).map(Self::Error),
            _ => None,
        }
    }

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

impl RpcMetaFlags {
    pub const READABLE: Self = Self(0x0100);
    pub const WRITABLE: Self = Self(0x0200);
    pub const PERSISTENT: Self = Self(0x0400);
    pub const BOOL: Self = Self(0x0800);
    pub const CAPTURE: Self = Self(0x1000);

    // Preserve unknown flag bits when decoding.
    const MASK: u16 = 0xff00;

    pub const fn from_meta(meta: u16) -> Self {
        Self(meta & Self::MASK)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
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

impl core::ops::BitOr for RpcMetaFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcAccess {
    ReadWrite,
    ReadOnly,
    WriteOnly,
    Action,
}

/// Bounded string length, as encoded in the metadata size nibble.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct RpcStringLen(u8);

impl RpcStringLen {
    pub const MAX: u8 = 0x0f;

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

impl core::fmt::Display for RpcStringLen {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

/// Value kind encoded in `rpc.info`.
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
            _ => None,
        }
    }

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
            Self::Raw { meta } => (meta & 0x00ff) as u8,
        }
    }
}

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

    /// Variable-size or otherwise unspecified RPC metadata.
    pub const ANY: Self = Self(0);
    /// Void action RPC: no readable/writable flags.
    pub const ACTION: Self = Self(Self::TYPED);

    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    /// Construct typed metadata, including the legacy type marker.
    pub const fn new(kind: RpcValueType, flags: RpcMetaFlags) -> Self {
        Self::typed(kind, flags)
    }

    pub const fn typed(kind: RpcValueType, flags: RpcMetaFlags) -> Self {
        Self(Self::TYPED | kind.low_byte() as u16 | flags.bits())
    }

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

    pub const fn string(flags: RpcMetaFlags) -> Self {
        Self::typed(RpcValueType::String { max_len: None }, flags)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn to_le_bytes(self) -> [u8; 2] {
        self.0.to_le_bytes()
    }

    pub const fn flags(self) -> RpcMetaFlags {
        RpcMetaFlags::from_meta(self.0)
    }

    pub const fn is_unknown(self) -> bool {
        self.0 == 0
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

    #[test]
    fn parse_by_name() {
        // id=7, method = BY_NAME | 8, "dev.name", no args
        let mut p = alloc_req(7, REQUEST_BY_NAME | 8);
        p.extend_from_slice(b"dev.name");
        let r = Request::parse(&p).unwrap();
        assert_eq!(r.id, RpcRequestId::new(7));
        assert_eq!(r.method, Method::ByName(b"dev.name"));
        assert!(r.args.is_empty());
    }

    #[test]
    fn parse_by_id_with_args() {
        let mut p = alloc_req(1, 4);
        p.extend_from_slice(&3u16.to_le_bytes());
        let r = Request::parse(&p).unwrap();
        assert_eq!(r.method, Method::ById(RpcMethodId::new(4)));
        assert_eq!(r.args, &3u16.to_le_bytes());
    }

    #[test]
    fn reply_layout() {
        let mut buf = [0u8; 32];
        let n = write_reply(&mut buf, RpcRequestId::new(0x1234), b"ok").unwrap();
        assert_eq!(&buf[..n], &[3, 0, 4, 0, 0x34, 0x12, b'o', b'k']);
        let n = write_error(&mut buf, RpcRequestId::new(1), RpcError::NotFound).unwrap();
        assert_eq!(&buf[..n], &[4, 0, 4, 0, 1, 0, 2, 0]);
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

    /// What the hub builds is what a device parses, both ways of naming a
    /// method.
    #[test]
    fn a_built_request_round_trips_through_the_server_parser() {
        let mut buf = [0u8; 64];

        let n = write_request(
            &mut buf,
            RpcRequestId::new(9),
            Method::ByName(b"dev.port.rate"),
            &1_843_200u32.to_le_bytes(),
        )
        .unwrap();
        let header = Header::parse((&buf[..HEADER_SIZE]).try_into().unwrap()).unwrap();
        assert_eq!(header.ptype, PacketType::RPC_REQ);
        assert_eq!(header.routing_size, 0, "the caller addresses it");
        assert_eq!(header.packet_len(), n);

        let request = Request::parse(&buf[header.payload_range()]).unwrap();
        assert_eq!(request.id, RpcRequestId::new(9));
        assert_eq!(request.method, Method::ByName(b"dev.port.rate"));
        assert_eq!(request.args, &1_843_200u32.to_le_bytes());

        let n = write_request(
            &mut buf,
            RpcRequestId::new(0xF001),
            Method::ById(RpcMethodId::new(0x7FFF)),
            b"args",
        )
        .unwrap();
        let request = Request::parse(&buf[HEADER_SIZE..n]).unwrap();
        assert_eq!(request.id, RpcRequestId::new(0xF001));
        assert_eq!(request.method, Method::ById(RpcMethodId::new(0x7FFF)));
        assert_eq!(request.args, b"args");
    }

    #[test]
    fn a_request_that_does_not_fit_is_refused_rather_than_truncated() {
        let mut small = [0u8; 8];
        assert_eq!(
            write_request(
                &mut small,
                RpcRequestId::new(1),
                Method::ByName(b"dev.name"),
                &[]
            ),
            None
        );
        let mut big = [0u8; crate::MAX_PACKET_SIZE];
        assert_eq!(
            write_request(
                &mut big,
                RpcRequestId::new(1),
                Method::ByName(b"x"),
                &[0; crate::MAX_PAYLOAD_SIZE]
            ),
            None,
            "payload past what a header can describe"
        );
    }

    /// The client's parsers against the exact bytes the server side emits.
    #[test]
    fn answers_parse_what_the_server_wrote() {
        let mut buf = [0u8; 32];

        let n = write_reply(&mut buf, RpcRequestId::new(0x1234), b"ok").unwrap();
        let header = Header::parse((&buf[..HEADER_SIZE]).try_into().unwrap()).unwrap();
        let Some(Answer::Reply(reply)) = Answer::parse(header.ptype, &buf[header.payload_range()])
        else {
            panic!("a reply packet parses as a reply");
        };
        assert_eq!(reply.req_id, RpcRequestId::new(0x1234));
        assert_eq!(reply.value, b"ok");
        assert_eq!(header.packet_len(), n);

        let n = write_error(&mut buf, RpcRequestId::new(7), RpcError::NotFound).unwrap();
        let header = Header::parse((&buf[..HEADER_SIZE]).try_into().unwrap()).unwrap();
        let Some(Answer::Error(error)) = Answer::parse(header.ptype, &buf[header.payload_range()])
        else {
            panic!("an error packet parses as an error");
        };
        assert_eq!(error.req_id, RpcRequestId::new(7));
        assert_eq!(error.error(), RpcError::NotFound);
        assert!(error.message.is_empty());
        assert_eq!(header.packet_len(), n);
    }

    /// The one field a hub rewrites, on the way down and on the way back.
    #[test]
    fn a_request_id_can_be_replaced_without_moving_anything_else() {
        let mut buf = [0u8; 32];

        let n = write_request(
            &mut buf,
            RpcRequestId::new(7),
            Method::ByName(b"dev.name"),
            b"x",
        )
        .unwrap();
        assert!(set_req_id(
            &mut buf[HEADER_SIZE..n],
            RpcRequestId::new(0x1234)
        ));
        let request = Request::parse(&buf[HEADER_SIZE..n]).unwrap();
        assert_eq!(request.id, RpcRequestId::new(0x1234));
        assert_eq!(request.method, Method::ByName(b"dev.name"));
        assert_eq!(request.args, b"x");

        let n = write_reply(&mut buf, RpcRequestId::new(0x1234), b"ok").unwrap();
        assert!(set_req_id(&mut buf[HEADER_SIZE..n], RpcRequestId::new(7)));
        let reply = Reply::parse(&buf[HEADER_SIZE..n]).unwrap();
        assert_eq!(reply.req_id, RpcRequestId::new(7));
        assert_eq!(reply.value, b"ok");

        assert!(!set_req_id(&mut [0; 1], RpcRequestId::new(1)), "no id here");
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
    fn a_truncated_answer_does_not_parse() {
        assert_eq!(Reply::parse(&[1]), None);
        assert_eq!(ErrorReply::parse(&[1, 0, 2]), None);
        assert_eq!(Answer::parse(PacketType::HEARTBEAT, &[1, 0]), None);
    }

    fn alloc_req(id: u16, method: u16) -> std::vec::Vec<u8> {
        let mut v = std::vec::Vec::new();
        v.extend_from_slice(&id.to_le_bytes());
        v.extend_from_slice(&method.to_le_bytes());
        v
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
}

/// RPC_UPDATE payload: `[method type u8]` then, for [`UPDATE_BY_ID`], the
/// method's `[id u16le]`, and for [`UPDATE_BY_NAME`], its
/// `[name len u16le][name bytes]` — a name neither NUL-terminated nor padded.
/// Nothing follows: the packet names a method, it does not carry a value.
///
/// Host-side invention; no device sends it, only a proxy.
/// When one client's RPC changes a method, the proxy tells the other clients
/// which method moved so they can re-read it, addressing it exactly as the
/// request did. A device never sees the packet, which is why its method field
/// is spelled out rather than packed into the request's `method` word.
pub const UPDATE_BY_ID: u8 = 0;
pub const UPDATE_BY_NAME: u8 = 1;
/// `[method type][id]`, or `[method type][name len]` before the name.
pub const UPDATE_HEADER_SIZE: usize = 3;

/// Parse the method an RPC_UPDATE payload names (packet header excluded).
/// `None` for an unknown method type, a truncated payload, or an id with the
/// by-name bit set.
pub fn parse_update(payload: &[u8]) -> Option<Method<'_>> {
    let (&method_type, rest) = payload.split_first()?;
    let (field, rest) = rest.split_at_checked(2)?;
    let field = u16::from_le_bytes([field[0], field[1]]);
    match method_type {
        UPDATE_BY_ID => Some(Method::ById(RpcMethodId::try_new(field)?)),
        UPDATE_BY_NAME => Some(Method::ByName(rest.get(..field as usize)?)),
        _ => None,
    }
}

/// Payload length of an update; `None` when the name overflows its field or a
/// packet.
pub fn update_payload_len(method: Method<'_>) -> Option<usize> {
    let len = match method {
        Method::ById(_) => UPDATE_HEADER_SIZE,
        Method::ByName(name) => UPDATE_HEADER_SIZE.checked_add(name.len())?,
    };
    (len <= crate::MAX_PAYLOAD_SIZE).then_some(len)
}

/// Serialize a full RPC_UPDATE packet (header included) into `buf`; returns
/// its length. `None` if `buf` is too small or the payload overflows a packet.
///
/// Like a request, the packet carries no routing: the proxy pushes the hops of
/// the device whose method changed before sending it on.
pub fn write_update(buf: &mut [u8], method: Method<'_>) -> Option<usize> {
    let payload_len = update_payload_len(method)?;
    let total = HEADER_SIZE + payload_len;
    if buf.len() < total {
        return None;
    }
    let hdr = Header::new(PacketType::RPC_UPDATE, payload_len as u16);
    hdr.write((&mut buf[..HEADER_SIZE]).try_into().unwrap());
    write_update_payload(&mut buf[HEADER_SIZE..total], method)?;
    Some(total)
}

/// Write just the update payload (no header) into `out`; returns its length.
pub fn write_update_payload(out: &mut [u8], method: Method<'_>) -> Option<usize> {
    let len = update_payload_len(method)?;
    if out.len() < len {
        return None;
    }
    let (method_type, field, name) = match method {
        Method::ById(id) => (UPDATE_BY_ID, id.value(), &[][..]),
        Method::ByName(name) => (UPDATE_BY_NAME, name.len() as u16, name),
    };
    out[0] = method_type;
    out[1..UPDATE_HEADER_SIZE].copy_from_slice(&field.to_le_bytes());
    out[UPDATE_HEADER_SIZE..len].copy_from_slice(name);
    Some(len)
}

#[cfg(test)]
mod update_tests {
    use super::*;

    #[test]
    fn round_trip() {
        for method in [
            Method::ById(RpcMethodId::new(0x1234)),
            Method::ByName(b"dev.name"),
            Method::ByName(b""),
        ] {
            let mut buf = [0u8; 64];
            let len = write_update(&mut buf, method).unwrap();
            assert_eq!(parse_update(&buf[HEADER_SIZE..len]), Some(method));
        }
    }

    /// Byte-exact layout of both method forms behind an RPC_UPDATE header
    /// with no routing. Host-side format; this test is the definition.
    #[test]
    fn wire_layout() {
        let mut buf = [0u8; 32];

        let len = write_update(&mut buf, Method::ByName(b"dev.name")).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            65, 0, 11, 0,       // header: RPC_UPDATE, no routing, payload 11
            1,                  // by name
            8, 0,               // name length, little-endian
            b'd', b'e', b'v', b'.', b'n', b'a', b'm', b'e',
        ]);

        let len = write_update(&mut buf, Method::ById(RpcMethodId::new(4))).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            65, 0, 3, 0,        // header: RPC_UPDATE, no routing, payload 3
            0,                  // by id
            4, 0,               // method id, little-endian
        ]);
    }

    /// The proxy addresses the updated method the way the request that changed
    /// it did, so both forms survive a round trip through a request.
    #[test]
    fn names_the_method_a_request_would_have_used() {
        let mut update = [0u8; 32];
        let len = write_update(&mut update, Method::ByName(b"dev.name")).unwrap();
        let method = parse_update(&update[HEADER_SIZE..len]).unwrap();

        let mut request = [0u8; 32];
        let len = write_request(&mut request, RpcRequestId::new(1), method, &[]).unwrap();
        assert_eq!(
            Request::parse(&request[HEADER_SIZE..len]).unwrap().method,
            Method::ByName(b"dev.name")
        );
    }

    #[test]
    fn trailing_bytes_after_an_id_or_a_name_are_ignored() {
        assert_eq!(
            parse_update(&[UPDATE_BY_ID, 4, 0, 9, 9]),
            Some(Method::ById(RpcMethodId::new(4)))
        );
        assert_eq!(
            parse_update(&[UPDATE_BY_NAME, 1, 0, b'x', 9]),
            Some(Method::ByName(b"x"))
        );
    }

    #[test]
    fn rejects_bad_and_truncated_payloads() {
        assert_eq!(parse_update(&[]), None);
        assert_eq!(parse_update(&[UPDATE_BY_ID, 4]), None, "half an id");
        assert_eq!(
            parse_update(&[UPDATE_BY_NAME, 2, 0, b'x']),
            None,
            "short name"
        );
        assert_eq!(parse_update(&[2, 0, 0]), None, "unknown method type");
        assert_eq!(
            parse_update(&[UPDATE_BY_ID, 0x00, 0x80]),
            None,
            "an id with the by-name bit set is not a method id"
        );
    }

    #[test]
    fn rejects_a_short_buffer_and_an_oversize_name() {
        let method = Method::ByName(b"dev.name");
        assert_eq!(write_update(&mut [0u8; 8], method), None);

        let max_name = crate::MAX_PAYLOAD_SIZE - UPDATE_HEADER_SIZE;
        assert_eq!(update_payload_len(Method::ByName(&[b'x'; 1])), Some(4));
        assert_eq!(
            update_payload_len(Method::ByName(&[b'x'; 498])),
            None,
            "one byte past the payload ceiling"
        );
        let mut buf = [0u8; crate::MAX_PACKET_SIZE];
        assert_eq!(
            write_update(&mut buf, Method::ByName(&[b'x'; 497])),
            Some(HEADER_SIZE + crate::MAX_PAYLOAD_SIZE)
        );
        assert_eq!(max_name, 497);
    }
}

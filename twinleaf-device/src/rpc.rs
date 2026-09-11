//! The RPC table, its `rpc.hash`, and the `rpc.*` methods that describe it.
//!
//! An entry carries the flags word libtio firmware uses, since `rpc.hash` is a
//! CRC over that encoding, and the wire metadata is derived from it.

use twinleaf_proto::packet::Packet;
use twinleaf_proto::rpc::{RpcError, RpcMetaFlags};
use twinleaf_proto::serial::CRC32;

/// Largest RPC reply: the payload less the two-byte request id.
pub const REPLY_MAX: usize = Packet::MAX_PAYLOAD - 2;

/// How a method is called.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    /// With a custom argument and reply format.
    Std = 1,
    /// With no argument and no reply.
    Action = 2,
    /// Read with no argument, written with a value.
    Prop = 3,
}

/// What a method's value is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Value {
    /// Undeclared.
    Any,
    /// None.
    Void,
    /// Unsigned integer of this many bytes.
    Uint(u16),
    /// Signed integer of this many bytes.
    Int(u16),
    /// Float of this many bytes.
    Float(u16),
    /// String.
    String,
}

impl Value {
    const fn bits(self) -> u32 {
        let (kind, size) = match self {
            Value::Any => (0, 0),
            Value::Void => (1, 0),
            Value::Uint(size) => (2, size),
            Value::Int(size) => (3, size),
            Value::Float(size) => (4, size),
            Value::String => (5, 0),
        };
        kind << 4 | (size as u32) << 8
    }
}

/// Who may read and write a method without privilege, and whether it persists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Access(u8);

impl Access {
    /// Privileged callers only.
    pub const NONE: Self = Self(0);
    /// Readable.
    pub const READ: Self = Self(0x1);
    /// Writable.
    pub const WRITE: Self = Self(0x2);
    /// Readable and writable.
    pub const RW: Self = Self(0x3);
    /// Persisted across reboots.
    pub const PERSISTENT: Self = Self(0x20);

    /// Whether every bit of `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl core::ops::BitOr for Access {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// One entry of the table: what `rpc.*` introspection and `rpc.hash` see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcSpec {
    /// Method name.
    pub name: &'static str,
    /// How it is called.
    pub method: Method,
    /// What its value is.
    pub value: Value,
    /// Who may call it.
    pub access: Access,
    /// Description, hashed but not otherwise reported.
    pub desc: &'static str,
    /// Signature, hashed but not otherwise reported.
    pub signature: &'static str,
    /// Metadata bits the flags word cannot carry, such as bool and capture.
    pub extra_meta: u16,
}

impl RpcSpec {
    /// An entry with no description or signature.
    pub const fn new(name: &'static str, method: Method, value: Value, access: Access) -> Self {
        Self {
            name,
            method,
            value,
            access,
            desc: "",
            signature: "",
            extra_meta: 0,
        }
    }

    /// A method with a custom argument and reply format.
    pub const fn std(name: &'static str, access: Access) -> Self {
        Self::new(name, Method::Std, Value::Any, access)
    }

    /// A method with no argument and no reply.
    pub const fn action(name: &'static str) -> Self {
        Self::new(name, Method::Action, Value::Void, Access::WRITE)
    }

    /// A property read with no argument and written with a value.
    pub const fn prop(name: &'static str, value: Value, access: Access) -> Self {
        Self::new(name, Method::Prop, value, access)
    }

    /// The same entry with metadata bits beyond the flags word.
    pub const fn with_extra_meta(self, extra_meta: u16) -> Self {
        Self { extra_meta, ..self }
    }

    /// The flags word libtio firmware keeps for the entry, which `rpc.hash`
    /// covers: method kind, value type and size, then access in the top byte.
    pub const fn flags(&self) -> u32 {
        self.method as u32 | self.value.bits() | (self.access.0 as u32) << 24
    }

    /// The metadata `rpc.info` and `rpc.listinfo` report for a public caller.
    pub fn legacy_metadata(&self) -> u16 {
        let (kind, size) = match self.value {
            Value::Any => return self.extra_meta,
            Value::Void => return 0x8000 | self.extra_meta,
            Value::Uint(size) => (0, size),
            Value::Int(size) => (1, size),
            Value::Float(size) => (2, size),
            Value::String => (3, 0),
        };
        if size > 0xF {
            return self.extra_meta;
        }
        let access = [
            (Access::READ, RpcMetaFlags::READABLE),
            (Access::WRITE, RpcMetaFlags::WRITABLE),
            (Access::PERSISTENT, RpcMetaFlags::PERSISTENT),
        ]
        .into_iter()
        .filter(|(bit, _)| self.access.contains(*bit))
        .fold(0, |meta, (_, flag)| meta | flag.bits());
        0x8000 | size << 4 | kind | access | self.extra_meta
    }
}

/// `rpc.hash`: a CRC32 over each entry's name, flags, description, and
/// signature, in table order.
pub fn hash(table: &[RpcSpec]) -> u32 {
    let mut digest = CRC32.digest();
    for spec in table {
        digest.update(spec.name.as_bytes());
        digest.update(&spec.flags().to_le_bytes());
        digest.update(spec.desc.as_bytes());
        digest.update(spec.signature.as_bytes());
    }
    digest.finalize()
}

/// `rpc.name`: the name of the method at an index.
pub fn name(table: &[RpcSpec], arg: &[u8], out: &mut [u8]) -> Result<usize, RpcError> {
    let spec = table.get(index(arg)?).ok_or(RpcError::Invalid)?;
    reply(out, &[spec.name.as_bytes()])
}

/// `rpc.id`: the index of the method with a name.
pub fn id(table: &[RpcSpec], arg: &[u8], out: &mut [u8]) -> Result<usize, RpcError> {
    let index = find(table, arg)?;
    reply(out, &[&(index as u16).to_le_bytes()])
}

/// `rpc.info`: the metadata of the method with a name.
pub fn info(table: &[RpcSpec], arg: &[u8], out: &mut [u8]) -> Result<usize, RpcError> {
    let spec = &table[find(table, arg)?];
    reply(out, &[&spec.legacy_metadata().to_le_bytes()])
}

/// `rpc.list` and `rpc.listinfo`: the table size with no argument, or the name
/// of the method at an index, preceded by its metadata for `rpc.listinfo`.
pub fn list(
    table: &[RpcSpec],
    arg: &[u8],
    with_info: bool,
    out: &mut [u8],
) -> Result<usize, RpcError> {
    if arg.is_empty() {
        return reply(out, &[&(table.len() as u16).to_le_bytes()]);
    }
    let spec = table.get(index(arg)?).ok_or(RpcError::Invalid)?;
    let meta = spec.legacy_metadata().to_le_bytes();
    let parts: &[&[u8]] = if with_info {
        &[&meta, spec.name.as_bytes()]
    } else {
        &[spec.name.as_bytes()]
    };
    reply(out, parts)
}

fn index(arg: &[u8]) -> Result<usize, RpcError> {
    let bytes: [u8; 2] = arg.try_into().map_err(|_| RpcError::ArgsSize)?;
    Ok(usize::from(u16::from_le_bytes(bytes)))
}

fn find(table: &[RpcSpec], name: &[u8]) -> Result<usize, RpcError> {
    if name.is_empty() {
        return Err(RpcError::Invalid);
    }
    table
        .iter()
        .position(|spec| spec.name.as_bytes() == name)
        .ok_or(RpcError::Invalid)
}

/// Concatenate `parts` into `out` as one reply, its length on success.
pub(crate) fn reply(out: &mut [u8], parts: &[&[u8]]) -> Result<usize, RpcError> {
    let len = parts.iter().map(|part| part.len()).sum();
    if out.len() < len {
        return Err(RpcError::NoBufs);
    }
    parts.iter().fold(0, |at, part| {
        out[at..at + part.len()].copy_from_slice(part);
        at + part.len()
    });
    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> [RpcSpec; 3] {
        [
            RpcSpec::prop("rpc.hash", Value::Uint(4), Access::READ),
            RpcSpec::action("dev.stop"),
            RpcSpec::prop("test.enable", Value::Uint(1), Access::RW)
                .with_extra_meta(RpcMetaFlags::BOOL.bits()),
        ]
    }

    #[test]
    fn flags_match_the_firmware_word() {
        let [hash, stop, enable] = table();
        assert_eq!(hash.flags(), 0x0100_0423);
        assert_eq!(stop.flags(), 0x0200_0012);
        assert_eq!(enable.flags(), 0x0300_0123);
        assert_eq!(RpcSpec::std("rpc.list", Access::RW).flags(), 0x0300_0001);
        assert_eq!(
            RpcSpec::prop("dev.name", Value::String, Access::READ).flags(),
            0x0100_0053
        );
    }

    #[test]
    fn legacy_metadata_matches_firmware_encoding() {
        let spec = RpcSpec::prop("test.amplitude", Value::Float(8), Access::RW);
        assert_eq!(
            spec.legacy_metadata(),
            0x8000 | (8 << 4) | 2 | 0x0100 | 0x0200
        );

        let [hash, stop, enable] = table();
        assert_eq!(hash.legacy_metadata(), 0x8000 | (4 << 4) | 0x0100);
        assert_eq!(stop.legacy_metadata(), 0x8000);
        assert_eq!(
            enable.legacy_metadata(),
            0x8000 | (1 << 4) | 0x0100 | 0x0200 | RpcMetaFlags::BOOL.bits()
        );

        let spec = RpcSpec::std("rpc.list", Access::RW);
        assert_eq!(spec.legacy_metadata(), 0);
        let spec = RpcSpec::prop("wide", Value::Uint(16), Access::RW);
        assert_eq!(spec.legacy_metadata(), 0);
    }

    #[test]
    fn hash_covers_name_flags_desc_signature() {
        let table = table();
        let base = hash(&table);
        assert_eq!(base, hash(&table.clone()));

        let mut renamed = table.clone();
        renamed[0].name = "a.x";
        assert_ne!(base, hash(&renamed));

        let mut reflagged = table.clone();
        reflagged[1].access = reflagged[1].access | Access::PERSISTENT;
        assert_ne!(base, hash(&reflagged));

        let mut redesc = table.clone();
        redesc[0].desc = "described";
        assert_ne!(base, hash(&redesc));

        let mut resig = table;
        resig[0].signature = "u32";
        assert_ne!(base, hash(&resig));
    }

    #[test]
    fn hash_is_the_iso_hdlc_crc_of_the_entries() {
        let table = [RpcSpec::std("a", Access::NONE)];
        assert_eq!(hash(&table), CRC32.checksum(b"a\x01\0\0\0"));
    }

    #[test]
    fn introspection_answers_by_index_and_by_name() {
        let table = table();
        let mut out = [0u8; REPLY_MAX];

        assert_eq!(list(&table, &[], false, &mut out), Ok(2));
        assert_eq!(&out[..2], &3u16.to_le_bytes());

        assert_eq!(name(&table, &1u16.to_le_bytes(), &mut out), Ok(8));
        assert_eq!(&out[..8], b"dev.stop");
        assert_eq!(
            name(&table, &3u16.to_le_bytes(), &mut out),
            Err(RpcError::Invalid)
        );
        assert_eq!(name(&table, &[1], &mut out), Err(RpcError::ArgsSize));

        assert_eq!(id(&table, b"test.enable", &mut out), Ok(2));
        assert_eq!(&out[..2], &2u16.to_le_bytes());
        assert_eq!(id(&table, b"", &mut out), Err(RpcError::Invalid));
        assert_eq!(id(&table, b"nope", &mut out), Err(RpcError::Invalid));

        assert_eq!(info(&table, b"dev.stop", &mut out), Ok(2));
        assert_eq!(&out[..2], &0x8000u16.to_le_bytes());

        assert_eq!(list(&table, &2u16.to_le_bytes(), true, &mut out), Ok(13));
        assert_eq!(&out[..2], &table[2].legacy_metadata().to_le_bytes());
        assert_eq!(&out[2..13], b"test.enable");
    }

    #[test]
    fn a_reply_that_does_not_fit_is_refused() {
        let table = table();
        let mut out = [0u8; 4];
        assert_eq!(
            name(&table, &0u16.to_le_bytes(), &mut out),
            Err(RpcError::NoBufs)
        );
    }
}

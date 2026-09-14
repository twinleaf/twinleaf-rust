//! The RPC table, its `rpc.hash`, and the `rpc.*` methods that describe it.
//!
//! An entry carries the flags word libtio firmware uses, since `rpc.hash` is a
//! CRC over that encoding, and the wire metadata is derived from it.

use twinleaf_proto::packet::Packet;
use twinleaf_proto::rpc::{RpcError, RpcMetaFlags};
use twinleaf_proto::serial::CRC32;

/// Largest RPC reply: the payload less the two-byte request id.
pub const REPLY_MAX: usize = Packet::MAX_PAYLOAD - 2;

/// The value an RPC replies with.
pub type Reply = heapless::Vec<u8, REPLY_MAX>;

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

/// What kind of value a method declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
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
    /// A flag: one byte on the wire, and marked as a bool to hosts.
    Bool,
}

impl Kind {
    const fn bits(self) -> u32 {
        let (kind, size) = match self {
            Kind::Any => (0, 0),
            Kind::Void => (1, 0),
            Kind::Uint(size) => (2, size),
            Kind::Int(size) => (3, size),
            Kind::Float(size) => (4, size),
            Kind::String => (5, 0),
            Kind::Bool => (2, 1),
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

    /// Both sets of bits.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl core::ops::BitOr for Access {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        self.union(other)
    }
}

/// One entry of the table: what `rpc.*` introspection and `rpc.hash` see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcSpec {
    /// Method name.
    pub name: &'static str,
    /// How it is called.
    pub method: Method,
    /// What kind of value it declares.
    pub kind: Kind,
    /// Who may call it.
    pub access: Access,
    /// Description, hashed but not otherwise reported.
    pub desc: &'static str,
    /// Signature, hashed but not otherwise reported.
    pub signature: &'static str,
    /// Metadata bits the flags word cannot carry, such as capture.
    pub extra_meta: u16,
}

impl RpcSpec {
    /// An entry with no description or signature.
    pub const fn new(name: &'static str, method: Method, kind: Kind, access: Access) -> Self {
        Self {
            name,
            method,
            kind,
            access,
            desc: "",
            signature: "",
            extra_meta: 0,
        }
    }

    /// A method with a custom argument and reply format.
    pub const fn std(name: &'static str, access: Access) -> Self {
        Self::new(name, Method::Std, Kind::Any, access)
    }

    /// A method with no argument and no reply.
    pub const fn action(name: &'static str) -> Self {
        Self::new(name, Method::Action, Kind::Void, Access::WRITE)
    }

    /// A property read with no argument and written with a value.
    pub const fn prop(name: &'static str, kind: Kind, access: Access) -> Self {
        Self::new(name, Method::Prop, kind, access)
    }

    /// The same entry with metadata bits beyond the flags word.
    pub const fn with_extra_meta(self, extra_meta: u16) -> Self {
        Self { extra_meta, ..self }
    }

    /// The flags word libtio firmware keeps for the entry, which `rpc.hash`
    /// covers: method kind, value type and size, then access in the top byte.
    pub const fn flags(&self) -> u32 {
        self.method as u32 | self.kind.bits() | (self.access.0 as u32) << 24
    }

    /// The metadata `rpc.info` and `rpc.listinfo` report for a public caller.
    pub fn legacy_metadata(&self) -> u16 {
        let (kind, size, marks) = match self.kind {
            Kind::Any => return self.extra_meta,
            Kind::Void => return 0x8000 | self.extra_meta,
            Kind::Uint(size) => (0, size, 0),
            Kind::Int(size) => (1, size, 0),
            Kind::Float(size) => (2, size, 0),
            Kind::String => (3, 0, 0),
            Kind::Bool => (0, 1, RpcMetaFlags::BOOL.bits()),
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
        0x8000 | size << 4 | kind | access | marks | self.extra_meta
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
pub fn name(table: &[RpcSpec], arg: &[u8], out: &mut Reply) -> Result<(), RpcError> {
    let spec = table.get(index(arg)?).ok_or(RpcError::Invalid)?;
    put(out, spec.name.as_bytes())
}

/// `rpc.id`: the index of the method with a name.
pub fn id(table: &[RpcSpec], arg: &[u8], out: &mut Reply) -> Result<(), RpcError> {
    let index = find(table, arg)?;
    put(out, &(index as u16).to_le_bytes())
}

/// `rpc.info`: the metadata of the method with a name.
pub fn info(table: &[RpcSpec], arg: &[u8], out: &mut Reply) -> Result<(), RpcError> {
    let spec = &table[find(table, arg)?];
    put(out, &spec.legacy_metadata().to_le_bytes())
}

/// `rpc.list` and `rpc.listinfo`: the table size with no argument, or the name
/// of the method at an index, preceded by its metadata for `rpc.listinfo`.
pub fn list(
    table: &[RpcSpec],
    arg: &[u8],
    with_info: bool,
    out: &mut Reply,
) -> Result<(), RpcError> {
    if arg.is_empty() {
        return put(out, &(table.len() as u16).to_le_bytes());
    }
    let spec = table.get(index(arg)?).ok_or(RpcError::Invalid)?;
    if with_info {
        put(out, &spec.legacy_metadata().to_le_bytes())?;
    }
    put(out, spec.name.as_bytes())
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

/// Append `bytes` to a reply.
pub fn put(out: &mut Reply, bytes: &[u8]) -> Result<(), RpcError> {
    out.extend_from_slice(bytes).map_err(|_| RpcError::NoBufs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> [RpcSpec; 3] {
        [
            RpcSpec::prop("rpc.hash", Kind::Uint(4), Access::READ),
            RpcSpec::action("dev.stop"),
            RpcSpec::prop("test.enable", Kind::Bool, Access::RW),
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
            RpcSpec::prop("dev.name", Kind::String, Access::READ).flags(),
            0x0100_0053
        );
    }

    #[test]
    fn legacy_metadata_matches_firmware_encoding() {
        let spec = RpcSpec::prop("test.amplitude", Kind::Float(8), Access::RW);
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
        let spec = RpcSpec::prop("wide", Kind::Uint(16), Access::RW);
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

    fn answer(call: impl FnOnce(&mut Reply) -> Result<(), RpcError>) -> Result<Vec<u8>, RpcError> {
        let mut out = Reply::new();
        call(&mut out).map(|()| out.to_vec())
    }

    #[test]
    fn introspection_answers_by_index_and_by_name() {
        let table = table();

        assert_eq!(
            answer(|out| list(&table, &[], false, out)),
            Ok(3u16.to_le_bytes().to_vec())
        );

        assert_eq!(
            answer(|out| name(&table, &1u16.to_le_bytes(), out)),
            Ok(b"dev.stop".to_vec())
        );
        assert_eq!(
            answer(|out| name(&table, &3u16.to_le_bytes(), out)),
            Err(RpcError::Invalid)
        );
        assert_eq!(
            answer(|out| name(&table, &[1], out)),
            Err(RpcError::ArgsSize)
        );

        assert_eq!(
            answer(|out| id(&table, b"test.enable", out)),
            Ok(2u16.to_le_bytes().to_vec())
        );
        assert_eq!(answer(|out| id(&table, b"", out)), Err(RpcError::Invalid));
        assert_eq!(
            answer(|out| id(&table, b"nope", out)),
            Err(RpcError::Invalid)
        );

        assert_eq!(
            answer(|out| info(&table, b"dev.stop", out)),
            Ok(0x8000u16.to_le_bytes().to_vec())
        );

        let listed = answer(|out| list(&table, &2u16.to_le_bytes(), true, out)).unwrap();
        assert_eq!(&listed[..2], &table[2].legacy_metadata().to_le_bytes());
        assert_eq!(&listed[2..], b"test.enable");
    }

    #[test]
    fn a_reply_that_does_not_fit_is_refused() {
        let mut out = Reply::new();
        assert_eq!(put(&mut out, &[0; REPLY_MAX]), Ok(()));
        assert_eq!(put(&mut out, &[0]), Err(RpcError::NoBufs));
    }
}

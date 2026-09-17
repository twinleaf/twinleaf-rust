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

/// Declare the standard table once: the variant [`Std`] answers by, the name
/// the entry carries, and the [`RpcSpec`] constructor that describes it.
macro_rules! standard {
    ($($variant:ident $name:literal $method:ident($($arg:expr),*),)*) => {
        /// One entry of [`STANDARD`], in the id order that is on the wire.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Std {
            $(#[doc = $name] $variant,)*
        }

        impl Std {
            /// The entry a table position names, or `None` past the standard
            /// table, where a board's own entries begin.
            pub fn at(index: usize) -> Option<Self> {
                const ALL: &[Std] = &[$(Std::$variant,)*];
                ALL.get(index).copied()
            }
        }

        /// The RPCs every Twinleaf platform declares, in the id order that is
        /// on the wire: tl-chibi's `tl_firmware_start`, then what it does not
        /// fix.
        ///
        /// A platform that does not implement a listed RPC answers
        /// [`RpcError::State`].
        pub static STANDARD: &[RpcSpec] = &[$(RpcSpec::$method($name $(, $arg)*),)*];
    };
}

standard! {
    Metadata "dev.metadata" std(Access::RW),
    Systime "dev.systime" prop(Kind::Uint(8), Access::READ),
    Reboot "dev.reboot" action(),
    Loglevel "dev.loglevel" prop(Kind::Uint(1), Access::RW),
    Name "dev.name" prop(Kind::String, Access::READ),
    Model "dev.model" prop(Kind::String, Access::READ),
    Uid "dev.uid" std(Access::READ),
    Serial "dev.serial" prop(Kind::String, Access::READ),
    Revision "dev.revision" prop(Kind::Uint(2), Access::READ),
    Desc "dev.desc" prop(Kind::String, Access::READ),
    Session "dev.session" prop(Kind::Uint(4), Access::READ),
    Mcu "dev.mcu.model" prop(Kind::String, Access::READ),
    FirmwareSerial "dev.firmware.serial" prop(Kind::String, Access::READ),
    ConfLoad "dev.conf.load" action(),
    ConfSave "dev.conf.save" action(),
    ConfReset "dev.conf.reset" action(),
    Uptime "dev.uptime" prop(Kind::Uint(4), Access::READ),
    Upload "dev.firmware.upload" std(Access::RW),
    Upgrade "dev.firmware.upgrade" action(),
    RpcName "rpc.name" std(Access::RW),
    RpcId "rpc.id" std(Access::RW),
    RpcInfo "rpc.info" std(Access::RW),
    RpcList "rpc.list" std(Access::RW),
    RpcListInfo "rpc.listinfo" std(Access::RW),
    RpcMatch "rpc.match" std(Access::RW),
    RpcHash "rpc.hash" prop(Kind::Uint(4), Access::READ),
    Start "dev.start" action(),
    Stop "dev.stop" action(),
    Restart "dev.restart" action(),
    Abort "dev.firmware.abort" action(),
    SettingsVersion "settings.version" prop(Kind::Uint(4), Access::READ),
    SyncStatus "sync.status" prop(Kind::Uint(1), Access::READ),
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

/// `rpc.match`: autocomplete over the method names, for a host completing what
/// a user typed.
///
/// The argument is a name prefix, optionally followed by `|N` selecting the
/// `N`-th (0-based) match:
///
/// - `dev.na` — the unique completion `dev.name` as a string when exactly one
///   name starts with the prefix, and a `u16` match count otherwise, zero when
///   nothing matches. A caller tells the two apart by whether the reply starts
///   with the prefix it sent.
/// - `dev.na|0` — the 0-th matching name, so a caller can cycle through them
///   all. Past the last match it is [`RpcError::Invalid`].
///
/// An empty argument is [`RpcError::Invalid`]. Byte-compatible with tl-chibi's
/// `rpc_match`, down to its parser: only the first `|` separates, and a
/// non-numeric tail after it still cuts the prefix but selects nothing.
pub fn match_name(table: &[RpcSpec], args: &[u8], out: &mut Reply) -> Result<(), RpcError> {
    if args.is_empty() {
        return Err(RpcError::Invalid);
    }
    let (prefix, select) = match args.iter().position(|&byte| byte == b'|') {
        Some(bar) => (&args[..bar], match_index(&args[bar + 1..])),
        None => (args, None),
    };
    let mut matches = table
        .iter()
        .map(|spec| spec.name)
        .filter(|name| name.as_bytes().starts_with(prefix));

    if let Some(index) = select {
        let name = matches.nth(index).ok_or(RpcError::Invalid)?;
        return put(out, name.as_bytes());
    }

    let mut count: u16 = 0;
    let mut unique = "";
    for name in matches {
        count = count.saturating_add(1);
        unique = name;
    }
    match count {
        1 => put(out, unique.as_bytes()),
        _ => put(out, &count.to_le_bytes()),
    }
}

/// The decimal index after `|`, or `None` when the tail is not all digits. An
/// empty tail is index zero, as tl-chibi's parser leaves it.
fn match_index(digits: &[u8]) -> Option<usize> {
    let mut index: usize = 0;
    for &byte in digits {
        if !byte.is_ascii_digit() {
            return None;
        }
        index = index
            .saturating_mul(10)
            .saturating_add((byte - b'0') as usize);
    }
    Some(index)
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

/// Answer a read-only property with `value`, refusing a write.
pub fn read(out: &mut Reply, args: &[u8], value: &[u8]) -> Result<(), RpcError> {
    if !args.is_empty() {
        return Err(RpcError::ReadOnly);
    }
    put(out, value)
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
    fn the_standard_table_order_is_pinned() {
        assert_eq!(STANDARD.len(), 32);
        assert_eq!(STANDARD.first().unwrap().name, "dev.metadata");
        assert_eq!(STANDARD.last().unwrap().name, "sync.status");
        assert_eq!(hash(STANDARD), 0xde3e_d53f);
    }

    #[test]
    fn every_position_of_the_standard_table_names_an_entry() {
        assert_eq!(Std::at(0), Some(Std::Metadata));
        assert_eq!(Std::at(STANDARD.len() - 1), Some(Std::SyncStatus));
        assert_eq!(Std::at(STANDARD.len()), None);
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

    /// A table shaped like a device's: the standard entries, then two of a
    /// board's own.
    fn named() -> Vec<RpcSpec> {
        STANDARD
            .iter()
            .cloned()
            .chain([
                RpcSpec::prop("board.first", Kind::Uint(4), Access::RW),
                RpcSpec::prop("board.second", Kind::Uint(4), Access::RW),
            ])
            .collect()
    }

    fn matched(args: &[u8]) -> Result<Reply, RpcError> {
        let mut out = Reply::new();
        match_name(&named(), args, &mut out).map(|()| out)
    }

    #[test]
    fn match_returns_the_unique_completion() {
        assert_eq!(matched(b"dev.na").unwrap().as_slice(), b"dev.name");
        // An exact name is still a match, and still the only one.
        assert_eq!(matched(b"dev.uptime").unwrap().as_slice(), b"dev.uptime");
    }

    #[test]
    fn match_counts_an_ambiguous_prefix() {
        let expected = named()
            .iter()
            .filter(|spec| spec.name.starts_with("dev.conf."))
            .count() as u16;
        assert!(expected > 1, "the prefix has to be ambiguous to be a test");
        assert_eq!(
            matched(b"dev.conf.").unwrap().as_slice(),
            &expected.to_le_bytes()
        );
    }

    #[test]
    fn match_reports_zero_for_no_match() {
        assert_eq!(
            matched(b"nonsense.").unwrap().as_slice(),
            &0u16.to_le_bytes()
        );
    }

    #[test]
    fn match_selects_by_index_and_sees_board_rpcs() {
        assert_eq!(matched(b"board.|0").unwrap().as_slice(), b"board.first");
        assert_eq!(matched(b"board.|1").unwrap().as_slice(), b"board.second");
        assert_eq!(
            matched(b"board.|2").unwrap_err(),
            RpcError::Invalid,
            "past the last match"
        );
        // An empty index is index zero, and a non-numeric tail selects nothing
        // but still cuts the prefix — both as tl-chibi's parser leaves them.
        assert_eq!(matched(b"board.|").unwrap().as_slice(), b"board.first");
        assert_eq!(
            matched(b"board.|x").unwrap().as_slice(),
            &2u16.to_le_bytes()
        );
    }

    #[test]
    fn match_rejects_an_empty_argument() {
        assert_eq!(matched(&[]).unwrap_err(), RpcError::Invalid);
    }
}

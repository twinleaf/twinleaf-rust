//! Dynamic RPC values, whose type is learned from an RPC's metadata at run
//! time.
//!
//! [`RpcValue`] holds a value of any type. [`RpcValueTypeExt`] encodes and
//! decodes it as bytes and as text, and [`RpcMetaExt`] names an RPC's type
//! and access for display.

use crate::proto::rpc::{RpcAccess, RpcMeta, RpcMetaFlags, RpcValueType};
use crate::tio::rpc::{RpcDecodeError, RpcReply};

/// An RPC value whose type was learned at run time.
#[derive(Debug, Clone, PartialEq)]
pub enum RpcValue {
    /// No value, as for an action or an empty reply.
    Unit,
    /// Any unsigned integer type, widened.
    U64(u64),
    /// Any signed integer type, widened.
    I64(i64),
    /// Any float type, widened.
    F64(f64),
    /// A bool.
    Bool(bool),
    /// A string.
    Str(String),
    /// Raw bytes, for a type with no better reading.
    Bytes(Vec<u8>),
}

impl std::fmt::Display for RpcValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcValue::Unit => Ok(()),
            RpcValue::U64(n) => write!(f, "{}", n),
            RpcValue::I64(n) => write!(f, "{}", n),
            RpcValue::F64(x) => write!(f, "{}", x),
            RpcValue::Bool(b) => write!(f, "{}", b),
            RpcValue::Str(s) => write!(f, "{}", s),
            RpcValue::Bytes(b) => {
                for byte in b {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
        }
    }
}

impl RpcValue {
    /// Formats the value as text for a person to read.
    ///
    /// A string is quoted and followed by its bytes, raw bytes are a list,
    /// and any other value is as [`Display`](std::fmt::Display) writes it.
    pub fn to_text(&self) -> String {
        match self {
            RpcValue::Str(value) => format!("\"{}\" {:?}", value, value.as_bytes()),
            RpcValue::Bytes(value) => format!("{:?}", value),
            other => other.to_string(),
        }
    }
}

/// Host-side codecs between an RPC's value type and owned [`RpcValue`]s, as
/// wire bytes and as text.
pub trait RpcValueTypeExt {
    /// The type names [`from_name`](Self::from_name) reads. They are each
    /// integer and float type as [`RpcMetaExt::type_str`] writes it, and
    /// `string`.
    const NAMES: &'static [&'static str];

    /// Encodes `value` as this type.
    ///
    /// # Errors
    ///
    /// Returns an [`RpcValueEncodeError`] if `value` is not of this type or
    /// does not fit it, or if this type has a width this build cannot encode.
    fn encode(self, value: &RpcValue) -> Result<Vec<u8>, RpcValueEncodeError>;
    /// Decodes `bytes` as this type. A string that is not UTF-8 decodes as
    /// [`RpcValue::Bytes`].
    ///
    /// # Errors
    ///
    /// Returns an [`RpcValueDecodeError`] if `bytes` is too short for this
    /// type, or if this type has a width this build cannot decode.
    fn decode(self, bytes: &[u8]) -> Result<RpcValue, RpcValueDecodeError>;

    /// Returns the type that one of [`NAMES`](Self::NAMES) names, such as
    /// `u16` or `string`, or `None` for any other name.
    fn from_name(name: &str) -> Option<Self>
    where
        Self: Sized;

    /// Parses `text` as a value of this type.
    ///
    /// An integer or a float is read in Rust's notation for it. A string or
    /// raw bytes is the text itself. A unit is read only from empty text.
    ///
    /// # Errors
    ///
    /// Returns an [`RpcValueEncodeError`] if `text` is not a number of this
    /// type, or is not empty for a unit.
    fn parse(self, text: &str) -> Result<RpcValue, RpcValueEncodeError>;

    /// Parses `text` as a value of this type and encodes it, as for an RPC
    /// argument a person typed.
    ///
    /// # Errors
    ///
    /// Fails as [`parse`](Self::parse) and [`encode`](Self::encode) do.
    fn encode_text(self, text: &str) -> Result<Vec<u8>, RpcValueEncodeError>
    where
        Self: Sized + Copy,
    {
        self.encode(&self.parse(text)?)
    }
}

impl RpcValueTypeExt for RpcValueType {
    const NAMES: &'static [&'static str] = &[
        "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "bool", "string",
    ];

    fn from_name(name: &str) -> Option<Self> {
        let int = |signed, size| RpcValueType::Int { signed, size };
        Some(match name {
            "u8" => int(false, 1),
            "u16" => int(false, 2),
            "u32" => int(false, 4),
            "u64" => int(false, 8),
            "i8" => int(true, 1),
            "i16" => int(true, 2),
            "i32" => int(true, 4),
            "i64" => int(true, 8),
            "f32" => RpcValueType::Float { size: 4 },
            "f64" => RpcValueType::Float { size: 8 },
            "bool" => RpcValueType::Bool,
            "string" => RpcValueType::String { max_len: None },
            _ => return None,
        })
    }

    fn parse(self, text: &str) -> Result<RpcValue, RpcValueEncodeError> {
        Ok(match self {
            RpcValueType::Unit if text.is_empty() => RpcValue::Unit,
            RpcValueType::Unit => return Err(RpcValueEncodeError::UnitText),
            RpcValueType::String { .. } => RpcValue::Str(text.to_owned()),
            RpcValueType::Int { signed: false, .. } => RpcValue::U64(text.parse()?),
            RpcValueType::Int { signed: true, .. } => RpcValue::I64(text.parse()?),
            RpcValueType::Float { size: 4 } => RpcValue::F64(text.parse::<f32>()?.into()),
            RpcValueType::Float { .. } => RpcValue::F64(text.parse()?),
            RpcValueType::Bool => RpcValue::Bool(text.parse()?),
            RpcValueType::Raw { .. } => RpcValue::Bytes(text.as_bytes().to_vec()),
        })
    }

    fn encode(self, value: &RpcValue) -> Result<Vec<u8>, RpcValueEncodeError> {
        match (self, value) {
            (RpcValueType::Unit, RpcValue::Unit) => Ok(Vec::new()),
            (
                target @ RpcValueType::Int {
                    signed: false,
                    size,
                },
                RpcValue::U64(value),
            ) => encode_int((*value).into(), false, size, target),
            (target @ RpcValueType::Int { signed: true, size }, RpcValue::I64(value)) => {
                encode_int((*value).into(), true, size, target)
            }
            (RpcValueType::Float { size: 4 }, RpcValue::F64(value)) => {
                Ok((*value as f32).to_le_bytes().to_vec())
            }
            (RpcValueType::Float { size: 8 }, RpcValue::F64(value)) => {
                Ok(value.to_le_bytes().to_vec())
            }
            (RpcValueType::Float { size }, RpcValue::F64(_)) => {
                Err(RpcValueEncodeError::UnsupportedFloatSize(size))
            }
            (RpcValueType::String { max_len }, RpcValue::Str(value)) => {
                if let Some(max) = max_len {
                    if value.len() > usize::from(max.get()) {
                        return Err(RpcValueEncodeError::StringTooLong {
                            max: max.get(),
                            actual: value.len(),
                        });
                    }
                }
                Ok(value.as_bytes().to_vec())
            }
            (RpcValueType::Bool, RpcValue::Bool(value)) => Ok(vec![u8::from(*value)]),
            (RpcValueType::Raw { .. }, RpcValue::Bytes(value)) => Ok(value.clone()),
            (expected, actual) => Err(RpcValueEncodeError::TypeMismatch {
                expected,
                actual: actual.kind_name(),
            }),
        }
    }

    fn decode(self, bytes: &[u8]) -> Result<RpcValue, RpcValueDecodeError> {
        match self {
            RpcValueType::Unit => Ok(RpcValue::Unit),
            RpcValueType::Int {
                signed: false,
                size,
            } => Ok(RpcValue::U64(match size {
                1 => scalar::<u8>(bytes)?.into(),
                2 => scalar::<u16>(bytes)?.into(),
                4 => scalar::<u32>(bytes)?.into(),
                8 => scalar::<u64>(bytes)?,
                _ => return Err(RpcValueDecodeError::UnsupportedIntegerSize(size)),
            })),
            RpcValueType::Int { signed: true, size } => Ok(RpcValue::I64(match size {
                1 => scalar::<i8>(bytes)?.into(),
                2 => scalar::<i16>(bytes)?.into(),
                4 => scalar::<i32>(bytes)?.into(),
                8 => scalar::<i64>(bytes)?,
                _ => return Err(RpcValueDecodeError::UnsupportedIntegerSize(size)),
            })),
            RpcValueType::Float { size: 4 } => Ok(RpcValue::F64(scalar::<f32>(bytes)?.into())),
            RpcValueType::Float { size: 8 } => Ok(RpcValue::F64(scalar::<f64>(bytes)?)),
            RpcValueType::Float { size } => Err(RpcValueDecodeError::UnsupportedFloatSize(size)),
            RpcValueType::Bool => Ok(RpcValue::Bool(scalar::<u8>(bytes)? != 0)),
            RpcValueType::String { .. } => match std::str::from_utf8(bytes) {
                Ok(value) => Ok(RpcValue::Str(value.to_owned())),
                Err(_) => Ok(RpcValue::Bytes(bytes.to_vec())),
            },
            RpcValueType::Raw { .. } => Ok(RpcValue::Bytes(bytes.to_vec())),
        }
    }
}

impl RpcValue {
    fn kind_name(&self) -> &'static str {
        match self {
            RpcValue::Unit => "unit",
            RpcValue::U64(_) => "unsigned integer",
            RpcValue::I64(_) => "signed integer",
            RpcValue::F64(_) => "float",
            RpcValue::Bool(_) => "bool",
            RpcValue::Str(_) => "string",
            RpcValue::Bytes(_) => "bytes",
        }
    }
}

/// Encodes `value` as the `size` byte integer `target`, signed or not.
/// Returns an error if `target` cannot hold it.
fn encode_int(
    value: i128,
    signed: bool,
    size: u8,
    target: RpcValueType,
) -> Result<Vec<u8>, RpcValueEncodeError> {
    if !matches!(size, 1 | 2 | 4 | 8) {
        return Err(RpcValueEncodeError::UnsupportedIntegerSize(size));
    }
    let bits = u32::from(size) * 8;
    let (min, max) = match signed {
        true => (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1),
        false => (0, (1i128 << bits) - 1),
    };
    if !(min..=max).contains(&value) {
        return Err(RpcValueEncodeError::OutOfRange {
            value: value.to_string(),
            target,
        });
    }
    Ok(value.to_le_bytes()[..usize::from(size)].to_vec())
}

/// Decodes the scalar at the front of `bytes` with the typed codec. Ignores
/// any bytes after it.
fn scalar<T: RpcReply>(mut bytes: &[u8]) -> Result<T, RpcDecodeError> {
    T::decode_from(&mut bytes)
}

/// Why a value could not be encoded as the type an RPC takes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RpcValueEncodeError {
    /// The value's kind is not the type's.
    #[error("cannot encode {actual} as {expected:?}")]
    TypeMismatch {
        /// The type the RPC takes.
        expected: RpcValueType,
        /// The kind of value offered.
        actual: &'static str,
    },
    /// The value does not fit the type.
    #[error("value {value} is out of range for {target:?}")]
    OutOfRange {
        /// The value, as written.
        value: String,
        /// The type it had to fit.
        target: RpcValueType,
    },
    /// The string exceeds the type's length.
    #[error("string too long ({actual} bytes, max {max})")]
    StringTooLong {
        /// Bytes the type allows.
        max: u8,
        /// Bytes in the string.
        actual: usize,
    },
    /// An integer width this build cannot handle.
    #[error("unsupported integer size: {0} bytes")]
    UnsupportedIntegerSize(u8),
    /// A float width this build cannot handle.
    #[error("unsupported float size: {0} bytes")]
    UnsupportedFloatSize(u8),
    /// The text is not an integer, or not one an integer type holds.
    #[error(transparent)]
    IntegerText(#[from] std::num::ParseIntError),
    /// The text is not a float.
    #[error(transparent)]
    FloatText(#[from] std::num::ParseFloatError),
    /// The text is not `true` or `false`.
    #[error(transparent)]
    BoolText(#[from] std::str::ParseBoolError),
    /// A unit is written as empty text, and this text is not empty.
    #[error("unit RPC arguments must be empty")]
    UnitText,
}

/// Why reply bytes could not be read as the type an RPC returns.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RpcValueDecodeError {
    /// The bytes are fewer than the type needs.
    #[error(transparent)]
    Reply(#[from] RpcDecodeError),
    /// An integer width this build cannot handle.
    #[error("unsupported integer size: {0} bytes")]
    UnsupportedIntegerSize(u8),
    /// A float width this build cannot handle.
    #[error("unsupported float size: {0} bytes")]
    UnsupportedFloatSize(u8),
}

/// Host-side display helpers over the shared metadata word.
pub trait RpcMetaExt {
    /// Returns the access as three letters for read, write, and persistent,
    /// with `-` for each the RPC lacks. An unknown RPC gives `???`.
    fn perm_str(&self) -> String;
    /// Returns the value type's name, or `capture` when the flag marks one.
    fn type_str(&self) -> String;
    /// Returns the type a person types this RPC's values in and reads its
    /// replies as. It is the value type, with raw bytes taken as a string.
    fn text_type(&self) -> RpcValueType;
}

impl RpcMetaExt for RpcMeta {
    fn perm_str(&self) -> String {
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

    fn type_str(&self) -> String {
        if self.flags().contains(RpcMetaFlags::CAPTURE) {
            return "capture".to_string();
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
            RpcValueType::Bool => "bool".to_string(),
            RpcValueType::String { max_len } => match max_len {
                Some(n) => format!("string<{n}>"),
                None => "string".to_string(),
            },
            RpcValueType::Raw { .. } => String::new(),
        }
    }

    fn text_type(&self) -> RpcValueType {
        match self.kind() {
            RpcValueType::Raw { .. } => RpcValueType::String { max_len: None },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const U16: RpcValueType = RpcValueType::Int {
        signed: false,
        size: 2,
    };
    const I8: RpcValueType = RpcValueType::Int {
        signed: true,
        size: 1,
    };

    /// An integer encodes at each end of its type's range, and one past
    /// either end is out of range.
    #[test]
    fn an_integer_encodes_within_its_range_only() {
        assert_eq!(U16.encode(&RpcValue::U64(65535)).unwrap(), [0xff, 0xff]);
        assert_eq!(I8.encode(&RpcValue::I64(-128)).unwrap(), [0x80]);
        assert_eq!(I8.encode(&RpcValue::I64(127)).unwrap(), [0x7f]);
        for (kind, value) in [
            (U16, RpcValue::U64(65536)),
            (I8, RpcValue::I64(128)),
            (I8, RpcValue::I64(-129)),
        ] {
            assert!(matches!(
                kind.encode(&value),
                Err(RpcValueEncodeError::OutOfRange { .. })
            ));
        }
        let u64_max = RpcValueType::Int {
            signed: false,
            size: 8,
        };
        assert_eq!(
            u64_max.encode(&RpcValue::U64(u64::MAX)).unwrap(),
            u64::MAX.to_le_bytes()
        );
    }

    /// Every type name reads back as the type `type_str` names it for.
    #[test]
    fn every_type_name_is_the_name_type_str_writes() {
        for name in RpcValueType::NAMES {
            let kind = RpcValueType::from_name(name).unwrap();
            let meta = RpcMeta::new(kind, RpcMetaFlags::READABLE);
            assert_eq!(meta.kind(), kind, "{name}");
            assert_eq!(meta.type_str(), *name);
        }
        assert_eq!(RpcValueType::from_name("string<8>"), None);
    }

    /// Text reads as each type writes it, and the reading fails on text the
    /// type cannot hold.
    #[test]
    fn text_encodes_as_the_type_reads_it() {
        let f32 = RpcValueType::Float { size: 4 };
        let string = RpcValueType::String { max_len: None };
        assert_eq!(U16.encode_text("513").unwrap(), [1, 2]);
        assert_eq!(I8.encode_text("-2").unwrap(), [0xfe]);
        assert_eq!(f32.encode_text("1.5").unwrap(), 1.5f32.to_le_bytes());
        assert_eq!(string.encode_text("a b").unwrap(), b"a b");
        assert_eq!(RpcValueType::Unit.encode_text("").unwrap(), b"");
        assert_eq!(f32.parse("0.1").unwrap(), RpcValue::F64(0.1f32.into()));
        assert_eq!(RpcValueType::Bool.encode_text("true").unwrap(), [1]);

        let error =
            |kind: RpcValueType, text: &str| kind.encode_text(text).unwrap_err().to_string();
        assert_eq!(error(U16, "-1"), "invalid digit found in string");
        assert_eq!(error(I8, "x"), "invalid digit found in string");
        assert_eq!(error(f32, "x"), "invalid float literal");
        assert_eq!(
            error(RpcValueType::Bool, "1"),
            "provided string was not `true` or `false`"
        );
        assert_eq!(
            error(U16, "65536"),
            "value 65536 is out of range for Int { signed: false, size: 2 }"
        );
        assert_eq!(
            error(RpcValueType::Unit, "x"),
            "unit RPC arguments must be empty"
        );
    }

    /// A reply reads as a person sees it, a string with its bytes beside it.
    #[test]
    fn a_value_as_text_shows_a_string_with_its_bytes() {
        assert_eq!(RpcValue::Str("ok".into()).to_text(), "\"ok\" [111, 107]");
        assert_eq!(RpcValue::Bytes(vec![1, 2]).to_text(), "[1, 2]");
        assert_eq!(RpcValue::I64(-3).to_text(), "-3");
        assert_eq!(RpcValue::F64(0.5).to_text(), "0.5");
        assert_eq!(RpcValue::Unit.to_text(), "");
    }

    /// Raw bytes are typed and shown as a string, and every other type as
    /// itself.
    #[test]
    fn a_raw_rpc_is_text_as_a_string() {
        let raw = RpcMeta::ANY;
        assert!(matches!(raw.kind(), RpcValueType::Raw { .. }));
        assert_eq!(raw.text_type(), RpcValueType::String { max_len: None });
        let f64 = RpcMeta::new(RpcValueType::Float { size: 8 }, RpcMetaFlags::READABLE);
        assert_eq!(f64.text_type(), RpcValueType::Float { size: 8 });
    }

    /// A scalar decodes as the typed codec reads it, and too few bytes are
    /// the codec's own error.
    #[test]
    fn a_scalar_decodes_through_the_typed_codec() {
        assert_eq!(I8.decode(&[0xff]).unwrap(), RpcValue::I64(-1));
        assert_eq!(U16.decode(&[1, 2]).unwrap(), RpcValue::U64(0x0201));
        assert_eq!(
            RpcValueType::Bool.decode(&[2]).unwrap(),
            RpcValue::Bool(true)
        );
        assert!(matches!(
            U16.decode(&[1]),
            Err(RpcValueDecodeError::Reply(RpcDecodeError::TooShort {
                expected: 2,
                actual: 1
            }))
        ));
    }
}

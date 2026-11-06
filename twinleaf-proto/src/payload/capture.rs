//! Capture metadata, an RPC reply format.
//!
//! A capture RPC called with the metadata selector replies with a fixed head
//! whose first byte is its own length, then four strings whose lengths the
//! head records. The strings are `name`, `units`, `x_name`, and `x_units`.
//! [`CaptureMetadata`] parses and writes the reply.

use zerocopy::byteorder::little_endian::{F32, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::data::{DataType, RecordReader, RecordWriter};

/// Version of the layout described here (`TL_CAPTURE_METADATA_VERSION`).
pub const METADATA_VERSION: u8 = 1;
/// Length of the head this build reads (`TL_CAPTURE_METADATA_FIXED_LEN`). A
/// device may declare a longer one.
pub const METADATA_FIXED_LEN: usize = size_of::<MetadataHead>();

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
struct MetadataHead {
    head_len: u8,
    version: u8,
    data_type: u8,
    reserved: u8,
    data_size: U32,
    block_size: U16,
    length: U32,
    y_calibration: F32,
    x_offset: F32,
    x_stride: F32,
    name_len: u8,
    units_len: u8,
    x_name_len: u8,
    x_units_len: u8,
}

/// What one capture holds and how to read it (`capture_metadata_fixed`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CaptureMetadata<'a> {
    /// Layout version of the reply. A reader that does not know it should stop.
    pub version: u8,
    /// Element encoding.
    pub data_type: DataType,
    /// Bytes of capture data the block RPCs serve, `length` elements in all.
    pub data_size: u32,
    /// Bytes one block RPC returns.
    pub block_size: u16,
    /// Elements captured.
    pub length: u32,
    /// Multiplier taking a raw element to its value in `units`.
    pub y_calibration: f32,
    /// X value of element zero, in `x_units`.
    pub x_offset: f32,
    /// X step between consecutive elements.
    pub x_stride: f32,
    /// Capture name.
    pub name: &'a str,
    /// Units of the values.
    pub units: &'a str,
    /// Name of the x axis.
    pub x_name: &'a str,
    /// Units of the x axis.
    pub x_units: &'a str,
}

impl<'a> CaptureMetadata<'a> {
    /// Writes the RPC reply payload into `buf` and returns its length. Strings
    /// are truncated to the room left after the head. Returns `None` if `buf`
    /// cannot hold the head.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let mut record = RecordWriter::new(buf, METADATA_FIXED_LEN)?;
        let name_len = record.push(self.name);
        let units_len = record.push(self.units);
        let x_name_len = record.push(self.x_name);
        let x_units_len = record.push(self.x_units);
        Some(record.finish(MetadataHead {
            head_len: METADATA_FIXED_LEN as u8,
            version: self.version,
            data_type: self.data_type.value(),
            reserved: 0,
            data_size: self.data_size.into(),
            block_size: self.block_size.into(),
            length: self.length.into(),
            y_calibration: self.y_calibration.into(),
            x_offset: self.x_offset.into(),
            x_stride: self.x_stride.into(),
            name_len,
            units_len,
            x_name_len,
            x_units_len,
        }))
    }

    /// Parses an RPC reply payload. Returns `None` if the reply is truncated,
    /// its head is shorter than [`METADATA_FIXED_LEN`], or a string is not
    /// UTF-8. [`Self::version`] is returned unchecked.
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        let (head, mut strings) = RecordReader::split::<MetadataHead>(payload)?;
        Some(Self {
            version: head.version,
            data_type: DataType::new(head.data_type),
            data_size: head.data_size.get(),
            block_size: head.block_size.get(),
            length: head.length.get(),
            y_calibration: head.y_calibration.get(),
            x_offset: head.x_offset.get(),
            x_stride: head.x_stride.get(),
            name: strings.take(head.name_len)?,
            units: strings.take(head.units_len)?,
            x_name: strings.take(head.x_name_len)?,
            x_units: strings.take(head.x_units_len)?,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Properties of the capture metadata reply.
    //!
    //! # Observations
    //!
    //! The bytes [`CaptureMetadata::write`] leaves in its buffer and the
    //! length it returns, and what [`CaptureMetadata::parse`] reads.
    //!
    //! # Sources
    //!
    //! The module doc, a head whose first byte is its own length, then the
    //! four strings whose lengths it records. The docs of `write` and
    //! `parse`, and `capture_metadata_fixed`, the firmware struct the head
    //! mirrors field for field, little endian, 30 bytes long.
    //!
    //! # Model
    //!
    //! [`Fields`] holds a reply with owned strings, and [`Fields::wire`] lays
    //! it out byte by byte, with no writer and no head type of the code.
    //!
    //! ```text
    //! [30, version, data_type, reserved 0, data_size u32, block_size u16,
    //!  length u32, y_calibration f32, x_offset f32, x_stride f32,
    //!  name_len, units_len, x_name_len, x_units_len]
    //! name, units, x_name, x_units
    //! ```
    //!
    //! Replies are compared with the bits of their floats, so a NaN equals
    //! itself.
    //!
    //! # Laws
    //!
    //! - C1, model based layout. A reply whose strings are at most 255 bytes
    //!   writes [`Fields::wire`] into a buffer of that length, whatever the
    //!   buffer held before, with the reserved byte zero. Source, the struct.
    //!   It rules out swapped fields, a big endian word, and a reserved byte
    //!   left as the buffer held it.
    //! - C2, round trip. [`Fields::wire`] parses back to the reply. Source,
    //!   the doc of `parse`.
    //! - C3, postcondition. Into a buffer that holds the head and some room
    //!   more, each string in turn keeps the longest prefix that ends on a
    //!   character boundary and fits both the room left and 255 bytes, and the
    //!   reply parses with them. A buffer short of the head is refused.
    //!   Source, the doc of `write`.
    //! - C4, metamorphic. Raising the head length by `k` and putting `k` bytes
    //!   of any value after the head leaves what `parse` reads unchanged.
    //!   Source, the doc of [`METADATA_FIXED_LEN`], a device may declare a
    //!   longer head.
    //! - C5, completeness. A reply parses exactly when [`strings_of`] reads
    //!   it from its bytes alone. Its head length is at least 30 and within
    //!   the reply, each string length the head holds fits the bytes left in
    //!   turn, and every string is UTF-8. The strings it reads are those
    //!   bytes. Source, the doc of `parse`. It rules out a parse that reads
    //!   text that is not UTF-8 as empty, or a string that runs past the
    //!   reply as a shorter one.
    //! - C6, implicit oracle. `parse` returns for any bytes.
    //!
    //! # Generator obligations
    //!
    //! Strings from [`text`], numbers at zero and all ones, floats that are
    //! zeros, infinities and NaNs with payloads, rooms at any byte with
    //! strings of up to 300 bytes, 1 to 225 extra head bytes, and replies
    //! with a head length, a string length or a string byte changed, so that
    //! each refusal [`strings_of`] names is reached. `class_counts` prints
    //! the share of each class with `TWINLEAF_PAYLOAD_CLASSES` set.
    //!
    //! # Exclusions
    //!
    //! A version this build does not know, which `parse` returns unchecked.
    //!
    //! # Examples that stay
    //!
    //! The pinned layout, each field a value no other field holds so that
    //! swapped fields fail, and the head size.

    use super::*;
    use crate::payload::strategy::{
        bytes, bytes_to, cut_classes, cut_in_turn, float, half, octet, strings_of, tally, text,
        word, write_over_any,
    };
    use proptest::prelude::*;
    use proptest::sample::Index;

    /// The longest string a length byte measures.
    const MAX_STRING: usize = u8::MAX as usize;

    /// A capture metadata reply with owned strings.
    #[derive(Debug, Clone)]
    struct Fields {
        version: u8,
        data_type: u8,
        data_size: u32,
        block_size: u16,
        length: u32,
        y_calibration: f32,
        x_offset: f32,
        x_stride: f32,
        strings: [String; 4],
    }

    impl Fields {
        /// The reply the code writes.
        fn metadata(&self) -> CaptureMetadata<'_> {
            let [name, units, x_name, x_units] = &self.strings;
            CaptureMetadata {
                version: self.version,
                data_type: DataType::new(self.data_type),
                data_size: self.data_size,
                block_size: self.block_size,
                length: self.length,
                y_calibration: self.y_calibration,
                x_offset: self.x_offset,
                x_stride: self.x_stride,
                name,
                units,
                x_name,
                x_units,
            }
        }

        /// The strings in the order the reply carries them.
        fn strings(&self) -> Vec<&str> {
            self.strings.iter().map(String::as_str).collect()
        }

        /// The head, byte by byte as the firmware struct lays it out.
        fn head(&self) -> Vec<u8> {
            let [name, units, x_name, x_units] = self.strings.each_ref().map(|s| s.len() as u8);
            [
                &[30, self.version, self.data_type, 0][..],
                &self.data_size.to_le_bytes(),
                &self.block_size.to_le_bytes(),
                &self.length.to_le_bytes(),
                &self.y_calibration.to_bits().to_le_bytes(),
                &self.x_offset.to_bits().to_le_bytes(),
                &self.x_stride.to_bits().to_le_bytes(),
                &[name, units, x_name, x_units],
            ]
            .concat()
        }

        /// The reply, its head then its strings.
        fn wire(&self) -> Vec<u8> {
            let mut reply = self.head();
            for text in &self.strings {
                reply.extend_from_slice(text.as_bytes());
            }
            reply
        }
    }

    /// A reply with its floats set aside as their bits, so that a NaN equals
    /// itself.
    fn by_bits(reply: CaptureMetadata<'_>) -> (CaptureMetadata<'_>, [u32; 3]) {
        let bits = [reply.y_calibration, reply.x_offset, reply.x_stride].map(f32::to_bits);
        let zeroed = CaptureMetadata {
            y_calibration: 0.0,
            x_offset: 0.0,
            x_stride: 0.0,
            ..reply
        };
        (zeroed, bits)
    }

    /// A reply with numbers from [`octet`], [`half`], [`word`] and
    /// [`float`], and strings from [`text`] of up to `max` bytes.
    fn fields(max: usize) -> impl Strategy<Value = Fields> {
        (
            (octet(), octet(), word(), half(), word()),
            (float(), float(), float()),
            [text(max), text(max), text(max), text(max)],
        )
            .prop_map(
                |(
                    (version, data_type, data_size, block_size, length),
                    (y_calibration, x_offset, x_stride),
                    strings,
                )| Fields {
                    version,
                    data_type,
                    data_size,
                    block_size,
                    length,
                    y_calibration,
                    x_offset,
                    x_stride,
                    strings,
                },
            )
    }

    proptest! {
        /// C1 and C2. A reply whose strings are at most 255 bytes is laid out
        /// as its firmware struct, whatever the buffer held before, and
        /// parses back.
        #[test]
        fn a_reply_is_laid_out_as_its_struct(fields in fields(MAX_STRING)) {
            let wire = fields.wire();
            let metadata = fields.metadata();
            let (len, reply) = write_over_any(wire.len(), |buf| metadata.write(buf))?;
            prop_assert_eq!(len, Some(wire.len()));
            prop_assert_eq!(&reply, &wire);
            prop_assert_eq!(CaptureMetadata::parse(&wire).map(by_bits), Some(by_bits(metadata)));
        }

        /// C3. Into a buffer that holds the head and some room more, each
        /// string in turn keeps what fits the room left and 255 bytes, cut on
        /// a character boundary. A buffer short of the head is refused.
        #[test]
        fn strings_are_cut_to_the_room_left(fields in fields(300), room in any::<Index>()) {
            let strings = fields.strings();
            let total: usize = strings.iter().map(|text| text.len()).sum();
            let room = room.index(total + 2);
            let kept = cut_in_turn(&strings, room);
            let expected = Fields {
                strings: [0, 1, 2, 3].map(|at| kept[at].to_owned()),
                ..fields.clone()
            };
            let wire = expected.wire();
            let metadata = fields.metadata();
            let (len, reply) =
                write_over_any(METADATA_FIXED_LEN + room, |buf| metadata.write(buf))?;
            prop_assert_eq!(len, Some(wire.len()));
            prop_assert_eq!(&reply[..wire.len()], &wire[..]);
            prop_assert_eq!(
                CaptureMetadata::parse(&wire).map(by_bits),
                Some(by_bits(expected.metadata()))
            );
            let (len, _) = write_over_any(METADATA_FIXED_LEN - 1, |buf| metadata.write(buf))?;
            prop_assert_eq!(len, None);
        }

        /// C4. A longer head than this build's is read as far as this build
        /// knows, and the strings start after it.
        #[test]
        fn a_longer_head_is_read_as_far_as_this_build_knows(
            fields in fields(MAX_STRING),
            extra in prop_oneof![Just(1), Just(MAX_STRING - 30), 1..=MAX_STRING - 30]
                .prop_flat_map(bytes),
        ) {
            let mut reply = fields.head();
            reply[0] += extra.len() as u8;
            reply.extend_from_slice(&extra);
            for text in fields.strings() {
                reply.extend_from_slice(text.as_bytes());
            }
            prop_assert_eq!(
                CaptureMetadata::parse(&reply).map(by_bits),
                Some(by_bits(fields.metadata()))
            );
        }

        /// C5 and C6. Any bytes parse or not without a panic. A reply parses
        /// exactly when its head length is at least 30 and within the reply,
        /// each string length fits the bytes left in turn, and every string
        /// is UTF-8, and its strings are the bytes its head measures.
        #[test]
        fn a_reply_parses_exactly_when_its_head_and_strings_hold(reply in any_reply()) {
            let parsed = CaptureMetadata::parse(&reply);
            let expected = strings_of(&reply, METADATA_FIXED_LEN, &LENGTH_OFFSETS);
            prop_assert_eq!(parsed.is_some(), expected.is_ok(), "{:?}", expected);
            if let (Some(parsed), Ok(strings)) = (parsed, expected) {
                prop_assert_eq!(
                    vec![parsed.name, parsed.units, parsed.x_name, parsed.x_units],
                    strings
                );
            }
        }
    }

    /// Where the head holds the length of each string, in their order.
    const LENGTH_OFFSETS: [usize; 4] = [26, 27, 28, 29];

    /// A reply. A tenth are any bytes, and the rest a written reply with
    /// strings of up to 8 bytes and one byte set to 0, 1, 2, 29, 31, 128,
    /// 255 or any value, a quarter of them cut anywhere.
    fn any_reply() -> impl Strategy<Value = Vec<u8>> {
        let byte = prop_oneof![
            Just(0),
            Just(1),
            Just(2),
            Just(29),
            Just(31),
            Just(128),
            Just(u8::MAX),
            any::<u8>(),
        ];
        let cut = prop_oneof![3 => Just(None), 1 => any::<Index>().prop_map(Some)];
        prop_oneof![
            1 => bytes_to(64),
            9 => (fields(8), any::<Index>(), byte, cut).prop_map(|(fields, at, byte, cut)| {
                let mut reply = fields.wire();
                let at = at.index(reply.len());
                reply[at] = byte;
                if let Some(cut) = cut {
                    reply.truncate(cut.index(reply.len() + 1));
                }
                reply
            }),
        ]
    }

    /// Prints the share of the classes the generator obligations name, when
    /// `TWINLEAF_PAYLOAD_CLASSES` is set.
    #[test]
    fn class_counts() {
        tally("C5 replies", any_reply(), |reply| {
            vec![strings_of(reply, METADATA_FIXED_LEN, &LENGTH_OFFSETS)
                .map_or_else(|refused| refused, |_| "parses")]
        });
        tally(
            "C3 strings and room",
            (fields(300), any::<Index>()),
            |(fields, room)| {
                let strings = fields.strings();
                let total: usize = strings.iter().map(|text| text.len()).sum();
                cut_classes(&strings, room.index(total + 2))
            },
        );
    }

    fn metadata() -> CaptureMetadata<'static> {
        CaptureMetadata {
            version: METADATA_VERSION,
            data_type: DataType::F32,
            data_size: 12,
            block_size: 256,
            length: 3,
            y_calibration: 1.0,
            x_offset: 0.5,
            x_stride: 0.25,
            name: "vbus",
            units: "mV",
            x_name: "t_rel",
            x_units: "second",
        }
    }

    #[test]
    fn round_trip() {
        let mut buf = [0u8; 128];
        let len = metadata().write(&mut buf).unwrap();
        assert_eq!(CaptureMetadata::parse(&buf[..len]), Some(metadata()));
    }

    /// Byte-exact packed layout of the fixed metadata head and the strings
    /// appended after it.
    #[test]
    fn matches_the_packed_wire_layout() {
        let mut buf = [0u8; 64];
        let len = metadata().write(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            30,                         // fixed_len
            1,                          // version
            0x42,                       // data_type, TL_DATA_TYPE_FLOAT32
            0,                          // reserved
            0x0C, 0x00, 0x00, 0x00,     // data_size 12
            0x00, 0x01,                 // block_size 256
            0x03, 0x00, 0x00, 0x00,     // length 3
            0x00, 0x00, 0x80, 0x3F,     // y_calibration 1.0f
            0x00, 0x00, 0x00, 0x3F,     // x_offset 0.5f
            0x00, 0x00, 0x80, 0x3E,     // x_stride 0.25f
            4, 2, 5, 6,                 // name, units, x_name, x_units lengths
            b'v', b'b', b'u', b's', b'm', b'V',
            b't', b'_', b'r', b'e', b'l', b's', b'e', b'c', b'o', b'n', b'd',
        ]);
    }

    #[test]
    fn fixed_len_matches_the_wire() {
        // TL_CAPTURE_METADATA_FIXED_LEN, static_assert'd against the struct.
        assert_eq!(METADATA_FIXED_LEN, 30);
    }

    #[test]
    fn head_length_lets_readers_skip_unknown_fields() {
        // A device that appended two head fields: strings still start where the
        // wire says they do.
        let mut buf = [0u8; 64];
        let len = metadata().write(&mut buf).unwrap();
        let mut extended = [0u8; 66];
        extended[..METADATA_FIXED_LEN].copy_from_slice(&buf[..METADATA_FIXED_LEN]);
        extended[0] = METADATA_FIXED_LEN as u8 + 2;
        extended[METADATA_FIXED_LEN..METADATA_FIXED_LEN + 2].copy_from_slice(&[0xAA, 0xBB]);
        extended[METADATA_FIXED_LEN + 2..len + 2].copy_from_slice(&buf[METADATA_FIXED_LEN..len]);
        assert_eq!(
            CaptureMetadata::parse(&extended[..len + 2]),
            Some(metadata())
        );
    }

    #[test]
    fn parse_rejects_truncated_replies() {
        let mut buf = [0u8; 64];
        let len = metadata().write(&mut buf).unwrap();
        assert_eq!(CaptureMetadata::parse(&[]), None);
        // The declared head runs past the reply.
        assert_eq!(CaptureMetadata::parse(&buf[..METADATA_FIXED_LEN - 1]), None);
        // A head shorter than this build's known fields.
        let mut short = buf;
        short[0] = METADATA_FIXED_LEN as u8 - 1;
        assert_eq!(CaptureMetadata::parse(&short[..len]), None);
        // x_units_len runs past the end of the reply.
        assert_eq!(CaptureMetadata::parse(&buf[..len - 1]), None);
    }

    #[test]
    fn write_rejects_a_buffer_too_small_for_the_head() {
        assert_eq!(metadata().write(&mut [0u8; METADATA_FIXED_LEN - 1]), None);
        // With room for the head alone the strings truncate away.
        let mut buf = [0u8; METADATA_FIXED_LEN];
        assert_eq!(metadata().write(&mut buf), Some(METADATA_FIXED_LEN));
        let parsed = CaptureMetadata::parse(&buf).unwrap();
        assert_eq!((parsed.name, parsed.units), ("", ""));
        assert_eq!(parsed.length, 3);
    }
}

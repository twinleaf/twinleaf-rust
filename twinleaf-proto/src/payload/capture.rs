//! Capture metadata, an RPC reply format.
//!
//! A capture RPC called with the metadata selector replies with a fixed head
//! whose first byte is its own length, then four strings whose lengths the
//! head records: `name`, `units`, `x_name`, `x_units`.

use zerocopy::byteorder::little_endian::{F32, U16, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::data::{DataType, RecordReader, RecordWriter};

/// `TL_CAPTURE_METADATA_VERSION`: the layout described here.
pub const METADATA_VERSION: u8 = 1;
/// `TL_CAPTURE_METADATA_FIXED_LEN`: the head this build reads; a device may
/// declare a longer one.
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
    /// Layout of the reply; a reader that does not know it should stop.
    pub version: u8,
    /// Element encoding.
    pub data_type: DataType,
    /// Bytes of capture data the block RPCs will serve: `length` elements.
    pub data_size: u32,
    /// Bytes one block RPC returns, so a reader knows how many to ask for.
    pub block_size: u16,
    /// Elements captured.
    pub length: u32,
    /// Multiplier taking a raw element to its value in `units`.
    pub y_calibration: f32,
    /// x of element zero, in `x_units`.
    pub x_offset: f32,
    /// x step between consecutive elements.
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
    /// Serialize an RPC reply payload into `buf`; returns its length. Strings
    /// are truncated to fit whatever room remains after the head.
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

    /// Parse an RPC reply payload. [`Self::version`] is reported rather than
    /// checked: only the caller knows which versions it can act on.
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
    use super::*;

    fn metadata() -> CaptureMetadata<'static> {
        CaptureMetadata {
            version: METADATA_VERSION,
            data_type: DataType::F32,
            data_size: 8,
            block_size: 256,
            length: 2,
            y_calibration: 1.0,
            x_offset: 0.5,
            x_stride: 0.25,
            name: "vb",
            units: "V",
            x_name: "t",
            x_units: "s",
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
            0x08, 0x00, 0x00, 0x00,     // data_size
            0x00, 0x01,                 // block_size 256
            0x02, 0x00, 0x00, 0x00,     // length
            0x00, 0x00, 0x80, 0x3F,     // y_calibration 1.0f
            0x00, 0x00, 0x00, 0x3F,     // x_offset 0.5f
            0x00, 0x00, 0x80, 0x3E,     // x_stride 0.25f
            2, 1, 1, 1,                 // name, units, x_name, x_units lengths
            b'v', b'b', b'V', b't', b's',
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
        assert_eq!(parsed.length, 2);
    }
}

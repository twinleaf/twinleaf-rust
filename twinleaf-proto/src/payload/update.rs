//! The payloads of a firmware update, the header a signed package opens with
//! and the chunks an image is uploaded in.
//!
//! A signed package is a [`PackageHeader`] of [`HEADER_SIZE`] bytes and the
//! image behind it. The header goes in a chunk of its own, and each chunk of
//! the image after it opens with the image offset its bytes start at, as a
//! little endian `u32` of [`PACKAGE_OFFSET_SIZE`] bytes.
//!
//! An image that is not a package goes in chunks of [`IMAGE_CHUNK_SIZE`]
//! bytes, of which [`IMAGE_CHUNK_ENVELOPE`] bytes are envelope and the rest
//! image.

/// The length of a package header.
pub const HEADER_SIZE: usize = 288;
/// The header layout this crate reads.
pub const FORMAT_VERSION: u16 = 1;
/// The header flag a package signed with a development key carries.
pub const FLAG_DEVELOPMENT: u16 = 1;

/// The size of the chunks an image is uploaded in.
pub const IMAGE_CHUNK_SIZE: usize = 288;
/// The bytes of each image chunk that are envelope, not image.
pub const IMAGE_CHUNK_ENVELOPE: usize = 32;
/// The bytes of offset that open each chunk of a package's image.
pub const PACKAGE_OFFSET_SIZE: usize = 4;

// Where each field of a package header sits.
const MAGIC: usize = 0;
const FORMAT: usize = 8;
const SIZE: usize = 10;
const BOARD_ID: usize = 12;
const HW_REV: usize = 20;
const FLAGS: usize = 22;
const IMAGE_LEN: usize = 24;
const SIGNATURE: usize = 32;

/// The fields of a package header, as its bytes hold them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackageHeader<'a> {
    /// The eight bytes that name the firmware the package is for.
    pub magic: &'a [u8; 8],
    /// The header layout, [`FORMAT_VERSION`] for the one this crate reads.
    pub format: u16,
    /// The length of the header, [`HEADER_SIZE`] for the layout this crate
    /// reads.
    pub header_size: u16,
    /// The board the package is for.
    pub board_id: &'a [u8; 8],
    /// The hardware revision the package is for.
    pub hw_rev: u16,
    /// The header flags, [`FLAG_DEVELOPMENT`] among them.
    pub flags: u16,
    /// The bytes of image behind the header.
    pub image_len: u32,
    /// The signature over the image.
    pub signature: &'a [u8; 64],
}

impl<'a> PackageHeader<'a> {
    /// Reads the fields of `header`. It judges none of them.
    pub fn parse(header: &'a [u8; HEADER_SIZE]) -> Self {
        let word = |at: usize| u16::from_le_bytes([header[at], header[at + 1]]);
        Self {
            magic: field(header, MAGIC),
            format: word(FORMAT),
            header_size: word(SIZE),
            board_id: field(header, BOARD_ID),
            hw_rev: word(HW_REV),
            flags: word(FLAGS),
            image_len: u32::from_le_bytes(*field(header, IMAGE_LEN)),
            signature: field(header, SIGNATURE),
        }
    }

    /// Writes the fields into `out`, and zero everywhere else.
    pub fn write(&self, out: &mut [u8; HEADER_SIZE]) {
        out.fill(0);
        let mut put = |at: usize, bytes: &[u8]| out[at..at + bytes.len()].copy_from_slice(bytes);
        put(MAGIC, self.magic);
        put(FORMAT, &self.format.to_le_bytes());
        put(SIZE, &self.header_size.to_le_bytes());
        put(BOARD_ID, self.board_id);
        put(HW_REV, &self.hw_rev.to_le_bytes());
        put(FLAGS, &self.flags.to_le_bytes());
        put(IMAGE_LEN, &self.image_len.to_le_bytes());
        put(SIGNATURE, self.signature);
    }
}

/// The `N` bytes of `header` from offset `at`.
fn field<const N: usize>(header: &[u8; HEADER_SIZE], at: usize) -> &[u8; N] {
    header[at..at + N]
        .try_into()
        .expect("every field lies inside the header")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> PackageHeader<'static> {
        PackageHeader {
            magic: b"MAGIC\x01\x02\x03",
            format: 0x0102,
            header_size: 0x0304,
            board_id: b"BOARD\x04\x05\x06",
            hw_rev: 0x0506,
            flags: 0x0708,
            image_len: 0x090A_0B0C,
            signature: &[0xEE; 64],
        }
    }

    /// A header written reads back field for field.
    #[test]
    fn a_written_header_reads_back() {
        let mut out = [0xFF; HEADER_SIZE];
        header().write(&mut out);
        assert_eq!(PackageHeader::parse(&out), header());
    }
}

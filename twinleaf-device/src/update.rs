//! The firmware upload cursor: what `dev.firmware.upload` has taken of an
//! image, and what `dev.firmware.upgrade` may commit.
//!
//! A chunk arrives encrypted. The platform decrypts it against the device key
//! the crate never holds, hands the plaintext to [`Upload::chunk`], and writes
//! the [`Persist`] it gets back. Nothing but a reboot moves the cursor back,
//! so a host that lost an acknowledgement reads the cursor and resumes there.

use twinleaf_proto::rpc::RpcError;
use twinleaf_proto::serial::CRC32;

/// The cipher block a chunk is encrypted in, and the size of the
/// initialization vector it opens with.
pub const BLOCK_SIZE: usize = 16;

/// The header a decrypted chunk begins with.
pub const HEADER_SIZE: usize = 16;

/// The smallest chunk: an initialization vector, a header, and one block.
pub const CHUNK_MIN: usize = BLOCK_SIZE + HEADER_SIZE + BLOCK_SIZE;

/// The largest chunk.
pub const CHUNK_MAX: usize = 288;

/// The most image bytes one chunk carries.
pub const DATA_MAX: usize = CHUNK_MAX - BLOCK_SIZE - HEADER_SIZE;

/// One chunk as it arrives: the initialization vector it opens with, and the
/// ciphertext the platform decrypts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Envelope<'a> {
    /// The AES-CBC initialization vector.
    pub iv: &'a [u8],
    /// A header and image bytes, still encrypted.
    pub cipher: &'a [u8],
}

impl<'a> Envelope<'a> {
    /// Split the argument of `dev.firmware.upload`, which is whole cipher
    /// blocks and no smaller than [`CHUNK_MIN`] or larger than [`CHUNK_MAX`].
    pub fn parse(args: &'a [u8]) -> Result<Self, RpcError> {
        let fits =
            args.len().is_multiple_of(BLOCK_SIZE) && (CHUNK_MIN..=CHUNK_MAX).contains(&args.len());
        if !fits {
            return Err(RpcError::ArgsSize);
        }
        let (iv, cipher) = args.split_at(BLOCK_SIZE);
        Ok(Self { iv, cipher })
    }
}

/// The header a decrypted chunk begins with: which image it belongs to, how
/// big that image is, where these bytes go, and a CRC32 over them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// Bytes in the whole image.
    pub size: u32,
    /// Where this chunk's bytes begin.
    pub offset: u32,
    /// CRC32 over this chunk's bytes.
    pub crc: u32,
    /// Which image this is, the same in every chunk of it.
    pub id: u32,
}

impl Header {
    /// The header of a decrypted chunk, and the image bytes after it.
    pub fn parse(plain: &[u8]) -> Result<(Self, &[u8]), RpcError> {
        let (header, data) = plain
            .split_at_checked(HEADER_SIZE)
            .ok_or(RpcError::ArgsSize)?;
        let (words, _) = header.as_chunks::<4>();
        let [size, offset, crc, id] = core::array::from_fn(|word| u32::from_le_bytes(words[word]));
        Ok((
            Self {
                size,
                offset,
                crc,
                id,
            },
            data,
        ))
    }

    /// The header as a chunk carries it.
    pub fn bytes(self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        let (fields, _) = bytes.as_chunks_mut::<4>();
        let words = [self.size, self.offset, self.crc, self.id];
        for (field, word) in fields.iter_mut().zip(words) {
            *field = word.to_le_bytes();
        }
        bytes
    }
}

/// An accepted chunk, for the platform to write and verify.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Persist<'a> {
    /// Where the bytes go in the image.
    pub offset: u32,
    /// The image bytes themselves.
    pub bytes: &'a [u8],
}

/// The image an upload is taking: how big it is, which one it is, and how much
/// of it the device holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Image {
    size: u32,
    id: u32,
    cursor: u32,
}

/// The upload cursor: the image `dev.firmware.upload` is taking, chunk by
/// chunk, until `dev.firmware.upgrade` commits it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Upload {
    image: Option<Image>,
}

impl Upload {
    /// An upload holding nothing, as a boot leaves it.
    pub const fn new() -> Self {
        Self { image: None }
    }

    /// What `dev.firmware.upload` replies: how much of the image the device
    /// holds, and where the next chunk must start.
    pub fn cursor(&self) -> u32 {
        self.image.map_or(0, |image| image.cursor)
    }

    /// Take one decrypted chunk. A chunk of the wrong size is
    /// [`RpcError::ArgsSize`]; one of another image, or one that does not
    /// start at the cursor, is [`RpcError::Invalid`] and leaves the cursor
    /// where it was.
    pub fn chunk<'a>(&mut self, plain: &'a [u8]) -> Result<Persist<'a>, RpcError> {
        let (header, bytes) = Header::parse(plain)?;
        let sized = (BLOCK_SIZE..=DATA_MAX).contains(&bytes.len())
            && bytes.len().is_multiple_of(BLOCK_SIZE);
        if !sized {
            return Err(RpcError::ArgsSize);
        }
        if header.size == 0 || CRC32.checksum(bytes) != header.crc {
            return Err(RpcError::Invalid);
        }
        let image = match self.image {
            Some(image)
                if (header.size, header.id, header.offset)
                    != (image.size, image.id, image.cursor) =>
            {
                return Err(RpcError::Invalid)
            }
            Some(image) => image,
            None if header.offset != 0 => return Err(RpcError::Invalid),
            None => Image {
                size: header.size,
                id: header.id,
                cursor: 0,
            },
        };
        let cursor = image
            .cursor
            .checked_add(bytes.len() as u32)
            .filter(|&cursor| cursor <= image.size)
            .ok_or(RpcError::Invalid)?;
        self.image = Some(Image { cursor, ..image });
        Ok(Persist {
            offset: image.cursor,
            bytes,
        })
    }

    /// Whether `dev.firmware.upgrade` may commit: every byte of an image is
    /// here. The platform verifies its signature and performs the swap.
    pub fn commit(&self) -> Result<(), RpcError> {
        match self.image {
            Some(image) if image.cursor == image.size => Ok(()),
            Some(_) | None => Err(RpcError::State),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: u32 = 0xA1B2_C3D4;

    /// One chunk of an image, as the packaging tool writes it and a device
    /// reads it once decrypted.
    fn chunk(size: u32, offset: u32, id: u32, bytes: &[u8]) -> Vec<u8> {
        let header = Header {
            size,
            offset,
            crc: CRC32.checksum(bytes),
            id,
        };
        [&header.bytes()[..], bytes].concat()
    }

    fn block(fill: u8) -> Vec<u8> {
        vec![fill; DATA_MAX]
    }

    #[test]
    fn an_envelope_is_whole_blocks_within_the_sizes_a_chunk_takes() {
        let args = vec![7u8; CHUNK_MAX];
        let envelope = Envelope::parse(&args).unwrap();
        assert_eq!(envelope.iv.len(), BLOCK_SIZE);
        assert_eq!(envelope.cipher.len(), HEADER_SIZE + DATA_MAX);

        for len in [0, CHUNK_MIN - BLOCK_SIZE, CHUNK_MIN - 1, CHUNK_MAX + 16] {
            assert_eq!(
                Envelope::parse(&vec![0u8; len]).err(),
                Some(RpcError::ArgsSize),
                "{len} bytes"
            );
        }
        assert!(Envelope::parse(&[0u8; CHUNK_MIN]).is_ok());
    }

    #[test]
    fn a_header_round_trips_through_the_bytes_a_chunk_carries() {
        let header = Header {
            size: 0x0001_0000,
            offset: 256,
            crc: 0xDEAD_BEEF,
            id: ID,
        };
        let plain = [&header.bytes()[..], &[1, 2, 3]].concat();
        assert_eq!(Header::parse(&plain), Ok((header, &[1, 2, 3][..])));
        assert_eq!(
            Header::parse(&[0; HEADER_SIZE - 1]),
            Err(RpcError::ArgsSize)
        );
    }

    #[test]
    fn chunks_advance_the_cursor_to_the_end_of_the_image() {
        let mut upload = Upload::new();
        assert_eq!(upload.cursor(), 0);
        assert_eq!(upload.commit(), Err(RpcError::State));

        let size = 2 * DATA_MAX as u32;
        let first = chunk(size, 0, ID, &block(1));
        assert_eq!(
            upload.chunk(&first),
            Ok(Persist {
                offset: 0,
                bytes: &block(1)[..]
            })
        );
        assert_eq!(upload.cursor(), DATA_MAX as u32);
        assert_eq!(upload.commit(), Err(RpcError::State));

        let second = chunk(size, DATA_MAX as u32, ID, &block(2));
        assert_eq!(
            upload.chunk(&second),
            Ok(Persist {
                offset: DATA_MAX as u32,
                bytes: &block(2)[..]
            })
        );
        assert_eq!(upload.cursor(), size);
        assert_eq!(upload.commit(), Ok(()));
    }

    #[test]
    fn a_chunk_that_does_not_start_at_the_cursor_is_refused() {
        let size = 3 * DATA_MAX as u32;
        let mut upload = Upload::new();
        assert_eq!(
            upload.chunk(&chunk(size, DATA_MAX as u32, ID, &block(1))),
            Err(RpcError::Invalid)
        );
        assert_eq!(upload.cursor(), 0);

        upload.chunk(&chunk(size, 0, ID, &block(1))).unwrap();
        for offset in [0, 2 * DATA_MAX as u32] {
            assert_eq!(
                upload.chunk(&chunk(size, offset, ID, &block(2))),
                Err(RpcError::Invalid)
            );
            assert_eq!(upload.cursor(), DATA_MAX as u32);
        }
    }

    #[test]
    fn a_chunk_of_another_image_is_refused_until_a_reboot() {
        let size = 2 * DATA_MAX as u32;
        let mut upload = Upload::new();
        upload.chunk(&chunk(size, 0, ID, &block(1))).unwrap();

        assert_eq!(
            upload.chunk(&chunk(size, DATA_MAX as u32, ID + 1, &block(2))),
            Err(RpcError::Invalid)
        );
        assert_eq!(
            upload.chunk(&chunk(
                size + DATA_MAX as u32,
                DATA_MAX as u32,
                ID,
                &block(2)
            )),
            Err(RpcError::Invalid)
        );
        assert_eq!(upload.cursor(), DATA_MAX as u32);

        upload = Upload::new();
        assert!(upload.chunk(&chunk(size, 0, ID + 1, &block(2))).is_ok());
    }

    #[test]
    fn a_chunk_is_refused_for_its_crc_its_size_or_running_past_the_image() {
        let mut upload = Upload::new();
        let mut corrupt = chunk(DATA_MAX as u32, 0, ID, &block(1));
        corrupt[HEADER_SIZE] ^= 0xFF;
        assert_eq!(upload.chunk(&corrupt), Err(RpcError::Invalid));

        assert_eq!(
            upload.chunk(&chunk(0, 0, ID, &block(1))),
            Err(RpcError::Invalid)
        );
        assert_eq!(
            upload.chunk(&chunk(DATA_MAX as u32, 0, ID, &[1, 2, 3])),
            Err(RpcError::ArgsSize)
        );
        assert_eq!(
            upload.chunk(&chunk(BLOCK_SIZE as u32, 0, ID, &block(1))),
            Err(RpcError::Invalid)
        );
        assert_eq!(upload.cursor(), 0);
    }

    #[test]
    fn a_short_last_chunk_completes_the_image() {
        let size = DATA_MAX as u32 + BLOCK_SIZE as u32;
        let mut upload = Upload::new();
        upload.chunk(&chunk(size, 0, ID, &block(1))).unwrap();
        let tail = vec![9u8; BLOCK_SIZE];
        assert_eq!(
            upload.chunk(&chunk(size, DATA_MAX as u32, ID, &tail)),
            Ok(Persist {
                offset: DATA_MAX as u32,
                bytes: &tail[..]
            })
        );
        assert_eq!(upload.commit(), Ok(()));
        assert_eq!(upload.cursor(), size);
    }
}

//! The firmware upload cursor: what `dev.firmware.upload` has taken of a
//! signed package, and what `dev.firmware.upgrade` may commit.
//!
//! A package opens with a header naming the board it is for and carrying a
//! signature over the image behind it. This module reads that header, names
//! the bytes the platform writes and where they go, and hands back the
//! manifest the platform verifies against a key the crate never holds.
//! Nothing but a reboot or a timeout moves the cursor back, so a host that
//! lost an acknowledgement reads the cursor and resumes there.

use twinleaf_proto::rpc::RpcError;
use twinleaf_proto::{BoardId, FirmwareMagic, HwRev};

/// The package header: magic, layout, board, revision, flags, image length,
/// and the signature over the image.
pub const HEADER_SIZE: usize = 288;

/// The header layout this crate reads.
pub const FORMAT_VERSION: u16 = 1;

/// The header flag a package signed with a development key carries.
const FLAG_DEVELOPMENT: u16 = 1;

/// How long an upload may sit idle before the next chunk starts a new one.
///
/// tl-chibi has no timeout at all, so a host that dies mid-transfer strands the
/// session until the next reset and no later host can upload anything. Thirty
/// seconds sits far above any gap a live host produces — a 288-byte chunk is
/// ~30 ms on a 115200 control port, and the page erase behind it tens of
/// milliseconds — and far below how long a person takes to retry.
pub const IDLE_TIMEOUT_NS: u64 = 30_000_000_000;

/// What a board takes a package for: who it is, and how big an image fits.
pub struct Package {
    /// The eight bytes a package for this board opens with.
    pub magic: FirmwareMagic,
    /// The board the package must name.
    pub board_id: BoardId,
    /// The hardware revision the package must name.
    pub hw_rev: HwRev,
    /// Whether the key this board verifies with is the development key.
    pub development: bool,
    /// The largest image the running partition holds.
    pub image_max: usize,
}

/// What a header promises: how long the image is, and the signature over it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Manifest {
    /// Bytes of image behind the header.
    pub image_len: u32,
    /// Ed25519 over the image, for the platform to verify.
    pub signature: [u8; 64],
}

impl Manifest {
    /// Read a package header against what this board takes.
    fn parse(package: &Package, data: &[u8]) -> Result<Self, RpcError> {
        let word = |at: usize| u16::from_le_bytes([data[at], data[at + 1]]);
        if data.len() != HEADER_SIZE || &data[..8] != package.magic.as_bytes() {
            return Err(RpcError::Invalid);
        }
        if word(8) != FORMAT_VERSION
            || word(10) as usize != HEADER_SIZE
            || &data[12..20] != package.board_id.as_bytes()
            || word(20) != package.hw_rev.value()
        {
            return Err(RpcError::Invalid);
        }
        let flags = word(22);
        if (flags & FLAG_DEVELOPMENT != 0) != package.development || flags & !FLAG_DEVELOPMENT != 0
        {
            return Err(RpcError::Invalid);
        }
        let image_len = u32::from_le_bytes(data[24..28].try_into().unwrap());
        if !(8..=package.image_max as u32).contains(&image_len) {
            return Err(RpcError::Range);
        }
        Ok(Self {
            image_len,
            signature: data[32..96].try_into().unwrap(),
        })
    }
}

/// How far through a package the device is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    /// Waiting for a package header. The manifest beside it means nothing.
    Header,
    /// Taking image bytes, this many of them held.
    Payload(u32),
    /// Every byte is here, waiting for `dev.firmware.upgrade`.
    Complete,
    /// The swap is armed; only the reboot that performs it clears this.
    Committed,
}

/// The upload cursor: the package `dev.firmware.upload` is taking, chunk by
/// chunk, until `dev.firmware.upgrade` commits it.
pub struct Upload {
    state: State,
    /// What the header this upload is taking promised, once it has one.
    manifest: Manifest,
    /// When the last chunk landed. `None` whenever no upload is in progress,
    /// so a fresh cursor never looks stale.
    touched: Option<u64>,
    /// Whether a part-written image has been thrown away since the last
    /// header, which the platform is told so it can clear the slot.
    discarded: bool,
}

impl Default for Upload {
    fn default() -> Self {
        Self::new()
    }
}

impl Upload {
    /// An upload holding nothing, as a boot leaves it.
    pub const fn new() -> Self {
        Self {
            state: State::Header,
            manifest: Manifest {
                image_len: 0,
                signature: [0; 64],
            },
            touched: None,
            discarded: false,
        }
    }

    /// What `dev.firmware.upload` replies: how much of the image the device
    /// holds, so a reconnecting host sees where it stands.
    ///
    /// Takes `&mut self` because it applies the idle timeout first: a host must
    /// not be told to resume at an offset the next chunk will not accept.
    pub fn cursor(&mut self, now_ns: u64) -> u32 {
        self.expire(now_ns);
        match self.state {
            State::Header => 0,
            State::Payload(received) => received,
            State::Complete | State::Committed => self.manifest.image_len,
        }
    }

    /// `dev.firmware.abort`: throw away a part-uploaded image.
    ///
    /// tl-chibi has no equivalent — there, `dev.reboot` is the only way out of
    /// a stranded upload.
    pub fn abort(&mut self) -> Result<(), RpcError> {
        match self.state {
            // The swap is armed in flash. Clearing the cursor here would claim
            // an undo that did not happen.
            State::Committed => Err(RpcError::State),
            State::Header | State::Payload(_) | State::Complete => {
                self.reset();
                Ok(())
            }
        }
    }

    /// What one chunk of `dev.firmware.upload` asks of the platform.
    pub fn chunk<'u, 'a>(
        &'u mut self,
        package: &Package,
        data: &'a [u8],
        now_ns: u64,
    ) -> Result<Take<'u, 'a>, RpcError> {
        self.expire(now_ns);
        match self.state {
            State::Header => {
                self.manifest = Manifest::parse(package, data)?;
                self.state = State::Payload(0);
                self.touched = Some(now_ns);
                Ok(Take::Header {
                    discarded: core::mem::take(&mut self.discarded),
                })
            }
            State::Payload(received) => {
                let remaining = self.manifest.image_len - received;
                if data.is_empty() || data.len() > remaining as usize {
                    return Err(RpcError::ArgsSize);
                }
                self.touched = Some(now_ns);
                Ok(Take::Image(Image {
                    upload: self,
                    offset: received,
                    bytes: data,
                }))
            }
            State::Complete | State::Committed => Err(RpcError::State),
        }
    }

    /// The image `dev.firmware.upgrade` may commit, for the platform to verify
    /// and swap in. Every byte of it is here, and the swap is not yet armed.
    pub fn upgrade(&mut self, now_ns: u64) -> Result<Manifest, RpcError> {
        self.expire(now_ns);
        match self.state {
            State::Complete => Ok(self.manifest),
            State::Header | State::Payload(_) | State::Committed => Err(RpcError::State),
        }
    }

    /// The swap is armed: only the reboot that performs it moves the cursor.
    pub fn armed(&mut self) {
        match self.state {
            State::Complete => {
                self.state = State::Committed;
                self.touched = None;
            }
            State::Header | State::Payload(_) | State::Committed => {}
        }
    }

    /// Forget an upload no host has touched for [`IDLE_TIMEOUT_NS`].
    ///
    /// Checked lazily, on the way into the next call, rather than from a timer:
    /// nothing has to happen at the instant a session goes stale, only before
    /// the next host is answered. Without this a host that dies mid-transfer
    /// leaves the cursor set until the next reset, and the following host's
    /// package header is written into flash *as image data*, failing at
    /// signature verification with nothing to say why.
    fn expire(&mut self, now_ns: u64) {
        let idle = self
            .touched
            .is_some_and(|last| now_ns.saturating_sub(last) >= IDLE_TIMEOUT_NS);
        // `Header` has nothing to lose, and `Committed` has already armed the
        // swap in flash, where only the reboot that performs it clears it.
        let losable = matches!(self.state, State::Payload(_) | State::Complete);
        if idle && losable {
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.discarded |= self.state != State::Header;
        self.state = State::Header;
        self.touched = None;
    }
}

/// What one chunk of `dev.firmware.upload` asks of the platform.
pub enum Take<'u, 'a> {
    /// A package header this board takes. `discarded` when a part-written
    /// image was thrown away and the slot must be cleared before this one.
    Header {
        /// Whether the slot holds bytes of an abandoned image.
        discarded: bool,
    },
    /// Image bytes to write.
    Image(Image<'u, 'a>),
}

/// Image bytes the platform writes, and the cursor they move once written.
///
/// Dropping one without [`Image::written`] leaves the cursor where it was, so
/// a failed write is resumed rather than skipped.
pub struct Image<'u, 'a> {
    upload: &'u mut Upload,
    offset: u32,
    bytes: &'a [u8],
}

impl Image<'_, '_> {
    /// Where the bytes go in the image.
    pub fn offset(&self) -> u32 {
        self.offset
    }

    /// The bytes themselves.
    pub fn bytes(&self) -> &[u8] {
        self.bytes
    }

    /// Whether these are the image's last bytes, the only ones a platform may
    /// take ragged.
    pub fn last(&self) -> bool {
        self.offset + self.bytes.len() as u32 == self.upload.manifest.image_len
    }

    /// The bytes reached the flash: the new cursor.
    pub fn written(self) -> u32 {
        let received = self.offset + self.bytes.len() as u32;
        self.upload.state = match received == self.upload.manifest.image_len {
            true => State::Complete,
            false => State::Payload(received),
        };
        received
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IMAGE_LEN: u32 = 64 * 1024;
    const SIGNATURE: [u8; 64] = [0xAB; 64];
    const BOOT: u64 = 0;

    fn package() -> Package {
        Package {
            magic: FirmwareMagic::new(*b"SANDIFW\0"),
            board_id: BoardId::from_ascii("COMM-USB"),
            hw_rev: HwRev::new(8),
            development: true,
            image_max: 224 * 1024,
        }
    }

    /// A package header the running firmware should take.
    fn header(package: &Package) -> [u8; HEADER_SIZE] {
        let mut data = [0; HEADER_SIZE];
        data[..8].copy_from_slice(package.magic.as_bytes());
        data[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        data[10..12].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
        data[12..20].copy_from_slice(package.board_id.as_bytes());
        data[20..22].copy_from_slice(&package.hw_rev.to_le_bytes());
        data[22..24].copy_from_slice(&FLAG_DEVELOPMENT.to_le_bytes());
        data[24..28].copy_from_slice(&IMAGE_LEN.to_le_bytes());
        data[32..96].copy_from_slice(&SIGNATURE);
        data
    }

    /// A header for an image of `len` bytes, and the image itself.
    fn small(package: &Package, len: u32) -> ([u8; HEADER_SIZE], Vec<u8>) {
        let mut header = header(package);
        header[24..28].copy_from_slice(&len.to_le_bytes());
        (header, (0..len).map(|byte| byte as u8).collect())
    }

    /// Take one chunk, writing nothing, and answer with the new cursor.
    fn take(
        upload: &mut Upload,
        package: &Package,
        data: &[u8],
        now: u64,
    ) -> Result<u32, RpcError> {
        match upload.chunk(package, data, now)? {
            Take::Header { discarded } => {
                assert!(!discarded, "nothing was thrown away");
                Ok(0)
            }
            Take::Image(image) => Ok(image.written()),
        }
    }

    #[test]
    fn the_cursor_follows_the_image_bytes_the_device_holds() {
        let package = package();
        let (header, image) = small(&package, 64);
        let mut upload = Upload::new();

        assert_eq!(upload.cursor(BOOT), 0, "nothing uploaded yet");
        assert_eq!(
            take(&mut upload, &package, &header, BOOT),
            Ok(0),
            "the header is not image data"
        );
        assert_eq!(upload.upgrade(BOOT), Err(RpcError::State));
        assert_eq!(take(&mut upload, &package, &image[..32], BOOT), Ok(32));
        assert_eq!(upload.cursor(BOOT), 32);
        assert_eq!(take(&mut upload, &package, &image[32..], BOOT), Ok(64));
        assert_eq!(upload.cursor(BOOT), 64, "a whole image reads back whole");
        assert_eq!(upload.upgrade(BOOT).unwrap().image_len, 64);
    }

    /// A write the platform refused leaves the cursor where it was, so the
    /// host resends those bytes rather than leaving a hole in the image.
    #[test]
    fn bytes_the_platform_did_not_write_do_not_move_the_cursor() {
        let package = package();
        let (header, image) = small(&package, 64);
        let mut upload = Upload::new();
        take(&mut upload, &package, &header, BOOT).unwrap();

        {
            let Ok(Take::Image(refused)) = upload.chunk(&package, &image[..32], BOOT) else {
                panic!("image bytes")
            };
            assert_eq!(
                (refused.offset(), refused.bytes().len(), refused.last()),
                (0, 32, false)
            );
        }
        assert_eq!(upload.cursor(BOOT), 0);

        take(&mut upload, &package, &image[..32], BOOT).unwrap();
        let Ok(Take::Image(tail)) = upload.chunk(&package, &image[32..], BOOT) else {
            panic!("image bytes")
        };
        assert!(tail.last(), "only the last bytes may be ragged");
    }

    /// The bug the timeout exists for: a host dies mid-transfer and the next
    /// one's package header would be taken as image data.
    #[test]
    fn an_abandoned_upload_gives_way_to_the_next_host() {
        let package = package();
        let (header, image) = small(&package, 64);
        let mut upload = Upload::new();
        take(&mut upload, &package, &header, BOOT).unwrap();
        take(&mut upload, &package, &image[..32], BOOT).unwrap();

        // Still inside the window: this really is the same host, so the header
        // bytes are image data and are taken as such.
        assert_eq!(upload.cursor(BOOT + IDLE_TIMEOUT_NS - 1), 32);

        let gone = BOOT + IDLE_TIMEOUT_NS;
        assert_eq!(upload.cursor(gone), 0, "the stale cursor is forgotten");
        let Ok(Take::Header { discarded }) = upload.chunk(&package, &header, gone) else {
            panic!("a header starts a fresh upload")
        };
        assert!(discarded, "the slot holds bytes of the abandoned image");
    }

    /// Every accepted chunk restarts the clock, so a slow but live host is
    /// never cut off.
    #[test]
    fn a_slow_host_keeps_its_cursor() {
        let package = package();
        let (header, image) = small(&package, 64);
        let mut upload = Upload::new();

        let mut at = BOOT;
        take(&mut upload, &package, &header, at).unwrap();
        for chunk in image.chunks(8) {
            at += IDLE_TIMEOUT_NS - 1;
            take(&mut upload, &package, chunk, at).unwrap();
        }
        assert_eq!(upload.cursor(at), 64);
    }

    /// A finished-but-uncommitted upload also times out, or a host that walked
    /// away between the last chunk and `dev.firmware.upgrade` would strand the
    /// device exactly as it does in tl-chibi.
    #[test]
    fn a_complete_upload_left_uncommitted_also_expires() {
        let package = package();
        let (header, image) = small(&package, 64);
        let mut upload = Upload::new();
        take(&mut upload, &package, &header, BOOT).unwrap();
        take(&mut upload, &package, &image, BOOT).unwrap();

        let gone = BOOT + IDLE_TIMEOUT_NS;
        assert_eq!(upload.upgrade(gone), Err(RpcError::State));
        assert_eq!(upload.cursor(gone), 0);
    }

    /// An armed swap is the one thing a host cannot take back.
    #[test]
    fn abort_frees_the_cursor_until_the_swap_is_armed() {
        let package = package();
        let (header, image) = small(&package, 64);
        let mut upload = Upload::new();

        assert_eq!(upload.abort(), Ok(()), "there is simply nothing to lose");
        take(&mut upload, &package, &header, BOOT).unwrap();
        take(&mut upload, &package, &image[..32], BOOT).unwrap();
        assert_eq!(upload.abort(), Ok(()));
        assert_eq!(upload.cursor(BOOT), 0);

        let Ok(Take::Header { discarded }) = upload.chunk(&package, &header, BOOT) else {
            panic!("the next host starts from the header")
        };
        assert!(discarded, "over what the aborted upload left");
        take(&mut upload, &package, &image, BOOT).unwrap();
        upload.upgrade(BOOT).unwrap();
        upload.armed();
        assert_eq!(upload.cursor(BOOT + IDLE_TIMEOUT_NS), 64, "and it stays");
        assert_eq!(upload.abort(), Err(RpcError::State));
        assert_eq!(upload.upgrade(BOOT), Err(RpcError::State));
        assert_eq!(
            upload.chunk(&package, &header, BOOT).err(),
            Some(RpcError::State)
        );
    }

    #[test]
    fn a_chunk_that_runs_past_the_image_or_carries_nothing_is_refused() {
        let package = package();
        let (header, image) = small(&package, 64);
        let mut upload = Upload::new();
        take(&mut upload, &package, &header, BOOT).unwrap();

        for data in [&[][..], &[0u8; 65][..]] {
            assert_eq!(
                upload.chunk(&package, data, BOOT).err(),
                Some(RpcError::ArgsSize)
            );
        }
        assert_eq!(take(&mut upload, &package, &image, BOOT), Ok(64));
    }

    #[test]
    fn accepts_a_matching_development_package() {
        let package = package();
        let manifest = Manifest::parse(&package, &header(&package)).unwrap();
        assert_eq!(manifest.image_len, IMAGE_LEN);
        assert_eq!(manifest.signature, SIGNATURE);
    }

    #[test]
    fn rejects_a_package_for_another_board_or_revision() {
        let package = package();

        let mut wrong_magic = header(&package);
        wrong_magic[..8].copy_from_slice(b"ETHANFW\0");
        assert_eq!(
            Manifest::parse(&package, &wrong_magic),
            Err(RpcError::Invalid)
        );

        let mut wrong_board = header(&package);
        wrong_board[12..20].copy_from_slice(BoardId::from_ascii("ETHAN").as_bytes());
        assert_eq!(
            Manifest::parse(&package, &wrong_board),
            Err(RpcError::Invalid)
        );

        let mut wrong_revision = header(&package);
        wrong_revision[20..22].copy_from_slice(&HwRev::new(7).to_le_bytes());
        assert_eq!(
            Manifest::parse(&package, &wrong_revision),
            Err(RpcError::Invalid)
        );

        let mut wrong_version = header(&package);
        wrong_version[8..10].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        assert_eq!(
            Manifest::parse(&package, &wrong_version),
            Err(RpcError::Invalid)
        );
    }

    /// A build carrying the test key must not take production packages, and a
    /// production build must not take development ones.
    #[test]
    fn development_and_release_packages_do_not_cross_over() {
        let development = package();
        let mut release_package = header(&development);
        release_package[22..24].copy_from_slice(&0u16.to_le_bytes());
        assert_eq!(
            Manifest::parse(&development, &release_package),
            Err(RpcError::Invalid)
        );

        let release = Package {
            development: false,
            ..package()
        };
        assert_eq!(
            Manifest::parse(&release, &header(&development)),
            Err(RpcError::Invalid)
        );
        assert!(Manifest::parse(&release, &release_package).is_ok());
    }

    #[test]
    fn rejects_unknown_flag_bits() {
        let package = package();
        let mut data = header(&package);
        data[22..24].copy_from_slice(&(FLAG_DEVELOPMENT | 0x8000).to_le_bytes());
        assert_eq!(Manifest::parse(&package, &data), Err(RpcError::Invalid));
    }

    #[test]
    fn rejects_an_image_that_does_not_fit_the_running_partition() {
        let package = package();
        let with_len = |len: u32| {
            let mut data = header(&package);
            data[24..28].copy_from_slice(&len.to_le_bytes());
            Manifest::parse(&package, &data)
        };
        assert_eq!(with_len(0), Err(RpcError::Range));
        assert_eq!(with_len(7), Err(RpcError::Range));
        assert_eq!(with_len(package.image_max as u32 + 1), Err(RpcError::Range));
        assert!(with_len(package.image_max as u32).is_ok());
    }

    #[test]
    fn rejects_a_truncated_header() {
        let package = package();
        let data = header(&package);
        assert_eq!(
            Manifest::parse(&package, &data[..HEADER_SIZE - 1]),
            Err(RpcError::Invalid)
        );
    }
}

//! Firmware updates.
//!
//! [`query_installed`] reads what firmware a device runs, [`check_for_update`]
//! compares it with a catalog of published firmware, and [`flash`] uploads an
//! image. The module prints nothing and asks the user nothing. Callers such
//! as the `tio` CLI and the macOS app provide the user interface.
//!
//! A [`FirmwareCatalog`] supplies the published firmware. The [`github`]
//! module provides one backed by GitHub, behind the `firmware-update`
//! feature.

use crate::device::Device;
use crate::proto::rpc as wire_rpc;
use crate::proto::update::{
    PackageHeader, FORMAT_VERSION, HEADER_SIZE, IMAGE_CHUNK_ENVELOPE, IMAGE_CHUNK_SIZE,
    PACKAGE_OFFSET_SIZE,
};
use crate::session::rpc::{pipelined, CallError};
use crate::session::{Presence, RecvError};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[cfg(feature = "firmware-update")]
mod directory;
#[cfg(feature = "firmware-update")]
pub mod github;
#[cfg(feature = "firmware-update")]
pub use directory::{catalog_from_env, DirectoryCatalog, DIR_VARIABLE};

/// The image bytes in each chunk of a signed package, behind the four bytes
/// of offset the chunk opens with. A power of two, so a chunk is a whole
/// number of flash writes.
const PACKAGE_CHUNK_DATA: usize = 256;
/// The most upload chunks awaiting acknowledgement at once.
const MAX_CHUNKS_IN_FLIGHT: usize = 2;
/// How many resumptions in a row may leave the device's upload cursor where
/// it was before the upload is abandoned.
const MAX_STALLS: u32 = 3;
/// How much longer each stalled resumption waits than the last, so a device
/// that refuses uploads while it starts up is ready before the upload gives up.
#[cfg(not(test))]
const STALL_BACKOFF: Duration = Duration::from_secs(1);
#[cfg(test)]
const STALL_BACKOFF: Duration = Duration::from_millis(20);
/// How long [`flash`] waits after committing for the device to come back.
#[cfg(not(test))]
const RESTART_DEADLINE: Duration = Duration::from_secs(60);
#[cfg(test)]
const RESTART_DEADLINE: Duration = Duration::from_millis(500);

/// A firmware build date (UTC calendar date). Ordered chronologically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FirmwareDate {
    /// Four-digit year.
    pub year: u16,
    /// Month, 1 to 12.
    pub month: u8,
    /// Day of the month, 1 to 31.
    pub day: u8,
}

impl FirmwareDate {
    /// Parses an ISO `YYYY-MM-DD` date. Returns `None` if any field is
    /// malformed.
    pub fn parse(s: &str) -> Option<Self> {
        let mut parts = s.split('-');
        let year = parts.next()?.parse().ok()?;
        let month = parts.next()?.parse().ok()?;
        let day = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self { year, month, day })
    }
}

impl std::fmt::Display for FirmwareDate {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// Firmware currently installed on a connected device.
#[derive(Debug, Clone)]
pub struct InstalledFirmware {
    /// The sensor name parsed from `dev.desc`, such as `ASM`. Empty if the
    /// description does not follow the `{vendor} {name} {revision}` header
    /// form.
    pub name: String,
    /// The hardware revision parsed from `dev.desc`, such as `R6`. Empty if
    /// absent.
    pub revision: String,
    /// The build date from the `[YYYY-MM-DD/...]` field of `dev.desc`, if
    /// present.
    pub build_date: Option<FirmwareDate>,
    /// The firmware build version, the text after `/` in the `[date/build]`
    /// field of `dev.desc`, such as `4b13b1-DEV` or `022547`. A value
    /// containing `DEV` marks a development build, as
    /// [`Self::is_development`] records.
    pub hash: Option<String>,
    /// The device serial or identifier from the parenthesized field of
    /// `dev.desc`. `(null)` becomes `None`.
    pub serial: Option<String>,
    /// Whether the firmware is a development build, whose build version
    /// contains `DEV`. [`check_for_update`] offers no release for it.
    pub is_development: bool,
    /// The raw `dev.desc` string, for display and diagnostics.
    pub description: String,
}

impl InstalledFirmware {
    /// Whether the device runs `release`, whose short hash is then the build
    /// version in any ASCII case.
    pub fn runs(&self, release: &FirmwareRelease) -> bool {
        (self.hash.as_ref()).is_some_and(|hash| hash.eq_ignore_ascii_case(&release.short_hash))
    }
}

/// A firmware image published in a [`FirmwareCatalog`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirmwareRelease {
    /// Sensor name the image is for, such as `ASM`.
    pub name: String,
    /// Hardware revision the image is for, such as `R6`.
    pub revision: String,
    /// Build date.
    pub date: FirmwareDate,
    /// Short build hash, as in the filename.
    pub short_hash: String,
    /// The image's filename in the catalog.
    pub filename: String,
    /// Catalog-specific locator used by [`FirmwareCatalog::download`].
    pub url: String,
}

impl FirmwareRelease {
    /// Parses a catalog's `filename`, of the form
    /// `{name}-{revision}-firmware-{YYYY-MM-DD}-{hash}.bin`, into the release
    /// of sensor `name` and hardware `revision` that `url` locates. Returns
    /// `None` if `filename` does not have that form.
    pub fn parse_filename(name: &str, revision: &str, filename: &str, url: &str) -> Option<Self> {
        let stem = filename.strip_suffix(".bin")?;
        let after = stem.split_once("firmware-")?.1;
        let mut parts = after.splitn(4, '-');
        let year = parts.next()?;
        let month = parts.next()?;
        let day = parts.next()?;
        let hash = parts.next()?;
        if hash.is_empty() {
            return None;
        }
        let date = FirmwareDate::parse(&format!("{year}-{month}-{day}"))?;
        Some(Self {
            name: name.to_string(),
            revision: revision.to_string(),
            date,
            short_hash: hash.to_string(),
            filename: filename.to_string(),
            url: url.to_string(),
        })
    }
}

/// The result of comparing installed firmware with the latest published
/// release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStatus {
    /// The installed firmware is at least as new as the latest release, or
    /// its hash matches.
    UpToDate,
    /// A strictly newer release is available.
    UpdateAvailable,
    /// No firmware is published for this name and revision.
    NoPublishedFirmware,
    /// The device runs a development build. The catalog is not consulted.
    DevelopmentBuild,
    /// The installed build date is unknown, and so is whether the latest
    /// release is newer. The caller decides whether to offer it.
    Unknown,
}

/// Everything needed to present an update decision to a user.
#[derive(Debug, Clone)]
pub struct UpdateReport {
    /// What the device runs now.
    pub installed: InstalledFirmware,
    /// All published releases for this name and revision, newest first.
    /// Empty for a development build or when nothing is published. A version
    /// picker, as for a downgrade, can list them.
    pub releases: Vec<FirmwareRelease>,
    /// The latest published release (i.e. `releases.first()`), if any.
    pub latest: Option<FirmwareRelease>,
    /// How `installed` compares to `latest`.
    pub status: UpdateStatus,
}

/// The result of the `dev.stop` that [`flash`] issues first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopOutcome {
    /// `dev.stop` succeeded and the device stopped streaming.
    Stopped,
    /// The device reported it was already stopped.
    AlreadyStopped,
    /// The device has no `dev.stop` RPC. Flashing proceeds anyway.
    Unsupported,
}

/// A progress event that [`flash`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlashEvent {
    /// About to issue `dev.stop`.
    Stopping,
    /// `dev.stop` completed with this outcome.
    Stopped(StopOutcome),
    /// `chunk` of `total` chunks have been acknowledged by the device.
    Uploading {
        /// Chunks acknowledged so far.
        chunk: usize,
        /// Chunks in the image.
        total: usize,
    },
    /// The upload failed with `error` and is resuming at `chunk` of `total`,
    /// where the device reports its upload cursor.
    Resuming {
        /// The chunk the upload resumes at, counted from one.
        chunk: usize,
        /// Chunks in the image.
        total: usize,
        /// What failed.
        error: String,
    },
    /// Upload finished. The commit RPC is being issued.
    Committing,
    /// Commit taken. The device restarts into the new image.
    Restarting,
    /// The device is back in a new session.
    Restarted,
}

/// What a device ran around an upgrade [`flash`] committed.
#[derive(Debug, Clone)]
pub struct Flashed {
    /// The firmware it ran before the commit.
    pub before: InstalledFirmware,
    /// The firmware it runs once back, or `None` if it was not back in time.
    pub after: Option<InstalledFirmware>,
}

/// An error from a firmware operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FirmwareError {
    /// A device RPC failed.
    #[error("device RPC failed: {0}")]
    Rpc(#[from] CallError),
    /// `dev.desc` did not describe the installed firmware.
    #[error("could not determine installed firmware: {0}")]
    Parse(String),
    /// The catalog could not list or fetch releases.
    #[error("firmware catalog error: {0}")]
    Catalog(String),
    /// The on-disk image cache could not be read or written.
    #[error("firmware cache I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The upload did not complete.
    #[error("firmware upload failed: {0}")]
    Upload(String),
    /// The device cannot take this image, so retrying does not help.
    #[error("the device cannot take this image: {0}")]
    Refused(String),
}

/// A source of published firmware images (e.g. a GitHub repository).
///
/// Implement this to point the updater at a different location, or use the
/// built-in [`github::GithubCatalog`] (requires the `firmware-update` feature).
pub trait FirmwareCatalog {
    /// Returns all published releases for sensor `name` and hardware
    /// `revision`, or an empty list if none are published.
    ///
    /// # Errors
    ///
    /// Returns [`FirmwareError::Catalog`] if the catalog cannot be listed.
    fn list_releases(
        &self,
        name: &str,
        revision: &str,
    ) -> Result<Vec<FirmwareRelease>, FirmwareError>;

    /// Downloads the raw image bytes of `release`.
    ///
    /// # Errors
    ///
    /// Returns [`FirmwareError::Catalog`] if the image cannot be fetched.
    fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError>;
}

/// Returns the text between the first `(` and the last `)`, if any.
fn paren_content(desc: &str) -> Option<&str> {
    let start = desc.find('(')?;
    let end = desc.rfind(')')?;
    (end > start).then(|| &desc[start + 1..end])
}

/// Returns the text inside the last `[...]` group, if any.
fn bracket_content(desc: &str) -> Option<&str> {
    let start = desc.rfind('[')?;
    let rest = &desc[start + 1..];
    let end = rest.find(']')?;
    Some(&rest[..end])
}

/// Parses a device's `dev.desc` string, which identifies its installed
/// firmware. Two layouts are seen in the field.
///
/// - `Twinleaf ASM R6 ((null)) [2026-06-04/022547]`
/// - `HUB-USB-RS422 (010000003D003B001850453657353320) [2026-05-28/4b13b1-DEV]`
///
/// The general shape is `{header} ({serial}) [{date}/{build}]`.
///
/// - `header` splits on whitespace into `{vendor} {name} {revision}`. The
///   name and revision are tokens 1 and 2, and are empty for a header in
///   another form.
/// - The parenthesized field is the device serial, and `(null)` means none.
/// - Inside `[...]`, the part before `/` is the build date and the part after
///   it is the build version. The build version contains `DEV` for a
///   development build.
fn parse_installed(desc: &str) -> InstalledFirmware {
    // Header is everything before the serial/build sections.
    let header_end = desc.find(['(', '[']).unwrap_or(desc.len());
    let header_tokens: Vec<&str> = desc[..header_end].split_whitespace().collect();
    let name = header_tokens
        .get(1)
        .map(|s| s.to_string())
        .unwrap_or_default();
    let revision = header_tokens
        .get(2)
        .map(|s| s.to_string())
        .unwrap_or_default();

    let serial = paren_content(desc)
        .map(|s| s.trim_matches(|c| c == '(' || c == ')'))
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("null"))
        .map(str::to_string);

    let (build_date, hash) = match bracket_content(desc) {
        Some(inside) => {
            let date = inside.split('/').next().and_then(FirmwareDate::parse);
            let build = inside
                .rsplit('/')
                .next()
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            (date, build)
        }
        None => (None, None),
    };

    let is_development = hash
        .as_deref()
        .is_some_and(|h| h.to_ascii_uppercase().contains("DEV"));

    InstalledFirmware {
        name,
        revision,
        build_date,
        hash,
        serial,
        is_development,
        description: desc.to_string(),
    }
}

/// Queries `device` for the firmware it runs, from its `dev.desc`.
///
/// # Errors
///
/// Returns [`FirmwareError::Rpc`] if the `dev.desc` RPC fails.
pub fn query_installed(device: &Device) -> Result<InstalledFirmware, FirmwareError> {
    let desc: String = device.get("dev.desc")?;
    Ok(parse_installed(&desc))
}

/// Returns whether `latest` is newer than the `installed` firmware.
fn compare(installed: &InstalledFirmware, latest: &FirmwareRelease) -> UpdateStatus {
    // An exact hash match means the same build, regardless of parsed dates.
    if installed.runs(latest) {
        return UpdateStatus::UpToDate;
    }
    match installed.build_date {
        Some(installed_date) if latest.date > installed_date => UpdateStatus::UpdateAvailable,
        Some(_) => UpdateStatus::UpToDate,
        None => UpdateStatus::Unknown,
    }
}

/// Compares the `installed` firmware with the latest release in `catalog`,
/// and returns everything needed to present the decision. A development
/// build is reported without consulting the catalog.
///
/// # Errors
///
/// Returns [`FirmwareError::Parse`] if the sensor name or revision of
/// `installed` names no plain folder, and the catalog's error if it cannot
/// list the releases.
pub fn check_for_update(
    installed: InstalledFirmware,
    catalog: &dyn FirmwareCatalog,
) -> Result<UpdateReport, FirmwareError> {
    // A development build can't be replaced by a published release; don't even
    // hit the network.
    if installed.is_development {
        return Ok(UpdateReport {
            installed,
            releases: Vec::new(),
            latest: None,
            status: UpdateStatus::DevelopmentBuild,
        });
    }

    let plain = |part: &str| {
        !part.is_empty() && part != "." && part != ".." && !part.contains(['/', '\\', ':'])
    };
    if !plain(&installed.name) || !plain(&installed.revision) {
        return Err(FirmwareError::Parse(format!(
            "could not determine sensor name and revision from dev.desc: {:?}",
            installed.description
        )));
    }

    let mut releases = catalog.list_releases(&installed.name, &installed.revision)?;
    // Sort newest-first so `releases[0]` is the latest and a picker reads
    // naturally top-to-bottom. The filename breaks a tie deterministically.
    releases.sort_by(|a, b| {
        b.date
            .cmp(&a.date)
            .then_with(|| b.filename.cmp(&a.filename))
    });
    let latest = releases.first().cloned();

    let status = match &latest {
        None => UpdateStatus::NoPublishedFirmware,
        Some(release) => compare(&installed, release),
    };

    Ok(UpdateReport {
        installed,
        releases,
        latest,
        status,
    })
}

/// Returns the default cache root for downloaded firmware,
/// `<os-cache>/twinleaf/firmware`, or `None` if the home directory cannot be
/// found.
pub fn default_cache_dir() -> Option<PathBuf> {
    directories::BaseDirs::new().map(|b| b.cache_dir().join("twinleaf").join("firmware"))
}

/// Returns the path `release` is cached at under `cache_root`.
pub fn cache_path(cache_root: &Path, release: &FirmwareRelease) -> PathBuf {
    cache_root
        .join(&release.name)
        .join(&release.revision)
        .join(&release.filename)
}

/// Returns the bytes of `release`, from the cache under `cache_root` if it
/// holds them. The cached file's name embeds the build date and hash. A
/// release missing from the cache is downloaded through `catalog` and written
/// to the cache through a temporary file and a rename. An interrupted write
/// leaves no truncated image in the cache.
///
/// # Errors
///
/// Returns the catalog's error if the download fails,
/// [`FirmwareError::Catalog`] if the image is empty, and
/// [`FirmwareError::Io`] if the cache cannot be read or written.
pub fn download_cached(
    catalog: &dyn FirmwareCatalog,
    release: &FirmwareRelease,
    cache_root: &Path,
) -> Result<Vec<u8>, FirmwareError> {
    download(catalog, release, Some(cache_root))
}

/// The bytes of `release` as [`download_cached`] gives them, or uncached with no `cache_root`.
/// # Errors
/// Returns the errors of [`download_cached`].
pub fn download(
    catalog: &dyn FirmwareCatalog,
    release: &FirmwareRelease,
    cache_root: Option<&Path>,
) -> Result<Vec<u8>, FirmwareError> {
    let path = cache_root.map(|root| cache_path(root, release));

    if let Some(path) = &path {
        if std::fs::metadata(path).is_ok_and(|meta| meta.is_file() && meta.len() > 0) {
            return Ok(std::fs::read(path)?);
        }
    }

    let data = catalog.download(release)?;
    if data.is_empty() {
        return Err(FirmwareError::Catalog(format!(
            "downloaded firmware {} is empty",
            release.filename
        )));
    }

    let Some(path) = path else {
        return Ok(data);
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("part");
    std::fs::write(&tmp, &data)?;
    std::fs::rename(&tmp, &path)?;

    Ok(data)
}

/// Uploads `firmware_data` to `device` and commits the upgrade. Reports
/// progress through `on_event` and prints nothing.
///
/// The firmware is either a signed package or an image, whose chunks carry
/// their own envelope. A device that answers `dev.firmware.abort` takes
/// signed packages, and one that does not takes images. That answer tells
/// them apart before anything is stopped or sent, and firmware the device
/// cannot take is refused then. An empty image is refused before that.
///
/// A chunk lost in either direction fails the upload window it is in. The
/// upload then reads the device's upload cursor and resumes from that chunk.
/// It gives up after three resumptions in a row that leave the cursor where
/// it was, each waiting longer than the last. After committing, it waits up
/// to a minute for the device to come back in a new session, or for one
/// whose heartbeats carry no session to be lost and come back, and reads what
/// it runs then. A commit the proxy answers with a timeout counts as taken
/// once the device is lost or back, as the proxy answers so the calls a lost
/// or restarted device left.
///
/// # Errors
///
/// Returns [`FirmwareError::Rpc`] if `dev.stop`, the commit, or a read of
/// `dev.desc` fails, or the connection ends while the device restarts,
/// [`FirmwareError::Refused`] if the image is empty, the device takes the
/// other kind of image or refuses a package's header, and
/// [`FirmwareError::Upload`] if an upload stalls or the device's cursor is
/// not a chunk boundary of the image.
pub fn flash(
    device: &Device,
    firmware_data: &[u8],
    mut on_event: impl FnMut(FlashEvent),
) -> Result<Flashed, FirmwareError> {
    if firmware_data.is_empty() {
        return Err(FirmwareError::Refused("the image is empty".into()));
    }
    let package = is_package(firmware_data);
    match (package, takes_packages(device)?) {
        (true, false) => {
            return Err(FirmwareError::Refused(
                "this is a signed package, and the device takes images".into(),
            ))
        }
        (false, true) => {
            return Err(FirmwareError::Refused(
                "the device takes signed packages, and this is an image".into(),
            ))
        }
        (true, true) | (false, false) => {}
    }

    // Stop streaming first. A device that lacks dev.stop, or is already
    // stopped, is fine to proceed with.
    on_event(FlashEvent::Stopping);
    let stop_outcome = match device.action("dev.stop") {
        Ok(()) => StopOutcome::Stopped,
        Err(CallError::DeviceError(ref e)) if matches!(e.error, wire_rpc::RpcError::NotFound) => {
            StopOutcome::Unsupported
        }
        Err(CallError::DeviceError(ref e)) if matches!(e.error, wire_rpc::RpcError::State) => {
            StopOutcome::AlreadyStopped
        }
        Err(e) => return Err(e.into()),
    };
    on_event(FlashEvent::Stopped(stop_outcome));

    match package {
        true => upload_package(device, firmware_data, &mut on_event)?,
        false => upload_image(device, firmware_data, &mut on_event)?,
    }

    on_event(FlashEvent::Committing);
    let presence = device.presence_updates();
    let before = query_installed(device)?;
    let session = match device.presence() {
        Presence::Alive { session } => session,
        Presence::Connecting | Presence::Lost { .. } => None,
    };
    let given_up = match device.action("dev.firmware.upgrade") {
        Ok(()) => None,
        Err(CallError::DeviceError(refusal)) if refusal.error == wire_rpc::RpcError::Timeout => {
            Some(refusal)
        }
        Err(error) => return Err(error.into()),
    };

    on_event(FlashEvent::Restarting);
    let deadline = Instant::now() + RESTART_DEADLINE;
    let mut lost = false;
    let back = loop {
        let now = match presence.recv_deadline(deadline) {
            Ok(now) => now,
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Timeout) => break false,
            Err(RecvError::Disconnected | RecvError::Stopped | RecvError::Closed) => {
                return Err(CallError::ResponseLost.into())
            }
        };
        match now {
            Presence::Connecting => {}
            Presence::Lost { .. } => lost = true,
            Presence::Alive { session: now } => {
                let restarted = match (session, now) {
                    (Some(old), Some(new)) => new != old,
                    (None, None) => lost,
                    (Some(_), None) | (None, Some(_)) => true,
                };
                if restarted {
                    break true;
                }
            }
        }
    };
    if let (false, false, Some(refusal)) = (back, lost, given_up) {
        return Err(CallError::DeviceError(refusal).into());
    }
    let after = match back {
        true => {
            on_event(FlashEvent::Restarted);
            Some(query_installed(device)?)
        }
        false => None,
    };
    Ok(Flashed { before, after })
}

/// Whether `data` is a signed package. Its header names the format version
/// and size `twinleaf-device` reads, and exactly the image behind it, which an
/// image matches only by chance in eight bytes.
fn is_package(data: &[u8]) -> bool {
    let Some((header, image)) = data.split_first_chunk::<HEADER_SIZE>() else {
        return false;
    };
    let header = PackageHeader::parse(header);
    !image.is_empty()
        && header.format == FORMAT_VERSION
        && usize::from(header.header_size) == HEADER_SIZE
        && header.image_len as usize == image.len()
}

/// Whether `device` takes signed packages, which it says by answering
/// `dev.firmware.abort`. Aborting first also throws away what an earlier
/// host left half uploaded.
fn takes_packages(device: &Device) -> Result<bool, FirmwareError> {
    match device.action("dev.firmware.abort") {
        Ok(()) => Ok(true),
        Err(CallError::DeviceError(ref e)) if matches!(e.error, wire_rpc::RpcError::NotFound) => {
            Ok(false)
        }
        Err(CallError::DeviceError(ref e)) if matches!(e.error, wire_rpc::RpcError::State) => {
            Err(FirmwareError::Upload(
                "the device already has an upgrade committed, which its next reboot installs"
                    .into(),
            ))
        }
        Err(e) => Err(e.into()),
    }
}

/// Uploads an image, whose chunks each carry the envelope the device checks
/// before appending them.
fn upload_image(
    device: &Device,
    firmware_data: &[u8],
    on_event: &mut impl FnMut(FlashEvent),
) -> Result<(), FirmwareError> {
    let total_chunks = firmware_data.len().div_ceil(IMAGE_CHUNK_SIZE);
    let mut resume_at = 0;
    let mut stalls = 0;
    loop {
        let sends = firmware_data
            .chunks(IMAGE_CHUNK_SIZE)
            .skip(resume_at)
            .map(|data| device.submit("dev.firmware.upload", data));
        let outcome = pipelined(sends, MAX_CHUNKS_IN_FLIGHT)
            .enumerate()
            .try_for_each(|(sent, ack)| {
                ack.map(|_| {
                    on_event(FlashEvent::Uploading {
                        chunk: resume_at + sent + 1,
                        total: total_chunks,
                    })
                })
            });
        let Err(error) = outcome else {
            return Ok(());
        };
        let next = match device.get::<u32>("dev.firmware.upload") {
            Ok(cursor) => chunk_at(cursor, firmware_data).ok_or_else(|| {
                FirmwareError::Upload(format!(
                    "the device's upload cursor ({cursor} bytes) is not a chunk boundary of this image"
                ))
            })?,
            Err(_) => resume_at,
        };
        stalls = if next > resume_at { 0 } else { stalls + 1 };
        if stalls == MAX_STALLS {
            return Err(FirmwareError::Upload(format!(
                "firmware chunk {}/{} failed {stalls} times without progress: {error}",
                next + 1,
                total_chunks
            )));
        }
        if next == total_chunks {
            return Ok(());
        }
        on_event(FlashEvent::Resuming {
            chunk: next + 1,
            total: total_chunks,
            error: error.to_string(),
        });
        std::thread::sleep(STALL_BACKOFF * stalls);
        resume_at = next;
    }
}

/// Uploads a signed package. The header goes first in a chunk of its own,
/// then the image in chunks that each open with the image offset they start
/// at, so the device refuses one that would land anywhere but its cursor.
///
/// The cursor counts image bytes. At zero it cannot tell whether the header
/// was taken, so the upload resends the header, which the device takes again
/// until image bytes arrive. A header the device refuses names another board,
/// revision or key, and fails the upload at once.
fn upload_package(
    device: &Device,
    package: &[u8],
    on_event: &mut impl FnMut(FlashEvent),
) -> Result<(), FirmwareError> {
    let (header, image) = package.split_at(HEADER_SIZE);
    let total_chunks = 1 + image.len().div_ceil(PACKAGE_CHUNK_DATA);
    // The chunk number, counted from one, of what starts at image `offset`.
    let chunk_of = |offset: usize| match offset {
        0 => 1,
        offset => 2 + offset / PACKAGE_CHUNK_DATA,
    };
    let mut resume_at = 0;
    let mut stalls = 0;
    loop {
        let first = chunk_of(resume_at);
        let header_chunk = (resume_at == 0).then(|| header.to_vec());
        let image_chunks = image[resume_at..]
            .chunks(PACKAGE_CHUNK_DATA)
            .enumerate()
            .map(|(index, data)| {
                let offset = (resume_at + index * PACKAGE_CHUNK_DATA) as u32;
                let mut chunk = Vec::with_capacity(PACKAGE_OFFSET_SIZE + data.len());
                chunk.extend_from_slice(&offset.to_le_bytes());
                chunk.extend_from_slice(data);
                chunk
            });
        let sends = header_chunk
            .into_iter()
            .chain(image_chunks)
            .map(|chunk| device.submit("dev.firmware.upload", &chunk));
        let mut acked = 0;
        let outcome = pipelined(sends, MAX_CHUNKS_IN_FLIGHT).try_for_each(|ack| {
            ack.map(|_| {
                acked += 1;
                on_event(FlashEvent::Uploading {
                    chunk: first + acked - 1,
                    total: total_chunks,
                })
            })
        });
        let Err(error) = outcome else {
            return Ok(());
        };
        if let (0, 0, CallError::DeviceError(refused)) = (resume_at, acked, &error) {
            if matches!(
                refused.error,
                wire_rpc::RpcError::Invalid | wire_rpc::RpcError::Range
            ) {
                return Err(FirmwareError::Refused(format!(
                    "its header was refused ({}), so the package is for \
                     another board or revision, signed with the other kind of key, or too large",
                    refused.error
                )));
            }
        }
        let next = match device.get::<u32>("dev.firmware.upload") {
            Ok(cursor) if cursor as usize <= image.len() => cursor as usize,
            Ok(cursor) => {
                return Err(FirmwareError::Upload(format!(
                    "the device's upload cursor ({cursor} bytes) is past the end of this image"
                )))
            }
            Err(_) => resume_at,
        };
        stalls = if next > resume_at { 0 } else { stalls + 1 };
        if stalls == MAX_STALLS {
            return Err(FirmwareError::Upload(format!(
                "firmware chunk {}/{} failed {stalls} times without progress: {error}",
                chunk_of(next),
                total_chunks
            )));
        }
        if next == image.len() {
            return Ok(());
        }
        on_event(FlashEvent::Resuming {
            chunk: chunk_of(next),
            total: total_chunks,
            error: error.to_string(),
        });
        std::thread::sleep(STALL_BACKOFF * stalls);
        resume_at = next;
    }
}

/// Returns the chunk of `image` that starts at the device's upload `cursor`.
/// Returns the chunk count if the cursor is at the image's end, and `None` if
/// the image has no chunk boundary there.
fn chunk_at(cursor: u32, image: &[u8]) -> Option<usize> {
    let total_chunks = image.len().div_ceil(IMAGE_CHUNK_SIZE);
    let data_per_chunk = IMAGE_CHUNK_SIZE - IMAGE_CHUNK_ENVELOPE;
    let data = image
        .len()
        .saturating_sub(total_chunks * IMAGE_CHUNK_ENVELOPE);
    let cursor = cursor as usize;
    if cursor == data {
        Some(total_chunks)
    } else if cursor.is_multiple_of(data_per_chunk) && cursor / data_per_chunk < total_chunks {
        Some(cursor / data_per_chunk)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{parts, LostCause, TestProxy, Update, WATCH};
    use crate::tio::packet::Packet;

    fn installed(date: Option<&str>, hash: Option<&str>) -> InstalledFirmware {
        InstalledFirmware {
            name: "ASM".into(),
            revision: "R6".into(),
            build_date: date.and_then(FirmwareDate::parse),
            hash: hash.map(str::to_string),
            serial: None,
            is_development: false,
            description: String::new(),
        }
    }

    fn release(date: &str, hash: &str) -> FirmwareRelease {
        FirmwareRelease {
            name: "ASM".into(),
            revision: "R6".into(),
            date: FirmwareDate::parse(date).unwrap(),
            short_hash: hash.into(),
            filename: format!("ASM-R6-firmware-{date}-{hash}.bin"),
            url: String::new(),
        }
    }

    #[test]
    fn firmware_dates_order_chronologically() {
        assert!(
            FirmwareDate::parse("2026-03-17").unwrap() > FirmwareDate::parse("2026-01-10").unwrap()
        );
        assert!(
            FirmwareDate::parse("2026-01-10").unwrap() > FirmwareDate::parse("2025-12-31").unwrap()
        );
        assert_eq!(
            FirmwareDate::parse("2026-3-7"),
            Some(FirmwareDate {
                year: 2026,
                month: 3,
                day: 7
            })
        );
        assert_eq!(FirmwareDate::parse("not-a-date"), None);
        assert_eq!(FirmwareDate::parse("2026-03"), None);
    }

    #[test]
    fn parses_release_filename() {
        let r = FirmwareRelease::parse_filename(
            "ASM",
            "R6",
            "ASM-R6-firmware-2026-03-17-5d1494.bin",
            "https://x/f.bin",
        )
        .unwrap();
        assert_eq!(r.date, FirmwareDate::parse("2026-03-17").unwrap());
        assert_eq!(r.short_hash, "5d1494");
        assert_eq!(r.filename, "ASM-R6-firmware-2026-03-17-5d1494.bin");
        assert_eq!(r.url, "https://x/f.bin");
    }

    #[test]
    fn rejects_non_firmware_names() {
        assert!(FirmwareRelease::parse_filename("ASM", "R6", "README.md", "u").is_none());
        // Missing hash component.
        assert!(FirmwareRelease::parse_filename(
            "ASM",
            "R6",
            "ASM-R6-firmware-2026-03-17.bin",
            "u"
        )
        .is_none());
    }

    #[test]
    fn parses_installed_from_desc() {
        // Twinleaf form: vendor/name/revision header, (null) serial, build time.
        let fw = parse_installed("Twinleaf ASM R6 ((null)) [2026-06-04/022547]");
        assert_eq!(fw.name, "ASM");
        assert_eq!(fw.revision, "R6");
        assert_eq!(fw.build_date, FirmwareDate::parse("2026-06-04"));
        assert_eq!(fw.hash.as_deref(), Some("022547")); // build version (after '/')
        assert_eq!(fw.serial, None); // (null) -> no serial
        assert!(!fw.is_development);

        // Hub form: single-token header, hex serial, build hash with -DEV.
        let fw = parse_installed(
            "HUB-USB-RS422 (010000003D003B001850453657353320) [2026-05-28/4b13b1-DEV]",
        );
        assert_eq!(fw.name, ""); // header is a single token; no name/revision
        assert_eq!(fw.revision, "");
        assert_eq!(fw.build_date, FirmwareDate::parse("2026-05-28"));
        assert_eq!(fw.hash.as_deref(), Some("4b13b1-DEV"));
        assert_eq!(
            fw.serial.as_deref(),
            Some("010000003D003B001850453657353320")
        );
        assert!(fw.is_development); // hash contains DEV

        // A Twinleaf-style dev build is also detected.
        let fw = parse_installed("Twinleaf ASM R6 (5d1494) [2026-03-17/abc123-dev]");
        assert_eq!((fw.name.as_str(), fw.revision.as_str()), ("ASM", "R6"));
        assert_eq!(fw.serial.as_deref(), Some("5d1494"));
        assert!(fw.is_development); // case-insensitive
    }

    struct PanicCatalog;
    impl FirmwareCatalog for PanicCatalog {
        fn list_releases(&self, _: &str, _: &str) -> Result<Vec<FirmwareRelease>, FirmwareError> {
            panic!("catalog must not be queried for a development build");
        }
        fn download(&self, _: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
            panic!("catalog must not be downloaded for a development build");
        }
    }

    struct ListCatalog(Vec<FirmwareRelease>);
    impl FirmwareCatalog for ListCatalog {
        fn list_releases(&self, _: &str, _: &str) -> Result<Vec<FirmwareRelease>, FirmwareError> {
            Ok(self.0.clone())
        }
        fn download(&self, _: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn report_lists_releases_newest_first() {
        let installed = parse_installed("Twinleaf ASM R6 (000000) [2026-02-01/000000]");
        let catalog = ListCatalog(vec![
            release("2026-01-10", "aaaaaa"),
            release("2026-03-17", "5d1494"),
            release("2025-12-31", "bbbbbb"),
        ]);
        let report = check_for_update(installed, &catalog).unwrap();

        let dates: Vec<String> = report.releases.iter().map(|r| r.date.to_string()).collect();
        assert_eq!(dates, ["2026-03-17", "2026-01-10", "2025-12-31"]);
        assert_eq!(report.latest.as_ref().unwrap().short_hash, "5d1494");
        // Installed 2026-02-01 is older than the newest (2026-03-17).
        assert_eq!(report.status, UpdateStatus::UpdateAvailable);
    }

    #[test]
    fn dev_build_refuses_without_touching_catalog() {
        let fw = parse_installed("HUB-USB-RS422 (0100ABCD) [2026-05-28/4b13b1-DEV]");
        let report = check_for_update(fw, &PanicCatalog).unwrap();
        assert_eq!(report.status, UpdateStatus::DevelopmentBuild);
        assert!(report.latest.is_none());
    }

    /// A build with no name or revision, or one that names no plain folder,
    /// is refused before the catalog is asked.
    #[test]
    fn missing_name_revision_errors_for_release() {
        for header in ["HUB", "T .. R6", "T ASM .", "T A/B R6", r"T ASM R\6"] {
            let fw = parse_installed(&format!("{header} (0100ABCD) [2026-05-28/4b13b1]"));
            let refused = check_for_update(fw, &PanicCatalog);
            assert!(matches!(refused, Err(FirmwareError::Parse(_))), "{header}");
        }
    }

    /// Of two releases built the same day, the later filename is the latest.
    #[test]
    fn a_filename_breaks_a_tie_between_release_dates() {
        let catalog = ListCatalog(vec![
            release("2026-03-17", "5d1494"),
            release("2026-03-17", "aaaaaa"),
            release("2025-12-31", "bbbbbb"),
        ]);
        let report = check_for_update(installed(Some("2026-01-01"), None), &catalog).unwrap();

        let latest = report.latest.unwrap();
        assert_eq!(latest.date, FirmwareDate::parse("2026-03-17").unwrap());
        assert_eq!(latest.short_hash, "aaaaaa");
    }

    /// What the fake device does with a chunk that arrived at its cursor.
    enum Fault {
        Deliver,
        LoseRequest,
        LoseAck,
        Reject(wire_rpc::RpcError),
    }

    /// The watches a fake device's session made of its presence.
    #[derive(Default)]
    struct Watches(Vec<u16>);

    impl Watches {
        /// Takes `packet` if it is a control request, and tells a watch it
        /// makes that the device is `alive` in a session. Returns whether it
        /// was one.
        fn take(&mut self, proxy: &TestProxy, packet: &Packet, alive: Option<u32>) -> bool {
            let crate::tio::packet::Payload::ProxyTx(request) = packet.payload() else {
                return false;
            };
            if matches!(request.method, wire_rpc::Method::ByName(name) if name == WATCH.as_bytes())
            {
                self.0.push(request.id.value());
                self.tell(proxy, Ok(alive), None);
            }
            true
        }

        /// Tells every watch `presence`, lost as `cause` if it says so.
        fn tell(
            &self,
            proxy: &TestProxy,
            presence: Result<Option<u32>, Option<u64>>,
            cause: Option<u8>,
        ) {
            self.0
                .iter()
                .flat_map(|&id| parts(id, &Update::Presence(presence, cause, None)))
                .for_each(|part| proxy.send(part));
        }
    }

    /// A chunked image whose every chunk announces its data offset in its
    /// first four bytes, so the fake device can check it as the real one does.
    fn image(len: usize) -> Vec<u8> {
        let mut image = vec![0xa5u8; len];
        for (index, chunk) in image.chunks_mut(IMAGE_CHUNK_SIZE).enumerate() {
            let offset = (index * (IMAGE_CHUNK_SIZE - IMAGE_CHUNK_ENVELOPE)) as u32;
            chunk[..4].copy_from_slice(&offset.to_le_bytes());
        }
        image
    }

    /// Run [`flash`] against a fake device that takes images and keeps an
    /// upload cursor. An empty request reads it, a chunk whose offset is
    /// elsewhere is rejected, and `fault` decides what happens to one that
    /// matches. Control RPCs are always acknowledged. Returns the outcome, the
    /// events, the chunk indices sent in order, and the device's final cursor.
    fn flash_against(
        firmware: Vec<u8>,
        cursor: u32,
        fault: impl FnMut(usize) -> Fault + Send + 'static,
    ) -> (
        Result<Flashed, FirmwareError>,
        Vec<FlashEvent>,
        Vec<usize>,
        u32,
    ) {
        let (device, proxy) = crate::device::Device::test_pair();
        let responder = std::thread::spawn(move || {
            let mut cursor = cursor;
            let mut fault = fault;
            let mut sent = Vec::new();
            let (mut watches, mut session) = (Watches::default(), 1);
            // The session's socket closes once the device is dropped.
            while let Ok(request) = proxy.sent.recv_timeout(Duration::from_secs(5)) {
                if watches.take(&proxy, &request, Some(session)) {
                    continue;
                }
                let route = request.route();
                let crate::tio::packet::Payload::RpcRequest(request) = request.payload() else {
                    panic!("flash only makes RPC calls");
                };
                let id = request.id.value();
                let wire_rpc::Method::ByName(name) = request.method else {
                    panic!("expected a call by name");
                };
                // A device that takes images does not answer dev.firmware.abort.
                if name == b"dev.firmware.abort" {
                    proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
                    continue;
                }
                if name == b"dev.firmware.upgrade" {
                    proxy.reply(id, &[], route);
                    session += 1;
                    watches.tell(&proxy, Ok(Some(session)), None);
                    continue;
                }
                if name != b"dev.firmware.upload" {
                    proxy.reply(id, &[], route);
                    continue;
                }
                if request.args.is_empty() {
                    proxy.reply(id, &cursor.to_le_bytes(), route);
                    continue;
                }
                let offset = u32::from_le_bytes(request.args[..4].try_into().unwrap());
                let data = (request.args.len() - IMAGE_CHUNK_ENVELOPE) as u32;
                let chunk = offset as usize / (IMAGE_CHUNK_SIZE - IMAGE_CHUNK_ENVELOPE);
                sent.push(chunk);
                if offset != cursor {
                    proxy.refuse(id, wire_rpc::RpcError::Invalid, route);
                    continue;
                }
                // A lost request or ack goes unanswered, and times out.
                match fault(chunk) {
                    Fault::Deliver => {
                        cursor += data;
                        proxy.reply(id, &[], route);
                    }
                    Fault::LoseRequest => {}
                    Fault::LoseAck => cursor += data,
                    Fault::Reject(error) => proxy.refuse(id, error, route),
                }
            }
            (sent, cursor)
        });
        // A lost chunk costs this much. The device without it is dropped,
        // so dropping this one closes the session.
        let device = {
            let device = device;
            device.with_timeout(Duration::from_millis(500))
        };

        let mut events = Vec::new();
        let result = flash(&device, &firmware, |e| events.push(e));
        drop(device);
        let (sent, cursor) = responder.join().unwrap();
        (result, events, sent, cursor)
    }

    /// Three chunks, two full ones and a short tail.
    const THREE_CHUNKS: usize = 2 * IMAGE_CHUNK_SIZE + 64;
    const THREE_CHUNKS_DATA: u32 = (THREE_CHUNKS - 3 * IMAGE_CHUNK_ENVELOPE) as u32;

    fn acked(events: &[FlashEvent]) -> Vec<usize> {
        events
            .iter()
            .filter_map(|e| match e {
                FlashEvent::Uploading { chunk, .. } => Some(*chunk),
                _ => None,
            })
            .collect()
    }

    fn resumed(events: &[FlashEvent]) -> Vec<usize> {
        events
            .iter()
            .filter_map(|e| match e {
                FlashEvent::Resuming { chunk, .. } => Some(*chunk),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn an_empty_image_is_refused_before_anything_is_sent() {
        let (device, proxy) = crate::device::Device::test_pair();
        let refused = matches!(flash(&device, &[], |_| {}), Err(FirmwareError::Refused(_)));
        assert!(refused && proxy.sent.try_recv().is_err());
    }

    #[test]
    fn flash_acknowledges_every_chunk_in_order_and_commits() {
        let (result, events, sent, cursor) =
            flash_against(image(THREE_CHUNKS), 0, |_| Fault::Deliver);

        result.unwrap();
        assert_eq!(sent, [0, 1, 2]);
        assert_eq!(acked(&events), [1, 2, 3]);
        assert_eq!(cursor, THREE_CHUNKS_DATA);
        assert!(events.contains(&FlashEvent::Restarted));
    }

    /// Losing chunk 1 also gets chunk 2, already in flight, rejected for its
    /// offset. The upload reads the cursor and resends both.
    #[test]
    fn a_lost_chunk_is_resent_from_the_devices_cursor() {
        let mut lost = false;
        let (result, events, sent, cursor) = flash_against(image(THREE_CHUNKS), 0, move |chunk| {
            if chunk == 1 && !std::mem::replace(&mut lost, true) {
                Fault::LoseRequest
            } else {
                Fault::Deliver
            }
        });

        result.unwrap();
        assert_eq!(sent, [0, 1, 2, 1, 2]);
        assert_eq!(resumed(&events), [2]);
        assert_eq!(cursor, THREE_CHUNKS_DATA);
    }

    /// A chunk the device applied but whose ack was lost is not resent, as the
    /// cursor is already past it.
    #[test]
    fn a_lost_ack_is_not_resent() {
        let (result, events, sent, cursor) = flash_against(image(THREE_CHUNKS), 0, |chunk| {
            if chunk == 1 {
                Fault::LoseAck
            } else {
                Fault::Deliver
            }
        });

        result.unwrap();
        assert_eq!(sent, [0, 1, 2]);
        assert!(resumed(&events).is_empty());
        assert_eq!(cursor, THREE_CHUNKS_DATA);
    }

    /// A device already holding the first chunk rejects chunk 0. The upload
    /// resumes from wherever the cursor is by then.
    #[test]
    fn a_partial_upload_resumes_where_the_device_left_off() {
        let first_chunk = (IMAGE_CHUNK_SIZE - IMAGE_CHUNK_ENVELOPE) as u32;
        let (result, events, sent, cursor) =
            flash_against(image(THREE_CHUNKS), first_chunk, |_| Fault::Deliver);

        result.unwrap();
        assert_eq!(sent, [0, 1, 2]);
        assert_eq!(resumed(&events), [3]);
        assert_eq!(cursor, THREE_CHUNKS_DATA);
    }

    #[test]
    fn an_upload_the_device_keeps_rejecting_fails_without_progress() {
        let (result, _, sent, _) = flash_against(image(100), 0, |_| {
            Fault::Reject(wire_rpc::RpcError::Invalid)
        });

        let err = result.unwrap_err().to_string();
        assert_eq!(sent.len(), MAX_STALLS as usize);
        assert!(
            err.contains("failed 3 times without progress") && err.contains("invalid arguments"),
            "unexpected error: {err}"
        );
    }

    /// A device that refuses uploads for a while after it starts takes one
    /// that waits it out.
    #[test]
    fn an_upload_waits_out_a_device_still_starting_up() {
        let ready = Instant::now() + 2 * STALL_BACKOFF;
        let (result, events, sent, cursor) = flash_against(image(THREE_CHUNKS), 0, move |_| {
            match Instant::now() < ready {
                true => Fault::Reject(wire_rpc::RpcError::State),
                false => Fault::Deliver,
            }
        });

        result.unwrap();
        assert!(!resumed(&events).is_empty() && sent.ends_with(&[0, 1, 2]));
        assert_eq!(cursor, THREE_CHUNKS_DATA);
    }

    #[test]
    fn an_upload_that_keeps_timing_out_fails_without_progress() {
        let (result, _, sent, _) = flash_against(image(100), 0, |_| Fault::LoseRequest);

        let err = result.unwrap_err().to_string();
        assert_eq!(sent.len(), MAX_STALLS as usize);
        assert!(
            err.contains("failed 3 times without progress") && err.contains("timed out"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn the_cursor_maps_to_a_chunk_only_on_a_boundary_of_this_image() {
        let image = image(THREE_CHUNKS);
        let data_per_chunk = (IMAGE_CHUNK_SIZE - IMAGE_CHUNK_ENVELOPE) as u32;

        assert_eq!(chunk_at(0, &image), Some(0));
        assert_eq!(chunk_at(data_per_chunk, &image), Some(1));
        assert_eq!(chunk_at(THREE_CHUNKS_DATA, &image), Some(3));
        assert_eq!(chunk_at(data_per_chunk + 1, &image), None);
        assert_eq!(chunk_at(THREE_CHUNKS_DATA + 256, &image), None);
    }

    use crate::proto::update::FLAG_DEVELOPMENT;
    use twinleaf_device::storage::update::{self as device_update, Take};

    /// The board the package fake takes packages for.
    fn board() -> device_update::Package {
        device_update::Package {
            magic: crate::proto::FirmwareMagic::new(*b"TESTFW\0\0"),
            board_id: crate::proto::BoardId::from_ascii("TEST"),
            hw_rev: crate::proto::HwRev::new(1),
            development: true,
            image_max: 64 * 1024,
        }
    }

    /// A signed package for `board`, the signature left blank, around an
    /// image of `len` bytes.
    fn package(board: &device_update::Package, len: usize) -> Vec<u8> {
        let mut header = [0; HEADER_SIZE];
        PackageHeader {
            magic: board.magic.as_bytes(),
            format: FORMAT_VERSION,
            header_size: HEADER_SIZE as u16,
            board_id: board.board_id.as_bytes(),
            hw_rev: board.hw_rev.value(),
            flags: FLAG_DEVELOPMENT,
            image_len: len as u32,
            signature: &[0; 64],
        }
        .write(&mut header);
        let mut data = header.to_vec();
        data.extend((0..len).map(|byte| byte as u8));
        data
    }

    /// What the package fake does with the commit.
    enum Commit {
        /// Answers it, and restarts into a new session.
        Restart,
        /// Refuses it with this error, and stays.
        Refuse(wire_rpc::RpcError),
        /// Takes it and goes silent, so the proxy answers it with a timeout
        /// and loses the device, which comes back if `back`. Its heartbeats
        /// carry its session unless `sessionless`.
        Lost { back: bool, sessionless: bool },
    }

    /// Run [`flash`] against a fake device that keeps the crate's own upload
    /// cursor for `board`, with `commit` deciding what it does with the
    /// commit and `fault` what happens to each chunk. Its `dev.desc` names
    /// its session. Returns the outcome, the events, the chunks sent in order
    /// (zero for the header, then one more than each image chunk's index),
    /// and the device's final cursor.
    fn flash_package_against(
        board: device_update::Package,
        firmware: Vec<u8>,
        commit: Commit,
        fault: impl FnMut(usize) -> Fault + Send + 'static,
    ) -> (
        Result<Flashed, FirmwareError>,
        Vec<FlashEvent>,
        Vec<usize>,
        u32,
    ) {
        let (device, proxy) = crate::device::Device::test_pair();
        let responder = std::thread::spawn(move || {
            let booted = std::time::Instant::now();
            let mut upload = device_update::Upload::new();
            let mut fault = fault;
            let mut sent = Vec::new();
            let (mut watches, mut session) = (Watches::default(), 1);
            let sessionless = matches!(
                commit,
                Commit::Lost {
                    sessionless: true,
                    ..
                }
            );
            let alive = |session: u32| (!sessionless).then_some(session);
            while let Ok(request) = proxy.sent.recv_timeout(Duration::from_secs(5)) {
                if watches.take(&proxy, &request, alive(session)) {
                    continue;
                }
                let now = booted.elapsed().as_nanos() as u64;
                let route = request.route();
                let crate::tio::packet::Payload::RpcRequest(request) = request.payload() else {
                    panic!("flash only makes RPC calls");
                };
                let id = request.id.value();
                let wire_rpc::Method::ByName(name) = request.method else {
                    panic!("expected a call by name");
                };
                let answer = |result: Result<Vec<u8>, wire_rpc::RpcError>| match result {
                    Ok(value) => proxy.reply(id, &value, route),
                    Err(error) => proxy.refuse(id, error, route),
                };
                match name {
                    b"dev.firmware.abort" => answer(upload.abort().map(|()| Vec::new())),
                    b"dev.firmware.upgrade" => match commit {
                        Commit::Restart => {
                            answer(upload.upgrade(now).map(|_| {
                                upload.armed();
                                Vec::new()
                            }));
                            session += 1;
                            watches.tell(&proxy, Ok(alive(session)), None);
                        }
                        Commit::Refuse(error) => answer(Err(error)),
                        Commit::Lost { back, .. } => {
                            upload.upgrade(now).unwrap();
                            upload.armed();
                            answer(Err(wire_rpc::RpcError::Timeout));
                            watches.tell(&proxy, Err(Some(0)), LostCause::Disconnected.code());
                            if back {
                                session += 1;
                                watches.tell(&proxy, Ok(alive(session)), None);
                            }
                        }
                    },
                    b"dev.desc" => answer(Ok(format!(
                        "Twinleaf TEST R1 ((null)) [2026-11-24/{session:06}-DEV]"
                    )
                    .into_bytes())),
                    b"dev.firmware.upload" if request.args.is_empty() => {
                        answer(Ok(upload.cursor(now).to_le_bytes().to_vec()))
                    }
                    b"dev.firmware.upload" => {
                        let chunk = match request.args.len() == HEADER_SIZE
                            && request.args[..8] == *board.magic.as_bytes()
                        {
                            true => 0,
                            false => {
                                let offset = u32::from_le_bytes(
                                    request.args[..PACKAGE_OFFSET_SIZE].try_into().unwrap(),
                                );
                                1 + offset as usize / PACKAGE_CHUNK_DATA
                            }
                        };
                        sent.push(chunk);
                        let mut take = || match upload.chunk(&board, request.args, now)? {
                            Take::Header { .. } => Ok(Vec::new()),
                            Take::Image(image) => {
                                image.written();
                                Ok(Vec::new())
                            }
                        };
                        // A lost request or ack goes unanswered, and times out.
                        match fault(chunk) {
                            Fault::Deliver => answer(take()),
                            Fault::LoseRequest => {}
                            Fault::LoseAck => drop(take()),
                            Fault::Reject(error) => answer(Err(error)),
                        }
                    }
                    _ => answer(Ok(Vec::new())),
                }
            }
            let cursor = upload.cursor(0);
            (sent, cursor)
        });
        let device = {
            let device = device;
            device.with_timeout(Duration::from_millis(500))
        };
        let mut events = Vec::new();
        let result = flash(&device, &firmware, |e| events.push(e));
        drop(device);
        let (sent, cursor) = responder.join().unwrap();
        (result, events, sent, cursor)
    }

    /// An image of two full chunks and a short tail.
    const PACKAGE_IMAGE: usize = 2 * PACKAGE_CHUNK_DATA + 88;

    #[test]
    fn a_package_is_told_from_an_image() {
        let board = board();
        assert!(is_package(&package(&board, PACKAGE_IMAGE)));
        assert!(!is_package(&image(THREE_CHUNKS)));
        let mut truncated = package(&board, PACKAGE_IMAGE);
        truncated.pop();
        assert!(
            !is_package(&truncated),
            "the header names the image behind it"
        );
    }

    #[test]
    fn a_package_goes_header_first_then_by_offset_and_commits() {
        let board = board();
        let firmware = package(&board, PACKAGE_IMAGE);
        let (result, events, sent, cursor) =
            flash_package_against(board, firmware, Commit::Restart, |_| Fault::Deliver);

        result.unwrap();
        assert_eq!(sent, [0, 1, 2, 3]);
        assert_eq!(acked(&events), [1, 2, 3, 4]);
        assert_eq!(cursor, PACKAGE_IMAGE as u32);
        assert!(events.contains(&FlashEvent::Restarted));
    }

    /// Losing image chunk 1 gets chunk 2, already in flight, refused for its
    /// offset rather than written in chunk 1's place. Both are resent.
    #[test]
    fn a_lost_package_chunk_never_lets_the_next_take_its_place() {
        let board = board();
        let firmware = package(&board, PACKAGE_IMAGE);
        let mut lost = false;
        let (result, events, sent, cursor) =
            flash_package_against(board, firmware, Commit::Restart, move |chunk| {
                if chunk == 2 && !std::mem::replace(&mut lost, true) {
                    Fault::LoseRequest
                } else {
                    Fault::Deliver
                }
            });

        result.unwrap();
        assert_eq!(sent, [0, 1, 2, 3, 2, 3]);
        assert_eq!(resumed(&events), [3]);
        assert_eq!(cursor, PACKAGE_IMAGE as u32);
    }

    /// The device took the header but its ack was lost, and the first image
    /// chunk was lost too. A cursor of zero cannot say whether the header was
    /// taken, so it is sent again, and the device takes it again.
    #[test]
    fn a_header_whose_ack_was_lost_is_sent_again() {
        let board = board();
        let firmware = package(&board, PACKAGE_IMAGE);
        let mut faults = vec![Fault::LoseAck, Fault::LoseRequest].into_iter();
        let (result, _, sent, cursor) =
            flash_package_against(board, firmware, Commit::Restart, move |_| {
                faults.next().unwrap_or(Fault::Deliver)
            });

        result.unwrap();
        assert_eq!(sent, [0, 1, 0, 1, 2, 3]);
        assert_eq!(cursor, PACKAGE_IMAGE as u32);
    }

    /// A header the device refuses fails at once, without resending.
    #[test]
    fn a_package_for_another_board_is_refused_at_its_header() {
        let other = device_update::Package {
            board_id: crate::proto::BoardId::from_ascii("OTHER"),
            ..board()
        };
        let firmware = package(&board(), PACKAGE_IMAGE);
        let (result, _, sent, _) =
            flash_package_against(other, firmware, Commit::Restart, |_| Fault::Deliver);

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("cannot take this image: its header was refused"),
            "unexpected error: {err}"
        );
        assert_eq!(sent.first(), Some(&0));
        assert_eq!(sent.iter().filter(|&&chunk| chunk == 0).count(), 1);
    }

    /// Each kind of device refuses the other kind of image before anything is
    /// stopped or sent.
    #[test]
    fn a_device_refuses_the_other_kind_of_image() {
        let (result, events, sent, _) =
            flash_package_against(board(), image(THREE_CHUNKS), Commit::Restart, |_| {
                Fault::Deliver
            });
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("takes signed packages"),
            "unexpected error: {err}"
        );
        assert!(events.is_empty() && sent.is_empty());

        let (result, events, sent, _) =
            flash_against(package(&board(), PACKAGE_IMAGE), 0, |_| Fault::Deliver);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("takes images"), "unexpected error: {err}");
        assert!(events.is_empty() && sent.is_empty());
    }

    /// The build a package fake runs in `session`.
    fn build(session: u32) -> Option<String> {
        Some(format!("{session:06}-DEV"))
    }

    /// A commit answered, or one the device went silent on and the proxy
    /// answered with a timeout, is followed to the device back in a new
    /// session, and what it ran before and runs after are read.
    #[test]
    fn a_commit_is_followed_to_the_device_back_in_a_new_session() {
        let commits = [
            Commit::Restart,
            Commit::Lost {
                back: true,
                sessionless: false,
            },
            Commit::Lost {
                back: true,
                sessionless: true,
            },
        ];
        for commit in commits {
            let board = board();
            let firmware = package(&board, PACKAGE_IMAGE);
            let (result, events, _, _) =
                flash_package_against(board, firmware, commit, |_| Fault::Deliver);

            let flashed = result.unwrap();
            assert_eq!(flashed.before.hash, build(1));
            assert_eq!(flashed.after.unwrap().hash, build(2));
            assert!(events.ends_with(&[FlashEvent::Restarting, FlashEvent::Restarted]));
        }
    }

    /// A device that refuses the commit and stays reports its refusal, a
    /// timeout among them.
    #[test]
    fn a_commit_refused_by_a_device_that_stays_fails_as_itself() {
        let refusals = [wire_rpc::RpcError::Invalid, wire_rpc::RpcError::Timeout];
        for refusal in refusals {
            let board = board();
            let firmware = package(&board, PACKAGE_IMAGE);
            let (result, events, _, _) =
                flash_package_against(board, firmware, Commit::Refuse(refusal), |_| Fault::Deliver);

            let refused = matches!(
                result,
                Err(FirmwareError::Rpc(CallError::DeviceError(ref payload))) if payload.error == refusal
            );
            assert!(refused, "{result:?}");
            assert!(!events.contains(&FlashEvent::Restarted));
        }
    }

    /// A device lost after its commit that never comes back leaves the
    /// upgrade committed, with nothing read after it.
    #[test]
    fn a_device_that_never_comes_back_leaves_nothing_read_after() {
        let board = board();
        let firmware = package(&board, PACKAGE_IMAGE);
        let lost = Commit::Lost {
            back: false,
            sessionless: false,
        };
        let (result, events, _, _) =
            flash_package_against(board, firmware, lost, |_| Fault::Deliver);

        let flashed = result.unwrap();
        assert_eq!(flashed.before.hash, build(1));
        assert!(flashed.after.is_none());
        assert_eq!(events.last(), Some(&FlashEvent::Restarting));
    }

    #[test]
    fn update_available_only_when_strictly_newer() {
        // Installed older than latest -> update.
        assert_eq!(
            compare(
                &installed(Some("2026-01-10"), None),
                &release("2026-03-17", "5d1494")
            ),
            UpdateStatus::UpdateAvailable
        );
        // Installed newer than latest -> up to date.
        assert_eq!(
            compare(
                &installed(Some("2026-06-04"), None),
                &release("2026-03-17", "5d1494")
            ),
            UpdateStatus::UpToDate
        );
        // Installed same date -> up to date.
        assert_eq!(
            compare(
                &installed(Some("2026-03-17"), None),
                &release("2026-03-17", "5d1494")
            ),
            UpdateStatus::UpToDate
        );
        // Unknown installed date -> unknown.
        assert_eq!(
            compare(&installed(None, None), &release("2026-03-17", "5d1494")),
            UpdateStatus::Unknown
        );
        // Matching hash short-circuits to up to date even with an older parsed date.
        assert_eq!(
            compare(
                &installed(Some("2026-01-10"), Some("5d1494")),
                &release("2026-03-17", "5d1494")
            ),
            UpdateStatus::UpToDate
        );
    }
}

//! Firmware update plumbing.
//!
//! This module is dependency-free "plumbing": it queries what firmware a
//! device is running, compares it against a catalog of published firmware,
//! and flashes an image — with **no** user interaction, printing, or progress
//! rendering. Callers (the `tio` CLI, the macOS app, …) supply the porcelain.
//!
//! Networking is abstracted behind the [`FirmwareCatalog`] trait so consumers
//! can plug in their own source. A ready-made GitHub-backed catalog is provided
//! in [`github`] behind the `firmware-update` feature.

use crate::device::rpc::{pipelined, CallError};
use crate::device::Device;
use crate::proto::rpc as wire_rpc;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(feature = "firmware-update")]
pub mod github;

/// Firmware images are uploaded to the device in fixed-size chunks.
const UPLOAD_CHUNK_SIZE: usize = 288;
/// Bytes of every chunk that are envelope rather than image data: the AES IV
/// and the size/offset/crc/id header the device checks before appending.
const CHUNK_ENVELOPE: usize = 32;
/// Maximum number of upload chunks awaiting acknowledgement at once.
const MAX_CHUNKS_IN_FLIGHT: usize = 2;
/// Consecutive resumptions that leave the device's upload cursor where it was
/// before the upload is abandoned.
const MAX_STALLS: u32 = 3;
/// Time to keep the link up after committing, so the device is not
/// power-cycled mid-write. Part of the safe upgrade procedure, not just UX.
#[cfg(not(test))]
const COMMIT_SETTLE_TIME: Duration = Duration::from_secs(5);
#[cfg(test)]
const COMMIT_SETTLE_TIME: Duration = Duration::from_millis(10);

/// A firmware build date (UTC calendar date). Ordered chronologically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FirmwareDate {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

impl FirmwareDate {
    /// Parse an ISO `YYYY-MM-DD` date. Returns `None` on any malformed field.
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
    /// Sensor name parsed from `dev.desc` (e.g. `ASM`). Empty if the
    /// description does not follow the `{vendor} {name} {revision}` header form.
    pub name: String,
    /// Hardware revision parsed from `dev.desc` (e.g. `R6`). Empty if absent.
    pub revision: String,
    /// Build date from the `[YYYY-MM-DD/...]` field of `dev.desc`, if present.
    pub build_date: Option<FirmwareDate>,
    /// Firmware build version: the text after `/` in the `[date/build]` field
    /// of `dev.desc` (e.g. `4b13b1-DEV`, `022547`). A value containing `DEV`
    /// marks a development build (see [`Self::is_development`]).
    pub hash: Option<String>,
    /// Device serial/identifier from the parenthesized field of `dev.desc`
    /// (`(null)` becomes `None`).
    pub serial: Option<String>,
    /// True when the firmware is a development build (its build version contains
    /// `DEV`) and therefore cannot be replaced by a published release.
    pub is_development: bool,
    /// The raw `dev.desc` string, for display/diagnostics.
    pub description: String,
}

/// A firmware image published in a [`FirmwareCatalog`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirmwareRelease {
    pub name: String,
    pub revision: String,
    pub date: FirmwareDate,
    pub short_hash: String,
    pub filename: String,
    /// Catalog-specific locator used by [`FirmwareCatalog::download`].
    pub url: String,
}

/// Result of comparing installed firmware against the latest published release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStatus {
    /// Installed firmware is at least as new as the latest release (or its hash
    /// matches), so there is nothing to do.
    UpToDate,
    /// A strictly newer release is available.
    UpdateAvailable,
    /// No firmware is published for this name/revision.
    NoPublishedFirmware,
    /// The device is running a development build, which cannot be replaced by a
    /// published release. No catalog lookup is performed in this case.
    DevelopmentBuild,
    /// The installed build date could not be determined, so freshness is
    /// unknown; the caller should decide whether to offer the latest release.
    Unknown,
}

/// The full picture needed to present an update decision to a user.
#[derive(Debug, Clone)]
pub struct UpdateReport {
    pub installed: InstalledFirmware,
    /// All published releases for this name/revision, sorted newest-first.
    /// Empty for development builds or when nothing is published. Useful for a
    /// "pick a version" (downgrade) flow.
    pub releases: Vec<FirmwareRelease>,
    /// The latest published release (i.e. `releases.first()`), if any.
    pub latest: Option<FirmwareRelease>,
    pub status: UpdateStatus,
}

/// Result of the `dev.stop` issued at the start of [`flash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopOutcome {
    /// `dev.stop` succeeded; the device stopped streaming.
    Stopped,
    /// The device reported it was already stopped.
    AlreadyStopped,
    /// The device has no `dev.stop` RPC; flashing proceeds anyway.
    Unsupported,
}

/// Progress events emitted by [`flash`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlashEvent {
    /// About to issue `dev.stop`.
    Stopping,
    /// `dev.stop` completed with this outcome.
    Stopped(StopOutcome),
    /// `chunk` of `total` chunks have been acknowledged by the device.
    Uploading { chunk: usize, total: usize },
    /// The upload failed with `error` and is resuming at `chunk` of `total`,
    /// where the device reports its upload cursor.
    Resuming {
        chunk: usize,
        total: usize,
        error: String,
    },
    /// Upload finished; the commit RPC is being issued.
    Committing,
    /// Commit accepted; settling before the link can be dropped.
    Finalizing,
    /// Upgrade fully committed.
    Complete,
}

/// Errors produced by firmware operations.
#[derive(Debug, thiserror::Error)]
pub enum FirmwareError {
    #[error("device RPC failed: {0}")]
    Rpc(#[from] CallError),
    #[error("could not determine installed firmware: {0}")]
    Parse(String),
    #[error("firmware catalog error: {0}")]
    Catalog(String),
    #[error("firmware cache I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("firmware upload failed: {0}")]
    Upload(String),
}

/// A source of published firmware images (e.g. a GitHub repository).
///
/// Implement this to point the updater at a different location, or use the
/// built-in [`github::GithubCatalog`] (requires the `firmware-update` feature).
pub trait FirmwareCatalog {
    /// All published releases for a given sensor name and hardware revision.
    /// Returns an empty list when none are published.
    fn list_releases(
        &self,
        name: &str,
        revision: &str,
    ) -> Result<Vec<FirmwareRelease>, FirmwareError>;

    /// Fetch the raw firmware image bytes for a release.
    fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError>;
}

/// Content between the first `(` and last `)`, if any.
fn paren_content(desc: &str) -> Option<&str> {
    let start = desc.find('(')?;
    let end = desc.rfind(')')?;
    (end > start).then(|| &desc[start + 1..end])
}

/// Content inside the last `[...]` group, if any.
fn bracket_content(desc: &str) -> Option<&str> {
    let start = desc.rfind('[')?;
    let rest = &desc[start + 1..];
    let end = rest.find(']')?;
    Some(&rest[..end])
}

/// Parse a device's `dev.desc` string, the single source of installed-firmware
/// identity. Two layouts are seen in the field, both handled here:
///
/// - `Twinleaf ASM R6 ((null)) [2026-06-04/022547]`
/// - `HUB-USB-RS422 (010000003D003B001850453657353320) [2026-05-28/4b13b1-DEV]`
///
/// The general shape is `{header} ({serial}) [{date}/{build}]`:
/// - `header` whitespace-splits into `{vendor} {name} {revision}` (name/revision
///   are tokens 1 and 2; absent for headers that aren't in that form),
/// - the parenthesized field is the device serial (`(null)` -> none),
/// - inside `[...]`, the part before `/` is the build date and the part after
///   `/` is the build version (which contains `DEV` for development builds).
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

/// Query a connected device for the firmware it is currently running.
///
/// Everything is derived from the single `dev.desc` RPC; `dev.name`/
/// `dev.revision` are not used.
pub fn query_installed(device: &Device) -> Result<InstalledFirmware, FirmwareError> {
    let desc: String = device.get("dev.desc")?;
    Ok(parse_installed(&desc))
}

/// Pick the newest release from a list (newest build date wins; filename breaks
/// ties deterministically).
pub fn latest_release(mut releases: Vec<FirmwareRelease>) -> Option<FirmwareRelease> {
    releases.sort_by(|a, b| {
        a.date
            .cmp(&b.date)
            .then_with(|| a.filename.cmp(&b.filename))
    });
    releases.pop()
}

/// Decide whether `latest` is newer than what is `installed`.
fn compare(installed: &InstalledFirmware, latest: &FirmwareRelease) -> UpdateStatus {
    // An exact hash match means the same build, regardless of parsed dates.
    if let Some(installed_hash) = &installed.hash {
        if installed_hash.eq_ignore_ascii_case(&latest.short_hash) {
            return UpdateStatus::UpToDate;
        }
    }
    match installed.build_date {
        Some(installed_date) if latest.date > installed_date => UpdateStatus::UpdateAvailable,
        Some(_) => UpdateStatus::UpToDate,
        None => UpdateStatus::Unknown,
    }
}

/// Compare the firmware installed on a device against the latest published in
/// `catalog`, returning everything needed to present the decision.
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

    if installed.name.is_empty() || installed.revision.is_empty() {
        return Err(FirmwareError::Parse(format!(
            "could not determine sensor name and revision from dev.desc: {:?}",
            installed.description
        )));
    }

    let mut releases = catalog.list_releases(&installed.name, &installed.revision)?;
    // Sort newest-first so `releases[0]` is the latest and a picker reads
    // naturally top-to-bottom.
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

/// Default on-disk cache root for downloaded firmware
/// (`<os-cache>/twinleaf/firmware`).
pub fn default_cache_dir() -> Option<PathBuf> {
    directories::BaseDirs::new().map(|b| b.cache_dir().join("twinleaf").join("firmware"))
}

/// Location a release is cached at under `cache_root`.
pub fn cache_path(cache_root: &Path, release: &FirmwareRelease) -> PathBuf {
    cache_root
        .join(&release.name)
        .join(&release.revision)
        .join(&release.filename)
}

/// Return a release's bytes, using the on-disk cache when possible.
///
/// If the exact file (its name embeds the build date and hash) is already
/// cached, it is read from disk; otherwise it is downloaded via `catalog` and
/// written atomically (temp file + rename) so an interrupted download never
/// leaves a truncated image behind.
pub fn download_cached(
    catalog: &dyn FirmwareCatalog,
    release: &FirmwareRelease,
    cache_root: &Path,
) -> Result<Vec<u8>, FirmwareError> {
    let path = cache_path(cache_root, release);

    if let Ok(meta) = std::fs::metadata(&path) {
        if meta.is_file() && meta.len() > 0 {
            return Ok(std::fs::read(&path)?);
        }
    }

    let data = catalog.download(release)?;
    if data.is_empty() {
        return Err(FirmwareError::Catalog(format!(
            "downloaded firmware {} is empty",
            release.filename
        )));
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("part");
    std::fs::write(&tmp, &data)?;
    std::fs::rename(&tmp, &path)?;

    Ok(data)
}

/// Upload a firmware image to the device and commit the upgrade.
///
/// Progress is reported through `on_event`; nothing is printed. A chunk lost
/// in either direction fails its window; the upload then reads the device's
/// cursor and resumes from that chunk, giving up only after three
/// resumptions in a row that move the cursor nowhere. The function blocks for
/// a short settle period after committing (see [`FlashEvent::Finalizing`]) so
/// the device is not power-cycled mid-write.
pub fn flash(
    device: &Device,
    firmware_data: &[u8],
    mut on_event: impl FnMut(FlashEvent),
) -> Result<(), FirmwareError> {
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

    let total_chunks = firmware_data.len().div_ceil(UPLOAD_CHUNK_SIZE);
    let mut resume_at = 0;
    let mut stalls = 0;
    loop {
        let sends = firmware_data
            .chunks(UPLOAD_CHUNK_SIZE)
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
        let Err(error) = outcome else { break };
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
            break;
        }
        on_event(FlashEvent::Resuming {
            chunk: next + 1,
            total: total_chunks,
            error: error.to_string(),
        });
        resume_at = next;
    }

    on_event(FlashEvent::Committing);
    device.action("dev.firmware.upgrade")?;

    on_event(FlashEvent::Finalizing);
    std::thread::sleep(COMMIT_SETTLE_TIME);

    on_event(FlashEvent::Complete);
    Ok(())
}

/// The chunk of `image` starting at the device's upload cursor, `image`'s chunk
/// count when the cursor is at its end, or None when this image has no chunk
/// boundary there.
fn chunk_at(cursor: u32, image: &[u8]) -> Option<usize> {
    let total_chunks = image.len().div_ceil(UPLOAD_CHUNK_SIZE);
    let data_per_chunk = UPLOAD_CHUNK_SIZE - CHUNK_ENVELOPE;
    let data = image.len().saturating_sub(total_chunks * CHUNK_ENVELOPE);
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
    use crate::tio::packet::Payload;
    use crate::tio::proxy::RawCallError;
    use crate::tio::proxy_core::ProxyCommand;

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

    #[test]
    fn missing_name_revision_errors_for_release() {
        // Non-dev build whose header has no name/revision can't be looked up.
        let fw = parse_installed("HUB-USB-RS422 (0100ABCD) [2026-05-28/4b13b1]");
        assert!(check_for_update(fw, &PanicCatalog).is_err());
    }

    #[test]
    fn latest_release_picks_newest_date() {
        let latest = latest_release(vec![
            release("2026-01-10", "aaaaaa"),
            release("2026-03-17", "5d1494"),
            release("2025-12-31", "bbbbbb"),
        ])
        .unwrap();
        assert_eq!(latest.date, FirmwareDate::parse("2026-03-17").unwrap());
        assert_eq!(latest.short_hash, "5d1494");
    }

    /// What the fake device does with a chunk that arrived at its cursor.
    enum Fault {
        Deliver,
        LoseRequest,
        LoseAck,
        Reject,
    }

    /// A chunked image whose every chunk announces its data offset in its
    /// first four bytes, so the fake device can check it as the real one does.
    fn image(len: usize) -> Vec<u8> {
        let mut image = vec![0xa5u8; len];
        for (index, chunk) in image.chunks_mut(UPLOAD_CHUNK_SIZE).enumerate() {
            let offset = (index * (UPLOAD_CHUNK_SIZE - CHUNK_ENVELOPE)) as u32;
            chunk[..4].copy_from_slice(&offset.to_le_bytes());
        }
        image
    }

    /// Run [`flash`] against a fake device that keeps tl-chibi's upload
    /// cursor: an empty request reads it, a chunk whose offset is elsewhere is
    /// rejected, and `fault` decides what happens to one that matches. Control
    /// RPCs are always acknowledged. Returns the outcome, the events, the
    /// chunk indices sent in order, and the device's final cursor.
    fn flash_against(
        firmware_len: usize,
        cursor: u32,
        fault: impl FnMut(usize) -> Fault + Send + 'static,
    ) -> (Result<(), FirmwareError>, Vec<FlashEvent>, Vec<usize>, u32) {
        let (device, calls, _worker) = crate::device::Device::test_pair();
        let responder = std::thread::spawn(move || {
            let mut cursor = cursor;
            let mut fault = fault;
            let mut sent = Vec::new();
            for call in calls.iter() {
                let ProxyCommand::Call {
                    request,
                    complete: result,
                    ..
                } = call
                else {
                    panic!("flash only submits direct RPC calls");
                };
                let Payload::RpcRequest(request) = request.payload() else {
                    panic!("expected an RPC request");
                };
                let wire_rpc::Method::ByName(name) = request.method else {
                    panic!("expected a call by name");
                };
                if name != b"dev.firmware.upload" {
                    result(Ok(Vec::new()));
                    continue;
                }
                if request.args.is_empty() {
                    result(Ok(cursor.to_le_bytes().to_vec()));
                    continue;
                }
                let offset = u32::from_le_bytes(request.args[..4].try_into().unwrap());
                let data = (request.args.len() - CHUNK_ENVELOPE) as u32;
                let chunk = offset as usize / (UPLOAD_CHUNK_SIZE - CHUNK_ENVELOPE);
                sent.push(chunk);
                let invalid = || RawCallError::Device {
                    error: wire_rpc::RpcError::Invalid,
                    message: Vec::new(),
                };
                if offset != cursor {
                    result(Err(invalid()));
                    continue;
                }
                let reply = match fault(chunk) {
                    Fault::Deliver => {
                        cursor += data;
                        Ok(Vec::new())
                    }
                    Fault::LoseRequest => Err(RawCallError::Timeout),
                    Fault::LoseAck => {
                        cursor += data;
                        Err(RawCallError::Timeout)
                    }
                    Fault::Reject => Err(invalid()),
                };
                result(reply);
            }
            (sent, cursor)
        });

        let mut events = Vec::new();
        let result = flash(&device, &image(firmware_len), |e| events.push(e));
        drop(device);
        let (sent, cursor) = responder.join().unwrap();
        (result, events, sent, cursor)
    }

    /// Three chunks: two full ones and a short tail.
    const THREE_CHUNKS: usize = 2 * UPLOAD_CHUNK_SIZE + 64;
    const THREE_CHUNKS_DATA: u32 = (THREE_CHUNKS - 3 * CHUNK_ENVELOPE) as u32;

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
    fn flash_acknowledges_every_chunk_in_order_and_commits() {
        let (result, events, sent, cursor) = flash_against(THREE_CHUNKS, 0, |_| Fault::Deliver);

        result.unwrap();
        assert_eq!(sent, [0, 1, 2]);
        assert_eq!(acked(&events), [1, 2, 3]);
        assert_eq!(cursor, THREE_CHUNKS_DATA);
        assert!(events.contains(&FlashEvent::Complete));
    }

    /// Losing chunk 1 also gets chunk 2, already in flight, rejected for its
    /// offset; the upload reads the cursor and resends both.
    #[test]
    fn a_lost_chunk_is_resent_from_the_devices_cursor() {
        let mut lost = false;
        let (result, events, sent, cursor) = flash_against(THREE_CHUNKS, 0, move |chunk| {
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

    /// A chunk the device applied but whose ack was lost is not resent: the
    /// cursor is already past it.
    #[test]
    fn a_lost_ack_is_not_resent() {
        let (result, events, sent, cursor) = flash_against(THREE_CHUNKS, 0, |chunk| {
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

    /// A device already holding the first chunk rejects chunk 0; the upload
    /// resumes from wherever the cursor is by then.
    #[test]
    fn a_partial_upload_resumes_where_the_device_left_off() {
        let first_chunk = (UPLOAD_CHUNK_SIZE - CHUNK_ENVELOPE) as u32;
        let (result, events, sent, cursor) =
            flash_against(THREE_CHUNKS, first_chunk, |_| Fault::Deliver);

        result.unwrap();
        assert_eq!(sent, [0, 1, 2]);
        assert_eq!(resumed(&events), [3]);
        assert_eq!(cursor, THREE_CHUNKS_DATA);
    }

    #[test]
    fn an_upload_the_device_keeps_rejecting_fails_without_progress() {
        let (result, _, sent, _) = flash_against(100, 0, |_| Fault::Reject);

        let err = result.unwrap_err().to_string();
        assert_eq!(sent.len(), MAX_STALLS as usize);
        assert!(
            err.contains("failed 3 times without progress") && err.contains("invalid arguments"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn an_upload_that_keeps_timing_out_fails_without_progress() {
        let (result, _, sent, _) = flash_against(100, 0, |_| Fault::LoseRequest);

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
        let data_per_chunk = (UPLOAD_CHUNK_SIZE - CHUNK_ENVELOPE) as u32;

        assert_eq!(chunk_at(0, &image), Some(0));
        assert_eq!(chunk_at(data_per_chunk, &image), Some(1));
        assert_eq!(chunk_at(THREE_CHUNKS_DATA, &image), Some(3));
        assert_eq!(chunk_at(data_per_chunk + 1, &image), None);
        assert_eq!(chunk_at(THREE_CHUNKS_DATA + 256, &image), None);
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

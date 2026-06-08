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

use crate::tio::proto::{DeviceRoute, Payload, RpcErrorCode};
use crate::tio::proxy::{Port, RecvError, RpcError};
use crate::tio::util::PacketBuilder;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(feature = "firmware-update")]
pub mod github;

/// Firmware images are uploaded to the device in fixed-size chunks.
const UPLOAD_CHUNK_SIZE: usize = 288;
/// Maximum number of upload chunks awaiting acknowledgement at once.
const MAX_CHUNKS_IN_FLIGHT: u16 = 2;
/// Time to keep the link up after committing, so the device is not
/// power-cycled mid-write. Part of the safe upgrade procedure, not just UX.
const COMMIT_SETTLE_TIME: Duration = Duration::from_secs(5);

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashEvent {
    /// About to issue `dev.stop`.
    Stopping,
    /// `dev.stop` completed with this outcome.
    Stopped(StopOutcome),
    /// `chunk` of `total` chunks have been acknowledged by the device.
    Uploading { chunk: usize, total: usize },
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
    Rpc(#[from] RpcError),
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
    let header_end = desc.find(|c| c == '(' || c == '[').unwrap_or(desc.len());
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
pub fn query_installed(device: &Port) -> Result<InstalledFirmware, FirmwareError> {
    let desc: String = device.rpc("dev.desc", ())?;
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
/// Progress is reported through `on_event`; nothing is printed. The function
/// blocks for a short settle period after committing (see
/// [`FlashEvent::Finalizing`]) so the device is not power-cycled mid-write.
pub fn flash(
    device: &Port,
    firmware_data: &[u8],
    mut on_event: impl FnMut(FlashEvent),
) -> Result<(), FirmwareError> {
    // Stop streaming first. A device that lacks dev.stop, or is already
    // stopped, is fine to proceed with.
    on_event(FlashEvent::Stopping);
    let stop_outcome = match device.action("dev.stop") {
        Ok(()) => StopOutcome::Stopped,
        Err(RpcError::ExecError(ref e)) if matches!(e.error, RpcErrorCode::NotFound) => {
            StopOutcome::Unsupported
        }
        Err(RpcError::ExecError(ref e)) if matches!(e.error, RpcErrorCode::WrongDeviceState) => {
            StopOutcome::AlreadyStopped
        }
        Err(e) => return Err(e.into()),
    };
    on_event(FlashEvent::Stopped(stop_outcome));

    let total_chunks = firmware_data.len().div_ceil(UPLOAD_CHUNK_SIZE);

    let mut next_send_chunk: u16 = 0;
    let mut next_ack_chunk: u16 = 0;
    let mut more_to_send = true;

    while more_to_send || (next_ack_chunk != next_send_chunk) {
        if more_to_send && ((next_send_chunk - next_ack_chunk) < MAX_CHUNKS_IN_FLIGHT) {
            let offset = usize::from(next_send_chunk) * UPLOAD_CHUNK_SIZE;
            let chunk_end = (offset + UPLOAD_CHUNK_SIZE).min(firmware_data.len());

            device
                .send(PacketBuilder::make_rpc_request(
                    "dev.firmware.upload",
                    &firmware_data[offset..chunk_end],
                    next_send_chunk,
                    DeviceRoute::root(),
                ))
                .map_err(|e| {
                    FirmwareError::Upload(format!(
                        "failed to send firmware chunk {}/{}: {}",
                        next_send_chunk + 1,
                        total_chunks,
                        e
                    ))
                })?;
            next_send_chunk += 1;
            more_to_send = chunk_end < firmware_data.len();
        }

        let pkt = if more_to_send && ((next_send_chunk - next_ack_chunk) < MAX_CHUNKS_IN_FLIGHT) {
            match device.try_recv() {
                Ok(pkt) => pkt,
                Err(RecvError::WouldBlock) => continue,
                Err(e) => {
                    return Err(FirmwareError::Upload(format!(
                        "failed to receive firmware upload ack: {}",
                        e
                    )))
                }
            }
        } else {
            device.recv().map_err(|e| {
                FirmwareError::Upload(format!("failed to receive firmware upload ack: {}", e))
            })?
        };

        match pkt.payload {
            Payload::RpcReply(rep) => {
                if rep.id != next_ack_chunk {
                    return Err(FirmwareError::Upload(format!(
                        "firmware chunk ack out of order (expected {}, got {})",
                        next_ack_chunk, rep.id
                    )));
                }
                next_ack_chunk += 1;
                on_event(FlashEvent::Uploading {
                    chunk: next_ack_chunk as usize,
                    total: total_chunks,
                });
            }
            Payload::RpcError(err) => {
                return Err(FirmwareError::Upload(format!(
                    "device rejected firmware chunk {}/{}: {}",
                    next_ack_chunk + 1,
                    total_chunks,
                    err.error
                )));
            }
            _ => continue,
        }
    }

    on_event(FlashEvent::Committing);
    device.action("dev.firmware.upgrade")?;

    on_event(FlashEvent::Finalizing);
    std::thread::sleep(COMMIT_SETTLE_TIME);

    on_event(FlashEvent::Complete);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

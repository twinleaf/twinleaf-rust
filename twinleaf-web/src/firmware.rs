//! The firmware of the hub's devices, and the upgrades the pages ask for.
//!
//! The hub reads what each device runs from its `dev.desc`, once each time
//! its RPC table loads and again after an upgrade, as `twinleaf::firmware`
//! reads it. A read that fails leaves what the hub knew. The catalog of
//! published firmware is looked up only when a page asks, as `tio upgrade`
//! looks it up when it is run, never on a timer. The hub keeps what it
//! found for each device, and once a device runs other firmware it compares
//! that with the releases it holds, without the catalog. What a device runs
//! and what the catalog holds for it is its [`Message::Firmware`].
//!
//! An upgrade installs the newest release, a release the page chose, or a
//! file the page sent, which the hub keeps until the upgrade is done, so
//! any page can run a failed one again. One upgrade runs at a time across
//! the hub, in the order asked, and the others wait. A release is
//! downloaded, through the cache when there is one, then flashed with
//! `twinleaf::firmware::flash`, and the hub reads `dev.desc` while the
//! device restarts. Where an upgrade stands is its [`Message::Upgrade`],
//! which a device has once a page asked an upgrade of it. An upgrade that
//! waits or downloads can be taken back, and nothing is sent to the device.
//! Once the image is on its way to the device it cannot.
//!
//! The catalog is GitHub's, unless [`DIR_VARIABLE`] names a directory laid
//! out as the published repository is, which is then read from disk. The
//! calls of `twinleaf::firmware` block, so each runs off the runtime's
//! threads.

use crate::devices::{Devices, Kind};
use crate::protocol::{
    CatalogState, Installed, Message, ReleaseInfo, SourceKind, Step, UpdateStatus, UpgradeState,
};
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use twinleaf::firmware::{
    self as fw, github::GithubCatalog, FirmwareCatalog, FirmwareError, FirmwareRelease, FlashEvent,
    InstalledFirmware, UpdateReport,
};
use twinleaf::{DeviceRoute, DeviceTree};

/// The largest image an upgrade takes, 16 MiB, well above any device's
/// firmware.
pub(crate) const MAX_IMAGE: u64 = 16 * 1024 * 1024;
/// The environment variable that names a directory to read the catalog
/// from in place of GitHub.
pub(crate) const DIR_VARIABLE: &str = "TWINLEAF_FIRMWARE_DIR";
/// How often an upload says how far it is.
const PROGRESS_EVERY: Duration = Duration::from_millis(100);

/// Where the hub finds published firmware: a catalog, and the directory
/// its downloads are kept in, or `None` to keep none.
#[derive(Clone)]
pub(crate) struct Catalog {
    catalog: Arc<dyn FirmwareCatalog + Send + Sync>,
    cache: Option<PathBuf>,
}

impl Catalog {
    pub(crate) fn new(
        catalog: Arc<dyn FirmwareCatalog + Send + Sync>,
        cache: Option<PathBuf>,
    ) -> Catalog {
        Catalog { catalog, cache }
    }

    /// The directory [`DIR_VARIABLE`] names, with no cache, or else the
    /// published repository on GitHub, cached where `tio upgrade` caches it.
    pub(crate) fn from_env() -> Catalog {
        match std::env::var_os(DIR_VARIABLE).filter(|dir| !dir.is_empty()) {
            Some(dir) => Catalog::new(Arc::new(Directory(PathBuf::from(dir))), None),
            None => Catalog::new(Arc::new(GithubCatalog::twinleaf()), fw::default_cache_dir()),
        }
    }

    /// The bytes of `release`, from the cache when it holds them.
    fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
        match &self.cache {
            Some(root) => fw::download_cached(&*self.catalog, release, root),
            None => match self.catalog.download(release)? {
                data if data.is_empty() => Err(FirmwareError::Catalog(format!(
                    "downloaded firmware {} is empty",
                    release.filename
                ))),
                data => Ok(data),
            },
        }
    }
}

/// A catalog in a directory laid out as the published repository is,
/// `{name}/{revision}/{name}-{revision}-firmware-{YYYY-MM-DD}-{hash}.bin`.
/// A name and revision with no folder have no release, and a root that is
/// no directory cannot be listed.
pub(crate) struct Directory(pub(crate) PathBuf);

/// Whether `part` names one folder inside another, and no other place.
fn plain(part: &str) -> bool {
    !part.is_empty() && part != "." && part != ".." && !part.contains(['/', '\\', ':'])
}

impl FirmwareCatalog for Directory {
    fn list_releases(
        &self,
        name: &str,
        revision: &str,
    ) -> Result<Vec<FirmwareRelease>, FirmwareError> {
        if !self.0.is_dir() {
            return Err(FirmwareError::Catalog(format!(
                "{} is no directory",
                self.0.display()
            )));
        }
        if !plain(name) || !plain(revision) {
            return Ok(Vec::new());
        }
        let folder = self.0.join(name).join(revision);
        let unlisted = |error: std::io::Error| {
            FirmwareError::Catalog(format!("cannot list {}: {error}", folder.display()))
        };
        let entries = match std::fs::read_dir(&folder) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(unlisted(error)),
        };
        let mut releases = Vec::new();
        for entry in entries {
            let entry = entry.map_err(unlisted)?;
            if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
                continue;
            }
            let Some(file) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let path = entry.path().to_string_lossy().into_owned();
            releases.extend(FirmwareRelease::parse_filename(
                name, revision, &file, &path,
            ));
        }
        Ok(releases)
    }

    fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
        std::fs::read(&release.url).map_err(|error| {
            FirmwareError::Catalog(format!("cannot read {}: {error}", release.url))
        })
    }
}

/// The releases the hub holds for a device, a catalog that lists them and
/// downloads none, so a status is found again without the catalog.
struct Held(Vec<FirmwareRelease>);

impl FirmwareCatalog for Held {
    fn list_releases(&self, _: &str, _: &str) -> Result<Vec<FirmwareRelease>, FirmwareError> {
        Ok(self.0.clone())
    }

    fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
        Err(FirmwareError::Catalog(format!(
            "{} is not downloaded from what the hub holds",
            release.filename
        )))
    }
}

/// Refuses an image of `size` bytes, the file `name`, that is empty or more
/// than [`MAX_IMAGE`].
pub(crate) fn bounded(name: &str, size: u64) -> Result<(), String> {
    match size {
        0 => Err(format!("the image {name} is empty")),
        size if size > MAX_IMAGE => Err(format!(
            "the image {name} is {size} bytes, more than the {MAX_IMAGE} an upgrade takes"
        )),
        _ => Ok(()),
    }
}

/// What a page asks an upgrade to install.
pub(crate) enum Wanted {
    /// The newest release the catalog lists for the device.
    Latest,
    /// The release of this file name.
    Release(String),
    /// The file `name` the page sent.
    File { name: String, bytes: Vec<u8> },
}

/// What an upgrade installs.
#[derive(Clone)]
enum Image {
    /// A release of the catalog, the newest when `latest`.
    Release {
        release: FirmwareRelease,
        latest: bool,
    },
    /// A file a page sent.
    File { name: String, bytes: Arc<Vec<u8>> },
}

impl Image {
    fn source(&self) -> SourceKind {
        match self {
            Image::Release { latest: true, .. } => SourceKind::Latest,
            Image::Release { latest: false, .. } => SourceKind::Release,
            Image::File { .. } => SourceKind::File,
        }
    }

    /// What the upgrade installs, as the page names it.
    fn target(&self) -> String {
        match self {
            Image::Release { release, .. } => format!("{} {}", release.date, release.short_hash),
            Image::File { name, .. } => name.clone(),
        }
    }
}

/// Whether an upgrade in `state` waits or runs.
fn runs(state: &Option<UpgradeState>) -> bool {
    !matches!(
        state,
        None | Some(UpgradeState::Idle | UpgradeState::Done { .. } | UpgradeState::Failed { .. })
    )
}

/// What the hub knows of one device's firmware.
struct Entry {
    installed: Option<InstalledFirmware>,
    catalog: CatalogState,
    /// The releases the catalog listed at the last check, newest first, or
    /// `None` when it listed none for the device.
    listed: Option<Vec<FirmwareRelease>>,
    /// What the device's firmware message last said.
    told: Option<(Option<Installed>, CatalogState)>,
    /// Where the upgrade last asked of it stands, `None` for none asked.
    upgrade: Option<UpgradeState>,
    /// What that upgrade installs, kept while it waits, runs or failed.
    image: Option<Image>,
    /// The number of the upgrade asked last. A task whose upgrade was taken
    /// back or replaced tells nothing more.
    job: u64,
    /// Takes back the download of the upgrade that runs.
    cancel: Option<watch::Sender<bool>>,
}

impl Default for Entry {
    fn default() -> Entry {
        Entry {
            installed: None,
            catalog: CatalogState::Unchecked,
            listed: None,
            told: None,
            upgrade: None,
            image: None,
            job: 0,
            cancel: None,
        }
    }
}

#[derive(Default)]
struct State {
    entries: BTreeMap<DeviceRoute, Entry>,
    /// The upgrades that wait, by device and number, in the order asked.
    queue: VecDeque<(DeviceRoute, u64)>,
    /// The device whose upgrade runs.
    active: Option<DeviceRoute>,
    /// Whether a task runs the queue. It ends only as it finds the queue
    /// empty, under the lock an ask starts another under.
    running: bool,
    /// Whether a check of the catalog runs.
    checking: bool,
}

/// The page's view of what a device runs.
fn info(installed: &InstalledFirmware) -> Installed {
    Installed {
        name: installed.name.clone(),
        revision: installed.revision.clone(),
        date: installed.build_date.map(|date| date.to_string()),
        build: installed.hash.clone(),
        serial: installed.serial.clone(),
        development: installed.is_development,
    }
}

/// What a check of the catalog says, and the releases it listed, `None`
/// when it listed none. A device whose `dev.desc` names no sensor and
/// revision has no published firmware to look up.
fn outcome(
    checked: Result<UpdateReport, FirmwareError>,
) -> (CatalogState, Option<Vec<FirmwareRelease>>) {
    let report = match checked {
        Ok(report) => report,
        Err(FirmwareError::Parse(_)) => {
            let none = CatalogState::Checked {
                status: UpdateStatus::None,
                releases: Vec::new(),
            };
            return (none, None);
        }
        Err(error) => {
            let error = error.to_string();
            return (CatalogState::Unreachable { error }, None);
        }
    };
    let status = match report.status {
        fw::UpdateStatus::UpdateAvailable => UpdateStatus::Update,
        fw::UpdateStatus::UpToDate => UpdateStatus::Current,
        fw::UpdateStatus::NoPublishedFirmware => UpdateStatus::None,
        fw::UpdateStatus::DevelopmentBuild => UpdateStatus::Development,
        fw::UpdateStatus::Unknown => UpdateStatus::Unknown,
    };
    let releases = report
        .releases
        .iter()
        .map(|release| ReleaseInfo {
            date: release.date.to_string(),
            build: release.short_hash.clone(),
            file: release.filename.clone(),
        })
        .collect();
    let listed = (status != UpdateStatus::Development).then_some(report.releases);
    (CatalogState::Checked { status, releases }, listed)
}

/// What the catalog holds for a device that runs `installed` now, from what
/// the last check found. A device the catalog listed releases for is
/// compared with them again. One it listed none for is looked up again by
/// the next check, unless it runs a development build.
fn evaluated(entry: &Entry, installed: &InstalledFirmware) -> CatalogState {
    match (&entry.catalog, &entry.listed) {
        (CatalogState::Checked { .. }, Some(listed)) => {
            outcome(fw::check_for_update(
                installed.clone(),
                &Held(listed.clone()),
            ))
            .0
        }
        (CatalogState::Checked { .. }, None) if installed.is_development => CatalogState::Checked {
            status: UpdateStatus::Development,
            releases: Vec::new(),
        },
        (CatalogState::Checked { .. }, None) => CatalogState::Unchecked,
        (other, _) => other.clone(),
    }
}

/// The step an upgrade failed at, given the step `flash` had reached as it
/// failed with `error`. An image the device cannot take fails the check,
/// whatever was sent.
fn step_of(reached: Step, error: &FirmwareError) -> Step {
    match error {
        FirmwareError::Refused(_) => Step::Check,
        _ => reached,
    }
}

/// The firmware of every device of the hub, and the task that runs the
/// upgrades the pages ask for.
pub(crate) struct Firmware {
    devices: Arc<Devices>,
    tree: DeviceTree,
    runtime: tokio::runtime::Handle,
    catalog: Catalog,
    state: Mutex<State>,
}

impl Firmware {
    /// The firmware of the devices `tree` sees, looked up in `catalog`, told
    /// through `devices`, its tasks run on `runtime`.
    pub(crate) fn new(
        devices: Arc<Devices>,
        tree: &DeviceTree,
        runtime: &tokio::runtime::Handle,
        catalog: Catalog,
    ) -> Arc<Firmware> {
        Arc::new(Firmware {
            devices,
            tree: tree.clone(),
            runtime: runtime.clone(),
            catalog,
            state: Mutex::new(State::default()),
        })
    }

    /// Reads what the device at `route` runs, as its table loads.
    pub(crate) fn read_installed(self: &Arc<Self>, device: twinleaf::Device, route: DeviceRoute) {
        let firmware = self.clone();
        self.runtime.spawn(async move {
            let read = tokio::task::spawn_blocking(move || fw::query_installed(&device)).await;
            match read {
                Ok(Ok(installed)) => firmware.installed(route, installed),
                Ok(Err(error)) => {
                    log::debug!("The hub could not read dev.desc of {route}: {error}")
                }
                Err(_) => {}
            }
        });
    }

    /// Keeps `installed` as what the device at `route` runs, and compares it
    /// with the releases the hub holds for it.
    fn installed(&self, route: DeviceRoute, installed: InstalledFirmware) {
        let mut state = self.state.lock().unwrap();
        let entry = state.entries.entry(route).or_default();
        entry.catalog = evaluated(entry, &installed);
        entry.installed = Some(installed);
        self.tell_firmware(route, entry);
    }

    /// Tells every page what the device at `route` runs and what the catalog
    /// holds for it, unless that is what they were last told.
    fn tell_firmware(&self, route: DeviceRoute, entry: &mut Entry) {
        let installed = entry.installed.as_ref().map(info);
        let told = Some((installed.clone(), entry.catalog.clone()));
        if entry.told == told {
            return;
        }
        entry.told = told;
        let message = Message::Firmware {
            route: route.to_string(),
            installed,
            catalog: entry.catalog.clone(),
        };
        self.devices.publish(route, Kind::Firmware, message);
    }

    /// Tells every page that the upgrade of the device at `route` stands as
    /// `state`, unless that is what they were last told.
    fn tell_upgrade(&self, route: DeviceRoute, entry: &mut Entry, state: UpgradeState) {
        if entry.upgrade.as_ref() == Some(&state) {
            return;
        }
        entry.upgrade = Some(state.clone());
        let message = Message::Upgrade {
            route: route.to_string(),
            state,
        };
        self.devices.publish(route, Kind::Upgrade, message);
    }

    /// Tells `state` of the upgrade `job` of the device at `route`, unless
    /// another upgrade replaced it. Returns whether it is still the one.
    fn tell(&self, route: DeviceRoute, job: u64, state: UpgradeState) -> bool {
        let mut guard = self.state.lock().unwrap();
        let entry = guard.entries.entry(route).or_default();
        if entry.job != job {
            return false;
        }
        self.tell_upgrade(route, entry, state);
        true
    }

    /// Looks every listed device whose firmware the hub knows up in the
    /// catalog, unless a check runs already.
    pub(crate) fn check(self: &Arc<Self>) {
        let asked: Vec<(DeviceRoute, InstalledFirmware)> = {
            let mut state = self.state.lock().unwrap();
            if state.checking {
                return;
            }
            let listed = self.devices.listed();
            let asked: Vec<_> = state
                .entries
                .iter()
                .filter(|(route, _)| listed.contains(route))
                .filter_map(|(route, entry)| Some((*route, entry.installed.clone()?)))
                .collect();
            if asked.is_empty() {
                return;
            }
            state.checking = true;
            for (route, _) in &asked {
                let entry = state
                    .entries
                    .get_mut(route)
                    .expect("an asked device has an entry");
                entry.catalog = CatalogState::Checking;
                self.tell_firmware(*route, entry);
            }
            asked
        };
        log::info!(
            "Looking up the firmware of {} devices in the catalog",
            asked.len()
        );
        let firmware = self.clone();
        self.runtime.spawn(async move {
            for (route, installed) in asked {
                let catalog = firmware.catalog.catalog.clone();
                let checked =
                    tokio::task::spawn_blocking(move || fw::check_for_update(installed, &*catalog))
                        .await
                        .unwrap_or_else(|error| Err(FirmwareError::Catalog(error.to_string())));
                firmware.checked(route, checked);
            }
            firmware.state.lock().unwrap().checking = false;
        });
    }

    /// Keeps what the check of the device at `route` found.
    fn checked(&self, route: DeviceRoute, checked: Result<UpdateReport, FirmwareError>) {
        let (catalog, listed) = outcome(checked);
        match &catalog {
            CatalogState::Unreachable { error } => {
                log::warn!("The firmware catalog could not be listed for {route}: {error}")
            }
            CatalogState::Checked { status, releases } => {
                let stands = match status {
                    UpdateStatus::Update => "the newest newer than what it runs",
                    UpdateStatus::Current => "none newer than what it runs",
                    UpdateStatus::None => "none published for it",
                    UpdateStatus::Development => "none for the development build it runs",
                    UpdateStatus::Unknown => "and what it runs has no date to compare",
                };
                log::info!(
                    "The firmware catalog lists {} releases for {route}, {stands}",
                    releases.len()
                )
            }
            _ => {}
        }
        let mut state = self.state.lock().unwrap();
        let entry = state.entries.entry(route).or_default();
        let development = matches!(
            catalog,
            CatalogState::Checked {
                status: UpdateStatus::Development,
                ..
            }
        );
        if !development {
            entry.listed = listed;
        }
        entry.catalog = catalog;
        self.tell_firmware(route, entry);
    }

    /// The listed device `route` names, or why there is none.
    fn device(&self, route: &str) -> Result<DeviceRoute, String> {
        route
            .parse::<DeviceRoute>()
            .ok()
            .filter(|parsed| self.devices.listed().contains(parsed))
            .ok_or_else(|| format!("there is no device at {route}"))
    }

    /// The listed device `route` names, when an upgrade may be asked of it.
    fn admit(&self, state: &State, route: &str) -> Result<DeviceRoute, String> {
        let device = self.device(route)?;
        if state
            .entries
            .get(&device)
            .is_some_and(|entry| runs(&entry.upgrade))
        {
            return Err(format!("an upgrade of {device} is already running"));
        }
        Ok(device)
    }

    /// Whether an upgrade may be asked of the device at `route` now, before
    /// a page sends the file it installs.
    pub(crate) fn may_upgrade(&self, route: &str) -> Result<(), String> {
        let state = self.state.lock().unwrap();
        self.admit(&state, route).map(|_| ())
    }

    /// The release of `file` the last check listed for `device`, or the
    /// newest for `None`.
    fn release(
        state: &State,
        device: DeviceRoute,
        file: Option<&str>,
    ) -> Result<FirmwareRelease, String> {
        let missing = || format!("the catalog lists no such firmware for {device}");
        let Some(Entry {
            catalog: CatalogState::Checked { releases, .. },
            listed: Some(listed),
            ..
        }) = state.entries.get(&device)
        else {
            return Err(missing());
        };
        let file = match file {
            Some(file) => file,
            None => &releases.first().ok_or_else(missing)?.file,
        };
        if !releases.iter().any(|release| release.file == file) {
            return Err(missing());
        }
        listed
            .iter()
            .find(|release| release.filename == file)
            .cloned()
            .ok_or_else(missing)
    }

    /// Asks for an upgrade of the device at `route` that installs `wanted`,
    /// after the upgrades asked before it.
    pub(crate) fn upgrade(self: &Arc<Self>, route: &str, wanted: Wanted) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        let device = self.admit(&state, route)?;
        let image = match wanted {
            Wanted::Latest => Image::Release {
                release: Firmware::release(&state, device, None)?,
                latest: true,
            },
            Wanted::Release(file) => Image::Release {
                release: Firmware::release(&state, device, Some(&file))?,
                latest: false,
            },
            Wanted::File { name, bytes } => {
                bounded(&name, bytes.len() as u64)?;
                Image::File {
                    name,
                    bytes: Arc::new(bytes),
                }
            }
        };
        self.enqueue(&mut state, device, image);
        Ok(())
    }

    /// Queues an upgrade of `device` that installs `image`, and starts the
    /// task that runs the queue when none runs.
    fn enqueue(self: &Arc<Self>, state: &mut State, device: DeviceRoute, image: Image) {
        let entry = state.entries.entry(device).or_default();
        entry.job += 1;
        let job = entry.job;
        let (source, target) = (image.source(), image.target());
        log::info!("An upgrade of {device} to {target} is queued");
        entry.image = Some(image);
        self.tell_upgrade(device, entry, UpgradeState::Queued { source, target });
        state.queue.push_back((device, job));
        if !state.running {
            state.running = true;
            self.runtime.spawn(self.clone().run());
        }
    }

    /// Takes back the upgrade of the device at `route` that waits or
    /// downloads, or clears one that is done or failed. One that is on its
    /// way to the device is refused, and a device with none is left as it is.
    pub(crate) fn cancel(&self, route: &str) -> Result<(), String> {
        let device = self.device(route)?;
        let mut guard = self.state.lock().unwrap();
        let State { entries, queue, .. } = &mut *guard;
        let Some(entry) = entries.get_mut(&device) else {
            return Ok(());
        };
        let stage = match &entry.upgrade {
            None | Some(UpgradeState::Idle) => return Ok(()),
            Some(UpgradeState::Uploading { .. }) => Some("uploading"),
            Some(UpgradeState::Finalizing { .. }) => Some("finalizing"),
            Some(UpgradeState::Restarting { .. }) => Some("restarting"),
            Some(_) => None,
        };
        if let Some(stage) = stage {
            return Err(format!(
                "the upgrade of {device} is {stage} and cannot be cancelled"
            ));
        }
        if runs(&entry.upgrade) {
            log::info!("The upgrade of {device} is cancelled");
        }
        entry.job += 1;
        entry.image = None;
        if let Some(cancel) = entry.cancel.take() {
            let _ = cancel.send(true);
        }
        queue.retain(|(queued, _)| *queued != device);
        self.tell_upgrade(device, entry, UpgradeState::Idle);
        Ok(())
    }

    /// Runs the failed upgrade of the device at `route` again, from the
    /// same source, after the upgrades asked before it.
    pub(crate) fn retry(self: &Arc<Self>, route: &str) -> Result<(), String> {
        let device = self.device(route)?;
        let mut state = self.state.lock().unwrap();
        let image = state
            .entries
            .get(&device)
            .filter(|entry| matches!(entry.upgrade, Some(UpgradeState::Failed { .. })))
            .and_then(|entry| entry.image.clone())
            .ok_or_else(|| format!("no upgrade of {device} has failed"))?;
        self.enqueue(&mut state, device, image);
        Ok(())
    }

    /// Whether an upgrade waits or runs.
    pub(crate) fn upgrading(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.active.is_some() || !state.queue.is_empty()
    }

    /// Runs the upgrades in the order asked, one at a time, until none
    /// waits.
    async fn run(self: Arc<Self>) {
        loop {
            let (device, job, image, cancelled) = {
                let mut guard = self.state.lock().unwrap();
                let state = &mut *guard;
                state.active = None;
                loop {
                    let Some((device, job)) = state.queue.pop_front() else {
                        state.running = false;
                        return;
                    };
                    let entry = state.entries.entry(device).or_default();
                    let Some(image) = entry.image.clone().filter(|_| entry.job == job) else {
                        continue;
                    };
                    let (cancel, cancelled) = watch::channel(false);
                    entry.cancel = Some(cancel);
                    state.active = Some(device);
                    break (device, job, image, cancelled);
                }
            };
            self.run_one(device, job, image, cancelled).await;
            let mut state = self.state.lock().unwrap();
            let entry = state.entries.entry(device).or_default();
            if entry.job == job {
                entry.cancel = None;
            }
        }
    }

    /// Runs the upgrade `job` of `device`, which installs `image`, until it
    /// is done, failed, or taken back as `cancelled` says.
    async fn run_one(
        self: &Arc<Self>,
        device: DeviceRoute,
        job: u64,
        image: Image,
        mut cancelled: watch::Receiver<bool>,
    ) {
        let (source, target) = (image.source(), image.target());
        log::info!("Upgrading {device} to {target}");
        let failed = |step: Step, error: String| {
            let at = format!("{step:?}").to_lowercase();
            log::warn!("The upgrade of {device} to {target} failed at the {at} step: {error}");
            let state = UpgradeState::Failed {
                source,
                target: target.clone(),
                step,
                error,
            };
            self.tell(device, job, state);
        };

        let bytes = match &image {
            Image::File { bytes, .. } => bytes.clone(),
            Image::Release { release, .. } => {
                let downloading = UpgradeState::Downloading {
                    source,
                    target: target.clone(),
                };
                if !self.tell(device, job, downloading) {
                    return;
                }
                log::info!("Downloading {} for {device}", release.filename);
                let (catalog, release) = (self.catalog.clone(), release.clone());
                let download = tokio::task::spawn_blocking(move || catalog.download(&release));
                let downloaded = tokio::select! {
                    downloaded = download => downloaded,
                    _ = cancelled.wait_for(|cancelled| *cancelled) => {
                        log::info!("The download for {device} is discarded as it comes, as its upgrade was cancelled");
                        return;
                    }
                };
                match downloaded
                    .unwrap_or_else(|error| Err(FirmwareError::Catalog(error.to_string())))
                {
                    Ok(bytes) => {
                        log::info!("Downloaded {} bytes of {target} for {device}", bytes.len());
                        Arc::new(bytes)
                    }
                    Err(error) => return failed(Step::Download, error.to_string()),
                }
            }
        };

        let size = bytes.len() as u64;
        let uploading = move |target: String, sent: u64| UpgradeState::Uploading {
            source,
            target,
            sent,
            size,
        };
        // From here on the image is on its way to the device, and an upgrade
        // that was cancelled sends nothing.
        if !self.tell(device, job, uploading(target.clone(), 0)) {
            return;
        }
        let flash = match self.tree.device(device) {
            Ok(flash) => flash,
            Err(error) => return failed(Step::Check, error.to_string()),
        };
        let firmware = self.clone();
        let flashing = target.clone();
        let flashed = tokio::task::spawn_blocking(move || {
            let mut reached = Step::Check;
            let mut told: Option<Instant> = None;
            let result = fw::flash(&flash, &bytes, |event| match event {
                FlashEvent::Stopping => {
                    reached = Step::Upload;
                    log::info!("Uploading {flashing} to {device}");
                }
                FlashEvent::Uploading { chunk, total } => {
                    let last = chunk == total;
                    if last || told.is_none_or(|told| told.elapsed() >= PROGRESS_EVERY) {
                        told = Some(Instant::now());
                        let sent = size * chunk as u64 / total.max(1) as u64;
                        firmware.tell(device, job, uploading(flashing.clone(), sent));
                    }
                }
                FlashEvent::Resuming {
                    chunk,
                    total,
                    error,
                } => {
                    log::warn!(
                        "The upload to {device} resumes at chunk {chunk} of {total}: {error}"
                    );
                }
                FlashEvent::Committing => {
                    reached = Step::Finalize;
                    log::info!("{device} commits {flashing}");
                    let state = UpgradeState::Finalizing {
                        source,
                        target: flashing.clone(),
                    };
                    firmware.tell(device, job, state);
                }
                _ => {}
            });
            (result, reached)
        })
        .await;
        match flashed {
            Ok((Ok(()), _)) => {}
            Ok((Err(error), reached)) => {
                return failed(step_of(reached, &error), error.to_string())
            }
            Err(error) => return failed(Step::Upload, error.to_string()),
        }

        log::info!("{device} restarts into {target}");
        let restarting = UpgradeState::Restarting {
            source,
            target: target.clone(),
        };
        self.tell(device, job, restarting);
        let installed = match self.tree.device(device) {
            Ok(reread) => tokio::task::spawn_blocking(move || fw::reread_installed(&reread))
                .await
                .ok()
                .flatten(),
            Err(_) => None,
        };
        match &installed {
            Some(installed) => log::info!(
                "The upgrade of {device} to {target} is done, and it runs {}",
                installed.description
            ),
            None => log::warn!(
                "The upgrade of {device} to {target} is done, and it did not say what it runs in time"
            ),
        }
        let mut state = self.state.lock().unwrap();
        let entry = state.entries.entry(device).or_default();
        if entry.job != job {
            return;
        }
        entry.image = None;
        let done = UpgradeState::Done {
            source,
            target,
            installed: installed.as_ref().map(info),
        };
        self.tell_upgrade(device, entry, done);
        if let Some(installed) = installed {
            entry.catalog = evaluated(entry, &installed);
            entry.installed = Some(installed);
            self.tell_firmware(device, entry);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::hub::Live;
    use crate::protocol::Frame;
    use serde_json::{json, Value};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use tokio::sync::broadcast;
    use twinleaf::proxy::{HostOptions, Layout, Mount, Proxy};
    use twinleaf::Connection;

    /// How long a test waits for what must come, on a machine however busy.
    /// An upgrade of the simulated device takes over five seconds, as the
    /// device settles for five after its commit.
    const WAIT: Duration = Duration::from_secs(90);
    /// How long a test waits to see that nothing comes.
    const QUIET: Duration = Duration::from_millis(1500);
    /// A build older than the newest release the tests' catalogs list.
    pub(crate) const OLD_BUILD: &str = "2026-01-10/aaaaaa";
    /// What a simulated device of [`OLD_BUILD`] says it runs.
    const OLD_DESC: &str = "Twinleaf tio-test R1 ((null)) [2026-01-10/aaaaaa]";
    /// What a simulated device says it runs after an upgrade.
    const UPGRADED_DESC: &str = "Twinleaf tio-test R1 ((null)) [2026-06-08/000002]";
    /// The bytes of the image the tests' catalogs serve.
    const IMAGE: usize = 20_000;

    /// An image a simulated device that takes images takes, as it takes
    /// any bytes that are no signed package.
    fn image(len: usize) -> Vec<u8> {
        vec![0x5a; len]
    }

    /// A signed package around an image of `len` bytes, in the layout
    /// `flash` tells from an image.
    fn package(len: usize) -> Vec<u8> {
        let mut data = vec![0u8; 288];
        data[8..10].copy_from_slice(&1u16.to_le_bytes());
        data[10..12].copy_from_slice(&288u16.to_le_bytes());
        data[24..28].copy_from_slice(&(len as u32).to_le_bytes());
        data.extend(std::iter::repeat_n(0xa5, len));
        data
    }

    /// The release of the simulated device built on `date` as `hash`.
    pub(crate) fn release(date: &str, hash: &str) -> FirmwareRelease {
        let file = file(date, hash);
        FirmwareRelease::parse_filename("tio-test", "R1", &file, &file).unwrap()
    }

    fn file(date: &str, hash: &str) -> String {
        format!("tio-test-R1-firmware-{date}-{hash}.bin")
    }

    /// A catalog of the test's own, which lists what the test says, serves
    /// [`IMAGE`] bytes for every release, and fails or holds a listing or a
    /// download when the test says.
    pub(crate) struct Fake {
        listing: Mutex<Result<Vec<FirmwareRelease>, String>>,
        listings: AtomicUsize,
        hold_listing: Mutex<Option<mpsc::Receiver<()>>>,
        hold_download: Mutex<Option<mpsc::Receiver<()>>>,
        /// How many downloads fail before one succeeds.
        failures: Mutex<usize>,
        downloaded: Mutex<Vec<String>>,
    }

    impl Fake {
        pub(crate) fn listing(releases: Vec<FirmwareRelease>) -> Arc<Fake> {
            Arc::new(Fake {
                listing: Mutex::new(Ok(releases)),
                listings: AtomicUsize::new(0),
                hold_listing: Mutex::new(None),
                hold_download: Mutex::new(None),
                failures: Mutex::new(0),
                downloaded: Mutex::new(Vec::new()),
            })
        }

        fn list(&self, listing: Result<Vec<FirmwareRelease>, String>) {
            *self.listing.lock().unwrap() = listing;
        }

        /// Holds the next listing until the sender returned sends or goes.
        fn hold_listing(&self) -> mpsc::Sender<()> {
            let (release, held) = mpsc::channel();
            *self.hold_listing.lock().unwrap() = Some(held);
            release
        }

        /// Holds the next download until the sender returned sends or goes.
        pub(crate) fn hold_download(&self) -> mpsc::Sender<()> {
            let (release, held) = mpsc::channel();
            *self.hold_download.lock().unwrap() = Some(held);
            release
        }

        fn downloaded(&self) -> Vec<String> {
            self.downloaded.lock().unwrap().clone()
        }
    }

    impl FirmwareCatalog for Fake {
        fn list_releases(&self, _: &str, _: &str) -> Result<Vec<FirmwareRelease>, FirmwareError> {
            self.listings.fetch_add(1, Ordering::Relaxed);
            let held = self.hold_listing.lock().unwrap().take();
            if let Some(held) = held {
                let _ = held.recv();
            }
            self.listing
                .lock()
                .unwrap()
                .clone()
                .map_err(FirmwareError::Catalog)
        }

        fn download(&self, release: &FirmwareRelease) -> Result<Vec<u8>, FirmwareError> {
            let held = self.hold_download.lock().unwrap().take();
            if let Some(held) = held {
                let _ = held.recv();
            }
            self.downloaded
                .lock()
                .unwrap()
                .push(release.filename.clone());
            let mut failures = self.failures.lock().unwrap();
            match failures.checked_sub(1) {
                Some(left) => {
                    *failures = left;
                    Err(FirmwareError::Catalog("the test's catalog failed".into()))
                }
                None => Ok(image(IMAGE)),
            }
        }
    }

    /// The simulated devices behind a proxy of the test's own, and a hub
    /// over the proxy's connection to them, which looks firmware up in the
    /// test's catalog.
    pub(crate) struct Bench {
        stop: Arc<AtomicBool>,
        proxy: Proxy,
        connection: Connection,
        pub(crate) live: Arc<Live>,
        pub(crate) catalog: Arc<Fake>,
        /// Every change the hub sent since it started.
        changes: broadcast::Receiver<Arc<Frame>>,
    }

    impl Drop for Bench {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            self.live.retire();
            self.proxy.shutdown();
        }
    }

    /// The messages of `frames`.
    fn messages(frames: &[Frame]) -> Vec<Value> {
        frames
            .iter()
            .filter_map(|frame| match frame {
                Frame::Text(text) => serde_json::from_str(text).ok(),
                Frame::Binary(_) => None,
            })
            .collect()
    }

    impl Bench {
        /// Starts the simulator `args` start, behind a hub whose catalog is
        /// `catalog` and whose downloads are kept in `cache`, and waits
        /// until the hub knows what each device at `routes` runs.
        pub(crate) async fn start(
            args: &[&str],
            catalog: Arc<Fake>,
            cache: Option<PathBuf>,
            routes: &[&str],
        ) -> Bench {
            let stop = Arc::new(AtomicBool::new(false));
            let args: Vec<&str> = ["--no-drop"].iter().chain(args).copied().collect();
            let url = twinleaf_tools::tools::simulate::serve(&args, stop.clone());
            let root = DeviceRoute::root();
            let proxy = Proxy::host(HostOptions {
                port: 0,
                name: "a test".to_string(),
                default: Some(Layout {
                    mounts: vec![Mount {
                        url,
                        source: root,
                        prefix: root,
                    }],
                }),
                ..HostOptions::default()
            })
            .unwrap();
            let connection = Connection::open(&proxy.endpoint()).unwrap();
            let runtime = tokio::runtime::Handle::current();
            let source = Catalog::new(catalog.clone(), cache);
            let live = Live::with_catalog(connection.clone(), None, &runtime, source).unwrap();
            let live = Arc::new(live);
            let (snapshot, changes) = live.snapshot(0).await.unwrap();
            let mut bench = Bench {
                stop,
                proxy,
                connection,
                live,
                catalog,
                changes,
            };
            let mut known: BTreeSet<String> = messages(&snapshot)
                .into_iter()
                .filter(|message| message["type"] == "firmware" && !message["installed"].is_null())
                .map(|message| message["route"].as_str().unwrap().to_string())
                .collect();
            while !routes.iter().all(|route| known.contains(*route)) {
                let message = bench.next("firmware").await;
                if !message["installed"].is_null() {
                    known.insert(message["route"].as_str().unwrap().to_string());
                }
            }
            bench
        }

        /// The next change that is a message of type `kind`.
        async fn next(&mut self, kind: &str) -> Value {
            let found = async {
                loop {
                    match self.changes.recv().await {
                        Ok(frame) => {
                            let Frame::Text(text) = &*frame else { continue };
                            let message: Value = serde_json::from_str(text).unwrap();
                            if message["type"] == kind {
                                return message;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => panic!("the hub ended"),
                    }
                }
            };
            match tokio::time::timeout(WAIT, found).await {
                Ok(message) => message,
                Err(_) => panic!("no {kind} message in {WAIT:?}"),
            }
        }

        /// Every upgrade state the hub tells of the device at `route`, up to
        /// the first that `last` holds of.
        pub(crate) async fn upgrades(
            &mut self,
            route: &str,
            last: impl Fn(&Value) -> bool,
        ) -> Vec<Value> {
            let mut states = Vec::new();
            loop {
                let state = self.next("upgrade").await;
                if state["route"] != route {
                    continue;
                }
                states.push(state);
                if last(&states[states.len() - 1]) {
                    return states;
                }
            }
        }

        /// The next firmware message of the device at `route` whose catalog
        /// stands as `catalog`.
        pub(crate) async fn catalog(&mut self, route: &str, catalog: &str) -> Value {
            loop {
                let message = self.next("firmware").await;
                if message["route"] == route && message["catalog"]["state"] == catalog {
                    return message;
                }
            }
        }

        /// The newest message of type `kind` of the device at `route` that a
        /// snapshot holds.
        async fn newest(&self, kind: &str, route: &str) -> Option<Value> {
            let (snapshot, _) = self.live.snapshot(0).await.unwrap();
            messages(&snapshot)
                .into_iter()
                .find(|message| message["type"] == kind && message["route"] == route)
        }

        /// The messages of type `kind` the hub tells in the next [`QUIET`].
        async fn told_in_quiet(&mut self, kind: &str) -> Vec<Value> {
            let mut told = Vec::new();
            let end = tokio::time::Instant::now() + QUIET;
            loop {
                match tokio::time::timeout_at(end, self.changes.recv()).await {
                    Ok(Ok(frame)) => {
                        let Frame::Text(text) = &*frame else { continue };
                        let message: Value = serde_json::from_str(text).unwrap();
                        if message["type"] == kind {
                            told.push(message);
                        }
                    }
                    Ok(Err(_)) => {}
                    Err(_) => return told,
                }
            }
        }

        /// What the device at `route` says it runs, asked past the hub.
        async fn desc(&self, route: &str) -> String {
            let device = self.connection.device(route.parse().unwrap());
            tokio::task::spawn_blocking(move || device.get::<String>("dev.desc"))
                .await
                .unwrap()
                .unwrap()
        }
    }

    /// Whether an upgrade in `state` is done or failed.
    fn settled(state: &Value) -> bool {
        state["state"] == "done" || state["state"] == "failed"
    }

    /// The states of `states` in order, a run of `uploading` as one.
    fn stages(states: &[Value]) -> Vec<&str> {
        let mut stages: Vec<&str> = states
            .iter()
            .map(|state| state["state"].as_str().unwrap())
            .collect();
        stages.dedup();
        stages
    }

    /// The bytes each `uploading` state of `states` says were sent, which
    /// start at none and come to the size each says, never going back.
    fn assert_uploads(states: &[Value], size: usize) {
        let sent: Vec<u64> = states
            .iter()
            .filter(|state| state["state"] == "uploading")
            .map(|state| {
                assert_eq!(state["size"], size);
                state["sent"].as_u64().unwrap()
            })
            .collect();
        assert_eq!(sent.first(), Some(&0), "{sent:?}");
        assert_eq!(sent.last(), Some(&(size as u64)), "{sent:?}");
        assert!(sent.windows(2).all(|pair| pair[0] <= pair[1]), "{sent:?}");
    }

    /// The releases of the tests' catalogs, in no order.
    fn releases() -> Vec<FirmwareRelease> {
        vec![
            release("2026-03-17", "5d1494"),
            release("2026-06-08", "000002"),
            release("2025-12-31", "bbbbbb"),
        ]
    }

    /// The hub knows what a device runs once its table loads, and looks it
    /// up in the catalog when a page asks. An upgrade to the newest release
    /// is downloaded into the cache and flashed, and tells each step in
    /// order. The device then runs that release, which the hub finds is the
    /// newest without the catalog.
    #[tokio::test]
    async fn the_newest_release_is_installed_and_then_found_current() {
        let cache =
            std::env::temp_dir().join(format!("twinleaf-web-firmware-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cache);
        let catalog = Fake::listing(releases());
        let args = ["--build", OLD_BUILD];
        let mut bench = Bench::start(&args, catalog, Some(cache.clone()), &["/"]).await;
        let page = bench.live.page();

        let known = bench.newest("firmware", "/").await.unwrap();
        assert_eq!(
            known,
            json!({"type": "firmware", "route": "/",
                "installed": {"name": "tio-test", "revision": "R1", "date": "2026-01-10",
                    "build": "aaaaaa", "serial": null, "development": false},
                "catalog": {"state": "unchecked"}})
        );
        page.check();
        bench.catalog("/", "checking").await;
        let checked = bench.catalog("/", "checked").await;
        let listed =
            |date: &str, hash: &str| json!({"date": date, "build": hash, "file": file(date, hash)});
        assert_eq!(
            checked["catalog"],
            json!({"state": "checked", "status": "update", "releases": [
                listed("2026-06-08", "000002"),
                listed("2026-03-17", "5d1494"),
                listed("2025-12-31", "bbbbbb"),
            ]})
        );

        page.upgrade("/", Wanted::Latest).unwrap();
        let states = bench.upgrades("/", settled).await;
        assert_eq!(
            stages(&states),
            [
                "queued",
                "downloading",
                "uploading",
                "finalizing",
                "restarting",
                "done"
            ],
            "{states:?}"
        );
        assert!(states
            .iter()
            .all(|state| state["source"] == "latest" && state["target"] == "2026-06-08 000002"));
        assert_uploads(&states, IMAGE);
        let done = &states[states.len() - 1];
        assert_eq!(done["installed"]["date"], "2026-06-08");
        assert_eq!(done["installed"]["build"], "000002");

        let firmware = bench.newest("firmware", "/").await.unwrap();
        assert_eq!(firmware["installed"], done["installed"]);
        assert_eq!(firmware["catalog"]["status"], "current");
        assert_eq!(bench.desc("/").await, UPGRADED_DESC);
        let name = file("2026-06-08", "000002");
        assert_eq!(bench.catalog.downloaded(), std::slice::from_ref(&name));
        assert!(cache.join("tio-test").join("R1").join(&name).is_file());
        drop(bench);
        let _ = std::fs::remove_dir_all(&cache);
    }

    /// A page may install any release the last check listed, an older one
    /// too, and no release it did not list.
    #[tokio::test]
    async fn a_release_the_page_chose_is_installed() {
        let catalog = Fake::listing(releases());
        let mut bench = Bench::start(&["--build", OLD_BUILD], catalog, None, &["/"]).await;
        let page = bench.live.page();
        let missing = "the catalog lists no such firmware for /".to_string();
        assert_eq!(page.upgrade("/", Wanted::Latest), Err(missing.clone()));

        page.check();
        bench.catalog("/", "checked").await;
        let other = Wanted::Release(file("2099-01-01", "ffffff"));
        assert_eq!(page.upgrade("/", other), Err(missing));
        let older = file("2025-12-31", "bbbbbb");
        page.upgrade("/", Wanted::Release(older.clone())).unwrap();
        let states = bench.upgrades("/", settled).await;
        assert_eq!(
            stages(&states),
            [
                "queued",
                "downloading",
                "uploading",
                "finalizing",
                "restarting",
                "done"
            ],
            "{states:?}"
        );
        assert!(states
            .iter()
            .all(|state| state["source"] == "release" && state["target"] == "2025-12-31 bbbbbb"));
        assert_eq!(bench.catalog.downloaded(), [older]);
    }

    /// A file a page sent is flashed with no download. Once it is on its
    /// way to the device it cannot be taken back, and once it is done there
    /// is nothing to run again, and taking it back clears it.
    #[tokio::test]
    async fn a_file_the_page_sent_is_installed() {
        let mut bench = Bench::start(&[], Fake::listing(Vec::new()), None, &["/"]).await;
        let page = bench.live.page();
        let bytes = image(IMAGE);
        let file = || Wanted::File {
            name: "image.bin".into(),
            bytes: bytes.clone(),
        };
        page.upgrade("/", file()).unwrap();
        let finalizing = bench
            .upgrades("/", |state| state["state"] == "finalizing")
            .await;
        assert_eq!(
            page.cancel("/"),
            Err("the upgrade of / is finalizing and cannot be cancelled".into())
        );
        let rest = bench.upgrades("/", settled).await;
        let states = [finalizing, rest].concat();
        assert_eq!(
            stages(&states),
            ["queued", "uploading", "finalizing", "restarting", "done"],
            "{states:?}"
        );
        assert!(states
            .iter()
            .all(|state| state["source"] == "file" && state["target"] == "image.bin"));
        assert_uploads(&states, IMAGE);
        assert_eq!(bench.desc("/").await, UPGRADED_DESC);

        assert_eq!(page.retry("/"), Err("no upgrade of / has failed".into()));
        page.cancel("/").unwrap();
        let idle = bench.next("upgrade").await;
        assert_eq!(
            (&idle["state"], &idle["source"]),
            (&json!("idle"), &Value::Null)
        );
        assert_eq!(bench.catalog.listings.load(Ordering::Relaxed), 0);
        assert!(bench.catalog.downloaded().is_empty());
    }

    /// An upgrade taken back while it downloads stands idle at once, and
    /// the image that comes after is discarded. Nothing reaches the device,
    /// and no page hears of an upload.
    #[tokio::test]
    async fn an_upgrade_cancelled_while_it_downloads_sends_the_device_nothing() {
        let catalog = Fake::listing(releases());
        let mut bench = Bench::start(&["--build", OLD_BUILD], catalog, None, &["/"]).await;
        let page = bench.live.page();
        page.check();
        bench.catalog("/", "checked").await;

        let held = bench.catalog.hold_download();
        page.upgrade("/", Wanted::Latest).unwrap();
        bench
            .upgrades("/", |state| state["state"] == "downloading")
            .await;
        page.cancel("/").unwrap();
        let idle = bench.next("upgrade").await;
        assert_eq!(idle["state"], "idle");
        held.send(()).unwrap();
        let downloaded = async {
            while bench.catalog.downloaded().is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(WAIT, downloaded).await.unwrap();
        assert_eq!(bench.told_in_quiet("upgrade").await, Vec::<Value>::new());
        assert!(!bench.live.upgrading());
        assert_eq!(bench.desc("/").await, OLD_DESC);
    }

    /// A signed package fails the check on a device that takes images, and
    /// nothing is stopped or sent.
    #[tokio::test]
    async fn a_package_to_a_device_that_takes_images_fails_at_the_check() {
        let mut bench = Bench::start(&[], Fake::listing(Vec::new()), None, &["/"]).await;
        let page = bench.live.page();
        let wanted = Wanted::File {
            name: "package.bin".into(),
            bytes: package(1000),
        };
        page.upgrade("/", wanted).unwrap();
        let states = bench.upgrades("/", settled).await;
        assert_eq!(
            stages(&states),
            ["queued", "uploading", "failed"],
            "{states:?}"
        );
        let failed = &states[states.len() - 1];
        assert_eq!(failed["step"], "check");
        assert_eq!(
            failed["error"],
            "the device cannot take this image: this is a signed package, and the device takes images"
        );
        assert!(bench.desc("/").await.ends_with("[2026-06-08/000001-DEV]"));
    }

    /// An image fails the check on a device that takes signed packages.
    #[tokio::test]
    async fn an_image_to_a_device_that_takes_packages_fails_at_the_check() {
        let args = ["--upload", "signed"];
        let mut bench = Bench::start(&args, Fake::listing(Vec::new()), None, &["/"]).await;
        let page = bench.live.page();
        let wanted = Wanted::File {
            name: "image.bin".into(),
            bytes: image(IMAGE),
        };
        page.upgrade("/", wanted).unwrap();
        let states = bench.upgrades("/", settled).await;
        assert_eq!(
            stages(&states),
            ["queued", "uploading", "failed"],
            "{states:?}"
        );
        let failed = &states[states.len() - 1];
        assert_eq!(failed["step"], "check");
        assert_eq!(
            failed["error"],
            "the device cannot take this image: the device takes signed packages, and this is an image"
        );
    }

    /// A device that stops answering while it takes an image fails the
    /// upgrade at the upload.
    #[tokio::test]
    async fn an_upload_the_device_stops_answering_fails_at_the_upload() {
        let mut bench = Bench::start(&[], Fake::listing(Vec::new()), None, &["/"]).await;
        let page = bench.live.page();
        let wanted = Wanted::File {
            name: "large.bin".into(),
            bytes: image(MAX_IMAGE as usize / 2),
        };
        page.upgrade("/", wanted).unwrap();
        bench
            .upgrades("/", |state| state["sent"].as_u64() > Some(0))
            .await;
        bench.stop.store(true, Ordering::Relaxed);
        let states = bench.upgrades("/", settled).await;
        let failed = &states[states.len() - 1];
        assert_eq!(failed["state"], "failed", "{states:?}");
        assert_eq!(failed["step"], "upload", "{failed}");
    }

    /// A download that fails fails the upgrade at the download, and any
    /// page may run it again, which installs the release.
    #[tokio::test]
    async fn a_failed_download_is_run_again_and_installs_the_release() {
        let catalog = Fake::listing(releases());
        *catalog.failures.lock().unwrap() = 1;
        let mut bench = Bench::start(&["--build", OLD_BUILD], catalog, None, &["/"]).await;
        let (asker, other) = (bench.live.page(), bench.live.page());
        asker.check();
        bench.catalog("/", "checked").await;

        assert_eq!(other.retry("/"), Err("no upgrade of / has failed".into()));
        asker.upgrade("/", Wanted::Latest).unwrap();
        let states = bench.upgrades("/", settled).await;
        assert_eq!(
            stages(&states),
            ["queued", "downloading", "failed"],
            "{states:?}"
        );
        let failed = &states[states.len() - 1];
        assert_eq!(failed["step"], "download");
        assert_eq!(
            failed["error"],
            "firmware catalog error: the test's catalog failed"
        );

        other.retry("/").unwrap();
        let again = bench.upgrades("/", settled).await;
        assert_eq!(
            stages(&again),
            [
                "queued",
                "downloading",
                "uploading",
                "finalizing",
                "restarting",
                "done"
            ],
            "{again:?}"
        );
        assert!(again
            .iter()
            .all(|state| state["source"] == "latest" && state["target"] == "2026-06-08 000002"));
        assert_eq!(bench.catalog.downloaded().len(), 2);
        assert_eq!(bench.desc("/").await, UPGRADED_DESC);
    }

    /// A page that connects while an upgrade runs finds where it stands in
    /// its snapshot, after the device's other messages. Every page hears
    /// each state, and a refused ask is the asker's alone, no change.
    #[tokio::test]
    async fn a_page_that_connects_during_an_upgrade_finds_it_in_its_snapshot() {
        let mut bench = Bench::start(&[], Fake::listing(Vec::new()), None, &["/"]).await;
        let (asker, other) = (bench.live.page(), bench.live.page());
        let (_, mut heard) = bench.live.snapshot(0).await.unwrap();
        let file = || Wanted::File {
            name: "image.bin".into(),
            bytes: image(IMAGE),
        };
        asker.upgrade("/", file()).unwrap();
        let mut told = bench
            .upgrades("/", |state| state["state"] == "finalizing")
            .await;

        let (snapshot, _) = bench.live.snapshot(0).await.unwrap();
        let snapshot = messages(&snapshot);
        let at = |kind: &str| {
            snapshot
                .iter()
                .position(|message| message["type"] == kind && message["route"] == "/")
                .unwrap_or_else(|| panic!("no {kind} in the snapshot"))
        };
        assert_eq!(snapshot[at("upgrade")]["state"], "finalizing");
        assert!(at("device") < at("firmware") && at("firmware") < at("upgrade"));
        assert_eq!(
            other.upgrade("/", file()),
            Err("an upgrade of / is already running".into())
        );
        told.extend(bench.upgrades("/", settled).await);

        let mut also = Vec::new();
        while let Ok(frame) = heard.try_recv() {
            if let Frame::Text(text) = &*frame {
                let message: Value = serde_json::from_str(text).unwrap();
                assert_ne!(message["type"], "refused");
                if message["type"] == "upgrade" {
                    also.push(message);
                }
            }
        }
        assert_eq!(also, told);
    }

    /// Upgrades run one at a time, in the order asked, and one waits while
    /// another runs. A device whose upgrade waits or runs takes no other,
    /// one that waits can be taken back, and a device there is not takes
    /// none.
    #[tokio::test]
    async fn upgrades_run_one_at_a_time_in_the_order_asked() {
        let routes = ["/0", "/1", "/2"];
        let args = ["--layout", "/0,/1,/2"];
        let mut bench = Bench::start(&args, Fake::listing(Vec::new()), None, &routes).await;
        let page = bench.live.page();
        let file = || Wanted::File {
            name: "image.bin".into(),
            bytes: image(IMAGE),
        };
        for route in routes {
            page.upgrade(route, file()).unwrap();
        }
        assert_eq!(
            page.upgrade("/0", file()),
            Err("an upgrade of /0 is already running".into())
        );
        assert_eq!(
            page.upgrade("/7", Wanted::Latest),
            Err("there is no device at /7".into())
        );
        page.cancel("/2").unwrap();

        let mut told = Vec::new();
        while !told
            .iter()
            .any(|state: &Value| state["route"] == "/1" && settled(state))
        {
            told.push(bench.next("upgrade").await);
        }
        told.extend(bench.told_in_quiet("upgrade").await);
        let of = |route: &str| -> Vec<Value> {
            told.iter()
                .filter(|state| state["route"] == route)
                .cloned()
                .collect()
        };
        let full = ["queued", "uploading", "finalizing", "restarting", "done"];
        assert_eq!(stages(&of("/0")), full);
        assert_eq!(stages(&of("/1")), full);
        assert_eq!(stages(&of("/2")), ["queued", "idle"]);
        let index = |route: &str, stage: &str| {
            told.iter()
                .position(|state| state["route"] == route && state["state"] == stage)
                .unwrap()
        };
        assert!(index("/0", "done") < index("/1", "uploading"));
        assert!(index("/1", "queued") < index("/0", "uploading"));
    }

    /// The catalog is looked up only when a page asks, and once for asks
    /// that come while it is. What it says of the device is unreachable,
    /// none, an update or current, as it lists.
    #[tokio::test]
    async fn the_catalog_is_looked_up_only_when_a_page_asks() {
        let catalog = Fake::listing(Vec::new());
        catalog.list(Err("offline".into()));
        let mut bench = Bench::start(&["--build", OLD_BUILD], catalog, None, &["/"]).await;
        let page = bench.live.page();
        let known = bench.newest("firmware", "/").await.unwrap();
        assert_eq!(known["catalog"], json!({"state": "unchecked"}));
        assert_eq!(bench.told_in_quiet("firmware").await, Vec::<Value>::new());
        assert_eq!(bench.catalog.listings.load(Ordering::Relaxed), 0);

        let held = bench.catalog.hold_listing();
        page.check();
        page.check();
        bench.catalog("/", "checking").await;
        drop(held);
        let unreachable = bench.catalog("/", "unreachable").await;
        assert_eq!(
            unreachable["catalog"]["error"],
            "firmware catalog error: offline"
        );
        assert_eq!(bench.catalog.listings.load(Ordering::Relaxed), 1);

        for (listing, status) in [
            (Vec::new(), "none"),
            (vec![release("2026-06-08", "000002")], "update"),
            (vec![release("2026-01-10", "bbbbbb")], "current"),
        ] {
            bench.catalog.list(Ok(listing));
            page.check();
            let checked = bench.catalog("/", "checked").await;
            assert_eq!(checked["catalog"]["status"], status, "{checked}");
        }
        assert_eq!(bench.catalog.listings.load(Ordering::Relaxed), 4);
    }

    /// A device that runs a development build is checked without the
    /// catalog, and the catalog offers it nothing.
    #[tokio::test]
    async fn a_development_build_is_never_looked_up() {
        let mut bench = Bench::start(&[], Fake::listing(releases()), None, &["/"]).await;
        let page = bench.live.page();
        page.check();
        let checked = bench.catalog("/", "checked").await;
        assert_eq!(
            checked["catalog"],
            json!({"state": "checked", "status": "development", "releases": []})
        );
        assert_eq!(checked["installed"]["development"], true);
        assert_eq!(bench.catalog.listings.load(Ordering::Relaxed), 0);
        assert_eq!(
            page.upgrade("/", Wanted::Latest),
            Err("the catalog lists no such firmware for /".into())
        );
    }

    /// The firmware a `dev.desc` of `desc` describes, in the form the
    /// simulated device gives.
    fn installed(desc: &str) -> InstalledFirmware {
        let header: Vec<&str> = desc.split(" (").next().unwrap().split(' ').collect();
        let inside = desc.rsplit('[').next().unwrap().trim_end_matches(']');
        let (date, build) = inside.split_once('/').unwrap();
        InstalledFirmware {
            name: header.get(1).copied().unwrap_or_default().to_string(),
            revision: header.get(2).copied().unwrap_or_default().to_string(),
            build_date: fw::FirmwareDate::parse(date),
            hash: Some(build.to_string()),
            serial: None,
            is_development: build.to_ascii_uppercase().contains("DEV"),
            description: desc.to_string(),
        }
    }

    /// What a check says of each answer the catalog gives. A device whose
    /// `dev.desc` names no sensor has no published firmware, and a catalog
    /// that cannot list is unreachable.
    #[test]
    fn a_check_says_what_the_catalog_answered() {
        let report = |desc: &str, listed: Vec<FirmwareRelease>| {
            fw::check_for_update(installed(desc), &Held(listed))
        };
        let (state, listed) = outcome(report(OLD_DESC, releases()));
        assert!(matches!(
            state,
            CatalogState::Checked {
                status: UpdateStatus::Update,
                ..
            }
        ));
        assert_eq!(listed.unwrap().len(), 3);

        let undated = "Twinleaf tio-test R1 ((null)) [unknown/aaaaaa]";
        let (state, _) = outcome(report(undated, releases()));
        assert!(matches!(
            state,
            CatalogState::Checked {
                status: UpdateStatus::Unknown,
                ..
            }
        ));

        let nameless = "HUB ((null)) [2026-01-10/aaaaaa]";
        let (state, listed) = outcome(report(nameless, releases()));
        assert_eq!(
            state,
            CatalogState::Checked {
                status: UpdateStatus::None,
                releases: Vec::new()
            }
        );
        assert!(listed.is_none());

        let (state, listed) = outcome(Err(FirmwareError::Catalog("offline".into())));
        assert_eq!(
            state,
            CatalogState::Unreachable {
                error: "firmware catalog error: offline".into()
            }
        );
        assert!(listed.is_none());

        let dev = "Twinleaf tio-test R1 ((null)) [2026-06-08/000001-DEV]";
        let (state, listed) = outcome(report(dev, releases()));
        assert!(matches!(
            state,
            CatalogState::Checked {
                status: UpdateStatus::Development,
                ..
            }
        ));
        assert!(listed.is_none());
    }

    /// Once a device runs other firmware, what the catalog holds for it is
    /// found again from the releases the hub holds. A device the catalog
    /// listed none for is looked up anew, unless it runs a development
    /// build, and a catalog not checked stays as it stood.
    #[test]
    fn other_firmware_is_compared_with_the_releases_the_hub_holds() {
        let mut entry = Entry {
            catalog: CatalogState::Checked {
                status: UpdateStatus::Update,
                releases: Vec::new(),
            },
            listed: Some(releases()),
            ..Entry::default()
        };
        let status = |state: CatalogState| match state {
            CatalogState::Checked { status, .. } => Some(status),
            _ => None,
        };
        assert_eq!(
            status(evaluated(&entry, &installed(UPGRADED_DESC))),
            Some(UpdateStatus::Current)
        );
        assert_eq!(
            status(evaluated(&entry, &installed(OLD_DESC))),
            Some(UpdateStatus::Update)
        );

        entry.listed = None;
        let dev = installed("Twinleaf tio-test R1 ((null)) [2026-06-08/000001-DEV]");
        assert_eq!(
            status(evaluated(&entry, &dev)),
            Some(UpdateStatus::Development)
        );
        assert_eq!(
            evaluated(&entry, &installed(OLD_DESC)),
            CatalogState::Unchecked
        );

        entry.catalog = CatalogState::Unreachable {
            error: "offline".into(),
        };
        assert_eq!(evaluated(&entry, &installed(OLD_DESC)), entry.catalog);
    }

    /// An upgrade fails at the step `flash` had reached, the upload once it
    /// stopped the device and the finalize once it committed, and at the
    /// check for an image the device cannot take, whatever it reached. The
    /// simulated device cannot be made to fail its commit, so this is where
    /// a failed commit is told as the finalize.
    #[test]
    fn a_failure_is_told_at_the_step_flash_reached() {
        let rpc = || FirmwareError::Rpc(twinleaf::device::rpc::CallError::Timeout);
        let refused = || FirmwareError::Refused("its header was refused".into());
        assert_eq!(step_of(Step::Check, &rpc()), Step::Check);
        assert_eq!(
            step_of(Step::Upload, &FirmwareError::Upload("stalled".into())),
            Step::Upload
        );
        assert_eq!(step_of(Step::Finalize, &rpc()), Step::Finalize);
        assert_eq!(step_of(Step::Upload, &refused()), Step::Check);
    }

    /// An image is neither empty nor more than [`MAX_IMAGE`].
    #[test]
    fn an_image_is_bounded() {
        assert_eq!(bounded("a.bin", 0), Err("the image a.bin is empty".into()));
        assert_eq!(bounded("a.bin", 1), Ok(()));
        assert_eq!(bounded("a.bin", MAX_IMAGE), Ok(()));
        assert_eq!(
            bounded("a.bin", MAX_IMAGE + 1),
            Err(
                "the image a.bin is 16777217 bytes, more than the 16777216 an upgrade takes".into()
            )
        );
    }

    /// A directory catalog lists the releases in the folder of a name and
    /// revision, and none for a name and revision with no folder or one
    /// that names another place. A root that is no directory cannot be
    /// listed.
    #[test]
    fn a_directory_is_a_catalog_laid_out_as_the_repository() {
        let root =
            std::env::temp_dir().join(format!("twinleaf-web-firmware-dir-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let folder = root.join("tio-test").join("R1");
        std::fs::create_dir_all(folder.join("nested")).unwrap();
        let name = file("2026-06-08", "000002");
        std::fs::write(folder.join(&name), b"image").unwrap();
        std::fs::write(folder.join("README.md"), b"not firmware").unwrap();
        let catalog = Directory(root.clone());

        let listed = catalog.list_releases("tio-test", "R1").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            (listed[0].filename.as_str(), listed[0].short_hash.as_str()),
            (name.as_str(), "000002")
        );
        assert_eq!(catalog.download(&listed[0]).unwrap(), b"image");
        assert!(catalog.list_releases("tio-test", "R2").unwrap().is_empty());
        assert!(catalog.list_releases("..", "..").unwrap().is_empty());

        let missing = Directory(root.join("missing"));
        assert!(matches!(
            missing.list_releases("tio-test", "R1"),
            Err(FirmwareError::Catalog(_))
        ));
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// Serves the page to a browser as `tio proxy` does, over simulated
    /// devices, on a port of the test's own, so the page and the hub are
    /// driven together without the port `tio proxy` serves it on. The
    /// port is `TL_SERVE_PORT` on loopback, the simulator's arguments
    /// `TL_SERVE_SIM`, split at spaces, and it serves for
    /// `TL_SERVE_SECONDS`. The hub's catalog is the directory
    /// `TWINLEAF_FIRMWARE_DIR` names. What this process logs at info and
    /// above goes to the page's log, as what `tio proxy` prints on its
    /// console does. Run with `--ignored`.
    #[tokio::test]
    #[ignore = "a server for a browser to drive"]
    async fn the_page_is_served_over_simulated_devices() {
        /// Hands each record to the page's log, as the console of `tio proxy` does.
        struct ToPage;
        impl log::Log for ToPage {
            fn enabled(&self, metadata: &log::Metadata) -> bool {
                metadata.level() <= log::Level::Info
            }
            fn log(&self, record: &log::Record) {
                // A device's own records reach the log as its log packets do.
                if self.enabled(record.metadata()) && !record.target().starts_with("device") {
                    crate::console(record.level(), record.target(), &record.args().to_string());
                }
            }
            fn flush(&self) {}
        }
        log::set_logger(&ToPage).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        let variable = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("set {name}"));
        let port: u16 = variable("TL_SERVE_PORT").parse().unwrap();
        let seconds: u64 = variable("TL_SERVE_SECONDS").parse().unwrap();
        let sim = std::env::var("TL_SERVE_SIM").unwrap_or_default();
        let args: Vec<&str> = ["--no-drop"]
            .into_iter()
            .chain(sim.split_whitespace())
            .collect();
        let stop = Arc::new(AtomicBool::new(false));
        let url = twinleaf_tools::tools::simulate::serve(&args, stop.clone());
        let root = DeviceRoute::root();
        // The hub the page's first request starts finds its proxy by this.
        std::env::set_var("TWINLEAF_PROXY", format!("tcp://127.0.0.1:{port}"));
        let proxy = Proxy::host(HostOptions {
            port,
            name: "a test".to_string(),
            default: Some(Layout {
                mounts: vec![Mount {
                    url,
                    source: root,
                    prefix: root,
                }],
            }),
            foreign: Some(crate::handler()),
            ..HostOptions::default()
        })
        .unwrap();
        println!("serving http://127.0.0.1:{port} for {seconds} s");
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        proxy.shutdown();
    }
}

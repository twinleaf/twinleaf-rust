//! Detached holders: one background `tio proxy` per serial device, found
//! through a per-user registry so every tool can share the port.
//!
//! A holder owns `<key>.lock` for its lifetime and advertises its loopback
//! endpoint in `<key>.url`. `<key>.pin` keeps it alive without clients.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::tio::transport::is_loopback;

/// How long an unpinned holder keeps serving with no clients.
pub const IDLE: Duration = Duration::from_secs(10);
/// How long a withdrawn holder waits for a client that read its endpoint.
pub const WITHDRAWAL: Duration = Duration::from_millis(300);
const STARTUP: Duration = Duration::from_secs(15);

/// Registry location: `$XDG_RUNTIME_DIR/twinleaf` or the local data directory.
pub fn runtime_dir() -> io::Result<PathBuf> {
    let root = match std::env::var_os("TWINLEAF_RUNTIME_DIR") {
        Some(path) => PathBuf::from(path),
        None => {
            let dirs = directories::BaseDirs::new()
                .ok_or_else(|| io::Error::other("cannot determine user directories"))?;
            dirs.runtime_dir()
                .map(|p| p.join("twinleaf"))
                .unwrap_or_else(|| dirs.data_local_dir().join("twinleaf/runtime"))
        }
    };
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(&root)?;
    Ok(root)
}

fn is_serial(url: &str) -> bool {
    url.starts_with("serial://") || url.starts_with("/dev/") || url.starts_with("COM")
}

fn key_from(identity: &str) -> String {
    identity
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn serial_locator(url: &str) -> (&str, &str) {
    let locator = url.strip_prefix("serial://").unwrap_or(url);
    let path = locator.split(':').next().unwrap_or_default();
    (path, &locator[path.len()..])
}

/// Registry key for a device: a network URL as given; a serial port by its
/// USB serial number when it reports one, else its path, ignoring baud.
pub fn device_key(url: &str) -> String {
    if !is_serial(url) {
        return key_from(url);
    }
    let (path, _) = serial_locator(url);
    let path = match path.strip_prefix("/dev/tty.") {
        Some(rest) if cfg!(target_os = "macos") => format!("/dev/cu.{rest}"),
        _ => path.to_string(),
    };
    key_from(&usb_identity(&path).unwrap_or_else(|| {
        let canonical = fs::canonicalize(&path).unwrap_or_else(|_| path.into());
        format!("path:{}", canonical.display())
    }))
}

/// Registry key of a serial `url`, `None` for network transports.
pub(crate) fn identity(url: &str) -> Option<String> {
    is_serial(url).then(|| device_key(url))
}

/// Where the device with `identity` is now, when a replug moved it off `url`.
pub(crate) fn relocate(url: &str, identity: Option<&str>) -> Option<String> {
    let key = identity?;
    let (_, baud) = serial_locator(url);
    super::discovery::enumerate_serial(true)
        .into_iter()
        .map(|device| device.url)
        .filter(|candidate| {
            !(cfg!(target_os = "macos") && candidate.starts_with("serial:///dev/tty."))
        })
        .find(|candidate| device_key(candidate) == key)
        .map(|candidate| format!("{candidate}{baud}"))
        .filter(|moved| moved != url)
}

/// `usb:<vid>:<pid>:<serial>` for `path`, unless several ports share it.
#[cfg(feature = "serial")]
fn usb_identity(path: &str) -> Option<String> {
    let ports = serialport::available_ports().ok()?;
    let usb = |port: &serialport::SerialPortInfo| match &port.port_type {
        serialport::SerialPortType::UsbPort(info) => info
            .serial_number
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| format!("usb:{:04x}:{:04x}:{s}", info.vid, info.pid)),
        _ => None,
    };
    let identity = usb(ports.iter().find(|p| p.port_name == path)?)?;
    let sharing = ports
        .iter()
        .filter(|p| !(cfg!(target_os = "macos") && p.port_name.starts_with("/dev/tty.")))
        .filter(|p| usb(p).as_ref() == Some(&identity))
        .count();
    (sharing == 1).then_some(identity)
}

#[cfg(not(feature = "serial"))]
fn usb_identity(_path: &str) -> Option<String> {
    None
}

fn lock_file(root: &Path, key: &str) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(format!("{key}.lock")))
}

fn live_endpoint(root: &Path, key: &str) -> Option<String> {
    match lock_file(root, key).ok()?.try_lock() {
        Err(TryLockError::WouldBlock) => fs::read_to_string(root.join(format!("{key}.url"))).ok(),
        Ok(()) | Err(TryLockError::Error(_)) => None,
    }
}

fn live_holders(root: &Path) -> io::Result<Vec<(String, String)>> {
    let entries = fs::read_dir(root)?.collect::<io::Result<Vec<_>>>()?;
    Ok(entries
        .iter()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|e| e == "url"))
        .filter_map(|path| {
            let key = path.file_stem()?.to_str()?.to_string();
            let endpoint = live_endpoint(root, &key)?;
            Some((key, endpoint))
        })
        .collect())
}

fn atomic_write(path: &Path, content: &str) -> io::Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    fs::write(&temporary, content)?;
    fs::rename(&temporary, path)
}

/// Ownership of a registry key for the lifetime of the serving process.
pub struct Holder {
    root: PathBuf,
    key: String,
    _lock: File,
}

impl Holder {
    /// Claim `key`, clearing what a previous holder left behind.
    /// `WouldBlock` when another process is serving it.
    pub fn claim(key: &str) -> io::Result<Holder> {
        let root = runtime_dir()?;
        let lock = lock_file(&root, key)?;
        lock.try_lock().map_err(io::Error::from)?;
        let holder = Holder {
            root,
            key: key.into(),
            _lock: lock,
        };
        holder.clear();
        Ok(holder)
    }

    fn path(&self, extension: &str) -> PathBuf {
        self.root.join(format!("{}.{extension}", self.key))
    }

    fn clear(&self) {
        for extension in ["url", "pin"] {
            let _ = fs::remove_file(self.path(extension));
        }
    }

    /// Advertise `endpoint` to resolvers.
    pub fn publish(&self, endpoint: &str) -> io::Result<()> {
        atomic_write(&self.path("url"), endpoint)
    }

    /// Stop advertising, ahead of an idle exit.
    pub fn withdraw(&self) -> io::Result<()> {
        fs::remove_file(self.path("url"))
    }

    /// False once `tio proxy stop` withdrew the advertisement.
    pub fn published(&self) -> bool {
        self.path("url").exists()
    }

    /// Whether `tio proxy --detach` asked this holder to outlive its clients.
    pub fn pinned(&self) -> bool {
        self.path("pin").exists()
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(windows)]
const DETACHED_PROCESS: u32 = 0x8;
#[cfg(windows)]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x200;

fn launch(root: &Path, key: &str, args: &[String]) -> io::Result<Child> {
    let executable = std::env::var_os("TWINLEAF_TIO")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::current_exe()
                .ok()
                .filter(|exe| exe.file_stem().is_some_and(|stem| stem == "tio"))
        })
        .unwrap_or_else(|| PathBuf::from("tio"));
    let log = File::create(root.join(format!("{key}.log")))?;
    let mut command = Command::new(executable);
    command
        .arg("proxy")
        .args(args)
        .arg("--holder-key")
        .arg(key)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    #[cfg(windows)]
    std::os::windows::process::CommandExt::creation_flags(
        &mut command,
        DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
    );
    command.spawn().map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot start tio holder (install tio or set TWINLEAF_TIO): {e}"),
        )
    })
}

fn failure(root: &Path, key: &str, status: ExitStatus) -> io::Error {
    let path = root.join(format!("{key}.log"));
    let tail = fs::read(&path)
        .map(|bytes| {
            String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(2048)..]).into_owned()
        })
        .unwrap_or_default();
    io::Error::other(format!(
        "holder exited ({status}); see {}\n{tail}",
        path.display()
    ))
}

/// Endpoint of the holder for `key`, starting `tio proxy <args>` when none is live.
fn ensure(root: &Path, key: &str, args: &[String]) -> io::Result<String> {
    let deadline = Instant::now() + STARTUP;
    let mut child: Option<Child> = None;
    loop {
        if let Some(endpoint) = live_endpoint(root, key) {
            if let Some(mut child) = child {
                std::thread::spawn(move || child.wait());
            }
            return Ok(endpoint);
        }
        if let Some(status) = child.as_mut().map(Child::try_wait).transpose()?.flatten() {
            if !status.success() {
                return Err(failure(root, key, status));
            }
            child = None;
        }
        if child.is_none() && lock_file(root, key)?.try_lock().is_ok() {
            child = Some(launch(root, key, args)?);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "holder did not become ready; run tio list to inspect the device",
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Where a tool connects for `url`: a device through its holder, a loopback
/// URL as given, `auto` or empty through the device in use or the only one.
pub fn resolve(url: &str) -> io::Result<String> {
    if url.is_empty() || url == "auto" {
        let (_, locator) = candidate(&runtime_dir()?)?;
        return resolve(&locator);
    }
    if is_loopback(url) {
        return Ok(url.into());
    }
    ensure(&runtime_dir()?, &device_key(url), &[url.to_string()])
}

/// Endpoint of a live holder for `url`, so probes need not take the device.
pub fn shared_endpoint(url: &str) -> Option<String> {
    if is_loopback(url) {
        return None;
    }
    live_endpoint(&runtime_dir().ok()?, &device_key(url))
}

/// A hosted hub, else the one device in use, else the only device attached.
fn candidate(root: &Path) -> io::Result<(String, String)> {
    let (hubs, devices): (Vec<_>, Vec<_>) = live_holders(root)?
        .into_iter()
        .partition(|(key, _)| key.starts_with("mount-"));
    let attached = || {
        super::discovery::enumerate_serial(false)
            .into_iter()
            .map(|device| (device_key(&device.url), device.url))
            .collect()
    };
    let candidates: BTreeMap<String, String> = [hubs, devices]
        .into_iter()
        .find(|live| !live.is_empty())
        .map_or_else(attached, |live| live.into_iter().collect());
    match candidates.len() {
        1 => Ok(candidates.into_iter().next().unwrap()),
        n => Err(io::Error::other(format!(
            "{n} device candidates; specify -r <url> or select devices with tio list"
        ))),
    }
}

fn pin(root: &Path, key: &str) -> io::Result<()> {
    fs::write(root.join(format!("{key}.pin")), "")
}

/// Pin the holder for `url`, or for the device in use or the only one attached.
pub fn detach(url: &str) -> io::Result<String> {
    let root = runtime_dir()?;
    let (key, locator) = if url.is_empty() || url == "auto" {
        candidate(&root)?
    } else {
        (device_key(url), url.to_string())
    };
    let endpoint = resolve(&locator)?;
    pin(&root, &key)?;
    Ok(endpoint)
}

/// The registry key under which `mounts` are hosted together.
pub fn composition_key(mounts: &[(String, crate::DeviceRoute)]) -> String {
    let identity = mounts
        .iter()
        .map(|(url, route)| format!("{}={route}", device_key(url)))
        .collect::<Vec<_>>()
        .join("+");
    key_from(&format!("mount:{identity}"))
}

/// Start a pinned holder mounting `mounts`, each through its own holder.
pub fn compose(mounts: &[(String, crate::DeviceRoute)]) -> io::Result<String> {
    if let [(url, route)] = mounts {
        if route.is_empty() && !is_loopback(url) {
            return detach(url);
        }
    }
    let key = composition_key(mounts);
    let root = runtime_dir()?;
    let mut args = Vec::new();
    for (url, route) in mounts {
        let endpoint = resolve(url)?;
        if route.is_empty() {
            args.push(endpoint);
        } else {
            args.extend(["--mount".to_string(), format!("{endpoint}={route}")]);
        }
    }
    let endpoint = ensure(&root, &key, &args)?;
    pin(&root, &key)?;
    Ok(endpoint)
}

/// Stop the holder advertising `endpoint`; its serial upstreams idle out after it.
pub fn stop(endpoint: &str) -> io::Result<()> {
    let root = runtime_dir()?;
    let (key, _) = live_holders(&root)?
        .into_iter()
        .find(|(_, live)| live == endpoint)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no live holder at that URL"))?;
    match fs::remove_file(root.join(format!("{key}.pin"))) {
        Ok(()) => (),
        Err(e) if e.kind() == io::ErrorKind::NotFound => (),
        Err(e) => return Err(e),
    }
    fs::remove_file(root.join(format!("{key}.url")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holder_is_live_only_while_its_lock_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let lock = lock_file(dir.path(), "ab").unwrap();
        atomic_write(&dir.path().join("ab.url"), "tcp://127.0.0.1:9").unwrap();
        assert_eq!(live_endpoint(dir.path(), "ab"), None);
        lock.lock().unwrap();
        assert_eq!(
            live_endpoint(dir.path(), "ab").as_deref(),
            Some("tcp://127.0.0.1:9")
        );
        assert_eq!(
            live_holders(dir.path()).unwrap(),
            vec![("ab".to_string(), "tcp://127.0.0.1:9".to_string())]
        );
        drop(lock);
        assert_eq!(live_endpoint(dir.path(), "ab"), None);
    }

    #[test]
    fn loopback_urls_pass_through_and_remote_devices_get_keys() {
        for url in [
            "tcp://localhost",
            "tcp://localhost:7855",
            "tcp6://[::1]:7855",
            "tcp6://::1",
            "udp://127.0.0.1:1234",
        ] {
            assert_eq!(resolve(url).unwrap(), url);
        }
        assert_eq!(device_key("tcp://192.0.2.1:7855"), "tcp---192.0.2.1-7855");
    }

    #[test]
    fn keys_ignore_baud_and_stay_filesystem_safe() {
        assert_eq!(
            device_key("serial:///dev/twinleaf-test"),
            device_key("serial:///dev/twinleaf-test:115200:115200")
        );
        assert_eq!(
            device_key("serial:///dev/twinleaf-none:9600"),
            key_from("path:/dev/twinleaf-none")
        );
        assert!(key_from("mount:usb/1=/1+tcp://h:1=/2")
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-'));
    }
}

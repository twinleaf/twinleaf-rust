//! Finding the machine's [proxy], or starting it.
//!
//! Every tool shares devices through the proxy on `127.0.0.1:7855`.
//! `TWINLEAF_PROXY` can name a different proxy, which is not started. A
//! [`Connection`](crate::Connection) connects to the proxy and asks it to
//! open a target. When nothing answers at the proxy's address, the
//! connection starts `tio proxy --background` and connects again. That proxy
//! exits ten seconds after its last client is gone. When `tio` cannot be
//! started, or exits at once as a `tio` older than 2.0 does, the connection
//! hosts the proxy in this process.
//!
//! A background proxy logs to `proxy.log` in `$XDG_RUNTIME_DIR/twinleaf` or
//! the local data directory, unless `TWINLEAF_RUNTIME_DIR` names another.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::process::{Child, Command};

use crate::proto::TCP_DEFAULT_PORT;
use crate::proxy::{self, HostOptions, Proxy, PROXY_VARIABLE};
use crate::rt::{BoxFuture, Supervisor};

/// How long [`ensure_proxy`] waits for the proxy it started to answer.
const STARTUP: Duration = Duration::from_secs(15);
/// How long [`answered`] waits after its first failed connect. The wait
/// doubles after each later failure, up to [`CONNECT_RETRY_MAX`].
const CONNECT_RETRY_FIRST: Duration = Duration::from_millis(10);
/// The longest [`answered`] waits between two connects.
const CONNECT_RETRY_MAX: Duration = Duration::from_millis(200);
/// The log file of a proxy [`spawn_proxy`] started, in [`runtime_dir`].
const PROXY_LOG: &str = "proxy.log";
/// How large the proxy log may grow, in bytes, before a spawn starts it over.
const PROXY_LOG_LIMIT: u64 = 1 << 20;

/// Returns the directory a background proxy logs in, creating it if needed.
/// That is `$XDG_RUNTIME_DIR/twinleaf` or the local data directory, unless
/// `TWINLEAF_RUNTIME_DIR` names another.
pub(crate) fn runtime_dir() -> io::Result<PathBuf> {
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

#[cfg(windows)]
const DETACHED_PROCESS: u32 = 0x8;
#[cfg(windows)]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x200;

/// Starts `tio proxy --background` on `port` of `127.0.0.1`, in its own
/// process group, with its stderr in `proxy.log` in [`runtime_dir`]. The
/// proxy outlives this process. Blocks while it opens the log and starts the
/// process. Must be called on a thread of a tokio runtime.
fn spawn_proxy(port: u16) -> io::Result<SpawnedProxy> {
    let log = runtime_dir()?.join(PROXY_LOG);
    let executable = std::env::var_os("TWINLEAF_TIO")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::current_exe()
                .ok()
                .filter(|exe| exe.file_stem().is_some_and(|stem| stem == "tio"))
        })
        .unwrap_or_else(|| PathBuf::from("tio"));
    let mut command = Command::new(executable);
    command
        .args(["proxy", "--background"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(open_log(&log)?);
    if port != TCP_DEFAULT_PORT {
        command.args(["--port", &port.to_string()]);
    }
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    let child = command.spawn().map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("cannot start a background proxy (install tio or set TWINLEAF_TIO): {e}"),
        )
    })?;
    Ok(SpawnedProxy { child, log })
}

/// Returns the error for a proxy that exited with `status`, with the end of
/// its `log`.
fn failure(log: &Path, status: ExitStatus) -> io::Error {
    let tail = fs::read(log)
        .map(|bytes| {
            String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(2048)..]).into_owned()
        })
        .unwrap_or_default();
    io::Error::other(format!(
        "the background proxy exited ({status}); see {}\n{tail}",
        log.display()
    ))
}

/// A proxy that [`spawn_proxy`] started, which [`wait_for_proxy`] waits
/// for.
#[derive(Debug)]
struct SpawnedProxy {
    child: Child,
    /// Where it logs.
    log: PathBuf,
}

/// Opens the proxy log to append, or starts it over once it outgrows
/// [`PROXY_LOG_LIMIT`].
fn open_log(path: &Path) -> io::Result<File> {
    if fs::metadata(path).is_ok_and(|meta| meta.len() > PROXY_LOG_LIMIT) {
        return File::create(path);
    }
    // Proxies spawned at once share the log, and truncating it would erase
    // what the one that won the port has written.
    fs::OpenOptions::new().create(true).append(true).open(path)
}

/// Waits up to `timeout` for the proxy on `port` to answer.
///
/// A `spawned` proxy that exits successfully found another proxy hosting,
/// and the wait goes on for that one.
///
/// # Errors
///
/// Fails with the end of its log if the proxy exits with an error, and with
/// [`io::ErrorKind::TimedOut`] once `timeout` passes.
async fn wait_for_proxy(
    port: u16,
    spawned: Option<SpawnedProxy>,
    timeout: Duration,
) -> io::Result<()> {
    let waited = async {
        if let Some(SpawnedProxy { mut child, log }) = spawned {
            tokio::select! {
                status = child.wait() => {
                    let status = status?;
                    if !status.success() {
                        let failed = move || failure(&log, status);
                        let reading = Supervisor::new().spawn_blocking(failed);
                        return Err(reading.await.unwrap_or_else(io::Error::other));
                    }
                }
                () = answered(port) => return Ok(()),
            }
        }
        answered(port).await;
        Ok(())
    };
    tokio::time::timeout(timeout, waited)
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no proxy answered on 127.0.0.1:{port} within {timeout:?}"),
            ))
        })
}

/// Connects to `port` of `127.0.0.1` until something answers. Waits
/// [`CONNECT_RETRY_FIRST`] after the first failure, and twice as long after
/// each later one, up to [`CONNECT_RETRY_MAX`].
async fn answered(port: u16) {
    let mut delay = CONNECT_RETRY_FIRST;
    while !proxy::answers_async(port).await {
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(CONNECT_RETRY_MAX);
    }
}

/// The proxy this process hosts itself, when no `tio` could be started.
static EMBEDDED: OnceLock<Proxy> = OnceLock::new();

/// Ensures the machine's proxy answers on `127.0.0.1:7855`.
///
/// When nothing does, starts `tio proxy --background` and waits for it. When
/// `tio` cannot be started, or exits with an error before the proxy answers,
/// hosts the proxy in this process until it exits. The future must run on a
/// tokio runtime.
pub(crate) fn ensure_proxy() -> BoxFuture<'static, io::Result<()>> {
    Box::pin(ensure_proxy_at(TCP_DEFAULT_PORT, &EMBEDDED))
}

/// Ensures a proxy answers on `port` as [`ensure_proxy`] does, keeping a
/// hosted proxy in `embedded`.
async fn ensure_proxy_at(port: u16, embedded: &OnceLock<Proxy>) -> io::Result<()> {
    if proxy::answers_async(port).await {
        return Ok(());
    }
    let spawning = Supervisor::new().spawn_blocking(move || spawn_proxy(port));
    let waited = match spawning.await.map_err(io::Error::other)? {
        Ok(spawned) => wait_for_proxy(port, Some(spawned), STARTUP).await,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
            ) =>
        {
            log::debug!("hosting the proxy in this process: {error}");
            return embed(port, embedded).await;
        }
        Err(error) => return Err(error),
    };
    match waited {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::TimedOut => Err(error),
        // A tio older than 2.0 exits at once on the flag it does not know.
        Err(error) => {
            log::warn!("hosting the proxy in this process, as {error}");
            embed(port, embedded).await
        }
    }
}

/// Hosts the proxy on `port` in this process, kept in `embedded`. If another
/// process holds the port, waits for its proxy to answer.
async fn embed(port: u16, embedded: &OnceLock<Proxy>) -> io::Result<()> {
    if embedded.get().is_some() {
        return Ok(());
    }
    let options = HostOptions {
        port,
        name: format!("{} (embedded)", proxy::executable_name()),
        ..HostOptions::default()
    };
    // Hosting checks the port with a blocking connect, and may call the
    // proxy that answers there.
    let hosting = Supervisor::new().spawn_blocking(move || Proxy::host(options));
    match hosting.await.map_err(io::Error::other)? {
        Ok(proxy) => {
            // One proxy at a time binds the port, so nothing else set it.
            let _ = embedded.set(proxy);
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            wait_for_proxy(port, None, STARTUP).await
        }
        Err(error) => Err(error),
    }
}

/// The proxy a connection goes through, and whether it may be started.
#[derive(Debug, Clone)]
pub(crate) struct ProxyAddress {
    /// Where it answers, such as `tcp://127.0.0.1:7855`.
    pub(crate) url: String,
    /// What finds or starts the proxy when nothing answers. Only the
    /// machine's own proxy has one.
    pub(crate) ensure: Option<fn() -> BoxFuture<'static, io::Result<()>>>,
}

impl ProxyAddress {
    /// Returns the proxy `TWINLEAF_PROXY` names, or else the machine's own
    /// on `127.0.0.1:7855`, which may be started.
    pub(crate) fn chosen() -> ProxyAddress {
        Self::of(proxy::named())
    }

    /// Returns the proxy as [`chosen`](Self::chosen) does, with `named` as
    /// the variable's value.
    fn of(named: Option<String>) -> ProxyAddress {
        match named {
            Some(url) => ProxyAddress { url, ensure: None },
            None => ProxyAddress {
                url: proxy::endpoint_at(TCP_DEFAULT_PORT),
                ensure: Some(ensure_proxy),
            },
        }
    }

    /// Returns the error to report when reaching the proxy failed with
    /// `error`. For a proxy `TWINLEAF_PROXY` names, the message names the
    /// variable, and says nothing answers there if the connection was refused
    /// or timed out.
    pub(crate) fn unreachable(&self, error: io::Error) -> io::Error {
        let (kind, url) = (error.kind(), &self.url);
        let message = match self.ensure {
            Some(_) => format!("cannot reach the proxy at {url}: {error}"),
            None if matches!(
                kind,
                io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut
            ) =>
            {
                format!("no proxy answers at {url} ({PROXY_VARIABLE})")
            }
            None => format!("cannot reach the proxy at {url} ({PROXY_VARIABLE}): {error}"),
        };
        io::Error::new(kind, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::fixtures::{self, ENVIRONMENT, WAIT};
    use crate::device::rpc::{RpcArgs, RpcReply};
    use std::sync::PoisonError;

    /// Runs [`ensure_proxy_at`] for `port` with nothing embedded yet.
    /// Returns its outcome and what it embedded.
    fn ensure_at(port: u16) -> (io::Result<()>, OnceLock<Proxy>) {
        let ensuring = async move {
            let embedded = OnceLock::new();
            let ensured = ensure_proxy_at(port, &embedded).await;
            Ok((ensured, embedded))
        };
        Supervisor::new().run("ensure", ensuring).unwrap()
    }

    /// A proxy that `TWINLEAF_PROXY` names is never started, and with none
    /// named the machine's own proxy is.
    #[test]
    fn the_variable_names_the_proxy_else_the_machines_proxy() {
        let named = ProxyAddress::of(Some("tcp://127.0.0.1:2".to_string()));
        assert_eq!(named.url, "tcp://127.0.0.1:2");
        assert!(named.ensure.is_none());

        let machine = ProxyAddress::of(None);
        assert_eq!(machine.url, "tcp://127.0.0.1:7855");
        assert!(machine.ensure.is_some());
    }

    /// Proxies spawned together share the log, so each spawn appends rather
    /// than erasing what another wrote, until the log outgrows its limit.
    #[test]
    fn the_proxy_log_is_appended_to_until_it_outgrows_its_limit() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(PROXY_LOG);
        open_log(&path).unwrap().write_all(b"winner\n").unwrap();
        open_log(&path).unwrap().write_all(b"loser\n").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"winner\nloser\n");

        fs::write(&path, vec![b'x'; PROXY_LOG_LIMIT as usize + 1]).unwrap();
        open_log(&path).unwrap().write_all(b"fresh\n").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"fresh\n");
    }

    /// With no `tio` to start, the process hosts the proxy itself, and
    /// later finds it answering.
    #[test]
    fn a_proxy_is_embedded_when_tio_cannot_be_started() {
        let _environment = ENVIRONMENT.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("TWINLEAF_TIO", dir.path().join("no-such-tio"));
        std::env::set_var("TWINLEAF_RUNTIME_DIR", dir.path());

        let attempt = fixtures::on_free_port(|port| {
            let (ensured, embedded) = ensure_at(port);
            embedded
                .get()
                .is_some()
                .then_some((port, ensured, embedded))
        });

        std::env::remove_var("TWINLEAF_TIO");
        std::env::remove_var("TWINLEAF_RUNTIME_DIR");
        let (port, ensured, embedded) = attempt.expect("no proxy was embedded");
        ensured.unwrap();
        let hosted = embedded.get().unwrap();
        assert_eq!(hosted.endpoint(), proxy::endpoint_at(port));
        let hello = proxy::Introduction::of_this_process("test").encode_args();
        let reply = proxy::call(&hosted.endpoint(), "proxy.hello", &hello, WAIT).unwrap();
        let host = proxy::Introduction::decode_reply(&reply).unwrap();
        assert!(host.name.ends_with("(embedded)"), "{host}");
        let (ensured, again) = ensure_at(port);
        ensured.unwrap();
        assert!(again.get().is_none(), "a second proxy was embedded");
    }

    /// A `tio` older than 2.0 starts, but exits unsuccessfully on the flag it
    /// does not know, so the process hosts the proxy itself.
    #[cfg(unix)]
    #[test]
    fn a_proxy_is_embedded_when_tio_cannot_run_it() {
        use std::os::unix::fs::PermissionsExt;

        let _environment = ENVIRONMENT.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = tempfile::tempdir().unwrap();
        let old_tio = dir.path().join("tio");
        let script =
            "#!/bin/sh\necho \"error: unexpected argument '--background' found\" >&2\nexit 2\n";
        fs::write(&old_tio, script).unwrap();
        fs::set_permissions(&old_tio, fs::Permissions::from_mode(0o755)).unwrap();
        std::env::set_var("TWINLEAF_TIO", &old_tio);
        std::env::set_var("TWINLEAF_RUNTIME_DIR", dir.path());

        let attempt = fixtures::on_free_port(|port| {
            let (ensured, embedded) = ensure_at(port);
            embedded.into_inner().map(|hosted| (port, ensured, hosted))
        });

        std::env::remove_var("TWINLEAF_TIO");
        std::env::remove_var("TWINLEAF_RUNTIME_DIR");
        let (port, ensured, hosted) = attempt.expect("no proxy was embedded");
        ensured.unwrap();
        assert_eq!(hosted.endpoint(), proxy::endpoint_at(port));
        let log = fs::read_to_string(dir.path().join(PROXY_LOG)).unwrap();
        assert!(log.contains("unexpected argument"), "{log}");
    }

    /// Of two proxies started at once, the one that finds the other hosting
    /// exits successfully, and its waiter waits for the other instead.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_proxy_that_lost_the_race_leaves_the_wait_to_the_winner() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join(PROXY_LOG);
        fs::write(&log, "no tio here\n").unwrap();
        let spawned = |code: u8| {
            Some(SpawnedProxy {
                child: Command::new("sh")
                    .args(["-c", &format!("exit {code}")])
                    .spawn()
                    .unwrap(),
                log: log.clone(),
            })
        };

        let closed = fixtures::ClosedPort::new();
        let port = closed.port();
        let (done, finished) = std::sync::mpsc::sync_channel::<()>(0);
        let winner = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            let _listener = closed.listen();
            let _ = finished.recv_timeout(WAIT);
        });
        wait_for_proxy(port, spawned(0), WAIT).await.unwrap();
        drop(done);
        winner.join().unwrap();

        let closed = fixtures::ClosedPort::new();
        let failed = wait_for_proxy(closed.port(), spawned(3), WAIT)
            .await
            .unwrap_err();
        assert!(failed.to_string().contains("no tio here"), "{failed}");
    }

    /// A proxy that keeps running is ready once its port answers.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_running_proxy_is_ready_once_its_port_answers() {
        let dir = tempfile::tempdir().unwrap();
        let closed = fixtures::ClosedPort::new();
        let port = closed.port();
        let _listener = closed.listen();
        let child = Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let spawned = SpawnedProxy {
            child,
            log: dir.path().join(PROXY_LOG),
        };

        wait_for_proxy(port, Some(spawned), WAIT).await.unwrap();
    }
}

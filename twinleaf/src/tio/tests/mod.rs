//! Test fixtures for `tio` and every layer above it, a fake device on a
//! loopback port, ports and URLs nothing answers, and the limits a test
//! waits within.

pub(crate) mod device;

use std::fmt::Debug;
use std::io;
use std::net::{Ipv4Addr, TcpListener};
use std::time::Duration;

/// The longest any step of a test waits.
pub(crate) const WAIT: Duration = Duration::from_secs(5);

/// The longest any step of a test on a paused clock waits. It outlasts every
/// timeout of the proxy, and costs no time.
pub(crate) const PAUSED_WAIT: Duration = Duration::from_secs(60);

/// The variable that, set to `1`, runs the tests that bind every interface
/// or join a multicast group. Windows asks whether to let each new build of
/// a binary open such a socket, so every other test binds loopback alone.
/// Run them with `TWINLEAF_TEST_ALL_INTERFACES=1 cargo test`.
pub(crate) const ALL_INTERFACES: &str = "TWINLEAF_TEST_ALL_INTERFACES";

/// Returns `true` if [`ALL_INTERFACES`] is `1`. Otherwise says on stderr
/// that `test` is skipped and how to run it, and returns `false`.
pub(crate) fn binds_every_interface(test: &str) -> bool {
    if std::env::var_os(ALL_INTERFACES).is_some_and(|value| value == "1") {
        return true;
    }
    eprintln!("skipped {test}, which binds every interface. Run it with {ALL_INTERFACES}=1");
    false
}

/// A URL that nothing answers. Opening it fails at once on every platform.
// A TCP port freed for the purpose could be taken by another test before it
// is dialed.
pub(crate) const UNREACHABLE: &str = "serial:///dev/twinleaf-test-unreachable";

/// Asserts that `took` is `expected`, to the millisecond that timers round
/// to.
pub(crate) fn assert_took(took: Duration, expected: Duration, what: impl Debug) {
    let rounded = expected + Duration::from_millis(1);
    assert!(
        (expected..=rounded).contains(&took),
        "{what:?} took {took:?}, not {expected:?}"
    );
}

/// A port of `127.0.0.1` that nothing listens on, so it refuses connections
/// unless another process takes it after it was found free.
pub(crate) struct ClosedPort(u16);

impl ClosedPort {
    /// Finds a free port and leaves it unbound.
    pub(crate) fn new() -> ClosedPort {
        ClosedPort(
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port(),
        )
    }

    /// Returns the port number.
    pub(crate) fn port(&self) -> u16 {
        self.0
    }

    /// Returns its URL, such as `tcp://127.0.0.1:40000`.
    pub(crate) fn url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.port())
    }

    /// Starts listening on the port.
    #[cfg(unix)]
    pub(crate) fn listen(self) -> TcpListener {
        TcpListener::bind((Ipv4Addr::LOCALHOST, self.0)).unwrap()
    }
}

/// How many ports [`on_free_port`] tries.
const PORT_ATTEMPTS: usize = 5;

/// Runs `scenario` on a port of `127.0.0.1` that was free a moment before,
/// for a test whose proxy binds the port itself.
///
/// Another process can take the port in that moment. When `scenario` returns
/// `None` and the port is held elsewhere, the scenario runs again on another
/// port. Returns `None` if the port was free and the scenario still failed,
/// or every port tried was taken.
pub(crate) fn on_free_port<T>(mut scenario: impl FnMut(u16) -> Option<T>) -> Option<T> {
    for _ in 0..PORT_ATTEMPTS {
        let port = ClosedPort::new().port();
        if let Some(done) = scenario(port) {
            return Some(done);
        }
        let taken = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
            .is_err_and(|error| error.kind() == io::ErrorKind::AddrInUse);
        if !taken {
            return None;
        }
    }
    None
}

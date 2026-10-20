//! The process's one mDNS daemon, which the transports resolve `.local`
//! names with.

use crate::rt::Supervisor;
use mdns_sd::{HostnameResolutionEvent, Receiver, ScopedIp, ServiceDaemon};
use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV6};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::time::timeout;

/// How long [`resolve`] waits for mDNS to answer.
// The daemon sends its first query after a jitter of 20 to 120 ms and
// repeats it after one second, so this covers two queries.
pub(crate) const RESOLVE_TIMEOUT: Duration = Duration::from_millis(1500);

/// The daemon, once started.
static DAEMON: Mutex<Option<ServiceDaemon>> = Mutex::new(None);

/// The lock each name's resolutions take in turn, by name.
// The daemon keeps one listener per name. A second resolution of a name
// would take the first one's answers, and stopping either would stop both.
static RESOLVING: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>> =
    Mutex::new(BTreeMap::new());

/// Locks `mutex`, even if a thread panicked while holding it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Returns the process's mDNS daemon, starting it on first use.
///
/// # Errors
///
/// Returns an error if the daemon cannot start. The next call tries again.
pub(crate) fn daemon() -> Result<ServiceDaemon, mdns_sd::Error> {
    let mut daemon = lock(&DAEMON);
    if let Some(running) = daemon.as_ref() {
        return Ok(running.clone());
    }
    let started = ServiceDaemon::new()?;
    *daemon = Some(started.clone());
    Ok(started)
}

/// Resolves `host`, a `.local` name in lower case without a trailing dot,
/// over mDNS. Returns the addresses of the first answer, each with `port`.
///
/// Waits for at most [`RESOLVE_TIMEOUT`], and longer only while another
/// resolution of the same name runs. Returns no address if mDNS answers
/// nothing in time or the daemon cannot start. Skips an IPv6 link-local
/// address that comes without its interface. Dropping the future stops the
/// search. Must be called on the crate's runtime.
pub(crate) async fn resolve(host: &str, port: u16) -> Vec<SocketAddr> {
    let name = format!("{host}.");
    let turn = lock(&RESOLVING).entry(name.clone()).or_default().clone();
    let _turn = turn.lock().await;
    // Starting the daemon binds its sockets and starts its thread.
    let started = Supervisor::new().spawn_blocking(daemon).await;
    let daemon = match started {
        Ok(Ok(daemon)) => daemon,
        Ok(Err(error)) => return unresolved(host, error),
        Err(panicked) => return unresolved(host, panicked),
    };
    let millis = u64::try_from(RESOLVE_TIMEOUT.as_millis()).ok();
    let answers = match daemon.resolve_hostname(&name, millis) {
        Ok(answers) => answers,
        Err(error) => return unresolved(host, error),
    };
    let search = Search {
        daemon,
        name,
        answers: Some(answers),
    };
    let searching = async {
        while let Ok(event) = search.answers().recv_async().await {
            match event {
                HostnameResolutionEvent::AddressesFound(_, addresses) => {
                    let found: Vec<_> = addresses
                        .iter()
                        .filter_map(|address| socket_addr(address, port))
                        .collect();
                    if !found.is_empty() {
                        return found;
                    }
                }
                HostnameResolutionEvent::SearchStopped(_) => break,
                _ => {}
            }
        }
        Vec::new()
    };
    timeout(RESOLVE_TIMEOUT, searching)
        .await
        .unwrap_or_default()
}

/// Logs why `host` cannot be resolved, and returns no address.
fn unresolved(host: &str, error: impl std::fmt::Display) -> Vec<SocketAddr> {
    log::debug!("cannot resolve {host} over mDNS: {error}");
    Vec::new()
}

/// One search of the daemon for a name, which stops once dropped.
struct Search {
    daemon: ServiceDaemon,
    /// The name searched for, with its trailing dot.
    name: String,
    /// The answers, until the search stops.
    answers: Option<Receiver<HostnameResolutionEvent>>,
}

impl Search {
    /// Returns the receiver of the answers.
    fn answers(&self) -> &Receiver<HostnameResolutionEvent> {
        self.answers
            .as_ref()
            .expect("a search has its answers until it stops")
    }
}

/// Stops the search.
impl Drop for Search {
    fn drop(&mut self) {
        // The daemon blocks on a full listener, so the receiver goes first.
        // Stopping only queues a command for the daemon's thread.
        drop(self.answers.take());
        let _ = self.daemon.stop_resolve_hostname(&self.name);
    }
}

/// Returns `address` as a socket address with `port`. An IPv6 link-local
/// address keeps its interface as its scope. Returns `None` for one that
/// comes without its interface.
fn socket_addr(address: &ScopedIp, port: u16) -> Option<SocketAddr> {
    match address {
        ScopedIp::V4(v4) => Some(SocketAddr::from((*v4.addr(), port))),
        ScopedIp::V6(v6) => {
            let ip = *v6.addr();
            let scope = if ip.is_unicast_link_local() {
                let index = v6.scope_id().index;
                if index == 0 {
                    return None;
                }
                index
            } else {
                0
            };
            Some(SocketAddr::V6(SocketAddrV6::new(ip, port, 0, scope)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    /// A resolution dropped while it waits stops its search and gives up the
    /// name's turn, so the next resolution of the name waits no longer than
    /// its own timeout.
    #[tokio::test]
    async fn a_resolution_dropped_while_it_waits_gives_up_its_turn() {
        let host = "twinleaf-test-nobody-answers.local";
        let short = Duration::from_millis(100);
        let _ = tokio::time::timeout(short, resolve(host, 7855)).await;

        let began = std::time::Instant::now();
        let next = tokio::time::timeout(RESOLVE_TIMEOUT * 2, resolve(host, 7855)).await;

        assert_eq!(next, Ok(Vec::new()), "the dropped resolution kept its turn");
        assert!(began.elapsed() < RESOLVE_TIMEOUT + short);
    }

    /// A link-local IPv6 address needs its interface, and one without is
    /// skipped. Other addresses need none.
    #[test]
    fn an_address_without_the_scope_it_needs_is_skipped() {
        let scoped = |ip: &str| ScopedIp::from(ip.parse::<IpAddr>().unwrap());

        assert_eq!(socket_addr(&scoped("fe80::1"), 7855), None);
        assert_eq!(
            socket_addr(&scoped("fd00::5"), 7855),
            Some("[fd00::5]:7855".parse().unwrap())
        );
        assert_eq!(
            socket_addr(&scoped("192.168.1.20"), 7000),
            Some("192.168.1.20:7000".parse().unwrap())
        );
    }
}

//! The crate's TCP sockets and name lookup.
//!
//! Built with `--cfg turmoil`, the crate's tests run them over turmoil's
//! simulated network, where hosts in one process connect, lose their links
//! and restart on a virtual clock. The command is
//!
//! ```text
//! RUSTFLAGS="--cfg turmoil" cargo test -p twinleaf --lib --target-dir target/turmoil turmoil
//! ```

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io;
use std::net::SocketAddr;

#[cfg(not(all(test, turmoil)))]
pub(crate) type TcpStream = tokio::net::TcpStream;
#[cfg(not(all(test, turmoil)))]
pub(crate) type TcpListener = tokio::net::TcpListener;
#[cfg(all(test, turmoil))]
pub(crate) type TcpStream = turmoil::net::TcpStream;
#[cfg(all(test, turmoil))]
pub(crate) type TcpListener = turmoil::net::TcpListener;

/// Returns the addresses `address`, a `host:port`, resolves to.
#[cfg(not(all(test, turmoil)))]
pub(crate) async fn lookup_host(address: String) -> io::Result<Vec<SocketAddr>> {
    Ok(tokio::net::lookup_host(address).await?.collect())
}

/// Returns the address of the simulated host `address` names.
#[cfg(all(test, turmoil))]
pub(crate) async fn lookup_host(address: String) -> io::Result<Vec<SocketAddr>> {
    if let Ok(found) = address.parse::<SocketAddr>() {
        return Ok(vec![found]);
    }
    let (host, port) = address
        .rsplit_once(':')
        .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, address.clone()))?;
    Ok(vec![SocketAddr::new(turmoil::lookup(host), port)])
}

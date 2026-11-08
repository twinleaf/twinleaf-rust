//! The primitives the crate's tasks share, which its tests swap for
//! simulated ones. Channels, locks, timers, task spawns and stream sets are
//! in `chan`, and TCP sockets and name lookup in `net`. The crate root
//! imports the two, so the rest of the crate names them `crate::chan` and
//! `crate::net`.
//!
//! Each backend is a module of its own. `real` is a normal build's. The
//! `shuttle` backend, built with `--cfg shuttle`, runs the channels, locks
//! and tasks on shuttle's scheduler, and the tests whose path holds
//! `shuttle` run their scenario under many schedules. The `turmoil`
//! backend, built with `--cfg turmoil --cfg tokio_unstable`, runs the
//! sockets and tasks on turmoil's simulated network and virtual clock, and
//! the tests whose path holds `turmoil` run a device, the proxy and apps
//! there. Each tier builds into a target directory of its own.
//!
//! ```text
//! cargo test-shuttle
//! cargo test-turmoil
//! ```
//!
//! A test backend starts from the real one and replaces what it models. The
//! crate's clippy configuration refuses the libraries' own primitives
//! anywhere outside this module, so no use of one slips past a backend.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(all(shuttle, turmoil))]
compile_error!("build with `--cfg shuttle` or `--cfg turmoil`, not both");

mod real;
#[cfg(all(test, shuttle))]
pub(crate) mod shuttle;
#[cfg(all(test, turmoil))]
pub(crate) mod turmoil;

#[cfg(not(all(test, any(shuttle, turmoil))))]
pub(crate) use real::{chan, net};
#[cfg(all(test, shuttle))]
pub(crate) use shuttle::{chan, net};
#[cfg(all(test, turmoil))]
pub(crate) use turmoil::{chan, net};

/// The socket a proxy hands a [`Foreign`] handler, with nothing read from it.
pub type ForeignStream = tokio::net::TcpStream;

/// What a proxy does with a connection that does not speak its protocol,
/// such as a browser's request, given the socket and the peer's address.
///
/// The handler is called on the proxy's runtime and must not block it. The
/// proxy keeps nothing of the connection after the call.
#[derive(Clone)]
pub struct Foreign(std::sync::Arc<dyn Fn(ForeignStream, std::net::SocketAddr) + Send + Sync>);

impl Foreign {
    /// Wraps `serve` as a handler.
    pub fn new(
        serve: impl Fn(ForeignStream, std::net::SocketAddr) + Send + Sync + 'static,
    ) -> Foreign {
        Foreign(std::sync::Arc::new(serve))
    }

    /// Hands `stream`, from `peer`, to the handler.
    pub(crate) fn serve(&self, stream: ForeignStream, peer: std::net::SocketAddr) {
        (self.0)(stream, peer);
    }
}

impl std::fmt::Debug for Foreign {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Foreign")
    }
}

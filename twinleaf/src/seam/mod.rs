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

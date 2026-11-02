//! The channels, locks, timers and task spawns that the crate's tasks share.
//! It is the one module that names the libraries behind them, and the
//! crate's clippy configuration refuses them anywhere else.
//!
//! A normal build takes its channels from flume, locks a std `Mutex` past a
//! panic of another holder, and takes `oneshot`, `Notify`, `sleep_until`, its
//! task spawns and their abort handles from tokio.
//!
//! The crate's own tests built with `--cfg turmoil` spawn each task on the
//! `LocalSet` of the turmoil host it is spawned from, whatever runtime it
//! names. A host that crashes then drops its tasks in the order they
//! started. Tasks of a tokio runtime would drop in an order set by their
//! task ids, which count across the whole process, so a scenario run twice
//! would crash two ways.
//!
//! The crate's own tests built with `--cfg shuttle` take each of them from
//! shuttle instead. Every send, receive, lock, notification, sleep and spawn
//! is then a point where shuttle's scheduler picks the thread or task that
//! runs next, and the tests whose path holds `shuttle` run their scenario
//! under many schedules. This command runs them, in a target directory of
//! their own.
//!
//! ```text
//! RUSTFLAGS="--cfg shuttle" cargo test -p twinleaf --lib --target-dir target/shuttle shuttle
//! ```
//!
//! Each test runs its scenario under shuttle's random scheduler and then its
//! PCT scheduler, as `check` says. A failing test prints the schedule that
//! broke it. The command above, filtered to that test and run with
//! `TWINLEAF_SHUTTLE_SCHEDULE` set to the schedule, replays only that
//! schedule. `TWINLEAF_SHUTTLE_ITERATIONS` sets how many schedules each
//! scheduler runs, 1000 by default. The tests reach the session, the pump,
//! their lanes and the store, and none of the proxy, whose sockets shuttle
//! does not model.
//!
//! Under shuttle the channels are the stand-in of the `modeled` module. It
//! differs from flume in these ways.
//!
//! - A receive with a timeout or a deadline never times out, as shuttle has
//!   no clock.
//! - A [`Selector`] builds, and waiting on it panics.
//! - A `bounded(0)` channel panics, as the stand-in has no rendezvous.
//! - An async send that waits for room blocks its task, as a blocking send
//!   blocks its thread.
//! - Waiting receivers are woken together, in no order.
//!
//! The rest of the crate keeps its own primitives under shuttle.
//!
//! - A tokio `mpsc` channel works there, and none of its operations is a
//!   scheduling point.
//! - The `select!` of the session and of the pump are `biased`, so they make
//!   no choice of their own.
//! - A tokio `sleep` or `timeout` other than [`sleep_until`] needs a tokio
//!   runtime. A `sleep_until` ends at its first poll unless its deadline is
//!   a year or more away.
//! - A supervisor's tasks run as shuttle tasks whatever runtime it names.
//!   Blocking work and the tasks of a `JoinMap` run on that runtime's real
//!   threads, outside shuttle's control, and must not touch a primitive of
//!   this module.
//! - A `OnceLock` is std's, and a `get_or_init` that waits on another thread
//!   would stall shuttle's one OS thread. So a test starts a stream's pump,
//!   which fills one, before it starts a second thread.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::future::Future;
use std::sync::{Mutex, MutexGuard};

// The names below are aliases and wrappers rather than re-exports, so that
// clippy, which sees through a re-export, tells their uses from a use of the
// library itself.
#[cfg(not(all(test, shuttle)))]
pub(crate) use flume::{RecvError, RecvTimeoutError, TryRecvError, TrySendError};
#[cfg(not(all(test, shuttle)))]
pub(crate) type Sender<T> = flume::Sender<T>;
#[cfg(not(all(test, shuttle)))]
pub(crate) type Receiver<T> = flume::Receiver<T>;
#[cfg(not(all(test, shuttle)))]
pub(crate) type RecvFut<'a, T> = flume::r#async::RecvFut<'a, T>;
#[cfg(not(all(test, shuttle)))]
pub(crate) type RecvStream<'a, T> = flume::r#async::RecvStream<'a, T>;
#[cfg(not(all(test, shuttle)))]
pub(crate) type Selector<'a, T> = flume::Selector<'a, T>;
#[cfg(not(all(test, shuttle)))]
pub(crate) type Notify = tokio::sync::Notify;
#[cfg(not(all(test, shuttle)))]
pub(crate) type JoinHandle<T> = tokio::task::JoinHandle<T>;
#[cfg(not(all(test, shuttle)))]
pub(crate) type AbortOnDropHandle<T> = tokio_util::task::AbortOnDropHandle<T>;

/// The receiver of an mDNS search's answers, a flume channel that the mDNS
/// library makes, which every build keeps.
#[cfg(feature = "mdns")]
pub(crate) type MdnsReceiver<T> = mdns_sd::Receiver<T>;

/// Returns a channel with room for `capacity` items.
#[cfg(not(all(test, shuttle)))]
pub(crate) fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    flume::bounded(capacity)
}

/// Returns a channel with room for any number of items.
#[cfg(not(all(test, shuttle)))]
pub(crate) fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    flume::unbounded()
}

/// Waits until `deadline`.
#[cfg(not(all(test, shuttle)))]
pub(crate) fn sleep_until(deadline: tokio::time::Instant) -> tokio::time::Sleep {
    tokio::time::sleep_until(deadline)
}

/// Channels that carry one item.
#[cfg(not(all(test, shuttle)))]
pub(crate) mod oneshot {
    pub(crate) use tokio::sync::oneshot::error;

    pub(crate) type Sender<T> = tokio::sync::oneshot::Sender<T>;
    pub(crate) type Receiver<T> = tokio::sync::oneshot::Receiver<T>;

    /// Returns a channel that carries one item.
    pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
        tokio::sync::oneshot::channel()
    }
}

#[cfg(all(test, shuttle))]
mod modeled;
#[cfg(all(test, shuttle))]
pub(crate) use modeled::{
    bounded, unbounded, Receiver, RecvError, RecvFut, RecvStream, RecvTimeoutError, Selector,
    Sender, TryRecvError, TrySendError,
};
#[cfg(all(test, shuttle))]
pub(crate) use shuttle_tokio::{
    sync::{oneshot, Notify},
    task::JoinHandle,
    time::sleep_until,
};

/// The handle of a shuttle task, which aborts the task when it drops.
#[cfg(all(test, shuttle))]
pub(crate) struct AbortOnDropHandle<T>(JoinHandle<T>);

#[cfg(all(test, shuttle))]
impl<T> AbortOnDropHandle<T> {
    /// Takes `task`, which aborts once this drops.
    pub(crate) fn new(task: JoinHandle<T>) -> AbortOnDropHandle<T> {
        AbortOnDropHandle(task)
    }
}

#[cfg(all(test, shuttle))]
impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Locks `mutex`, past a panic of another holder. It suits a lock that
/// guards no change a panic could leave half made.
#[cfg(not(all(test, shuttle)))]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Locks `mutex`, past a panic of another holder. Shuttle runs every thread
/// and task on one OS thread, where a lock that blocks would stop them all,
/// so each try is a scheduling point instead, and one that fails gives the
/// holder its turn.
#[cfg(all(test, shuttle))]
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    loop {
        shuttle::thread::yield_now();
        match mutex.try_lock() {
            Ok(guard) => return guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => return poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {}
        }
    }
}

/// Spawns `future` as a task of the runtime of `handle`.
#[cfg(not(all(test, any(shuttle, turmoil))))]
pub(crate) fn spawn_on<F>(handle: &tokio::runtime::Handle, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    handle.spawn(future)
}

/// Spawns `future` as a task of the turmoil host it is spawned from,
/// whatever runtime `handle` names. That host is the one turmoil is
/// ticking when the call is made, so a task spawned for another host's
/// runtime runs on this one.
///
/// # Panics
///
/// Panics if it is called outside a turmoil host.
#[cfg(all(test, turmoil, not(shuttle)))]
pub(crate) fn spawn_on<F>(_handle: &tokio::runtime::Handle, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::spawn_local(future)
}

/// Spawns `future` as a shuttle task, whatever runtime `handle` names.
#[cfg(all(test, shuttle))]
pub(crate) fn spawn_on<F>(_handle: &tokio::runtime::Handle, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    shuttle_tokio::spawn(future)
}

/// The depth of the PCT scheduler, one more than the number of times per
/// schedule it lowers the priority of the running thread.
#[cfg(all(test, shuttle))]
const PCT_DEPTH: usize = 3;

/// Runs `scenario` under shuttle's random scheduler, then under its PCT
/// scheduler, for `iterations` schedules each, or the number that
/// `TWINLEAF_SHUTTLE_ITERATIONS` holds. With `TWINLEAF_SHUTTLE_SCHEDULE` set,
/// it replays that schedule alone.
///
/// # Panics
///
/// Panics with the failing schedule when `scenario` panics or deadlocks under
/// one.
#[cfg(all(test, shuttle))]
pub(crate) fn check(iterations: usize, scenario: impl Fn() + Send + Sync + 'static) {
    let scenario = std::sync::Arc::new(scenario);
    let run = move || {
        let scenario = std::sync::Arc::clone(&scenario);
        move || scenario()
    };
    if let Ok(schedule) = std::env::var("TWINLEAF_SHUTTLE_SCHEDULE") {
        return shuttle::replay(run(), schedule.trim());
    }
    let iterations = std::env::var("TWINLEAF_SHUTTLE_ITERATIONS")
        .ok()
        .and_then(|iterations| iterations.parse().ok())
        .unwrap_or(iterations);
    shuttle::check_random(run(), iterations);
    shuttle::check_pct(run(), iterations, PCT_DEPTH);
}

/// Spawns `future` as a task of the runtime it is called from.
///
/// # Panics
///
/// Panics if it is called outside a runtime.
#[cfg(all(test, not(shuttle), not(turmoil)))]
pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future)
}

/// Spawns `future` as a task of the turmoil host it is called from.
///
/// # Panics
///
/// Panics if it is called outside a turmoil host.
#[cfg(all(test, turmoil, not(shuttle)))]
pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::spawn_local(future)
}

/// Spawns `future` as a shuttle task.
#[cfg(all(test, shuttle))]
pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    shuttle_tokio::spawn(future)
}

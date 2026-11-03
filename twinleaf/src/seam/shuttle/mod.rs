//! The primitives of the shuttle tests. Channels, locks, notifications,
//! sleeps and spawns are shuttle's, so each is a point where shuttle's
//! scheduler picks the thread or task that runs next. Sockets are a normal
//! build's, as shuttle does not model them.
//!
//! The channels are those of the [`channel`] module, which differs from
//! flume in these ways.
//!
//! - A receive with a timeout or a deadline never times out, as shuttle has
//!   no clock.
//! - A [`Selector`](chan::Selector) builds, and waiting on it panics.
//! - A `bounded(0)` channel panics, as it has no rendezvous.
//! - An async send that waits for room blocks its task, as a blocking send
//!   blocks its thread.
//! - Waiting receivers are woken together, in no order.
//!
//! The rest of the crate keeps its own primitives.
//!
//! - A tokio `mpsc` channel works, and none of its operations is a
//!   scheduling point.
//! - The `select!` of the session and of the pump are `biased`, so they make
//!   no choice of their own.
//! - A tokio `sleep` or `timeout` other than `chan::sleep_until` needs a
//!   tokio runtime. A `sleep_until` ends at its first poll unless its
//!   deadline is a year or more away.
//! - A supervisor's tasks run as shuttle tasks whatever runtime it names.
//!   Blocking work and the tasks of a `JoinMap` run on that runtime's real
//!   threads, outside shuttle's control, and must not touch a channel or
//!   lock of this module.
//! - A `OnceLock` is std's, and a `get_or_init` that waits on another thread
//!   would stall shuttle's one OS thread. So a test starts a stream's pump,
//!   which fills one, before it starts a second thread.

mod channel;

pub(crate) use super::real::net;

/// Channels, locks, timers and task spawns, on shuttle's scheduler.
pub(crate) mod chan {
    pub(crate) use super::super::real::chan::*;

    pub(crate) use super::channel::{
        bounded, unbounded, Receiver, RecvError, RecvFut, RecvStream, RecvTimeoutError, Selector,
        Sender, TryRecvError, TrySendError,
    };
    pub(crate) use shuttle_tokio::sync::{oneshot, Notify};
    pub(crate) use shuttle_tokio::task::JoinHandle;
    pub(crate) use shuttle_tokio::time::sleep_until;

    use std::future::Future;
    use std::sync::{Mutex, MutexGuard};

    /// The handle of a shuttle task, which aborts the task when it drops.
    pub(crate) struct AbortOnDropHandle<T>(JoinHandle<T>);

    impl<T> AbortOnDropHandle<T> {
        /// Takes `task`, which aborts once this drops.
        pub(crate) fn new(task: JoinHandle<T>) -> AbortOnDropHandle<T> {
            AbortOnDropHandle(task)
        }
    }

    impl<T> Drop for AbortOnDropHandle<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    /// Locks `mutex`, past a panic of another holder. Shuttle runs every
    /// thread and task on one OS thread, where a lock that blocks would stop
    /// them all, so each try is a scheduling point instead, and one that
    /// fails gives the holder its turn.
    pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        loop {
            ::shuttle::thread::yield_now();
            match mutex.try_lock() {
                Ok(guard) => return guard,
                Err(std::sync::TryLockError::Poisoned(poisoned)) => return poisoned.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
        }
    }

    /// Spawns `future` as a shuttle task, whatever runtime `handle` names.
    pub(crate) fn spawn_on<F>(_handle: &tokio::runtime::Handle, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        shuttle_tokio::spawn(future)
    }

    /// Spawns `future` as a shuttle task.
    pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        shuttle_tokio::spawn(future)
    }
}

/// How many schedules each scheduler runs per test, unless
/// `TWINLEAF_SHUTTLE_ITERATIONS` says.
const ITERATIONS: usize = 1000;

/// The depth of the PCT scheduler, one more than the number of times per
/// schedule it lowers the priority of the running thread.
const PCT_DEPTH: usize = 3;

/// The schedule `TWINLEAF_SHUTTLE_SCHEDULE` names, which a run replays
/// alone.
fn replayed() -> Option<String> {
    std::env::var("TWINLEAF_SHUTTLE_SCHEDULE").ok()
}

/// Returns `true` if this run replays one schedule, so a test does not
/// expect what only many schedules reach.
pub(crate) fn replaying() -> bool {
    replayed().is_some()
}

/// Runs `scenario` under shuttle's random scheduler, then under its PCT
/// scheduler, for [`ITERATIONS`] schedules each. With
/// `TWINLEAF_SHUTTLE_SCHEDULE` set, it replays that schedule alone.
///
/// # Panics
///
/// Panics with the failing schedule when `scenario` panics or deadlocks under
/// one.
pub(crate) fn check(scenario: impl Fn() + Send + Sync + 'static) {
    let scenario = std::sync::Arc::new(scenario);
    let run = move || {
        let scenario = std::sync::Arc::clone(&scenario);
        move || scenario()
    };
    if let Some(schedule) = replayed() {
        return ::shuttle::replay(run(), schedule.trim());
    }
    let iterations = std::env::var("TWINLEAF_SHUTTLE_ITERATIONS")
        .ok()
        .and_then(|iterations| iterations.parse().ok())
        .unwrap_or(ITERATIONS);
    ::shuttle::check_random(run(), iterations);
    ::shuttle::check_pct(run(), iterations, PCT_DEPTH);
}

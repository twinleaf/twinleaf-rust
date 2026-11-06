//! The primitives of the turmoil tests. Sockets are turmoil's simulated
//! ones, and each task runs on the turmoil host it is spawned from. The
//! rest of the crate keeps a normal build's channels and locks, which
//! turmoil does not need to see.
//!
//! A host that crashes drops its tasks in the order they started. Tasks of
//! a tokio runtime would drop in an order set by their task ids, which count
//! across the whole process, so a run made twice would crash two ways. For
//! the same reason a `StreamMap` is the copy in `stream_map`.
//!
//! Each run takes a thread of its own from [`seeded`], whose randomness
//! follows the run's seed, so a run made twice with one seed runs the same
//! way.

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("the turmoil tests run on Linux and macOS, whose randomness they seed");

pub(crate) mod net;
mod random;
mod stream_map;
mod tasks;

pub(crate) use net::{bytes_read, live_streams_on};
pub(crate) use random::seeded;
pub(crate) use tasks::{live_tasks, polls};

/// Channels, locks, timers and task spawns, each task on its turmoil host.
pub(crate) mod chan {
    pub(crate) use super::super::real::chan::*;
    pub(crate) use super::stream_map::StreamMap;

    use super::tasks::{counted, Polled};
    use std::future::Future;
    use std::hash::Hash;
    use tokio::runtime::Handle;
    use tokio_util::task::{JoinMap, TaskTracker};

    /// Spawns `future` as a task of the turmoil host it is spawned from,
    /// whatever runtime `handle` names. That host is the one turmoil is
    /// ticking when the call is made, so a task spawned for another host's
    /// runtime runs on this one.
    ///
    /// # Panics
    ///
    /// Panics if it is called outside a turmoil host.
    pub(crate) fn spawn_on<F>(_handle: &Handle, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::task::spawn_local(Polled::new(counted(future)))
    }

    /// Spawns `future` into `map` under `key`, as a task of the turmoil host
    /// it is spawned from, as [`spawn_on`] does.
    pub(crate) fn spawn_in_map<K, F>(
        map: &mut JoinMap<K, F::Output>,
        key: K,
        future: F,
        _handle: &Handle,
    ) where
        K: Hash + Eq,
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        map.spawn_local(key, Polled::new(counted(future)));
    }

    /// Runs `work` as a task of the turmoil host, tracked by `tracker`. A
    /// blocking thread ends in real time, so the step of the simulation that
    /// hears it would vary with the machine's load.
    pub(crate) fn spawn_blocking_on<R>(
        tracker: &TaskTracker,
        work: impl FnOnce() -> R + Send + 'static,
        handle: &Handle,
    ) -> JoinHandle<R>
    where
        R: Send + 'static,
    {
        spawn_on(handle, tracker.track_future(async move { work() }))
    }

    /// Spawns `future` as a task of the turmoil host it is called from.
    ///
    /// # Panics
    ///
    /// Panics if it is called outside a turmoil host.
    pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::task::spawn_local(Polled::new(future))
    }
}

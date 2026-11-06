//! What the tasks of a run do, which tells a host that idles from one that
//! spins, and a host that ended its tasks from one that leaked them.
//!
//! Turmoil's clock stands still while any task of the host it ticks is
//! ready, so a task that is always ready holds one virtual instant forever.
//! The [`SPIN`]th poll in a row at one instant of their host panics, which
//! fails the run at once.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::runtime::{Handle, Id};
use tokio::time::Instant;

/// How many polls of tasks one virtual instant may take, far more than an
/// instant of a host that idles takes.
const SPIN: u64 = 10_000;

/// The count of live tasks of one runtime.
type LiveCount = Arc<AtomicUsize>;

thread_local! {
    /// The polls of tasks on this thread, which runs every host of its run.
    static POLLS: Cell<u64> = const { Cell::new(0) };
    /// The instant of the last poll, on the clock of its host, and how many
    /// polls in a row came at it.
    static STREAK: Cell<Option<(Instant, u64)>> = const { Cell::new(None) };
    /// The count of live tasks of each runtime on this thread.
    static LIVE: RefCell<Vec<(Id, LiveCount)>> = RefCell::default();
}

/// Returns how many times a task spawned on this thread was polled.
pub(crate) fn polls() -> u64 {
    POLLS.with(Cell::get)
}

/// Returns how many tasks of the runtime it is called on live now, of those
/// [`counted`] counts. tokio's own count leaves out the tasks of a
/// `LocalSet`, where every task of a turmoil host runs.
pub(crate) fn live_tasks() -> usize {
    live_count().load(Ordering::SeqCst)
}

/// A task that counts its polls, and panics at the [`SPIN`]th in a row at
/// one virtual instant.
pub(super) struct Polled<F> {
    future: Pin<Box<F>>,
}

impl<F> Polled<F> {
    pub(super) fn new(future: F) -> Polled<F> {
        Polled {
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for Polled<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        POLLS.with(|polls| polls.set(polls.get() + 1));
        let now = Instant::now();
        let streak = match STREAK.get() {
            Some((at, streak)) if at == now => streak + 1,
            _ => 1,
        };
        STREAK.set(Some((now, streak)));
        assert!(
            streak < SPIN,
            "tasks were polled {SPIN} times at one virtual instant, so a host spins"
        );
        self.future.as_mut().poll(cx)
    }
}

/// Returns `future` counted as a live task of the runtime it is made on,
/// from now until it is dropped.
pub(super) fn counted<F: Future>(future: F) -> impl Future<Output = F::Output> {
    struct Live(LiveCount);
    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let count = live_count();
    count.fetch_add(1, Ordering::SeqCst);
    let live = Live(count);
    async move {
        let _live = live;
        future.await
    }
}

/// The count of live tasks of the runtime it is called on.
fn live_count() -> LiveCount {
    let runtime = Handle::current().id();
    LIVE.with(|live| {
        let mut live = live.borrow_mut();
        if let Some((_, count)) = live.iter().find(|(id, _)| *id == runtime) {
            return count.clone();
        }
        let count = LiveCount::default();
        live.push((runtime, count.clone()));
        count
    })
}

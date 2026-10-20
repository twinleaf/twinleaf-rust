//! The crate's internal tokio runtime, which runs its I/O tasks.
//!
//! One multi-thread runtime runs the task of every [port](crate::tio), the
//! tasks of the [proxy](crate::proxy) that hold its links, serve its
//! clients, accept them and stop it, the session and stream pump of each
//! [connection](crate::Connection), and the scans and probes of each
//! [discovery](crate::device::discovery) session. It starts on first use
//! and runs until the process exits. Its worker threads are named
//! `twinleaf-io`. On Windows each runs at the platform's latency critical
//! priority for as long as it lives.
//!
//! Each open port holds an [`Activity`], which keeps the system from idle
//! sleep on macOS and Windows.
//!
//! A [`Supervisor`] spawns tasks, logs a task that panics, and waits for its
//! tasks to end. A caller outside the runtime waits for a task on a
//! flume channel that the task answers on or drops.
//!
//! A child process forked after the runtime started has none of its
//! threads. The child panics on its first use of the runtime.

// The public API is synchronous and may be called from inside an
// application's own tokio runtime, where `block_on`, `blocking_recv` and
// `blocking_send` panic. The channels above block without that check.

use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::process;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::task::{Context, Poll};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::task::{JoinMap, TaskTracker};

/// Returns the handle of the runtime, and starts the runtime on first use.
///
/// # Panics
///
/// Panics if the runtime cannot start, as when the OS refuses a thread. Panics
/// in a child process forked after the runtime started.
pub(crate) fn handle() -> &'static Handle {
    static RUNTIME: OnceLock<(Runtime, u32)> = OnceLock::new();
    let (runtime, started_in) = RUNTIME.get_or_init(|| (start(), process::id()));
    // A forked child has a copy of the runtime but none of its threads, so
    // a task it spawned would never run, and a wait for one never return.
    assert!(
        *started_in == process::id(),
        "twinleaf cannot run in a process forked after twinleaf started. \
         Start child processes with exec, or with the spawn or forkserver \
         method of Python's multiprocessing"
    );
    runtime.handle()
}

/// Builds the runtime.
///
/// # Panics
///
/// Panics if the runtime cannot start.
fn start() -> Runtime {
    let mut builder = Builder::new_multi_thread();
    builder.thread_name("twinleaf-io").enable_all();
    // Blocking threads run `on_thread_start` too, and only workers unpark.
    #[cfg(target_os = "windows")]
    builder.on_thread_unpark(priority::raise);
    builder.build().expect("the I/O runtime starts")
}

/// A boxed future that can move between threads.
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A spawner of tasks on one runtime. It tracks each task it spawns, and logs
/// a task that panics under the target `twinleaf::task`.
#[derive(Clone, Debug)]
pub(crate) struct Supervisor {
    handle: Handle,
    tracker: TaskTracker,
}

impl Supervisor {
    /// Returns a supervisor of tasks on the crate's runtime.
    ///
    /// # Panics
    ///
    /// Panics if the runtime cannot start.
    pub(crate) fn new() -> Supervisor {
        Supervisor::on(handle().clone())
    }

    /// Returns a supervisor of tasks on the runtime of `handle`.
    fn on(handle: Handle) -> Supervisor {
        Supervisor {
            handle,
            tracker: TaskTracker::new(),
        }
    }

    /// Spawns `future` as the task named `task`. Returns the task's handle,
    /// which resolves to `None` if the task panicked.
    pub(crate) fn spawn<F>(&self, task: impl Display, future: F) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let supervised = Supervised::new(task, future);
        self.tracker.spawn_on(supervised, &self.handle)
    }

    /// Spawns a supervised task under its application key. Tracking and panic
    /// reporting use the same supervisor as other tasks.
    pub(crate) fn spawn_in_map<K, F>(
        &self,
        map: &mut JoinMap<K, Option<F::Output>>,
        key: K,
        task: impl Display,
        future: F,
    ) where
        K: Hash + Eq,
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let supervised = Supervised::new(task, future);
        map.spawn_on(key, self.tracker.track_future(supervised), &self.handle);
    }

    /// Runs `future` as the task named `task`, and blocks the calling thread
    /// until the task ends. Returns what the task returns. The caller must
    /// not be a task of the runtime.
    ///
    /// # Errors
    ///
    /// Returns the task's error, and an error if the task panicked.
    pub(crate) fn run<T: Send + 'static>(
        &self,
        task: &str,
        future: impl Future<Output = io::Result<T>> + Send + 'static,
    ) -> io::Result<T> {
        let (answer, answered) = flume::bounded(1);
        self.spawn(task, async move {
            let _ = answer.send(future.await);
        });
        let panicked = || io::Error::other(format!("{task} panicked"));
        answered.recv().unwrap_or_else(|_| Err(panicked()))
    }

    /// Runs `work` on a thread where it may block. Returns the handle of its
    /// task, which resolves to what `work` returns.
    pub(crate) fn spawn_blocking<R>(
        &self,
        work: impl FnOnce() -> R + Send + 'static,
    ) -> JoinHandle<R>
    where
        R: Send + 'static,
    {
        self.tracker.spawn_blocking_on(work, &self.handle)
    }

    /// Waits until every task it spawned has ended, tasks spawned meanwhile
    /// included.
    pub(crate) async fn wait(&self) {
        self.tracker.close();
        self.tracker.wait().await;
    }
}

/// Waits until `deadline`, or forever if there is none.
pub(crate) async fn sleep_until_some(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Waits for the future in `slot`, and empties the slot once it has
/// resolved. Returns what it resolved to. Never returns while the slot is
/// empty.
pub(crate) async fn finish<T>(slot: &mut Option<BoxFuture<'static, T>>) -> T {
    let Some(future) = slot else {
        return std::future::pending().await;
    };
    let output = future.await;
    *slot = None;
    output
}

/// A task's future, which resolves to `None` if the task panics, once it
/// has logged the panic.
struct Supervised<F> {
    /// The task's name, which the log names it by.
    task: String,
    future: Pin<Box<F>>,
}

impl<F> Supervised<F> {
    /// Returns the future of the task named `task`, which runs `future`.
    fn new(task: impl Display, future: F) -> Supervised<F> {
        Supervised {
            task: task.to_string(),
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for Supervised<F> {
    type Output = Option<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.future.as_mut();
        match panic::catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
            Ok(polled) => polled.map(Some),
            Err(panic) => {
                let message = match (panic.downcast_ref::<&str>(), panic.downcast_ref::<String>()) {
                    (Some(message), _) => message,
                    (_, Some(message)) => message.as_str(),
                    _ => "no message",
                };
                log::error!(target: "twinleaf::task", "{} panicked: {message}", self.task);
                Poll::Ready(None)
            }
        }
    }
}

/// A hold on the activity that keeps the system from idle sleep. The first
/// hold begins the activity, and the last to drop ends it. On macOS the
/// activity also asks for low I/O latency. On other platforms than macOS and
/// Windows a hold does nothing.
pub(crate) struct Activity {
    _awake: Arc<Awake>,
}

impl Activity {
    /// Returns a new hold, and begins the activity if no other hold is kept.
    pub(crate) fn hold() -> Activity {
        static AWAKE: Mutex<Weak<Awake>> = Mutex::new(Weak::new());
        Activity::hold_on(&AWAKE)
    }

    /// Returns a new hold on the activity `awake` refers to, and begins the
    /// activity if no hold on it is kept.
    fn hold_on(awake: &Mutex<Weak<Awake>>) -> Activity {
        let mut awake = awake.lock().unwrap_or_else(PoisonError::into_inner);
        let held = awake.upgrade().unwrap_or_else(|| {
            let held = Arc::new(Awake::begin());
            *awake = Arc::downgrade(&held);
            held
        });
        Activity { _awake: held }
    }
}

/// What keeps the system awake, from when it begins until it drops.
#[cfg(target_os = "macos")]
struct Awake {
    _activity: Option<crate::tio::os::macos_helpers::ActivityGuard>,
}

// SAFETY: NSProcessInfo's beginActivity and endActivity are thread safe.
// The guard holds only the activity's token, which it passes back to
// endActivity when the last hold drops, on whichever thread that is.
#[cfg(target_os = "macos")]
unsafe impl Send for Awake {}

// SAFETY: a shared `Awake` gives no access to the token.
#[cfg(target_os = "macos")]
unsafe impl Sync for Awake {}

#[cfg(target_os = "macos")]
impl Awake {
    /// Begins the `NSProcessInfo` activity.
    fn begin() -> Awake {
        use crate::tio::os::macos_helpers::ActivityGuard;
        Awake {
            _activity: ActivityGuard::latency_critical("Twinleaf I/O"),
        }
    }
}

/// What keeps the system awake, from when it begins until it drops.
#[cfg(target_os = "windows")]
struct Awake {
    /// Disconnects when it drops, which ends the thread that keeps the
    /// system awake.
    _keep: std::sync::mpsc::Sender<()>,
}

#[cfg(target_os = "windows")]
impl Awake {
    /// Starts the thread `twinleaf-awake`, which keeps the system awake.
    fn begin() -> Awake {
        use crate::tio::os::windows_helpers::SleepGuard;
        // The execution state that prevents sleep belongs to the thread that
        // sets it, so one thread sets it and waits until it is dropped.
        let (keep, kept) = std::sync::mpsc::channel::<()>();
        let started = std::thread::Builder::new()
            .name("twinleaf-awake".to_string())
            .spawn(move || {
                let _awake = SleepGuard::prevent_sleep(false)
                    .map_err(|e| log::warn!("twinleaf-awake: failed to prevent sleep: {e}"))
                    .ok();
                let _ = kept.recv();
            });
        if let Err(e) = started {
            log::warn!("twinleaf-awake: failed to start: {e}");
        }
        Awake { _keep: keep }
    }
}

/// What keeps the system awake, which is nothing on this platform.
#[cfg(not(any(target_os = "macos", target_os = "windows")))]
struct Awake;

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
impl Awake {
    /// Returns nothing to keep.
    fn begin() -> Awake {
        Awake
    }
}

#[cfg(target_os = "windows")]
mod priority {
    use crate::tio::os::windows_helpers::ActivityGuard;
    use std::cell::OnceCell;

    thread_local! {
        /// The guard that keeps this thread's priority raised until the
        /// thread exits, set once the thread has tried to raise it.
        static GUARD: OnceCell<Option<ActivityGuard>> = const { OnceCell::new() };
    }

    /// Raises the priority of the calling thread for as long as it lives.
    /// Only the first call on a thread tries.
    pub(super) fn raise() {
        GUARD.with(|guard| {
            guard.get_or_init(|| {
                ActivityGuard::latency_critical()
                    .map_err(|e| log::warn!("twinleaf-io: failed to raise thread priority: {e}"))
                    .ok()
            });
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{Activity, Supervisor};
    use std::sync::{Arc, Mutex, Weak};
    use tokio::runtime::Handle;

    impl Supervisor {
        /// Returns a supervisor of tasks on the runtime it is called from.
        ///
        /// # Panics
        ///
        /// Panics if it is called outside a runtime.
        pub(crate) fn current() -> Supervisor {
            Supervisor::on(Handle::current())
        }
    }

    /// A task resolves to what its future returns, and to `None` if it
    /// panics.
    #[tokio::test]
    async fn a_task_that_panics_resolves_to_none() {
        let tasks = Supervisor::current();

        let answered = tasks.spawn("answering", async { 7 });
        let panicked = tasks.spawn("panicking", async { panic!("a test panic") });

        assert_eq!(answered.await.unwrap(), Some(7));
        assert_eq!(panicked.await.unwrap(), None::<()>);
    }

    /// Completion, panic and abort retain the application's task key and
    /// release the supervisor's tracked task.
    #[tokio::test]
    async fn keyed_tasks_keep_their_identity_and_supervision() {
        use std::time::Duration;
        use tokio_util::task::JoinMap;

        let tasks = Supervisor::current();
        let mut running = JoinMap::new();
        tasks.spawn_in_map(&mut running, "answer", "answering", async { 7 });
        tasks.spawn_in_map(&mut running, "panic", "panicking", async {
            panic!("a test panic")
        });
        tasks.spawn_in_map(
            &mut running,
            "abort",
            "waiting",
            std::future::pending::<usize>(),
        );
        assert!(running.abort(&"abort"));
        let completed = async {
            let mut seen = std::collections::HashSet::new();
            while let Some((key, result)) = running.join_next().await {
                assert!(seen.insert(key));
                match key {
                    "answer" => assert_eq!(result.unwrap(), Some(7)),
                    "panic" => assert_eq!(result.unwrap(), None),
                    "abort" => assert!(result.unwrap_err().is_cancelled()),
                    _ => unreachable!(),
                }
            }
            assert_eq!(seen.len(), 3);
        };
        tokio::time::timeout(Duration::from_secs(1), completed)
            .await
            .expect("keyed tasks did not finish");
        tokio::time::timeout(Duration::from_secs(1), tasks.wait())
            .await
            .expect("a keyed task stayed tracked");
    }

    /// The first hold begins the activity, later holds share it, and the
    /// last release ends it. A hold after that begins it again.
    #[test]
    fn the_first_hold_begins_and_the_last_release_ends() {
        let awake = Mutex::new(Weak::new());
        let active = || awake.lock().unwrap().strong_count();

        let first = Activity::hold_on(&awake);
        let second = Activity::hold_on(&awake);
        assert!(Arc::ptr_eq(&first._awake, &second._awake));
        drop(first);
        assert_eq!(active(), 1);
        drop(second);
        assert_eq!(active(), 0);

        let _third = Activity::hold_on(&awake);
        assert_eq!(active(), 1);
    }

    /// A child forked after the runtime started panics on its first use of
    /// the runtime.
    #[cfg(unix)]
    #[test]
    fn a_child_forked_after_the_runtime_started_cannot_use_it() {
        use std::time::{Duration, Instant};
        super::handle();
        // SAFETY: the child only calls `handle`, catches its panic and exits
        // without running destructors.
        let child = unsafe { libc::fork() };
        if child == 0 {
            let used = std::panic::catch_unwind(|| {
                super::handle();
            });
            unsafe { libc::_exit(i32::from(used.is_ok())) };
        }
        assert!(
            child > 0,
            "fork failed: {}",
            std::io::Error::last_os_error()
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut status = 0;
        while unsafe { libc::waitpid(child, &mut status, libc::WNOHANG) } == 0 {
            if Instant::now() > deadline {
                unsafe { libc::kill(child, libc::SIGKILL) };
                unsafe { libc::waitpid(child, &mut status, 0) };
                panic!("the forked child hung");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(libc::WIFEXITED(status), "the forked child did not exit");
        assert_eq!(
            libc::WEXITSTATUS(status),
            0,
            "the forked child used the runtime"
        );
    }
}

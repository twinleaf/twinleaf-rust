//! The hub, which keeps what every page shows, from one connection to the
//! proxy.
//!
//! Every batch goes into one [`History`], whose buffer keeps the newest raw
//! rows of each stream. Each trace, one float column, is a set of
//! [`ColumnProcessor`]s over that buffer. `FpcsLevels` decimates it for the
//! live view at several ratios, each LEVEL_FACTOR times the last, none kept
//! longer than the raw history, and `Welch` estimates its spectrum. Every
//! level goes to every page, which draws each plot from the level that suits
//! its span.
//!
//! A paused view reads a window of the history instead, the rows between
//! two times as about two points a pixel. The history reads it from the
//! rows themselves when they are few enough, and otherwise from the
//! column's decimations at ratios that double. So a window costs by its
//! width in pixels, whatever its span, and is never coarser than its rows
//! decimated for it would be. While some page holds a pause, the history
//! holds a copy of itself, which windows read where the buffer no longer
//! reaches.
//!
//! The hub lives on a thread of its own, which alone touches it. It takes
//! the batches of samples straight from its connection's queue, and the
//! pages' requests from its mailbox as jobs, in the order sent, and a
//! request's answer goes back on a channel of its own. What takes long is
//! done elsewhere: a read takes the rows it needs, which share the buffer's
//! chunks, and copies them off the thread, and a change of decimation
//! replays a frozen copy of the buffer on a thread of its own.
//!
//! The connection's queue is the one place samples wait for the hub, and
//! the one bound on them. It holds 1024 batches, and drops those that come
//! while it is full, so the proxy's connection never waits on the hub. The
//! hub's thread runs at the priority the threads that carry packets do, so
//! other work on a busy machine does not keep it from the queue.
//!
//! Each batch says what was lost before it and where, the rows its device
//! skipped, those lost with its link down, those the proxy dropped for the
//! hub's connection and those the connection's queue dropped. The buffer
//! counts them by stream. The hub's [`Losses`] counts each drop of a
//! device's link, and sends every page both counts each second they grew.
//!
//! The hub keeps the log's lines in its [`Logbook`], those its devices
//! send as log packets and its own, and notes there the time of the newest
//! sample it took of each device, which stamps that device's lines.
//!
//! A run lasts across lost samples, and [`Run::boundaries`] says where they
//! fell. The hub splits the points it sends there, so the page lifts the pen.
//!
//! A hub ends one way, its `End`, whether a page mounts other devices
//! ([`Live::retire`]) or the connection's samples end (the thread that feeds
//! it). Its pages' sockets then close, and the next request starts the hub
//! that takes its place.

use crate::captures::{Captures, Outbox, Want};
use crate::devices::Devices;
use crate::feed::Feed;
use crate::firmware::{Catalog, Firmware, Wanted};
use crate::logbook::{self, Logbook};
use crate::loss::Losses;
use crate::protocol::{self, Frame, Message, MountInfo, Rows, TraceInfo, Window, WindowChunk};
use crate::wire::{encode, encode_spectra, split_at_breaks, Chunk, Spectrum};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};
use twinleaf::data::csv::MergedColumn;
use twinleaf::data::ops::{FpcsLevels, NoSpectrum, Stretch, Welch};
use twinleaf::data::{ColumnKey, ColumnOp, ColumnProcessor, Covered, Gap, History, Run};
use twinleaf::flume::{self, select::SelectError, Selector};
use twinleaf::proxy::Layout;
use twinleaf::{ColumnId, Connection, DeviceRoute, DeviceTree, Receiver, RecvError, SampleBatch};

/// How much of the finest decimation the hub keeps for the live view, as
/// the page does. Each coarser level keeps LEVEL_FACTOR times as much.
const HISTORY_SECONDS: f64 = 120.0;
/// How many times longer each level's windows are than the level before.
const LEVEL_FACTOR: usize = 4;
/// How many levels of decimation each trace has. The coarsest keeps
/// HISTORY_SECONDS·LEVEL_FACTOR⁵, about 34 hours, when the raw history is
/// that long.
const LEVELS: usize = 6;
/// The raw history the hub starts with: this span of each stream's time, and
/// at most this many bytes across every stream.
const RAW_SECONDS: f64 = 600.0;
const RAW_BYTES: usize = 1_000_000_000;
/// How far behind the hub's newest sample a page's view may be, in seconds,
/// and still reach the whole History setting before it: the buffer keeps
/// this much more of each stream than the setting. A page's view ends at its
/// clock less its display delay, at most 0.5 s, and its clock may trail the
/// newest sample by 0.5 s before it is set at once; the rest is for a pause's
/// hold to arrive, by when the buffer has dropped that much more. A view
/// further behind finds the oldest of the setting's span already dropped.
const HISTORY_HEADROOM: f64 = 2.0;
/// How often pending points go out.
const FLUSH: Duration = Duration::from_millis(16);
/// How often spectra are recomputed.
const SPECTRUM_EVERY: Duration = Duration::from_millis(100);
/// How often the hub says what its buffer holds.
const MEMORY_EVERY: Duration = Duration::from_secs(1);
/// How long a job may hold the hub's thread before the log says so.
const SLOW_JOB: Duration = Duration::from_millis(50);
/// Samples per Welch estimate, at most.
const WELCH_WINDOW: usize = 16384;
/// The newest rows of a span a reader's spectrum is estimated from, at
/// most.
const SPAN_SPECTRUM_ROWS: usize = 1 << 20;
/// The span a reader's read takes when it names none, in seconds before
/// the newest row.
const SPAN_SECONDS: f64 = 10.0;
/// The decimation a hub starts with, in points per second per trace.
const POINTS_PER_SECOND: f64 = 500.0;

/// A trace as the page names it: `stream.column` on the root device, or
/// `0/stream.column` on the device at route `/0`.
fn signal_name(info: &TraceInfo) -> String {
    match info.route.as_str() {
        "/" | "" => format!("{}.{}", info.stream, info.column),
        route => format!("{}/{}.{}", &route[1..], info.stream, info.column),
    }
}

struct Trace {
    info: TraceInfo,
    key: ColumnKey,
    live: ColumnProcessor<FpcsLevels>,
    spectrum: ColumnProcessor<Welch>,
    /// Time of the last point sent of each level.
    last_sent: Vec<f64>,
    /// The ordinal of the run the last points came from. Another is a new run.
    run: Option<u64>,
}

/// What every page shows, and the channel each page hears it on.
pub(crate) struct Hub {
    /// The raw rows of every stream, the decimations windows are read from,
    /// and the copy of both held while some page holds a pause.
    history: History,
    /// Every trace, its id its index.
    traces: Vec<Trace>,
    points_per_second: f64,
    /// The traces each page shows as spectra, by trace id, by page. Only
    /// these are estimated.
    spectra_shown: HashMap<u64, BTreeSet<u16>>,
    /// The pages that hold the history, which it holds a copy of from when
    /// the first of them paused until the last lets go.
    holders: BTreeSet<u64>,
    /// What the last `memory` message said, rounded as the page shows it.
    memory_sent: Option<MemoryShown>,
    feed: Arc<Feed>,
    /// The seconds of each stream's time the raw history keeps, which no
    /// level of the decimation keeps longer.
    history_seconds: f64,
    /// The hub's own mailbox, which the replay of a change of decimation
    /// posts its result to. A hub without one, as in a test, replays at once.
    mailbox: Option<flume::Sender<Job>>,
    /// The number of the newest change of decimation, so the replay of an
    /// older one is dropped.
    decimation: u64,
    /// What was lost of each device before the hub held it, which the
    /// watchers of the devices add each drop of a link to, and which sends
    /// the rows the buffer counted lost.
    losses: Arc<Losses>,
    /// The log's lines, which the hub tells the time of each device's
    /// newest sample.
    log: Arc<Logbook>,
}

/// A `memory` message as a page shows it: its bytes, budget, seconds,
/// maximum age and rate, where the oldest trace's history begins, and the
/// held history's bytes and seconds. Another goes out when this changes.
type MemoryShown = (u64, u64, i64, i64, i64, i64, Option<(u64, i64)>);

/// A hub fed by its connection to the proxy, and the devices that connection
/// sees.
pub(crate) struct Live {
    /// Names this hub to the page. A page drops what it holds of a hub as it
    /// hears another's snapshot, and a window request names the hub its
    /// trace id is of.
    pub(crate) id: String,
    /// The hub's thread, which alone holds the hub.
    mailbox: flume::Sender<Job>,
    pub(crate) devices: Arc<Devices>,
    /// What was lost of each device, which the hub's thread sends.
    pub(crate) losses: Arc<Losses>,
    /// The log's lines, which a snapshot holds the newest of.
    pub(crate) log: Arc<Logbook>,
    /// The tree the connection sees, which the page's calls go through.
    pub(crate) tree: DeviceTree,
    /// The captures the pages ask for.
    captures: Arc<Captures>,
    /// The devices' firmware, and the upgrades the pages ask for.
    firmware: Arc<Firmware>,
    /// The number the next page to connect is known by.
    next_page: AtomicU64,
    end: End,
}

/// How a hub ends, shared by the hub and the thread that feeds it.
#[derive(Clone)]
struct End {
    connection: Connection,
    retired: Arc<watch::Sender<bool>>,
}

impl End {
    fn retire(&self) {
        self.retired.send_replace(true);
        self.connection.close();
    }
}

/// The hub's thread has ended, so it answers nothing more.
#[derive(Debug)]
pub(crate) struct Ended;

/// Work for the hub's thread, named for the log when it runs long.
struct Job {
    name: &'static str,
    run: Box<dyn FnOnce(&mut Hub) + Send>,
}

impl Live {
    /// Opens `layout`, or the proxy's default for `None`, and starts the
    /// hub's thread and the watchers of its devices on `runtime`. Blocks
    /// until the proxy has opened it.
    pub(crate) fn start(
        layout: Option<&Layout>,
        runtime: &tokio::runtime::Handle,
    ) -> std::io::Result<Live> {
        let connection = match layout {
            Some(layout) => Connection::open_layout(layout)?,
            None => Connection::connect()?,
        };
        Live::over(connection, layout, runtime)
    }

    /// Starts a hub over `connection`, which opened `layout`, or the
    /// proxy's default for `None`, with the firmware catalog the
    /// environment names.
    pub(crate) fn over(
        connection: Connection,
        layout: Option<&Layout>,
        runtime: &tokio::runtime::Handle,
    ) -> std::io::Result<Live> {
        Live::with_catalog(connection, layout, runtime, Catalog::from_env())
    }

    /// Starts a hub over `connection`, which opened `layout`, or the
    /// proxy's default for `None`, whose upgrades look firmware up in
    /// `catalog`.
    pub(crate) fn with_catalog(
        connection: Connection,
        layout: Option<&Layout>,
        runtime: &tokio::runtime::Handle,
        catalog: Catalog,
    ) -> std::io::Result<Live> {
        let tree = connection.tree(DeviceRoute::root());
        let (mailbox, mail) = flume::unbounded();
        let mut hub = Hub::new();
        hub.mailbox = Some(mailbox.clone());
        let devices = Devices::new(hub.feed.clone());
        let mounts = layout.map(|layout| {
            layout
                .mounts
                .iter()
                .map(|m| MountInfo {
                    url: m.url.clone(),
                    source: m.source.to_string(),
                    prefix: m.prefix.to_string(),
                })
                .collect()
        });
        let losses = hub.losses.clone();
        let log = hub.log.clone();
        logbook::CONSOLE.attach(&log);
        let firmware = Firmware::new(devices.clone(), &tree, runtime, catalog);
        crate::devices::watch(
            &connection,
            mounts,
            devices.clone(),
            losses.clone(),
            log.clone(),
            firmware.clone(),
            runtime,
        );
        runtime.spawn(logbook::read(connection.packets(), log.clone()));
        let captures = Captures::new(devices.clone(), &tree, runtime);
        let end = End {
            connection: connection.clone(),
            retired: Arc::new(watch::channel(false).0),
        };
        let samples = connection.samples();
        let ending = end.clone();
        std::thread::Builder::new()
            .name("twinleaf-web hub".to_string())
            .spawn(move || {
                twinleaf::device::raise_thread_priority();
                run(hub, &samples, &mail);
                ending.retire();
            })?;
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64);
        Ok(Live {
            id: id.to_string(),
            mailbox,
            devices,
            losses,
            log,
            tree,
            captures,
            firmware,
            next_page: AtomicU64::new(0),
            end,
        })
    }

    /// Runs `work` with the hub on its thread, after what was sent before it.
    fn tell(&self, name: &'static str, work: impl FnOnce(&mut Hub) + Send + 'static) {
        // A hub whose thread has ended drops the job, as it has no use for it.
        let _ = self.mailbox.send(Job {
            name,
            run: Box::new(work),
        });
    }

    /// Runs `work` with the hub on its thread, after what was sent before it,
    /// and returns what it gives.
    async fn ask<T: Send + 'static>(
        &self,
        name: &'static str,
        work: impl FnOnce(&mut Hub) -> T + Send + 'static,
    ) -> Result<T, Ended> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        self.tell(name, move |hub| {
            let _ = reply.send(work(hub));
        });
        answer.await.map_err(|_| Ended)
    }

    /// A page that connected to this hub, whose spectra and captures go
    /// once it does.
    pub(crate) fn page(self: &Arc<Live>) -> Page {
        Page {
            id: self.next_page.fetch_add(1, Ordering::Relaxed),
            live: self.clone(),
            outbox: Arc::default(),
        }
    }

    /// Sets the raw history the hub keeps: `seconds` of each stream's time,
    /// in at most `bytes` across every stream. No level of the decimation
    /// keeps more than `seconds` either.
    pub(crate) async fn set_history(&self, seconds: f64, bytes: usize) -> Result<(), Ended> {
        self.ask("set history", move |hub| hub.set_history(seconds, bytes))
            .await
    }

    /// Changes the decimation every page shares. The history is decimated
    /// anew off the hub's thread, and goes to every page once it is.
    pub(crate) async fn set_points_per_second(&self, pps: f64) -> Result<(), Ended> {
        self.ask("set decimation", move |hub| hub.set_points_per_second(pps))
            .await
    }

    /// The raw rows of trace `id` within [t0, t1], to read off the hub's
    /// thread, or `None` for no such trace.
    pub(crate) async fn gather(
        &self,
        id: u16,
        t0: f64,
        t1: f64,
    ) -> Result<Option<Gathered>, Ended> {
        self.ask("gather", move |hub| hub.gather(id, t0, t1)).await
    }

    /// Trace `id` within [t0, t1] for a view `width` pixels wide, to make a
    /// window of off the hub's thread, or `None` for no such trace.
    pub(crate) async fn window(
        &self,
        id: u16,
        t0: f64,
        t1: f64,
        width: usize,
    ) -> Result<Option<WindowRead>, Ended> {
        self.ask("window", move |hub| hub.window(id, t0, t1, width))
            .await
    }

    /// Every raw row of traces `ids` within [t0, t1], to export, as
    /// [`Hub::export`] gathers them.
    pub(crate) async fn export(
        &self,
        ids: Vec<u16>,
        t0: f64,
        t1: f64,
    ) -> Result<Vec<MergedColumn>, Ended> {
        self.ask("export", move |hub| hub.export(&ids, t0, t1))
            .await
    }

    /// Every trace, grouped by device and stream, as [`Hub::signals`]
    /// lists them.
    pub(crate) async fn signals(&self) -> Result<Signals, Ended> {
        self.ask("signals", |hub| hub.signals()).await
    }

    /// The traces of the signals `names`, and the span of them that
    /// `seconds`, `t0` and `t1` name, as [`Hub::span`] reads them in one
    /// read, or why there is none, in words.
    pub(crate) async fn span(
        &self,
        names: Vec<String>,
        seconds: Option<f64>,
        t0: Option<f64>,
        t1: Option<f64>,
    ) -> Result<Result<Spanned, String>, Ended> {
        self.ask("span", move |hub| hub.span(&names, seconds, t0, t1))
            .await
    }

    /// The gaps of trace `id` within [t0, t1], as [`Hub::gaps`] reads them,
    /// or `None` for no such trace.
    pub(crate) async fn gaps(&self, id: u16, t0: f64, t1: f64) -> Result<Option<Vec<Gap>>, Ended> {
        self.ask("gaps", move |hub| hub.gaps(id, t0, t1)).await
    }

    /// Whether a firmware upgrade waits or runs, which another hub taking
    /// this one's place would cut short.
    pub(crate) fn upgrading(&self) -> bool {
        self.firmware.upgrading()
    }

    /// Whether the hub has ended, so another must take its place.
    pub(crate) fn is_retired(&self) -> bool {
        *self.end.retired.borrow()
    }

    /// Ends the hub. Its connection closes and every page on it reconnects.
    pub(crate) fn retire(&self) {
        self.end.retire();
    }

    /// Resolves once the hub is retired.
    pub(crate) fn retired(&self) -> watch::Receiver<bool> {
        self.end.retired.subscribe()
    }

    /// What a page needs to show this hub from scratch, and the receiver of
    /// everything sent after it. `behind` is how many changes the page fell
    /// behind, when the snapshot comes in their place, and 0 as the page
    /// connects. The hub's thread takes both in one job, so no point is
    /// missed or doubled. The messages of the devices and of what was lost
    /// each replace the last of their kind, and a line of the log is known
    /// by its number, so one heard twice does no harm, and they are taken
    /// after subscribing.
    pub(crate) async fn snapshot(
        &self,
        behind: u64,
    ) -> Result<(Vec<Frame>, broadcast::Receiver<Arc<Frame>>), Ended> {
        let (hub, seq, rx) = self
            .ask("snapshot", |hub| {
                let (seq, rx) = hub.feed.subscribe();
                (hub.snapshot(), seq, rx)
            })
            .await?;
        let mut frames = vec![Message::Hub {
            id: self.id.clone(),
            seq,
            behind,
        }
        .frame()];
        frames.extend(hub);
        frames.extend(self.devices.snapshot());
        frames.extend(self.losses.snapshot());
        frames.extend(self.log.snapshot());
        Ok((frames, rx))
    }
}

impl Live {
    /// The frames a page is sent for `heard`, what its receiver of changes
    /// gave it: the change, or, for a page that fell so far behind that the
    /// channel dropped changes it had not read, a snapshot in their place,
    /// which says how many they were. `changes` then hears what comes after
    /// the snapshot. `None` once the hub has ended.
    pub(crate) async fn frames_for(
        &self,
        heard: Result<Arc<Frame>, broadcast::error::RecvError>,
        changes: &mut broadcast::Receiver<Arc<Frame>>,
    ) -> Option<Vec<Arc<Frame>>> {
        match heard {
            Ok(frame) => Some(vec![frame]),
            Err(broadcast::error::RecvError::Lagged(behind)) => {
                log::debug!("A page fell {behind} frames behind and is sent a snapshot");
                let (snapshot, again) = self.snapshot(behind).await.ok()?;
                *changes = again;
                Some(snapshot.into_iter().map(Arc::new).collect())
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}

/// A page on a socket of the hub.
pub(crate) struct Page {
    id: u64,
    live: Arc<Live>,
    /// The captures the page asked for and has not been sent.
    outbox: Arc<Outbox>,
}

impl Page {
    /// Shows the page the spectra of `traces`, in place of those it showed.
    pub(crate) fn show_spectra(&self, traces: BTreeSet<u16>) {
        let id = self.id;
        self.live
            .tell("show spectra", move |hub| hub.show_spectra(id, traces));
    }

    /// Holds the history as it is now while `on`, for the page's paused
    /// plots, or lets it go.
    pub(crate) fn hold(&self, on: bool) {
        let id = self.id;
        self.live.tell("hold", move |hub| hub.hold(id, on));
    }

    /// Asks `want` of the capture `rpc` of the device at `route`.
    pub(crate) fn capture(&self, route: &str, rpc: &str, want: Want) {
        self.live
            .captures
            .ask(self.id, &self.outbox, route, rpc, want);
    }

    /// Waits for captures the page asked for, and takes the frames to send
    /// it, the newest of each RPC.
    pub(crate) async fn captures(&self) -> Vec<Arc<Frame>> {
        self.outbox.take().await
    }

    /// Looks the devices' firmware up in the catalog.
    pub(crate) fn check(&self) {
        self.live.firmware.check();
    }

    /// Whether an upgrade may be asked of the device at `route` now, before
    /// the page sends the file it installs.
    pub(crate) fn may_upgrade(&self, route: &str) -> Result<(), String> {
        self.live.firmware.may_upgrade(route)
    }

    /// Asks for an upgrade of the device at `route` that installs `wanted`.
    pub(crate) fn upgrade(&self, route: &str, wanted: Wanted) -> Result<(), String> {
        self.live.firmware.upgrade(route, wanted)
    }

    /// Takes back or clears the upgrade of the device at `route`.
    pub(crate) fn cancel(&self, route: &str) -> Result<(), String> {
        self.live.firmware.cancel(route)
    }

    /// Runs the failed upgrade of the device at `route` again.
    pub(crate) fn retry(&self, route: &str) -> Result<(), String> {
        self.live.firmware.retry(route)
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        self.show_spectra(BTreeSet::new());
        self.hold(false);
        self.live.captures.forget(self.id);
    }
}

/// What the hub's thread takes next.
enum Next {
    Samples(Result<SampleBatch, RecvError>),
    Job(Result<Job, flume::RecvError>),
}

/// Runs the hub until its samples end: each batch of `samples` and each job
/// of `mail` as they come, and every FLUSH, SPECTRUM_EVERY and MEMORY_EVERY
/// the new points, the spectra and what the buffer holds. Batches that
/// wait are taken one after another until the next flush is due, and then
/// a job that waits has its turn. Only this thread touches the hub, and
/// whatever it does delays everything after it, so each job must be short.
/// One that is not is logged.
fn run(mut hub: Hub, samples: &Receiver<SampleBatch>, mail: &flume::Receiver<Job>) {
    let mut last_flush = Instant::now();
    let mut last_spectrum = Instant::now();
    let mut last_memory = Instant::now();
    loop {
        let next = Selector::new()
            .recv(samples.as_flume(), |queued| {
                Next::Samples(samples.resolve(queued))
            })
            .recv(mail, Next::Job)
            .wait_timeout(Duration::from_millis(5));
        match next {
            Ok(Next::Samples(first)) => {
                let mut taken = first.map(Some);
                loop {
                    match taken {
                        Ok(Some(batch)) => timed("ingest", || hub.ingest(&batch)),
                        Ok(None) => break,
                        // The batch after says what the queue dropped of its
                        // stream.
                        Err(RecvError::Lagged(_)) => {}
                        Err(error) => {
                            log::warn!("The web page's samples ended: {error}");
                            return;
                        }
                    }
                    if last_flush.elapsed() >= FLUSH {
                        break;
                    }
                    taken = samples.try_recv();
                }
            }
            Ok(Next::Job(Ok(job))) => timed(job.name, || (job.run)(&mut hub)),
            Ok(Next::Job(Err(flume::RecvError::Disconnected))) => return,
            Err(SelectError::Timeout) => {}
        }
        // Each period runs from the start of the last work, so the work's own
        // time does not stretch it.
        if last_flush.elapsed() >= FLUSH {
            last_flush = Instant::now();
            timed("flush", || hub.flush());
        }
        if last_spectrum.elapsed() >= SPECTRUM_EVERY {
            last_spectrum = Instant::now();
            timed("spectra", || hub.spectra());
        }
        if last_memory.elapsed() >= MEMORY_EVERY {
            last_memory = Instant::now();
            timed("memory", || hub.send_memory());
            hub.losses.send_changed(hub.history.buffer());
        }
    }
}

/// Runs `work` on the hub's thread, and logs it when it takes longer than
/// SLOW_JOB.
fn timed(name: &str, work: impl FnOnce()) {
    let start = Instant::now();
    work();
    let took = start.elapsed();
    if took >= SLOW_JOB {
        log::warn!(
            "The web page's hub spent {took:?} on {name}, which held up everything after it"
        );
    }
}

fn ratio_for(rate: f64, pps: f64) -> usize {
    ((rate / pps).round() as usize).max(1)
}

/// The times of the run's gaps, oldest first.
fn gap_times(run: Option<&Run>) -> Vec<f64> {
    run.map(|r| r.boundaries().map(|gap| gap.after).collect())
        .unwrap_or_default()
}

impl Hub {
    fn new() -> Hub {
        let feed = Feed::new();
        let log = Logbook::new(feed.clone());
        let mut history = History::new(usize::MAX);
        history.set_retention(Some(RAW_SECONDS + HISTORY_HEADROOM), Some(RAW_BYTES));
        Hub {
            history,
            traces: Vec::new(),
            points_per_second: POINTS_PER_SECOND,
            spectra_shown: HashMap::new(),
            holders: BTreeSet::new(),
            memory_sent: None,
            losses: Losses::new(feed.clone(), log.clone()),
            log,
            feed,
            history_seconds: RAW_SECONDS,
            mailbox: None,
            decimation: 0,
        }
    }

    /// The levels a trace of `rate` is decimated to, at the hub's decimation.
    fn levels_for(&self, rate: f64) -> FpcsLevels {
        let mut levels = FpcsLevels::new(
            ratio_for(rate, self.points_per_second),
            LEVEL_FACTOR,
            LEVELS,
            HISTORY_SECONDS,
        );
        levels.set_retention(HISTORY_SECONDS, self.history_seconds);
        levels
    }

    fn ingest(&mut self, batch: &SampleBatch) {
        // An empty batch adds no rows, and the history counts what it says
        // was lost.
        let new = self.history.process_batch(batch);
        let Some(newest) = batch.timestamps().last() else {
            return;
        };
        self.log.heard(batch.route(), *newest);
        for key in new {
            self.register(batch, key);
        }
    }

    /// Adds a trace for `key`, a float column of `batch` not seen before.
    fn register(&mut self, batch: &SampleBatch, key: ColumnKey) {
        let schema = batch.schema();
        let column = schema.column(key.column_id).expect("a column of the batch");
        let rate = batch.metadata().rate_hz();
        let info = TraceInfo {
            id: self.traces.len() as u16,
            route: key.route.to_string(),
            stream: schema.stream().name().to_string(),
            column: column.name().to_string(),
            units: column.units().to_string(),
            rate,
            epoch: batch.metadata().segment().epoch().to_string(),
        };
        let window = ((rate * 8.0) as usize).clamp(256, WELCH_WINDOW);
        let decimation = self.levels_for(rate);
        self.feed.message(&Message::Trace {
            trace: info.clone(),
        });
        self.traces.push(Trace {
            info,
            key,
            live: ColumnProcessor::new(key, decimation),
            spectrum: ColumnProcessor::new(key, Welch::new(window, rate)),
            last_sent: vec![f64::NEG_INFINITY; LEVELS],
            run: None,
        });
    }

    /// Catches each trace's decimators up with the buffer and sends the new
    /// points of every level. The decimations windows are read from keep
    /// what the buffer still holds rows of.
    fn flush(&mut self) {
        self.history.catch_up();
        let buffer = self.history.buffer();
        let mut chunks = Vec::new();
        for trace in &mut self.traces {
            let Some(run) = buffer.run(&trace.key.stream_key()) else {
                continue;
            };
            let ordinal = run.ordinal();
            let new_run = trace.run.is_some_and(|last| last != ordinal);
            trace.run = Some(ordinal);
            let breaks = gap_times(Some(run));
            let levels = trace.live.catch_up(buffer);
            for (level, (fpcs, last_sent)) in levels.iter().zip(&mut trace.last_sent).enumerate() {
                if new_run {
                    // The processor replayed a new run, whose times may start over.
                    *last_sent = f64::NEG_INFINITY;
                }
                let out = fpcs.output();
                let start = out.partition_point(|&(t, _)| t <= *last_sent);
                if start == out.len() {
                    continue;
                }
                let mut split = split_at_breaks(
                    trace.info.id,
                    level as u8,
                    *last_sent,
                    &out[start..],
                    &breaks,
                );
                if new_run {
                    if let Some(first) = split.first_mut() {
                        first.brk = true;
                    }
                }
                *last_sent = out.last().unwrap().0;
                chunks.extend(split);
            }
        }
        if !chunks.is_empty() {
            self.feed.binary(|seq| encode(&chunks, seq));
        }
    }

    /// Shows `traces` as spectra to `page`, in place of what it showed, or
    /// none once it goes.
    fn show_spectra(&mut self, page: u64, traces: BTreeSet<u16>) {
        if traces.is_empty() {
            self.spectra_shown.remove(&page);
        } else {
            self.spectra_shown.insert(page, traces);
        }
    }

    /// Estimates and sends the spectrum of each trace some page shows.
    fn spectra(&mut self) {
        let shown: BTreeSet<u16> = self.spectra_shown.values().flatten().copied().collect();
        let spectra: Vec<Spectrum> = self
            .traces
            .iter_mut()
            .filter(|trace| shown.contains(&trace.info.id))
            .filter_map(|trace| {
                let estimate = trace
                    .spectrum
                    .catch_up(self.history.buffer())
                    .as_ref()
                    .ok()?;
                Some(Spectrum {
                    id: trace.info.id,
                    bins: &estimate.points,
                    floor: estimate.noise_floor,
                })
            })
            .collect();
        if !spectra.is_empty() {
            self.feed.binary(|seq| encode_spectra(&spectra, seq));
        }
    }

    /// Every decimated point already sent, split at breaks.
    fn history(&self) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        for trace in &self.traces {
            let breaks = gap_times(self.history.run(&trace.key.stream_key()));
            for (level, (fpcs, &last_sent)) in
                trace.live.output().iter().zip(&trace.last_sent).enumerate()
            {
                let out = fpcs.output();
                let end = out.partition_point(|&(t, _)| t <= last_sent);
                chunks.extend(split_at_breaks(
                    trace.info.id,
                    level as u8,
                    f64::NEG_INFINITY,
                    &out[..end],
                    &breaks,
                ));
            }
        }
        chunks
    }

    /// The decimation as the page hears it: points per second per trace, and
    /// the seconds each level keeps.
    fn config(&self) -> Message {
        Message::Config {
            pps: self.points_per_second,
            levels: (0..LEVELS)
                .map(|k| {
                    (HISTORY_SECONDS * LEVEL_FACTOR.pow(k as u32) as f64).min(self.history_seconds)
                })
                .collect(),
        }
    }

    /// The hub's part of a page's snapshot, its traces and their history.
    fn snapshot(&self) -> Vec<Frame> {
        let mut frames: Vec<Frame> = self
            .traces
            .iter()
            .map(|t| {
                Message::Trace {
                    trace: t.info.clone(),
                }
                .frame()
            })
            .collect();
        let chunks = self.history();
        if !chunks.is_empty() {
            frames.push(Frame::Binary(encode(&chunks, 0)));
        }
        frames.push(self.config().frame());
        frames.push(self.memory().frame());
        frames
    }

    /// Holds the history as it is now for page `page` while `on`, or lets
    /// it go once no page holds it. While held, the buffer goes on as it
    /// would unheld, and the held history keeps what it drops in the room
    /// it leaves.
    fn hold(&mut self, page: u64, on: bool) {
        let was = !self.holders.is_empty();
        if on {
            self.holders.insert(page);
        } else {
            self.holders.remove(&page);
        }
        match (was, !self.holders.is_empty()) {
            (false, true) => self.history.hold(),
            (true, false) => self.history.release(),
            _ => return,
        }
        self.memory_sent = None;
        self.send_memory();
    }

    /// What the buffer holds and may hold, as a `memory` message.
    fn memory(&self) -> Message {
        let history = &self.history;
        let rate = (history.stream_keys())
            .filter_map(|key| history.run(key))
            .map(|run| run.row_bytes() as f64 * run.metadata().rate_hz())
            .sum();
        // The seconds go up to the History setting, as the headroom is not
        // history a page counts.
        let held = history.held_seconds().map(|seconds| protocol::Held {
            bytes: history.held_bytes() as u64,
            seconds: seconds.min(self.history_seconds),
        });
        let oldest = (self.traces.iter())
            .filter_map(|trace| Some((trace.info.id, history.oldest(&trace.key.stream_key())?)))
            .collect();
        Message::Memory {
            bytes: history.bytes() as u64,
            budget: history.budget().unwrap_or(0) as u64,
            seconds: history.seconds().min(self.history_seconds),
            max_age: self.history_seconds,
            rate,
            held,
            oldest,
        }
    }

    /// Sets the raw history the buffer keeps, `seconds` and
    /// HISTORY_HEADROOM more of each stream within `bytes`, dropping what no
    /// longer fits at once, and the most each level of the decimation keeps,
    /// and tells every page. The held history follows, as
    /// [`History::set_retention`] says.
    fn set_history(&mut self, seconds: f64, bytes: usize) {
        self.history_seconds = seconds;
        for trace in &mut self.traces {
            trace.live.op_mut().set_retention(HISTORY_SECONDS, seconds);
        }
        self.feed.message(&self.config());
        let max_age = Some(seconds + HISTORY_HEADROOM);
        self.history.set_retention(max_age, Some(bytes));
        self.memory_sent = None;
        self.send_memory();
    }

    /// Tells every page what the buffer holds, when that changed.
    fn send_memory(&mut self) {
        let message = self.memory();
        let Message::Memory {
            bytes,
            budget,
            seconds,
            max_age,
            rate,
            held,
            oldest,
        } = &message
        else {
            unreachable!()
        };
        let shown = (
            *bytes,
            *budget,
            seconds.round() as i64,
            max_age.round() as i64,
            rate.round() as i64,
            // Where each trace's history begins moves as it is dropped.
            oldest.values().map(|t| t.floor() as i64).min().unwrap_or(0),
            // What the held history holds beyond the buffer grows as the
            // buffer moves on, while all else stays as it was.
            held.map(|held| (held.bytes, held.seconds.round() as i64)),
        );
        if self.memory_sent != Some(shown) {
            self.memory_sent = Some(shown);
            self.feed.message(&message);
        }
    }

    /// Changes the decimation. Each trace's levels are decimated anew from
    /// every raw row the buffer keeps, which takes long, so with a mailbox
    /// that runs on a thread of its own over a frozen copy of the buffer, and
    /// posts the new levels back. Until then the old levels go on. A trace
    /// that comes in the meantime is decimated anew from the start.
    fn set_points_per_second(&mut self, pps: f64) {
        self.points_per_second = pps;
        self.decimation += 1;
        let decimation = self.decimation;
        let frozen = self.history.buffer().frozen();
        let fresh: Vec<ColumnProcessor<FpcsLevels>> = self
            .traces
            .iter()
            .map(|trace| ColumnProcessor::new(trace.key, self.levels_for(trace.info.rate)))
            .collect();
        let replay = move || -> Vec<ColumnProcessor<FpcsLevels>> {
            fresh
                .into_iter()
                .map(|mut levels| {
                    levels.catch_up(&frozen);
                    levels
                })
                .collect()
        };
        let Some(mailbox) = self.mailbox.clone() else {
            let levels = replay();
            return self.decimated(decimation, levels);
        };
        let replaying = std::thread::Builder::new()
            .name("twinleaf-web decimation".to_string())
            .spawn(move || {
                let levels = replay();
                let _ = mailbox.send(Job {
                    name: "decimated",
                    run: Box::new(move |hub| hub.decimated(decimation, levels)),
                });
            });
        if let Err(error) = replaying {
            log::warn!("The web page's hub could not decimate anew: {error}");
        }
    }

    /// Puts `levels`, decimated anew for change `decimation`, in place of
    /// each trace's, caught up with the rows that came since, and sends every
    /// page the history again. A replay a later change overtook is dropped.
    fn decimated(&mut self, decimation: u64, levels: Vec<ColumnProcessor<FpcsLevels>>) {
        if decimation != self.decimation {
            return;
        }
        for (trace, mut live) in self.traces.iter_mut().zip(levels) {
            let caught_up = live.catch_up(self.history.buffer());
            for (fpcs, last_sent) in caught_up.iter().zip(&mut trace.last_sent) {
                *last_sent = fpcs.output().last().map_or(f64::NEG_INFINITY, |p| p.0);
            }
            trace.live = live;
        }
        self.feed.message(&Message::Clear);
        let chunks = self.history();
        if !chunks.is_empty() {
            self.feed.binary(|seq| encode(&chunks, seq));
        }
        self.feed.message(&self.config());
    }

    /// The raw rows of trace `id` within [t0, t1], from the history held for
    /// paused plots where the buffer no longer reaches, to read once the hub
    /// is released. The batches share the history's chunks, so gathering
    /// them copies no rows.
    fn gather(&self, id: u16, t0: f64, t1: f64) -> Option<Gathered> {
        let trace = self.traces.get(id as usize)?;
        let key = trace.key.stream_key();
        self.history.run(&key)?;
        Some(Gathered {
            column: trace.key.column_id,
            oldest: self.history.oldest(&key),
            batches: self.history.rows(&key, t0, t1),
        })
    }

    /// Trace `id` within [t0, t1] for a view `width` pixels wide, at about
    /// two points a pixel, to make a window of once the hub is released.
    /// Fewer than 128 rows a point are the rows themselves, to decimate.
    /// More are the points of the level of the trace's decimations whose
    /// windows are the longest no longer than that, so no coarser than the
    /// rows decimated would be, with each end's own row. The held history's
    /// part is read from the decimations held with it.
    fn window(&mut self, id: u16, t0: f64, t1: f64, width: usize) -> Option<WindowRead> {
        let key = self.traces.get(id as usize)?.key;
        let window = self.history.window(&key, t0, t1, (2 * width).max(64))?;
        Some(WindowRead {
            t0,
            t1,
            ratio: window.ratio,
            spans: window.stretches,
        })
    }

    /// Every raw row of traces `ids` within [t0, t1], from the history held
    /// for paused plots where the buffer no longer reaches, to export. The
    /// batches share the history's chunks, so gathering them copies no rows.
    /// An id of no trace is passed over.
    fn export(&self, ids: &[u16], t0: f64, t1: f64) -> Vec<MergedColumn> {
        ids.iter()
            .filter_map(|&id| {
                let trace = self.traces.get(id as usize)?;
                Some(MergedColumn {
                    name: signal_name(&trace.info),
                    units: trace.info.units.clone(),
                    unix: trace.info.epoch == "unix",
                    column: trace.key.column_id,
                    batches: self.history.rows(&trace.key.stream_key(), t0, t1),
                })
            })
            .collect()
    }

    /// Every trace, by id, with the name the page gives it and the time of
    /// its stream's newest row the buffer holds.
    fn signals(&self) -> Signals {
        let mut signals = Signals::new();
        for trace in &self.traces {
            let signal = Signal {
                name: signal_name(&trace.info),
                info: trace.info.clone(),
            };
            let group = (trace.info.route.clone(), trace.info.stream.clone());
            signals.entry(group).or_default().push(signal);
        }
        signals
    }

    /// The traces of the signals `names`, and the span of them `seconds`,
    /// `t0` and `t1` name, whose newest row is the newest of any of them.
    /// It is `t0` to `t1`, from `t0` to the newest row, the `seconds`
    /// before `t1`, or the newest `seconds`, ten when none are named. Why
    /// there is none is in words.
    fn span(
        &self,
        names: &[String],
        seconds: Option<f64>,
        t0: Option<f64>,
        t1: Option<f64>,
    ) -> Result<Spanned, String> {
        if names.is_empty() {
            return Err("name at least one signal".into());
        }
        if self.traces.is_empty() {
            return Err("the hub has no signals yet".into());
        }
        let mut ids = Vec::new();
        let mut newest = f64::NEG_INFINITY;
        for name in names {
            let trace = (self.traces.iter())
                .find(|trace| signal_name(&trace.info) == *name)
                .ok_or_else(|| {
                    format!("there is no signal {name}, and list_devices names those there are")
                })?;
            let run = self.history.run(&trace.key.stream_key());
            newest = newest.max(
                run.and_then(Run::last_timestamp)
                    .unwrap_or(f64::NEG_INFINITY),
            );
            ids.push(trace.info.id);
        }
        if !newest.is_finite() {
            return Err(format!("the hub holds no rows of {}", names.join(", ")));
        }
        let seconds = seconds.unwrap_or(SPAN_SECONDS);
        if !(seconds.is_finite() && seconds > 0.0) {
            return Err(format!("seconds must be a positive number, not {seconds}"));
        }
        let (t0, t1) = match (t0, t1) {
            (Some(t0), Some(t1)) => (t0, t1),
            (Some(t0), None) => (t0, newest),
            (None, Some(t1)) => (t1 - seconds, t1),
            (None, None) => (newest - seconds, newest),
        };
        if !(t0.is_finite() && t1.is_finite() && t0 < t1) {
            return Err(format!(
                "the span from {t0} to {t1} holds no time, as t0 must come before t1"
            ));
        }
        Ok((ids, t0, t1))
    }

    /// The gaps of trace `id` within [t0, t1], each between two rows an
    /// export of the span holds, from the run's own record, as
    /// [`History::gaps`] reads them. `None` for no such trace.
    fn gaps(&self, id: u16, t0: f64, t1: f64) -> Option<Vec<Gap>> {
        let trace = self.traces.get(id as usize)?;
        Some(self.history.gaps(&trace.key.stream_key(), t0, t1))
    }
}

/// The traces of a span and its start and end, as [`Hub::span`] reads them.
pub(crate) type Spanned = (Vec<u16>, f64, f64);

/// Every trace as a reader of the hub names it, grouped by the route of its
/// device and the name of its stream, each group in the order of its
/// traces.
pub(crate) type Signals = BTreeMap<(String, String), Vec<Signal>>;

/// The Welch estimate of the newest rows of `column` since its last break,
/// at most [`SPAN_SPECTRUM_ROWS`] of them, or why there is none, and the
/// rows it covers. The estimate is over every row it is fed.
pub(crate) fn spectrum_of_rows(
    column: &MergedColumn,
) -> (
    Result<twinleaf::data::ops::Spectrum, NoSpectrum>,
    Option<Covered>,
) {
    let rate = (column.batches.first()).map_or(0.0, |batch| batch.metadata().rate_hz());
    let rows = newest_rows(&column.batches, SPAN_SPECTRUM_ROWS);
    let mut welch = Welch::new(rows.iter().map(SampleBatch::len).sum(), rate);
    let (estimate, covered) = welch.feed_rows(&rows, column.column);
    (estimate.clone(), covered)
}

/// The newest `n` rows of `batches`, in order.
fn newest_rows(batches: &[SampleBatch], n: usize) -> Vec<SampleBatch> {
    let mut left = n;
    let mut newest = Vec::new();
    for batch in batches.iter().rev() {
        if left == 0 {
            break;
        }
        let taken = batch.len().min(left);
        newest.push(batch.slice(batch.len() - taken..batch.len()));
        left -= taken;
    }
    newest.reverse();
    newest
}

/// A trace as a reader of the hub names it.
pub(crate) struct Signal {
    /// The name the page gives it, as `stream.column` or `0/stream.column`.
    pub(crate) name: String,
    /// Its id, device, stream, units and rate.
    pub(crate) info: TraceInfo,
}

/// The raw rows of one trace within a range of time, gathered on the hub's
/// thread and read off it. Reading copies every row, which would hold up
/// the samples behind it.
pub(crate) struct Gathered {
    column: ColumnId,
    /// The oldest time the history holds of the trace.
    oldest: Option<f64>,
    /// The held history's rows, then the buffer's, which share their chunks.
    batches: Vec<SampleBatch>,
}

/// One trace within [t0, t1], gathered on the hub's thread for a window to
/// be made of off it, as the copying and decimating would hold up the
/// samples behind it.
pub(crate) struct WindowRead {
    t0: f64,
    t1: f64,
    /// The rows a point of the window stands for.
    ratio: usize,
    /// The held history's part, then the buffer's.
    spans: Vec<Stretch>,
}

impl WindowRead {
    /// The window: its points, split at each of a run's gaps and where the
    /// held history ends and the buffer begins, which may be a hole.
    pub(crate) fn window(self) -> Window {
        let mut chunks = Vec::new();
        for span in &self.spans {
            let segments = span.points();
            let kept = segments.iter().filter(|segment| !segment.is_empty());
            chunks.extend(kept.map(|points| WindowChunk {
                x: points.iter().map(|p| p.0).collect(),
                y: points.iter().map(|p| p.1).collect(),
            }));
        }
        Window {
            t0: self.t0,
            t1: self.t1,
            ratio: self.ratio,
            chunks,
        }
    }
}

impl Gathered {
    /// Every row, none decimated; or, when there are more than `limit`, only
    /// how many. With them, the oldest time the history still holds of the
    /// trace.
    pub(crate) fn rows(self, limit: usize) -> Rows {
        let count = self.batches.iter().map(SampleBatch::len).sum();
        let (mut x, mut y) = (Vec::new(), Vec::new());
        if count <= limit {
            for batch in &self.batches {
                if let Some(values) = batch.column(self.column) {
                    x.extend_from_slice(batch.timestamps());
                    y.extend(values.to_f64().iter());
                }
            }
        }
        Rows {
            count,
            oldest: self.oldest,
            x,
            y,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::decode;
    use twinleaf::data::ops::Fpcs;
    use twinleaf::data::PacketParser;
    use twinleaf::data::StreamKey;
    use twinleaf::proto::data::{self as data, DataType};
    use twinleaf::proto::sync::Epoch;
    use twinleaf::tio::Packet;
    use twinleaf::{SegmentId, SessionId, StreamId};

    /// A stream of one f32 column at 100 Hz, as its device describes it.
    fn parser() -> PacketParser {
        parser_at(100)
    }

    /// A stream of one f32 column at `rate` Hz, as its device describes it.
    fn parser_at(rate: u32) -> PacketParser {
        let mut parser = PacketParser::new(false);
        describe(&mut parser, rate);
        parser
    }

    /// Tells `parser` of the stream, at `rate` Hz, as its device describes it.
    fn describe(parser: &mut PacketParser, rate: u32) {
        let (id, route) = (StreamId::new(1), DeviceRoute::root());
        for record in [
            data::Metadata::Device(data::Device {
                session: SessionId::new(1),
                n_streams: 1,
                name: "sim",
                serial: "S1",
                firmware: "1",
                protocol: 0,
            }),
            data::Metadata::Stream(data::Stream {
                stream_id: id,
                n_columns: 1,
                n_segments: 2,
                sample_size: 4,
                buf_samples: 128,
                name: "field",
            }),
            segment(0, rate),
            data::Metadata::Column(data::Column {
                stream_id: id,
                index: ColumnId::new(0),
                data_type: DataType::F32,
                name: "x",
                units: "nT",
                description: "",
            }),
        ] {
            parser
                .push_packet(&Packet::metadata(record, data::MetadataFlags::UPDATE, route).unwrap())
                .unwrap();
        }
    }

    /// Samples `first..first + n` of the stream, each its own number, in
    /// packets of 100.
    fn samples(parser: &mut PacketParser, first: u32, n: u32) -> Vec<SampleBatch> {
        valued(parser, 100, first, n, |i| i as f32)
    }

    /// The whole seconds of a `rate` Hz stream one segment numbers, as a
    /// sample's number is 24 bits.
    fn segment_seconds(rate: u32) -> u32 {
        (1 << 24) / rate
    }

    /// The record of the stream's segment `k`, which starts where segment
    /// `k - 1` ends.
    fn segment(k: u32, rate: u32) -> data::Metadata<'static> {
        data::Metadata::Segment(data::Segment {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new((k % 2) as u8),
            flags: data::SegmentFlags::default(),
            epoch: Epoch::UNIX,
            timeref_serial: "",
            timeref_session: SessionId::new(1),
            start_time: k * segment_seconds(rate),
            sampling_rate: rate,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: data::FilterType::NONE,
        })
    }

    /// Samples `first..first + n` of the stream at `rate` Hz, sample `i`
    /// worth `value(i)`, in packets of 100. The stream rolls over to its
    /// next segment as a device does, so a batch fed here must follow the
    /// one before it across a segment's end.
    fn valued(
        parser: &mut PacketParser,
        rate: u32,
        first: u32,
        n: u32,
        value: impl Fn(u32) -> f32,
    ) -> Vec<SampleBatch> {
        let per_segment = segment_seconds(rate) * rate;
        let route = DeviceRoute::root();
        let mut start = first;
        while start < first + n {
            let (k, number) = (start / per_segment, start % per_segment);
            if k > 0 && number == 0 {
                let record = Packet::metadata(segment(k, rate), data::MetadataFlags::UPDATE, route);
                parser.push_packet(&record.unwrap()).unwrap();
            }
            let end = (start + 100).min(first + n).min((k + 1) * per_segment);
            let data: Vec<u8> = (start..end).flat_map(|i| value(i).to_le_bytes()).collect();
            let packet = Packet::samples(1, (k % 2) as u8, number, &data, route);
            parser.push_packet(&packet.unwrap()).unwrap();
            start = end;
        }
        std::iter::from_fn(|| parser.pop_batch()).collect()
    }

    /// Every point of every frame `rx` holds, by trace id, and the breaks.
    fn heard(rx: &mut broadcast::Receiver<Arc<Frame>>) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            if let Frame::Binary(bytes) = &*frame {
                chunks.extend(decode(bytes));
            }
        }
        chunks
    }

    /// The points of level `level` in `chunks`, in order.
    fn points(chunks: &[Chunk], level: u8) -> Vec<(f64, f32)> {
        chunks
            .iter()
            .filter(|c| c.level == level)
            .flat_map(|c| c.points.iter().copied())
            .collect()
    }

    #[test]
    fn memory_says_where_each_trace_begins() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 100) {
            hub.ingest(&batch);
        }
        let Message::Memory { oldest, .. } = hub.memory() else {
            panic!("memory is a memory message")
        };
        assert_eq!(oldest.get(&0), Some(&0.01));
    }

    #[test]
    fn the_history_reaches_the_setting_before_any_view_within_the_headroom() {
        let mut hub = Hub::new();
        let mut parser = parser();
        hub.set_history(10.0, RAW_BYTES);
        // At 100 Hz, row n stands at (n + 1) / 100 s: the newest at 30 s.
        for batch in samples(&mut parser, 0, 3000) {
            hub.ingest(&batch);
        }
        let said = |hub: &Hub| {
            let Message::Memory {
                oldest,
                max_age,
                seconds,
                ..
            } = hub.memory()
            else {
                panic!("memory is a memory message")
            };
            (oldest[&0], max_age, seconds)
        };
        let (oldest, max_age, seconds) = said(&hub);
        // A view that ends the headroom behind the newest sample still has
        // the whole setting before it, and no view has more than a row
        // beyond that.
        let view = 30.0 - HISTORY_HEADROOM;
        assert!(oldest <= view - 10.0 + 1e-9, "oldest {oldest}");
        assert!(oldest > view - 10.0 - 0.011, "oldest {oldest}");
        // The message says the setting, not what the buffer keeps beyond it.
        assert_eq!((max_age, seconds), (10.0, 10.0));
        // The history a pause holds begins where the buffer did, and stays.
        hub.hold(7, true);
        for batch in samples(&mut parser, 3000, 1000) {
            hub.ingest(&batch);
        }
        assert_eq!(said(&hub), (oldest, 10.0, 10.0));
        hub.hold(7, false);
        assert!(said(&hub).0 > oldest, "let go, the buffer has moved on");
    }

    /// A reader of the hub finds each trace by the name the page gives it,
    /// with the time of its newest row, and the gaps of a span from the
    /// run's own record.
    #[test]
    fn a_reader_finds_each_trace_and_the_gaps_of_a_span() {
        let mut hub = Hub::new();
        let mut parser = parser();
        // At 100 Hz, row n stands at (n + 1) / 100 s. Samples 100 to 109
        // are lost.
        for batch in samples(&mut parser, 0, 100) {
            hub.ingest(&batch);
        }
        for batch in samples(&mut parser, 110, 90) {
            hub.ingest(&batch);
        }
        let signals: Vec<Signal> = hub.signals().into_values().flatten().collect();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].name, "field.x");
        assert_eq!((signals[0].info.id, signals[0].info.rate), (0, 100.0));
        let field = || vec!["field.x".to_string()];
        assert_eq!(
            hub.span(&field(), None, None, None),
            Ok((vec![0], -8.0, 2.0))
        );
        assert_eq!(
            hub.span(&field(), Some(0.5), None, Some(1.5)),
            Ok((vec![0], 1.0, 1.5))
        );
        assert_eq!(
            hub.span(&field(), None, Some(1.0), None),
            Ok((vec![0], 1.0, 2.0))
        );
        for refused in [
            hub.span(&[], None, None, None),
            hub.span(&["no.such".into()], None, None, None),
            hub.span(&field(), Some(-1.0), None, None),
            hub.span(&field(), None, Some(2.0), Some(1.0)),
        ] {
            assert!(refused.is_err(), "{refused:?}");
        }
        let gaps = hub.gaps(0, 0.0, 10.0).unwrap();
        let times: Vec<(f64, f64)> = gaps.iter().map(|gap| (gap.before, gap.after)).collect();
        assert_eq!(times, [(1.0, 1.11)]);
        assert!(hub.gaps(0, 1.11, 10.0).unwrap().is_empty());
        assert!(hub.gaps(9, 0.0, 10.0).is_none());
    }

    #[test]
    fn rows_are_every_raw_row_in_range_or_their_count_past_the_limit() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        // At 100 Hz, row n stands at (n + 1) / 100 s.
        let rows = hub.gather(0, 1.0, 2.0).map(|g| g.rows(1000)).unwrap();
        assert_eq!(rows.count, 101);
        assert_eq!(rows.y.first(), Some(&99.0));
        assert_eq!(rows.x.len(), 101);
        assert_eq!(rows.oldest, Some(0.01));
        let over = hub.gather(0, 1.0, 2.0).map(|g| g.rows(100)).unwrap();
        assert_eq!((over.count, over.x.len()), (101, 0));
    }

    #[test]
    fn an_export_gives_each_traces_column_and_its_raw_rows_in_range() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        let traces = hub.export(&[0, 99], 1.0, 1.05);
        // The id of no trace is passed over.
        assert_eq!(traces.len(), 1);
        let trace = &traces[0];
        assert_eq!(trace.name, "field.x");
        assert_eq!(trace.units, "nT");
        assert!(trace.unix);
        assert_eq!(trace.column, ColumnId::new(0));
        // At 100 Hz, rows 99 to 104 stand at 1.00 to 1.05 s.
        assert_eq!(trace.rows(), 6);
        let (times, values): (Vec<f64>, Vec<f64>) = (trace.batches.iter())
            .flat_map(|batch| {
                let values = batch.column(trace.column).unwrap().to_f64().into_owned();
                batch.timestamps().to_vec().into_iter().zip(values)
            })
            .unzip();
        assert_eq!(values, [99.0, 100.0, 101.0, 102.0, 103.0, 104.0]);
        assert_eq!(times.first(), Some(&1.0));
        assert!(times.last().is_some_and(|&t| (t - 1.05).abs() < 1e-9));
    }

    /// The simulated device, which streams 5000 rows a second and drops
    /// none, behind a proxy of the test's own, and a hub over a connection
    /// to it.
    struct Fed {
        /// The URL the proxy opens the device at.
        url: String,
        stop: Arc<std::sync::atomic::AtomicBool>,
        proxy: twinleaf::proxy::Proxy,
        live: Live,
    }

    impl Drop for Fed {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            self.live.retire();
            self.proxy.shutdown();
        }
    }

    impl Fed {
        const RATE: f64 = 5000.0;

        fn start() -> Fed {
            use twinleaf::proxy::{HostOptions, Mount, Proxy};
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let url = twinleaf_tools::tools::simulate::serve(
                &["--no-drop", "--samplerate", "5000"],
                stop.clone(),
            );
            let root = DeviceRoute::root();
            let proxy = Proxy::host(HostOptions {
                port: 0,
                name: "a test".to_string(),
                default: Some(Layout {
                    mounts: vec![Mount {
                        url: url.clone(),
                        source: root,
                        prefix: root,
                    }],
                }),
                ..HostOptions::default()
            })
            .unwrap();
            let connection = Connection::open(&proxy.endpoint()).unwrap();
            let runtime = tokio::runtime::Handle::current();
            let live = Live::over(connection, None, &runtime).unwrap();
            Fed {
                url,
                stop,
                proxy,
                live,
            }
        }

        /// Serves the device again at the address it had, after it was
        /// stopped.
        fn restart(&mut self) {
            let port = self.url.rsplit(':').next().unwrap().to_string();
            self.stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let args = ["--no-drop", "--samplerate", "5000", "--port", &port];
            let url = twinleaf_tools::tools::simulate::serve(&args, self.stop.clone());
            assert_eq!(url, self.url);
        }
    }

    /// The next message of type `kind` among `frames`, then `changes`, that
    /// `wanted` accepts.
    async fn next_message(
        frames: Vec<Frame>,
        changes: &mut broadcast::Receiver<Arc<Frame>>,
        kind: &str,
        wanted: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let wait = Duration::from_secs(30);
        let found = message_within(wait, frames, changes, kind, wanted).await;
        found.unwrap_or_else(|| panic!("no {kind} message came"))
    }

    /// The next message of type `kind` among `frames`, then `changes`, that
    /// `wanted` accepts, or `None` once `wait` has passed.
    async fn message_within(
        wait: Duration,
        frames: Vec<Frame>,
        changes: &mut broadcast::Receiver<Arc<Frame>>,
        kind: &str,
        wanted: impl Fn(&serde_json::Value) -> bool,
    ) -> Option<serde_json::Value> {
        let wanted = |frame: &Frame| {
            let Frame::Text(text) = frame else {
                return None;
            };
            let message: serde_json::Value = serde_json::from_str(text).unwrap();
            (message["type"] == kind && wanted(&message)).then_some(message)
        };
        if let Some(message) = frames.iter().find_map(wanted) {
            return Some(message);
        }
        let found = async {
            loop {
                match changes.recv().await {
                    Ok(frame) => {
                        if let Some(message) = wanted(&frame) {
                            return message;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => panic!("the hub ended"),
                }
            }
        };
        tokio::time::timeout(wait, found).await.ok()
    }

    /// A hub held up for longer than its connection's queue lasts counts
    /// every row the queue dropped, and tells the pages. The queue holds
    /// 1024 batches, under a second of the device's. So of the rows the
    /// stream's time spans, each is in the history or counted lost, and the
    /// hub's count, which its `loss` message says, is what the queue
    /// dropped. The hub is held up again if the device's link was down
    /// meanwhile, as on a busy machine, and the queue did not fill.
    #[tokio::test]
    async fn a_hub_held_up_counts_every_row_its_queue_dropped() {
        let fed = Fed::start();
        let stream = StreamKey::new(DeviceRoute::root(), StreamId::new(1));
        let held = move |hub: &mut Hub| {
            let run = hub.history.run(&stream)?;
            let losses = run.boundaries().filter_map(|gap| gap.boundary.loss.clone());
            let (lost, dropped) = losses.fold((0, 0), |(lost, dropped), loss| {
                (lost + loss.samples, dropped + loss.dropped_samples)
            });
            let rows = run.retained_rows();
            let span = run.last_timestamp()? - run.first_timestamp()?;
            let counted = crate::loss::of(hub.history.buffer(), stream.route, "sine").hub;
            Some((rows.end - rows.start, span, lost, dropped, counted))
        };
        // The stream flows before the hub is held up.
        let started = Instant::now();
        while fed.live.ask("held", held).await.unwrap().is_none() {
            assert!(started.elapsed() < Duration::from_secs(30), "no samples");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (_, mut changes) = fed.live.snapshot(0).await.unwrap();
        let dropped = |loss: &serde_json::Value| loss["streams"]["sine"]["hub"].as_u64() > Some(0);
        let mut told = None;
        for _ in 0..5 {
            fed.live
                .tell("held up", |_| std::thread::sleep(Duration::from_secs(3)));
            let wait = Duration::from_secs(6);
            told = message_within(wait, Vec::new(), &mut changes, "loss", dropped).await;
            if told.is_some() {
                break;
            }
        }
        let told = told.expect("the queue held 3 s of batches");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (rows, span, lost, dropped, counted) =
            fed.live.ask("held", held).await.unwrap().unwrap();
        assert!(dropped > 0, "the queue held 3 s of batches");
        assert_eq!(counted, dropped, "the hub counted what its queue dropped");
        assert_eq!(rows + lost, (span * Fed::RATE).round() as u64 + 1);
        assert_eq!(told["route"], "/");
        assert_eq!(
            told["streams"]["sine"]["hub"], dropped,
            "the pages were told"
        );
    }

    /// A page that fell further behind than the channel of changes holds
    /// is sent a snapshot in place of the changes it missed, which says how
    /// many they were, and hears every change after the snapshot.
    #[tokio::test]
    async fn a_page_that_fell_behind_is_sent_a_snapshot_that_says_how_far() {
        let fed = Fed::start();
        let (_, mut changes) = fed.live.snapshot(0).await.unwrap();
        let sent = 3000;
        let flood = move |hub: &mut Hub| {
            for _ in 0..sent {
                hub.feed.message(&Message::Clear);
            }
        };
        fed.live.ask("flood", flood).await.unwrap();
        let heard = changes.recv().await;
        let Err(broadcast::error::RecvError::Lagged(behind)) = heard else {
            panic!("the page kept up: {heard:?}");
        };
        assert!(behind >= sent - 1024, "{behind}");
        let frames = fed.live.frames_for(heard, &mut changes).await.unwrap();
        let Frame::Text(first) = &*frames[0] else {
            panic!("a snapshot starts with its hub");
        };
        let hub: serde_json::Value = serde_json::from_str(first).unwrap();
        assert_eq!(hub["type"], "hub");
        assert_eq!(hub["behind"], behind);
        // The next change is the one after the snapshot's last.
        fed.live
            .tell("one more", |hub| hub.feed.message(&Message::Clear));
        let next = next_message(Vec::new(), &mut changes, "clear", |_| true).await;
        assert!(next["seq"].as_u64() > hub["seq"].as_u64());

        let kept = changes.recv().await;
        let one = fed
            .live
            .frames_for(kept.clone(), &mut changes)
            .await
            .unwrap();
        assert_eq!(one.len(), 1, "a change the page kept up with goes as it is");
        assert_eq!(*one[0], *kept.unwrap());
    }

    /// A device whose link drops is counted once, by the watcher of the
    /// device, and the hub's thread tells the pages, with how long the link
    /// has been down so far.
    #[tokio::test]
    async fn a_device_whose_link_drops_is_counted_and_the_pages_are_told() {
        let fed = Fed::start();
        let (snapshot, mut changes) = fed.live.snapshot(0).await.unwrap();
        next_message(snapshot, &mut changes, "device", |device| {
            device["presence"] == "alive"
        })
        .await;
        // On a busy machine the link may have dropped before, and come back.
        let before = fed.live.losses.drops(DeviceRoute::root());
        fed.stop.store(true, Ordering::Relaxed);
        let stopped = Instant::now();
        let dropped = |loss: &serde_json::Value| loss["link"]["times"].as_u64() > Some(before);
        let mut told = next_message(Vec::new(), &mut changes, "loss", dropped).await;
        while stopped.elapsed() < Duration::from_secs(3) {
            told = next_message(Vec::new(), &mut changes, "loss", dropped).await;
        }
        assert_eq!(told["route"], "/");
        assert_eq!(told["link"]["times"], before + 1);
        let later = next_message(Vec::new(), &mut changes, "loss", dropped).await;
        assert_eq!(
            later["link"]["times"],
            before + 1,
            "a link that is down drops once"
        );
        assert!(
            later["link"]["seconds"].as_f64() > told["link"]["seconds"].as_f64(),
            "the outage grows while the link is down"
        );
        let (again, _) = fed.live.snapshot(0).await.unwrap();
        let held = next_message(again, &mut changes, "loss", |_| true).await;
        assert_eq!(
            held["link"]["times"],
            before + 1,
            "a snapshot holds the counts"
        );
    }

    #[test]
    fn a_new_column_is_a_trace_named_by_its_stream_and_column() {
        let mut hub = Hub::new();
        let mut rx = hub.feed.subscribe().1;
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 10) {
            hub.ingest(&batch);
        }
        let Frame::Text(text) = &*rx.try_recv().unwrap() else {
            panic!("a trace comes as text")
        };
        let trace: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(trace["type"], "trace");
        assert_eq!(trace["trace"]["route"], "/");
        assert_eq!(
            (
                trace["trace"]["stream"].as_str(),
                trace["trace"]["column"].as_str()
            ),
            (Some("field"), Some("x"))
        );
    }

    #[test]
    fn a_snapshot_holds_exactly_the_points_sent_before_it() {
        let mut hub = Hub::new();
        let mut rx = hub.feed.subscribe().1;
        let mut parser = parser();
        for first in (0..2000).step_by(100) {
            for batch in samples(&mut parser, first, 100) {
                hub.ingest(&batch);
            }
            hub.flush();
        }
        let sent = heard(&mut rx);
        assert!(!points(&sent, 0).is_empty());
        let snapshot: Vec<Chunk> = hub
            .snapshot()
            .iter()
            .filter_map(|f| match f {
                Frame::Binary(bytes) => Some(decode(bytes)),
                Frame::Text(_) => None,
            })
            .flatten()
            .collect();
        for level in 0..LEVELS as u8 {
            assert_eq!(
                points(&snapshot, level),
                points(&sent, level),
                "level {level}"
            );
        }
        // What comes after the snapshot starts where it ends.
        let mut later = hub.feed.subscribe().1;
        for batch in samples(&mut parser, 2000, 100) {
            hub.ingest(&batch);
        }
        hub.flush();
        let next = points(&heard(&mut later), 0);
        assert!(next.first().unwrap().0 > points(&sent, 0).last().unwrap().0);
    }

    /// An empty batch adds no rows, and the rows it says were lost count.
    #[test]
    fn the_rows_an_empty_batch_says_were_lost_count() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 10) {
            hub.ingest(&batch);
        }
        let after = samples(&mut parser, 15, 10).remove(0);
        let loss = after.boundary().and_then(|b| b.loss.as_ref());
        assert_eq!(loss.map(|loss| loss.samples), Some(5));
        hub.ingest(&after.slice(0..0));
        let lost = crate::loss::of(hub.history.buffer(), DeviceRoute::root(), "field");
        assert_eq!(lost.skipped, 5);
    }

    /// A stream the buffer forgot goes on under the name its run had, and
    /// the rows it loses after are told with those before.
    #[test]
    fn a_forgotten_stream_keeps_its_counts_under_its_name() {
        let mut hub = Hub::new();
        let mut rx = hub.feed.subscribe().1;
        let mut parser = parser();
        let mut told = |hub: &Hub| -> Vec<serde_json::Value> {
            hub.losses.send_changed(hub.history.buffer());
            std::iter::from_fn(|| rx.try_recv().ok())
                .filter_map(|frame| match &*frame {
                    Frame::Text(text) => serde_json::from_str(text).ok(),
                    Frame::Binary(_) => None,
                })
                .filter(|message: &serde_json::Value| message["type"] == "loss")
                .map(|message| message["streams"]["field"]["skipped"].clone())
                .collect()
        };
        for first in [0, 15] {
            for batch in samples(&mut parser, first, 10) {
                hub.ingest(&batch);
            }
        }
        assert_eq!(told(&hub), [5]);
        let key = StreamKey::new(DeviceRoute::root(), StreamId::new(1));
        assert!(hub.history.forget(&key));
        let after = samples(&mut parser, 30, 10).remove(0);
        hub.ingest(&after.slice(0..0));
        assert!(hub.history.run(&key).is_none());
        assert_eq!(told(&hub), [10]);
    }

    /// Two streams of a device that have one name are one stream to the
    /// pages, and the rows each lost add up.
    #[test]
    fn the_rows_two_streams_of_one_name_lost_add_up() {
        let mut hub = Hub::new();
        let mut parser = PacketParser::new(false);
        let route = DeviceRoute::root();
        let device = data::Metadata::Device(data::Device {
            session: SessionId::new(1),
            n_streams: 2,
            name: "sim",
            serial: "S1",
            firmware: "1",
            protocol: 0,
        });
        let mut records = vec![device];
        for id in [1, 2] {
            let stream_id = StreamId::new(id);
            records.push(data::Metadata::Stream(data::Stream {
                stream_id,
                n_columns: 1,
                n_segments: 1,
                sample_size: 4,
                buf_samples: 128,
                name: "field",
            }));
            let data::Metadata::Segment(segment) = segment(0, 100) else {
                unreachable!()
            };
            records.push(data::Metadata::Segment(data::Segment {
                stream_id,
                ..segment
            }));
            records.push(data::Metadata::Column(data::Column {
                stream_id,
                index: ColumnId::new(0),
                data_type: DataType::F32,
                name: "x",
                units: "nT",
                description: "",
            }));
        }
        for record in records {
            let packet = Packet::metadata(record, data::MetadataFlags::UPDATE, route).unwrap();
            parser.push_packet(&packet).unwrap();
        }
        // Stream 1 loses 5 rows and stream 2 loses 10.
        for (id, first) in [(1, 0), (2, 0), (1, 15), (2, 20)] {
            let data: Vec<u8> = (0..10u32).flat_map(|i| (i as f32).to_le_bytes()).collect();
            let packet = Packet::samples(id, 0, first, &data, route).unwrap();
            parser.push_packet(&packet).unwrap();
        }
        for batch in std::iter::from_fn(|| parser.pop_batch()) {
            hub.ingest(&batch);
        }
        assert_eq!(hub.history.stream_keys().count(), 2);
        let lost = crate::loss::of(hub.history.buffer(), route, "field");
        assert_eq!(lost.skipped, 15);
    }

    /// The hub counts the rows each batch says were lost before it by where
    /// they went: skipped with nothing said, the proxy's after its count of
    /// what it dropped, the hub's after its connection dropped a packet, and
    /// the link's after the link was lost. The pages are told once, and
    /// again only when a count grows.
    #[test]
    fn the_hub_counts_what_each_batch_says_was_lost_by_where() {
        use crate::protocol::Lost;
        let mut hub = Hub::new();
        let mut rx = hub.feed.subscribe().1;
        let mut parser = parser();
        let root = DeviceRoute::root();
        let mut feed = |parser: &mut PacketParser, first: u32| {
            for batch in samples(parser, first, 10) {
                hub.ingest(&batch);
            }
            crate::loss::of(hub.history.buffer(), root, "field")
        };
        let lost = |skipped, link, proxy, hub| Lost {
            skipped,
            link,
            proxy,
            hub,
        };
        assert_eq!(feed(&mut parser, 0), lost(0, 0, 0, 0));
        assert_eq!(feed(&mut parser, 15), lost(5, 0, 0, 0));
        parser
            .push_packet(&Packet::dropped(1, 1).with_route(root))
            .unwrap();
        assert_eq!(feed(&mut parser, 32), lost(5, 0, 7, 0));
        parser.note_dropped_packets();
        assert_eq!(feed(&mut parser, 50), lost(5, 0, 7, 8));
        let link = Packet::link(twinleaf::tio::packet::LinkStatus::SensorDisconnected);
        parser.push_packet(&link.with_route(root)).unwrap();
        describe(&mut parser, 100);
        assert_eq!(feed(&mut parser, 63), lost(5, 3, 7, 8));
        assert_eq!(feed(&mut parser, 73), lost(5, 3, 7, 8));

        let told = |hub: &Hub, rx: &mut broadcast::Receiver<Arc<Frame>>| {
            hub.losses.send_changed(hub.history.buffer());
            let sent: Vec<serde_json::Value> = std::iter::from_fn(|| rx.try_recv().ok())
                .filter_map(|frame| match &*frame {
                    Frame::Text(text) => serde_json::from_str(text).ok(),
                    Frame::Binary(_) => None,
                })
                .filter(|message: &serde_json::Value| message["type"] == "loss")
                .collect();
            sent
        };
        let sent = told(&hub, &mut rx);
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0]["streams"],
            serde_json::json!({"field": {"skipped": 5, "link": 3, "proxy": 7, "hub": 8}})
        );
        assert!(told(&hub, &mut rx).is_empty(), "nothing more was lost");
        let in_snapshot = hub.losses.snapshot();
        assert_eq!(in_snapshot.len(), 1, "a snapshot holds the counts");
    }

    /// What a stream lost is a line of the log for each place it was lost,
    /// once the counts are sent, with the stream and the samples as data.
    /// Each line carries the time of the newest sample the hub took of the
    /// device before it. Counts that did not grow are no line.
    #[test]
    fn what_a_stream_lost_is_a_line_of_the_log_for_each_place() {
        let mut hub = Hub::new();
        let mut rx = hub.feed.subscribe().1;
        let mut parser = parser();
        let root = DeviceRoute::root();
        let mut newest = f64::NAN;
        let mut feed = |hub: &mut Hub, parser: &mut PacketParser, first: u32| {
            for batch in samples(parser, first, 10) {
                newest = *batch.timestamps().last().unwrap();
                hub.ingest(&batch);
            }
        };
        feed(&mut hub, &mut parser, 0);
        feed(&mut hub, &mut parser, 15);
        parser
            .push_packet(&Packet::dropped(1, 1).with_route(root))
            .unwrap();
        feed(&mut hub, &mut parser, 32);
        assert!(logbook::tests::heard(&mut rx).is_empty());
        hub.losses.send_changed(hub.history.buffer());
        let lines = logbook::tests::heard(&mut rx);
        let lost: Vec<serde_json::Value> = lines.iter().map(|line| line["loss"].clone()).collect();
        assert_eq!(
            lost,
            [
                serde_json::json!({"cause": "skipped", "stream": "field", "samples": 5}),
                serde_json::json!({"cause": "proxy", "stream": "field", "samples": 7}),
            ]
        );
        for line in &lines {
            assert_eq!(line["time"], newest, "{line}");
            assert_eq!(line["level"], "warn");
            assert_eq!(line["source"], "proxy");
            assert_eq!(line["route"], "/");
        }
        hub.losses.send_changed(hub.history.buffer());
        assert!(logbook::tests::heard(&mut rx).is_empty(), "nothing more");
    }

    /// A log packet of a device is a line every page hears, said by the
    /// device at its route, with a time on its samples' clock, and the
    /// snapshot of a page that connects later holds it, with the proxy's
    /// line that names the device.
    #[tokio::test]
    async fn a_devices_log_packet_is_a_line_the_pages_hear_and_a_snapshot_holds() {
        let fed = Fed::start();
        let (snapshot, mut changes) = fed.live.snapshot(0).await.unwrap();
        let said = |line: &serde_json::Value| line["source"] == "device" && !line["time"].is_null();
        let line = next_message(snapshot, &mut changes, "log", said).await;
        assert_eq!(line["route"], "/");
        let text = line["text"].as_str().unwrap();
        assert!(text.contains("lucky number"), "{text}");
        let level = line["level"].as_str().unwrap();
        assert!(["error", "warn", "info"].contains(&level), "{level}");
        assert!(line.get("loss").is_none());
        let trace = fed.live.ask("newest", |hub| {
            let run = hub.history.run(&hub.traces[0].key.stream_key());
            run.and_then(Run::last_timestamp)
        });
        let newest = trace.await.unwrap().expect("the stream has rows");
        let time = line["time"].as_f64().unwrap();
        assert!(time <= newest && newest - time < 60.0, "{time} {newest}");

        let (again, mut later) = fed.live.snapshot(0).await.unwrap();
        let same = |held: &serde_json::Value| held["n"] == line["n"];
        let held = next_message(again.clone(), &mut later, "log", same).await;
        assert_eq!(held["text"], line["text"]);
        assert!(held.get("seq").is_none(), "a snapshot's line has no number");
        let found = |held: &serde_json::Value| held["part"] == "hub";
        let named = next_message(again, &mut later, "log", found).await;
        assert_eq!(named["route"], "/");
        assert_eq!(named["part"], "hub");
        assert!(named["text"].as_str().unwrap().starts_with("Found "));
    }

    /// A record of the proxy's console said before the hub is in the
    /// snapshot of a page of it, and one said after is a change the page
    /// hears, each a line of the proxy's with the record's level.
    #[tokio::test]
    async fn a_console_record_is_a_line_the_pages_of_the_hub_hear() {
        let console = logbook::Console::new();
        console.take(log::Level::Warn, "mdns", "said before the hub");
        let fed = Fed::start();
        console.attach(&fed.live.log);
        let (snapshot, mut changes) = fed.live.snapshot(0).await.unwrap();
        let before = |line: &serde_json::Value| line["text"] == "said before the hub";
        let held = next_message(snapshot, &mut changes, "log", before).await;
        assert_eq!(held["level"], "warn");
        assert_eq!(held["source"], "proxy");
        assert_eq!(held["part"], "mdns");
        assert_eq!(held["route"], serde_json::Value::Null);
        console.take(log::Level::Error, "server", "said after");
        let after = |line: &serde_json::Value| line["text"] == "said after";
        let line = next_message(Vec::new(), &mut changes, "log", after).await;
        assert_eq!(line["level"], "error");
        assert_eq!(line["part"], "server");
        assert!(line["seq"].as_u64().is_some(), "a change every page hears");
    }

    /// A device whose link drops is one line of the log, which carries why
    /// the proxy lost it and the address of its link, and a link that
    /// returns is one line, which carries the address and how long it was
    /// down.
    #[tokio::test]
    async fn a_link_that_drops_and_returns_is_one_line_each_with_why_and_where() {
        let mut fed = Fed::start();
        let (snapshot, mut changes) = fed.live.snapshot(0).await.unwrap();
        next_message(snapshot, &mut changes, "device", |device| {
            device["presence"] == "alive"
        })
        .await;
        fed.stop.store(true, Ordering::Relaxed);
        let of_link = |line: &serde_json::Value| line["loss"]["cause"] == "link";
        let down = next_message(Vec::new(), &mut changes, "log", of_link).await;
        assert_eq!(down["route"], "/");
        assert_eq!(down["part"], "hub");
        assert_eq!(down["level"], "error");
        let url = fed.url.clone();
        assert_eq!(
            down["loss"],
            serde_json::json!({"cause": "link", "seconds": null, "why": "disconnected", "url": url})
        );
        // The device answers again at the same address.
        fed.restart();
        let back = next_message(Vec::new(), &mut changes, "log", of_link).await;
        assert_eq!(back["level"], "info");
        assert_eq!(back["loss"]["url"], url);
        assert!(back["loss"]["seconds"].as_f64() > Some(0.0), "{back}");
        assert!(back["loss"].get("why").is_none());
        // No other line of the link came between them or comes after.
        let more = message_within(
            Duration::from_secs(2),
            Vec::new(),
            &mut changes,
            "log",
            of_link,
        );
        assert_eq!(more.await, None);
        let (again, _) = fed.live.snapshot(0).await.unwrap();
        let held = again.iter().filter(|frame| match frame {
            Frame::Text(text) => text.contains("\"cause\":\"link\""),
            Frame::Binary(_) => false,
        });
        assert_eq!(
            held.count(),
            2,
            "one line for the drop and one for the return"
        );
    }

    #[test]
    fn lost_samples_lift_the_pen() {
        let mut hub = Hub::new();
        let mut rx = hub.feed.subscribe().1;
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 500) {
            hub.ingest(&batch);
        }
        hub.flush();
        for batch in samples(&mut parser, 1000, 500) {
            hub.ingest(&batch);
        }
        hub.flush();
        let chunks = heard(&mut rx);
        assert!(chunks.iter().any(|c| c.brk), "no chunk starts a new line");
    }

    #[test]
    fn a_new_decimation_clears_and_resends_the_history() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        hub.flush();
        let mut rx = hub.feed.subscribe().1;
        hub.set_points_per_second(10.0);
        let kinds: Vec<String> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|frame| match &*frame {
                Frame::Text(text) => serde_json::from_str::<serde_json::Value>(text).unwrap()
                    ["type"]
                    .as_str()
                    .unwrap()
                    .to_string(),
                Frame::Binary(_) => "points".to_string(),
            })
            .collect();
        assert_eq!(kinds, ["clear", "points", "config"]);
    }

    /// The type of each message `rx` heard, "points" for a binary frame.
    fn kinds(rx: &mut broadcast::Receiver<Arc<Frame>>) -> Vec<String> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|frame| match &*frame {
                Frame::Text(text) => serde_json::from_str::<serde_json::Value>(text).unwrap()
                    ["type"]
                    .as_str()
                    .unwrap()
                    .to_string(),
                Frame::Binary(_) => "points".to_string(),
            })
            .collect()
    }

    /// The job a hub with a mailbox posted to it, run with the hub.
    fn run_posted(hub: &mut Hub, mail: &flume::Receiver<Job>) -> &'static str {
        match mail.recv_timeout(Duration::from_secs(10)) {
            Ok(job) => {
                (job.run)(hub);
                job.name
            }
            _ => panic!("no job was posted"),
        }
    }

    /// With a mailbox, a new decimation is replayed off the hub's thread.
    /// The samples that come meanwhile are decimated at the old one, and the
    /// new levels take them in when they arrive.
    #[test]
    fn a_new_decimation_is_replayed_off_the_hubs_thread() {
        let mut hub = Hub::new();
        let (mailbox, mail) = flume::unbounded();
        hub.mailbox = Some(mailbox);
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        hub.flush();
        let mut rx = hub.feed.subscribe().1;
        hub.set_points_per_second(10.0);
        assert!(
            kinds(&mut rx).is_empty(),
            "the old levels go on until the new arrive"
        );
        for batch in samples(&mut parser, 1000, 500) {
            hub.ingest(&batch);
        }
        let newest = hub
            .history
            .run(&hub.traces[0].key.stream_key())
            .unwrap()
            .last_timestamp();
        assert_eq!(run_posted(&mut hub, &mail), "decimated");
        assert_eq!(kinds(&mut rx), ["clear", "points", "config"]);
        let finest = &hub.traces[0].live.output()[0];
        assert_eq!(finest.ratio(), ratio_for(hub.traces[0].info.rate, 10.0));
        let last = finest.output().last().unwrap().0;
        assert!(
            newest.is_some_and(|newest| newest - last < 1.0),
            "the new levels took in what came meanwhile"
        );
    }

    /// A replay that a later change of decimation overtook is dropped.
    #[test]
    fn an_overtaken_decimation_is_dropped() {
        let mut hub = Hub::new();
        let (mailbox, mail) = flume::unbounded();
        hub.mailbox = Some(mailbox);
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        let mut rx = hub.feed.subscribe().1;
        hub.set_points_per_second(10.0);
        hub.set_points_per_second(20.0);
        run_posted(&mut hub, &mail);
        run_posted(&mut hub, &mail);
        assert_eq!(kinds(&mut rx), ["clear", "points", "config"]);
        let finest = &hub.traces[0].live.output()[0];
        assert_eq!(finest.ratio(), ratio_for(hub.traces[0].info.rate, 20.0));
    }

    /// No level keeps more than the raw history, and the page hears so.
    #[test]
    fn the_raw_history_caps_every_level() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 100) {
            hub.ingest(&batch);
        }
        let mut rx = hub.feed.subscribe().1;
        hub.set_history(600.0, RAW_BYTES);
        let kept: Vec<f64> = hub.traces[0]
            .live
            .output()
            .iter()
            .map(Fpcs::retention_seconds)
            .collect();
        assert_eq!(kept, [120.0, 480.0, 600.0, 600.0, 600.0, 600.0]);
        let Frame::Text(text) = &*rx.try_recv().unwrap() else {
            panic!("the config is text")
        };
        let config: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(
            config["levels"],
            serde_json::json!([120.0, 480.0, 600.0, 600.0, 600.0, 600.0])
        );
    }

    #[test]
    fn only_a_spectrum_some_page_shows_is_sent() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 2000) {
            hub.ingest(&batch);
        }
        let mut rx = hub.feed.subscribe().1;
        hub.spectra();
        assert!(rx.try_recv().is_err(), "a spectrum no page shows was sent");
        hub.show_spectra(7, BTreeSet::from([0]));
        hub.spectra();
        let Frame::Binary(frame) = &*rx.try_recv().unwrap() else {
            panic!("a spectrum is binary")
        };
        assert_eq!((frame[0], frame[16], frame[24]), (2, 1, 0));
        hub.show_spectra(7, BTreeSet::new());
        hub.spectra();
        assert!(
            rx.try_recv().is_err(),
            "a page that went still gets spectra"
        );
    }

    #[test]
    fn a_window_splits_at_lost_samples() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 500)
            .into_iter()
            .chain(samples(&mut parser, 1000, 500))
        {
            hub.ingest(&batch);
        }
        let window = hub.window(0, -1e9, 1e9, 10_000).unwrap().window();
        assert_eq!(window.chunks.len(), 2);
    }

    #[test]
    fn a_window_reads_raw_rows() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        let window = hub.window(0, 1.0, 2.0, 1000).unwrap().window();
        assert_eq!(window.ratio, 1);
        assert!(window.chunks[0].x.len() >= 100);
        assert!(hub.window(1, 1.0, 2.0, 1000).is_none());
    }

    /// Every level of a trace goes out, each coarser one with about a
    /// LEVEL_FACTOR of the points of the one before.
    #[test]
    fn every_level_goes_out() {
        let mut hub = Hub::new();
        let mut rx = hub.feed.subscribe().1;
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 20_000) {
            hub.ingest(&batch);
        }
        hub.flush();
        let sent = heard(&mut rx);
        let counts: Vec<usize> = (0..3).map(|level| points(&sent, level).len()).collect();
        for pair in counts.windows(2) {
            let ratio = pair[0] as f64 / pair[1] as f64;
            assert!((3.0..5.0).contains(&ratio), "{counts:?}");
        }
    }

    /// A read gathered from the hub gives the rows it gathered, though the
    /// hub takes more samples and drops its oldest before the read is done.
    #[test]
    fn a_gathered_read_keeps_what_it_gathered() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        let gathered = hub.gather(0, -1e9, 1e9).unwrap();
        hub.set_history(1.0, RAW_BYTES);
        for batch in samples(&mut parser, 1000, 2000) {
            hub.ingest(&batch);
        }
        let now = hub.gather(0, -1e9, 1e9).unwrap().rows(usize::MAX);
        assert!(now.x[0] > 0.0, "the hub still holds its first row");
        let rows = gathered.rows(usize::MAX);
        assert_eq!(rows.count, 1000);
        assert_eq!(rows.y, (0..1000).map(f64::from).collect::<Vec<_>>());
    }

    /// A tone, noise from `seed` and a spike about every 5,000 samples, so
    /// every decimation has extremes to keep.
    fn spiky(i: u32, seed: u32) -> f32 {
        let hash = (i ^ seed).wrapping_mul(2_654_435_761) >> 8;
        let spike = if hash.is_multiple_of(5003) { 5.0 } else { 0.0 };
        (i as f32 * 1e-3).sin() + 0.2 * (hash as f32 / (1 << 24) as f32 - 0.5) + spike
    }

    #[test]
    fn while_a_page_holds_the_history_a_window_reads_what_the_buffer_dropped() {
        let mut hub = Hub::new();
        let mut parser = parser();
        // 16 bytes a row: the buffer keeps 1200 rows, 10 s and the headroom,
        // which leaves room for the first 1000 held.
        hub.set_history(10.0, 40_000);
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        hub.hold(7, true);
        for batch in samples(&mut parser, 1000, 3000) {
            hub.ingest(&batch);
        }
        let first = |hub: &mut Hub| {
            hub.window(0, 0.0, 1.0, 8192)
                .map(WindowRead::window)
                .and_then(|w| w.chunks.first().map(|c| c.x[0]))
        };
        assert!(
            first(&mut hub).is_some_and(|t| t <= 0.02),
            "the held history reaches the start"
        );
        hub.hold(7, false);
        assert_eq!(
            first(&mut hub),
            None,
            "let go, the buffer alone no longer reaches it"
        );
        assert_eq!(hub.history.budget(), Some(40_000));
    }

    #[test]
    fn a_range_across_the_held_history_and_the_buffer_reads_both_once() {
        let mut hub = Hub::new();
        let mut parser = parser();
        hub.set_history(10.0, 40_000);
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        hub.hold(7, true);
        for batch in samples(&mut parser, 1000, 3000) {
            hub.ingest(&batch);
        }
        // Rows 0..4000 stand at 0.01..40 s; all of them, each once.
        let rows = hub
            .gather(0, 0.0, 40.0)
            .map(|g| g.rows(usize::MAX))
            .unwrap();
        let mut seen = rows.y.clone();
        seen.dedup();
        assert_eq!(seen.len(), rows.y.len(), "no row twice");
        assert_eq!(rows.y.first(), Some(&0.0));
        assert_eq!(rows.y.last(), Some(&3999.0));
        assert_eq!(rows.oldest, Some(0.01));
    }

    /// The last `memory` message among the frames `rx` holds.
    fn last_memory(rx: &mut broadcast::Receiver<Arc<Frame>>) -> Option<serde_json::Value> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|frame| match &*frame {
                Frame::Text(text) => serde_json::from_str(text).ok(),
                Frame::Binary(_) => None,
            })
            .filter(|message: &serde_json::Value| message["type"] == "memory")
            .last()
    }

    /// The last `memory` message the pages were told, which must be what
    /// `hub.memory()` says now.
    fn told(hub: &Hub, rx: &mut broadcast::Receiver<Arc<Frame>>) -> serde_json::Value {
        let mut told = last_memory(rx).expect("the pages were told");
        told.as_object_mut().unwrap().remove("seq");
        assert_eq!(told, serde_json::to_value(hub.memory()).unwrap());
        told
    }

    /// The pages are told the `memory` message the hub says, with what is
    /// held, on a new History setting, on a hold, as the held history grows,
    /// on a new setting while held, and on a release.
    #[test]
    fn a_page_is_told_the_memory_the_hub_says_with_what_is_held() {
        let mut hub = Hub::new();
        let mut parser = parser();
        let (_, mut rx) = hub.feed.subscribe();
        hub.set_history(10.0, 40_000);
        assert!(told(&hub, &mut rx)["held"].is_null());
        // At 100 Hz, row n stands at (n + 1) / 100 s.
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        hub.hold(7, true);
        assert!(told(&hub, &mut rx)["held"].is_object());
        for batch in samples(&mut parser, 1000, 3000) {
            hub.ingest(&batch);
        }
        hub.send_memory();
        let held = told(&hub, &mut rx)["held"]["bytes"].as_u64().unwrap();
        assert!(held > 0, "the held history keeps what the buffer dropped");
        hub.set_history(5.0, 40_000);
        let said = told(&hub, &mut rx);
        assert_eq!(said["seconds"].as_f64(), Some(5.0));
        assert_eq!(said["held"]["seconds"].as_f64(), Some(5.0));
        hub.hold(7, false);
        assert!(told(&hub, &mut rx)["held"].is_null());
    }

    /// The seconds a page is told of the buffer and of the held history go
    /// up to the History setting, though both keep the headroom beyond it.
    #[test]
    fn the_seconds_a_page_is_told_go_up_to_the_history_setting() {
        let mut hub = Hub::new();
        let mut parser = parser();
        hub.set_history(10.0, RAW_BYTES);
        for batch in samples(&mut parser, 0, 3000) {
            hub.ingest(&batch);
        }
        hub.hold(7, true);
        assert!(hub.history.seconds() > 10.0);
        assert!(hub.history.held_seconds().is_some_and(|held| held > 10.0));
        let Message::Memory {
            seconds,
            max_age,
            held,
            ..
        } = hub.memory()
        else {
            unreachable!()
        };
        assert_eq!((seconds, max_age), (10.0, 10.0));
        assert_eq!(held.map(|held| held.seconds), Some(10.0));
    }

    /// Prints what a window read costs at each span of `WINDOW_HOURS` hours
    /// (one by default) of a 5 kHz signal, read through `Hub::window`. Run it
    /// in a release build, as
    /// `cargo test --release -p twinleaf-web -- --ignored --nocapture window_read_times`.
    #[test]
    #[ignore = "a measurement, not a check"]
    fn window_read_times() {
        let hours: f64 = std::env::var("WINDOW_HOURS")
            .ok()
            .and_then(|hours| hours.parse().ok())
            .unwrap_or(1.0);
        let rate = 5000;
        let rows = (hours * 3600.0 * f64::from(rate)) as u32;
        let mut hub = Hub::new();
        hub.set_history(hours * 3600.0 + 10.0, usize::MAX / 2);
        let mut parser = parser_at(rate);
        let (mut ingest, mut flush) = (Duration::ZERO, Duration::ZERO);
        for first in (0..rows).step_by(rate as usize) {
            let some = rate.min(rows - first);
            let batches = valued(&mut parser, rate, first, some, |i| spiky(i, 0));
            let start = Instant::now();
            for batch in &batches {
                hub.ingest(batch);
            }
            ingest += start.elapsed();
            let start = Instant::now();
            hub.flush();
            flush += start.elapsed();
        }
        println!("{rows} rows: ingest {ingest:?}, flush {flush:?}");
        println!("the buffer holds {} bytes", hub.history.bytes());
        let ms = |d: Duration| d.as_secs_f64() * 1e3;
        let end = f64::from(rows) / f64::from(rate);
        let spans = [
            5.0, 28.0, 138.0, 600.0, 2779.0, 3600.0, 14400.0, 43200.0, 86400.0,
        ];
        let (width, repeats) = (2840, 9);
        println!("span s | rows | ratio | points | hub ms | off the hub ms | json ms | bytes");
        for span in spans.into_iter().filter(|&span| span <= end) {
            let t0 = (end - span) / 2.0;
            let mut times = Vec::new();
            let mut said = String::new();
            for _ in 0..repeats {
                let start = Instant::now();
                let read = hub.window(0, t0, t0 + span, width).unwrap();
                let gather = start.elapsed();
                let start = Instant::now();
                let window = read.window();
                let reduce = start.elapsed();
                let start = Instant::now();
                let json = serde_json::to_vec(&window).unwrap();
                let encode = start.elapsed();
                let points: usize = window.chunks.iter().map(|c| c.x.len()).sum();
                said = format!("{} | {} | {points}", span * f64::from(rate), window.ratio);
                times.push((gather + reduce + encode, gather, reduce, encode, json.len()));
            }
            times.sort();
            let (_, gather, reduce, encode, bytes) = times[repeats / 2];
            println!(
                "{span} | {said} | {:.3} | {:.3} | {:.3} | {bytes}",
                ms(gather),
                ms(reduce),
                ms(encode)
            );
        }
    }
}

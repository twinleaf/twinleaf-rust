//! The hub, which keeps what every page shows, from one connection to the
//! proxy.
//!
//! Every batch goes into one [`Buffer`], which keeps the newest raw rows of
//! each stream. Each trace, one float column, is a set of
//! [`ColumnProcessor`]s over that buffer. `FpcsLevels` decimates it for the
//! live view at several ratios, each LEVEL_FACTOR times the last, none kept
//! longer than the raw history, and `Welch` estimates its spectrum. Every
//! level goes to every page, which draws each plot from the level that suits
//! its span.
//!
//! A paused view reads a window of the history instead, the rows between
//! two times as about two points a pixel. A window of few enough rows is
//! decimated from the rows themselves. Any other is read from the trace's
//! [`Pyramid`], decimations of every row the history holds at ratios that
//! double, at the coarsest level no coarser than the window needs. So a
//! window costs by its width in pixels, whatever its span, and is never
//! coarser than its rows decimated for it would be.
//!
//! The hub lives on a thread of its own, which alone touches it. Batches of
//! samples and the pages' requests come to it as mail, in the order sent,
//! and a request's answer goes back on a channel of its own. What takes long
//! is done elsewhere: a read takes the rows it needs, which share the
//! buffer's chunks, and copies them off the thread, and a change of
//! decimation replays a frozen copy of the buffer on a thread of its own.
//!
//! A run lasts across lost samples, and [`Run::boundaries`] says where they
//! fell. The hub splits the points it sends there, so the page lifts the pen.
//!
//! A hub ends one way, its `End`, whether a page mounts other devices
//! ([`Live::retire`]) or the connection's samples end (the thread that feeds
//! it). Its pages' sockets then close, and the next request starts the hub
//! that takes its place.

use crate::devices::Devices;
use crate::export::ExportTrace;
use crate::feed::Feed;
use crate::protocol::{self, Frame, Message, MountInfo, Rows, TraceInfo, Window, WindowChunk};
use crate::pyramid::{self, Level, Point, Pyramid, Slice};
use crate::wire::{encode, encode_spectra, split_at_breaks, Chunk, Spectrum};
use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};
use twinleaf::data::ops::{Fpcs, FpcsLevels, Welch};
use twinleaf::data::{Buffer, ColumnKey, ColumnOp, ColumnProcessor, Run, StreamKey};
use twinleaf::proxy::Layout;
use twinleaf::{ColumnId, Connection, DeviceRoute, DeviceTree, RecvError, SampleBatch};

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
/// The share of the history budget the history held for paused plots keeps.
const HELD_SHARE: f64 = 0.5;
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
    /// The decimations a window of the history is read from, of every row
    /// the buffer holds.
    reduced: ColumnProcessor<Pyramid>,
    spectrum: ColumnProcessor<Welch>,
    /// Time of the last point sent of each level.
    last_sent: Vec<f64>,
    /// The stream generation the last points came from. A new one is a new run.
    generation: Option<u32>,
}

/// What every page shows, and the channel each page hears it on.
pub(crate) struct Hub {
    buffer: Buffer,
    /// Every trace, its id its index.
    traces: Vec<Trace>,
    by_key: HashMap<ColumnKey, usize>,
    points_per_second: f64,
    /// The traces each page shows as spectra, by trace id, by page. Only
    /// these are estimated.
    spectra_shown: HashMap<u64, BTreeSet<u16>>,
    /// The history as it was when the first page that now holds a plot
    /// paused paused it, which reads of what the buffer no longer reaches
    /// prefer, and the pages that hold it. Its chunks are the buffer's,
    /// shared, so it costs memory only as the buffer drops them.
    held: Option<Held>,
    holders: BTreeSet<u64>,
    /// The bytes the buffer and the held history may hold together. The held
    /// history keeps at most HELD_SHARE of it, from its newest rows, and the
    /// buffer the rest.
    budget: Option<usize>,
    /// What the last `memory` message said, rounded as the page shows it.
    memory_sent: Option<(u64, u64, i64, i64, i64, i64)>,
    feed: Arc<Feed>,
    /// The seconds of each stream's time the raw history keeps, which no
    /// level of the decimation keeps longer.
    history_seconds: f64,
    /// The hub's own mailbox, which the replay of a change of decimation
    /// posts its result to. A hub without one, as in a test, replays at once.
    mailbox: Option<mpsc::Sender<Mail>>,
    /// The number of the newest change of decimation, so the replay of an
    /// older one is dropped.
    decimation: u64,
}

/// The history as it was when it was held: a frozen copy of the buffer, and
/// of each trace's decimations of it, by trace id. Both share what the hub
/// goes on with and keep what it drops, down to the held history's budget.
struct Held {
    buffer: Buffer,
    reduced: Vec<Vec<Level>>,
}

impl Held {
    /// Gives the held rows `budget` bytes, dropping the oldest over it, and
    /// drops the decimations of the rows no longer held.
    fn set_budget(&mut self, budget: Option<usize>, traces: &[Trace]) {
        self.buffer.set_budget(budget);
        for (levels, trace) in self.reduced.iter_mut().zip(traces) {
            let run = self.buffer.run(&trace.key.stream_key());
            if let Some(oldest) = run.and_then(Run::first_timestamp) {
                levels
                    .iter_mut()
                    .for_each(|level| level.drop_before(oldest));
            }
        }
    }
}

/// A hub fed by its connection to the proxy, and the devices that connection
/// sees.
pub(crate) struct Live {
    /// Names this hub to the page. A page drops what it holds of a hub as it
    /// hears another's snapshot, and a window request names the hub its
    /// trace id is of.
    pub(crate) id: String,
    /// The hub's thread, which alone holds the hub.
    mailbox: mpsc::Sender<Mail>,
    pub(crate) devices: Arc<Devices>,
    /// The tree the connection sees, which the page's calls go through.
    pub(crate) tree: DeviceTree,
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

/// What the hub's thread is sent: the next batch of samples or why they
/// ended, or a job to run with the hub.
enum Mail {
    Samples(Result<SampleBatch, RecvError>),
    Job(Job),
}

/// Work for the hub's thread, named for the log when it runs long.
struct Job {
    name: &'static str,
    run: Box<dyn FnOnce(&mut Hub) + Send>,
}

impl Live {
    /// Opens `layout`, or the proxy's default for `None`, and starts the
    /// hub's thread, the thread that takes its samples and the watchers of
    /// its devices on `runtime`. Blocks until the proxy has opened it.
    pub(crate) fn start(
        layout: Option<&Layout>,
        runtime: &tokio::runtime::Handle,
    ) -> std::io::Result<Live> {
        let connection = match layout {
            Some(layout) => Connection::open_layout(layout)?,
            None => Connection::connect()?,
        };
        let tree = connection.tree(DeviceRoute::root());
        let (mailbox, mail) = mpsc::channel();
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
        crate::devices::watch(&connection, mounts, devices.clone(), runtime);
        let end = End {
            connection: connection.clone(),
            retired: Arc::new(watch::channel(false).0),
        };
        take_samples(&connection, mailbox.clone())?;
        let ending = end.clone();
        std::thread::Builder::new()
            .name("twinleaf-web hub".to_string())
            .spawn(move || {
                run(hub, &mail);
                ending.retire();
            })?;
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64);
        Ok(Live {
            id: id.to_string(),
            mailbox,
            devices,
            tree,
            next_page: AtomicU64::new(0),
            end,
        })
    }

    /// Runs `work` with the hub on its thread, after what was sent before it.
    fn tell(&self, name: &'static str, work: impl FnOnce(&mut Hub) + Send + 'static) {
        // A hub whose thread has ended drops the job, as it has no use for it.
        let _ = self.mailbox.send(Mail::Job(Job {
            name,
            run: Box::new(work),
        }));
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

    /// A page that connected to this hub, whose spectra go once it does.
    pub(crate) fn page(self: &Arc<Live>) -> Page {
        Page {
            id: self.next_page.fetch_add(1, Ordering::Relaxed),
            live: self.clone(),
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
    ) -> Result<Vec<ExportTrace>, Ended> {
        self.ask("export", move |hub| hub.export(&ids, t0, t1))
            .await
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
    /// everything sent after it. The hub's thread takes both in one job, so
    /// no point is missed or doubled. The devices' messages each replace the
    /// last of their kind, so one heard twice does no harm, and they are
    /// taken after subscribing.
    pub(crate) async fn snapshot(
        &self,
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
        }
        .frame()];
        frames.extend(hub);
        frames.extend(self.devices.snapshot());
        Ok((frames, rx))
    }
}

/// A page on a socket of the hub.
pub(crate) struct Page {
    id: u64,
    live: Arc<Live>,
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
}

impl Drop for Page {
    fn drop(&mut self) {
        self.show_spectra(BTreeSet::new());
        self.hold(false);
    }
}

/// Starts the thread that takes each batch of `connection`'s samples as it
/// comes and posts it to the hub's thread. The connection queues 1024
/// batches and drops those that come while it is full, so taking them never
/// waits on the hub. A busy hub delays the samples and loses none.
fn take_samples(connection: &Connection, mailbox: mpsc::Sender<Mail>) -> std::io::Result<()> {
    let samples = connection.samples();
    std::thread::Builder::new()
        .name("twinleaf-web samples".to_string())
        .spawn(move || loop {
            let next = samples.recv();
            if let Err(RecvError::Lagged(lost)) = next {
                log::warn!("The web page lost {lost} batches of samples");
                continue;
            }
            let ended = next.is_err();
            if mailbox.send(Mail::Samples(next)).is_err() || ended {
                return;
            }
        })?;
    Ok(())
}

/// Runs the hub until its samples end: each batch and job in the order they
/// come, and every FLUSH, SPECTRUM_EVERY and MEMORY_EVERY the new points,
/// the spectra and what the buffer holds. Only this thread touches the hub,
/// and whatever it does delays everything after it, so each job must be
/// short. One that is not is logged.
fn run(mut hub: Hub, mail: &mpsc::Receiver<Mail>) {
    let mut last_flush = Instant::now();
    let mut last_spectrum = Instant::now();
    let mut last_memory = Instant::now();
    loop {
        match mail.recv_timeout(Duration::from_millis(5)) {
            Ok(Mail::Samples(Ok(batch))) => timed("ingest", || hub.ingest(&batch)),
            Ok(Mail::Samples(Err(error))) => {
                log::warn!("The web page's samples ended: {error}");
                return;
            }
            Ok(Mail::Job(job)) => timed(job.name, || (job.run)(&mut hub)),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
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
    run.map(|r| r.boundaries().map(|(_, time, _)| time).collect())
        .unwrap_or_default()
}

impl Hub {
    fn new() -> Hub {
        Hub {
            buffer: {
                let mut buffer = Buffer::with_budget(RAW_BYTES);
                buffer.set_max_age(Some(RAW_SECONDS + HISTORY_HEADROOM));
                buffer
            },
            traces: Vec::new(),
            by_key: HashMap::new(),
            points_per_second: POINTS_PER_SECOND,
            spectra_shown: HashMap::new(),
            held: None,
            holders: BTreeSet::new(),
            budget: Some(RAW_BYTES),
            memory_sent: None,
            feed: Feed::new(),
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
        if batch.is_empty() {
            return;
        }
        self.buffer.process_batch(batch);
        // As the buffer drops what the held history keeps, the held history
        // costs more, and the buffer gets that much less.
        if self.held.is_some() {
            self.share_budget();
        }
        self.register(batch);
    }

    /// Adds a trace for each float column not seen before.
    fn register(&mut self, batch: &SampleBatch) {
        let stream = batch.stream_key();
        let schema = batch.schema();
        let rate = batch.metadata().rate_hz();
        let epoch = batch.metadata().segment().epoch().to_string();
        for (index, column) in schema.columns().enumerate() {
            use twinleaf::proto::data::DataType;
            if !matches!(column.data_type(), DataType::F64 | DataType::F32) {
                continue;
            }
            let key = ColumnKey::new(stream.route, stream.stream_id, ColumnId::new(index as u8));
            if self.by_key.contains_key(&key) {
                continue;
            }
            let info = TraceInfo {
                id: self.traces.len() as u16,
                route: stream.route.to_string(),
                stream: schema.stream().name().to_string(),
                column: column.name().to_string(),
                units: column.units().to_string(),
                rate,
                epoch: epoch.clone(),
            };
            let window = ((rate * 8.0) as usize).clamp(256, WELCH_WINDOW);
            let decimation = self.levels_for(rate);
            self.feed.message(&Message::Trace {
                trace: info.clone(),
            });
            self.by_key.insert(key, self.traces.len());
            self.traces.push(Trace {
                info,
                key,
                live: ColumnProcessor::new(key, decimation),
                reduced: ColumnProcessor::new(key, Pyramid::new()),
                spectrum: ColumnProcessor::new(key, Welch::new(window, rate)),
                last_sent: vec![f64::NEG_INFINITY; LEVELS],
                generation: None,
            });
        }
    }

    /// Catches each trace's decimators up with the buffer and sends the new
    /// points of every level. The decimations windows are read from keep
    /// what the buffer still holds rows of.
    fn flush(&mut self) {
        let mut chunks = Vec::new();
        for trace in &mut self.traces {
            let Some(run) = self.buffer.run(&trace.key.stream_key()) else {
                continue;
            };
            trace.reduced.catch_up(&self.buffer);
            if let Some(oldest) = run.first_timestamp() {
                trace.reduced.op_mut().drop_before(oldest);
            }
            let generation = run.generations().stream;
            let new_run = trace.generation.is_some_and(|g| g != generation);
            trace.generation = Some(generation);
            let breaks = gap_times(Some(run));
            let levels = trace.live.catch_up(&self.buffer);
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
                let estimate = trace.spectrum.catch_up(&self.buffer).as_ref().ok()?;
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
            let breaks = gap_times(self.buffer.run(&trace.key.stream_key()));
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
    /// it go once no page holds it. While held, the buffer goes on keeping
    /// the newest rows in what of the budget the held history leaves.
    fn hold(&mut self, page: u64, on: bool) {
        let was = !self.holders.is_empty();
        if on {
            self.holders.insert(page);
        } else {
            self.holders.remove(&page);
        }
        match (was, !self.holders.is_empty()) {
            (false, true) => {
                let mut held = Held {
                    buffer: self.buffer.frozen(),
                    reduced: (self.traces.iter_mut())
                        .map(|trace| trace.reduced.catch_up(&self.buffer).clone())
                        .collect(),
                };
                held.set_budget(self.held_budget(), &self.traces);
                self.held = Some(held);
            }
            (true, false) => self.held = None,
            _ => return,
        }
        self.share_budget();
        self.memory_sent = None;
        self.send_memory();
    }

    /// The bytes the held history may keep: HELD_SHARE of the budget.
    fn held_budget(&self) -> Option<usize> {
        self.budget.map(|b| (b as f64 * HELD_SHARE) as usize)
    }

    /// Gives the buffer what of the budget the held history leaves: the bytes
    /// the held history alone holds, as the chunks both hold are counted once,
    /// in the buffer.
    fn share_budget(&mut self) {
        let held = self
            .held
            .as_ref()
            .map_or(0, |held| held.buffer.bytes_not_in(&self.buffer));
        self.buffer
            .set_budget(self.budget.map(|b| b.saturating_sub(held)));
    }

    /// Where the rows of `key` within [t0, t1] are, in time order: the held
    /// history's up to where the buffer begins, then the buffer's.
    fn spans(&self, key: &StreamKey, t0: f64, t1: f64) -> Vec<Span<'_>> {
        let live = self.buffer.run(key);
        let live_first = live.and_then(Run::first_timestamp);
        let mut spans = Vec::new();
        if let Some(run) = self.held.as_ref().and_then(|h| h.buffer.run(key)) {
            if live_first.is_none_or(|first| first > t0) {
                // Up to just before the buffer's first row, which the buffer reads.
                let end = live_first.map_or(t1, |first| t1.min(first - 1e-9));
                let rows = run.rows_between(t0, end);
                spans.push(Span {
                    run,
                    rows,
                    held: true,
                });
            }
        }
        if let Some(run) = live {
            let rows = run.rows_between(t0, t1);
            spans.push(Span {
                run,
                rows,
                held: false,
            });
        }
        spans.retain(|span| span.rows.start < span.rows.end);
        spans
    }

    /// The oldest time the history holds of `key`, held or not.
    fn oldest(&self, key: &StreamKey) -> Option<f64> {
        let held = self
            .held
            .as_ref()
            .and_then(|h| h.buffer.run(key))
            .and_then(Run::first_timestamp);
        let live = self.buffer.run(key).and_then(Run::first_timestamp);
        held.into_iter().chain(live).reduce(f64::min)
    }

    /// What the buffer holds and may hold, as a `memory` message.
    fn memory(&self) -> Message {
        let (mut rate, mut seconds) = (0.0, f64::INFINITY);
        for key in self.buffer.stream_keys() {
            let Some(run) = self.buffer.run(key) else {
                continue;
            };
            rate += run.row_bytes() as f64 * run.metadata().rate_hz();
            let rows = run.retained_rows();
            let first = run
                .rows(rows.start..rows.start + 1)
                .first()
                .and_then(|b| b.timestamps().first().copied());
            if let (Some(first), Some(last)) = (first, run.last_timestamp()) {
                seconds = seconds.min(last - first);
            }
        }
        let held = self.held.as_ref().map(|held| protocol::Held {
            bytes: held.buffer.bytes_not_in(&self.buffer) as u64,
        });
        let oldest = self
            .traces
            .iter()
            .filter_map(|trace| Some((trace.info.id, self.oldest(&trace.key.stream_key())?)))
            .collect();
        Message::Memory {
            bytes: self.buffer.bytes() as u64,
            budget: self.budget.unwrap_or(0) as u64,
            // The setting and what is held of it: the headroom is not history a page counts.
            seconds: if seconds.is_finite() {
                seconds.min(self.history_seconds)
            } else {
                0.0
            },
            max_age: self.history_seconds,
            rate,
            held,
            oldest,
        }
    }

    /// Sets the raw history the buffer keeps, `seconds` and
    /// HISTORY_HEADROOM more of each stream within `bytes`, dropping what no
    /// longer fits at once, and the most each level of the decimation keeps,
    /// and tells every page.
    fn set_history(&mut self, seconds: f64, bytes: usize) {
        self.history_seconds = seconds;
        for trace in &mut self.traces {
            trace.live.op_mut().set_retention(HISTORY_SECONDS, seconds);
        }
        self.feed.message(&self.config());
        self.buffer.set_max_age(Some(seconds + HISTORY_HEADROOM));
        self.budget = Some(bytes);
        let held_budget = self.held_budget();
        if let Some(held) = &mut self.held {
            held.set_budget(held_budget, &self.traces);
        }
        self.share_budget();
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
            oldest,
            ..
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
        let frozen = self.buffer.frozen();
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
                let _ = mailbox.send(Mail::Job(Job {
                    name: "decimated",
                    run: Box::new(move |hub| hub.decimated(decimation, levels)),
                }));
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
            for (fpcs, last_sent) in live.catch_up(&self.buffer).iter().zip(&mut trace.last_sent) {
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
        self.buffer.run(&key)?;
        let spans = self
            .spans(&key, t0, t1)
            .iter()
            .map(GatheredSpan::of)
            .collect();
        Some(Gathered {
            column: trace.key.column_id,
            oldest: self.oldest(&key),
            spans,
        })
    }

    /// Trace `id` within [t0, t1] for a view `width` pixels wide, at about
    /// two points a pixel, to make a window of once the hub is released.
    /// Fewer than FINEST rows a point are the rows themselves, to decimate.
    /// More are the points of the level of the trace's decimations whose
    /// windows are the longest no longer than that, so no coarser than the
    /// rows decimated would be, with each end's own row. The held history's
    /// part is read from the decimations held with it.
    fn window(&mut self, id: u16, t0: f64, t1: f64, width: usize) -> Option<WindowRead> {
        let trace = self.traces.get_mut(id as usize)?;
        let key = trace.key.stream_key();
        self.buffer.run(&key)?;
        trace.reduced.catch_up(&self.buffer);
        let trace = &self.traces[id as usize];
        let column = trace.key.column_id;
        let spans = self.spans(&key, t0, t1);
        let rows: u64 = spans
            .iter()
            .map(|span| span.rows.end - span.rows.start)
            .sum();
        let ratio = (rows as usize).div_ceil((2 * width).max(64)).max(1);
        let level = pyramid::level_for(ratio);
        let spans = spans
            .iter()
            .map(|span| {
                let levels = match &self.held {
                    Some(held) if span.held => held.reduced.get(id as usize),
                    _ => Some(trace.reduced.output()),
                };
                level
                    .and_then(|(level, _)| levels?.get(level))
                    .and_then(|level| ReducedSpan::of(span, column, level))
                    .map_or_else(|| SpanRead::Rows(GatheredSpan::of(span)), SpanRead::Reduced)
            })
            .collect();
        Some(WindowRead {
            t0,
            t1,
            column,
            ratio: level.map_or(ratio, |(_, ratio)| ratio),
            spans,
        })
    }

    /// Every raw row of traces `ids` within [t0, t1], from the history held
    /// for paused plots where the buffer no longer reaches, to export. The
    /// batches share the history's chunks, so gathering them copies no rows.
    /// An id of no trace is passed over.
    fn export(&self, ids: &[u16], t0: f64, t1: f64) -> Vec<ExportTrace> {
        ids.iter()
            .filter_map(|&id| {
                let trace = self.traces.get(id as usize)?;
                let key = trace.key.stream_key();
                let batches = self
                    .spans(&key, t0, t1)
                    .into_iter()
                    .flat_map(|span| span.run.rows(span.rows))
                    .collect();
                Some(ExportTrace {
                    name: signal_name(&trace.info),
                    units: trace.info.units.clone(),
                    unix: trace.info.epoch == "unix",
                    column: trace.key.column_id,
                    batches,
                })
            })
            .collect()
    }
}

/// Rows of one run to read: the held history's, or the buffer's.
struct Span<'a> {
    run: &'a Run,
    rows: Range<u64>,
    held: bool,
}

/// The raw rows of one trace within a range of time, gathered on the hub's
/// thread and read off it. Reading copies every row, which would hold up
/// the samples behind it.
pub(crate) struct Gathered {
    column: ColumnId,
    /// The oldest time the history holds of the trace.
    oldest: Option<f64>,
    /// The held history's rows, then the buffer's.
    spans: Vec<GatheredSpan>,
}

/// The rows of one run within a range of time.
struct GatheredSpan {
    /// The run's row number of the first row.
    start: u64,
    len: u64,
    /// The row numbers, in order, at which the run's samples were lost.
    gaps: Vec<u64>,
    batches: Vec<SampleBatch>,
}

impl GatheredSpan {
    /// The rows of `span`, which share the run's chunks.
    fn of(span: &Span) -> GatheredSpan {
        let rows = span.rows.clone();
        GatheredSpan {
            start: rows.start,
            len: rows.end - rows.start,
            gaps: (span.run.boundaries())
                .map(|(row, _, _)| row)
                .filter(|row| rows.contains(row))
                .collect(),
            batches: span.run.rows(rows),
        }
    }

    /// The rows of `column`, split where samples were lost, each stretch
    /// decimated by `ratio`, which may be one, with its first and last rows
    /// kept, as the decimator emits the earlier extreme of its first window
    /// and holds back its last.
    fn decimated(&self, column: ColumnId, ratio: usize) -> Vec<Vec<Point>> {
        let mut segments = Vec::new();
        let mut fpcs = Fpcs::new(ratio, 0.0);
        // The first and last rows fed to the decimator since the last loss.
        let mut ends: Option<(Point, Point)> = None;
        let mut finish = |fpcs: &mut Fpcs, ends: &mut Option<(Point, Point)>| {
            let Some((first, last)) = ends.take() else {
                return;
            };
            let emitted = fpcs.output();
            let mut points = Vec::with_capacity(emitted.len() + 2);
            if emitted.first().is_none_or(|point| first.0 < point.0) {
                points.push(first);
            }
            points.extend_from_slice(emitted);
            if points.last().is_some_and(|point| point.0 < last.0) {
                points.push(last);
            }
            segments.push(points);
            fpcs.reset();
        };
        let mut gaps = self.gaps.iter().copied().peekable();
        let mut row = self.start;
        for batch in &self.batches {
            let (times, end) = (batch.timestamps(), row + batch.len() as u64);
            let mut from = 0;
            while from < times.len() {
                let lost = gaps.next_if(|&gap| gap < end);
                let to = lost.map_or(times.len(), |gap| gap.saturating_sub(row) as usize);
                let values = batch.column(column).map(|values| values.slice(from..to));
                if let Some(values) = values.filter(|values| !values.is_empty()) {
                    fpcs.update_batch(&times[from..to], &values);
                    let row = |at: usize| Some((times[from + at], values.get(at).try_as_f64()?));
                    if let Some((first, last)) = row(0).zip(row(to - from - 1)) {
                        ends = Some((ends.map_or(first, |ends| ends.0), last));
                    }
                }
                if lost.is_some() {
                    finish(&mut fpcs, &mut ends);
                }
                from = to;
            }
            row = end;
        }
        finish(&mut fpcs, &mut ends);
        segments
    }
}

/// The points of one run within a range of time at a level of its
/// decimations, with the range's first and last rows. Every point is a row
/// of the run.
struct ReducedSpan {
    first: Point,
    last: Point,
    slices: Vec<Slice>,
    /// The time of the first row after each loss of samples, in order.
    gaps: Vec<f64>,
}

impl ReducedSpan {
    /// The points of `span` at `level`, one of the decimations of its run,
    /// or `None` for a run without `column`. It finds them by bisection,
    /// and copies none but the level's newest, fewer than a block.
    fn of(span: &Span, column: ColumnId, level: &Level) -> Option<ReducedSpan> {
        let (run, rows) = (span.run, span.rows.clone());
        let row = |row: u64| -> Option<Point> {
            let batch = run.rows(row..row + 1).pop()?;
            let value = batch.column(column)?.get(0).try_as_f64()?;
            Some((batch.timestamps()[0], value))
        };
        let (first, last) = (row(rows.start)?, row(rows.end - 1)?);
        Some(ReducedSpan {
            first,
            last,
            slices: level.between(first.0, last.0),
            gaps: (run.boundaries())
                .filter(|(row, _, _)| rows.contains(row))
                .map(|(_, time, _)| time)
                .collect(),
        })
    }

    /// The points, split where samples were lost.
    fn segments(&self) -> Vec<Vec<Point>> {
        let mut segments: Vec<Vec<Point>> = vec![Vec::new()];
        let mut gaps = self.gaps.iter().peekable();
        let leveled = self.slices.iter().flat_map(|slice| slice.points());
        let ends = [self.first, self.last];
        let mut newest = f64::NEG_INFINITY;
        // The first row, the level's points, then the last row, each end
        // unless the level has it. They are in time order but for the ends.
        for &point in ends[..1].iter().chain(leveled).chain(&ends[1..]) {
            if point.0 <= newest {
                continue;
            }
            newest = point.0;
            let mut lost = false;
            while gaps.next_if(|&&gap| gap <= point.0).is_some() {
                lost = true;
            }
            if lost && !segments.last().unwrap().is_empty() {
                segments.push(Vec::new());
            }
            segments.last_mut().unwrap().push(point);
        }
        segments
    }
}

/// One trace within [t0, t1], gathered on the hub's thread for a window to
/// be made of off it, as the copying and decimating would hold up the
/// samples behind it.
pub(crate) struct WindowRead {
    t0: f64,
    t1: f64,
    column: ColumnId,
    /// The rows a point of the window stands for.
    ratio: usize,
    /// The held history's part, then the buffer's.
    spans: Vec<SpanRead>,
}

/// One run's part of a [`WindowRead`].
enum SpanRead {
    /// Its rows, to decimate by the window's ratio.
    Rows(GatheredSpan),
    /// Its points at the window's ratio, already decimated.
    Reduced(ReducedSpan),
}

impl WindowRead {
    /// The window: its points, split at each loss of samples and where the
    /// held history ends and the buffer begins, which may be a hole.
    pub(crate) fn window(self) -> Window {
        let mut chunks = Vec::new();
        for span in &self.spans {
            let segments = match span {
                SpanRead::Rows(span) => span.decimated(self.column, self.ratio),
                SpanRead::Reduced(span) => span.segments(),
            };
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

    /// How many rows and points of the history the window is made of.
    #[cfg(test)]
    fn touched(&self) -> usize {
        (self.spans.iter())
            .map(|span| match span {
                SpanRead::Rows(span) => span.len as usize,
                SpanRead::Reduced(span) => {
                    let points = span.slices.iter().map(|slice| slice.points().len());
                    2 + points.sum::<usize>()
                }
            })
            .sum()
    }
}

impl Gathered {
    /// Every row, none decimated; or, when there are more than `limit`, only
    /// how many. With them, the oldest time the history still holds of the
    /// trace.
    pub(crate) fn rows(self, limit: usize) -> Rows {
        let count = self.spans.iter().map(|span| span.len as usize).sum();
        let (mut x, mut y) = (Vec::new(), Vec::new());
        if count <= limit {
            for batch in self.spans.iter().flat_map(|span| &span.batches) {
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
    use proptest::prelude::*;
    use twinleaf::data::PacketParser;
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
        let (id, route) = (StreamId::new(1), DeviceRoute::root());
        let mut parser = PacketParser::new(false);
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
        parser
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
    fn while_a_page_holds_the_history_a_window_reads_what_the_buffer_dropped() {
        let mut hub = Hub::new();
        let mut parser = parser();
        // 20 bytes a row: half the budget holds the first 1000 rows.
        hub.set_history(1e6, 40_000);
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
        assert_eq!(hub.buffer.budget(), Some(40_000));
    }

    #[test]
    fn a_range_across_the_held_history_and_the_buffer_reads_both_once() {
        let mut hub = Hub::new();
        let mut parser = parser();
        hub.set_history(1e6, 40_000);
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
    fn an_export_writes_every_raw_row_in_range_as_csv() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        let traces = hub.export(&[0, 99], 1.0, 1.05);
        // The id of no trace is passed over.
        assert_eq!(traces.len(), 1);
        let mut out = Vec::new();
        crate::export::write_csv(&traces, &mut out).unwrap();
        let csv = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert!(lines[0].starts_with("time (Unix s),"));
        // At 100 Hz, rows 99 to 104 stand at 1.00 to 1.05 s.
        assert_eq!(lines.len(), 1 + 6);
        assert_eq!(lines[1], "1,99");
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
    fn run_posted(hub: &mut Hub, mail: &mpsc::Receiver<Mail>) -> &'static str {
        match mail.recv_timeout(Duration::from_secs(10)) {
            Ok(Mail::Job(job)) => {
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
        let (mailbox, mail) = mpsc::channel();
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
            .buffer
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
        let (mailbox, mail) = mpsc::channel();
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

    /// The window of [t0, t1] made by decimating every row of the range for
    /// a view `width` pixels wide, or of every row itself for no width,
    /// which a window read from the levels is held against.
    fn of_rows(hub: &Hub, t0: f64, t1: f64, width: Option<usize>) -> Window {
        let trace = &hub.traces[0];
        let spans = hub.spans(&trace.key.stream_key(), t0, t1);
        let rows: u64 = spans.iter().map(|s| s.rows.end - s.rows.start).sum();
        let ratio = width.map_or(1, |width| (rows as usize).div_ceil((2 * width).max(64)));
        let read = WindowRead {
            t0,
            t1,
            column: trace.key.column_id,
            ratio: ratio.max(1),
            spans: (spans.iter())
                .map(|span| SpanRead::Rows(GatheredSpan::of(span)))
                .collect(),
        };
        read.window()
    }

    /// Reads the window of [t0, t1] at `width` and holds it against the rows
    /// of its range:
    /// - its points are rows of the range, in time order, the first and the
    ///   last row among them;
    /// - it is no coarser than the rows decimated for it, and the same as
    ///   those when they are fewer than FINEST a point;
    /// - it is split wherever samples were lost between two of its points,
    ///   and where the held history ends, and nowhere else;
    /// - read from a level, each row further than two of the level's
    ///   windows from the ends of its run's part has a point at least as
    ///   high and one at least as low within two windows of it.
    fn checked(hub: &mut Hub, t0: f64, t1: f64, width: usize) -> Result<Window, TestCaseError> {
        let window = hub.window(0, t0, t1, width).unwrap().window();
        let rows = hub.gather(0, t0, t1).unwrap().rows(usize::MAX);
        let reference = of_rows(hub, t0, t1, Some(width));
        let chunks = |window: &Window| -> Vec<(Vec<f64>, Vec<f64>)> {
            let chunk = |chunk: &WindowChunk| (chunk.x.clone(), chunk.y.clone());
            window.chunks.iter().map(chunk).collect()
        };
        let points: Vec<Point> = chunks(&window)
            .into_iter()
            .flat_map(|(x, y)| x.into_iter().zip(y))
            .collect();

        prop_assert!(points.windows(2).all(|pair| pair[0].0 < pair[1].0));
        let index: Vec<usize> = points
            .iter()
            .map(|point| rows.x.partition_point(|&t| t < point.0))
            .collect();
        for (point, &i) in points.iter().zip(&index) {
            prop_assert_eq!(rows.x.get(i).map(|&t| (t, rows.y[i])), Some(*point));
        }
        let row = |i: usize| (rows.x[i], rows.y[i]);
        prop_assert_eq!(points.first().copied(), (rows.count > 0).then(|| row(0)));
        prop_assert_eq!(points.last().copied(), rows.count.checked_sub(1).map(row));

        prop_assert!(window.ratio <= reference.ratio);
        if reference.ratio < pyramid::FINEST {
            prop_assert_eq!(window.ratio, reference.ratio);
            prop_assert_eq!(chunks(&window), chunks(&reference));
        }

        // The time of the first row after each loss, and of each part.
        let key = hub.traces[0].key.stream_key();
        let spans = hub.spans(&key, t0, t1);
        let mut breaks = Vec::new();
        let mut parts = Vec::new();
        for span in &spans {
            let start = parts.last().map_or(0, |part: &Range<usize>| part.end);
            if start > 0 {
                breaks.push(rows.x[start]);
            }
            let lost = span.run.boundaries().filter(|b| span.rows.contains(&b.0));
            breaks.extend(lost.map(|(_, time, _)| time));
            parts.push(start..start + (span.rows.end - span.rows.start) as usize);
        }
        let broken = |a: f64, b: f64| breaks.iter().any(|&at| a < at && at <= b);
        for chunk in &window.chunks {
            prop_assert!(chunk.x.windows(2).all(|pair| !broken(pair[0], pair[1])));
        }
        for pair in window.chunks.windows(2) {
            prop_assert!(broken(*pair[0].x.last().unwrap(), pair[1].x[0]));
        }

        if window.ratio >= pyramid::FINEST {
            let reach = 2 * window.ratio;
            for part in parts {
                for i in part.start + reach..part.end.saturating_sub(reach) {
                    let first = index.partition_point(|&at| at + reach <= i);
                    let near = (index[first..].iter()).take_while(|&&at| at < i + reach);
                    let (low, high) = near.fold((f64::MAX, f64::MIN), |(low, high), &at| {
                        (low.min(rows.y[at]), high.max(rows.y[at]))
                    });
                    prop_assert!(low <= rows.y[i] && rows.y[i] <= high, "row {}", i);
                }
            }
        }
        Ok(window)
    }

    /// Feeds the hub samples `first..first + n` at 100 Hz, as `spiky` values
    /// them, and flushes it.
    fn feed(hub: &mut Hub, parser: &mut PacketParser, first: u32, n: u32, seed: u32) {
        for batch in valued(parser, 100, first, n, |i| spiky(i, seed)) {
            hub.ingest(&batch);
        }
        hub.flush();
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// A window of any span, place and width shows the rows of its
        /// range as `checked` says, whatever samples were lost, and with a
        /// history held from any moment on within any budget.
        #[test]
        fn a_window_shows_the_rows_of_its_range(
            n in 2_000u32..60_000,
            losses in prop::collection::vec((0.0..1.0f64, 1u32..400), 0..4),
            hold in prop::option::of((0.1..0.9f64, 0.2..2.0f64)),
            views in prop::collection::vec((0.0..1.0f64, 0.0005..1.2f64, 16usize..120), 1..6),
            seed in any::<u32>(),
        ) {
            let mut hub = Hub::new();
            let mut parser = parser();
            // The rows after which samples are lost or the history is held.
            let mut stops: Vec<(u32, u32)> = losses
                .iter()
                .map(|&(at, lost)| ((at * f64::from(n)) as u32, lost))
                .collect();
            if let Some((at, budget)) = hold {
                hub.set_history(1e9, (budget * f64::from(n) * 20.0) as usize);
                stops.push(((at * f64::from(n)) as u32, 0));
            }
            stops.push((n, 0));
            stops.sort();
            let (mut fed, mut number) = (0, 0);
            for (at, lost) in stops {
                feed(&mut hub, &mut parser, number, at - fed, seed);
                number += at - fed + lost;
                fed = at;
                if lost == 0 && fed < n {
                    hub.hold(1, true);
                }
            }
            let end = f64::from(number) / 100.0;
            for (from, span, width) in views {
                checked(&mut hub, from * end, (from + span) * end, width)?;
            }
        }
    }

    /// A window's rows and points are bounded by its width, whatever its
    /// span: fewer than FINEST rows a point of the rows themselves, and
    /// otherwise the points of a level, at most two a window.
    #[test]
    fn a_window_costs_by_its_width_not_its_rows() {
        let mut hub = Hub::new();
        let mut parser = parser();
        hub.set_history(1e9, RAW_BYTES);
        let n = 400_000;
        feed(&mut hub, &mut parser, 0, n, 7);
        let (width, points) = (100, 200);
        let mut leveled = 0;
        for span in [1.0, 10.0, 100.0, 200.0, 300.0, 600.0, 1500.0, 3999.0, 1e6] {
            let (t0, t1) = (2000.0 - span / 2.0, 2000.0 + span / 2.0);
            let read = hub.window(0, t0, t1, width).unwrap();
            let rows = hub.gather(0, t0, t1).unwrap().rows(0).count;
            if rows < pyramid::FINEST * points {
                assert_eq!(read.ratio, rows.div_ceil(points), "span {span}");
                assert!(read.touched() <= rows + 1, "span {span}");
            } else {
                leveled += 1;
                assert!(read.ratio >= pyramid::FINEST, "span {span}");
                assert!(
                    read.touched() <= 4 * points + 8,
                    "span {span} touched {}",
                    read.touched()
                );
            }
            let made: usize = read.window().chunks.iter().map(|c| c.x.len()).sum();
            assert!(made <= 4 * points + 8, "span {span} made {made}");
        }
        assert_eq!(leveled, 5);
    }

    /// The held history's part of a window is read from the levels held
    /// with it, which keep what the hub's own have dropped since, and both
    /// drop the levels of the rows their budgets let go.
    #[test]
    fn a_window_reads_the_held_history_from_the_levels_held_with_it() {
        let mut hub = Hub::new();
        let mut parser = parser();
        let key = StreamKey::new(DeviceRoute::root(), StreamId::new(1));
        let first = |level: &Level| level.between(f64::MIN, f64::MAX)[0].points()[0].0;
        // 16 bytes a row: 187,500 rows in all, half of them the held
        // history's once held. At 100 Hz, row n stands at (n + 1) / 100 s.
        hub.set_history(1e9, 3_000_000);
        for start in (0..250_000).step_by(10_000) {
            feed(&mut hub, &mut parser, start, 10_000, 3);
        }
        let before = first(&hub.traces[0].reduced.output()[0]);
        hub.hold(1, true);
        // The held history is about rows 160,000 to 250,000, and its levels
        // have dropped a block the hub's own, of rows from 62,500, keep.
        let held = hub.held.as_ref().unwrap();
        let held_from = held.buffer.run(&key).unwrap().first_timestamp().unwrap();
        assert!((1500.0..1800.0).contains(&held_from), "{held_from}");
        let held_first = first(&held.reduced[0][0]);
        assert!(before < held_first && held_first <= held_from);
        assert!(held_from - held_first < 1000.0, "less than a block before");
        // The buffer goes on to about rows 310,000 to 400,000, and its
        // levels drop the blocks of the rows it dropped.
        for start in (250_000..400_000).step_by(10_000) {
            feed(&mut hub, &mut parser, start, 10_000, 3);
        }
        let live_from = hub.buffer.run(&key).unwrap().first_timestamp().unwrap();
        assert!((3000.0..3300.0).contains(&live_from), "{live_from}");
        let live_first = first(&hub.traces[0].reduced.output()[0]);
        assert!(2500.0 < live_first && live_first <= live_from);
        // Wide enough to read the finest level, of which the hub's own no
        // longer has the held history's points.
        let window = checked(&mut hub, 0.0, 1e9, 400).unwrap();
        assert_eq!(window.ratio, pyramid::FINEST);
        assert_eq!(window.chunks.len(), 2);
        assert_eq!(window.chunks[0].x.first(), Some(&held_from));
        assert_eq!(window.chunks[0].x.last(), Some(&2500.0));
        assert_eq!(window.chunks[1].x.first(), Some(&live_from));
        // Let go, the window is the buffer's part alone.
        hub.hold(1, false);
        let window = checked(&mut hub, 0.0, 1e9, 400).unwrap();
        assert_eq!(window.chunks.len(), 1);
        assert_eq!(window.chunks[0].x.first(), Some(&live_from));
    }

    /// The lowest and the highest the line through `window`'s points goes
    /// in each of `columns` equal columns of its range, as a plot that wide
    /// shows it.
    fn drawn(window: &Window, columns: usize) -> Vec<(f64, f64)> {
        let mut drawn = vec![(f64::INFINITY, f64::NEG_INFINITY); columns];
        let scale = columns as f64 / (window.t1 - window.t0);
        let column = |t: f64| (((t - window.t0) * scale) as usize).min(columns - 1);
        let mut mark = |column: usize, y: f64| {
            let (low, high) = drawn[column];
            drawn[column] = (low.min(y), high.max(y));
        };
        for chunk in &window.chunks {
            for (i, (&x, &y)) in chunk.x.iter().zip(&chunk.y).enumerate() {
                mark(column(x), y);
                let Some((&next_x, &next_y)) = chunk.x.get(i + 1).zip(chunk.y.get(i + 1)) else {
                    continue;
                };
                // Where the line to the next point crosses into each column.
                for crossed in column(x) + 1..=column(next_x) {
                    let at = window.t0 + crossed as f64 / scale;
                    let height = y + (next_y - y) * (at - x) / (next_x - x);
                    mark(crossed - 1, height);
                    mark(crossed, height);
                }
            }
        }
        drawn
    }

    /// Prints what a window read costs at each span of `WINDOW_HOURS` hours
    /// (one by default) of a 5 kHz signal, and how far what a plot draws of
    /// it lies from what it draws of every row. Run it in a release build, as
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
        let levels: usize = hub.traces[0].reduced.output().iter().map(Level::len).sum();
        println!("{rows} rows: ingest {ingest:?}, flush {flush:?}");
        println!(
            "the buffer holds {} bytes, the levels {levels} points",
            hub.buffer.bytes()
        );
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
        // What a plot 1420 px wide draws of each window, against what it
        // draws of every row: the furthest and the mean distance of a
        // column's lowest and highest, for the rows decimated for the window
        // and for the window read from the levels.
        println!("span s | decimated: worst, mean, ms | from the levels: worst, mean");
        for span in spans.into_iter().filter(|&span| span <= end.min(14400.0)) {
            let t0 = (end - span) / 2.0;
            let every = drawn(&of_rows(&hub, t0, t0 + span, None), 1420);
            let off = |window: &Window| {
                let columns = drawn(window, 1420);
                let apart = (columns.iter().zip(&every))
                    .filter(|(_, every)| every.0 <= every.1)
                    .map(|(a, b)| (a.0 - b.0).abs().max((a.1 - b.1).abs()));
                let (worst, sum, n) = apart.fold((0.0f64, 0.0, 0), |(worst, sum, n), d| {
                    (worst.max(d), sum + d, n + 1)
                });
                format!("{worst:.4}, {:.5}", sum / f64::from(n))
            };
            let start = Instant::now();
            let decimated = of_rows(&hub, t0, t0 + span, Some(width));
            let took = start.elapsed();
            let window = hub.window(0, t0, t0 + span, width).unwrap().window();
            println!(
                "{span} | {}, {:.1} | {}",
                off(&decimated),
                ms(took),
                off(&window)
            );
        }
    }
}

//! The hub, which keeps what every page shows, from one connection to the
//! proxy.
//!
//! Every batch goes into one [`Buffer`], which keeps the newest raw rows of
//! each stream. Each trace, one float column, is a set of
//! [`ColumnProcessor`]s over that buffer. `FpcsLevels` decimates it for the
//! live view at several ratios, each LEVEL_FACTOR times the last, none kept
//! longer than the raw history, and `Welch` estimates its spectrum. Every
//! level goes to every page, which draws each plot from the level that suits
//! its span. A window read gives the raw rows between two times, for a
//! paused view zoomed in.
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
use crate::protocol::{Frame, Held, Message, MountInfo, Rows, TraceInfo, Window, WindowChunk};
use crate::wire::{encode, encode_spectra, split_at_breaks, Chunk, Spectrum};
use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};
use twinleaf::data::ops::{Fpcs, FpcsLevels, Welch};
use twinleaf::data::{Buffer, ColumnArray, ColumnKey, ColumnOp, ColumnProcessor, Run, StreamKey};
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
    held: Option<Buffer>,
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
                spectrum: ColumnProcessor::new(key, Welch::new(window, rate)),
                last_sent: vec![f64::NEG_INFINITY; LEVELS],
                generation: None,
            });
        }
    }

    /// Catches each trace's decimator up with the buffer and sends the new
    /// points of every level.
    fn flush(&mut self) {
        let mut chunks = Vec::new();
        for trace in &mut self.traces {
            let Some(run) = self.buffer.run(&trace.key.stream_key()) else {
                continue;
            };
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
                let mut held = self.buffer.frozen();
                held.set_budget(self.held_budget());
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
            .map_or(0, |held| held.bytes_not_in(&self.buffer));
        self.buffer
            .set_budget(self.budget.map(|b| b.saturating_sub(held)));
    }

    /// Where the rows of `key` within [t0, t1] are, in time order: the held
    /// history's up to where the buffer begins, then the buffer's. Each is a
    /// run and the rows of it to read.
    fn spans(&self, key: &StreamKey, t0: f64, t1: f64) -> Vec<(&Run, Range<u64>)> {
        let live = self.buffer.run(key);
        let live_first = live.and_then(Run::first_timestamp);
        let mut spans = Vec::new();
        if let Some(held) = self.held.as_ref().and_then(|h| h.run(key)) {
            if live_first.is_none_or(|first| first > t0) {
                // Up to just before the buffer's first row, which the buffer reads.
                let end = live_first.map_or(t1, |first| t1.min(first - 1e-9));
                spans.push((held, held.rows_between(t0, end)));
            }
        }
        if let Some(live) = live {
            spans.push((live, live.rows_between(t0, t1)));
        }
        spans.retain(|(_, rows)| rows.start < rows.end);
        spans
    }

    /// The oldest time the history holds of `key`, held or not.
    fn oldest(&self, key: &StreamKey) -> Option<f64> {
        let held = self
            .held
            .as_ref()
            .and_then(|h| h.run(key))
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
        let held = self.held.as_ref().map(|held| Held {
            bytes: held.bytes_not_in(&self.buffer) as u64,
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
            held.set_budget(held_budget);
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
            .into_iter()
            .map(|(run, rows)| GatheredSpan {
                start: rows.start,
                len: rows.end - rows.start,
                gaps: run
                    .boundaries()
                    .map(|(row, _, _)| row)
                    .filter(|row| rows.contains(row))
                    .collect(),
                batches: run.rows(rows),
            })
            .collect();
        Some(Gathered {
            t0,
            t1,
            column: trace.key.column_id,
            oldest: self.oldest(&key),
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
                    .flat_map(|(run, rows)| run.rows(rows))
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

/// The raw rows of one trace within [t0, t1], gathered on the hub's thread
/// and read off it. Reading copies every row and may decimate them, which
/// would hold up the samples behind it.
pub(crate) struct Gathered {
    t0: f64,
    t1: f64,
    column: ColumnId,
    /// The oldest time the history holds of the trace.
    oldest: Option<f64>,
    /// The held history's rows, then the buffer's.
    spans: Vec<GatheredSpan>,
}

/// The rows of one run within a [`Gathered`] range.
struct GatheredSpan {
    /// The run's row number of the first row.
    start: u64,
    len: u64,
    /// The row numbers, in order, at which the run's samples were lost.
    gaps: Vec<u64>,
    batches: Vec<SampleBatch>,
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

    /// The rows split at each gap, decimated to about two points per pixel
    /// of `width` only when there are more than that.
    pub(crate) fn window(self, width: usize) -> Window {
        let mut segments: Vec<Vec<(f64, f64)>> = vec![Vec::new()];
        let mut raw: usize = 0;
        for span in &self.spans {
            raw += span.len as usize;
            // Where the held history ends and the buffer begins may be a hole,
            // so a new span is a new segment, which the page draws apart.
            if !segments.last().unwrap().is_empty() {
                segments.push(Vec::new());
            }
            let mut row = span.start;
            for batch in &span.batches {
                let Some(values) = batch.column(self.column) else {
                    row += batch.timestamps().len() as u64;
                    continue;
                };
                for (&t, &v) in batch.timestamps().iter().zip(values.to_f64().iter()) {
                    if span.gaps.binary_search(&row).is_ok() && !segments.last().unwrap().is_empty()
                    {
                        segments.push(Vec::new());
                    }
                    segments.last_mut().unwrap().push((t, v));
                    row += 1;
                }
            }
        }
        segments.retain(|s| !s.is_empty());
        let budget = (2 * width).max(64);
        let ratio = raw.div_ceil(budget).max(1);
        let chunks = segments
            .into_iter()
            .map(|seg| {
                let points = if ratio == 1 {
                    seg
                } else {
                    decimate(&seg, ratio)
                };
                WindowChunk {
                    x: points.iter().map(|p| p.0).collect(),
                    y: points.iter().map(|p| p.1).collect(),
                }
            })
            .collect();
        Window {
            t0: self.t0,
            t1: self.t1,
            ratio,
            chunks,
        }
    }
}

/// `points` decimated by `ratio`, its last point kept, as the decimator holds
/// back its last window.
fn decimate(points: &[(f64, f64)], ratio: usize) -> Vec<(f64, f64)> {
    let mut fpcs = Fpcs::new(ratio, 0.0);
    let ts: Vec<f64> = points.iter().map(|p| p.0).collect();
    let vs: Vec<f64> = points.iter().map(|p| p.1).collect();
    fpcs.update_batch(&ts, &ColumnArray::F64(vs.into()));
    let mut out = fpcs.output().clone();
    if let Some(&last) = points.last() {
        if out.last().is_none_or(|p| p.0 < last.0) {
            out.push(last);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::decode;
    use twinleaf::data::PacketParser;
    use twinleaf::proto::data::{self as data, DataType};
    use twinleaf::proto::sync::Epoch;
    use twinleaf::tio::Packet;
    use twinleaf::{SegmentId, SessionId, StreamId};

    /// A stream of one f32 column at 100 Hz, as its device describes it.
    fn parser() -> PacketParser {
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
                n_segments: 1,
                sample_size: 4,
                buf_samples: 128,
                name: "field",
            }),
            data::Metadata::Segment(data::Segment {
                stream_id: id,
                segment_id: SegmentId::new(0),
                flags: data::SegmentFlags::default(),
                epoch: Epoch::UNIX,
                timeref_serial: "",
                timeref_session: SessionId::new(1),
                start_time: 0,
                sampling_rate: 100,
                decimation: 1,
                filter_cutoff: 0.0,
                filter_type: data::FilterType::NONE,
            }),
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
        for start in (first..first + n).step_by(100) {
            let end = (start + 100).min(first + n);
            let data: Vec<u8> = (start..end)
                .flat_map(|i| (i as f32).to_le_bytes())
                .collect();
            parser
                .push_packet(&Packet::samples(1, 0, start, &data, DeviceRoute::root()).unwrap())
                .unwrap();
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
        let first = |hub: &Hub| {
            hub.gather(0, 0.0, 1.0)
                .map(|g| g.window(8192))
                .and_then(|w| w.chunks.first().map(|c| c.x[0]))
        };
        assert!(
            first(&hub).is_some_and(|t| t <= 0.02),
            "the held history reaches the start"
        );
        hub.hold(7, false);
        assert_eq!(
            first(&hub),
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
        let window = hub.gather(0, -1e9, 1e9).map(|g| g.window(10_000)).unwrap();
        assert_eq!(window.chunks.len(), 2);
    }

    #[test]
    fn a_window_reads_raw_rows() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        let window = hub.gather(0, 1.0, 2.0).map(|g| g.window(1000)).unwrap();
        assert_eq!(window.ratio, 1);
        assert!(window.chunks[0].x.len() >= 100);
        assert!(hub.gather(1, 1.0, 2.0).map(|g| g.window(1000)).is_none());
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
}

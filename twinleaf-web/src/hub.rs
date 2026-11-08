//! The hub, which keeps what every page shows, from one connection to the
//! proxy.
//!
//! Every batch goes into one [`Buffer`], which keeps the newest raw rows of
//! each stream. Each trace, one float column, is a set of
//! [`ColumnProcessor`]s over that buffer. `Fpcs` decimates it for the live
//! view, and `Welch` estimates its spectrum. A window read gives the raw rows
//! between two times, for a paused view zoomed in.
//!
//! A run lasts across lost samples, and [`Run::boundaries`] says where they
//! fell. The hub splits the points it sends there, so the page lifts the pen.
//!
//! A hub ends one way, its `End`, whether a page mounts other devices
//! ([`Live::retire`]) or the connection's samples end (the thread that feeds
//! it). Its pages' sockets then close, and the next request starts the hub
//! that takes its place.

use crate::devices::Devices;
use crate::feed::Feed;
use crate::protocol::{Frame, Held, Message, MountInfo, Rows, TraceInfo, Window, WindowChunk};
use crate::wire::{encode, encode_spectra, split_at_breaks, Chunk, Spectrum};
use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch};
use twinleaf::data::ops::{Fpcs, Welch};
use twinleaf::data::{Buffer, ColumnArray, ColumnKey, ColumnOp, ColumnProcessor, Run, StreamKey};
use twinleaf::proxy::Layout;
use twinleaf::{ColumnId, Connection, DeviceRoute, DeviceTree, RecvError, SampleBatch};

/// How much decimated history the hub keeps for the live view, as the page
/// does.
const HISTORY_SECONDS: f64 = 120.0;
/// The raw history the hub starts with: this span of each stream's time, and
/// at most this many bytes across every stream.
const RAW_SECONDS: f64 = 600.0;
const RAW_BYTES: usize = 1_000_000_000;
/// The share of the history budget the history held for paused plots keeps.
const HELD_SHARE: f64 = 0.5;
/// How often pending points go out.
const FLUSH: Duration = Duration::from_millis(16);
/// How often spectra are recomputed.
const SPECTRUM_EVERY: Duration = Duration::from_millis(100);
/// How often the hub says what its buffer holds.
const MEMORY_EVERY: Duration = Duration::from_secs(1);
/// Samples per Welch estimate, at most.
const WELCH_WINDOW: usize = 16384;
/// The decimation a hub starts with, in points per second per trace.
const POINTS_PER_SECOND: f64 = 500.0;

struct Trace {
    info: TraceInfo,
    key: ColumnKey,
    live: ColumnProcessor<Fpcs>,
    spectrum: ColumnProcessor<Welch>,
    /// Time of the last decimated point sent.
    last_sent: f64,
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
}

/// A hub fed by its connection to the proxy, and the devices that connection
/// sees.
pub(crate) struct Live {
    /// Names this hub to the page. A page drops what it holds of a hub as it
    /// hears another's snapshot, and a window request names the hub its
    /// trace id is of.
    pub(crate) id: String,
    hub: Arc<Mutex<Hub>>,
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

impl Live {
    /// Opens `layout`, or the proxy's default for `None`, and starts the
    /// thread that feeds the hub and the watchers of its devices on
    /// `runtime`. Blocks until the proxy has opened it.
    pub(crate) fn start(
        layout: Option<&Layout>,
        runtime: &tokio::runtime::Handle,
    ) -> std::io::Result<Live> {
        let connection = match layout {
            Some(layout) => Connection::open_layout(layout)?,
            None => Connection::connect()?,
        };
        let tree = connection.tree(DeviceRoute::root());
        let hub = Hub::new();
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
        let hub = Arc::new(Mutex::new(hub));
        let end = End {
            connection: connection.clone(),
            retired: Arc::new(watch::channel(false).0),
        };
        let (feeding, ending) = (hub.clone(), end.clone());
        std::thread::Builder::new()
            .name("twinleaf-web hub".to_string())
            .spawn(move || {
                feed(&feeding, &ending.connection);
                ending.retire();
            })?;
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64);
        Ok(Live {
            id: id.to_string(),
            hub,
            devices,
            tree,
            next_page: AtomicU64::new(0),
            end,
        })
    }

    /// A page that connected to this hub, whose spectra go once it does.
    pub(crate) fn page(self: &Arc<Live>) -> Page {
        Page {
            id: self.next_page.fetch_add(1, Ordering::Relaxed),
            live: self.clone(),
        }
    }

    /// Sets the raw history the hub keeps: `seconds` of each stream's time,
    /// in at most `bytes` across every stream.
    pub(crate) fn set_history(&self, seconds: f64, bytes: usize) {
        self.hub.lock().unwrap().set_history(seconds, bytes);
    }

    /// Changes the decimation every page shares, re-decimating the history.
    /// It blocks while it works.
    pub(crate) fn set_points_per_second(&self, pps: f64) {
        self.hub.lock().unwrap().set_points_per_second(pps);
    }

    /// The raw rows of trace `id` within [t0, t1], as [`Hub::window`] reads
    /// them. It blocks while it works.
    pub(crate) fn window(&self, id: u16, t0: f64, t1: f64, width: usize) -> Option<Window> {
        self.hub.lock().unwrap().window(id, t0, t1, width)
    }

    /// Every raw row of trace `id` within [t0, t1], as [`Hub::rows`] reads
    /// them. It blocks while it works.
    pub(crate) fn rows(&self, id: u16, t0: f64, t1: f64, limit: usize) -> Option<Rows> {
        self.hub.lock().unwrap().rows(id, t0, t1, limit)
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
    /// everything sent after it. Both are taken under the hub's lock, so no
    /// point is missed or doubled. The devices' messages each replace the
    /// last of their kind, so one heard twice does no harm, and they are
    /// taken after subscribing.
    pub(crate) fn snapshot(&self) -> (Vec<Frame>, broadcast::Receiver<Arc<Frame>>) {
        let (hub, seq, rx) = {
            let hub = self.hub.lock().unwrap();
            let (seq, rx) = hub.feed.subscribe();
            (hub.snapshot(), seq, rx)
        };
        let mut frames = vec![Message::Hub {
            id: self.id.clone(),
            seq,
        }
        .frame()];
        frames.extend(hub);
        frames.extend(self.devices.snapshot());
        (frames, rx)
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
        self.live.hub.lock().unwrap().show_spectra(self.id, traces);
    }

    /// Holds the history as it is now while `on`, for the page's paused
    /// plots, or lets it go.
    pub(crate) fn hold(&self, on: bool) {
        self.live.hub.lock().unwrap().hold(self.id, on);
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        self.show_spectra(BTreeSet::new());
        self.hold(false);
    }
}

/// Feeds `hub` the samples of `connection` until they end.
fn feed(hub: &Mutex<Hub>, connection: &Connection) {
    let samples = connection.samples();
    let mut last_flush = Instant::now();
    let mut last_spectrum = Instant::now();
    let mut last_memory = Instant::now();
    loop {
        match samples.recv_timeout(Duration::from_millis(5)) {
            Ok(batch) => hub.lock().unwrap().ingest(&batch),
            Err(RecvError::Timeout) | Err(RecvError::Lagged(_)) => {}
            Err(error) => {
                log::warn!("The web page's samples ended: {error}");
                return;
            }
        }
        // Each period runs from the start of the last work, so the work's own
        // time does not stretch it.
        if last_flush.elapsed() >= FLUSH {
            last_flush = Instant::now();
            hub.lock().unwrap().flush();
        }
        if last_spectrum.elapsed() >= SPECTRUM_EVERY {
            last_spectrum = Instant::now();
            hub.lock().unwrap().spectra();
        }
        if last_memory.elapsed() >= MEMORY_EVERY {
            last_memory = Instant::now();
            hub.lock().unwrap().send_memory();
        }
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
                buffer.set_max_age(Some(RAW_SECONDS));
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
        }
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
            let decimation = Fpcs::new(ratio_for(rate, self.points_per_second), HISTORY_SECONDS);
            self.feed.message(&Message::Trace {
                trace: info.clone(),
            });
            self.by_key.insert(key, self.traces.len());
            self.traces.push(Trace {
                info,
                key,
                live: ColumnProcessor::new(key, decimation),
                spectrum: ColumnProcessor::new(key, Welch::new(window, rate)),
                last_sent: f64::NEG_INFINITY,
                generation: None,
            });
        }
    }

    /// Catches each trace's decimator up with the buffer and sends the new
    /// points.
    fn flush(&mut self) {
        let mut chunks = Vec::new();
        for trace in &mut self.traces {
            let Some(run) = self.buffer.run(&trace.key.stream_key()) else {
                continue;
            };
            let generation = run.generations().stream;
            let new_run = trace.generation.is_some_and(|g| g != generation);
            trace.generation = Some(generation);
            let out = trace.live.catch_up(&self.buffer);
            if new_run {
                // The processor replayed a new run, whose times may start over.
                trace.last_sent = f64::NEG_INFINITY;
            }
            let start = out.partition_point(|&(t, _)| t <= trace.last_sent);
            if start == out.len() {
                continue;
            }
            let breaks = gap_times(self.buffer.run(&trace.key.stream_key()));
            let mut split = split_at_breaks(trace.info.id, trace.last_sent, &out[start..], &breaks);
            if new_run {
                if let Some(first) = split.first_mut() {
                    first.brk = true;
                }
            }
            trace.last_sent = out.last().unwrap().0;
            chunks.extend(split);
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
            let out = trace.live.output();
            let end = out.partition_point(|&(t, _)| t <= trace.last_sent);
            let breaks = gap_times(self.buffer.run(&trace.key.stream_key()));
            chunks.extend(split_at_breaks(
                trace.info.id,
                f64::NEG_INFINITY,
                &out[..end],
                &breaks,
            ));
        }
        chunks
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
        frames.push(
            Message::Config {
                pps: self.points_per_second,
            }
            .frame(),
        );
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
            seconds: if seconds.is_finite() { seconds } else { 0.0 },
            max_age: self.buffer.max_age().unwrap_or(0.0),
            rate,
            held,
            oldest,
        }
    }

    /// Sets the raw history the buffer keeps, dropping what no longer fits
    /// at once, and tells every page.
    fn set_history(&mut self, seconds: f64, bytes: usize) {
        self.buffer.set_max_age(Some(seconds));
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

    /// Changes the decimation and re-decimates every raw row the buffer keeps.
    fn set_points_per_second(&mut self, pps: f64) {
        self.points_per_second = pps;
        for trace in &mut self.traces {
            trace
                .live
                .op_mut()
                .set_ratio(ratio_for(trace.info.rate, pps));
            trace.live.invalidate();
            trace.live.catch_up(&self.buffer);
            trace.last_sent = trace
                .live
                .output()
                .last()
                .map_or(f64::NEG_INFINITY, |p| p.0);
        }
        self.feed.message(&Message::Clear);
        let chunks = self.history();
        if !chunks.is_empty() {
            self.feed.binary(|seq| encode(&chunks, seq));
        }
        self.feed.message(&Message::Config { pps });
    }

    /// Every raw row of trace `id` within [t0, t1], none decimated, from the
    /// history held for paused plots where the buffer no longer reaches; or,
    /// when there are more than `limit`, only how many. With them, the
    /// oldest time the history still holds of the trace.
    fn rows(&self, id: u16, t0: f64, t1: f64, limit: usize) -> Option<Rows> {
        let trace = self.traces.get(id as usize)?;
        let key = trace.key.stream_key();
        self.buffer.run(&key)?;
        let spans = self.spans(&key, t0, t1);
        let count = spans
            .iter()
            .map(|(_, rows)| (rows.end - rows.start) as usize)
            .sum();
        let oldest = self.oldest(&key);
        let (mut x, mut y) = (Vec::new(), Vec::new());
        if count <= limit {
            for (run, rows) in spans {
                for batch in run.rows(rows) {
                    if let Some(values) = batch.column(trace.key.column_id) {
                        x.extend_from_slice(batch.timestamps());
                        y.extend(values.to_f64().iter());
                    }
                }
            }
        }
        Some(Rows {
            count,
            oldest,
            x,
            y,
        })
    }

    /// The raw rows of trace `id` within [t0, t1], decimated to about two
    /// points per pixel only when there are more than that.
    fn window(&self, id: u16, t0: f64, t1: f64, width: usize) -> Option<Window> {
        let trace = self.traces.get(id as usize)?;
        let key = trace.key.stream_key();
        self.buffer.run(&key)?;
        // The raw rows, the held history's then the buffer's, split at each
        // run's gaps.
        let mut segments: Vec<Vec<(f64, f64)>> = vec![Vec::new()];
        let mut raw: usize = 0;
        for (run, rows) in self.spans(&key, t0, t1) {
            raw += (rows.end - rows.start) as usize;
            // Where the held history ends and the buffer begins may be a hole,
            // so a new span is a new segment, which the page draws apart.
            if !segments.last().unwrap().is_empty() {
                segments.push(Vec::new());
            }
            let gaps: Vec<u64> = run.boundaries().map(|(row, _, _)| row).collect();
            let mut row = rows.start;
            for batch in run.rows(rows) {
                let Some(values) = batch.column(trace.key.column_id) else {
                    row += batch.timestamps().len() as u64;
                    continue;
                };
                for (&t, &v) in batch.timestamps().iter().zip(values.to_f64().iter()) {
                    if gaps.binary_search(&row).is_ok() && !segments.last().unwrap().is_empty() {
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
        Some(Window {
            t0,
            t1,
            ratio,
            chunks,
        })
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

    fn points(chunks: &[Chunk]) -> Vec<(f64, f32)> {
        chunks
            .iter()
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
            hub.window(0, 0.0, 1.0, 8192)
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
        let rows = hub.rows(0, 0.0, 40.0, usize::MAX).unwrap();
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
    fn rows_are_every_raw_row_in_range_or_their_count_past_the_limit() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        // At 100 Hz, row n stands at (n + 1) / 100 s.
        let rows = hub.rows(0, 1.0, 2.0, 1000).unwrap();
        assert_eq!(rows.count, 101);
        assert_eq!(rows.y.first(), Some(&99.0));
        assert_eq!(rows.x.len(), 101);
        assert_eq!(rows.oldest, Some(0.01));
        let over = hub.rows(0, 1.0, 2.0, 100).unwrap();
        assert_eq!((over.count, over.x.len()), (101, 0));
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
        let sent = points(&heard(&mut rx));
        assert!(!sent.is_empty());
        let snapshot: Vec<Chunk> = hub
            .snapshot()
            .iter()
            .filter_map(|f| match f {
                Frame::Binary(bytes) => Some(decode(bytes)),
                Frame::Text(_) => None,
            })
            .flatten()
            .collect();
        assert_eq!(points(&snapshot), sent);
        // What comes after the snapshot starts where it ends.
        let mut later = hub.feed.subscribe().1;
        for batch in samples(&mut parser, 2000, 100) {
            hub.ingest(&batch);
        }
        hub.flush();
        let next = points(&heard(&mut later));
        assert!(next.first().unwrap().0 > sent.last().unwrap().0);
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
        let window = hub.window(0, -1e9, 1e9, 10_000).unwrap();
        assert_eq!(window.chunks.len(), 2);
    }

    #[test]
    fn a_window_reads_raw_rows() {
        let mut hub = Hub::new();
        let mut parser = parser();
        for batch in samples(&mut parser, 0, 1000) {
            hub.ingest(&batch);
        }
        let window = hub.window(0, 1.0, 2.0, 1000).unwrap();
        assert_eq!(window.ratio, 1);
        assert!(window.chunks[0].x.len() >= 100);
        assert!(hub.window(1, 1.0, 2.0, 1000).is_none());
    }
}

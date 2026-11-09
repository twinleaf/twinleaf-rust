//! Bounded retention of the newest samples of every live stream.
//!
//! [`Buffer`] keeps one [`Run`] per stream. A run lasts while the stream's
//! time stays monotonic and its schema holds. A boundary that keeps time
//! monotonic, such as lost samples, a rate change or an acquisition restart,
//! stays inside the run and the run records it. Any other boundary, such as
//! a new session, a time reference change or time moving backward, starts a
//! new run. A run retains its rows as frozen [`SampleBatch`]es. A
//! [`BatchCoalescer`] merges incoming batches into chunks, and retention
//! drops the oldest rows by slicing the front chunk.
//!
//! A buffer may also keep each stream to a span of time, by the stream's
//! own clock, so rows older than its newest by more than that go.
//!
//! A buffer may also hold a budget of bytes across every stream. Each chunk
//! records when its first row arrived, by a count of the batches the buffer
//! has processed, and over the budget the chunk that arrived first goes
//! first. Arrival, not sample time, orders them, as streams of different
//! devices may run on clocks that disagree.
//! Reads return slices of the retained chunks and copy only the
//! still-accumulating rows they ask for.
//!
//! [`Buffer::frozen`] copies a buffer as it is, to read while the buffer goes
//! on. The copy shares the chunks, which never change, so only the rows still
//! accumulating are copied.

use crate::data::model::continuity::{Boundary, Generations};
use crate::data::model::sample::SampleBatch;
use crate::data::model::schema::{StreamKey, StreamMetadataSnapshot};
use crate::data::store::coalesce::BatchCoalescer;
use crate::proto::SampleNumber;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Range;
/// The most bytes a chunk holds, so eviction under a budget frees memory in
/// small steps.
const CHUNK_BYTES: usize = 256 * 1024;
#[cfg(feature = "host")]
use std::time::Instant;

/// The newest samples of every stream that has delivered data, each capped at
/// `capacity` rows and, given a maximum age, that span of its own time, and
/// all of them, given a budget, at that many bytes. Each limit holds at once,
/// so the tightest decides.
///
/// # Examples
///
/// Batches usually come from
/// [`Device::samples`](crate::device::Device::samples). This example replays a
/// recorded log of three samples into a buffer that keeps two.
///
/// ```
/// use twinleaf::data::{Buffer, LogFile};
/// # use std::io::Write;
/// # use twinleaf::proto::data::{self as wire, DataType};
/// # use twinleaf::proto::sync::Epoch;
/// # use twinleaf::tio::Packet;
/// # use twinleaf::{ColumnId, DeviceRoute, SegmentId, SessionId, StreamId};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let (id, route) = (StreamId::new(1), DeviceRoute::root());
/// # let mut file = tempfile::NamedTempFile::new()?;
/// # for record in [
/// #     wire::Metadata::Device(wire::Device { session: SessionId::new(1), n_streams: 1, name: "sensor", serial: "S1", firmware: "1", protocol: 0 }),
/// #     wire::Metadata::Stream(wire::Stream { stream_id: id, n_columns: 1, n_segments: 1, sample_size: 4, buf_samples: 128, name: "field" }),
/// #     wire::Metadata::Segment(wire::Segment { stream_id: id, segment_id: SegmentId::new(0), flags: wire::SegmentFlags::default(), epoch: Epoch::UNIX, timeref_serial: "", timeref_session: SessionId::new(1), start_time: 0, sampling_rate: 10, decimation: 1, filter_cutoff: 0.0, filter_type: wire::FilterType::NONE }),
/// #     wire::Metadata::Column(wire::Column { stream_id: id, index: ColumnId::new(0), data_type: DataType::F32, name: "x", units: "nT", description: "" }),
/// # ] {
/// #     file.write_all(Packet::metadata(record, wire::MetadataFlags::UPDATE, route)?.as_bytes())?;
/// # }
/// # let data: Vec<u8> = [1.0f32, 2.0, 3.0].iter().flat_map(|x| x.to_le_bytes()).collect();
/// # file.write_all(Packet::samples(1, 0, 0, &data, route)?.as_bytes())?;
/// # let path = file.path();
/// let mut buffer = Buffer::new(2);
/// for batch in LogFile::open(path)?.scan(false).batches(4096) {
///     buffer.process_batch(&batch?);
/// }
///
/// let key = *buffer.stream_keys().next().expect("one stream");
/// let run = buffer.run(&key).expect("a run");
/// assert_eq!(run.retained_rows(), 1..3);
/// assert_eq!(run.metadata().rate_hz(), 10.0);
///
/// let latest = buffer.latest_row(&key).expect("a retained row");
/// println!("{}", latest.row(0).expect("one row"));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Buffer {
    capacity: usize,
    /// The bytes every run may hold together, or `None` for no such limit.
    budget: Option<usize>,
    /// The span of its own time each run keeps, in seconds, or `None`.
    max_age: Option<f64>,
    /// How many batches the buffer has processed, which is the arrival number
    /// of the newest.
    arrivals: u64,
    runs: HashMap<StreamKey, Run>,
}

impl Buffer {
    /// Creates a buffer that keeps at most `capacity` rows of each stream.
    pub fn new(capacity: usize) -> Buffer {
        Buffer {
            capacity,
            budget: None,
            max_age: None,
            arrivals: 0,
            runs: HashMap::new(),
        }
    }

    /// Creates a buffer whose streams hold at most `budget` bytes together.
    /// Over it, the oldest chunk by arrival goes first, from whichever stream
    /// holds it, so every stream keeps about the same span of history.
    pub fn with_budget(budget: usize) -> Buffer {
        Buffer {
            budget: Some(budget),
            ..Buffer::new(usize::MAX)
        }
    }

    /// Sets the bytes the streams may hold together, or lifts the limit with
    /// `None`, and drops the oldest chunks at once to meet it.
    pub fn set_budget(&mut self, budget: Option<usize>) {
        self.budget = budget;
        self.trim_to_budget();
    }

    /// Keeps each stream to rows no older than its newest by more than
    /// `seconds`, by the stream's own clock, or lifts the limit with `None`.
    /// Older rows go at once.
    pub fn set_max_age(&mut self, seconds: Option<f64>) {
        self.max_age = seconds;
        for run in self.runs.values_mut() {
            run.max_age = seconds;
            let newest = run.last_timestamp();
            run.evict(newest);
        }
    }

    /// Returns the span of its own time each stream keeps, if limited.
    pub fn max_age(&self) -> Option<f64> {
        self.max_age
    }

    /// Returns the bytes the streams may hold together, if limited.
    pub fn budget(&self) -> Option<usize> {
        self.budget
    }

    /// Returns the bytes the streams hold, each retained row's time, sample
    /// number and columns counted at their native widths.
    pub fn bytes(&self) -> usize {
        self.runs.values().map(Run::bytes).sum()
    }

    /// Drops the chunk that arrived first, run by run, while the streams hold
    /// more than the budget. The rows still accumulating in each run stay,
    /// so a budget smaller than those is held to them.
    fn trim_to_budget(&mut self) {
        let Some(budget) = self.budget else { return };
        while self.bytes() > budget {
            let oldest = self
                .runs
                .iter()
                .filter_map(|(key, run)| Some((run.front_arrival()?, *key)))
                .min();
            let Some((_, key)) = oldest else { break };
            if let Some(run) = self.runs.get_mut(&key) {
                run.drop_front_chunk();
            }
        }
    }

    /// Appends `batch`'s rows to its stream's run. A batch of the run's
    /// latest per-stream generation joins the run. So does a batch of a newer
    /// one whose boundary is monotonic and whose schema is the run's, and the
    /// run records that boundary. Any other batch starts a new run. Empty
    /// batches are ignored.
    pub fn process_batch(&mut self, batch: &SampleBatch) {
        if batch.is_empty() {
            return;
        }
        self.arrivals += 1;
        let stream_key = batch.stream_key();
        match self.runs.get_mut(&stream_key) {
            Some(run) if run.admits(batch) => run.append(batch, self.arrivals),
            _ => {
                let run = Run::new(batch, self.capacity, self.max_age, self.arrivals);
                self.runs.insert(stream_key, run);
            }
        }
        self.trim_to_budget();
    }

    /// A copy of the buffer as it is now, which takes no more batches and
    /// drops nothing by age. Its runs share this buffer's chunks, and the
    /// rows still accumulating are copied into a chunk of their own. A
    /// budget, if given one later, drops its oldest chunks as this buffer's
    /// does.
    pub fn frozen(&self) -> Buffer {
        Buffer {
            capacity: self.capacity,
            budget: None,
            max_age: None,
            arrivals: self.arrivals,
            runs: self
                .runs
                .iter()
                .map(|(key, run)| (*key, run.frozen()))
                .collect(),
        }
    }

    /// Returns the bytes of this buffer's completed chunks that `newer`, the
    /// buffer this one was frozen from, no longer holds, which are those that
    /// arrived before the oldest chunk `newer` keeps of the same stream. The
    /// chunks both hold are shared, and count once, in `newer`.
    pub fn bytes_not_in(&self, newer: &Buffer) -> usize {
        self.runs
            .iter()
            .map(|(key, run)| {
                let kept_from = newer
                    .run(key)
                    .and_then(Run::front_arrival)
                    .unwrap_or(u64::MAX);
                run.bytes_arrived_before(kept_from)
            })
            .sum()
    }

    /// Returns the stream's current run, or `None` if it has delivered no data.
    pub fn run(&self, stream_key: &StreamKey) -> Option<&Run> {
        self.runs.get(stream_key)
    }

    /// Returns an iterator over the streams that hold a run, in no particular
    /// order.
    pub fn stream_keys(&self) -> impl Iterator<Item = &StreamKey> {
        self.runs.keys()
    }

    /// Returns the stream's newest retained row as a one-row batch, copying at
    /// most that row. Returns `None` if the stream has no run or the run
    /// retains no rows.
    pub fn latest_row(&self, stream_key: &StreamKey) -> Option<SampleBatch> {
        self.runs.get(stream_key)?.last_row()
    }
}

/// The retained rows of one stream's current run, oldest first.
///
/// Time never moves backward within a run, so its rows are in time order.
/// Rows may be missing between them, and [`boundaries`](Run::boundaries)
/// says where.
pub struct Run {
    /// The continuity generations of the batch that opened this run.
    generations: Generations,
    /// The continuity generations of the run's newest batch.
    latest: Generations,
    /// Each boundary within the run that is not continuous, with the logical
    /// position and time of the row it precedes, oldest first.
    boundaries: VecDeque<(u64, f64, Boundary)>,
    /// The newest batch's metadata. Its schema is constant within a run.
    metadata: StreamMetadataSnapshot,
    /// Completed chunks in row order. The coalescer holds the rows after them.
    chunks: VecDeque<SampleBatch>,
    /// The arrival number of each chunk's first row, chunk by chunk.
    arrived: VecDeque<u64>,
    /// The arrival number of the first row the coalescer holds.
    tail_arrived: u64,
    coalescer: BatchCoalescer,
    /// Exclusive logical position of the next row appended to this run.
    next_row: u64,
    /// Retained rows, the tail included.
    rows: usize,
    capacity: usize,
    /// The span of its own time the run keeps, in seconds.
    max_age: Option<f64>,
    /// The bytes of one row, its time, sample number and columns together.
    row_bytes: usize,
    #[cfg(feature = "host")]
    last_seen: Instant,
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Run")
            .field("generations", &self.generations)
            .field("latest", &self.latest)
            .field("boundaries", &self.boundaries.len())
            .field("rows", &self.rows)
            .field("capacity", &self.capacity)
            .field("next_row", &self.next_row)
            .finish_non_exhaustive()
    }
}

impl Run {
    fn new(batch: &SampleBatch, capacity: usize, max_age: Option<f64>, arrival: u64) -> Run {
        let columns: usize = batch.schema().columns().map(|c| c.data_type().size()).sum();
        let row_bytes = size_of::<f64>() + size_of::<SampleNumber>() + columns;
        let target = (capacity / 16)
            .clamp(256, 65_536)
            .min((CHUNK_BYTES / row_bytes).max(256));
        let mut run = Run {
            generations: batch.generations(),
            latest: batch.generations(),
            boundaries: VecDeque::new(),
            metadata: batch.metadata().clone(),
            chunks: VecDeque::new(),
            arrived: VecDeque::new(),
            tail_arrived: arrival,
            coalescer: BatchCoalescer::new(Some(target)),
            next_row: 0,
            rows: 0,
            capacity,
            max_age,
            row_bytes,
            #[cfg(feature = "host")]
            last_seen: Instant::now(),
        };
        run.append(batch, arrival);
        run
    }

    /// A copy of the run that takes no more rows and drops none by age. It
    /// shares the run's chunks, and the rows still accumulating become a
    /// chunk of its own.
    fn frozen(&self) -> Run {
        let mut chunks = self.chunks.clone();
        let mut arrived = self.arrived.clone();
        let buffered = self.coalescer.buffered_len();
        if buffered > 0 {
            chunks.push_back(
                self.coalescer
                    .buffered_rows(0..buffered)
                    .expect("a nonempty buffered tail"),
            );
            arrived.push_back(self.tail_arrived);
        }
        Run {
            generations: self.generations,
            latest: self.latest,
            boundaries: self.boundaries.clone(),
            metadata: self.metadata.clone(),
            chunks,
            arrived,
            tail_arrived: self.tail_arrived,
            coalescer: BatchCoalescer::new(None),
            next_row: self.next_row,
            rows: self.rows,
            capacity: self.capacity,
            max_age: None,
            row_bytes: self.row_bytes,
            #[cfg(feature = "host")]
            last_seen: self.last_seen,
        }
    }

    /// Whether `batch` continues this run, as a batch of the run's latest
    /// generation does, and one whose boundary keeps time monotonic and whose
    /// schema is the run's.
    fn admits(&self, batch: &SampleBatch) -> bool {
        if batch.generations().stream == self.latest.stream {
            return true;
        }
        batch
            .boundary()
            .is_some_and(|b| b.is_monotonic() && !b.is_initial())
            && batch.schema() == self.metadata.schema()
    }

    /// Appends `batch`, which arrived as the buffer's batch `arrival`.
    fn append(&mut self, batch: &SampleBatch, arrival: u64) {
        debug_assert!(
            batch.schema() == self.metadata.schema(),
            "a run admits only batches of its schema"
        );
        if self.rows > 0 || !self.chunks.is_empty() || self.coalescer.buffered_len() > 0 {
            if let Some(boundary) = batch.boundary().filter(|b| !b.is_continuous()) {
                let time = batch.timestamps()[0];
                self.boundaries
                    .push_back((self.next_row, time, boundary.clone()));
            }
        }
        self.latest = batch.generations();
        if self.coalescer.buffered_len() == 0 {
            self.tail_arrived = arrival;
        }
        self.coalescer.push(batch);
        self.take_completed_chunks(arrival);

        self.metadata = batch.metadata().clone();
        self.next_row += batch.len() as u64;
        self.rows += batch.len();
        #[cfg(feature = "host")]
        {
            self.last_seen = Instant::now();
        }
        self.evict(batch.timestamps().last().copied());
    }

    /// Moves the coalescer's completed chunks to the run. The first began
    /// with the tail's first row, and any after it with batch `arrival`.
    fn take_completed_chunks(&mut self, arrival: u64) {
        while let Some(chunk) = self.coalescer.next_completed_batch() {
            self.chunks.push_back(chunk);
            self.arrived.push_back(self.tail_arrived);
            self.tail_arrived = arrival;
        }
    }

    /// Drops the oldest rows until at most `capacity` remain and, given a
    /// maximum age, every row older than `newest` by more than it. A cut
    /// that falls inside the front chunk slices it.
    fn evict(&mut self, newest: Option<f64>) {
        let over_capacity = self.rows.saturating_sub(self.capacity);
        let too_old = match (self.max_age, newest) {
            (Some(age), Some(newest)) => {
                let keep_from = self.first_row_at_or_after(newest - age, false);
                (keep_from - self.retained_rows().start) as usize
            }
            _ => 0,
        };
        let mut excess = over_capacity.max(too_old);
        if excess == 0 {
            return;
        }
        if self.rows - self.coalescer.buffered_len() < excess {
            // The rows to drop reach into the tail, so freeze it first.
            self.coalescer.finish_buffered_batch();
            self.take_completed_chunks(self.tail_arrived);
        }
        while excess > 0 {
            let front = self.chunks.front_mut().expect("chunks cover the excess");
            let dropped = excess.min(front.len());
            if dropped == front.len() {
                self.chunks.pop_front();
                self.arrived.pop_front();
            } else {
                *front = front.slice(dropped..front.len());
            }
            self.rows -= dropped;
            excess -= dropped;
        }
        self.forget_dropped_boundaries();
    }

    /// Drops the oldest completed chunk, if any. The rows still
    /// accumulating are never dropped this way.
    fn drop_front_chunk(&mut self) {
        let Some(chunk) = self.chunks.pop_front() else {
            return;
        };
        self.arrived.pop_front();
        self.rows -= chunk.len();
        self.forget_dropped_boundaries();
    }

    /// The arrival number of the oldest completed chunk's first row.
    fn front_arrival(&self) -> Option<u64> {
        self.arrived.front().copied()
    }

    /// The bytes of the completed chunks whose first row arrived before
    /// `arrival`.
    fn bytes_arrived_before(&self, arrival: u64) -> usize {
        self.chunks
            .iter()
            .zip(&self.arrived)
            .filter(|&(_, &arrived)| arrived < arrival)
            .map(|(chunk, _)| chunk.len() * self.row_bytes)
            .sum()
    }

    /// Forgets the boundaries before the first retained row.
    fn forget_dropped_boundaries(&mut self) {
        let start = self.retained_rows().start;
        while self
            .boundaries
            .front()
            .is_some_and(|&(row, _, _)| row < start)
        {
            self.boundaries.pop_front();
        }
    }

    /// Returns the retained rows within `rows` as contiguous spans in row
    /// order.
    pub(in crate::data) fn row_spans(&self, rows: Range<u64>) -> Vec<SampleBatch> {
        let retained = self.retained_rows();
        let (first, last) = (rows.start.max(retained.start), rows.end.min(retained.end));
        if first >= last {
            return Vec::new();
        }
        let start = (first - retained.start) as usize;
        let end = (last - retained.start) as usize;

        let mut spans = Vec::new();
        let mut offset = 0;
        for batch in &self.chunks {
            let from = start.max(offset);
            let to = end.min(offset + batch.len());
            if from < to {
                spans.push(batch.slice(from - offset..to - offset));
            }
            offset += batch.len();
        }
        let buffered = self.coalescer.buffered_len();
        if buffered > 0 {
            let (from, to) = (start.max(offset), end.min(offset + buffered));
            if from < to {
                spans.push(
                    self.coalescer
                        .buffered_rows(from - offset..to - offset)
                        .expect("a nonempty buffered tail"),
                );
            }
        }
        spans
    }

    /// Returns the newest retained row as a one-row batch.
    fn last_row(&self) -> Option<SampleBatch> {
        let buffered = self.coalescer.buffered_len();
        if buffered > 0 {
            return self.coalescer.buffered_rows(buffered - 1..buffered);
        }
        let chunk = self.chunks.back()?;
        Some(chunk.slice(chunk.len() - 1..chunk.len()))
    }

    /// Returns the continuity generations of the batch that opened this run.
    /// They identify the run, as a new run opens with newer ones. Batches that
    /// joined the run across a monotonic boundary carry newer ones, as
    /// [`latest_generations`](Self::latest_generations) returns.
    pub fn generations(&self) -> Generations {
        self.generations
    }

    /// Returns the continuity generations of the run's newest batch.
    pub fn latest_generations(&self) -> Generations {
        self.latest
    }

    /// Returns each retained boundary within the run that is not continuous,
    /// oldest first, as the logical position and the time of the row after
    /// it, and the boundary. Every one keeps time monotonic. Most are lost
    /// samples, others a rate change or an acquisition restart.
    pub fn boundaries(
        &self,
    ) -> impl DoubleEndedIterator<Item = (u64, f64, &Boundary)> + ExactSizeIterator + '_ {
        self.boundaries
            .iter()
            .map(|(row, time, b)| (*row, *time, b))
    }

    /// Returns the retained rows within `rows`, a range of logical positions,
    /// as contiguous batches in row order. Rows outside the retained ones are
    /// left out. Only the newest rows, those not yet frozen into a chunk, are
    /// copied.
    pub fn rows(&self, rows: Range<u64>) -> Vec<SampleBatch> {
        self.row_spans(rows)
    }

    /// Returns the logical positions of the retained rows whose time lies in
    /// `start..=end`, in seconds after the segment's epoch, as
    /// [`SampleBatch::timestamps`] reports them. Time never moves backward
    /// within a run, so the rows are one range. It is empty when no retained
    /// row lies in the window.
    pub fn rows_between(&self, start: f64, end: f64) -> Range<u64> {
        let first = self.first_row_at_or_after(start, false);
        let last = self.first_row_at_or_after(end, true);
        first..last.max(first)
    }

    /// The logical position of the first retained row whose time is at least
    /// `time`, or after it when `strictly`, or the end of the retained rows.
    fn first_row_at_or_after(&self, time: f64, strictly: bool) -> u64 {
        let before = |t: f64| if strictly { t <= time } else { t < time };
        let mut row = self.retained_rows().start;
        for chunk in &self.chunks {
            let stamps = chunk.timestamps();
            if stamps.last().is_some_and(|&t| before(t)) {
                row += stamps.len() as u64;
                continue;
            }
            return row + stamps.partition_point(|&t| before(t)) as u64;
        }
        let buffered = self.coalescer.buffered_len();
        if buffered > 0 {
            let tail = self
                .coalescer
                .buffered_rows(0..buffered)
                .expect("a nonempty buffered tail");
            row += tail.timestamps().partition_point(|&t| before(t)) as u64;
        }
        row
    }

    /// Returns when this run last received a batch through
    /// [`Buffer::process_batch`].
    #[cfg(feature = "host")]
    #[cfg_attr(docsrs, doc(cfg(feature = "host")))]
    pub fn last_seen(&self) -> Instant {
        self.last_seen
    }

    /// Returns the metadata of the run's newest batch. Its schema is the
    /// run's, and its segment and rate are the newest.
    pub fn metadata(&self) -> &StreamMetadataSnapshot {
        &self.metadata
    }

    /// Returns the end-of-sample time of the oldest retained row, in seconds
    /// after the segment's epoch, or `None` if no rows remain.
    pub fn first_timestamp(&self) -> Option<f64> {
        let first = self.retained_rows().start;
        self.row_spans(first..first + 1)
            .first()
            .map(|row| row.timestamps()[0])
    }

    /// Returns the end-of-sample time of the newest retained row, in seconds
    /// after the segment's epoch, or `None` if no rows remain.
    pub fn last_timestamp(&self) -> Option<f64> {
        self.last_row().map(|row| row.timestamps()[0])
    }

    /// Returns the sample number of the newest retained row, or `None` if no
    /// rows remain.
    pub fn last_sample_number(&self) -> Option<SampleNumber> {
        self.last_row().map(|row| row.sample_numbers()[0])
    }

    /// Returns the logical positions of the retained rows, counted from the
    /// run's first row.
    pub fn retained_rows(&self) -> Range<u64> {
        self.next_row - self.rows as u64..self.next_row
    }

    /// Returns the bytes of one row, its time, sample number and columns
    /// together at their native widths.
    pub fn row_bytes(&self) -> usize {
        self.row_bytes
    }

    /// Returns the bytes the retained rows hold.
    pub fn bytes(&self) -> usize {
        self.rows * self.row_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::graph::pipeline::{ColumnOp, ColumnProcessor, Continuity};
    use crate::data::model::continuity::{BoundaryClass, Change, Loss};
    use crate::data::model::sample::{BatchContext, ColumnArray, ColumnData, SampleBatchBuilder};
    use crate::data::model::schema::{ColumnKey, DeviceRecord, SegmentRecord};
    use crate::data::tests::fixtures;
    use crate::proto::data as wire;
    use crate::proto::data::DataType;
    use crate::proto::ColumnId;
    use crate::proto::DeviceRoute;

    /// Records every sample it is fed, plus the length of each span it was fed as,
    /// so tests can see where the buffer's chunk boundaries fall.
    #[derive(Default)]
    struct Collect {
        samples: Vec<(f64, f64)>,
        spans: Vec<usize>,
        resets: usize,
    }

    impl ColumnOp for Collect {
        type Output = Vec<(f64, f64)>;

        fn reset(&mut self) {
            self.samples.clear();
            self.spans.clear();
            self.resets += 1;
        }

        fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray) {
            assert_eq!(
                timestamps.len(),
                values.len(),
                "spans must line up row-wise"
            );
            self.spans.push(timestamps.len());
            for (row, &t) in timestamps.iter().enumerate() {
                let value = values.get(row).try_as_f64().expect("a numeric column");
                self.samples.push((t, value));
            }
        }

        fn output(&self) -> &Self::Output {
            &self.samples
        }
    }

    struct Fixture {
        stream_key: StreamKey,
        column_keys: Vec<ColumnKey>,
        metadata: StreamMetadataSnapshot,
    }

    impl Fixture {
        /// Pushes one batch stamped with `stream_generation`. A change of
        /// generation starts a new run.
        fn push_rows(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            rows: &[(u32, Vec<ColumnData>)],
        ) {
            self.push_in_segment(
                buffer,
                stream_generation,
                self.metadata.segment_record(),
                rows,
            );
        }

        fn push_in_segment(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            segment: &SegmentRecord,
            rows: &[(u32, Vec<ColumnData>)],
        ) {
            let mut builder = SampleBatchBuilder::new(
                BatchContext {
                    metadata: StreamMetadataSnapshot::new(
                        self.metadata.schema().clone(),
                        DeviceRecord::encode(fixtures::device()).unwrap(),
                        segment.clone(),
                    ),
                    boundary: None,
                    generations: Generations {
                        stream: stream_generation,
                        device: 0,
                        global: 0,
                    },
                },
                rows.len(),
            );
            for &(sample_number, ref row) in rows {
                assert_eq!(row.len(), self.column_keys.len());
                builder.push_row(SampleNumber::new(sample_number), row.iter().cloned());
            }
            let batch = builder.finish();
            buffer.process_batch(&batch);
        }

        /// Push `samples` as one batch of a single float column whose values
        /// are the sample numbers, opening with `boundary`.
        fn push_floats_after(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            samples: Range<u32>,
            boundary: Boundary,
        ) {
            let mut builder = SampleBatchBuilder::new(
                BatchContext {
                    metadata: self.metadata.clone(),
                    boundary: Some(boundary),
                    generations: Generations {
                        stream: stream_generation,
                        device: 0,
                        global: 0,
                    },
                },
                samples.len(),
            );
            for n in samples {
                builder.push_row(SampleNumber::new(n), [ColumnData::F64(f64::from(n))]);
            }
            buffer.process_batch(&builder.finish());
        }

        /// Push `samples` as one batch of a single float column whose values are
        /// the sample numbers.
        fn push_floats(&self, buffer: &mut Buffer, stream_generation: u32, samples: Range<u32>) {
            let rows: Vec<_> = samples
                .map(|n| (n, vec![ColumnData::F64(f64::from(n))]))
                .collect();
            self.push_rows(buffer, stream_generation, &rows);
        }
    }

    /// One stream sampled at 1 Hz from time zero, so a row's timestamp is its
    /// sample number plus one.
    fn test_fixture(column_types: &[DataType]) -> Fixture {
        stream_fixture(1, column_types)
    }

    /// Stream `stream_id` of the root device, as `test_fixture` makes stream 1.
    fn stream_fixture(stream_id: u8, column_types: &[DataType]) -> Fixture {
        let route = DeviceRoute::root();

        let names: Vec<_> = (0..column_types.len())
            .map(|index| {
                (
                    format!("col_{index}"),
                    format!("u{index}"),
                    format!("column {index}"),
                )
            })
            .collect();
        let columns: Vec<_> = column_types
            .iter()
            .zip(&names)
            .enumerate()
            .map(
                |(index, (data_type, (name, units, description)))| wire::Column {
                    name,
                    units,
                    description,
                    ..fixtures::column(stream_id, index as u8, *data_type)
                },
            )
            .collect();

        Fixture {
            stream_key: StreamKey::new(route, crate::proto::StreamId::new(stream_id)),
            column_keys: (0..columns.len())
                .map(|index| {
                    ColumnKey::new(
                        route,
                        crate::proto::StreamId::new(stream_id),
                        ColumnId::new(u8::try_from(index).unwrap()),
                    )
                })
                .collect(),
            metadata: fixtures::metadata(
                route,
                fixtures::device(),
                wire::Stream {
                    n_columns: columns.len() as u8,
                    sample_size: 0,
                    buf_samples: 1024,
                    ..fixtures::stream(stream_id)
                },
                fixtures::segment(stream_id),
                &columns,
            ),
        }
    }

    /// The samples a float run should hold for sample numbers `samples`.
    fn expected(samples: Range<u32>) -> Vec<(f64, f64)> {
        samples
            .map(|n| (f64::from(n) + 1.0, f64::from(n)))
            .collect()
    }

    /// `samples` lost, as the parser reports when the device or link dropped them.
    fn loss(samples: u64) -> Boundary {
        Boundary {
            change: None,
            loss: Some(Loss {
                samples,
                expected: None,
                dropped_batches: 0,
            }),
        }
    }

    /// Collect, fed across gaps.
    #[derive(Default)]
    struct CollectAcross(Collect);

    impl ColumnOp for CollectAcross {
        type Output = Vec<(f64, f64)>;
        fn continuity(&self) -> Continuity {
            Continuity::Monotonic
        }
        fn reset(&mut self) {
            self.0.reset();
        }
        fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray) {
            self.0.update_batch(timestamps, values);
        }
        fn output(&self) -> &Self::Output {
            self.0.output()
        }
    }

    #[test]
    fn a_run_continues_across_lost_samples_and_records_the_gap() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..10);
        // The device lost samples 10..15, and the parser bumped the generation.
        fx.push_floats_after(&mut buffer, 2, 15..20, loss(5));
        // A receiver's queue dropped 20..22, which bumps no generation.
        fx.push_floats_after(&mut buffer, 2, 22..25, loss(2));

        let run = buffer.run(&fx.stream_key).expect("one run");
        assert_eq!(run.retained_rows(), 0..18, "both gaps stay inside the run");
        assert_eq!(run.generations().stream, 1, "the run keeps its identity");
        assert_eq!(run.latest_generations().stream, 2);
        let gaps: Vec<_> = run
            .boundaries()
            .map(|(row, time, b)| (row, time, b.class()))
            .collect();
        assert_eq!(
            gaps,
            vec![
                (10, 16.0, BoundaryClass::DataLoss),
                (15, 23.0, BoundaryClass::DataLoss)
            ],
            "each gap at the row after it, with that row's time"
        );
    }

    #[test]
    fn a_boundary_that_turns_time_back_starts_a_new_run() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..10);
        let initial = Boundary {
            change: Some(Change::Initial),
            loss: None,
        };
        fx.push_floats_after(&mut buffer, 2, 0..4, initial);

        let run = buffer.run(&fx.stream_key).expect("the new run");
        assert_eq!(run.retained_rows(), 0..4);
        assert_eq!(run.generations().stream, 2);
        assert_eq!(
            run.boundaries().len(),
            0,
            "a run's opening boundary is not inside it"
        );
    }

    #[test]
    fn evicted_gaps_are_forgotten() {
        let mut buffer = Buffer::new(8);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..4);
        fx.push_floats_after(&mut buffer, 2, 10..14, loss(6));
        assert_eq!(buffer.run(&fx.stream_key).unwrap().boundaries().len(), 1);
        fx.push_floats(&mut buffer, 2, 14..20);

        let run = buffer.run(&fx.stream_key).unwrap();
        assert_eq!(run.retained_rows(), 6..14);
        assert_eq!(
            run.boundaries().len(),
            0,
            "the gap fell before the oldest retained row"
        );
    }

    #[test]
    fn rows_between_finds_a_time_window_across_chunks_and_the_tail() {
        let mut buffer = Buffer::new(4096);
        let fx = test_fixture(&[DataType::F64]);
        // Chunks of 300 rows, 300 rows and a buffered tail of 50, as above.
        for start in (0..650).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..(start + 100).min(650));
        }
        let run = buffer.run(&fx.stream_key).unwrap();
        // Row n has time n + 1.
        assert_eq!(
            run.rows_between(100.0, 200.0),
            99..200,
            "inclusive at both ends"
        );
        assert_eq!(
            run.rows_between(299.5, 301.5),
            299..301,
            "across a chunk edge"
        );
        assert_eq!(
            run.rows_between(640.0, 1e9),
            639..650,
            "into the buffered tail"
        );
        assert_eq!(run.rows_between(-5.0, 0.5), 0..0, "before the first row");
        assert_eq!(
            run.rows_between(700.0, 800.0),
            650..650,
            "after the last row"
        );

        let window: Vec<f64> = run
            .rows(run.rows_between(298.0, 302.0))
            .iter()
            .flat_map(|batch| batch.timestamps().to_vec())
            .collect();
        assert_eq!(window, vec![298.0, 299.0, 300.0, 301.0, 302.0]);
    }

    #[test]
    fn a_continuous_op_starts_over_at_each_gap_and_a_monotonic_op_feeds_across() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..10);
        fx.push_floats_after(&mut buffer, 2, 15..20, loss(5));

        let mut continuous = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        let mut monotonic = ColumnProcessor::new(fx.column_keys[0], CollectAcross::default());
        assert_eq!(
            continuous.catch_up(&buffer),
            &expected(15..20),
            "a replay starts at the last gap"
        );
        assert_eq!(continuous.op().resets, 1, "only the replay's own reset");
        let mut both = expected(0..10);
        both.extend(expected(15..20));
        assert_eq!(monotonic.catch_up(&buffer), &both);
        assert_eq!(
            monotonic.op().0.spans,
            vec![10, 5],
            "no span crosses the gap"
        );

        // New rows open with another gap: the continuous op starts over at it.
        fx.push_floats_after(&mut buffer, 3, 25..28, loss(5));
        assert_eq!(continuous.catch_up(&buffer), &expected(25..28));
        assert_eq!(continuous.op().resets, 2);
        both.extend(expected(25..28));
        assert_eq!(monotonic.catch_up(&buffer), &both);
        assert_eq!(
            monotonic.op().0.resets,
            1,
            "a monotonic op is never reset at a gap"
        );
    }

    #[test]
    fn a_run_retains_every_row_until_capacity_is_reached() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::F64]);

        for start in (0..300).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..start + 100);
        }

        let run = buffer.run(&fx.stream_key).expect("the run");
        assert_eq!(run.retained_rows(), 0..300);
        assert_eq!(run.last_timestamp(), Some(300.0));
        assert_eq!(run.last_sample_number(), Some(SampleNumber::new(299)));
        assert_eq!(run.metadata().rate_hz(), 1.0);

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(0..300));
    }

    #[test]
    fn reads_span_the_frozen_chunks_and_the_unfrozen_tail() {
        let mut buffer = Buffer::new(4096);
        let fx = test_fixture(&[DataType::F64]);

        // The chunk target is capacity / 16, so every third batch of 100 rows
        // completes a chunk and the rest stay in the tail.
        for start in (0..650).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..(start + 100).min(650));
        }

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(0..650));
        assert_eq!(
            processor.op().spans,
            vec![300, 300, 50],
            "two frozen chunks then the buffered tail"
        );
        assert_eq!(processor.op().resets, 1);

        // Rows appended after the read continue the same run, tail included.
        fx.push_floats(&mut buffer, 1, 650..660);
        assert_eq!(processor.catch_up(&buffer), &expected(0..660));
        assert_eq!(
            processor.op().resets,
            1,
            "an append must not force a replay"
        );
    }

    #[test]
    fn eviction_is_row_exact_across_chunk_boundaries() {
        let mut buffer = Buffer::new(1000);
        let fx = test_fixture(&[DataType::F64]);

        for start in (0..3000).step_by(100) {
            fx.push_floats(&mut buffer, 1, start..start + 100);
        }

        let run = buffer.run(&fx.stream_key).expect("the run");
        assert_eq!(
            run.retained_rows(),
            2000..3000,
            "retention drops whole rows, not whole chunks"
        );
        assert_eq!(run.last_timestamp(), Some(3000.0));

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(2000..3000));
        assert!(
            processor.op().spans.len() > 1,
            "1000 rows must span several chunks, got {:?}",
            processor.op().spans
        );
        assert_eq!(processor.op().spans.iter().sum::<usize>(), 1000);
    }

    #[test]
    fn a_segment_rollover_keeps_the_run_and_delivers_both_sides() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..4);

        // A seamless rollover keeps the stream generation, so the run continues even
        // though the new segment starts a new chunk.
        let rolled = SegmentRecord::encode(wire::Segment {
            segment_id: crate::proto::SegmentId::new(1),
            start_time: 4,
            ..fx.metadata.segment().fields()
        })
        .unwrap();
        let rows: Vec<_> = (0..3)
            .map(|n| (n, vec![ColumnData::F64(f64::from(n) + 100.0)]))
            .collect();
        fx.push_in_segment(&mut buffer, 1, &rolled, &rows);

        let run = buffer.run(&fx.stream_key).expect("the run");
        assert_eq!(run.retained_rows(), 0..7);
        assert_eq!(run.metadata().segment(), rolled.get(), "the newest segment");

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        let out = processor.catch_up(&buffer).clone();
        assert_eq!(
            out,
            vec![
                (1.0, 0.0),
                (2.0, 1.0),
                (3.0, 2.0),
                (4.0, 3.0),
                (5.0, 100.0),
                (6.0, 101.0),
                (7.0, 102.0),
            ]
        );
        assert_eq!(processor.op().spans, vec![4, 3], "one span per segment");
        assert_eq!(processor.op().resets, 1);
    }

    #[test]
    fn a_new_stream_generation_starts_a_run_that_discards_the_old_rows() {
        let mut buffer = Buffer::new(1024);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..500);
        assert_eq!(buffer.run(&fx.stream_key).unwrap().retained_rows(), 0..500);

        fx.push_floats(&mut buffer, 2, 0..3);
        let run = buffer.run(&fx.stream_key).expect("the new run");
        assert_eq!(run.generations().stream, 2);
        assert_eq!(
            run.retained_rows(),
            0..3,
            "row positions restart with the run"
        );
        assert_eq!(run.last_sample_number(), Some(SampleNumber::new(2)));

        let mut processor = ColumnProcessor::new(fx.column_keys[0], Collect::default());
        assert_eq!(processor.catch_up(&buffer), &expected(0..3));
    }

    #[test]
    fn latest_row_is_the_newest_retained_row_with_its_typed_values() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::F64, DataType::I64, DataType::U64]);

        fx.push_rows(
            &mut buffer,
            1,
            &[
                (
                    0,
                    vec![
                        ColumnData::F64(0.5),
                        ColumnData::I64(-1),
                        ColumnData::U64(10),
                    ],
                ),
                (
                    1,
                    vec![
                        ColumnData::F64(1.5),
                        ColumnData::I64(-2),
                        ColumnData::U64(11),
                    ],
                ),
            ],
        );
        // A later, separate batch must overwrite the newest row.
        fx.push_rows(
            &mut buffer,
            1,
            &[(
                2,
                vec![
                    ColumnData::F64(2.5),
                    ColumnData::I64(-3),
                    ColumnData::U64(12),
                ],
            )],
        );

        let row = buffer.latest_row(&fx.stream_key).expect("the newest row");
        assert_eq!(row.len(), 1);
        assert_eq!(row.sample_numbers(), [2]);
        assert_eq!(row.timestamps(), [3.0]);
        assert_eq!(row.schema(), fx.metadata.schema());
        assert_eq!(row.metadata().segment(), fx.metadata.segment());

        // Values come back in schema order, each in its column's own variant.
        let values: Vec<ColumnData> = row.row(0).expect("the only row").values().collect();
        assert_eq!(values.len(), 3);
        assert!(matches!(values[0], ColumnData::F64(v) if v == 2.5));
        assert!(matches!(values[1], ColumnData::I64(-3)));
        assert!(matches!(values[2], ColumnData::U64(12)));

        // No run for a stream that never received data.
        let other = StreamKey::new(DeviceRoute::root(), crate::proto::StreamId::new(99));
        assert!(buffer.latest_row(&other).is_none());
        assert!(buffer.run(&other).is_none());
    }

    #[test]
    fn latest_row_follows_the_newest_row_out_of_the_tail_and_across_eviction() {
        let mut buffer = Buffer::new(64);
        let fx = test_fixture(&[DataType::F64]);

        fx.push_floats(&mut buffer, 1, 0..1);
        let row = buffer.latest_row(&fx.stream_key).expect("the buffered row");
        assert_eq!(row.sample_numbers(), [0]);

        fx.push_floats(&mut buffer, 1, 1..200);
        let row = buffer.latest_row(&fx.stream_key).expect("the newest row");
        assert_eq!(row.sample_numbers(), [199]);
        assert_eq!(row.timestamps(), [200.0]);
        assert_eq!(
            buffer.run(&fx.stream_key).unwrap().retained_rows(),
            136..200
        );
    }

    #[test]
    fn run_metadata_comes_from_the_newest_schema() {
        /// The description, units and type of the column at `key`.
        fn column(buffer: &Buffer, key: &ColumnKey) -> Option<(String, String, DataType)> {
            let run = buffer.run(&key.stream_key())?;
            let column = run.metadata().schema().column(key.column_id)?;
            Some((
                column.description().to_string(),
                column.units().to_string(),
                column.data_type(),
            ))
        }

        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::F64]);
        assert!(column(&buffer, &fx.column_keys[0]).is_none());

        fx.push_floats(&mut buffer, 1, 0..2);
        assert_eq!(
            column(&buffer, &fx.column_keys[0]),
            Some(("column 0".to_string(), "u0".to_string(), DataType::F64))
        );

        // A schema change rides a new stream generation, and the new run's columns
        // replace the old ones.
        let changed = test_fixture(&[DataType::F64, DataType::U64]);
        changed.push_rows(
            &mut buffer,
            2,
            &[(0, vec![ColumnData::F64(1.0), ColumnData::U64(2)])],
        );
        assert_eq!(
            column(&buffer, &changed.column_keys[1]).map(|(_, _, data_type)| data_type),
            Some(DataType::U64)
        );
        let missing = ColumnKey::new(
            DeviceRoute::root(),
            crate::proto::StreamId::new(1),
            ColumnId::new(7),
        );
        assert!(column(&buffer, &missing).is_none());
    }

    #[test]
    fn stream_keys_lists_every_stream_that_delivered_data() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..2);

        let keys: Vec<_> = buffer.stream_keys().copied().collect();
        assert_eq!(keys, vec![fx.stream_key]);
    }

    #[test]
    fn an_empty_batch_never_starts_a_run() {
        let mut buffer = Buffer::new(16);
        let fx = test_fixture(&[DataType::F64]);
        fx.push_floats(&mut buffer, 1, 0..0);
        assert!(buffer.run(&fx.stream_key).is_none());
        assert_eq!(buffer.stream_keys().count(), 0);
    }

    /// The bytes of a row of one f64 column, and the rows of a full chunk of them.
    const ROW: usize = 8 + 4 + 8;
    const CHUNK: u32 = (CHUNK_BYTES / ROW) as u32;

    /// Pushes `chunks` batches of a full chunk each, which the coalescer
    /// completes one chunk apiece, numbered on from `first`.
    fn push_chunks(fx: &Fixture, buffer: &mut Buffer, first: u32, chunks: u32) {
        for chunk in 0..chunks {
            let start = first + chunk * CHUNK;
            fx.push_floats(buffer, 0, start..start + CHUNK);
        }
    }

    #[test]
    fn a_row_is_its_time_sample_number_and_columns() {
        let fx = test_fixture(&[DataType::F64, DataType::F32]);
        let mut buffer = Buffer::new(usize::MAX);
        fx.push_rows(
            &mut buffer,
            0,
            &[(0, vec![ColumnData::F64(1.0), ColumnData::F32(2.0)])],
        );
        let run = buffer.run(&fx.stream_key).expect("a run");
        assert_eq!(run.row_bytes(), 8 + 4 + 8 + 4);
        assert_eq!(buffer.bytes(), run.row_bytes());
    }

    #[test]
    fn over_the_budget_the_chunk_that_arrived_first_goes() {
        let (a, b) = (
            stream_fixture(1, &[DataType::F64]),
            stream_fixture(2, &[DataType::F64]),
        );
        let budget = (5 * CHUNK as usize + CHUNK as usize / 2) * ROW;
        let mut buffer = Buffer::with_budget(budget);
        push_chunks(&a, &mut buffer, 0, 4);
        push_chunks(&b, &mut buffer, 0, 4);
        // Stream a's rows arrived first, so its oldest chunks went, though
        // its samples are no older than b's.
        let all = 0..4 * u64::from(CHUNK);
        assert_eq!(
            buffer.run(&a.stream_key).unwrap().retained_rows(),
            3 * u64::from(CHUNK)..all.end
        );
        assert_eq!(buffer.run(&b.stream_key).unwrap().retained_rows(), all);
        assert!(buffer.bytes() <= budget);
    }

    #[test]
    fn a_lower_budget_drops_the_oldest_at_once() {
        let fx = test_fixture(&[DataType::F64]);
        let mut buffer = Buffer::new(usize::MAX);
        let c = u64::from(CHUNK);
        push_chunks(&fx, &mut buffer, 0, 4);
        buffer.set_budget(Some(2 * CHUNK as usize * ROW));
        assert_eq!(
            buffer.run(&fx.stream_key).unwrap().retained_rows(),
            2 * c..4 * c
        );
        buffer.set_budget(None);
        push_chunks(&fx, &mut buffer, 4 * CHUNK, 2);
        assert_eq!(
            buffer.run(&fx.stream_key).unwrap().retained_rows(),
            2 * c..6 * c
        );
    }

    #[test]
    fn a_frozen_copy_keeps_what_the_buffer_goes_on_to_drop() {
        let fx = test_fixture(&[DataType::F64]);
        let mut buffer = Buffer::with_budget(2 * CHUNK as usize * ROW);
        push_chunks(&fx, &mut buffer, 0, 2);
        fx.push_floats(&mut buffer, 2 * CHUNK, 0..10);
        let had = buffer.run(&fx.stream_key).unwrap().retained_rows();
        let first = buffer.run(&fx.stream_key).unwrap().first_timestamp();
        let frozen = buffer.frozen();
        push_chunks(&fx, &mut buffer, 2 * CHUNK + 10, 3);
        // The buffer moved on; the copy holds the rows it had, the tail too.
        let held = frozen.run(&fx.stream_key).unwrap();
        assert_eq!(held.retained_rows(), had);
        assert_eq!(held.first_timestamp(), first);
        assert!(buffer.run(&fx.stream_key).unwrap().retained_rows().start > had.start);
    }

    #[test]
    fn a_frozen_copy_costs_only_what_the_buffer_has_dropped_since() {
        let fx = test_fixture(&[DataType::F64]);
        let mut buffer = Buffer::with_budget(2 * CHUNK as usize * ROW);
        push_chunks(&fx, &mut buffer, 0, 2);
        let frozen = buffer.frozen();
        // At first every chunk is the buffer's too.
        assert_eq!(frozen.bytes_not_in(&buffer), 0);
        push_chunks(&fx, &mut buffer, 2 * CHUNK, 2);
        // The buffer moved on past both, which the copy alone now holds.
        assert_eq!(frozen.bytes_not_in(&buffer), 2 * CHUNK as usize * ROW);
    }

    #[test]
    fn a_frozen_copy_given_a_budget_drops_its_oldest() {
        let fx = test_fixture(&[DataType::F64]);
        let c = u64::from(CHUNK);
        let mut buffer = Buffer::new(usize::MAX);
        push_chunks(&fx, &mut buffer, 0, 4);
        let mut frozen = buffer.frozen();
        frozen.set_budget(Some(2 * CHUNK as usize * ROW));
        assert_eq!(
            frozen.run(&fx.stream_key).unwrap().retained_rows(),
            2 * c..4 * c
        );
    }

    #[test]
    fn the_rows_still_accumulating_outlast_any_budget() {
        let fx = test_fixture(&[DataType::F64]);
        let mut buffer = Buffer::with_budget(0);
        fx.push_floats(&mut buffer, 0, 0..100);
        let run = buffer.run(&fx.stream_key).unwrap();
        assert_eq!(run.retained_rows(), 0..100);
        assert_eq!(buffer.bytes(), 100 * run.row_bytes());
    }

    #[test]
    fn a_maximum_age_keeps_each_stream_to_a_span_of_its_own_time() {
        // At 1 Hz from time zero, a row's time is its sample number plus one.
        let fx = test_fixture(&[DataType::F64]);
        let mut buffer = Buffer::new(usize::MAX);
        buffer.set_max_age(Some(100.0));
        fx.push_floats(&mut buffer, 0, 0..300);
        assert_eq!(
            buffer.run(&fx.stream_key).unwrap().retained_rows(),
            199..300
        );
        buffer.set_max_age(Some(50.0));
        assert_eq!(
            buffer.run(&fx.stream_key).unwrap().retained_rows(),
            249..300
        );
    }
}

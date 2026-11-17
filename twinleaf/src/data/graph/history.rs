//! The history of every stream, kept to read back spans of any length.
//!
//! A [`History`] holds a [`Buffer`] and, for each float column that has
//! delivered rows, a [`Pyramid`] of that column's rows over the buffer. A
//! read of a span by time returns its rows as batches that share the
//! buffer's chunks. A window of a span for a view of some points reads
//! the column's levels, so it costs by the points, whatever the span.
//!
//! A history may hold a copy of itself as it was when [`History::hold`] was
//! called, a frozen buffer and each column's levels. The copy shares the
//! chunks and blocks the history goes on with, so it costs memory only as
//! the history drops them. The buffer keeps its whole budget, and the copy
//! only what of the budget the buffer leaves, and no more of each stream's
//! time before its own newest row than the buffer keeps. So a hold never
//! shortens the history the buffer keeps. Reads take the copy's rows the
//! buffer no longer holds, then the buffer's, each row once, as
//! [`History::rows`] says.
//!
//! A history runs no thread. Its owner feeds it and reads it.

use crate::data::ops::{self, Level, Pyramid, Stretch};
use crate::data::{
    Buffer, ColumnKey, ColumnProcessor, DataType, Gap, Lost, Run, SampleBatch, StreamKey,
};
use crate::proto::ColumnId;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::Range;

/// The newest rows of every stream in a [`Buffer`], the decimations of each
/// float column, and a copy of both held while a reader needs the history
/// as it was.
///
/// # Examples
///
/// ```
/// use twinleaf::data::{ColumnKey, History};
/// # use twinleaf::data::LogFile;
/// # use std::io::Write;
/// # use twinleaf::proto::data::{self as wire, DataType};
/// # use twinleaf::proto::sync::Epoch;
/// # use twinleaf::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let (id, mut buf) = (StreamId::new(1), [0u8; 512]);
/// # let mut file = tempfile::NamedTempFile::new()?;
/// # for record in [
/// #     wire::Metadata::Device(wire::Device { session: SessionId::new(1), n_streams: 1, name: "sensor", serial: "S1", firmware: "1", protocol: 0 }),
/// #     wire::Metadata::Stream(wire::Stream { stream_id: id, n_columns: 1, n_segments: 1, sample_size: 4, buf_samples: 128, name: "field" }),
/// #     wire::Metadata::Segment(wire::Segment { stream_id: id, segment_id: SegmentId::new(0), flags: wire::SegmentFlags::default(), epoch: Epoch::UNIX, timeref_serial: "", timeref_session: SessionId::new(1), start_time: 0, sampling_rate: 10, decimation: 1, filter_cutoff: 0.0, filter_type: wire::FilterType::NONE }),
/// #     wire::Metadata::Column(wire::Column { stream_id: id, index: ColumnId::new(0), data_type: DataType::F32, name: "x", units: "nT", description: "" }),
/// # ] {
/// #     let len = record.write(wire::MetadataFlags::UPDATE, &mut buf).ok_or("a record fits")?;
/// #     file.write_all(&buf[..len])?;
/// # }
/// # let data: Vec<u8> = [1.0f32, 2.0, 3.0].iter().flat_map(|x| x.to_le_bytes()).collect();
/// # let samples = wire::Samples { stream_id: id, segment_id: SegmentId::new(0), first: SampleNumber::new(0), data: &data };
/// # let len = samples.write(&mut buf).ok_or("the samples fit")?;
/// # file.write_all(&buf[..len])?;
/// # let path = file.path();
/// let mut history = History::new(usize::MAX);
/// history.set_retention(Some(600.0), Some(100_000_000));
/// let mut columns = Vec::new();
/// for batch in LogFile::open(path)?.scan(false).batches(4096) {
///     columns.extend(history.process_batch(&batch?));
/// }
///
/// let key: ColumnKey = columns[0];
/// let rows = history.rows(&key.stream_key(), 0.0, 1.0);
/// assert_eq!(rows.iter().map(|batch| batch.len()).sum::<usize>(), 3);
/// let window = history.window(&key, 0.0, 1.0, 100).expect("a run");
/// assert_eq!(window.ratio, 1);
/// let values: Vec<f64> = window.stretches[0].points()[0].iter().map(|point| point.1).collect();
/// assert_eq!(values, [1.0, 2.0, 3.0]);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct History {
    buffer: Buffer,
    /// The decimations of each float column that has delivered rows.
    levels: HashMap<ColumnKey, ColumnProcessor<Pyramid>>,
    held: Option<Held>,
}

/// A read of a column within a span for a view of some points, as
/// [`History::window`] returns it.
#[derive(Debug)]
pub struct Window {
    /// A [`Stretch`] of each part of the span that [`History::rows`] reads,
    /// in the same order.
    pub stretches: Vec<Stretch>,
    /// The rows a point stands for, as the stretches were read.
    pub ratio: usize,
}

/// Where some rows of a stream are, a range of one run's rows.
struct Span<'a> {
    run: &'a Run,
    /// The logical positions of the rows in `run`.
    rows: Range<u64>,
    /// Whether `run` is the held copy's, whose levels are held with it.
    held: bool,
}

/// The history as it was when it was held. Both share what the history
/// goes on with and keep what it drops, as much as the room left them holds.
#[derive(Debug)]
struct Held {
    buffer: Buffer,
    levels: HashMap<ColumnKey, Vec<Level>>,
}

impl Held {
    /// Keeps each stream's rows no older than its newest by more than `live`
    /// keeps of its own, and at most `room` bytes of the rows `live` no
    /// longer holds, dropping the oldest first, and drops the levels of the
    /// rows no longer held.
    fn trim(&mut self, live: &Buffer, room: usize) {
        if self.buffer.max_age() != live.max_age() {
            self.buffer.set_max_age(live.max_age());
        }
        self.buffer.trim_not_in(live, room);
        for (key, levels) in &mut self.levels {
            let run = self.buffer.run(&key.stream_key());
            if let Some(oldest) = run.and_then(Run::first_timestamp) {
                levels
                    .iter_mut()
                    .for_each(|level| level.drop_before(oldest));
            }
        }
    }
}

impl History {
    /// Creates a history that keeps at most `capacity` rows of each stream,
    /// as [`Buffer::new`] does, and holds no copy.
    pub fn new(capacity: usize) -> History {
        History {
            buffer: Buffer::new(capacity),
            levels: HashMap::new(),
            held: None,
        }
    }

    /// Appends `batch` to the buffer, as [`Buffer::process_batch`] does, and
    /// trims the held copy to the room the buffer leaves. Returns the float
    /// columns of the batch that delivered rows for the first time, in the
    /// order of its schema.
    pub fn process_batch(&mut self, batch: &SampleBatch) -> Vec<ColumnKey> {
        self.buffer.process_batch(batch);
        // As the buffer drops what the held copy keeps, the copy costs more,
        // and gives up its oldest to stay within the room left.
        self.trim_held();
        if batch.is_empty() {
            return Vec::new();
        }
        let stream = batch.stream_key();
        let mut new = Vec::new();
        for (index, column) in batch.schema().columns().enumerate() {
            if !matches!(column.data_type(), DataType::F64 | DataType::F32) {
                continue;
            }
            let key = ColumnKey::new(stream.route, stream.stream_id, ColumnId::new(index as u8));
            if let Entry::Vacant(entry) = self.levels.entry(key) {
                entry.insert(ColumnProcessor::new(key, Pyramid::new()));
                new.push(key);
            }
        }
        new
    }

    /// Keeps each stream to `seconds` of its own time and every stream
    /// together to `bytes`, or lifts either limit with `None`, dropping what
    /// no longer fits at once. The held copy keeps the same span before its
    /// own newest row, and the room the buffer leaves of `bytes`. So a lower
    /// setting frees what the copy held beyond them at once, and a higher
    /// one brings back nothing it dropped.
    pub fn set_retention(&mut self, seconds: Option<f64>, bytes: Option<usize>) {
        self.buffer.set_max_age(seconds);
        self.buffer.set_budget(bytes);
        self.trim_held();
    }

    /// Returns the span of its own time each stream keeps, if limited.
    pub fn max_age(&self) -> Option<f64> {
        self.buffer.max_age()
    }

    /// Returns the bytes the streams may hold together, if limited.
    pub fn budget(&self) -> Option<usize> {
        self.buffer.budget()
    }

    /// Trims the held copy to the span the buffer keeps of each stream,
    /// before the copy's own newest row, and to the room the buffer leaves
    /// of the budget. That room is the budget less what the buffer holds, as
    /// the chunks both hold count once, in the buffer.
    fn trim_held(&mut self) {
        let Some(held) = &mut self.held else { return };
        let budget = self.buffer.budget();
        let room = budget.map_or(usize::MAX, |b| b.saturating_sub(self.buffer.bytes()));
        held.trim(&self.buffer, room);
    }

    /// Holds a copy of the history as it is now, in place of any copy held,
    /// with each column's decimations caught up with the buffer.
    pub fn hold(&mut self) {
        let levels = (self.levels.iter_mut())
            .map(|(key, levels)| (*key, levels.catch_up(&self.buffer).clone()))
            .collect();
        self.held = Some(Held {
            buffer: self.buffer.frozen(),
            levels,
        });
    }

    /// Lets the held copy go, and with it what only the copy held.
    pub fn release(&mut self) {
        self.held = None;
    }

    /// Returns the buffer of the held copy, or `None` when no copy is held.
    fn held(&self) -> Option<&Buffer> {
        self.held.as_ref().map(|held| &held.buffer)
    }

    /// Returns the bytes the buffer holds.
    pub fn bytes(&self) -> usize {
        self.buffer.bytes()
    }

    /// Returns the bytes of the rows the held copy holds and the buffer no
    /// longer does, or 0 when no copy is held.
    pub fn held_bytes(&self) -> usize {
        self.held()
            .map_or(0, |held| held.bytes_not_in(&self.buffer))
    }

    /// Returns the seconds of its own time every stream of the buffer holds,
    /// from its oldest row to its newest, the least of any stream. It is 0
    /// when the buffer holds no row.
    pub fn seconds(&self) -> f64 {
        seconds_of(&self.buffer)
    }

    /// Returns the seconds of its own time every stream of the held copy
    /// holds, as [`seconds`](Self::seconds) counts them, or `None` when no
    /// copy is held.
    pub fn held_seconds(&self) -> Option<f64> {
        self.held().map(seconds_of)
    }

    /// Catches each column's decimations up with the buffer, and drops their
    /// points of rows the buffer no longer holds, a block at a time. A window
    /// catches up the column it reads, so this spreads the work of
    /// decimating over the calls, and frees the points of dropped rows.
    pub fn catch_up(&mut self) {
        for levels in self.levels.values_mut() {
            let Some(run) = self.buffer.run(&levels.key().stream_key()) else {
                continue;
            };
            levels.catch_up(&self.buffer);
            if let Some(oldest) = run.first_timestamp() {
                levels.op_mut().drop_before(oldest);
            }
        }
    }

    /// Returns the column's decimations as of the last catch up, or `None`
    /// for a column that is not a float column of the history.
    fn levels(&self, key: &ColumnKey) -> Option<&[Level]> {
        self.levels
            .get(key)
            .map(|levels| levels.output().as_slice())
    }

    /// Returns the column's decimations held with the copy, or `None` when
    /// no copy is held or the column had none when it was.
    fn held_levels(&self, key: &ColumnKey) -> Option<&[Level]> {
        self.held.as_ref()?.levels.get(key).map(Vec::as_slice)
    }

    /// Returns where the stream's rows within `t0..=t1` are, the held copy's
    /// part first, then the buffer's, as [`rows`](Self::rows) says. No row
    /// is in two of them, and none is empty.
    fn spans(&self, key: &StreamKey, t0: f64, t1: f64) -> Vec<Span<'_>> {
        let live = self.buffer.run(key);
        let mut spans = Vec::new();
        if let Some(run) = self.held().and_then(|held| held.run(key)) {
            let mut rows = run.rows_between(t0, t1);
            // One run counts its rows from its first, in the copy as in the
            // buffer, so the buffer's retained rows begin at a place in both.
            let same = live.filter(|live| live.ordinal() == run.ordinal());
            if let Some(live) = same {
                rows.end = rows.end.min(live.retained_rows().start);
            }
            spans.push(Span {
                run,
                rows,
                held: true,
            });
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

    /// Returns every retained row of the stream within `t0..=t1`, the held
    /// copy's part, then the buffer's. When the copy's run is the buffer's
    /// run, of one [ordinal](Run::ordinal), the copy's part ends where the
    /// buffer's retained rows begin, by their place in the run, and the
    /// buffer reads the rest. When they are two runs, the copy's part is
    /// every row of its run within the span, and it comes before the
    /// buffer's. A run that opens after the stream is forgotten is another
    /// run, though its batches carry the generations of the run before. So
    /// no row is read twice. The batches share the chunks they come from, so
    /// only the rows still accumulating are copied. Each carries the ordinal
    /// of its run as [`SampleBatch::run`] returns it.
    pub fn rows(&self, key: &StreamKey, t0: f64, t1: f64) -> Vec<SampleBatch> {
        (self.spans(key, t0, t1).into_iter())
            .flat_map(|span| span.run.rows(span.rows))
            .collect()
    }

    /// Returns each gap between two rows that [`rows`](Self::rows) reads of
    /// the stream within `t0..=t1`, oldest first, as its run records it in
    /// [`Run::boundaries`]. A gap counts when the rows before and after it
    /// are both read. So one where the held copy's part meets the buffer's
    /// within one run counts, and the place where the copy's run ends and
    /// the buffer's other run begins does not, as neither run records it.
    pub fn gaps(&self, key: &StreamKey, t0: f64, t1: f64) -> Vec<Gap> {
        let mut gaps = Vec::new();
        let mut before: Option<(u64, u64)> = None;
        for span in self.spans(key, t0, t1) {
            let run = span.run.ordinal();
            // The part goes on from the part before it, in the same run.
            let joined = before == Some((run, span.rows.start));
            let within = |row: u64| {
                (row > span.rows.start || (joined && row == span.rows.start)) && row < span.rows.end
            };
            gaps.extend(span.run.boundaries().filter(|gap| within(gap.row)).cloned());
            before = Some((run, span.rows.end));
        }
        gaps
    }

    /// Reads the column within `t0..=t1` for a view of `points` points, with
    /// the rows a point stands for the rows of the span over `points`. It
    /// catches the column's decimations up first. Returns a [`Stretch`] of
    /// each part [`rows`](Self::rows) reads, the held copy's part read from
    /// the levels held with it, and the rows a point stands for as they were
    /// read. Returns `None` when the buffer holds no run of the column's
    /// stream.
    pub fn window(&mut self, key: &ColumnKey, t0: f64, t1: f64, points: usize) -> Option<Window> {
        let stream = key.stream_key();
        self.buffer.run(&stream)?;
        if let Some(levels) = self.levels.get_mut(key) {
            levels.catch_up(&self.buffer);
        }
        let spans = self.spans(&stream, t0, t1);
        let rows: u64 = (spans.iter())
            .map(|span| span.rows.end - span.rows.start)
            .sum();
        let ratio = (rows as usize).div_ceil(points.max(1)).max(1);
        let stretches = (spans.into_iter())
            .map(|span| {
                let levels = match span.held {
                    true => self.held_levels(key),
                    false => self.levels(key),
                };
                Stretch::read(span.run, span.rows, key.column_id, ratio, levels)
            })
            .collect();
        let ratio = ops::level_for(ratio).map_or(ratio, |(_, ratio)| ratio);
        Some(Window { stretches, ratio })
    }

    /// Returns the oldest time the history holds of the stream, held or not.
    pub fn oldest(&self, key: &StreamKey) -> Option<f64> {
        let held = (self.held())
            .and_then(|held| held.run(key))
            .and_then(Run::first_timestamp);
        let live = self.buffer.run(key).and_then(Run::first_timestamp);
        held.into_iter().chain(live).reduce(f64::min)
    }

    /// Forgets the stream, as [`Buffer::forget`] does, and the decimations of
    /// its columns, and returns whether it had a run. A column of it that
    /// delivers rows again is new again. The held copy keeps what it held.
    pub fn forget(&mut self, key: &StreamKey) -> bool {
        self.levels.retain(|column, _| column.stream_key() != *key);
        self.buffer.forget(key)
    }

    /// Returns the buffer, for a caller that drives its own
    /// [`ColumnProcessor`]s over it.
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// Returns the stream's run in the buffer, as [`Buffer::run`] does.
    pub fn run(&self, key: &StreamKey) -> Option<&Run> {
        self.buffer.run(key)
    }

    /// Returns an iterator over the streams that hold a run in the buffer, in
    /// no particular order.
    pub fn stream_keys(&self) -> impl Iterator<Item = &StreamKey> {
        self.buffer.stream_keys()
    }

    /// Returns the rows the stream lost, as [`Buffer::lost`] does.
    pub fn lost(&self, key: &StreamKey) -> Lost {
        self.buffer.lost(key)
    }
}

/// The seconds of its own time every stream of `buffer` holds, the least of
/// any, or 0 with no row.
fn seconds_of(buffer: &Buffer) -> f64 {
    (buffer.stream_keys())
        .filter_map(|key| buffer.run(key))
        .filter_map(|run| Some(run.last_timestamp()? - run.first_timestamp()?))
        .reduce(f64::min)
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;
    use crate::data::fixtures::packet;
    use crate::data::model::continuity::Generations;
    use crate::data::model::sample::{BatchContext, ColumnData, SampleBatchBuilder};
    use crate::data::ops::{Point, BLOCK, FINEST};
    use crate::data::PacketParser;
    use crate::data::StreamMetadataSnapshot;
    use crate::proto::data as wire;
    use crate::proto::sync::Epoch;
    use crate::proto::{DeviceRoute, SampleNumber, SegmentId, SessionId, StreamId};
    use proptest::prelude::*;

    /// The bytes of a row of one f64 column, its time, sample number and
    /// value, and of a full chunk of them.
    const ROW: usize = 8 + 4 + 8;
    const CHUNK: usize = 256 * 1024 / ROW * ROW;

    /// The one column of stream 1 of the root device.
    fn column_key() -> ColumnKey {
        ColumnKey::new(DeviceRoute::root(), StreamId::new(1), ColumnId::new(0))
    }

    fn stream_key() -> StreamKey {
        column_key().stream_key()
    }

    /// A batch of stream 1, one f64 column at 1 Hz from time zero, so the
    /// row of sample `n` stands at `n + 1` s, of the samples `samples`,
    /// each worth its own number, stamped with `generation`.
    fn batch(generation: u32, samples: Range<u32>) -> SampleBatch {
        batch_in(fixtures::segment(1), generation, samples)
    }

    /// Where the Unix-time batches begin, in seconds, where one unit in the
    /// last place of a time is 2.4e-7 s.
    const UNIX_START: u32 = 1_700_000_000;

    /// A batch of stream 1, one f64 column at `rate` Hz from `UNIX_START`, of
    /// the samples `samples`, each worth its own number.
    fn unix_batch(rate: u32, samples: Range<u32>) -> SampleBatch {
        let segment = wire::Segment {
            start_time: UNIX_START,
            sampling_rate: rate,
            ..fixtures::segment(1)
        };
        batch_in(segment, 0, samples)
    }

    /// A batch of stream 1 in `segment`, one f64 column, of the samples
    /// `samples`, each worth its own number, stamped with `generation`.
    fn batch_in(
        segment: wire::Segment<'static>,
        generation: u32,
        samples: Range<u32>,
    ) -> SampleBatch {
        let metadata: StreamMetadataSnapshot = fixtures::metadata(
            DeviceRoute::root(),
            fixtures::device(),
            wire::Stream {
                sample_size: 8,
                ..fixtures::stream(1)
            },
            segment,
            &[fixtures::column(1, 0, DataType::F64)],
        );
        let context = BatchContext {
            metadata,
            boundary: None,
            generations: Generations {
                stream: generation,
                device: 0,
                global: 0,
            },
        };
        let mut builder = SampleBatchBuilder::new(context, samples.len());
        for n in samples {
            builder.push_row(SampleNumber::new(n), [ColumnData::F64(f64::from(n))]);
        }
        builder.finish()
    }

    /// Feeds `history` the samples `samples` in batches of 1000.
    fn push(history: &mut History, samples: Range<u32>) {
        for start in samples.clone().step_by(1000) {
            history.process_batch(&batch(0, start..(start + 1000).min(samples.end)));
        }
    }

    /// The values of the rows of `batches`, in order.
    fn values(batches: Vec<SampleBatch>) -> Vec<f64> {
        let column = |batch: SampleBatch| batch.column(ColumnId::new(0)).unwrap().to_f64().to_vec();
        batches.into_iter().flat_map(column).collect()
    }

    /// The samples `range` as the values of their rows.
    fn samples(range: Range<u32>) -> Vec<f64> {
        range.map(f64::from).collect()
    }

    /// Where the held copy of the one stream begins and ends.
    fn held_span(history: &History) -> Option<(f64, f64)> {
        let run = history.held()?.run(&stream_key())?;
        Some((run.first_timestamp()?, run.last_timestamp()?))
    }

    #[test]
    fn a_float_column_is_new_once() {
        let mut history = History::new(usize::MAX);
        assert!(history.process_batch(&batch(0, 0..0)).is_empty(), "no rows");
        assert_eq!(history.process_batch(&batch(0, 0..1)), [column_key()]);
        assert!(history.process_batch(&batch(0, 1..2)).is_empty());
        assert!(history.levels(&column_key()).is_some());
    }

    /// The held copy holds 9,999 to 60,000, and the buffer goes on to hold
    /// from 29,999. A read takes the copy's rows up to where the buffer
    /// begins and the buffer's after, each row once.
    #[test]
    fn a_read_reaches_into_the_held_copy_below_where_the_buffer_begins() {
        let mut history = History::new(usize::MAX);
        history.set_retention(Some(50_000.0), Some(10 * CHUNK));
        push(&mut history, 0..60_000);
        history.hold();
        push(&mut history, 60_000..80_000);
        let key = stream_key();
        assert_eq!(held_span(&history), Some((10_000.0, 60_000.0)));
        assert_eq!(history.run(&key).unwrap().first_timestamp(), Some(30_000.0));
        let all = history.rows(&key, f64::MIN, f64::MAX);
        assert_eq!(values(all), samples(9_999..80_000));
        let across = history.rows(&key, 29_990.0, 30_010.0);
        assert_eq!(values(across), samples(29_989..30_010));
        let spans = history.spans(&key, 29_990.0, 30_010.0);
        let parts: Vec<_> = (spans.iter())
            .map(|span| (span.rows.end - span.rows.start, span.held))
            .collect();
        assert_eq!(parts, [(10, true), (11, false)]);
        assert_eq!(history.oldest(&key), Some(10_000.0));
        history.release();
        assert!(history.held().is_none());
        let after = history.rows(&key, 0.0, 30_001.0);
        assert_eq!(values(after), samples(29_999..30_001));
        assert_eq!(history.oldest(&key), Some(30_000.0));
    }

    /// A window read after rows arrived reads them from the column's levels,
    /// caught up for the read.
    #[test]
    fn a_window_read_after_rows_arrived_includes_them() {
        let mut history = History::new(usize::MAX);
        let key = column_key();
        push(&mut history, 0..100_000);
        let window = history.window(&key, f64::MIN, f64::MAX, 100).unwrap();
        assert_eq!(window.ratio, 512);
        push(&mut history, 100_000..200_000);
        let window = history.window(&key, f64::MIN, f64::MAX, 100).unwrap();
        assert_eq!(window.ratio, 1024);
        let points: Vec<_> = (window.stretches)
            .iter()
            .flat_map(Stretch::points)
            .flatten()
            .collect();
        let new = points.iter().filter(|p| p.0 > 100_001.0 && p.0 < 200_000.0);
        assert!(new.count() > 50, "{} points", points.len());
        assert_eq!(points.last(), Some(&(200_000.0, 199_999.0)));
        let missing = ColumnKey::new(DeviceRoute::root(), StreamId::new(2), ColumnId::new(0));
        assert!(history.window(&missing, 0.0, 1.0, 100).is_none());
    }

    #[test]
    fn forgetting_a_stream_forgets_its_columns() {
        let mut history = History::new(usize::MAX);
        push(&mut history, 0..10);
        assert!(history.forget(&stream_key()));
        assert!(history.levels(&column_key()).is_none());
        assert!(history.run(&stream_key()).is_none());
        assert_eq!(history.process_batch(&batch(1, 20..21)), [column_key()]);
    }

    /// A held copy keeps the rows of a stream the history forgets. The
    /// buffer then holds none of them, and a read of their span returns the
    /// copy's rows and its window the copy's points.
    #[test]
    fn a_stream_forgotten_while_held_is_read_from_the_copy() {
        let mut history = History::new(usize::MAX);
        push(&mut history, 0..10_000);
        history.hold();
        assert!(history.forget(&stream_key()));
        assert!(history.run(&stream_key()).is_none());
        assert_eq!(history.bytes(), 0);
        assert_eq!(held_span(&history), Some((1.0, 10_000.0)));
        let read = history.rows(&stream_key(), 100.0, 200.0);
        assert_eq!(values(read), samples(99..200));
        assert_eq!(history.oldest(&stream_key()), Some(1.0));
        // A window reads the buffer's run of the stream, and there is none.
        assert!(history.window(&column_key(), 100.0, 200.0, 100).is_none());
        // The stream's rows come again, in a run the window reads with the
        // copy's rows before it.
        history.process_batch(&batch(1, 10_000..10_010));
        let window = history.window(&column_key(), 0.0, 1e9, 100_000).unwrap();
        let points: Vec<Point> = (window.stretches.iter())
            .flat_map(Stretch::points)
            .flatten()
            .collect();
        assert_eq!(points.len(), 10_010);
        assert_eq!(points[0], (1.0, 0.0));
        assert_eq!(points[10_000], (10_001.0, 10_000.0));
    }

    /// What the retention keeps of each stream beyond the span a test below
    /// asks it to keep.
    const HEADROOM: f64 = 2.0;

    /// The bytes of a row of the one f32 column `describe` names, its time,
    /// sample number and value.
    const ROW_F32: usize = 8 + 4 + 4;

    /// The most bytes one chunk of the buffer holds, which is how far from
    /// its room the held copy may be after giving up a chunk.
    const CHUNK_BYTES: u64 = 256 * 1024;

    /// A history that keeps 600 s and HEADROOM of each stream within 1 GB.
    fn fresh() -> History {
        let mut history = History::new(usize::MAX);
        retain(&mut history, 600.0, 1_000_000_000);
        history
    }

    /// Keeps `seconds` and HEADROOM of each stream within `bytes`.
    fn retain(history: &mut History, seconds: f64, bytes: usize) {
        history.set_retention(Some(seconds + HEADROOM), Some(bytes));
    }

    /// A parser told of stream 1 of one f32 column at `rate` Hz.
    fn parser_at(rate: u32) -> PacketParser {
        let mut parser = PacketParser::new(false);
        let (id, route) = (StreamId::new(1), DeviceRoute::root());
        for record in [
            wire::Metadata::Device(wire::Device {
                session: SessionId::new(1),
                n_streams: 1,
                name: "sim",
                serial: "S1",
                firmware: "1",
                protocol: 0,
            }),
            wire::Metadata::Stream(wire::Stream {
                stream_id: id,
                n_columns: 1,
                n_segments: 2,
                sample_size: 4,
                buf_samples: 128,
                name: "field",
            }),
            segment(0, rate),
            wire::Metadata::Column(wire::Column {
                stream_id: id,
                index: ColumnId::new(0),
                data_type: DataType::F32,
                name: "x",
                units: "nT",
                description: "",
            }),
        ] {
            let record = packet::metadata(record, wire::MetadataFlags::UPDATE, route);
            parser.push_packet(fixtures::checked(&record)).unwrap();
        }
        parser
    }

    /// The whole seconds of a `rate` Hz stream one segment numbers, as a
    /// sample's number is 24 bits.
    fn segment_seconds(rate: u32) -> u32 {
        (1 << 24) / rate
    }

    /// The record of the stream's segment `k`, which starts where segment
    /// `k - 1` ends.
    fn segment(k: u32, rate: u32) -> wire::Metadata<'static> {
        wire::Metadata::Segment(wire::Segment {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new((k % 2) as u8),
            flags: wire::SegmentFlags::default(),
            epoch: Epoch::UNIX,
            timeref_serial: "",
            timeref_session: SessionId::new(1),
            start_time: k * segment_seconds(rate),
            sampling_rate: rate,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: wire::FilterType::NONE,
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
                let record = packet::metadata(segment(k, rate), wire::MetadataFlags::UPDATE, route);
                parser.push_packet(fixtures::checked(&record)).unwrap();
            }
            let end = (start + 100).min(first + n).min((k + 1) * per_segment);
            let data: Vec<u8> = (start..end).flat_map(|i| value(i).to_le_bytes()).collect();
            let samples = packet::samples(1, (k % 2) as u8, number, &data, route);
            parser.push_packet(fixtures::checked(&samples)).unwrap();
            start = end;
        }
        std::iter::from_fn(|| parser.pop_batch()).collect()
    }

    /// Samples `first..first + n` of a 100 Hz stream, each its own number.
    fn numbered(parser: &mut PacketParser, first: u32, n: u32) -> Vec<SampleBatch> {
        valued(parser, 100, first, n, |i| i as f32)
    }

    /// A tone, noise from `seed` and a spike about every 5,000 samples, so
    /// every decimation has extremes to keep.
    fn spiky(i: u32, seed: u32) -> f32 {
        let hash = (i ^ seed).wrapping_mul(2_654_435_761) >> 8;
        let spike = if hash.is_multiple_of(5003) { 5.0 } else { 0.0 };
        (i as f32 * 1e-3).sin() + 0.2 * (hash as f32 / (1 << 24) as f32 - 0.5) + spike
    }

    /// Feeds `history` samples `first..first + n` at 100 Hz, as `spiky`
    /// values them, and catches its decimations up.
    fn feed(history: &mut History, parser: &mut PacketParser, first: u32, n: u32, seed: u32) {
        for batch in valued(parser, 100, first, n, |i| spiky(i, seed)) {
            history.process_batch(&batch);
        }
        history.catch_up();
    }

    /// The points a view of `width` pixels reads, two a pixel and at least
    /// 64.
    fn points_for(width: usize) -> usize {
        (2 * width).max(64)
    }

    /// A window as a view draws it, its parts to draw unbroken in order,
    /// the rows a point stands for, and the cost of the read.
    struct Drawn {
        parts: Vec<Vec<Point>>,
        ratio: usize,
        cost: usize,
    }

    impl Drawn {
        fn of(window: Window) -> Drawn {
            let cost = window.stretches.iter().map(Stretch::cost).sum();
            let parts = (window.stretches.iter())
                .flat_map(Stretch::points)
                .filter(|part| !part.is_empty())
                .collect();
            Drawn {
                parts,
                ratio: window.ratio,
                cost,
            }
        }

        /// Every point, in order.
        fn points(&self) -> Vec<Point> {
            self.parts.iter().flatten().copied().collect()
        }
    }

    /// The window of the one column within [t0, t1] for a view `width`
    /// pixels wide.
    fn window(history: &mut History, t0: f64, t1: f64, width: usize) -> Drawn {
        let window = history.window(&column_key(), t0, t1, points_for(width));
        Drawn::of(window.unwrap())
    }

    /// The times and values of every row of the one column within [t0, t1].
    fn gathered(history: &History, t0: f64, t1: f64) -> (Vec<f64>, Vec<f64>) {
        let (mut x, mut y) = (Vec::new(), Vec::new());
        for batch in history.rows(&stream_key(), t0, t1) {
            x.extend_from_slice(batch.timestamps());
            y.extend(batch.column(ColumnId::new(0)).unwrap().to_f64().iter());
        }
        (x, y)
    }

    /// The window of [t0, t1] made by decimating every row of the range for
    /// a view `width` pixels wide, or of every row itself for no width,
    /// which a window read from the levels is held against.
    fn of_rows(history: &History, t0: f64, t1: f64, width: Option<usize>) -> Drawn {
        let spans = history.spans(&stream_key(), t0, t1);
        let rows: u64 = spans.iter().map(|s| s.rows.end - s.rows.start).sum();
        let ratio = width.map_or(1, |width| (rows as usize).div_ceil(points_for(width)));
        let column = ColumnId::new(0);
        Drawn::of(Window {
            stretches: (spans.iter())
                .map(|span| Stretch::rows(span.run, span.rows.clone(), column, ratio.max(1)))
                .collect(),
            ratio: ratio.max(1),
        })
    }

    /// Reads the window of [t0, t1] at `width` and holds it against the rows
    /// of its range. Its points are rows of the range, in time order, the
    /// first and the last row among them. It is no coarser than the rows
    /// decimated for it, and the same as those when they are fewer than
    /// FINEST a point. It is split at each gap of a run between two of its
    /// points, and where the held copy ends, and nowhere else. Read from a
    /// level, each row further than two of the level's windows from the
    /// ends of its run's part has a point at least as high and one at least
    /// as low within two windows of it.
    fn checked(
        history: &mut History,
        t0: f64,
        t1: f64,
        width: usize,
    ) -> Result<Drawn, TestCaseError> {
        let drawn = window(history, t0, t1, width);
        let (x, y) = gathered(history, t0, t1);
        let reference = of_rows(history, t0, t1, Some(width));
        let points = drawn.points();

        prop_assert!(points.windows(2).all(|pair| pair[0].0 < pair[1].0));
        let index: Vec<usize> = (points.iter())
            .map(|point| x.partition_point(|&t| t < point.0))
            .collect();
        for (point, &i) in points.iter().zip(&index) {
            prop_assert_eq!(x.get(i).map(|&t| (t, y[i])), Some(*point));
        }
        let row = |i: usize| (x[i], y[i]);
        prop_assert_eq!(points.first().copied(), (!x.is_empty()).then(|| row(0)));
        prop_assert_eq!(points.last().copied(), x.len().checked_sub(1).map(row));

        prop_assert!(drawn.ratio <= reference.ratio);
        if reference.ratio < FINEST {
            prop_assert_eq!(drawn.ratio, reference.ratio);
            prop_assert_eq!(&drawn.parts, &reference.parts);
        }

        // The time of the first row after each gap, and of each part.
        let spans = history.spans(&stream_key(), t0, t1);
        let mut breaks = Vec::new();
        let mut parts = Vec::new();
        for span in &spans {
            let start = parts.last().map_or(0, |part: &Range<usize>| part.end);
            if start > 0 {
                breaks.push(x[start]);
            }
            let lost = (span.run.boundaries()).filter(|gap| span.rows.contains(&gap.row));
            breaks.extend(lost.map(|gap| gap.after));
            parts.push(start..start + (span.rows.end - span.rows.start) as usize);
        }
        let broken = |a: f64, b: f64| breaks.iter().any(|&at| a < at && at <= b);
        for part in &drawn.parts {
            prop_assert!(part.windows(2).all(|pair| !broken(pair[0].0, pair[1].0)));
        }
        for pair in drawn.parts.windows(2) {
            prop_assert!(broken(pair[0].last().unwrap().0, pair[1][0].0));
        }

        if drawn.ratio >= FINEST {
            let reach = 2 * drawn.ratio;
            for part in parts {
                for i in part.start + reach..part.end.saturating_sub(reach) {
                    let first = index.partition_point(|&at| at + reach <= i);
                    let near = (index[first..].iter()).take_while(|&&at| at < i + reach);
                    let (low, high) = near.fold((f64::MAX, f64::MIN), |(low, high), &at| {
                        (low.min(y[at]), high.max(y[at]))
                    });
                    prop_assert!(low <= y[i] && y[i] <= high, "row {}", i);
                }
            }
        }
        Ok(drawn)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// A window of any span, place and width shows the rows of its
        /// range as `checked` says, whatever samples were lost, and with a
        /// copy held from any moment on within any budget.
        #[test]
        fn a_window_shows_the_rows_of_its_range(
            n in 2_000u32..60_000,
            losses in prop::collection::vec((0.0..1.0f64, 1u32..400), 0..4),
            hold in prop::option::of((0.1..0.9f64, 0.2..2.0f64)),
            views in prop::collection::vec((0.0..1.0f64, 0.0005..1.2f64, 16usize..120), 1..6),
            seed in any::<u32>(),
        ) {
            let mut history = fresh();
            let mut parser = parser_at(100);
            // The rows after which samples are lost or the history is held.
            let mut stops: Vec<(u32, u32)> = losses
                .iter()
                .map(|&(at, lost)| ((at * f64::from(n)) as u32, lost))
                .collect();
            if let Some((at, budget)) = hold {
                // A span that needs half the budget, at 100 Hz and 16 bytes
                // a row, so a held copy has the other half.
                let bytes = (budget * f64::from(n) * 20.0) as usize;
                retain(&mut history, bytes as f64 / 3200.0, bytes);
                stops.push(((at * f64::from(n)) as u32, 0));
            }
            stops.push((n, 0));
            stops.sort();
            let (mut fed, mut number, mut held) = (0, 0, false);
            for (at, lost) in stops {
                feed(&mut history, &mut parser, number, at - fed, seed);
                number += at - fed + lost;
                fed = at;
                if lost == 0 && fed < n && !held {
                    history.hold();
                    held = true;
                }
            }
            let end = f64::from(number) / 100.0;
            for (from, span, width) in views {
                checked(&mut history, from * end, (from + span) * end, width)?;
            }
        }
    }

    /// Asserts that `gaps` are `wanted`, each time within a microsecond.
    fn assert_near(gaps: Vec<(f64, f64)>, wanted: &[(f64, f64)]) {
        let near = |a: f64, b: f64| (a - b).abs() < 1e-6;
        let same = gaps.len() == wanted.len()
            && (gaps.iter().zip(wanted)).all(|(g, w)| near(g.0, w.0) && near(g.1, w.1));
        assert!(same, "{gaps:?} is not {wanted:?}");
    }

    /// The times of each gap of the one stream within [t0, t1].
    fn gap_times(history: &History, t0: f64, t1: f64) -> Vec<(f64, f64)> {
        let gaps = history.gaps(&stream_key(), t0, t1);
        gaps.iter().map(|gap| (gap.before, gap.after)).collect()
    }

    /// A gap counts where the rows on both sides of it are read, in the
    /// held copy's part or the buffer's, and the seam between the copy's
    /// run and another run of the buffer is none.
    #[test]
    fn the_gaps_of_a_span_are_its_runs_own_between_two_rows_read() {
        let mut history = fresh();
        let mut parser = parser_at(100);
        // At 100 Hz, sample n stands at (n + 1) / 100 s. Samples 1000 to
        // 1009 and 3000 to 3004 are lost.
        feed(&mut history, &mut parser, 0, 1000, 1);
        feed(&mut history, &mut parser, 1010, 1990, 1);
        history.hold();
        feed(&mut history, &mut parser, 3005, 995, 1);
        let at = |sample: u32| f64::from(sample + 1) / 100.0;
        let (first, second) = ((at(999), at(1010)), (at(2999), at(3005)));
        assert_near(gap_times(&history, 0.0, 1e9), &[first, second]);
        assert_near(gap_times(&history, 9.0, 20.0), &[first]);
        // A span that begins at the row after a gap or ends at the row
        // before it holds no rows on both sides of it.
        assert!(gap_times(&history, 10.11, 29.0).is_empty());
        assert!(gap_times(&history, 0.0, 10.0).is_empty());
        // The stream forgotten and gone on with no loss and the same
        // generation is a run of its own, which follows the held copy's
        // with no gap the runs record. The copy's gap stays, and the new
        // run's own gap counts.
        assert!(history.forget(&stream_key()));
        feed(&mut history, &mut parser, 4000, 100, 1);
        feed(&mut history, &mut parser, 4110, 90, 1);
        let third = (at(4099), at(4110));
        assert_near(gap_times(&history, 0.0, 1e9), &[first, third]);
        let rows = history.rows(&stream_key(), 0.0, 1e9);
        let count: usize = rows.iter().map(SampleBatch::len).sum();
        assert_eq!(count, 1000 + 1990 + 100 + 90);
    }

    /// The values of every row it is fed since it was last reset, in order.
    /// It needs its rows continuous.
    #[derive(Default)]
    struct Collect(Vec<f64>);

    impl crate::data::ColumnOp for Collect {
        type Output = Vec<f64>;
        fn reset(&mut self) {
            self.0.clear();
        }
        fn update_batch(&mut self, _times: &[f64], values: &crate::data::ColumnArray) {
            self.0.extend(values.to_f64().iter());
        }
        fn output(&self) -> &Vec<f64> {
            &self.0
        }
    }

    /// An op fed the rows of a read of a span is reset where its continuity
    /// needs it. A continuous op starts over after each gap and each new
    /// run, and a monotonic one is fed every row, in the held copy's part
    /// and the buffer's alike.
    #[test]
    fn an_op_fed_a_spans_rows_is_reset_where_its_continuity_needs() {
        use crate::data::ColumnOp;
        let mut history = fresh();
        let mut parser = parser_at(100);
        let column = ColumnId::new(0);
        // Samples 1000 to 1009 are lost, then 3000 to 3004.
        let numbered = |history: &mut History, parser: &mut PacketParser, first, n| {
            for batch in valued(parser, 100, first, n, |i| i as f32) {
                history.process_batch(&batch);
            }
        };
        numbered(&mut history, &mut parser, 0, 1000);
        numbered(&mut history, &mut parser, 1010, 1990);
        history.hold();
        numbered(&mut history, &mut parser, 3005, 995);
        let rows = history.rows(&stream_key(), 0.0, 1e9);
        let mut collect = Collect::default();
        // At 100 Hz, sample n stands at (n + 1) / 100 s.
        let at = |sample: u32| f64::from(sample + 1) / 100.0;
        let covers = |covered: Option<crate::data::Covered>, rows: usize, first: u32, last: u32| {
            let covered = covered.expect("rows were fed");
            assert_eq!(covered.rows, rows);
            assert!((covered.first - at(first)).abs() < 1e-9, "{covered:?}");
            assert!((covered.last - at(last)).abs() < 1e-9, "{covered:?}");
        };
        let (fed, covered) = collect.feed_rows(&rows, column);
        assert_eq!(fed, &samples(3005..4000));
        covers(covered, 995, 3005, 3999);
        let mut stats = ops::Stats::new();
        let (stats, covered) = stats.feed_rows(&rows, column);
        assert_eq!(stats.count(), 1000 + 1990 + 995);
        covers(covered, 1000 + 1990 + 995, 0, 3999);
        assert_eq!((stats.min(), stats.max()), (Some(0.0), Some(3999.0)));
        // A span with no gap in it is fed whole, across the held copy's
        // part and the buffer's.
        let within = history.rows(&stream_key(), 10.2, 29.9);
        assert_eq!(collect.feed_rows(&within, column).0, &samples(1019..2989));
        // A run that opens after the stream is forgotten starts over too,
        // though the stream goes on with no loss and the same generation.
        history.hold();
        assert!(history.forget(&stream_key()));
        numbered(&mut history, &mut parser, 4000, 10);
        let rows = history.rows(&stream_key(), 0.0, 1e9);
        let seam = (rows.windows(2))
            .find(|pair| pair[0].run() != pair[1].run())
            .expect("the held copy's run, then the new run");
        assert_eq!(seam[0].generations(), seam[1].generations());
        assert!(seam[1].is_continuous());
        let (fed, covered) = collect.feed_rows(&rows, column);
        assert_eq!(fed, &samples(4000..4010));
        covers(covered, 10, 4000, 4009);
        let mut stats = ops::Stats::new();
        assert_eq!(
            stats.feed_rows(&rows, column).0.count(),
            1000 + 1990 + 995 + 10
        );
    }

    /// A window's rows and points are bounded by its width, whatever its
    /// span. Under FINEST rows a point it reads the rows themselves, and
    /// otherwise the points of a level, at most two a window.
    #[test]
    fn a_window_costs_by_its_width_not_its_rows() {
        let mut history = fresh();
        let mut parser = parser_at(100);
        retain(&mut history, 1e9, 1_000_000_000);
        let n = 400_000;
        feed(&mut history, &mut parser, 0, n, 7);
        let (width, points) = (100, 200);
        let mut leveled = 0;
        for span in [1.0, 10.0, 100.0, 200.0, 300.0, 600.0, 1500.0, 3999.0, 1e6] {
            let (t0, t1) = (2000.0 - span / 2.0, 2000.0 + span / 2.0);
            let read = window(&mut history, t0, t1, width);
            let rows = gathered(&history, t0, t1).0.len();
            if rows < FINEST * points {
                assert_eq!(read.ratio, rows.div_ceil(points), "span {span}");
                assert!(read.cost <= rows + 1, "span {span}");
            } else {
                leveled += 1;
                assert!(read.ratio >= FINEST, "span {span}");
                assert!(
                    read.cost <= 4 * points + 8,
                    "span {span} cost {}",
                    read.cost
                );
            }
            let made = read.points().len();
            assert!(made <= 4 * points + 8, "span {span} made {made}");
        }
        assert_eq!(leveled, 5);
    }

    /// The held copy's part of a window is read from the levels held with
    /// it, which keep what the history's own have dropped since, and both
    /// drop the levels of the rows their budgets let go.
    #[test]
    fn a_window_reads_the_held_copy_from_the_levels_held_with_it() {
        let mut history = fresh();
        let mut parser = parser_at(100);
        let key = stream_key();
        let first = |level: &Level| level.between(f64::MIN, f64::MAX)[0].points()[0].0;
        // 16 bytes a row, 187,500 rows in all, of which the buffer keeps
        // 130,200, 1300 s and the headroom, and the held copy what is left.
        // At 100 Hz, row n stands at (n + 1) / 100 s.
        retain(&mut history, 1300.0, 3_000_000);
        for start in (0..250_000).step_by(10_000) {
            feed(&mut history, &mut parser, start, 10_000, 3);
        }
        let column = column_key();
        let before = first(&history.levels(&column).unwrap()[0]);
        history.hold();
        // The held copy is rows 119,800 to 250,000, and its levels are the
        // history's own.
        let held = history.held().unwrap();
        let held_from = held.run(&key).unwrap().first_timestamp().unwrap();
        assert!((1197.0..1200.0).contains(&held_from), "{held_from}");
        assert_eq!(first(&history.held_levels(&column).unwrap()[0]), before);
        // The buffer goes on to rows 369,800 to 500,000, and its levels
        // drop the blocks of the rows it dropped. The held copy gives up its
        // oldest rows to fit in what the buffer leaves, and its levels the
        // blocks of those.
        for start in (250_000..500_000).step_by(10_000) {
            feed(&mut history, &mut parser, start, 10_000, 3);
        }
        let held = history.held().unwrap();
        let held_from = held.run(&key).unwrap().first_timestamp().unwrap();
        assert!((1900.0..2100.0).contains(&held_from), "{held_from}");
        let held_first = first(&history.held_levels(&column).unwrap()[0]);
        assert!(
            before < held_first && held_first <= held_from,
            "{before} {held_first} {held_from}"
        );
        assert!(held_from - held_first < 1000.0, "less than a block before");
        let live_from = history.run(&key).unwrap().first_timestamp().unwrap();
        assert!((3697.0..3700.0).contains(&live_from), "{live_from}");
        let live_first = first(&history.levels(&column).unwrap()[0]);
        assert!(2500.0 < live_first && live_first <= live_from);
        // Wide enough to read the finest level, of which the history's own
        // no longer has the held copy's points.
        let drawn = checked(&mut history, 0.0, 1e9, 400).unwrap();
        assert_eq!(drawn.ratio, FINEST);
        assert_eq!(drawn.parts.len(), 2);
        assert_eq!(drawn.parts[0].first().map(|p| p.0), Some(held_from));
        assert_eq!(drawn.parts[0].last().map(|p| p.0), Some(2500.0));
        assert_eq!(drawn.parts[1].first().map(|p| p.0), Some(live_from));
        // Released, the window is the buffer's part alone.
        history.release();
        let drawn = checked(&mut history, 0.0, 1e9, 400).unwrap();
        assert_eq!(drawn.parts.len(), 1);
        assert_eq!(drawn.parts[0].first().map(|p| p.0), Some(live_from));
    }

    /// How many points of each level held with the copy are before `time`,
    /// the most of any level.
    fn held_points_before(history: &History, time: f64) -> usize {
        assert!(history.held().is_some());
        (history.held_levels(&column_key()).into_iter())
            .flatten()
            .map(|level| {
                (level.between(f64::MIN, time).iter())
                    .map(|slice| slice.points().iter().filter(|p| p.0 < time).count())
                    .sum::<usize>()
            })
            .max()
            .unwrap_or(0)
    }

    /// A lower retention while a copy is held cuts the copy by the buffer's
    /// rule, from its own newest row, with its levels, and frees what it
    /// held. A higher one again brings nothing back.
    #[test]
    fn a_lower_retention_while_held_cuts_the_copy_from_its_newest_row() {
        let mut history = fresh();
        let mut parser = parser_at(100);
        // At 100 Hz, row n stands at (n + 1) / 100 s. It is held at 2000 s,
        // and the buffer goes on to 3000 s, all of it within the retention.
        retain(&mut history, 3000.0, 1_000_000_000);
        for start in (0..200_000).step_by(10_000) {
            feed(&mut history, &mut parser, start, 10_000, 3);
        }
        history.hold();
        for start in (200_000..300_000).step_by(10_000) {
            feed(&mut history, &mut parser, start, 10_000, 3);
        }
        assert_eq!(held_span(&history), Some((0.01, 2000.0)));
        assert!(held_points_before(&history, 1000.0) > BLOCK);

        retain(&mut history, 100.0, 1_000_000_000);
        let (first, last) = held_span(&history).unwrap();
        assert_eq!(last, 2000.0, "the held copy keeps its newest");
        let from = 2000.0 - 100.0 - HEADROOM;
        assert!((from..from + 0.011).contains(&first), "{first}");
        assert!(held_points_before(&history, first) < BLOCK);
        let live = history.run(&stream_key()).unwrap();
        let live_first = live.first_timestamp().unwrap();
        assert!((2898.0..2898.011).contains(&live_first), "{live_first}");
        // What it holds beyond the buffer is its 102 s at 16 bytes a row.
        let held = history.held_bytes();
        assert!((160_000..=165_000).contains(&held), "{held}");
        assert!(history.seconds() >= 100.0);
        assert_eq!(history.held_seconds(), Some(last - first));

        retain(&mut history, 3000.0, 1_000_000_000);
        assert_eq!(
            held_span(&history),
            Some((first, last)),
            "nothing comes back"
        );
        let live = history.run(&stream_key()).unwrap();
        assert_eq!(live.first_timestamp(), Some(live_first));
        assert_eq!(history.held_bytes(), held);
    }

    /// What a history holds after a retention is set, where the buffer's
    /// rows begin and end, where the held copy's do, and the bytes of each.
    #[derive(Debug, PartialEq)]
    struct Kept {
        live: (f64, f64),
        held: Option<(f64, f64)>,
        bytes: usize,
        held_bytes: Option<usize>,
    }

    /// Keeps `seconds` and HEADROOM of each stream of `history` within
    /// `bytes`, and checks that the buffer and the held copy keep no more
    /// than that leaves, and the held copy its newest row and the levels of
    /// its rows.
    fn retain_and_check(
        history: &mut History,
        seconds: f64,
        bytes: usize,
    ) -> Result<Kept, TestCaseError> {
        let held_before = held_span(history);
        retain(history, seconds, bytes);
        let run = history.run(&stream_key()).unwrap();
        let live = (
            run.first_timestamp().unwrap(),
            run.last_timestamp().unwrap(),
        );
        prop_assert!(live.1 - live.0 <= seconds + HEADROOM + 1e-9, "{:?}", live);
        prop_assert_eq!(history.seconds(), live.1 - live.0);
        let held_bytes = history.held_seconds().map(|_| history.held_bytes());
        prop_assert!(history.bytes() + held_bytes.unwrap_or(0) <= bytes);
        let span = held_span(history);
        let oldest = history.oldest(&stream_key()).unwrap();
        if let (Some((first, last)), Some((first_before, last_before))) = (span, held_before) {
            prop_assert_eq!(last, last_before, "the held copy keeps its newest");
            prop_assert!(first >= first_before, "and gets back nothing");
            prop_assert!(last - first <= seconds + HEADROOM + 1e-9);
            prop_assert!(held_points_before(history, first) < BLOCK);
            prop_assert_eq!(history.held_seconds(), Some(last - first));
            prop_assert_eq!(oldest, first.min(live.0));
        } else {
            prop_assert_eq!(history.held_seconds().is_some(), history.held().is_some());
        }
        Ok(Kept {
            live,
            held: span,
            bytes: history.bytes(),
            held_bytes,
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        /// From any retention and fill, held or not, a new retention holds
        /// the buffer and the held copy to it at once, a lower one frees
        /// what they held beyond it, and the old one again brings nothing
        /// back.
        #[test]
        fn a_new_retention_holds_the_buffer_and_the_held_copy_to_it(
            seconds in 10.0..300.0f64,
            budget in 300_000usize..2_000_000,
            new_seconds in 10.0..300.0f64,
            new_budget in 300_000usize..2_000_000,
            fill in 0.2..2.5f64,
            held_for in prop::option::of(0.0..2.0f64),
        ) {
            let mut history = fresh();
            let mut parser = parser_at(100);
            // At 100 Hz, row n stands at (n + 1) / 100 s, 16 bytes a row.
            let rows = |seconds: f64| ((seconds * 100.0).round() as u32).max(1);
            let mut fed = 0;
            let mut feed_to = |history: &mut History, until: u32| {
                for batch in numbered(&mut parser, fed, until - fed) {
                    history.process_batch(&batch);
                }
                fed = until;
            };
            retain(&mut history, seconds, budget);
            feed_to(&mut history, rows(fill * seconds));
            if let Some(held_for) = held_for {
                history.hold();
                feed_to(&mut history, rows((fill + held_for) * seconds));
            }
            let before = retain_and_check(&mut history, seconds, budget)?;
            let after = retain_and_check(&mut history, new_seconds, new_budget)?;
            if new_seconds <= seconds && new_budget <= budget {
                // The held copy counts a chunk whole once the buffer has
                // dropped any of its rows, so the chunk where the buffer
                // now begins may count in both.
                let together = |kept: &Kept| (kept.bytes + kept.held_bytes.unwrap_or(0)) as u64;
                prop_assert!(together(&after) <= together(&before) + CHUNK_BYTES);
            }
            // The old retention again gets back nothing, and where it raises
            // both limits drops nothing either.
            let again = retain_and_check(&mut history, seconds, budget)?;
            prop_assert!(again.live.0 >= after.live.0, "the buffer gets back nothing");
            if seconds >= new_seconds && budget >= new_budget {
                prop_assert_eq!(again.live, after.live);
                prop_assert_eq!(again.held, after.held);
            }
        }
    }

    /// One moment of a history, the seconds of its buffer up to the span it
    /// keeps, the rows and bytes of its buffer, the bytes of its held copy,
    /// where its history begins, and where its held copy begins and ends,
    /// from infinity when the copy holds no row.
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Moment {
        seconds: f64,
        rows: u64,
        bytes: usize,
        held: Option<usize>,
        oldest: f64,
        held_span: Option<(f64, f64)>,
    }

    /// Two histories fed one `rate` Hz stream, the first of them held at
    /// times and the second never, and their moments as recorded.
    struct Twins {
        histories: [History; 2],
        parsers: [PacketParser; 2],
        rate: u32,
        /// The span both were first told to keep, which a moment's seconds
        /// go up to.
        seconds: f64,
        fed: u32,
        /// Whether the first was held, its moment and its twin's.
        moments: Vec<(bool, Moment, Moment)>,
    }

    impl Twins {
        /// Twins of a `rate` Hz stream that keep `seconds` within `budget`.
        fn new(rate: u32, seconds: f64, budget: usize) -> Twins {
            let mut histories = [fresh(), fresh()];
            for history in &mut histories {
                retain(history, seconds, budget);
            }
            Twins {
                histories,
                parsers: [parser_at(rate), parser_at(rate)],
                rate,
                seconds,
                fed: 0,
                moments: Vec::new(),
            }
        }

        /// The history that is held at times.
        fn held(&mut self) -> &mut History {
            &mut self.histories[0]
        }

        /// Feeds both the stream's rows up to `seconds` of its time.
        fn feed_to(&mut self, seconds: f64) {
            let until = (seconds * f64::from(self.rate)).round() as u32;
            let (rate, fed) = (self.rate, self.fed);
            for (history, parser) in self.histories.iter_mut().zip(&mut self.parsers) {
                for batch in valued(parser, rate, fed, until - fed, |i| i as f32) {
                    history.process_batch(&batch);
                }
            }
            self.fed = until;
        }

        /// Keeps `seconds` and HEADROOM of the stream within `bytes`, in
        /// both.
        fn retain(&mut self, seconds: f64, bytes: usize) {
            for history in &mut self.histories {
                retain(history, seconds, bytes);
            }
        }

        fn moment(&self, history: &History) -> Moment {
            let key = stream_key();
            let run = history.held().and_then(|held| held.run(&key));
            let rows = history.run(&key).unwrap().retained_rows();
            Moment {
                seconds: history.seconds().min(self.seconds),
                rows: rows.end - rows.start,
                bytes: history.bytes(),
                held: history.held_seconds().map(|_| history.held_bytes()),
                oldest: history.oldest(&key).unwrap(),
                held_span: run.map(|run| {
                    let first = run.first_timestamp().unwrap_or(f64::INFINITY);
                    (first, run.last_timestamp().unwrap_or(f64::NAN))
                }),
            }
        }

        /// The moments of the first and of its twin as they are now.
        fn now(&self) -> (Moment, Moment) {
            (
                self.moment(&self.histories[0]),
                self.moment(&self.histories[1]),
            )
        }

        /// Records the moments as they are now.
        fn record(&mut self) {
            let (moment, twin) = self.now();
            let held = self.histories[0].held().is_some();
            self.moments.push((held, moment, twin));
        }
    }

    /// A hold, a history of `budget` bytes and `seconds` seconds fed a
    /// `rate` Hz stream for `fill` seconds, held for `held_for` and then
    /// released for `after`, beside a twin that is never held. The moments
    /// are recorded every `step` seconds.
    struct Hold {
        budget: usize,
        seconds: f64,
        rate: u32,
        fill: f64,
        held_for: f64,
        after: f64,
        step: f64,
    }

    impl Hold {
        /// The twins filled, the first held, and both fed for `held_for`.
        fn held(&self) -> Twins {
            let mut twins = Twins::new(self.rate, self.seconds, self.budget);
            twins.feed_to(self.fill);
            twins.held().hold();
            twins.record();
            let mut t = 0.0;
            while t < self.held_for {
                t = (t + self.step).min(self.held_for);
                twins.feed_to(self.fill + t);
                twins.record();
            }
            twins
        }

        /// Releases the held copy of `twins` and feeds both for `after`.
        fn released(&self, twins: &mut Twins) {
            twins.held().release();
            twins.record();
            let mut t = 0.0;
            while t < self.after {
                t = (t + self.step).min(self.after);
                twins.feed_to(self.fill + self.held_for + t);
                twins.record();
            }
        }

        /// The moments of the whole hold and of the time after it.
        fn run(&self) -> Vec<(bool, Moment, Moment)> {
            let mut twins = self.held();
            self.released(&mut twins);
            twins.moments
        }
    }

    /// A hold of a history whose hour needs three quarters of the budget,
    /// the history full, held for four hours, then released for an hour and
    /// a half.
    fn example() -> Hold {
        Hold {
            budget: 7_680_000,
            seconds: 3600.0,
            rate: 100,
            fill: 5400.0,
            held_for: 4.0 * 3600.0,
            after: 5400.0,
            step: 300.0,
        }
    }

    /// Checks the rules a hold keeps at every moment of `moments`, of a
    /// history of `budget` bytes that keeps `seconds` seconds.
    fn check_hold(
        moments: &[(bool, Moment, Moment)],
        budget: usize,
        seconds: f64,
    ) -> Result<(), TestCaseError> {
        let held_at = moments[0].1;
        let mut last_first = f64::MIN;
        for &(held, moment, twin) in moments {
            // A hold never shortens the history the buffer keeps.
            prop_assert_eq!(
                (moment.seconds, moment.rows, moment.bytes),
                (twin.seconds, twin.rows, twin.bytes)
            );
            prop_assert!(moment.seconds <= seconds);
            // Together they hold no more than the budget.
            let beyond = moment.held.unwrap_or(0);
            prop_assert!(moment.bytes + beyond <= budget, "{:?}", moment);
            if !held {
                prop_assert_eq!(moment.held, None);
                prop_assert_eq!(moment.held_span, None);
                prop_assert_eq!(moment.oldest, twin.oldest);
                continue;
            }
            // The held copy keeps its newest, gives up its oldest first,
            // and the history begins where it or the buffer begins.
            let (first, last) = moment.held_span.unwrap();
            if first.is_finite() {
                prop_assert_eq!(last, held_at.held_span.unwrap().1);
            }
            prop_assert!(first >= last_first);
            last_first = first;
            prop_assert_eq!(moment.oldest, first.min(twin.oldest));
            // Once it gives any up, it keeps as much as fits.
            if first > held_at.oldest {
                let room = moment.bytes + beyond + 2 * CHUNK_BYTES as usize;
                prop_assert!(room > budget, "{:?}", moment);
            }
        }
        Ok(())
    }

    /// While a copy is held, the buffer keeps what a twin never held keeps.
    /// A retention set while it is held holds the buffer as it holds the
    /// twin's. A lower one cuts the copy from its own newest row, and a
    /// lower budget holds the buffer and the copy together to it.
    #[test]
    fn a_hold_never_shortens_the_history_the_buffer_keeps() {
        let hold = example();
        let mut twins = hold.held();
        check_hold(&twins.moments, hold.budget, hold.seconds).unwrap();

        twins.retain(1000.0, hold.budget);
        let (moment, twin) = twins.now();
        assert_eq!((moment.rows, moment.bytes), (twin.rows, twin.bytes));
        let (first, last) = moment.held_span.unwrap();
        assert_eq!(last, hold.fill, "the held copy keeps its newest");
        let from = hold.fill - 1000.0 - HEADROOM;
        assert!((from..from + 0.011).contains(&first), "{first}");

        let budget = hold.budget / 3;
        twins.retain(2.0 * hold.seconds, budget);
        let (moment, twin) = twins.now();
        assert_eq!((moment.rows, moment.bytes), (twin.rows, twin.bytes));
        assert!(moment.bytes + moment.held.unwrap() <= budget, "{moment:?}");
    }

    /// The held copy gives up its oldest rows first, so the history begins
    /// later and later while the copy's newest stays, and its levels give up
    /// the blocks of the rows it gave up. Released, what it alone held is
    /// gone. The buffer keeps its whole span throughout.
    #[test]
    fn a_held_copy_gives_up_its_oldest_first_and_goes_when_released() {
        let hold = example();
        let mut twins = hold.held();
        let held_at = twins.moments[0].1;
        let &(_, end, twin) = twins.moments.last().unwrap();
        let (first, last) = end.held_span.unwrap();
        assert!(first > held_at.oldest + 1.0, "it gave up its oldest");
        assert_eq!(last, held_at.held_span.unwrap().1, "and kept its newest");
        assert!(last < twin.oldest, "which the buffer has long dropped");
        assert_eq!(end.oldest, first);
        let together = |moment: &Moment| moment.bytes + moment.held.unwrap_or(0);
        assert!(
            together(&end) + 2 * CHUNK_BYTES as usize > hold.budget,
            "{end:?}"
        );
        let history = twins.held();
        let finest = &history.held_levels(&column_key()).unwrap()[0];
        let point = finest.between(f64::MIN, f64::MAX)[0].points()[0].0;
        assert!(point <= first, "{point} {first}");
        assert!(held_points_before(history, first) < BLOCK);

        hold.released(&mut twins);
        let (_, released, twin) = twins.moments.iter().find(|(held, ..)| !*held).unwrap();
        assert_eq!((released.held, released.oldest), (None, twin.oldest));
        for &(_, moment, _) in &twins.moments {
            assert_eq!(moment.seconds, hold.seconds, "{moment:?}");
        }
        check_hold(&twins.moments, hold.budget, hold.seconds).unwrap();
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        /// Whatever the budget, rate, retention and length of the hold, a
        /// hold keeps the rules `check_hold` checks.
        #[test]
        fn a_hold_of_any_length_keeps_the_history_and_the_budget(
            budget in 300_000usize..2_000_000,
            rate in prop::sample::select(vec![50u32, 100, 400]),
            need in 0.2..1.5f64,
            fill in 0.5..2.0f64,
            held_for in 0.2..4.0f64,
            steps in 4.0..16.0f64,
        ) {
            let seconds = (need * budget as f64 / (ROW_F32 as f64 * f64::from(rate))).round().max(10.0);
            let hold = Hold {
                budget,
                seconds,
                rate,
                fill: fill * seconds,
                held_for: held_for * seconds,
                after: seconds,
                step: seconds / steps,
            };
            check_hold(&hold.run(), budget, seconds)?;
        }
    }

    /// A copy held of an older run is read whole and before the buffer's
    /// newer run, though the newer run's time starts over below it. No row
    /// of either is lost.
    #[test]
    fn a_held_older_run_reads_before_the_newer_run_with_no_row_lost() {
        let mut history = History::new(usize::MAX);
        push(&mut history, 0..1000);
        history.hold();
        // A newer run, whose rows stand at 1 s to 500 s again.
        history.process_batch(&batch(1, 0..500));
        let run = history.run(&stream_key()).unwrap();
        assert_eq!(run.generations().stream, 1);
        let mut want = samples(0..1000);
        want.extend(samples(0..500));
        assert_eq!(
            values(history.rows(&stream_key(), f64::MIN, f64::MAX)),
            want
        );
        let window = history
            .window(&column_key(), f64::MIN, f64::MAX, 10_000)
            .unwrap();
        assert_eq!(window.ratio, 1);
        let parts: Vec<Vec<Point>> = (window.stretches.iter())
            .flat_map(Stretch::points)
            .collect();
        assert_eq!(parts.len(), 2);
        let ys = |part: &Vec<Point>| part.iter().map(|point| point.1).collect::<Vec<_>>();
        assert_eq!(ys(&parts[0]), samples(0..1000));
        assert_eq!(ys(&parts[1]), samples(0..500));
        // A span of both runs' times reads each run's rows within it.
        let some = history.rows(&stream_key(), 100.0, 101.0);
        assert_eq!(values(some), [99.0, 100.0, 99.0, 100.0]);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Whatever the retention, and wherever the history is held and
        /// released, a read of any span at Unix times returns each row once
        /// and in time order, as rows and as the points of a window.
        #[test]
        fn a_read_at_unix_times_returns_each_row_once_in_time_order(
            rate in prop::sample::select(vec![10u32, 100, 1000]),
            max_age in prop::option::of(1.0..100.0f64),
            budget in prop::option::of(1000usize..40_000),
            steps in prop::collection::vec((1u32..6000, 0u8..4), 1..12),
            views in prop::collection::vec((0.0..1.0f64, 0.0..1.2f64, 1usize..400), 1..6),
        ) {
            let mut history = History::new(usize::MAX);
            history.set_retention(max_age, budget.map(|rows| rows * ROW));
            let mut fed = 0;
            for (rows, action) in steps {
                for start in (fed..fed + rows).step_by(1000) {
                    history.process_batch(&unix_batch(rate, start..(start + 1000).min(fed + rows)));
                }
                fed += rows;
                match action {
                    0 => history.hold(),
                    1 => history.release(),
                    _ => {}
                }
            }
            let start = f64::from(UNIX_START);
            let end = f64::from(fed + 1) / f64::from(rate);
            for (from, span, points) in views {
                let (t0, t1) = (start + from * end, start + (from + span) * end);
                let times: Vec<f64> = (history.rows(&stream_key(), t0, t1).iter())
                    .flat_map(|batch| batch.timestamps().to_vec())
                    .collect();
                for pair in times.windows(2) {
                    prop_assert!(pair[0] < pair[1], "rows {} then {} in {}..={}", pair[0], pair[1], t0, t1);
                }
                let Some(window) = history.window(&column_key(), t0, t1, points) else {
                    continue;
                };
                let points: Vec<Point> = (window.stretches.iter())
                    .flat_map(Stretch::points)
                    .flatten()
                    .collect();
                for pair in points.windows(2) {
                    prop_assert!(pair[0].0 < pair[1].0, "points {:?} then {:?} in {}..={}", pair[0], pair[1], t0, t1);
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// However the stream is held, released and forgotten, and though it
        /// goes on with no loss and the same generation after each forget, a
        /// read of any span returns each row the history holds once, in
        /// order. The held copy's rows are those of the moment it was held,
        /// read whole when the buffer has forgotten its run since.
        #[test]
        fn a_read_after_a_forget_loses_and_repeats_no_row(
            steps in prop::collection::vec((1u32..3000, 0u8..5), 1..12),
            views in prop::collection::vec((0.0..1.0f64, 0.0..1.2f64), 1..6),
        ) {
            let mut history = History::new(usize::MAX);
            // The samples of the buffer's run, and of the held copy with
            // whether it is still of the buffer's run.
            let mut live: Vec<u32> = Vec::new();
            let mut held: Option<(Vec<u32>, bool)> = None;
            let mut fed = 0;
            for (rows, action) in steps {
                for start in (fed..fed + rows).step_by(1000) {
                    history.process_batch(&batch(0, start..(start + 1000).min(fed + rows)));
                }
                live.extend(fed..fed + rows);
                fed += rows;
                match action {
                    0 => {
                        history.hold();
                        held = Some((live.clone(), true));
                    }
                    1 => {
                        history.release();
                        held = None;
                    }
                    2 => {
                        history.forget(&stream_key());
                        live.clear();
                        if let Some((_, same)) = &mut held {
                            *same = false;
                        }
                    }
                    _ => {}
                }
            }
            let mut all: Vec<u32> = match &held {
                Some((rows, false)) => rows.clone(),
                _ => Vec::new(),
            };
            all.extend(&live);
            if let Some((rows, true)) = &held {
                prop_assert!(rows.iter().zip(&live).all(|(a, b)| a == b));
            }
            let end = f64::from(fed + 1);
            for (from, span) in views {
                let (t0, t1) = (from * end, (from + span) * end);
                let want: Vec<f64> = (all.iter())
                    .filter(|&&n| (t0..=t1).contains(&f64::from(n + 1)))
                    .map(|&n| f64::from(n))
                    .collect();
                let read = values(history.rows(&stream_key(), t0, t1));
                prop_assert_eq!(read, want, "in {}..={}", t0, t1);
                prop_assert!(history.gaps(&stream_key(), t0, t1).is_empty());
            }
        }
    }
}

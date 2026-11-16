//! Decimations of one column kept so that reading any span of its history
//! costs by the points asked for, not by the rows the span holds.
//!
//! A [`Pyramid`] holds [`LEVELS`] [`Fpcs`] decimations of a column, over
//! windows of [`FINEST`] rows, twice that, four times, and so on. Every level
//! is its own FPCS over the column's rows, none a decimation of another, so
//! each holds what FPCS gives the samples at its ratio. A level's points are
//! rows of the column, each at its own time.
//!
//! A [`Level`] is every point its decimator has emitted, then the extremes
//! of the rows it has yet to close a window over. So the level bounds every
//! row fed. No row is above the highest of the level's points within two of
//! its windows of the row, or below the lowest. A read of a stretch of time
//! holds that for the rows further than two windows from its ends, as the
//! point that bounds a row nearer may lie outside the stretch.
//!
//! A level keeps its points in blocks of [`BLOCK`]. A full block never
//! changes, and a copy of the level shares it, as a frozen
//! [`Buffer`](crate::data::Buffer) shares its chunks. So a copy of the levels
//! taken beside a frozen buffer reads as the levels do, by bisection, and
//! costs only what the levels drop after it.
//!
//! [`Stretch::read`] reads rows of one run of a column for a ratio of rows a
//! point. It decimates the rows themselves when the ratio is under
//! [`FINEST`], and reads the run's level of that ratio otherwise.
//!
//! A level of windows of `r` rows emits one or two points a window, 16 bytes
//! each, and the levels' windows double, so a column's pyramid costs at most
//! 64/[`FINEST`] bytes a row. That is half a byte a row, 9 MB an hour at 5 kHz.

use crate::data::graph::ops::Fpcs;
use crate::data::{ColumnArray, ColumnOp, Continuity, Run, SampleBatch};
use crate::proto::ColumnId;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

/// A row of a column, as its time and its value.
pub type Point = (f64, f64);

/// The rows in a window of the finest level. A read of fewer rows a point
/// than this decimates the rows themselves.
pub const FINEST: usize = 128;
/// How many levels a pyramid has, each of windows twice as long as the one
/// before. The coarsest has windows of `FINEST` times 2¹⁵ rows, over four
/// million.
pub const LEVELS: usize = 16;
/// The points in a full block of a level.
pub const BLOCK: usize = 1024;

/// Returns the level whose windows are the longest of no more than `ratio`
/// rows, and the rows in its windows, or `None` when even the finest level's
/// are longer.
pub fn level_for(ratio: usize) -> Option<(usize, usize)> {
    let level = (ratio / FINEST).checked_ilog2()? as usize;
    let level = level.min(LEVELS - 1);
    Some((level, FINEST << level))
}

/// The points of one level of a pyramid, oldest first.
#[derive(Clone, Debug, Default)]
pub struct Level {
    /// The full blocks, which never change and which a copy shares.
    sealed: VecDeque<Arc<[Point]>>,
    /// The points emitted after them, fewer than BLOCK.
    open: Vec<Point>,
    /// The points after those, which the next rows may replace. They are the
    /// newest point emitted and the extremes of the rows no window has
    /// closed over, at most three.
    unsettled: Vec<Point>,
}

/// Some points of a level in time order, a stretch of one block.
#[derive(Clone, Debug)]
pub struct Slice {
    block: Arc<[Point]>,
    range: Range<usize>,
}

impl Slice {
    /// Returns the points.
    pub fn points(&self) -> &[Point] {
        &self.block[self.range.clone()]
    }
}

impl Level {
    /// Takes what `fpcs` has emitted for good, and what it has not.
    fn take(&mut self, fpcs: &mut Fpcs) {
        for point in fpcs.take_settled() {
            self.open.push(point);
            if self.open.len() == BLOCK {
                self.sealed.push_back(std::mem::take(&mut self.open).into());
            }
        }
        self.unsettled.clear();
        for point in fpcs.output().iter().copied().chain(fpcs.pending()) {
            // The decimator emits no point earlier than its last either.
            if self.unsettled.last().is_none_or(|last| last.0 < point.0) {
                self.unsettled.push(point);
            }
        }
    }

    /// Drops the full blocks whose every point is before `time`.
    pub fn drop_before(&mut self, time: f64) {
        while self
            .sealed
            .front()
            .is_some_and(|block| block.last().is_some_and(|point| point.0 < time))
        {
            self.sealed.pop_front();
        }
    }

    /// Returns the points whose time is within `from..=to`, oldest first. It
    /// finds them by bisection, shares the full blocks and copies at most
    /// the points after them.
    pub fn between(&self, from: f64, to: f64) -> Vec<Slice> {
        let within = |points: &[Point]| {
            let start = points.partition_point(|point| point.0 < from);
            let end = points.partition_point(|point| point.0 <= to);
            start..end.max(start)
        };
        let first = self
            .sealed
            .partition_point(|block| block.last().is_some_and(|point| point.0 < from));
        let mut slices = Vec::new();
        for block in self.sealed.range(first..) {
            if block.first().is_some_and(|point| point.0 > to) {
                return slices;
            }
            let range = within(block);
            if !range.is_empty() {
                slices.push(Slice {
                    block: block.clone(),
                    range,
                });
            }
        }
        for points in [&self.open, &self.unsettled] {
            let range = within(points);
            if !range.is_empty() {
                slices.push(Slice {
                    block: Arc::from(&points[range.clone()]),
                    range: 0..range.len(),
                });
            }
        }
        slices
    }

    /// Returns how many points the level holds.
    pub fn len(&self) -> usize {
        let sealed: usize = self.sealed.iter().map(|block| block.len()).sum();
        sealed + self.open.len() + self.unsettled.len()
    }

    /// Returns `true` if the level holds no point.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// FPCS decimations of one column at [`LEVELS`] ratios, each kept whole
/// until [`drop_before`](Self::drop_before) says how far back the column's
/// rows still reach.
#[derive(Debug)]
pub struct Pyramid {
    /// The decimator of each level, finest first. Its output is moved to
    /// the level as it settles.
    fpcs: Vec<Fpcs>,
    levels: Vec<Level>,
}

impl Default for Pyramid {
    fn default() -> Pyramid {
        Pyramid::new()
    }
}

impl Pyramid {
    /// Creates a pyramid that holds no points.
    pub fn new() -> Pyramid {
        Pyramid {
            fpcs: (0..LEVELS).map(|k| Fpcs::new(FINEST << k, 0.0)).collect(),
            levels: vec![Level::default(); LEVELS],
        }
    }

    /// Drops what each level holds before `time`, the oldest row the column
    /// still holds, a block at a time.
    pub fn drop_before(&mut self, time: f64) {
        for level in &mut self.levels {
            level.drop_before(time);
        }
    }
}

impl ColumnOp for Pyramid {
    type Output = Vec<Level>;

    /// As each level does, the pyramid keeps its points across gaps.
    fn continuity(&self) -> Continuity {
        Continuity::Monotonic
    }

    fn reset(&mut self) {
        for fpcs in &mut self.fpcs {
            fpcs.reset();
        }
        self.levels = vec![Level::default(); LEVELS];
    }

    fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray) {
        // Converted once, for every level to borrow.
        let values = match values {
            ColumnArray::F64(_) => values.clone(),
            other => ColumnArray::F64(other.to_f64().into_owned().into()),
        };
        for (fpcs, level) in self.fpcs.iter_mut().zip(&mut self.levels) {
            fpcs.update_batch(timestamps, &values);
            level.take(fpcs);
        }
    }

    fn output(&self) -> &Self::Output {
        &self.levels
    }
}

/// A stretch of rows of one run, read for one column at a ratio of rows a
/// point, to turn into points later.
///
/// A read takes what it needs and copies none of the run's rows, so it is
/// quick. [`points`](Self::points) does the work of decimating, and needs
/// neither the run nor its levels.
#[derive(Debug)]
pub struct Stretch {
    column: ColumnId,
    read: Read,
}

/// What a [`Stretch`] read.
#[derive(Debug)]
enum Read {
    /// The rows themselves, to decimate by `ratio`.
    Rows {
        ratio: usize,
        /// The run's position of the first row.
        start: u64,
        /// The positions, in order, of the rows after each of the run's gaps.
        gaps: Vec<u64>,
        /// The rows, which share the run's chunks.
        batches: Vec<SampleBatch>,
    },
    /// The points of a level, with the stretch's first and last rows. Every
    /// point is a row of the run.
    Points {
        first: Point,
        last: Point,
        slices: Vec<Slice>,
        /// The time of the first row after each of the run's gaps, in order.
        gaps: Vec<f64>,
    },
}

impl Stretch {
    /// Reads `rows`, positions in `run`, for `column` at `ratio` rows a
    /// point. Under [`FINEST`] rows a point, it reads the rows themselves.
    /// Otherwise it reads the points of the level [`level_for`] names among
    /// `levels`, the run's pyramid, with the stretch's first and last rows,
    /// by bisection. It reads the rows themselves, to decimate at that
    /// level's ratio, when `levels` is `None` or lacks the level, or `run`
    /// lacks a numeric `column`. A stretch of no rows reads none.
    pub fn read(
        run: &Run,
        rows: Range<u64>,
        column: ColumnId,
        ratio: usize,
        levels: Option<&[Level]>,
    ) -> Stretch {
        let level = level_for(ratio);
        let read = level
            .and_then(|(level, _)| levels?.get(level))
            .and_then(|level| Read::points(run, rows.clone(), column, level));
        match read {
            Some(read) => Stretch { column, read },
            None => Stretch::rows(run, rows, column, level.map_or(ratio, |(_, ratio)| ratio)),
        }
    }

    /// Reads `rows`, positions in `run`, for `column` as the rows themselves,
    /// to decimate by `ratio`, which may be 1.
    pub fn rows(run: &Run, rows: Range<u64>, column: ColumnId, ratio: usize) -> Stretch {
        let read = Read::Rows {
            ratio,
            start: rows.start,
            gaps: (run.boundaries())
                .map(|(row, _, _)| row)
                .filter(|row| rows.contains(row))
                .collect(),
            batches: run.rows(rows),
        };
        Stretch { column, read }
    }

    /// Returns how many rows or points of the history the stretch is made
    /// of, the stretch's ends counted among a level's points.
    pub fn len(&self) -> usize {
        match &self.read {
            Read::Rows { batches, .. } => batches.iter().map(SampleBatch::len).sum(),
            Read::Points { slices, .. } => {
                2 + slices.iter().map(|s| s.points().len()).sum::<usize>()
            }
        }
    }

    /// Returns `true` if the stretch holds no row.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the points, split at each of the run's gaps, with the
    /// stretch's first and last rows among them. Rows are decimated with
    /// [`Fpcs`], each part since a gap over again. A gap is each boundary
    /// within the run that is not continuous, as [`Run::boundaries`] gives
    /// them, so lost samples, a change of rate or a restart of acquisition.
    pub fn points(&self) -> Vec<Vec<Point>> {
        match &self.read {
            Read::Rows {
                ratio,
                start,
                gaps,
                batches,
            } => decimated(self.column, *ratio, *start, gaps, batches),
            Read::Points {
                first,
                last,
                slices,
                gaps,
            } => leveled(*first, *last, slices, gaps),
        }
    }
}

impl Read {
    /// The points of `rows` of `run` at `level`, one of the run's
    /// decimations, or `None` for no rows or a run without a numeric
    /// `column`.
    fn points(run: &Run, rows: Range<u64>, column: ColumnId, level: &Level) -> Option<Read> {
        if rows.is_empty() {
            return None;
        }
        let row = |row: u64| -> Option<Point> {
            let batch = run.rows(row..row + 1).pop()?;
            let value = batch.column(column)?.get(0).try_as_f64()?;
            Some((batch.timestamps()[0], value))
        };
        let (first, last) = (row(rows.start)?, row(rows.end - 1)?);
        Some(Read::Points {
            first,
            last,
            slices: level.between(first.0, last.0),
            gaps: (run.boundaries())
                .filter(|(row, _, _)| rows.contains(row))
                .map(|(_, time, _)| time)
                .collect(),
        })
    }
}

/// The rows of `column` in `batches`, the first at position `start`, split
/// before each position of `gaps`, each part decimated by `ratio` with its
/// first and last rows kept, as the decimator emits the earlier extreme of
/// its first window and holds back its last.
fn decimated(
    column: ColumnId,
    ratio: usize,
    start: u64,
    gaps: &[u64],
    batches: &[SampleBatch],
) -> Vec<Vec<Point>> {
    let mut segments = Vec::new();
    let mut fpcs = Fpcs::new(ratio, 0.0);
    // The first and last rows fed to the decimator since the last gap.
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
    let mut gaps = gaps.iter().copied().peekable();
    let mut row = start;
    for batch in batches {
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

/// The points of `slices` between the rows `first` and `last`, split at the
/// first point at or after each time of `gaps`.
fn leveled(first: Point, last: Point, slices: &[Slice], gaps: &[f64]) -> Vec<Vec<Point>> {
    let mut segments: Vec<Vec<Point>> = vec![Vec::new()];
    let mut gaps = gaps.iter().peekable();
    let leveled = slices.iter().flat_map(|slice| slice.points());
    let ends = [first, last];
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::model::continuity::{Boundary, Generations, Loss};
    use crate::data::model::sample::{BatchContext, ColumnData, SampleBatchBuilder};
    use crate::data::tests::fixtures;
    use crate::data::{Buffer, ColumnKey, ColumnProcessor, StreamMetadataSnapshot};
    use crate::proto::data::{self as wire, DataType};
    use crate::proto::{DeviceRoute, SampleNumber, StreamId};
    use proptest::prelude::*;

    /// `n` rows at 1 Hz from time one. Each is a slow tone and noise from
    /// `seed`, with a spike where a hash of the row divides by 997.
    fn rows(n: usize, seed: u64) -> (Vec<f64>, Vec<f64>) {
        let ts: Vec<f64> = (1..=n).map(|i| i as f64).collect();
        let vs = (0..n as u64)
            .map(|i| {
                let hash = (i ^ seed).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40;
                let spike = if hash.is_multiple_of(997) { 9.0 } else { 0.0 };
                (i as f64 * 0.01).sin() + hash as f64 / (1u64 << 24) as f64 + spike
            })
            .collect();
        (ts, vs)
    }

    fn fed(n: usize, seed: u64, batch: usize) -> Pyramid {
        let (ts, vs) = rows(n, seed);
        let mut pyramid = Pyramid::new();
        for (tc, vc) in ts.chunks(batch).zip(vs.chunks(batch)) {
            pyramid.update_batch(tc, &ColumnArray::F64(vc.to_vec().into()));
        }
        pyramid
    }

    fn flat(slices: &[Slice]) -> Vec<Point> {
        slices
            .iter()
            .flat_map(|slice| slice.points().iter().copied())
            .collect()
    }

    fn all(level: &Level) -> Vec<Point> {
        flat(&level.between(f64::NEG_INFINITY, f64::INFINITY))
    }

    /// The one column of stream 1 of the root device.
    fn column_key() -> ColumnKey {
        ColumnKey::new(DeviceRoute::root(), StreamId::new(1), ColumnId::new(0))
    }

    /// Stream 1 of one f64 column at 1 Hz from time zero, so the row of
    /// sample `n` stands at `n + 1` s.
    fn metadata() -> StreamMetadataSnapshot {
        fixtures::metadata(
            DeviceRoute::root(),
            fixtures::device(),
            wire::Stream {
                sample_size: 8,
                ..fixtures::stream(1)
            },
            fixtures::segment(1),
            &[fixtures::column(1, 0, DataType::F64)],
        )
    }

    /// The boundary of `samples` lost by the device.
    fn lost(samples: u64) -> Boundary {
        Boundary {
            change: None,
            loss: Some(Loss {
                samples,
                expected: None,
                dropped_batches: 0,
                dropped_samples: 0,
                link_samples: 0,
                proxy_samples: 0,
            }),
        }
    }

    /// Pushes the samples `samples` into `buffer` in batches of 1000, each
    /// valued as `rows` values the row at its time, the first batch after
    /// `boundary`.
    fn push(buffer: &mut Buffer, samples: Range<u32>, mut boundary: Option<Boundary>) {
        let (_, values) = rows(samples.end as usize, 5);
        let numbers: Vec<u32> = samples.collect();
        for chunk in numbers.chunks(1000) {
            let context = BatchContext {
                metadata: metadata(),
                boundary: boundary.take(),
                generations: Generations {
                    stream: 1,
                    device: 0,
                    global: 0,
                },
            };
            let mut builder = SampleBatchBuilder::new(context, chunk.len());
            for &n in chunk {
                let value = ColumnData::F64(values[n as usize]);
                builder.push_row(SampleNumber::new(n), [value]);
            }
            buffer.process_batch(&builder.finish());
        }
    }

    /// A buffer of the samples `0..n` but those of `gap`, which the device
    /// lost, and the pyramid of its column.
    fn history(n: u32, gap: Range<u32>) -> (Buffer, Vec<Level>) {
        let mut buffer = Buffer::new(usize::MAX);
        push(&mut buffer, 0..gap.start, None);
        let boundary = (!gap.is_empty()).then(|| lost(u64::from(gap.end - gap.start)));
        push(&mut buffer, gap.end..n, boundary);
        let mut pyramid = ColumnProcessor::new(column_key(), Pyramid::new());
        let levels = pyramid.catch_up(&buffer).clone();
        (buffer, levels)
    }

    /// The rows of the samples `0..n`, as `history` holds them.
    fn points_of(n: u32) -> Vec<Point> {
        let (times, values) = rows(n as usize, 5);
        times.into_iter().zip(values).collect()
    }

    /// `points` decimated by `ratio` with FPCS, the first and last kept.
    fn decimate(points: &[Point], ratio: usize) -> Vec<Point> {
        let (times, values): (Vec<f64>, Vec<f64>) = points.iter().copied().unzip();
        let mut fpcs = Fpcs::new(ratio, 0.0);
        fpcs.update_batch(&times, &ColumnArray::F64(values.into()));
        let (first, last) = (points[0], points[points.len() - 1]);
        let mut want = vec![first];
        want.extend(fpcs.output().iter().filter(|point| point.0 > first.0));
        if want.last().unwrap().0 < last.0 {
            want.push(last);
        }
        want
    }

    /// Every point of `parts`, in order.
    fn total(parts: &[Vec<Point>]) -> usize {
        parts.iter().map(Vec::len).sum()
    }

    /// Under FINEST rows a point a stretch is read from its rows, and its
    /// points are those rows decimated at the ratio, with its first and
    /// last rows.
    #[test]
    fn a_stretch_of_few_rows_a_point_is_its_rows_decimated_with_both_ends() {
        let n = 20_000;
        let (buffer, levels) = history(n, n..n);
        let run = buffer.run(&column_key().stream_key()).unwrap();
        let all = points_of(n);
        let column = ColumnId::new(0);
        for (rows, ratio) in [
            (0..u64::from(n), 1),
            (1234..5678, 17),
            (100..101, 5),
            (999..19_001, FINEST - 1),
        ] {
            let stretch = Stretch::read(run, rows.clone(), column, ratio, Some(&levels));
            let held = &all[rows.start as usize..rows.end as usize];
            assert_eq!(stretch.len(), held.len(), "{rows:?} read its rows");
            assert_eq!(stretch.points(), vec![decimate(held, ratio)], "{rows:?}");
            let raw = Stretch::rows(run, rows.clone(), column, ratio);
            assert_eq!(stretch.points(), raw.points(), "{rows:?}");
        }
    }

    /// From FINEST rows a point a stretch is read from the level
    /// `level_for` names, and its points are the stretch's first row, that
    /// level's points between it and the last, and the last.
    #[test]
    fn a_stretch_of_many_rows_a_point_is_read_from_its_level_within_its_times() {
        let n = 300_000;
        let (buffer, levels) = history(n, n..n);
        let run = buffer.run(&column_key().stream_key()).unwrap();
        let all = points_of(n);
        for (rows, ratio) in [
            (0..u64::from(n), 1000),
            (12_345..234_567, FINEST),
            (50_000..250_000, 5000),
            (1..u64::from(n) - 1, usize::MAX),
        ] {
            let (level, _) = level_for(ratio).unwrap();
            let stretch = Stretch::read(run, rows.clone(), ColumnId::new(0), ratio, Some(&levels));
            let (first, last) = (all[rows.start as usize], all[rows.end as usize - 1]);
            let leveled = flat(&levels[level].between(first.0, last.0));
            assert_eq!(stretch.len(), 2 + leveled.len(), "{rows:?} read its level");
            let mut want = vec![first];
            want.extend(leveled.iter().filter(|p| first.0 < p.0 && p.0 < last.0));
            want.push(last);
            let points = stretch.points();
            assert_eq!(points, vec![want], "{rows:?}");
            assert!(points[0].iter().all(|p| first.0 <= p.0 && p.0 <= last.0));
            assert!(total(&points) < (rows.end - rows.start) as usize);
        }
    }

    /// A run that lost rows is split where it lost them, a part for each
    /// side, whether read from its rows or from a level. Read from its rows,
    /// each part keeps its first and last rows.
    #[test]
    fn a_stretch_across_lost_rows_is_split_at_the_gap() {
        let (buffer, levels) = history(100_000, 40_000..40_500);
        let run = buffer.run(&column_key().stream_key()).unwrap();
        assert_eq!(run.boundaries().len(), 1);
        let rows = run.retained_rows();
        assert_eq!(rows, 0..99_500);
        let column = ColumnId::new(0);
        // Rows 0..40,000 stand at 1..=40,000 s, and the row after the gap,
        // of sample 40,500, at 40,501 s.
        for ratio in [1, 50, FINEST - 1] {
            let parts = Stretch::read(run, rows.clone(), column, ratio, Some(&levels)).points();
            assert_eq!(parts.len(), 2, "ratio {ratio}");
            let ends = |part: &Vec<Point>| (part[0].0, part[part.len() - 1].0);
            assert_eq!(ends(&parts[0]), (1.0, 40_000.0), "ratio {ratio}");
            assert_eq!(ends(&parts[1]), (40_501.0, 100_000.0), "ratio {ratio}");
        }
        for ratio in [FINEST, 1000, 20_000] {
            let parts = Stretch::read(run, rows.clone(), column, ratio, Some(&levels)).points();
            assert_eq!(parts.len(), 2, "ratio {ratio}");
            assert_eq!(parts[0][0].0, 1.0, "ratio {ratio}");
            assert!(parts[0].iter().all(|p| p.0 <= 40_000.0), "ratio {ratio}");
            assert!(parts[1].iter().all(|p| p.0 >= 40_501.0), "ratio {ratio}");
            assert_eq!(parts[1].last().unwrap().0, 100_000.0, "ratio {ratio}");
        }
        // A stretch on one side of the gap is one part.
        for ratio in [1, 50, FINEST, 1000] {
            for side in [0..40_000, 40_000..99_500] {
                let parts = Stretch::read(run, side, column, ratio, Some(&levels)).points();
                assert_eq!(parts.len(), 1, "ratio {ratio}");
            }
        }
    }

    /// A stretch's `len` counts the rows or points it read. Read from its
    /// rows at one row a point, those are its points. Read from a level, it
    /// counts both ends, which the level may also hold, so its points are no
    /// more. A stretch of no rows is empty at any ratio.
    #[test]
    fn len_counts_what_a_stretch_read_and_a_stretch_of_no_rows_is_empty() {
        let (buffer, levels) = history(10_000, 4000..4100);
        let run = buffer.run(&column_key().stream_key()).unwrap();
        let rows = run.retained_rows();
        let column = ColumnId::new(0);
        let read = |rows: Range<u64>, ratio| Stretch::read(run, rows, column, ratio, Some(&levels));
        let every = read(rows.clone(), 1);
        assert_eq!(every.len(), 9900);
        assert_eq!(total(&every.points()), every.len());
        let leveled = read(rows.clone(), 1000);
        assert!(total(&leveled.points()) <= leveled.len());
        for ratio in [1, 10, FINEST, 1000, usize::MAX] {
            for at in [0, 5, 4000, 9900] {
                let stretch = read(at..at, ratio);
                assert!(stretch.is_empty(), "ratio {ratio} at {at}");
                assert_eq!(stretch.len(), 0);
                assert!(stretch.points().is_empty(), "ratio {ratio} at {at}");
            }
        }
    }

    /// Each level begins with what FPCS emits of the rows at its ratio, and
    /// goes on with at most two points more, in time order.
    #[test]
    fn each_level_is_fpcs_of_the_rows() {
        let n = 300_000;
        let (ts, vs) = rows(n, 1);
        let pyramid = fed(n, 1, 1000);
        for (k, level) in pyramid.output().iter().enumerate() {
            let mut fpcs = Fpcs::new(FINEST << k, 0.0);
            fpcs.update_batch(&ts, &ColumnArray::F64(vs.clone().into()));
            let want = fpcs.output();
            let got = all(level);
            assert_eq!(got.len(), level.len());
            assert_eq!(got[..want.len()], want[..], "level {k}");
            assert!(got.len() <= want.len() + 2, "level {k}");
            assert!(got.windows(2).all(|pair| pair[0].0 < pair[1].0));
        }
        // The finest level has filled blocks, so the read crossed them.
        assert!(pyramid.output()[0].len() > 2 * BLOCK);
    }

    /// A stretch of a level is the level's points within it, wherever its
    /// ends fall among the blocks.
    #[test]
    fn a_stretch_of_a_level_is_its_points_within_it() {
        let n = 300_000.0;
        let pyramid = fed(n as usize, 2, 777);
        let level = &pyramid.output()[0];
        let all = all(level);
        let newest = all.last().unwrap().0;
        for (from, to) in [
            (0.0, 1.0),
            (1.0, n),
            (123_456.5, 123_999.0),
            (250_000.0, n - 10_000.25),
            (n - 1000.0, 1e9),
            (newest, newest),
            (2.0 * n, 3.0 * n),
            (all[BLOCK - 1].0, all[BLOCK].0),
            (all[BLOCK].0, all[BLOCK].0),
        ] {
            let want: Vec<Point> = all
                .iter()
                .copied()
                .filter(|point| (from..=to).contains(&point.0))
                .collect();
            assert_eq!(flat(&level.between(from, to)), want, "{from}..={to}");
        }
    }

    /// A copy of the levels keeps what it held though the pyramid drops its
    /// oldest and takes more rows.
    #[test]
    fn a_copy_of_the_levels_keeps_what_it_held() {
        let (ts, vs) = rows(350_000, 3);
        let mut pyramid = fed(300_000, 3, 1000);
        let copy = pyramid.output().clone();
        let held = all(&copy[0]);
        pyramid.drop_before(250_000.0);
        pyramid.update_batch(
            &ts[300_000..],
            &ColumnArray::F64(vs[300_000..].to_vec().into()),
        );
        assert_eq!(all(&copy[0]), held);
        let live = all(&pyramid.output()[0]);
        // A block at a time: what is left begins within a block of the cut.
        let cut = held.partition_point(|point| point.0 < 250_000.0);
        assert!(live[0].0 > held[cut - BLOCK].0 && live[0].0 <= held[cut].0);
        assert!(live.last().unwrap().0 > 349_000.0);
    }

    #[test]
    fn the_level_for_a_ratio_is_the_coarsest_no_coarser_than_it() {
        assert_eq!(level_for(1), None);
        assert_eq!(level_for(FINEST - 1), None);
        assert_eq!(level_for(FINEST), Some((0, FINEST)));
        assert_eq!(level_for(2 * FINEST - 1), Some((0, FINEST)));
        assert_eq!(level_for(2 * FINEST), Some((1, 2 * FINEST)));
        assert_eq!(level_for(1000 * FINEST), Some((9, 512 * FINEST)));
        assert_eq!(
            level_for(usize::MAX),
            Some((LEVELS - 1, FINEST << (LEVELS - 1)))
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        /// Wherever the rows stop, and however they came in batches, every
        /// level bounds every row. Within two of the level's windows of the
        /// row it has a point at least as high and one at least as low.
        #[test]
        fn every_level_bounds_every_row(
            n in 1usize..40_000,
            seed in any::<u64>(),
            batch in 1usize..3000,
        ) {
            let (_, vs) = rows(n, seed);
            let pyramid = fed(n, seed, batch);
            for (k, level) in pyramid.output().iter().enumerate().take(6) {
                let points = all(level);
                let reach = (2 * (FINEST << k)) as f64;
                for &(t, v) in &points {
                    prop_assert_eq!(v, vs[t as usize - 1], "a point is a row");
                }
                for (i, &v) in vs.iter().enumerate() {
                    // Row `i` stands at `i + 1` s.
                    let at = (i + 1) as f64;
                    let first = points.partition_point(|point| point.0 <= at - reach);
                    let near = points[first..].iter().take_while(|point| point.0 < at + reach);
                    let (lowest, highest) = near.fold((f64::MAX, f64::MIN), |(low, high), point| {
                        (low.min(point.1), high.max(point.1))
                    });
                    prop_assert!(lowest <= v && v <= highest, "level {} row {} of {}", k, i, n);
                }
            }
        }
    }
}

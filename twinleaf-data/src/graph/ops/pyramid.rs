//! Decimations of one column kept so that reading any span of its history
//! costs by the points asked for, not by the rows the span holds.
//!
//! A [`Pyramid`] holds 22 [`Fpcs`] decimations of a column. Level `k` is
//! over windows of `2^k` rows, so level 0 is every row. Every level is its
//! own FPCS over the column's rows, none a decimation of another, so each
//! holds what FPCS gives the samples at its ratio. A level's points are rows
//! of the column, each at its own time. The ratios of the levels are fixed.
//!
//! A level numbers the points its decimator emits in the order emitted. A
//! pyramid's first levels number from 0. A level made anew by a reset
//! numbers from one past the next number of the level it replaces, once
//! that level has numbered a point. So a reader that keeps the number of
//! the next point it has not read learns from the level what it missed, as
//! [`Level::emitted_from`] says.
//!
//! A [`Level`] is the points its decimator has emitted, as far back as it
//! keeps them, then the extremes of the rows it has yet to close a window
//! over. So the level bounds every row from its oldest point on. No row is
//! above the highest of the level's points within two of its windows of the
//! row, or below the lowest. A read of a stretch of time holds that for the
//! rows further than two windows from its ends, as the point that bounds a
//! row nearer may lie outside the stretch.
//!
//! A level keeps its points in blocks of 1024. A full block never
//! changes, and a copy of the level shares it, as a frozen
//! [`Buffer`](crate::Buffer) shares its chunks. So a copy of the levels
//! taken beside a frozen buffer reads as the levels do, by bisection, and
//! costs only what the levels drop after it.
//!
//! The seven levels of windows under 128 rows are the fine levels. A fine
//! level keeps its newest points only, at most 64 full blocks and the points
//! after them. A fine level drops its oldest blocks as it takes rows, by
//! count, whether a reader has read them or not. A level from 128 rows up
//! keeps its points until [`Pyramid::drop_before`] drops the full blocks
//! whose every point is older than a time. Its owner names that time, the
//! oldest row its column still holds. The drop never takes a level's
//! newest full block. So a reader that reads a level after each drop
//! misses no point to the drop while the points emitted between two drops
//! fill at most one block. A level from 128 rows up emits at most two
//! points a window, so fewer than 65,536 rows fill at most one block of it.
//! A level may then still hold points older than that row, in its oldest
//! block, in its newest full block kept whole, and in the points after it.
//!
//! A [`Stretch`] is a read of rows of one run of a column for a ratio of
//! rows a point. It decimates the rows themselves when the ratio is under
//! 128, and reads the run's level of that ratio otherwise.
//!
//! A point is 16 bytes. A fine level holds at most 64 full blocks, fewer
//! than 1024 points after them and three points more, 1,064,992 bytes. So
//! the seven fine levels of a column cost at most 7,454,944 bytes, 7.1 MiB.
//! A level of windows of `r` rows from 128 up emits one or two points a
//! window, and the levels' windows double, so those levels cost at most
//! 64/128 bytes a row. That is half a byte a row, 9 MB an hour at 5 kHz.
//! After a drop, each of those fifteen levels holds at most two blocks and
//! three points older than the time it was given, one block more than a
//! drop of every such block would leave. That is 32,816 bytes a level and
//! 492,240 bytes a column.

use crate::graph::ops::Fpcs;
use crate::graph::pipeline::walk;
use crate::proto::ColumnId;
use crate::{ColumnArray, ColumnOp, Continuity, Run, SampleBatch};
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

/// A row of a column, as its time and its value.
pub(crate) type Point = (f64, f64);

/// The rows in a window of the finest level a window read takes. A read of
/// fewer rows a point than this decimates the rows themselves.
pub(crate) const FINEST: usize = 128;
/// How many levels a pyramid has, each of windows twice as long as the one
/// before. The coarsest has windows of 2²¹ rows, over two million.
const LEVELS: usize = 22;
/// How many levels have windows under `FINEST` rows. These are the fine
/// levels, which keep their newest points only.
pub(crate) const FINE: usize = FINEST.ilog2() as usize;
/// The points in a full block of a level.
pub(crate) const BLOCK: usize = 1024;
/// The full blocks a fine level keeps, its newest.
const KEPT_BLOCKS: usize = 64;

/// Returns the level whose windows are the longest of no more than `ratio`
/// rows, and the rows in its windows, or `None` when `ratio` is under
/// `FINEST`.
pub(crate) fn level_for(ratio: usize) -> Option<(usize, usize)> {
    if ratio < FINEST {
        return None;
    }
    let level = (ratio.ilog2() as usize).min(LEVELS - 1);
    Some((level, 1 << level))
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
    /// How many of `unsettled`, the first of them, the decimator emitted.
    emitted: usize,
    /// The number of the oldest point the level holds, or of the next it
    /// emits when it holds none. It grows by a full block for each block
    /// dropped from the front.
    start: u64,
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
        self.unsettled.extend_from_slice(fpcs.output());
        self.emitted = self.unsettled.len();
        for point in fpcs.pending() {
            // The decimator emits no point earlier than its last either.
            if self.unsettled.last().is_none_or(|last| last.0 < point.0) {
                self.unsettled.push(point);
            }
        }
    }

    /// Drops the full blocks but the newest `blocks`.
    fn keep_newest(&mut self, blocks: usize) {
        let over = self.sealed.len().saturating_sub(blocks);
        self.sealed.drain(..over);
        self.start += (over * BLOCK) as u64;
    }

    /// A level made anew in place of `replaced`, which numbers its points
    /// from one past the number the next point of `replaced` would have had.
    /// In place of a level that numbered no point it numbers from 0, as no
    /// reader can have read a point of it.
    fn after(replaced: &Level) -> Level {
        let (next, _) = replaced.emitted();
        Level {
            start: if next == 0 { 0 } else { next + 1 },
            ..Level::default()
        }
    }

    /// Returns the number the next point the decimator emits will have, and
    /// the time of the newest point it emitted since the level was made, or
    /// negative infinity for none. A pyramid's first levels number their
    /// points from 0, so for them the number is how many points the
    /// decimator emitted, those dropped among them.
    pub fn emitted(&self) -> (u64, f64) {
        let held = self.sealed.len() * BLOCK + self.open.len() + self.emitted;
        let newest = (self.unsettled[..self.emitted].last())
            .or(self.open.last())
            .or(self.sealed.back().and_then(|block| block.last()));
        let newest = newest.map_or(f64::NEG_INFINITY, |point| point.0);
        (self.start + held as u64, newest)
    }

    /// Returns the emitted points numbered `from` or later that the level
    /// holds, oldest first, with the number of the first of them. With no
    /// such point it returns none and the number the next point will have.
    ///
    /// The points are numbered in the order emitted, and a pyramid never
    /// numbers two points alike. A level made anew by a reset numbers its
    /// points from one past the number the next point of the level it
    /// replaces would have had, so one number is skipped. A level that has
    /// numbered no point is made anew numbering from 0 again. So the number
    /// returned is `from` exactly when nothing lies between the reader and
    /// the level. Points dropped unread make it greater. So does a reset of
    /// a level that had numbered a point, also for a reader that had read
    /// every point and also when the new level has no point yet. So a
    /// reader learns of such a reset in the first read after it. A reader
    /// of a level that had numbered no point has read none of it, and reads
    /// the new level from 0.
    ///
    /// A reader's number is of one pyramid. A number past the next is of
    /// another, and the read then returns what the level holds. One at or
    /// under the next reads on as if it were of this pyramid. So a reader
    /// of a column that was forgotten starts again from 0.
    ///
    /// The points are what the output of an [`Fpcs`] at the level's ratio,
    /// fed the same rows since the level was made and never drained, holds
    /// from that number on, as far back as the level keeps its points.
    pub fn emitted_from(&self, from: u64) -> (u64, Vec<Point>) {
        let (next, _) = self.emitted();
        let first = match from > next {
            true => self.start,
            false => from.max(self.start),
        };
        // Every full block holds BLOCK points, so the number names its place.
        let skip = (first - self.start) as usize;
        let sealed = self.sealed.range((skip / BLOCK).min(self.sealed.len())..);
        let after = [&self.open[..], &self.unsettled[..self.emitted]];
        let skip = skip - (skip / BLOCK).min(self.sealed.len()) * BLOCK;
        let points = sealed.map(|block| &block[..]).chain(after).flatten();
        (first, points.skip(skip).copied().collect())
    }

    /// Drops the full blocks whose every point is before `time`, but never
    /// the newest full block.
    pub fn drop_before(&mut self, time: f64) {
        while self.sealed.len() > 1
            && (self.sealed.front())
                .is_some_and(|block| block.last().is_some_and(|point| point.0 < time))
        {
            self.sealed.pop_front();
            self.start += BLOCK as u64;
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
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let sealed: usize = self.sealed.iter().map(|block| block.len()).sum();
        sealed + self.open.len() + self.unsettled.len()
    }

    /// Returns how many full blocks the level holds.
    #[cfg(test)]
    pub(crate) fn blocks(&self) -> usize {
        self.sealed.len()
    }
}

/// FPCS decimations of one column at 22 ratios, over windows of 1, 2, 4
/// and so on to 2²¹ rows. The fine levels, of windows under 128 rows, keep
/// their newest 64 full blocks, and drop older ones as they take rows. A
/// level from 128 rows up keeps its points until
/// [`drop_before`](Self::drop_before) says how far back the column's rows
/// still reach.
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
            fpcs: (0..LEVELS).map(|k| Fpcs::new(1 << k, 0.0)).collect(),
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
        for level in &mut self.levels {
            *level = Level::after(level);
        }
    }

    fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray) {
        // Converted once, for every level to borrow.
        let values = match values {
            ColumnArray::F64(_) => values.clone(),
            other => ColumnArray::F64(other.to_f64().into_owned().into()),
        };
        for (k, (fpcs, level)) in self.fpcs.iter_mut().zip(&mut self.levels).enumerate() {
            fpcs.update_batch(timestamps, &values);
            level.take(fpcs);
            if k < FINE {
                level.keep_newest(KEPT_BLOCKS);
            }
        }
    }

    fn output(&self) -> &Self::Output {
        &self.levels
    }
}

/// A stretch of rows of one run, read for one column at a ratio of rows a
/// point, to turn into points later. A [`History`](crate::History)
/// reads it, from the run and the levels of that run's own column.
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
    pub(crate) fn read(
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
    pub(crate) fn rows(run: &Run, rows: Range<u64>, column: ColumnId, ratio: usize) -> Stretch {
        let read = Read::Rows {
            ratio,
            start: rows.start,
            gaps: run.gap_rows(&rows),
            batches: run.rows(rows),
        };
        Stretch { column, read }
    }

    /// Returns the cost of the read, which is how many rows or level points
    /// it took. Read from its rows, it is the rows. Read from a level, it is
    /// the level's points between the stretch's ends and the two ends. It is
    /// not the number of points the stretch gives, which may be fewer.
    #[cfg(test)]
    pub(crate) fn cost(&self) -> usize {
        match &self.read {
            Read::Rows { batches, .. } => batches.iter().map(SampleBatch::len).sum(),
            Read::Points { slices, .. } => {
                2 + slices.iter().map(|s| s.points().len()).sum::<usize>()
            }
        }
    }

    /// Returns `true` if the stretch took no row, as a stretch of no rows.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.cost() == 0
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
                .filter(|gap| rows.contains(&gap.row))
                .map(|gap| gap.after)
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
    walk(start, gaps, batches, |gap, rows| {
        if gap {
            finish(&mut fpcs, &mut ends);
        }
        let (times, Some(values)) = (rows.timestamps(), rows.column(column)) else {
            return;
        };
        fpcs.update_batch(times, values);
        let row = |at: usize| Some((times[at], values.get(at).try_as_f64()?));
        if let Some((first, last)) = row(0).zip(row(times.len() - 1)) {
            ends = Some((ends.map_or(first, |ends| ends.0), last));
        }
    });
    finish(&mut fpcs, &mut ends);
    segments
}

/// The points of `slices` between the rows `first` and `last`, split at the
/// first point at or after each time of `gaps`.
fn leveled(first: Point, last: Point, slices: &[Slice], gaps: &[f64]) -> Vec<Vec<Point>> {
    let leveled = slices.iter().flat_map(|slice| slice.points());
    let ends = [first, last];
    let mut newest = f64::NEG_INFINITY;
    // The first row, the level's points, then the last row, each end
    // unless the level has it. They are in time order but for the ends.
    let points = ends[..1].iter().chain(leveled).chain(&ends[1..]).copied();
    let points = points.filter(|point| {
        let later = newest < point.0;
        newest = newest.max(point.0);
        later
    });
    let parts = split_at_gaps(f64::NEG_INFINITY, points, gaps);
    parts.into_iter().map(|(_, part)| part).collect()
}

/// Splits `points`, in time order, into parts that each start at the first
/// point at or after a time of `gaps`, oldest first. `before` is the time of
/// the point before the first, or negative infinity for none. Each part
/// comes with whether a gap lies before its first point, which is so for
/// every part but the first, and for the first when a gap lies between
/// `before` and it.
pub fn split_at_gaps(
    before: f64,
    points: impl IntoIterator<Item = (f64, f64)>,
    gaps: &[f64],
) -> Vec<(bool, Vec<(f64, f64)>)> {
    let mut parts: Vec<(bool, Vec<Point>)> = Vec::new();
    let mut previous = before;
    for point in points {
        let next = gaps.partition_point(|&gap| gap <= previous);
        let gap = previous.is_finite() && gaps.get(next).is_some_and(|&gap| gap <= point.0);
        match parts.last_mut() {
            Some((_, part)) if !gap => part.push(point),
            _ => parts.push((gap, vec![point])),
        }
        previous = point.0;
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;
    use crate::model::continuity::{Boundary, Generations, Loss};
    use crate::model::sample::{BatchContext, ColumnData, SampleBatchBuilder};
    use crate::proto::data::{self as wire, DataType};
    use crate::proto::{DeviceRoute, SampleNumber, StreamId};
    use crate::{Buffer, ColumnKey, ColumnProcessor, StreamMetadataSnapshot};
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
            assert_eq!(stretch.cost(), held.len(), "{rows:?} read its rows");
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
            assert_eq!(stretch.cost(), 2 + leveled.len(), "{rows:?} read its level");
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

    /// A gap between two points starts a new part, and a gap between the
    /// point before and the first breaks the first part.
    #[test]
    fn a_gap_between_two_points_starts_a_new_part() {
        let points = [(1.0, 10.0), (2.0, 20.0), (5.0, 50.0)];
        assert_eq!(
            split_at_gaps(f64::NEG_INFINITY, points, &[3.0]),
            vec![(false, points[..2].to_vec()), (true, points[2..].to_vec())]
        );
        assert_eq!(
            split_at_gaps(2.0, [points[2]], &[3.0]),
            vec![(true, points[2..].to_vec())]
        );
    }

    /// A stretch's cost counts the rows or points it read. Read from its
    /// rows at one row a point, those are its points. Read from a level, it
    /// counts both ends, which the level may also hold, so its points are no
    /// more. A stretch of no rows is empty at any ratio.
    #[test]
    fn the_cost_counts_what_a_stretch_read_and_a_stretch_of_no_rows_is_empty() {
        let (buffer, levels) = history(10_000, 4000..4100);
        let run = buffer.run(&column_key().stream_key()).unwrap();
        let rows = run.retained_rows();
        let column = ColumnId::new(0);
        let read = |rows: Range<u64>, ratio| Stretch::read(run, rows, column, ratio, Some(&levels));
        let every = read(rows.clone(), 1);
        assert_eq!(every.cost(), 9900);
        assert_eq!(total(&every.points()), every.cost());
        let leveled = read(rows.clone(), 1000);
        assert!(total(&leveled.points()) <= leveled.cost());
        for ratio in [1, 10, FINEST, 1000, usize::MAX] {
            for at in [0, 5, 4000, 9900] {
                let stretch = read(at..at, ratio);
                assert!(stretch.is_empty(), "ratio {ratio} at {at}");
                assert_eq!(stretch.cost(), 0);
                assert!(stretch.points().is_empty(), "ratio {ratio} at {at}");
            }
        }
    }

    /// Rows enough that the finest level emits more points among them than
    /// it keeps.
    const BEHIND: usize = 70_000;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(6))]

        /// However the rows come in batches, the points each level emitted
        /// from the number of the next unread, read after each batch and
        /// joined, are what FPCS emits of the rows at its ratio since the
        /// last reset. So too for a reader that reads once BEHIND rows have
        /// come since it last read, so falls more than a fine level keeps
        /// behind. After each batch and before the reads, `drop_before` is
        /// given a time from far before the batch to past its last row, as
        /// a catch up drops after it feeds. The first number a read returns
        /// is the number asked but on the first read after a reset of a
        /// level that had numbered a point, where it is greater for either
        /// reader, and where points were dropped unread, where it is the
        /// level's oldest. The reader that reads
        /// after each batch misses no point of a level from FINEST up,
        /// whatever the time, as a batch fills at most one block of such a
        /// level and the newest full block stays. It misses none of a fine
        /// level while every time is FINEST rows or more before the batch.
        /// What a reader read joined with what it missed is what FPCS emits.
        /// Without the drop, a level from FINEST up holds every point it
        /// emitted since the reset, and at most two points more, in time
        /// order.
        #[test]
        fn each_level_is_fpcs_of_the_rows(
            sizes in prop::collection::vec(1usize..5000, 1..40),
            kept in prop::option::of(prop_oneof![3 => 0usize..256, 1 => 50_000usize..200_000]),
            resets in prop::collection::vec(1usize..300_000, 0..3),
        ) {
            let n = 300_000;
            let (ts, vs) = rows(n, 1);
            let mut pyramid = Pyramid::new();
            // The number of the first point of each level since the last
            // reset, and the row of that reset.
            let (mut bases, mut since) = (vec![0u64; LEVELS], 0);
            // Each reader's number of the next point of each level, what it
            // read of each level since the last reset at the number of each
            // point, `None` for a point it missed, and whether a reset came
            // since it last read.
            let mut next = [vec![0u64; LEVELS], vec![0u64; LEVELS]];
            let mut read: [Vec<Vec<Option<Point>>>; 2] = [
                vec![Vec::new(); LEVELS],
                vec![Vec::new(); LEVELS],
            ];
            let mut reset = [[false; LEVELS]; 2];
            let (mut at, mut late) = (0, 0);
            // Whether every time given to `drop_before` was FINEST rows or
            // more before the batch it followed.
            let mut early = true;
            for size in sizes.iter().cycle() {
                let end = (at + size).min(n);
                pyramid.update_batch(&ts[at..end], &ColumnArray::F64(vs[at..end].to_vec().into()));
                let before = std::mem::replace(&mut at, end);
                if let Some(kept) = kept {
                    let time = ts.get(at.saturating_sub(kept)).copied().unwrap_or(f64::INFINITY);
                    early &= time <= ts[before.saturating_sub(FINEST)];
                    pyramid.drop_before(time);
                }
                for reader in 0..2 {
                    if reader == 1 {
                        if at - late < BEHIND && at < n {
                            continue;
                        }
                        late = at;
                    }
                    for (k, level) in pyramid.output().iter().enumerate() {
                        let asked = next[reader][k];
                        let (first, points) = level.emitted_from(asked);
                        if reset[reader][k] {
                            prop_assert!(first > asked, "level {} after a reset", k);
                            read[reader][k].clear();
                        } else if first != asked {
                            let may_miss = reader == 1 || (k < FINE && !early);
                            prop_assert!(may_miss, "level {} read after each batch missed {}", k, first - asked);
                            prop_assert!(first > asked && first == level.start, "level {}", k);
                        }
                        next[reader][k] = first + points.len() as u64;
                        let read = &mut read[reader][k];
                        read.resize((first - bases[k]) as usize, None);
                        read.extend(points.into_iter().map(Some));
                    }
                    reset[reader] = [false; LEVELS];
                }
                if at == n {
                    break;
                }
                if resets.iter().any(|&row| before < row && row <= at) {
                    let mut numbered = [false; LEVELS];
                    for (k, level) in pyramid.output().iter().enumerate() {
                        // A level that numbered no point numbers from 0 again.
                        let (next, _) = level.emitted();
                        (bases[k], numbered[k]) = if next == 0 { (0, false) } else { (next + 1, true) };
                    }
                    pyramid.reset();
                    since = at;
                    for reset in &mut reset {
                        reset.iter_mut().zip(numbered).for_each(|(reset, numbered)| *reset |= numbered);
                    }
                }
            }
            for (k, level) in pyramid.output().iter().enumerate() {
                let mut fpcs = Fpcs::new(1 << k, 0.0);
                fpcs.update_batch(&ts[since..], &ColumnArray::F64(vs[since..].to_vec().into()));
                let want = fpcs.output();
                let newest = want.last().map_or(f64::NEG_INFINITY, |p| p.0);
                prop_assert_eq!(level.emitted(), (bases[k] + want.len() as u64, newest));
                for read in &read {
                    prop_assert_eq!(read[k].len(), want.len(), "level {}", k);
                    let wrong = read[k].iter().zip(want).position(|(got, want)| got.is_some_and(|got| got != *want));
                    prop_assert_eq!(wrong, None, "level {}", k);
                }
                if k >= FINE || early {
                    prop_assert!(read[0][k].iter().all(Option::is_some), "level {}", k);
                }
                if k < FINE || kept.is_some() {
                    continue;
                }
                let got = all(level);
                prop_assert_eq!(got.len(), level.len());
                prop_assert_eq!(&got[..want.len()], &want[..], "level {}", k);
                prop_assert!(got.len() <= want.len() + 2, "level {}", k);
                prop_assert!(got.windows(2).all(|pair| pair[0].0 < pair[1].0));
            }
            prop_assert!(pyramid.output()[0].len() < n);
            if since == 0 {
                // The level of FINEST rows has filled blocks, so the reads
                // crossed them, and the reader that falls behind missed
                // points of the finest.
                prop_assert!(pyramid.output()[FINE].emitted().0 > 2 * BLOCK as u64);
                prop_assert!(read[1][0].iter().any(Option::is_none));
            }
        }
    }

    /// A fine level keeps its newest `KEPT_BLOCKS` full blocks, the newest of
    /// what FPCS emits at its ratio, and the level of FINEST rows fed the
    /// same rows keeps every point it emitted.
    #[test]
    fn a_fine_level_keeps_its_newest_blocks_and_a_coarser_one_all() {
        // A sawtooth of FINEST rows a tooth, so each window of FINEST rows
        // emits two points, which fill more than KEPT_BLOCKS blocks.
        let n = 34_000 * FINEST;
        let mut pyramid = Pyramid::new();
        let mut alone: Vec<Fpcs> = (0..=FINE).map(|k| Fpcs::new(1 << k, 0.0)).collect();
        for start in (0..n).step_by(100_000) {
            let rows = start..(start + 100_000).min(n);
            let ts: Vec<f64> = rows.clone().map(|i| i as f64).collect();
            let vs: Vec<f64> = rows.map(|i| (i % FINEST) as f64).collect();
            let vs = ColumnArray::F64(vs.into());
            pyramid.update_batch(&ts, &vs);
            for fpcs in &mut alone {
                fpcs.update_batch(&ts, &vs);
            }
        }
        let levels = pyramid.output();
        for (k, (level, fpcs)) in levels.iter().zip(&alone).enumerate().take(FINE) {
            assert!(level.blocks() <= KEPT_BLOCKS, "level {k}");
            let (first, kept) = level.emitted_from(0);
            let want = fpcs.output();
            assert!(want.ends_with(&kept), "level {k}");
            assert_eq!(first as usize + kept.len(), want.len(), "level {k}");
            assert!(kept.len() > KEPT_BLOCKS * BLOCK || kept.len() == want.len());
        }
        assert_eq!(levels[0].blocks(), KEPT_BLOCKS);
        assert!(levels[FINE].blocks() > KEPT_BLOCKS);
        let all = levels[FINE].emitted_from(0);
        assert_eq!(all, (0, alone[FINE].output().clone()));
    }

    /// A stretch of a level is the level's points within it, wherever its
    /// ends fall among the blocks.
    #[test]
    fn a_stretch_of_a_level_is_its_points_within_it() {
        let n = 300_000.0;
        let pyramid = fed(n as usize, 2, 777);
        let level = &pyramid.output()[FINE];
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
        let held = all(&copy[FINE]);
        pyramid.drop_before(250_000.0);
        pyramid.update_batch(
            &ts[300_000..],
            &ColumnArray::F64(vs[300_000..].to_vec().into()),
        );
        assert_eq!(all(&copy[FINE]), held);
        let live = all(&pyramid.output()[FINE]);
        // A block at a time: what is left begins within a block of the cut.
        let cut = held.partition_point(|point| point.0 < 250_000.0);
        assert!(live[0].0 > held[cut - BLOCK].0 && live[0].0 <= held[cut].0);
        assert!(live.last().unwrap().0 > 349_000.0);
    }

    #[test]
    fn the_level_for_a_ratio_is_the_coarsest_no_coarser_than_it() {
        assert_eq!(level_for(1), None);
        assert_eq!(level_for(FINEST - 1), None);
        assert_eq!(level_for(FINEST), Some((7, FINEST)));
        assert_eq!(level_for(2 * FINEST - 1), Some((7, FINEST)));
        assert_eq!(level_for(2 * FINEST), Some((8, 2 * FINEST)));
        assert_eq!(level_for(1000 * FINEST), Some((16, 512 * FINEST)));
        assert_eq!(level_for(usize::MAX), Some((LEVELS - 1, 1 << (LEVELS - 1))));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        /// Wherever the rows stop, and however they came in batches, each of
        /// six levels from FINEST up bounds every row. Within two of the
        /// level's windows of the row it has a point at least as high and
        /// one at least as low.
        #[test]
        fn every_level_bounds_every_row(
            n in 1usize..40_000,
            seed in any::<u64>(),
            batch in 1usize..3000,
        ) {
            let (_, vs) = rows(n, seed);
            let pyramid = fed(n, seed, batch);
            for (k, level) in pyramid.output().iter().enumerate().skip(FINE).take(6) {
                let points = all(level);
                let reach = (2 * (1 << k)) as f64;
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

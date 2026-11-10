//! Decimations of one column kept so that reading any span of its history
//! costs by the points asked for, not by the rows the span holds.
//!
//! A [`Pyramid`] holds LEVELS [`Fpcs`] decimations of a column, over windows
//! of FINEST rows, twice FINEST, four times, and so on. Every level is its
//! own FPCS over the column's rows, none a decimation of another, so each
//! holds what FPCS gives the samples at its ratio. A level's points are rows
//! of the column, each at its own time.
//!
//! A [`Level`] is every point its decimator has emitted, then the extremes
//! of the rows it has yet to close a window over. So the level bounds every
//! row fed: no row is above the highest of the level's points within two of
//! its windows of the row, or below the lowest. A read of a stretch of time
//! holds that for the rows further than two windows from its ends, as the
//! point that bounds a row nearer may lie outside the stretch.
//!
//! A level keeps its points in blocks of BLOCK. A full block never changes,
//! and a copy of the level shares it, as a frozen
//! [`Buffer`](twinleaf::data::Buffer) shares its chunks. So the levels a
//! pause holds are a copy of the live ones, and both are read the same way,
//! by bisection.
//!
//! The levels are not counted in the hub's history budget. A level of
//! windows of `r` rows emits one or two points a window, 16 bytes each, and
//! the levels' windows double, so a column costs at most 64/FINEST bytes a
//! row beside the 16 or more bytes of the row: half a byte, which is 9 MB an
//! hour at 5 kHz.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use twinleaf::data::ops::Fpcs;
use twinleaf::data::{ColumnArray, ColumnOp, Continuity};

/// A row of a column: its time and its value.
pub(crate) type Point = (f64, f64);

/// The rows in a window of the finest level. A read of fewer rows a point
/// than this decimates the raw rows themselves.
pub(crate) const FINEST: usize = 128;
/// How many levels a pyramid has, each of windows twice as long as the one
/// before. The coarsest has windows of FINEST·2¹⁵ rows, about four million.
pub(crate) const LEVELS: usize = 16;
/// The points in a full block of a level.
const BLOCK: usize = 1024;

/// The level whose windows are the longest of no more than `ratio` rows, and
/// the rows in its windows, or `None` when even the finest level's are
/// longer.
pub(crate) fn level_for(ratio: usize) -> Option<(usize, usize)> {
    let level = (ratio / FINEST).checked_ilog2()? as usize;
    let level = level.min(LEVELS - 1);
    Some((level, FINEST << level))
}

/// The points of one level of a pyramid, oldest first.
#[derive(Clone, Debug, Default)]
pub(crate) struct Level {
    /// The full blocks, which never change and which a copy shares.
    sealed: VecDeque<Arc<[Point]>>,
    /// The points emitted after them, fewer than BLOCK.
    open: Vec<Point>,
    /// The points after those, which the next rows may replace: the newest
    /// point emitted, and the extremes of the rows no window has closed
    /// over. At most three.
    unsettled: Vec<Point>,
}

/// Some points of a level, in time order: a stretch of one block.
#[derive(Clone, Debug)]
pub(crate) struct Slice {
    block: Arc<[Point]>,
    range: Range<usize>,
}

impl Slice {
    pub(crate) fn points(&self) -> &[Point] {
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
    pub(crate) fn drop_before(&mut self, time: f64) {
        while self
            .sealed
            .front()
            .is_some_and(|block| block.last().is_some_and(|point| point.0 < time))
        {
            self.sealed.pop_front();
        }
    }

    /// The points whose time is within `from..=to`, oldest first. It finds
    /// them by bisection, shares the full blocks and copies at most the
    /// points after them.
    pub(crate) fn between(&self, from: f64, to: f64) -> Vec<Slice> {
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

    /// How many points the level holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let sealed: usize = self.sealed.iter().map(|block| block.len()).sum();
        sealed + self.open.len() + self.unsettled.len()
    }
}

/// FPCS decimations of one column at LEVELS ratios, each kept whole until
/// [`drop_before`](Self::drop_before) says how far back the column's rows
/// still reach.
#[derive(Debug)]
pub(crate) struct Pyramid {
    /// The decimator of each level, finest first. Its output is moved to
    /// the level as it settles.
    fpcs: Vec<Fpcs>,
    levels: Vec<Level>,
}

impl Pyramid {
    pub(crate) fn new() -> Pyramid {
        Pyramid {
            fpcs: (0..LEVELS).map(|k| Fpcs::new(FINEST << k, 0.0)).collect(),
            levels: vec![Level::default(); LEVELS],
        }
    }

    /// Drops what each level holds before `time`, the oldest row the column
    /// still holds, a block at a time.
    pub(crate) fn drop_before(&mut self, time: f64) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// `n` rows at 1 Hz from time one: a slow tone, noise from `seed`, and a
    /// spike about every 997 rows.
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
        /// level bounds every row: within two of the level's windows of the
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

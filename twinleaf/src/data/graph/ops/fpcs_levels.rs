//! Several FPCS decimations of one column, each coarser than the last, so a
//! plot of any span finds one with about as many points as it has pixels.
//!
//! Every level is its own [`Fpcs`] over the column's samples, not over the
//! level finer than it. FPCS keeps the earlier extreme of each window and
//! defers the later one to the next, so decimating a decimation would not
//! give what FPCS gives the samples. Each level only appends as samples
//! come, so a plot that scrolls along one sees no point it has drawn move.
//! Each keeps about as many points as the others, so a coarser level
//! reaches further back.

use super::Fpcs;
use crate::data::{ColumnArray, ColumnOp, Continuity};

/// [`Fpcs`] over windows of `base`, `base·factor`, `base·factor²`, …
/// samples, level `k` keeping `retention_seconds·factor^k` of its output.
#[derive(Debug)]
pub struct FpcsLevels {
    base: usize,
    factor: usize,
    levels: Vec<Fpcs>,
}

impl FpcsLevels {
    /// Creates `count` levels, at least one. The finest is over windows of
    /// `base` samples, kept for `retention_seconds`, and each next one over
    /// windows `factor` times as long, kept `factor` times as long. A
    /// `factor` below two is two.
    pub fn new(base: usize, factor: usize, count: usize, retention_seconds: f64) -> Self {
        let (base, factor) = (base.max(1), factor.max(2));
        let levels = (0..count.max(1))
            .map(|k| {
                let scale = factor.pow(k as u32);
                Fpcs::new(base * scale, retention_seconds * scale as f64)
            })
            .collect();
        Self {
            base,
            factor,
            levels,
        }
    }

    /// Returns the samples per window of the finest level.
    pub fn base(&self) -> usize {
        self.base
    }

    /// Returns how many times longer each level's windows are than the
    /// level before.
    pub fn factor(&self) -> usize {
        self.factor
    }

    /// Sets the samples per window of the finest level, and of every other
    /// in proportion. Reset the levels and replay the column after a change,
    /// as [`ColumnProcessor::invalidate`](crate::data::ColumnProcessor::invalidate)
    /// arranges.
    pub fn set_base(&mut self, base: usize) {
        self.base = base.max(1);
        let mut ratio = self.base;
        for level in &mut self.levels {
            level.set_ratio(ratio);
            ratio *= self.factor;
        }
    }

    /// Returns the levels, finest first.
    pub fn levels(&self) -> &[Fpcs] {
        &self.levels
    }

    /// Keeps each level's output for `retention_seconds·factor^k`, as
    /// [`new`](Self::new) does, but for no longer than `limit_seconds`.
    /// Shortening it drops points with the next samples, and lengthening it
    /// keeps more from then on, as [`Fpcs::set_retention_seconds`] says.
    pub fn set_retention(&mut self, retention_seconds: f64, limit_seconds: f64) {
        let mut seconds = retention_seconds;
        for level in &mut self.levels {
            level.set_retention_seconds(seconds.min(limit_seconds));
            seconds *= self.factor as f64;
        }
    }
}

impl ColumnOp for FpcsLevels {
    type Output = Vec<Fpcs>;

    /// As each level does, the levels keep their output across gaps.
    fn continuity(&self) -> Continuity {
        Continuity::Monotonic
    }

    fn reset(&mut self) {
        for level in &mut self.levels {
            level.reset();
        }
    }

    fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray) {
        let values = values.to_f64();
        for level in &mut self.levels {
            level.update_f64(timestamps, &values);
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

    /// `seconds` of a 1 kHz signal from `seed`, two tones, a ramp and a
    /// spike every 0.37 s, so every level has extremes to keep.
    fn signal(seconds: f64, seed: u64) -> (Vec<f64>, Vec<f64>) {
        let n = (seconds * 1000.0) as usize;
        let phase = seed as f64 * 0.618;
        let ts: Vec<f64> = (0..n).map(|i| i as f64 / 1000.0).collect();
        let vs = ts
            .iter()
            .map(|&t| {
                let spike = if ((t * 1000.0) as u64 % 370) == seed % 370 {
                    4.0
                } else {
                    0.0
                };
                (t * 7.3 + phase).sin() + 0.3 * (t * 113.0).sin() + 0.05 * t + spike
            })
            .collect();
        (ts, vs)
    }

    fn feed(op: &mut impl ColumnOp, ts: &[f64], vs: &[f64], chunk: usize) {
        for (tc, vc) in ts.chunks(chunk).zip(vs.chunks(chunk)) {
            op.update_batch(tc, &ColumnArray::F64(vc.to_vec().into()));
        }
    }

    proptest! {
        /// Each level is exactly what one decimator at its ratio and
        /// retention gives the same samples in the same batches.
        #[test]
        fn each_level_is_fpcs_of_the_samples(seed in 0u64..1000, chunk in 1usize..400, base in 2usize..12) {
            let (ts, vs) = signal(20.0, seed);
            let mut levels = FpcsLevels::new(base, 4, 4, 5.0);
            feed(&mut levels, &ts, &vs, chunk);
            for (k, level) in levels.levels().iter().enumerate() {
                let scale = 4usize.pow(k as u32);
                let mut alone = Fpcs::new(base * scale, 5.0 * scale as f64);
                feed(&mut alone, &ts, &vs, chunk);
                prop_assert_eq!(level.output(), alone.output(), "level {}", k);
            }
        }

        /// A level only ever adds points after the last it has. What it held
        /// before, less what aged out, starts what it holds after.
        #[test]
        fn a_level_only_appends(seed in 0u64..1000, chunk in 1usize..400) {
            let (ts, vs) = signal(30.0, seed);
            let mut levels = FpcsLevels::new(5, 4, 3, 4.0);
            let mut before: Vec<Vec<(f64, f64)>> = vec![Vec::new(); 3];
            for (tc, vc) in ts.chunks(chunk).zip(vs.chunks(chunk)) {
                levels.update_batch(tc, &ColumnArray::F64(vc.to_vec().into()));
                for (k, level) in levels.levels().iter().enumerate() {
                    let now = level.output();
                    let kept = before[k].iter().position(|p| Some(p) == now.first()).unwrap_or(before[k].len());
                    let still = &before[k][kept..];
                    prop_assert!(now.len() >= still.len() && &now[..still.len()] == still, "level {} changed a point it had", k);
                    before[k] = now.clone();
                }
            }
        }
    }

    /// Each level holds about the same number of points, so each coarser
    /// one reaches `factor` times as far back.
    #[test]
    fn each_level_holds_about_as_many_points_reaching_further_back() {
        let (ts, vs) = signal(400.0, 3);
        let mut levels = FpcsLevels::new(5, 4, 3, 5.0);
        feed(&mut levels, &ts, &vs, 1000);
        // Five seconds at 1 kHz in windows of five: a thousand windows, one
        // or two points each, and up to a third more before old ones go.
        let newest = *ts.last().unwrap();
        for (k, level) in levels.levels().iter().enumerate() {
            let out = level.output();
            assert!(
                (990..=2700).contains(&out.len()),
                "level {k} holds {}",
                out.len()
            );
            let reach = newest - out[0].0;
            let kept = 5.0 * 4f64.powi(k as i32);
            assert!(
                reach >= kept && reach <= kept * 1.4,
                "level {k} reaches back {reach} s"
            );
        }
    }

    /// A limit caps every level's retention, the finer ones keeping theirs.
    #[test]
    fn a_limit_caps_every_retention() {
        let mut levels = FpcsLevels::new(5, 4, 4, 10.0);
        levels.set_retention(10.0, 100.0);
        let kept: Vec<f64> = levels
            .levels()
            .iter()
            .map(Fpcs::retention_seconds)
            .collect();
        assert_eq!(kept, [10.0, 40.0, 100.0, 100.0]);
    }

    /// A new base moves every level's windows in proportion.
    #[test]
    fn a_new_base_sets_every_ratio() {
        let mut levels = FpcsLevels::new(5, 4, 3, 5.0);
        levels.set_base(8);
        let ratios: Vec<usize> = levels.levels().iter().map(Fpcs::ratio).collect();
        assert_eq!(ratios, [8, 32, 128]);
    }
}

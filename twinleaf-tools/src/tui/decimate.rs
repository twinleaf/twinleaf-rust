//! Streaming FPCS (Feature-Preserving Compensated Sampling) decimator.
//!
//! Reference algorithm: Li, Yang, Chua, "FPCS", IEEE TVCG 2025; adapted from
//! <https://github.com/tmichela/fpcs> (MIT), `src/fpcs/fpcs_pure.py`.

use twinleaf::data::{ColumnOp, ColumnView, ColumnWindow};

/// Streaming FPCS decimator: consumes fixed-`ratio` windows, emits the earlier
/// of each window's min/max, and carries the later one forward.
///
/// Compensation: when consecutive windows retain the same kind of extremum
/// (min-after-min / max-after-max), the first's deferred point is emitted
/// between them, so fast oscillations aren't collapsed to every-other extreme.
#[derive(Debug)]
pub struct Fpcs {
    ratio: usize,
    retention_seconds: f64,
    out: Vec<(f64, f64)>,

    // `None` until the window's first non-NaN sample.
    min_point: Option<(f64, f64)>,
    max_point: Option<(f64, f64)>,
    counter: usize,
    /// Held in time order, reinserted around the extremum when the window closes.
    pending_nans: Vec<(f64, f64)>,
    /// Extremum deferred by the previous close, awaiting possible compensation.
    potential_point: Option<(f64, f64)>,
    /// Kind the previous window retained (min = `true`), for compensation.
    previous_min_retained: Option<bool>,
}

impl Fpcs {
    /// `ratio` is clamped to >= 1; `retention_seconds` prunes old output verbatim.
    pub fn new(ratio: usize, retention_seconds: f64) -> Self {
        Self {
            ratio: ratio.max(1),
            retention_seconds,
            out: Vec::new(),
            min_point: None,
            max_point: None,
            counter: 0,
            pending_nans: Vec::new(),
            potential_point: None,
            previous_min_retained: None,
        }
    }

    pub fn ratio(&self) -> usize {
        self.ratio
    }

    /// Callers must `reset` and replay afterward, or output mixes two ratios.
    pub fn set_ratio(&mut self, ratio: usize) {
        self.ratio = ratio.max(1);
    }

    /// Drops `p` instead of pushing if it would break `out`'s time ordering
    /// (e.g. an upstream backward timestamp jump)
    fn push_out(&mut self, p: (f64, f64)) {
        if self.out.last().is_some_and(|&(t, _)| p.0 < t) {
            return;
        }
        self.out.push(p);
    }

    fn add_point(&mut self, x: f64, y: f64) {
        if self.ratio == 1 {
            self.push_out((x, y));
            return;
        }

        if y.is_nan() {
            self.pending_nans.push((x, y));
            self.counter += 1;
            if self.counter >= self.ratio {
                self.close_window();
            }
            return;
        }

        let Some(max_point) = self.max_point else {
            // Seed both extrema with the window's first non-NaN sample.
            self.min_point = Some((x, y));
            self.max_point = Some((x, y));
            self.counter += 1;
            return;
        };
        let min_point = self.min_point.expect("seeded together with max_point");

        self.counter += 1;
        if y >= max_point.1 {
            self.max_point = Some((x, y));
        } else if y < min_point.1 {
            self.min_point = Some((x, y));
        }

        if self.counter >= self.ratio {
            self.close_window();
        }
    }

    fn close_window(&mut self) {
        let (Some(min_point), Some(max_point)) = (self.min_point, self.max_point) else {
            // Only NaNs this window: flush them and keep waiting.
            let nans = std::mem::take(&mut self.pending_nans);
            for nan in nans {
                self.push_out(nan);
            }
            self.counter = 0;
            return;
        };

        let (retained, deferred, min_retained) = if min_point.0 < max_point.0 {
            (min_point, max_point, true)
        } else {
            (max_point, min_point, false)
        };

        // Same kind retained twice: the deferred point predates this window
        // and would otherwise never surface, so emit it first.
        if self.previous_min_retained == Some(min_retained) {
            if let Some(pp) = self.potential_point {
                if pp != retained {
                    self.push_out(pp);
                }
            }
        }

        // `deferred` isn't emitted yet (it may surface later via compensation),
        // so hold back any NaN after its time to keep them ordered after it.
        let hold_at = self.pending_nans.partition_point(|p| p.0 <= deferred.0);
        let held = self.pending_nans.split_off(hold_at);
        self.emit_window_points(retained);
        self.pending_nans = held;

        self.potential_point = Some(deferred);
        self.min_point = Some(deferred);
        self.max_point = Some(deferred);
        self.previous_min_retained = Some(min_retained);
        self.counter = 0;
    }

    /// Emit the retained extremum inserted into its window's NaNs in time order.
    fn emit_window_points(&mut self, retained: (f64, f64)) {
        let nans = std::mem::take(&mut self.pending_nans);
        let insert_at = nans.partition_point(|p| p.0 <= retained.0);
        for &nan in &nans[..insert_at] {
            self.push_out(nan);
        }
        self.push_out(retained);
        for &nan in &nans[insert_at..] {
            self.push_out(nan);
        }
    }

    /// Amortized front-prune: only drain once the stale prefix is a quarter of `out`.
    fn prune_retention(&mut self) {
        if !self.retention_seconds.is_finite() || self.retention_seconds <= 0.0 {
            return;
        }
        let Some(&(newest_t, _)) = self.out.last() else {
            return;
        };
        let cutoff = newest_t - self.retention_seconds;
        let cut = self.out.partition_point(|&(t, _)| t < cutoff);
        if cut * 4 >= self.out.len() {
            self.out.drain(..cut);
        }
    }

    fn push_values(&mut self, times: impl Iterator<Item = f64>, values: impl Iterator<Item = f64>) {
        for (t, v) in times.zip(values) {
            self.add_point(t, v);
        }
    }
}

impl ColumnOp for Fpcs {
    type Output = Vec<(f64, f64)>;

    fn reset(&mut self) {
        self.out.clear();
        self.min_point = None;
        self.max_point = None;
        self.counter = 0;
        self.pending_nans.clear();
        self.potential_point = None;
        self.previous_min_retained = None;
    }

    fn push(&mut self, chunk: &ColumnWindow) {
        let (ta, tb) = chunk.timestamps;
        let times = ta.iter().chain(tb.iter()).copied();
        match chunk.values {
            ColumnView::F64(a, b) => self.push_values(times, a.iter().chain(b.iter()).copied()),
            ColumnView::I64(a, b) => {
                self.push_values(times, a.iter().chain(b.iter()).map(|&x| x as f64))
            }
            ColumnView::U64(a, b) => {
                self.push_values(times, a.iter().chain(b.iter()).map(|&x| x as f64))
            }
        }
        self.prune_retention();
    }

    fn output(&self) -> &Self::Output {
        &self.out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use twinleaf::tio::proto::{ColumnMetadata, DataType};

    fn meta() -> Arc<ColumnMetadata> {
        Arc::new(ColumnMetadata {
            stream_id: 0,
            index: 0,
            data_type: DataType::Float64,
            name: "ch".into(),
            units: "V".into(),
            description: "test".into(),
        })
    }

    fn window<'a>(ts: &'a [f64], vals: &'a [f64]) -> ColumnWindow<'a> {
        ColumnWindow {
            run_id: 0,
            effective_rate: 1.0,
            timestamps: (ts, &[]),
            values: ColumnView::F64(vals, &[]),
            column_metadata: meta(),
        }
    }

    fn seam_window<'a>(
        ta: &'a [f64],
        tb: &'a [f64],
        va: &'a [f64],
        vb: &'a [f64],
    ) -> ColumnWindow<'a> {
        ColumnWindow {
            run_id: 0,
            effective_rate: 1.0,
            timestamps: (ta, tb),
            values: ColumnView::F64(va, vb),
            column_metadata: meta(),
        }
    }

    fn is_monotonic(out: &[(f64, f64)]) -> bool {
        out.windows(2).all(|w| w[1].0 >= w[0].0)
    }

    /// Point equality treating NaN values as equal, for output containing NaNs.
    fn points_eq(a: &[(f64, f64)], b: &[(f64, f64)]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b.iter()).all(|(&(t1, v1), &(t2, v2))| {
                t1 == t2 && (v1 == v2 || (v1.is_nan() && v2.is_nan()))
            })
    }

    #[test]
    fn ratio_one_is_passthrough() {
        let ts: Vec<f64> = (0..20).map(|i| i as f64).collect();
        let vals: Vec<f64> = (0..20).map(|i| (i as f64 * 0.7).sin()).collect();
        let mut op = Fpcs::new(1, 1000.0);
        op.push(&window(&ts, &vals));
        let expected: Vec<(f64, f64)> = ts.iter().copied().zip(vals.iter().copied()).collect();
        assert_eq!(op.output(), &expected);
    }

    #[test]
    fn streaming_matches_batch_across_chunkings() {
        let n = 500;
        let ts: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let vals: Vec<f64> = (0..n)
            .map(|i| ((i as f64) * 0.31).sin() * 10.0 + (i % 13) as f64)
            .collect();

        let mut batch = Fpcs::new(7, 1e9);
        batch.push(&window(&ts, &vals));
        let expected = batch.output().clone();
        assert!(!expected.is_empty());

        for chunk_size in [1, 2, 3, 5, 7, 16, 64, 500] {
            let mut streamed = Fpcs::new(7, 1e9);
            for (tc, vc) in ts.chunks(chunk_size).zip(vals.chunks(chunk_size)) {
                streamed.push(&window(tc, vc));
            }
            assert_eq!(
                streamed.output(),
                &expected,
                "chunk_size={chunk_size} diverged from single-chunk batch"
            );
        }
    }

    #[test]
    fn streaming_matches_batch_across_ring_seam() {
        let n = 40;
        let ts: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let vals: Vec<f64> = (0..n).map(|i| ((i as f64) * 0.9).sin() * 5.0).collect();

        let mut batch = Fpcs::new(4, 1e9);
        batch.push(&window(&ts, &vals));
        let expected = batch.output().clone();

        for seam in 1..n {
            let mut streamed = Fpcs::new(4, 1e9);
            streamed.push(&seam_window(
                &ts[..seam],
                &ts[seam..],
                &vals[..seam],
                &vals[seam..],
            ));
            assert_eq!(streamed.output(), &expected, "seam={seam} diverged");
        }
    }

    /// Streaming == batch with NaNs interleaved; guards the NaN hold logic.
    #[test]
    fn streaming_matches_batch_with_nans_across_chunkings() {
        let n = 600;
        let ts: Vec<f64> = (0..n).map(|i| i as f64 * 0.5).collect();
        let vals: Vec<f64> = (0..n)
            .map(|i| {
                if i % 11 == 0 {
                    f64::NAN
                } else {
                    ((i as f64) * 0.17).sin() * 20.0 - (i % 9) as f64
                }
            })
            .collect();

        for ratio in [2, 3, 5, 9] {
            let mut batch = Fpcs::new(ratio, 1e9);
            batch.push(&window(&ts, &vals));
            let expected = batch.output().clone();
            assert!(is_monotonic(&expected));

            for chunk_size in [1, 4, 11, 13, 37, 600] {
                let mut streamed = Fpcs::new(ratio, 1e9);
                for (tc, vc) in ts.chunks(chunk_size).zip(vals.chunks(chunk_size)) {
                    streamed.push(&window(tc, vc));
                }
                assert!(
                    points_eq(streamed.output(), &expected),
                    "ratio={ratio} chunk_size={chunk_size} diverged: {:?} vs {:?}",
                    streamed.output(),
                    expected
                );
            }
        }
    }

    /// Two min-retaining windows (ratio=2) must emit the first window's
    /// deferred max between them. A=(t0:-1, t1:100) retains min@t0, defers
    /// max@t1; B=(t2:-20, t3:150) retains min@t2 again -> 100 goes between.
    #[test]
    fn compensation_emits_deferred_extremum_between_same_kind_windows() {
        let ts = vec![0.0, 1.0, 2.0, 3.0];
        let vals = vec![-1.0, 100.0, -20.0, 150.0];
        let mut op = Fpcs::new(2, 1e9);
        op.push(&window(&ts, &vals));

        let out = op.output().clone();
        let pos = |p: (f64, f64)| {
            out.iter()
                .position(|&q| q == p)
                .unwrap_or_else(|| panic!("expected point {:?} in output {:?}", p, out))
        };

        let min_a = pos((0.0, -1.0));
        let deferred_max = pos((1.0, 100.0));
        let min_b = pos((2.0, -20.0));
        assert!(
            min_a < deferred_max && deferred_max < min_b,
            "compensation ordering violated: {:?}",
            out
        );
    }

    /// A NaN must reach the output as a gap; the streaming==batch tests can't
    /// catch this, since both sides run identical code.
    #[test]
    fn nan_emitted_as_gap_with_decimation() {
        let ts = vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        let vals = vec![1.0, f64::NAN, 3.0, -5.0, 2.0, 9.0];
        let mut op = Fpcs::new(3, 1e9);
        op.push(&window(&ts, &vals));
        assert!(op.output().iter().any(|&(t, v)| t == 1.0 && v.is_nan()));
        assert!(is_monotonic(op.output()));
    }

    #[test]
    fn reset_clears_all_state() {
        let ts: Vec<f64> = (0..20).map(|i| i as f64).collect();
        let vals: Vec<f64> = (0..20).map(|i| i as f64 * 2.0).collect();
        let mut op = Fpcs::new(3, 1e9);
        op.push(&window(&ts, &vals));
        assert!(!op.output().is_empty());

        op.reset();
        assert!(op.output().is_empty());

        // No residual state leaks into the next run.
        op.push(&window(&ts, &vals));
        assert!(!op.output().is_empty());
    }

    #[test]
    fn backward_time_jump_is_dropped() {
        let ts = vec![0.0, 1.0, 2.0, -30.0, 3.0, 4.0];
        let vals = vec![10.0, 11.0, 12.0, 99.0, 13.0, 14.0];
        let mut op = Fpcs::new(1, 1e9);
        op.push(&window(&ts, &vals));

        assert!(is_monotonic(op.output()));
        assert_eq!(
            op.output(),
            &[
                (0.0, 10.0),
                (1.0, 11.0),
                (2.0, 12.0),
                (3.0, 13.0),
                (4.0, 14.0)
            ]
        );
    }

    #[test]
    fn retention_prunes_old_points() {
        let mut op = Fpcs::new(1, 5.0);
        let ts: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let vals: Vec<f64> = vec![0.0; 100];
        op.push(&window(&ts, &vals));

        let out = op.output();
        let newest = out.last().unwrap().0;
        assert!(out.len() < 100, "expected pruning to shrink the buffer");
        assert!(out.iter().all(|&(t, _)| t >= newest - 5.0));
    }
}

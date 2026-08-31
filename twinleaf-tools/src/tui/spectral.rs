//! Streaming Welch spectral density: holds a fixed-capacity ring of the raw
//! signal and recomputes the estimate only when `push` delivers new samples.

use std::collections::VecDeque;

use twinleaf::data::{ColumnArray, ColumnOp};
use welch_sde::{Build, SpectralDensity};

const WELCH_DEFAULT_SEGMENTS: usize = 4;
const WELCH_DEFAULT_OVERLAP: f64 = 0.5;
const WELCH_DFT_MAX_SIZE: usize = 4096;
const MIN_FFT_SAMPLES: usize = 60;

#[derive(Debug, Clone)]
pub struct FftReadyData {
    pub points: Vec<(f64, f64)>,
    pub median_asd: f64,
    pub sample_count: usize,
    pub total_sample_count: usize,
    pub sampling_hz: f64,
    pub segment_size: usize,
    pub hop_size: usize,
}

#[derive(Debug, Clone)]
pub enum FftStatus {
    WaitingForSelection,
    WaitingForSamples,
    InvalidSampleRate {
        sampling_rate: u32,
        decimation: u32,
    },
    TooFewSamples {
        have: usize,
        need: usize,
        sampling_hz: f64,
        window_seconds: f64,
    },
    NoValidFrequencyBins {
        sample_count: usize,
        sampling_hz: f64,
    },
}

#[derive(Debug)]
pub struct WelchOp {
    window_samples: usize,
    sampling_hz: f64,
    plot_window_seconds: f64,
    ring: VecDeque<f64>,
    result: Result<FftReadyData, FftStatus>,
}

impl WelchOp {
    pub fn new(window_samples: usize, sampling_hz: f64, plot_window_seconds: f64) -> Self {
        Self {
            window_samples,
            sampling_hz,
            plot_window_seconds,
            ring: VecDeque::new(),
            result: Err(FftStatus::WaitingForSamples),
        }
    }

    pub fn window_samples(&self) -> usize {
        self.window_samples
    }

    pub fn sampling_hz(&self) -> f64 {
        self.sampling_hz
    }

    pub fn plot_window_seconds(&self) -> f64 {
        self.plot_window_seconds
    }

    /// If `window_samples` or `sampling_hz` changes, callers must `reset`
    /// afterward, or the ring mixes samples computed under different parameters.
    /// `plot_window_seconds` alone only affects a diagnostic string.
    pub fn configure(&mut self, window_samples: usize, sampling_hz: f64, plot_window_seconds: f64) {
        self.window_samples = window_samples;
        self.sampling_hz = sampling_hz;
        self.plot_window_seconds = plot_window_seconds;
    }

    fn recompute(&mut self) {
        self.result = Self::welch(&self.ring, self.sampling_hz, self.plot_window_seconds);
    }

    fn welch(
        ring: &VecDeque<f64>,
        sampling_hz: f64,
        plot_window_seconds: f64,
    ) -> Result<FftReadyData, FftStatus> {
        if !sampling_hz.is_finite() || sampling_hz <= 0.0 {
            return Err(FftStatus::InvalidSampleRate {
                sampling_rate: 0,
                decimation: 0,
            });
        }

        let total_sample_count = ring.len();
        if total_sample_count < MIN_FFT_SAMPLES {
            return Err(FftStatus::TooFewSamples {
                have: total_sample_count,
                need: MIN_FFT_SAMPLES,
                sampling_hz,
                window_seconds: plot_window_seconds,
            });
        }

        let signal: Vec<f64> = ring.iter().copied().collect();
        let (fft_signal, segment_size, hop_size) = latest_complete_welch_signal(&signal);

        let mean_val = fft_signal.iter().sum::<f64>() / fft_signal.len() as f64;
        let detrended: Vec<f64> = fft_signal.iter().map(|x| x - mean_val).collect();

        let welch: SpectralDensity<f64> = SpectralDensity::builder(&detrended, sampling_hz).build();
        let sd = welch.periodogram();
        let pts: Vec<(f64, f64)> = sd
            .frequency()
            .into_iter()
            .zip(sd.iter().copied())
            .filter_map(|(f, d)| {
                if f > 0.0 && d.is_finite() && d > 0.0 {
                    Some((f, d.sqrt()))
                } else {
                    None
                }
            })
            .collect();

        if pts.is_empty() {
            return Err(FftStatus::NoValidFrequencyBins {
                sample_count: fft_signal.len(),
                sampling_hz,
            });
        }

        let mut asd_values: Vec<f64> = pts.iter().map(|(_, d)| *d).collect();
        asd_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_asd = if asd_values.len().is_multiple_of(2) {
            (asd_values[asd_values.len() / 2 - 1] + asd_values[asd_values.len() / 2]) / 2.0
        } else {
            asd_values[asd_values.len() / 2]
        };

        Ok(FftReadyData {
            points: pts,
            median_asd,
            sample_count: fft_signal.len(),
            total_sample_count,
            sampling_hz,
            segment_size,
            hop_size,
        })
    }
}

impl ColumnOp for WelchOp {
    type Output = Result<FftReadyData, FftStatus>;

    fn reset(&mut self) {
        self.ring.clear();
        self.result = Err(FftStatus::WaitingForSamples);
    }

    fn update_batch(&mut self, _timestamps: &[f64], values: &ColumnArray) {
        match values {
            ColumnArray::F64(v) => self.ring.extend(v.iter().copied()),
            ColumnArray::I64(v) => self.ring.extend(v.iter().map(|&x| x as f64)),
            ColumnArray::U64(v) => self.ring.extend(v.iter().map(|&x| x as f64)),
        }
        while self.ring.len() > self.window_samples {
            self.ring.pop_front();
        }
        self.recompute();
    }

    fn output(&self) -> &Self::Output {
        &self.result
    }
}

/// Longest signal prefix (from the end, i.e. the most recent samples) whose
/// length is an exact `segment_size + (segments - 1) * hop_size` for Welch's
/// method with `WELCH_DEFAULT_SEGMENTS` segments overlapped
/// `WELCH_DEFAULT_OVERLAP`, capped so the per-segment DFT never exceeds
/// `WELCH_DFT_MAX_SIZE`. Returns `(signal_slice, segment_size, hop_size)`.
fn latest_complete_welch_signal(signal: &[f64]) -> (&[f64], usize, usize) {
    let denominator =
        WELCH_DEFAULT_SEGMENTS as f64 * (1.0 - WELCH_DEFAULT_OVERLAP) + WELCH_DEFAULT_OVERLAP;
    let default_segment_size = (signal.len() as f64 / denominator).trunc().max(1.0) as usize;

    if default_segment_size.next_power_of_two() <= WELCH_DFT_MAX_SIZE {
        let hop_size = default_segment_size
            - (default_segment_size as f64 * WELCH_DEFAULT_OVERLAP).round() as usize;
        return (signal, default_segment_size, hop_size.max(1));
    }

    let segment_size = WELCH_DFT_MAX_SIZE;
    let hop_size = segment_size - (segment_size as f64 * WELCH_DEFAULT_OVERLAP).round() as usize;
    let segment_count = (signal.len() - segment_size) / hop_size + 1;
    let used_len = (segment_count - 1) * hop_size + segment_size;

    (
        &signal[signal.len() - used_len..],
        segment_size,
        hop_size.max(1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;

    /// Feed one span of `f64` samples.
    fn push(op: &mut WelchOp, ts: &[f64], vals: &[f64]) {
        op.update_batch(ts, &ColumnArray::F64(vals.to_vec().into()));
    }

    fn sine_signal(n: usize, freq_hz: f64, sampling_hz: f64) -> (Vec<f64>, Vec<f64>) {
        let ts: Vec<f64> = (0..n).map(|i| i as f64 / sampling_hz).collect();
        let vals: Vec<f64> = ts.iter().map(|&t| (2.0 * PI * freq_hz * t).sin()).collect();
        (ts, vals)
    }

    #[test]
    fn sine_wave_peak_matches_known_frequency() {
        let sampling_hz = 1000.0;
        let freq_hz = 50.0;
        let n = 4096;
        let (ts, vals) = sine_signal(n, freq_hz, sampling_hz);

        let mut op = WelchOp::new(n, sampling_hz, 10.0);
        push(&mut op, &ts, &vals);

        let data = op.output().as_ref().expect("expected Ok result");
        let (peak_freq, _) =
            data.points
                .iter()
                .copied()
                .fold((0.0, f64::NEG_INFINITY), |acc, (f, d)| {
                    if d > acc.1 {
                        (f, d)
                    } else {
                        acc
                    }
                });

        let bin_hz = sampling_hz / data.segment_size as f64;
        assert!(
            (peak_freq - freq_hz).abs() <= 2.0 * bin_hz,
            "peak at {peak_freq} Hz, expected near {freq_hz} Hz (bin={bin_hz})"
        );
    }

    #[test]
    fn too_few_samples_reports_have_and_need() {
        let n = MIN_FFT_SAMPLES - 1;
        let ts: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let vals: Vec<f64> = vec![1.0; n];

        let mut op = WelchOp::new(1000, 100.0, 5.0);
        push(&mut op, &ts, &vals);

        match op.output() {
            Err(FftStatus::TooFewSamples { have, need, .. }) => {
                assert_eq!(*have, n);
                assert_eq!(*need, MIN_FFT_SAMPLES);
            }
            other => panic!("expected TooFewSamples, got {other:?}"),
        }
    }

    #[test]
    fn invalid_sampling_rate_is_rejected() {
        for bad_hz in [0.0, f64::NAN, -1.0] {
            let n = MIN_FFT_SAMPLES + 10;
            let ts: Vec<f64> = (0..n).map(|i| i as f64).collect();
            let vals: Vec<f64> = vec![1.0; n];

            let mut op = WelchOp::new(1000, bad_hz, 5.0);
            push(&mut op, &ts, &vals);

            assert!(
                matches!(op.output(), Err(FftStatus::InvalidSampleRate { .. })),
                "bad_hz={bad_hz} did not produce InvalidSampleRate: {:?}",
                op.output()
            );
        }
    }

    #[test]
    fn ring_truncates_to_window_samples() {
        let window_samples = 200;
        let n = window_samples * 3;
        let ts: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64 * 0.1).sin()).collect();

        let mut op = WelchOp::new(window_samples, 100.0, 5.0);
        push(&mut op, &ts, &vals);

        assert_eq!(op.ring.len(), window_samples);
        let data = op.output().as_ref().expect("expected Ok result");
        assert_eq!(data.total_sample_count, window_samples);
    }

    #[test]
    fn chunked_pushes_match_single_batch_push() {
        let window_samples = 4096;
        let n = 3000;
        let sampling_hz = 500.0;
        let (ts, vals) = sine_signal(n, 37.0, sampling_hz);

        let mut batch = WelchOp::new(window_samples, sampling_hz, 5.0);
        push(&mut batch, &ts, &vals);
        let expected = batch
            .output()
            .as_ref()
            .expect("expected Ok result")
            .points
            .clone();

        for chunk_size in [1, 7, 64, 500] {
            let mut streamed = WelchOp::new(window_samples, sampling_hz, 5.0);
            for (tc, vc) in ts.chunks(chunk_size).zip(vals.chunks(chunk_size)) {
                push(&mut streamed, tc, vc);
            }
            let got = &streamed
                .output()
                .as_ref()
                .expect("expected Ok result")
                .points;
            assert_eq!(
                got, &expected,
                "chunk_size={chunk_size} diverged from single-chunk batch"
            );
        }
    }
}

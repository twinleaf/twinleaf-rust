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
    /// Robust white-noise floor: rejects 1/f content and narrow peaks. `None`
    /// when the spectrum is too short or too contaminated to support one.
    pub noise_floor: Option<f64>,
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
        // The signal is real, so its spectrum is Hermitian: the negative
        // frequencies mirror the positive ones rather than carrying separate
        // information, and each bin's in-phase and quadrature parts are already
        // combined by the magnitude-squared. `welch-sde` keeps the first
        // `dft_size / 2` bins without the factor of two that folds the mirror
        // back on, so what it returns is a *two-sided* density. An amplitude
        // density in `units/sqrt(Hz)` is conventionally one-sided, so every bin
        // with a mirror partner carries twice the power. DC would be exempt and
        // is dropped by the `f > 0` filter; Nyquist would be too, but it sits
        // at index `dft_size / 2` and is never among the bins kept.
        //
        // Frequencies are spaced directly rather than read from the crate's
        // `frequency()`, which spreads its `n` bins evenly over `0..=fs/2` so
        // the last lands exactly on Nyquist. Those bins are really the first
        // `dft_size / 2` DFT bins, spaced `fs / dft_size` apart, so the last
        // sits one spacing below Nyquist and the crate's mapping stretches the
        // axis by `n / (n - 1)` — 0.05% for a 4096-point transform, and about
        // 3% for the short ones a brief window produces.
        let bin_spacing = sampling_hz / welch.dft_size as f64;
        let pts: Vec<(f64, f64)> = sd
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(index, d)| {
                let f = index as f64 * bin_spacing;
                if f > 0.0 && d.is_finite() && d > 0.0 {
                    Some((f, (2.0 * d).sqrt()))
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

        let noise_floor = estimate_white_noise_floor(&pts);

        Ok(FftReadyData {
            points: pts,
            noise_floor,
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

/// Estimate the flat, broadband ASD level while rejecting low-frequency 1/f
/// content and narrow spectral peaks.
///
/// Equal-sized frequency bands make the band medians insensitive to narrow
/// peaks. The quiet quartile of all but the lowest bands identifies the white
/// plateau without assuming a fixed corner frequency. Only bands close to
/// that plateau contribute samples, and a final one-sided MAD clip removes
/// any peaks that survived their band's median.
pub fn estimate_white_noise_floor(points: &[(f64, f64)]) -> Option<f64> {
    let log_values: Vec<f64> = points
        .iter()
        .filter(|(f, d)| f.is_finite() && *f > 0.0 && d.is_finite() && *d > 0.0)
        .map(|(_, d)| d.ln())
        .collect();
    if log_values.len() < 16 {
        return None;
    }

    let band_count = (log_values.len() / 16).clamp(8, 32).min(log_values.len());
    let band_size = log_values.len().div_ceil(band_count);
    let bands: Vec<&[f64]> = log_values.chunks(band_size).collect();
    if bands.len() < 4 {
        return None;
    }

    // Never seed the plateau from the lowest 1/8 of the frequency bands.
    let low_band_count = (bands.len() / 8).max(1);
    let mut usable_band_medians: Vec<f64> = bands[low_band_count..]
        .iter()
        .filter_map(|band| median(band))
        .collect();
    if usable_band_medians.len() < 3 {
        return None;
    }
    usable_band_medians.sort_by(f64::total_cmp);

    // A lower-quartile seed is resistant to both 1/f bands and broad peaks,
    // while remaining representative of a noisy white plateau.
    let plateau_seed = quantile_sorted(&usable_band_medians, 0.25)?;
    let lower_half_end = usable_band_medians.len().div_ceil(2);
    let lower_half = &usable_band_medians[..lower_half_end];
    let band_mad = median_absolute_deviation(lower_half, plateau_seed).unwrap_or(0.0);
    let plateau_tolerance = (3.0 * 1.4826 * band_mad).clamp((1.5_f64).ln(), (2.0_f64).ln());

    let mut candidates = Vec::new();
    for band in &bands[low_band_count..] {
        let Some(band_median) = median(band) else {
            continue;
        };
        if band_median <= plateau_seed + plateau_tolerance {
            candidates.extend_from_slice(band);
        }
    }
    if candidates.len() < 8 {
        return None;
    }

    // Iterative, upper-only clipping preserves the center of the broadband
    // distribution while removing spectral lines at any amplitude.
    for _ in 0..4 {
        let Some(center) = median(&candidates) else {
            return None;
        };
        let mad = median_absolute_deviation(&candidates, center).unwrap_or(0.0);
        let upper_limit = center + (3.5 * 1.4826 * mad).max((1.5_f64).ln());
        let previous_len = candidates.len();
        candidates.retain(|value| *value <= upper_limit);
        if candidates.len() == previous_len || candidates.len() < 8 {
            break;
        }
    }

    median(&candidates).map(f64::exp).filter(|value| value.is_finite() && *value > 0.0)
}

fn median(values: &[f64]) -> Option<f64> {
    let mut sorted: Vec<f64> = values.iter().copied().filter(|value| value.is_finite()).collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(f64::total_cmp);
    quantile_sorted(&sorted, 0.5)
}

fn median_absolute_deviation(values: &[f64], center: f64) -> Option<f64> {
    let deviations: Vec<f64> = values.iter().map(|value| (value - center).abs()).collect();
    median(&deviations)
}

fn quantile_sorted(sorted: &[f64], quantile: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let position = quantile.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let fraction = position - lower as f64;
    Some(sorted[lower] + (sorted[upper] - sorted[lower]) * fraction)
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

    /// Box-Muller over a xorshift stream: repeatable normal noise for tests
    /// without pulling in an RNG dependency.
    fn gaussian_noise(seed: u64) -> impl FnMut() -> f64 {
        let mut state = seed;
        move || {
            let mut next = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 11) as f64 / (1u64 << 53) as f64
            };
            let u1: f64 = next().max(1e-12);
            let u2: f64 = next();
            (-2.0 * u1.ln()).sqrt() * (2.0 * PI * u2).cos()
        }
    }

    #[test]
    fn amplitude_density_matches_injected_white_noise() {
        // The displayed spectrum is an amplitude density in units/sqrt(Hz),
        // which is a one-sided quantity. `welch-sde` returns the two-sided
        // form, so without folding it the plot reads a factor of sqrt(2) low
        // against a known noise source.
        let sampling_hz: f64 = 1000.0;
        let target_asd = 0.02_f64;
        let sigma = target_asd * (sampling_hz / 2.0).sqrt();
        let n = 16_384;
        let mut gauss = gaussian_noise(0x2545F4914F6CDD1D);

        let ts: Vec<f64> = (0..n).map(|i| i as f64 / sampling_hz).collect();
        let vals: Vec<f64> = (0..n).map(|_| sigma * gauss()).collect();

        let mut op = WelchOp::new(n, sampling_hz, 10.0);
        push(&mut op, &ts, &vals);

        let data = op.output().as_ref().expect("expected Ok result");
        let broadband: Vec<f64> = data
            .points
            .iter()
            .filter(|(f, d)| *f > 100.0 && *f < 400.0 && d.is_finite())
            .map(|(_, d)| *d)
            .collect();
        assert!(!broadband.is_empty(), "no broadband bins");

        let rms = (broadband.iter().map(|d| d * d).sum::<f64>() / broadband.len() as f64).sqrt();
        let ratio = rms / target_asd;
        assert!(
            (0.95..1.05).contains(&ratio),
            "amplitude density drifted from the injected noise: {ratio}"
        );
    }

    #[test]
    fn noise_floor_matches_injected_level_and_rejects_a_tone() {
        // A strong tone on top of known white noise. The robust estimate must
        // track the broadband level; the plain median, which sees every bin,
        // is the thing it improves on.
        let sampling_hz: f64 = 1000.0;
        let target_asd = 0.02_f64;
        let sigma = target_asd * (sampling_hz / 2.0).sqrt();
        let n = 16_384;
        let mut gauss = gaussian_noise(0x2545F4914F6CDD1D);

        let ts: Vec<f64> = (0..n).map(|i| i as f64 / sampling_hz).collect();
        let vals: Vec<f64> = (0..n)
            .map(|i| {
                let t = i as f64 / sampling_hz;
                (2.0 * PI * 60.0 * t).sin() + sigma * gauss()
            })
            .collect();

        let mut op = WelchOp::new(n, sampling_hz, 10.0);
        push(&mut op, &ts, &vals);
        let data = op.output().as_ref().expect("expected Ok result");

        let floor = data.noise_floor.expect("estimate");
        let ratio = floor / target_asd;
        assert!(
            (0.95..1.05).contains(&ratio),
            "noise floor drifted from the injected level: {ratio}"
        );

        let peak = data
            .points
            .iter()
            .map(|(_, d)| *d)
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(
            floor < peak / 10.0,
            "estimate {floor} was pulled toward the {peak} peak"
        );
    }

    #[test]
    fn spectrum_axis_is_spaced_by_fs_over_dft_size() {
        // Bins are spaced `fs / dft_size` apart starting at DC, so the last
        // sits one spacing below Nyquist. `welch-sde`'s own `frequency()`
        // instead spreads them to land exactly on Nyquist, stretching every
        // frequency by `n / (n - 1)`. DC is filtered out, so the first
        // surviving bin is one spacing up.
        let sampling_hz: f64 = 1000.0;
        let n = 16_384;
        let (ts, vals) = sine_signal(n, 400.0, sampling_hz);

        let mut op = WelchOp::new(n, sampling_hz, 10.0);
        push(&mut op, &ts, &vals);
        let data = op.output().as_ref().expect("expected Ok result");

        // DC is dropped, so the kept bins number dft_size / 2 - 1.
        let dft_size = 2 * (data.points.len() + 1);
        let expected_spacing = sampling_hz / dft_size as f64;

        let spacing = data.points[1].0 - data.points[0].0;
        assert!(
            (spacing - expected_spacing).abs() < 1e-9,
            "bin spacing {spacing}, expected {expected_spacing}"
        );
        assert!(
            (data.points[0].0 - expected_spacing).abs() < 1e-9,
            "first kept bin {} should be one spacing above DC",
            data.points[0].0
        );

        let last = data.points[data.points.len() - 1].0;
        let expected_last = sampling_hz / 2.0 - expected_spacing;
        assert!(
            (last - expected_last).abs() < 1e-9,
            "last bin {last}, expected {expected_last} (one spacing below Nyquist)"
        );
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

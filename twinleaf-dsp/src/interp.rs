//! Band-limited interpolation, the one filter behind `decimate`, `resample`
//! and `align`.
//!
//! All three need a sample that was not taken, at a lower rate or at an
//! instant between two that were. Each is the same sum, a Kaiser-windowed
//! sinc centred on the wanted instant. Because the window is centred, the
//! filter has no phase response at all. It costs latency, half its width,
//! and nothing else. That is what lets one arm of a gradiometer be decimated
//! and the other not without the two drifting apart in time.

use std::f64::consts::PI;

/// How far into its band a filter stays flat, as a fraction of the band's
/// edge. The rest is the transition.
pub const PASSBAND: f64 = 0.8;

/// How far down the filter is at the band's edge and beyond, in dB.
pub const STOPBAND_DB: f64 = 90.0;

/// A windowed-sinc low-pass that can be evaluated between samples.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Interpolator {
    /// The middle of the transition band, in cycles per input sample.
    cutoff: f64,
    /// How many input samples the filter reaches on each side.
    half_width: usize,
    /// The Kaiser window's shape.
    beta: f64,
}

impl Interpolator {
    /// Designs for a band that ends at `edge` cycles per input sample, which
    /// is at most one half. The filter is flat to [`PASSBAND`] of it, and
    /// [`STOPBAND_DB`] down from it on.
    pub fn for_band(edge: f64) -> Self {
        let transition = (1.0 - PASSBAND) * edge;
        // Kaiser's estimate of the length a transition width calls for.
        let length = (STOPBAND_DB - 7.95) / (2.285 * 2.0 * PI * transition);
        Self {
            cutoff: edge - transition / 2.0,
            half_width: (length / 2.0).ceil() as usize,
            beta: 0.1102 * (STOPBAND_DB - 8.7),
        }
    }

    /// How many input samples the filter reaches on each side, which is also
    /// its latency.
    pub fn half_width(&self) -> usize {
        self.half_width
    }

    /// The edge of the flat band, in cycles per input sample.
    pub fn passband(&self) -> f64 {
        self.cutoff - (self.stopband() - self.cutoff)
    }

    /// Where the filter is fully down, in cycles per input sample.
    pub fn stopband(&self) -> f64 {
        self.cutoff / (1.0 - (1.0 - PASSBAND) / 2.0)
    }

    /// The weights of the `2 * half_width + 1` input samples centred on
    /// `base`, for the instant `fraction` of a sample after `base`. They sum
    /// to one, so a constant passes through exactly.
    pub fn taps(&self, fraction: f64) -> Vec<f64> {
        let width = self.half_width as f64;
        let scale = 2.0 * self.cutoff;
        let mut taps: Vec<f64> = (-(self.half_width as i64)..=self.half_width as i64)
            .map(|tap| {
                let t = fraction - tap as f64;
                if t.abs() > width {
                    0.0
                } else {
                    scale * sinc(scale * t) * self.window(t / width)
                }
            })
            .collect();
        let sum: f64 = taps.iter().sum();
        for tap in &mut taps {
            *tap /= sum;
        }
        taps
    }

    fn window(&self, x: f64) -> f64 {
        bessel_i0(self.beta * (1.0 - x * x).max(0.0).sqrt()) / bessel_i0(self.beta)
    }
}

fn sinc(x: f64) -> f64 {
    if x == 0.0 {
        1.0
    } else {
        (PI * x).sin() / (PI * x)
    }
}

/// The modified Bessel function of the first kind, order zero, by its series.
fn bessel_i0(x: f64) -> f64 {
    let quarter = x * x / 4.0;
    let (mut sum, mut term, mut k) = (1.0, 1.0, 1.0);
    while term > 1e-17 * sum {
        term *= quarter / (k * k);
        sum += term;
        k += 1.0;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The filter's gain at `frequency` cycles per input sample, for the
    /// instant `fraction` after a sample.
    fn gain(taps: &[f64], frequency: f64) -> f64 {
        let half = (taps.len() / 2) as f64;
        let (mut re, mut im) = (0.0, 0.0);
        for (index, tap) in taps.iter().enumerate() {
            let angle = -2.0 * PI * frequency * (index as f64 - half);
            re += tap * angle.cos();
            im += tap * angle.sin();
        }
        re.hypot(im)
    }

    #[test]
    fn bessel_matches_known_values() {
        assert_eq!(bessel_i0(0.0), 1.0);
        assert!((bessel_i0(1.0) - 1.266_065_877_752_008_4).abs() < 1e-14);
        assert!((bessel_i0(8.0) - 427.564_115_721_804_8).abs() < 1e-10);
    }

    #[test]
    fn the_filter_is_flat_in_its_passband_and_down_in_its_stopband() {
        for edge in [0.5, 0.25, 0.1] {
            let filter = Interpolator::for_band(edge);
            assert!((filter.passband() - PASSBAND * edge).abs() < 1e-12);
            assert!((filter.stopband() - edge).abs() < 1e-12);
            for fraction in [0.0, 0.25, 0.5, 0.9] {
                let taps = filter.taps(fraction);
                assert_eq!(taps.len(), 2 * filter.half_width() + 1);
                assert!((taps.iter().sum::<f64>() - 1.0).abs() < 1e-14);
                for step in 0..=20 {
                    let frequency = filter.passband() * f64::from(step) / 20.0;
                    assert!(
                        (gain(&taps, frequency) - 1.0).abs() < 1e-3,
                        "edge {edge}, fraction {fraction}: gain at {frequency}"
                    );
                }
                // The sampled sum aliases the stopband onto itself, so the
                // bound here is looser than the window's own.
                let floor = 10f64.powf(-(STOPBAND_DB - 12.0) / 20.0);
                for step in 0..=20 {
                    let frequency = edge + (0.5 - edge) * f64::from(step) / 20.0;
                    if edge < 0.5 {
                        assert!(
                            gain(&taps, frequency) < floor,
                            "edge {edge}, fraction {fraction}: gain at {frequency}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn taps_on_a_sample_are_symmetric_and_mirror_between_samples() {
        let filter = Interpolator::for_band(0.25);
        let on = filter.taps(0.0);
        let mirrored: Vec<f64> = on.iter().rev().copied().collect();
        assert_eq!(on, mirrored);

        let early = filter.taps(0.25);
        let late = filter.taps(0.75);
        // A quarter after one sample is a quarter before the next, so the
        // two sets of weights are each other's mirror, one tap along.
        for (a, b) in early[1..].iter().zip(late.iter().rev()) {
            assert!((a - b).abs() < 1e-15);
        }
    }
}

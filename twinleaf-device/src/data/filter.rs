//! The anti-alias low pass a decimated stream's float columns pass through.
//!
//! Two trapezoidal state variable Butterworth sections with Kahan compensated
//! integrators, ported from tl-chibi's `filter/tl_lpf_bw4`: the state variable
//! form reproduces a constant input exactly in single precision at the low
//! normalized corners decimation asks for, where a direct form biquad stalls
//! short of it.

/// Which anti-alias low pass a stream declares. Every stream holds a
/// [`Butterworth`] whichever it names, so the state costs the same RAM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Filter {
    /// Decimated samples are picked as they come.
    None,
    /// The fourth order [`Butterworth`] low pass.
    Butterworth4,
}

/// Corner of the filter, as a fraction of the output Nyquist frequency.
pub const CORNER: f32 = 0.8;

/// Float columns of one stream the filter holds state for; the widest stream
/// any board declares carries six.
pub const MAX_COLUMNS: usize = 8;

/// Cascaded second order sections.
const SECTIONS: usize = 2;

/// Levels of the continued fraction [`tan`] evaluates.
const LEVELS: usize = 10;

/// Section dampings `1/Q = 2 cos(theta)`, for `theta = pi/8` and `3 pi/8`.
const DAMPING: [f64; SECTIONS] = [1.8477590650225735, 0.7653668647301796];

/// The trapezoidal coefficients of one section.
#[derive(Clone, Copy)]
struct Coefficients {
    a1: f32,
    a2: f32,
    a3: f32,
}

/// One section's two integrator states for one channel, and the compensation
/// the second integrator sums with.
#[derive(Clone, Copy)]
struct Integrators {
    ic1: f32,
    ic2: f32,
    c2: f32,
}

/// A fourth order Butterworth low pass over up to [`MAX_COLUMNS`] channels.
pub struct Butterworth {
    coefficients: [Coefficients; SECTIONS],
    state: [[Integrators; SECTIONS]; MAX_COLUMNS],
    /// Whether a channel is settled, which tl-chibi reads off a non-finite
    /// state instead so that a filter at rest here is all zeros.
    primed: [bool; MAX_COLUMNS],
    running: bool,
}

impl Butterworth {
    /// A bypassed filter with no state, which is all zeros.
    pub const fn new() -> Self {
        Self {
            coefficients: [Coefficients {
                a1: 0.0,
                a2: 0.0,
                a3: 0.0,
            }; SECTIONS],
            state: [[Integrators {
                ic1: 0.0,
                ic2: 0.0,
                c2: 0.0,
            }; SECTIONS]; MAX_COLUMNS],
            primed: [false; MAX_COLUMNS],
            running: false,
        }
    }

    /// Whether samples are filtered rather than passed through.
    pub fn running(&self) -> bool {
        self.running
    }

    /// Set the corner, normalized to the sampling rate, leaving every channel
    /// where it was. A corner outside `(0, 0.5)` bypasses the filter.
    pub fn setup(&mut self, cutoff: f32) {
        self.running = (cutoff > 0.0) && (cutoff < 0.5);
        if !self.running {
            return;
        }
        let g = tan(core::f64::consts::PI * f64::from(cutoff));
        self.coefficients = core::array::from_fn(|section| {
            let a1 = 1.0 / (1.0 + g * (g + DAMPING[section]));
            Coefficients {
                a1: a1 as f32,
                a2: (g * a1) as f32,
                a3: (g * g * a1) as f32,
            }
        });
    }

    /// Filter one channel's next value. Channels are independent, and an
    /// unsettled one starts settled at this value; a non-finite value
    /// unsettles the channel again.
    pub fn sample(&mut self, channel: usize, value: f32) -> f32 {
        if !self.running {
            return value;
        }
        if !self.primed[channel] {
            self.state[channel] = [Integrators {
                ic1: 0.0,
                ic2: value,
                c2: 0.0,
            }; SECTIONS];
            self.primed[channel] = value.is_finite();
            return value;
        }
        let out = self.coefficients.iter().zip(&mut self.state[channel]).fold(
            value,
            |v0, (coefficients, state)| {
                let v3 = v0 - state.ic2;
                let v1 = coefficients.a1 * state.ic1 + coefficients.a2 * v3;
                let half = coefficients.a2 * state.ic1 + coefficients.a3 * v3;
                let v2 = state.ic2 + half;
                state.ic1 = 2.0 * v1 - state.ic1;
                let y = 2.0 * half - state.c2;
                let t = state.ic2 + y;
                state.c2 = (t - state.ic2) - y;
                state.ic2 = t;
                v2
            },
        );
        self.primed[channel] = out.is_finite();
        out
    }
}

/// A bypassed filter with no state.
impl Default for Butterworth {
    fn default() -> Self {
        Self::new()
    }
}

/// `tan(x)` for `x` in `(0, pi/2)`, by ten levels of Lambert's continued
/// fraction, which is a relative 1e-15 over `(0, 0.4 pi]` and 5e-9 to the pole.
fn tan(x: f64) -> f64 {
    let square = x * x;
    let denominator = (1..LEVELS)
        .rev()
        .fold((2 * LEVELS - 1) as f64, |below, level| {
            (2 * level - 1) as f64 - square / below
        });
    x / denominator
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::f64::consts::PI;

    /// Magnitude of the bilinear fourth order Butterworth with prewarped
    /// corner, every frequency normalized to the sampling rate.
    fn analytic_gain(f: f64, cutoff: f64) -> f64 {
        let r = (PI * f).tan() / (PI * cutoff).tan();
        1.0 / (1.0 + r.powi(8)).sqrt()
    }

    /// Steady state gain of a tone at `cycles / period`, by quadrature
    /// projection over `periods` periods after `settle` of them.
    fn measure_gain(
        filter: &mut Butterworth,
        cycles: u32,
        period: u32,
        settle: u32,
        periods: u32,
    ) -> f64 {
        let f = f64::from(cycles) / f64::from(period);
        let (mut re, mut im) = (0.0, 0.0);
        for i in 0..(settle + periods) * period {
            let phase = 2.0 * PI * f * f64::from(i);
            let y = f64::from(filter.sample(0, phase.sin() as f32));
            if i >= settle * period {
                re += y * phase.sin();
                im += y * phase.cos();
            }
        }
        let n = f64::from(periods * period);
        2.0 * (re * re + im * im).sqrt() / n
    }

    fn tuned(cutoff: f32) -> Butterworth {
        let mut filter = Butterworth::new();
        filter.setup(cutoff);
        filter
    }

    /// Drive `filter` to a settled state at zero, as tl-chibi's `initval(0.0)`
    /// leaves it.
    fn settled(cutoff: f32) -> Butterworth {
        let mut filter = tuned(cutoff);
        filter.sample(0, 0.0);
        filter
    }

    #[test]
    fn the_response_is_the_analytic_bilinear_butterworth() {
        let cutoff = 0.4 / 16.0;
        for cycles in [1u32, 4, 8, 12, 16, 25, 40, 100] {
            let gain = measure_gain(&mut settled(cutoff), cycles, 400, 40, 20);
            let expected = analytic_gain(f64::from(cycles) / 400.0, f64::from(cutoff));
            assert!(
                (gain - expected).abs() < 2e-5 + 1e-4 * expected,
                "f={}: gain {gain} expected {expected}",
                cycles as f64 / 400.0
            );
        }
    }

    /// The branch quotes a droop of -0.1 dB at a quarter of the output rate,
    /// against -1.9 dB for the two chained single poles it replaced. At
    /// decimation 16 a period of 640 input samples puts that quarter on cycle
    /// 10.
    #[test]
    fn the_passband_droops_by_a_tenth_of_a_decibel() {
        let gain = measure_gain(&mut settled(0.4 / 16.0), 10, 640, 20, 20);
        let droop = 20.0 * gain.log10();
        assert!(
            (-0.11..=-0.09).contains(&droop),
            "droop {droop} dB at a quarter of the output rate"
        );
    }

    /// The branch quotes a worst alias leakage of -28 dB for signals landing
    /// in the lower tenth of the output band, against -12 dB before it: an
    /// input `offset` below a multiple of the output rate folds onto `offset`
    /// when the decimated sample is picked. At decimation 16 a period of 800
    /// input samples puts the output rate on cycle 50.
    #[test]
    fn an_aliasing_input_leaks_by_twenty_eight_decibels() {
        let worst = (1..=3)
            .flat_map(|fold| (0..=5).map(move |offset| 50 * fold - offset))
            .map(|cycles| {
                20.0 * measure_gain(&mut settled(0.4 / 16.0), cycles, 800, 20, 20).log10()
            })
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(worst <= -28.0, "worst alias leakage {worst} dB");
    }

    /// The lowest corner the framework produces, 25.6 kSPS decimated to 10 Hz:
    /// a constant input comes back exactly, which is what the compensated
    /// integrators buy over a direct form biquad.
    #[test]
    fn a_constant_input_survives_the_lowest_corner_exactly() {
        let mut filter = tuned(0.4 / 2560.0);
        filter.sample(0, 0.0);
        let settled = (0..200_000).map(|_| filter.sample(0, 1.0)).last().unwrap();
        assert!(
            (settled - 1.0).abs() < 1e-6,
            "DC after a step: {settled}, error {}",
            settled - 1.0
        );

        let (mut sum, mut sum2, mut n) = (0.0f64, 0.0f64, 0u32);
        for i in 0..400_000 {
            let x = 1.0 + 1e-3 * (2.0 * PI * 0.05 * f64::from(i)).sin() as f32;
            let y = f64::from(filter.sample(0, x)) - 1.0;
            if i >= 200_000 {
                sum += y;
                sum2 += y * y;
                n += 1;
            }
        }
        let mean = sum / f64::from(n);
        let rms = (sum2 / f64::from(n) - mean * mean).sqrt();
        assert!(mean.abs() < 1e-6, "mean DC error {mean}");
        assert!(rms < 1e-6, "roundoff rms {rms} against a 1e-3 input tone");
    }

    #[test]
    fn a_corner_outside_the_band_passes_every_sample_through() {
        let mut filter = tuned(0.0);
        assert!(!filter.running());
        let out: Vec<f32> = (0..10).map(|i| filter.sample(0, i as f32 * 1.5)).collect();
        assert_eq!(out, (0..10).map(|i| i as f32 * 1.5).collect::<Vec<f32>>());

        filter.setup(0.5);
        assert!(!filter.running());
        assert_eq!(filter.sample(0, 3.0), 3.0);
    }

    #[test]
    fn an_unsettled_channel_starts_at_its_first_value() {
        let mut filter = tuned(0.1);
        assert_eq!(filter.sample(0, 5.0), 5.0);
        let settled = (0..100).map(|_| filter.sample(0, 5.0)).last().unwrap();
        assert_eq!(settled, 5.0);

        assert!(filter.sample(0, f32::NAN).is_nan());
        assert_eq!(filter.sample(0, 7.0), 7.0);
        let next = filter.sample(0, 8.0);
        assert!((7.0..8.0).contains(&next), "after re-priming: {next}");
    }

    #[test]
    fn channels_are_independent() {
        let mut multi = tuned(0.05);
        let mut single: Vec<Butterworth> = (0..3).map(|_| tuned(0.05)).collect();
        for i in 0..1000 {
            let values = [
                (0.1 * f64::from(i)).sin() as f32,
                (0.03 * f64::from(i)).cos() as f32,
                (i % 7) as f32,
            ];
            for (channel, value) in values.iter().enumerate() {
                assert_eq!(
                    multi.sample(channel, *value),
                    single[channel].sample(0, *value)
                );
            }
        }
    }

    /// tl-chibi's `setup` recomputes coefficients and leaves the state alone,
    /// so a retune does not restart a continuous input from a transient.
    #[test]
    fn a_setup_leaves_every_channel_where_it_was() {
        let mut filter = tuned(0.1);
        (0..100).for_each(|_| {
            filter.sample(0, 5.0);
        });
        filter.setup(0.2);
        let next = filter.sample(0, 9.0);
        assert!((5.0..6.0).contains(&next), "after a retune: {next}");
    }

    #[test]
    fn the_continued_fraction_is_the_tangent_over_the_corners_a_setup_takes() {
        let (mut worst, mut worst_at) = (0.0f64, 0.0);
        for i in 1..50_000u32 {
            let cutoff = 0.5 * f64::from(i) / 50_000.0;
            let x = PI * cutoff;
            let error = ((tan(x) - x.tan()) / x.tan()).abs();
            assert!(tan(x).is_finite() && tan(x) > 0.0, "tan at {cutoff}");
            if error > worst {
                (worst, worst_at) = (error, cutoff);
            }
            if cutoff <= 0.4 {
                assert!(error < 1e-15, "relative error {error} at {cutoff}");
            }
        }
        assert!(worst < 5e-9, "relative error {worst} at {worst_at}");
    }
}

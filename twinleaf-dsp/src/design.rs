//! Filter design. It makes notches from the RBJ cookbook, and Butterworth
//! low-, high- and band-pass filters, all as cascades of second-order
//! sections.
//!
//! The Butterworth path is the textbook one. The analog prototype's poles
//! are moved to the wanted band, then mapped to the unit circle by the
//! bilinear transform, with the band edges pre-warped so that they land
//! exactly where they were asked for.
//!
//! The phase response is the product here, so the path from a frequency to a
//! coefficient is owned outright rather than inherited from a crate.

use crate::nodes::Section;
use num_complex::Complex64;
use std::f64::consts::PI;

/// Why a filter could not be designed.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum DesignError {
    #[error(
        "a frequency of {frequency} Hz is not between zero and the Nyquist frequency, {nyquist} Hz"
    )]
    Frequency { frequency: f64, nyquist: f64 },
    #[error("a band from {low} Hz to {high} Hz is empty")]
    Band { low: f64, high: f64 },
    #[error("a quality factor must be positive, not {q}")]
    Quality { q: f64 },
    #[error("a filter has at least one pole")]
    Order,
}

fn in_band(frequency: f64, rate: f64) -> Result<(), DesignError> {
    let nyquist = rate / 2.0;
    if frequency > 0.0 && frequency < nyquist {
        Ok(())
    } else {
        Err(DesignError::Frequency { frequency, nyquist })
    }
}

/// A notch at `frequency` whose -3 dB width is `frequency / q`, measured on
/// the pre-warped axis. Unity gain at zero and at the Nyquist frequency.
pub fn notch(frequency: f64, q: f64, rate: f64) -> Result<Section, DesignError> {
    in_band(frequency, rate)?;
    if q.is_nan() || q <= 0.0 {
        return Err(DesignError::Quality { q });
    }
    let w0 = 2.0 * PI * frequency / rate;
    let alpha = w0.sin() / (2.0 * q);
    let a0 = 1.0 + alpha;
    let b1 = -2.0 * w0.cos() / a0;
    Ok(Section {
        b0: 1.0 / a0,
        b1,
        b2: 1.0 / a0,
        a1: b1,
        a2: (1.0 - alpha) / a0,
    })
}

/// Notches at `frequency` and its next harmonics, `harmonics` in all, each
/// with the same quality factor. Every one must lie below Nyquist.
pub fn comb(
    frequency: f64,
    q: f64,
    harmonics: usize,
    rate: f64,
) -> Result<Vec<Section>, DesignError> {
    if harmonics == 0 {
        return Err(DesignError::Order);
    }
    (1..=harmonics)
        .map(|harmonic| notch(frequency * harmonic as f64, q, rate))
        .collect()
}

/// A Butterworth low-pass with `order` poles, 3 dB down at `cutoff`.
pub fn lowpass(order: usize, cutoff: f64, rate: f64) -> Result<Vec<Section>, DesignError> {
    in_band(cutoff, rate)?;
    let w = warp(cutoff, rate);
    let poles = prototype(order)?.iter().map(|p| p * w).collect();
    Ok(sections(Band::Low, poles, w.powi(order as i32), rate))
}

/// A Butterworth high-pass with `order` poles, 3 dB down at `cutoff`.
pub fn highpass(order: usize, cutoff: f64, rate: f64) -> Result<Vec<Section>, DesignError> {
    in_band(cutoff, rate)?;
    let w = warp(cutoff, rate);
    let poles = prototype(order)?.iter().map(|p| w / p).collect();
    Ok(sections(Band::High, poles, 1.0, rate))
}

/// A Butterworth band-pass, 3 dB down at `low` and at `high`. Each of the
/// prototype's `order` poles becomes two, so the filter has `2 * order`.
pub fn bandpass(order: usize, low: f64, high: f64, rate: f64) -> Result<Vec<Section>, DesignError> {
    in_band(low, rate)?;
    in_band(high, rate)?;
    if low >= high {
        return Err(DesignError::Band { low, high });
    }
    let (w_low, w_high) = (warp(low, rate), warp(high, rate));
    let width = w_high - w_low;
    let center_squared = w_low * w_high;
    let mut poles = Vec::with_capacity(2 * order);
    for p in prototype(order)? {
        let half = p * (width / 2.0);
        let offset = (half * half - center_squared).sqrt();
        poles.push(half + offset);
        poles.push(half - offset);
    }
    Ok(sections(Band::Pass, poles, width.powi(order as i32), rate))
}

/// The response of a cascade at `frequency`, which is its gain and phase as
/// a complex number.
pub fn response(sections: &[Section], frequency: f64, rate: f64) -> Complex64 {
    let z1 = Complex64::from_polar(1.0, -2.0 * PI * frequency / rate);
    let z2 = z1 * z1;
    sections
        .iter()
        .map(|s| (s.b0 + s.b1 * z1 + s.b2 * z2) / (1.0 + s.a1 * z1 + s.a2 * z2))
        .product()
}

#[derive(Clone, Copy, PartialEq)]
enum Band {
    Low,
    High,
    Pass,
}

/// The analog frequency that the bilinear transform maps onto `frequency`.
fn warp(frequency: f64, rate: f64) -> f64 {
    2.0 * rate * (PI * frequency / rate).tan()
}

/// The poles of the analog Butterworth low-pass with unit cutoff. They are
/// evenly spaced on the left half of the unit circle.
fn prototype(order: usize) -> Result<Vec<Complex64>, DesignError> {
    if order == 0 {
        return Err(DesignError::Order);
    }
    Ok((0..order)
        .map(|k| {
            let angle = PI * (2 * k + order + 1) as f64 / (2 * order) as f64;
            let pole = Complex64::from_polar(1.0, angle);
            // An odd order has one pole on the real axis, at -1, which the
            // sine leaves a rounding error above it.
            if pole.im.abs() < 1e-12 {
                Complex64::new(pole.re, 0.0)
            } else {
                pole
            }
        })
        .collect())
}

/// Maps analog poles to digital ones and groups them into sections.
///
/// The zeros need no bookkeeping. A low-pass has all of them at infinity,
/// which the bilinear transform puts at -1. A high-pass has them at zero,
/// which it puts at +1. A band-pass has half at each.
fn sections(band: Band, analog: Vec<Complex64>, analog_gain: f64, rate: f64) -> Vec<Section> {
    let k = 2.0 * rate;
    let digital: Vec<Complex64> = analog.iter().map(|p| (k + p) / (k - p)).collect();
    let denominator: Complex64 = analog.iter().map(|p| k - p).product();
    let finite_zeros = match band {
        Band::Low => 0,
        Band::High => analog.len(),
        Band::Pass => analog.len() / 2,
    };
    let gain = analog_gain * (k.powi(finite_zeros as i32) / denominator).re;

    // One section per conjugate pair, then real poles two at a time.
    let scale = digital.iter().map(|p| p.norm()).fold(1.0, f64::max);
    let is_real = |p: &Complex64| p.im.abs() <= 1e-10 * scale;
    let mut denominators: Vec<(f64, f64, usize)> = digital
        .iter()
        .filter(|p| !is_real(p) && p.im > 0.0)
        .map(|p| (-2.0 * p.re, p.norm_sqr(), 2))
        .collect();
    let mut real: Vec<f64> = digital
        .iter()
        .filter(|p| is_real(p))
        .map(|p| p.re)
        .collect();
    real.sort_by(f64::total_cmp);
    for pair in real.chunks(2) {
        denominators.push(match pair {
            [p, q] => (-(p + q), p * q, 2),
            [p] => (-p, 0.0, 1),
            _ => unreachable!("chunks of two"),
        });
    }

    let mut out: Vec<Section> = denominators
        .into_iter()
        .map(|(a1, a2, poles)| {
            let (b0, b1, b2) = match (band, poles) {
                (Band::Low, 2) => (1.0, 2.0, 1.0),
                (Band::Low, _) => (1.0, 1.0, 0.0),
                (Band::High, 2) => (1.0, -2.0, 1.0),
                (Band::High, _) => (1.0, -1.0, 0.0),
                // A band-pass always has an even number of poles, and each
                // pair takes one zero from each end of the band.
                (Band::Pass, _) => (1.0, 0.0, -1.0),
            };
            Section { b0, b1, b2, a1, a2 }
        })
        .collect();
    if let Some(first) = out.first_mut() {
        first.b0 *= gain;
        first.b1 *= gain;
        first.b2 *= gain;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gain(sections: &[Section], frequency: f64, rate: f64) -> f64 {
        response(sections, frequency, rate).norm()
    }

    #[test]
    fn a_notch_is_exact_at_its_frequency_and_as_wide_as_its_q_says() {
        let (f0, q, rate) = (60.0, 30.0, 1000.0);
        let section = [notch(f0, q, rate).unwrap()];
        assert!(gain(&section, f0, rate) < 1e-12);
        assert!((gain(&section, 0.0, rate) - 1.0).abs() < 1e-12);
        assert!((gain(&section, rate / 2.0, rate) - 1.0).abs() < 1e-12);

        // Find the two -3 dB points by bisection on either side.
        let edge = |mut inside: f64, mut outside: f64| {
            for _ in 0..80 {
                let middle = (inside + outside) / 2.0;
                if gain(&section, middle, rate) < std::f64::consts::FRAC_1_SQRT_2 {
                    inside = middle;
                } else {
                    outside = middle;
                }
            }
            inside
        };
        let width = edge(f0, f0 + 10.0) - edge(f0, f0 - 10.0);
        // The width is exact on the warped axis, and within a few percent
        // here, a decade below Nyquist.
        assert!((width - f0 / q).abs() < 0.03 * f0 / q, "width {width}");
    }

    #[test]
    fn a_comb_notches_each_harmonic() {
        let rate = 1000.0;
        let sections = comb(60.0, 30.0, 3, rate).unwrap();
        assert_eq!(sections.len(), 3);
        for harmonic in [60.0, 120.0, 180.0] {
            assert!(gain(&sections, harmonic, rate) < 1e-12);
        }
        assert!(gain(&sections, 240.0, rate) > 0.9);
        assert!(comb(200.0, 30.0, 3, rate).is_err());
    }

    #[test]
    fn butterworth_filters_are_3_db_down_at_their_edges() {
        let rate = 500.0;
        let half_power = std::f64::consts::FRAC_1_SQRT_2;
        for order in 1..=9 {
            let low = lowpass(order, 40.0, rate).unwrap();
            assert_eq!(low.len(), order.div_ceil(2));
            assert!((gain(&low, 0.0, rate) - 1.0).abs() < 1e-12);
            assert!((gain(&low, 40.0, rate) - half_power).abs() < 1e-12);
            assert!(gain(&low, rate / 2.0, rate) < 1e-12);

            let high = highpass(order, 40.0, rate).unwrap();
            assert!(gain(&high, 0.0, rate) < 1e-12);
            assert!((gain(&high, 40.0, rate) - half_power).abs() < 1e-12);
            assert!((gain(&high, rate / 2.0, rate) - 1.0).abs() < 1e-12);

            let band = bandpass(order, 1.0, 40.0, rate).unwrap();
            assert_eq!(band.len(), order);
            assert!(gain(&band, 0.0, rate) < 1e-12);
            assert!((gain(&band, 1.0, rate) - half_power).abs() < 1e-9);
            assert!((gain(&band, 40.0, rate) - half_power).abs() < 1e-9);
            assert!(gain(&band, rate / 2.0, rate) < 1e-12);
        }
    }

    #[test]
    fn every_pole_is_inside_the_unit_circle() {
        let designs = [
            lowpass(8, 0.5, 1000.0).unwrap(),
            highpass(7, 0.5, 1000.0).unwrap(),
            bandpass(6, 0.1, 10.0, 500.0).unwrap(),
            bandpass(4, 45.0, 55.0, 1000.0).unwrap(),
        ];
        for sections in designs {
            for s in sections {
                // The stability triangle of a second-order denominator.
                assert!(s.a2.abs() < 1.0, "{s:?}");
                assert!(s.a1.abs() < 1.0 + s.a2, "{s:?}");
            }
        }
    }

    #[test]
    fn what_cannot_be_designed_is_refused() {
        assert_eq!(lowpass(0, 10.0, 100.0), Err(DesignError::Order));
        assert!(matches!(
            lowpass(2, 50.0, 100.0),
            Err(DesignError::Frequency { nyquist, .. }) if nyquist == 50.0
        ));
        assert!(matches!(
            highpass(2, 0.0, 100.0),
            Err(DesignError::Frequency { .. })
        ));
        assert_eq!(
            bandpass(2, 20.0, 10.0, 100.0),
            Err(DesignError::Band {
                low: 20.0,
                high: 10.0
            })
        );
        assert_eq!(
            notch(10.0, 0.0, 100.0),
            Err(DesignError::Quality { q: 0.0 })
        );
    }
}

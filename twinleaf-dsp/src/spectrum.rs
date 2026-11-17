//! Spectra, by the twinleaf library's own Welch estimate, the one the app's
//! spectrum plots are drawn from. `psd` computes nothing of its own. It
//! hands a run's samples to that estimate and squares what comes back, so
//! that a spectrum from a graph and a spectrum on a plot cannot differ.

use twinleaf::data::ops::Welch;
use twinleaf::data::{ColumnArray, ColumnOp};

/// The amplitude spectral density of `samples`, taken at `rate`. It gives
/// frequency and density for each bin, rising from one bin above zero. Empty
/// if the estimate has nothing to give for so few samples.
pub fn density(samples: &[f64], rate: f64) -> Vec<(f64, f64)> {
    let mut welch = Welch::new(samples.len(), rate);
    let times: Vec<f64> = (0..samples.len()).map(|n| n as f64 / rate).collect();
    welch.update_batch(&times, &ColumnArray::F64(samples.to_vec().into()));
    match welch.output() {
        Ok(spectrum) => spectrum.points.clone(),
        Err(_) => Vec::new(),
    }
}

/// How the spectrum of `samples` samples at `rate` is laid out. That is how
/// many bins it has, and how far apart they are in hertz. The first bin is
/// one spacing above zero. `None` if that many samples give no spectrum.
///
/// The layout is the estimate's to decide, so it is asked of the estimate,
/// with a signal that leaves no bin empty, and not worked out here in a
/// copy of its arithmetic.
pub fn shape(samples: usize, rate: f64) -> Option<(usize, f64)> {
    let probe: Vec<f64> = (0..samples as u64)
        .map(|n| ((n * 2_654_435_761 + 7919) % 1_000_003) as f64 / 1_000_003.0 - 0.5)
        .collect();
    let points = density(&probe, rate);
    let spacing = points.first()?.0;
    let last = points.last()?.0;
    // Evenly spaced from the first, with none left out.
    let even = (last / spacing).round() as usize == points.len();
    (even && spacing > 0.0).then_some((points.len(), spacing))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_layout_is_the_estimates_own() {
        // A thousand samples at 1 kHz: four segments of 400, half
        // overlapped, each padded to 512 points. Bins 1 to 255.
        let (bins, spacing) = shape(1000, 1000.0).unwrap();
        assert_eq!(bins, 255);
        assert!((spacing - 1000.0 / 512.0).abs() < 1e-12);
        // Too few samples for the estimate to say anything.
        assert_eq!(shape(10, 1000.0), None);
    }
}

//! The crate's Butterworth designs against scipy's, by impulse response.
//!
//! `tests/fixtures/butterworth.txt` is written by the script beside it. The
//! two designs group poles into sections differently, so coefficients are not
//! compared; what a filter does to an impulse is.

use twinleaf_dsp::design::{bandpass, highpass, lowpass};
use twinleaf_dsp::nodes::Section;

/// Runs an impulse through a cascade, one section after another, in the same
/// transposed direct form II the engine uses.
fn impulse_response(sections: &[Section], samples: usize) -> Vec<f64> {
    let mut signal = vec![0.0; samples];
    signal[0] = 1.0;
    for s in sections {
        let (mut s1, mut s2) = (0.0, 0.0);
        for sample in &mut signal {
            let x = *sample;
            let y = s.b0 * x + s1;
            s1 = s.b1 * x - s.a1 * y + s2;
            s2 = s.b2 * x - s.a2 * y;
            *sample = y;
        }
    }
    signal
}

#[test]
fn butterworth_designs_match_scipy() {
    let fixtures = include_str!("fixtures/butterworth.txt");
    let mut cases = 0;
    for line in fixtures.lines().filter(|line| !line.starts_with('#')) {
        let (head, samples) = line.split_once(" : ").unwrap();
        let expected: Vec<f64> = samples.split(' ').map(|x| x.parse().unwrap()).collect();
        let [kind, order, low, high, rate] = head.split(' ').collect::<Vec<_>>()[..] else {
            panic!("malformed fixture line: {head}");
        };
        let order: usize = order.parse().unwrap();
        let (low, high, rate): (f64, f64, f64) = (
            low.parse().unwrap(),
            high.parse().unwrap(),
            rate.parse().unwrap(),
        );
        let sections = match kind {
            "lowpass" => lowpass(order, low, rate),
            "highpass" => highpass(order, low, rate),
            "bandpass" => bandpass(order, low, high, rate),
            other => panic!("unknown kind {other}"),
        }
        .unwrap();

        let got = impulse_response(&sections, expected.len());
        let scale = expected.iter().fold(0.0_f64, |m, x| m.max(x.abs()));
        for (index, (got, want)) in got.iter().zip(&expected).enumerate() {
            assert!(
                (got - want).abs() <= 1e-9 * scale,
                "{head}: sample {index} is {got}, and scipy has {want}"
            );
        }
        cases += 1;
    }
    assert_eq!(cases, 32);
}

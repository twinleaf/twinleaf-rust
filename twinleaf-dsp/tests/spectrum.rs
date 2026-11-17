//! `psd`: the spectrum of a run, held to what a spectrum must be. The
//! numbers come from the twinleaf library's Welch estimate, so what is
//! checked here is what this crate adds: that the estimate is given the
//! right samples, that what comes back is laid out and scaled as a power
//! spectral density, and that it is said to be one.

use std::f64::consts::PI;
use std::path::Path;
use twinleaf_dsp::lang;
use twinleaf_dsp::run::{run_log, OutputTrace, RunOptions};
use twinleaf_dsp::session::Session;
use twinleaf_dsp::Options;

mod common;
use common::*;

const SPEC: &str = "\
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)

out spectrum = a | psd(4 s)
";

/// Uniform noise of unit width, each call's its own.
struct Random(u64);

impl Random {
    fn next(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        (z >> 11) as f64 / (1u64 << 53) as f64 - 0.5
    }
}

/// White noise on x, a tone in a little noise on y, and nothing but an
/// offset on z.
fn rows(samples: usize) -> Vec<[f64; 3]> {
    let (mut white, mut faint) = (Random(1), Random(2));
    (0..samples)
        .map(|n| {
            let t = n as f64 / 1000.0;
            [
                6.0 * white.next(),
                4.0 * (2.0 * PI * 125.0 * t).sin() + 0.02 * faint.next(),
                7.5,
            ]
        })
        .collect()
}

fn run(log: &Path, options: &RunOptions) -> OutputTrace {
    let analysis = lang::analyze(SPEC);
    assert!(
        analysis.diagnostics.is_empty(),
        "{:?}",
        analysis.diagnostics
    );
    let mut recording = run_log(&analysis.program, log, options).unwrap();
    recording.outputs.remove(0)
}

#[test]
fn a_spectrum_is_laid_out_and_scaled_as_a_power_spectral_density() {
    let log = record("spectrum", &[Sensor::new("/0", FAST, rows(8000))]);
    let spectrum = run(&log, &RunOptions::default());

    // It says what it is: bins, not samples, in the unit squared per
    // hertz, the first bin one spacing above zero.
    assert!(spectrum.spectrum);
    let spacing = 1.0 / spectrum.rate;
    assert!((spectrum.origin - spacing).abs() < 1e-12);
    // Four seconds at 1 kHz: four half-overlapped segments of 1600
    // samples, each padded to 2048 points.
    assert!((spacing - 1000.0 / 2048.0).abs() < 1e-12);
    let units: Vec<&str> = spectrum.channels.iter().map(|c| c.unit.symbol()).collect();
    assert_eq!(units, ["nT^2/Hz"; 3]);
    let [only] = spectrum.runs.as_slice() else {
        panic!("one run gives one spectrum");
    };
    assert_eq!((only.first, only.rows[0].len()), (0, 1023));

    // White noise of variance σ² at 1 kHz has σ² / 500 Hz in every bin,
    // and all of σ² under the curve.
    let variance = 6.0 * 6.0 / 12.0;
    let white = &only.rows[0];
    let area: f64 = white.iter().sum::<f64>() * spacing;
    assert!(
        (area / variance - 1.0).abs() < 0.08,
        "{area} under the curve"
    );
    let level = white.iter().sum::<f64>() / white.len() as f64;
    assert!(
        (level / (variance / 500.0) - 1.0).abs() < 0.08,
        "a level of {level}"
    );

    // A tone of amplitude A has A² / 2 of power, all of it at its
    // frequency, give or take the window's width.
    let tone = &only.rows[1];
    let peak = (0..tone.len())
        .max_by(|&a, &b| tone[a].total_cmp(&tone[b]))
        .unwrap();
    let frequency = spectrum.origin + peak as f64 * spacing;
    assert!(
        (frequency - 125.0).abs() <= spacing,
        "the peak is at {frequency} Hz"
    );
    let near: f64 = tone[peak - 6..=peak + 6].iter().sum::<f64>() * spacing;
    assert!(
        (near / (4.0 * 4.0 / 2.0) - 1.0).abs() < 0.05,
        "{near} in the peak"
    );
    // And next to nothing anywhere else.
    let elsewhere: f64 = tone.iter().sum::<f64>() * spacing - near;
    assert!(elsewhere < 1e-3, "{elsewhere} outside the peak");

    // The estimate takes the mean out, so an offset leaves nothing.
    assert!(only.rows[2].iter().all(|&density| density < 1e-20));
}

#[test]
fn a_spectrum_is_written_out_over_frequency() {
    let log = record("spectrum-csv", &[Sensor::new("/0", FAST, rows(4000))]);
    let spectrum = run(&log, &RunOptions::default());
    let mut csv = Vec::new();
    spectrum.write_csv(&mut csv).unwrap();
    let csv = String::from_utf8(csv).unwrap();
    let mut lines = csv.lines();
    assert_eq!(lines.next(), Some("bin,frequency,x,y,z"));
    let first: Vec<&str> = lines.next().unwrap().split(',').collect();
    assert_eq!(first[0], "0");
    assert_eq!(first[1].parse::<f64>().unwrap(), 1000.0 / 2048.0);
    assert_eq!(csv.lines().count(), 1 + 1023);
}

#[test]
fn how_the_run_arrives_changes_nothing_and_a_short_run_gives_none() {
    let log = record("spectrum-cut", &[Sensor::new("/0", FAST, rows(6000))]);
    let whole = run(&log, &RunOptions::default());
    for (block_rows, threads) in [(1, 1), (333, 3)] {
        let options = RunOptions {
            graph: Options {
                threads,
                ..Options::default()
            },
            block_rows,
            ..RunOptions::default()
        };
        assert!(run(&log, &options) == whole, "blocks of {block_rows}");
    }

    // Three seconds is less than the window asks for. There is no
    // spectrum of it, rather than one of something else.
    let log = record("spectrum-short", &[Sensor::new("/0", FAST, rows(3000))]);
    assert!(run(&log, &RunOptions::default()).runs.is_empty());

    // A gap makes two runs, and each long enough gets its own spectrum,
    // of its own last four seconds.
    let log = record(
        "spectrum-gap",
        &[Sensor {
            dropped: &[250],
            ..Sensor::new("/0", FAST, rows(10_000))
        }],
    );
    let two = run(&log, &RunOptions::default());
    assert_eq!(two.runs.len(), 2);
    assert!(two.runs[0] != two.runs[1]);
}

#[test]
fn a_live_graph_refuses_a_spectrum() {
    let Err(diagnostics) = Session::open(SPEC.to_owned(), RunOptions::default(), "the device")
    else {
        panic!("a live session took a spec with a spectrum in it");
    };
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, Some("E0502"));
    assert_eq!(
        diagnostics[0].message,
        "`psd` needs a whole run, and a live graph never has one"
    );
}

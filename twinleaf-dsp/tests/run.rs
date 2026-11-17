//! Running the example spec over a recording, end to end.
//!
//! The recordings are synthesized here, packet by packet, as two sensors on
//! one time base would write them. What the spec should make of the first one
//! was worked out independently, in `tests/fixtures/gradiometer.py`, and is
//! never taken from this crate's own output.

use std::fs;
use std::path::{Path, PathBuf};
use twinleaf_dsp::lang;
use twinleaf_dsp::run::{run_log, Recording, RunError, RunOptions};
use twinleaf_dsp::Options;

mod common;
use common::*;

fn example() -> (PathBuf, String) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/gradiometer.tsl");
    let source = fs::read_to_string(&path).unwrap();
    (path, source)
}

/// Runs the example spec, or `spec` in its place, over a recording.
fn run(spec: Option<&str>, log: &Path, options: &RunOptions) -> Result<Recording, RunError> {
    let source = spec.map_or_else(|| example().1, str::to_owned);
    let analysis = lang::analyze(&source);
    assert!(!analysis.has_errors(), "{:?}", analysis.diagnostics);
    run_log(&analysis.program, log, options)
}

#[test]
fn the_gradiometer_matches_an_independent_calculation() {
    let log = record(
        "golden",
        &[
            Sensor::new("/0", FAST, first_sensor(2400, 1.0, 0.01)),
            Sensor::new("/1", SLOW, second_sensor(1200, 0.01)),
        ],
    );
    let recording = run(None, &log, &RunOptions::default()).unwrap();
    let [gradient] = recording.outputs.as_slice() else {
        panic!("one output");
    };
    assert_eq!((gradient.name.as_str(), gradient.rate), ("gradient", 500.0));
    assert_eq!(gradient.origin, 1_700_000_000.0);
    let names: Vec<&str> = gradient.channels.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["x", "y", "z"]);
    assert!(gradient.channels.iter().all(|c| c.unit.symbol() == "nT"));
    let [run] = gradient.runs.as_slice() else {
        panic!("one unbroken stretch");
    };

    let mut lines = include_str!("fixtures/gradiometer.txt")
        .lines()
        .filter(|line| !line.starts_with('#'));
    let first: u64 = lines
        .next()
        .and_then(|line| line.strip_prefix("first "))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(run.first, first);
    for (row, line) in run.rows.iter().zip(lines) {
        let (name, values) = line.split_once(' ').unwrap();
        let expected: Vec<f64> = values.split(' ').map(|v| v.parse().unwrap()).collect();
        assert_eq!(row.len(), expected.len(), "channel {name}");
        // The gradient is what is left when two 50 nT signals cancel, so
        // the tolerance is set against those and not against what is left.
        for (index, (got, want)) in row.iter().zip(&expected).enumerate() {
            assert!(
                (got - want).abs() <= 1e-11 * 50.0,
                "channel {name}, sample {index}: {got} is not {want}"
            );
        }
    }
}

#[test]
fn output_is_bit_identical_across_block_size_and_core_count() {
    // Both sensors lose packets, so the output comes in several stretches.
    let log = record(
        "gappy",
        &[
            Sensor {
                dropped: &[120, 121],
                ..Sensor::new("/0", FAST, first_sensor(4000, 1.0, 0.01))
            },
            Sensor {
                dropped: &[30],
                ..Sensor::new("/1", SLOW, second_sensor(2000, 0.01))
            },
        ],
    );
    let with = |threads, block_rows| RunOptions {
        graph: Options {
            threads,
            bundle: 2,
            inline_below: 0,
        },
        block_rows,
        ..RunOptions::default()
    };
    let reference = run(None, &log, &with(1, 1)).unwrap();
    let gradient = &reference.outputs[0];
    assert_eq!(gradient.runs.len(), 3, "a stretch on each side of two gaps");
    assert!(gradient.runs.iter().all(|run| run.rows[0].len() > 100));

    let bits = |recording: &Recording| -> Vec<(u64, Vec<Vec<u64>>)> {
        recording.outputs[0]
            .runs
            .iter()
            .map(|run| {
                let rows = run.rows.iter();
                (
                    run.first,
                    rows.map(|r| r.iter().map(|v| v.to_bits()).collect())
                        .collect(),
                )
            })
            .collect()
    };
    for (threads, block_rows) in [(1, 4096), (16, 1), (16, 4096), (5, 37)] {
        let other = run(None, &log, &with(threads, block_rows)).unwrap();
        assert!(
            bits(&other) == bits(&reference),
            "{threads} thread(s) with blocks of {block_rows} disagree with 1 thread and blocks of 1"
        );
    }
}

#[test]
fn the_common_field_is_rejected_and_the_local_one_kept() {
    // No noise: what is left of the common field is what the two arms'
    // different paths through the graph cost.
    let quiet = |local| {
        let log = record(
            if local == 0.0 { "common" } else { "local" },
            &[
                Sensor::new("/0", FAST, first_sensor(6000, local, 0.0)),
                Sensor::new("/1", SLOW, second_sensor(3000, 0.0)),
            ],
        );
        run(None, &log, &RunOptions::default())
            .unwrap()
            .outputs
            .remove(0)
    };
    let peak = |row: &[f64]| row.iter().fold(0.0_f64, |m, v| m.max(v.abs()));

    // The common field is 50 nT at 7 Hz, in band. One arm is decimated and
    // the other is not, and it still cancels to better than 80 dB.
    let rejected = quiet(0.0);
    for (row, share) in rejected.runs[0].rows.iter().zip(COMMON) {
        let left = peak(row) / (50.0 * share.abs());
        assert!(left < 1e-4, "{left} of the common field is left");
    }

    // The 11 Hz source beside the first sensor comes through whole, once
    // the band-pass has settled.
    let kept = quiet(1.0);
    for (row, amplitude) in kept.runs[0].rows.iter().zip(LOCAL) {
        let settled = &row[row.len() - 500..];
        assert!(
            (peak(settled) - amplitude).abs() < 0.02 * amplitude + 1e-3,
            "a {amplitude} nT source reads {}",
            peak(settled)
        );
    }
}

#[test]
fn a_param_can_be_set_for_a_run() {
    let log = record(
        "set",
        &[
            Sensor::new("/0", FAST, first_sensor(3000, 1.0, 0.0)),
            Sensor::new("/1", SLOW, second_sensor(1500, 0.0)),
        ],
    );
    let with = |set: &[(&str, f64)]| RunOptions {
        set: set
            .iter()
            .map(|(name, value)| ((*name).to_owned(), *value))
            .collect(),
        ..RunOptions::default()
    };
    // Narrowing the band to below the 11 Hz source takes most of it out.
    let wide = run(None, &log, &with(&[])).unwrap();
    let narrow = run(None, &log, &with(&[("hi", 10.0), ("lo", 0.5)])).unwrap();
    let tail = |recording: &Recording| {
        let row = &recording.outputs[0].runs[0].rows[0];
        row[row.len() - 300..]
            .iter()
            .fold(0.0_f64, |m, v| m.max(v.abs()))
    };
    assert!(tail(&narrow) < 0.75 * tail(&wide));

    assert!(matches!(
        run(None, &log, &with(&[("hi", 500.0)])),
        Err(RunError::Setting(message)) if message == "`hi` cannot be 500: its range is 10 to 200"
    ));
    assert!(matches!(
        run(None, &log, &with(&[("gain", 2.0)])),
        Err(RunError::Setting(message)) if message == "the spec has no param `gain`"
    ));
}

/// The diagnostics a run stops with, each as its message and the text of the
/// spec it points at.
fn refusal(spec: Option<&str>, log: &Path) -> Vec<(String, String)> {
    let source = spec.map_or_else(|| example().1, str::to_owned);
    match run(spec, log, &RunOptions::default()) {
        Err(RunError::Spec(diagnostics)) => diagnostics
            .into_iter()
            .map(|d| {
                assert!(d.runtime && d.code == Some("E0501"));
                let line = d.span.text(&source).lines().next().unwrap_or("").to_owned();
                (d.message, line)
            })
            .collect(),
        other => panic!(
            "expected the spec to be refused, got {:?}",
            other.map(|_| ())
        ),
    }
}

#[test]
fn a_spec_that_does_not_fit_the_recording_is_told_where() {
    let rows = || first_sensor(400, 1.0, 0.0);
    let slow = || second_sensor(200, 0.0);

    // The second sensor is missing, and the first runs at another rate.
    let log = record("unfit", &[Sensor::new("/0", SLOW, rows())]);
    assert_eq!(
        refusal(None, &log),
        [
            (
                "`a` is declared at 1 kHz, and the recording has `/0/field` at 500 Hz".to_owned(),
                "source a = /0/field  @rate(1000) @channels(x, y, z) @unit(nT)".to_owned()
            ),
            (
                "the recording has no stream `/1/field`".to_owned(),
                "source b = /1/field  @rate(500)  @channels(x, y, z) @unit(nT)".to_owned()
            ),
        ]
    );

    // A column under another name, and a unit that is not the one declared.
    let log = record(
        "columns",
        &[
            Sensor {
                columns: ["x", "y", "w"],
                ..Sensor::new("/0", FAST, rows())
            },
            Sensor {
                units: "V",
                ..Sensor::new("/1", SLOW, slow())
            },
        ],
    );
    let messages: Vec<String> = refusal(None, &log).into_iter().map(|(m, _)| m).collect();
    assert_eq!(
        messages,
        [
            "the recording's `/0/field` has no column `z`",
            "`b` is declared in nT, and the recording has `/1/field/x` in V",
        ]
    );

    // Two sensors that never shared a clock. Nothing about the samples says
    // so; only their time references do, and the error is at the align that
    // assumed they could be compared.
    let log = record(
        "unsynced",
        &[
            Sensor::new("/0", FAST, rows()),
            Sensor {
                clock: ("other hub", 9),
                ..Sensor::new("/1", SLOW, slow())
            },
        ],
    );
    let [(message, place)] = &refusal(None, &log)[..] else {
        panic!("one diagnostic");
    };
    assert!(
        message.starts_with("`align` cannot merge signals that do not share a time base"),
        "{message}"
    );
    assert_eq!(place, "align(a | decimate(2), b)");

    // There was once a function the checker knew and the engine did not
    // have. Every function runs now, and this one over a recording.
    let spec = "\
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)
out steady = a | detrend(mean)
";
    let log = record("unsupported", &[Sensor::new("/0", FAST, rows())]);
    let steady = run(Some(spec), &log, &RunOptions::default()).unwrap();
    assert_eq!(steady.outputs[0].frames(), 400);
}

#[test]
fn what_a_refused_run_prints_is_checked_in() {
    let log = record(
        "rendered",
        &[
            Sensor::new("/0", SLOW, first_sensor(200, 1.0, 0.0)),
            Sensor {
                clock: ("other hub", 9),
                columns: ["x", "y", "w"],
                ..Sensor::new("/1", SLOW, second_sensor(200, 0.0))
            },
        ],
    );
    let (_, source) = example();
    let Err(RunError::Spec(diagnostics)) = run(None, &log, &RunOptions::default()) else {
        panic!("expected the spec to be refused");
    };
    let rendered = lang::diag::render_all(&diagnostics, "gradiometer.tsl", &source);
    let snapshot = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots/recording.stderr");
    if std::env::var_os("DSP_BLESS").is_some() {
        fs::write(&snapshot, &rendered).unwrap();
    }
    assert_eq!(
        fs::read_to_string(&snapshot).ok().as_deref(),
        Some(rendered.as_str()),
        "run with DSP_BLESS=1 to accept"
    );
}

#[cfg(feature = "cli")]
#[test]
fn the_command_runs_a_recording_into_csv() {
    let log = record(
        "command",
        &[
            Sensor::new("/0", FAST, first_sensor(2400, 1.0, 0.01)),
            Sensor::new("/1", SLOW, second_sensor(1200, 0.01)),
        ],
    );
    let csv = Path::new(env!("CARGO_TARGET_TMPDIR")).join("command.csv");
    let (spec, _) = example();
    let (spec, log, csv_arg) = (
        spec.to_str().unwrap(),
        log.to_str().unwrap(),
        csv.to_str().unwrap(),
    );

    let (code, stdout, stderr) = command(&[
        "run",
        spec,
        "--log",
        log,
        "--csv",
        csv_arg,
        "--threads",
        "3",
    ]);
    assert_eq!((code, stderr.as_str()), (0, ""));
    assert!(
        stdout.starts_with("  out    gradient   1142 samples at 500 Hz -> "),
        "{stdout}"
    );

    let written = fs::read_to_string(&csv).unwrap();
    let mut lines = written.lines();
    assert_eq!(lines.next(), Some("sample,time,x,y,z"));
    let first: Vec<&str> = lines.next().unwrap().split(',').collect();
    assert_eq!(&first[..2], ["29", "1700000000.058"]);
    assert!((first[2].parse::<f64>().unwrap() + 0.004_621_726_757).abs() < 1e-9);
    assert_eq!(written.lines().count(), 1 + 1142);

    // The run fails with the spec's own text when the two do not fit.
    let unfit = record(
        "command-unfit",
        &[Sensor::new("/0", SLOW, first_sensor(200, 1.0, 0.0))],
    );
    let (code, _, stderr) = command(&["run", spec, "--log", unfit.to_str().unwrap()]);
    assert_eq!(code, 1);
    assert!(
        stderr.starts_with("error[E0501]: `a` is declared at 1 kHz"),
        "{stderr}"
    );
    assert!(stderr.contains("5 | source a = /0/field"), "{stderr}");

    // A wrong command line is a different failure.
    assert_eq!(command(&["run", spec]).0, 2);
    assert_eq!(command(&["run", spec, "--log", log, "--set", "hi"]).0, 2);
    assert_eq!(command(&["run", spec, "--log", log, "--threads", "0"]).0, 2);
    assert_eq!(command(&["run", spec, "--log", "absent.tio"]).0, 2);
}

/// The amplitude of the wave at `frequency` in samples taken at `rate`:
/// what a lock-in at that frequency would read.
fn amplitude(samples: &[f64], rate: f64, frequency: f64) -> f64 {
    let (mut inphase, mut quadrature) = (0.0, 0.0);
    for (n, sample) in samples.iter().enumerate() {
        let phase = 2.0 * std::f64::consts::PI * frequency * n as f64 / rate;
        inphase += sample * phase.sin();
        quadrature += sample * phase.cos();
    }
    2.0 * inphase.hypot(quadrature) / samples.len() as f64
}

#[test]
fn a_spec_decorrelates_two_sensors_and_keeps_what_only_one_saw() {
    // The plan's physics test. Both sensors see a field of 50 nT at 7 Hz
    // and 20 nT at 60 Hz, each axis by its own share of it. The first also
    // sees a source of its own: 3 nT at 11 Hz on x. The spec is §6's, with
    // the subtraction swapped for a decorrelator, each way it can follow.
    const SPEC: &str = "\
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)
source b = /1/field @rate(500) @channels(x, y, z) @unit(nT)

let fr = align(a | decimate(2), b)
let clean = fr | bandpass(1 Hz, 100 Hz)

out ls : nT = clean.a | decorrelate(ref = clean.b, window = 1 s)
out rls : nT = clean.a | decorrelate(ref = clean.b, window = 1 s, method = rls, taps = 2)
out nlms : nT = clean.a | decorrelate(ref = clean.b, window = 1 s, method = nlms, taps = 2)
out plain : nT = clean.a - clean.b
";
    let seconds = 12;
    let log = record(
        "decorrelated",
        &[
            Sensor::new("/0", FAST, first_sensor(1000 * seconds, 1.0, 0.01)),
            Sensor::new("/1", SLOW, second_sensor(500 * seconds, 0.01)),
        ],
    );
    let recording = run(Some(SPEC), &log, &RunOptions::default()).unwrap();
    let names: Vec<&str> = recording.outputs.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(names, ["ls", "rls", "nlms", "plain"]);

    // The last six seconds, by when even the slowest has settled.
    let settled = |output: usize, channel: usize| {
        let rows = &recording.outputs[output].runs[0].rows[channel];
        rows[rows.len() - 3000..].to_vec()
    };
    // Least squares and RLS take the shared field down by more than 40 dB,
    // from 50 nT and from 20 nT. NLMS is on its way and no more: the three
    // axes of the reference say nearly the same thing, and it is slow
    // along every direction in which they barely differ. It is held to
    // 10 dB, which is what it can be counted on for here.
    for (output, down) in [(0, 100.0), (1, 100.0), (2, 10.0_f64.sqrt())] {
        let name = names[output];
        let x = settled(output, 0);
        let (field, mains, own) = (
            amplitude(&x, 500.0, 7.0),
            amplitude(&x, 500.0, 60.0),
            amplitude(&x, 500.0, 11.0),
        );
        assert!(
            field < 50.0 / down && mains < 20.0 / down,
            "{name}: {field:.3} nT at 7 Hz, {mains:.3} at 60"
        );
        // And the sensor's own source is still there.
        assert!((own - 3.0).abs() < 0.3, "{name}: {own:.3} nT at 11 Hz");
    }
    // The y axis saw none of the first sensor's own source, so there is
    // next to nothing left of it at all: half the shared field, taken out.
    let y = settled(0, 1);
    let left = (y.iter().map(|v| v * v).sum::<f64>() / y.len() as f64).sqrt();
    assert!(left < 0.05, "y is left with {left:.4} nT rms");

    // Plain subtraction does as well here only because the two sensors'
    // gains were made the same. It has nothing to follow them with.
    let plain = settled(3, 0);
    assert!(amplitude(&plain, 500.0, 7.0) < 0.5);
}

#[test]
fn a_spec_folded_into_functions_runs_to_the_same_samples() {
    // The example, with its filter chain written once as a function and
    // its subtraction as another. A function is its body written out, so
    // the output is not close to the example's: it is the example's.
    const FOLDED: &str = "\
source a = /0/field  @rate(1000) @channels(x, y, z) @unit(nT)
source b = /1/field  @rate(500)  @channels(x, y, z) @unit(nT)

param line : Hz = 60        [45 .. 65]
param lo   : Hz = 1         [0.1 .. 10]
param hi   : Hz = 40        [10 .. 200]

fn clean(s) = s | notch(line, q = 30) | bandpass(lo, hi, order = 4)
fn difference(pair) = pair.a - pair.b

out gradient : nT = align(a | decimate(2), b) | clean() | difference()
";
    let log = record(
        "folded",
        &[
            Sensor::new("/0", FAST, first_sensor(2400, 1.0, 0.01)),
            Sensor::new("/1", SLOW, second_sensor(1200, 0.01)),
        ],
    );
    let written_out = run(None, &log, &RunOptions::default()).unwrap();
    let folded = run(Some(FOLDED), &log, &RunOptions::default()).unwrap();
    assert_eq!(folded.outputs.len(), 1);
    assert!(folded.outputs[0].runs == written_out.outputs[0].runs);
    assert!(!folded.outputs[0].runs[0].rows[0].is_empty());
}

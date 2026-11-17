//! The functions that need the whole of a run: filters run both ways and
//! `detrend`. Over a recording they match scipy sample for sample
//! (`fixtures/whole.py`); live, they are refused.

use std::fs;
use std::path::Path;
use twinleaf_dsp::lang;
use twinleaf_dsp::run::{run_log, Recording, RunOptions};
use twinleaf_dsp::session::Session;
use twinleaf_dsp::Options;

mod common;
use common::*;

const SPEC: &str = "\
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)

out low : nT = a | lowpass(40 Hz, phase = zero)
out band : nT = a | bandpass(5 Hz, 80 Hz, order = 3, phase = zero)
out notched : nT = a | notch(60 Hz, q = 30, phase = zero)
out level : nT = a | detrend(mean)
out line : nT = a | detrend(linear)
out curve : nT = a | detrend(quadratic)
out both : nT = a | detrend(linear) | lowpass(40 Hz, phase = zero)
";

const NAMES: [&str; 7] = ["low", "band", "notched", "level", "line", "curve", "both"];

fn run(log: &Path, options: &RunOptions) -> Recording {
    let analysis = lang::analyze(SPEC);
    assert!(
        analysis.diagnostics.is_empty(),
        "{:?}",
        analysis.diagnostics
    );
    run_log(&analysis.program, log, options).unwrap()
}

/// What scipy made of the same run, by output and channel.
fn expected(name: &str, channel: usize) -> Vec<f64> {
    let wanted = format!("{name} {} ", ["x", "y", "z"][channel]);
    include_str!("fixtures/whole.txt")
        .lines()
        .find_map(|line| line.strip_prefix(&wanted))
        .unwrap_or_else(|| panic!("no {wanted}in the fixture"))
        .split(' ')
        .map(|value| value.parse().unwrap())
        .collect()
}

#[test]
fn a_whole_run_is_filtered_both_ways_and_detrended_as_scipy_does_it() {
    let log = record(
        "whole",
        &[Sensor::new("/0", FAST, first_sensor(1200, 1.0, 0.01))],
    );
    let recording = run(&log, &RunOptions::default());
    let names: Vec<&str> = recording.outputs.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(names, NAMES);
    for output in &recording.outputs {
        // One run, all of it, from its first sample: nothing is lost at
        // either end to a filter's start-up or to look-ahead.
        let [run] = output.runs.as_slice() else {
            panic!("{} came in {} stretches", output.name, output.runs.len());
        };
        assert_eq!(run.first, 0);
        for (channel, got) in run.rows.iter().enumerate() {
            let want = expected(&output.name, channel);
            assert_eq!(got.len(), want.len(), "{}", output.name);
            let worst = got
                .iter()
                .zip(&want)
                .map(|(g, w)| (g - w).abs())
                .fold(0.0, f64::max);
            // Against signals of 50 nT.
            assert!(
                worst <= 1e-9 * 50.0,
                "{} channel {channel} is off by {worst:e}",
                output.name
            );
        }
    }
}

#[test]
fn how_a_whole_run_arrives_changes_nothing() {
    let log = record(
        "whole-cut",
        &[Sensor::new("/0", FAST, first_sensor(1200, 1.0, 0.01))],
    );
    let whole = run(&log, &RunOptions::default());
    for (block_rows, threads) in [(1, 1), (37, 4), (4096, 16)] {
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
}

#[test]
fn a_gap_ends_one_whole_run_and_begins_another() {
    // Packets of twenty samples, with the sixth left out: two runs, of a
    // hundred samples and of what is left, and each is handled whole.
    let log = record(
        "whole-gap",
        &[Sensor {
            dropped: &[5],
            ..Sensor::new("/0", FAST, first_sensor(400, 1.0, 0.01))
        }],
    );
    let recording = run(&log, &RunOptions::default());
    for output in &recording.outputs {
        let stretches: Vec<(u64, usize)> = output
            .runs
            .iter()
            .map(|run| (run.first, run.rows[0].len()))
            .collect();
        assert_eq!(stretches, [(0, 100), (120, 280)], "{}", output.name);
    }
    // Each stretch has had its own mean taken out, and not the mean of
    // both together.
    let level = &recording.outputs[3];
    for run in &level.runs {
        let mean = run.rows[0].iter().sum::<f64>() / run.rows[0].len() as f64;
        assert!(mean.abs() < 1e-9, "a stretch is left with a mean of {mean}");
    }
}

#[test]
fn a_live_graph_refuses_what_needs_a_whole_run() {
    let Err(diagnostics) = Session::open(SPEC.to_owned(), RunOptions::default(), "the device")
    else {
        panic!("a live session took a spec that needs whole runs");
    };
    // One refusal for each call that would wait for ever, each at its
    // call, and `both` has two.
    assert_eq!(diagnostics.len(), 8);
    assert!(diagnostics.iter().all(|d| d.code == Some("E0502")));
    let rendered = lang::diag::render_all(&diagnostics[..1], "dsp.spec", SPEC)
        + "\n"
        + &lang::diag::render_all(&diagnostics[3..4], "dsp.spec", SPEC);
    let snapshot = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots/live.stderr");
    if std::env::var_os("DSP_BLESS").is_some() {
        fs::write(&snapshot, &rendered).unwrap();
    }
    assert_eq!(
        fs::read_to_string(&snapshot).ok().as_deref(),
        Some(rendered.as_str()),
        "run with DSP_BLESS=1 to accept"
    );

    // The same filters running forwards only are taken, and a session
    // that is running one refuses to be changed into one that cannot.
    let causal = SPEC
        .lines()
        .filter(|line| !line.contains("detrend"))
        .collect::<Vec<_>>()
        .join("\n")
        .replace(", phase = zero", "");
    let mut session = Session::open(causal.clone(), RunOptions::default(), "the device").unwrap();
    let refused = session.replace(SPEC.to_owned()).unwrap_err();
    assert!(refused.iter().all(|d| d.code == Some("E0502")));
    assert_eq!(session.source(), causal);
}

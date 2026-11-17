//! The functions of the node library that keep state from sample to sample:
//! each against what numpy makes of the same signals from its definition
//! (`fixtures/functions.py`), and each the same however the samples are cut
//! into blocks.

use std::f64::consts::PI;
use std::sync::Arc;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;
use twinleaf_dsp::nodes::{Decorrelate, Derivative, Integrate, Method, Rms};
use twinleaf_dsp::{Block, ChannelSet, Graph, GraphBuilder, Options, RunId, SignalType, Unit};

const RATE: f64 = 200.0;
const SAMPLES: usize = 600;

fn noise(seed: u64, channel: u64, n: u64) -> f64 {
    ((n * 2_654_435_761 + channel * 40_503 + seed * 7919) % 1_000_003) as f64 / 1_000_003.0 - 0.5
}

/// What both signals share: a wave and some noise, one for each channel.
fn common(channel: u64, n: u64) -> f64 {
    let t = n as f64 / RATE;
    let wave = if channel == 0 {
        (2.0 * PI * 3.0 * t).sin()
    } else {
        (2.0 * PI * 5.0 * t).cos()
    };
    wave + 0.5 * noise(3, channel, n)
}

fn reference(channel: u64, n: u64) -> f64 {
    common(channel, n) + 0.02 * noise(4, channel, n)
}

/// The first channel is a fixed mix of what is shared, with a wave of its
/// own. The second has a gain on it that drifts.
fn signal(channel: u64, n: u64) -> f64 {
    let t = n as f64 / RATE;
    if channel == 0 {
        let local = 0.3 * (2.0 * PI * 11.0 * t).sin();
        2.0 * common(0, n) - 0.7 * common(1, n) + local + 0.01 * noise(5, 0, n)
    } else {
        (1.0 + 0.5 * t) * common(0, n) + 0.1 * noise(5, 1, n)
    }
}

const OUTPUTS: [&str; 7] = ["integrate", "derivative", "rms", "ls", "ls3", "nlms", "rls"];

/// Every function under test over the same two sources.
fn graph() -> (Graph, [SignalType; 2]) {
    let mut builder = GraphBuilder::new();
    let domain = builder
        .domain(Epoch::UNIX, SessionId::new(1), RATE, 0.0)
        .unwrap();
    let ty = |names: [&str; 2]| SignalType {
        domain,
        channels: ChannelSet::uniform(names, &Unit::new("nT")).unwrap(),
    };
    let types = [ty(["x", "y"]), ty(["u", "v"])];
    let a = builder.source("a", types[0].clone()).unwrap();
    let r = builder.source("r", types[1].clone()).unwrap();
    let fit = |method, taps| Decorrelate {
        method,
        window: 0.5,
        taps,
    };
    let nodes = [
        builder.node(Integrate, &[a]),
        builder.node(Derivative, &[a]),
        builder.node(Rms { window: 0.25 }, &[a]),
        builder.node(fit(Method::LeastSquares, 1), &[a, r]),
        builder.node(fit(Method::LeastSquares, 3), &[a, r]),
        builder.node(fit(Method::Nlms, 4), &[a, r]),
        builder.node(fit(Method::Rls, 2), &[a, r]),
    ];
    for (name, node) in OUTPUTS.iter().zip(nodes) {
        builder.output(name, node.unwrap()).unwrap();
    }
    let options = Options {
        threads: 1,
        ..Options::default()
    };
    (builder.build(options).unwrap(), types)
}

/// One output's samples: where they begin, and each channel's.
#[derive(Debug, Clone, PartialEq, Default)]
struct Output {
    first: Option<u64>,
    channels: [Vec<f64>; 2],
}

/// Runs every sample through the graph in blocks of `size`.
fn run(size: usize) -> Vec<Output> {
    let (mut graph, types) = graph();
    let mut outputs = vec![Output::default(); OUTPUTS.len()];
    let mut first = 0;
    while first < SAMPLES {
        let frames = size.min(SAMPLES - first);
        let block = |ty: &SignalType, value: fn(u64, u64) -> f64| Block {
            domain: ty.domain,
            run: RunId(1),
            first: first as u64,
            frames,
            channels: Arc::clone(&ty.channels),
            data: (0..2)
                .flat_map(|c| (first..first + frames).map(move |n| value(c, n as u64)))
                .collect(),
        };
        graph
            .tick(vec![block(&types[0], signal), block(&types[1], reference)])
            .unwrap();
        for (output, name) in outputs.iter_mut().zip(OUTPUTS) {
            let block = graph.output(name).unwrap();
            if block.frames > 0 {
                output.first.get_or_insert(block.first);
                for (channel, held) in output.channels.iter_mut().enumerate() {
                    held.extend_from_slice(block.channel(channel));
                }
            }
        }
        first += frames;
    }
    outputs
}

/// What numpy made of the same signals: `(first, samples)` by name and
/// channel.
fn expected(name: &str, channel: usize) -> (u64, Vec<f64>) {
    let wanted = format!("{name} {} ", ["x", "y"][channel]);
    let line = include_str!("fixtures/functions.txt")
        .lines()
        .find_map(|line| line.strip_prefix(&wanted))
        .unwrap_or_else(|| panic!("no {wanted}in the fixture"));
    let mut fields = line.split(' ');
    let first = fields.next().unwrap().parse().unwrap();
    (first, fields.map(|v| v.parse().unwrap()).collect())
}

/// Holds one output to the fixture, from sample `from` on, to within
/// `tolerance` of the signals' own size, which is about one.
fn check(outputs: &[Output], name: &str, from: usize, tolerance: f64) {
    let output = &outputs[OUTPUTS.iter().position(|n| *n == name).unwrap()];
    for channel in 0..2 {
        let (first, want) = expected(name, channel);
        assert_eq!(output.first, Some(first), "{name} begins elsewhere");
        let got = &output.channels[channel];
        assert_eq!(got.len(), want.len(), "{name} has another length");
        let worst = got[from..]
            .iter()
            .zip(&want[from..])
            .map(|(g, w)| (g - w).abs())
            .fold(0.0, f64::max);
        assert!(
            worst <= tolerance,
            "{name} channel {channel} is off by {worst:e}"
        );
    }
}

#[test]
fn each_function_computes_what_its_definition_says() {
    let outputs = run(64);
    // Sums and differences of a few terms: as exact as arithmetic is.
    check(&outputs, "integrate", 0, 1e-13);
    check(&outputs, "derivative", 0, 1e-10);
    check(&outputs, "rms", 0, 1e-12);
    check(&outputs, "nlms", 0, 1e-11);

    // A fit over fewer samples than it has weights is whatever the solver
    // makes of it: no two solvers agree there, and nothing should be read
    // into it. Once the window holds enough to fix the weights they do.
    check(&outputs, "ls", 8, 1e-9);
    check(&outputs, "ls3", 24, 1e-8);
    // The same goes for the first steps of a memory that starts out sure
    // of nothing.
    check(&outputs, "rls", 24, 1e-8);
}

#[test]
fn how_the_samples_are_cut_into_blocks_changes_nothing() {
    let whole = run(SAMPLES);
    for size in [1, 7, 64, 199] {
        let cut = run(size);
        for (name, (a, b)) in OUTPUTS.iter().zip(whole.iter().zip(&cut)) {
            assert!(a == b, "{name} differs in blocks of {size}");
        }
    }
}

#[test]
fn a_derivative_runs_one_sample_late_and_loses_the_ends() {
    // Sample `n` waits for sample `n + 1`: fed one at a time, each tick
    // from the third on reports the sample before the one it was given.
    let (mut graph, types) = graph();
    for n in 0..6u64 {
        let block = |ty: &SignalType, value: fn(u64, u64) -> f64| Block {
            domain: ty.domain,
            run: RunId(1),
            first: n,
            frames: 1,
            channels: Arc::clone(&ty.channels),
            data: vec![value(0, n), value(1, n)],
        };
        graph
            .tick(vec![block(&types[0], signal), block(&types[1], reference)])
            .unwrap();
        let out = graph.output("derivative").unwrap();
        if n < 2 {
            assert_eq!(out.frames, 0, "nothing can be said at sample {n}");
        } else {
            assert_eq!((out.first, out.frames), (n - 1, 1));
            let want = (signal(0, n) - signal(0, n - 2)) * RATE / 2.0;
            assert!((out.channel(0)[0] - want).abs() < 1e-10);
        }
    }
    let derivative = &run(SAMPLES)[1];
    assert_eq!(derivative.first, Some(1));
    assert_eq!(derivative.channels[0].len(), SAMPLES - 2);
}

/// Noise that is its own: uniform, of unit width, and unlike the cheap
/// noise above not the same sequence shifted when the seed is another.
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

/// The power of a signal's samples from `from` on.
fn power(samples: &[f64], from: usize) -> f64 {
    samples[from..].iter().map(|x| x * x).sum::<f64>() / (samples.len() - from) as f64
}

#[test]
fn what_is_shared_is_taken_out_as_far_as_the_reference_allows() {
    // Two sensors see one field `c`. The reference sees it with noise of
    // its own, `c + n2`; the signal sees `g c` and noise `n1`. The best any
    // one gain can do leaves
    //
    //     n1² + g² c² n2² / (c² + n2²)
    //
    // of power: the signal's own noise, and what the reference's noise
    // lets through. That is the bound, and a fit over a long window should
    // sit on it.
    const GAIN: f64 = 3.0;
    const FIELD: f64 = 10.0;
    const N1: f64 = 0.2;
    const N2: f64 = 0.5;
    // Uniform noise of unit width has a variance of 1/12.
    let (c, n1, n2) = (FIELD * FIELD / 12.0, N1 * N1 / 12.0, N2 * N2 / 12.0);
    let bound = n1 + GAIN * GAIN * c * n2 / (c + n2);
    let before = GAIN * GAIN * c + n1;

    // NLMS closes on the right gain by a factor of e in a window or two,
    // and starts a gain of three away. Fifteen windows in, it has arrived.
    let (count, settled) = (90_000, 60_000);
    for (method, taps, slack) in [
        (Method::LeastSquares, 1, 0.03),
        (Method::Rls, 1, 0.03),
        // Its weights keep being nudged by the noise, which costs a little.
        (Method::Nlms, 1, 0.05),
    ] {
        let mut builder = GraphBuilder::new();
        let domain = builder
            .domain(Epoch::UNIX, SessionId::new(1), 1000.0, 0.0)
            .unwrap();
        let ty = |name: &str| SignalType {
            domain,
            channels: ChannelSet::uniform([name], &Unit::new("nT")).unwrap(),
        };
        let (signal_ty, reference_ty) = (ty("x"), ty("r"));
        let x = builder.source("x", signal_ty.clone()).unwrap();
        let r = builder.source("r", reference_ty.clone()).unwrap();
        let node = Decorrelate {
            method,
            window: 4.0,
            taps,
        };
        let out = builder.node(node, &[x, r]).unwrap();
        builder.output("out", out).unwrap();
        let mut graph = builder.build(Options::default()).unwrap();

        let block = |ty: &SignalType, data: Vec<f64>| Block {
            domain: ty.domain,
            run: RunId(1),
            first: 0,
            frames: count,
            channels: Arc::clone(&ty.channels),
            data,
        };
        let (mut shared, mut own, mut theirs) = (Random(11), Random(12), Random(13));
        let field: Vec<f64> = (0..count).map(|_| FIELD * shared.next()).collect();
        let signal: Vec<f64> = field.iter().map(|c| GAIN * c + N1 * own.next()).collect();
        let reference: Vec<f64> = field.iter().map(|c| c + N2 * theirs.next()).collect();
        assert!((power(&signal, 0) / before - 1.0).abs() < 0.03);
        graph
            .tick(vec![
                block(&signal_ty, signal),
                block(&reference_ty, reference),
            ])
            .unwrap();

        let left = power(graph.output("out").unwrap().channel(0), settled);
        let ratio = left / bound;
        assert!(
            ratio > 1.0 - 0.03 && ratio < 1.0 + slack,
            "{method:?} leaves {left:.5}, and the bound is {bound:.5}"
        );
        // Which is the shared field down by what the reference's own noise
        // allows, and no further.
        let rejection = (GAIN * GAIN * c) / (left - n1);
        let most = (c + n2) / n2;
        assert!(
            rejection > 0.85 * most && rejection < 1.1 * most,
            "{method:?}: {rejection:.0} of {most:.0}"
        );
    }
}

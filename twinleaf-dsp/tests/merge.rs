//! The nodes that change the rate and merge domains: what they compute, and
//! that it does not depend on how their inputs arrive.

use std::f64::consts::PI;
use std::sync::Arc;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;
use twinleaf_dsp::nodes::{Align, Biquad, Pick, Resample, Section, Sub};
use twinleaf_dsp::{
    Block, BuildError, ChannelSet, DomainId, Graph, GraphBuilder, Options, RunId, SignalType, Unit,
};

fn signal(domain: DomainId, names: &[&str]) -> SignalType {
    SignalType {
        domain,
        channels: ChannelSet::uniform(names.iter().copied(), &Unit::new("nT")).unwrap(),
    }
}

fn block(ty: &SignalType, run: u64, first: u64, rows: &[Vec<f64>]) -> Block {
    Block {
        domain: ty.domain,
        run: RunId(run),
        first,
        frames: rows.first().map_or(0, Vec::len),
        channels: Arc::clone(&ty.channels),
        data: rows.concat(),
    }
}

fn serial() -> Options {
    Options {
        threads: 1,
        ..Options::default()
    }
}

/// Feeds one source its samples in one block and returns the output block.
fn run_once(graph: &mut Graph, ty: &SignalType, rows: &[Vec<f64>], output: &str) -> Block {
    graph.tick(vec![block(ty, 1, 0, rows)]).unwrap();
    graph.output(output).unwrap().clone()
}

fn tone(frequency: f64, rate: f64, first: f64, frames: usize) -> Vec<f64> {
    (0..frames)
        .map(|n| (2.0 * PI * frequency * (first + n as f64) / rate).sin())
        .collect()
}

fn decimator(factor: u64) -> (Graph, SignalType) {
    let mut builder = GraphBuilder::new();
    let domain = builder
        .domain(Epoch::UNIX, SessionId::new(1), 1000.0, 0.0)
        .unwrap();
    let ty = signal(domain, &["x"]);
    let source = builder.source("a", ty.clone()).unwrap();
    let slow = builder.node(Resample::decimate(factor), &[source]).unwrap();
    builder.output("slow", slow).unwrap();
    (builder.build(serial()).unwrap(), ty)
}

#[test]
fn decimation_keeps_every_nth_instant_and_does_not_delay() {
    let (mut graph, ty) = decimator(2);
    let half_width = Resample::decimate(2).filter().half_width() as u64;
    let out = run_once(&mut graph, &ty, &[tone(10.0, 1000.0, 0.0, 4000)], "slow");

    // The result is at half the rate, on the same clock.
    let domain = graph.domain(out.domain).unwrap();
    assert_eq!((domain.rate, domain.origin), (500.0, 0.0));

    // Sample k is input sample 2k. The first and last are those whose
    // windows fit inside the input.
    assert_eq!(out.first, half_width.div_ceil(2));
    assert_eq!(
        out.first + out.frames as u64,
        (4000 - half_width).div_ceil(2)
    );

    // A tone well inside the band comes out as itself at those instants:
    // same amplitude, no shift in time.
    for (index, got) in out.channel(0).iter().enumerate() {
        let k = out.first + index as u64;
        let want = (2.0 * PI * 10.0 * (2 * k) as f64 / 1000.0).sin();
        assert!((got - want).abs() < 1e-4, "sample {k}: {got} is not {want}");
    }
}

#[test]
fn decimation_removes_what_would_alias() {
    // 300 Hz is above the new Nyquist frequency, 250 Hz. Left in, it would
    // fold down to 200 Hz.
    let (mut graph, ty) = decimator(2);
    let out = run_once(&mut graph, &ty, &[tone(300.0, 1000.0, 0.0, 4000)], "slow");
    assert!(out.frames > 1000);
    let peak = out.channel(0).iter().fold(0.0_f64, |m, x| m.max(x.abs()));
    assert!(peak < 1e-4, "{peak} of the tone survived");
}

#[test]
fn resampling_reads_between_samples() {
    // 1 kHz to 300 Hz is three samples out for every ten in, so most output
    // instants fall between input samples.
    let mut builder = GraphBuilder::new();
    let domain = builder
        .domain(Epoch::UNIX, SessionId::new(1), 1000.0, 0.0)
        .unwrap();
    let ty = signal(domain, &["x"]);
    let source = builder.source("a", ty.clone()).unwrap();
    let ratio = Resample::between(1000.0, 300.0).unwrap();
    assert_eq!(ratio, Resample::ratio(3, 10));
    let slow = builder.node(ratio, &[source]).unwrap();
    builder.output("slow", slow).unwrap();
    let mut graph = builder.build(serial()).unwrap();

    let out = run_once(&mut graph, &ty, &[tone(10.0, 1000.0, 0.0, 5000)], "slow");
    assert_eq!(graph.domain(out.domain).unwrap().rate, 300.0);
    assert!(out.frames > 1300);
    for (index, got) in out.channel(0).iter().enumerate() {
        let k = out.first + index as u64;
        let want = (2.0 * PI * 10.0 * k as f64 / 300.0).sin();
        assert!((got - want).abs() < 1e-4, "sample {k}: {got} is not {want}");
    }
}

#[test]
fn a_rate_ratio_is_found_from_two_rates() {
    assert_eq!(
        Resample::between(1000.0, 500.0),
        Some(Resample::decimate(2))
    );
    assert_eq!(
        Resample::between(44_100.0, 48_000.0),
        Some(Resample::ratio(160, 147))
    );
    assert_eq!(
        Resample::between(1000.0, 1000.0 / 3.0),
        Some(Resample::decimate(3))
    );
    // No ratio of small whole numbers.
    assert_eq!(Resample::between(1000.0, 1000.0 * PI), None);

    let mut builder = GraphBuilder::new();
    let domain = builder
        .domain(Epoch::UNIX, SessionId::new(1), 1000.0, 0.0)
        .unwrap();
    let source = builder.source("a", signal(domain, &["x"])).unwrap();
    assert!(matches!(
        builder.node(Resample::ratio(3, 3), &[source]),
        Err(BuildError::NoRateChange { kind: "decimate" })
    ));
    // Two decimations alike land in one domain, so their results combine.
    let one = builder.node(Resample::decimate(4), &[source]).unwrap();
    let other = builder.node(Resample::ratio(2, 8), &[source]).unwrap();
    assert!(builder.node(Sub, &[one, other]).is_ok());
}

/// Two arms at 500 Hz whose sample 0 is `offset` seconds apart, merged.
fn aligner(offset: f64, session: u32) -> (Graph, SignalType, SignalType) {
    let mut builder = GraphBuilder::new();
    let first = builder
        .domain(Epoch::UNIX, SessionId::new(1), 500.0, 100.0)
        .unwrap();
    let second = builder
        .domain(Epoch::UNIX, SessionId::new(session), 500.0, 100.0 + offset)
        .unwrap();
    let (a_ty, b_ty) = (signal(first, &["x"]), signal(second, &["x"]));
    let a = builder.source("a", a_ty.clone()).unwrap();
    let b = builder.source("b", b_ty.clone()).unwrap();
    let merged = builder.node(Align::new(["a", "b"]), &[a, b]).unwrap();
    builder.output("fr", merged).unwrap();
    (builder.build(serial()).unwrap(), a_ty, b_ty)
}

#[test]
fn arms_on_one_grid_are_shifted_and_not_touched() {
    // The second arm's sample 0 is three samples later, so its sample j is
    // the first arm's sample j + 3.
    let (mut graph, a_ty, b_ty) = aligner(3.0 / 500.0, 1);
    let a: Vec<f64> = (0..40).map(|n| 1000.0 + f64::from(n)).collect();
    let b: Vec<f64> = (0..30).map(|n| 2000.0 + f64::from(n)).collect();
    graph
        .tick(vec![block(&a_ty, 1, 0, &[a]), block(&b_ty, 1, 0, &[b])])
        .unwrap();
    let out = graph.output("fr").unwrap();
    let names: Vec<&str> = out.channels.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["a.x", "b.x"]);

    // The result is on the first arm's grid and covers what both arms do:
    // from the second arm's first sample to the end of the shorter arm.
    assert_eq!((out.first, out.frames), (3, 30));
    for (index, (a, b)) in out.channel(0).iter().zip(out.channel(1)).enumerate() {
        assert_eq!(*a, 1003.0 + index as f64);
        assert_eq!(*b, 2000.0 + index as f64);
    }
    assert_eq!(graph.domain(out.domain).unwrap().origin, 100.0);
}

#[test]
fn arms_between_grids_are_read_at_the_same_instants() {
    // A quarter of a sample apart. Both arms sample one 20 Hz tone, each at
    // its own instants, so after alignment they must agree.
    let rate = 500.0;
    let (mut graph, a_ty, b_ty) = aligner(0.25 / rate, 1);
    let a = tone(20.0, rate, 0.0, 2000);
    let b = tone(20.0, rate, 0.25, 2000);
    graph
        .tick(vec![block(&a_ty, 1, 0, &[a]), block(&b_ty, 1, 0, &[b])])
        .unwrap();
    let out = graph.output("fr").unwrap();
    assert!(out.frames > 1900);
    for (index, (a, b)) in out.channel(0).iter().zip(out.channel(1)).enumerate() {
        let j = out.first + index as u64;
        let want = (2.0 * PI * 20.0 * j as f64 / rate).sin();
        assert!((a - want).abs() < 1e-4, "first arm at {j}");
        // The arms go through the same filter, so they agree with each
        // other more closely than either does with the ideal.
        assert!((a - b).abs() < 2e-5, "arms differ by {} at {j}", a - b);
    }
}

#[test]
fn arms_on_different_time_bases_do_not_merge() {
    let mut builder = GraphBuilder::new();
    let first = builder
        .domain(Epoch::UNIX, SessionId::new(1), 500.0, 0.0)
        .unwrap();
    let second = builder
        .domain(Epoch::UNIX, SessionId::new(2), 500.0, 0.0)
        .unwrap();
    let slow = builder
        .domain(Epoch::UNIX, SessionId::new(1), 250.0, 0.0)
        .unwrap();
    let a = builder.source("a", signal(first, &["x"])).unwrap();
    let b = builder.source("b", signal(second, &["x"])).unwrap();
    let c = builder.source("c", signal(slow, &["x"])).unwrap();
    assert!(matches!(
        builder.node(Align::new(["a", "b"]), &[a, b]),
        Err(BuildError::Timebase { kind: "align", .. })
    ));
    assert!(matches!(
        builder.node(Align::new(["a", "c"]), &[a, c]),
        Err(BuildError::DomainMismatch { kind: "align", .. })
    ));
    // Only align may be handed two domains.
    assert!(matches!(
        builder.node(Sub, &[a, b]),
        Err(BuildError::DomainMismatch { kind: "sub", .. })
    ));
}

/// The gradiometer's shape: one arm at twice the rate and a quarter of a
/// sample off the other's grid.
struct Gradiometer {
    graph: Graph,
    a: SignalType,
    b: SignalType,
}

fn gradiometer(options: Options) -> Gradiometer {
    let mut builder = GraphBuilder::new();
    let fast = builder
        .domain(Epoch::UNIX, SessionId::new(1), 1000.0, 0.0)
        .unwrap();
    let slow = builder
        .domain(Epoch::UNIX, SessionId::new(1), 500.0, 0.0005)
        .unwrap();
    let (a_ty, b_ty) = (
        signal(fast, &["x", "y", "z"]),
        signal(slow, &["x", "y", "z"]),
    );
    let a = builder.source("a", a_ty.clone()).unwrap();
    let b = builder.source("b", b_ty.clone()).unwrap();
    let halved = builder.node(Resample::decimate(2), &[a]).unwrap();
    let merged = builder.node(Align::new(["a", "b"]), &[halved, b]).unwrap();
    let section = Section {
        b0: 0.067_455_27,
        b1: 0.134_910_55,
        b2: 0.067_455_27,
        a1: -1.142_980_5,
        a2: 0.412_801_6,
    };
    let clean = builder
        .node(Biquad::cascade(vec![section]), &[merged])
        .unwrap();
    let arm = |name: &str| Pick::new(["x", "y", "z"].map(|c| (format!("{name}.{c}"), c)));
    let left = builder.node(arm("a"), &[clean]).unwrap();
    let right = builder.node(arm("b"), &[clean]).unwrap();
    let gradient = builder.node(Sub, &[left, right]).unwrap();
    builder.output("merged", merged).unwrap();
    builder.output("gradient", gradient).unwrap();
    Gradiometer {
        graph: builder.build(options).unwrap(),
        a: a_ty,
        b: b_ty,
    }
}

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn rows(&mut self, frames: usize) -> Vec<Vec<f64>> {
        (0..3)
            .map(|_| {
                (0..frames)
                    .map(|_| (self.next() >> 11) as f64 / (1u64 << 52) as f64 - 1.0)
                    .collect()
            })
            .collect()
    }
}

/// What the graph emitted: for each output and run, where it began and its
/// samples as bits.
type Trace = Vec<(String, u64, u64, Vec<Vec<u64>>)>;

fn absorb(trace: &mut Trace, graph: &Graph) {
    for (name, block) in graph.outputs() {
        if block.frames == 0 {
            continue;
        }
        let known = trace
            .iter_mut()
            .rev()
            .find(|entry| entry.0 == name)
            .filter(|entry| entry.1 == block.run.0);
        let entry = match known {
            Some(entry) => {
                assert_eq!(block.first, entry.2 + entry.3[0].len() as u64);
                entry
            }
            None => {
                let rows = vec![Vec::new(); block.channels.len()];
                trace.push((name.to_owned(), block.run.0, block.first, rows));
                trace.last_mut().unwrap()
            }
        };
        for (channel, row) in entry.3.iter_mut().enumerate() {
            row.extend(block.channel(channel).iter().map(|v| v.to_bits()));
        }
    }
}

/// Feeds both arms in pieces. Each call to `cut` says how many samples of
/// each arm the next tick carries; either may be none.
///
/// The first arm has a gap in it. A run's first sample resets what is
/// downstream of it, so where it falls among the other arm's samples is part
/// of the input. The pieces differ in everything else: everything ahead of a
/// run's first sample is delivered before it, as a recording read in order
/// would deliver it.
fn drive(options: Options, mut cut: impl FnMut() -> (usize, usize)) -> Trace {
    let mut fixture = gradiometer(options);
    let mut random = Random(0x6AAD);
    // (arm, run, first sample, samples)
    let phases = [
        vec![(0, 1, 0, random.rows(3000)), (1, 1, 0, random.rows(1400))],
        vec![
            (0, 2, 3200, random.rows(2800)),
            (1, 1, 1400, random.rows(1500)),
        ],
    ];
    let mut trace = Trace::new();
    let mut cursor = [(RunId(0), 0u64); 2];
    for phase in &phases {
        let mut at = [0usize; 2];
        loop {
            let (mut want_a, mut want_b) = cut();
            // A sample that opens a new run goes in its phase's first tick,
            // so that nothing which follows it in the recording arrives
            // ahead of it.
            let opens = |arm: usize| at[arm] == 0 && phase[arm].1 != cursor[arm].0 .0;
            if at == [0, 0] {
                if opens(0) {
                    want_a = want_a.max(1);
                }
                if opens(1) {
                    want_b = want_b.max(1);
                }
            }
            let mut blocks = Vec::new();
            let mut any = false;
            for (arm, ty) in [&fixture.a, &fixture.b].into_iter().enumerate() {
                let (_, run, first, rows) = phase.iter().find(|p| p.0 == arm).unwrap();
                let left = rows[0].len() - at[arm];
                let take = [want_a, want_b][arm].min(left);
                if take > 0 {
                    cursor[arm] = (RunId(*run), first + (at[arm] + take) as u64);
                    let piece: Vec<Vec<f64>> = rows
                        .iter()
                        .map(|row| row[at[arm]..at[arm] + take].to_vec())
                        .collect();
                    blocks.push(block(ty, *run, first + at[arm] as u64, &piece));
                    at[arm] += take;
                    any = true;
                } else {
                    // Nothing new for this arm: an empty block where it is.
                    blocks.push(block(ty, cursor[arm].0 .0, cursor[arm].1, &[]));
                }
            }
            if !any && at[0] == phase[0].3[0].len() && at[1] == phase[1].3[0].len() {
                break;
            }
            if any {
                fixture.graph.tick(blocks).unwrap();
                absorb(&mut trace, &fixture.graph);
            }
        }
    }
    trace
}

#[test]
fn merged_output_does_not_depend_on_how_the_arms_arrive() {
    let whole = drive(serial(), || (usize::MAX, usize::MAX));
    // Both outputs, each in two runs: before the gap and after it.
    assert_eq!(whole.len(), 4);
    assert!(whole.iter().all(|entry| entry.3[0].len() > 1000));

    let pooled = |threads, bundle| Options {
        threads,
        bundle,
        inline_below: 0,
    };
    // One sample at a time, the arms taking turns.
    let mut turn = 0;
    let alternating = drive(pooled(16, 2), || {
        turn += 1;
        if turn % 2 == 0 {
            (1, 0)
        } else {
            (0, 1)
        }
    });
    assert!(alternating == whole, "one sample at a time disagrees");

    // One arm far ahead of the other, then the other catching up.
    let mut step = 0;
    let lopsided = drive(pooled(1, 3), || {
        step += 1;
        if step % 50 < 40 {
            (97, 0)
        } else {
            (0, 211)
        }
    });
    assert!(lopsided == whole, "a lagging arm disagrees");

    let mut random = Random(0xC0FFEE);
    for (threads, bundle) in [(1, 1), (5, 2), (16, 8)] {
        let ragged = drive(pooled(threads, bundle), || {
            let n = random.next();
            ((n % 64) as usize, ((n >> 8) % 9) as usize)
        });
        assert!(ragged == whole, "{threads} thread(s), bundles of {bundle}");
    }
}

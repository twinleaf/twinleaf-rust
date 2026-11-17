//! A graph's output is a function of the samples it was fed, and of nothing
//! else: not how they were cut into blocks, not the thread count, not the lane
//! bundle width. These tests run one input through one graph under different
//! settings and require the outputs to agree bit for bit.

use std::ops::Range;
use std::sync::Arc;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;
use twinleaf_dsp::nodes::{Biquad, Gain, Magnitude, Section, Sub};
use twinleaf_dsp::{
    Block, BuildError, ChannelSet, Coupling, DomainId, Domains, Graph, GraphBuilder, Inputs,
    Kernel, Node, Options, RunId, SignalType, Span, Stage, TickError, Unit,
};

/// Five channels, so that a bundle of two leaves a ragged last lane.
const CHANNELS: [&str; 5] = ["x", "y", "z", "u", "v"];

/// One uninterrupted stretch of input: its run, its first sample index, and
/// its samples, one row per channel.
struct Run {
    id: RunId,
    first: u64,
    rows: Vec<Vec<f64>>,
}

impl Run {
    fn frames(&self) -> usize {
        self.rows[0].len()
    }
}

/// SplitMix64, so the input is the same everywhere without a dependency.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A sample in [-1, 1).
    fn sample(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 52) as f64 - 1.0
    }
}

/// Two runs with a gap in the sample numbering between them. Neither length
/// is a multiple of any block size used below.
fn input() -> Vec<Run> {
    let mut random = Random(0x7157);
    [(1, 0, 5000), (2, 20_000, 7411)]
        .into_iter()
        .map(|(id, first, frames)| Run {
            id: RunId(id),
            first,
            rows: CHANNELS
                .iter()
                .map(|_| (0..frames).map(|_| random.sample()).collect())
                .collect(),
        })
        .collect()
}

struct Fixture {
    graph: Graph,
    domain: DomainId,
    channels: Arc<ChannelSet>,
}

/// A notch near 60 Hz and a 100 Hz low-pass at 1 kHz, as fixed coefficients.
fn filter() -> Biquad {
    Biquad::cascade(vec![
        Section {
            b0: 0.993_902,
            b1: -1.848_214,
            b2: 0.993_902,
            a1: -1.848_214,
            a2: 0.987_804,
        },
        Section {
            b0: 0.067_455_27,
            b1: 0.134_910_55,
            b2: 0.067_455_27,
            a1: -1.142_980_5,
            a2: 0.412_801_6,
        },
    ])
}

/// A diamond that uses every kind of node the engine distinguishes:
///
/// ```text
///   source ──► biquad ──┐
///      │                ├──► sub ──► magnitude
///      └─────► gain ────┘     │          │
///                           "diff"     "mag"
/// ```
///
/// `middle` is spliced in after the filter, for the control test.
fn fixture(options: Options, middle: Option<Box<dyn Node>>) -> Fixture {
    let mut builder = GraphBuilder::new();
    let domain = builder
        .domain(Epoch::UNIX, SessionId::new(7), 1000.0, 0.0)
        .unwrap();
    let channels = ChannelSet::uniform(CHANNELS, &Unit::new("nT")).unwrap();
    let ty = SignalType {
        domain,
        channels: Arc::clone(&channels),
    };
    let source = builder.source("field", ty).unwrap();
    let mut filtered = builder.node(filter(), &[source]).unwrap();
    if let Some(node) = middle {
        filtered = builder.node(node, &[filtered]).unwrap();
    }
    let scaled = builder.node(Gain::new(0.25), &[source]).unwrap();
    let diff = builder.node(Sub, &[filtered, scaled]).unwrap();
    let mag = builder.node(Magnitude, &[diff]).unwrap();
    builder.output("diff", diff).unwrap();
    builder.output("mag", mag).unwrap();
    Fixture {
        graph: builder.build(options).unwrap(),
        domain,
        channels,
    }
}

/// Everything a graph emitted, as bit patterns: for each output and each of
/// its runs, where the run began and each channel's samples.
#[derive(Debug, PartialEq, Eq, Default)]
struct Trace {
    stretches: Vec<Stretch>,
}

#[derive(Debug, PartialEq, Eq)]
struct Stretch {
    output: String,
    run: RunId,
    first: u64,
    rows: Vec<Vec<u64>>,
}

impl Trace {
    fn absorb(&mut self, output: &str, block: &Block) {
        if block.frames == 0 {
            return;
        }
        let known = self
            .stretches
            .iter_mut()
            .rev()
            .find(|s| s.output == output)
            .filter(|s| s.run == block.run);
        let stretch = match known {
            Some(stretch) => {
                let next = stretch.first + stretch.rows[0].len() as u64;
                assert_eq!(block.first, next, "`{output}` skipped samples in a run");
                stretch
            }
            None => {
                self.stretches.push(Stretch {
                    output: output.to_owned(),
                    run: block.run,
                    first: block.first,
                    rows: vec![Vec::new(); block.channels.len()],
                });
                self.stretches.last_mut().unwrap()
            }
        };
        for (channel, row) in stretch.rows.iter_mut().enumerate() {
            row.extend(block.channel(channel).iter().map(|v| v.to_bits()));
        }
    }

    fn samples(&self) -> usize {
        self.stretches
            .iter()
            .map(|s| s.rows.iter().map(Vec::len).sum::<usize>())
            .sum()
    }
}

/// Feeds `runs` through the fixture, cutting each run into blocks of the sizes
/// `block_size` hands out, and returns everything the graph emitted.
fn trace(mut fixture: Fixture, runs: &[Run], mut block_size: impl FnMut() -> usize) -> Trace {
    let mut trace = Trace::default();
    for run in runs {
        let mut at = 0;
        while at < run.frames() {
            let frames = block_size().clamp(1, run.frames() - at);
            let block = Block {
                domain: fixture.domain,
                run: run.id,
                first: run.first + at as u64,
                frames,
                channels: Arc::clone(&fixture.channels),
                data: run
                    .rows
                    .iter()
                    .flat_map(|row| &row[at..at + frames])
                    .copied()
                    .collect(),
            };
            fixture.graph.tick(vec![block]).unwrap();
            for (name, block) in fixture.graph.outputs() {
                trace.absorb(name, block);
            }
            at += frames;
        }
    }
    trace
}

/// Always in the pool, however small the tick, so that one-sample blocks are
/// scheduled across threads too.
fn pooled(threads: usize, bundle: usize) -> Options {
    Options {
        threads,
        bundle,
        inline_below: 0,
    }
}

#[test]
fn output_is_bit_identical_across_segmentation_and_core_count() {
    let runs = input();
    let reference = trace(fixture(pooled(1, 2), None), &runs, || 1);

    // Six output channels' worth of every input frame, in two stretches each.
    let frames: usize = runs.iter().map(Run::frames).sum();
    assert_eq!(reference.samples(), frames * (CHANNELS.len() + 1));
    assert_eq!(reference.stretches.len(), 4);

    for (threads, block) in [(1, 4096), (16, 1), (16, 4096)] {
        let other = trace(fixture(pooled(threads, 2), None), &runs, || block);
        assert!(
            other == reference,
            "{threads} thread(s) with {block}-sample blocks disagree with 1 thread and 1-sample blocks"
        );
    }
}

#[test]
fn output_is_bit_identical_across_ragged_blocks_bundles_and_inlining() {
    let runs = input();
    let reference = trace(fixture(pooled(1, 2), None), &runs, || usize::MAX);

    let mut random = Random(0xB10C);
    for threads in [1, 5] {
        for bundle in [1, 2, 3, 8] {
            for inline_below in [0, Options::default().inline_below] {
                let options = Options {
                    threads,
                    bundle,
                    inline_below,
                };
                // Mostly small blocks with the odd large one, so that a pooled
                // graph crosses its inline threshold in both directions.
                let other = trace(fixture(options, None), &runs, || {
                    let size = random.next();
                    if size.is_multiple_of(16) {
                        (size >> 8) as usize % 3000
                    } else {
                        (size >> 8) as usize % 40
                    }
                });
                assert!(
                    other == reference,
                    "{options:?} disagrees with the reference"
                );
            }
        }
    }
}

/// A node that is wrong on purpose: it removes each block's own mean, so its
/// output depends on where the blocks were cut.
struct BlockMean;

impl Node for BlockMean {
    fn kind(&self) -> &'static str {
        "block_mean"
    }
    fn bind(&self, inputs: &[SignalType], _: &mut Domains) -> Result<Box<dyn Stage>, BuildError> {
        Ok(Box::new(BlockMeanStage(inputs[0].clone())))
    }
}

struct BlockMeanStage(SignalType);

impl Stage for BlockMeanStage {
    fn output(&self) -> SignalType {
        self.0.clone()
    }
    fn fingerprint(&self) -> u64 {
        twinleaf_dsp::node::fingerprint("block_mean")
    }
    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }
    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(BlockMeanKernel { lane })
    }
}

struct BlockMeanKernel {
    lane: Range<usize>,
}

impl Kernel for BlockMeanKernel {
    fn reset(&mut self) {}

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        for (row, channel) in out.chunks_exact_mut(span.frames).zip(self.lane.clone()) {
            let samples = input.channel(channel);
            let mean = samples.iter().sum::<f64>() / samples.len() as f64;
            for (y, x) in row.iter_mut().zip(samples) {
                *y = x - mean;
            }
        }
    }
}

#[test]
fn the_comparison_catches_a_node_that_depends_on_segmentation() {
    let runs = input();
    let broken = || fixture(pooled(1, 2), Some(Box::new(BlockMean)));
    let whole = trace(broken(), &runs, || usize::MAX);
    assert!(whole == trace(broken(), &runs, || usize::MAX));
    assert!(whole != trace(broken(), &runs, || 4096));
}

#[test]
fn a_new_run_resets_state() {
    let runs = input();
    let both = trace(fixture(pooled(4, 2), None), &runs, || 512);
    let second_alone = trace(fixture(pooled(4, 2), None), &runs[1..], || 512);

    // The second run's samples do not remember the first run. Only the run
    // numbering differs, since the graph counts the runs it has seen.
    let rows = |trace: &Trace, output: &str, nth: usize| -> Vec<Vec<u64>> {
        trace
            .stretches
            .iter()
            .filter(|s| s.output == output)
            .nth(nth)
            .map(|s| s.rows.clone())
            .unwrap()
    };
    for output in ["diff", "mag"] {
        assert_eq!(rows(&both, output, 1), rows(&second_alone, output, 0));
        assert_ne!(rows(&both, output, 0), rows(&both, output, 1));
    }
}

#[test]
fn a_gap_inside_a_run_is_refused_and_changes_nothing() {
    let runs = input();
    let mut fixture = fixture(pooled(1, 2), None);
    let block = |first: u64, frames: usize| Block {
        domain: fixture.domain,
        run: RunId(1),
        first,
        frames,
        channels: Arc::clone(&fixture.channels),
        data: runs[0]
            .rows
            .iter()
            .flat_map(|row| &row[first as usize..first as usize + frames])
            .copied()
            .collect(),
    };
    let (first, skipped, second) = (block(0, 100), block(150, 100), block(100, 100));

    fixture.graph.tick(vec![first]).unwrap();
    assert_eq!(
        fixture.graph.tick(vec![skipped]),
        Err(TickError::Discontinuity {
            name: "field".into(),
            expected: 100,
            found: 150,
        })
    );
    // The refused block left no trace: the run carries on where it was.
    fixture.graph.tick(vec![second]).unwrap();
    let after_refusal = fixture.graph.output("mag").unwrap().clone();

    let whole = trace(self::fixture(pooled(1, 2), None), &runs[..1], || 100);
    let expected: Vec<u64> = whole.stretches[1].rows[0][100..200].to_vec();
    assert_eq!(whole.stretches[1].output, "mag");
    assert_eq!(
        after_refusal
            .data
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        expected
    );
}

//! What a graph computes, how it is expanded into lanes, and what the builder
//! and the tick refuse.

use std::sync::Arc;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;
use twinleaf_dsp::nodes::{Biquad, Gain, Magnitude, Section, Sub};
use twinleaf_dsp::{
    Block, BuildError, Channel, ChannelSet, DomainId, GraphBuilder, Options, RunId, SignalType,
    TickError, Unit,
};

const SECTIONS: [Section; 2] = [
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
];

fn domain(builder: &mut GraphBuilder, rate: f64) -> DomainId {
    builder
        .domain(Epoch::UNIX, SessionId::new(7), rate, 0.0)
        .unwrap()
}

fn signal(domain: DomainId, names: &[&str], unit: &str) -> SignalType {
    SignalType {
        domain,
        channels: ChannelSet::uniform(names.iter().copied(), &Unit::new(unit)).unwrap(),
    }
}

fn block(ty: &SignalType, rows: &[Vec<f64>]) -> Block {
    Block {
        domain: ty.domain,
        run: RunId(1),
        first: 0,
        frames: rows[0].len(),
        channels: Arc::clone(&ty.channels),
        data: rows.concat(),
    }
}

/// A few hundred samples that are not special in any way.
fn wiggle(seed: u32, frames: usize) -> Vec<f64> {
    (0..frames)
        .map(|i| {
            let t = (i as f64 + f64::from(seed) * 0.37) * 0.11;
            t.sin() + 0.3 * (t * 7.3).cos() + 0.01 * f64::from(seed)
        })
        .collect()
}

/// The cascade's difference equations, written out in direct form I.
fn reference_filter(x: &[f64]) -> Vec<f64> {
    let mut signal = x.to_vec();
    for s in SECTIONS {
        let (mut x1, mut x2, mut y1, mut y2) = (0.0, 0.0, 0.0, 0.0);
        for value in &mut signal {
            let x0 = *value;
            let y0 = s.b0 * x0 + s.b1 * x1 + s.b2 * x2 - s.a1 * y1 - s.a2 * y2;
            (x2, x1, y2, y1) = (x1, x0, y1, y0);
            *value = y0;
        }
    }
    signal
}

#[test]
fn each_node_computes_what_it_says() {
    let mut builder = GraphBuilder::new();
    let d = domain(&mut builder, 1000.0);
    let ty = signal(d, &["x", "y", "z"], "nT");
    let source = builder.source("field", ty.clone()).unwrap();
    let filtered = builder
        .node(Biquad::cascade(SECTIONS.to_vec()), &[source])
        .unwrap();
    let scaled = builder.node(Gain::new(0.25), &[source]).unwrap();
    let diff = builder.node(Sub, &[filtered, scaled]).unwrap();
    let mag = builder.node(Magnitude, &[diff]).unwrap();
    for (name, node) in [
        ("filtered", filtered),
        ("scaled", scaled),
        ("diff", diff),
        ("mag", mag),
    ] {
        builder.output(name, node).unwrap();
    }
    let mut graph = builder.build(Options::default()).unwrap();

    let rows: Vec<Vec<f64>> = (0..3).map(|c| wiggle(c, 300)).collect();
    graph.tick(vec![block(&ty, &rows)]).unwrap();
    let out = |name: &str| graph.output(name).unwrap();

    for (c, row) in rows.iter().enumerate() {
        // Transposed direct form II and direct form I round differently, so
        // the filter is compared within a tolerance and the rest exactly.
        let expected = reference_filter(row);
        for (got, want) in out("filtered").channel(c).iter().zip(&expected) {
            assert!((got - want).abs() < 1e-9, "filtered: {got} is not {want}");
        }
        for (i, x) in row.iter().enumerate() {
            assert_eq!(out("scaled").channel(c)[i], x * 0.25);
            assert_eq!(
                out("diff").channel(c)[i],
                out("filtered").channel(c)[i] - out("scaled").channel(c)[i]
            );
        }
    }
    let diff = out("diff");
    for (i, got) in out("mag").channel(0).iter().enumerate() {
        let squared: f64 = (0..3).map(|c| diff.channel(c)[i].powi(2)).sum();
        assert!((got - squared.sqrt()).abs() < 1e-12);
    }

    let mag = graph.output_type("mag").unwrap();
    assert_eq!(mag.channels.len(), 1);
    assert_eq!(mag.channels.get(0).unwrap().unit, Unit::new("nT"));
    assert_eq!(graph.domain(mag.domain).unwrap().rate, 1000.0);
}

#[test]
fn subtraction_pairs_channels_by_name_not_position() {
    let mut builder = GraphBuilder::new();
    let d = domain(&mut builder, 1000.0);
    let left = signal(d, &["x", "y", "z"], "nT");
    let right = signal(d, &["z", "x", "y"], "nT");
    let a = builder.source("a", left.clone()).unwrap();
    let b = builder.source("b", right.clone()).unwrap();
    let diff = builder.node(Sub, &[a, b]).unwrap();
    builder.output("diff", diff).unwrap();
    let mut graph = builder.build(Options::default()).unwrap();

    // a = (x, y, z) = (10, 20, 30); b stores (z, x, y) = (3, 1, 2).
    let a_rows = vec![vec![10.0; 4], vec![20.0; 4], vec![30.0; 4]];
    let b_rows = vec![vec![3.0; 4], vec![1.0; 4], vec![2.0; 4]];
    graph
        .tick(vec![block(&left, &a_rows), block(&right, &b_rows)])
        .unwrap();
    let out = graph.output("diff").unwrap();
    assert_eq!(out.channel(0), [9.0; 4]); // x: 10 - 1
    assert_eq!(out.channel(1), [18.0; 4]); // y: 20 - 2
    assert_eq!(out.channel(2), [27.0; 4]); // z: 30 - 3
}

#[test]
fn independent_nodes_expand_into_bundles_and_coupled_nodes_stay_whole() {
    let build = |bundle: usize| {
        let mut builder = GraphBuilder::new();
        let d = domain(&mut builder, 500.0);
        let ty = signal(d, &["a.x", "a.y", "a.z", "b.x", "b.y", "b.z"], "nT");
        let source = builder.source("fr", ty).unwrap();
        let filtered = builder
            .node(Biquad::cascade(SECTIONS.to_vec()), &[source])
            .unwrap();
        let mag = builder.node(Magnitude, &[filtered]).unwrap();
        builder.output("mag", mag).unwrap();
        builder
            .build(Options {
                bundle,
                ..Options::default()
            })
            .unwrap()
    };
    // Six channels in bundles of two: three filter tasks, one magnitude task.
    assert_eq!(build(2).lane_tasks(), [("biquad", 3), ("magnitude", 1)]);
    assert_eq!(build(4).lane_tasks(), [("biquad", 2), ("magnitude", 1)]);
    assert_eq!(build(64).lane_tasks(), [("biquad", 1), ("magnitude", 1)]);
}

#[test]
fn signals_in_different_domains_do_not_combine() {
    let mut builder = GraphBuilder::new();
    let fast = domain(&mut builder, 1000.0);
    let slow = domain(&mut builder, 500.0);
    let a = builder
        .source("a", signal(fast, &["x", "y", "z"], "nT"))
        .unwrap();
    let b = builder
        .source("b", signal(slow, &["x", "y", "z"], "nT"))
        .unwrap();
    let error = builder.node(Sub, &[a, b]).unwrap_err();
    assert!(matches!(
        error,
        BuildError::DomainMismatch { kind: "sub", left, right } if left.rate == 1000.0 && right.rate == 500.0
    ));
    assert_eq!(
        error.to_string(),
        "`sub` cannot combine signals in different domains \
         (1000 Hz, unix time, session 7) and (500 Hz, unix time, session 7)"
    );
}

#[test]
fn the_builder_refuses_what_cannot_be_paired() {
    let mut builder = GraphBuilder::new();
    let d = domain(&mut builder, 1000.0);
    let xyz = builder
        .source("xyz", signal(d, &["x", "y", "z"], "nT"))
        .unwrap();
    let xy = builder.source("xy", signal(d, &["x", "y"], "nT")).unwrap();
    let volts = builder
        .source("volts", signal(d, &["x", "y", "z"], "V"))
        .unwrap();

    let unpaired = |name: &str| BuildError::UnpairedChannel {
        kind: "sub",
        name: name.into(),
    };
    assert_eq!(builder.node(Sub, &[xyz, xy]), Err(unpaired("z")));
    assert_eq!(builder.node(Sub, &[xy, xyz]), Err(unpaired("z")));
    assert_eq!(
        builder.node(Sub, &[xyz, volts]),
        Err(BuildError::UnitMismatch {
            kind: "sub",
            channel: "x".into(),
            left: Unit::new("nT"),
            right: Unit::new("V"),
        })
    );
    assert_eq!(
        builder.node(Sub, &[xyz]),
        Err(BuildError::Arity {
            kind: "sub",
            expected: 2,
            found: 1,
        })
    );

    let mixed = ChannelSet::new(vec![
        Channel {
            name: "field".into(),
            unit: Unit::new("nT"),
        },
        Channel {
            name: "temperature".into(),
            unit: Unit::new("°C"),
        },
    ])
    .unwrap();
    let mixed = builder
        .source(
            "mixed",
            SignalType {
                domain: d,
                channels: mixed,
            },
        )
        .unwrap();
    assert!(matches!(
        builder.node(Magnitude, &[mixed]),
        Err(BuildError::UnitMismatch {
            kind: "magnitude",
            ..
        })
    ));

    assert_eq!(
        builder.source("xy", signal(d, &["x"], "nT")),
        Err(BuildError::DuplicateSource { name: "xy".into() })
    );
    assert!(matches!(
        builder.build(Options::default()),
        Err(BuildError::NoOutputs)
    ));
}

#[test]
fn a_tick_refuses_blocks_that_do_not_fit_the_sources() {
    let mut builder = GraphBuilder::new();
    let d = domain(&mut builder, 1000.0);
    let other = domain(&mut builder, 500.0);
    let ty = signal(d, &["x", "y"], "nT");
    let source = builder.source("field", ty.clone()).unwrap();
    let scaled = builder.node(Gain::new(2.0), &[source]).unwrap();
    builder.output("scaled", scaled).unwrap();
    let mut graph = builder.build(Options::default()).unwrap();

    let rows = vec![vec![1.0; 8], vec![2.0; 8]];
    let good = block(&ty, &rows);
    let name = || "field".to_owned();

    assert_eq!(
        graph.tick(vec![]),
        Err(TickError::InputCount {
            expected: 1,
            found: 0,
        })
    );
    let mut short = good.clone();
    short.data.pop();
    assert_eq!(
        graph.tick(vec![short]),
        Err(TickError::WrongLength {
            name: name(),
            expected: 16,
            found: 15,
        })
    );
    let mut elsewhere = good.clone();
    elsewhere.domain = other;
    assert_eq!(
        graph.tick(vec![elsewhere]),
        Err(TickError::WrongDomain { name: name() })
    );
    let mut renamed = good.clone();
    renamed.channels = signal(d, &["x", "z"], "nT").channels;
    assert_eq!(
        graph.tick(vec![renamed]),
        Err(TickError::WrongChannels { name: name() })
    );

    // An equal channel set that is not the same allocation is accepted.
    let mut equal = good.clone();
    equal.channels = signal(d, &["x", "y"], "nT").channels;
    graph.tick(vec![equal]).unwrap();
    assert_eq!(graph.output("scaled").unwrap().channel(1), [4.0; 8]);

    // An empty block is a tick in which nothing happens.
    let mut empty = good;
    empty.first = 8;
    empty.frames = 0;
    empty.data.clear();
    graph.tick(vec![empty]).unwrap();
    assert_eq!(graph.output("scaled").unwrap().frames, 0);
}

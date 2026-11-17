//! Changing a spec while it runs restarts only what the change touches.

use std::sync::Arc;
use twinleaf::data::PacketParser;
use twinleaf::proto::rpc::Method;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;
use twinleaf_dsp::lang;
use twinleaf_dsp::nodes::{Biquad, Gain, Section};
use twinleaf_dsp::run::{run_log, RunOptions};
use twinleaf_dsp::serve::Device;
use twinleaf_dsp::{Block, ChannelSet, Graph, GraphBuilder, Options, RunId, SignalType, Unit};

mod common;
use common::*;

const LOW_PASS: Section = Section {
    b0: 0.067_455_27,
    b1: 0.134_910_55,
    b2: 0.067_455_27,
    a1: -1.142_980_5,
    a2: 0.412_801_6,
};

const NOTCH: Section = Section {
    b0: 0.993_902,
    b1: -1.848_214,
    b2: 0.993_902,
    a1: -1.848_214,
    a2: 0.987_804,
};

/// A source, a filter and a gain, with both the filtered and the scaled
/// signal as outputs.
fn graph(filter: Section, gain: f64) -> (Graph, SignalType) {
    let mut builder = GraphBuilder::new();
    let domain = builder
        .domain(Epoch::UNIX, SessionId::new(1), 1000.0, 0.0)
        .unwrap();
    let ty = SignalType {
        domain,
        channels: ChannelSet::uniform(["x", "y", "z"], &Unit::new("nT")).unwrap(),
    };
    let source = builder.source("a", ty.clone()).unwrap();
    let filtered = builder
        .node(Biquad::cascade(vec![filter]), &[source])
        .unwrap();
    let scaled = builder.node(Gain::new(gain), &[filtered]).unwrap();
    builder.output("filtered", filtered).unwrap();
    builder.output("scaled", scaled).unwrap();
    let options = Options {
        threads: 1,
        ..Options::default()
    };
    (builder.build(options).unwrap(), ty)
}

/// Block `index` of an input that is not special in any way.
fn block(ty: &SignalType, index: u64) -> Block {
    let frames = 50;
    let first = index * frames as u64;
    let data = (0..3)
        .flat_map(|channel| {
            (0..frames as u64).map(move |i| {
                let t = (first + i) as f64 * 0.013 + f64::from(channel);
                t.sin() + 0.4 * (t * 5.1).cos()
            })
        })
        .collect();
    Block {
        domain: ty.domain,
        run: RunId(1),
        first,
        frames,
        channels: Arc::clone(&ty.channels),
        data,
    }
}

/// Feeds blocks `range` and returns each output's samples as bits.
fn feed(graph: &mut Graph, ty: &SignalType, range: std::ops::Range<u64>) -> Vec<Vec<u64>> {
    let mut outputs = vec![Vec::new(); 2];
    for index in range {
        graph.tick(vec![block(ty, index)]).unwrap();
        for (output, (_, block)) in outputs.iter_mut().zip(graph.outputs()) {
            output.extend(block.data.iter().map(|v| v.to_bits()));
        }
    }
    outputs
}

fn run_of(graph: &Graph, output: &str) -> u64 {
    graph
        .output_runs()
        .find(|(name, _)| *name == output)
        .unwrap()
        .1
         .0
}

#[test]
fn a_node_that_did_not_change_keeps_its_state() {
    // Run with a gain of 2, then change only the gain, to 3.
    let (mut before, ty) = graph(LOW_PASS, 2.0);
    feed(&mut before, &ty, 0..10);
    let (mut after, _) = graph(LOW_PASS, 3.0);
    after.adopt(before);
    let changed = feed(&mut after, &ty, 10..20);

    // The filter never noticed. From the change on, both outputs are what a
    // graph with a gain of 3 all along would have made of the same samples.
    let (mut always, _) = graph(LOW_PASS, 3.0);
    feed(&mut always, &ty, 0..10);
    assert!(changed == feed(&mut always, &ty, 10..20));
    // The filter's run carried on; the gain's began again, under a number
    // no run before the change had.
    assert_eq!(run_of(&after, "filtered"), 1);
    assert_eq!(run_of(&after, "scaled"), 2);
}

#[test]
fn a_node_that_changed_starts_over_with_what_it_feeds() {
    // Change the filter itself. It starts from rest, and so the gain after
    // it sees a new run too.
    let (mut before, ty) = graph(LOW_PASS, 2.0);
    feed(&mut before, &ty, 0..10);
    let (mut after, _) = graph(NOTCH, 2.0);
    after.adopt(before);
    let changed = feed(&mut after, &ty, 10..20);

    // What a new graph makes of those samples, having seen none before.
    let (mut fresh, _) = graph(NOTCH, 2.0);
    assert!(changed == feed(&mut fresh, &ty, 10..20));
    // And not what one that had been running makes of them.
    let (mut always, _) = graph(NOTCH, 2.0);
    feed(&mut always, &ty, 0..10);
    assert!(changed != feed(&mut always, &ty, 10..20));
}

#[test]
fn a_graph_adopts_nothing_from_one_with_other_sources() {
    // The same nodes over a source at another rate share nothing.
    let (mut before, ty) = graph(LOW_PASS, 2.0);
    feed(&mut before, &ty, 0..10);

    let mut builder = GraphBuilder::new();
    let domain = builder
        .domain(Epoch::UNIX, SessionId::new(1), 500.0, 0.0)
        .unwrap();
    let other = SignalType {
        domain,
        channels: Arc::clone(&ty.channels),
    };
    let source = builder.source("a", other.clone()).unwrap();
    let filtered = builder
        .node(Biquad::cascade(vec![LOW_PASS]), &[source])
        .unwrap();
    let scaled = builder.node(Gain::new(2.0), &[filtered]).unwrap();
    builder.output("filtered", filtered).unwrap();
    builder.output("scaled", scaled).unwrap();
    let options = Options {
        threads: 1,
        ..Options::default()
    };
    let mut after = builder.build(options).unwrap();
    after.adopt(before);

    let (mut fresh, _) = graph(LOW_PASS, 2.0);
    let expected = feed(&mut fresh, &ty, 10..20);
    assert!(feed(&mut after, &other, 10..20) == expected);
}

/// A filter with memory, then a gain. Setting the gain must not disturb the
/// filter; setting the filter's edge must.
const SPEC: &str = "\
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)

param gain = 1 [0.5 .. 4]
param edge : Hz = 40 [5 .. 200]

out y : nT = (a | lowpass(edge)) * gain
";

/// Runs the spec live over a recording, setting a param partway, and returns
/// every published sample of the first channel by its index.
fn live(log: &std::path::Path, param: &str, value: f64) -> Vec<(u64, f64)> {
    let mut device = Device::new(SPEC.to_owned(), RunOptions::default()).unwrap();
    let link = device.attach();
    let batches = batches(log);
    for (index, batch) in batches.iter().enumerate() {
        if index == batches.len() / 2 {
            device
                .call(Method::ByName(param.as_bytes()), &value.to_le_bytes())
                .unwrap();
        }
        device.take(batch);
    }
    let mut parser = PacketParser::new(false);
    for packet in link.try_iter() {
        let _ = parser.push_packet(&packet);
    }
    let mut samples = Vec::new();
    for batch in parser.finish() {
        let x = batch.columns()[0].to_f64();
        for (row, value) in x.iter().enumerate() {
            let time = batch.row(row).unwrap().timestamp_begin();
            samples.push((((time - 1_700_000_000.0) * 1000.0).round() as u64, *value));
        }
    }
    samples
}

#[test]
fn setting_a_param_over_rpc_restarts_only_what_it_feeds() {
    let log = record(
        "reload",
        &[Sensor::new("/0", FAST, first_sensor(4000, 1.0, 0.01))],
    );
    let analysis = lang::analyze(SPEC);
    let offline = run_log(&analysis.program, &log, &RunOptions::default()).unwrap();
    let expected = &offline.outputs[0].runs[0].rows[0];

    // The gain comes after the filter. Setting it loses no sample, and
    // every sample after it is exactly twice what the run computes, as if
    // the filter had never been touched: it had not.
    let samples = live(&log, "dsp.gain", 2.0);
    assert_eq!(samples.len(), 4000, "samples were lost at the change");
    let mut doubled = 0;
    for (place, (index, value)) in samples.iter().enumerate() {
        assert_eq!(*index, place as u64);
        let gain = value / expected[place];
        assert!(gain == 1.0 || gain == 2.0, "sample {index} has gain {gain}");
        doubled += usize::from(gain == 2.0);
    }
    assert!(doubled > 1000 && doubled < 3000, "{doubled} were doubled");

    // The edge is the filter's own. Setting it starts the filter from rest,
    // so the samples after it are not the run's, and they settle back
    // towards a filter at the new edge.
    let samples = live(&log, "dsp.edge", 41.0);
    assert_eq!(samples.len(), 4000);
    let differs = samples
        .iter()
        .enumerate()
        .position(|(place, (_, value))| *value != expected[place])
        .unwrap();
    assert!(
        differs > 1000 && differs < 3000,
        "the change shows at {differs}"
    );
    let jump = (samples[differs].1 - expected[differs]).abs();
    let later = (samples[differs + 400].1 - expected[differs + 400]).abs();
    assert!(jump > 1.0, "restarting from rest shows as a step of {jump}");
    assert!(later < jump / 10.0, "and it settles: {later}");
}

/// A library with one more function, which builds a gain of one and
/// refuses an even argument: something only it can know to object to.
fn fussy() -> twinleaf_dsp::Library {
    use twinleaf_dsp::lang::builtins::{required, ArgKind, Builtin, Rule, Signals};
    const FUSSY: Builtin = Builtin {
        name: "fussy",
        usage: "fussy(n)",
        signals: Signals::One,
        named_signal: None,
        args: &[required("n", ArgKind::Integer { min: 1 })],
        rule: Rule::Same,
    };
    twinleaf_dsp::Library::standard()
        .with(FUSSY, |call| match call.whole("n") {
            Some(n) if n % 2 == 1 => Ok(Box::new(Gain::new(1.0))),
            n => Err(format!("{n:?} is not odd")),
        })
        .unwrap()
}

#[test]
fn a_text_that_cannot_be_built_leaves_the_running_graph_alone() {
    use twinleaf::proto::rpc::RpcError;
    use twinleaf_dsp::client::{write_spec, TransferError};

    const RUNNING: &str = "\
source a = /0/field @rate(1000) @channels(x, y, z) @unit(nT)

out y : nT = a | lowpass(40 Hz) | fussy(3)
";
    let log = record(
        "unbuildable",
        &[Sensor::new("/0", FAST, first_sensor(4000, 1.0, 0.01))],
    );
    let options = RunOptions {
        library: fussy(),
        ..RunOptions::default()
    };
    let mut device = Device::new(RUNNING.to_owned(), options).unwrap();
    let link = device.attach();
    let batches = batches(&log);
    for (index, batch) in batches.iter().enumerate() {
        if index == batches.len() / 2 {
            // It checks: 4 is a whole number. Only building it finds out.
            let unbuildable = RUNNING.replace("fussy(3)", "fussy(4)");
            let mut call =
                |name: &str, args: &[u8]| device.call(Method::ByName(name.as_bytes()), args);
            assert_eq!(
                write_spec(&mut call, &unbuildable),
                Err(TransferError::Call(RpcError::Invalid))
            );
            assert_eq!(device.source(), RUNNING);
        }
        device.take(batch);
    }

    // Why is in the log, at the call, and nothing else was said to be wrong.
    let mut parser = PacketParser::new(false);
    let mut said = Vec::new();
    for packet in link.try_iter() {
        if let twinleaf::tio::packet::Payload::Log(log) = packet.payload() {
            if log.level.value() <= 2 {
                said.push(String::from_utf8_lossy(log.message).into_owned());
            }
        }
        let _ = parser.push_packet(&packet);
    }
    assert_eq!(said.len(), 1, "{said:?}");
    assert!(
        said[0].starts_with("error[E0501]: `fussy`: Some(4) is not odd"),
        "{}",
        said[0]
    );
    assert!(said[0].contains("--> dsp.spec:3:"));

    // And the graph never noticed: every sample is there, and each is what
    // an undisturbed run of the same text makes of it.
    let analysis = lang::analyze_with(RUNNING, &fussy());
    let offline = run_log(&analysis.program, &log, &RunOptions::default()).unwrap();
    let expected = &offline.outputs[0].runs[0].rows[0];
    let published: Vec<f64> = parser
        .finish()
        .iter()
        .flat_map(|batch| batch.columns()[0].to_f64().into_owned())
        .collect();
    assert_eq!(published.len(), 4000);
    assert!(published == *expected);
}

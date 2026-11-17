//! Turning a checked program into a graph that runs.
//!
//! The checker has already typed every node, so lowering is a walk in order.
//! Each node of the program becomes a node of the graph, with its filter
//! designed for the rate it will run at and its params at the values given.
//! The checker is the authority on units. Where an engine node would label a
//! channel differently, the program's labels are put back.

use crate::design::{self, DesignError};
use crate::error::BuildError;
use crate::graph::{Graph, GraphBuilder, NodeId, Options};
use crate::lang::check::{ArgValue, CallArg, Op, Program};
use crate::nodes::{
    Add, Align, Biquad, Decorrelate, Derivative, Detrend, Gain, Integrate, Magnitude, Method, Pick,
    Psd, Relabel, Resample, Rms, Sub, ZeroPhase,
};
use crate::signal::SignalType;
use std::collections::HashMap;
use std::sync::Arc;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;

/// What only the data can say about a source, which is its time base, and
/// when its sample 0 was taken.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceFacts {
    pub epoch: Epoch,
    /// Which clock the time is told by. Sources merge only if they share one.
    pub timebase: SessionId,
    /// The time of sample 0, in seconds on `epoch`.
    pub origin: f64,
}

/// The functions with signal processing behind them. The rest check, and
/// cannot run yet.
pub const RUNNABLE: &[&str] = &[
    "align",
    "decimate",
    "resample",
    "notch",
    "bandpass",
    "lowpass",
    "highpass",
    "magnitude",
    "decorrelate",
    "rms",
    "integrate",
    "derivative",
    "detrend",
    "psd",
];

/// The functions that always need the whole of a run. A filter needs one
/// only when it is asked for `phase = zero`.
pub const WHOLE_RUN: &[&str] = &["detrend", "psd"];

/// The calls of a program that need the whole of a run before they can say
/// anything, such as a filter with `phase = zero` or a `detrend`. A
/// recording has whole runs. A live graph does not, and cannot run these.
pub fn whole_run_calls(program: &Program) -> Vec<usize> {
    program
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| match &node.op {
            Op::Call { function, args, .. } => {
                WHOLE_RUN.contains(function)
                    || args.iter().any(|arg| {
                        arg.name == "phase" && matches!(arg.value, ArgValue::Choice("zero"))
                    })
            }
            _ => false,
        })
        .map(|(index, _)| index)
        .collect()
}

/// Why a program could not be made into a graph. `node` is the node of the
/// program at fault, for pointing at its place in the spec.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LowerError {
    #[error("`{function}` is checked but cannot run yet")]
    Unsupported { node: usize, function: &'static str },
    /// A function a private build added would not make its node.
    #[error("`{function}`: {reason}")]
    Private {
        node: usize,
        function: &'static str,
        reason: String,
    },
    #[error("{error}")]
    Build { node: usize, error: BuildError },
    #[error("{error}")]
    Design { node: usize, error: DesignError },
    #[error("no ratio of small whole numbers takes {from} Hz to {to} Hz")]
    Ratio { node: usize, from: f64, to: f64 },
    #[error("the checker and the engine disagree about this signal's type")]
    Mismatch { node: usize },
    #[error("{0}")]
    Graph(BuildError),
}

impl LowerError {
    /// The node of the program the error is about.
    pub fn node(&self) -> Option<usize> {
        match self {
            Self::Unsupported { node, .. }
            | Self::Private { node, .. }
            | Self::Build { node, .. }
            | Self::Design { node, .. }
            | Self::Ratio { node, .. }
            | Self::Mismatch { node } => Some(*node),
            Self::Graph(_) => None,
        }
    }
}

/// Builds the graph for `program`. `facts` has one entry per source and
/// `values` one per param, each in the program's order.
pub fn lower(
    program: &Program,
    facts: &[SourceFacts],
    values: &[f64],
    options: Options,
) -> Result<Graph, LowerError> {
    let mut builder = GraphBuilder::new();
    let mut streams = HashMap::new();
    let mut ids: Vec<NodeId> = Vec::with_capacity(program.nodes.len());
    for (index, node) in program.nodes.iter().enumerate() {
        let build = |error| LowerError::Build { node: index, error };
        let inputs: Vec<NodeId> = node.inputs.iter().map(|&input| ids[input]).collect();
        let id = match &node.op {
            Op::Source { source } => {
                let known = facts[*source];
                let rate = program.domain(node.ty.domain).rate;
                // Sources that name one stream share its domain.
                let domain = match streams.get(&node.ty.domain) {
                    Some(domain) => *domain,
                    None => {
                        let domain = builder
                            .domain(known.epoch, known.timebase, rate, known.origin)
                            .map_err(build)?;
                        streams.insert(node.ty.domain, domain);
                        domain
                    }
                };
                let ty = SignalType {
                    domain,
                    channels: Arc::clone(&node.ty.channels),
                };
                builder.source(&program.sources[*source].name, ty)
            }
            Op::Select { name } => {
                let whole = &program.nodes[node.inputs[0]].ty.channels;
                let picks = node.ty.channels.iter().map(|channel| {
                    // Either the one channel called `name`, or the group
                    // under it with the prefix dropped.
                    if whole.position(name).is_some() {
                        (name.clone(), channel.name.clone())
                    } else {
                        (format!("{name}.{}", channel.name), channel.name.clone())
                    }
                });
                builder.node(Pick::new(picks), &inputs)
            }
            Op::Call {
                function,
                args,
                arms,
            } => {
                let rate = program.rate_of(node.inputs[0]);
                let call = Call {
                    program,
                    values,
                    args,
                    node: index,
                    rate,
                };
                call.lower(&mut builder, function, arms, &inputs, rate)?
            }
            Op::Add => builder.node(Add, &inputs),
            Op::Subtract => builder.node(Sub, &inputs),
            Op::Scale { factor, divide } => {
                let factor = program.evaluate(&factor.expr, values);
                let gain = if *divide {
                    Gain::dividing(factor)
                } else {
                    Gain::new(factor)
                };
                builder.node(gain, &inputs)
            }
            Op::Negate => builder.node(Gain::new(-1.0), &inputs),
            Op::Declare { .. } => builder.node(Relabel::to(Arc::clone(&node.ty.channels)), &inputs),
        }
        .map_err(build)?;

        // Hold the engine to what the checker decided.
        let built = builder
            .type_of(id)
            .ok_or(LowerError::Mismatch { node: index })?;
        let rate = builder.domain_info(built.domain).map(|domain| domain.rate);
        let same_names = built.channels.len() == node.ty.channels.len()
            && built
                .channels
                .iter()
                .zip(node.ty.channels.iter())
                .all(|(a, b)| a.name == b.name);
        if !same_names || rate != Some(program.rate_of(index)) {
            return Err(LowerError::Mismatch { node: index });
        }
        ids.push(if *built.channels == *node.ty.channels {
            id
        } else {
            builder
                .node(Relabel::to(Arc::clone(&node.ty.channels)), &[id])
                .map_err(build)?
        });
    }
    for output in &program.outputs {
        builder
            .output(&output.name, ids[output.node])
            .map_err(LowerError::Graph)?;
    }
    builder.build(options).map_err(LowerError::Graph)
}

/// How long after a sample is taken an output can report it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Latency {
    /// Seconds the output always waits, which is the look-ahead of every
    /// `decimate` and `resample` on the way to it.
    pub seconds: f64,
    /// Seconds more it waits when the arms of an `align` on the way do not
    /// sample at the same instants, and have to be interpolated.
    pub if_interpolated: f64,
}

/// The latency of each output, in the program's order. Only what the graph
/// itself adds is counted, not how samples travel to and from it. A filter
/// adds none. It is causal, and its delay is phase, not waiting.
pub fn latency(program: &Program) -> Vec<Latency> {
    let none = Latency {
        seconds: 0.0,
        if_interpolated: 0.0,
    };
    let mut at: Vec<Latency> = Vec::with_capacity(program.nodes.len());
    for node in &program.nodes {
        let mut worst = node
            .inputs
            .iter()
            .map(|&input| at[input])
            .fold(none, |a, b| Latency {
                seconds: a.seconds.max(b.seconds),
                if_interpolated: a.if_interpolated.max(b.if_interpolated),
            });
        if let (Op::Call { function, args, .. }, Some(&input)) = (&node.op, node.inputs.first()) {
            let rate = program.rate_of(input);
            let wait = |filter: crate::interp::Interpolator| filter.half_width() as f64 / rate;
            match *function {
                "decimate" | "resample" => {
                    let ratio = args.iter().find_map(|arg| match arg.value {
                        ArgValue::Integer(factor) => Some(Resample::decimate(factor as u64)),
                        ArgValue::Number(to) => Resample::between(rate, to),
                        _ => None,
                    });
                    if let Some(ratio) = ratio {
                        worst.seconds += wait(ratio.filter());
                    }
                }
                "align" => {
                    worst.if_interpolated += wait(crate::interp::Interpolator::for_band(0.5));
                }
                // The slope at a sample needs the sample after it.
                "derivative" => worst.seconds += 1.0 / rate,
                _ => {}
            }
        }
        at.push(worst);
    }
    program
        .outputs
        .iter()
        .map(|output| at[output.node])
        .collect()
}

/// One call of a function in a program that is being built, with what is
/// needed to read its arguments. A function a private build adds makes its
/// node from one of these.
///
/// Arguments are read by the names the function declared them under,
/// whether the spec wrote them by name or by place. One the spec left out
/// reads as `None`, and the function supplies its default. An argument that
/// follows a param reads as the param stands now. When the param is set the
/// graph is built again, the node is made again from the new value, and it
/// keeps its state only if its stage's fingerprint has not changed.
pub struct Call<'a> {
    program: &'a Program,
    values: &'a [f64],
    args: &'a [CallArg],
    node: usize,
    rate: f64,
}

impl Call<'_> {
    /// The sample rate of the signal the function is given, in hertz.
    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// The signal the function is given, as the checker typed it, with its
    /// channels and their units.
    pub fn input(&self) -> &SignalType {
        &self.program.nodes[self.program.nodes[self.node].inputs[0]].ty
    }

    /// An argument that is a number. It is a frequency in hertz, a time in
    /// seconds, or a number without a unit.
    pub fn number(&self, name: &str) -> Option<f64> {
        match self.arg(name)? {
            ArgValue::Scalar(scalar) => Some(self.program.evaluate(&scalar.expr, self.values)),
            ArgValue::Number(value) => Some(*value),
            ArgValue::Integer(value) => Some(*value as f64),
            ArgValue::Choice(_) => None,
        }
    }

    /// An argument that is a whole number.
    pub fn whole(&self, name: &str) -> Option<i64> {
        match self.arg(name)? {
            ArgValue::Integer(value) => Some(*value),
            _ => None,
        }
    }

    /// An argument that is one of a set of words.
    pub fn choice(&self, name: &str) -> Option<&'static str> {
        match self.arg(name)? {
            ArgValue::Choice(word) => Some(word),
            _ => None,
        }
    }

    fn arg(&self, name: &str) -> Option<&ArgValue> {
        self.args
            .iter()
            .find(|arg| arg.name == name)
            .map(|arg| &arg.value)
    }

    /// A number that may follow a param, or `default` when it was left out.
    fn scalar(&self, name: &str, default: f64) -> f64 {
        match self.arg(name) {
            Some(ArgValue::Scalar(scalar)) => self.program.evaluate(&scalar.expr, self.values),
            Some(ArgValue::Number(value)) => *value,
            _ => default,
        }
    }

    fn integer(&self, name: &str, default: usize) -> usize {
        match self.arg(name) {
            Some(ArgValue::Integer(value)) => *value as usize,
            _ => default,
        }
    }

    fn lower(
        &self,
        builder: &mut GraphBuilder,
        function: &'static str,
        arms: &[String],
        inputs: &[NodeId],
        rate: f64,
    ) -> Result<Result<NodeId, BuildError>, LowerError> {
        let design = |error| LowerError::Design {
            node: self.node,
            error,
        };
        let order = self.integer("order", 4);
        let sections = match function {
            "align" => return Ok(builder.node(Align::new(arms.iter().cloned()), inputs)),
            "decimate" => {
                let factor = self.integer("n", 2) as u64;
                return Ok(builder.node(Resample::decimate(factor), inputs));
            }
            "resample" => {
                let to = self.scalar("rate", rate);
                let ratio = Resample::between(rate, to).ok_or(LowerError::Ratio {
                    node: self.node,
                    from: rate,
                    to,
                })?;
                return Ok(builder.node(ratio, inputs));
            }
            "magnitude" => return Ok(builder.node(Magnitude, inputs)),
            "detrend" => {
                let degree = match self.choice("mode") {
                    Some("quadratic") => 2,
                    Some("linear") => 1,
                    _ => 0,
                };
                return Ok(builder.node(Detrend { degree }, inputs));
            }
            "psd" => {
                let window = self.scalar("window", 0.0);
                return Ok(builder.node(Psd { window }, inputs));
            }
            "integrate" => return Ok(builder.node(Integrate, inputs)),
            "derivative" => return Ok(builder.node(Derivative, inputs)),
            "rms" => {
                let window = self.scalar("window", 0.0);
                return Ok(builder.node(Rms { window }, inputs));
            }
            "decorrelate" => {
                let method = match self.choice("method") {
                    Some("nlms") => Method::Nlms,
                    Some("rls") => Method::Rls,
                    _ => Method::LeastSquares,
                };
                let node = Decorrelate {
                    method,
                    window: self.scalar("window", 0.0),
                    taps: self.integer("taps", 1),
                };
                return Ok(builder.node(node, inputs));
            }
            "notch" => design::comb(
                self.scalar("f", 0.0),
                self.scalar("q", 30.0),
                self.integer("harmonics", 1),
                rate,
            ),
            "bandpass" => {
                design::bandpass(order, self.scalar("lo", 0.0), self.scalar("hi", 0.0), rate)
            }
            "lowpass" => design::lowpass(order, self.scalar("f", 0.0), rate),
            "highpass" => design::highpass(order, self.scalar("f", 0.0), rate),
            _ => {
                // Not one of the public functions that run. A private
                // build may have added it.
                let Some(build) = self.program.library.build(function) else {
                    return Err(LowerError::Unsupported {
                        node: self.node,
                        function,
                    });
                };
                let node = build(self).map_err(|reason| LowerError::Private {
                    node: self.node,
                    function,
                    reason,
                })?;
                return Ok(builder.node(node, inputs));
            }
        }
        .map_err(design)?;
        Ok(match self.choice("phase") {
            Some("zero") => builder.node(ZeroPhase::cascade(sections), inputs),
            _ => builder.node(Biquad::cascade(sections), inputs),
        })
    }
}

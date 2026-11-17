//! A spec that is running and can be changed while it runs.
//!
//! The spec's text is the one source of truth. Setting a param does not
//! reach into the graph. It replaces the param's literal in the text, checks
//! the text again, and runs what that gives. So the text a session hands
//! back always says what is running, and a change made through a slider, an
//! RPC or an editor is the same change.
//!
//! A text that does not check is not taken. The session keeps running the
//! last one that did, and reports what was wrong with the new one.
//!
//! A change restarts only what it changes. Each node of the new text that
//! computes what a node of the old one did, from the same sources, keeps its
//! state. Retuning a band-pass resets it and what it feeds, and leaves the
//! notch ahead of it as it was.

use crate::lang::check::Op;
use crate::lang::check::Param;
use crate::lang::diag::code;
use crate::lang::{self, Diagnostic, Program};
use crate::library::Library;
use crate::lower::whole_run_calls;
use crate::run::{Emitted, OutputInfo, Pipeline, RunError, RunOptions};
use twinleaf::data::SampleBatch;

/// Why a param was not set.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SetError {
    #[error("the spec has no param `{0}`")]
    Unknown(String),
    #[error("`{name}` cannot be {value}: its range is {low} to {high}")]
    OutOfRange {
        name: String,
        value: f64,
        low: f64,
        high: f64,
    },
    #[error("`{name}` cannot be {value}")]
    NotANumber { name: String, value: f64 },
    /// The spec no longer checks with the new value, as when a frequency
    /// would pass the Nyquist limit of its signal.
    #[error("`{name}` cannot be {value}: {reason}")]
    Rejected {
        name: String,
        value: f64,
        reason: String,
    },
}

pub struct Session {
    source: String,
    options: RunOptions,
    whence: &'static str,
    pipeline: Pipeline,
    /// How many texts this session has run. A change of text is a change of
    /// outputs, as far as anyone watching them can assume.
    revision: u64,
}

impl Session {
    /// Starts a session on `source`, which must check.
    pub fn open(
        source: String,
        options: RunOptions,
        whence: &'static str,
    ) -> Result<Self, Vec<Diagnostic>> {
        // A session's params are set through its text and nowhere else.
        let options = RunOptions {
            set: Vec::new(),
            ..options
        };
        let program = checked(&source, &options.library)?;
        let pipeline = pipeline(&program, &options, whence)?;
        Ok(Self {
            source,
            options,
            whence,
            pipeline,
            revision: 1,
        })
    }

    /// The spec as it is running now.
    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn program(&self) -> &Program {
        self.pipeline.program()
    }

    pub fn params(&self) -> &[Param] {
        &self.program().params
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Replaces the spec. A text that does not check, or that checks and
    /// cannot be built over the streams the running one reads, changes
    /// nothing and is answered with what is wrong with it.
    pub fn replace(&mut self, source: String) -> Result<(), Vec<Diagnostic>> {
        let program = checked(&source, &self.options.library)?;
        let fresh = pipeline(&program, &self.options, self.whence)?;
        // What the two texts have in common carries on where it was.
        let old = std::mem::replace(&mut self.pipeline, fresh);
        if let Err(refused) = self.pipeline.adopt(old) {
            self.pipeline = refused.old;
            return Err(refused.why);
        }
        self.source = source;
        self.revision += 1;
        Ok(())
    }

    /// Sets a param by rewriting its literal in the spec.
    pub fn set_param(&mut self, name: &str, value: f64) -> Result<(), SetError> {
        let param = self
            .params()
            .iter()
            .find(|param| param.name == name)
            .ok_or_else(|| SetError::Unknown(name.to_owned()))?;
        let name = name.to_owned();
        if !value.is_finite() {
            return Err(SetError::NotANumber { name, value });
        }
        if let Some((low, high)) = param
            .range
            .filter(|(low, high)| !(*low..=*high).contains(&value))
        {
            return Err(SetError::OutOfRange {
                name,
                value,
                low,
                high,
            });
        }
        if param.value == value {
            return Ok(());
        }
        let mut source = self.source.clone();
        source.replace_range(
            param.value_span.start..param.value_span.end,
            &value.to_string(),
        );
        self.replace(source)
            .map_err(|diagnostics| SetError::Rejected {
                name,
                value,
                reason: diagnostics
                    .first()
                    .map_or_else(String::new, |d| d.message.clone()),
            })
    }

    /// Takes a batch as it arrives.
    pub fn offer(&mut self, batch: &SampleBatch) -> Result<(), RunError> {
        self.pipeline.offer(batch)
    }

    /// Runs ticks until no sample is waiting.
    pub fn deliver(&mut self) -> Result<(), RunError> {
        self.pipeline.deliver()
    }

    /// Takes what the outputs have emitted since the last call.
    pub fn drain(&mut self) -> Vec<Emitted> {
        self.pipeline.drain()
    }

    /// Whether the graph is built and taking samples.
    pub fn is_running(&self) -> bool {
        self.pipeline.is_running()
    }

    /// How many times this text's graph has been built. With the revision,
    /// it says whether the outputs are still the ones last described.
    pub fn build(&self) -> u64 {
        self.pipeline.builds()
    }

    /// Starts the running text over from nothing, as after a fault.
    pub fn restart(&mut self) {
        if let Ok(pipeline) = pipeline(self.program(), &self.options, self.whence) {
            self.pipeline = pipeline;
            self.revision += 1;
        }
    }

    /// Each output of the running graph, or none while it is not built.
    pub fn outputs(&self) -> Vec<OutputInfo> {
        self.pipeline.outputs()
    }

    /// The paths of the streams the spec reads that have not been seen.
    pub fn waiting_for(&self) -> Vec<String> {
        let mut paths: Vec<String> = self
            .pipeline
            .unbound()
            .iter()
            .map(|source| source.path.stream_path())
            .collect();
        paths.dedup();
        paths
    }
}

fn checked(source: &str, library: &Library) -> Result<Program, Vec<Diagnostic>> {
    let analysis = lang::analyze_with(source, library);
    if analysis.has_errors() {
        return Err(analysis.diagnostics);
    }
    // A session runs live: samples keep arriving, and no run is ever
    // whole. What needs a whole run would hold every sample and never
    // answer, so it is refused here, where that can be said.
    let waiting: Vec<Diagnostic> = whole_run_calls(&analysis.program)
        .into_iter()
        .map(|node| {
            let node = &analysis.program.nodes[node];
            let (what, instead) = match &node.op {
                Op::Call {
                    function: "detrend",
                    ..
                } => (
                    "`detrend`".to_owned(),
                    "to take a drift out as it arrives, use `highpass`",
                ),
                Op::Call {
                    function: "psd", ..
                } => (
                    "`psd`".to_owned(),
                    "the app draws the spectrum of any stream: plot the signal itself",
                ),
                Op::Call { function, .. } => (
                    format!("`{function}` with `phase = zero`"),
                    "here, use the filter without it, running forwards only",
                ),
                _ => ("this".to_owned(), "take it out of the spec"),
            };
            Diagnostic::error(
                code::WHOLE_RUN,
                node.span,
                format!("{what} needs a whole run, and a live graph never has one"),
            )
            .label("cannot run live")
            .note(format!(
                "it works over a recording: `{} run --log`",
                library.command()
            ))
            .help(instead)
        })
        .collect();
    if waiting.is_empty() {
        Ok(analysis.program)
    } else {
        Err(waiting)
    }
}

fn pipeline(
    program: &Program,
    options: &RunOptions,
    whence: &'static str,
) -> Result<Pipeline, Vec<Diagnostic>> {
    // The options were taken when the session was opened and name no params,
    // so this cannot fail on them.
    Pipeline::new(program, options, whence).map_err(|_| Vec::new())
}

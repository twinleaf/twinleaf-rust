//! Building a graph, expanding it into lanes, and running it.
//!
//! A graph is built node by node, each wired to nodes that already exist, so
//! it is acyclic by construction. Adding a node binds it to its inputs, which
//! type-checks it. Building expands every stage into its lane kernels and
//! sorts the stages into levels, where a stage's level is one more than the
//! deepest of its inputs.
//!
//! A tick has two parts. First, serially and without touching any sample, it
//! settles what span every stage emits and which stages start a new run.
//! Then it runs the levels in order, and the lane tasks of one level
//! concurrently. Each task writes its own slice of its stage's output block
//! and reads only blocks finished in earlier levels, so the order in which
//! tasks are scheduled cannot reach the results.

use crate::error::{BuildError, TickError};
use crate::node::{fingerprint, Coupling, Extent, Inputs, Kernel, Node, Stage};
use crate::signal::{Block, Domain, DomainId, Domains, RunId, SignalType, Span};
use rayon::prelude::*;
use std::ops::Range;
use std::sync::Arc;
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::SessionId;

/// Names a node while a graph is being built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(usize);

/// How a graph is run. None of it changes the numbers a graph produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    /// Worker threads. With one, every tick runs on the calling thread.
    pub threads: usize,
    /// Channels per lane task of an independent stage.
    pub bundle: usize,
    /// A tick whose blocks hold fewer samples than this in all runs on the
    /// calling thread, where a fork-join would cost more than the work.
    pub inline_below: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
            bundle: 2,
            inline_below: 4096,
        }
    }
}

struct Pending {
    name: String,
    stage: Option<Box<dyn Stage>>,
    merges: bool,
    inputs: Vec<usize>,
    ty: SignalType,
    level: usize,
}

/// Builds a [`Graph`] node by node.
#[derive(Default)]
pub struct GraphBuilder {
    domains: Domains,
    nodes: Vec<Pending>,
    outputs: Vec<(String, usize)>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares a rate domain.
    pub fn domain(
        &mut self,
        epoch: Epoch,
        session: SessionId,
        rate: f64,
        origin: f64,
    ) -> Result<DomainId, BuildError> {
        self.domains.add(epoch, session, rate, origin)
    }

    /// Declares a source. [`Graph::tick`] takes one block per source, in the
    /// order the sources were declared.
    pub fn source(&mut self, name: &str, ty: SignalType) -> Result<NodeId, BuildError> {
        self.domain_of(ty.domain)?;
        if self
            .nodes
            .iter()
            .any(|n| n.stage.is_none() && n.name == name)
        {
            return Err(BuildError::DuplicateSource {
                name: name.to_owned(),
            });
        }
        self.nodes.push(Pending {
            name: name.to_owned(),
            stage: None,
            merges: false,
            inputs: Vec::new(),
            ty,
            level: 0,
        });
        Ok(NodeId(self.nodes.len() - 1))
    }

    /// Adds a node fed by `inputs`, and type-checks it.
    pub fn node(&mut self, node: impl Node, inputs: &[NodeId]) -> Result<NodeId, BuildError> {
        let kind = node.kind();
        if inputs.is_empty() {
            return Err(BuildError::Arity {
                kind,
                expected: 1,
                found: 0,
            });
        }
        let mut types = Vec::with_capacity(inputs.len());
        let mut level = 0;
        for id in inputs {
            let input = self.nodes.get(id.0).ok_or(BuildError::UnknownNode)?;
            types.push(input.ty.clone());
            level = level.max(input.level + 1);
        }
        // Nothing crosses domains implicitly. Only a node that says it
        // merges them may be given more than one.
        let merges = node.merges_domains();
        let left = self.domain_of(types[0].domain)?;
        for ty in &types[1..] {
            let right = self.domain_of(ty.domain)?;
            if !merges && right.id != left.id {
                return Err(BuildError::DomainMismatch { kind, left, right });
            }
        }
        let stage = node.bind(&types, &mut self.domains)?;
        let ty = stage.output();
        self.domain_of(ty.domain)?;
        self.nodes.push(Pending {
            name: kind.to_owned(),
            stage: Some(stage),
            merges,
            inputs: inputs.iter().map(|id| id.0).collect(),
            ty,
            level,
        });
        Ok(NodeId(self.nodes.len() - 1))
    }

    /// Returns the type of the signal `node` produces.
    pub fn type_of(&self, node: NodeId) -> Option<&SignalType> {
        self.nodes.get(node.0).map(|pending| &pending.ty)
    }

    /// Returns a domain declared here or made by a node.
    pub fn domain_info(&self, id: DomainId) -> Option<&Domain> {
        self.domains.get(id)
    }

    /// Publishes the signal `node` produces as the output called `name`.
    pub fn output(&mut self, name: &str, node: NodeId) -> Result<(), BuildError> {
        if node.0 >= self.nodes.len() {
            return Err(BuildError::UnknownNode);
        }
        if self.outputs.iter().any(|(n, _)| n == name) {
            return Err(BuildError::DuplicateOutput {
                name: name.to_owned(),
            });
        }
        self.outputs.push((name.to_owned(), node.0));
        Ok(())
    }

    /// Expands the stages into lane kernels and schedules them.
    pub fn build(self, options: Options) -> Result<Graph, BuildError> {
        if options.threads == 0 {
            return Err(BuildError::InvalidOptions(
                "a graph needs at least one thread",
            ));
        }
        if options.bundle == 0 {
            return Err(BuildError::InvalidOptions(
                "a lane bundle holds at least one channel",
            ));
        }
        if self.outputs.is_empty() {
            return Err(BuildError::NoOutputs);
        }

        // Order by level. The sort is stable, so sources stay in the order
        // they were declared and fill level 0, which nothing else can reach.
        let mut ordered: Vec<(usize, Pending)> = self.nodes.into_iter().enumerate().collect();
        ordered.sort_by_key(|(_, pending)| pending.level);
        let mut place = vec![0; ordered.len()];
        for (position, (original, _)) in ordered.iter().enumerate() {
            place[*original] = position;
        }

        let mut steps: Vec<Step> = Vec::with_capacity(ordered.len());
        let mut kernels = Vec::with_capacity(ordered.len());
        let mut blocks = Vec::with_capacity(ordered.len());
        let mut levels: Vec<Range<usize>> = Vec::new();
        let mut sources = Vec::new();
        for (position, (_, pending)) in ordered.into_iter().enumerate() {
            if pending.level == levels.len() {
                levels.push(position..position);
            }
            levels[pending.level].end = position + 1;

            let inputs: Vec<usize> = pending.inputs.iter().map(|&i| place[i]).collect();
            let channels = pending.ty.channels.len();
            let mut lanes = Vec::new();
            let mut lane_channels = channels;
            let mut buffers = false;
            match &pending.stage {
                None => sources.push(position),
                Some(stage) => {
                    lane_channels = match stage.coupling() {
                        Coupling::Independent => options.bundle.min(channels),
                        Coupling::Coupled => channels,
                    };
                    buffers = stage.buffers();
                    for start in (0..channels).step_by(lane_channels) {
                        lanes.push(stage.kernel(start..(start + lane_channels).min(channels)));
                    }
                }
            }
            blocks.push(Block {
                domain: pending.ty.domain,
                run: RunId(0),
                first: 0,
                frames: 0,
                channels: Arc::clone(&pending.ty.channels),
                data: Vec::new(),
            });
            kernels.push(lanes);
            // What this step computes: its stage, what feeds it, and the
            // channels it is called by. A source is its name and its domain.
            let names: Vec<(&str, &str)> = pending
                .ty
                .channels
                .iter()
                .map(|channel| (channel.name.as_str(), channel.unit.symbol()))
                .collect();
            let made_of = match &pending.stage {
                Some(stage) => {
                    let fed_by: Vec<u64> = inputs.iter().map(|&i| steps[i].fingerprint).collect();
                    fingerprint((stage.fingerprint(), fed_by, &names))
                }
                None => {
                    let domain = self.domains.get(pending.ty.domain).copied();
                    let domain = domain.map(|d| {
                        (
                            d.epoch.value(),
                            d.session.value(),
                            d.rate.to_bits(),
                            d.origin.to_bits(),
                        )
                    });
                    fingerprint(("source", &pending.name, domain, &names))
                }
            };
            steps.push(Step {
                name: pending.name,
                stage: pending.stage,
                merges: pending.merges,
                buffers,
                since: vec![0; inputs.len()],
                inputs,
                ty: pending.ty,
                lane_channels,
                runs: 0,
                fingerprint: made_of,
                fresh: true,
            });
        }

        let pool = if options.threads > 1 {
            Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(options.threads)
                    .thread_name(|index| format!("dsp-{index}"))
                    .build()
                    .map_err(|error| BuildError::Pool(error.to_string()))?,
            )
        } else {
            None
        };

        Ok(Graph {
            domains: self.domains,
            cursors: vec![None; sources.len()],
            spans: Vec::with_capacity(steps.len()),
            restarts: Vec::with_capacity(steps.len()),
            extents: Vec::new(),
            outputs: self
                .outputs
                .into_iter()
                .map(|(name, node)| (name, place[node]))
                .collect(),
            steps,
            kernels,
            blocks,
            levels,
            sources,
            pool,
            options,
        })
    }

    fn domain_of(&self, id: DomainId) -> Result<Domain, BuildError> {
        self.domains
            .get(id)
            .copied()
            .ok_or(BuildError::UnknownDomain)
    }
}

/// One node in execution order.
struct Step {
    /// The source's name, or the node's kind.
    name: String,
    /// `None` for a source.
    stage: Option<Box<dyn Stage>>,
    /// Whether the inputs may be in different domains and cover different
    /// spans.
    merges: bool,
    /// Whether the kernels run even when the stage emits nothing.
    buffers: bool,
    /// Positions of the input steps, all in earlier levels.
    inputs: Vec<usize>,
    /// For each input, the first sample given since this step's run began.
    since: Vec<u64>,
    ty: SignalType,
    /// Channels per lane task.
    lane_channels: usize,
    /// How many runs this step has started, which numbers its output's runs.
    runs: u64,
    /// The same for two steps exactly when they compute the same thing from
    /// the same sources.
    fingerprint: u64,
    /// Whether the step has yet to run. A step that has not starts a run at
    /// its first tick, whatever its inputs are doing.
    fresh: bool,
}

/// A graph ready to run.
pub struct Graph {
    domains: Domains,
    steps: Vec<Step>,
    /// The lane kernels of each step. Empty for a source.
    kernels: Vec<Vec<Box<dyn Kernel>>>,
    /// The output block of each step, reused from tick to tick.
    blocks: Vec<Block>,
    /// The steps of each level. Level 0 is exactly the sources.
    levels: Vec<Range<usize>>,
    /// The step of each source, in declaration order.
    sources: Vec<usize>,
    /// Each source's run and the sample expected next in it.
    cursors: Vec<Option<(RunId, u64)>>,
    outputs: Vec<(String, usize)>,
    pool: Option<rayon::ThreadPool>,
    options: Options,
    // Scratch for planning a tick, kept to avoid allocating in one.
    spans: Vec<Span>,
    restarts: Vec<bool>,
    extents: Vec<Extent>,
}

impl Graph {
    /// Advances the graph by one block per source, in declaration order. A
    /// source with nothing new is given an empty block.
    ///
    /// How the input is cut into ticks does not change the output. Feeding a
    /// run in one block or sample by sample produces the same samples.
    /// A block whose `run` differs from the source's previous block starts a
    /// new run and resets every stage downstream of that source. Within a run
    /// blocks must be consecutive.
    pub fn tick(&mut self, inputs: Vec<Block>) -> Result<(), TickError> {
        self.advance(inputs, false)
    }

    /// Ends every source's run where it stands. Stages that were waiting
    /// for the whole of a run emit it, and the outputs hold what that
    /// gives. The next block of any source begins a new run, whatever run
    /// it says it is in.
    ///
    /// A recording is closed at its end. A live graph never is, which is
    /// why a stage that needs a whole run cannot be in one.
    pub fn close(&mut self) -> Result<(), TickError> {
        let ended: Vec<Block> = self
            .sources
            .iter()
            .zip(&self.cursors)
            .map(|(&step, cursor)| {
                let ty = &self.steps[step].ty;
                let (run, first) = cursor.unwrap_or((RunId(0), 0));
                Block {
                    domain: ty.domain,
                    run,
                    first,
                    frames: 0,
                    channels: Arc::clone(&ty.channels),
                    data: Vec::new(),
                }
            })
            .collect();
        self.advance(ended, true)?;
        self.cursors.fill(None);
        Ok(())
    }

    fn advance(&mut self, inputs: Vec<Block>, closing: bool) -> Result<(), TickError> {
        self.plan(&inputs, closing)?;

        // Nothing below can fail, so a refused tick has changed nothing.
        for (slot, block) in inputs.into_iter().enumerate() {
            self.cursors[slot] = Some((block.run, block.first + block.frames as u64));
            self.blocks[self.sources[slot]] = block;
        }
        for (index, step) in self.steps.iter_mut().enumerate() {
            // A source has run once it has been given a block, so that a
            // graph taking over from this one carries on from its cursor.
            step.fresh = false;
            if step.stage.is_none() {
                continue;
            }
            if self.restarts[index] {
                for kernel in &mut self.kernels[index] {
                    kernel.reset();
                }
                step.runs += 1;
                for (since, &input) in step.since.iter_mut().zip(&step.inputs) {
                    *since = self.spans[input].first;
                }
            }
            let span = self.spans[index];
            let block = &mut self.blocks[index];
            block.run = RunId(step.runs);
            block.first = span.first;
            block.frames = span.frames;
            block.data.resize(step.ty.channels.len() * span.frames, 0.0);
        }

        let work: usize = self.blocks.iter().map(|block| block.data.len()).sum();
        let parallel = self.pool.is_some() && work >= self.options.inline_below;
        let Self {
            steps,
            kernels,
            blocks,
            levels,
            pool,
            ..
        } = self;
        let mut run = || {
            for level in levels.iter().skip(1) {
                let (done, rest) = blocks.split_at_mut(level.start);
                let done: &[Block] = done;
                let current = &mut rest[..level.len()];
                let lanes = &mut kernels[level.clone()];
                let level_steps = &steps[level.clone()];
                if parallel {
                    current
                        .par_iter_mut()
                        .zip(lanes.par_iter_mut())
                        .zip(level_steps.par_iter())
                        .for_each(|((block, lanes), step)| {
                            let inputs = Inputs::new(done, &step.inputs);
                            let span = block.span();
                            let size = step.lane_channels * block.frames;
                            if size > 0 {
                                block
                                    .data
                                    .par_chunks_mut(size)
                                    .zip(lanes.par_iter_mut())
                                    .for_each(|(out, kernel)| kernel.process(&inputs, span, out));
                            } else if step.buffers {
                                lanes
                                    .par_iter_mut()
                                    .for_each(|kernel| kernel.process(&inputs, span, &mut []));
                            }
                        });
                } else {
                    for ((block, lanes), step) in current.iter_mut().zip(lanes).zip(level_steps) {
                        let inputs = Inputs::new(done, &step.inputs);
                        let span = block.span();
                        let size = step.lane_channels * block.frames;
                        if size > 0 {
                            for (out, kernel) in block.data.chunks_mut(size).zip(lanes) {
                                kernel.process(&inputs, span, out);
                            }
                        } else if step.buffers {
                            for kernel in lanes {
                                kernel.process(&inputs, span, &mut []);
                            }
                        }
                    }
                }
            }
        };
        match pool {
            Some(pool) if parallel => pool.install(run),
            _ => run(),
        }
        Ok(())
    }

    /// Checks the tick's blocks and settles, before anything is changed, what
    /// span every step emits and which steps start a new run.
    fn plan(&mut self, inputs: &[Block], closing: bool) -> Result<(), TickError> {
        if inputs.len() != self.sources.len() {
            return Err(TickError::InputCount {
                expected: self.sources.len(),
                found: inputs.len(),
            });
        }
        for (slot, block) in inputs.iter().enumerate() {
            let step = &self.steps[self.sources[slot]];
            let name = || step.name.clone();
            if block.domain != step.ty.domain {
                return Err(TickError::WrongDomain { name: name() });
            }
            if !Arc::ptr_eq(&block.channels, &step.ty.channels)
                && block.channels != step.ty.channels
            {
                return Err(TickError::WrongChannels { name: name() });
            }
            let expected = block.channels.len() * block.frames;
            if block.data.len() != expected {
                return Err(TickError::WrongLength {
                    name: name(),
                    expected,
                    found: block.data.len(),
                });
            }
            if let Some((run, next)) = self.cursors[slot] {
                if run == block.run && next != block.first {
                    return Err(TickError::Discontinuity {
                        name: name(),
                        expected: next,
                        found: block.first,
                    });
                }
            }
        }

        self.spans.clear();
        self.restarts.clear();
        let mut slot = 0;
        for step in &self.steps {
            let Some(stage) = &step.stage else {
                let block = &inputs[slot];
                self.spans.push(block.span());
                self.restarts
                    .push(self.cursors[slot].is_none_or(|(run, _)| run != block.run));
                slot += 1;
                continue;
            };
            let restart = step.fresh || step.inputs.iter().any(|&i| self.restarts[i]);
            self.extents.clear();
            for (&input, &since) in step.inputs.iter().zip(&step.since) {
                let span = self.spans[input];
                self.extents.push(Extent {
                    // A run that starts now starts at this tick's samples.
                    since: if restart { span.first } else { since },
                    first: span.first,
                    frames: span.frames,
                    closed: closing,
                });
            }
            let first = self.extents[0].span();
            if !step.merges && self.extents.iter().any(|e| e.span() != first) {
                return Err(TickError::SpanMismatch {
                    kind: step.name.clone(),
                });
            }
            self.spans.push(stage.span(&self.extents));
            self.restarts.push(restart);
        }
        Ok(())
    }

    /// Takes over from a graph that was running, when a spec is changed
    /// under it. Every step of this graph that computes what a step of
    /// `old` did, from the same sources, takes that step's state and carries
    /// on its run. The rest start over at the next tick, and so does
    /// whatever they feed.
    ///
    /// Both graphs must have been built with the same options.
    pub fn adopt(&mut self, mut old: Graph) {
        let mut taken = vec![false; old.steps.len()];
        // A step that starts over numbers its runs after every run `old`
        // made, so that an output's run changes exactly when it restarted.
        let past = old.steps.iter().map(|step| step.runs).max().unwrap_or(0);
        for (index, step) in self.steps.iter_mut().enumerate() {
            let twin = old.steps.iter().enumerate().position(|(at, other)| {
                !taken[at]
                    && other.fingerprint == step.fingerprint
                    && !other.fresh
                    && other.lane_channels == step.lane_channels
                    && old.kernels[at].len() == self.kernels[index].len()
            });
            let Some(twin) = twin else {
                step.runs = past;
                continue;
            };
            taken[twin] = true;
            std::mem::swap(&mut self.kernels[index], &mut old.kernels[twin]);
            step.runs = old.steps[twin].runs;
            step.since.clone_from(&old.steps[twin].since);
            step.fresh = false;
            // A source carries on from the sample it had reached.
            let slots = (
                self.sources.iter().position(|&source| source == index),
                old.sources.iter().position(|&source| source == twin),
            );
            if let (Some(slot), Some(old_slot)) = slots {
                self.cursors[slot] = old.cursors[old_slot];
            }
        }
    }

    /// Returns the run each output's samples are in, by output name.
    pub fn output_runs(&self) -> impl Iterator<Item = (&str, RunId)> {
        self.outputs
            .iter()
            .map(|(name, step)| (name.as_str(), RunId(self.steps[*step].runs)))
    }

    /// Returns each output's name and its block from the latest tick.
    pub fn outputs(&self) -> impl Iterator<Item = (&str, &Block)> {
        self.outputs
            .iter()
            .map(|(name, step)| (name.as_str(), &self.blocks[*step]))
    }

    /// Returns the latest block of the output called `name`.
    pub fn output(&self, name: &str) -> Option<&Block> {
        self.outputs().find(|(n, _)| *n == name).map(|(_, b)| b)
    }

    /// Returns the type of the output called `name`.
    pub fn output_type(&self, name: &str) -> Option<&SignalType> {
        self.outputs
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, step)| &self.steps[*step].ty)
    }

    /// Returns a domain of this graph.
    pub fn domain(&self, id: DomainId) -> Option<&Domain> {
        self.domains.get(id)
    }

    /// Returns each source's name and type, in the order `tick` takes blocks.
    pub fn sources(&self) -> impl Iterator<Item = (&str, &SignalType)> {
        self.sources
            .iter()
            .map(|&step| (self.steps[step].name.as_str(), &self.steps[step].ty))
    }

    /// Returns each node's kind and how many lane tasks it was expanded into,
    /// in execution order. Sources are left out.
    pub fn lane_tasks(&self) -> Vec<(&str, usize)> {
        self.steps
            .iter()
            .zip(&self.kernels)
            .filter(|(step, _)| step.stage.is_some())
            .map(|(step, lanes)| (step.name.as_str(), lanes.len()))
            .collect()
    }
}

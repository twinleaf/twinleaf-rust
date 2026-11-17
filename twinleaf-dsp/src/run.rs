//! Running a checked program over sample batches, from a recording or live.
//!
//! Batches are taken in the order they arrive, and that order is part of the
//! input. A gap in one stream resets whatever that stream feeds, and so drops
//! what the other arms of an `align` were still holding for it. How the
//! batches are then gathered into ticks is not part of the input. Samples wait
//! until `block_rows` have gathered or a gap is reached, whichever is first,
//! so everything ahead of a gap is delivered before it at any block size, and
//! the output is the same at any block size.

use crate::graph::{Graph, Options};
use crate::lang::check::{format_rate, Program, Source};
use crate::lang::diag::{code, Diagnostic};
use crate::lang::units::UnitExpr;
use crate::library::Library;
use crate::lower::{lower, SourceFacts};
use crate::signal::{Block, Channel, Domain, RunId, SignalType};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use twinleaf::data::{LogFile, PacketParser, SampleBatch, StreamKey, StreamMetadataSnapshot};
use twinleaf::proto::SessionId;

/// How a program is run.
#[derive(Debug, Clone, PartialEq)]
pub struct RunOptions {
    pub graph: Options,
    /// The most samples of one source a tick carries. It changes how fast a
    /// run goes and nothing else.
    pub block_rows: usize,
    /// Values for params, by name, in place of the ones the spec declares.
    pub set: Vec<(String, f64)>,
    /// The functions a spec may call, for whoever reads a spec's text with
    /// these options. A session, when its text is changed, checks the new
    /// one against the same functions as the old.
    pub library: Library,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            graph: Options::default(),
            block_rows: 4096,
            set: Vec::new(),
            library: Library::standard(),
        }
    }
}

/// Everything a run emitted.
#[derive(Debug, Clone, PartialEq)]
pub struct Recording {
    pub outputs: Vec<OutputTrace>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutputTrace {
    pub name: String,
    /// Samples per second.
    pub rate: f64,
    /// The time of sample 0, in seconds on the sources' epoch.
    pub origin: f64,
    pub channels: Vec<Channel>,
    /// Whether this is a spectrum. If so its samples are frequency bins,
    /// `rate` is bins per hertz, and `origin` is the first bin's frequency.
    pub spectrum: bool,
    /// The stretches of output between gaps, in order.
    pub runs: Vec<RunTrace>,
}

/// An unbroken stretch of output.
#[derive(Debug, Clone, PartialEq)]
pub struct RunTrace {
    /// The index of the first sample.
    pub first: u64,
    /// The samples of each channel.
    pub rows: Vec<Vec<f64>>,
}

impl OutputTrace {
    /// How many samples there are in all, per channel.
    pub fn frames(&self) -> usize {
        self.runs.iter().map(|run| run.rows[0].len()).sum()
    }

    /// Writes the output as CSV, one row per sample, with its index, its
    /// time on the sources' epoch, and each channel. A spectrum has a row
    /// per bin, with its frequency in hertz where the time would be.
    pub fn write_csv(&self, out: &mut impl Write) -> io::Result<()> {
        let names: Vec<&str> = self.channels.iter().map(|c| c.name.as_str()).collect();
        let axes = match self.spectrum {
            true => "bin,frequency",
            false => "sample,time",
        };
        writeln!(out, "{axes},{}", names.join(","))?;
        for run in &self.runs {
            for frame in 0..run.rows[0].len() {
                let index = run.first + frame as u64;
                write!(out, "{index},{}", self.origin + index as f64 / self.rate)?;
                for row in &run.rows {
                    write!(out, ",{}", row[frame])?;
                }
                writeln!(out)?;
            }
        }
        Ok(())
    }
}

/// Why a run did not finish.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("cannot read the recording: {0}")]
    Io(#[from] std::io::Error),
    #[error("the recording is damaged at byte {offset}: {message}")]
    Log { offset: usize, message: String },
    /// The spec does not fit the data. Each diagnostic points at the part of
    /// the spec that assumed otherwise.
    #[error("the spec does not fit the data")]
    Spec(Vec<Diagnostic>),
    #[error("{0}")]
    Setting(String),
    #[error("{0}")]
    Tick(#[from] crate::error::TickError),
}

/// The clock a stream's time is told by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timebase {
    /// The serial of the time reference, or empty when the stream names none
    /// and keeps its own device's clock.
    pub serial: String,
    pub session: SessionId,
    /// What tells this clock apart from another, which is the reference, or
    /// the device.
    pub(crate) key: String,
}

/// One output of a running pipeline, described.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputInfo {
    pub name: String,
    pub channels: Vec<Channel>,
    /// The domain its samples are in.
    pub domain: Domain,
    /// The clock that domain's time is told by.
    pub timebase: Timebase,
}

/// A block one output emitted.
#[derive(Debug, Clone, PartialEq)]
pub struct Emitted {
    /// Which output, by its place in the program.
    pub output: usize,
    /// How many times the graph had been built when this was emitted. A
    /// block from a later build is not a continuation of an earlier one,
    /// whatever its run says.
    pub build: u64,
    pub block: Block,
}

/// A text that could not take over from the one that was running.
pub struct Refused {
    /// The pipeline that was running, untouched.
    pub old: Pipeline,
    /// What is wrong with the new text, each at its place in it.
    pub why: Vec<Diagnostic>,
}

/// A program being run, with its sources bound to streams as they are seen,
/// and its graph built once every source is bound.
pub struct Pipeline {
    program: Program,
    values: Vec<f64>,
    options: Options,
    block_rows: usize,
    /// What the data comes from, for messages, such as "the recording".
    whence: &'static str,
    /// How each source is read, once its stream has been seen.
    feeds: Vec<Option<(Feed, SourceFacts)>>,
    timebases: Vec<Timebase>,
    /// The sources each stream seen so far feeds. Empty for a stream the
    /// spec does not name.
    by_stream: HashMap<StreamKey, Vec<usize>>,
    runner: Option<Runner>,
    builds: u64,
    emitted: Vec<Emitted>,
}

impl Pipeline {
    /// Prepares to run `program`. `whence` names where the data comes from
    /// in messages, as in "the recording".
    pub fn new(
        program: &Program,
        options: &RunOptions,
        whence: &'static str,
    ) -> Result<Self, RunError> {
        let mut values = program.declared();
        for (name, value) in &options.set {
            let Some(index) = program.params.iter().position(|param| param.name == *name) else {
                return Err(RunError::Setting(format!("the spec has no param `{name}`")));
            };
            if let Some((low, high)) = program.params[index].range {
                if !(low..=high).contains(value) {
                    return Err(RunError::Setting(format!(
                        "`{name}` cannot be {value}: its range is {low} to {high}"
                    )));
                }
            }
            values[index] = *value;
        }
        Ok(Self {
            program: program.clone(),
            values,
            options: options.graph,
            block_rows: options.block_rows.max(1),
            whence,
            feeds: vec![None; program.sources.len()],
            timebases: Vec::new(),
            by_stream: HashMap::new(),
            runner: None,
            builds: 0,
            emitted: Vec::new(),
        })
    }

    pub fn program(&self) -> &Program {
        &self.program
    }

    /// The value each param is running at, in the program's order.
    pub fn values(&self) -> &[f64] {
        &self.values
    }

    /// Tells the pipeline about a stream. Every source that names it is bound
    /// to it, or reported as not fitting it, with the source's place.
    pub fn describe(
        &mut self,
        key: StreamKey,
        metadata: &StreamMetadataSnapshot,
    ) -> Vec<(usize, Diagnostic)> {
        let route = key.route.to_string();
        let name = metadata.schema().stream().name().to_owned();
        let slots: Vec<usize> = self
            .program
            .sources
            .iter()
            .enumerate()
            .filter(|(_, source)| route_of(source) == route && source.path.stream == name)
            .map(|(slot, _)| slot)
            .collect();
        let mut problems = Vec::new();
        for &slot in &slots {
            match &self.feeds[slot] {
                // Seen before. Only the rate can have changed under it.
                Some((feed, _)) if feed.rate == metadata.rate_hz() => {}
                Some(_) => problems.push((slot, self.rate_problem(slot, metadata.rate_hz()))),
                None => match self.bind(slot, key, metadata) {
                    Ok(bound) => self.feeds[slot] = Some(bound),
                    Err(problem) => problems.push((slot, problem)),
                },
            }
        }
        self.by_stream.insert(key, slots);
        problems
    }

    /// The sources no stream has been seen for yet.
    pub fn unbound(&self) -> Vec<&Source> {
        self.program
            .sources
            .iter()
            .zip(&self.feeds)
            .filter(|(_, feed)| feed.is_none())
            .map(|(source, _)| source)
            .collect()
    }

    /// Whether the graph is built and taking samples.
    pub fn is_running(&self) -> bool {
        self.runner.is_some()
    }

    /// How many times the graph has been built.
    pub fn builds(&self) -> u64 {
        self.builds
    }

    /// Builds the graph. Every source must be bound.
    pub fn start(&mut self) -> Result<(), RunError> {
        let facts: Vec<SourceFacts> = self
            .feeds
            .iter()
            .map(|feed| feed.as_ref().map(|(_, facts)| *facts))
            .collect::<Option<_>>()
            .expect("every source is bound before the graph is built");
        let graph = lower(&self.program, &facts, &self.values, self.options).map_err(|error| {
            let span = error.node().map_or(crate::lang::Span::at(0), |node| {
                self.program.nodes[node].span
            });
            RunError::Spec(vec![runtime(span, error.to_string())])
        })?;
        let types: Vec<SignalType> = graph.sources().map(|(_, ty)| ty.clone()).collect();
        self.builds += 1;
        self.runner = Some(Runner {
            pending: types
                .iter()
                .map(|ty| Pending::new(ty.channels.len()))
                .collect(),
            graph,
            types,
            block_rows: self.block_rows,
            build: self.builds,
            whole_runs: !crate::lower::whole_run_calls(&self.program).is_empty(),
        });
        Ok(())
    }

    /// Takes over from a pipeline that was running an earlier text of the
    /// spec. If this text reads the sources that one read, their streams
    /// stay bound and no sample is let go while the graph is rebuilt, and
    /// every node the two texts have in common keeps its state.
    ///
    /// If that pipeline was running and this text cannot be built over the
    /// same streams, nothing is taken. The old pipeline is handed back as
    /// it was, with what was wrong with the new text.
    pub fn adopt(&mut self, old: Pipeline) -> Result<(), Box<Refused>> {
        self.builds = old.builds;
        let same_sources = self.program.sources.len() == old.program.sources.len()
            && self
                .program
                .sources
                .iter()
                .zip(&old.program.sources)
                .all(|(new, was)| {
                    new.name == was.name
                        && new.path == was.path
                        && self.program.nodes[new.node].ty.channels
                            == old.program.nodes[was.node].ty.channels
                        && self.program.rate_of(new.node) == old.program.rate_of(was.node)
                });
        if !same_sources {
            return Ok(());
        }
        self.feeds.clone_from(&old.feeds);
        self.timebases.clone_from(&old.timebases);
        self.by_stream.clone_from(&old.by_stream);
        if old.runner.is_none() {
            return Ok(());
        }
        if self.unbound().is_empty() {
            match self.start() {
                Ok(()) => {}
                Err(RunError::Spec(why)) => return Err(Box::new(Refused { old, why })),
                // Building reads no data, so it fails in no other way.
                Err(_) => {
                    return Err(Box::new(Refused {
                        old,
                        why: Vec::new(),
                    }))
                }
            }
        }
        if let (Some(runner), Some(was)) = (&mut self.runner, old.runner) {
            runner.graph.adopt(was.graph);
            runner.pending = was.pending;
        }
        Ok(())
    }

    /// Each output of the running graph, or none while it is not built.
    pub fn outputs(&self) -> Vec<OutputInfo> {
        let Some(runner) = &self.runner else {
            return Vec::new();
        };
        runner
            .graph
            .outputs()
            .map(|(name, block)| {
                let domain = *runner
                    .graph
                    .domain(block.domain)
                    .expect("an output has a domain");
                OutputInfo {
                    name: name.to_owned(),
                    channels: block.channels.iter().cloned().collect(),
                    domain,
                    timebase: self.timebases[domain.session.value() as usize - 1].clone(),
                }
            })
            .collect()
    }

    /// Takes a batch as it arrives. A stream seen for the first time is
    /// bound, and the graph is built once every source has one. Samples that
    /// arrive before then are let go, as they would be before a device was
    /// plugged in.
    pub fn offer(&mut self, batch: &SampleBatch) -> Result<(), RunError> {
        let key = batch.stream_key();
        let rate = batch.metadata().rate_hz();
        let changed = self.by_stream.get(&key).is_some_and(|slots| {
            slots.iter().any(|&slot| {
                self.feeds[slot]
                    .as_ref()
                    .is_some_and(|(feed, _)| feed.rate != rate)
            })
        });
        if changed {
            // The stream was reconfigured. What was built for the old rate
            // is over; start again from what the stream is now.
            self.deliver()?;
            self.runner = None;
            for slot in self.by_stream.remove(&key).unwrap_or_default() {
                self.feeds[slot] = None;
            }
        }
        if !self.by_stream.contains_key(&key) {
            let problems = self.describe(key, batch.metadata());
            if !problems.is_empty() {
                return Err(RunError::Spec(
                    problems.into_iter().map(|(_, d)| d).collect(),
                ));
            }
        }
        if self.runner.is_none() {
            if !self.unbound().is_empty() {
                return Ok(());
            }
            self.start()?;
        }
        let slots = &self.by_stream[&key];
        if slots.is_empty() {
            return Ok(());
        }
        let feeds: Vec<&Feed> = slots
            .iter()
            .map(|&slot| {
                &self.feeds[slot]
                    .as_ref()
                    .expect("a known stream is bound")
                    .0
            })
            .collect();
        let runner = self.runner.as_mut().expect("the graph was just built");
        runner.take(batch, &feeds, &mut self.emitted)
    }

    /// Runs ticks until no sample is waiting.
    pub fn deliver(&mut self) -> Result<(), RunError> {
        match &mut self.runner {
            Some(runner) => runner.deliver(&mut self.emitted),
            None => Ok(()),
        }
    }

    /// Ends the run every source is in. There is no more to come, as at the
    /// end of a recording. Stages that needed the whole of a run emit it.
    pub fn close(&mut self) -> Result<(), RunError> {
        match &mut self.runner {
            Some(runner) => runner.close(&mut self.emitted),
            None => Ok(()),
        }
    }

    /// Takes what the outputs have emitted since the last call.
    pub fn drain(&mut self) -> Vec<Emitted> {
        std::mem::take(&mut self.emitted)
    }

    fn rate_problem(&self, slot: usize, recorded: f64) -> Diagnostic {
        let source = &self.program.sources[slot];
        runtime(
            source.span,
            format!(
                "`{}` is declared at {}, and {} has `{}` at {}",
                source.name,
                format_rate(self.program.rate_of(source.node)),
                self.whence,
                source.path.stream_path(),
                format_rate(recorded)
            ),
        )
    }

    /// Works out how a source is read from a stream.
    // The error is built once, on a path taken once, and kept as it is.
    #[allow(clippy::result_large_err)]
    fn bind(
        &mut self,
        slot: usize,
        key: StreamKey,
        metadata: &StreamMetadataSnapshot,
    ) -> Result<(Feed, SourceFacts), Diagnostic> {
        let source = &self.program.sources[slot];
        let ty = &self.program.nodes[source.node].ty;
        let declared = self.program.rate_of(source.node);
        let path = source.path.stream_path();
        let whence = self.whence;
        if metadata.rate_hz() != declared {
            return Err(self.rate_problem(slot, metadata.rate_hz()));
        }

        let recorded: Vec<(String, String)> = metadata
            .schema()
            .columns()
            .map(|column| (column.name().to_owned(), column.units().to_owned()))
            .collect();
        let mut columns = Vec::with_capacity(ty.channels.len());
        for channel in ty.channels.iter() {
            let Some(position) = recorded.iter().position(|(name, _)| *name == channel.name) else {
                let names: Vec<&str> = recorded.iter().map(|(name, _)| name.as_str()).collect();
                return Err(runtime(
                    source.span,
                    format!("{whence}'s `{path}` has no column `{}`", channel.name),
                )
                .note(format!("it has: {}", names.join(", "))));
            };
            // Compared as units, so that two spellings of one are one.
            let unit = &recorded[position].1;
            let same = UnitExpr::parse(unit).is_some_and(|unit| unit.label() == channel.unit);
            if !channel.unit.is_none() && !unit.is_empty() && !same {
                return Err(runtime(
                    source.span,
                    format!(
                        "`{}` is declared in {}, and {whence} has `{path}/{}` in {unit}",
                        source.name, channel.unit, channel.name
                    ),
                ));
            }
            columns.push(position);
        }

        // Streams share a time base when they name the same time reference.
        // One that names none keeps its own device's clock.
        let segment = metadata.segment();
        let timebase = if segment.timeref_session().value() != 0 {
            Timebase {
                serial: segment.timeref_serial().to_owned(),
                session: segment.timeref_session(),
                key: format!("{}#{}", segment.timeref_serial(), segment.timeref_session()),
            }
        } else {
            Timebase {
                serial: String::new(),
                session: SessionId::new(0),
                key: format!("device {}#{}", key.route, metadata.device().session()),
            }
        };
        let number = match self
            .timebases
            .iter()
            .position(|known| known.key == timebase.key)
        {
            Some(position) => position,
            None => {
                self.timebases.push(timebase);
                self.timebases.len() - 1
            }
        };
        let facts = SourceFacts {
            epoch: segment.epoch(),
            timebase: SessionId::new(number as u32 + 1),
            origin: f64::from(segment.start_time()),
        };
        let feed = Feed {
            slot,
            columns,
            rate: declared,
            start: segment.start_time(),
        };
        Ok((feed, facts))
    }
}

/// Runs `program` over the recording at `path`.
pub fn run_log(
    program: &Program,
    path: &Path,
    options: &RunOptions,
) -> Result<Recording, RunError> {
    let log = LogFile::open(path)?;
    let index = log.scan(false);
    let streams = index.summary().streams();
    let mut pipeline = Pipeline::new(program, options, "the recording")?;

    // Bind every source before the first sample, so that none is let go
    // while another source's stream has yet to appear.
    let mut problems = Vec::new();
    for (key, runs) in streams {
        for run in runs {
            problems.extend(pipeline.describe(*key, run.metadata()));
        }
    }
    let unbound: Vec<String> = pipeline
        .unbound()
        .iter()
        .map(|source| source.name.clone())
        .collect();
    for (slot, source) in program.sources.iter().enumerate() {
        if !unbound.contains(&source.name) || problems.iter().any(|(known, _)| *known == slot) {
            continue;
        }
        let mut present: Vec<String> = streams
            .iter()
            .filter_map(|(key, runs)| {
                let name = runs.first()?.metadata().schema().stream().name().to_owned();
                Some(format!(
                    "{}/{name}",
                    key.route.to_string().trim_end_matches('/')
                ))
            })
            .collect();
        present.dedup();
        let missing = runtime(
            source.span,
            format!(
                "the recording has no stream `{}`",
                source.path.stream_path()
            ),
        );
        problems.push((
            slot,
            missing.note(if present.is_empty() {
                "it has no streams at all".to_owned()
            } else {
                format!("it has: {}", present.join(", "))
            }),
        ));
    }
    if !problems.is_empty() {
        problems.sort_by_key(|(slot, _)| *slot);
        problems.dedup_by(|a, b| a.0 == b.0 && a.1.message == b.1.message);
        return Err(RunError::Spec(
            problems.into_iter().map(|(_, d)| d).collect(),
        ));
    }
    pipeline.start()?;

    let mut recording = Recording {
        outputs: pipeline
            .outputs()
            .into_iter()
            .zip(&program.outputs)
            .map(|(output, written)| OutputTrace {
                name: output.name,
                rate: output.domain.rate,
                origin: output.domain.origin,
                channels: output.channels,
                spectrum: program.is_spectrum(written.node),
                runs: Vec::new(),
            })
            .collect(),
    };
    let mut last_run: Vec<Option<RunId>> = vec![None; recording.outputs.len()];
    let mut collect = |pipeline: &mut Pipeline| {
        for emitted in pipeline.drain() {
            let output = &mut recording.outputs[emitted.output];
            let block = emitted.block;
            if last_run[emitted.output] != Some(block.run) {
                last_run[emitted.output] = Some(block.run);
                output.runs.push(RunTrace {
                    first: block.first,
                    rows: vec![Vec::new(); block.channels.len()],
                });
            }
            let run = output.runs.last_mut().expect("a run was just pushed");
            for (channel, row) in run.rows.iter_mut().enumerate() {
                row.extend_from_slice(block.channel(channel));
            }
        }
    };

    let mut parser = PacketParser::new(false);
    let mut packets = log.packets();
    loop {
        let offset = packets.position();
        let Some(packet) = packets.next() else { break };
        let packet = packet.map_err(|error| RunError::Log {
            offset,
            message: error.to_string(),
        })?;
        // A packet the parser refuses loses its rows and nothing else, as it
        // would have live. The gap it leaves is reported by the rows after.
        if parser.push_packet(&packet).is_err() {
            continue;
        }
        parser.flush();
        while let Some(batch) = parser.pop_batch() {
            pipeline.offer(&batch)?;
        }
        collect(&mut pipeline);
    }
    // The recording is over, and so is every run in it.
    pipeline.close()?;
    collect(&mut pipeline);
    Ok(recording)
}

/// A diagnostic raised once the data is in hand, which still points at the
/// part of the spec that assumed otherwise.
fn runtime(span: crate::lang::Span, message: String) -> Diagnostic {
    let mut diagnostic = Diagnostic::error(code::RECORDING, span, message);
    diagnostic.runtime = true;
    diagnostic
}

fn route_of(source: &Source) -> String {
    if source.path.route.is_empty() {
        return "/".to_owned();
    }
    source
        .path
        .route
        .iter()
        .map(|hop| format!("/{hop}"))
        .collect()
}

/// How one source is read out of its stream.
#[derive(Debug, Clone)]
struct Feed {
    slot: usize,
    /// Which column of the stream each of the source's channels is.
    columns: Vec<usize>,
    rate: f64,
    /// The start, in whole seconds, of the segment sample 0 is counted from.
    start: u32,
}

/// One source's samples waiting for a tick.
struct Pending {
    /// The stream's own count of its runs, as of the last batch.
    generation: Option<u32>,
    /// This source's count of unbroken stretches, which numbers its runs.
    run: RunId,
    /// The index of the first waiting sample, or of the next one to come.
    first: u64,
    rows: Vec<Vec<f64>>,
}

impl Pending {
    fn new(channels: usize) -> Self {
        Self {
            generation: None,
            run: RunId(0),
            first: 0,
            rows: vec![Vec::new(); channels],
        }
    }

    fn len(&self) -> usize {
        self.rows[0].len()
    }
}

/// A built graph and the samples waiting to go into it.
struct Runner {
    graph: Graph,
    types: Vec<SignalType>,
    pending: Vec<Pending>,
    block_rows: usize,
    build: u64,
    /// Whether some stage needs whole runs. A gap in any source then ends
    /// the run for all of them, so that what came before it is emitted.
    whole_runs: bool,
}

impl Runner {
    /// Takes a batch of one stream for the sources that read it.
    fn take(
        &mut self,
        batch: &SampleBatch,
        feeds: &[&Feed],
        emitted: &mut Vec<Emitted>,
    ) -> Result<(), RunError> {
        if batch.is_empty() {
            return Ok(());
        }
        let generation = Some(batch.generations().stream);
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| column.to_f64())
            .collect();
        for feed in feeds {
            // Sample numbers count from the start of a segment, and segments
            // start on whole seconds.
            let segment = batch.metadata().segment().start_time();
            let offset = (f64::from(segment) - f64::from(feed.start)) * feed.rate;
            let first = offset.round() as i64 + i64::from(batch.sample_numbers()[0].value());
            let first = first.max(0) as u64;

            // A stretch ends where the stream says it does, and also wherever
            // the next sample is not the one expected, as when batches were
            // dropped on the way here.
            let waiting = &self.pending[feed.slot];
            let continues =
                waiting.generation == generation && waiting.first + waiting.len() as u64 == first;
            if !continues {
                // A gap. Everything ahead of it goes first.
                self.deliver(emitted)?;
                if self.whole_runs && self.pending[feed.slot].generation.is_some() {
                    self.close(emitted)?;
                }
                let waiting = &mut self.pending[feed.slot];
                waiting.generation = generation;
                waiting.run = RunId(waiting.run.0 + 1);
                waiting.first = first;
            }
            let waiting = &mut self.pending[feed.slot];
            for (row, &column) in waiting.rows.iter_mut().zip(&feed.columns) {
                row.extend_from_slice(&columns[column]);
            }
        }
        if feeds
            .iter()
            .any(|feed| self.pending[feed.slot].len() >= self.block_rows)
        {
            self.deliver(emitted)?;
        }
        Ok(())
    }

    /// Ends the run every source is in, after everything waiting has gone
    /// in, and takes what the stages that were waiting for that emit.
    fn close(&mut self, emitted: &mut Vec<Emitted>) -> Result<(), RunError> {
        self.deliver(emitted)?;
        self.graph.close()?;
        self.collect(emitted);
        Ok(())
    }

    fn collect(&self, emitted: &mut Vec<Emitted>) {
        for (output, (_, block)) in self.graph.outputs().enumerate() {
            if block.frames > 0 {
                emitted.push(Emitted {
                    output,
                    build: self.build,
                    block: block.clone(),
                });
            }
        }
    }

    /// Runs ticks until nothing is waiting.
    fn deliver(&mut self, emitted: &mut Vec<Emitted>) -> Result<(), RunError> {
        while self.pending.iter().any(|waiting| waiting.len() > 0) {
            let blocks = self
                .pending
                .iter_mut()
                .zip(&self.types)
                .map(|(waiting, ty)| {
                    let frames = waiting.len().min(self.block_rows);
                    let mut data = Vec::with_capacity(frames * waiting.rows.len());
                    for row in &mut waiting.rows {
                        data.extend(row.drain(..frames));
                    }
                    let block = Block {
                        domain: ty.domain,
                        run: waiting.run,
                        first: waiting.first,
                        frames,
                        channels: Arc::clone(&ty.channels),
                        data,
                    };
                    waiting.first += frames as u64;
                    block
                })
                .collect();
            self.graph.tick(blocks)?;
            self.collect(emitted);
        }
        Ok(())
    }
}

//! The command line. It checks a spec, draws it, runs it over a recording,
//! or serves it as a device.
//!
//! `tio dsp` is this command line for the public functions. It is here, in
//! the library, so that a private build is the same program with more
//! functions. Its `main` makes a [`Library`], adds to it, and calls [`run`].
//! Everything a user of `tio dsp` knows then holds for it, under its own
//! name.
//!
//! The command line is parsed by clap into [`DspCli`]. The doc comments of
//! these types are the `--help` text.

use crate::figure::{self, Format};
use crate::lang::{self, check::format_rate, diag, Analysis, Severity};
use crate::library::Library;
use crate::run::{run_log, RunError, RunOptions};
use crate::serve::{MountSpec, ServeError, ServeOptions, Server};
use clap::{ArgGroup, Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use clap::{Command, ValueHint};
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// The exit status of a command whose spec has an error, or does not fit
/// what it was run over.
pub const FAILED: u8 = 1;

/// The exit status of a command line that was wrong, or of a file that
/// could not be read or written. It is the status clap exits with too.
pub const USAGE_ERROR: u8 = 2;

/// The arguments of `tio dsp`, and of a private build's own command.
#[derive(Args, Debug, Clone)]
pub struct DspCli {
    #[command(subcommand)]
    pub command: DspCommand,
}

#[derive(Subcommand, Debug, Clone)]
pub enum DspCommand {
    /// Parse and check a spec, and exit non-zero if it has an error
    Check(CheckCli),

    /// List the functions a spec may call
    Functions,

    /// Draw a spec's graph as Graphviz DOT, a Typst figure, a PDF or an SVG
    Graph(GraphCli),

    /// Run a spec over a recording
    Run(RunCli),

    /// Serve sensors and a spec's outputs as one device tree
    Serve(ServeCli),
}

/// The arguments of `check`.
#[derive(Args, Debug, Clone)]
pub struct CheckCli {
    /// The spec to check
    #[arg(value_hint = ValueHint::FilePath)]
    pub spec: String,
}

/// The arguments of `graph`. Exactly one of the forms is asked for.
#[derive(Args, Debug, Clone)]
#[command(group = ArgGroup::new("form").required(true).multiple(false))]
pub struct GraphCli {
    /// The spec to draw
    #[arg(value_hint = ValueHint::FilePath)]
    pub spec: String,

    /// Print the graph in Graphviz DOT
    #[arg(long, group = "form")]
    pub dot: bool,

    /// Print the graph as a Typst figure, to include or edit
    #[arg(long, group = "form")]
    pub typst: bool,

    /// Compile that figure into a PDF file
    #[arg(long, value_name = "FILE", group = "form", value_hint = ValueHint::FilePath)]
    pub pdf: Option<PathBuf>,

    /// Compile that figure into an SVG file
    #[arg(long, value_name = "FILE", group = "form", value_hint = ValueHint::FilePath)]
    pub svg: Option<PathBuf>,

    /// A directory to find the figure's Typst packages in, laid out as `preview/<name>/<version>`. Those not there are looked for where Typst keeps them, and then fetched from Typst Universe
    #[arg(
        long,
        value_name = "DIR",
        value_hint = ValueHint::DirPath,
        conflicts_with_all = ["dot", "typst"],
    )]
    pub packages: Option<PathBuf>,
}

/// The arguments of `run`.
#[derive(Args, Debug, Clone)]
pub struct RunCli {
    /// The spec to run
    #[arg(value_hint = ValueHint::FilePath)]
    pub spec: String,

    /// The recording to run it over
    #[arg(long, value_name = "RECORDING", value_hint = ValueHint::FilePath)]
    pub log: PathBuf,

    /// Write the output as CSV. With more than one output, each goes to a file with its name worked in
    #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath)]
    pub csv: Option<PathBuf>,

    /// Threads to run the graph on. This changes how fast a run goes and never its numbers
    #[arg(long, value_name = "N", value_parser = count)]
    pub threads: Option<usize>,

    /// The most samples of one source in a block. This changes how fast a run goes and never its numbers
    #[arg(long, value_name = "SAMPLES", value_parser = count)]
    pub block: Option<usize>,

    /// Give a param a value other than the one the spec declares (repeatable)
    #[arg(long = "set", value_name = "PARAM=VALUE", value_parser = setting)]
    pub set: Vec<(String, f64)>,
}

/// The arguments of `serve`.
#[derive(Args, Debug, Clone)]
pub struct ServeCli {
    /// The spec to serve
    #[arg(value_hint = ValueHint::FilePath)]
    pub spec: String,

    /// A sensor to mount (repeatable). Sensors go at `/0`, `/1` and so on in the order given, or at the `/N` asked for. A sensor behind a hub is named by its route there, as in `URL=/0@/1`
    #[arg(
        long = "mount",
        value_name = "URL[=/N[@/ROUTE]]",
        required = true,
        value_hint = ValueHint::Url,
    )]
    pub mounts: Vec<String>,

    /// TCP port to listen on for clients. Zero takes any free port
    #[arg(short = 'p', long = "port", default_value_t = 0)]
    pub port: u16,

    /// Interfaces to listen on for clients
    #[arg(long = "listen", value_enum, default_value_t = ListenOn::Loopback)]
    pub listen: ListenOn,

    /// Threads to run the graph on
    #[arg(long, value_name = "N", value_parser = count)]
    pub threads: Option<usize>,
}

/// The interfaces a served graph listens on, as `--listen` names them.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenOn {
    /// This machine only, on loopback
    Loopback,
    /// Every interface, so other machines can connect too
    All,
}

/// Reads a whole number above zero.
fn count(text: &str) -> Result<usize, String> {
    text.parse::<usize>()
        .ok()
        .filter(|count| *count > 0)
        .ok_or_else(|| format!("a whole number above zero is wanted, not `{text}`"))
}

/// Reads `param=number`.
fn setting(text: &str) -> Result<(String, f64), String> {
    text.split_once('=')
        .filter(|(name, _)| !name.is_empty())
        .and_then(|(name, value)| Some((name.to_owned(), value.parse().ok()?)))
        .ok_or_else(|| format!("`param=number` is wanted, not `{text}`"))
}

/// The command line of a build that is nothing else, as a private one is.
#[derive(Parser, Debug)]
#[command(
    about = "Check, draw, run and serve signal graphs",
    disable_help_subcommand = true
)]
struct Standalone {
    #[command(flatten)]
    dsp: DspCli,
}

/// Where a command writes. A program hands over its standard streams, and a
/// test hands over buffers it can read back.
pub struct Console<'a> {
    pub out: &'a mut dyn Write,
    pub err: &'a mut dyn Write,
}

/// Writes a line to one side of a [`Console`]. A stream that has gone away
/// is not something a command can report, so the failure is dropped.
macro_rules! say {
    ($to:expr, $($arg:tt)*) => {{
        let _ = writeln!($to, $($arg)*);
    }};
}

/// Runs the command line the process was started with, for a build whose
/// specs may call the functions in `library`.
///
/// This is the whole `main` of a private build. A command line that does
/// not parse is reported by clap, which ends the process.
pub fn run(library: Library) -> ExitCode {
    let cli = match parse(&library, std::env::args_os()) {
        Ok(cli) => cli,
        Err(error) => error.exit(),
    };
    ExitCode::from(run_command(&library, cli))
}

/// Parses a command line for a build with `library`. The first of `args` is
/// the program's name, as the operating system gives it.
///
/// # Errors
///
/// Returns clap's error if the command line is wrong, or if it asks for
/// help, which clap also reports this way.
pub fn parse<I, T>(library: &Library, args: I) -> Result<DspCli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let command: Command = Standalone::command()
        .name(library.command())
        .bin_name(library.command());
    let matches = command.try_get_matches_from(args)?;
    Ok(Standalone::from_arg_matches(&matches)?.dsp)
}

/// Runs one command on the process's own standard streams and returns its
/// exit status.
pub fn run_command(library: &Library, cli: DspCli) -> u8 {
    let mut out = std::io::stdout();
    let mut err = std::io::stderr();
    let mut console = Console {
        out: &mut out,
        err: &mut err,
    };
    execute(library, cli, &mut console)
}

/// Runs one command and returns its exit status. That is zero if it went
/// well, [`FAILED`] if the spec is at fault, and [`USAGE_ERROR`] if the
/// command line or a file is.
pub fn execute(library: &Library, cli: DspCli, console: &mut Console<'_>) -> u8 {
    match cli.command {
        DspCommand::Check(check) => {
            let Some(spec) = Spec::read(library, &check.spec, console) else {
                return USAGE_ERROR;
            };
            let Some(analysis) = spec.check(library, console) else {
                return FAILED;
            };
            let _ = write!(console.out, "{}", summary(&spec.name, &analysis));
            0
        }
        DspCommand::Functions => {
            let _ = write!(console.out, "{}", functions(library));
            0
        }
        DspCommand::Graph(graph) => draw(library, &graph, console),
        DspCommand::Run(run) => over_log(library, &run, console),
        DspCommand::Serve(serve) => self::serve(library, &serve, console),
    }
}

/// The functions of a library, each as it is written. One that is checked
/// and cannot run yet says so.
fn functions(library: &Library) -> String {
    let mut out = String::new();
    for function in library.functions() {
        let runs = library.runs(function.name);
        let note = if runs {
            ""
        } else {
            "   (checked; cannot run yet)"
        };
        out.push_str(&format!("  {}{note}\n", function.usage));
    }
    out
}

/// A spec that was read.
struct Spec {
    /// The file as the command line named it, which is how messages name it.
    name: String,
    source: String,
}

impl Spec {
    /// Reads a spec, or says why it could not be read.
    fn read(library: &Library, name: &str, console: &mut Console<'_>) -> Option<Self> {
        match std::fs::read_to_string(name) {
            Ok(source) => Some(Self {
                name: name.to_owned(),
                source,
            }),
            Err(error) => {
                say!(
                    console.err,
                    "{}: cannot read {name}: {error}",
                    library.command()
                );
                None
            }
        }
    }

    /// Checks the spec and writes its diagnostics. Returns the analysis if
    /// it has no errors.
    fn check(&self, library: &Library, console: &mut Console<'_>) -> Option<Analysis> {
        let name = &self.name;
        let analysis = lang::analyze_with(&self.source, library);
        if !analysis.diagnostics.is_empty() {
            say!(
                console.err,
                "{}",
                diag::render_all(&analysis.diagnostics, name, &self.source)
            );
        }
        let count = |severity| {
            analysis
                .diagnostics
                .iter()
                .filter(|d| d.severity == severity)
                .count()
        };
        let (errors, warnings) = (count(Severity::Error), count(Severity::Warning));
        if errors > 0 {
            say!(
                console.err,
                "error: {name} does not check: {}{}",
                plural(errors, "error"),
                if warnings > 0 {
                    format!(", {}", plural(warnings, "warning"))
                } else {
                    String::new()
                }
            );
            return None;
        }
        if warnings > 0 {
            say!(
                console.err,
                "warning: {name}: {}",
                plural(warnings, "warning")
            );
        }
        Some(analysis)
    }

    /// The name of the file without its directory or extension.
    fn stem(&self) -> &str {
        Path::new(&self.name)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("spec")
    }
}

/// Writes a spec's graph in the form asked for.
fn draw(library: &Library, graph: &GraphCli, console: &mut Console<'_>) -> u8 {
    let Some(spec) = Spec::read(library, &graph.spec, console) else {
        return USAGE_ERROR;
    };
    let Some(analysis) = spec.check(library, console) else {
        return FAILED;
    };
    let program = &analysis.program;
    let (format, file) = match (&graph.pdf, &graph.svg) {
        (Some(file), _) => (Format::Pdf, file),
        (None, Some(file)) => (Format::Svg, file),
        (None, None) => {
            let text = if graph.dot {
                lang::dot::dot(program, spec.stem())
            } else {
                lang::typst::figure(program, &spec.name)
            };
            let _ = write!(console.out, "{text}");
            return 0;
        }
    };
    let me = library.command();
    let source = lang::typst::document(program, &spec.name);
    let made = match figure::render(&source, format, graph.packages.as_deref()) {
        Ok(made) => made,
        Err(error) => {
            say!(console.err, "{me}: {error}");
            return FAILED;
        }
    };
    if let Err(error) = std::fs::write(file, made) {
        say!(
            console.err,
            "{me}: cannot write {}: {error}",
            file.display()
        );
        return USAGE_ERROR;
    }
    0
}

fn plural(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// What a spec that checks will do. It lists the spec's sources, params and
/// outputs.
fn summary(name: &str, analysis: &Analysis) -> String {
    let program = &analysis.program;
    let mut out = format!("{name}: ok\n");
    let signal = |node: usize| {
        let ty = &program.nodes[node].ty;
        let names: Vec<&str> = ty.channels.iter().map(|c| c.name.as_str()).collect();
        let mut text = format!("{} ch ({})", names.len(), names.join(", "));
        let unit = &ty
            .channels
            .get(0)
            .expect("a channel set is never empty")
            .unit;
        if !unit.is_none() && ty.channels.iter().all(|c| c.unit == *unit) {
            text.push_str(&format!(", {unit}"));
        }
        format!("{text}, {}", program.sampling_of(node))
    };
    for source in &program.sources {
        out.push_str(&format!(
            "  source {:<10} {}  {}\n",
            source.name,
            source.path,
            signal(source.node)
        ));
    }
    for param in &program.params {
        let unit = if param.unit.is_none() {
            String::new()
        } else {
            format!(" {}", param.unit)
        };
        let range = match param.range {
            Some((low, high)) => format!("  [{low} .. {high}]"),
            None => String::new(),
        };
        out.push_str(&format!(
            "  param  {:<10} {}{unit}{range}\n",
            param.name, param.value
        ));
    }
    let latency = crate::lower::latency(program);
    for (output, latency) in program.outputs.iter().zip(latency) {
        out.push_str(&format!(
            "  out    {:<10} /255/{}  {}{}\n",
            output.name,
            output.name,
            signal(output.node),
            late(latency)
        ));
    }
    out
}

/// How late an output runs, when the graph makes it wait at all.
fn late(latency: crate::lower::Latency) -> String {
    let ms = |seconds: f64| format!("{} ms", (seconds * 1e4).round() / 10.0);
    match (latency.seconds > 0.0, latency.if_interpolated > 0.0) {
        (false, false) => String::new(),
        (true, false) => format!(", {} late", ms(latency.seconds)),
        (false, true) => format!(
            ", {} late if its arms sample at different instants",
            ms(latency.if_interpolated)
        ),
        (true, true) => format!(
            ", {} late, or {} if its arms sample at different instants",
            ms(latency.seconds),
            ms(latency.seconds + latency.if_interpolated)
        ),
    }
}

impl RunCli {
    /// How the graph is run, as the flags ask.
    fn options(&self, library: &Library) -> RunOptions {
        let mut options = RunOptions {
            set: self.set.clone(),
            library: library.clone(),
            ..RunOptions::default()
        };
        if let Some(threads) = self.threads {
            options.graph.threads = threads;
        }
        if let Some(block) = self.block {
            options.block_rows = block;
        }
        options
    }

    /// Where an output's CSV goes. That is the file asked for when there
    /// is one output, and that name with the output's worked in when there
    /// are more.
    fn csv_for(&self, output: &str, outputs: usize) -> Option<PathBuf> {
        let path = self.csv.as_ref()?;
        if outputs == 1 {
            return Some(path.clone());
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("out");
        let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("csv");
        Some(path.with_file_name(format!("{stem}.{output}.{extension}")))
    }
}

/// Runs a spec over a recording, says what came out, and writes it if asked.
fn over_log(library: &Library, run: &RunCli, console: &mut Console<'_>) -> u8 {
    let me = library.command();
    let Some(spec) = Spec::read(library, &run.spec, console) else {
        return USAGE_ERROR;
    };
    let Some(analysis) = spec.check(library, console) else {
        return FAILED;
    };
    let recording = match run_log(&analysis.program, &run.log, &run.options(library)) {
        Ok(recording) => recording,
        Err(RunError::Spec(diagnostics)) => {
            say!(
                console.err,
                "{}",
                diag::render_all(&diagnostics, &spec.name, &spec.source)
            );
            say!(
                console.err,
                "error: {} does not fit {}",
                spec.name,
                run.log.display()
            );
            return FAILED;
        }
        Err(error @ (RunError::Io(_) | RunError::Setting(_))) => {
            say!(console.err, "{me}: {error}");
            return USAGE_ERROR;
        }
        Err(error) => {
            say!(console.err, "error: {error}");
            return FAILED;
        }
    };
    let count = recording.outputs.len();
    for output in &recording.outputs {
        let mut line = match output.spectrum {
            true => format!(
                "  out    {:<10} {} bins of {} Hz",
                output.name,
                output.frames(),
                1.0 / output.rate
            ),
            false => format!(
                "  out    {:<10} {} samples at {}",
                output.name,
                output.frames(),
                format_rate(output.rate)
            ),
        };
        if output.runs.len() > 1 {
            line.push_str(&format!(", in {} stretches", output.runs.len()));
        }
        if let Some(path) = run.csv_for(&output.name, count) {
            let written = File::create(&path).and_then(|file| {
                let mut out = BufWriter::new(file);
                output.write_csv(&mut out)?;
                out.flush()
            });
            if let Err(error) = written {
                say!(
                    console.err,
                    "{me}: cannot write {}: {error}",
                    path.display()
                );
                return USAGE_ERROR;
            }
            line.push_str(&format!(" -> {}", path.display()));
        }
        say!(console.out, "{line}");
    }
    0
}

impl ServeCli {
    /// What is served and how, as the flags ask.
    fn options(&self, library: &Library) -> Result<ServeOptions, String> {
        let mut options = ServeOptions {
            mounts: Vec::new(),
            port: self.port,
            all_interfaces: self.listen == ListenOn::All,
            run: RunOptions {
                library: library.clone(),
                ..RunOptions::default()
            },
        };
        if let Some(threads) = self.threads {
            options.run.graph.threads = threads;
        }
        for text in &self.mounts {
            let mount = mount(text, options.mounts.len())?;
            if options
                .mounts
                .iter()
                .any(|known| known.prefix == mount.prefix)
            {
                return Err(format!("two sensors are mounted at {}", mount.prefix));
            }
            options.mounts.push(mount);
        }
        Ok(options)
    }
}

/// Reads `<url>`, `<url>=/<n>` or `<url>=/<n>@/<route>`.
fn mount(text: &str, position: usize) -> Result<MountSpec, String> {
    let route = |text: &str| {
        text.parse()
            .map_err(|_| format!("`{text}` is not a route such as `/0`"))
    };
    let (url, placed) = match text.rsplit_once('=') {
        Some((url, placed)) => (url, Some(placed)),
        None => (text, None),
    };
    let (prefix, source) = match placed.map(|placed| placed.split_once('@')) {
        None => (format!("/{position}"), "/"),
        Some(None) => (placed.unwrap_or("").to_owned(), "/"),
        Some(Some((prefix, source))) => (prefix.to_owned(), source),
    };
    if url.is_empty() {
        return Err(format!("`--mount {text}` names no sensor"));
    }
    Ok(MountSpec {
        url: url.to_owned(),
        prefix: route(&prefix)?,
        source: route(source)?,
    })
}

/// Serves a spec until the process is stopped.
fn serve(library: &Library, serve: &ServeCli, console: &mut Console<'_>) -> u8 {
    let me = library.command();
    let options = match serve.options(library) {
        Ok(options) => options,
        Err(message) => {
            say!(console.err, "{me}: {message}");
            return USAGE_ERROR;
        }
    };
    let Some(spec) = Spec::read(library, &serve.spec, console) else {
        return USAGE_ERROR;
    };
    match Server::start(spec.source.clone(), options) {
        Ok(server) => {
            say!(
                console.out,
                "{me}: serving {} at {}, outputs at {}",
                spec.name,
                server.endpoint(),
                server.route()
            );
            let _ = console.out.flush();
            server.wait();
            0
        }
        Err(ServeError::Spec(diagnostics)) => {
            say!(
                console.err,
                "{}",
                diag::render_all(&diagnostics, &spec.name, &spec.source)
            );
            say!(console.err, "error: {} does not check", spec.name);
            FAILED
        }
        Err(error) => {
            say!(console.err, "{me}: {error}");
            USAGE_ERROR
        }
    }
}

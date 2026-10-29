//! The arguments of `tio log` and its subcommands.

use super::{parse_device_route, TioOpts};
use clap::{Args, Subcommand, ValueEnum, ValueHint};
use std::time::Duration;
use twinleaf::DeviceRoute;

#[derive(Args, Debug)]
#[command(args_conflicts_with_subcommands = true)]
pub struct LogCli {
    #[command(flatten)]
    pub tio: TioOpts,

    #[command(subcommand)]
    pub subcommands: Option<LogSubcommands>,

    /// Output log file path
    #[arg(short = 'f', default_value_t = default_log_path())]
    pub file: String,

    /// Unbuffered output (flush every packet)
    #[arg(short = 'u')]
    pub unbuffered: bool,

    /// Raw mode, skipping the metadata request and dumping all packets
    #[arg(long)]
    pub raw: bool,

    /// Routing depth limit (unlimited by default)
    #[arg(long = "depth")]
    pub depth: Option<usize>,

    /// Stop after this wall-clock duration (e.g. 30s, 5m, 2h)
    #[arg(long, value_parser = humantime::parse_duration)]
    pub duration: Option<Duration>,
}

#[derive(Subcommand, Debug)]
pub enum LogSubcommands {
    /// Log metadata to a file. See "tio log meta --help" for more options
    #[command(args_conflicts_with_subcommands = true)]
    Meta(LogMetaCli),

    /// Dump data from binary log file(s)
    Dump(LogDumpCli),

    /// Summarize the contents of binary log file(s)
    Inspect(LogInspectCli),

    /// Convert binary log data to CSV
    Csv(LogCsvCli),

    /// Convert binary log files to HDF5 format
    #[command(alias = "hdf5")]
    Hdf(LogHdfCli),
}

#[derive(Args, Debug)]
pub struct LogMetaCli {
    #[command(flatten)]
    pub tio: TioOpts,

    #[command(subcommand)]
    pub subcommands: Option<MetaSubcommands>,

    /// Output metadata file path
    #[arg(short = 'f', default_value = "meta.tio")]
    pub file: String,
}

#[derive(Args, Debug)]
pub struct LogDumpCli {
    /// Input log file(s)
    #[arg(value_hint = ValueHint::FilePath, required = true, num_args = 1..)]
    pub files: Vec<String>,

    /// Show parsed data samples
    #[arg(short = 'd', long = "data")]
    pub data: bool,

    /// Show metadata on boundaries
    #[arg(short = 'm', long = "meta")]
    pub meta: bool,

    /// Sensor path in the sensor tree (e.g., /, /0, /0/1)
    #[arg(short = 's', long = "sensor", default_value = "/", value_parser = parse_device_route)]
    pub sensor: DeviceRoute,

    /// Filter streams/columns using a glob pattern (e.g. "sync", "/0/vector").
    /// Requires -d.
    #[arg(short = 'g', long = "glob", requires = "data")]
    pub glob: Option<String>,

    /// Routing depth limit (unlimited by default)
    #[arg(long = "depth")]
    pub depth: Option<usize>,
}

#[derive(Args, Debug)]
pub struct LogInspectCli {
    /// Input log file(s)
    #[arg(value_hint = ValueHint::FilePath, required = true, num_args = 1..)]
    pub files: Vec<String>,
}

#[derive(Args, Debug)]
pub struct LogCsvCli {
    /// Stream selector (name or id, optionally with a route prefix like
    /// /0/field) and input .tio files (order-independent)
    #[arg(value_hint = ValueHint::FilePath)]
    pub args: Vec<String>,

    /// Sensor route in the device tree (defaults to /), overridden by a route
    /// prefix in the selector
    #[arg(short = 's', value_parser = parse_device_route)]
    pub sensor: Option<DeviceRoute>,

    /// Output filename prefix
    #[arg(short = 'o')]
    pub output: Option<String>,

    /// Overwrite the output file if it already exists
    #[arg(short = 'f', long)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct LogHdfCli {
    /// Input log file(s)
    #[arg(value_hint = ValueHint::FilePath, required = true, num_args = 1..)]
    pub files: Vec<String>,

    /// Output file path (defaults to input filename with .h5 extension)
    #[arg(short = 'o')]
    pub output: Option<String>,

    /// Filter streams using a glob pattern (e.g. "/*/vector")
    #[arg(short = 'g', long = "glob")]
    pub filter: Option<String>,

    /// Enable deflate compression (saves space, slows down write significantly)
    #[arg(short = 'c', long = "compress")]
    pub compress: bool,

    /// Enable debug output for glob matching
    #[arg(short = 'd', long)]
    pub debug: bool,

    /// How to organize runs in the output (none=flat, stream=per-stream, device=per-device, global=all-shared)
    #[arg(short = 'l', long = "split", default_value = "none")]
    pub split_level: SplitLevel,

    /// When to detect discontinuities (continuous=any gap, monotonic=only time backward)
    #[arg(short = 'p', long = "policy", default_value = "continuous")]
    pub split_policy: SplitPolicy,
}

#[derive(Subcommand, Debug)]
pub enum MetaSubcommands {
    /// Reroute metadata packets in a metadata file
    Reroute(MetaRerouteCli),
}

#[derive(Args, Debug)]
pub struct MetaRerouteCli {
    /// Input metadata file path
    #[arg(value_hint = ValueHint::FilePath)]
    pub input: String,

    /// New device route (e.g., /0/1)
    #[arg(short = 's', long = "sensor", value_parser = parse_device_route)]
    pub route: DeviceRoute,

    /// Output metadata file path (defaults to <input>_rerouted.tio)
    #[arg(short = 'o', long = "output")]
    pub output: Option<String>,
}

/// How the user referred to a stream on the `log csv` command line.
#[derive(Clone, Debug)]
pub enum StreamSel {
    /// A numeric stream id, matched against `stream_id`.
    Id(u8),
    /// A stream name, matched against `name`.
    Name(String),
}

/// A parsed `log csv` stream selector, an optional route prefix plus the
/// stream.
///
/// The last `/`-separated segment is the stream, by id or name, and anything
/// before it is the route. `field` and `1` carry no route, while `/0/field`
/// and `/0/1` pin the route to `/0`.
#[derive(Clone, Debug)]
pub struct CsvTarget {
    pub route: Option<DeviceRoute>,
    pub stream: StreamSel,
}

pub fn parse_csv_target(s: &str) -> Result<CsvTarget, String> {
    if s.is_empty() {
        return Err("empty stream selector".into());
    }
    // The stream is always the final path segment; the rest (if any) is route.
    let last = s.rsplit('/').next().unwrap_or(s);
    if last.is_empty() {
        return Err(format!("missing stream name or id in {s:?}"));
    }
    let stream = match last.parse::<u8>() {
        Ok(id) => StreamSel::Id(id),
        Err(_) => StreamSel::Name(last.to_string()),
    };
    let route = if s.contains('/') {
        // Strip the trailing stream segment (and its separator) to get the route.
        let prefix = s[..s.len() - last.len()].trim_end_matches('/');
        Some(parse_device_route(prefix)?)
    } else {
        None
    };
    Ok(CsvTarget { route, stream })
}

fn default_log_path() -> String {
    chrono::Local::now()
        .format("log.%Y%m%d-%H%M%S.tio")
        .to_string()
}

/// When a discontinuity splits a run in the HDF5 output.
#[derive(ValueEnum, Clone, Debug, Default)]
pub enum SplitPolicy {
    /// Split on any discontinuity (gaps, rate changes, etc.)
    #[default]
    Continuous,
    /// Only split when time goes backward (allows gaps)
    Monotonic,
}

#[cfg(feature = "hdf5")]
impl From<SplitPolicy> for twinleaf::data::export::SplitPolicy {
    fn from(policy: SplitPolicy) -> Self {
        match policy {
            SplitPolicy::Continuous => Self::Continuous,
            SplitPolicy::Monotonic => Self::Monotonic,
        }
    }
}

/// How runs are organized in the HDF5 output.
#[derive(ValueEnum, Clone, Debug, Default)]
pub enum SplitLevel {
    /// No run splitting, one table per stream at /{route}/{stream}
    #[default]
    None,
    /// Each stream has independent run counter (separate table per run)
    Stream,
    /// All streams on a device share run counter
    Device,
    /// All streams globally share run counter
    Global,
}

#[cfg(feature = "hdf5")]
impl From<SplitLevel> for twinleaf::data::export::RunSplitLevel {
    fn from(level: SplitLevel) -> Self {
        match level {
            SplitLevel::None => Self::None,
            SplitLevel::Stream => Self::PerStream,
            SplitLevel::Device => Self::PerDevice,
            SplitLevel::Global => Self::Global,
        }
    }
}

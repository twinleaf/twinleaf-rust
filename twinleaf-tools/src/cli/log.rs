use clap::{Args, Subcommand, ValueEnum, ValueHint};
use std::time::Duration;

use crate::TioOpts;

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

    /// Raw mode: skip metadata request and dump all packets
    #[arg(long)]
    pub raw: bool,

    /// Routing depth (only used in --raw mode)
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
    Meta {
        #[command(flatten)]
        tio: TioOpts,

        #[command(subcommand)]
        subcommands: Option<MetaSubcommands>,

        /// Output metadata file path
        #[arg(short = 'f', default_value = "meta.tio")]
        file: String,
    },

    /// Dump data from binary log file(s)
    Dump {
        /// Input log file(s)
        #[arg(value_hint = ValueHint::FilePath, required = true, num_args = 1..)]
        files: Vec<String>,

        /// Show parsed data samples
        #[arg(short = 'd', long = "data")]
        data: bool,

        /// Show metadata on boundaries
        #[arg(short = 'm', long = "meta")]
        meta: bool,

        /// Sensor path in the sensor tree (e.g., /, /0, /0/1)
        #[arg(short = 's', long = "sensor", default_value = "/")]
        sensor: String,

        /// Routing depth limit (default: unlimited)
        #[arg(long = "depth")]
        depth: Option<usize>,
    },

    /// Summarize the contents of binary log file(s)
    Inspect {
        /// Input log file(s)
        #[arg(value_hint = ValueHint::FilePath, required = true, num_args = 1..)]
        files: Vec<String>,
    },

    /// Convert binary log data to CSV
    Csv {
        /// Stream ID/name and input .tio files (order-independent)
        #[arg(value_hint = ValueHint::FilePath)]
        args: Vec<String>,

        /// Sensor route in the device tree (default: /)
        #[arg(short = 's')]
        sensor: Option<String>,

        /// Output filename prefix
        #[arg(short = 'o')]
        output: Option<String>,
    },

    /// Convert binary log files to HDF5 format
    #[command(alias = "hdf5")]
    Hdf {
        /// Input log file(s)
        #[arg(value_hint = ValueHint::FilePath, required = true, num_args = 1..)]
        files: Vec<String>,

        /// Output file path (defaults to input filename with .h5 extension)
        #[arg(short = 'o')]
        output: Option<String>,

        /// Filter streams using a glob pattern (e.g. "/*/vector")
        #[arg(short = 'g', long = "glob")]
        filter: Option<String>,

        /// Enable deflate compression (saves space, slows down write significantly)
        #[arg(short = 'c', long = "compress")]
        compress: bool,

        /// Enable debug output for glob matching
        #[arg(short = 'd', long)]
        debug: bool,

        /// How to organize runs in the output (none=flat, stream=per-stream, device=per-device, global=all-shared)
        #[arg(short = 'l', long = "split", default_value = "none")]
        split_level: SplitLevel,

        /// When to detect discontinuities (continuous=any gap, monotonic=only time backward)
        #[arg(short = 'p', long = "policy", default_value = "continuous")]
        split_policy: SplitPolicy,
    },
}

#[derive(Subcommand, Debug)]
pub enum MetaSubcommands {
    /// Reroute metadata packets in a metadata file
    Reroute {
        /// Input metadata file path
        #[arg(value_hint = ValueHint::FilePath)]
        input: String,

        /// New device route (e.g., /0/1)
        #[arg(short = 's', long = "sensor")]
        route: String,

        /// Output metadata file path (defaults to <input>_rerouted.tio)
        #[arg(short = 'o', long = "output")]
        output: Option<String>,
    },
}

fn default_log_path() -> String {
    chrono::Local::now()
        .format("log.%Y%m%d-%H%M%S.tio")
        .to_string()
}

/// Controls when discontinuities trigger run splits
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

/// Controls how runs are organized in the HDF5 output
#[derive(ValueEnum, Clone, Debug, Default)]
pub enum SplitLevel {
    /// No run splitting - one table per stream: /{route}/{stream}
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

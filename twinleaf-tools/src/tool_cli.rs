use std::time::Duration;
use clap::{Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "tio-tool",
    version,
    about = "Twinleaf sensor management and data logging tool"
)]
pub struct TioToolCli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// List available RPCs on the device
    RpcList {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Execute an RPC on the device
    Rpc {
        #[command(flatten)]
        tio: TioOpts,

        /// RPC name to execute
        rpc_name: String,

        /// RPC argument value
        #[arg(
            allow_negative_numbers = true,
            value_name = "ARG",
            help_heading = "RPC Arguments"
        )]
        rpc_arg: Option<String>,

        /// RPC request type (one of: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, string)
        #[arg(short = 't', long = "req-type", help_heading = "Type Options")]
        req_type: Option<String>,

        /// RPC reply type (one of: u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, string)
        #[arg(short = 'T', long = "rep-type", help_heading = "Type Options")]
        rep_type: Option<String>,

        /// Enable debug output
        #[arg(short = 'd', long)]
        debug: bool,
    },

    /// Dump RPC data from the device
    RpcDump {
        #[command(flatten)]
        tio: TioOpts,

        /// RPC name to dump
        rpc_name: String,

        /// Trigger a capture before dumping
        #[arg(long)]
        capture: bool,
    },

    /// Dump data from a live device
    Dump {
        #[command(flatten)]
        tio: TioOpts,

        /// Show parsed data samples
        #[arg(short = 'd', long = "data")]
        data: bool,

        /// Show metadata on boundaries
        #[arg(short = 'm', long = "meta")]
        meta: bool,

        /// Routing depth limit (default: unlimited)
        #[arg(long = "depth")]
        depth: Option<usize>,
    },

    /// Log samples to a file (includes metadata by default)
    Log {
        #[command(flatten)]
        tio: TioOpts,

        /// Output log file path
        #[arg(short = 'f', default_value_t = default_log_path())]
        file: String,

        /// Unbuffered output (flush every packet)
        #[arg(short = 'u')]
        unbuffered: bool,

        /// Raw mode: skip metadata request and dump all packets
        #[arg(long)]
        raw: bool,

        /// Routing depth (only used in --raw mode)
        #[arg(long = "depth")]
        depth: Option<usize>,
    },

    /// Log metadata to a file
    LogMetadata {
        #[command(flatten)]
        tio: TioOpts,

        /// Output metadata file path
        #[arg(short = 'f', default_value = "meta.tio")]
        file: String,
    },

    /// Dump data from binary log file(s)
    LogDump {
        /// Input log file(s)
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

    /// Dump parsed data from binary log file(s) [DEPRECATED: use log-dump -d]
    #[command(hide = true)]
    LogDataDump {
        /// Input log file(s)
        files: Vec<String>,
    },

    /// Convert binary log data to CSV
    LogCsv {
        /// Stream ID (e.g., 1) or Name (e.g., "vector", "field")
        stream: String,

        /// Input log file(s)
        files: Vec<String>,

        /// Sensor route in the device tree (default: /)
        #[arg(short = 's')]
        sensor: Option<String>,
        
	/// External metadata file path (optional)
        #[arg(short = 'm')]
        metadata: Option<String>,

        /// Output filename prefix
        #[arg(short = 'o')]
        output: Option<String>,
    },

    /// Convert binary log files to HDF5 format
    LogHdf {
        /// Input log file(s)
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

    /// Upgrade device firmware
    FirmwareUpgrade {
        #[command(flatten)]
        tio: TioOpts,

        /// Input firmware image path
        firmware_path: String,
    },

    /// Dump data samples from the device [DEPRECATED: use dump -d -s <ROUTE>]
    #[command(hide = true)]
    DataDump {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Dump data samples from all devices in the tree [DEPRECATED: use dump -a -d]
    #[command(hide = true)]
    DataDumpAll {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Dump device metadata [DEPRECATED: use dump -m -s <ROUTE>]
    #[command(hide = true)]
    MetaDump {
        #[command(flatten)]
        tio: TioOpts,
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
    /// No run splitting - flat structure: /{route}/{stream}/{datasets}
    #[default]
    None,
    /// Each stream has independent run counter
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

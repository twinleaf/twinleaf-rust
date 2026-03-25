use clap::{ValueEnum, Subcommand};

include!("proxy_cli.rs");
include!("health_cli.rs");

#[derive(Parser, Debug)]
#[command(
    name = "tio",
    version,
    about = "Twinleaf sensor management and data logging tool"
)]
pub struct TioCli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
	Proxy(ProxyCli),
    ///Live sensor data and plot display
	Monitor {
		#[command(flatten)]
		tio: TioOpts,
		#[arg(short = 'a', long = "all")]
		all: bool,
		#[arg(long = "fps", default_value_t = 20)]
		fps: u32,
		#[arg(short = 'c', long = "colors")]
		colors: Option<String>,
	},
	Health(HealthCli),
    ///Bridge Twinleaf sensor data to NMEA TCP stream
    TextProxy{
        #[command(flatten)]
        tio: TioOpts,

        #[arg(
            short = 'p',
            long = "port",
            default_value = "7800",
            help = "TCP port to listen on"
        )]
        tcp_port: u16,
    },

    /// Execute an RPC on the device
    Rpc {
        #[command(flatten)]
        tio: TioOpts,

        #[command(subcommand)]
        subcommands: Option<RPCSubcommands>,

        /// RPC name to execute
        rpc_name: Option<String>,

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
    /// Log samples to a file (includes metadata by default)
    Log {
        #[command(flatten)]
        tio: TioOpts,

        #[command(subcommand)]
        subcommands: Option<LogSubcommands>,

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
    /// Reroute metadata packets in a metadata file
    MetaReroute {
        /// Input metadata file path
        input: String,

        /// New device route (e.g., /0/1)
        #[arg(short = 's', long = "sensor")]
        route: String,

        /// Output metadata file path (defaults to <input>_rerouted.tio)
        #[arg(short = 'o', long = "output")]
        output: Option<String>,
    },
    /// Dump data from a live device
    Dump {
        #[command(flatten)]
        tio: TioOpts,

        #[command(subcommand)]
        subcommands: Option<DumpSubcommands>,

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
    /// Upgrade device firmware
    FirmwareUpgrade {
        #[command(flatten)]
        tio: TioOpts,

        /// Input firmware image path
        firmware_path: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum RPCSubcommands{
    /// List available RPCs on the device
    List {
        #[command(flatten)]
        tio: TioOpts,
    },
    /// Dump RPC data from the device
    Dump {
        #[command(flatten)]
        tio: TioOpts,

        /// RPC name to dump
        rpc_name: String,

        /// Trigger a capture before dumping
        #[arg(long)]
        capture: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum LogSubcommands{
    /// Log metadata to a file
    Metadata {
        #[command(flatten)]
        tio: TioOpts,

        /// Output metadata file path
        #[arg(short = 'f', default_value = "meta.tio")]
        file: String,
    },

    /// Dump data from binary log file(s)
    Dump {
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
    DataDump {
        /// Input log file(s)
        files: Vec<String>,
    },

    /// Convert binary log data to CSV
    Csv {
        /// Stream ID/name and input .tio files (order-independent)
        args: Vec<String>,

        /// Sensor route in the device tree (default: /)
        #[arg(short = 's')]
        sensor: Option<String>,

        /// Output filename prefix
        #[arg(short = 'o')]
        output: Option<String>,
    },

    /// Convert binary log files to HDF5 format
    Hdf {
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
}

#[derive(Subcommand, Debug)]
pub enum DumpSubcommands{
    /// Dump data samples from the device [DEPRECATED: use dump -d -s <ROUTE>]
    #[command(hide = true)]
    Data {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Dump data samples from all devices in the tree [DEPRECATED: use dump -a -d]
    #[command(hide = true)]
    DataAll {
        #[command(flatten)]
        tio: TioOpts,
    },

    /// Dump device metadata [DEPRECATED: use dump -m -s <ROUTE>]
    #[command(hide = true)]
    Meta {
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

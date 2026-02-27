use std::time::Duration;
use std::num::ParseFloatError;
use clap::{Parser, Subcommand, ValueEnum};
use tio::proto::DeviceRoute;
use tio::util;
use twinleaf::tio;

#[derive(Parser, Debug, Clone)]
pub struct TioOpts {
    /// Sensor root address (e.g., tcp://localhost, serial:///dev/ttyUSB0)
    #[arg(
        short = 'r',
        long = "root",
        default_value_t = util::default_proxy_url().to_string(),
        help = "Sensor root address"
    )]
    pub root: String,

    /// Sensor path in the sensor tree (e.g., /, /0, /0/1)
    #[arg(
        short = 's',
        long = "sensor",
        default_value = "/",
        help = "Sensor path in the sensor tree"
    )]
    pub route_path: String,
}

impl TioOpts {
    pub fn parse_route(&self) -> DeviceRoute {
        DeviceRoute::from_str(&self.route_path).unwrap_or_else(|_| DeviceRoute::root())
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "tio-proxy",
    version,
    about = "Multiplexes access to a sensor, exposing the functionality of tio::proxy via TCP"
)]
pub struct ProxyCli {
    /// Sensor URL (e.g., tcp://localhost, serial:///dev/ttyUSB0)
    /// Required unless --auto or --enum is specified
    pub sensor_url: Option<String>,

    /// TCP port to listen on for clients
    #[arg(short = 'p', long = "port", default_value = "7855")]
    pub port: u16,

    /// Kick off slow clients instead of dropping traffic
    #[arg(short = 'k', long)]
    pub kick_slow: bool,

    /// Sensor subtree to look at
    #[arg(short = 's', long = "subtree", default_value = "/")]
    pub subtree: String,

    /// Verbose output
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Debugging output
    #[arg(short = 'd', long)]
    pub debug: bool,

    /// Timestamp format
    #[arg(short = 't', long = "timestamp", default_value = "%T%.3f ")]
    pub timestamp_format: String,

    /// Time limit for sensor reconnection attempts (seconds)
    #[arg(short = 'T', long = "timeout", default_value = "30")]
    pub reconnect_timeout: u64,

    /// Dump packet traffic except sample data/metadata or heartbeats
    #[arg(long)]
    pub dump: bool,

    /// Dump sample data traffic
    #[arg(long)]
    pub dump_data: bool,

    /// Dump sample metadata traffic
    #[arg(long)]
    pub dump_meta: bool,

    /// Dump heartbeat traffic
    #[arg(long)]
    pub dump_hb: bool,

    #[arg(short = 'a', long = "auto")]
    pub auto: bool,

    /// Enumerate all serial devices, then quit
    #[arg(short = 'e', long = "enumerate", name = "enum")]
    pub enumerate: bool,
}

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

#[derive(Parser, Debug)]
#[command(name = "tio-monitor", version, about = "Display live sensor data")]
pub struct MonitorCli {
    #[command(flatten)]
    pub tio: TioOpts,
    #[arg(short = 'a', long = "all")]
    pub all: bool,
    #[arg(long = "fps", default_value_t = 20)]
    pub fps: u32,
    #[arg(short = 'c', long = "colors")]
    pub colors: Option<String>,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "tio-health",
    version,
    about = "Live timing & rate diagnostics for TIO (Twinleaf) devices"
)]
pub struct HealthCli {
    #[command(flatten)]
    pub tio: TioOpts,

    /// Time window in seconds for calculating sample rate
    #[arg(
        long = "rate-window",
        default_value = "5",
        value_name = "SECONDS",
        value_parser = clap::value_parser!(u64).range(1..),
    )]
    pub rate_window: u64,

    /// Time window in seconds for calculating jitter statistics
    #[arg(
        long = "jitter-window",
        default_value = "10",
        value_name = "SECONDS",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Seconds for jitter calculation window (>= 1)"
    )]
    pub jitter_window: u64,

    /// PPM threshold for yellow warning indicators
    #[arg(
        long = "ppm-warn",
        default_value = "100",
        value_name = "PPM",
        value_parser = nonneg_f64,
        help = "Warning threshold in parts per million (>= 0)"
    )]
    pub ppm_warn: f64,

    /// PPM threshold for red error indicators
    #[arg(
        long = "ppm-err",
        default_value = "200",
        value_name = "PPM",
        value_parser = nonneg_f64,
        help = "Error threshold in parts per million (>= 0)"
    )]
    pub ppm_err: f64,

    /// Filter to only show specific stream IDs (comma-separated)
    #[arg(
        long = "streams",
        value_delimiter = ',',
        value_name = "IDS",
        value_parser = clap::value_parser!(u8),
        help = "Comma-separated stream IDs to monitor (e.g., 0,1,5)"
    )]
    pub streams: Option<Vec<u8>>,

    /// Suppress the footer help text
    #[arg(short = 'q', long = "quiet")]
    pub quiet: bool,

    /// UI refresh rate for animations and stale detection (data updates are immediate)
    #[arg(
        long = "fps",
        default_value = "30",
        value_name = "FPS",
        value_parser = clap::value_parser!(u64).range(1..=60),
        help = "UI refresh rate for heartbeat animation and stale detection (1–60)"
    )]
    pub fps: u64,

    /// Time in milliseconds before marking a stream as stale
    #[arg(
        long = "stale-ms",
        default_value = "2000",
        value_name = "MS",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Mark streams as stale after this many milliseconds without data (>= 1)"
    )]
    pub stale_ms: u64,

    /// Maximum number of events to keep in the event log
    #[arg(
        short = 'n',
        long = "event-log-size",
        default_value = "100",
        value_name = "N",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Maximum number of events to keep in history (>= 1)"
    )]
    pub event_log_size: u64,

    /// Number of event lines to display on screen
    #[arg(
        long = "event-display-lines",
        default_value = "8",
        value_name = "LINES",
        value_parser = clap::value_parser!(u16).range(3..),
        help = "Number of event lines to show (>= 3)"
    )]
    pub event_display_lines: u16,

    /// Only show warning and error events in the log
    #[arg(short = 'w', long = "warnings-only")]
    pub warnings_only: bool,
}

impl HealthCli {
    pub fn rate_window_dur(&self) -> Duration {
        Duration::from_secs(self.rate_window)
    }
    pub fn stale_dur(&self) -> Duration {
        Duration::from_millis(self.stale_ms)
    }
}

fn nonneg_f64(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|e: ParseFloatError| e.to_string())?;
    if v < 0.0 {
        Err("must be ≥ 0".into())
    } else {
        Ok(v)
    }
}

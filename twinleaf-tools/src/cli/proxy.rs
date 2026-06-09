use clap::{Parser, Subcommand, ValueHint};
use twinleaf::device::DeviceRoute;

use crate::{parse_device_route, TioOpts};

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Multiplexes access to a sensor, exposing the functionality of tio::proxy via TCP",
    args_conflicts_with_subcommands = true
)]
pub struct ProxyCli {
    #[command(subcommand)]
    pub subcommands: Option<ProxySubcommands>,

    /// Sensor URL (e.g., tcp://localhost, serial:///dev/ttyUSB0); defaults to auto-detecting a single connected device
    #[arg(value_hint = ValueHint::Url, conflicts_with = "mounts")]
    pub(crate) sensor_url: Option<String>,

    /// Mount a sensor at a route prefix to multiplex multiple devices (repeatable)
    #[arg(long = "mount", value_name = "LOCATOR=/N", value_parser = parse_mount)]
    pub(crate) mounts: Vec<MountArg>,

    /// TCP port to listen on for clients
    #[arg(short = 'p', long = "port", default_value = "7855")]
    pub(crate) port: u16,

    /// Kick off slow clients instead of dropping traffic
    #[arg(short = 'k', long)]
    pub(crate) kick_slow: bool,

    /// Sensor subtree to look at
    #[arg(
        short = 's',
        long = "subtree",
        default_value = "/",
        value_parser = parse_device_route,
        conflicts_with = "mounts",
    )]
    pub(crate) subtree: DeviceRoute,

    /// Verbose output
    #[arg(short = 'v', long)]
    pub(crate) verbose: bool,

    /// Debugging output
    #[arg(short = 'd', long)]
    pub(crate) debug: bool,

    /// Deprecated; timestamps are now emitted by the logger (set RUST_LOG to control verbosity)
    #[arg(short = 't', long = "timestamp", default_value = "%T%.3f ", hide = true)]
    pub(crate) timestamp_format: String,

    /// Time limit for sensor reconnection attempts (seconds)
    #[arg(short = 'T', long = "timeout", default_value = "30")]
    pub(crate) reconnect_timeout: u64,

    /// Dump packet traffic except sample data/metadata or heartbeats
    #[arg(long)]
    pub(crate) dump: bool,

    /// Dump sample data traffic
    #[arg(long)]
    pub(crate) dump_data: bool,

    /// Dump sample metadata traffic
    #[arg(long)]
    pub(crate) dump_meta: bool,

    /// Dump heartbeat traffic
    #[arg(long)]
    pub(crate) dump_hb: bool,

    /// Deprecated; running without -s <url> now auto-detects by default.
    #[arg(short = 'a', long = "auto", hide = true)]
    pub(crate) auto: bool,

    /// Deprecated; use `tio list` instead.
    #[arg(short = 'e', long = "enumerate", name = "enum", hide = true)]
    pub(crate) enumerate: bool,
}

/// A `--mount LOCATOR=/N` argument: a sensor URL bound to a route prefix.
#[derive(Debug, Clone)]
pub struct MountArg {
    pub locator: String,
    pub prefix: DeviceRoute,
}

fn parse_mount(s: &str) -> Result<MountArg, String> {
    let Some((locator, prefix_str)) = s.split_once('=') else {
        return Err(format!(
            "expected LOCATOR=/N (e.g. serial:///dev/ttyUSB0=/1), got {s:?}"
        ));
    };
    if locator.is_empty() {
        return Err(format!("missing sensor locator before '=' in {s:?}"));
    }
    let prefix = DeviceRoute::from_str(prefix_str)
        .map_err(|_| format!("invalid route prefix: {prefix_str:?}"))?;
    if prefix.len() != 1 {
        return Err(format!(
            "mount prefix must be a single segment like /1, got {prefix_str:?}"
        ));
    }
    Ok(MountArg {
        locator: locator.to_string(),
        prefix,
    })
}

#[derive(Subcommand, Debug)]
pub enum ProxySubcommands {
    /// Bridge Twinleaf sensor data to NMEA TCP stream
    Nmea {
        #[command(flatten)]
        tio: TioOpts,

        /// TCP port to listen on
        #[arg(short = 'p', long = "port", default_value = "7800")]
        tcp_port: u16,
    },
}

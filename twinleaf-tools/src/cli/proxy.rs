//! The arguments of `tio proxy` and its subcommands.

use super::{parse_device_route, ListCli, TioOpts};
use clap::{Parser, Subcommand, ValueHint};
use twinleaf::DeviceRoute;

#[derive(Parser, Debug)]
#[command(args_conflicts_with_subcommands = true)]
pub struct ProxyCli {
    /// Internal. Host the proxy on `127.0.0.1:7855` until it idles out or `tio proxy stop` stops it
    #[arg(long, hide = true)]
    pub(crate) background: bool,

    #[command(subcommand)]
    pub subcommands: Option<ProxySubcommands>,

    /// Sensor URL (e.g., `serial:///dev/ttyUSB0`, `tcp://192.168.1.20`). Defaults to auto-detecting a single serial device (use `tio proxy list` for network discovery)
    #[arg(value_hint = ValueHint::Url, conflicts_with = "mounts")]
    pub(crate) sensor_url: Option<String>,

    /// Mount a sensor at a route prefix to multiplex multiple devices (repeatable)
    #[arg(long = "mount", value_name = "LOCATOR=/N", value_parser = parse_mount)]
    pub(crate) mounts: Vec<MountArg>,

    /// TCP port to listen on for clients
    #[arg(short = 'p', long = "port", default_value = "7855")]
    pub(crate) port: u16,

    /// Advertise this proxy over mDNS (off by default)
    #[arg(long = "mdns")]
    pub(crate) mdns: bool,

    /// Kick off slow clients (by default their packets are dropped)
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

    /// Give up reconnecting to the sensor after this many seconds (retries forever by default)
    #[arg(short = 'T', long = "timeout")]
    pub(crate) reconnect_timeout: Option<u64>,

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
}

/// A sensor URL bound to a route prefix, as a `--mount LOCATOR=/N` argument
/// gives it.
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
    let prefix = prefix_str
        .parse::<DeviceRoute>()
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
    /// Stop the background proxy that a tool started, on `127.0.0.1:7855` unless `TWINLEAF_PROXY` names another
    Stop,
    /// Discover devices and host a selection as the default until Ctrl-C
    List(ListCli),

    /// Bridge Twinleaf sensor data to NMEA TCP stream
    Nmea {
        #[command(flatten)]
        tio: TioOpts,

        /// TCP port to listen on
        #[arg(short = 'p', long = "port", default_value = "7800")]
        tcp_port: u16,
    },
}

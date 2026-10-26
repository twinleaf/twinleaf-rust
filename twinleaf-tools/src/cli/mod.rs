//! The command line of `tio`, parsed by clap into [`TioCli`]. The doc
//! comments of these types are the `--help` text.

mod capture;
mod dump;
mod health;
mod list;
mod log;
mod monitor;
mod proxy;
mod rpc;
mod simulate;
mod upgrade;

pub use capture::CaptureCli;
pub use dump::DumpCli;
pub use health::HealthCli;
pub use list::ListCli;
pub use log::{
    parse_csv_target, CsvTarget, LogCli, LogCsvCli, LogDumpCli, LogHdfCli, LogInspectCli,
    LogMetaCli, LogSubcommands, MetaRerouteCli, MetaSubcommands, SplitLevel, SplitPolicy,
    StreamSel,
};
pub use monitor::MonitorCli;
pub use proxy::{MountArg, ProxyCli, ProxySubcommands};
pub use rpc::{RPCSubcommands, RpcCli};
pub use simulate::{SimulateCli, UploadKind};
pub use upgrade::UpgradeCli;

use clap::{Args, Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;
use twinleaf::DeviceRoute;

pub(crate) fn parse_device_route(s: &str) -> Result<DeviceRoute, String> {
    s.parse::<DeviceRoute>()
        .map_err(|_| format!("invalid sensor route: {s:?}"))
}

pub(crate) fn parse_existing_file(s: &str) -> Result<PathBuf, String> {
    let p = PathBuf::from(s);
    match std::fs::metadata(&p) {
        Ok(m) if m.is_file() => Ok(p),
        Ok(_) => Err(format!("not a regular file: {s:?}")),
        Err(e) => Err(format!("cannot read {s:?}: {e}")),
    }
}

pub(crate) fn nonneg_f64(s: &str) -> Result<f64, String> {
    let v: f64 = s
        .parse()
        .map_err(|e: std::num::ParseFloatError| e.to_string())?;
    if v < 0.0 {
        Err("must be >= 0".into())
    } else {
        Ok(v)
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "tio",
    version,
    about = "Twinleaf sensor management and data logging tool",
    disable_help_subcommand = true
)]
pub struct TioCli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Shortcut for `tio proxy list`
    List(ListCli),

    /// Live sensor data display
    Monitor(MonitorCli),

    /// Live timing and rate diagnostics
    Health(HealthCli),

    /// Dump raw packets from a device
    Dump(DumpCli),

    /// Log samples to a file
    Log(LogCli),

    /// Execute a device RPC
    Rpc(RpcCli),

    /// Trigger and read a capture RPC
    Capture(CaptureCli),

    /// Upgrade device firmware
    #[command(alias = "firmware-upgrade")]
    Upgrade(UpgradeCli),

    /// Multiplex a sensor over TCP
    Proxy(ProxyCli),

    /// Run a simulated sine wave Twinleaf device over UDP
    Simulate(SimulateCli),

    /// Generate shell completions for tio
    #[command(long_about = "\
Generate shell completions for tio.

Add one of these lines to your shell's config file:

  Bash (~/.bashrc):
    eval \"$(tio completions bash)\"

  Zsh (~/.zshrc):
    eval \"$(tio completions zsh)\"

  Fish (~/.config/fish/config.fish):
    tio completions fish | source

  PowerShell ($PROFILE):
    tio completions powershell | Invoke-Expression")]
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
}

#[derive(Args, Debug, Clone)]
pub struct TioOpts {
    /// Sensor root address (e.g., `tcp://localhost`, `serial:///dev/ttyUSB0`)
    #[arg(
        short = 'r',
        long = "root",
        default_value = "auto",
        value_hint = clap::ValueHint::Url,
        help = "Sensor root address (auto selects the default or only device)"
    )]
    pub root: String,

    /// Sensor path in the sensor tree (e.g., /, /0, /0/1)
    #[arg(
        short = 's',
        long = "sensor",
        default_value = "/",
        value_parser = parse_device_route,
        help = "Sensor path in the sensor tree"
    )]
    pub route: DeviceRoute,
}

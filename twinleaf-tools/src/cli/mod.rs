mod capture;
mod dump;
mod health;
mod log;
mod monitor;
mod proxy;
mod rpc;
mod simulate;
mod upgrade;

pub use capture::CaptureCli;
pub use dump::DumpCli;
pub use health::HealthCli;
pub use log::{LogCli, LogSubcommands, MetaSubcommands, SplitLevel, SplitPolicy};
pub use monitor::MonitorCli;
pub use proxy::{ProxyCli, ProxySubcommands};
pub use rpc::{RPCSubcommands, RpcCli};
pub use simulate::SimulateCli;
pub use upgrade::UpgradeCli;

use clap::{Parser, Subcommand};
use clap_complete::Shell;

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
    /// List connected devices
    List {
        /// Include serial ports with unknown VID/PID
        #[arg(short = 'a', long = "all")]
        all: bool,
    },

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

    /// (deprecated, use `simulate`) Run a simulated sine wave Twinleaf device over UDP
    #[command(hide = true)]
    Test(SimulateCli),

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

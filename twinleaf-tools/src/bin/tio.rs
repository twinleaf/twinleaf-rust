//! The `tio` binary. It parses the command line, installs the logger, and
//! runs the subcommand asked for.

use clap::{CommandFactory, Parser};
use twinleaf_tools::tools::{
    capture::run_capture,
    dump::run_dump,
    health::run_health,
    log::run_log,
    monitor::run_monitor,
    proxy::{run_list, run_proxy},
    rpc::run_rpc,
    simulate::run_simulate,
    upgrade::run_upgrade,
};
use twinleaf_tools::{Commands, TioCli};

fn main() -> eyre::Result<()> {
    twinleaf_tools::install_error_handler()?;
    let cli = TioCli::parse();

    twinleaf_tools::logging::init(&cli.command);

    match cli.command {
        Commands::List(list_cli) => run_list(list_cli),
        Commands::Proxy(proxy_cli) => run_proxy(proxy_cli),
        Commands::Simulate(simulate_cli) => run_simulate(simulate_cli),
        Commands::Monitor(monitor_cli) => run_monitor(monitor_cli),
        Commands::Health(health_cli) => run_health(health_cli),
        Commands::Rpc(rpc_cli) => run_rpc(rpc_cli),
        Commands::Capture(capture_cli) => run_capture(capture_cli),
        Commands::Dump(dump_cli) => run_dump(dump_cli),
        Commands::Log(log_cli) => run_log(log_cli),
        Commands::Upgrade(upgrade_cli) => run_upgrade(upgrade_cli),
        Commands::Completions { shell } => {
            clap_complete::generate(shell, &mut TioCli::command(), "tio", &mut std::io::stdout());
            Ok(())
        }
    }
}

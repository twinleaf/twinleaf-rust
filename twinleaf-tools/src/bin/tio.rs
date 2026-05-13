use clap::{CommandFactory, Parser};
use twinleaf_tools::tools::{
    health::{run_health, HealthConfig},
    list::list_devices,
    monitor::{run_monitor, MonitorConfig},
    proxy::run_proxy,
    tio_test::run_test,
    tool::{run_dump, run_log, run_rpc, run_upgrade},
};
use twinleaf_tools::{Commands, TioCli};

fn main() -> eyre::Result<()> {
    twinleaf_tools::install_error_handler()?;
    let cli = TioCli::parse();

    if !matches!(cli.command, Commands::Health(_) | Commands::Monitor(_)) {
        twinleaf_tools::init_logging();
    }

    match cli.command {
        Commands::List { all } => list_devices(all),
        Commands::Proxy(proxy_cli) => run_proxy(proxy_cli),
        Commands::Test(test_cli) => run_test(test_cli),
        Commands::Monitor(monitor_cli) => run_monitor(MonitorConfig::from(monitor_cli)),
        Commands::Health(health_cli) => run_health(HealthConfig::from(health_cli)),
        Commands::Rpc(rpc_cli) => run_rpc(rpc_cli),
        Commands::Dump(dump_cli) => run_dump(dump_cli),
        Commands::Log(log_cli) => run_log(log_cli),
        Commands::Upgrade(upgrade_cli) => run_upgrade(upgrade_cli),
        Commands::Completions { shell } => {
            clap_complete::generate(shell, &mut TioCli::command(), "tio", &mut std::io::stdout());
            Ok(())
        }
    }
}

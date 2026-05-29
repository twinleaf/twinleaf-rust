use clap::{CommandFactory, Parser};
use twinleaf_tools::tools::{
    dump::run_dump,
    health::{run_health, HealthConfig},
    list::run_list,
    log::run_log,
    monitor::{run_monitor, MonitorConfig},
    proxy::run_proxy,
    rpc::run_rpc,
    simulate::run_simulate,
    upgrade::run_upgrade,
};
use twinleaf_tools::{Commands, TioCli};

fn main() -> eyre::Result<()> {
    twinleaf_tools::install_error_handler()?;
    let cli = TioCli::parse();

    if !matches!(cli.command, Commands::Health(_) | Commands::Monitor(_)) {
        twinleaf_tools::init_logging();
    }

    match cli.command {
        Commands::List { all } => run_list(all),
        Commands::Proxy(proxy_cli) => run_proxy(proxy_cli),
        Commands::Simulate(simulate_cli) => run_simulate(simulate_cli),
        Commands::Test(simulate_cli) => {
            eprintln!("warning: `tio test` is deprecated; use `tio simulate` instead");
            run_simulate(simulate_cli)
        }
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

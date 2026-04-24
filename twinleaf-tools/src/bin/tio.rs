use clap::{CommandFactory, Parser};
use twinleaf_tools::tools::{
    health::run_health,
    list::list_devices,
    monitor::run_monitor,
    proxy::run_proxy,
    proxy_nmea::run_nmea_proxy,
    tool::{
        dump, firmware_upgrade, list_rpcs, log, log_csv, log_dump, log_hdf, log_metadata,
        meta_reroute, rpc, rpc_dump,
    },
};
use twinleaf_tools::{
    Commands, LogSubcommands, MetaSubcommands, ProxySubcommands, RPCSubcommands, TioCli,
};

fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let cli = TioCli::parse();

    match cli.command {
        Commands::List { all } => list_devices(all),
        Commands::Proxy(mut proxy_cli) => match proxy_cli.subcommands.take() {
            Some(ProxySubcommands::Nmea { tio, tcp_port }) => run_nmea_proxy(tio, tcp_port),
            None => run_proxy(proxy_cli),
        },
        Commands::Monitor {
            tio,
            all,
            fps,
            colors,
        } => run_monitor(tio, all, fps, colors),
        Commands::Health(health_cli) => run_health(health_cli),
        Commands::Rpc {
            tio,
            subcommands,
            rpc_name,
            rpc_arg,
            req_type,
            rep_type,
            debug,
        } => match subcommands {
            Some(RPCSubcommands::List { tio }) => list_rpcs(&tio),
            Some(RPCSubcommands::Dump {
                tio,
                rpc_name,
                capture,
            }) => rpc_dump(&tio, rpc_name, capture),
            None => rpc(
                &tio,
                rpc_name.unwrap_or("".to_string()),
                rpc_arg,
                req_type,
                rep_type,
                debug,
            ),
        },
        Commands::Dump {
            tio,
            data,
            meta,
            depth,
        } => dump(&tio, data, meta, depth),
        Commands::Log {
            tio,
            subcommands,
            file,
            unbuffered,
            raw,
            depth,
        } => match subcommands {
            Some(LogSubcommands::Meta {
                tio,
                subcommands,
                file,
            }) => match subcommands {
                Some(MetaSubcommands::Reroute {
                    input,
                    route,
                    output,
                }) => meta_reroute(input, route, output),
                None => log_metadata(&tio, file),
            },
            Some(LogSubcommands::Dump {
                files,
                data,
                meta,
                sensor,
                depth,
            }) => log_dump(files, data, meta, sensor, depth),
            Some(LogSubcommands::Csv {
                args,
                sensor,
                output,
            }) => log_csv(args, sensor, output),
            Some(LogSubcommands::Hdf {
                files,
                output,
                filter,
                compress,
                debug,
                split_level,
                split_policy,
            }) => log_hdf(
                files,
                output,
                filter,
                compress,
                debug,
                split_level,
                split_policy,
            ),
            None => log(&tio, file, unbuffered, raw, depth),
        },
        Commands::Upgrade {
            tio,
            firmware_path,
            yes,
        } => firmware_upgrade(&tio, firmware_path, yes),
        Commands::Completions { shell } => {
            clap_complete::generate(shell, &mut TioCli::command(), "tio", &mut std::io::stdout());
            Ok(())
        }
    }
}

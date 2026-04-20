use clap::{CommandFactory, Parser};
use std::process::ExitCode;
use twinleaf_tools::tools::{
    tio_health::run_health,
    tio_monitor::run_monitor,
    tio_proxy::run_proxy,
    tio_text_proxy::run_text_proxy,
    tio_tool::{
        data_dump_all_deprecated, data_dump_deprecated, dump, firmware_upgrade, list_rpcs, log,
        log_csv, log_data_dump_deprecated, log_dump, log_hdf, log_metadata, meta_dump_deprecated,
        meta_reroute, rpc, rpc_dump,
    },
};
use twinleaf_tools::{Commands, DumpSubcommands, LogSubcommands, RPCSubcommands, TioCli};

fn main() -> ExitCode {
    let cli = TioCli::parse();

    //TODO: Work on exit code logic
    let result = match cli.command {
        Commands::Proxy(proxy_cli) => run_proxy(proxy_cli),
        Commands::Monitor {
            tio,
            all,
            fps,
            colors,
        } => run_monitor(tio, all, fps, colors),
        Commands::Health(health_cli) => run_health(health_cli),
        Commands::NmeaProxy { tio, tcp_port } => run_text_proxy(tio, tcp_port),
        Commands::Rpc {
            tio,
            subcommands,
            rpc_name,
            rpc_arg,
            req_type,
            rep_type,
            debug,
        } => {
            let _ = match subcommands {
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
            };
            Ok(())
        }
        Commands::Dump {
            tio,
            subcommands,
            data,
            meta,
            depth,
        } => {
            let _ = match subcommands {
                Some(DumpSubcommands::Data { tio }) => data_dump_deprecated(&tio),
                Some(DumpSubcommands::DataAll { tio }) => data_dump_all_deprecated(&tio),
                Some(DumpSubcommands::Meta { tio }) => meta_dump_deprecated(&tio),
                None => dump(&tio, data, meta, depth),
            };
            Ok(())
        }
        Commands::Log {
            tio,
            subcommands,
            file,
            unbuffered,
            raw,
            depth,
        } => {
            let _ = match subcommands {
                Some(LogSubcommands::Metadata { tio, file }) => log_metadata(&tio, file),
                Some(LogSubcommands::Dump {
                    files,
                    data,
                    meta,
                    sensor,
                    depth,
                }) => log_dump(files, data, meta, sensor, depth),
                Some(LogSubcommands::DataDump { files }) => log_data_dump_deprecated(files),
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
            };
            Ok(())
        }
        Commands::MetaReroute {
            input,
            route,
            output,
        } => meta_reroute(input, route, output),
        Commands::FirmwareUpgrade {
            tio,
            firmware_path,
            yes,
        } => firmware_upgrade(&tio, firmware_path, yes),
        Commands::Completions { shell } => {
            clap_complete::generate(shell, &mut TioCli::command(), "tio", &mut std::io::stdout());
            Ok(())
        }
    };

    if result.is_ok() {
        ExitCode::SUCCESS
    } else {
        eprintln!("FAILED");
        ExitCode::FAILURE
    }
}

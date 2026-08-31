use std::collections::HashSet;

use crate::{LogCli, LogSubcommands, MetaSubcommands};
use twinleaf::device::DeviceRoute;
use twinleaf::tio;

mod csv;
mod dump;
#[cfg(feature = "hdf5")]
mod hdf;
mod inspect;
mod progress;
mod record;

pub use csv::log_csv;
pub use dump::log_dump;
#[cfg(feature = "hdf5")]
pub use hdf::log_hdf;
pub use inspect::log_inspect;
pub use record::{log, log_metadata, meta_reroute};

const LOG_BATCH_ROWS: usize = 65_536;

pub fn run_log(log_cli: LogCli) -> eyre::Result<()> {
    match log_cli.subcommands {
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
            glob,
            depth,
        }) => log_dump(files, data, meta, sensor, glob, depth),
        Some(LogSubcommands::Inspect { files }) => log_inspect(files),
        Some(LogSubcommands::Csv {
            args,
            sensor,
            output,
            force,
        }) => log_csv(args, sensor, output, force),
        #[cfg(feature = "hdf5")]
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
        #[cfg(not(feature = "hdf5"))]
        Some(LogSubcommands::Hdf { .. }) => {
            use color_eyre::Help;

            Err(
                eyre::eyre!("this build of twinleaf-tools does not include HDF5 support")
                    .suggestion(
                        "reinstall with: cargo install twinleaf-tools --features hdf5 --force",
                    ),
            )
        }
        None => log(
            &log_cli.tio,
            log_cli.file,
            log_cli.unbuffered,
            log_cli.raw,
            log_cli.depth,
            log_cli.duration,
        ),
    }
}

// Track which routes ever produced a sample and which ever dropped a stream-data
// packet. A route is only "missing metadata" if it dropped data but never parsed
// anything; routes that parse fine but drop a few leading/boundary packets are
// working as intended.
fn record_parse_result(
    parsed: &mut HashSet<DeviceRoute>,
    unparsed: &mut HashSet<DeviceRoute>,
    pkt: &tio::Packet,
    samples_len: usize,
) {
    record_parse_result_parts(
        parsed,
        unparsed,
        &pkt.route(),
        matches!(pkt.payload(), tio::proto::Payload::Samples(data) if !data.data.is_empty()),
        samples_len,
    );
}

fn record_parse_result_parts(
    parsed: &mut HashSet<DeviceRoute>,
    unparsed: &mut HashSet<DeviceRoute>,
    route: &DeviceRoute,
    has_stream_data: bool,
    samples_len: usize,
) {
    if samples_len != 0 {
        parsed.insert(*route);
        return;
    }
    if has_stream_data {
        unparsed.insert(*route);
    }
}

fn unparseable_routes(
    parsed: &HashSet<DeviceRoute>,
    unparsed: &HashSet<DeviceRoute>,
) -> Vec<DeviceRoute> {
    unparsed.difference(parsed).cloned().collect()
}

fn report_missing_metadata(mut routes: Vec<DeviceRoute>) {
    if routes.is_empty() {
        return;
    }
    routes.sort();
    crate::multi_progress().suspend(|| {
        use console::style;
        let warning = style("Warning").yellow().bold();
        let suggestion = style("Suggestion").yellow().bold();
        if routes.len() == 1 {
            eprintln!(
                "{}: stream data at route {} could not be parsed because metadata is missing or incompatible.",
                warning, routes[0]
            );
        } else {
            eprintln!(
                "{}: stream data at these routes could not be parsed because metadata is missing or incompatible:",
                warning
            );
            for route in routes.iter().take(5) {
                eprintln!("  {}", route);
            }
            if routes.len() > 5 {
                eprintln!("  ... and {} more", routes.len() - 5);
            }
        }
        eprintln!(
            "{}: ensure the log includes metadata or capture it with `tio log metadata`, including it as an argument before the log.",
            suggestion
        );
    });
}

//! The `tio log` subcommand and its subcommands.
//!
//! [`run_log`] records samples to a file with [`log()`], or runs the subcommand
//! asked for. [`log_metadata`] and [`meta_reroute`] write and rewrite
//! metadata files. [`log_dump`] and [`log_inspect`] read logs back, and
//! [`log_csv`] and `log_hdf` convert them.

use crate::{LogCli, LogMetaCli, LogSubcommands, MetaSubcommands};
use twinleaf::DeviceRoute;

mod csv;
mod dump;
#[cfg(feature = "hdf5")]
mod hdf;
mod inspect;
mod progress;
mod read;
mod record;
#[cfg(test)]
mod tests;

pub use csv::log_csv;
pub use dump::log_dump;
#[cfg(feature = "hdf5")]
pub use hdf::log_hdf;
pub use inspect::log_inspect;
pub use record::{log, log_metadata, meta_reroute};

const LOG_BATCH_ROWS: usize = 65_536;

pub fn run_log(log_cli: LogCli) -> eyre::Result<()> {
    match log_cli.subcommands {
        Some(LogSubcommands::Meta(LogMetaCli {
            subcommands: Some(MetaSubcommands::Reroute(cli)),
            ..
        })) => meta_reroute(cli),
        Some(LogSubcommands::Meta(cli)) => log_metadata(cli),
        Some(LogSubcommands::Dump(cli)) => log_dump(cli),
        Some(LogSubcommands::Inspect(cli)) => log_inspect(cli),
        Some(LogSubcommands::Csv(cli)) => log_csv(cli),
        #[cfg(feature = "hdf5")]
        Some(LogSubcommands::Hdf(cli)) => log_hdf(cli),
        #[cfg(not(feature = "hdf5"))]
        Some(LogSubcommands::Hdf(_)) => {
            use color_eyre::Help;

            Err(
                eyre::eyre!("this build of twinleaf-tools does not include HDF5 support")
                    .suggestion(
                        "reinstall with: cargo install twinleaf-tools --features hdf5 --force",
                    ),
            )
        }
        None => log(log_cli),
    }
}

/// Warns that stream data at `routes`, in route order, could not be decoded
/// for lack of metadata.
fn report_missing_metadata(routes: &[DeviceRoute]) {
    if routes.is_empty() {
        return;
    }
    crate::logging::multi_progress().suspend(|| {
        use console::style;
        let warning = style("Warning").yellow().bold();
        let suggestion = style("Suggestion").yellow().bold();
        if let [route] = routes {
            eprintln!(
                "{}: stream data at route {} could not be parsed because metadata is missing or incompatible.",
                warning, route
            );
        } else {
            eprintln!(
                "{}: stream data at these routes could not be parsed because metadata is missing or incompatible:",
                warning
            );
            eprint_routes(routes);
        }
        eprintln!("{}: {}.", suggestion, MISSING_METADATA_HINT);
    });
}

/// The suggestion for stream data that could not be decoded for lack of
/// metadata.
const MISSING_METADATA_HINT: &str = "ensure the log includes metadata or capture it with \
     `tio log meta`, including it as an argument before the log";

/// Returns the error for an output left empty when stream data at `routes`,
/// in route order, could not be decoded for lack of metadata.
fn missing_metadata_error(routes: &[DeviceRoute]) -> eyre::Report {
    use color_eyre::Help;

    let at = match routes {
        [route] => format!("route {route}"),
        _ => {
            let routes: Vec<_> = routes.iter().map(DeviceRoute::to_string).collect();
            format!("routes {}", routes.join(", "))
        }
    };
    eyre::eyre!(
        "stream data at {at} could not be parsed because metadata is missing or incompatible"
    )
    .suggestion(MISSING_METADATA_HINT)
}

/// Lists up to five of `routes` on stderr, one to a line.
fn eprint_routes(routes: &[DeviceRoute]) {
    for route in routes.iter().take(5) {
        eprintln!("  {}", route);
    }
    if routes.len() > 5 {
        eprintln!("  ... and {} more", routes.len() - 5);
    }
}

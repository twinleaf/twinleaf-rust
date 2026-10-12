//! The `tio log dump` subcommand, which prints the packets or samples of
//! logs with [`log_dump`].

use super::read::LogReader;
use super::{eprint_routes, report_missing_metadata};
use crate::tools::dump::Printer;
use crate::LogDumpCli;
use std::collections::HashSet;
use twinleaf::data::PacketParser;
use twinleaf::DeviceRoute;

pub fn log_dump(cli: LogDumpCli) -> eyre::Result<()> {
    let printer = Printer::new(cli.data, cli.meta, cli.glob.as_deref())?;
    let reader = LogReader::open(&cli.files)?;

    let target_route = cli.sensor;
    let route_matches = |route: &DeviceRoute| -> bool {
        match target_route.relative_route(route) {
            Ok(rel) => cli.depth.is_none_or(|max| rel.len() <= max),
            Err(_) => false,
        }
    };
    let in_subtree = |route: &DeviceRoute| -> bool { target_route.relative_route(route).is_ok() };

    let mut printed_any = false;
    let mut deeper_routes: HashSet<DeviceRoute> = HashSet::new();

    if printer.prints_samples() {
        let parser = PacketParser::new(cli.files.len() > 1);
        let read = reader.batches(
            parser,
            |_| true,
            |batch| {
                let route = batch.route();
                if route_matches(&route) {
                    printed_any |= printer.print_batch(&batch);
                } else if in_subtree(&route) {
                    deeper_routes.insert(route);
                }
                Ok(())
            },
        )?;
        let missing: Vec<_> = read
            .missing_metadata
            .into_iter()
            .filter(route_matches)
            .collect();
        report_missing_metadata(&missing);
    } else {
        reader.packets(|packet| {
            let route = packet.route();
            if route_matches(&route) {
                printed_any |= printer.print_packet(packet);
            } else if in_subtree(&route) && printer.shows_packet(packet) {
                deeper_routes.insert(route);
            }
        });
    }

    // Warn if nothing printed but data exists at deeper routes
    if !printed_any && !deeper_routes.is_empty() {
        let mut routes: Vec<_> = deeper_routes.into_iter().collect();
        routes.sort();
        eprintln!("No data at route {}, but found data at:", target_route);
        eprint_routes(&routes);
        eprintln!();
        eprintln!("Use -s to specify a different route, or remove --depth to include all");
    }

    Ok(())
}

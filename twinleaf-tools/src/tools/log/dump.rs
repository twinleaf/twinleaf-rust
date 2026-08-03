use super::{record_parse_result, report_missing_metadata, unparseable_routes};
use crate::tools::dump::{print_batch_meta, print_metadata_payload, print_sample};
use std::collections::HashSet;
use twinleaf::data::PacketParser;
use twinleaf::device::DeviceRoute;
use twinleaf::tio;

pub fn log_dump(
    files: Vec<String>,
    data: bool,
    meta: bool,
    sensor: DeviceRoute,
    glob: Option<String>,
    depth: Option<usize>,
) -> eyre::Result<()> {
    use eyre::WrapErr;
    use twinleaf::data::ColumnFilter;

    let filter = if let Some(p) = glob {
        Some(ColumnFilter::new(&p).map_err(|e| eyre::eyre!("invalid glob pattern: {}", e))?)
    } else {
        None
    };
    let target_route = sensor;
    let max_depth = depth;

    let route_matches = |route: &DeviceRoute| -> bool {
        match target_route.relative_route(route) {
            Ok(rel) => max_depth.map_or(true, |max| rel.len() <= max),
            Err(_) => false,
        }
    };

    let in_subtree = |route: &DeviceRoute| -> bool { target_route.relative_route(route).is_ok() };

    let mut printed_any = false;
    let mut deeper_routes: HashSet<DeviceRoute> = HashSet::new();

    // Helper to iterate packets from files
    let iter_packets = |files: &[String]| -> eyre::Result<Vec<(String, Vec<u8>)>> {
        files
            .iter()
            .map(|path| {
                std::fs::read(path)
                    .map(|data| (path.clone(), data))
                    .wrap_err_with(|| format!("could not read {}", path))
            })
            .collect()
    };

    match (data, meta) {
        // Raw mode (no flags): dump all packets
        (false, false) => {
            for (path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = tio::Packet::deserialize(rest)
                        .wrap_err_with(|| format!("could not parse packet in {}", path))?;
                    rest = &rest[len..];

                    if route_matches(&pkt.routing) {
                        println!("{:?}", pkt);
                        printed_any = true;
                    } else if in_subtree(&pkt.routing) {
                        deeper_routes.insert(pkt.routing.clone());
                    }
                }
            }
        }

        // Metadata-only mode (-m): filter to metadata packets
        (false, true) => {
            for (path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = match tio::Packet::deserialize(rest) {
                        Ok(res) => res,
                        Err(e) => {
                            log::warn!(
                                "{}: parse error at offset {} ({:?}); stopping",
                                path,
                                file_data.len() - rest.len(),
                                e
                            );
                            break;
                        }
                    };
                    rest = &rest[len..];

                    if let tio::proto::Payload::Metadata(mp) = &pkt.payload {
                        if route_matches(&pkt.routing) {
                            print_metadata_payload(&pkt.routing, mp);
                            printed_any = true;
                        } else if in_subtree(&pkt.routing) {
                            deeper_routes.insert(pkt.routing.clone());
                        }
                    }
                }
            }
        }

        // Sample mode (-d or -d -m): parse and print samples
        (true, _) => {
            let ignore_session = files.len() > 1;
            let mut parser = PacketParser::new(DeviceRoute::root(), ignore_session);
            let mut parsed_routes: HashSet<DeviceRoute> = HashSet::new();
            let mut unparsed_routes: HashSet<DeviceRoute> = HashSet::new();

            for (path, file_data) in iter_packets(&files)? {
                let mut rest: &[u8] = &file_data;
                while !rest.is_empty() {
                    let (pkt, len) = match tio::Packet::deserialize(rest) {
                        Ok(res) => res,
                        Err(e) => {
                            log::warn!(
                                "{}: parse error at offset {} ({:?}); stopping",
                                path,
                                file_data.len() - rest.len(),
                                e
                            );
                            break;
                        }
                    };
                    rest = &rest[len..];

                    let parsed = parser.process_packet(&pkt);
                    record_parse_result(
                        &mut parsed_routes,
                        &mut unparsed_routes,
                        &pkt,
                        parsed.as_ref().map_or(0, |b| b.len()),
                    );

                    let Some(batch) = parsed else {
                        continue;
                    };
                    if route_matches(&pkt.routing) {
                        // Schema questions are answered once per batch.
                        let matched = filter.as_ref().map_or(true, |f| {
                            batch.schema().iter().any(|series| {
                                f.matches(&pkt.routing, &batch.stream.name, &series.metadata.name)
                            })
                        });
                        if !matched {
                            continue;
                        }
                        if meta {
                            print_batch_meta(&batch, Some(&pkt.routing));
                        }
                        for row in batch.iter() {
                            print_sample(row, Some(&pkt.routing));
                        }
                        printed_any = true;
                    } else if in_subtree(&pkt.routing) {
                        deeper_routes.insert(pkt.routing.clone());
                    }
                }
            }

            let missing_routes: Vec<_> = unparseable_routes(&parsed_routes, &unparsed_routes)
                .into_iter()
                .filter(route_matches)
                .collect();
            report_missing_metadata(missing_routes);
        }
    }

    // Warn if nothing printed but data exists at deeper routes
    if !printed_any && !deeper_routes.is_empty() {
        let mut routes: Vec<_> = deeper_routes.into_iter().collect();
        routes.sort();
        eprintln!("No data at route {}, but found data at:", target_route);
        for r in routes.iter().take(5) {
            eprintln!("  {}", r);
        }
        if routes.len() > 5 {
            eprintln!("  ... and {} more", routes.len() - 5);
        }
        eprintln!();
        eprintln!("Use -s to specify a different route, or remove --depth to include all");
    }

    Ok(())
}

use crate::{DumpCli, ProxyHelp, TioOpts};
use twinleaf::data::{ColumnFilter, SampleBatch, SampleRow};
use twinleaf::device::{DeviceRoute, DeviceTree, TreeItem};
use twinleaf::tio::{self, proxy};

pub fn run_dump(dump_cli: DumpCli) -> eyre::Result<()> {
    dump(
        &dump_cli.tio,
        dump_cli.data,
        dump_cli.meta,
        dump_cli.glob,
        dump_cli.depth,
        dump_cli.duration,
    )
}

pub fn dump(
    tio: &TioOpts,
    data: bool,
    meta: bool,
    glob: Option<String>,
    depth: Option<usize>,
    duration: Option<std::time::Duration>,
) -> eyre::Result<()> {
    use eyre::WrapErr;
    use std::time::Instant;

    let filter = if let Some(p) = glob {
        Some(ColumnFilter::new(&p).map_err(|e| eyre::eyre!("invalid glob pattern: {}", e))?)
    } else {
        None
    };

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route.clone();
    let port_depth = depth.unwrap_or(tio::proto::TIO_PACKET_MAX_ROUTING_SIZE);

    let port = proxy
        .new_port(None, route.clone(), port_depth, true, true)
        .wrap_err_with(|| format!("could not open port on {}", tio.root))
        .with_proxy_help()?;

    let started = Instant::now();
    let duration_elapsed = || duration.is_some_and(|d| started.elapsed() >= d);

    log::info!("dumping from {} (route {})", tio.root, route);

    match (data, meta) {
        // Raw mode (no flags): dump all packets
        (false, false) => {
            for pkt in port.iter() {
                if duration_elapsed() {
                    break;
                }
                let abs_pkt = tio::Packet {
                    routing: route.absolute_route(&pkt.routing)?,
                    ..pkt
                };
                println!("{:?}", abs_pkt);
            }
        }

        // Metadata-only mode (-m): filter to metadata packets
        (false, true) => {
            for pkt in port.iter() {
                if duration_elapsed() {
                    break;
                }
                if let tio::proto::Payload::Metadata(mp) = &pkt.payload {
                    let abs_route = route.absolute_route(&pkt.routing)?;
                    print_metadata_payload(&abs_route, mp);
                }
            }
        }

        // Sample mode (-d or -d -m): use DeviceTree for parsed samples
        (true, _) => {
            let mut tree = DeviceTree::new(port, route.clone());

            while !duration_elapsed() {
                match tree.next_item() {
                    Ok(TreeItem::Batch(batch)) => {
                        let sample_route = batch.route.clone();
                        // Schema questions are answered once per batch.
                        let matched = filter.as_ref().map_or(true, |f| {
                            batch.schema().iter().any(|series| {
                                f.matches(&sample_route, &batch.stream.name, &series.metadata.name)
                            })
                        });
                        if !matched {
                            continue;
                        }
                        if meta {
                            print_batch_meta(&batch, Some(&sample_route));
                        }
                        for row in batch.iter() {
                            print_sample(row, Some(&sample_route));
                        }
                    }
                    Ok(TreeItem::Event(_)) => {}
                    Err(e) => {
                        return Err(eyre::Report::new(e).wrap_err("stream ended"));
                    }
                }
            }
        }
    }

    if duration_elapsed() {
        log::info!("duration elapsed");
        Ok(())
    } else {
        Err(eyre::eyre!("stream ended"))
    }
}

/// Prints the boundary/metadata lines for a batch, once, from `batch.boundary`.
pub fn print_batch_meta(batch: &SampleBatch, route: Option<&DeviceRoute>) {
    let route_str = if let Some(r) = route {
        format!("{} ", r)
    } else {
        "".to_string()
    };

    if let Some(boundary) = &batch.boundary {
        println!("# {}BOUNDARY {:?}", route_str, boundary.reason);
        if !boundary.is_continuous() {
            println!("# {}DEVICE {:?}", route_str, batch.device);
            println!("# {}STREAM {:?}", route_str, batch.stream);
            for series in batch.schema() {
                println!("# {}COLUMN {:?}", route_str, series.metadata);
            }
        }
        println!("# {}SEGMENT {:?}", route_str, batch.segment);
    }
}

/// Prints one row's data line.
pub fn print_sample(row: SampleRow, route: Option<&DeviceRoute>) {
    let route_str = if let Some(r) = route {
        format!("{} ", r)
    } else {
        "".to_string()
    };
    println!("{}{}", route_str, row);
}

pub fn print_metadata_payload(route: &DeviceRoute, payload: &tio::proto::MetadataPayload) {
    let route_str = format!("{} ", route);
    match &payload.content {
        tio::proto::meta::MetadataContent::Device(dm) => {
            println!("# {}DEVICE {:?}", route_str, dm);
        }
        tio::proto::meta::MetadataContent::Stream(sm) => {
            println!("# {}STREAM {:?}", route_str, sm);
        }
        tio::proto::meta::MetadataContent::Segment(sm) => {
            println!("# {}SEGMENT {:?}", route_str, sm);
        }
        tio::proto::meta::MetadataContent::Column(cm) => {
            println!("# {}COLUMN {:?}", route_str, cm);
        }
        tio::proto::meta::MetadataContent::Unknown(mtype) => {
            println!("# {}METADATA Unknown({})", route_str, mtype);
        }
    }
}

use crate::{DumpCli, ProxyHelp, TioOpts};
use twinleaf::device::{DeviceRoute, DeviceTree};
use twinleaf::tio::{self, proxy};

pub fn run_dump(dump_cli: DumpCli) -> eyre::Result<()> {
    dump(
        &dump_cli.tio,
        dump_cli.data,
        dump_cli.meta,
        dump_cli.depth,
        dump_cli.duration,
    )
}

pub fn dump(
    tio: &TioOpts,
    data: bool,
    meta: bool,
    depth: Option<usize>,
    duration: Option<std::time::Duration>,
) -> eyre::Result<()> {
    use eyre::WrapErr;
    use std::time::Instant;

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
                    routing: route.absolute_route(&pkt.routing),
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
                    let abs_route = route.absolute_route(&pkt.routing);
                    print_metadata_payload(&abs_route, mp);
                }
            }
        }

        // Sample mode (-d or -d -m): use DeviceTree for parsed samples
        (true, _) => {
            let mut tree = DeviceTree::new(port, route.clone());

            while !duration_elapsed() {
                match tree.next() {
                    Ok((sample, sample_route)) => {
                        print_sample(&sample, Some(&sample_route), meta, true);
                    }
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

pub fn print_sample(
    sample: &twinleaf::data::Sample,
    route: Option<&DeviceRoute>,
    print_meta: bool,
    print_data: bool,
) {
    let route_str = if let Some(r) = route {
        format!("{} ", r)
    } else {
        "".to_string()
    };

    if print_meta {
        if let Some(boundary) = &sample.boundary {
            println!("# {}BOUNDARY {:?}", route_str, boundary.reason);
            if !boundary.is_continuous() {
                println!("# {}DEVICE {:?}", route_str, sample.device);
                println!("# {}STREAM {:?}", route_str, sample.stream);
                for col in &sample.columns {
                    println!("# {}COLUMN {:?}", route_str, col.desc);
                }
            }
            println!("# {}SEGMENT {:?}", route_str, sample.segment);
        }
    }

    if print_data {
        println!("{}{}", route_str, sample);
    }
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

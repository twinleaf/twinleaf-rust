use crate::tools::recv_before;
use crate::{DumpCli, ProxyHelp, TioOpts};
use std::time::Instant;
use twinleaf::data::{ColumnFilter, SampleBatch, SampleRow};
use twinleaf::device::{DeviceRoute, RecvError, RecvTimeoutError};
use twinleaf::tio::{self, proxy};
use twinleaf_proto::data;

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

    let filter = if let Some(p) = glob {
        Some(ColumnFilter::new(&p).map_err(|e| eyre::eyre!("invalid glob pattern: {}", e))?)
    } else {
        None
    };

    let proxy = proxy::Connection::open(&tio.root);
    let route = tio.route;
    let port_depth = depth.unwrap_or(twinleaf_proto::MAX_ROUTING_SIZE);

    let deadline = duration.map(|duration| Instant::now() + duration);

    log::info!("dumping from {} (route {})", tio.root, route);

    // The packet modes read the wire directly; only sample mode parses.
    let raw_port = |what: &str| {
        proxy::open_port(&proxy, None, route, port_depth, true, true)
            .wrap_err_with(|| format!("could not open {what} port on {}", tio.root))
            .with_proxy_help()
    };

    match (data, meta) {
        // Raw mode (no flags): dump all packets
        (false, false) => {
            let port = raw_port("packet")?;
            while let Some(pkt) = recv_before(&port, deadline)
                .map_err(|error| eyre::Report::new(error).wrap_err("stream ended"))?
            {
                let abs_pkt = pkt.with_route(route.absolute_route(&pkt.route())?);
                println!("{:?}", abs_pkt);
            }
        }

        // Metadata-only mode (-m): filter to metadata packets
        (false, true) => {
            let port = raw_port("metadata")?;
            while let Some(pkt) = recv_before(&port, deadline)
                .map_err(|error| eyre::Report::new(error).wrap_err("stream ended"))?
            {
                if let tio::proto::Payload::Metadata(record, _) = pkt.payload() {
                    let abs_route = route.absolute_route(&pkt.route())?;
                    print_metadata_record(&abs_route, record);
                }
            }
        }

        // Sample mode (-d or -d -m): use DeviceTree for parsed samples
        (true, _) => {
            let tree = proxy
                .tree_with(route, port_depth, None)
                .wrap_err_with(|| format!("could not open device tree on {}", tio.root))
                .with_proxy_help()?;
            let batches = tree
                .subscribe()
                .wrap_err("could not start the data stream")
                .with_proxy_help()?;

            loop {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    break;
                }
                let next = match deadline {
                    Some(deadline) => batches.recv_deadline(deadline),
                    None => match batches.recv() {
                        Ok(batch) => Ok(batch),
                        Err(RecvError::Lagged(skipped)) => Err(RecvTimeoutError::Lagged(skipped)),
                        Err(RecvError::Disconnected) => Err(RecvTimeoutError::Disconnected),
                    },
                };
                let batch = match next {
                    Ok(batch) => batch,
                    Err(RecvTimeoutError::Lagged(skipped)) => {
                        log::warn!("dropped {skipped} sample batches");
                        continue;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(error @ RecvTimeoutError::Disconnected) => {
                        return Err(eyre::Report::new(error).wrap_err("stream ended"));
                    }
                };
                let sample_route = batch.route();
                // Schema questions are answered once per batch.
                let matched = filter.as_ref().is_none_or(|f| {
                    batch.schema().iter().any(|series| {
                        f.matches(&sample_route, batch.stream().name, series.metadata().name)
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
        }
    }

    if deadline.is_some() {
        log::info!("duration elapsed");
    }
    Ok(())
}

/// Prints the boundary/metadata lines for a batch, once, from `batch.boundary`.
pub fn print_batch_meta(batch: &SampleBatch, route: Option<&DeviceRoute>) {
    let route_str = if let Some(r) = route {
        format!("{} ", r)
    } else {
        "".to_string()
    };

    if let Some(boundary) = batch.boundary() {
        println!("# {}BOUNDARY {:?}", route_str, boundary.reason);
        if !boundary.is_continuous() {
            println!("# {}DEVICE {:?}", route_str, batch.device());
            println!("# {}STREAM {:?}", route_str, batch.stream());
            for series in batch.schema() {
                println!("# {}COLUMN {:?}", route_str, series.metadata());
            }
        }
        println!("# {}SEGMENT {:?}", route_str, batch.segment());
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

/// Print one metadata record, labelled by the descriptor it carries.
pub fn print_metadata_record(route: &DeviceRoute, record: data::Metadata<'_>) {
    let route_str = format!("{} ", route);
    match record {
        data::Metadata::Device(record) => println!("# {}DEVICE {:?}", route_str, record),
        data::Metadata::Stream(record) => println!("# {}STREAM {:?}", route_str, record),
        data::Metadata::Segment(record) => println!("# {}SEGMENT {:?}", route_str, record),
        data::Metadata::Column(record) => println!("# {}COLUMN {:?}", route_str, record),
    }
}

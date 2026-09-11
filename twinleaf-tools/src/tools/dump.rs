use crate::tools::recv_before;
use crate::{DumpCli, TioOpts};
use std::time::Instant;
use twinleaf::data::{ColumnFilter, SampleBatch, SampleRow};
use twinleaf::proto::data;
use twinleaf::tio;
use twinleaf::Connection;
use twinleaf::DeviceRoute;

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

    let connection = Connection::open(&tio.root)?;
    let route = tio.route;
    let tree = connection.tree(route);
    let tree = depth.map_or_else(|| tree.clone(), |depth| tree.to_depth(depth));

    let deadline = duration.map(|duration| Instant::now() + duration);

    log::info!("dumping from {} (route {})", tio.root, route);

    match (data, meta) {
        // Raw mode (no flags): dump all packets
        (false, false) => {
            let packets = tree.packets();
            while let Some(pkt) =
                recv_before(&packets, deadline, "packets").wrap_err("stream ended")?
            {
                println!("{:?}", pkt);
            }
        }

        // Metadata-only mode (-m): filter to metadata packets
        (false, true) => {
            let packets = tree.packets();
            while let Some(pkt) =
                recv_before(&packets, deadline, "packets").wrap_err("stream ended")?
            {
                if let tio::packet::Payload::Metadata(record, _) = pkt.payload() {
                    print_metadata_record(&pkt.route(), record);
                }
            }
        }

        // Sample mode (-d or -d -m): parsed samples
        (true, _) => {
            let batches = tree.samples();
            while let Some(batch) =
                recv_before(&batches, deadline, "sample batches").wrap_err("stream ended")?
            {
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
        println!("# {}BOUNDARY {:?}", route_str, boundary);
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

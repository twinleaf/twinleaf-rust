//! The `tio dump` subcommand, and the `Printer` that it and `tio log dump`
//! share.

use crate::DumpCli;
use std::time::Instant;
use twinleaf::data::{ColumnFilter, SampleBatch};
use twinleaf::proto::data;
use twinleaf::tio::{self, packet::Payload};
use twinleaf::DeviceRoute;
use twinleaf::{Connection, Receiver, RecvError};

pub fn run_dump(cli: DumpCli) -> eyre::Result<()> {
    let printer = Printer::new(cli.data, cli.meta, cli.glob.as_deref())?;

    let connection = Connection::open(&cli.tio.root)?;
    let route = cli.tio.route;
    let tree = connection.tree(route);
    let tree = cli
        .depth
        .map_or_else(|| tree.clone(), |depth| tree.to_depth(depth));

    let deadline = cli.duration.map(|duration| Instant::now() + duration);

    log::info!("dumping from {} (route {})", cli.tio.root, route);

    if printer.prints_samples() {
        let batches = tree.samples();
        while let Some(batch) = recv_before(&batches, deadline, "sample batches")? {
            printer.print_batch(&batch);
        }
    } else {
        let packets = tree.packets();
        while let Some(packet) = recv_before(&packets, deadline, "packets")? {
            printer.print_packet(&packet);
        }
    }

    if deadline.is_some() {
        log::info!("duration elapsed");
    }
    Ok(())
}

/// What `tio dump` and `tio log dump` print, as their `-d`, `-m` and `-g`
/// flags ask.
pub(crate) enum Printer {
    /// Every packet.
    Packets,
    /// The metadata packets alone.
    Metadata,
    /// The samples of each batch that has a column `filter` keeps, after the
    /// batch's metadata at a boundary when `meta` asks.
    Samples {
        meta: bool,
        filter: Option<ColumnFilter>,
    },
}

impl Printer {
    /// Creates the printer for the `-d` and `-m` flags and a `-g` glob.
    pub(crate) fn new(data: bool, meta: bool, glob: Option<&str>) -> eyre::Result<Printer> {
        let filter = glob
            .map(|glob| {
                ColumnFilter::new(glob).map_err(|e| eyre::eyre!("invalid glob pattern: {}", e))
            })
            .transpose()?;
        Ok(match (data, meta) {
            (false, false) => Printer::Packets,
            (false, true) => Printer::Metadata,
            (true, meta) => Printer::Samples { meta, filter },
        })
    }

    /// Returns `true` if this printer prints sample batches, not packets.
    pub(crate) fn prints_samples(&self) -> bool {
        matches!(self, Printer::Samples { .. })
    }

    /// Returns `true` if [`print_packet`](Self::print_packet) prints `packet`.
    pub(crate) fn shows_packet(&self, packet: &tio::Packet) -> bool {
        match self {
            Printer::Packets => true,
            Printer::Metadata => matches!(packet.payload(), Payload::Metadata(..)),
            Printer::Samples { .. } => false,
        }
    }

    /// Prints `packet` if this printer shows it. Returns `true` if it did.
    pub(crate) fn print_packet(&self, packet: &tio::Packet) -> bool {
        match (self, packet.payload()) {
            (Printer::Packets, _) => println!("{:?}", packet),
            (Printer::Metadata, Payload::Metadata(record, _)) => {
                print_metadata_record(&packet.route(), record)
            }
            _ => return false,
        }
        true
    }

    /// Prints `batch` if this printer shows it. Returns `true` if it did.
    pub(crate) fn print_batch(&self, batch: &SampleBatch) -> bool {
        let Printer::Samples { meta, filter } = self else {
            return false;
        };
        let route = batch.route();
        // Schema questions are answered once per batch.
        let matched = filter.as_ref().is_none_or(|f| {
            batch
                .schema()
                .iter()
                .any(|series| f.matches(&route, batch.stream().name, series.metadata().name))
        });
        if !matched {
            return false;
        }
        if *meta {
            print_batch_meta(batch);
        }
        for row in batch.iter() {
            println!("{} {}", route, row);
        }
        true
    }
}

/// Blocks for the next item, returning `None` once `deadline` passes. Lag is
/// logged and skipped. When the stream ends, returns the error that says why.
fn recv_before<T>(
    items: &Receiver<T>,
    deadline: Option<Instant>,
    kind: &str,
) -> Result<Option<T>, RecvError> {
    loop {
        let next = match deadline {
            Some(deadline) => items.recv_deadline(deadline),
            None => items.recv(),
        };
        return match next {
            Ok(item) => Ok(Some(item)),
            Err(RecvError::Lagged(skipped)) => {
                log::warn!("dropped {skipped} {kind}");
                continue;
            }
            Err(RecvError::Timeout) => Ok(None),
            Err(end) => Err(end),
        };
    }
}

/// Prints a batch's boundary, and the metadata in force from it, when the
/// batch starts at one.
fn print_batch_meta(batch: &SampleBatch) {
    let route = batch.route();
    if let Some(boundary) = batch.boundary() {
        println!("# {} BOUNDARY {:?}", route, boundary);
        if !boundary.is_continuous() {
            println!("# {} DEVICE {:?}", route, batch.device());
            println!("# {} STREAM {:?}", route, batch.stream());
            for series in batch.schema() {
                println!("# {} COLUMN {:?}", route, series.metadata());
            }
        }
        println!("# {} SEGMENT {:?}", route, batch.segment());
    }
}

/// Prints one metadata record, labelled by the descriptor it carries.
fn print_metadata_record(route: &DeviceRoute, record: data::Metadata<'_>) {
    match record {
        data::Metadata::Device(record) => println!("# {} DEVICE {:?}", route, record),
        data::Metadata::Stream(record) => println!("# {} STREAM {:?}", route, record),
        data::Metadata::Segment(record) => println!("# {} SEGMENT {:?}", route, record),
        data::Metadata::Column(record) => println!("# {} COLUMN {:?}", route, record),
    }
}

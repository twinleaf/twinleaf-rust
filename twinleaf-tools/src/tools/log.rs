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

#[cfg(test)]
mod fixtures {
    //! Logs written for the tests of the log tools.

    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use twinleaf::proto::data::{self as wire, DataType};
    use twinleaf::proto::sync::Epoch;
    use twinleaf::proto::{ColumnId, SegmentId, SessionId, StreamId};
    use twinleaf::tio::Packet;
    use twinleaf::DeviceRoute;

    /// Bytes that do not start a packet. A log stops at them.
    pub(super) const DAMAGE: &[u8] = &[0];

    /// Returns a path in the temporary directory that no other test uses.
    pub(super) fn temp_path(label: &str, extension: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "twinleaf_log_tools_{label}_{}_{}.{extension}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ))
    }

    /// Returns the metadata of one device at the root, with one 1 Hz stream
    /// named `test` of one `f32` column named `value`.
    pub(super) fn metadata() -> Vec<Packet> {
        let records = [
            wire::Metadata::Device(wire::Device {
                session: SessionId::new(1),
                n_streams: 1,
                name: "device",
                serial: "SN1",
                firmware: "fw",
            }),
            wire::Metadata::Stream(wire::Stream {
                stream_id: StreamId::new(1),
                n_columns: 1,
                n_segments: 1,
                sample_size: 4,
                buf_samples: 128,
                name: "test",
            }),
            wire::Metadata::Segment(wire::Segment {
                stream_id: StreamId::new(1),
                segment_id: SegmentId::new(0),
                flags: wire::SegmentFlags::default(),
                epoch: Epoch::UNIX,
                timeref_serial: "clock",
                timeref_session: SessionId::new(1),
                start_time: 0,
                sampling_rate: 1,
                decimation: 1,
                filter_cutoff: 0.0,
                filter_type: wire::FilterType::NONE,
            }),
            wire::Metadata::Column(wire::Column {
                stream_id: StreamId::new(1),
                index: ColumnId::new(0),
                data_type: DataType::F32,
                name: "value",
                units: "",
                description: "",
            }),
        ];
        records
            .into_iter()
            .map(|record| {
                Packet::metadata(record, wire::MetadataFlags::default(), DeviceRoute::root())
                    .expect("a valid metadata record")
            })
            .collect()
    }

    /// Returns `count` samples of that stream from sample 0, one to a packet.
    pub(super) fn samples(count: u32) -> Vec<Packet> {
        (0..count)
            .map(|n| {
                Packet::samples(1, 0, n, &(n as f32).to_le_bytes(), DeviceRoute::root())
                    .expect("valid samples")
            })
            .collect()
    }

    /// Writes `packets` and then `tail` to a new log, and returns its path.
    pub(super) fn write_log(label: &str, packets: &[Packet], tail: &[u8]) -> PathBuf {
        let path = temp_path(label, "tio");
        let mut bytes: Vec<u8> = packets
            .iter()
            .flat_map(|packet| packet.as_bytes().to_vec())
            .collect();
        bytes.extend_from_slice(tail);
        std::fs::write(&path, bytes).expect("write the log");
        path
    }
}

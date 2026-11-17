#![cfg_attr(docsrs, feature(doc_cfg))]
//! Host-side interpretation of `tio` stream data.
//!
//! The crate reads packets through [`proto`] and knows nothing of
//! transports, tasks, sessions or RPC.
//!
//! [`PacketParser`] decodes routed packets into [`SampleBatch`]es and tracks
//! each device's metadata and continuity as it goes.
//!
//! ```text
//! routed packets ──► PacketParser ──► SampleBatch
//!                           │
//!                    metadata + runs
//! ```
//!
//! Live connections expose this pipeline through a `Device`'s `samples`.
//! Most applications receive batches from there and do not construct a
//! parser.
//!
//! Restarts, rate changes, and the packet loss the parser finds start a new
//! run. The run's first batch carries a [`Boundary`], as does each batch
//! after samples its receiver lost. Every batch carries the [`Generations`]
//! of the run it belongs to.
//!
//! The crate also provides these higher-level tools.
//!
//! - [`Buffer`] retains a bounded tail of each stream.
//! - [`ColumnProcessor`] applies a [`ColumnOp`] incrementally.
//! - [`LogFile`] memory-maps and indexes recorded logs.
//! - [`csv`] writes batches as CSV tables.
#![cfg_attr(feature = "hdf5", doc = "- [`export`] writes HDF5 (`hdf5` feature).")]
#![cfg_attr(not(feature = "hdf5"), doc = "- The `hdf5` feature adds HDF5 export.")]
#![cfg_attr(
    feature = "processing",
    doc = "- [`ops`] decimates and estimates spectra (`processing` feature)."
)]
#![cfg_attr(
    feature = "processing",
    doc = "- [`History`] keeps a buffer with decimations of its columns and a held copy, to read spans of any length (`processing` feature)."
)]
#![cfg_attr(
    not(feature = "processing"),
    doc = "- The `processing` feature adds decimation and spectral estimates."
)]
//!
//! # Examples
//!
//! A [`ColumnProcessor`] runs a [`ColumnOp`] over a column that a [`Buffer`]
//! holds, and catches up with each batch the buffer takes in.
//!
//! Scan a recorded log for its structure, then decode it in batches.
//!
//! ```no_run
//! use twinleaf_data::LogFile;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let log = LogFile::open("recording.tio")?;
//! let index = log.scan(false);
//!
//! for (stream, runs) in index.summary().streams() {
//!     println!("{stream:?}: {} runs", runs.len());
//! }
//!
//! for batch in index.batches(4096) {
//!     let batch = batch?;
//!     println!("{} rows", batch.len());
//! }
//! # Ok(())
//! # }
//! ```
#![cfg_attr(
    feature = "hdf5",
    doc = r#"
Export a recorded log to HDF5.

```no_run
use std::path::Path;
use twinleaf_data::export::{Hdf5Appender, RunSplitLevel, SplitPolicy};
use twinleaf_data::LogFile;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let log = LogFile::open("recording.tio")?;
let index = log.scan(false);

let mut writer = Hdf5Appender::with_options(
    Path::new("recording.h5"),
    true,
    None,
    SplitPolicy::Continuous,
    RunSplitLevel::PerDevice,
)?;

for batch in index.batches(4096) {
    writer.write_batch(batch?)?;
}
println!("{} samples", writer.finish()?.total_samples);
# Ok(())
# }
```
"#
)]
#![deny(missing_docs)]
#![warn(
    missing_debug_implementations,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown
)]
#![warn(rustdoc::all)]

/// The wire protocol, shared with device firmware.
pub use twinleaf_proto as proto;

// The crate is kept in five parts, each a folder: `model`, what a batch is;
// `decode`, packets into batches; `store`, what is kept of each stream;
// `graph`, computation over what is kept; and `logfile`, recorded logs. Every
// public item is exported here.
mod decode;
mod graph;
mod logfile;
mod model;
mod store;

pub use logfile::csv;

#[cfg(feature = "hdf5")]
#[cfg_attr(docsrs, doc(cfg(feature = "hdf5")))]
pub use logfile::export;

#[cfg(feature = "processing")]
#[cfg_attr(docsrs, doc(cfg(feature = "processing")))]
pub use graph::ops;

#[cfg(any(test, feature = "test-util"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test-util")))]
pub mod fixtures;

#[doc(no_inline)]
pub use crate::proto::data::DataType;
pub use decode::parser::{PacketOutcome, PacketParser};
pub use decode::state::{DataError, StreamDataError};
#[cfg(feature = "processing")]
#[cfg_attr(docsrs, doc(cfg(feature = "processing")))]
pub use graph::history::{History, Window};
pub use graph::pipeline::{ColumnOp, ColumnProcessor, Continuity, Covered};
pub use logfile::reader::{LogError, LogFile, LogIndex, LogSummary, StreamSummary};
pub use logfile::writer::{LogWriter, RecordMode};
pub use model::carried::Carried;
pub use model::continuity::{Boundary, BoundaryClass, Change, Generations, Loss, Lost};
pub use model::filter::ColumnFilter;
pub use model::sample::{ColumnArray, ColumnData, SampleBatch, SampleRow, ScalarBuffer};
pub use model::schema::{
    ColumnKey, DeviceMetadataSnapshot, MetadataQuery, StreamKey, StreamMetadataSnapshot,
    StreamSchema,
};
pub use store::buffer::{Buffer, Gap, Run, RunStart};

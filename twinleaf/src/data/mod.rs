//! Host-side interpretation of `tio` stream data.
//!
//! [`PacketParser`] tracks metadata and continuity while decoding routed packets:
//!
//! ```text
//! routed packets ──► PacketParser ──► SampleBatch
//!                           │
//!                    metadata + runs
//! ```
//!
//! Live connections expose this pipeline through
//! [`Device::samples`](crate::device::Device::samples); applications usually
//! receive batches instead of constructing a parser.
//!
//! Restarts, rate changes, and packet loss start a new run. Its first batch
//! carries a [`BoundaryReason`], and every batch is stamped with [`Generations`] so
//! rows from separate runs stay separate.
//!
//! Higher-level tools:
//!
//! - [`Buffer`] retains a bounded tail of each stream.
//! - [`ColumnProcessor`] applies a [`ColumnOp`] incrementally.
//! - [`LogFile`] memory-maps and indexes recorded logs.
#![cfg_attr(feature = "hdf5", doc = "- [`export`] writes HDF5 (`hdf5` feature).")]
#![cfg_attr(not(feature = "hdf5"), doc = "- The `hdf5` feature adds HDF5 export.")]
//!
//! # Examples
//!
//! Retain a bounded history of a live stream and run an incremental
//! computation over one of its columns:
//!
//! ```no_run
//! use twinleaf::data::{Buffer, ColumnArray, ColumnKey, ColumnOp, ColumnProcessor};
//! use twinleaf::{ColumnId, Connection, DeviceRoute, StreamId};
//!
//! /// Running mean of every sample since the run began.
//! #[derive(Default)]
//! struct Mean {
//!     sum: f64,
//!     count: usize,
//!     mean: f64,
//! }
//!
//! impl ColumnOp for Mean {
//!     type Output = f64;
//!
//!     fn reset(&mut self) {
//!         *self = Mean::default();
//!     }
//!
//!     fn update_batch(&mut self, _timestamps: &[f64], values: &ColumnArray) {
//!         if let ColumnArray::F64(values) = values {
//!             self.sum += values.iter().sum::<f64>();
//!             self.count += values.len();
//!             self.mean = self.sum / self.count as f64;
//!         }
//!     }
//!
//!     fn output(&self) -> &f64 {
//!         &self.mean
//!     }
//! }
//!
//! let connection = Connection::open("tcp://localhost");
//! let device = connection.device(DeviceRoute::root());
//! let samples = device.samples();
//!
//! let mut buffer = Buffer::new(100_000);
//! let key = ColumnKey::new(DeviceRoute::root(), StreamId::new(1), ColumnId::new(0));
//! let mut mean = ColumnProcessor::new(key, Mean::default());
//!
//! while let Ok(batch) = samples.recv() {
//!     buffer.process_batch(&batch);
//!     println!("mean = {}", mean.catch_up(&buffer));
//! }
//! ```
//!
//! Scan a recorded log for its structure, then decode it in batches:
//!
//! ```no_run
//! use twinleaf::data::LogFile;
//! use twinleaf::DeviceRoute;
//!
//! let log = LogFile::open("recording.tio").expect("open the log");
//! let index = log.scan(DeviceRoute::root(), false);
//!
//! for (stream, runs) in index.summary().streams() {
//!     println!("{stream:?}: {} runs", runs.len());
//! }
//!
//! for batch in index.batches(4096) {
//!     let batch = batch.expect("a valid log");
//!     println!("{} rows", batch.len());
//! }
//! ```
#![cfg_attr(
    feature = "hdf5",
    doc = r#"
Export a recorded log to HDF5:

```no_run
use std::path::Path;
use twinleaf::data::export::{Hdf5Appender, RunSplitLevel, SplitPolicy};
use twinleaf::data::LogFile;
use twinleaf::DeviceRoute;

let log = LogFile::open("recording.tio").expect("open the log");
let index = log.scan(DeviceRoute::root(), false);

let mut writer = Hdf5Appender::with_options(
    Path::new("recording.h5"),
    true,
    false,
    None,
    SplitPolicy::Continuous,
    RunSplitLevel::PerDevice,
)
.expect("create the file");

for batch in index.batches(4096) {
    writer.write_batch(batch.expect("a valid log")).expect("write rows");
}
println!("{} samples", writer.finish().expect("finish").total_samples);
```
"#
)]

mod buffer;
mod coalesce;
mod filter;
mod metadata;
mod parser;
mod pipeline;
mod reader;
mod sample;
mod state;

#[cfg(feature = "hdf5")]
#[cfg_attr(docsrs, doc(cfg(feature = "hdf5")))]
pub mod export;

#[cfg(test)]
mod fixtures;

pub use crate::tio::proto::DataType;
pub use buffer::{Buffer, Run};
pub use filter::ColumnFilter;
pub use metadata::{DeviceMetadataSnapshot, MetadataQuery, StreamMetadataSnapshot};
pub use parser::{PacketOutcome, PacketParser};
pub use pipeline::{ColumnOp, ColumnProcessor};
pub use reader::{LogError, LogFile, LogIndex, LogSummary, PacketIter, StreamSummary};
pub use sample::{
    BoundaryClass, BoundaryReason, ColumnArray, ColumnData, ColumnKey, Generations, SampleBatch,
    SampleRow, ScalarBuffer, Series, StreamKey,
};
pub use state::{PacketError, StreamDataError};

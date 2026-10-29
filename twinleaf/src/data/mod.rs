//! Host-side interpretation of `tio` stream data.
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
//! Live connections expose this pipeline through
//! [`Device::samples`](crate::device::Device::samples). Most applications
//! receive batches from there and do not construct a parser.
//!
//! Restarts, rate changes, and packet loss start a new run. The run's first
//! batch carries a [`BoundaryReason`]. Every batch carries the [`Generations`]
//! of the run it belongs to.
//!
//! The module also provides these higher-level tools.
//!
//! - [`Buffer`] retains a bounded tail of each stream.
//! - [`ColumnProcessor`] applies a [`ColumnOp`] incrementally.
//! - [`LogFile`] memory-maps and indexes recorded logs.
#![cfg_attr(feature = "hdf5", doc = "- [`export`] writes HDF5 (`hdf5` feature).")]
#![cfg_attr(not(feature = "hdf5"), doc = "- The `hdf5` feature adds HDF5 export.")]
#![cfg_attr(
    feature = "processing",
    doc = "- [`ops`] decimates and estimates spectra (`processing` feature)."
)]
#![cfg_attr(
    not(feature = "processing"),
    doc = "- The `processing` feature adds decimation and spectral estimates."
)]
//!
//! # Examples
//!
//! Retain a bounded history of a live stream and run an incremental
//! computation over one of its columns.
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
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let connection = Connection::connect()?;
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
//! # Ok(())
//! # }
//! ```
//!
//! Scan a recorded log for its structure, then decode it in batches.
//!
//! ```no_run
//! use twinleaf::data::LogFile;
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
use twinleaf::data::export::{Hdf5Appender, RunSplitLevel, SplitPolicy};
use twinleaf::data::LogFile;

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

#[cfg(feature = "processing")]
#[cfg_attr(docsrs, doc(cfg(feature = "processing")))]
pub mod ops;

#[cfg(test)]
mod fixtures;

#[doc(no_inline)]
pub use crate::proto::data::DataType;
pub use buffer::{Buffer, Run};
pub use filter::ColumnFilter;
pub use metadata::{
    ColumnKey, DeviceMetadataSnapshot, MetadataQuery, StreamKey, StreamMetadataSnapshot,
};
pub use parser::{PacketOutcome, PacketParser};
pub use pipeline::{ColumnOp, ColumnProcessor};
pub use reader::{LogError, LogFile, LogIndex, LogSummary, PacketIter, StreamSummary};
pub use sample::{
    BoundaryClass, BoundaryReason, ColumnArray, ColumnData, Generations, SampleBatch, SampleRow,
    ScalarBuffer, Series,
};
pub use state::{PacketError, StreamDataError};

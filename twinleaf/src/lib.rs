#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
//! # Layers
//!
//! Each layer uses only the layers above it in this table, and
//! `tests/architecture.rs` checks that.
//!
//! | Layer | Holds | Knows nothing of |
//! |---|---|---|
//! | `seam` | the channels, locks, timers and sockets, real or simulated | packets |
//! | `rt` | the crate's runtime, and the system's sleep and console guards its threads hold | packets |
//! | [`tio`] | packets, and the serial, TCP and UDP ports that move them | samples, RPC tables or the proxy |
//! | [`data`] | samples decoded from packets, and their batches and exports, in the crate `twinleaf-data` re-exported here | transports or tasks |
//! | `session` | the call session between an app and its proxy, not a device's boot session, with the device state, its store and the wire protocol of both ends | the proxy's links or the app's views |
//! | [`proxy`] | each device link, shared among its clients | [`Connection`] and its views |
//! | [`device`] | the app's API, whose runtime alone starts a proxy | |
//! | [`firmware`] | firmware queries and updates through a [`Device`] | |
//!
//! `twinleaf` is the host library. Its layers run tasks and talk to devices.
//! The data layer is the crate `twinleaf-data`, re-exported as [`data`]. It
//! depends on the protocol crate alone and is the part that also builds for
//! `wasm32-unknown-unknown`. Its
//! [`LogFile::open`](data::LogFile::open) memory-maps a file and exists
//! wherever there is an operating system.
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
#![deny(missing_docs)]
#![warn(
    missing_debug_implementations,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown
)]
#![warn(rustdoc::all)]
#![deny(clippy::await_holding_lock)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

pub use twinleaf_data as data;
pub mod device;
pub mod firmware;
pub mod proxy;
mod rt;
mod seam;
mod session;
pub mod tio;

use seam::{chan, net};

/// The wire protocol, shared with device firmware.
pub use twinleaf_proto as proto;

#[doc(no_inline)]
pub use flume;

pub use data::{SampleBatch, SampleRow};
pub use device::{Connection, Device, DeviceTree, Receiver, RecvError};
#[doc(no_inline)]
pub use proto::{ColumnId, DeviceRoute, SampleNumber, SegmentId, SessionId, StreamId};

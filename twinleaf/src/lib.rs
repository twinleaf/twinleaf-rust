//! Host-side access to Twinleaf instruments over serial, TCP, or UDP.
//!
//! The whole API is six nouns. A [`Connection`] owns the link and its I/O
//! thread; [`Connection::tree`] gives the [`DeviceTree`] it reaches;
//! [`DeviceTree::device`] mints a [`Device`], which is where every RPC lives.
//! Both the tree and a device hand out owned [`Receiver`]s: `subscribe()`
//! delivers [`SampleBatch`]es (route-tagged, columnar, cheap to clone) and
//! `events()` delivers [`TreeEvent`]s. Nothing here needs a UI loop to make
//! progress — the library drives the transport itself.
//!
//! ```no_run
//! use std::time::Duration;
//! use twinleaf::{Connection, DeviceRoute};
//!
//! let conn = Connection::open("tcp://localhost");
//! let device = conn.tree().device(DeviceRoute::root());
//!
//! let name: String = device.get("dev.name")?;
//! println!("connected to {name}");
//!
//! let batches = device.subscribe()?;
//! let batch = batches.recv_timeout(Duration::from_secs(5))?;
//! for row in batch.iter() {
//!     let values: Vec<_> = row.values().collect();
//!     println!("{} {values:?}", row.timestamp_end());
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! [`data`] holds the sample, metadata, and log vocabulary the batches speak;
//! [`firmware`] updates instruments; [`tio`] is the raw packet layer beneath
//! all of it, for tools that need to see the wire.

pub mod data;
pub mod device;
pub mod firmware;
pub mod tio;

pub use data::{SampleBatch, SampleRow};
pub use device::{Device, DeviceRoute, DeviceTree, Receiver, TreeEvent};
pub use tio::proxy::Connection;

//! Host-side access to Twinleaf instruments over serial, TCP, or UDP.
//!
//! The high-level API follows the routed connection. A [`Connection`] owns the
//! link and its I/O thread; [`Connection::tree`] gives the [`DeviceTree`] it
//! reaches; [`DeviceTree`] takes a route per operation, while
//! [`DeviceTree::device`] mints a [`Device`] that binds one route for repeated
//! operations. All three are free, cloneable views of the one connection, and
//! all three offer the same three subscriptions, each an owned [`Receiver`]
//! filtered to what the view covers: `samples()` delivers [`SampleBatch`]es
//! (route-tagged, columnar, cheap to clone), `events()` delivers [`Event`]s —
//! the link's, the population's, and each device's — and `packets()` taps the
//! wire itself. Nothing here needs a UI loop to make progress — the library
//! drives the transport itself.
//!
//! ```no_run
//! use std::time::Duration;
//! use twinleaf::{Connection, DeviceRoute};
//!
//! let conn = Connection::open("tcp://localhost");
//! let device = conn.device(DeviceRoute::root());
//!
//! let name: String = device.get("dev.name").expect("read device name");
//! println!("connected to {name}");
//!
//! let batches = device.samples();
//! let batch = batches
//!     .recv_timeout(Duration::from_secs(5))
//!     .expect("receive a sample batch");
//! for row in batch.iter() {
//!     let values: Vec<_> = row.values().collect();
//!     println!("{} {values:?}", row.timestamp_end());
//! }
//! ```
//!
//! Most applications stop at `SampleBatch`: its metadata accessors return the
//! borrowed device, stream, segment, and column fields defined by
//! `twinleaf-proto`. [`data`] also provides optional buffering, column
//! processing, log reading, and packet parsing for applications that need a
//! host-side data pipeline. [`firmware`] updates instruments; [`tio`] is the
//! wire vocabulary and the proxy-server plumbing beneath all of it, not part
//! of this story.

pub mod data;
pub mod device;
pub mod firmware;
pub mod tio;

pub use data::{SampleBatch, SampleRow};
pub use device::{
    Connection, Device, DeviceEvent, DeviceRoute, DeviceTree, Event, LinkEvent, Receiver,
    RecvError, TreeEvent,
};
pub use twinleaf_proto::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};

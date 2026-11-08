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
//! | [`data`] | samples decoded from packets, and their batches and exports | transports or tasks |
//! | `session` | the call session between an app and its proxy, not a device's boot session, with the device state, its store and the wire protocol of both ends | the proxy's links or the app's views |
//! | [`proxy`] | each device link, shared among its clients | [`Connection`] and its views |
//! | [`device`] | the app's API, whose runtime alone starts a proxy | |
//! | [`firmware`] | firmware queries and updates through a [`Device`] | |
//!
//! The `host` feature, on by default, holds every layer that runs tasks or
//! reaches the operating system. These are `seam`, `rt`, the transports of
//! [`tio`], `session`, [`proxy`], [`device`], [`firmware`], and
//! [`LogFile::open`](data::LogFile::open). Without it the crate is the
//! packet codec and the data layer, which build for `wasm32-unknown-unknown`.
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

pub mod data;
#[cfg(feature = "host")]
pub mod device;
#[cfg(feature = "host")]
pub mod firmware;
#[cfg(feature = "host")]
pub mod proxy;
#[cfg(feature = "host")]
mod rt;
#[cfg(feature = "host")]
mod seam;
#[cfg(feature = "host")]
mod session;
pub mod tio;

#[cfg(feature = "host")]
use seam::{chan, net};

/// The wire protocol, shared with device firmware.
pub use twinleaf_proto as proto;

#[doc(no_inline)]
#[cfg(feature = "host")]
pub use flume;

pub use data::{SampleBatch, SampleRow};
#[cfg(feature = "host")]
pub use device::{Connection, Device, DeviceTree, Receiver, RecvError};
#[doc(no_inline)]
pub use proto::{ColumnId, DeviceRoute, SampleNumber, SegmentId, SessionId, StreamId};

#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![warn(
    missing_debug_implementations,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown
)]
#![warn(rustdoc::all)]
#![deny(clippy::await_holding_lock)]

pub mod data;
pub mod device;
pub mod firmware;
pub mod proxy;
mod rt;
pub mod tio;

/// The wire protocol, shared with device firmware.
pub use twinleaf_proto as proto;

#[doc(no_inline)]
pub use flume;

pub use data::{SampleBatch, SampleRow};
pub use device::{
    Connection, Device, DeviceEvent, DeviceTree, Event, LinkEvent, Receiver, RecvError,
};
#[doc(no_inline)]
pub use proto::{ColumnId, DeviceRoute, SampleNumber, SegmentId, SessionId, StreamId};

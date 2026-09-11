#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![warn(rustdoc::all)]

pub mod data;
pub mod device;
pub mod firmware;
pub mod tio;

/// The wire protocol, shared with device firmware.
pub use twinleaf_proto as proto;

pub use data::{SampleBatch, SampleRow};
pub use device::{
    Connection, Device, DeviceEvent, DeviceTree, Event, LinkEvent, Receiver, RecvError, TreeEvent,
};
#[doc(no_inline)]
pub use proto::{ColumnId, DeviceRoute, SampleNumber, SegmentId, SessionId, StreamId};

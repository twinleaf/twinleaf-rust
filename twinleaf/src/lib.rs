#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(rustdoc::all)]

#[deny(missing_docs)]
pub mod data;
#[deny(missing_docs)]
pub mod device;
pub mod firmware;
pub mod tio;

pub use data::{SampleBatch, SampleRow};
pub use device::{
    Connection, Device, DeviceEvent, DeviceRoute, DeviceTree, Event, LinkEvent, Receiver,
    RecvError, TreeEvent,
};
pub use twinleaf_proto::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};

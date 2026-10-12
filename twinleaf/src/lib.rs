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

pub mod legacy;
pub mod proxy;
mod rt;
pub mod tio;

/// The wire protocol, shared with device firmware.
pub use twinleaf_proto as proto;

#[doc(no_inline)]
pub use proto::{ColumnId, DeviceRoute, SampleNumber, SegmentId, SessionId, StreamId};

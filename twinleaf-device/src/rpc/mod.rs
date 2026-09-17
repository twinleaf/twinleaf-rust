//! What a request finds: the table a device declares, the settings its
//! properties read and write, and the capture buffer one of them reads back.
//!
//! Inward come a method name and its argument bytes; outward go a [`Reply`],
//! the flags word `rpc.hash` covers, and the [`Changed`] a write reports so
//! its announcement can be sent. Nothing here knows which request is being
//! answered or when: [`crate::device::Device`] decides that.

mod capture;
mod settings;
mod table;

pub use capture::{Capture, Selector, Status};
pub use settings::{Changed, Persisted, Scalar, Setting, Text};
pub use table::{
    hash, id, info, list, match_name, name, put, read, Access, Kind, Method, Reply, RpcSpec, Std,
    REPLY_MAX, STANDARD,
};

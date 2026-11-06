#![doc = include_str!("../README.md")]
#![cfg_attr(not(test), no_std)]
#![deny(missing_docs)]
#![warn(
    missing_debug_implementations,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown
)]
#![warn(rustdoc::all)]

#[cfg(feature = "contract")]
extern crate alloc;

#[cfg(feature = "contract")]
pub mod contract;
pub mod identifiers;
pub mod packet;
mod payload;
pub mod route;
pub mod serial;

pub use payload::{capture, data, heartbeat, log, rpc, settings, sync};

pub use identifiers::{
    BoardId, ColumnId, DeviceSerial, FirmwareMagic, FirmwareSerial, HwRev, RpcMethodId,
    RpcRequestId, SampleNumber, SegmentId, SessionId, StreamId,
};
pub use packet::{lane_of, Lane, PacketType};
pub use route::{DeviceRoute, RouteError};

/// The protocol revision a device declares in its record when it keeps the laws `contract` states.
pub const PROTOCOL: u8 = 1;

/// Default TCP port of a proxy or a networked device, packets back to back.
pub const TCP_DEFAULT_PORT: u16 = 7855;
/// Default UDP port, one packet per datagram.
pub const UDP_DEFAULT_PORT: u16 = 7855;
/// Default WebSocket port, one packet per binary message.
pub const WS_DEFAULT_PORT: u16 = 7853;

//! `no_std` Twinleaf wire types shared by hosts and devices.
//!
//! [`packet`] and [`serial`] frame the wire, [`route`] addresses it, and
//! [`identifiers`] names its entities; [`payload`] holds one codec per packet
//! domain. Every wire layout is normative here, locked by byte-exact tests.

#![cfg_attr(not(test), no_std)]

pub mod identifiers;
pub mod packet;
pub mod payload;
pub mod route;
pub mod serial;

pub use payload::{capture, data, heartbeat, log, rpc, settings, sync};

pub use identifiers::{
    BoardId, ColumnId, DeviceSerial, FirmwareMagic, FirmwareSerial, HwRev, RpcMethodId,
    RpcRequestId, SampleNumber, SegmentId, SessionId, StreamId,
};
pub use packet::PacketType;
pub use route::{DeviceRoute, RouteError};

pub const TCP_DEFAULT_PORT: u16 = 7855;
pub const UDP_DEFAULT_PORT: u16 = 7855;
pub const WS_DEFAULT_PORT: u16 = 7853;

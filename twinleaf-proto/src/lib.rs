#![doc = include_str!("../README.md")]
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

/// Default TCP port of a proxy or a networked device, packets back to back.
pub const TCP_DEFAULT_PORT: u16 = 7855;
/// Default UDP port, one packet per datagram.
pub const UDP_DEFAULT_PORT: u16 = 7855;
/// Default WebSocket port, one packet per binary message.
pub const WS_DEFAULT_PORT: u16 = 7853;

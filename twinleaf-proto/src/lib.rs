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
    BoardId, DeviceSerial, FirmwareMagic, FirmwareSerial, HwRev, RpcMethodId, RpcRequestId,
    SessionId,
};
pub use packet::PacketType;
pub use route::{DeviceRoute, RouteError};

use crc::{Crc, CRC_32_ISO_HDLC};

/// Packet header size in bytes.
pub const HEADER_SIZE: usize = 4;
pub const MAX_PACKET_SIZE: usize = 512;
pub const MAX_ROUTING_SIZE: usize = 8;
pub const MAX_TTL: u8 = 15;
pub const MAX_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - HEADER_SIZE - MAX_ROUTING_SIZE;

pub const TCP_DEFAULT_PORT: u16 = 7855;
pub const UDP_DEFAULT_PORT: u16 = 7855;
pub const WS_DEFAULT_PORT: u16 = 7853;

pub const SLIP_END: u8 = 0xC0;
pub const SLIP_ESC: u8 = 0xDB;
pub const SLIP_ESC_END: u8 = 0xDC;
pub const SLIP_ESC_ESC: u8 = 0xDD;

pub const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_is_the_iso_hdlc_variant() {
        assert_eq!(CRC32.checksum(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn size_constants() {
        assert_eq!(MAX_PAYLOAD_SIZE, 500);
    }
}

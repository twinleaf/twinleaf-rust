//! The packet header and whole-packet buffers.
//!
//! Wire layout: `[type u8][routing_size_and_ttl u8][payload_size u16le]`,
//! then payload, then up to 8 routing bytes appended after the payload.

use core::ops::Range;

use crate::route::{DeviceRoute, ForwardError};

/// Valid packet type, including unknown extension values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PacketType(u8);

impl PacketType {
    /// Log message.
    pub const LOG: Self = Self(1);
    /// RPC request.
    pub const RPC_REQ: Self = Self(2);
    /// RPC reply.
    pub const RPC_REP: Self = Self(3);
    /// RPC error.
    pub const RPC_ERROR: Self = Self(4);
    /// Heartbeat.
    pub const HEARTBEAT: Self = Self(5);
    /// Legacy timebase descriptor, unsupported.
    pub const LEGACY_TIMEBASE: Self = Self(6);
    /// Legacy source descriptor, unsupported.
    pub const LEGACY_SOURCE: Self = Self(7);
    /// Legacy stream descriptor, unsupported.
    pub const LEGACY_STREAM: Self = Self(8);
    /// Stream metadata record.
    pub const METADATA: Self = Self(11);
    /// Setting change broadcast.
    pub const SETTING: Self = Self(12);
    /// Time reference, hub to children.
    pub const SYNC: Self = Self(62);
    /// Console text.
    pub const TEXT: Self = Self(63);
    /// First application defined type.
    pub const USER: Self = Self(64);
    /// Proxy link status, a `USER` packet.
    pub const PROXY_STATUS: Self = Self(64);
    /// Samples of stream 0. Stream n is `STREAM0 + n`.
    pub const STREAM0: Self = Self(128);

    /// Type from its byte. None for 0, 9, 10, and 13.
    pub const fn try_new(value: u8) -> Option<Self> {
        if matches!(value, 0 | 9 | 10 | 13) {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Create a packet type, panicking for reserved values.
    pub const fn new(value: u8) -> Self {
        assert!(
            !matches!(value, 0 | 9 | 10 | 13),
            "invalid or reserved packet type"
        );
        Self(value)
    }

    /// The type byte.
    pub const fn value(self) -> u8 {
        self.0
    }

    /// Sample packet type of stream `stream_id`. None above 127.
    pub const fn stream(stream_id: u8) -> Option<Self> {
        match 128u8.checked_add(stream_id) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// The stream id if this is a sample packet type.
    pub const fn stream_id(self) -> Option<u8> {
        if self.0 >= Self::STREAM0.0 {
            Some(self.0 - Self::STREAM0.0)
        } else {
            None
        }
    }
}

/// Why bytes do not parse as a packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum PacketError {
    /// Not enough bytes yet.
    NeedMore,
    /// Type byte 0, 9, 10, or 13.
    InvalidPacketType,
    /// Payload above 500 bytes, or packet above 512.
    PayloadTooBig,
    /// Routing above 8 bytes.
    RoutingTooBig,
}

/// The four byte packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Header {
    /// Packet type.
    pub ptype: PacketType,
    /// Number of routing bytes, 0 to 8.
    pub routing_size: u8,
    /// Hops remaining, 0 to 15.
    pub ttl: u8,
    /// Payload length, 0 to 500.
    pub payload_size: u16,
}

impl Header {
    /// Header length, 4 bytes.
    pub const SIZE: usize = 4;
    /// Largest TTL, 15.
    pub const MAX_TTL: u8 = 15;

    /// Header with no routing and a TTL of 0.
    pub fn new(ptype: PacketType, payload_size: u16) -> Self {
        Self {
            ptype,
            routing_size: 0,
            ttl: 0,
            payload_size,
        }
    }

    /// Parse the 4-byte header. Returns `None` for a malformed header.
    pub fn parse(buf: &[u8; Header::SIZE]) -> Option<Self> {
        Self::parse_prefix(buf).ok()
    }

    /// Parse a header from a possibly incomplete packet.
    pub fn parse_prefix(buf: &[u8]) -> Result<Self, PacketError> {
        let Some(&raw_ptype) = buf.first() else {
            return Err(PacketError::NeedMore);
        };
        let ptype = PacketType::try_new(raw_ptype).ok_or(PacketError::InvalidPacketType)?;
        if buf.len() < Header::SIZE {
            return Err(PacketError::NeedMore);
        }
        let routing_size = buf[1] & 0x0F;
        let payload_size = u16::from_le_bytes([buf[2], buf[3]]);
        if routing_size as usize > DeviceRoute::MAX_HOPS {
            return Err(PacketError::RoutingTooBig);
        }
        if payload_size as usize > Packet::MAX_PAYLOAD {
            return Err(PacketError::PayloadTooBig);
        }
        let total = Header::SIZE + payload_size as usize + routing_size as usize;
        if total > Packet::MAX_SIZE {
            return Err(PacketError::PayloadTooBig);
        }
        Ok(Self {
            ptype,
            routing_size,
            ttl: buf[1] >> 4,
            payload_size,
        })
    }

    /// Write the four header bytes.
    pub fn write(&self, buf: &mut [u8; Header::SIZE]) {
        buf[0] = self.ptype.value();
        buf[1] = (self.ttl << 4) | (self.routing_size & 0x0F);
        buf[2..4].copy_from_slice(&self.payload_size.to_le_bytes());
    }

    /// Bytes following the header on the wire (payload + routing).
    pub fn body_len(&self) -> usize {
        self.payload_size as usize + self.routing_size as usize
    }

    /// Total packet length.
    pub fn packet_len(&self) -> usize {
        Header::SIZE + self.body_len()
    }

    /// Byte range of the payload within the packet.
    pub fn payload_range(&self) -> Range<usize> {
        Header::SIZE..Header::SIZE + self.payload_size as usize
    }
}

/// Borrowed packet payload and routing bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketView<'a> {
    /// The parsed header.
    pub header: Header,
    /// The payload bytes.
    pub payload: &'a [u8],
    /// The routing bytes, next hop last.
    pub routing: &'a [u8],
}

impl<'a> PacketView<'a> {
    /// Parse one packet from the start of `raw`, returning the packet and the
    /// number of bytes consumed. Trailing bytes belong to the next packet.
    pub fn parse_prefix(raw: &'a [u8]) -> Result<(Self, usize), PacketError> {
        let header = Header::parse_prefix(raw)?;
        let packet_len = header.packet_len();
        if raw.len() < packet_len {
            return Err(PacketError::NeedMore);
        }
        let payload_range = header.payload_range();
        let routing_start = payload_range.end;
        Ok((
            Self {
                header,
                payload: &raw[payload_range],
                routing: &raw[routing_start..packet_len],
            },
            packet_len,
        ))
    }
}

/// Complete wire packet with space for route tagging.
#[derive(Clone)]
pub struct Packet {
    len: u16,
    buf: [u8; Packet::MAX_SIZE],
}

impl Packet {
    /// Largest packet, 512 bytes.
    pub const MAX_SIZE: usize = 512;
    /// Largest payload, 500 bytes.
    pub const MAX_PAYLOAD: usize = Self::MAX_SIZE - Header::SIZE - DeviceRoute::MAX_HOPS;

    /// Copy one complete, valid packet.
    pub fn from_slice(data: &[u8]) -> Option<Self> {
        let header = Header::parse_prefix(data).ok()?;
        if header.packet_len() != data.len() {
            return None;
        }
        let mut packet = Self {
            len: data.len() as u16,
            buf: [0; Packet::MAX_SIZE],
        };
        packet.buf[..data.len()].copy_from_slice(data);
        Some(packet)
    }

    /// The packet bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    /// The payload bytes, writable. The header and routing bytes stay as they are.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        // Every constructor parses, and pop/push_hop only edit routing, so a
        // `Packet` always has a readable header.
        let range = match Header::parse_prefix(self.as_slice()) {
            Ok(header) => header.payload_range(),
            Err(_) => 0..0,
        };
        &mut self.buf[range]
    }

    /// Remove the next hop, for a router deciding where a packet goes.
    pub fn pop_hop(&mut self) -> Result<u8, ForwardError> {
        let (hop, len) = crate::route::pop_hop(&mut self.buf[..self.len as usize])?;
        self.len = len as u16;
        Ok(hop)
    }

    /// Tag a packet with the port it arrived on.
    pub fn push_hop(&mut self, hop: u8) -> Result<(), ForwardError> {
        let len = crate::route::push_hop(&mut self.buf, hop)?;
        self.len = len as u16;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_ceiling_is_the_packet_less_header_and_routing() {
        assert_eq!(Packet::MAX_PAYLOAD, 500);
    }

    #[test]
    fn header_roundtrip() {
        let h = Header {
            ptype: PacketType::RPC_REQ,
            routing_size: 2,
            ttl: 3,
            payload_size: 500,
        };
        let mut buf = [0u8; 4];
        h.write(&mut buf);
        assert_eq!(buf, [2, 0x32, 0xF4, 0x01]);
        assert_eq!(Header::parse(&buf), Some(h));
    }

    #[test]
    fn header_rejects_oversize() {
        // Payload has a fixed 500-byte ceiling even when routing is empty.
        assert!(Header::parse(&[8, 0, 0xFD, 0x01]).is_none());
        // routing nibble 9 > max 8
        assert!(Header::parse(&[8, 0x09, 0, 0]).is_none());
    }

    #[test]
    fn packet_view_borrows_one_packet() {
        let raw = [PacketType::RPC_REQ.value(), 0x12, 3, 0, 10, 11, 12, 7, 99];
        let (packet, used) = PacketView::parse_prefix(&raw).unwrap();
        assert_eq!(used, 9);
        assert_eq!(packet.header.ttl, 1);
        assert_eq!(packet.payload, [10, 11, 12]);
        assert_eq!(packet.routing, [7, 99]);
    }

    /// A router edits the payload and nothing else: the header and the hops
    /// tagged after it stay where they were.
    #[test]
    fn payload_mut_reaches_the_payload_alone() {
        let raw = [PacketType::RPC_REP.value(), 0, 3, 0, 10, 11, 12];
        let mut packet = Packet::from_slice(&raw).unwrap();
        packet.push_hop(4).unwrap();
        packet.payload_mut().copy_from_slice(&[1, 2, 3]);

        let (view, _) = PacketView::parse_prefix(packet.as_slice()).unwrap();
        assert_eq!(view.payload, [1, 2, 3]);
        assert_eq!(view.routing, [4]);
        assert_eq!(view.header.ptype, PacketType::RPC_REP);
    }

    #[test]
    fn prefix_parser_distinguishes_incomplete_and_invalid() {
        assert_eq!(Header::parse_prefix(&[]), Err(PacketError::NeedMore));
        assert_eq!(
            Header::parse_prefix(&[9]),
            Err(PacketError::InvalidPacketType)
        );
        assert_eq!(
            PacketView::parse_prefix(&[PacketType::HEARTBEAT.value(), 0, 4, 0, 1]),
            Err(PacketError::NeedMore)
        );
    }

    #[test]
    fn packet_type_preserves_extensions_and_stream_ids() {
        assert_eq!(PacketType::new(77).value(), 77);
        assert_eq!(PacketType::stream(7).unwrap().value(), 135);
        assert_eq!(PacketType::stream(7).unwrap().stream_id(), Some(7));
        for reserved in [0, 9, 10, 13] {
            assert!(PacketType::try_new(reserved).is_none());
        }
    }
}

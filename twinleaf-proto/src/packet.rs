//! Packet header parsing/serialization
//!
//! Wire layout: `[type u8][routing_size_and_ttl u8][payload_size u16le]`,
//! then payload, then up to 8 routing bytes appended after the payload.

use core::ops::Range;

use crate::route::ForwardError;

use crate::{HEADER_SIZE, MAX_PACKET_SIZE, MAX_PAYLOAD_SIZE, MAX_ROUTING_SIZE};

/// Valid packet type, including unknown extension values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PacketType(u8);

impl PacketType {
    pub const LOG: Self = Self(1);
    pub const RPC_REQ: Self = Self(2);
    pub const RPC_REP: Self = Self(3);
    pub const RPC_ERROR: Self = Self(4);
    pub const HEARTBEAT: Self = Self(5);
    pub const LEGACY_TIMEBASE: Self = Self(6);
    pub const LEGACY_SOURCE: Self = Self(7);
    pub const LEGACY_STREAM: Self = Self(8);
    pub const METADATA: Self = Self(11);
    pub const SETTING: Self = Self(12);
    /// Firmware-internal synchronization packet.
    pub const SYNC: Self = Self(62);
    pub const TEXT: Self = Self(63);
    pub const USER: Self = Self(64);
    pub const PROXY_STATUS: Self = Self(64);
    pub const RPC_UPDATE: Self = Self(65);
    pub const STREAM0: Self = Self(128);

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

    pub const fn value(self) -> u8 {
        self.0
    }

    pub const fn stream(stream_id: u8) -> Option<Self> {
        match 128u8.checked_add(stream_id) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn stream_id(self) -> Option<u8> {
        if self.0 >= Self::STREAM0.0 {
            Some(self.0 - Self::STREAM0.0)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum PacketError {
    NeedMore,
    InvalidPacketType,
    PayloadTooBig,
    RoutingTooBig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Header {
    pub ptype: PacketType,
    pub routing_size: u8,
    pub ttl: u8,
    pub payload_size: u16,
}

impl Header {
    pub fn new(ptype: PacketType, payload_size: u16) -> Self {
        Self {
            ptype,
            routing_size: 0,
            ttl: 0,
            payload_size,
        }
    }

    /// Parse the 4-byte header. Returns `None` for a malformed header.
    pub fn parse(buf: &[u8; HEADER_SIZE]) -> Option<Self> {
        Self::parse_prefix(buf).ok()
    }

    /// Parse a header from a possibly incomplete packet.
    pub fn parse_prefix(buf: &[u8]) -> Result<Self, PacketError> {
        let Some(&raw_ptype) = buf.first() else {
            return Err(PacketError::NeedMore);
        };
        let ptype = PacketType::try_new(raw_ptype).ok_or(PacketError::InvalidPacketType)?;
        if buf.len() < HEADER_SIZE {
            return Err(PacketError::NeedMore);
        }
        let routing_size = buf[1] & 0x0F;
        let payload_size = u16::from_le_bytes([buf[2], buf[3]]);
        if routing_size as usize > MAX_ROUTING_SIZE {
            return Err(PacketError::RoutingTooBig);
        }
        if payload_size as usize > MAX_PAYLOAD_SIZE {
            return Err(PacketError::PayloadTooBig);
        }
        let total = HEADER_SIZE + payload_size as usize + routing_size as usize;
        if total > MAX_PACKET_SIZE {
            return Err(PacketError::PayloadTooBig);
        }
        Ok(Self {
            ptype,
            routing_size,
            ttl: buf[1] >> 4,
            payload_size,
        })
    }

    pub fn write(&self, buf: &mut [u8; HEADER_SIZE]) {
        buf[0] = self.ptype.value();
        buf[1] = (self.ttl << 4) | (self.routing_size & 0x0F);
        buf[2..4].copy_from_slice(&self.payload_size.to_le_bytes());
    }

    /// Bytes following the header on the wire (payload + routing).
    pub fn body_len(&self) -> usize {
        self.payload_size as usize + self.routing_size as usize
    }

    pub fn packet_len(&self) -> usize {
        HEADER_SIZE + self.body_len()
    }

    pub fn payload_range(&self) -> Range<usize> {
        HEADER_SIZE..HEADER_SIZE + self.payload_size as usize
    }
}

/// Borrowed packet payload and routing bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketView<'a> {
    pub header: Header,
    pub payload: &'a [u8],
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
    buf: [u8; crate::MAX_PACKET_SIZE],
}

impl Packet {
    /// Copy one complete, valid packet.
    pub fn from_slice(data: &[u8]) -> Option<Self> {
        let header = Header::parse_prefix(data).ok()?;
        if header.packet_len() != data.len() {
            return None;
        }
        let mut packet = Self {
            len: data.len() as u16,
            buf: [0; crate::MAX_PACKET_SIZE],
        };
        packet.buf[..data.len()].copy_from_slice(data);
        Some(packet)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    /// The payload alone, for a router editing a packet in place — a hub
    /// rewriting the RPC request ids it forwards. The header and the routing
    /// bytes are out of reach, so the packet stays as valid as it was built.
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
        assert_eq!(PacketType::try_new(65), Some(PacketType::RPC_UPDATE));
        assert_eq!(PacketType::new(77).value(), 77);
        assert_eq!(PacketType::stream(7).unwrap().value(), 135);
        assert_eq!(PacketType::stream(7).unwrap().stream_id(), Some(7));
        for reserved in [0, 9, 10, 13] {
            assert!(PacketType::try_new(reserved).is_none());
        }
    }
}

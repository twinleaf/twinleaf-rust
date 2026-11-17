//! The packet header and whole-packet buffers.
//!
//! A packet is the header `[type u8][routing_size_and_ttl u8][payload_size u16le]`,
//! then the payload, then up to 8 routing bytes.
//!
//! [`Header`] parses and writes the header. [`PacketView`] borrows a framed
//! packet and [`Packet`] owns one. [`Checked`] borrows a packet whose route
//! and payload have been read, and is the one check that bytes are a whole
//! valid packet. [`Payload`] is a payload read as the type its header names.

use core::ops::Range;

use crate::data::{self, split_metadata, MetadataType};
use crate::heartbeat::Heartbeat;
use crate::log::LogMessage;
use crate::proxy::{LinkStatus, ProxyRx};
use crate::route::{DeviceRoute, ForwardError};
use crate::rpc;
use crate::settings::Setting;
use crate::StreamId;

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
    /// Legacy proxy link status, a `USER` packet. Hosts still decode it, and
    /// proxies send it only to clients that predate `PROXY_RX`.
    pub const PROXY_STATUS: Self = Self(64);
    /// Proxy control request, from a client to its proxy. Host only, and not
    /// routed to devices.
    pub const PROXY_TX: Self = Self(66);
    /// Proxy control reply or link status, from a proxy to its client. Host
    /// only, and not routed to devices.
    pub const PROXY_RX: Self = Self(67);
    /// Samples of stream 0. Stream n is `STREAM0 + n`.
    pub const STREAM0: Self = Self(128);

    /// Creates a packet type from its byte. Returns `None` for 0, 9, 10, and 13.
    pub const fn try_new(value: u8) -> Option<Self> {
        if matches!(value, 0 | 9 | 10 | 13) {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Creates a packet type from its byte.
    ///
    /// # Panics
    ///
    /// Panics if `value` is 0, 9, 10, or 13. In a `const` context this is a
    /// compile error.
    pub const fn new(value: u8) -> Self {
        assert!(
            !matches!(value, 0 | 9 | 10 | 13),
            "invalid or reserved packet type"
        );
        Self(value)
    }

    /// Returns the type byte.
    pub const fn value(self) -> u8 {
        self.0
    }

    /// Returns the sample packet type of stream `stream_id`, or `None` above 127.
    pub const fn stream(stream_id: u8) -> Option<Self> {
        match 128u8.checked_add(stream_id) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the stream id of a sample packet type, or `None` for any other
    /// type.
    pub const fn stream_id(self) -> Option<u8> {
        if self.0 >= Self::STREAM0.0 {
            Some(self.0 - Self::STREAM0.0)
        } else {
            None
        }
    }

    /// Returns the stream of a sample packet type, for streams 1 to 127. Returns
    /// `None` for stream 0 and for any type that carries no samples.
    pub const fn sample_stream(self) -> Option<StreamId> {
        match self.stream_id() {
            Some(stream_id) => StreamId::try_new(stream_id),
            None => None,
        }
    }
}

/// The lane a device's packet travels, control for its state and data for its time series. Each
/// lane keeps its write order, and lanes keep none between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Lane {
    /// Heartbeats, announcements and the device, stream and column records to every port, replies
    /// to the port that asked.
    Control,
    /// Each stream's rows and segment records.
    Data,
    /// Logs.
    Event,
}

/// The lane `packet` travels from a device or a hub, the same at every hop, or `None` for a
/// request, a time reference, console text, a legacy descriptor or an application packet.
pub fn lane_of(packet: PacketView<'_>) -> Option<Lane> {
    match packet.header.ptype {
        PacketType::SETTING
        | PacketType::RPC_REP
        | PacketType::RPC_ERROR
        | PacketType::HEARTBEAT => Some(Lane::Control),
        PacketType::METADATA => {
            match split_metadata(packet.payload).map(|(kind, ..)| kind.into()) {
                Some(MetadataType::Device | MetadataType::Stream | MetadataType::Column) => {
                    Some(Lane::Control)
                }
                Some(MetadataType::Segment | MetadataType::Unknown(_)) | None => Some(Lane::Data),
            }
        }
        PacketType::LEGACY_STREAM => Some(Lane::Data),
        PacketType::LOG => Some(Lane::Event),
        ptype if ptype.stream_id().is_some() => Some(Lane::Data),
        _ => None,
    }
}

/// Why bytes do not parse as a packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum PacketError {
    /// Not enough bytes yet.
    #[error("more data needed")]
    NeedMore,
    /// Type byte 0, 9, 10, or 13.
    #[error("invalid packet type")]
    InvalidPacketType,
    /// Payload above 500 bytes, or packet above 512.
    #[error("payload too big")]
    PayloadTooBig,
    /// Routing above 8 bytes.
    #[error("routing too big")]
    RoutingTooBig,
    /// The payload is shorter than its type requires.
    #[error("payload too short")]
    PayloadTooShort,
    /// The payload does not parse as its type.
    #[error("invalid payload")]
    InvalidPayload,
    /// Bytes follow the packet where exactly one packet was expected.
    #[error("bytes after the packet")]
    TrailingBytes,
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

    /// Creates a header with no routing and a TTL of 0.
    pub fn new(ptype: PacketType, payload_size: u16) -> Self {
        Self {
            ptype,
            routing_size: 0,
            ttl: 0,
            payload_size,
        }
    }

    /// Parses a 4-byte header. Returns `None` if the header is malformed.
    pub fn parse(buf: &[u8; Header::SIZE]) -> Option<Self> {
        Self::parse_prefix(buf).ok()
    }

    /// Parses the header at the start of a possibly incomplete packet.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::NeedMore`] if the header is incomplete. Returns
    /// another [`PacketError`] if the type, the routing length, or the payload
    /// length is invalid.
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

    /// Writes the four header bytes into `buf`.
    pub fn write(&self, buf: &mut [u8; Header::SIZE]) {
        buf[0] = self.ptype.value();
        buf[1] = (self.ttl << 4) | (self.routing_size & 0x0F);
        buf[2..4].copy_from_slice(&self.payload_size.to_le_bytes());
    }

    /// Returns the length of the payload and routing that follow the header.
    pub fn body_len(&self) -> usize {
        self.payload_size as usize + self.routing_size as usize
    }

    /// Returns the total packet length.
    pub fn packet_len(&self) -> usize {
        Header::SIZE + self.body_len()
    }

    /// Returns the byte range of the payload within the packet.
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
    /// Parses one packet from the start of `raw`. Returns the packet and the
    /// number of bytes it took. Any bytes after it belong to the next packet.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::NeedMore`] if `raw` holds less than a whole
    /// packet. Returns another [`PacketError`] if the header is invalid.
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

    /// Parses `raw` as exactly one packet, its header and length alone. The
    /// payload is not read, for an endpoint that forwards or dispatches
    /// packets without reading every payload.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::NeedMore`] if `raw` holds less than a whole
    /// packet, [`PacketError::TrailingBytes`] if bytes follow it, and another
    /// [`PacketError`] if the header is invalid.
    pub fn parse_exact(raw: &'a [u8]) -> Result<Self, PacketError> {
        let (view, len) = Self::parse_prefix(raw)?;
        if len != raw.len() {
            return Err(PacketError::TrailingBytes);
        }
        Ok(view)
    }

    /// Returns the payload, read as the type its header names.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::PayloadTooShort`] if the payload is shorter
    /// than its type requires, and [`PacketError::InvalidPayload`] if it does
    /// not parse as its type.
    pub fn payload(&self) -> Result<Payload<'a>, PacketError> {
        parse_payload(self.header, self.payload)
    }

    /// Returns the route the packet is addressed to or came from.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::RoutingTooBig`] if the routing bytes hold more
    /// than eight hops.
    pub fn route(&self) -> Result<DeviceRoute, PacketError> {
        DeviceRoute::from_wire(self.routing).map_err(|_| PacketError::RoutingTooBig)
    }
}

/// One whole packet whose route and payload have been read.
///
/// [`exact`](Self::exact) and [`prefix`](Self::prefix) are its only
/// constructors, so a `Checked` always holds the bytes of one valid packet.
#[derive(Debug, Clone, Copy)]
pub struct Checked<'a> {
    bytes: &'a [u8],
    view: PacketView<'a>,
    payload: Payload<'a>,
    route: DeviceRoute,
}

impl<'a> Checked<'a> {
    /// Checks that `raw` is exactly one packet. The header parses, the bytes
    /// hold the whole packet and nothing after it, the route reads, and the
    /// payload reads as its type.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::NeedMore`] if `raw` holds less than a whole
    /// packet, [`PacketError::TrailingBytes`] if bytes follow it, and another
    /// [`PacketError`] if the header, the route or the payload is invalid.
    pub fn exact(raw: &'a [u8]) -> Result<Self, PacketError> {
        Self::read(raw, PacketView::parse_exact(raw)?)
    }

    /// Checks the packet at the start of `raw` as [`exact`](Self::exact)
    /// does. Returns it and the number of bytes it took. Any bytes after it
    /// belong to the next packet.
    ///
    /// # Errors
    ///
    /// Returns [`PacketError::NeedMore`] if `raw` holds less than a whole
    /// packet, and another [`PacketError`] if the header, the route or the
    /// payload is invalid.
    pub fn prefix(raw: &'a [u8]) -> Result<(Self, usize), PacketError> {
        let (view, len) = PacketView::parse_prefix(raw)?;
        Ok((Self::read(&raw[..len], view)?, len))
    }

    /// Reads the route and the payload of `view`, the framing of `bytes`.
    fn read(bytes: &'a [u8], view: PacketView<'a>) -> Result<Self, PacketError> {
        let route = view.route()?;
        let payload = view.payload()?;
        Ok(Self {
            bytes,
            view,
            payload,
            route,
        })
    }

    /// Returns the whole packet as it travels the wire.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Returns the header.
    pub fn header(&self) -> Header {
        self.view.header
    }

    /// Returns the payload, read as the type its header names.
    pub fn payload(&self) -> Payload<'a> {
        self.payload
    }

    /// Returns the payload's bytes.
    pub fn payload_bytes(&self) -> &'a [u8] {
        self.view.payload
    }

    /// Returns the route the packet is addressed to or came from.
    pub fn route(&self) -> DeviceRoute {
        self.route
    }
}

/// Addresses the packet at the start of `buf` to `routing`. The packet's
/// header and payload are its first `len` bytes. Writes the route after them
/// and its length into the header, and returns the packet's new length.
/// Returns `None` if `buf` does not start with a header or has no room for
/// the route.
pub fn write_routing(buf: &mut [u8], len: usize, routing: DeviceRoute) -> Option<usize> {
    let total = len.checked_add(routing.len())?;
    let mut header = Header::parse(buf.get(..Header::SIZE)?.try_into().ok()?)?;
    header.routing_size = routing.len() as u8;
    routing.write_wire(buf.get_mut(len..total)?).ok()?;
    header.write((&mut buf[..Header::SIZE]).try_into().ok()?);
    Some(total)
}

/// The shortest data payload a host accepts, the sample header plus one byte.
const MIN_SAMPLE_PAYLOAD: usize = data::SAMPLE_HEADER_SIZE + 1;

/// Borrowed view of a packet's payload, read as the type its header names.
///
/// Text fields are `&str` only where the payload codec validated them as
/// UTF-8. Everything else stays bytes, and callers convert at the point of
/// use.
#[derive(Debug, Clone, Copy)]
pub enum Payload<'a> {
    /// A device log line.
    Log(LogMessage<'a>),
    /// An RPC request, from a host or a hub.
    RpcRequest(rpc::Request<'a>),
    /// A successful RPC reply.
    RpcReply(rpc::Reply<'a>),
    /// An RPC refused with an error code.
    RpcError(rpc::ErrorReply<'a>),
    /// A keepalive, with the device's session id when it carries one.
    Heartbeat(Heartbeat<'a>),
    /// One metadata record and the flags describing it.
    Metadata(data::Metadata<'a>, data::MetadataFlags),
    /// A run of samples from one stream.
    Samples(data::Samples<'a>),
    /// A device announcing that one of its settings changed.
    Setting(Setting<'a>),
    /// A proxy's link status in the legacy form, type 64, which old proxies
    /// and old recordings still carry.
    LegacyStatus(LinkStatus),
    /// A control request from a client to its proxy, an RPC request naming its
    /// method.
    ProxyTx(rpc::Request<'a>),
    /// A control reply or a link status from a proxy to its client.
    ProxyRx(ProxyRx<'a>),
    /// A packet type this build does not interpret, kept whole for forwarding
    /// and logging.
    Unknown(PacketType, &'a [u8]),
}

impl Payload<'_> {
    /// Returns the link status a payload announces in either form, the legacy
    /// `PROXY_STATUS` or a `PROXY_RX` link. Returns `None` for any other
    /// payload.
    pub fn link_status(&self) -> Option<LinkStatus> {
        match *self {
            Self::LegacyStatus(status) | Self::ProxyRx(ProxyRx::Link(status)) => Some(status),
            _ => None,
        }
    }
}

/// Reads `payload` as the type `header` names.
fn parse_payload(header: Header, payload: &[u8]) -> Result<Payload<'_>, PacketError> {
    let ptype = header.ptype;
    let too_short = PacketError::PayloadTooShort;
    Ok(match ptype {
        PacketType::LOG => Payload::Log(LogMessage::parse(payload).ok_or(too_short)?),
        PacketType::RPC_REQ => Payload::RpcRequest(rpc::Request::parse(payload).ok_or(too_short)?),
        PacketType::RPC_REP => Payload::RpcReply(rpc::Reply::parse(payload).ok_or(too_short)?),
        PacketType::RPC_ERROR => {
            Payload::RpcError(rpc::ErrorReply::parse(payload).ok_or(too_short)?)
        }
        PacketType::HEARTBEAT => {
            Payload::Heartbeat(Heartbeat::parse(payload).ok_or(PacketError::InvalidPayload)?)
        }
        PacketType::METADATA => parse_metadata(ptype, payload)?,
        PacketType::SETTING => Payload::Setting(Setting::parse(payload).ok_or(too_short)?),
        PacketType::PROXY_STATUS => {
            Payload::LegacyStatus(LinkStatus::parse_status(payload).ok_or(too_short)?)
        }
        PacketType::PROXY_TX => Payload::ProxyTx(rpc::Request::parse(payload).ok_or(too_short)?),
        PacketType::PROXY_RX => Payload::ProxyRx(ProxyRx::parse(payload).ok_or(too_short)?),
        _ => match ptype.sample_stream() {
            Some(_) => {
                if payload.len() < MIN_SAMPLE_PAYLOAD {
                    return Err(too_short);
                }
                Payload::Samples(
                    data::Samples::parse(header, payload).ok_or(PacketError::InvalidPayload)?,
                )
            }
            // Legacy metadata and stream types, and anything this build
            // predates, ride along whole so a proxy can forward them.
            _ => Payload::Unknown(ptype, payload),
        },
    })
}

/// Reads a metadata payload. A record kind this build does not know has its
/// framing checked and is kept whole as [`Payload::Unknown`].
fn parse_metadata(ptype: PacketType, payload: &[u8]) -> Result<Payload<'_>, PacketError> {
    let (kind, _, record) = split_metadata(payload).ok_or(PacketError::PayloadTooShort)?;
    match data::Metadata::parse(payload) {
        Some((record, flags)) => Ok(Payload::Metadata(record, flags)),
        None if data::Metadata::defines(kind) => Err(PacketError::InvalidPayload),
        None => {
            data::split_record(record).ok_or(PacketError::InvalidPayload)?;
            Ok(Payload::Unknown(ptype, payload))
        }
    }
}

/// Complete wire packet with space for route tagging.
#[derive(Clone)]
pub struct Packet {
    len: u16,
    buf: [u8; Packet::MAX_SIZE],
}

impl core::fmt::Debug for Packet {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Packet")
            .field("bytes", &self.as_slice())
            .finish_non_exhaustive()
    }
}

impl Packet {
    /// Largest packet, 512 bytes.
    pub const MAX_SIZE: usize = 512;
    /// Largest payload, 500 bytes.
    pub const MAX_PAYLOAD: usize = Self::MAX_SIZE - Header::SIZE - DeviceRoute::MAX_HOPS;

    /// Copies a packet from `data`. Returns `None` unless `data` is exactly one
    /// complete, valid packet.
    pub fn from_slice(data: &[u8]) -> Option<Self> {
        PacketView::parse_exact(data).ok()?;
        let mut packet = Self {
            len: data.len() as u16,
            buf: [0; Packet::MAX_SIZE],
        };
        packet.buf[..data.len()].copy_from_slice(data);
        Some(packet)
    }

    /// Returns the packet bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    /// Returns the payload bytes for writing, without the header or routing.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        // Every constructor parses, and pop/push_hop only edit routing, so a
        // `Packet` always has a readable header.
        let range = match Header::parse_prefix(self.as_slice()) {
            Ok(header) => header.payload_range(),
            Err(_) => 0..0,
        };
        &mut self.buf[range]
    }

    /// Removes and returns the next hop, the port a router sends the packet to.
    ///
    /// # Errors
    ///
    /// Returns [`ForwardError::NotRouted`] if the packet is addressed to this
    /// device.
    pub fn pop_hop(&mut self) -> Result<u8, ForwardError> {
        let (hop, len) = crate::route::pop_hop(&mut self.buf[..self.len as usize])?;
        self.len = len as u16;
        Ok(hop)
    }

    /// Appends `hop`, the port the packet arrived on, to its routing.
    ///
    /// # Errors
    ///
    /// Returns [`ForwardError::RoutingFull`] if the packet already has eight
    /// hops.
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

    /// A router edits the payload and nothing else. The header and the hops
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

    /// The payload of a view of the whole packet `raw`.
    fn payload_of(raw: &[u8]) -> Result<Payload<'_>, PacketError> {
        let (view, used) = PacketView::parse_prefix(raw).unwrap();
        assert_eq!(used, raw.len());
        view.payload()
    }

    /// The packet a writer puts at the start of a fresh buffer.
    fn written(write: impl FnOnce(&mut [u8]) -> Option<usize>) -> ([u8; Packet::MAX_SIZE], usize) {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = write(&mut buf).unwrap();
        (buf, len)
    }

    #[test]
    fn each_packet_type_reads_as_its_payload() {
        fn read(raw: &[u8]) -> Payload<'_> {
            payload_of(raw).unwrap()
        }
        assert!(matches!(
            read(&[1, 0, 6, 0, 1, 0, 0, 0, 3, b'h']),
            Payload::Log(log) if log.message == b"h"
        ));
        assert!(matches!(
            read(&[2, 2, 7, 0, 0x34, 0x12, 1, 0x80, b'x', 1, 2, 2, 1]),
            Payload::RpcRequest(request) if request.args == [1, 2]
        ));
        assert!(matches!(
            read(&[3, 0, 3, 0, 1, 0, 9]),
            Payload::RpcReply(reply) if reply.value == [9]
        ));
        assert!(matches!(
            read(&[4, 0, 4, 0, 1, 0, 2, 0]),
            Payload::RpcError(error) if error.code == 2
        ));
        assert!(matches!(
            read(&[5, 0, 4, 0, 7, 0, 0, 0]),
            Payload::Heartbeat(Heartbeat::Session { session, .. }) if session == crate::SessionId::new(7)
        ));
        let device = data::Metadata::Device(data::Device {
            session: crate::SessionId::new(1),
            n_streams: 1,
            name: "sensor",
            serial: "S1",
            firmware: "1",
            protocol: 0,
        });
        let (buf, len) = written(|buf| device.write(data::MetadataFlags::UPDATE, buf));
        assert!(matches!(
            read(&buf[..len]),
            Payload::Metadata(data::Metadata::Device(record), _) if record.name == "sensor"
        ));
        let unknown_record = [11, 0, 6, 0, 5, 0, 3, 0xaa, 0xbb, 0xcc];
        assert!(matches!(
            read(&unknown_record),
            Payload::Unknown(PacketType::METADATA, body) if body == &unknown_record[4..]
        ));
        let samples = data::Samples {
            stream_id: crate::StreamId::new(1),
            segment_id: crate::SegmentId::new(0),
            first: crate::SampleNumber::new(5),
            data: &[1, 2, 3, 4],
        };
        let (buf, len) = written(|buf| samples.write(buf));
        assert!(matches!(
            read(&buf[..len]),
            Payload::Samples(read) if read.data == [1, 2, 3, 4]
        ));
        let setting = Setting {
            name: b"x",
            seq: None,
            reply: &[1],
        };
        let (buf, len) = written(|buf| setting.write(buf));
        assert!(matches!(
            read(&buf[..len]),
            Payload::Setting(read) if read.name == b"x"
        ));
        assert!(matches!(
            read(&[64, 0, 1, 0, 2]),
            Payload::LegacyStatus(LinkStatus::FailedToReconnect)
        ));
        assert!(matches!(
            read(&[66, 0, 5, 0, 1, 0, 1, 0x80, b'x']),
            Payload::ProxyTx(request) if request.method == rpc::Method::ByName(b"x")
        ));
        assert!(matches!(
            read(&[67, 0, 2, 0, 2, 4]),
            Payload::ProxyRx(ProxyRx::Link(LinkStatus::Stopped))
        ));
        assert!(matches!(
            read(&[63, 0, 1, 0, b'a']),
            Payload::Unknown(PacketType::TEXT, b"a")
        ));
    }

    #[test]
    fn a_short_payload_is_refused() {
        for raw in [
            &[1, 0, 4, 0, 0, 0, 0, 0][..],   // a log with no level
            &[3, 0, 1, 0, 1][..],            // a reply with half a request id
            &[11, 0, 1, 0, 1][..],           // a metadata record with no flags
            &[129, 0, 4, 0, 0, 0, 0, 0][..], // samples with no data
            &[67, 0, 0, 0][..],              // a proxy packet with no kind
        ] {
            assert_eq!(
                payload_of(raw).unwrap_err(),
                PacketError::PayloadTooShort,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn a_payload_that_breaks_its_type_is_invalid() {
        // A device record whose name length runs past the record.
        assert_eq!(
            payload_of(&[11, 0, 11, 0, 1, 0, 9, 9, 0, 0, 0, 0, 0, 0, 0]).unwrap_err(),
            PacketError::InvalidPayload
        );
    }

    #[test]
    fn write_routing_addresses_a_packet_and_keeps_its_ttl() {
        let mut buf = [0u8; 16];
        buf[..6].copy_from_slice(&[PacketType::RPC_REP.value(), 0x30, 2, 0, 1, 0]);
        let route = DeviceRoute::from_hops(&[1, 2]).unwrap();

        let len = write_routing(&mut buf, 6, route).unwrap();
        assert_eq!(buf[..len], [3, 0x32, 2, 0, 1, 0, 2, 1]);
        let (view, _) = PacketView::parse_prefix(&buf[..len]).unwrap();
        assert_eq!(view.route(), Ok(route));
        assert_eq!(view.header.ttl, 3);

        assert_eq!(write_routing(&mut buf[..7], 6, route), None);
        assert_eq!(write_routing(&mut [0u8; 8], 4, route), None);
    }

    #[test]
    fn a_view_reads_its_route() {
        let (view, _) = PacketView::parse_prefix(&[5, 2, 0, 0, 2, 1]).unwrap();
        assert_eq!(view.route(), Ok(DeviceRoute::from_hops(&[1, 2]).unwrap()));
        let view = PacketView {
            routing: &[0; 9],
            ..view
        };
        assert_eq!(view.route(), Err(PacketError::RoutingTooBig));
    }

    /// A reply to request 1 with value `[9]`, routed through hops 1 and 2.
    const REPLY: [u8; 9] = [3, 2, 3, 0, 1, 0, 9, 2, 1];

    #[test]
    fn a_checked_packet_reads_one_whole_packet() {
        let checked = Checked::exact(&REPLY).unwrap();
        assert_eq!(checked.as_bytes(), REPLY);
        assert_eq!(checked.header().ptype, PacketType::RPC_REP);
        assert_eq!(checked.payload_bytes(), [1, 0, 9]);
        assert_eq!(
            PacketView::parse_exact(checked.as_bytes()).unwrap().routing,
            [2, 1]
        );
        assert_eq!(checked.route(), DeviceRoute::from_hops(&[1, 2]).unwrap());
        assert!(matches!(checked.payload(), Payload::RpcReply(reply) if reply.value == [9]));

        let mut raw = REPLY.to_vec();
        raw.extend_from_slice(&[5, 0, 0, 0]);
        let (checked, used) = Checked::prefix(&raw).unwrap();
        assert_eq!(used, REPLY.len());
        assert_eq!(checked.as_bytes(), REPLY);
    }

    #[test]
    fn a_checked_packet_refuses_bytes_after_it_only_when_exact() {
        let mut raw = REPLY.to_vec();
        raw.push(0);
        assert_eq!(
            Checked::exact(&raw).unwrap_err(),
            PacketError::TrailingBytes
        );
        assert_eq!(
            PacketView::parse_exact(&raw).unwrap_err(),
            PacketError::TrailingBytes
        );
        assert_eq!(Checked::prefix(&raw).unwrap().1, REPLY.len());
    }

    #[test]
    fn a_checked_packet_refuses_a_short_buffer() {
        let short = &REPLY[..REPLY.len() - 1];
        assert_eq!(Checked::exact(short).unwrap_err(), PacketError::NeedMore);
        assert_eq!(Checked::prefix(short).unwrap_err(), PacketError::NeedMore);
        assert_eq!(Checked::exact(&[]).unwrap_err(), PacketError::NeedMore);
    }

    #[test]
    fn a_checked_packet_refuses_a_payload_that_breaks_its_type() {
        // A reply with half a request id, framed correctly.
        let raw = [3, 0, 1, 0, 1];
        assert!(PacketView::parse_exact(&raw).is_ok());
        assert_eq!(
            Checked::exact(&raw).unwrap_err(),
            PacketError::PayloadTooShort
        );
        assert_eq!(
            Checked::prefix(&raw).unwrap_err(),
            PacketError::PayloadTooShort
        );
        // A device record whose name length runs past the record.
        let raw = [11, 0, 11, 0, 1, 0, 9, 9, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            Checked::exact(&raw).unwrap_err(),
            PacketError::InvalidPayload
        );
    }

    /// Every whole-packet writer writes bytes the check accepts as one packet.
    #[test]
    fn every_whole_packet_writer_passes_the_check() {
        fn check(write: impl FnOnce(&mut [u8]) -> Option<usize>) -> PacketType {
            let (buf, len) = written(write);
            match Checked::exact(&buf[..len]) {
                Ok(checked) => checked.header().ptype,
                Err(error) => panic!("{error:?} for {:?}", &buf[..len]),
            }
        }
        let id = crate::RpcRequestId::new(1);
        let log = LogMessage {
            level: crate::log::LogLevel::INFO,
            data: 7,
            message: b"h",
        };
        assert_eq!(check(|buf| log.write(buf)), PacketType::LOG);
        let method = rpc::Method::ByName(b"x");
        assert_eq!(
            check(|buf| rpc::write_request(buf, id, method, &[1, 2])),
            PacketType::RPC_REQ
        );
        assert_eq!(
            check(|buf| rpc::write_reply(buf, id, &[9])),
            PacketType::RPC_REP
        );
        assert_eq!(
            check(|buf| rpc::write_error(buf, id, rpc::RpcError::NotFound)),
            PacketType::RPC_ERROR
        );
        let beat = Heartbeat::Session {
            session: crate::SessionId::new(7),
            settings: None,
        };
        assert_eq!(check(|buf| beat.write(buf)), PacketType::HEARTBEAT);
        let device = data::Metadata::Device(data::Device {
            session: crate::SessionId::new(1),
            n_streams: 1,
            name: "sensor",
            serial: "S1",
            firmware: "1",
            protocol: 0,
        });
        assert_eq!(
            check(|buf| device.write(data::MetadataFlags::UPDATE, buf)),
            PacketType::METADATA
        );
        let samples = data::Samples {
            stream_id: crate::StreamId::new(1),
            segment_id: crate::SegmentId::new(0),
            first: crate::SampleNumber::new(5),
            data: &[1, 2, 3, 4],
        };
        assert_eq!(
            check(|buf| samples.write(buf)),
            PacketType::stream(1).unwrap()
        );
        let setting = Setting {
            name: b"x",
            seq: None,
            reply: &[1],
        };
        assert_eq!(check(|buf| setting.write(buf)), PacketType::SETTING);
        assert_eq!(
            check(|buf| LinkStatus::Stopped.write(buf)),
            PacketType::PROXY_STATUS
        );
        assert_eq!(
            check(|buf| rpc::write_proxy_request(buf, id, method, b"auto")),
            PacketType::PROXY_TX
        );
        for rx in [
            ProxyRx::Reply(rpc::Reply {
                req_id: id,
                value: b"ok",
            }),
            ProxyRx::Error(rpc::ErrorReply {
                req_id: id,
                code: 2,
                message: b"no",
            }),
            ProxyRx::Link(LinkStatus::Stopped),
            ProxyRx::Alive,
            ProxyRx::Dropped {
                stream_id: 3,
                packets: 9,
            },
            ProxyRx::Unknown(9, &[0xaa]),
        ] {
            assert_eq!(check(|buf| rx.write(buf)), PacketType::PROXY_RX);
        }
    }

    /// The sample codec writes a packet with no sample bytes, which the check
    /// refuses as too short.
    #[test]
    fn a_sample_packet_with_no_samples_is_written_but_refused() {
        let samples = data::Samples {
            stream_id: crate::StreamId::new(1),
            segment_id: crate::SegmentId::new(0),
            first: crate::SampleNumber::new(5),
            data: &[],
        };
        let (buf, len) = written(|buf| samples.write(buf));
        assert_eq!(
            Checked::exact(&buf[..len]).unwrap_err(),
            PacketError::PayloadTooShort
        );
    }

    #[test]
    fn each_whole_proxy_packet_matches_the_wire_layout() {
        let (buf, len) = written(|buf| LinkStatus::Stopped.write(buf));
        assert_eq!(buf[..len], [64, 0, 1, 0, 2]);
        let (buf, len) = written(|buf| ProxyRx::Link(LinkStatus::Stopped).write(buf));
        assert_eq!(buf[..len], [67, 0, 2, 0, 2, 4]);
        let id = crate::RpcRequestId::new(1);
        let (buf, len) =
            written(|buf| rpc::write_proxy_request(buf, id, rpc::Method::ByName(b"x"), &[5]));
        assert_eq!(buf[..len], [66, 0, 6, 0, 1, 0, 1, 0x80, b'x', 5]);
        assert_eq!(LinkStatus::Stopped.write(&mut [0; 4]), None);
        assert_eq!(ProxyRx::Alive.write(&mut [0; 4]), None);
        let big = [0; Packet::MAX_PAYLOAD];
        assert_eq!(
            ProxyRx::Unknown(9, &big).write(&mut [0; 2 * Packet::MAX_SIZE]),
            None
        );
    }

    #[test]
    fn a_payload_names_its_link_status_in_either_form() {
        let status = |raw: &[u8]| Checked::exact(raw).unwrap().payload().link_status();
        assert_eq!(
            status(&[64, 0, 1, 0, 1]),
            Some(LinkStatus::SensorReconnected)
        );
        assert_eq!(status(&[67, 0, 2, 0, 2, 4]), Some(LinkStatus::Stopped));
        assert_eq!(status(&[67, 0, 1, 0, 3]), None);
        assert_eq!(status(&REPLY), None);
    }

    #[test]
    fn only_streams_1_to_127_carry_samples() {
        assert_eq!(PacketType::STREAM0.sample_stream(), None);
        assert_eq!(
            PacketType::stream(1).unwrap().sample_stream(),
            Some(crate::StreamId::new(1))
        );
        assert_eq!(
            PacketType::stream(127).unwrap().sample_stream(),
            Some(crate::StreamId::new(127))
        );
        assert_eq!(PacketType::HEARTBEAT.sample_stream(), None);
    }

    /// Device, stream and column records share control with announcements, replies and
    /// heartbeats, a segment record shares data with rows, and a log travels alone.
    #[test]
    fn each_packet_type_has_one_lane() {
        let lane = |ptype: PacketType, payload: &[u8]| {
            let raw = [&[ptype.value(), 0, payload.len() as u8, 0][..], payload].concat();
            lane_of(PacketView::parse_prefix(&raw).unwrap().0)
        };
        let control = [
            PacketType::SETTING,
            PacketType::RPC_REP,
            PacketType::RPC_ERROR,
        ];
        assert!(control
            .into_iter()
            .all(|ptype| lane(ptype, &[]) == Some(Lane::Control)));
        assert_eq!(lane(PacketType::HEARTBEAT, &[0; 8]), Some(Lane::Control));
        assert!([1, 2, 4]
            .into_iter()
            .all(|kind| lane(PacketType::METADATA, &[kind, 0]) == Some(Lane::Control)));
        assert!([3, 9]
            .into_iter()
            .all(|kind| lane(PacketType::METADATA, &[kind, 0]) == Some(Lane::Data)));
        assert_eq!(lane(PacketType::stream(1).unwrap(), &[]), Some(Lane::Data));
        assert_eq!(lane(PacketType::LEGACY_STREAM, &[]), Some(Lane::Data));
        assert_eq!(lane(PacketType::LOG, &[]), Some(Lane::Event));
        assert!([
            PacketType::RPC_REQ,
            PacketType::SYNC,
            PacketType::TEXT,
            PacketType::USER
        ]
        .into_iter()
        .all(|ptype| lane(ptype, &[]).is_none()));
    }
}

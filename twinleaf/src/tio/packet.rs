//! The host packet: one validated wire buffer, read through the
//! [`proto`](crate::proto) codecs.

use crate::proto::data as wire;
use crate::proto::heartbeat::Heartbeat;
use crate::proto::log::LogMessage;
use crate::proto::packet::Packet as WirePacket;
use crate::proto::packet::{Header, PacketError, PacketView};
use crate::proto::rpc as wire_rpc;
use crate::proto::settings::Setting;
use crate::proto::SessionId as WireSessionId;
use crate::proto::{RpcMethodId, RpcRequestId};
use bytes::Bytes;
use num_enum::{FromPrimitive, IntoPrimitive};
use std::fmt;

use crate::proto::data::MAX_SAMPLE_NUMBER;
use crate::proto::packet::PacketType;
use crate::proto::DeviceRoute;

/// Shortest data payload a host accepts: the sample header plus one byte.
const MIN_SAMPLE_PAYLOAD: usize = wire::SAMPLE_HEADER_SIZE + 1;

/// One complete, validated TIO packet.
///
/// The bytes are the packet: every accessor re-reads them through the wire
/// codecs rather than holding a decoded copy.
#[derive(Clone)]
pub struct Packet {
    raw: Bytes,
}

/// Borrowed view of a packet's payload.
///
/// Text fields are `&str` only where the wire codec validated them as UTF-8.
/// Everything else stays bytes, and callers convert at the point of use.
#[derive(Debug, Clone, Copy)]
pub enum Payload<'a> {
    /// A device log line.
    Log(LogMessage<'a>),
    /// An RPC request, from a host or a hub.
    RpcRequest(wire_rpc::Request<'a>),
    /// A successful RPC reply.
    RpcReply(wire_rpc::Reply<'a>),
    /// An RPC refused with an error code.
    RpcError(wire_rpc::ErrorReply<'a>),
    /// A keepalive, with the device's session id when it carries one.
    Heartbeat(Heartbeat<'a>),
    /// One metadata record and the flags describing it.
    Metadata(wire::Metadata<'a>, wire::MetadataFlags),
    /// A run of samples from one stream.
    Samples(wire::Samples<'a>),
    /// A device announcing that one of its settings changed.
    Setting(Setting<'a>),
    /// The proxy's own link status.
    ProxyStatus(ProxyStatus),
    /// A packet type this build does not interpret, kept whole so it can be
    /// forwarded and logged.
    Unknown(PacketType, &'a [u8]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
/// The link status a proxy announces to its clients.
pub enum ProxyStatus {
    /// The device went quiet or its transport closed.
    SensorDisconnected = 0,
    /// The device is answering again after a disconnect.
    SensorReconnected = 1,
    /// Reconnecting was given up on.
    FailedToReconnect = 2,
    /// The transport never opened.
    FailedToConnect = 3,
    /// A status code this build does not know.
    #[num_enum(catch_all)]
    Unknown(u8),
}

/// Why a buffer is not one packet. Transports count and log these, and none
/// of them is matched on beyond [`Self::NeedMore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The buffer ends before the packet does.
    #[error("more data needed")]
    NeedMore,
    /// The serial frame's checksum did not match.
    #[error("CRC32 mismatch")]
    CRC32,
    /// Longer than any packet may be.
    #[error("packet too big")]
    PacketTooBig,
    /// Shorter than a header.
    #[error("packet too small")]
    PacketTooSmall,
    /// The header names a packet type that does not exist.
    #[error("invalid packet type")]
    InvalidPacketType,
    /// The header declares a payload longer than the maximum.
    #[error("payload too big")]
    PayloadTooBig,
    /// The header declares more routing bytes than the maximum.
    #[error("routing too big")]
    RoutingTooBig,
    /// The payload is shorter than its type requires.
    #[error("payload too short")]
    PayloadTooShort,
    /// The payload does not parse as its type.
    #[error("invalid payload")]
    InvalidPayload,
}

impl From<PacketError> for DecodeError {
    fn from(error: PacketError) -> Self {
        match error {
            PacketError::NeedMore => Self::NeedMore,
            PacketError::InvalidPacketType => Self::InvalidPacketType,
            PacketError::PayloadTooBig => Self::PayloadTooBig,
            PacketError::RoutingTooBig => Self::RoutingTooBig,
        }
    }
}

/// Why a value could not be written as a packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    /// The payload would exceed the maximum.
    #[error("encoded payload is {actual} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Bytes the payload needs.
        actual: usize,
        /// Bytes a payload may have.
        maximum: usize,
    },
    /// A field does not fit its wire width.
    #[error("value {value} exceeds the encoding maximum of {maximum}")]
    ValueTooLarge {
        /// The value offered.
        value: usize,
        /// The largest the field holds.
        maximum: usize,
    },
    /// Stream ids run from 1 to 127.
    #[error("stream ID {0} is outside the encodable range 1..=127")]
    InvalidStreamId(u8),
    /// Sample numbers are 24 bits on the wire.
    #[error("sample number {0} does not fit in the 24-bit packet field")]
    SampleNumberTooLarge(u32),
}

/// An RPC method named the way the request that reached it named it. Owned, so
/// the proxy can hold it while a request is outstanding.
#[derive(Debug, Clone)]
pub enum RpcMethod {
    /// By numeric id.
    Id(u16),
    /// By name.
    Name(String),
}

impl RpcMethod {
    /// The borrowed form a request is written from.
    pub fn to_wire(&self) -> Result<wire_rpc::Method<'_>, EncodeError> {
        Ok(match self {
            Self::Id(id) => wire_rpc::Method::ById(RpcMethodId::try_new(*id).ok_or(
                EncodeError::ValueTooLarge {
                    value: usize::from(*id),
                    maximum: usize::from(RpcMethodId::MAX),
                },
            )?),
            Self::Name(name) => wire_rpc::Method::ByName(name.as_bytes()),
        })
    }
}

fn parse_payload<'a>(header: Header, payload: &'a [u8]) -> Result<Payload<'a>, DecodeError> {
    let ptype = header.ptype;
    let too_short = DecodeError::PayloadTooShort;
    Ok(match ptype {
        PacketType::LOG => Payload::Log(LogMessage::parse(payload).ok_or(too_short)?),
        PacketType::RPC_REQ => {
            Payload::RpcRequest(wire_rpc::Request::parse(payload).ok_or(too_short)?)
        }
        PacketType::RPC_REP => Payload::RpcReply(wire_rpc::Reply::parse(payload).ok_or(too_short)?),
        PacketType::RPC_ERROR => {
            Payload::RpcError(wire_rpc::ErrorReply::parse(payload).ok_or(too_short)?)
        }
        PacketType::HEARTBEAT => {
            Payload::Heartbeat(Heartbeat::parse(payload).ok_or(DecodeError::InvalidPayload)?)
        }
        PacketType::METADATA => parse_metadata(ptype, payload)?,
        PacketType::SETTING => Payload::Setting(Setting::parse(payload).ok_or(too_short)?),
        PacketType::PROXY_STATUS => {
            Payload::ProxyStatus(ProxyStatus::from(*payload.first().ok_or(too_short)?))
        }
        _ => match ptype.stream_id() {
            Some(stream_id) if stream_id >= wire::FIRST_STREAM_ID => {
                if payload.len() < MIN_SAMPLE_PAYLOAD {
                    return Err(too_short);
                }
                Payload::Samples(
                    wire::Samples::parse(header, payload).ok_or(DecodeError::InvalidPayload)?,
                )
            }
            // Legacy metadata and stream types, and anything this build
            // predates, ride along whole so a proxy can forward them.
            _ => Payload::Unknown(ptype, payload),
        },
    })
}

/// A metadata record this build knows, or its framing checked and the payload
/// kept whole for forwarding.
fn parse_metadata(ptype: PacketType, payload: &[u8]) -> Result<Payload<'_>, DecodeError> {
    let (kind, _, record) = wire::split_metadata(payload).ok_or(DecodeError::PayloadTooShort)?;
    match wire::Metadata::parse(payload) {
        Some((record, flags)) => Ok(Payload::Metadata(record, flags)),
        None if wire::Metadata::defines(kind) => Err(DecodeError::InvalidPayload),
        None => {
            wire::split_record(record).ok_or(DecodeError::InvalidPayload)?;
            Ok(Payload::Unknown(ptype, payload))
        }
    }
}

impl Packet {
    /// Validate and take ownership of exactly one packet's bytes. Every other
    /// constructor funnels through here.
    pub fn from_wire(raw: Bytes) -> Result<Packet, DecodeError> {
        let (view, len) = PacketView::parse_prefix(&raw)?;
        if len != raw.len() {
            return Err(DecodeError::PacketTooBig);
        }
        DeviceRoute::from_wire(view.routing).map_err(|_| DecodeError::RoutingTooBig)?;
        parse_payload(view.header, view.payload)?;
        Ok(Packet { raw })
    }

    /// Parse one packet from the start of `raw`, sharing its storage, and
    /// report how many bytes it consumed.
    pub fn from_wire_prefix(raw: &Bytes) -> Result<(Packet, usize), DecodeError> {
        let len = PacketView::parse_prefix(raw)?.1;
        Ok((Self::from_wire(raw.slice(..len))?, len))
    }

    /// As [`from_wire_prefix`](Self::from_wire_prefix), copying the packet out
    /// of a transport's byte stream.
    pub fn from_slice_prefix(raw: &[u8]) -> Result<(Packet, usize), DecodeError> {
        let len = PacketView::parse_prefix(raw)?.1;
        Ok((Self::from_wire(Bytes::copy_from_slice(&raw[..len]))?, len))
    }

    /// A request for `name` with encoded `arg`, tagged `id` for its reply.
    pub fn rpc_request(
        name: &str,
        arg: &[u8],
        id: u16,
        routing: DeviceRoute,
    ) -> Result<Packet, EncodeError> {
        let method = wire_rpc::Method::ByName(name.as_bytes());
        let payload_len =
            wire_rpc::request_payload_len(method, arg).ok_or(EncodeError::ValueTooLarge {
                value: name.len(),
                maximum: wire_rpc::NAMELEN_MASK as usize,
            })?;
        if payload_len > WirePacket::MAX_PAYLOAD {
            return Err(EncodeError::PayloadTooLarge {
                actual: payload_len,
                maximum: WirePacket::MAX_PAYLOAD,
            });
        }
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = wire_rpc::write_request(&mut buf, RpcRequestId::new(id), method, arg)
            .expect("payload sized above");
        Ok(finish(&mut buf, len, routing))
    }

    /// The reply to request `id`, carrying `value`.
    pub fn rpc_reply(id: u16, value: &[u8], routing: DeviceRoute) -> Result<Packet, EncodeError> {
        let payload_len = 2 + value.len();
        if payload_len > WirePacket::MAX_PAYLOAD {
            return Err(EncodeError::PayloadTooLarge {
                actual: payload_len,
                maximum: WirePacket::MAX_PAYLOAD,
            });
        }
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = wire_rpc::write_reply(&mut buf, RpcRequestId::new(id), value)
            .expect("payload sized above");
        Ok(finish(&mut buf, len, routing))
    }

    /// Refuse a request with a bare error code and no message.
    pub fn rpc_error(id: u16, code: wire_rpc::RpcError, routing: DeviceRoute) -> Packet {
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = Header::SIZE + 4;
        Header::new(PacketType::RPC_ERROR, 4).write((&mut buf[..Header::SIZE]).try_into().unwrap());
        wire_rpc::write_error(&mut buf, RpcRequestId::new(id), code)
            .expect("a bare error payload always fits");
        finish(&mut buf, len, routing)
    }

    /// The keepalive a host sends to hold a link open.
    pub fn heartbeat(routing: DeviceRoute) -> Packet {
        Self::write_heartbeat(Heartbeat::Any(&[]), routing)
    }

    /// A heartbeat announcing `session`, as a device sends it.
    pub fn heartbeat_session(session: u32, routing: DeviceRoute) -> Packet {
        Self::write_heartbeat(Heartbeat::Session(WireSessionId::new(session)), routing)
    }

    fn write_heartbeat(beat: Heartbeat<'_>, routing: DeviceRoute) -> Packet {
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = beat.write(&mut buf).expect("a heartbeat always fits");
        finish(&mut buf, len, routing)
    }

    /// The proxy's own connection-status announcement, which has no route.
    pub fn proxy_status(status: ProxyStatus) -> Packet {
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        Header::new(PacketType::PROXY_STATUS, 1)
            .write((&mut buf[..Header::SIZE]).try_into().unwrap());
        buf[Header::SIZE] = u8::from(status);
        finish(&mut buf, Header::SIZE + 1, DeviceRoute::root())
    }

    /// One metadata record with its flags.
    pub fn metadata(
        record: wire::Metadata<'_>,
        flags: wire::MetadataFlags,
        routing: DeviceRoute,
    ) -> Result<Packet, EncodeError> {
        let payload_len = wire::METADATA_HEADER_SIZE + record.record_len();
        if payload_len > WirePacket::MAX_PAYLOAD {
            return Err(EncodeError::PayloadTooLarge {
                actual: payload_len,
                maximum: WirePacket::MAX_PAYLOAD,
            });
        }
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = record.write(flags, &mut buf).expect("payload sized above");
        Ok(finish(&mut buf, len, routing))
    }

    /// As [`Self::metadata`], carrying a record byte for byte as it was
    /// received rather than re-encoding it from parsed fields. `kind` is the
    /// record's wire type byte.
    pub fn metadata_record(
        kind: u8,
        flags: wire::MetadataFlags,
        record: &[u8],
        routing: DeviceRoute,
    ) -> Result<Packet, EncodeError> {
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = wire::write_metadata_record(&mut buf, kind, flags, record).ok_or(
            EncodeError::PayloadTooLarge {
                actual: wire::METADATA_HEADER_SIZE + record.len(),
                maximum: WirePacket::MAX_PAYLOAD,
            },
        )?;
        Ok(finish(&mut buf, len, routing))
    }

    /// Packed sample `data` for one stream, starting at `first_sample_n`.
    pub fn samples(
        stream_id: u8,
        segment_id: u8,
        first_sample_n: u32,
        data: &[u8],
        routing: DeviceRoute,
    ) -> Result<Packet, EncodeError> {
        if first_sample_n > MAX_SAMPLE_NUMBER {
            return Err(EncodeError::SampleNumberTooLarge(first_sample_n));
        }
        let payload_len = wire::SAMPLE_HEADER_SIZE + data.len();
        if payload_len > WirePacket::MAX_PAYLOAD {
            return Err(EncodeError::PayloadTooLarge {
                actual: payload_len,
                maximum: WirePacket::MAX_PAYLOAD,
            });
        }
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let stream_id = crate::proto::StreamId::try_new(stream_id)
            .ok_or(EncodeError::InvalidStreamId(stream_id))?;
        let len = wire::Samples {
            stream_id,
            segment_id: crate::proto::SegmentId::new(segment_id),
            first: crate::proto::SampleNumber::new(first_sample_n),
            data,
        }
        .write(&mut buf)
        .ok_or(EncodeError::InvalidStreamId(stream_id.value()))?;
        Ok(finish(&mut buf, len, routing))
    }

    /// The payload, re-parsed from the packet's bytes.
    pub fn payload(&self) -> Payload<'_> {
        let view = self.view();
        parse_payload(view.header, view.payload).expect("payload validated in from_wire")
    }

    /// The payload's bytes, for a caller retaining part of it verbatim rather
    /// than reading it through [`Self::payload`].
    pub fn payload_bytes(&self) -> &[u8] {
        self.view().payload
    }

    /// The packet type from the header.
    pub fn ptype(&self) -> PacketType {
        self.view().header.ptype
    }

    /// The route the packet is addressed to or came from.
    pub fn route(&self) -> DeviceRoute {
        DeviceRoute::from_wire(self.view().routing).expect("routing validated in from_wire")
    }

    /// Hops left before a hub drops the packet.
    pub fn ttl(&self) -> u8 {
        self.view().header.ttl
    }

    /// True for a packet carrying stream samples, legacy encodings included.
    pub fn is_data(&self) -> bool {
        self.ptype().stream_id().is_some()
    }

    /// The whole packet as it travels the wire.
    pub fn as_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// The packet's bytes, without copying.
    pub fn into_bytes(self) -> Bytes {
        self.raw
    }

    /// Re-address this packet, keeping its payload and TTL.
    pub fn with_route(&self, route: DeviceRoute) -> Packet {
        if route == self.route() {
            return self.clone();
        }
        self.rebuilt(route, self.ttl())
    }

    /// Re-stamp this packet's TTL, keeping its payload and route.
    pub fn with_ttl(&self, ttl: u8) -> Result<Packet, EncodeError> {
        if ttl > Header::MAX_TTL {
            return Err(EncodeError::ValueTooLarge {
                value: usize::from(ttl),
                maximum: usize::from(Header::MAX_TTL),
            });
        }
        Ok(self.rebuilt(self.route(), ttl))
    }

    /// Renumber the request an RPC packet belongs to. Requests, replies, and
    /// errors all lead with that id, so a proxy remapping the ids it forwards
    /// edits both directions here. Any other packet is returned unchanged.
    pub fn with_rpc_id(&self, id: u16) -> Packet {
        let view = self.view();
        if !matches!(
            view.header.ptype,
            PacketType::RPC_REQ | PacketType::RPC_REP | PacketType::RPC_ERROR
        ) {
            return self.clone();
        }
        let mut raw = self.raw.to_vec();
        let payload = &mut raw[view.header.payload_range()];
        if !wire_rpc::set_req_id(payload, RpcRequestId::new(id)) {
            return self.clone();
        }
        Packet { raw: raw.into() }
    }

    fn view(&self) -> PacketView<'_> {
        PacketView::parse_prefix(&self.raw)
            .expect("bytes validated in from_wire")
            .0
    }

    /// Copy the packet with a new header. Routing and TTL are the only header
    /// fields a host edits, and neither can push a valid packet over its size
    /// ceiling.
    fn rebuilt(&self, route: DeviceRoute, ttl: u8) -> Packet {
        let view = self.view();
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let payload_end = Header::SIZE + view.payload.len();
        Header {
            ptype: view.header.ptype,
            routing_size: route.len() as u8,
            ttl,
            payload_size: view.header.payload_size,
        }
        .write((&mut buf[..Header::SIZE]).try_into().unwrap());
        buf[Header::SIZE..payload_end].copy_from_slice(view.payload);
        finish(&mut buf, payload_end, route)
    }
}

/// Stamp a route onto a header-and-payload a wire writer left unaddressed, and
/// freeze the result. A valid payload plus the longest route still fits a
/// packet, so this cannot overflow `buf`.
fn finish(buf: &mut [u8; WirePacket::MAX_SIZE], len: usize, routing: DeviceRoute) -> Packet {
    let mut header = Header::parse((&buf[..Header::SIZE]).try_into().unwrap())
        .expect("a wire writer stamped this header");
    header.routing_size = routing.len() as u8;
    header.write((&mut buf[..Header::SIZE]).try_into().unwrap());
    let total = len + routing.len();
    routing
        .write_wire(&mut buf[len..total])
        .expect("a route fits behind its payload");
    Packet {
        raw: Bytes::copy_from_slice(&buf[..total]),
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Packet")
            .field("payload", &self.payload())
            .field("routing", &self.route())
            .field("ttl", &self.ttl())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_TTL: usize = Header::MAX_TTL as usize;

    fn route(value: &str) -> DeviceRoute {
        value.parse().unwrap()
    }

    #[test]
    fn encode_reports_payload_size_limit() {
        let arg = vec![0; WirePacket::MAX_PAYLOAD];
        let error = Packet::rpc_request("x", &arg, 0, DeviceRoute::root()).unwrap_err();

        assert_eq!(
            error,
            EncodeError::PayloadTooLarge {
                actual: 5 + WirePacket::MAX_PAYLOAD,
                maximum: WirePacket::MAX_PAYLOAD,
            }
        );
    }

    #[test]
    fn rpc_request_encoding_preserves_wire_layout() {
        let packet = Packet::rpc_request("x", &[1, 2], 0x1234, route("/1/2")).unwrap();

        assert_eq!(
            packet.as_bytes(),
            [
                2, 2, 7, 0, // RPC request header, two routing hops
                0x34, 0x12, // request ID
                1, 0x80, b'x', // by-name method
                1, 2, // arguments
                2, 1, // routing is reversed on the wire
            ]
        );
    }

    #[test]
    fn ttl_survives_a_serialize_roundtrip() {
        let sent = Packet::heartbeat_session(1, route("/1/2"))
            .with_ttl(3)
            .unwrap();
        let received = Packet::from_wire(sent.clone().into_bytes()).unwrap();

        assert_eq!(received.ttl(), 3);
        assert_eq!(received.route(), sent.route());
        assert!(matches!(
            received.payload(),
            Payload::Heartbeat(Heartbeat::Session(session)) if session.value() == 1
        ));
    }

    #[test]
    fn encode_rejects_a_ttl_that_does_not_fit_the_header_nibble() {
        let error = Packet::heartbeat_session(1, DeviceRoute::root())
            .with_ttl(MAX_TTL as u8 + 1)
            .unwrap_err();

        assert_eq!(
            error,
            EncodeError::ValueTooLarge {
                value: MAX_TTL + 1,
                maximum: MAX_TTL,
            }
        );
    }

    #[test]
    fn a_metadata_record_this_build_does_not_know_is_forwarded_whole() {
        let payload = [5, wire::MetadataFlags::UPDATE.bits(), 3, 0xaa, 0xbb, 0xcc];
        let mut raw = vec![PacketType::METADATA.value(), 0, payload.len() as u8, 0];
        raw.extend_from_slice(&payload);
        let packet = Packet::from_wire(Bytes::from(raw.clone())).unwrap();

        assert!(matches!(
            packet.payload(),
            Payload::Unknown(PacketType::METADATA, body) if body == payload
        ));
        assert_eq!(packet.as_bytes(), raw);
    }

    #[test]
    fn a_broken_metadata_record_is_rejected() {
        // A device record whose name length runs past the record.
        let payload = [1, 0, 9, 9, 0, 0, 0, 0, 0, 0, 0];
        let mut raw = vec![PacketType::METADATA.value(), 0, payload.len() as u8, 0];
        raw.extend_from_slice(&payload);

        assert!(matches!(
            Packet::from_wire(Bytes::from(raw)),
            Err(DecodeError::InvalidPayload)
        ));
    }

    #[test]
    fn a_prefix_shares_storage_and_reports_what_it_consumed() {
        let first = Packet::heartbeat_session(1, DeviceRoute::root());
        let second = Packet::rpc_error(7, wire_rpc::RpcError::Timeout, DeviceRoute::root());
        let mut stream = first.as_bytes().to_vec();
        stream.extend_from_slice(second.as_bytes());
        let stream = Bytes::from(stream);

        let (packet, used) = Packet::from_wire_prefix(&stream).unwrap();
        assert_eq!(used, first.as_bytes().len());
        assert_eq!(packet.as_bytes(), first.as_bytes());

        let (packet, used) = Packet::from_wire_prefix(&stream.slice(used..)).unwrap();
        assert_eq!(used, second.as_bytes().len());
        assert_eq!(packet.as_bytes(), second.as_bytes());
    }

    #[test]
    fn trailing_bytes_are_not_a_packet() {
        let mut raw = Packet::heartbeat(DeviceRoute::root()).as_bytes().to_vec();
        raw.push(0);

        assert!(matches!(
            Packet::from_wire(Bytes::from(raw)),
            Err(DecodeError::PacketTooBig)
        ));
    }

    #[test]
    fn rescoping_a_route_keeps_the_payload() {
        let packet = Packet::rpc_request("dev.name", b"", 3, route("/1"))
            .unwrap()
            .with_route(route("/4/5"));

        assert_eq!(packet.route(), route("/4/5"));
        assert!(matches!(
            packet.payload(),
            Payload::RpcRequest(request)
                if request.method == wire_rpc::Method::ByName(b"dev.name")
        ));
    }
}

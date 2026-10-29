//! The host's [`Packet`], one validated wire buffer read through the
//! [`proto`](crate::proto) codecs, and the [`Payload`] it decodes to.

use crate::proto::data as wire;
use crate::proto::heartbeat::Heartbeat;
use crate::proto::log::LogMessage;
use crate::proto::packet::Packet as WirePacket;
use crate::proto::packet::{Header, PacketError, PacketView};
use crate::proto::rpc as wire_rpc;
use crate::proto::settings::Setting;
use crate::proto::RpcRequestId;
use crate::proto::SessionId as WireSessionId;
use bytes::Bytes;
use num_enum::{FromPrimitive, IntoPrimitive};
use std::fmt;

use crate::proto::data::MAX_SAMPLE_NUMBER;
use crate::proto::packet::PacketType;
use crate::proto::DeviceRoute;

/// The shortest data payload a host accepts, the sample header plus one byte.
const MIN_SAMPLE_PAYLOAD: usize = wire::SAMPLE_HEADER_SIZE + 1;

/// One complete, validated TIO packet.
///
/// The packet holds only its wire bytes. Every accessor reads them through
/// the wire codecs.
#[derive(Clone)]
pub struct Packet {
    raw: Bytes,
}

/// Borrowed view of a packet's payload.
///
/// Text fields are `&str` only where the wire codec validated them as UTF-8.
/// Everything else stays bytes, and callers convert at the point of use.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
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
    /// A proxy's link status in the legacy form, type 64, which old proxies
    /// and old recordings still carry.
    LegacyStatus(LinkStatus),
    /// A control request from a client to its proxy, an RPC request naming its
    /// method.
    ProxyTx(wire_rpc::Request<'a>),
    /// A control reply or a link status from a proxy to its client.
    ProxyRx(ProxyRx<'a>),
    /// A packet type this build does not interpret, kept whole for forwarding
    /// and logging.
    Unknown(PacketType, &'a [u8]),
}

/// What a proxy tells its client in a `PROXY_RX` packet. The payload leads
/// with a kind byte, and the body that follows depends on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProxyRx<'a> {
    /// The answer to a control request, kind 0.
    Reply(wire_rpc::Reply<'a>),
    /// A control request refused, kind 1. Its message may say why.
    Error(wire_rpc::ErrorReply<'a>),
    /// The link status of the subtree the packet's route names, kind 2.
    Link(LinkStatus),
    /// A sign that the proxy is alive, kind 3, sent when nothing else was.
    Alive,
    /// A kind this build does not know, with the body after its kind byte.
    Unknown(u8, &'a [u8]),
}

/// `PROXY_RX` kind byte of a reply.
const PROXY_RX_REPLY: u8 = 0;
/// `PROXY_RX` kind byte of an error.
const PROXY_RX_ERROR: u8 = 1;
/// `PROXY_RX` kind byte of a link status.
const PROXY_RX_LINK: u8 = 2;
/// `PROXY_RX` kind byte of a sign of life.
const PROXY_RX_ALIVE: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
/// The link status a proxy announces to its clients. [`Packet::link_status`]
/// reads it from either form of status packet.
#[non_exhaustive]
pub enum LinkStatus {
    /// The device went quiet or its transport closed.
    SensorDisconnected = 0,
    /// The device is answering again after a disconnect.
    SensorReconnected = 1,
    /// The proxy gave up reconnecting.
    FailedToReconnect = 2,
    /// The transport did not open.
    FailedToConnect = 3,
    /// The proxy stopped. The client must not reconnect.
    Stopped = 4,
    /// A status code this build does not know.
    #[num_enum(catch_all)]
    Unknown(u8),
}

/// Why a buffer is not one packet. Transports count and log these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The buffer ends before the packet does.
    #[error("more data needed")]
    NeedMore,
    /// The serial frame's checksum did not match.
    #[error("CRC32 mismatch")]
    CRC32,
    /// The buffer is longer than the packet, or than any packet may be.
    #[error("packet too big")]
    PacketTooBig,
    /// The buffer is shorter than a header.
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
#[non_exhaustive]
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
    /// The stream id is outside the range 1 to 127.
    #[error("stream ID {0} is outside the encodable range 1..=127")]
    InvalidStreamId(u8),
    /// The sample number does not fit the 24 bits it has on the wire.
    #[error("sample number {0} does not fit in the 24-bit packet field")]
    SampleNumberTooLarge(u32),
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
            Payload::LegacyStatus(legacy_link_status(*payload.first().ok_or(too_short)?))
        }
        PacketType::PROXY_TX => {
            Payload::ProxyTx(wire_rpc::Request::parse(payload).ok_or(too_short)?)
        }
        PacketType::PROXY_RX => Payload::ProxyRx(parse_proxy_rx(payload)?),
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

/// Returns the status a legacy type 64 packet carries. The legacy form has no
/// stop, and code 4 there reads as [`LinkStatus::Unknown`].
fn legacy_link_status(code: u8) -> LinkStatus {
    match LinkStatus::from(code) {
        LinkStatus::Stopped => LinkStatus::Unknown(code),
        status => status,
    }
}

/// Parses a `PROXY_RX` payload, a kind byte followed by that kind's body. A
/// kind this build does not know becomes [`ProxyRx::Unknown`].
fn parse_proxy_rx(payload: &[u8]) -> Result<ProxyRx<'_>, DecodeError> {
    let too_short = DecodeError::PayloadTooShort;
    let (&kind, body) = payload.split_first().ok_or(too_short)?;
    Ok(match kind {
        PROXY_RX_REPLY => ProxyRx::Reply(wire_rpc::Reply::parse(body).ok_or(too_short)?),
        PROXY_RX_ERROR => ProxyRx::Error(wire_rpc::ErrorReply::parse(body).ok_or(too_short)?),
        PROXY_RX_LINK => ProxyRx::Link(LinkStatus::from(*body.first().ok_or(too_short)?)),
        PROXY_RX_ALIVE => ProxyRx::Alive,
        _ => ProxyRx::Unknown(kind, body),
    })
}

/// Parses a metadata payload. A record kind this build does not know has its
/// framing checked and is kept whole as [`Payload::Unknown`].
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
    /// Validates `raw` as exactly one packet and takes ownership of it.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError`] if `raw` is not exactly one valid packet.
    pub fn from_wire(raw: Bytes) -> Result<Packet, DecodeError> {
        let (view, len) = PacketView::parse_prefix(&raw)?;
        if len != raw.len() {
            return Err(DecodeError::PacketTooBig);
        }
        DeviceRoute::from_wire(view.routing).map_err(|_| DecodeError::RoutingTooBig)?;
        parse_payload(view.header, view.payload)?;
        Ok(Packet { raw })
    }

    /// Parses one packet from the start of `raw`, sharing its storage.
    /// Returns the packet and the number of bytes it used.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::NeedMore`] if `raw` ends before the packet
    /// does, and another [`DecodeError`] if the packet is invalid.
    pub fn from_wire_prefix(raw: &Bytes) -> Result<(Packet, usize), DecodeError> {
        let len = PacketView::parse_prefix(raw)?.1;
        Ok((Self::from_wire(raw.slice(..len))?, len))
    }

    /// Parses one packet from the start of `raw` as
    /// [`from_wire_prefix`](Self::from_wire_prefix) does, copying its bytes.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`from_wire_prefix`](Self::from_wire_prefix).
    pub fn from_slice_prefix(raw: &[u8]) -> Result<(Packet, usize), DecodeError> {
        let len = PacketView::parse_prefix(raw)?.1;
        Ok((Self::from_wire(Bytes::copy_from_slice(&raw[..len]))?, len))
    }

    /// Creates an RPC request for `name` with encoded `arg`, addressed to
    /// `routing` and tagged `id` for its reply.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if the name or the payload is too long.
    pub fn rpc_request(
        name: &str,
        arg: &[u8],
        id: u16,
        routing: DeviceRoute,
    ) -> Result<Packet, EncodeError> {
        let method = wire_rpc::Method::ByName(name.as_bytes());
        let payload_len = request_payload_len(name, arg)?;
        build(payload_len, routing, |buf| {
            wire_rpc::write_request(buf, RpcRequestId::new(id), method, arg)
        })
    }

    /// Creates the reply to request `id`, carrying `value`, addressed to
    /// `routing`.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::PayloadTooLarge`] if `value` does not fit a
    /// packet.
    pub fn rpc_reply(id: u16, value: &[u8], routing: DeviceRoute) -> Result<Packet, EncodeError> {
        let payload_len = wire_rpc::REPLY_HEADER_SIZE + value.len();
        build(payload_len, routing, |buf| {
            wire_rpc::write_reply(buf, RpcRequestId::new(id), value)
        })
    }

    /// Creates the refusal of request `id` with `code` and no message,
    /// addressed to `routing`.
    // A bare error packet is 8 bytes, so it always fits the buffer.
    #[allow(clippy::missing_panics_doc)]
    pub fn rpc_error(id: u16, code: wire_rpc::RpcError, routing: DeviceRoute) -> Packet {
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = wire_rpc::write_error(&mut buf, RpcRequestId::new(id), code)
            .expect("a bare error payload always fits");
        finish(&mut buf, len, routing)
    }

    /// Creates the keepalive a host sends to hold a link open.
    pub fn heartbeat(routing: DeviceRoute) -> Packet {
        Self::write_heartbeat(Heartbeat::Any(&[]), routing)
    }

    /// Creates a heartbeat announcing `session` alone, as a device with no
    /// settings sequence sends it.
    pub fn heartbeat_session(session: u32, routing: DeviceRoute) -> Packet {
        let beat = Heartbeat::Session {
            session: WireSessionId::new(session),
            settings: None,
        };
        Self::write_heartbeat(beat, routing)
    }

    fn write_heartbeat(beat: Heartbeat<'_>, routing: DeviceRoute) -> Packet {
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = beat.write(&mut buf).expect("a heartbeat always fits");
        finish(&mut buf, len, routing)
    }

    /// Creates a link status in the legacy form, type 64, for a client that
    /// predates `PROXY_RX`. [`LinkStatus::Stopped`] goes out as
    /// [`LinkStatus::FailedToReconnect`]. The packet has no route.
    // A one byte payload always fits a packet.
    #[allow(clippy::missing_panics_doc)]
    pub fn legacy_status(status: LinkStatus) -> Packet {
        let code = match status {
            LinkStatus::Stopped => LinkStatus::FailedToReconnect,
            status => status,
        };
        routeless(PacketType::PROXY_STATUS, 1, |payload| {
            payload[0] = u8::from(code);
            Some(1)
        })
        .expect("a legacy status always fits")
    }

    /// Creates a link status as a `PROXY_RX` packet. The status concerns the
    /// subtree at the packet's route. The packet has no route until
    /// [`with_route`](Self::with_route) gives it one.
    // A two byte payload always fits a packet.
    #[allow(clippy::missing_panics_doc)]
    pub fn link(status: LinkStatus) -> Packet {
        proxy_rx(PROXY_RX_LINK, 1, |body| {
            body[0] = u8::from(status);
            Some(1)
        })
        .expect("a link status always fits")
    }

    /// Creates the sign of life a proxy sends a client. The packet has no
    /// route.
    // An empty body always fits a packet.
    #[allow(clippy::missing_panics_doc)]
    pub fn alive() -> Packet {
        proxy_rx(PROXY_RX_ALIVE, 0, |_| Some(0)).expect("a sign of life always fits")
    }

    /// Creates a control request to the proxy itself, calling `name` with
    /// encoded `arg` and tagged `id` for its reply. The packet has no route.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if the name or the payload is too long.
    pub fn proxy_request(name: &str, arg: &[u8], id: u16) -> Result<Packet, EncodeError> {
        let method = wire_rpc::Method::ByName(name.as_bytes());
        let payload_len = request_payload_len(name, arg)?;
        routeless(PacketType::PROXY_TX, payload_len, |payload| {
            wire_rpc::write_request_payload(payload, RpcRequestId::new(id), method, arg)
        })
    }

    /// Creates the proxy's reply to control request `id`, carrying `value`.
    /// The packet has no route.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::PayloadTooLarge`] if `value` does not fit a
    /// packet.
    pub fn proxy_reply(id: u16, value: &[u8]) -> Result<Packet, EncodeError> {
        let body_len = wire_rpc::REPLY_HEADER_SIZE + value.len();
        proxy_rx(PROXY_RX_REPLY, body_len, |body| {
            wire_rpc::write_reply_payload(body, RpcRequestId::new(id), value)
        })
    }

    /// Creates the proxy's refusal of control request `id` with `code` and a
    /// `message`, which may be empty. The packet has no route.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::PayloadTooLarge`] if `message` does not fit a
    /// packet.
    pub fn proxy_error(
        id: u16,
        code: wire_rpc::RpcError,
        message: &str,
    ) -> Result<Packet, EncodeError> {
        let body_len = 4 + message.len();
        proxy_rx(PROXY_RX_ERROR, body_len, |body| {
            wire_rpc::write_error_payload(
                body,
                RpcRequestId::new(id),
                code.value(),
                message.as_bytes(),
            )
        })
    }

    /// Creates a packet of one metadata `record` with its `flags`, addressed
    /// to `routing`.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::PayloadTooLarge`] if the record does not fit a
    /// packet.
    pub fn metadata(
        record: wire::Metadata<'_>,
        flags: wire::MetadataFlags,
        routing: DeviceRoute,
    ) -> Result<Packet, EncodeError> {
        let payload_len = wire::METADATA_HEADER_SIZE + record.record_len();
        build(payload_len, routing, |buf| record.write(flags, buf))
    }

    /// Creates a metadata packet as [`Self::metadata`] does, from a `record`
    /// already in its wire form. `kind` is the record's wire type byte.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError::PayloadTooLarge`] if the record does not fit a
    /// packet.
    pub fn metadata_record(
        kind: u8,
        flags: wire::MetadataFlags,
        record: &[u8],
        routing: DeviceRoute,
    ) -> Result<Packet, EncodeError> {
        let payload_len = wire::METADATA_HEADER_SIZE + record.len();
        build(payload_len, routing, |buf| {
            wire::write_metadata_record(buf, kind, flags, record)
        })
    }

    /// Creates a packet of packed sample `data` for stream `stream_id`,
    /// starting at sample `first_sample_n`.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if `stream_id` is outside 1 to 127, if
    /// `first_sample_n` does not fit 24 bits, or if `data` does not fit a
    /// packet.
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
        let samples = wire::Samples {
            stream_id: crate::proto::StreamId::try_new(stream_id)
                .ok_or(EncodeError::InvalidStreamId(stream_id))?,
            segment_id: crate::proto::SegmentId::new(segment_id),
            first: crate::proto::SampleNumber::new(first_sample_n),
            data,
        };
        let payload_len = wire::SAMPLE_HEADER_SIZE + data.len();
        build(payload_len, routing, |buf| samples.write(buf))
    }

    /// Returns the payload, parsed from the packet's bytes.
    // Every packet holds a payload that parses. `from_wire` checks it and the
    // constructors write it.
    #[allow(clippy::missing_panics_doc)]
    pub fn payload(&self) -> Payload<'_> {
        let view = self.view();
        parse_payload(view.header, view.payload).expect("payload validated in from_wire")
    }

    /// Returns the link status this packet announces in either form, or
    /// `None` if it announces none.
    pub fn link_status(&self) -> Option<LinkStatus> {
        match self.payload() {
            Payload::LegacyStatus(status) | Payload::ProxyRx(ProxyRx::Link(status)) => Some(status),
            _ => None,
        }
    }

    /// Returns the payload's raw bytes, for a caller that keeps part of them
    /// as they are.
    pub fn payload_bytes(&self) -> &[u8] {
        self.view().payload
    }

    /// Returns the packet type from the header.
    pub fn ptype(&self) -> PacketType {
        self.view().header.ptype
    }

    /// Returns the route the packet is addressed to or came from.
    // Every packet holds a valid route. `from_wire` checks it and the
    // constructors write it.
    #[allow(clippy::missing_panics_doc)]
    pub fn route(&self) -> DeviceRoute {
        DeviceRoute::from_wire(self.view().routing).expect("routing validated in from_wire")
    }

    /// Returns the hops left before a hub drops the packet.
    pub fn ttl(&self) -> u8 {
        self.view().header.ttl
    }

    /// Returns `true` if the packet carries stream samples, legacy encodings
    /// included.
    pub fn is_data(&self) -> bool {
        self.ptype().stream_id().is_some()
    }

    /// Returns the whole packet as it travels the wire.
    pub fn as_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// Returns the packet's bytes without copying.
    pub fn into_bytes(self) -> Bytes {
        self.raw
    }

    /// Returns a copy of the packet addressed to `route`, with the same
    /// payload and TTL.
    pub fn with_route(&self, route: DeviceRoute) -> Packet {
        if route == self.route() {
            return self.clone();
        }
        let view = self.view();
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let payload_end = Header::SIZE + view.payload.len();
        buf[..payload_end].copy_from_slice(&self.raw[..payload_end]);
        finish(&mut buf, payload_end, route)
    }

    /// Returns a copy of an RPC request, reply or error with its request id
    /// set to `id`. Returns any other packet unchanged.
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
}

/// Returns the payload length of a request calling `name` with `arg`.
/// Returns [`EncodeError::ValueTooLarge`] if the name is too long to encode.
fn request_payload_len(name: &str, arg: &[u8]) -> Result<usize, EncodeError> {
    let method = wire_rpc::Method::ByName(name.as_bytes());
    wire_rpc::request_payload_len(method, arg).ok_or(EncodeError::ValueTooLarge {
        value: name.len(),
        maximum: wire_rpc::NAMELEN_MASK as usize,
    })
}

/// Builds a packet addressed to `routing`. `write` fills the header and the
/// `payload_len` byte payload from the start of the buffer and returns their
/// length. Returns [`EncodeError::PayloadTooLarge`] before `write` runs if
/// `payload_len` exceeds the maximum.
fn build(
    payload_len: usize,
    routing: DeviceRoute,
    write: impl FnOnce(&mut [u8]) -> Option<usize>,
) -> Result<Packet, EncodeError> {
    if payload_len > WirePacket::MAX_PAYLOAD {
        return Err(EncodeError::PayloadTooLarge {
            actual: payload_len,
            maximum: WirePacket::MAX_PAYLOAD,
        });
    }
    let mut buf = [0u8; WirePacket::MAX_SIZE];
    let len = write(&mut buf).expect("payload sized above");
    Ok(finish(&mut buf, len, routing))
}

/// Builds a packet of type `ptype` with no route, whose `len` byte payload
/// `write` fills. Returns [`EncodeError::PayloadTooLarge`] before `write` runs
/// if `len` exceeds the maximum.
fn routeless(
    ptype: PacketType,
    len: usize,
    write: impl FnOnce(&mut [u8]) -> Option<usize>,
) -> Result<Packet, EncodeError> {
    build(len, DeviceRoute::root(), |buf| {
        let end = Header::SIZE + len;
        Header::new(ptype, len as u16).write((&mut buf[..Header::SIZE]).try_into().unwrap());
        write(&mut buf[Header::SIZE..end])?;
        Some(end)
    })
}

/// Builds a `PROXY_RX` packet of `kind` with no route. `write` fills the
/// `len` byte body after the kind byte.
fn proxy_rx(
    kind: u8,
    len: usize,
    write: impl FnOnce(&mut [u8]) -> Option<usize>,
) -> Result<Packet, EncodeError> {
    routeless(PacketType::PROXY_RX, 1 + len, |payload| {
        let (head, body) = payload.split_first_mut()?;
        *head = kind;
        write(body).map(|written| 1 + written)
    })
}

/// Writes `routing` after the first `len` bytes of `buf`, a header and
/// payload with no route, and returns the packet. Any valid payload plus the
/// longest route fits in `buf`.
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

    /// The packet a receiver decodes from `packet`'s bytes.
    fn received(packet: &Packet) -> Packet {
        Packet::from_wire(packet.clone().into_bytes()).expect("a valid packet")
    }

    /// The `PROXY_RX` payload `packet` carries.
    fn proxy_rx_of(packet: &Packet) -> ProxyRx<'_> {
        let Payload::ProxyRx(rx) = packet.payload() else {
            panic!("expected a PROXY_RX payload, got {:?}", packet.payload());
        };
        rx
    }

    /// A `PROXY_TX` payload is exactly an RPC request payload, method by name.
    #[test]
    fn a_proxy_request_matches_the_wire_layout() {
        let packet = Packet::proxy_request("proxy.open", b"auto", 1).unwrap();

        #[rustfmt::skip]
        assert_eq!(packet.as_bytes(), [
            66, 0, 18, 0,               // PROXY_TX, no routing, payload 18
            1, 0,                       // request id
            10, 0x80,                   // by name, 10 bytes
            b'p', b'r', b'o', b'x', b'y', b'.', b'o', b'p', b'e', b'n',
            b'a', b'u', b't', b'o',     // argument
        ]);
    }

    #[test]
    fn a_proxy_reply_matches_the_wire_layout() {
        let packet = Packet::proxy_reply(1, b"ok").unwrap();

        #[rustfmt::skip]
        assert_eq!(packet.as_bytes(), [
            67, 0, 5, 0,                // PROXY_RX, no routing, payload 5
            0,                          // kind: reply
            1, 0,                       // request id
            b'o', b'k',                 // value
        ]);
    }

    #[test]
    fn a_proxy_error_matches_the_wire_layout() {
        let packet = Packet::proxy_error(1, wire_rpc::RpcError::NotFound, "no link").unwrap();

        #[rustfmt::skip]
        assert_eq!(packet.as_bytes(), [
            67, 0, 12, 0,               // PROXY_RX, no routing, payload 12
            1,                          // kind: error
            1, 0,                       // request id
            2, 0,                       // code: not found
            b'n', b'o', b' ', b'l', b'i', b'n', b'k',
        ]);
    }

    /// A link packet's route names the subtree it concerns, as type 64's does.
    #[test]
    fn a_link_packet_matches_the_wire_layout() {
        let packet = Packet::link(LinkStatus::Stopped).with_route(route("/1/2"));

        #[rustfmt::skip]
        assert_eq!(packet.as_bytes(), [
            67, 2, 2, 0,                // PROXY_RX, two routing hops, payload 2
            2,                          // kind: link
            4,                          // status: stopped
            2, 1,                       // routing is reversed on the wire
        ]);
    }

    /// A legacy client knows no stop, so it hears that reconnecting failed.
    #[test]
    fn a_legacy_stop_encodes_as_failed_to_reconnect() {
        let packet = Packet::legacy_status(LinkStatus::Stopped);

        assert_eq!(packet.as_bytes(), [64, 0, 1, 0, 2]);
    }

    /// Only the `PROXY_RX` form carries a stop, so a 4 in a type 64 packet is
    /// some other `USER` packet and must not end its subtree's session.
    #[test]
    fn a_legacy_status_never_reads_as_stopped() {
        let packet = Packet::from_wire(Bytes::from_static(&[64, 0, 1, 0, 4])).unwrap();

        assert_eq!(packet.link_status(), Some(LinkStatus::Unknown(4)));
    }

    #[test]
    fn a_proxy_request_decodes_as_it_was_sent() {
        let packet = received(&Packet::proxy_request("proxy.open", b"auto", 1).unwrap());

        let Payload::ProxyTx(request) = packet.payload() else {
            panic!("expected a PROXY_TX payload, got {:?}", packet.payload());
        };
        assert_eq!(request.id, RpcRequestId::new(1));
        assert_eq!(request.method, wire_rpc::Method::ByName(b"proxy.open"));
        assert_eq!(request.args, b"auto");
    }

    #[test]
    fn every_proxy_rx_kind_decodes_as_it_was_sent() {
        let reply = received(&Packet::proxy_reply(1, b"ok").unwrap());
        assert_eq!(
            proxy_rx_of(&reply),
            ProxyRx::Reply(wire_rpc::Reply {
                req_id: RpcRequestId::new(1),
                value: b"ok",
            })
        );

        let error =
            received(&Packet::proxy_error(1, wire_rpc::RpcError::NotFound, "no link").unwrap());
        assert_eq!(
            proxy_rx_of(&error),
            ProxyRx::Error(wire_rpc::ErrorReply {
                req_id: RpcRequestId::new(1),
                code: wire_rpc::RpcError::NotFound.value(),
                message: b"no link",
            })
        );

        let link = received(&Packet::link(LinkStatus::Stopped).with_route(route("/1/2")));
        assert_eq!(proxy_rx_of(&link), ProxyRx::Link(LinkStatus::Stopped));
        assert_eq!(link.route(), route("/1/2"));

        assert_eq!(proxy_rx_of(&received(&Packet::alive())), ProxyRx::Alive);

        // Kind 9, which this build does not know, with a two byte body.
        let unknown = Packet::from_wire(Bytes::from_static(&[67, 0, 3, 0, 9, 0xaa, 0xbb])).unwrap();
        assert_eq!(proxy_rx_of(&unknown), ProxyRx::Unknown(9, &[0xaa, 0xbb]));
    }

    #[test]
    fn a_legacy_status_decodes_as_it_was_sent() {
        let packet = received(&Packet::legacy_status(LinkStatus::SensorReconnected));

        assert!(matches!(
            packet.payload(),
            Payload::LegacyStatus(LinkStatus::SensorReconnected)
        ));
        assert!(matches!(
            received(&Packet::legacy_status(LinkStatus::Stopped)).payload(),
            Payload::LegacyStatus(LinkStatus::FailedToReconnect)
        ));
    }

    /// A `PROXY_RX` payload must name its kind, and a link must carry its
    /// status. Replies and errors too short to name a request fail the same
    /// way, as `RPC_REP` and `RPC_ERROR` do.
    #[test]
    fn a_proxy_rx_too_short_for_its_kind_is_rejected() {
        for raw in [
            &[67, 0, 0, 0][..],          // no kind
            &[67, 0, 1, 0, 2][..],       // a link with no status
            &[67, 0, 2, 0, 0, 1][..],    // a reply with half a request id
            &[67, 0, 3, 0, 1, 1, 0][..], // an error with no code
            &[66, 0, 3, 0, 1, 0, 0][..], // a request with half a method word
        ] {
            assert_eq!(
                Packet::from_wire(Bytes::copy_from_slice(raw)).unwrap_err(),
                DecodeError::PayloadTooShort,
                "{raw:?}"
            );
        }
    }

    /// Both forms announce the same status, which the legacy form can only
    /// differ on for a stop.
    #[test]
    fn both_forms_announce_the_same_link_status() {
        for status in [
            LinkStatus::SensorDisconnected,
            LinkStatus::SensorReconnected,
            LinkStatus::FailedToReconnect,
            LinkStatus::FailedToConnect,
            LinkStatus::Unknown(9),
        ] {
            let link = received(&Packet::link(status));
            let legacy = received(&Packet::legacy_status(status));

            assert_eq!(link.ptype(), PacketType::PROXY_RX);
            assert_eq!(legacy.ptype(), PacketType::PROXY_STATUS);
            assert_eq!(link.link_status(), Some(status));
            assert_eq!(legacy.link_status(), link.link_status());
        }
        assert_eq!(
            Packet::link(LinkStatus::Stopped).link_status(),
            Some(LinkStatus::Stopped)
        );
    }

    #[test]
    fn only_a_status_announces_a_link_status() {
        assert_eq!(Packet::heartbeat(DeviceRoute::root()).link_status(), None);
        assert_eq!(Packet::proxy_reply(1, b"").unwrap().link_status(), None);
        assert_eq!(
            Packet::proxy_request("proxy.open", b"", 1)
                .unwrap()
                .link_status(),
            None
        );
    }

    #[test]
    fn a_proxy_packet_reports_the_payload_size_limit() {
        let value = vec![0; WirePacket::MAX_PAYLOAD];

        assert_eq!(
            Packet::proxy_reply(1, &value).unwrap_err(),
            EncodeError::PayloadTooLarge {
                actual: 3 + WirePacket::MAX_PAYLOAD,
                maximum: WirePacket::MAX_PAYLOAD,
            }
        );
        assert_eq!(
            Packet::proxy_request("x", &value, 1).unwrap_err(),
            EncodeError::PayloadTooLarge {
                actual: 5 + WirePacket::MAX_PAYLOAD,
                maximum: WirePacket::MAX_PAYLOAD,
            }
        );
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

//! The host's [`Packet`], one validated wire buffer read through the
//! [`proto`](crate::proto) codecs into the [`Payload`] it carries.

use crate::proto::data as wire;
use crate::proto::heartbeat::Heartbeat;
use crate::proto::packet::Packet as WirePacket;
use crate::proto::packet::{write_routing, Checked, Header, PacketError, PacketView, WriteError};
use crate::proto::rpc as wire_rpc;
use crate::proto::RpcRequestId;
use crate::proto::SessionId as WireSessionId;
use bytes::Bytes;
use std::fmt;

use crate::proto::packet::PacketType;
use crate::proto::DeviceRoute;

pub use crate::proto::packet::Payload;
pub use crate::proto::proxy::{LinkStatus, ProxyRx};

/// One complete, validated TIO packet.
///
/// The packet holds only its wire bytes. Every accessor reads them through
/// the wire codecs.
#[derive(Clone)]
pub struct Packet {
    raw: Bytes,
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
            PacketError::PayloadTooShort => Self::PayloadTooShort,
            PacketError::InvalidPayload => Self::InvalidPayload,
            PacketError::TrailingBytes => Self::PacketTooBig,
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
    /// A sample packet carries no sample bytes.
    #[error("a sample packet needs at least one sample byte")]
    NoSamples,
}

impl Packet {
    /// Validates `raw` as exactly one packet and takes ownership of it.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError`] if `raw` is not exactly one valid packet.
    pub fn from_wire(raw: Bytes) -> Result<Packet, DecodeError> {
        Checked::exact(&raw)?;
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
        let len = Checked::prefix(raw)?.1;
        Ok((
            Packet {
                raw: raw.slice(..len),
            },
            len,
        ))
    }

    /// Parses one packet from the start of `raw` as
    /// [`from_wire_prefix`](Self::from_wire_prefix) does, copying its bytes.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`from_wire_prefix`](Self::from_wire_prefix).
    pub fn from_slice_prefix(raw: &[u8]) -> Result<(Packet, usize), DecodeError> {
        let (checked, len) = Checked::prefix(raw)?;
        Ok((Packet::from(checked), len))
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
        let asked = Asked::Request { name, arg };
        build(routing, asked, |buf| {
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
        build(routing, Asked::Reply { value }, |buf| {
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

    /// Creates a link status in the legacy form, type 64, as a proxy that
    /// predates `PROXY_RX` sends it. [`LinkStatus::Stopped`] goes out as
    /// [`LinkStatus::FailedToReconnect`]. The packet has no route.
    // A one byte payload always fits a packet.
    #[allow(clippy::missing_panics_doc)]
    pub fn legacy_status(status: LinkStatus) -> Packet {
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let len = status.write(&mut buf).expect("a legacy status always fits");
        finish(&mut buf, len, DeviceRoute::root())
    }

    /// Creates a link status as a `PROXY_RX` packet. The status concerns the
    /// subtree at the packet's route. The packet has no route until
    /// [`with_route`](Self::with_route) gives it one.
    // A two byte payload always fits a packet.
    #[allow(clippy::missing_panics_doc)]
    pub fn link(status: LinkStatus) -> Packet {
        proxy_rx(ProxyRx::Link(status)).expect("a link status always fits")
    }

    /// Creates the sign of life a proxy sends a client. The packet has no
    /// route.
    // An empty body always fits a packet.
    #[allow(clippy::missing_panics_doc)]
    pub fn alive() -> Packet {
        proxy_rx(ProxyRx::Alive).expect("a sign of life always fits")
    }

    /// Creates the count of `packets` sample packets of stream `stream_id`
    /// that a proxy dropped from its queue for a client. The packet has no
    /// route until [`with_route`](Self::with_route) names the device.
    // A six byte payload always fits a packet.
    #[allow(clippy::missing_panics_doc)]
    pub fn dropped(stream_id: u8, packets: u32) -> Packet {
        proxy_rx(ProxyRx::Dropped { stream_id, packets })
            .expect("a count of dropped packets always fits")
    }

    /// Creates a control request to the proxy itself, calling `name` with
    /// encoded `arg` and tagged `id` for its reply. The packet has no route.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if the name or the payload is too long.
    pub fn proxy_request(name: &str, arg: &[u8], id: u16) -> Result<Packet, EncodeError> {
        let method = wire_rpc::Method::ByName(name.as_bytes());
        let asked = Asked::Request { name, arg };
        build(DeviceRoute::root(), asked, |buf| {
            wire_rpc::write_proxy_request(buf, RpcRequestId::new(id), method, arg)
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
        proxy_rx(ProxyRx::Reply(wire_rpc::Reply {
            req_id: RpcRequestId::new(id),
            value,
        }))
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
        proxy_rx(ProxyRx::Error(wire_rpc::ErrorReply {
            req_id: RpcRequestId::new(id),
            code: code.value(),
            message: message.as_bytes(),
        }))
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
        build(routing, Asked::Metadata(record), |buf| {
            record.write(flags, buf)
        })
    }

    /// Creates a packet of packed sample `data` for stream `stream_id`,
    /// starting at sample `first_sample_n`.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if `stream_id` is outside 1 to 127, if
    /// `first_sample_n` does not fit 24 bits, or if `data` is empty or does
    /// not fit a packet.
    pub fn samples(
        stream_id: u8,
        segment_id: u8,
        first_sample_n: u32,
        data: &[u8],
        routing: DeviceRoute,
    ) -> Result<Packet, EncodeError> {
        let samples = wire::Samples {
            stream_id: crate::proto::StreamId::try_new(stream_id)
                .ok_or(EncodeError::InvalidStreamId(stream_id))?,
            segment_id: crate::proto::SegmentId::new(segment_id),
            first: crate::proto::SampleNumber::new(first_sample_n),
            data,
        };
        let asked = Asked::Samples {
            first: first_sample_n,
            data,
        };
        build(routing, asked, |buf| samples.write(buf))
    }

    /// Returns the packet's checked bytes.
    // Every packet holds bytes that pass the check. `from_wire` and the
    // prefix readers check them, and `finish` asserts it of the constructors.
    #[allow(clippy::missing_panics_doc)]
    pub fn checked(&self) -> Checked<'_> {
        Checked::exact(&self.raw).expect("a packet holds checked bytes")
    }

    /// Returns the framing of the packet's bytes, which were checked whole
    /// when the packet was made.
    fn framing(&self) -> PacketView<'_> {
        PacketView::parse_exact(&self.raw).expect("the bytes were checked when the packet was made")
    }

    /// Returns the payload, parsed from the packet's bytes.
    // Every packet holds a payload that parses. It was checked when the
    // packet was made.
    #[allow(clippy::missing_panics_doc)]
    pub fn payload(&self) -> Payload<'_> {
        self.framing()
            .payload()
            .expect("the payload was checked when the packet was made")
    }

    /// Returns the link status this packet announces in either form, or
    /// `None` if it announces none.
    pub fn link_status(&self) -> Option<LinkStatus> {
        self.payload().link_status()
    }

    /// Returns the payload's raw bytes, for a caller that keeps part of them
    /// as they are.
    pub fn payload_bytes(&self) -> &[u8] {
        self.framing().payload
    }

    /// Returns the packet type from the header.
    pub fn ptype(&self) -> PacketType {
        self.framing().header.ptype
    }

    /// Returns the route the packet is addressed to or came from.
    // Every packet holds a route that reads. It was checked when the packet
    // was made.
    #[allow(clippy::missing_panics_doc)]
    pub fn route(&self) -> DeviceRoute {
        self.framing()
            .route()
            .expect("the route was checked when the packet was made")
    }

    /// Returns the header's TTL field. The field is reserved, and a sender
    /// writes 0.
    pub fn ttl(&self) -> u8 {
        self.framing().header.ttl
    }

    /// Returns `true` if the packet carries the samples of a stream from 1 to
    /// 127.
    pub fn is_data(&self) -> bool {
        self.ptype().sample_stream().is_some()
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
        let mut buf = [0u8; WirePacket::MAX_SIZE];
        let payload_end = Header::SIZE + self.payload_bytes().len();
        buf[..payload_end].copy_from_slice(&self.raw[..payload_end]);
        finish(&mut buf, payload_end, route)
    }

    /// Returns a copy of an RPC request, reply or error with its request id
    /// set to `id`. Returns any other packet unchanged.
    pub fn with_rpc_id(&self, id: u16) -> Packet {
        let header = self.framing().header;
        if !matches!(
            header.ptype,
            PacketType::RPC_REQ | PacketType::RPC_REP | PacketType::RPC_ERROR
        ) {
            return self.clone();
        }
        let mut raw = self.raw.to_vec();
        let payload = &mut raw[header.payload_range()];
        if !wire_rpc::set_req_id(payload, RpcRequestId::new(id)) {
            return self.clone();
        }
        Packet { raw: raw.into() }
    }
}

impl<'a> From<&'a Packet> for Checked<'a> {
    fn from(packet: &'a Packet) -> Self {
        packet.checked()
    }
}

impl From<Checked<'_>> for Packet {
    fn from(checked: Checked<'_>) -> Self {
        Packet {
            raw: Bytes::copy_from_slice(checked.as_bytes()),
        }
    }
}

/// What a constructor asked a writer to write, for the numbers of the
/// [`EncodeError`] a refusal becomes.
#[derive(Clone, Copy)]
enum Asked<'a> {
    /// A request calling `name` with `arg`.
    Request { name: &'a str, arg: &'a [u8] },
    /// A reply carrying `value`.
    Reply { value: &'a [u8] },
    /// A `PROXY_RX` payload.
    ProxyRx(ProxyRx<'a>),
    /// A metadata record.
    Metadata(wire::Metadata<'a>),
    /// Samples from sample number `first`.
    Samples { first: u32, data: &'a [u8] },
}

/// Builds a packet addressed to `routing`. `write` fills the header and the
/// payload from the start of a buffer of [`WirePacket::MAX_SIZE`] bytes and
/// returns their length.
fn build(
    routing: DeviceRoute,
    asked: Asked<'_>,
    write: impl FnOnce(&mut [u8]) -> Result<usize, WriteError>,
) -> Result<Packet, EncodeError> {
    let mut buf = [0u8; WirePacket::MAX_SIZE];
    let len = write(&mut buf).map_err(|error| refused(error, asked))?;
    Ok(finish(&mut buf, len, routing))
}

/// The [`EncodeError`] a writer's refusal of `asked` becomes. The buffer
/// holds any packet, so a buffer too small means a payload too large. The
/// numbers are worked out here, from the protocol crate's lengths.
fn refused(error: WriteError, asked: Asked<'_>) -> EncodeError {
    let actual = match (error, asked) {
        (WriteError::NoSamples, _) => return EncodeError::NoSamples,
        (WriteError::SampleNumberTooLarge, Asked::Samples { first, .. }) => {
            return EncodeError::SampleNumberTooLarge(first)
        }
        (_, Asked::Request { name, arg }) => {
            let method = wire_rpc::Method::ByName(name.as_bytes());
            match (error, wire_rpc::request_payload_len(method, arg)) {
                (WriteError::NameTooLong, _) | (_, Err(_)) => {
                    return EncodeError::ValueTooLarge {
                        value: name.len(),
                        maximum: wire_rpc::NAMELEN_MASK as usize,
                    }
                }
                (_, Ok(len)) => len,
            }
        }
        (_, Asked::Reply { value }) => wire_rpc::REPLY_HEADER_SIZE + value.len(),
        (_, Asked::ProxyRx(rx)) => rx.payload_len(),
        (_, Asked::Metadata(record)) => wire::METADATA_HEADER_SIZE + record.record_len(),
        (_, Asked::Samples { data, .. }) => wire::SAMPLE_HEADER_SIZE + data.len(),
    };
    EncodeError::PayloadTooLarge {
        actual,
        maximum: WirePacket::MAX_PAYLOAD,
    }
}

/// Builds a `PROXY_RX` packet carrying `rx` with no route.
fn proxy_rx(rx: ProxyRx<'_>) -> Result<Packet, EncodeError> {
    build(DeviceRoute::root(), Asked::ProxyRx(rx), |buf| rx.write(buf))
}

/// Writes `routing` after the first `len` bytes of `buf`, a header and
/// payload with no route, and returns the packet. Any valid payload plus the
/// longest route fits in `buf`.
fn finish(buf: &mut [u8; WirePacket::MAX_SIZE], len: usize, routing: DeviceRoute) -> Packet {
    let total = write_routing(buf, len, routing)
        .expect("a wire writer stamped this header and a route fits behind its payload");
    debug_assert!(Checked::exact(&buf[..total]).is_ok());
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

    /// The check refuses a sample packet with no sample bytes, so the host
    /// constructor never builds one.
    #[test]
    fn a_sample_packet_needs_samples() {
        assert_eq!(
            Packet::samples(1, 0, 0, &[], DeviceRoute::root()).unwrap_err(),
            EncodeError::NoSamples
        );
        assert!(Packet::samples(1, 0, 0, &[0], DeviceRoute::root())
            .unwrap()
            .is_data());
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

    /// A metadata packet at `/1/2` with a TTL of 5, carrying `record` of
    /// type `kind` with the update flag.
    fn metadata_raw(kind: u8, record: &[u8]) -> Vec<u8> {
        let payload_len = wire::METADATA_HEADER_SIZE + record.len();
        let mut raw = vec![
            PacketType::METADATA.value(),
            (5 << 4) | 2,
            payload_len as u8,
            0,
            kind,
            wire::MetadataFlags::UPDATE.bits(),
        ];
        raw.extend_from_slice(record);
        raw.extend_from_slice(&[2, 1]);
        raw
    }

    /// A column record of stream 3, index 4 and type f32 whose units hold
    /// `units`.
    fn column_with_units(units: &[u8]) -> Vec<u8> {
        let head = [7, 3, 4, 0x42, 4, units.len() as u8, 5];
        [&head[..], b"temp", units, b"probe"].concat()
    }

    #[test]
    fn a_metadata_packet_that_parses_keeps_its_bytes() {
        let raw = Bytes::from(metadata_raw(4, &column_with_units(b"degC")));
        let packet = Packet::from_wire(raw.clone()).unwrap();

        assert_eq!(packet.as_bytes(), &raw[..]);
        assert_eq!(packet.as_bytes().as_ptr(), raw.as_ptr(), "not copied");
    }

    #[test]
    fn a_metadata_record_with_a_string_past_its_end_is_still_rejected() {
        let mut record = column_with_units(&[0xB0, b'C']);
        record[6] = 9; // the description runs past the record
        let raw = metadata_raw(4, &record);

        assert_eq!(
            Packet::from_wire(Bytes::from(raw)).unwrap_err(),
            DecodeError::InvalidPayload
        );
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

    /// The legacy form knows no stop, so it says that reconnecting failed.
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

        let dropped = received(&Packet::dropped(3, 70_000).with_route(route("/1")));
        assert_eq!(
            proxy_rx_of(&dropped),
            ProxyRx::Dropped {
                stream_id: 3,
                packets: 70_000,
            }
        );
        assert_eq!(dropped.route(), route("/1"));

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

    /// Each reason a writer refuses for becomes the encode error that names
    /// it, with the numbers of what was asked. A payload too large for the
    /// buffer is a payload too large.
    #[test]
    fn a_writer_refusal_becomes_the_encode_error_with_its_numbers() {
        let root = DeviceRoute::root();
        let too_large = |actual| EncodeError::PayloadTooLarge {
            actual,
            maximum: WirePacket::MAX_PAYLOAD,
        };
        let over = vec![0; WirePacket::MAX_PAYLOAD + 1];
        let past_the_buffer = vec![0; WirePacket::MAX_SIZE];

        assert_eq!(
            Packet::rpc_request("x", &over, 0, root).unwrap_err(),
            too_large(5 + over.len())
        );
        assert_eq!(
            Packet::rpc_request("x", &past_the_buffer, 0, root).unwrap_err(),
            too_large(5 + past_the_buffer.len())
        );
        assert_eq!(
            Packet::rpc_reply(0, &over, root).unwrap_err(),
            too_large(2 + over.len())
        );
        assert_eq!(
            Packet::rpc_reply(0, &past_the_buffer, root).unwrap_err(),
            too_large(2 + past_the_buffer.len())
        );
        assert_eq!(
            Packet::proxy_error(0, wire_rpc::RpcError::Range, &"e".repeat(500)).unwrap_err(),
            too_large(505)
        );
        let long = "n".repeat(255);
        let record = wire::Metadata::Device(wire::Device {
            session: WireSessionId::new(1),
            n_streams: 1,
            name: &long,
            serial: &long,
            firmware: &long,
            protocol: 0,
        });
        assert_eq!(
            Packet::metadata(record, wire::MetadataFlags::UPDATE, root).unwrap_err(),
            too_large(wire::METADATA_HEADER_SIZE + record.record_len())
        );
        assert_eq!(
            Packet::samples(1, 0, 0, &over, root).unwrap_err(),
            too_large(4 + over.len())
        );
        assert_eq!(
            Packet::samples(1, 0, 0, &past_the_buffer, root).unwrap_err(),
            too_large(4 + past_the_buffer.len())
        );
        assert_eq!(
            Packet::samples(1, 0, 0, &[], root).unwrap_err(),
            EncodeError::NoSamples
        );
        assert_eq!(
            Packet::samples(1, 0, 1 << 24, &[0], root).unwrap_err(),
            EncodeError::SampleNumberTooLarge(1 << 24)
        );
        let name = "n".repeat(usize::from(wire_rpc::NAMELEN_MASK) + 1);
        let named_too_long = EncodeError::ValueTooLarge {
            value: name.len(),
            maximum: usize::from(wire_rpc::NAMELEN_MASK),
        };
        assert_eq!(
            Packet::rpc_request(&name, &[], 0, root).unwrap_err(),
            named_too_long
        );
        assert_eq!(
            Packet::proxy_request(&name, &[], 0).unwrap_err(),
            named_too_long
        );
    }
}

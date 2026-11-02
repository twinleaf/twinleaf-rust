//! The packet framing of each transport, without any I/O.
//!
//! A codec turns the bytes a link receives into packets, and packets into
//! the bytes the link sends. It opens no socket or serial port and keeps no
//! clock or thread. The port that uses it owns the buffers and does the I/O.
//!
//! - `tcp` frames packets back to back on a byte stream.
//! - `serial` frames each packet with SLIP and a CRC32, and passes on the
//!   text a device prints between frames.
//! - `udp` frames one packet per datagram.

#[cfg(feature = "serial")]
pub(crate) mod serial;
pub(crate) mod tcp;
pub(crate) mod udp;

use crate::tio::packet::{self, Packet};

/// Why received bytes did not decode as a packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    /// Bytes that are not a valid packet, with the reason.
    Invalid(packet::DecodeError),
    /// A frame that holds more bytes after its packet.
    TrailingBytes,
    /// A stream with no packet boundary left to find, after a header that
    /// did not decode.
    LostFraming(packet::DecodeError),
}

/// Decodes `raw`, a whole frame or datagram, as exactly one packet.
///
/// # Errors
///
/// Returns [`Error::TrailingBytes`] if the packet ends before `raw` does.
/// Returns [`Error::Invalid`] if `raw` does not start with a valid packet,
/// with [`PacketTooSmall`](packet::DecodeError::PacketTooSmall) if it ends
/// before the packet does.
fn decode_exact(raw: &[u8]) -> Result<Packet, Error> {
    match Packet::from_slice_prefix(raw) {
        Ok((packet, size)) if size == raw.len() => Ok(packet),
        Ok(_) => Err(Error::TrailingBytes),
        // A frame is a whole packet or nothing, so a short one is not a
        // packet still arriving.
        Err(packet::DecodeError::NeedMore) => {
            Err(Error::Invalid(packet::DecodeError::PacketTooSmall))
        }
        Err(error) => Err(Error::Invalid(error)),
    }
}

/// The packets, streams and cuts the codec properties draw from.
///
/// [`packet`] draws a packet of any kind a link carries, half from the typed
/// constructors and half as wire bytes from [`raw`], each with a TTL of 0 to
/// 15. [`invalid_body`] draws a packet whose header decodes and whose payload
/// is not valid for its type, [`invalid_header`] a header that does not
/// decode, and [`not_one_packet`] the bytes of a frame or datagram that are
/// not exactly one packet, each with the error it gives. Every draw of these
/// names its class, and [`tally`] prints how often a run draws each class.
#[cfg(test)]
pub(crate) mod strategy {
    use super::Error;
    use crate::proto::data::{MAX_SAMPLE_NUMBER, SAMPLE_HEADER_SIZE};
    use crate::proto::packet::Packet as WirePacket;
    use crate::proto::rpc::RpcError;
    use crate::proto::DeviceRoute;
    use crate::tio::packet::{DecodeError, LinkStatus, Packet};
    use proptest::prelude::*;
    use proptest::sample::{select, Index};
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;

    /// The largest payload, 500 bytes.
    const MAX: usize = WirePacket::MAX_PAYLOAD;

    /// The variable that makes [`tally`] print its counts.
    const COUNTING: &str = "TWINLEAF_CODEC_CLASSES";

    /// A route of up to eight hops.
    fn route() -> impl Strategy<Value = DeviceRoute> {
        hops(0..=DeviceRoute::MAX_HOPS)
    }

    /// A route of as many hops as `count` allows.
    fn hops(count: std::ops::RangeInclusive<usize>) -> impl Strategy<Value = DeviceRoute> {
        prop::collection::vec(any::<u8>(), count)
            .prop_map(|hops| DeviceRoute::from_hops(&hops).expect("eight hops at most"))
    }

    /// The routing bytes of a packet, none or eight as often as all between.
    fn routing() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            Just(0),
            Just(DeviceRoute::MAX_HOPS),
            0..=DeviceRoute::MAX_HOPS
        ]
        .prop_flat_map(|len| prop::collection::vec(any::<u8>(), len))
    }

    /// Bytes of any value, `len` of them.
    fn bytes(len: std::ops::RangeInclusive<usize>) -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), len)
    }

    /// Bytes of any value, `min` to `max` of them. Each end is drawn one time
    /// in five, and so is a length whose low byte is a line end, 10, 13, 266
    /// or 269, where the range holds one. A serial link reads a line end in
    /// a header as text.
    fn payload(min: usize, max: usize) -> impl Strategy<Value = Vec<u8>> {
        let line_ends: Vec<usize> = [10, 13, 266, 269]
            .into_iter()
            .filter(|len| (min..=max).contains(len))
            .collect();
        let line_end = if line_ends.is_empty() {
            vec![min]
        } else {
            line_ends
        };
        prop_oneof![
            1 => Just(min),
            1 => Just(max),
            1 => select(line_end),
            2 => min..=max,
        ]
        .prop_flat_map(|len| prop::collection::vec(any::<u8>(), len))
    }

    /// Samples of any stream, segment and first sample number.
    fn samples(
        data: std::ops::RangeInclusive<usize>,
        route: impl Strategy<Value = DeviceRoute>,
    ) -> impl Strategy<Value = Packet> {
        (
            1..=127u8,
            any::<u8>(),
            0..=MAX_SAMPLE_NUMBER,
            bytes(data),
            route,
        )
            .prop_filter_map("fits a packet", |(stream, segment, first, data, route)| {
                Packet::samples(stream, segment, first, &data, route).ok()
            })
    }

    /// A link status a proxy sends, known or not.
    fn status() -> impl Strategy<Value = LinkStatus> {
        any::<u8>().prop_map(LinkStatus::from)
    }

    /// An RPC error code.
    fn rpc_error() -> impl Strategy<Value = RpcError> {
        select(vec![
            RpcError::Undefined,
            RpcError::NotFound,
            RpcError::Timeout,
            RpcError::Busy,
        ])
    }

    /// A packet a link carries, of any kind and any size up to the largest,
    /// on any route and with any TTL. Half come from the typed constructors
    /// and half from [`raw`].
    pub(crate) fn packet() -> impl Strategy<Value = Packet> {
        prop_oneof![
            (typed(), 0..=15u8).prop_map(|(packet, ttl)| {
                let mut raw = packet.as_bytes().to_vec();
                raw[1] = (ttl << 4) | (raw[1] & 0x0f);
                wire(raw)
            }),
            raw().prop_map(|raw| wire(raw.bytes)),
        ]
    }

    /// The packet the valid wire bytes `raw` hold.
    pub(crate) fn wire(raw: Vec<u8>) -> Packet {
        Packet::from_wire(raw.into()).expect("a valid packet")
    }

    /// A packet from a typed constructor, of any size up to the largest, on
    /// any route. It is a heartbeat, samples, an RPC request, reply or error,
    /// a link status, a sign of life, or a control request, reply or error.
    /// One in eight is as large as a packet can be.
    fn typed() -> impl Strategy<Value = Packet> {
        prop_oneof![
            7 => any_typed(),
            1 => prop_oneof![
                (any::<u16>(), bytes(MAX - 2..=MAX - 2), hops(8..=8)).prop_map(
                    |(id, value, route)| {
                        Packet::rpc_reply(id, &value, route).expect("the largest reply fits")
                    }
                ),
                samples(MAX - SAMPLE_HEADER_SIZE..=MAX - SAMPLE_HEADER_SIZE, hops(8..=8)),
            ],
        ]
    }

    /// A packet of any kind [`typed`] gives, of any size.
    fn any_typed() -> impl Strategy<Value = Packet> {
        prop_oneof![
            (any::<u32>(), route())
                .prop_map(|(session, route)| Packet::heartbeat_session(session, route)),
            route().prop_map(Packet::heartbeat),
            samples(1..=MAX, route()),
            Just(Packet::alive()),
            status().prop_map(Packet::legacy_status),
            (status(), route()).prop_map(|(status, route)| Packet::link(status).with_route(route)),
            (any::<u16>(), rpc_error(), route())
                .prop_map(|(id, code, route)| Packet::rpc_error(id, code, route)),
            (any::<u16>(), bytes(0..=500)).prop_filter_map("fits a packet", |(id, value)| {
                Packet::proxy_reply(id, &value).ok()
            }),
            (any::<u16>(), rpc_error(), "[ -~]{0,40}")
                .prop_filter_map("fits a packet", |(id, code, message)| {
                    Packet::proxy_error(id, code, &message).ok()
                }),
            ("[a-z.]{1,24}", bytes(0..=63), any::<u16>())
                .prop_filter_map("fits a packet", |(name, arg, id)| {
                    Packet::proxy_request(&name, &arg, id).ok()
                }),
            ("[a-z.]{1,24}", bytes(0..=63), any::<u16>(), route())
                .prop_filter_map("fits a packet", |(name, arg, id, route)| {
                    Packet::rpc_request(&name, &arg, id, route).ok()
                }),
            (any::<u16>(), bytes(0..=500), route())
                .prop_filter_map("fits a packet", |(id, value, route)| {
                    Packet::rpc_reply(id, &value, route).ok()
                }),
        ]
    }

    /// The wire bytes of a packet of type `ptype` that carries `payload`,
    /// with `ttl` and the routing bytes `hops`. The header is the type, then
    /// the TTL above the routing size, then the payload size, little endian.
    pub(crate) fn assemble(ptype: u8, ttl: u8, payload: &[u8], hops: &[u8]) -> Vec<u8> {
        let size = u16::try_from(payload.len()).expect("a payload fits its size field");
        let mut raw = vec![ptype, (ttl << 4) | hops.len() as u8];
        raw.extend(size.to_le_bytes());
        raw.extend(payload);
        raw.extend(hops);
        raw
    }

    /// A packet built from its wire bytes, with its class.
    #[derive(Debug, Clone)]
    pub(crate) struct Raw {
        /// The header, the payload and the routing.
        pub(crate) bytes: Vec<u8>,
        /// What the type byte makes the payload.
        pub(crate) class: &'static str,
    }

    /// A packet of any valid type byte, built from the wire layout. It has
    /// any TTL, a route of 0 to 8 hops, and a payload valid for its type, of
    /// the shortest length the type allows up to 500 bytes. The types are
    /// those the host reads, the legacy types 6 to 8, SYNC and TEXT, the user
    /// types, stream 0, the samples of streams 1 to 127, and types no layout
    /// defines.
    pub(crate) fn raw() -> impl Strategy<Value = Raw> {
        (body(), 0..=15u8, routing()).prop_map(|((ptype, payload, class), ttl, hops)| Raw {
            bytes: assemble(ptype, ttl, &payload, &hops),
            class,
        })
    }

    /// A type byte, a payload valid for that type, and their class.
    fn body() -> impl Strategy<Value = (u8, Vec<u8>, &'static str)> {
        prop_oneof![
            1 => payload(5, MAX).prop_map(|p| (1, p, "log")),
            1 => request().prop_map(|p| (2, p, "rpc request")),
            1 => payload(2, MAX).prop_map(|p| (3, p, "rpc reply")),
            1 => payload(4, MAX).prop_map(|p| (4, p, "rpc error")),
            1 => prop_oneof![payload(4, 4), payload(8, 8), payload(0, MAX)]
                .prop_map(|p| (5, p, "heartbeat")),
            1 => (6..=8u8, payload(0, MAX)).prop_map(|(t, p)| (t, p, "legacy 6 to 8")),
            2 => metadata().prop_map(|p| (11, p, "metadata")),
            1 => setting().prop_map(|p| (12, p, "setting")),
            1 => payload(0, MAX).prop_map(|p| (62, p, "sync")),
            1 => payload(0, MAX).prop_map(|p| (63, p, "text")),
            1 => payload(1, MAX).prop_map(|p| (64, p, "legacy status")),
            1 => request().prop_map(|p| (66, p, "proxy request")),
            1 => proxy_rx().prop_map(|p| (67, p, "proxy reply")),
            1 => (prop_oneof![14..=61u8, Just(65), 68..=127u8], payload(0, MAX))
                .prop_map(|(t, p)| (t, p, "unknown type")),
            1 => payload(0, MAX).prop_map(|p| (128, p, "stream 0")),
            1 => (129..=255u8, payload(5, MAX)).prop_map(|(t, p)| (t, p, "samples")),
        ]
    }

    /// An RPC request payload, the request id and the method word, then the
    /// name if the word's top bit is set, then the arguments. The name is as
    /// long as the low 15 bits of the word say.
    fn request() -> impl Strategy<Value = Vec<u8>> {
        (
            any::<u16>(),
            any::<bool>(),
            payload(0, MAX - 4),
            any::<Index>(),
        )
            .prop_map(|(id, by_name, rest, split)| {
                let method = if by_name {
                    0x8000 | split.index(rest.len() + 1) as u16
                } else {
                    split.index(0x8000) as u16
                };
                [&id.to_le_bytes()[..], &method.to_le_bytes()[..], &rest[..]].concat()
            })
    }

    /// A setting payload, the name length and the flags, then the sequence
    /// number if the flags have bit 0, then the name and the value.
    fn setting() -> impl Strategy<Value = Vec<u8>> {
        (
            any::<u8>(),
            any::<u32>(),
            payload(0, 255),
            payload(0, MAX - 6 - 255),
        )
            .prop_map(|(flags, seq, name, value)| {
                let mut p = vec![name.len() as u8, flags];
                if flags & 1 != 0 {
                    p.extend(seq.to_le_bytes());
                }
                p.extend(name);
                p.extend(value);
                p
            })
    }

    /// A metadata payload, the record type and the flags, then one record.
    /// The record is a device record laid out as the wire format gives it,
    /// with a head of 9 bytes or longer and three strings, or a record of a
    /// type no layout defines, framed by the head length its first byte
    /// declares.
    fn metadata() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            (
                any::<u8>(),
                any::<[u8; 4]>(),
                any::<u8>(),
                payload(0, 4),
                "[ -~]{0,40}",
                "[ -~]{0,40}",
                "[ -~]{0,40}",
            )
                .prop_map(|(flags, session, streams, more, name, serial, firmware)| {
                    let mut p = vec![1, flags, 9 + more.len() as u8, name.len() as u8];
                    p.extend(session);
                    p.extend([serial.len() as u8, firmware.len() as u8, streams]);
                    p.extend(more);
                    p.extend(name.bytes().chain(serial.bytes()).chain(firmware.bytes()));
                    p
                }),
            (
                prop_oneof![Just(0u8), 5..=255u8],
                any::<u8>(),
                payload(1, 254),
                payload(0, MAX - 2 - 255),
            )
                .prop_map(|(kind, flags, head, strings)| {
                    let mut p = vec![kind, flags, head.len() as u8 + 1];
                    p.extend(head);
                    p.extend(strings);
                    p
                }),
        ]
    }

    /// A proxy reply payload, a kind byte then that kind's body. Kind 0 holds
    /// a reply, 1 an error, 2 a link status and 3 nothing, and any other
    /// kind holds any bytes.
    fn proxy_rx() -> impl Strategy<Value = Vec<u8>> {
        let kind = |kind: u8, body: Vec<u8>| [vec![kind], body].concat();
        prop_oneof![
            payload(2, MAX - 1).prop_map(move |b| kind(0, b)),
            payload(4, MAX - 1).prop_map(move |b| kind(1, b)),
            payload(1, MAX - 1).prop_map(move |b| kind(2, b)),
            payload(0, MAX - 1).prop_map(move |b| kind(3, b)),
            (4..=255u8, payload(0, MAX - 1)).prop_map(move |(k, b)| kind(k, b)),
        ]
    }

    /// A packet whose header decodes and whose payload is not valid for its
    /// type, with the error that gives and its class.
    #[derive(Debug, Clone)]
    pub(crate) struct Invalid {
        /// The header, the payload and the routing.
        pub(crate) bytes: Vec<u8>,
        /// The error the payload gives.
        pub(crate) error: DecodeError,
        /// What is wrong with the payload.
        pub(crate) class: &'static str,
    }

    /// A packet whose header decodes and whose payload is not valid for its
    /// type, on any route and with any TTL. Each type whose layout has a
    /// shortest payload is drawn shorter, from empty to one byte less. The
    /// RPC request, setting and metadata layouts are also drawn with a name
    /// or a record that runs past the payload, and a metadata record with a
    /// string that is not UTF8.
    pub(crate) fn invalid_body() -> impl Strategy<Value = Invalid> {
        (bad_body(), 0..=15u8, routing()).prop_map(|((ptype, payload, error, class), ttl, hops)| {
            Invalid {
                bytes: assemble(ptype, ttl, &payload, &hops),
                error,
                class,
            }
        })
    }

    /// A type byte, a payload not valid for it, the error and the class.
    fn bad_body() -> impl Strategy<Value = (u8, Vec<u8>, DecodeError, &'static str)> {
        use DecodeError::{InvalidPayload, PayloadTooShort};
        prop_oneof![
            payload(0, 4).prop_map(|p| (1, p, PayloadTooShort, "log short")),
            (select(vec![2u8, 66]), payload(0, 3)).prop_map(|(t, p)| (
                t,
                p,
                PayloadTooShort,
                "request short"
            )),
            (
                select(vec![2u8, 66]),
                any::<u16>(),
                payload(0, MAX - 4),
                any::<Index>()
            )
                .prop_map(|(t, id, rest, over)| {
                    let name = rest.len() + 1 + over.index(0x8000 - rest.len() - 1);
                    let method = 0x8000 | name as u16;
                    let p = [&id.to_le_bytes()[..], &method.to_le_bytes()[..], &rest[..]].concat();
                    (t, p, PayloadTooShort, "request name overruns")
                }),
            payload(0, 1).prop_map(|p| (3, p, PayloadTooShort, "reply short")),
            payload(0, 3).prop_map(|p| (4, p, PayloadTooShort, "error short")),
            setting_short().prop_map(|p| (12, p, PayloadTooShort, "setting short")),
            Just(vec![]).prop_map(|p| (64, p, PayloadTooShort, "legacy status empty")),
            proxy_rx_short().prop_map(|p| (67, p, PayloadTooShort, "proxy reply short")),
            payload(0, 1).prop_map(|p| (11, p, PayloadTooShort, "metadata short")),
            unframed().prop_map(|p| (11, p, InvalidPayload, "metadata record unframed")),
            device_overrun().prop_map(|p| (11, p, InvalidPayload, "metadata strings invalid")),
            (129..=255u8, payload(0, 4)).prop_map(|(t, p)| (
                t,
                p,
                PayloadTooShort,
                "samples short"
            )),
        ]
    }

    /// A setting payload cut short. It is under the two head bytes, or
    /// numbered with a sequence number cut short, or its name runs past the
    /// payload.
    fn setting_short() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            payload(0, 1),
            (any::<u8>(), any::<u8>(), payload(0, 3)).prop_map(|(name, flags, rest)| [
                vec![name, flags | 1],
                rest
            ]
            .concat()),
            (any::<u8>(), any::<u32>(), payload(0, 254), any::<Index>()).prop_map(
                |(flags, seq, rest, over)| {
                    let name = rest.len() + 1 + over.index(255 - rest.len());
                    let mut p = vec![name as u8, flags];
                    if flags & 1 != 0 {
                        p.extend(seq.to_le_bytes());
                    }
                    p.extend(rest);
                    p
                }
            ),
        ]
    }

    /// A proxy reply payload cut short. It is empty, or a reply under its
    /// request id, an error under its id and code, or a link status with no
    /// status byte.
    fn proxy_rx_short() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            Just(vec![]),
            payload(0, 1).prop_map(|b| [vec![0], b].concat()),
            payload(0, 3).prop_map(|b| [vec![1], b].concat()),
            Just(vec![2]),
        ]
    }

    /// A metadata payload of any record type whose record is not framed. It
    /// is empty, or declares a head under two bytes, or declares a head
    /// longer than the record.
    fn unframed() -> impl Strategy<Value = Vec<u8>> {
        let record = prop_oneof![
            Just(vec![]),
            (0..=1u8, payload(0, MAX - 3)).prop_map(|(head, rest)| [vec![head], rest].concat()),
            (2..=255u8).prop_flat_map(|head| {
                payload(0, usize::from(head) - 2).prop_map(move |rest| [vec![head], rest].concat())
            }),
        ];
        (any::<u8>(), any::<u8>(), record)
            .prop_map(|(kind, flags, record)| [vec![kind, flags], record].concat())
    }

    /// A device record payload whose strings run past the record, or whose
    /// name is not UTF8.
    fn device_overrun() -> impl Strategy<Value = Vec<u8>> {
        let head =
            |flags: u8, lens: [u8; 3]| vec![1, flags, 9, lens[0], 0, 0, 0, 0, lens[1], lens[2], 0];
        prop_oneof![
            (any::<u8>(), any::<[u8; 3]>(), any::<Index>())
                .prop_filter("names a string", |(_, lens, _)| lens != &[0; 3])
                .prop_map(move |(flags, lens, cut)| {
                    let total: usize = lens.iter().map(|&len| usize::from(len)).sum();
                    let strings = vec![b'a'; cut.index(total.min(MAX - 11 + 1))];
                    [head(flags, lens), strings].concat()
                }),
            (any::<u8>(), 1..=40u8).prop_map(move |(flags, len)| {
                [head(flags, [len, 0, 0]), vec![0xff; usize::from(len)]].concat()
            }),
        ]
    }

    /// A header that does not decode, of one to four bytes, with the error
    /// it gives and its class. A type byte of 0, 9, 10 or 13 fails alone,
    /// with any bytes after it. A valid type fails with a routing size over 8
    /// and a payload size of at most 500, or a payload size over 500 and a
    /// routing size of at most 8.
    pub(crate) fn invalid_header() -> impl Strategy<Value = (Vec<u8>, DecodeError, &'static str)> {
        let valid = any::<u8>().prop_filter("a valid type", |t| !matches!(t, 0 | 9 | 10 | 13));
        prop_oneof![
            (select(vec![0u8, 9, 10, 13]), payload(0, 3)).prop_map(|(ptype, rest)| (
                [vec![ptype], rest].concat(),
                DecodeError::InvalidPacketType,
                "invalid type"
            )),
            (valid.clone(), 0..16u8, 9..16u8, 0..=500u16).prop_map(|(ptype, ttl, hops, size)| {
                let [low, high] = size.to_le_bytes();
                (
                    vec![ptype, (ttl << 4) | hops, low, high],
                    DecodeError::RoutingTooBig,
                    "routing too big",
                )
            }),
            (
                valid,
                0..16u8,
                0..=8u8,
                prop_oneof![Just(501), 501..=u16::MAX]
            )
                .prop_map(|(ptype, ttl, hops, size)| {
                    let [low, high] = size.to_le_bytes();
                    (
                        vec![ptype, (ttl << 4) | hops, low, high],
                        DecodeError::PayloadTooBig,
                        "payload too big",
                    )
                }),
        ]
    }

    /// The bytes of a frame or a datagram that are not exactly one packet,
    /// at most 512 of them, with the error they give and their class. They
    /// are a packet with bytes after it, a packet cut short anywhere from
    /// empty to one byte less, a header that does not decode, or a packet
    /// whose payload is not valid for its type.
    pub(crate) fn not_one_packet() -> impl Strategy<Value = (Vec<u8>, Error, &'static str)> {
        let largest = WirePacket::MAX_SIZE;
        prop_oneof![
            (raw(), payload(1, largest))
                .prop_filter("leaves room after it", move |(raw, _)| raw.bytes.len()
                    < largest)
                .prop_map(move |(raw, mut more)| {
                    more.truncate(largest - raw.bytes.len());
                    let bytes = [raw.bytes, more].concat();
                    (bytes, Error::TrailingBytes, "bytes after the packet")
                }),
            (raw(), prop::option::of(any::<Index>())).prop_map(|(raw, cut)| {
                let len = cut.map_or(0, |cut| cut.index(raw.bytes.len()));
                let error = Error::Invalid(DecodeError::PacketTooSmall);
                (raw.bytes[..len].to_vec(), error, "cut short")
            }),
            invalid_header().prop_map(|(bytes, error, _)| (
                bytes,
                Error::Invalid(error),
                "invalid header"
            )),
            invalid_body().prop_map(|invalid| (
                invalid.bytes,
                Error::Invalid(invalid.error),
                "invalid body"
            )),
        ]
    }

    /// `wire` cut at each of `cuts`, in order, as the reads that deliver it.
    pub(crate) fn pieces<'a>(wire: &'a [u8], cuts: &[Index]) -> Vec<&'a [u8]> {
        let mut at: Vec<usize> = cuts.iter().map(|cut| cut.index(wire.len() + 1)).collect();
        at.sort_unstable();
        let ends = at.iter().copied().chain([wire.len()]);
        let starts = [0].into_iter().chain(at.iter().copied());
        starts
            .zip(ends)
            .map(|(start, end)| &wire[start..end])
            .collect()
    }

    /// Draws a run of cases from `strategy` and prints the share of cases in
    /// each class that `classes` names, when the variable
    /// `TWINLEAF_CODEC_CLASSES` is set. Run it with `--nocapture` to see the
    /// counts.
    pub(crate) fn tally<S>(
        title: &str,
        strategy: S,
        classes: impl Fn(&S::Value) -> Vec<&'static str>,
    ) where
        S: Strategy,
        S::Value: std::fmt::Debug,
    {
        if std::env::var_os(COUNTING).is_none() {
            return;
        }
        let counts = RefCell::new(BTreeMap::<&'static str, usize>::new());
        let cases = Cell::new(0usize);
        proptest!(|(value in strategy)| {
            cases.set(cases.get() + 1);
            for class in classes(&value) {
                *counts.borrow_mut().entry(class).or_default() += 1;
            }
        });
        println!("{title}, {} cases", cases.get());
        for (class, count) in counts.into_inner() {
            let share = 100.0 * count as f64 / cases.get() as f64;
            println!("  {class:<32} {count:>6} {share:>6.1}%");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::strategy;

    /// The TTL, routing and payload size classes of the packet `raw`.
    fn header(raw: &[u8]) -> Vec<&'static str> {
        let ttl = match raw[1] >> 4 {
            0..=1 => "TTL 0 or 1",
            2..=7 => "TTL 2 to 7",
            _ => "TTL 8 to 15",
        };
        let hops = match raw[1] & 0x0f {
            0 => "no hops",
            8 => "8 hops",
            _ => "1 to 7 hops",
        };
        let payload = match u16::from_le_bytes([raw[2], raw[3]]) {
            0 => "empty payload",
            500 => "500 byte payload",
            _ => "payload of 1 to 499 bytes",
        };
        vec![ttl, hops, payload]
    }

    /// Prints the share of each class the shared strategies draw, when
    /// `TWINLEAF_CODEC_CLASSES` is set.
    #[test]
    fn class_counts() {
        strategy::tally("raw packets by type", strategy::raw(), |raw| {
            vec![raw.class]
        });
        strategy::tally("packets by header", strategy::packet(), |packet| {
            header(packet.as_bytes())
        });
        strategy::tally("invalid payloads", strategy::invalid_body(), |invalid| {
            let mut classes = vec![invalid.class];
            classes.extend(header(&invalid.bytes));
            classes
        });
        strategy::tally(
            "invalid headers",
            strategy::invalid_header(),
            |(bytes, _, class)| {
                vec![
                    *class,
                    ["1 byte", "2 bytes", "3 bytes", "4 bytes"][bytes.len() - 1],
                ]
            },
        );
        strategy::tally(
            "not one packet",
            strategy::not_one_packet(),
            |(_, _, class)| vec![*class],
        );
    }
}

//! The TCP framing.
//!
//! TIO packets go on the stream unchanged, back to back. The decoder splits
//! the stream into packets by the size each header gives.

use super::Error;
use crate::proto::packet::Header;
use crate::tio::packet::{self, Packet};
use bytes::{Buf, BytesMut};

/// A codec of packets back to back on a byte stream.
#[derive(Debug, Default)]
pub(crate) struct Codec {
    /// Set once the stream holds bytes no header explains.
    lost_framing: Option<packet::DecodeError>,
}

impl Codec {
    /// Decodes the packet at the start of `src` and removes its bytes.
    /// Returns `None` while `src` holds less than a whole packet.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] for a packet whose header decodes but
    /// whose body does not, and removes that packet. Returns
    /// [`Error::LostFraming`] for a header that does not decode, and leaves
    /// `src` as it is. Every later call returns the same terminal error.
    pub(crate) fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Packet>, Error> {
        if let Some(error) = self.lost_framing {
            return Err(Error::LostFraming(error));
        }
        match Packet::from_slice_prefix(src) {
            Ok((packet, size)) => {
                src.advance(size);
                Ok(Some(packet))
            }
            Err(packet::DecodeError::NeedMore) => Ok(None),
            Err(error) => {
                // A header that parses says where its packet ends, so only that
                // packet is skipped. One that does not leaves no boundary to
                // find, so decoding stops once this error is reported.
                match Header::parse_prefix(src) {
                    Ok(header) => {
                        src.advance(header.packet_len());
                        Err(Error::Invalid(error))
                    }
                    Err(_) => {
                        self.lost_framing = Some(error);
                        Err(Error::LostFraming(error))
                    }
                }
            }
        }
    }

    /// Appends the wire bytes of `packet` to `dst`.
    pub(crate) fn encode(&mut self, packet: &Packet, dst: &mut BytesMut) {
        dst.extend_from_slice(packet.as_bytes());
    }
}

impl tokio_util::codec::Decoder for Codec {
    type Item = super::super::TransportEvent;
    type Error = super::super::RecvError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Codec::decode(self, src) {
            Ok(packet) => Ok(packet.map(Self::Item::Packet)),
            Err(error) => super::super::rejected_frame(error).map(Some),
        }
    }
}

impl tokio_util::codec::Encoder<Packet> for Codec {
    type Error = super::super::RecvError;

    fn encode(&mut self, packet: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Codec::encode(self, &packet, dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Properties of the TCP framing.
    //!
    //! # Observations
    //!
    //! What each call of [`Codec::decode`] returns, a packet, nothing yet, an
    //! invalid packet or the lost framing, and the bytes it leaves in `src`.
    //!
    //! # Sources
    //!
    //! The module doc and the doc of [`Codec::decode`]. The header layout in
    //! the [`proto::packet`](crate::proto::packet) module doc, with the type
    //! bytes [`PacketType::try_new`](crate::proto::packet::PacketType::try_new)
    //! refuses and the limits [`PacketError`](crate::proto::packet::PacketError)
    //! names. The payload layouts in the module docs of the
    //! [`proto`](crate::proto) payloads, and the errors
    //! [`DecodeError`](packet::DecodeError) names.
    //!
    //! # Model
    //!
    //! `H(s)` reads the header at the start of the stream `s` from the wire
    //! format alone, in [`head`]. It waits while `s` is empty, or starts with
    //! a valid type byte and holds fewer than four bytes. It fails on a type
    //! byte of 0, 9, 10 or 13, on a routing size over 8, or on a payload size
    //! over 500. The docs do not order the last two, so when both fail it may
    //! name either. Otherwise it gives the packet's length, four bytes of
    //! header, then the payload, then the routing.
    //!
    //! `B(f)` says whether the payload of a whole packet `f` is valid for its
    //! type, in [`body`], by the shortest payload each layout documents, the
    //! name an RPC request or a setting declares, and the framing of a
    //! metadata record. It names the error when the payload is not valid.
    //!
    //! `D(s)` is what a fresh codec yields for `s`, up to the first `None` or
    //! lost framing. [`walk`] gives `D(s)` as the model says it.
    //!
    //! # Laws
    //!
    //! - T1, inductive base. `D(s)` is empty when `H(s)` waits, or gives a
    //!   length longer than `s`. Source, the doc of `decode`, which returns
    //!   `None` while `src` holds less than a whole packet. It rules out a
    //!   codec that decodes a packet before its last routing byte arrives.
    //! - T2, inductive step and round trip. `D(p ++ s)` is `p` then `D(s)` for
    //!   any packet `p` and any bytes `s`. Source, the module doc, packets go
    //!   on the stream unchanged and back to back. It rules out a codec that
    //!   clears the TTL, or refuses a type it does not read instead of passing
    //!   it on whole.
    //! - T3, inductive step. When `H(f)` gives the length of `f` and `B(f)`
    //!   names the error `e`, `D(f ++ s)` is `Invalid(e)` then `D(s)`. Source,
    //!   the doc of `decode`, which reports and removes that packet. It rules
    //!   out a codec that skips the header and routing of the invalid packet
    //!   but not its payload, or that names one error for every payload.
    //! - T4, absorbing state. When `H(s)` fails with `e`, `D(s)` is
    //!   `LostFraming(e)`. Every later call returns the same error and leaves
    //!   `src` as it is, as more bytes arrive. One invalid type byte is enough.
    //!   Source, the doc of `decode`. It rules out a codec that clears `src`
    //!   on the lost framing, decodes the bytes that arrive after it, or waits
    //!   for a whole header before refusing its type byte.
    //! - T5, completeness. At every step a stream meets exactly one of T1 to
    //!   T4, so `D(s)` is what [`walk`] gives for any `s`. It is checked on
    //!   arbitrary bytes and on streams built from packets, invalid packets,
    //!   invalid headers and noise, cut short and with one byte changed, and
    //!   each step is labelled with its law. It rules out any behaviour
    //!   outside the four cases, such as a codec that searches for the next
    //!   header after one that does not decode.
    //! - T6, metamorphic. For any `s` and any cuts, feeding the pieces in
    //!   turn yields `D(s)`, and so does feeding one byte at a time. Source,
    //!   the module doc, TCP is a byte stream. It rules out a codec that
    //!   loses a header split across reads.
    //! - T7, postcondition and progress. `None` and the lost framing remove
    //!   nothing. A packet or an invalid packet removes exactly its length.
    //!   Source, the doc of `decode`. It rules out a codec that removes only
    //!   the header, so that a decode loop spins or reads a payload as a
    //!   header.
    //! - T8, implicit oracle. No input panics.
    //!
    //! # Generator obligations
    //!
    //! - Packets of each class [`strategy::raw`] names, built from wire bytes,
    //!   and from the typed constructors, with a TTL of 0 to 15, routes of 0
    //!   and 8 hops, and payloads of 0 and 500 bytes.
    //! - Invalid payloads of each class [`strategy::invalid_body`] names,
    //!   every type whose layout has a shortest payload, from empty to one
    //!   byte short, and longer payloads whose name or record runs past them.
    //! - Invalid headers of one to four bytes, with each of the three errors.
    //! - Streams whose steps reach every label [`labels`] gives, T1 at an
    //!   empty stream, in a header and in a body, T2, T3 with each error, and
    //!   T4 with each error and with both.
    //! - Cuts in a header, in a body and at a packet boundary, which random
    //!   cuts and the byte at a time feed reach.
    //!
    //! `class_counts` prints the share of each class with
    //! `TWINLEAF_CODEC_CLASSES` set.
    //!
    //! # Exclusions
    //!
    //! Whether a metadata record of a type with a defined layout, 1 to 4,
    //! parses is left to the metadata record properties of `twinleaf-proto`.
    //! The generator builds valid device records, type 1, and no valid
    //! records of types 2 to 4. For a record of a defined type whose framing
    //! holds, the model takes the packet or `InvalidPayload`. The bytes
    //! [`Codec::encode`] appends are the packet's own, which T2 covers.
    //!
    //! # Examples that stay
    //!
    //! None. The wire bytes of each payload are pinned in the payload suites.

    use super::*;
    use crate::tio::transport::codec::strategy;
    use proptest::prelude::*;
    use proptest::sample::Index;

    use packet::DecodeError;

    /// The bytes of `packets` back to back, as the codec encodes them.
    fn wire(packets: &[Packet]) -> Vec<u8> {
        let mut codec = Codec::default();
        let mut wire = BytesMut::new();
        for packet in packets {
            codec.encode(packet, &mut wire);
        }
        wire.to_vec()
    }

    /// What decoding `packets` gives, each packet as its bytes.
    fn decoded(packets: &[Packet]) -> Vec<Result<Vec<u8>, Error>> {
        packets.iter().map(|p| Ok(p.as_bytes().to_vec())).collect()
    }

    /// What one codec decodes from `pieces` fed in turn, packets as their
    /// bytes. Stops at [`Error::LostFraming`].
    fn decode_pieces<'a>(
        pieces: impl IntoIterator<Item = &'a [u8]>,
    ) -> Vec<Result<Vec<u8>, Error>> {
        let mut codec = Codec::default();
        let mut src = BytesMut::new();
        let mut decoded = Vec::new();
        for piece in pieces {
            src.extend_from_slice(piece);
            while let Some(result) = codec.decode(&mut src).transpose() {
                let lost = matches!(result, Err(Error::LostFraming(_)));
                decoded.push(result.map(|p| p.as_bytes().to_vec()));
                if lost {
                    return decoded;
                }
            }
        }
        decoded
    }

    /// What `H` makes of the header at the start of a stream.
    #[derive(Debug)]
    enum Head {
        /// Too few bytes to tell.
        Waits,
        /// A header that does not decode, with each error it may name.
        Fails(Vec<DecodeError>),
        /// A header that decodes, with the length of its packet.
        Frames(usize),
    }

    /// `H(s)`, the header at the start of `s`, read from the wire format.
    fn head(s: &[u8]) -> Head {
        let Some(&ptype) = s.first() else {
            return Head::Waits;
        };
        if matches!(ptype, 0 | 9 | 10 | 13) {
            return Head::Fails(vec![DecodeError::InvalidPacketType]);
        }
        let [_, routing, low, high, ..] = *s else {
            return Head::Waits;
        };
        let hops = usize::from(routing & 0x0f);
        let size = usize::from(u16::from_le_bytes([low, high]));
        let mut errors = Vec::new();
        if hops > 8 {
            errors.push(DecodeError::RoutingTooBig);
        }
        if size > 500 {
            errors.push(DecodeError::PayloadTooBig);
        }
        if errors.is_empty() {
            Head::Frames(4 + size + hops)
        } else {
            Head::Fails(errors)
        }
    }

    /// What `B` makes of the payload of a whole packet.
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Body {
        /// Valid for its type.
        Valid,
        /// Not valid for its type, with the error it gives.
        Invalid(DecodeError),
        /// A framed metadata record of a type with a defined layout, whose
        /// parse the payload suites check. It is a packet or
        /// `InvalidPayload`.
        Record,
    }

    /// `B(f)`, the payload of the whole packet `f` against its type's
    /// layout.
    fn body(f: &[u8]) -> Body {
        let short = Body::Invalid(DecodeError::PayloadTooShort);
        let size = usize::from(u16::from_le_bytes([f[2], f[3]]));
        let payload = &f[4..4 + size];
        let at_least = |len: usize| {
            if payload.len() < len {
                short
            } else {
                Body::Valid
            }
        };
        match f[0] {
            // LOG, the data word and the level.
            1 => at_least(5),
            // RPC_REQ and PROXY_TX, the id, the method word and any name.
            2 | 66 => match payload {
                [_, _, low, high, rest @ ..] => {
                    let method = u16::from_le_bytes([*low, *high]);
                    let name = if method & 0x8000 == 0 {
                        0
                    } else {
                        method & 0x7fff
                    };
                    if rest.len() < usize::from(name) {
                        short
                    } else {
                        Body::Valid
                    }
                }
                _ => short,
            },
            // RPC_REP, the id.
            3 => at_least(2),
            // RPC_ERROR, the id and the code.
            4 => at_least(4),
            11 => metadata(payload),
            // SETTING, the name length, the flags, any sequence and the name.
            12 => match payload {
                [name, flags, rest @ ..] => {
                    let seq = if flags & 1 == 0 { 0 } else { 4 };
                    if rest.len() < seq + usize::from(*name) {
                        short
                    } else {
                        Body::Valid
                    }
                }
                _ => short,
            },
            // The legacy link status, its code.
            64 => at_least(1),
            // PROXY_RX, the kind and the head of that kind's body.
            67 => match payload {
                [0, rest @ ..] if rest.len() < 2 => short,
                [1, rest @ ..] if rest.len() < 4 => short,
                [2] | [] => short,
                _ => Body::Valid,
            },
            // Samples of streams 1 to 127, the sample header and a byte.
            129.. => at_least(5),
            _ => Body::Valid,
        }
    }

    /// `B` of a metadata payload, the record type and flags, then a record
    /// framed by the head length its first byte declares, at least two.
    fn metadata(payload: &[u8]) -> Body {
        let [kind, _, record @ ..] = payload else {
            return Body::Invalid(DecodeError::PayloadTooShort);
        };
        let framed = record
            .first()
            .is_some_and(|&head| head >= 2 && record.len() >= usize::from(head));
        match (framed, (1..=4).contains(kind)) {
            (false, _) => Body::Invalid(DecodeError::InvalidPayload),
            (true, true) => Body::Record,
            (true, false) => Body::Valid,
        }
    }

    /// One step of `D(s)` as the model walks the stream.
    #[derive(Debug)]
    enum Step<'a> {
        /// T1, nothing yet, with the bytes left.
        Waits(&'a [u8]),
        /// T2 or T3, a whole packet and what its payload is.
        Frame(&'a [u8], Body),
        /// T4, the lost framing, with the bytes left and the errors it may
        /// name.
        Lost(&'a [u8], Vec<DecodeError>),
    }

    /// `D(s)` as the model walks `stream`, to the first wait or lost
    /// framing.
    fn walk(stream: &[u8]) -> Vec<Step<'_>> {
        let mut steps = Vec::new();
        let mut rest = stream;
        loop {
            match head(rest) {
                Head::Frames(len) if len <= rest.len() => {
                    let (frame, after) = rest.split_at(len);
                    steps.push(Step::Frame(frame, body(frame)));
                    rest = after;
                }
                Head::Frames(_) | Head::Waits => {
                    steps.push(Step::Waits(rest));
                    return steps;
                }
                Head::Fails(errors) => {
                    steps.push(Step::Lost(rest, errors));
                    return steps;
                }
            }
        }
    }

    /// The law and case of each step of `stream`.
    fn labels(stream: &[u8]) -> Vec<&'static str> {
        let label = |step: &Step| match step {
            Step::Waits([]) => "T1 empty",
            Step::Waits(rest) if matches!(head(rest), Head::Waits) => "T1 in a header",
            Step::Waits(_) => "T1 in a body",
            Step::Frame(_, Body::Valid) => "T2 packet",
            Step::Frame(_, Body::Record) => "T2 or T3 metadata record",
            Step::Frame(_, Body::Invalid(DecodeError::PayloadTooShort)) => "T3 payload too short",
            Step::Frame(_, Body::Invalid(_)) => "T3 invalid payload",
            Step::Lost(_, errors) if errors.len() > 1 => "T4 routing and payload too big",
            Step::Lost(_, errors) => match errors[0] {
                DecodeError::InvalidPacketType => "T4 invalid type",
                DecodeError::RoutingTooBig => "T4 routing too big",
                _ => "T4 payload too big",
            },
        };
        walk(stream).iter().map(label).collect()
    }

    /// Checks one codec against [`walk`] on `stream` held in one buffer.
    /// Each call returns what its step says (T1 to T5) and leaves the bytes
    /// its step leaves (T7). A lost framing then sees `arrival` arrive, and
    /// stays lost with `src` as it is (T4).
    fn check(stream: &[u8], arrival: &[u8]) -> Result<(), TestCaseError> {
        let mut codec = Codec::default();
        let mut src = BytesMut::from(stream);
        let mut at = 0;
        for step in walk(stream) {
            let result = codec.decode(&mut src);
            match step {
                Step::Waits(rest) => {
                    prop_assert!(matches!(result, Ok(None)), "{:?}", result);
                    prop_assert_eq!(&src[..], rest);
                }
                Step::Frame(frame, body) => {
                    match (body, &result) {
                        (Body::Valid | Body::Record, Ok(Some(packet))) => {
                            prop_assert_eq!(packet.as_bytes(), frame);
                        }
                        (Body::Invalid(error), Err(Error::Invalid(actual))) => {
                            prop_assert_eq!(*actual, error);
                        }
                        (Body::Record, Err(Error::Invalid(DecodeError::InvalidPayload))) => {}
                        _ => prop_assert!(false, "{:?} decoded as {:?}", body, result),
                    }
                    at += frame.len();
                    prop_assert_eq!(&src[..], &stream[at..]);
                }
                Step::Lost(rest, errors) => {
                    let Err(Error::LostFraming(error)) = result else {
                        return Err(TestCaseError::fail(format!("{result:?}, not lost")));
                    };
                    prop_assert!(errors.contains(&error), "{:?} of {:?}", error, errors);
                    prop_assert_eq!(&src[..], rest);
                    src.extend_from_slice(arrival);
                    prop_assert_eq!(
                        codec.decode(&mut src).err(),
                        Some(Error::LostFraming(error))
                    );
                    prop_assert_eq!(&src[..], &[rest, arrival].concat()[..]);
                }
            }
        }
        Ok(())
    }

    /// A stream to decode. It is arbitrary bytes, or a run of packets,
    /// invalid packets, invalid headers and noise, which may be cut short and
    /// may have one byte changed.
    fn stream() -> impl Strategy<Value = Vec<u8>> {
        let part = prop_oneof![
            4 => strategy::raw().prop_map(|raw| raw.bytes),
            3 => strategy::invalid_body().prop_map(|invalid| invalid.bytes),
            1 => strategy::invalid_header().prop_map(|(header, _, _)| header),
            1 => prop::collection::vec(any::<u8>(), 1..8),
        ];
        prop_oneof![
            1 => prop::collection::vec(any::<u8>(), 0..=1100),
            3 => (
                prop::collection::vec(part, 0..6),
                prop::option::of(any::<Index>()),
                prop::option::of((any::<Index>(), any::<u8>())),
            )
                .prop_map(|(parts, cut, change)| {
                    let mut stream = parts.concat();
                    if let Some(cut) = cut {
                        stream.truncate(cut.index(stream.len() + 1));
                    }
                    if let (Some((at, value)), false) = (change, stream.is_empty()) {
                        let at = at.index(stream.len());
                        stream[at] = value;
                    }
                    stream
                }),
        ]
    }

    proptest! {
        /// T2 and T6. Any packets encoded back to back decode unchanged,
        /// however the stream is cut into reads, and when it arrives a byte
        /// at a time.
        #[test]
        fn encoded_packets_decode_unchanged_however_the_stream_is_cut(
            packets in prop::collection::vec(strategy::packet(), 0..8),
            cuts in prop::collection::vec(any::<Index>(), 0..16),
        ) {
            let wire = wire(&packets);
            prop_assert_eq!(decode_pieces(strategy::pieces(&wire, &cuts)), decoded(&packets));
            prop_assert_eq!(decode_pieces(wire.chunks(1)), decoded(&packets));
        }

        /// T2 and T7. Each call decodes the next packet and removes exactly
        /// its bytes.
        #[test]
        fn each_call_decodes_one_packet_and_leaves_the_rest(
            packets in prop::collection::vec(strategy::packet(), 0..8),
        ) {
            let mut src = BytesMut::from(&wire(&packets)[..]);
            let mut codec = Codec::default();
            for (i, packet) in packets.iter().enumerate() {
                let decoded = codec.decode(&mut src).unwrap();
                prop_assert_eq!(decoded.as_ref().map(Packet::as_bytes), Some(packet.as_bytes()));
                prop_assert_eq!(&src[..], &wire(&packets[i + 1..])[..]);
            }
            prop_assert!(codec.decode(&mut src).unwrap().is_none());
        }

        /// T3 and T7. A packet whose header decodes and whose payload is not
        /// valid for its type gives that payload's error, and the call
        /// removes exactly the packet. The packets on either side decode
        /// unchanged, however the stream is cut.
        #[test]
        fn a_packet_whose_payload_is_invalid_is_skipped_alone(
            before in prop::collection::vec(strategy::packet(), 0..4),
            invalid in strategy::invalid_body(),
            after in prop::collection::vec(strategy::packet(), 0..4),
            cuts in prop::collection::vec(any::<Index>(), 0..16),
        ) {
            let mut stream = wire(&before);
            stream.extend(&invalid.bytes);
            stream.extend(wire(&after));
            let mut expected = decoded(&before);
            expected.push(Err(Error::Invalid(invalid.error)));
            expected.extend(decoded(&after));
            prop_assert_eq!(decode_pieces(strategy::pieces(&stream, &cuts)), expected);

            let mut codec = Codec::default();
            let mut src = BytesMut::from(&[&invalid.bytes[..], &wire(&after)].concat()[..]);
            prop_assert_eq!(codec.decode(&mut src).err(), Some(Error::Invalid(invalid.error)));
            prop_assert_eq!(&src[..], &wire(&after)[..]);
        }

        /// T4. A header that does not decode, from a lone invalid type byte
        /// to four header bytes, leaves no packet boundary to find. The
        /// packets before it decode, then the codec reports the lost framing
        /// and decodes nothing more. Every later call repeats the error and
        /// leaves `src` as it is while more bytes arrive.
        #[test]
        fn a_header_that_does_not_decode_loses_the_framing(
            before in prop::collection::vec(strategy::packet(), 0..4),
            (header, error, _) in strategy::invalid_header(),
            after in prop_oneof![Just(vec![]), prop::collection::vec(any::<u8>(), 1..64)],
            arrivals in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 1..4),
            cuts in prop::collection::vec(any::<Index>(), 0..16),
        ) {
            let mut stream = wire(&before);
            stream.extend(&header);
            stream.extend(&after);
            let mut expected = decoded(&before);
            expected.push(Err(Error::LostFraming(error)));
            prop_assert_eq!(decode_pieces(strategy::pieces(&stream, &cuts)), expected);

            let mut codec = Codec::default();
            let mut src = BytesMut::from(&stream[..]);
            for _ in &before {
                codec.decode(&mut src).unwrap();
            }
            let mut left = [&header[..], &after[..]].concat();
            prop_assert_eq!(codec.decode(&mut src).err(), Some(Error::LostFraming(error)));
            prop_assert_eq!(&src[..], &left[..]);
            for bytes in arrivals {
                src.extend_from_slice(&bytes);
                left.extend(bytes);
                prop_assert_eq!(codec.decode(&mut src).err(), Some(Error::LostFraming(error)));
                prop_assert_eq!(&src[..], &left[..]);
            }
        }

        /// T5, T6, T7 and T8. Any stream decodes step by step as the model
        /// walks it, each call leaves the bytes its step leaves, and the
        /// stream decodes the same however it is cut. Nothing panics.
        #[test]
        fn any_stream_decodes_as_its_headers_say(
            stream in stream(),
            arrival in prop::collection::vec(any::<u8>(), 0..8),
            cuts in prop::collection::vec(any::<Index>(), 0..16),
        ) {
            check(&stream, &arrival)?;
            let whole = decode_pieces([&stream[..]]);
            prop_assert_eq!(&decode_pieces(strategy::pieces(&stream, &cuts)), &whole);
            prop_assert_eq!(&decode_pieces(stream.chunks(1)), &whole);
        }
    }

    /// Prints the share of the stream property's cases whose steps reach
    /// each label, when `TWINLEAF_CODEC_CLASSES` is set.
    #[test]
    fn class_counts() {
        strategy::tally("TCP stream steps", stream(), |stream| {
            let mut labels = labels(stream);
            labels.sort_unstable();
            labels.dedup();
            labels
        });
    }
}

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
    use super::*;
    use crate::proto::packet::PacketType;
    use crate::proto::DeviceRoute;

    /// A heartbeat, a routed request and a packet of the largest size.
    fn packets() -> Vec<Packet> {
        vec![
            Packet::heartbeat_session(1, DeviceRoute::root()),
            Packet::rpc_request("dev.name", &[], 7, "/1/2".parse().unwrap()).unwrap(),
            Packet::rpc_reply(0, &[0xa5; 498], DeviceRoute::root()).unwrap(),
        ]
    }

    /// The bytes of `packets` back to back.
    fn wire(packets: &[Packet]) -> Vec<u8> {
        packets.iter().flat_map(|p| p.as_bytes()).copied().collect()
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

    #[test]
    fn a_stream_split_at_any_byte_decodes_to_the_same_packets() {
        let packets = packets();
        let wire = wire(&packets);
        let expected: Vec<_> = packets.iter().map(|p| Ok(p.as_bytes().to_vec())).collect();

        for at in 0..=wire.len() {
            assert_eq!(
                decode_pieces([&wire[..at], &wire[at..]]),
                expected,
                "split at {at}"
            );
        }
        assert_eq!(decode_pieces(wire.chunks(1)), expected);
    }

    /// Each call decodes one packet and leaves the rest in the buffer.
    #[test]
    fn several_packets_in_one_buffer_decode_one_per_call() {
        let packets = packets();
        let mut src = BytesMut::from(&wire(&packets)[..]);
        let mut codec = Codec::default();

        for (i, packet) in packets.iter().enumerate() {
            let decoded = codec.decode(&mut src).unwrap().unwrap();
            assert_eq!(decoded.as_bytes(), packet.as_bytes());
            assert_eq!(src.len(), wire(&packets[i + 1..]).len());
        }
        assert!(codec.decode(&mut src).unwrap().is_none());
    }

    #[test]
    fn a_packet_whose_header_decodes_is_skipped_alone() {
        let heartbeat = Packet::heartbeat(DeviceRoute::root());
        // A PROXY_RX header announcing an empty payload, which has no kind byte.
        let mut wire = vec![PacketType::PROXY_RX.value(), 0, 0, 0];
        wire.extend_from_slice(heartbeat.as_bytes());

        assert_eq!(
            decode_pieces([&wire[..]]),
            [
                Err(Error::Invalid(packet::DecodeError::PayloadTooShort)),
                Ok(heartbeat.as_bytes().to_vec()),
            ]
        );
    }

    /// An invalid type, a payload over 500 bytes and a route over 8 hops
    /// each leave no packet boundary.
    #[test]
    fn a_header_that_does_not_decode_loses_the_framing() {
        let heartbeat = Packet::heartbeat(DeviceRoute::root());
        for (header, error) in [
            ([0, 0, 0, 0], packet::DecodeError::InvalidPacketType),
            ([5, 0, 0xf5, 0x01], packet::DecodeError::PayloadTooBig),
            ([5, 9, 0, 0], packet::DecodeError::RoutingTooBig),
        ] {
            let mut wire = header.to_vec();
            wire.extend_from_slice(heartbeat.as_bytes());

            assert_eq!(
                decode_pieces([&wire[..]]),
                [Err(Error::LostFraming(error))],
                "{header:?}"
            );
        }
    }

    #[test]
    fn encoded_packets_decode_unchanged() {
        let packets = packets();
        let mut codec = Codec::default();
        let mut dst = BytesMut::new();
        for packet in &packets {
            codec.encode(packet, &mut dst);
        }

        assert_eq!(dst, wire(&packets));
        for packet in &packets {
            let decoded = codec.decode(&mut dst).unwrap().unwrap();
            assert_eq!(decoded.as_bytes(), packet.as_bytes());
        }
        assert!(dst.is_empty());
    }
}

//! The UDP framing.
//!
//! Each TIO packet travels unchanged in a datagram of its own.

use super::Error;
use crate::tio::packet::Packet;

/// Decodes `datagram` as the one packet it holds.
///
/// # Errors
///
/// Returns [`Error::TrailingBytes`] if bytes follow the packet, and
/// [`Error::Invalid`] if `datagram` does not start with a valid packet.
pub(crate) fn decode(datagram: &[u8]) -> Result<Packet, Error> {
    super::decode_exact(datagram)
}

/// Returns the datagram that carries `packet`.
pub(crate) fn encode(packet: &Packet) -> &[u8] {
    packet.as_bytes()
}

/// Exactly one TIO packet per datagram, including a rejected empty datagram.
#[derive(Debug, Default)]
pub(crate) struct Codec {
    decoded: bool,
}

impl tokio_util::codec::Decoder for Codec {
    type Item = super::super::TransportEvent;
    type Error = super::super::RecvError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode_eof(src)
    }

    fn decode_eof(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if std::mem::replace(&mut self.decoded, true) {
            self.decoded = false;
            return Ok(None);
        }
        let decoded = decode(src);
        src.clear();
        match decoded {
            Ok(packet) => Ok(Some(Self::Item::Packet(packet))),
            Err(error) => super::super::rejected_frame(error).map(Some),
        }
    }
}

impl tokio_util::codec::Encoder<Packet> for Codec {
    type Error = super::super::RecvError;

    fn encode(&mut self, packet: Packet, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(encode(&packet));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Properties of the UDP framing.
    //!
    //! # Observations
    //!
    //! What [`decode`] returns for a datagram, the bytes [`encode`] gives for
    //! a packet, and the events and buffer the [`Codec`] decoder leaves.
    //!
    //! # Sources
    //!
    //! The module doc, the docs of [`decode`] and [`Codec`], and the header
    //! layout in the [`proto::packet`](crate::proto::packet) module doc.
    //!
    //! # Laws
    //!
    //! - U1, round trip. The datagram of any packet decodes as that packet.
    //!   Source, the module doc. It rules out a codec that drops the routing
    //!   or refuses a type it does not read.
    //! - U2, postcondition. A datagram that is not exactly one packet gives
    //!   one error. Bytes after the packet give `TrailingBytes`, a datagram
    //!   that ends before its packet does, the empty one too, gives
    //!   `PacketTooSmall`, and an invalid header or payload gives the error
    //!   the packet gives. Source, the doc of `decode`. It rules out a codec
    //!   that ignores trailing bytes.
    //! - U3, the decoder adapter. Each datagram gives exactly one event, a
    //!   packet or a rejected frame, then `None`, and leaves the buffer
    //!   empty. Source, the doc of [`Codec`]. It rules out an adapter that
    //!   repeats a datagram or loses the next one.
    //!
    //! # Generator obligations
    //!
    //! Packets as the TCP properties draw them, and each class
    //! [`strategy::not_one_packet`] names.
    //!
    //! # Examples that stay
    //!
    //! None. U1 and U2 hold every case the examples held.

    use super::*;
    use crate::tio::transport::codec::strategy;
    use crate::tio::transport::TransportEvent;
    use bytes::BytesMut;
    use proptest::prelude::*;
    use tokio_util::codec::Decoder;

    proptest! {
        /// U1. The datagram of any packet decodes as that packet.
        #[test]
        fn any_packet_decodes_from_its_datagram(packet in strategy::packet()) {
            let decoded = decode(encode(&packet)).unwrap();
            prop_assert_eq!(decoded.as_bytes(), packet.as_bytes());
        }

        /// U2. A datagram that is not exactly one packet gives its error.
        #[test]
        fn a_datagram_that_is_not_one_packet_is_its_error(
            (datagram, error, _) in strategy::not_one_packet(),
        ) {
            prop_assert_eq!(decode(&datagram).err(), Some(error));
        }

        /// U3. The decoder gives one event for each datagram, then `None`,
        /// and empties the buffer. Trailing bytes are rejected as too big.
        #[test]
        fn the_decoder_gives_one_event_for_each_datagram(
            datagrams in prop::collection::vec(prop_oneof![
                strategy::packet().prop_map(|packet| (packet.as_bytes().to_vec(), None)),
                strategy::not_one_packet().prop_map(|(datagram, error, _)| (datagram, Some(error))),
            ], 1..4),
        ) {
            let mut codec = Codec::default();
            for (datagram, error) in datagrams {
                let mut src = BytesMut::from(&datagram[..]);
                let event = codec.decode(&mut src).unwrap();
                prop_assert!(src.is_empty());
                match (error, event) {
                    (None, Some(TransportEvent::Packet(packet))) => {
                        prop_assert_eq!(packet.as_bytes(), &datagram[..]);
                    }
                    (Some(Error::TrailingBytes), Some(TransportEvent::RejectedFrame(rejected))) => {
                        prop_assert_eq!(rejected, crate::tio::packet::DecodeError::PacketTooBig);
                    }
                    (Some(Error::Invalid(error)), Some(TransportEvent::RejectedFrame(rejected))) => {
                        prop_assert_eq!(rejected, error);
                    }
                    (error, event) => prop_assert!(false, "{:?} gave {:?}", error, event),
                }
                prop_assert!(codec.decode(&mut src).unwrap().is_none());
            }
        }
    }
}

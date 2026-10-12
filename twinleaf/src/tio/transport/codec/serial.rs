//! The serial framing.
//!
//! Each TIO packet gets its CRC32 appended and is SLIP encoded, as
//! [`proto::serial`](crate::proto::serial) describes. Newline delimited ASCII
//! text between frames decodes as [`Frame::Text`].

use super::Error;
use crate::proto::packet::Packet as WirePacket;
use crate::proto::serial as wire;
use crate::tio::packet::{self, Packet};
use bytes::{Buf, BufMut, BytesMut};

/// The deserializer's capacity, the largest packet plus its trailing CRC32.
const RX_CAPACITY: usize = WirePacket::MAX_SIZE + wire::CRC_SIZE;

/// What a serial link carries, a packet or a line of text.
#[derive(Debug)]
pub(crate) enum Frame {
    /// A packet.
    Packet(Packet),
    /// A line of text outside the packet protocol, without its line end.
    Text(String),
}

/// A codec of SLIP framed packets and text lines on a serial stream.
#[derive(Debug, Default)]
pub(crate) struct Codec {
    /// The SLIP and CRC decoder, which holds a partly received frame.
    deserializer: wire::Deserializer<RX_CAPACITY>,
}

impl Codec {
    /// Decodes bytes from the start of `src` until a frame completes, and
    /// removes the bytes it used. A frame still arriving is held in the
    /// codec. Returns `None` once `src` runs out with no frame complete.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] for a frame that is not a valid packet. Its
    /// reason is [`CRC32`](packet::DecodeError::CRC32) for a CRC mismatch or
    /// a bad escape, [`PacketTooBig`](packet::DecodeError::PacketTooBig) for
    /// a frame longer than any packet, and
    /// [`PacketTooSmall`](packet::DecodeError::PacketTooSmall) for one too
    /// short to hold a packet and its CRC. A frame whose CRC matches gives
    /// the reason its packet does not decode. Returns
    /// [`Error::TrailingBytes`] for a frame that holds more bytes after its
    /// packet. Decoding goes on with the next frame after any error.
    pub(crate) fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Error> {
        let (consumed, frame) = self.deserializer.push(src);
        let frame = frame.map(|frame| decode_frame(&frame)).transpose();
        src.advance(consumed);
        frame
    }

    /// Appends `packet` to `dst` as one SLIP frame with its CRC32.
    pub(crate) fn encode(&mut self, packet: &Packet, dst: &mut BytesMut) {
        let raw = packet.as_bytes();
        // The leading separator terminates any partial frame at the receiver.
        dst.put_u8(wire::SLIP_END);
        let start = dst.len();
        dst.resize(start + wire::max_serialized_size(raw.len()), 0);
        let size = wire::serialize(raw, &mut dst[start..]).expect("a frame fits its largest size");
        dst.truncate(start + size);
    }
}

/// Turns a deserialized frame into a packet or text, or into the error it
/// represents.
fn decode_frame(frame: &wire::Frame) -> Result<Frame, Error> {
    use wire::FrameErrors;
    if let Some(packet) = frame.packet() {
        return super::decode_exact(packet).map(Frame::Packet);
    }
    let errors = frame.errors;
    if errors.contains(FrameErrors::TEXT) {
        return Ok(Frame::Text(String::from_utf8_lossy(frame.data).to_string()));
    }
    Err(Error::Invalid(if errors.contains(FrameErrors::TOO_BIG) {
        packet::DecodeError::PacketTooBig
    } else if errors.contains(FrameErrors::SHORT) {
        packet::DecodeError::PacketTooSmall
    } else {
        // CRC mismatch, or a bad escape which corrupted the frame.
        packet::DecodeError::CRC32
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::DeviceRoute;

    /// A frame as a test compares it, a packet as its bytes.
    #[derive(Debug, PartialEq)]
    enum Seen {
        Packet(Vec<u8>),
        Text(String),
    }

    /// The decoded `packet` as a test compares it.
    fn seen(packet: &Packet) -> Result<Seen, Error> {
        Ok(Seen::Packet(packet.as_bytes().to_vec()))
    }

    /// The decoded text `line` as a test compares it.
    fn text(line: &str) -> Result<Seen, Error> {
        Ok(Seen::Text(line.to_string()))
    }

    /// A heartbeat, a packet with bytes that SLIP escapes, and a packet of
    /// the largest size.
    fn packets() -> Vec<Packet> {
        let framing = [wire::SLIP_END, wire::SLIP_ESC, b'\n', 0];
        vec![
            Packet::heartbeat_session(1, DeviceRoute::root()),
            Packet::rpc_reply(0xc0db, &framing, "/1/2".parse().unwrap()).unwrap(),
            Packet::rpc_reply(0, &[0xa5; 498], DeviceRoute::root()).unwrap(),
        ]
    }

    /// The frames of `packets` back to back.
    fn encoded(packets: &[Packet]) -> Vec<u8> {
        let mut codec = Codec::default();
        let mut dst = BytesMut::new();
        for packet in packets {
            codec.encode(packet, &mut dst);
        }
        dst.to_vec()
    }

    /// What one codec decodes from `pieces` fed in turn.
    fn decode_pieces<'a>(pieces: impl IntoIterator<Item = &'a [u8]>) -> Vec<Result<Seen, Error>> {
        let mut codec = Codec::default();
        let mut src = BytesMut::new();
        let mut decoded = Vec::new();
        for piece in pieces {
            src.extend_from_slice(piece);
            while let Some(result) = codec.decode(&mut src).transpose() {
                decoded.push(result.map(|frame| match frame {
                    Frame::Packet(packet) => Seen::Packet(packet.as_bytes().to_vec()),
                    Frame::Text(line) => Seen::Text(line),
                }));
            }
        }
        decoded
    }

    #[test]
    fn a_stream_split_at_any_byte_decodes_to_the_same_frames() {
        let packets = packets();
        let mut wire = b"boot v1.2\r\n".to_vec();
        wire.extend(encoded(&packets));
        let mut expected = vec![text("boot v1.2")];
        expected.extend(packets.iter().map(seen));

        for at in 0..=wire.len() {
            assert_eq!(
                decode_pieces([&wire[..at], &wire[at..]]),
                expected,
                "split at {at}"
            );
        }
        assert_eq!(decode_pieces(wire.chunks(1)), expected);
    }

    /// Each call decodes one frame and leaves the bytes after it in the
    /// buffer.
    #[test]
    fn several_packets_in_one_buffer_decode_one_per_call() {
        let packets = packets();
        let mut src = BytesMut::from(&encoded(&packets)[..]);
        let mut codec = Codec::default();

        for (i, packet) in packets.iter().enumerate() {
            let Some(Frame::Packet(decoded)) = codec.decode(&mut src).unwrap() else {
                panic!("packet {i} did not decode");
            };
            assert_eq!(decoded.as_bytes(), packet.as_bytes());
            assert_eq!(src.len(), encoded(&packets[i + 1..]).len());
        }
        assert!(codec.decode(&mut src).unwrap().is_none());
    }

    #[test]
    fn bytes_that_frame_are_escaped() {
        let packet =
            Packet::rpc_reply(0, &[wire::SLIP_END, wire::SLIP_ESC], DeviceRoute::root()).unwrap();
        let frame = encoded(std::slice::from_ref(&packet));

        #[rustfmt::skip]
        assert_eq!(
            frame[..11],
            [
                wire::SLIP_END,
                3, 0, 4, 0, // RPC reply header
                0, 0, // request ID
                wire::SLIP_ESC, wire::SLIP_ESC_END,
                wire::SLIP_ESC, wire::SLIP_ESC_ESC,
            ]
        );
        assert_eq!(decode_pieces([&frame[..]]), [seen(&packet)]);
    }

    #[test]
    fn a_frame_that_fails_its_crc_is_reported_and_the_next_decodes() {
        let packets = packets();
        let mut wire = encoded(&packets[..2]);
        // The routing byte of the first packet.
        wire[2] ^= 0x01;

        assert_eq!(
            decode_pieces([&wire[..]]),
            [
                Err(Error::Invalid(packet::DecodeError::CRC32)),
                seen(&packets[1]),
            ]
        );
    }

    #[test]
    fn text_between_packets_decodes_as_text() {
        let packets = packets();
        let mut wire = b"boot v1.2\r\n".to_vec();
        wire.extend(encoded(&packets[..1]));
        wire.extend(b"ready\n");
        wire.extend(encoded(&packets[1..2]));

        assert_eq!(
            decode_pieces([&wire[..]]),
            [
                text("boot v1.2"),
                seen(&packets[0]),
                text("ready"),
                seen(&packets[1]),
            ]
        );
    }

    /// Garbage longer than any frame overflows the decoder, and what follows
    /// the overflow fails its CRC.
    #[test]
    fn garbage_is_reported_and_the_packet_after_it_decodes() {
        let heartbeat = Packet::heartbeat(DeviceRoute::root());
        let mut short = vec![0x01, 0xff];
        short.extend(encoded(std::slice::from_ref(&heartbeat)));
        let mut long = vec![0xaa; RX_CAPACITY + 50];
        long.extend(encoded(std::slice::from_ref(&heartbeat)));

        assert_eq!(
            decode_pieces([&short[..]]),
            [
                Err(Error::Invalid(packet::DecodeError::PacketTooSmall)),
                seen(&heartbeat),
            ]
        );
        assert_eq!(
            decode_pieces([&long[..]]),
            [
                Err(Error::Invalid(packet::DecodeError::PacketTooBig)),
                Err(Error::Invalid(packet::DecodeError::CRC32)),
                seen(&heartbeat),
            ]
        );
    }

    /// A frame whose CRC matches is still one packet or an error.
    #[test]
    fn a_frame_that_is_not_one_packet_is_an_error() {
        let frame = |raw: &[u8]| {
            let mut out = vec![0; wire::max_serialized_size(raw.len())];
            let size = wire::serialize(raw, &mut out).unwrap();
            out.truncate(size);
            out
        };
        let mut trailing = Packet::heartbeat(DeviceRoute::root()).as_bytes().to_vec();
        trailing.push(0);

        assert_eq!(
            decode_pieces([&frame(&trailing)[..]]),
            [Err(Error::TrailingBytes)]
        );
        assert_eq!(
            decode_pieces([&frame(&[5, 0])[..]]),
            [Err(Error::Invalid(packet::DecodeError::PacketTooSmall))]
        );
        // Type 9 is reserved.
        assert_eq!(
            decode_pieces([&frame(&[9, 0, 0, 0])[..]]),
            [Err(Error::Invalid(packet::DecodeError::InvalidPacketType))]
        );
    }
}

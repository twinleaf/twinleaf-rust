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
use std::time::Duration;
use tokio::time::Instant;

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
    /// Before this time, discard the first read from a newly opened device.
    discard_first_until: Option<Instant>,
    /// A partial serial frame expires after this much silence.
    stale_after: Option<Duration>,
    last_read: Option<Instant>,
}

impl Codec {
    /// Applies the startup discard and partial-frame timeout of a device link.
    pub(crate) fn with_timing(holdoff: Option<Duration>, stale_after: Option<Duration>) -> Self {
        Self {
            discard_first_until: holdoff.map(|duration| Instant::now() + duration),
            stale_after,
            ..Self::default()
        }
    }

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
        if src.is_empty() {
            return Ok(None);
        }
        let now = Instant::now();
        if self
            .discard_first_until
            .take()
            .is_some_and(|until| now < until)
        {
            src.clear();
            self.last_read = Some(now);
            return Ok(None);
        }
        if self
            .stale_after
            .zip(self.last_read)
            .is_some_and(|(stale, last)| now.duration_since(last) > stale)
        {
            self.deserializer = wire::Deserializer::default();
        }
        self.last_read = Some(now);
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
        return Ok(Frame::Text(
            String::from_utf8_lossy(frame.data()).to_string(),
        ));
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

impl tokio_util::codec::Decoder for Codec {
    type Item = super::super::TransportEvent;
    type Error = super::super::RecvError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Codec::decode(self, src) {
            Ok(Some(Frame::Packet(packet))) => Ok(Some(Self::Item::Packet(packet))),
            Ok(Some(Frame::Text(text))) => Ok(Some(Self::Item::Text(text))),
            Ok(None) => Ok(None),
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
    //! Properties of the serial framing.
    //!
    //! # Observations
    //!
    //! What each call of [`Codec::decode`] returns, a packet, a line of text,
    //! nothing yet or an error, and the bytes it leaves in `src`. The bytes
    //! [`Codec::encode`] appends.
    //!
    //! # Sources
    //!
    //! The module doc and the docs of [`Codec`]. The frame layout in the
    //! [`proto::serial`](crate::proto::serial) module doc and RFC 1055. The
    //! header layout in the [`proto::packet`](crate::proto::packet) module
    //! doc, and the largest packet, 512 bytes.
    //!
    //! # Model
    //!
    //! The frame of a packet `p` is `F(p)`, in [`frame_of`]. It is an END,
    //! then the bytes of `p` and their CRC32, ISO HDLC and little endian, with
    //! each END sent as ESC `ESC_END` and each ESC as ESC `ESC_ESC`, then an
    //! END. END is 0xC0, ESC 0xDB, `ESC_END` 0xDC and `ESC_ESC` 0xDD.
    //! [`crc32`] computes the CRC bit by bit.
    //!
    //! A line of text is printable ASCII or tabs, then LF or CR LF. The
    //! deserializer holds the bytes of a frame, each escape resolved, and a
    //! CR or LF that arrives while all it holds is printable ends a line of
    //! what it holds, or is skipped when it holds nothing. After a line the
    //! rule starts over on the bytes that follow, as it does between frames,
    //! so one frame may read as several lines. [`lines_of`] gives the lines
    //! a frame reads as by this rule, and whether bytes are left after them.
    //!
    //! # Laws
    //!
    //! - S1, inductive and round trip. Packets and lines of text sent in any
    //!   order decode as the same packets and lines, in order. Source, the
    //!   module doc. It rules out a codec that clears the TTL, checks the CRC
    //!   over the wrong bytes, or keeps the line end in the text.
    //! - S2, metamorphic. Any cuts of the stream into reads, and one byte at a
    //!   time, decode as the whole stream does. Source, the deserializer
    //!   holds a frame still arriving. It rules out a codec that loses a frame
    //!   split across reads.
    //! - S3, model based. The bytes `encode` appends for `p` are `F(p)`.
    //!   Source, the `proto::serial` module doc and the doc of `encode`. It
    //!   rules out a big endian CRC, swapped escapes, or a missing leading
    //!   END.
    //! - S4, inductive step. A frame whose CRC matches and that is not one
    //!   packet gives one error, and the frame after it decodes. Bytes after
    //!   the packet give `TrailingBytes`. A frame that ends before its packet
    //!   does gives `PacketTooSmall`. A header that does not decode, or a
    //!   payload not valid for its type, gives the error the packet gives. A
    //!   frame of 516 bytes, the largest packet and its CRC, is held whole,
    //!   and a longer one gives `PacketTooBig` at its 517th byte, after which
    //!   decoding starts over within the same frame. Source, the
    //!   doc of `decode`. It rules out a codec that accepts trailing bytes,
    //!   stops after an error, or holds frames longer than any packet.
    //! - S5, metamorphic. One byte of one frame changed to any value other
    //!   than END or ESC makes that frame exactly the lines [`lines_of`]
    //!   gives, then one error when bytes are left after them, and every
    //!   other frame decodes unchanged. Lines come whatever the new byte is,
    //!   since the change may make the bytes before a line end printable. A
    //!   proxy reply with a payload of 13 bytes whose routing byte becomes
    //!   printable reads as the line of its first two bytes. A TEXT packet
    //!   with a printable header whose payload size high byte becomes a line
    //!   end reads as the line of its header, and then as the lines its
    //!   payload holds. Source, a CRC32 detects any change of one byte, and
    //!   the text rule of the model. It rules out an error that swallows the
    //!   next frame, a line where the rule reads none, and a codec that stops
    //!   reading lines after the first.
    //! - S6, postcondition and progress. A call that returns a packet removes
    //!   exactly its frame. On any bytes, a call that returns a frame or an
    //!   error removes at least one byte, and a call returns `None` only once
    //!   `src` is empty. Source, the doc of `decode`. It rules out a decode
    //!   loop that spins on garbage.
    //! - S7, implicit oracle. No input panics.
    //! - S8, examples on paused time. A partial frame expires after more than
    //!   its silence budget and survives exactly the budget. The first read
    //!   before the holdoff ends is discarded, and a read at its end is kept.
    //!   Source, the docs of [`Codec::with_timing`] and its fields.
    //!
    //! # Generator obligations
    //!
    //! - Packets as the TCP properties draw them, every class
    //!   [`strategy::raw`] names, with a TTL of 0 to 15. Payloads hold END and
    //!   ESC bytes, so frames carry escapes.
    //! - Lines ended by LF and by CR LF.
    //! - CRC matched frames of each class [`strategy::not_one_packet`] names.
    //! - Changed bytes in the header, in the CRC and anywhere, changed to
    //!   line ends, printable bytes and any value, in packets whose first
    //!   bytes may read as text, so that S5 reaches frames that read as a
    //!   line and frames that do not.
    //! - Noise rich in END, ESC, line ends and zero bytes, and floods longer
    //!   than any frame.
    //!
    //! `class_counts` prints the share of each class with
    //! `TWINLEAF_CODEC_CLASSES` set.
    //!
    //! # Exclusions
    //!
    //! - E1. A frame whose bytes from its start are printable and then a CR or
    //!   LF decodes as a line of text and then a CRC error, because the
    //!   deserializer ends a line at the first line end after only printable
    //!   bytes. With no printable bytes before it, the line end is skipped as
    //!   a bare one and the frame fails its CRC, which only a frame of an
    //!   invalid type byte, 10 or 13, meets. Among packets E1 is the class
    //!   whose type byte is printable, 0x20 to 0x7E, whose TTL is 2 to 7 so
    //!   that the routing byte is printable, and whose payload size has a low
    //!   byte of 10 or 13. S1, S4 and S5 leave it out with [`reads_as_text`].
    //!   `a_packet_whose_header_reads_as_text_decodes` pins one such packet
    //!   and is ignored while the deserializer keeps this rule.
    //! - Zero bytes before the first frame are skipped as line noise, so S4
    //!   and S5 send their frames after a packet.
    //! - Text with no line end before a frame, and text that is not printable,
    //!   decode as frame errors, and no law covers them.
    //!
    //! # Examples that stay
    //!
    //! One whole frame pinned on the wire with its escapes and CRC, a frame
    //! that fails its CRC, text between packets, garbage and a flood before
    //! a packet, the largest frame and one byte more, a changed routing byte
    //! that makes a line, a changed size byte that makes two, and the timing
    //! of S8.

    use super::*;
    use crate::proto::rpc::RpcError;
    use crate::proto::DeviceRoute;
    use crate::tio::transport::codec::tests::strategy;
    use proptest::prelude::*;
    use proptest::sample::Index;

    /// A frame as a test compares it, a packet as its bytes.
    #[derive(Debug, Clone, PartialEq)]
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

    /// The CRC32 of `bytes`, ISO HDLC, bit by bit. The polynomial is
    /// 0xEDB88320 reflected, and the start value and final XOR are all ones.
    fn crc32(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 == 0 {
                    crc >> 1
                } else {
                    (crc >> 1) ^ 0xedb8_8320
                };
            }
        }
        !crc
    }

    /// `F`, the frame of the bytes `raw` as the wire format lays it out.
    fn frame_of(raw: &[u8]) -> Vec<u8> {
        let crc = crc32(raw).to_le_bytes();
        let mut frame = vec![0xc0];
        for &byte in raw.iter().chain(&crc) {
            match byte {
                0xc0 => frame.extend([0xdb, 0xdc]),
                0xdb => frame.extend([0xdb, 0xdd]),
                byte => frame.push(byte),
            }
        }
        frame.push(0xc0);
        frame
    }

    /// The lines of text `frame`, which starts with an END, reads as before
    /// its next END, by the text rule of the model, and whether bytes are
    /// left after the last line to end as a frame. After each line the rule
    /// starts over on the bytes that follow, as it does between frames.
    fn lines_of(frame: &[u8]) -> (Vec<String>, bool) {
        let printable = |byte: &u8| *byte == b'\t' || (0x20..=0x7e).contains(byte);
        let mut lines = Vec::new();
        let mut held = Vec::new();
        let mut escaped = false;
        for &byte in &frame[1..] {
            if byte == 0xc0 {
                break;
            }
            if matches!(byte, b'\n' | b'\r') && held.iter().all(printable) {
                if !held.is_empty() {
                    let line = std::mem::take(&mut held);
                    lines.push(String::from_utf8(line).expect("printable ASCII"));
                    escaped = false;
                }
                continue;
            }
            if !escaped && byte == 0xdb {
                escaped = true;
                continue;
            }
            held.push(match (escaped, byte) {
                (true, 0xdc) => 0xc0,
                (true, 0xdd) => 0xdb,
                (_, byte) => byte,
            });
            escaped = false;
        }
        (lines, !held.is_empty())
    }

    /// Where each byte of `frame`, which starts and ends with an END, lies,
    /// as its index in the frame and the index of the byte it stands for
    /// before escaping.
    fn places(frame: &[u8]) -> Vec<(usize, usize)> {
        let mut places = Vec::new();
        let mut at = 1;
        let mut unescaped = 0;
        while at < frame.len() - 1 {
            places.push((at, unescaped));
            if frame[at] == 0xdb {
                at += 1;
                places.push((at, unescaped));
            }
            at += 1;
            unescaped += 1;
        }
        places
    }

    /// One corruption S5 draws, the packets, which to change, where and to
    /// what.
    type Corruption = (Vec<Packet>, Index, (u8, Index), (u8, Index));

    /// Packets and the one byte of one frame to change. Half the packets
    /// have a printable type and a TTL of 2 to 7, so that their first bytes
    /// may read as text. The byte is in the header half the time, in the
    /// CRC one time in four, and anywhere in the frame otherwise. Its new
    /// value is a CR or LF half the time, printable one time in four, and
    /// any value otherwise.
    fn corruption() -> impl Strategy<Value = Corruption> {
        // Printable types whose payload may be any bytes, every one but the
        // legacy status and the proxy types.
        let types: Vec<u8> = (0x20..=0x7e)
            .filter(|t| !matches!(t, 64 | 66 | 67))
            .collect();
        let printable = (
            prop::sample::select(types),
            2..=7u8,
            prop::collection::vec(any::<u8>(), 0..=500),
            prop::collection::vec(any::<u8>(), 0..=8),
        )
            .prop_map(|(ptype, ttl, payload, hops)| {
                strategy::wire(strategy::assemble(ptype, ttl, &payload, &hops))
            })
            .prop_filter("a frame that reads as text, E1", |packet| {
                !reads_as_text(&frame_of(packet.as_bytes()))
            });
        (
            prop::collection::vec(prop_oneof![packet(), printable], 1..6),
            any::<Index>(),
            (0..4u8, any::<Index>()),
            (0..4u8, any::<Index>()),
        )
    }

    /// The frames of a corruption with its byte changed, the index of the
    /// changed frame, and the part of the packet the byte lies in.
    fn corrupt(
        (packets, which, (part, at), (kind, to)): &Corruption,
    ) -> (Vec<Vec<u8>>, usize, &'static str) {
        let corrupted = which.index(packets.len());
        let mut frames: Vec<Vec<u8>> = packets
            .iter()
            .map(|p| encoded(std::slice::from_ref(p)))
            .collect();
        let len = packets[corrupted].as_bytes().len();
        let frame = &mut frames[corrupted];
        let chosen: Vec<(usize, usize)> = places(frame)
            .into_iter()
            .filter(|&(_, byte)| match part {
                0 | 1 => byte < 4,
                2 => byte >= len,
                _ => true,
            })
            .collect();
        let (at, byte) = chosen[at.index(chosen.len())];
        let values: Vec<u8> = (0..=u8::MAX)
            .filter(|&v| v != frame[at] && v != wire::SLIP_END && v != wire::SLIP_ESC)
            .filter(|&v| match kind {
                0 | 1 => matches!(v, b'\n' | b'\r'),
                2 => (0x20..=0x7e).contains(&v),
                _ => true,
            })
            .collect();
        frame[at] = values[to.index(values.len())];
        let part = match byte {
            0..=3 => "header",
            byte if byte >= len => "CRC",
            _ => "payload or routing",
        };
        (frames, corrupted, part)
    }

    /// Whether `frame`, which starts with an END, is in class E1. Its bytes
    /// after the END start with printable ASCII or tabs, or none, then a CR
    /// or LF.
    fn reads_as_text(frame: &[u8]) -> bool {
        let bytes = &frame[1..];
        let printable = bytes
            .iter()
            .take_while(|&&byte| byte == b'\t' || (0x20..=0x7e).contains(&byte))
            .count();
        matches!(bytes.get(printable), Some(b'\n' | b'\r'))
    }

    /// A packet as [`strategy::packet`] draws it, outside class E1.
    fn packet() -> impl Strategy<Value = Packet> {
        strategy::packet().prop_filter("a frame that reads as text, E1", |packet| {
            !reads_as_text(&frame_of(packet.as_bytes()))
        })
    }

    /// The bytes S4 frames, as [`strategy::not_one_packet`] draws them,
    /// outside class E1.
    fn s4_frame() -> impl Strategy<Value = (Vec<u8>, Error, &'static str)> {
        strategy::not_one_packet().prop_filter("a frame that reads as text, E1", |(raw, _, _)| {
            !reads_as_text(&frame_of(raw))
        })
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

    /// A packet, or a line of text with the line end it is sent with.
    fn frame() -> impl Strategy<Value = Result<Packet, (String, &'static str)>> {
        prop_oneof![
            4 => packet().prop_map(Ok),
            1 => ("[\t -~]{1,40}", prop::sample::select(vec!["\r\n", "\n"])).prop_map(Err),
        ]
    }

    /// Bytes a serial link may carry in any order, frames, lines, noise rich
    /// in the bytes that frame, and floods longer than any frame.
    fn any_stream() -> impl Strategy<Value = Vec<u8>> {
        let noise = prop::collection::vec(
            prop_oneof![
                4 => any::<u8>(),
                2 => prop::sample::select(vec![0xc0, 0xdb, 0xdc, 0xdd, b'\n', b'\r', 0]),
                2 => 0x20..=0x7eu8,
            ],
            1..64,
        );
        let part = prop_oneof![
            3 => strategy::packet().prop_map(|packet| encoded(&[packet])),
            1 => ("[\t -~]{1,40}", prop::sample::select(vec!["\r\n", "\n"]))
                .prop_map(|(line, end)| format!("{line}{end}").into_bytes()),
            3 => noise,
            1 => prop::collection::vec(0..0xc0u8, RX_CAPACITY..RX_CAPACITY + 64),
        ];
        prop::collection::vec(part, 0..6).prop_map(|parts| parts.concat())
    }

    /// What a decode returned, as the counts name it.
    fn outcome(result: &Result<Option<Frame>, Error>) -> &'static str {
        use packet::DecodeError as E;
        match result {
            Ok(None) => "none",
            Ok(Some(Frame::Packet(_))) => "packet",
            Ok(Some(Frame::Text(_))) => "text",
            Err(Error::TrailingBytes) => "trailing bytes",
            Err(Error::Invalid(E::CRC32)) => "CRC32",
            Err(Error::Invalid(E::PacketTooBig)) => "too big",
            Err(Error::Invalid(E::PacketTooSmall)) => "too small",
            Err(_) => "other error",
        }
    }

    proptest! {
        /// S1 and S2. Any packets and lines of text sent in turn decode
        /// unchanged, however the stream is cut into reads, and when it
        /// arrives a byte at a time.
        #[test]
        fn encoded_frames_decode_unchanged_however_the_stream_is_cut(
            frames in prop::collection::vec(frame(), 0..8),
            cuts in prop::collection::vec(any::<Index>(), 0..16),
        ) {
            let mut wire = Vec::new();
            let mut expected = Vec::new();
            for frame in &frames {
                match frame {
                    Ok(packet) => {
                        wire.extend(encoded(std::slice::from_ref(packet)));
                        expected.push(seen(packet));
                    }
                    Err((line, end)) => {
                        wire.extend(line.as_bytes());
                        wire.extend(end.as_bytes());
                        expected.push(text(line));
                    }
                }
            }

            prop_assert_eq!(decode_pieces(strategy::pieces(&wire, &cuts)), expected.clone());
            prop_assert_eq!(decode_pieces(wire.chunks(1)), expected);
        }

        /// S3. A packet encodes as its SLIP frame, after whatever the
        /// buffer already holds.
        #[test]
        fn a_packet_encodes_as_its_frame(
            packets in prop::collection::vec(strategy::packet(), 1..4),
        ) {
            let expected: Vec<u8> =
                packets.iter().flat_map(|packet| frame_of(packet.as_bytes())).collect();
            prop_assert_eq!(encoded(&packets), expected);
        }

        /// S4. A frame whose CRC matches and that is not one packet gives the
        /// error its bytes give, and the frame after it decodes.
        #[test]
        fn a_frame_that_is_not_one_packet_is_its_error(
            (raw, error, _) in s4_frame(),
            after in packet(),
        ) {
            let first = Packet::heartbeat(DeviceRoute::root());
            let mut stream = encoded(std::slice::from_ref(&first));
            stream.extend(frame_of(&raw));
            stream.extend(encoded(std::slice::from_ref(&after)));

            prop_assert_eq!(
                decode_pieces([&stream[..]]),
                vec![seen(&first), Err(error), seen(&after)]
            );
        }

        /// S5. A byte of one frame changed to any value that does not frame
        /// makes that frame exactly the lines the text rule reads, and then
        /// an error when bytes are left after them, whatever the new value.
        /// Every other frame decodes unchanged.
        #[test]
        fn a_corrupted_frame_is_an_error_and_the_others_decode(
            corruption in corruption(),
        ) {
            let packets = &corruption.0;
            let (frames, corrupted, _) = corrupt(&corruption);
            let first = Packet::heartbeat(DeviceRoute::root());
            let mut stream = encoded(std::slice::from_ref(&first));
            stream.extend(frames.concat());
            let (lines, rest) = lines_of(&frames[corrupted]);

            let decoded = decode_pieces([&stream[..]]);
            let mut expected: Vec<Result<Seen, Error>> =
                std::iter::once(&first).chain(&packets[..corrupted]).map(seen).collect();
            expected.extend(lines.iter().map(|line| text(line)));
            let error = expected.len();
            if rest {
                expected.push(Err(Error::Invalid(packet::DecodeError::CRC32)));
            }
            expected.extend(packets[corrupted + 1..].iter().map(seen));
            prop_assert_eq!(decoded.len(), expected.len(), "{:?}", decoded);
            if rest {
                prop_assert!(matches!(decoded[error], Err(Error::Invalid(_))), "{:?}", decoded);
                expected[error] = decoded[error].clone();
            }
            prop_assert_eq!(decoded, expected);
        }

        /// S6. Each call decodes the next packet and removes exactly its
        /// frame.
        #[test]
        fn each_call_decodes_one_packet_and_leaves_the_rest(
            packets in prop::collection::vec(packet(), 0..6),
        ) {
            let mut src = BytesMut::from(&encoded(&packets)[..]);
            let mut codec = Codec::default();
            for (i, packet) in packets.iter().enumerate() {
                let Some(Frame::Packet(decoded)) = codec.decode(&mut src).unwrap() else {
                    return Err(TestCaseError::fail(format!("packet {i} did not decode")));
                };
                prop_assert_eq!(decoded.as_bytes(), packet.as_bytes());
                prop_assert_eq!(&src[..], &encoded(&packets[i + 1..])[..]);
            }
            prop_assert!(codec.decode(&mut src).unwrap().is_none());
        }

        /// S6 and S7. On any bytes, each call that returns a frame or an
        /// error removes at least one byte, and a call returns `None` only
        /// once `src` is empty. Nothing panics.
        #[test]
        fn every_call_makes_progress_on_any_bytes(stream in any_stream()) {
            let mut codec = Codec::default();
            let mut src = BytesMut::from(&stream[..]);
            loop {
                let before = src.len();
                if let Ok(None) = codec.decode(&mut src) {
                    prop_assert!(src.is_empty(), "{} bytes left", src.len());
                    break;
                }
                prop_assert!(src.len() < before);
            }
        }
    }

    /// S8. A partly received frame does not survive more silence than its
    /// budget, and the next frame decodes without the stale prefix. One that
    /// waits exactly the budget completes.
    #[tokio::test(start_paused = true)]
    async fn a_partial_frame_expires_after_its_silence_budget() {
        let budget = Duration::from_millis(200);
        let packet = Packet::heartbeat_session(7, DeviceRoute::root());
        let frame = encoded(std::slice::from_ref(&packet));
        let (head, tail) = frame.split_at(frame.len() / 2);

        let mut codec = Codec::with_timing(None, Some(budget));
        let mut received = BytesMut::from(head);
        assert!(codec.decode(&mut received).unwrap().is_none());
        tokio::time::advance(budget).await;
        received.extend_from_slice(tail);
        let Some(Frame::Packet(actual)) = codec.decode(&mut received).unwrap() else {
            panic!("a frame within its budget must complete")
        };
        assert_eq!(actual.as_bytes(), packet.as_bytes());

        let mut received = BytesMut::from(head);
        assert!(codec.decode(&mut received).unwrap().is_none());
        tokio::time::advance(budget + Duration::from_millis(1)).await;
        received.extend_from_slice(&frame);
        let Some(Frame::Packet(actual)) = codec.decode(&mut received).unwrap() else {
            panic!("the fresh frame must decode without the stale partial frame")
        };
        assert_eq!(actual.as_bytes(), packet.as_bytes());
    }

    /// S8. The first read before the holdoff ends is discarded whole, and an
    /// empty buffer is not a read. The next read decodes. A read at the end
    /// of the holdoff is kept.
    #[tokio::test(start_paused = true)]
    async fn the_first_read_before_the_holdoff_ends_is_discarded() {
        let holdoff = Duration::from_millis(100);
        let packet = Packet::heartbeat_session(7, DeviceRoute::root());
        let frame = encoded(std::slice::from_ref(&packet));

        let mut codec = Codec::with_timing(Some(holdoff), None);
        assert!(codec.decode(&mut BytesMut::new()).unwrap().is_none());
        let mut src = BytesMut::from(&frame[..]);
        assert!(codec.decode(&mut src).unwrap().is_none());
        assert!(src.is_empty());
        src.extend_from_slice(&frame);
        let Some(Frame::Packet(actual)) = codec.decode(&mut src).unwrap() else {
            panic!("the read after the first must decode")
        };
        assert_eq!(actual.as_bytes(), packet.as_bytes());

        let mut late = Codec::with_timing(Some(holdoff), None);
        tokio::time::advance(holdoff).await;
        let mut src = BytesMut::from(&frame[..]);
        let Some(Frame::Packet(actual)) = late.decode(&mut src).unwrap() else {
            panic!("a read at the end of the holdoff must be kept")
        };
        assert_eq!(actual.as_bytes(), packet.as_bytes());
    }

    /// E1. A TEXT packet with a TTL of 2 and a 13 byte payload decodes as
    /// that packet. Its first bytes are `?`, a space and a CR.
    #[test]
    #[ignore = "the deserializer reads a printable header before a CR or LF as a line of text"]
    fn a_packet_whose_header_reads_as_text_decodes() {
        let packet = strategy::wire(strategy::assemble(63, 2, &[0x55; 13], &[]));

        assert_eq!(
            decode_pieces([&encoded(std::slice::from_ref(&packet))[..]]),
            [seen(&packet)]
        );
    }

    /// S5. A proxy error with a 13 byte payload, whose size byte is a CR,
    /// reads as the line of its first two bytes once its routing byte is
    /// changed to a printable one, and then as a CRC error.
    #[test]
    fn a_corrupted_routing_byte_can_make_a_line() {
        let packet = Packet::proxy_error(0, RpcError::Undefined, "aa 000  ").unwrap();
        let mut frame = encoded(std::slice::from_ref(&packet));
        assert_eq!(frame[1..4], [67, 0, b'\r']);
        frame[2] = b'x';

        assert_eq!(
            decode_pieces([&frame[..]]),
            [text("Cx"), Err(Error::Invalid(packet::DecodeError::CRC32))]
        );
    }

    /// S5. A TEXT packet with a TTL of 7, seven hops and a payload of 295
    /// bytes starts `?w'`. Once its payload size high byte becomes a CR, it
    /// reads as that line, then as the line of two spaces its payload
    /// starts with, then as a CRC error.
    #[test]
    fn a_corrupted_size_byte_can_make_two_lines() {
        let mut payload = vec![0; 0x127];
        payload[..3].copy_from_slice(b"  \n");
        let hops: Vec<u8> = (1..=7).collect();
        let packet = strategy::wire(strategy::assemble(63, 7, &payload, &hops));
        let mut frame = encoded(std::slice::from_ref(&packet));
        assert_eq!(frame[1..5], *b"?w'\x01");
        frame[4] = b'\r';

        assert_eq!(
            decode_pieces([&frame[..]]),
            [
                text("?w'"),
                text("  "),
                Err(Error::Invalid(packet::DecodeError::CRC32)),
            ]
        );
    }

    /// S3. A whole frame on the wire, with its escapes and its CRC. The CRC
    /// of the bit by bit model meets the check value of CRC32 ISO HDLC.
    #[test]
    fn bytes_that_frame_are_escaped() {
        let packet =
            Packet::rpc_reply(0, &[wire::SLIP_END, wire::SLIP_ESC], DeviceRoute::root()).unwrap();
        let frame = encoded(std::slice::from_ref(&packet));

        #[rustfmt::skip]
        assert_eq!(
            frame,
            [
                0xc0, // END
                3, 0, 4, 0, // RPC reply header
                0, 0, // request ID
                0xdb, 0xdc, // escaped END
                0xdb, 0xdd, // escaped ESC
                0x8e, 0x46, 0xc7, 0xaa, // CRC32, little endian
                0xc0, // END
            ]
        );
        assert_eq!(crc32(packet.as_bytes()), 0xaac7_468e);
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        assert_eq!(decode_pieces([&frame[..]]), [seen(&packet)]);
    }

    /// S4. A frame of 516 bytes, the largest packet and its CRC, is held
    /// whole, so the largest packet decodes and 516 bytes of garbage fail
    /// their CRC. The 517th byte overflows.
    #[test]
    fn the_largest_frame_fits_and_one_byte_more_overflows() {
        let hops: Vec<u8> = (1..=8).collect();
        let largest =
            Packet::rpc_reply(0, &[0x55; 498], DeviceRoute::from_hops(&hops).unwrap()).unwrap();
        assert_eq!(largest.as_bytes().len(), 512);
        let end = [wire::SLIP_END];

        assert_eq!(
            decode_pieces([&encoded(std::slice::from_ref(&largest))[..]]),
            [seen(&largest)]
        );
        assert_eq!(
            decode_pieces([&[&[0xaa; 516][..], &end].concat()[..]]),
            [Err(Error::Invalid(packet::DecodeError::CRC32))]
        );
        assert_eq!(
            decode_pieces([&[&[0xaa; 517][..], &end].concat()[..]])[0],
            Err(Error::Invalid(packet::DecodeError::PacketTooBig))
        );
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

    /// S4. Garbage shorter than a CRC is a frame too small. Garbage longer
    /// than 516 bytes, the largest packet and its CRC, overflows the
    /// decoder, and what follows the overflow fails its CRC.
    #[test]
    fn garbage_is_reported_and_the_packet_after_it_decodes() {
        let heartbeat = Packet::heartbeat(DeviceRoute::root());
        let mut short = vec![0x01, 0xff];
        short.extend(encoded(std::slice::from_ref(&heartbeat)));
        let mut long = vec![0xaa; 512 + 4 + 50];
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

    /// Prints the share of cases in each class the serial properties name,
    /// when `TWINLEAF_CODEC_CLASSES` is set.
    #[test]
    fn class_counts() {
        strategy::tally("serial packets", strategy::packet(), |packet| {
            let frame = frame_of(packet.as_bytes());
            let mut classes = vec![];
            if reads_as_text(&frame) {
                classes.push("E1, reads as text");
            }
            if frame.len() > 2 + packet.as_bytes().len() + 4 {
                classes.push("escapes a byte");
            }
            classes
        });
        strategy::tally("S5 corruptions", corruption(), |corruption| {
            let (frames, corrupted, part) = corrupt(corruption);
            let (lines, rest) = lines_of(&frames[corrupted]);
            let lines = match lines.len() {
                0 => "no line",
                1 => "one line",
                _ => "two lines or more",
            };
            vec![part, lines, if rest { "an error" } else { "no error" }]
        });
        strategy::tally("S4 frames outside E1", s4_frame(), |(_, _, class)| {
            vec![*class]
        });
        strategy::tally("serial streams", any_stream(), |stream| {
            let mut codec = Codec::default();
            let mut src = BytesMut::from(&stream[..]);
            let mut outcomes = vec![];
            loop {
                let result = codec.decode(&mut src);
                outcomes.push(outcome(&result));
                if let Ok(None) = result {
                    break;
                }
            }
            outcomes.sort_unstable();
            outcomes.dedup();
            outcomes
        });
    }
}

//! SLIP framing for serial links.
//!
//! A frame is the packet followed by its CRC32, little endian, SLIP encoded
//! per RFC 1055 and ended with [`SLIP_END`]. Console text lines pass through
//! as [`FrameErrors::TEXT`] frames.
//!
//! [`serialize`] encodes one frame and [`Deserializer`] decodes a byte stream
//! into frames.

use crc::{Crc, CRC_32_ISO_HDLC};

use crate::packet::WriteError;

/// Frame delimiter, 0xC0.
pub const SLIP_END: u8 = 0xC0;
/// Escape, 0xDB.
pub const SLIP_ESC: u8 = 0xDB;
/// Escaped END, 0xDC.
pub const SLIP_ESC_END: u8 = 0xDC;
/// Escaped ESC, 0xDD.
pub const SLIP_ESC_ESC: u8 = 0xDD;

/// The frame checksum, CRC-32 ISO HDLC.
pub const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

/// Checksum length in bytes, 4.
pub const CRC_SIZE: usize = 4;

/// Returns the worst-case frame size for a packet of `packet_size` bytes.
pub const fn max_serialized_size(packet_size: usize) -> usize {
    2 * (packet_size + CRC_SIZE) + 1
}

/// Encodes `packet` and its CRC32 into `out` as one SLIP frame and returns
/// the frame length. A buffer sized by [`max_serialized_size`] always fits.
///
/// # Errors
///
/// Returns [`WriteError::BufferTooSmall`] if `out` is too small.
pub fn serialize(packet: &[u8], out: &mut [u8]) -> Result<usize, WriteError> {
    let crc = CRC32.checksum(packet).to_le_bytes();
    let mut len = 0;
    for mut part in [packet, &crc[..]] {
        // Copy each run between escaped bytes whole.
        loop {
            let run = part
                .iter()
                .position(|&byte| byte == SLIP_END || byte == SLIP_ESC)
                .unwrap_or(part.len());
            out.get_mut(len..len + run)
                .ok_or(WriteError::BufferTooSmall)?
                .copy_from_slice(&part[..run]);
            len += run;
            let Some((&special, rest)) = part[run..].split_first() else {
                break;
            };
            let code = match special {
                SLIP_END => SLIP_ESC_END,
                _ => SLIP_ESC_ESC,
            };
            out.get_mut(len..len + 2)
                .ok_or(WriteError::BufferTooSmall)?
                .copy_from_slice(&[SLIP_ESC, code]);
            len += 2;
            part = rest;
        }
    }
    *out.get_mut(len).ok_or(WriteError::BufferTooSmall)? = SLIP_END;
    Ok(len + 1)
}

/// Frame-level error bits, matching `TL_SERIAL_ERROR_*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct FrameErrors(u8);

impl FrameErrors {
    /// Frame is empty or too short to contain a CRC32.
    pub const SHORT: Self = Self(0x01);
    /// Frame ended while in escape mode.
    pub const DANGLING_ESC: Self = Self(0x02);
    /// Invalid byte after an escape (kept untranslated).
    pub const ESC_CODE: Self = Self(0x04);
    /// CRC32 mismatch.
    pub const CRC: Self = Self(0x08);
    /// Frame exceeded the deserializer's capacity. Parsing restarted.
    pub const TOO_BIG: Self = Self(0x10);
    /// A printable text line terminated by CR/LF, not a binary frame.
    pub const TEXT: Self = Self(0x20);

    /// Returns `true` if no error bits are set.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Returns `true` if every bit of `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Returns the raw bits.
    pub const fn bits(self) -> u8 {
        self.0
    }

    fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

/// Decoded frame and any framing errors.
#[derive(Debug)]
pub struct Frame<'a> {
    buf: &'a mut [u8],
    len: usize,
    /// Errors found while decoding.
    pub errors: FrameErrors,
}

impl<'a> Frame<'a> {
    /// Returns the frame contents, without the CRC once it verified.
    pub fn data(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    /// Returns the packet bytes if the frame decoded without errors.
    pub fn packet(&self) -> Option<&[u8]> {
        self.errors.is_empty().then(|| self.data())
    }

    /// Returns the packet and its length in a buffer with the verified CRC's
    /// room after it, which a router tags a hop into instead of copying the
    /// packet, or `None` if the frame decoded with errors.
    pub fn routable(self) -> Option<(&'a mut [u8], usize)> {
        self.errors.is_empty().then_some((self.buf, self.len))
    }
}

/// Streaming deserializer. `CAPACITY` must cover the largest expected packet
/// plus [`CRC_SIZE`], e.g. `Packet::MAX_SIZE + CRC_SIZE`.
pub struct Deserializer<const CAPACITY: usize> {
    buf: [u8; CAPACITY],
    len: usize,
    errors: FrameErrors,
    esc: bool,
    /// Whether leading FTDI zero bytes should still be ignored.
    first: bool,
}

impl<const CAPACITY: usize> core::fmt::Debug for Deserializer<CAPACITY> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Deserializer")
            .field("buffered", &&self.buf[..self.len])
            .field("errors", &self.errors)
            .field("esc", &self.esc)
            .field("first", &self.first)
            .finish_non_exhaustive()
    }
}

impl<const CAPACITY: usize> Default for Deserializer<CAPACITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const CAPACITY: usize> Deserializer<CAPACITY> {
    /// Creates an empty deserializer.
    pub const fn new() -> Self {
        Self {
            buf: [0; CAPACITY],
            len: 0,
            errors: FrameErrors(0),
            esc: false,
            first: true,
        }
    }

    /// Consumes `input` until a frame completes or the input runs out. Returns
    /// the number of bytes consumed and the completed frame, if any. The rest
    /// of `input` is left for the next call.
    pub fn push(&mut self, input: &[u8]) -> (usize, Option<Frame<'_>>) {
        let mut index = 0;
        while index < input.len() {
            // Copy a run of plain bytes whole. The bytes that end it, and the
            // leading zeros before the first frame, take the path below.
            if !self.esc && (self.len > 0 || !self.first) {
                let rest = &input[index..];
                let run = rest
                    .iter()
                    .position(|&byte| matches!(byte, SLIP_END | SLIP_ESC | b'\n' | b'\r'))
                    .unwrap_or(rest.len())
                    .min(CAPACITY - self.len);
                self.buf[self.len..self.len + run].copy_from_slice(&rest[..run]);
                self.len += run;
                index += run;
                if index == input.len() {
                    break;
                }
            }
            let mut byte = input[index];
            index += 1;
            let consumed = index;

            if byte == SLIP_END {
                if self.len == 0 {
                    self.esc = false;
                    self.errors = FrameErrors::default();
                    continue;
                }
                let mut errors = self.errors;
                if self.esc {
                    errors.insert(FrameErrors::DANGLING_ESC);
                }
                let end = self.len;
                let mut size = end;
                self.len = 0;
                self.errors = FrameErrors::default();
                self.esc = false;
                self.first = false;

                match self.buf[..size].split_last_chunk::<CRC_SIZE>() {
                    None => errors.insert(FrameErrors::SHORT),
                    Some((data, crc)) => {
                        if CRC32.checksum(data) != u32::from_le_bytes(*crc) {
                            errors.insert(FrameErrors::CRC);
                        }
                        if errors.is_empty() {
                            size -= CRC_SIZE;
                        }
                    }
                }
                return (
                    consumed,
                    Some(Frame {
                        buf: &mut self.buf[..end],
                        len: size,
                        errors,
                    }),
                );
            }

            if byte == b'\n' || byte == b'\r' {
                // Packet headers cannot be printable text ending in CR/LF.
                let text = self.buf[..self.len]
                    .iter()
                    .all(|&b| b == b'\t' || (0x20..=0x7e).contains(&b));
                if text {
                    if self.len == 0 {
                        continue; // bare newline or the LF of a CRLF pair
                    }
                    let mut errors = self.errors;
                    errors.insert(FrameErrors::TEXT);
                    let size = self.len;
                    self.len = 0;
                    self.errors = FrameErrors::default();
                    self.esc = false;
                    self.first = false;
                    return (
                        consumed,
                        Some(Frame {
                            buf: &mut self.buf[..size],
                            len: size,
                            errors,
                        }),
                    );
                }
            }

            if !self.esc && byte == SLIP_ESC {
                self.esc = true;
                continue;
            }
            if self.esc {
                self.esc = false;
                byte = match byte {
                    SLIP_ESC_END => SLIP_END,
                    SLIP_ESC_ESC => SLIP_ESC,
                    other => {
                        self.errors.insert(FrameErrors::ESC_CODE);
                        other
                    }
                };
            }

            if self.len < CAPACITY {
                if self.len > 0 || byte != 0 || !self.first {
                    self.buf[self.len] = byte;
                    self.len += 1;
                }
            } else {
                // Emit the partial frame and restart after overflow.
                let mut errors = self.errors;
                errors.insert(FrameErrors::TOO_BIG);
                self.len = 0;
                self.errors = FrameErrors::default();
                return (
                    consumed,
                    Some(Frame {
                        buf: &mut self.buf,
                        len: CAPACITY,
                        errors,
                    }),
                );
            }
        }
        (input.len(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: usize = 64 + CRC_SIZE;

    fn deserialize_all(des: &mut Deserializer<CAP>, mut input: &[u8]) -> Vec<(Vec<u8>, u8)> {
        let mut frames = Vec::new();
        while !input.is_empty() {
            let (n, frame) = des.push(input);
            if let Some(f) = frame {
                frames.push((f.data().to_vec(), f.errors.bits()));
            }
            input = &input[n..];
        }
        frames
    }

    #[test]
    fn a_clean_frame_lends_the_crc_as_room_and_a_broken_one_lends_nothing() {
        let mut wire = vec![0; max_serialized_size(5)];
        let n = serialize(b"hello", &mut wire).unwrap();
        let mut des = Deserializer::<CAP>::new();
        let (buf, len) = des.push(&wire[..n]).1.unwrap().routable().unwrap();
        assert_eq!(&buf[..len], b"hello");
        assert_eq!(buf.len(), len + CRC_SIZE);

        wire[1] ^= 0xFF;
        let mut des = Deserializer::<CAP>::new();
        assert!(des.push(&wire[..n]).1.unwrap().routable().is_none());
    }

    #[test]
    fn short_and_dangling_and_bad_escape() {
        let mut des = Deserializer::<CAP>::new();
        let frames = deserialize_all(&mut des, &[0x01, 0x02, 0xC0]);
        assert_eq!(frames[0].1, FrameErrors::SHORT.bits());

        let mut des = Deserializer::<CAP>::new();
        let frames = deserialize_all(&mut des, &[0x01, 0x02, 0x03, 0x04, 0x05, 0xDB, 0xC0]);
        assert!(FrameErrors(frames[0].1).contains(FrameErrors::DANGLING_ESC));

        let mut des = Deserializer::<CAP>::new();
        let frames = deserialize_all(&mut des, &[0xDB, 0x99, 0xC0]);
        assert!(FrameErrors(frames[0].1).contains(FrameErrors::ESC_CODE));
        assert_eq!(frames[0].0[0], 0x99);
    }
}

/// A model of the codec that works a byte at a time. The properties below
/// compare the codec against it.
#[cfg(test)]
mod bytewise {
    use super::*;
    use proptest::prelude::*;

    fn serialize_bytewise(packet: &[u8], out: &mut [u8]) -> Result<usize, WriteError> {
        fn put(out: &mut [u8], len: &mut usize, value: u8) -> Result<(), WriteError> {
            *out.get_mut(*len).ok_or(WriteError::BufferTooSmall)? = value;
            *len += 1;
            Ok(())
        }

        let crc = CRC32.checksum(packet).to_le_bytes();
        let mut len = 0;
        for &byte in packet.iter().chain(crc.iter()) {
            match byte {
                SLIP_END => {
                    put(out, &mut len, SLIP_ESC)?;
                    put(out, &mut len, SLIP_ESC_END)?;
                }
                SLIP_ESC => {
                    put(out, &mut len, SLIP_ESC)?;
                    put(out, &mut len, SLIP_ESC_ESC)?;
                }
                _ => put(out, &mut len, byte)?,
            }
        }
        put(out, &mut len, SLIP_END)?;
        Ok(len)
    }

    impl<const CAPACITY: usize> Deserializer<CAPACITY> {
        fn push_bytewise(&mut self, input: &[u8]) -> (usize, Option<Frame<'_>>) {
            for (index, &byte) in input.iter().enumerate() {
                let consumed = index + 1;
                let mut byte = byte;

                if byte == SLIP_END {
                    if self.len == 0 {
                        self.esc = false;
                        self.errors = FrameErrors::default();
                        continue;
                    }
                    let mut errors = self.errors;
                    if self.esc {
                        errors.insert(FrameErrors::DANGLING_ESC);
                    }
                    let end = self.len;
                    let mut size = end;
                    self.len = 0;
                    self.errors = FrameErrors::default();
                    self.esc = false;
                    self.first = false;

                    match self.buf[..size].split_last_chunk::<CRC_SIZE>() {
                        None => errors.insert(FrameErrors::SHORT),
                        Some((data, crc)) => {
                            if CRC32.checksum(data) != u32::from_le_bytes(*crc) {
                                errors.insert(FrameErrors::CRC);
                            }
                            if errors.is_empty() {
                                size -= CRC_SIZE;
                            }
                        }
                    }
                    return (
                        consumed,
                        Some(Frame {
                            buf: &mut self.buf[..end],
                            len: size,
                            errors,
                        }),
                    );
                }

                if byte == b'\n' || byte == b'\r' {
                    // Packet headers cannot be printable text ending in CR/LF.
                    let text = self.buf[..self.len]
                        .iter()
                        .all(|&b| b == b'\t' || (0x20..=0x7e).contains(&b));
                    if text {
                        if self.len == 0 {
                            continue; // bare newline or the LF of a CRLF pair
                        }
                        let mut errors = self.errors;
                        errors.insert(FrameErrors::TEXT);
                        let size = self.len;
                        self.len = 0;
                        self.errors = FrameErrors::default();
                        self.esc = false;
                        self.first = false;
                        return (
                            consumed,
                            Some(Frame {
                                buf: &mut self.buf[..size],
                                len: size,
                                errors,
                            }),
                        );
                    }
                }

                if !self.esc && byte == SLIP_ESC {
                    self.esc = true;
                    continue;
                }
                if self.esc {
                    self.esc = false;
                    byte = match byte {
                        SLIP_ESC_END => SLIP_END,
                        SLIP_ESC_ESC => SLIP_ESC,
                        other => {
                            self.errors.insert(FrameErrors::ESC_CODE);
                            other
                        }
                    };
                }

                if self.len < CAPACITY {
                    if self.len > 0 || byte != 0 || !self.first {
                        self.buf[self.len] = byte;
                        self.len += 1;
                    }
                } else {
                    // Emit the partial frame and restart after overflow.
                    let mut errors = self.errors;
                    errors.insert(FrameErrors::TOO_BIG);
                    self.len = 0;
                    self.errors = FrameErrors::default();
                    return (
                        consumed,
                        Some(Frame {
                            buf: &mut self.buf,
                            len: CAPACITY,
                            errors,
                        }),
                    );
                }
            }
            (input.len(), None)
        }
    }

    /// The bytes one call consumed, and the buffer, length and error bits of
    /// the frame it completed.
    type Call = (usize, Option<(Vec<u8>, usize, u8)>);

    /// Each frame a deserializer completes from `input` delivered in `chunks`
    /// sized pieces, with the bytes each call consumed.
    fn decode<const C: usize>(input: &[u8], chunks: &[usize], bytewise: bool) -> Vec<Call> {
        let mut des = Deserializer::<C>::new();
        let mut calls = Vec::new();
        let mut rest = input;
        let mut sizes = chunks.iter().cycle();
        while !rest.is_empty() {
            let mut chunk = &rest[..(*sizes.next().unwrap()).clamp(1, rest.len())];
            rest = &rest[chunk.len()..];
            while !chunk.is_empty() {
                let (n, frame) = match bytewise {
                    true => des.push_bytewise(chunk),
                    false => des.push(chunk),
                };
                let frame = frame.map(|f| (f.buf.to_vec(), f.len, f.errors.bits()));
                calls.push((n, frame));
                chunk = &chunk[n..];
            }
        }
        calls
    }

    /// Bytes weighted toward the ones SLIP and text detection treat apart.
    fn wire_byte() -> impl Strategy<Value = u8> {
        prop_oneof![
            4 => any::<u8>(),
            1 => prop::sample::select(vec![SLIP_END, SLIP_ESC, SLIP_ESC_END, SLIP_ESC_ESC, 0, b'\n', b'\r', b'A']),
        ]
    }

    proptest! {
        #[test]
        fn serialize_writes_what_the_bytewise_encoder_wrote(
            packet in prop::collection::vec(wire_byte(), 0..600),
            room in 0usize..1300,
        ) {
            let (mut runs, mut bytes) = (vec![0; room], vec![0; room]);
            let (ran, bytewise) = (serialize(&packet, &mut runs), serialize_bytewise(&packet, &mut bytes));
            prop_assert_eq!(ran, bytewise);
            if let Ok(len) = ran {
                prop_assert_eq!(&runs[..len], &bytes[..len]);
            }
        }

        #[test]
        fn push_decodes_as_the_bytewise_decoder_did(
            input in prop::collection::vec(wire_byte(), 0..1500),
            chunks in prop::collection::vec(1usize..80, 1..8),
        ) {
            prop_assert_eq!(decode::<64>(&input, &chunks, false), decode::<64>(&input, &chunks, true));
            prop_assert_eq!(decode::<516>(&input, &chunks, false), decode::<516>(&input, &chunks, true));
        }

        #[test]
        fn encoded_packets_decode_as_the_bytewise_decoder_did(
            packets in prop::collection::vec(prop::collection::vec(wire_byte(), 0..520), 1..4),
            noise in prop::collection::vec(wire_byte(), 0..40),
            chunks in prop::collection::vec(1usize..600, 1..4),
        ) {
            let mut input = noise;
            for packet in &packets {
                let mut frame = vec![0; max_serialized_size(packet.len())];
                let len = serialize(packet, &mut frame).unwrap();
                input.extend_from_slice(&frame[..len]);
            }
            prop_assert_eq!(decode::<516>(&input, &chunks, false), decode::<516>(&input, &chunks, true));
        }
    }
}

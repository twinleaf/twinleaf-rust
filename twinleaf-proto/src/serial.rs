//! Twinleaf SLIP/CRC serial framing.
//!
//! The decoder also handles console text and the leading-zero workaround
//! some senders emit before a frame.

use crc::{Crc, CRC_32_ISO_HDLC};

pub const SLIP_END: u8 = 0xC0;
pub const SLIP_ESC: u8 = 0xDB;
pub const SLIP_ESC_END: u8 = 0xDC;
pub const SLIP_ESC_ESC: u8 = 0xDD;

pub const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

pub const CRC_SIZE: usize = 4;

/// Worst-case encoded size for a packet.
pub const fn max_serialized_size(packet_size: usize) -> usize {
    2 * (packet_size + CRC_SIZE) + 1
}

/// Serialize a packet and CRC32 as one SLIP frame.
pub fn serialize(packet: &[u8], out: &mut [u8]) -> Option<usize> {
    fn put(out: &mut [u8], len: &mut usize, value: u8) -> Option<()> {
        *out.get_mut(*len)? = value;
        *len += 1;
        Some(())
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
    Some(len)
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
    /// Frame exceeded the deserializer's capacity; parsing restarted.
    pub const TOO_BIG: Self = Self(0x10);
    /// A printable text line terminated by CR/LF, not a binary frame.
    pub const TEXT: Self = Self(0x20);

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

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
    pub data: &'a [u8],
    pub errors: FrameErrors,
}

impl<'a> Frame<'a> {
    /// The packet bytes, if this frame deserialized cleanly.
    pub fn packet(&self) -> Option<&'a [u8]> {
        if self.errors.is_empty() {
            Some(self.data)
        } else {
            None
        }
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

impl<const CAPACITY: usize> Default for Deserializer<CAPACITY> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const CAPACITY: usize> Deserializer<CAPACITY> {
    pub const fn new() -> Self {
        Self {
            buf: [0; CAPACITY],
            len: 0,
            errors: FrameErrors(0),
            esc: false,
            first: true,
        }
    }

    /// Consume input until a frame completes or the input is exhausted.
    pub fn push(&mut self, input: &[u8]) -> (usize, Option<Frame<'_>>) {
        for (index, &byte) in input.iter().enumerate() {
            let consumed = index + 1;
            let mut byte = byte;

            if byte == SLIP_END {
                if self.len == 0 {
                    continue;
                }
                let mut errors = self.errors;
                if self.esc {
                    errors.insert(FrameErrors::DANGLING_ESC);
                }
                let mut size = self.len;
                self.len = 0;
                self.errors = FrameErrors::default();
                self.esc = false;
                self.first = false;

                if size < CRC_SIZE {
                    errors.insert(FrameErrors::SHORT);
                } else {
                    let (data, crc) = self.buf[..size].split_at(size - CRC_SIZE);
                    if CRC32.checksum(data) != u32::from_le_bytes(crc.try_into().unwrap()) {
                        errors.insert(FrameErrors::CRC);
                    }
                    if errors.is_empty() {
                        size -= CRC_SIZE;
                    }
                }
                return (
                    consumed,
                    Some(Frame {
                        data: &self.buf[..size],
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
                            data: &self.buf[..size],
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
                        data: &self.buf,
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

    #[test]
    fn crc32_is_the_iso_hdlc_variant() {
        assert_eq!(CRC32.checksum(b"123456789"), 0xCBF4_3926);
    }

    const CAP: usize = 64 + CRC_SIZE;

    fn deserialize_all(des: &mut Deserializer<CAP>, mut input: &[u8]) -> Vec<(Vec<u8>, u8)> {
        let mut frames = Vec::new();
        while !input.is_empty() {
            let (n, frame) = des.push(input);
            if let Some(f) = frame {
                frames.push((f.data.to_vec(), f.errors.bits()));
            }
            input = &input[n..];
        }
        frames
    }

    fn round_trip(packet: &[u8]) -> Vec<u8> {
        let mut wire = vec![0; max_serialized_size(packet.len())];
        let n = serialize(packet, &mut wire).unwrap();
        wire.truncate(n);
        let mut des = Deserializer::<CAP>::new();
        let frames = deserialize_all(&mut des, &wire);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].1, 0, "unexpected frame errors");
        frames[0].0.clone()
    }

    #[test]
    fn round_trips_plain_and_escaped_packets() {
        assert_eq!(round_trip(b"hello"), b"hello");
        let nasty = [0xC0, 0xDB, 0x00, 0xC0, 0xDC, 0xDD, 0xDB];
        assert_eq!(round_trip(&nasty), nasty);
    }

    #[test]
    fn wire_format_matches_serial_proto_c() {
        let packet = [0x01, 0xC0, 0xDB];
        let crc = CRC32.checksum(&packet).to_le_bytes();
        let mut wire = vec![0; max_serialized_size(packet.len())];
        let n = serialize(&packet, &mut wire).unwrap();
        let mut expected = vec![0x01, 0xDB, 0xDC, 0xDB, 0xDD];
        for b in crc {
            match b {
                SLIP_END => expected.extend([SLIP_ESC, SLIP_ESC_END]),
                SLIP_ESC => expected.extend([SLIP_ESC, SLIP_ESC_ESC]),
                _ => expected.push(b),
            }
        }
        expected.push(SLIP_END);
        assert_eq!(&wire[..n], expected);
    }

    #[test]
    fn serialize_rejects_short_buffer() {
        let mut small = [0u8; 4];
        assert!(serialize(b"hello", &mut small).is_none());
    }

    #[test]
    fn byte_at_a_time_delivery() {
        let packet = [0xC0, 0x42, 0xDB];
        let mut wire = vec![0; max_serialized_size(packet.len())];
        let n = serialize(&packet, &mut wire).unwrap();
        let mut des = Deserializer::<CAP>::new();
        let mut frames = Vec::new();
        for &b in &wire[..n] {
            if let (_, Some(f)) = des.push(&[b]) {
                frames.push((f.data.to_vec(), f.errors.bits()));
            }
        }
        assert_eq!(frames, vec![(packet.to_vec(), 0)]);
    }

    #[test]
    fn crc_corruption_reports_error_with_raw_bytes() {
        let mut wire = vec![0; max_serialized_size(4)];
        let n = serialize(b"data", &mut wire).unwrap();
        wire[1] ^= 0xFF; // corrupt payload; CRC no longer matches
        let mut des = Deserializer::<CAP>::new();
        let frames = deserialize_all(&mut des, &wire[..n]);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].1, FrameErrors::CRC.bits());
        assert_eq!(frames[0].0.len(), 4 + CRC_SIZE, "CRC not stripped on error");
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

    #[test]
    fn text_lines_are_surfaced_not_fatal() {
        let mut des = Deserializer::<CAP>::new();
        let frames = deserialize_all(&mut des, b"boot: hello v1.2\r\n");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].1, FrameErrors::TEXT.bits());
        assert_eq!(frames[0].0, b"boot: hello v1.2");

        let mut wire = vec![0; max_serialized_size(3)];
        let n = serialize(&[1, 2, 3], &mut wire).unwrap();
        let frames = deserialize_all(&mut des, &wire[..n]);
        assert_eq!(frames, vec![(vec![1, 2, 3], 0)]);
    }

    #[test]
    fn leading_zeros_skipped_only_before_first_frame() {
        let mut wire = vec![0; max_serialized_size(2)];
        let n = serialize(&[9, 9], &mut wire).unwrap();
        let mut des = Deserializer::<CAP>::new();
        let mut input = vec![0u8, 0, 0];
        input.extend_from_slice(&wire[..n]);
        let frames = deserialize_all(&mut des, &input);
        assert_eq!(frames, vec![(vec![9, 9], 0)]);

        let mut wire2 = vec![0; max_serialized_size(3)];
        let n2 = serialize(&[0, 7, 0], &mut wire2).unwrap();
        let frames = deserialize_all(&mut des, &wire2[..n2]);
        assert_eq!(frames, vec![(vec![0, 7, 0], 0)]);
    }

    #[test]
    fn empty_frames_ignored() {
        let mut des = Deserializer::<CAP>::new();
        assert!(deserialize_all(&mut des, &[0xC0, 0xC0, 0xC0]).is_empty());
    }

    #[test]
    fn oversized_frame_restarts_parser() {
        let mut des = Deserializer::<CAP>::new();
        let flood = vec![0x55u8; CAP + 10];
        let frames = deserialize_all(&mut des, &flood);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].1, FrameErrors::TOO_BIG.bits());
        assert_eq!(frames[0].0.len(), CAP);

        // Terminate the overflow remnant before the valid frame.
        let mut wire = vec![0xC0];
        let mut good = vec![0; max_serialized_size(2)];
        let n = serialize(&[1, 2], &mut good).unwrap();
        wire.extend_from_slice(&good[..n]);
        let frames = deserialize_all(&mut des, &wire);
        assert_eq!(frames.last().unwrap(), &(vec![1, 2], 0));
    }

    #[test]
    fn worst_case_size_bound_is_tight() {
        let packet = [SLIP_END; 8];
        let mut wire = vec![0; max_serialized_size(8)];
        let n = serialize(&packet, &mut wire).unwrap();
        assert!(n <= max_serialized_size(8));
    }
}

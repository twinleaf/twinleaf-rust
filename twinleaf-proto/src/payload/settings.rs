//! The `SETTING` packet, a setting change broadcast.
//!
//! The payload is `{ name_len: u8, flags: u8 }`, then a `u32le` sequence
//! number if `flags` has [`VERSION`], then the name, then the value. The value
//! runs to the end of the payload. Neither is NUL-terminated. The value is the
//! bytes the RPC of that name would return. The other `flags` bits are
//! reserved and sent as zero. [`Setting`] parses and writes the packet.

use crate::packet::{Header, Packet, PacketType};

/// Size of the `{ name_len, flags }` head before the name and value.
pub const SETTING_HEADER_SIZE: usize = 2;
/// Most bytes of name and value together in an unnumbered announcement
/// (`TL_SETTING_MAX_PAYLOAD_SIZE`).
pub const MAX_SETTING_SIZE: usize = Packet::MAX_PAYLOAD - SETTING_HEADER_SIZE;
/// The `flags` bit saying a sequence number follows the head, 0x01.
pub const VERSION: u8 = 0x01;
/// Size of the sequence number [`VERSION`] puts after the head.
pub const SEQUENCE_SIZE: usize = 4;

/// One setting announcement, as it travels in a `SETTING` packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setting<'a> {
    /// RPC name, not NUL-terminated. At most 255 bytes, the limit of its
    /// one-byte length field.
    pub name: &'a [u8],
    /// The device's `settings.version` once this change is counted, or `None`
    /// from firmware that does not number its announcements.
    pub seq: Option<u32>,
    /// The new value, as the RPC of that name would have replied it.
    pub reply: &'a [u8],
}

impl<'a> Setting<'a> {
    /// Parses an announcement from a `SETTING` packet payload, without the
    /// packet header. Returns `None` if the payload is truncated.
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        let (&name_len, rest) = payload.split_first()?;
        let (&flags, rest) = rest.split_first()?;
        let (seq, rest) = match flags & VERSION {
            0 => (None, rest),
            _ => rest
                .split_first_chunk()
                .map(|(seq, rest)| (Some(u32::from_le_bytes(*seq)), rest))?,
        };
        let (name, reply) = rest.split_at_checked(name_len as usize)?;
        Some(Self { name, seq, reply })
    }

    /// Returns the name as text, or `None` if it is not UTF-8.
    pub fn name_str(&self) -> Option<&'a str> {
        core::str::from_utf8(self.name).ok()
    }

    /// Returns the payload length. Returns `None` if the name is longer than
    /// 255 bytes or the payload exceeds [`Packet::MAX_PAYLOAD`].
    pub fn payload_len(&self) -> Option<usize> {
        if self.name.len() > u8::MAX as usize {
            return None;
        }
        let len = self.head_len() + self.name.len() + self.reply.len();
        (len <= Packet::MAX_PAYLOAD).then_some(len)
    }

    /// Writes a full `SETTING` packet into `buf`. Returns its length, or `None`
    /// if it does not fit or the name exceeds 255 bytes.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let payload_len = self.payload_len()?;
        let total = Header::SIZE + payload_len;
        if buf.len() < total {
            return None;
        }
        let hdr = Header::new(PacketType::SETTING, payload_len as u16);
        hdr.write(buf.first_chunk_mut()?);
        self.write_payload(&mut buf[Header::SIZE..total])?;
        Some(total)
    }

    /// Writes the payload alone, without a packet header, into `out`. Returns
    /// its length, or `None` if it does not fit or the name exceeds 255 bytes.
    pub fn write_payload(&self, out: &mut [u8]) -> Option<usize> {
        let len = self.payload_len()?;
        if out.len() < len {
            return None;
        }
        out[0] = self.name.len() as u8;
        out[1] = match self.seq {
            Some(seq) => {
                out[SETTING_HEADER_SIZE..][..SEQUENCE_SIZE].copy_from_slice(&seq.to_le_bytes());
                VERSION
            }
            None => 0,
        };
        let name_start = self.head_len();
        let value_start = name_start + self.name.len();
        out[name_start..value_start].copy_from_slice(self.name);
        out[value_start..len].copy_from_slice(self.reply);
        Some(len)
    }

    fn head_len(&self) -> usize {
        match self.seq {
            Some(_) => SETTING_HEADER_SIZE + SEQUENCE_SIZE,
            None => SETTING_HEADER_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::strategy::{buffer, bytes, len_to, slack, word, write_over_any};
    use proptest::prelude::*;

    /// The longest name a one-byte length field measures.
    const MAX_NAME: usize = u8::MAX as usize;

    /// A name's bytes, and the text they read as when that is known, `None`
    /// inside for bytes that are not UTF-8.
    type Name = (Vec<u8>, Option<Option<String>>);

    /// A name of up to `max` bytes, with the text it reads as when that is
    /// known. Half are bytes of any value, whose text is not known. A
    /// quarter are text that may hold multibyte characters, cut to `max`
    /// bytes on a character boundary, which read as that text. A quarter hold
    /// a 0xFF byte, which no UTF-8 text holds, and read as no text.
    fn name(max: usize) -> impl Strategy<Value = Name> {
        prop_oneof![
            2 => len_to(max).prop_flat_map(bytes).prop_map(|name| (name, None)),
            1 => "\\PC{0,128}".prop_map(move |mut text| {
                let mut end = text.len().min(max);
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                text.truncate(end);
                (text.as_bytes().to_vec(), Some(Some(text)))
            }),
            1 => (len_to(max - 1).prop_flat_map(bytes), any::<prop::sample::Index>()).prop_map(
                |(mut name, at)| {
                    name.insert(at.index(name.len() + 1), 0xFF);
                    (name, Some(None))
                }
            ),
        ]
    }

    /// A name with the text it reads as when that is known, a sequence
    /// number or none, and a value that together fit a payload. Two in five
    /// values fill the payload or leave one byte of it, two in five are
    /// empty or one byte long, and the rest are any length that fits.
    fn setting() -> impl Strategy<Value = (Name, Option<u32>, Vec<u8>)> {
        (name(MAX_NAME), prop::option::of(word())).prop_flat_map(|(name, seq)| {
            let head = SETTING_HEADER_SIZE + seq.map_or(0, |_| SEQUENCE_SIZE);
            let room = Packet::MAX_PAYLOAD - head - name.0.len();
            (Just(name), Just(seq), len_to(room).prop_flat_map(bytes))
        })
    }

    /// A name of up to one past the longest, a sequence number or none, and
    /// a value that brings the payload to the largest, one past it, or any
    /// length up to it.
    fn any_size() -> impl Strategy<Value = (Vec<u8>, Option<u32>, Vec<u8>)> {
        (
            prop_oneof![len_to(MAX_NAME + 1), Just(MAX_NAME)].prop_flat_map(bytes),
            prop::option::of(Just(0)),
        )
            .prop_flat_map(|(name, seq)| {
                let head = SETTING_HEADER_SIZE + seq.map_or(0, |_| SEQUENCE_SIZE);
                let room = Packet::MAX_PAYLOAD - head - name.len();
                let reply = prop_oneof![Just(room), Just(room + 1), 0..=room + 1];
                (Just(name), Just(seq), reply.prop_flat_map(bytes))
            })
    }

    /// A payload too short for a head, or a head whose flags are none, all,
    /// or any, whose name length is any, and a body that holds the name and
    /// sequence number it claims, a byte short of that, or a few bytes over.
    fn any_payload() -> impl Strategy<Value = Vec<u8>> {
        let head = (
            any::<u8>(),
            prop_oneof![Just(0), Just(VERSION), Just(u8::MAX), any::<u8>()],
            prop_oneof![Just(-1), Just(0), 1..=4isize],
        );
        prop_oneof![
            1 => (0..SETTING_HEADER_SIZE).prop_flat_map(bytes),
            4 => head.prop_flat_map(|(name_len, flags, over)| {
                let claimed = (flags & VERSION) as usize * SEQUENCE_SIZE + name_len as usize;
                let body = claimed.checked_add_signed(over).unwrap_or(0);
                bytes(body).prop_map(move |body| [vec![name_len, flags], body].concat())
            }),
        ]
    }

    proptest! {
        /// An announcement whose name is at most 255 bytes and whose payload
        /// is at most [`Packet::MAX_PAYLOAD`] bytes writes a `SETTING` packet
        /// of exactly its length, whose payload parses back to it, whatever
        /// the buffer held before. Its name reads as text when the name is
        /// UTF-8 and as none otherwise.
        #[test]
        fn a_setting_decodes_as_it_was_encoded(((name, text), seq, reply) in setting()) {
            let setting = Setting { name: &name, seq, reply: &reply };
            let head = SETTING_HEADER_SIZE + seq.map_or(0, |_| SEQUENCE_SIZE);
            let payload_len = head + name.len() + reply.len();
            prop_assert_eq!(setting.payload_len(), Some(payload_len));
            let (len, buf) = write_over_any(Header::SIZE + payload_len, |buf| setting.write(buf))?;
            prop_assert_eq!(len, Some(buf.len()));
            let header = Header::parse(buf[..Header::SIZE].try_into().unwrap()).unwrap();
            prop_assert_eq!(header, Header::new(PacketType::SETTING, payload_len as u16));
            prop_assert_eq!(Setting::parse(&buf[Header::SIZE..]), Some(setting));
            let (len, payload) = write_over_any(payload_len, |out| setting.write_payload(out))?;
            prop_assert_eq!(len, Some(payload_len));
            prop_assert_eq!(&payload[..], &buf[Header::SIZE..]);
            if let Some(text) = text {
                prop_assert_eq!(setting.name_str(), text.as_deref());
            }
        }

        /// A payload parses when it holds the head, the sequence number its
        /// flags announce, and the name its length claims. The value is the
        /// rest. Writing what was parsed gives back the same payload with the
        /// reserved flag bits cleared, whatever the buffer held before.
        #[test]
        fn any_payload_parses_when_it_holds_what_its_head_claims(payload in any_payload()) {
            let [name_len, flags, ..] = payload[..] else {
                prop_assert_eq!(Setting::parse(&payload), None);
                return Ok(());
            };
            let seq = flags & VERSION != 0;
            let claimed = SETTING_HEADER_SIZE + usize::from(seq) * SEQUENCE_SIZE + name_len as usize;
            let Some(setting) = Setting::parse(&payload) else {
                prop_assert!(payload.len() < claimed);
                return Ok(());
            };
            prop_assert!(payload.len() >= claimed);
            prop_assert_eq!(setting.name.len(), name_len as usize);
            prop_assert_eq!(setting.seq.is_some(), seq);
            let (len, written) = write_over_any(payload.len(), |out| setting.write_payload(out))?;
            prop_assert_eq!(len, Some(payload.len()));
            let mut canonical = payload.clone();
            canonical[1] = flags & VERSION;
            prop_assert_eq!(written, canonical);
        }

        /// An announcement is written when its name is at most 255 bytes, its
        /// payload at most [`Packet::MAX_PAYLOAD`] bytes, and the buffer holds
        /// it, and refused otherwise. The payload alone needs room for itself.
        #[test]
        fn a_setting_is_written_only_when_it_fits(
            (name, seq, reply) in any_size(),
            slack in slack(),
        ) {
            let setting = Setting { name: &name, seq, reply: &reply };
            let head = SETTING_HEADER_SIZE + seq.map_or(0, |_| SEQUENCE_SIZE);
            let payload_len = head + name.len() + reply.len();
            let valid = name.len() <= MAX_NAME && payload_len <= Packet::MAX_PAYLOAD;
            prop_assert_eq!(setting.payload_len(), valid.then_some(payload_len));

            let mut buf = buffer(Header::SIZE + payload_len, slack);
            let fits = valid && buf.len() >= Header::SIZE + payload_len;
            prop_assert_eq!(setting.write(&mut buf), fits.then_some(Header::SIZE + payload_len));
            let mut out = buffer(payload_len, slack);
            let fits = valid && out.len() >= payload_len;
            prop_assert_eq!(setting.write_payload(&mut out), fits.then_some(payload_len));
        }
    }

    /// Byte-exact packed layout of the setting payload behind its packet
    /// header (`type = TL_PTYPE_SETTING`, `payload_size` covering header, name,
    /// and value), with the RPC's reply moved in behind the name.
    #[test]
    fn matches_the_packed_wire_layout() {
        let setting = Setting {
            name: b"rpc.hash",
            seq: None,
            reply: &0x0102_0304u32.to_le_bytes(),
        };
        let mut buf = [0u8; 32];
        let len = setting.write(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            12, 0, 14, 0,                               // header: SETTING, no routing, payload 14
            8,                                          // name_len
            0,                                          // flags
            b'r', b'p', b'c', b'.', b'h', b'a', b's', b'h',
            0x04, 0x03, 0x02, 0x01,                     // the rpc.hash reply, verbatim
        ]);
    }

    /// What a receiver does with the packet: `name_len` bytes of name, then
    /// the rest of the payload as the value.
    #[test]
    fn decodes_name_and_value_from_the_payload() {
        let raw = [
            12, 0, 11, 0, // SETTING, no routing, payload_size 11
            8, 0, // name_len 8, flags 0
            b'd', b'e', b'v', b'.', b'n', b'a', b'm', b'e', //
            b'X',
        ];
        let header = Header::parse((&raw[..Header::SIZE]).try_into().unwrap()).unwrap();
        assert_eq!(header.ptype, PacketType::SETTING);
        assert_eq!(header.packet_len(), raw.len());

        let setting = Setting::parse(&raw[header.payload_range()]).unwrap();
        assert_eq!(setting.name_str(), Some("dev.name"));
        assert_eq!(
            setting.reply.len(),
            header.payload_size as usize - SETTING_HEADER_SIZE - setting.name.len()
        );
        assert_eq!(setting.reply, b"X");
    }

    /// Byte-exact: the head with [`VERSION`] set, the sequence number, then
    /// the name and value as before.
    #[test]
    fn a_numbered_announcement_puts_its_sequence_before_the_name() {
        let setting = Setting {
            name: b"dev.name",
            seq: Some(0x0102_0304),
            reply: b"X",
        };
        let mut buf = [0u8; 32];
        let len = setting.write(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            12, 0, 15, 0,           // header: SETTING, no routing, payload 15
            8,                      // name_len
            VERSION,                // flags
            0x04, 0x03, 0x02, 0x01, // sequence, little-endian
            b'd', b'e', b'v', b'.', b'n', b'a', b'm', b'e',
            b'X',
        ]);
        assert_eq!(Setting::parse(&buf[Header::SIZE..len]), Some(setting));
        assert_eq!(
            Setting::parse(&[0, VERSION, 1, 0, 0]),
            None,
            "a short sequence"
        );
    }

    #[test]
    fn max_setting_size_matches_the_wire() {
        // TL_PACKET_MAX_PAYLOAD_SIZE - sizeof(tl_rpc_setting_header)
        assert_eq!(MAX_SETTING_SIZE, 500 - 2);
    }
}

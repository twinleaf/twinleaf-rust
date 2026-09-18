//! SETTING packet, a setting change broadcast.
//!
//! Payload: `{ name_len: u8, flags: u8 }` then the name, then the value, which
//! runs to the end of the payload. Neither is NUL-terminated. The value is
//! the bytes the RPC of that name would return. `flags` is reserved and sent
//! as zero.

use crate::packet::{Header, Packet, PacketType};

/// `{ name_len, flags }` preceding the name and value.
pub const SETTING_HEADER_SIZE: usize = 2;
/// Name and value together, at most (`TL_SETTING_MAX_PAYLOAD_SIZE`).
pub const MAX_SETTING_SIZE: usize = Packet::MAX_PAYLOAD - SETTING_HEADER_SIZE;

/// One setting announcement, as it travels in a SETTING packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setting<'a> {
    /// RPC name, not NUL-terminated. At most 255 bytes: the length field is
    /// one byte.
    pub name: &'a [u8],
    /// Reserved; devices send zero.
    pub flags: u8,
    /// The new value, as the RPC of that name would have replied it.
    pub reply: &'a [u8],
}

impl<'a> Setting<'a> {
    /// Parse an announcement from a SETTING packet payload (packet header
    /// excluded).
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        let (&name_len, rest) = payload.split_first()?;
        let (&flags, rest) = rest.split_first()?;
        let (name, reply) = rest.split_at_checked(name_len as usize)?;
        Some(Self { name, flags, reply })
    }

    /// The name as text, for the hosts that index settings by name. `None` if
    /// a device sent something that is not UTF-8.
    pub fn name_str(&self) -> Option<&'a str> {
        core::str::from_utf8(self.name).ok()
    }

    /// Payload length; `None` when the name overflows its one-byte field or
    /// the two together overflow a packet.
    pub fn payload_len(&self) -> Option<usize> {
        if self.name.len() > u8::MAX as usize {
            return None;
        }
        let len = SETTING_HEADER_SIZE + self.name.len() + self.reply.len();
        (len <= Packet::MAX_PAYLOAD).then_some(len)
    }

    /// Write a full SETTING packet into `buf`. Returns its length, or None if
    /// it does not fit or the name exceeds 255 bytes.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        let payload_len = self.payload_len()?;
        let total = Header::SIZE + payload_len;
        if buf.len() < total {
            return None;
        }
        let hdr = Header::new(PacketType::SETTING, payload_len as u16);
        hdr.write((&mut buf[..Header::SIZE]).try_into().unwrap());
        self.write_payload(&mut buf[Header::SIZE..total])?;
        Some(total)
    }

    /// Write just the payload (no header) into `out`; returns its length.
    pub fn write_payload(&self, out: &mut [u8]) -> Option<usize> {
        let len = self.payload_len()?;
        if out.len() < len {
            return None;
        }
        out[0] = self.name.len() as u8;
        out[1] = self.flags;
        let value_start = SETTING_HEADER_SIZE + self.name.len();
        out[SETTING_HEADER_SIZE..value_start].copy_from_slice(self.name);
        out[value_start..len].copy_from_slice(self.reply);
        Some(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let setting = Setting {
            name: b"aux.data.decimation",
            flags: 0,
            reply: &100u32.to_le_bytes(),
        };
        let mut buf = [0u8; 64];
        let len = setting.write(&mut buf).unwrap();
        assert_eq!(len, Header::SIZE + SETTING_HEADER_SIZE + 19 + 4);
        assert_eq!(Setting::parse(&buf[Header::SIZE..len]), Some(setting));
        assert_eq!(setting.name_str(), Some("aux.data.decimation"));
    }

    /// Byte-exact packed layout of the setting payload behind its packet
    /// header (`type = TL_PTYPE_SETTING`, `payload_size` covering header, name,
    /// and value), with the RPC's reply moved in behind the name.
    #[test]
    fn matches_the_packed_wire_layout() {
        let setting = Setting {
            name: b"rpc.hash",
            flags: 0,
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

    /// The name is bytes on the wire; a host that wants text asks for it, and
    /// a device sending something else does not break the parse.
    #[test]
    fn a_non_utf8_name_still_parses() {
        let setting = Setting::parse(&[2, 0, 0xFF, 0xFE, 7]).unwrap();
        assert_eq!(setting.name, &[0xFF, 0xFE]);
        assert_eq!(setting.name_str(), None);
        assert_eq!(setting.reply, &[7]);
    }

    /// An action RPC replies with nothing, and so does its announcement.
    #[test]
    fn an_empty_value_is_a_valid_announcement() {
        let setting = Setting {
            name: b"dev.stop",
            flags: 0,
            reply: b"",
        };
        let mut buf = [0u8; 32];
        let len = setting.write(&mut buf).unwrap();
        assert_eq!(len, Header::SIZE + SETTING_HEADER_SIZE + 8);
        assert_eq!(Setting::parse(&buf[Header::SIZE..len]), Some(setting));
    }

    #[test]
    fn rejects_truncated_payloads() {
        assert_eq!(Setting::parse(&[]), None);
        assert_eq!(Setting::parse(&[4]), None, "no flags byte");
        assert_eq!(
            Setting::parse(&[4, 0, b'a']),
            None,
            "name shorter than claimed"
        );
    }

    #[test]
    fn rejects_an_overlong_name_and_an_oversize_payload() {
        let long_name = Setting {
            name: &[b'x'; 256],
            flags: 0,
            reply: b"",
        };
        assert_eq!(long_name.payload_len(), None);
        assert_eq!(long_name.write(&mut [0u8; Packet::MAX_SIZE]), None);

        let oversize = Setting {
            name: b"n",
            flags: 0,
            reply: &[0u8; MAX_SETTING_SIZE],
        };
        assert_eq!(oversize.write(&mut [0u8; Packet::MAX_SIZE]), None);

        // Name and value together fill the payload exactly.
        let full = Setting {
            reply: &[0u8; MAX_SETTING_SIZE - 1],
            ..oversize
        };
        let mut buf = [0u8; Packet::MAX_SIZE];
        assert_eq!(
            full.write(&mut buf),
            Some(Header::SIZE + Packet::MAX_PAYLOAD)
        );
        // A short buffer is refused rather than truncated.
        assert_eq!(full.write(&mut [0u8; 16]), None);
    }

    #[test]
    fn max_setting_size_matches_the_wire() {
        // TL_PACKET_MAX_PAYLOAD_SIZE - sizeof(tl_rpc_setting_header)
        assert_eq!(MAX_SETTING_SIZE, 500 - 2);
    }
}

//! HEARTBEAT packet.
//!
//! The payload is the sender's session id, 4 bytes little endian, or anything
//! else, usually empty. A hub sends empty heartbeats to keep a child port
//! awake, and a host sends one when it opens a serial port.

use crate::packet::{Header, Packet, PacketType};
use crate::SessionId;

/// Payload length of a session-announcing heartbeat, `sizeof(tl_session_id)`.
pub const SESSION_PAYLOAD_SIZE: usize = 4;

/// One heartbeat, as it travels in a HEARTBEAT packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heartbeat<'a> {
    /// The sender's session id, telling a receiver which boot it is talking to.
    Session(SessionId),
    /// Any other payload, including the empty one a keepalive carries.
    Any(&'a [u8]),
}

impl<'a> Heartbeat<'a> {
    /// Parse a heartbeat from a HEARTBEAT packet payload (packet header
    /// excluded). Never fails: a payload that is not a session id is
    /// [`Any`](Self::Any).
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        Some(match payload.try_into() {
            Ok(bytes) => Self::Session(SessionId::from_le_bytes(bytes)),
            Err(_) => Self::Any(payload),
        })
    }

    /// The session id this heartbeat announces, if it announces one.
    pub const fn session(&self) -> Option<SessionId> {
        match self {
            Self::Session(session) => Some(*session),
            Self::Any(_) => None,
        }
    }

    /// Serialize a full HEARTBEAT packet (header included) into `buf`; returns
    /// its length. `None` if `buf` is too small or the payload exceeds
    /// [`Packet::MAX_PAYLOAD`].
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        match self {
            Self::Session(session) => write_packet(buf, &session.to_le_bytes()),
            Self::Any(payload) => write_packet(buf, payload),
        }
    }
}

fn write_packet(buf: &mut [u8], payload: &[u8]) -> Option<usize> {
    if payload.len() > Packet::MAX_PAYLOAD {
        return None;
    }
    let total = Header::SIZE + payload.len();
    if buf.len() < total {
        return None;
    }
    let hdr = Header::new(PacketType::HEARTBEAT, payload.len() as u16);
    hdr.write((&mut buf[..Header::SIZE]).try_into().unwrap());
    buf[Header::SIZE..total].copy_from_slice(payload);
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for beat in [
            Heartbeat::Session(SessionId::new(0xDEAD_BEEF)),
            Heartbeat::Any(b""),
            Heartbeat::Any(b"hello"),
        ] {
            let mut buf = [0u8; 32];
            let len = beat.write(&mut buf).unwrap();
            assert_eq!(Heartbeat::parse(&buf[Header::SIZE..len]), Some(beat));
        }
    }

    /// Byte-exact: a HEARTBEAT header with no routing over the session id,
    /// little-endian.
    #[test]
    fn matches_the_packed_wire_layout() {
        let beat = Heartbeat::Session(SessionId::new(0x0102_0304));
        let mut buf = [0u8; 16];
        let len = beat.write(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            5, 0, 4, 0,             // header: HEARTBEAT, no routing, payload 4
            0x04, 0x03, 0x02, 0x01, // tl_session_id, little-endian
        ]);
    }

    /// The keepalive a hub sends a child port, which hosts also send to open
    /// a serial port (header bytes before the CRC).
    #[test]
    fn an_empty_heartbeat_is_a_bare_header() {
        let mut buf = [0u8; 8];
        let len = Heartbeat::Any(b"").write(&mut buf).unwrap();
        assert_eq!(&buf[..len], &[0x05, 0x00, 0x00, 0x00]);
    }

    /// Only a payload of exactly four bytes is a session id; anything else is
    /// carried through untouched rather than rejected.
    #[test]
    fn only_four_bytes_read_as_a_session() {
        assert_eq!(
            Heartbeat::parse(&[1, 0, 0, 0]),
            Some(Heartbeat::Session(SessionId::new(1)))
        );
        assert_eq!(
            Heartbeat::parse(&[1, 0, 0]),
            Some(Heartbeat::Any(&[1, 0, 0]))
        );
        assert_eq!(
            Heartbeat::parse(&[1, 0, 0, 0, 0]),
            Some(Heartbeat::Any(&[1, 0, 0, 0, 0]))
        );
        assert_eq!(Heartbeat::parse(&[]), Some(Heartbeat::Any(&[])));
        assert_eq!(SESSION_PAYLOAD_SIZE, 4);
    }

    #[test]
    fn rejects_a_short_buffer_and_an_oversize_payload() {
        let beat = Heartbeat::Session(SessionId::new(7));
        assert_eq!(beat.write(&mut [0u8; Header::SIZE + 3]), None);
        assert_eq!(beat.session(), Some(SessionId::new(7)));

        let oversize = Heartbeat::Any(&[0u8; Packet::MAX_PAYLOAD + 1]);
        assert_eq!(oversize.write(&mut [0u8; Packet::MAX_SIZE]), None);
        assert_eq!(oversize.session(), None);

        let full = Heartbeat::Any(&[0u8; Packet::MAX_PAYLOAD]);
        let mut buf = [0u8; Packet::MAX_SIZE];
        assert_eq!(
            full.write(&mut buf),
            Some(Header::SIZE + Packet::MAX_PAYLOAD)
        );
    }
}

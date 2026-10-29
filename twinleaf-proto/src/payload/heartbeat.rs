//! The `HEARTBEAT` packet, parsed and written by [`Heartbeat`].
//!
//! The payload is `{ session: u32le, settings: u32le }`, the sender's session
//! id and its settings sequence number, or the session id alone from firmware
//! that has no sequence, or anything else, usually empty. A hub sends empty
//! heartbeats to keep a child port awake, and a host sends one when it opens a
//! serial port.

use crate::packet::{Header, Packet, PacketType};
use crate::SessionId;

/// Payload length of a heartbeat carrying only a session, `sizeof(tl_session_id)`.
pub const SESSION_PAYLOAD_SIZE: usize = 4;
/// Payload length of a heartbeat carrying a session and a settings sequence.
pub const SETTINGS_PAYLOAD_SIZE: usize = 8;

/// One heartbeat, as it travels in a `HEARTBEAT` packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heartbeat<'a> {
    /// Which boot the sender is, and how far its settings have moved in it.
    Session {
        /// The session id, telling a receiver which boot it is talking to.
        session: SessionId,
        /// The sequence number of the sender's last setting announcement, or
        /// `None` from firmware that sends only the session.
        settings: Option<u32>,
    },
    /// Any other payload, including the empty one a keepalive carries.
    Any(&'a [u8]),
}

impl<'a> Heartbeat<'a> {
    /// Parses a `HEARTBEAT` payload. A 4- or 8-byte payload is a
    /// [`Session`](Self::Session) and any other is [`Any`](Self::Any). Always
    /// returns `Some`.
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        Some(match payload.split_first_chunk() {
            Some((session, settings))
                if matches!(payload.len(), SESSION_PAYLOAD_SIZE | SETTINGS_PAYLOAD_SIZE) =>
            {
                Self::Session {
                    session: SessionId::from_le_bytes(*session),
                    settings: settings.try_into().ok().map(u32::from_le_bytes),
                }
            }
            _ => Self::Any(payload),
        })
    }

    /// Returns the session id this heartbeat announces, if it announces one.
    pub const fn session(&self) -> Option<SessionId> {
        match self {
            Self::Session { session, .. } => Some(*session),
            Self::Any(_) => None,
        }
    }

    /// Returns the settings sequence number this heartbeat carries, if it carries one.
    pub const fn settings(&self) -> Option<u32> {
        match self {
            Self::Session { settings, .. } => *settings,
            Self::Any(_) => None,
        }
    }

    /// Writes a full `HEARTBEAT` packet into `buf`. Returns its length, or
    /// `None` if it does not fit.
    pub fn write(&self, buf: &mut [u8]) -> Option<usize> {
        match self {
            Self::Session {
                session,
                settings: None,
            } => write_packet(buf, &session.to_le_bytes()),
            Self::Session {
                session,
                settings: Some(settings),
            } => {
                let mut payload = [0; SETTINGS_PAYLOAD_SIZE];
                payload[..4].copy_from_slice(&session.to_le_bytes());
                payload[4..].copy_from_slice(&settings.to_le_bytes());
                write_packet(buf, &payload)
            }
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

    const fn beat(session: u32, settings: Option<u32>) -> Heartbeat<'static> {
        Heartbeat::Session {
            session: SessionId::new(session),
            settings,
        }
    }

    #[test]
    fn round_trip() {
        for beat in [
            beat(0xDEAD_BEEF, Some(7)),
            beat(0xDEAD_BEEF, None),
            Heartbeat::Any(b""),
            Heartbeat::Any(b"hello"),
        ] {
            let mut buf = [0u8; 32];
            let len = beat.write(&mut buf).unwrap();
            assert_eq!(Heartbeat::parse(&buf[Header::SIZE..len]), Some(beat));
        }
    }

    /// Byte-exact: a HEARTBEAT header with no routing over the session id
    /// and the settings sequence, little-endian.
    #[test]
    fn matches_the_packed_wire_layout() {
        let mut buf = [0u8; 16];
        let len = beat(0x0102_0304, Some(0x0A0B_0C0D))
            .write(&mut buf)
            .unwrap();
        #[rustfmt::skip]
        assert_eq!(&buf[..len], &[
            5, 0, 8, 0,             // header: HEARTBEAT, no routing, payload 8
            0x04, 0x03, 0x02, 0x01, // tl_session_id, little-endian
            0x0D, 0x0C, 0x0B, 0x0A, // settings sequence, little-endian
        ]);
        let len = beat(0x0102_0304, None).write(&mut buf).unwrap();
        assert_eq!(&buf[..len], &[5, 0, 4, 0, 0x04, 0x03, 0x02, 0x01]);
    }

    /// The keepalive a hub sends a child port, which hosts also send to open
    /// a serial port (header bytes before the CRC).
    #[test]
    fn an_empty_heartbeat_is_a_bare_header() {
        let mut buf = [0u8; 8];
        let len = Heartbeat::Any(b"").write(&mut buf).unwrap();
        assert_eq!(&buf[..len], &[0x05, 0x00, 0x00, 0x00]);
    }

    /// Four bytes are a session alone and eight a session and its settings
    /// sequence; anything else is carried through untouched rather than rejected.
    #[test]
    fn four_or_eight_bytes_read_as_a_session() {
        assert_eq!(Heartbeat::parse(&[1, 0, 0, 0]), Some(beat(1, None)));
        assert_eq!(
            Heartbeat::parse(&[1, 0, 0, 0, 2, 0, 0, 0]),
            Some(beat(1, Some(2)))
        );
        for other in [&[1, 0, 0][..], &[1, 0, 0, 0, 0], &[0; 9], &[]] {
            assert_eq!(Heartbeat::parse(other), Some(Heartbeat::Any(other)));
        }
        assert_eq!(beat(1, Some(2)).settings(), Some(2));
        assert_eq!(beat(1, None).settings(), None);
        assert_eq!(Heartbeat::Any(&[]).settings(), None);
    }

    #[test]
    fn rejects_a_short_buffer_and_an_oversize_payload() {
        let beat = beat(7, Some(1));
        assert_eq!(beat.write(&mut [0u8; Header::SIZE + 7]), None);
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

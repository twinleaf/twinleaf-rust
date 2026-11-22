//! The `HEARTBEAT` packet, parsed and written by [`Heartbeat`].
//!
//! The payload is `{ session: u32le, settings: u32le }`, the sender's session
//! id and its settings sequence number, or the session id alone from firmware
//! that has no sequence, or anything else, usually empty. A hub sends empty
//! heartbeats to keep a child port awake, and a host sends one when it opens a
//! serial port.

use crate::packet::{Header, PacketType, WriteError};
use crate::SessionId;

/// How often a device sends a heartbeat.
pub const PERIOD: core::time::Duration = core::time::Duration::from_millis(200);

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

    /// Writes a full `HEARTBEAT` packet into `buf` and returns its length.
    ///
    /// # Errors
    ///
    /// Returns [`WriteError::PayloadTooLarge`] if the payload exceeds
    /// [`Packet::MAX_PAYLOAD`], and [`WriteError::BufferTooSmall`] if `buf`
    /// cannot hold the packet.
    pub fn write(&self, buf: &mut [u8]) -> Result<usize, WriteError> {
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

fn write_packet(buf: &mut [u8], payload: &[u8]) -> Result<usize, WriteError> {
    let hdr = Header::for_payload(PacketType::HEARTBEAT, payload.len())?;
    let total = Header::SIZE + payload.len();
    if buf.len() < total {
        return Err(WriteError::BufferTooSmall);
    }
    hdr.write((&mut buf[..Header::SIZE]).try_into().unwrap());
    buf[Header::SIZE..total].copy_from_slice(payload);
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::Packet;
    use crate::payload::strategy::{buffer, bytes, bytes_to, slack, word, write_over_any};
    use proptest::prelude::*;

    const fn beat(session: u32, settings: Option<u32>) -> Heartbeat<'static> {
        Heartbeat::Session {
            session: SessionId::new(session),
            settings,
        }
    }

    /// The payload a session heartbeat carries.
    fn payload_of(session: u32, settings: Option<u32>) -> Vec<u8> {
        let mut payload = session.to_le_bytes().to_vec();
        payload.extend(settings.iter().flat_map(|settings| settings.to_le_bytes()));
        payload
    }

    /// A payload of any length up to the largest. Half are three to nine
    /// bytes long, around the lengths of a session, and most others empty,
    /// one byte, or a byte short of the largest or the largest.
    fn any_payload() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            prop::sample::select(vec![3, 4, 5, 7, 8, 9]).prop_flat_map(bytes),
            bytes_to(Packet::MAX_PAYLOAD),
        ]
    }

    /// A payload of any length up to one past the largest a packet carries.
    fn any_length() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![any_payload(), bytes(Packet::MAX_PAYLOAD + 1)]
    }

    proptest! {
        /// A session heartbeat, with or without a settings sequence, writes a
        /// `HEARTBEAT` packet whose payload parses back to it, whatever the
        /// buffer held before.
        #[test]
        fn a_heartbeat_decodes_as_it_was_encoded(
            session in word(),
            settings in prop::option::of(word()),
        ) {
            let beat = beat(session, settings);
            let (len, buf) =
                write_over_any(Header::SIZE + SETTINGS_PAYLOAD_SIZE, |buf| beat.write(buf))?;
            let len = len.unwrap();
            let header = Header::parse(buf[..Header::SIZE].try_into().unwrap()).unwrap();
            let payload = &buf[Header::SIZE..len];
            prop_assert_eq!(header, Header::new(PacketType::HEARTBEAT, payload.len() as u16));
            prop_assert_eq!(payload, &payload_of(session, settings)[..]);
            prop_assert_eq!(Heartbeat::parse(payload), Some(beat));
            prop_assert_eq!(beat.session(), Some(SessionId::new(session)));
            prop_assert_eq!(beat.settings(), settings);
        }

        /// Every payload parses. Four bytes are a session alone, eight a
        /// session and its settings sequence, and any other length is carried
        /// as it is. Writing what was parsed gives back the same payload,
        /// whatever the buffer held before.
        #[test]
        fn any_payload_parses_and_writes_back_unchanged(payload in any_payload()) {
            let beat = Heartbeat::parse(&payload).unwrap();
            if matches!(payload.len(), SESSION_PAYLOAD_SIZE | SETTINGS_PAYLOAD_SIZE) {
                let word = |at: usize| {
                    u32::from(payload[at])
                        | u32::from(payload[at + 1]) << 8
                        | u32::from(payload[at + 2]) << 16
                        | u32::from(payload[at + 3]) << 24
                };
                let session = SessionId::new(word(0));
                let settings = (payload.len() == SETTINGS_PAYLOAD_SIZE).then(|| word(4));
                prop_assert_eq!(beat, Heartbeat::Session { session, settings });
                prop_assert_eq!(beat.session(), Some(session));
                prop_assert_eq!(beat.settings(), settings);
            } else {
                prop_assert_eq!(beat, Heartbeat::Any(&payload));
                prop_assert_eq!(beat.session(), None);
                prop_assert_eq!(beat.settings(), None);
            }
            let (len, buf) = write_over_any(Header::SIZE + payload.len(), |buf| beat.write(buf))?;
            prop_assert_eq!(len, Ok(buf.len()));
            prop_assert_eq!(&buf[Header::SIZE..], &payload[..]);
        }

        /// A heartbeat is written when its payload is at most
        /// [`Packet::MAX_PAYLOAD`] bytes and the buffer holds the whole
        /// packet, and refused otherwise.
        #[test]
        fn a_heartbeat_is_written_only_when_it_fits(payload in any_length(), slack in slack()) {
            let beat = Heartbeat::parse(&payload).unwrap();
            let needed = Header::SIZE + payload.len();
            let mut buf = buffer(needed, slack);
            let fits = payload.len() <= Packet::MAX_PAYLOAD && buf.len() >= needed;
            prop_assert_eq!(beat.write(&mut buf).ok(), fits.then_some(needed));
        }
    }
}

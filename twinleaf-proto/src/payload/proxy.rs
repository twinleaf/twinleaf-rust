//! The payloads a proxy sends its clients, read and written by [`ProxyRx`]
//! and [`LinkStatus`].
//!
//! A `PROXY_RX` payload is `{ kind: u8 }` followed by that kind's body.
//!
//! | kind | body |
//! | ---- | ---- |
//! | 0, a reply | an RPC reply payload, `{ req_id: u16le }` and the value |
//! | 1, an error | an RPC error payload, `{ req_id: u16le, code: u16le }` and the message |
//! | 2, a link status | `{ status: u8 }` |
//! | 3, a sign of life | nothing |
//! | 4, a count of dropped packets | `{ stream_id: u8, packets: u32le }` |
//!
//! A reader ignores bytes past the body of kinds 2, 3 and 4.
//!
//! A `PROXY_STATUS` payload is `{ status: u8 }`, the old form of a link
//! status. It has no stop, so a stop goes out as a failure to reconnect, and
//! code 4 there reads as [`LinkStatus::Unknown`].

use crate::packet::{Header, PacketType, WriteError};
use crate::rpc::{write_error_payload, write_reply_payload, ErrorReply, Reply};

/// Kind byte of a reply.
const REPLY: u8 = 0;
/// Kind byte of an error.
const ERROR: u8 = 1;
/// Kind byte of a link status.
const LINK: u8 = 2;
/// Kind byte of a sign of life.
const ALIVE: u8 = 3;
/// Kind byte of a count of dropped sample packets.
const DROPPED: u8 = 4;

/// The link status a proxy announces to its clients, one byte on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum LinkStatus {
    /// The device went quiet or its transport closed, 0.
    SensorDisconnected,
    /// The device is answering again after a disconnect, 1.
    SensorReconnected,
    /// The proxy gave up reconnecting, 2.
    FailedToReconnect,
    /// The transport did not open, 3.
    FailedToConnect,
    /// The proxy stopped, 4. The client must not reconnect.
    Stopped,
    /// A status code this build does not know.
    Unknown(u8),
}

impl LinkStatus {
    /// Returns true if a host forgets what it knew of the devices behind the
    /// link, which it does after a disconnect or a stop.
    pub const fn forgets_device(self) -> bool {
        matches!(self, Self::SensorDisconnected | Self::Stopped)
    }
}

impl From<u8> for LinkStatus {
    fn from(code: u8) -> Self {
        match code {
            0 => Self::SensorDisconnected,
            1 => Self::SensorReconnected,
            2 => Self::FailedToReconnect,
            3 => Self::FailedToConnect,
            4 => Self::Stopped,
            code => Self::Unknown(code),
        }
    }
}

impl From<LinkStatus> for u8 {
    fn from(status: LinkStatus) -> Self {
        match status {
            LinkStatus::SensorDisconnected => 0,
            LinkStatus::SensorReconnected => 1,
            LinkStatus::FailedToReconnect => 2,
            LinkStatus::FailedToConnect => 3,
            LinkStatus::Stopped => 4,
            LinkStatus::Unknown(code) => code,
        }
    }
}

impl LinkStatus {
    /// Reads a `PROXY_STATUS` payload. Code 4 reads as
    /// [`Unknown`](Self::Unknown), since the old form has no stop. Returns
    /// `None` if the payload is empty.
    pub(crate) fn parse_status(payload: &[u8]) -> Option<Self> {
        let code = *payload.first()?;
        Some(match Self::from(code) {
            Self::Stopped => Self::Unknown(code),
            status => status,
        })
    }

    /// Writes this status as a `PROXY_STATUS` payload into `out`.
    /// [`Stopped`](Self::Stopped) goes out as
    /// [`FailedToReconnect`](Self::FailedToReconnect). Returns the length.
    ///
    /// # Errors
    ///
    /// Returns [`WriteError::BufferTooSmall`] if `out` is empty.
    fn write_status(self, out: &mut [u8]) -> Result<usize, WriteError> {
        let status = match self {
            Self::Stopped => Self::FailedToReconnect,
            status => status,
        };
        *out.first_mut().ok_or(WriteError::BufferTooSmall)? = u8::from(status);
        Ok(1)
    }

    /// Writes this status as a full `PROXY_STATUS` packet with no routing
    /// into `buf`. [`Stopped`](Self::Stopped) goes out as
    /// [`FailedToReconnect`](Self::FailedToReconnect). Returns the packet
    /// length.
    ///
    /// # Errors
    ///
    /// Returns [`WriteError::BufferTooSmall`] if the packet does not fit.
    pub fn write(self, buf: &mut [u8]) -> Result<usize, WriteError> {
        let (head, body) = buf
            .split_first_chunk_mut::<{ Header::SIZE }>()
            .ok_or(WriteError::BufferTooSmall)?;
        let len = self.write_status(body)?;
        Header::new(PacketType::PROXY_STATUS, len as u16).write(head);
        Ok(Header::SIZE + len)
    }
}

/// What a proxy tells its client in a `PROXY_RX` packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyRx<'a> {
    /// The answer to a control request, kind 0.
    Reply(Reply<'a>),
    /// A control request refused, kind 1. Its message may say why.
    Error(ErrorReply<'a>),
    /// The link status of the subtree the packet's route names, kind 2.
    Link(LinkStatus),
    /// A sign that the proxy is alive, kind 3, sent when nothing else was.
    Alive,
    /// The proxy dropped sample packets from its queue for this client,
    /// which read too slowly, kind 4. They were `packets` packets of stream
    /// `stream_id` of the device the packet's route names, and the samples
    /// that stream sends next come after them.
    Dropped {
        /// The stream the packets were of.
        stream_id: u8,
        /// How many of its packets were dropped.
        packets: u32,
    },
    /// A kind this build does not know, with the body after its kind byte.
    Unknown(u8, &'a [u8]),
}

impl<'a> ProxyRx<'a> {
    /// Parses a `PROXY_RX` payload. A kind this build does not know becomes
    /// [`Unknown`](Self::Unknown). Returns `None` if the payload has no kind
    /// byte or is shorter than its kind's body.
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        let (&kind, body) = payload.split_first()?;
        Some(match kind {
            REPLY => Self::Reply(Reply::parse(body)?),
            ERROR => Self::Error(ErrorReply::parse(body)?),
            LINK => Self::Link(LinkStatus::from(*body.first()?)),
            ALIVE => Self::Alive,
            DROPPED => {
                let (&stream_id, count) = body.split_first()?;
                Self::Dropped {
                    stream_id,
                    packets: u32::from_le_bytes(*count.first_chunk()?),
                }
            }
            _ => Self::Unknown(kind, body),
        })
    }

    /// Returns the length of the `PROXY_RX` payload, the packet without its
    /// header.
    pub fn payload_len(&self) -> usize {
        1 + match self {
            Self::Reply(reply) => 2 + reply.value.len(),
            Self::Error(error) => 4 + error.message.len(),
            Self::Link(_) => 1,
            Self::Alive => 0,
            Self::Dropped { .. } => 5,
            Self::Unknown(_, body) => body.len(),
        }
    }

    /// Writes the `PROXY_RX` payload alone, without a packet header, into
    /// `out`, and returns its length.
    ///
    /// # Errors
    ///
    /// Returns [`WriteError::BufferTooSmall`] if the payload does not fit.
    fn write_payload(&self, out: &mut [u8]) -> Result<usize, WriteError> {
        let (kind, body) = out.split_first_mut().ok_or(WriteError::BufferTooSmall)?;
        let written = match *self {
            Self::Reply(reply) => {
                *kind = REPLY;
                write_reply_payload(body, reply.req_id, reply.value)?
            }
            Self::Error(error) => {
                *kind = ERROR;
                write_error_payload(body, error.req_id, error.code, error.message)?
            }
            Self::Link(status) => {
                *kind = LINK;
                *body.first_mut().ok_or(WriteError::BufferTooSmall)? = u8::from(status);
                1
            }
            Self::Alive => {
                *kind = ALIVE;
                0
            }
            Self::Dropped { stream_id, packets } => {
                *kind = DROPPED;
                let (stream, count) = body.split_first_mut().ok_or(WriteError::BufferTooSmall)?;
                *stream = stream_id;
                *count.first_chunk_mut().ok_or(WriteError::BufferTooSmall)? = packets.to_le_bytes();
                5
            }
            Self::Unknown(code, payload) => {
                *kind = code;
                body.get_mut(..payload.len())
                    .ok_or(WriteError::BufferTooSmall)?
                    .copy_from_slice(payload);
                payload.len()
            }
        };
        Ok(1 + written)
    }

    /// Writes a full `PROXY_RX` packet with no routing into `buf`, and
    /// returns its length.
    ///
    /// # Errors
    ///
    /// Returns [`WriteError::PayloadTooLarge`] if the payload exceeds
    /// [`Packet::MAX_PAYLOAD`], and [`WriteError::BufferTooSmall`] if the
    /// packet does not fit `buf`.
    pub fn write(&self, buf: &mut [u8]) -> Result<usize, WriteError> {
        let header = Header::for_payload(PacketType::PROXY_RX, self.payload_len())?;
        let (head, body) = buf
            .split_first_chunk_mut::<{ Header::SIZE }>()
            .ok_or(WriteError::BufferTooSmall)?;
        let len = self.write_payload(body)?;
        header.write(head);
        Ok(Header::SIZE + len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::strategy::{
        buffer, bytes, bytes_to, half, octet, slack, word, write_over_any,
    };
    use crate::RpcRequestId;
    use proptest::prelude::*;

    /// The payload `rx` writes into a buffer of exactly its length.
    fn written(rx: ProxyRx<'_>) -> Vec<u8> {
        let len = rx.payload_len();
        let mut out = vec![0; len];
        assert_eq!(rx.write_payload(&mut out), Ok(len));
        out
    }

    /// A status of any code. Half are the codes this build names.
    fn status() -> impl Strategy<Value = LinkStatus> {
        prop_oneof![0..=4u8, octet()].prop_map(LinkStatus::from)
    }

    /// The parts of a `PROXY_RX` payload of any kind, as owned bytes, and the
    /// value they make.
    #[derive(Debug, Clone)]
    enum Drawn {
        Reply(u16, Vec<u8>),
        Error(u16, u16, Vec<u8>),
        Link(LinkStatus),
        Alive,
        Dropped(u8, u32),
        Unknown(u8, Vec<u8>),
    }

    impl Drawn {
        fn rx(&self) -> ProxyRx<'_> {
            match self {
                Self::Reply(id, value) => ProxyRx::Reply(Reply {
                    req_id: RpcRequestId::new(*id),
                    value,
                }),
                Self::Error(id, code, message) => ProxyRx::Error(ErrorReply {
                    req_id: RpcRequestId::new(*id),
                    code: *code,
                    message,
                }),
                Self::Link(status) => ProxyRx::Link(*status),
                Self::Alive => ProxyRx::Alive,
                Self::Dropped(stream_id, packets) => ProxyRx::Dropped {
                    stream_id: *stream_id,
                    packets: *packets,
                },
                Self::Unknown(kind, body) => ProxyRx::Unknown(*kind, body),
            }
        }
    }

    /// A `PROXY_RX` value of any kind, the unknown ones from 5 up.
    fn any_rx() -> impl Strategy<Value = Drawn> {
        prop_oneof![
            (half(), bytes_to(16)).prop_map(|(id, value)| Drawn::Reply(id, value)),
            (half(), half(), bytes_to(16))
                .prop_map(|(id, code, message)| Drawn::Error(id, code, message)),
            status().prop_map(Drawn::Link),
            Just(Drawn::Alive),
            (octet(), word()).prop_map(|(stream_id, packets)| Drawn::Dropped(stream_id, packets)),
            (5..=u8::MAX, bytes_to(16)).prop_map(|(kind, body)| Drawn::Unknown(kind, body)),
        ]
    }

    proptest! {
        /// Every value writes a payload of the length it states that parses
        /// back to it, whatever the buffer held before.
        #[test]
        fn a_proxy_rx_decodes_as_it_was_encoded(drawn in any_rx()) {
            let rx = drawn.rx();
            let (len, buf) = write_over_any(rx.payload_len(), |out| rx.write_payload(out))?;
            prop_assert_eq!(len, Ok(rx.payload_len()));
            prop_assert_eq!(ProxyRx::parse(&buf), Some(rx));
        }

        /// A value is written when the buffer holds its payload, and refused
        /// otherwise.
        #[test]
        fn a_proxy_rx_is_written_only_when_it_fits(drawn in any_rx(), slack in slack()) {
            let rx = drawn.rx();
            let needed = rx.payload_len();
            let mut buf = buffer(needed, slack);
            prop_assert_eq!(
                rx.write_payload(&mut buf),
                (buf.len() >= needed).then_some(needed).ok_or(WriteError::BufferTooSmall)
            );
        }

        /// Every payload either parses or is too short for its kind, and what
        /// parses writes back as the payload up to the end of its body.
        #[test]
        fn any_payload_parses_and_writes_back_its_body(payload in bytes_to(12)) {
            let Some(rx) = ProxyRx::parse(&payload) else {
                let short = match payload.first() {
                    None => true,
                    Some(&REPLY) => payload.len() < 3,
                    Some(&ERROR) => payload.len() < 5,
                    Some(&LINK) => payload.len() < 2,
                    Some(&DROPPED) => payload.len() < 6,
                    Some(_) => false,
                };
                prop_assert!(short, "a payload long enough for its kind was refused");
                return Ok(());
            };
            let written = written(rx);
            prop_assert_eq!(&written[..], &payload[..written.len()]);
        }

        /// A status writes a `PROXY_STATUS` payload that reads back as it was,
        /// except a stop, which reads back as a failure to reconnect.
        #[test]
        fn a_status_decodes_as_it_was_encoded(status in status(), payload in bytes(2)) {
            let mut out = payload.clone();
            prop_assert_eq!(status.write_status(&mut out), Ok(1));
            prop_assert_eq!(out[1], payload[1]);
            let expected = match status {
                LinkStatus::Stopped => LinkStatus::FailedToReconnect,
                status => status,
            };
            prop_assert_eq!(LinkStatus::parse_status(&out), Some(expected));
        }

        /// Every code but 4 reads the same in both forms.
        #[test]
        fn both_forms_read_a_code_alike(code in octet()) {
            let status = LinkStatus::parse_status(&[code]).unwrap();
            prop_assert_eq!(u8::from(status), code);
            if code == 4 {
                prop_assert_eq!(status, LinkStatus::Unknown(4));
            } else {
                prop_assert_eq!(status, LinkStatus::from(code));
            }
        }
    }
}

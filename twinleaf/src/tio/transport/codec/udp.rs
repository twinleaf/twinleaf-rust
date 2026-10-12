//! The UDP framing.
//!
//! Each TIO packet travels unchanged in a datagram of its own.

use super::Error;
use crate::tio::packet::Packet;

/// Decodes `datagram` as the one packet it holds.
///
/// # Errors
///
/// Returns [`Error::TrailingBytes`] if bytes follow the packet, and
/// [`Error::Invalid`] if `datagram` does not start with a valid packet.
pub(crate) fn decode(datagram: &[u8]) -> Result<Packet, Error> {
    super::decode_exact(datagram)
}

/// Returns the datagram that carries `packet`.
pub(crate) fn encode(packet: &Packet) -> &[u8] {
    packet.as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::DeviceRoute;
    use crate::tio::packet;

    #[test]
    fn an_encoded_packet_decodes_unchanged() {
        let packet = Packet::rpc_request("dev.name", &[], 7, "/1/2".parse().unwrap()).unwrap();

        assert_eq!(
            decode(encode(&packet)).unwrap().as_bytes(),
            packet.as_bytes()
        );
    }

    #[test]
    fn a_datagram_that_is_not_one_packet_is_an_error() {
        let heartbeat = encode(&Packet::heartbeat(DeviceRoute::root())).to_vec();
        let mut trailing = heartbeat.clone();
        trailing.push(0);

        assert_eq!(decode(&trailing).unwrap_err(), Error::TrailingBytes);
        assert_eq!(
            decode(&heartbeat[..3]).unwrap_err(),
            Error::Invalid(packet::DecodeError::PacketTooSmall)
        );
        assert_eq!(
            decode(&[0, 0, 0, 0]).unwrap_err(),
            Error::Invalid(packet::DecodeError::InvalidPacketType)
        );
    }
}

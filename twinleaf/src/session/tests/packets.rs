//! The packets a device sends, for the tests of `session` and every layer
//! above it. The bytes of each sample and metadata packet come from the
//! builders of the tests of `data`, and a test here holds those builders
//! equal to the host packet constructors.

use crate::data::fixtures::{metadata_records, packet};
use crate::proto::data as wire;
use crate::proto::DeviceRoute;
use crate::tio::Packet;

/// The packet `bytes` hold.
fn packet_of(bytes: Vec<u8>) -> Packet {
    Packet::from_wire(bytes.into()).expect("a builder writes a valid packet")
}

/// A packet of one f32 sample of stream 1 at `route`, numbered `first`.
pub(crate) fn samples(first: u32, route: DeviceRoute) -> Packet {
    packet_of(packet::samples(1, 0, first, &[0; 4], route))
}

/// The metadata packet that broadcasts `record` at `route`.
pub(crate) fn broadcast(record: wire::Metadata<'_>, route: DeviceRoute) -> Packet {
    packet_of(packet::metadata(record, wire::MetadataFlags::UPDATE, route))
}

/// The packets that describe the device at `route` in `session`.
pub(crate) fn description(route: DeviceRoute, session: u32) -> Vec<Packet> {
    let records = metadata_records(session).into_iter();
    records.map(|record| broadcast(record, route)).collect()
}

/// One SETTING announcement, as a device sends it.
pub(crate) fn announcement(
    name: &str,
    seq: Option<u32>,
    reply: &[u8],
    route: DeviceRoute,
) -> Packet {
    let mut raw = [0u8; 64];
    let len = crate::proto::settings::Setting {
        name: name.as_bytes(),
        seq,
        reply,
    }
    .write(&mut raw)
    .expect("the announcement fits");
    Packet::from_slice_prefix(&raw[..len])
        .expect("a valid packet")
        .0
        .with_route(route)
}

/// One `dev.metadata` reply frame describing a one-stream device.
pub(crate) fn device_reply() -> Vec<u8> {
    let mut record = [0u8; 64];
    let len = metadata_records(1)[0]
        .write_reply_frame(&mut record)
        .expect("one record fits");
    record[..len].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tio::packet::LinkStatus;

    fn route(value: &str) -> DeviceRoute {
        value.parse().unwrap()
    }

    /// The packet builders of the tests of `data` write the bytes of the
    /// constructor of their name, so those tests push what a device sends.
    #[test]
    fn the_packet_builders_of_the_data_tests_write_what_packet_constructs() {
        use crate::data::fixtures::packet as built;

        for routing in [DeviceRoute::root(), route("/1"), route("/3/2/1")] {
            let data = [1, 2, 3, 4, 5, 6, 7, 8];
            assert_eq!(
                built::samples(5, 1, 70_000, &data, routing),
                Packet::samples(5, 1, 70_000, &data, routing)
                    .unwrap()
                    .as_bytes()
            );
            for flags in [wire::MetadataFlags::UPDATE, wire::MetadataFlags::default()] {
                for record in metadata_records(9) {
                    assert_eq!(
                        built::metadata(record, flags, routing),
                        Packet::metadata(record, flags, routing).unwrap().as_bytes()
                    );
                }
            }
            assert_eq!(
                built::heartbeat_session(43, routing),
                Packet::heartbeat_session(43, routing).as_bytes()
            );
            assert_eq!(
                built::heartbeat(routing),
                Packet::heartbeat(routing).as_bytes()
            );
            assert_eq!(
                built::rpc_reply(7, &[0x5a; 400], routing),
                Packet::rpc_reply(7, &[0x5a; 400], routing)
                    .unwrap()
                    .as_bytes()
            );
            assert_eq!(
                built::dropped(3, 70_000, routing),
                Packet::dropped(3, 70_000).with_route(routing).as_bytes()
            );
            for status in [
                LinkStatus::SensorDisconnected,
                LinkStatus::SensorReconnected,
                LinkStatus::FailedToReconnect,
                LinkStatus::FailedToConnect,
                LinkStatus::Stopped,
                LinkStatus::Unknown(9),
            ] {
                assert_eq!(
                    built::link(status, routing),
                    Packet::link(status).with_route(routing).as_bytes()
                );
                assert_eq!(
                    built::legacy_status(status, routing),
                    Packet::legacy_status(status).with_route(routing).as_bytes()
                );
            }
        }
    }
}

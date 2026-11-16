//! Shared field values for the unit tests of `data`, and the packets a
//! device sends, for the tests of every layer above it.

use crate::data::model::schema::{
    ColumnRecord, DeviceRecord, SegmentRecord, StreamKey, StreamMetadataSnapshot, StreamRecord,
    StreamSchema,
};
use crate::proto::data as wire;
use crate::proto::data::DataType;
use crate::proto::sync::Epoch;
use crate::proto::{ColumnId, DeviceRoute, SegmentId, SessionId, StreamId};
use crate::tio::packet::Packet;
use std::sync::Arc;

/// Retains the records of one routed stream as the parser would, with the
/// stream's schema built from `stream` and `columns`.
pub(crate) fn metadata(
    route: DeviceRoute,
    device: wire::Device<'_>,
    stream: wire::Stream<'_>,
    segment: wire::Segment<'_>,
    columns: &[wire::Column<'_>],
) -> StreamMetadataSnapshot {
    let schema = StreamSchema::new(
        StreamKey::new(route, stream.stream_id),
        StreamRecord::encode(stream).unwrap(),
        columns
            .iter()
            .map(|column| ColumnRecord::encode(*column).unwrap())
            .collect(),
    );
    StreamMetadataSnapshot::new(
        Arc::new(schema),
        DeviceRecord::encode(device).unwrap(),
        SegmentRecord::encode(segment).unwrap(),
    )
}

pub(crate) fn device() -> wire::Device<'static> {
    wire::Device {
        session: SessionId::new(42),
        n_streams: 1,
        name: "test-device",
        serial: "SN123",
        firmware: "fw",
        protocol: 0,
    }
}

pub(crate) fn stream(stream_id: u8) -> wire::Stream<'static> {
    wire::Stream {
        stream_id: StreamId::new(stream_id),
        n_columns: 1,
        n_segments: 1,
        sample_size: 4,
        buf_samples: 128,
        name: "test-stream",
    }
}

pub(crate) fn segment(stream_id: u8) -> wire::Segment<'static> {
    wire::Segment {
        stream_id: StreamId::new(stream_id),
        segment_id: SegmentId::new(0),
        flags: wire::SegmentFlags::default(),
        epoch: Epoch::UNIX,
        timeref_serial: "clock",
        timeref_session: SessionId::new(7),
        start_time: 0,
        sampling_rate: 1,
        decimation: 1,
        filter_cutoff: 0.0,
        filter_type: wire::FilterType::NONE,
    }
}

pub(crate) fn column(stream_id: u8, index: u8, data_type: DataType) -> wire::Column<'static> {
    wire::Column {
        stream_id: StreamId::new(stream_id),
        index: ColumnId::new(index),
        data_type,
        name: "col_0",
        units: "",
        description: "",
    }
}

/// A packet of one f32 sample of stream 1 at `route`, numbered `first`.
pub(crate) fn samples(first: u32, route: DeviceRoute) -> Packet {
    Packet::samples(1, 0, first, &[0; 4], route).expect("valid samples")
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

/// The four records that let a parser decode one f32 stream, as the device
/// in `session` describes them.
pub(crate) fn metadata_records(session: u32) -> [wire::Metadata<'static>; 4] {
    [
        wire::Metadata::Device(wire::Device {
            session: SessionId::new(session),
            n_streams: 1,
            name: "d",
            serial: "s",
            firmware: "f",
            protocol: 0,
        }),
        wire::Metadata::Stream(wire::Stream {
            stream_id: StreamId::new(1),
            n_columns: 1,
            n_segments: 2,
            sample_size: 4,
            buf_samples: 128,
            name: "stream",
        }),
        wire::Metadata::Segment(segment_record(0)),
        wire::Metadata::Column(wire::Column {
            stream_id: StreamId::new(1),
            index: ColumnId::new(0),
            data_type: DataType::F32,
            name: "col",
            units: "",
            description: "",
        }),
    ]
}

/// The stream's segment `segment_id`, starting one second per segment in.
pub(crate) fn segment_record(segment_id: u8) -> wire::Segment<'static> {
    wire::Segment {
        stream_id: StreamId::new(1),
        segment_id: SegmentId::new(segment_id),
        flags: wire::SegmentFlags::default(),
        epoch: Epoch::UNIX,
        timeref_serial: "clock",
        timeref_session: SessionId::new(7),
        start_time: u32::from(segment_id),
        sampling_rate: 1,
        decimation: 1,
        filter_cutoff: 0.0,
        filter_type: wire::FilterType::NONE,
    }
}

/// The metadata packet that broadcasts `record` at `route`.
pub(crate) fn broadcast(record: wire::Metadata<'_>, route: DeviceRoute) -> Packet {
    Packet::metadata(record, wire::MetadataFlags::UPDATE, route).expect("one record fits")
}

/// The packets that describe the device at `route` in `session`.
pub(crate) fn description(route: DeviceRoute, session: u32) -> Vec<Packet> {
    let records = metadata_records(session).into_iter();
    records.map(|record| broadcast(record, route)).collect()
}

/// One `dev.metadata` reply frame describing a one-stream device.
pub(crate) fn device_reply() -> Vec<u8> {
    let mut record = [0u8; 64];
    let len = metadata_records(1)[0]
        .write_reply_frame(&mut record)
        .expect("one record fits");
    record[..len].to_vec()
}

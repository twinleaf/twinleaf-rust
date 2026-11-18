//! Records, packet bytes and expected values for tests of code that uses
//! `data`.

use crate::model::continuity::{Boundary, Change, Loss};
use crate::model::schema::{
    ColumnRecord, DeviceRecord, SegmentRecord, StreamKey, StreamMetadataSnapshot, StreamRecord,
    StreamSchema,
};
use crate::proto::data as wire;
use crate::proto::data::DataType;
use crate::proto::packet::Checked;
use crate::proto::sync::Epoch;
use crate::proto::{ColumnId, DeviceRoute, SampleNumber, SegmentId, SessionId, StreamId};
use std::sync::Arc;

/// The boundary that names `change` and `loss`.
pub fn boundary(change: Option<Change>, loss: Option<Loss>) -> Boundary {
    Boundary { change, loss }
}

/// The loss with the fields of [`Loss`] in the order it declares them.
pub fn loss(
    samples: u64,
    expected: Option<SampleNumber>,
    dropped_batches: u64,
    dropped_samples: u64,
    link_samples: u64,
    proxy_samples: u64,
) -> Loss {
    Loss {
        samples,
        expected,
        dropped_batches,
        dropped_samples,
        link_samples,
        proxy_samples,
    }
}

/// Returns a function that hands each batch to the table numbering of an
/// HDF5 export at `level` that splits where continuity breaks. The function
/// returns how many tables the batch moved its stream on.
///
/// # Panics
///
/// The function panics if `level` does not split runs.
#[cfg(feature = "hdf5")]
pub fn table_steps(
    level: crate::export::RunSplitLevel,
) -> Box<dyn FnMut(&crate::SampleBatch) -> u64> {
    use crate::logfile::export::{RunTables, SplitPolicy};
    let mut tables = RunTables::new(SplitPolicy::Continuous, level);
    Box::new(move |batch| {
        let key = batch.stream_key();
        let before = tables.index(key).expect("tables split by run");
        tables.observe(key, batch.generations(), batch.boundary());
        let after = tables.index(key).expect("tables split by run");
        after - before
    })
}

/// Retains the records of one routed stream as the parser would, with the
/// stream's schema built from `stream` and `columns`.
///
/// # Panics
///
/// Panics if a record does not encode.
pub fn metadata(
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

/// A record of a device in session 42 with one stream.
pub fn device() -> wire::Device<'static> {
    wire::Device {
        session: SessionId::new(42),
        n_streams: 1,
        name: "test-device",
        serial: "SN123",
        firmware: "fw",
        protocol: 0,
    }
}

/// A record of stream `stream_id` with one column of four bytes.
pub fn stream(stream_id: u8) -> wire::Stream<'static> {
    wire::Stream {
        stream_id: StreamId::new(stream_id),
        n_columns: 1,
        n_segments: 1,
        sample_size: 4,
        buf_samples: 128,
        name: "test-stream",
    }
}

/// Segment 0 of stream `stream_id`, at one sample per second.
pub fn segment(stream_id: u8) -> wire::Segment<'static> {
    wire::Segment {
        stream_id: StreamId::new(stream_id),
        segment_id: SegmentId::new(0),
        flags: wire::SegmentFlags::VALID,
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

/// Column `index` of stream `stream_id`, of type `data_type`.
pub fn column(stream_id: u8, index: u8, data_type: DataType) -> wire::Column<'static> {
    wire::Column {
        stream_id: StreamId::new(stream_id),
        index: ColumnId::new(index),
        data_type,
        name: "col_0",
        units: "",
        description: "",
    }
}

/// The four records that let a parser decode one f32 stream, as the device
/// in `session` describes them.
pub fn metadata_records(session: u32) -> [wire::Metadata<'static>; 4] {
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
pub fn segment_record(segment_id: u8) -> wire::Segment<'static> {
    wire::Segment {
        stream_id: StreamId::new(1),
        segment_id: SegmentId::new(segment_id),
        flags: wire::SegmentFlags::VALID,
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

/// One `dev.metadata` reply carrying `records`, in wire framing.
///
/// # Panics
///
/// Panics if the records do not fit one reply.
pub fn metadata_reply(records: &[wire::Metadata<'_>]) -> Vec<u8> {
    let mut reply = vec![0u8; wire::MAX_METADATA_REPLY_SIZE];
    let mut written = 0;
    for record in records {
        written += record
            .write_reply_frame(&mut reply[written..])
            .expect("the records fit one reply");
    }
    reply.truncate(written);
    reply
}

/// Returns `packet` checked as one whole packet.
///
/// # Panics
///
/// Panics if `packet` is not one whole valid packet.
pub fn checked(packet: &[u8]) -> Checked<'_> {
    Checked::exact(packet).expect("one whole valid packet")
}

/// The bytes of packets for tests, written by the proto codecs. Each builder writes the bytes the host packet constructor of its
/// name builds, addressed to the route it takes. A test of the host packet
/// holds the two equal.
pub mod packet {
    use crate::proto::data as wire;
    use crate::proto::heartbeat::Heartbeat;
    use crate::proto::packet::{write_routing, Packet, WriteError};
    use crate::proto::proxy::{LinkStatus, ProxyRx};
    use crate::proto::rpc;
    use crate::proto::{DeviceRoute, RpcRequestId, SampleNumber, SegmentId, SessionId, StreamId};

    /// The packet `write` puts at the start of a buffer, addressed to
    /// `route`.
    fn routed(
        route: DeviceRoute,
        write: impl FnOnce(&mut [u8]) -> Result<usize, WriteError>,
    ) -> Vec<u8> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = write(&mut buf).expect("the packet fits");
        let len = write_routing(&mut buf, len, route).expect("the route fits");
        buf[..len].to_vec()
    }

    /// Packed sample `data` of stream `stream_id` in segment `segment_id`,
    /// starting at sample `first`.
    pub fn samples(
        stream_id: u8,
        segment_id: u8,
        first: u32,
        data: &[u8],
        route: DeviceRoute,
    ) -> Vec<u8> {
        let samples = wire::Samples {
            stream_id: StreamId::new(stream_id),
            segment_id: SegmentId::new(segment_id),
            first: SampleNumber::new(first),
            data,
        };
        routed(route, |buf| samples.write(buf))
    }

    /// One metadata `record` with its `flags`.
    pub fn metadata(
        record: wire::Metadata<'_>,
        flags: wire::MetadataFlags,
        route: DeviceRoute,
    ) -> Vec<u8> {
        routed(route, |buf| record.write(flags, buf))
    }

    /// The heartbeat of a device in `session` with no settings sequence.
    pub fn heartbeat_session(session: u32, route: DeviceRoute) -> Vec<u8> {
        let beat = Heartbeat::Session {
            session: SessionId::new(session),
            settings: None,
        };
        routed(route, |buf| beat.write(buf))
    }

    /// The keepalive a host sends.
    pub fn heartbeat(route: DeviceRoute) -> Vec<u8> {
        routed(route, |buf| Heartbeat::Any(&[]).write(buf))
    }

    /// The reply to request `id`, carrying `value`.
    pub fn rpc_reply(id: u16, value: &[u8], route: DeviceRoute) -> Vec<u8> {
        routed(route, |buf| {
            rpc::write_reply(buf, RpcRequestId::new(id), value)
        })
    }

    /// A link status in the legacy form, type 64, about the subtree at
    /// `route`.
    pub fn legacy_status(status: LinkStatus, route: DeviceRoute) -> Vec<u8> {
        routed(route, |buf| status.write(buf))
    }

    /// A link status as a `PROXY_RX` packet about the subtree at `route`.
    pub fn link(status: LinkStatus, route: DeviceRoute) -> Vec<u8> {
        proxy_rx(ProxyRx::Link(status), route)
    }

    /// A proxy's count of `packets` sample packets of stream `stream_id` it
    /// dropped for a client, about the device at `route`.
    pub fn dropped(stream_id: u8, packets: u32, route: DeviceRoute) -> Vec<u8> {
        proxy_rx(ProxyRx::Dropped { stream_id, packets }, route)
    }

    fn proxy_rx(rx: ProxyRx<'_>, route: DeviceRoute) -> Vec<u8> {
        routed(route, |buf| rx.write(buf))
    }
}

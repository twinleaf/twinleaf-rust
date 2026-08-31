//! Metadata fixtures shared by this module's tests. Each returns one record's
//! fields, which a caller varies with struct-update syntax before encoding it.

use crate::tio::proto::DataType;
use twinleaf_proto::data as wire;
use twinleaf_proto::sync::Epoch;
use twinleaf_proto::SessionId;

pub(super) fn device() -> wire::Device<'static> {
    wire::Device {
        session: SessionId::new(42),
        n_streams: 1,
        name: "test-device",
        serial: "SN123",
        firmware: "fw",
    }
}

pub(super) fn stream(stream_id: u8) -> wire::Stream<'static> {
    wire::Stream {
        stream_id,
        n_columns: 1,
        n_segments: 1,
        sample_size: 4,
        buf_samples: 128,
        name: "test-stream",
    }
}

/// A 1 Hz segment starting at time zero.
pub(super) fn segment(stream_id: u8) -> wire::Segment<'static> {
    wire::Segment {
        stream_id,
        segment_id: 0,
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

pub(super) fn column(stream_id: u8, index: u8, data_type: DataType) -> wire::Column<'static> {
    wire::Column {
        stream_id,
        index,
        data_type,
        name: "col_0",
        units: "",
        description: "",
    }
}

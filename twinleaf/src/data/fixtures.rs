//! Shared field values for the unit tests of this module.

use crate::proto::data as wire;
use crate::proto::data::DataType;
use crate::proto::sync::Epoch;
use crate::proto::{ColumnId, SegmentId, SessionId, StreamId};

pub(crate) fn device() -> wire::Device<'static> {
    wire::Device {
        session: SessionId::new(42),
        n_streams: 1,
        name: "test-device",
        serial: "SN123",
        firmware: "fw",
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

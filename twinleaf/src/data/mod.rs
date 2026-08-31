//! Decoded sample batches and optional host-side data facilities.
//!
//! Most live applications consume [`SampleBatch`] and inspect its borrowed
//! protocol metadata. [`Buffer`] and [`ColumnProcessor`] add bounded retention
//! and incremental processing; [`LogFile`] and [`PacketParser`] serve offline
//! and lower-level packet workflows.

mod buffer;
mod coalesce;
mod filter;
pub(crate) mod metadata;
mod parser;
mod pipeline;
mod reader;
mod sample;
mod state;

#[cfg(feature = "hdf5")]
pub mod export;

pub use crate::tio::proto::DataType;
pub use buffer::{Buffer, Run};
pub use filter::ColumnFilter;
pub use metadata::{DeviceMetadataSnapshot, MetadataQuery, StreamMetadataSnapshot};
pub(crate) use metadata::{
    BufferType, ColumnRecord, DeviceRecord, MetadataType, SegmentRecord, StreamRecord,
};
pub use parser::{PacketOutcome, PacketParser};
pub use pipeline::{ColumnOp, ColumnProcessor};
pub use reader::{LogError, LogFile, LogIndex, LogSummary, PacketIter, StreamSummary};
pub use sample::{
    Boundary, BoundaryClass, BoundaryReason, ColumnArray, ColumnData, ColumnKey, Generations,
    SampleBatch, SampleRow, ScalarBuffer, Series, StreamKey,
};
pub use state::{PacketError, StreamDataError};

/// Shared field values for unit tests. They live here because they are test
/// support for the whole data module, not a production metadata abstraction.
#[cfg(test)]
pub(crate) mod fixtures {
    use crate::tio::proto::DataType;
    use twinleaf_proto::data as wire;
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::{ColumnId, SegmentId, SessionId, StreamId};

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
}

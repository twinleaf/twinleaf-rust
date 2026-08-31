mod buffer;
pub(crate) mod coalesce;
mod filter;
mod keys;
pub(crate) mod metadata;
mod parser;
mod pipeline;
mod reader;
#[cfg(test)]
mod records;
mod sample;
mod state;

#[cfg(feature = "hdf5")]
pub mod export;

pub use crate::tio::proto::{DataType, DeviceRoute, MAX_SAMPLE_NUMBER};
pub use buffer::{Buffer, Run};
pub use filter::ColumnFilter;
pub use keys::{
    ColumnId, ColumnKey, SampleNumber, SegmentId, SessionId, StreamId, StreamKey, TimeRefSessionId,
};
pub use metadata::{
    BufferType, ColumnRecord, DataTypeExt, DeviceRecord, MetadataType, SegmentExt, SegmentRecord,
    StreamRecord,
};
pub use parser::{PacketOutcome, PacketParser, ParserCheckpoint};
pub use pipeline::{ColumnOp, ColumnProcessor};
pub use reader::{LogError, LogFile, LogIndex, LogSummary, PacketIter, StreamSummary};
pub use sample::{
    Boundary, BoundaryClass, BoundaryReason, ColumnArray, ColumnData, Generations, SampleBatch,
    SampleRow, ScalarBuffer, Series,
};
pub use state::{
    DeviceMetadataSnapshot, MetadataQuery, PacketError, ScannedRows, StreamDataError,
    StreamMetadataSnapshot,
};

pub mod buffer;
mod device;
mod reader;
mod subscription;
mod tree;
pub mod util;
pub mod rpc_meta;

pub use buffer::{Buffer, BufferEvent};
pub use device::Device;
pub use reader::Reader;
pub use subscription::SubscriptionManager;
pub use tree::DeviceTree;

pub type SampleNumber = u32;
pub type SessionId = u32;
pub type SegmentId = u8;
pub type StreamId = u8;
pub type ColumnId = usize;
pub type SubscriptionId = usize;

use crate::tio::proto::DeviceRoute;
pub type StreamKey = (DeviceRoute, StreamId);

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ColumnSpec {
    pub route: DeviceRoute,
    pub stream_id: StreamId,
    pub column_id: ColumnId,
}

impl ColumnSpec {
    pub fn stream_key(&self) -> StreamKey {
        (self.route.clone(), self.stream_id)
    }
}

pub struct CursorPosition {
    pub session_id: SessionId,
    pub segment_id: SegmentId,
    pub last_sample_number: SampleNumber,
}

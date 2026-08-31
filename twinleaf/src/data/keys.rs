//! Ids, and the hashmap keys naming a stream or a column within one across the
//! device tree. Host data-plane identity, not a wire concept.

use crate::tio::proto::DeviceRoute;

pub type SampleNumber = u32;
pub type SessionId = u32;
pub type SegmentId = u8;
pub type StreamId = u8;
pub type ColumnId = usize;
pub type TimeRefSessionId = u32;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct StreamKey {
    pub route: DeviceRoute,
    pub stream_id: StreamId,
}

impl StreamKey {
    pub fn new(route: DeviceRoute, stream_id: StreamId) -> Self {
        Self { route, stream_id }
    }
}

impl std::fmt::Display for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]:{}", self.route, self.stream_id)
    }
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct ColumnKey {
    pub route: DeviceRoute,
    pub stream_id: StreamId,
    pub column_id: ColumnId,
}

impl ColumnKey {
    pub fn new(route: DeviceRoute, stream_id: StreamId, column_id: ColumnId) -> Self {
        Self {
            route,
            stream_id,
            column_id,
        }
    }

    pub fn stream_key(&self) -> StreamKey {
        StreamKey {
            route: self.route,
            stream_id: self.stream_id,
        }
    }
}

impl std::fmt::Display for ColumnKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]:{}/{}", self.route, self.stream_id, self.column_id)
    }
}

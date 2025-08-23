use crate::tio;

use std::sync::Arc;
use tio::proto::meta::{ColumnMetadata, DeviceMetadata, SegmentMetadata, StreamMetadata};

#[derive(Debug, Clone)]
pub enum ColumnData {
    Int(i64),
    UInt(u64),
    Float(f64),
    Unknown,
}

impl ColumnData {
    pub fn try_as_f64(&self) -> Option<f64> {
        match *self {
            ColumnData::Int(i) => Some(i as f64),
            ColumnData::UInt(u) => Some(u as f64),
            ColumnData::Float(f) => Some(f),
            ColumnData::Unknown => None,
        }
    }
}

impl std::fmt::Display for ColumnData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ColumnData::Int(x) => write!(f, "{}", x),
            ColumnData::UInt(x) => write!(f, "{}", x),
            ColumnData::Float(x) => write!(f, "{}", x),
            ColumnData::Unknown => write!(f, "?"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub value: ColumnData,
    pub desc: Arc<ColumnMetadata>,
}

impl Column {
    pub fn from_le_bytes(data: &[u8], md: Arc<ColumnMetadata>) -> Column {
        use tio::proto::DataType;
        Column {
            value: match md.data_type {
                DataType::Int8 => ColumnData::Int(i8::from_le_bytes([data[0]]).into()),
                DataType::UInt8 => ColumnData::UInt(data[0].into()),
                DataType::Int16 => ColumnData::Int(i16::from_le_bytes([data[0], data[1]]).into()),
                DataType::UInt16 => ColumnData::UInt(u16::from_le_bytes([data[0], data[1]]).into()),
                DataType::Int24 => {
                    ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], 0]).into())
                }
                DataType::UInt24 => {
                    ColumnData::UInt(u32::from_le_bytes([data[0], data[1], data[2], 0]).into())
                }
                DataType::Int32 => {
                    ColumnData::Int(i32::from_le_bytes([data[0], data[1], data[2], data[3]]).into())
                }
                DataType::UInt32 => ColumnData::UInt(
                    u32::from_le_bytes([data[0], data[1], data[2], data[3]]).into(),
                ),
                DataType::Int64 => ColumnData::Int(
                    i64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])
                    .into(),
                ),
                DataType::UInt64 => ColumnData::UInt(
                    u64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])
                    .into(),
                ),
                DataType::Float32 => ColumnData::Float(
                    f32::from_le_bytes([data[0], data[1], data[2], data[3]]).into(),
                ),
                DataType::Float64 => ColumnData::Float(
                    f64::from_le_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])
                    .into(),
                ),
                DataType::Unknown(_) => ColumnData::Unknown,
            },
            desc: md,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub n: u32,
    pub columns: Vec<Column>,
    pub segment: Arc<SegmentMetadata>,
    pub stream: Arc<StreamMetadata>,
    pub device: Arc<DeviceMetadata>,
    pub segment_changed: bool,
    pub meta_changed: bool,
    pub source: tio::proto::StreamDataPayload,
}

impl Sample {
    pub fn timestamp_begin(&self) -> f64 {
        let period =
            1.0 / f64::from(self.segment.sampling_rate) * f64::from(self.segment.decimation);
        f64::from(self.segment.start_time) + period * f64::from(self.n)
    }
    pub fn timestamp_end(&self) -> f64 {
        let period =
            1.0 / f64::from(self.segment.sampling_rate) * f64::from(self.segment.decimation);
        f64::from(self.segment.start_time) + period * f64::from(self.n + 1)
    }
}

impl std::fmt::Display for Sample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SAMPLE({}:{}) {:.6}",
            self.stream.stream_id,
            self.segment.segment_id,
            self.timestamp_end()
        )?;
        for col in &self.columns {
            write!(f, " {}: {}", col.desc.name, col.value)?;
        }
        write!(f, " [#{}]", self.n)
    }
}

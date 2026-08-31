//! Metadata descriptors the host retains, and the vocabulary it reads off
//! them: a record's type, a column's buffer kind, a segment's clock.

use super::SampleNumber;
use crate::tio::proto::{DataType, DeviceRoute, EncodeError, Packet};
use std::fmt;
use std::sync::Arc;
use twinleaf_proto::data as wire;
use twinleaf_proto::MAX_PAYLOAD_SIZE;

pub use wire::MetadataType;

macro_rules! metadata_record {
    ($record:ident, $kind:ident) => {
        /// One retained metadata descriptor: the bare wire record, without the
        /// `[type][flags]` header a METADATA packet puts in front of it.
        /// Equality is byte equality, so a field a newer device adds still
        /// counts as a change.
        #[derive(Clone, PartialEq, Eq)]
        pub struct $record(Arc<[u8]>);

        impl $record {
            /// Retain a copy of `record` if it parses as this descriptor.
            pub fn new(record: &[u8]) -> Option<Self> {
                wire::$kind::parse(record)?;
                Some(Self(Arc::from(record)))
            }

            /// Retain a record this host built rather than received.
            pub fn encode(fields: wire::$kind<'_>) -> Option<Self> {
                let record = wire::Metadata::$kind(fields);
                let mut buf = [0u8; MAX_PAYLOAD_SIZE];
                let (_, len) = record.write_record(buf.get_mut(..record.record_len())?)?;
                Self::new(&buf[..len])
            }

            pub fn get(&self) -> wire::$kind<'_> {
                wire::$kind::parse(&self.0).expect("record parsed when retained")
            }

            /// This descriptor as an UPDATE broadcast, carrying the record
            /// exactly as retained.
            pub fn update(&self, routing: DeviceRoute) -> Result<Packet, EncodeError> {
                Packet::metadata_record(
                    MetadataType::$kind.into(),
                    wire::MetadataFlags::UPDATE,
                    &self.0,
                    routing,
                )
            }
        }

        impl fmt::Debug for $record {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

metadata_record!(DeviceRecord, Device);
metadata_record!(StreamRecord, Stream);
metadata_record!(SegmentRecord, Segment);
metadata_record!(ColumnRecord, Column);

/// Host-side classification of a column's wire type.
pub trait DataTypeExt {
    fn type_name(&self) -> String;

    /// The buffer a column of this type lands in, defaulting undecodable wire
    /// types to [`BufferType::Float`].
    fn buffer_type(&self) -> BufferType;

    /// The buffer a decoded column of this type lands in, or `None` for a wire
    /// type this build does not know how to decode.
    fn decoded_buffer_type(&self) -> Option<BufferType>;
}

impl DataTypeExt for DataType {
    fn type_name(&self) -> String {
        self.to_string()
    }

    fn buffer_type(&self) -> BufferType {
        self.decoded_buffer_type().unwrap_or(BufferType::Float)
    }

    fn decoded_buffer_type(&self) -> Option<BufferType> {
        Some(match *self {
            DataType::F32 | DataType::F64 => BufferType::Float,

            DataType::I8 | DataType::I16 | DataType::I24 | DataType::I32 | DataType::I64 => {
                BufferType::Int
            }

            DataType::U8 | DataType::U16 | DataType::U24 | DataType::U32 | DataType::U64 => {
                BufferType::UInt
            }

            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferType {
    Float,
    Int,
    UInt,
}

/// A segment's map from sample number to time, in seconds after its epoch.
/// Read once by a caller timestamping many rows.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SampleClock {
    start_time: f64,
    period: f64,
}

impl SampleClock {
    pub(crate) fn of(segment: wire::Segment<'_>) -> Self {
        Self {
            start_time: f64::from(segment.start_time),
            period: f64::from(segment.decimation) / f64::from(segment.sampling_rate),
        }
    }

    pub(crate) fn time_at(&self, n: SampleNumber) -> f64 {
        self.start_time + self.period * f64::from(n)
    }
}

/// Host-side timing over a segment descriptor.
pub trait SegmentExt {
    fn time_at(&self, n: SampleNumber) -> f64;
}

impl SegmentExt for wire::Segment<'_> {
    fn time_at(&self, n: SampleNumber) -> f64 {
        SampleClock::of(*self).time_at(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf_proto::SessionId;

    fn route(value: &str) -> DeviceRoute {
        value.parse().unwrap()
    }

    #[test]
    fn a_retained_record_is_relayed_byte_for_byte() {
        let fields = wire::Device {
            session: SessionId::new(42),
            n_streams: 1,
            name: "device",
            serial: "serial",
            firmware: "firmware",
        };
        let record = DeviceRecord::encode(fields).expect("a short record fits");

        assert_eq!(record.get(), fields);
        assert_eq!(
            record.update(route("/1")).unwrap().as_bytes(),
            Packet::metadata(
                wire::Metadata::Device(fields),
                wire::MetadataFlags::UPDATE,
                route("/1"),
            )
            .unwrap()
            .as_bytes()
        );
    }

    #[test]
    fn a_record_too_long_for_a_payload_cannot_be_retained() {
        let long = "n".repeat(200);
        assert!(DeviceRecord::encode(wire::Device {
            session: SessionId::new(42),
            n_streams: 1,
            name: &long,
            serial: &long,
            firmware: &long,
        })
        .is_none());
    }
}

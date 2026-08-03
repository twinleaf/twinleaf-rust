use super::identifiers::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};
use super::{DataType, DecodeError, EncodeError};
use super::{DeviceRoute, Packet, Payload};
use num_enum::{FromPrimitive, IntoPrimitive};

#[derive(Debug, Clone)]
pub struct DeviceMetadata {
    pub serial_number: String,
    pub firmware_hash: String,
    pub n_streams: usize,
    pub session_id: SessionId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamMetadata {
    pub stream_id: StreamId,
    pub name: String,
    pub n_columns: usize,
    pub n_segments: usize,
    pub sample_size: usize,
    pub buf_samples: usize,
}

#[derive(Debug, Clone, PartialEq)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum MetadataEpoch {
    Invalid = 0,
    Zero = 1,
    Systime = 2,
    Unix = 3,
    #[num_enum(catch_all)]
    Unknown(u8),
}

#[derive(Debug, Clone, PartialEq)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum MetadataFilter {
    Unfiltered = 0,
    FirstOrderCascade1 = 1,
    FirstOrderCascade2 = 2,
    #[num_enum(catch_all)]
    Unknown(u8),
}

static TL_METADATA_SEGMENT_VALID: u8 = 0x01;
static TL_METADATA_SEGMENT_ACTIVE: u8 = 0x02;

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentMetadata {
    pub stream_id: StreamId,
    pub segment_id: SegmentId,
    pub flags: u8,
    pub time_ref_epoch: MetadataEpoch,
    pub time_ref_serial: String,
    pub time_ref_session_id: u32,
    pub start_time: u32,
    pub sampling_rate: u32,
    pub decimation: u32,
    pub filter_cutoff: f32,
    pub filter_type: MetadataFilter,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMetadata {
    pub stream_id: StreamId,
    pub index: ColumnId,
    pub data_type: DataType,
    pub name: String,
    pub units: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub enum MetadataContent {
    Device(DeviceMetadata),
    Stream(StreamMetadata),
    Segment(SegmentMetadata),
    Column(ColumnMetadata),
    Unknown(u8),
}

#[derive(Debug, Clone)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum MetadataType {
    Device = 1,
    Stream = 2,
    Segment = 3,
    Column = 4,
    #[num_enum(catch_all)]
    Unknown(u8),
}

static TL_METADATA_PERIODIC: u8 = 0x01;
static TL_METADATA_UPDATE: u8 = 0x02;
static TL_METADATA_LAST: u8 = 0x04;

#[derive(Debug, Clone)]
pub struct MetadataPayload {
    pub content: MetadataContent,
    pub flags: u8,
    // Metadata could have unknown extensions or unknown types, so to be able to
    // re-serialize the packet we carry the unknowns around.
    pub unknown_fixed: Vec<u8>,
    pub unknown_varlen: Vec<u8>,
}

impl DeviceMetadata {
    pub fn deserialize(raw: &[u8]) -> Result<(DeviceMetadata, Vec<u8>, Vec<u8>), DecodeError> {
        let (fixed, varlen) = split_fields(raw)?;
        if fixed.len() < 9 {
            return Err(DecodeError::PayloadTooShort {
                expected: 9,
                actual: fixed.len(),
            });
        }
        let (name, varlen) = take_string(varlen, fixed[1])?;
        let (serial, varlen) = take_string(varlen, fixed[6])?;
        let (firmware, varlen) = take_string(varlen, fixed[7])?;
        if (fixed.len() > 9) && (!varlen.is_empty()) {
            return Err(DecodeError::InvalidPayload);
        }
        Ok((
            DeviceMetadata {
                serial_number: serial,
                firmware_hash: firmware,
                n_streams: fixed[8].into(),
                session_id: u32::from_le_bytes([fixed[2], fixed[3], fixed[4], fixed[5]]),
                name,
            },
            fixed[9..].to_vec(),
            varlen.to_vec(),
        ))
    }
    pub fn serialize(
        &self,
        extra_fixed: &[u8],
        extra_varlen: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), EncodeError> {
        let mut fixed = vec![];
        let mut varlen = vec![];

        fixed.push(9u8);
        fixed.push(append_string(&mut varlen, &self.name)?);
        fixed.extend(self.session_id.to_le_bytes());
        fixed.push(append_string(&mut varlen, &self.serial_number)?);
        fixed.push(append_string(&mut varlen, &self.firmware_hash)?);
        fixed.push(checked_u8_size(self.n_streams)?);
        finish_fields(fixed, varlen, extra_fixed, extra_varlen)
    }
    pub fn make_update_with_route(&self, routing: DeviceRoute) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content: MetadataContent::Device(self.clone()),
                flags: TL_METADATA_UPDATE,
                unknown_fixed: vec![],
                unknown_varlen: vec![],
            }),
            routing,
            ttl: 0,
        }
    }
    pub fn make_update(&self) -> Packet {
        self.make_update_with_route(DeviceRoute::root())
    }
}

impl StreamMetadata {
    pub fn deserialize(raw: &[u8]) -> Result<(StreamMetadata, Vec<u8>, Vec<u8>), DecodeError> {
        let (fixed, varlen) = split_fields(raw)?;
        if fixed.len() < 9 {
            return Err(DecodeError::PayloadTooShort {
                expected: 9,
                actual: fixed.len(),
            });
        }
        let (name, varlen) = take_string(varlen, fixed[8])?;
        if (fixed.len() > 9) && (!varlen.is_empty()) {
            return Err(DecodeError::InvalidPayload);
        }
        Ok((
            StreamMetadata {
                stream_id: fixed[1],
                name,
                n_columns: fixed[2].into(),
                n_segments: fixed[3].into(),
                sample_size: u16::from_le_bytes([fixed[4], fixed[5]]).into(),
                buf_samples: u16::from_le_bytes([fixed[6], fixed[7]]).into(),
            },
            fixed[9..].to_vec(),
            varlen.to_vec(),
        ))
    }
    pub fn serialize(
        &self,
        extra_fixed: &[u8],
        extra_varlen: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), EncodeError> {
        let mut fixed = vec![];
        let mut varlen = vec![];

        fixed.push(9u8);
        fixed.push(self.stream_id);
        fixed.push(checked_u8_size(self.n_columns)?);
        fixed.push(checked_u8_size(self.n_segments)?);
        fixed.extend(checked_u16_size(self.sample_size)?.to_le_bytes());
        fixed.extend(checked_u16_size(self.buf_samples)?.to_le_bytes());
        fixed.push(append_string(&mut varlen, &self.name)?);
        finish_fields(fixed, varlen, extra_fixed, extra_varlen)
    }
    pub fn make_update_with_route(&self, routing: DeviceRoute) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content: MetadataContent::Stream(self.clone()),
                flags: TL_METADATA_UPDATE,
                unknown_fixed: vec![],
                unknown_varlen: vec![],
            }),
            routing,
            ttl: 0,
        }
    }
    pub fn make_update(&self) -> Packet {
        self.make_update_with_route(DeviceRoute::root())
    }
}

impl SegmentMetadata {
    pub fn valid(&self) -> bool {
        (self.flags & TL_METADATA_SEGMENT_VALID) != 0
    }
    pub fn active(&self) -> bool {
        (self.flags & TL_METADATA_SEGMENT_ACTIVE) != 0
    }
    pub fn time_at(&self, n: SampleNumber) -> f64 {
        let period = 1.0 / f64::from(self.sampling_rate) * f64::from(self.decimation);
        f64::from(self.start_time) + period * f64::from(n)
    }
    pub fn deserialize(raw: &[u8]) -> Result<(SegmentMetadata, Vec<u8>, Vec<u8>), DecodeError> {
        let (fixed, varlen) = split_fields(raw)?;
        if fixed.len() < 27 {
            return Err(DecodeError::PayloadTooShort {
                expected: 27,
                actual: fixed.len(),
            });
        }
        let (timeref_serial, varlen) = take_string(varlen, fixed[5])?;
        if (fixed.len() > 27) && (!varlen.is_empty()) {
            return Err(DecodeError::InvalidPayload);
        }
        Ok((
            SegmentMetadata {
                stream_id: fixed[1],
                segment_id: fixed[2],
                flags: fixed[3],
                time_ref_epoch: MetadataEpoch::from(fixed[4]),
                time_ref_serial: timeref_serial,
                time_ref_session_id: u32::from_le_bytes([fixed[6], fixed[7], fixed[8], fixed[9]]),
                start_time: u32::from_le_bytes([fixed[10], fixed[11], fixed[12], fixed[13]]),
                sampling_rate: u32::from_le_bytes([fixed[14], fixed[15], fixed[16], fixed[17]]),
                decimation: u32::from_le_bytes([fixed[18], fixed[19], fixed[20], fixed[21]]),
                filter_cutoff: f32::from_le_bytes([fixed[22], fixed[23], fixed[24], fixed[25]]),
                filter_type: MetadataFilter::from(fixed[26]),
            },
            fixed[27..].to_vec(),
            varlen.to_vec(),
        ))
    }
    pub fn serialize(
        &self,
        extra_fixed: &[u8],
        extra_varlen: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), EncodeError> {
        let mut fixed = vec![];
        let mut varlen = vec![];

        fixed.push(27u8);
        fixed.push(self.stream_id);
        fixed.push(self.segment_id);
        fixed.push(self.flags);
        fixed.push(self.time_ref_epoch.clone().into());
        fixed.push(append_string(&mut varlen, &self.time_ref_serial)?);
        fixed.extend(self.time_ref_session_id.to_le_bytes());
        fixed.extend(self.start_time.to_le_bytes());
        fixed.extend(self.sampling_rate.to_le_bytes());
        fixed.extend(self.decimation.to_le_bytes());
        fixed.extend(self.filter_cutoff.to_le_bytes());
        fixed.push(self.filter_type.clone().into());
        finish_fields(fixed, varlen, extra_fixed, extra_varlen)
    }
    pub fn make_update_with_route(&self, routing: DeviceRoute) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content: MetadataContent::Segment(self.clone()),
                flags: TL_METADATA_UPDATE,
                unknown_fixed: vec![],
                unknown_varlen: vec![],
            }),
            routing,
            ttl: 0,
        }
    }
    pub fn make_update(&self) -> Packet {
        self.make_update_with_route(DeviceRoute::root())
    }
}

impl ColumnMetadata {
    pub fn deserialize(raw: &[u8]) -> Result<(ColumnMetadata, Vec<u8>, Vec<u8>), DecodeError> {
        let (fixed, varlen) = split_fields(raw)?;
        if fixed.len() < 7 {
            return Err(DecodeError::PayloadTooShort {
                expected: 7,
                actual: fixed.len(),
            });
        }
        let (name, varlen) = take_string(varlen, fixed[4])?;
        let (units, varlen) = take_string(varlen, fixed[5])?;
        let (desc, varlen) = take_string(varlen, fixed[6])?;
        if (fixed.len() > 7) && (!varlen.is_empty()) {
            return Err(DecodeError::InvalidPayload);
        }
        Ok((
            ColumnMetadata {
                stream_id: fixed[1],
                index: fixed[2].into(),
                data_type: DataType::from(fixed[3]),
                name,
                units,
                description: desc,
            },
            fixed[7..].to_vec(),
            varlen.to_vec(),
        ))
    }
    pub fn serialize(
        &self,
        extra_fixed: &[u8],
        extra_varlen: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), EncodeError> {
        let mut fixed = vec![];
        let mut varlen = vec![];

        fixed.push(7u8);
        fixed.push(self.stream_id);
        fixed.push(checked_u8_size(self.index)?);
        fixed.push(self.data_type.into());
        fixed.push(append_string(&mut varlen, &self.name)?);
        fixed.push(append_string(&mut varlen, &self.units)?);
        fixed.push(append_string(&mut varlen, &self.description)?);
        finish_fields(fixed, varlen, extra_fixed, extra_varlen)
    }
    pub fn make_update_with_route(&self, routing: DeviceRoute) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content: MetadataContent::Column(self.clone()),
                flags: TL_METADATA_UPDATE,
                unknown_fixed: vec![],
                unknown_varlen: vec![],
            }),
            routing,
            ttl: 0,
        }
    }
    pub fn make_update(&self) -> Packet {
        self.make_update_with_route(DeviceRoute::root())
    }
}

impl MetadataPayload {
    pub fn periodic(&self) -> bool {
        (self.flags & TL_METADATA_PERIODIC) != 0
    }
    pub fn update(&self) -> bool {
        (self.flags & TL_METADATA_UPDATE) != 0
    }
    pub fn last(&self) -> bool {
        (self.flags & TL_METADATA_LAST) != 0
    }
    pub fn deserialize(raw: &[u8]) -> Result<MetadataPayload, DecodeError> {
        if raw.len() < 2 {
            return Err(DecodeError::PayloadTooShort {
                expected: 2,
                actual: raw.len(),
            });
        }
        let (content, ufixed, uvarlen) = match MetadataType::from(raw[0]) {
            MetadataType::Device => {
                let (dm, uf, uv) = DeviceMetadata::deserialize(&raw[2..])?;
                (MetadataContent::Device(dm), uf, uv)
            }
            MetadataType::Stream => {
                let (sm, uf, uv) = StreamMetadata::deserialize(&raw[2..])?;
                (MetadataContent::Stream(sm), uf, uv)
            }
            MetadataType::Segment => {
                let (sm, uf, uv) = SegmentMetadata::deserialize(&raw[2..])?;
                (MetadataContent::Segment(sm), uf, uv)
            }
            MetadataType::Column => {
                let (cm, uf, uv) = ColumnMetadata::deserialize(&raw[2..])?;
                (MetadataContent::Column(cm), uf, uv)
            }
            MetadataType::Unknown(mtype) => {
                let (uf, uv) = split_fields(&raw[2..])?;
                (MetadataContent::Unknown(mtype), uf.to_vec(), uv.to_vec())
            }
        };
        Ok(MetadataPayload {
            content,
            flags: raw[1],
            unknown_fixed: ufixed,
            unknown_varlen: uvarlen,
        })
    }
    pub(super) fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        let (fixed, varlen, mtype) = match &self.content {
            MetadataContent::Device(dm) => {
                let (f, v) = dm.serialize(&self.unknown_fixed, &self.unknown_varlen)?;
                (f, v, MetadataType::Device)
            }
            MetadataContent::Stream(sm) => {
                let (f, v) = sm.serialize(&self.unknown_fixed, &self.unknown_varlen)?;
                (f, v, MetadataType::Stream)
            }
            MetadataContent::Segment(sm) => {
                let (f, v) = sm.serialize(&self.unknown_fixed, &self.unknown_varlen)?;
                (f, v, MetadataType::Segment)
            }
            MetadataContent::Column(cm) => {
                let (f, v) = cm.serialize(&self.unknown_fixed, &self.unknown_varlen)?;
                (f, v, MetadataType::Column)
            }
            MetadataContent::Unknown(mtype) => (
                self.unknown_fixed.clone(),
                self.unknown_varlen.clone(),
                MetadataType::Unknown(*mtype),
            ),
        };
        output.push(mtype.into());
        output.push(self.flags);
        output.extend(fixed);
        output.extend(varlen);
        Ok(())
    }
}

fn split_fields(raw: &[u8]) -> Result<(&[u8], &[u8]), DecodeError> {
    let Some(&fixed_len) = raw.first() else {
        return Err(DecodeError::PayloadTooShort {
            expected: 1,
            actual: 0,
        });
    };
    let fixed_len = usize::from(fixed_len);
    if fixed_len < 2 {
        return Err(DecodeError::InvalidPayload);
    }
    if fixed_len > raw.len() {
        return Err(DecodeError::PayloadTooShort {
            expected: fixed_len,
            actual: raw.len(),
        });
    }
    Ok(raw.split_at(fixed_len))
}

fn take_string(varlen: &[u8], len: u8) -> Result<(String, &[u8]), DecodeError> {
    let len = usize::from(len);
    if len > varlen.len() {
        return Err(DecodeError::PayloadTooShort {
            expected: len,
            actual: varlen.len(),
        });
    }
    let (value, remaining) = varlen.split_at(len);
    Ok((String::from_utf8_lossy(value).into_owned(), remaining))
}

fn checked_u8_size(size: usize) -> Result<u8, EncodeError> {
    u8::try_from(size).map_err(|_| EncodeError::ValueTooLarge {
        value: size,
        maximum: usize::from(u8::MAX),
    })
}

fn checked_u16_size(size: usize) -> Result<u16, EncodeError> {
    u16::try_from(size).map_err(|_| EncodeError::ValueTooLarge {
        value: size,
        maximum: usize::from(u16::MAX),
    })
}

fn append_string(varlen: &mut Vec<u8>, value: &str) -> Result<u8, EncodeError> {
    let len = checked_u8_size(value.len())?;
    varlen.extend(value.as_bytes());
    Ok(len)
}

fn finish_fields(
    mut fixed: Vec<u8>,
    mut varlen: Vec<u8>,
    extra_fixed: &[u8],
    extra_varlen: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), EncodeError> {
    if !extra_varlen.is_empty() && extra_fixed.is_empty() {
        return Err(EncodeError::VariableExtensionWithoutFixed);
    }
    if fixed.is_empty() || usize::from(fixed[0]) != fixed.len() {
        return Err(EncodeError::InvalidFixedExtension);
    }
    fixed[0] = checked_u8_size(fixed.len() + extra_fixed.len())?;
    fixed.extend(extra_fixed);
    varlen.extend(extra_varlen);
    Ok((fixed, varlen))
}

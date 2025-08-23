use super::{
    too_small, vararg, DataType, Error, TioPktHdr, TioPktType, TIO_PACKET_MAX_PAYLOAD_SIZE,
};
use super::{DeviceRoute, Packet, Payload};
use num_enum::{FromPrimitive, IntoPrimitive};

#[derive(Debug, Clone)]
pub struct DeviceMetadata {
    pub serial_number: String,
    pub firmware_hash: String,
    pub n_streams: usize,
    pub session_id: u32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamMetadata {
    pub stream_id: u8,
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
    pub stream_id: u8,
    pub segment_id: u8,
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
    pub stream_id: u8,
    pub index: usize,
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
    pub fn deserialize(
        raw: &[u8],
        full_data: &[u8],
    ) -> Result<(DeviceMetadata, Vec<u8>, Vec<u8>), Error> {
        let (fixed, varlen) = vararg::split(raw, full_data)?;
        if fixed.len() < 9 {
            return Err(too_small(full_data));
        }
        let (name, varlen) = vararg::peel_string(varlen, fixed[1], full_data)?;
        let (serial, varlen) = vararg::peel_string(varlen, fixed[6], full_data)?;
        let (firmware, varlen) = vararg::peel_string(varlen, fixed[7], full_data)?;
        if (fixed.len() > 9) && (varlen.len() > 0) {
            return Err(Error::InvalidPayload(full_data.to_vec()));
        }
        Ok((
            DeviceMetadata {
                serial_number: serial,
                firmware_hash: firmware,
                n_streams: fixed[8].into(),
                session_id: u32::from_le_bytes([fixed[2], fixed[3], fixed[4], fixed[5]]),
                name: name,
            },
            fixed[9..].to_vec(),
            varlen.to_vec(),
        ))
    }
    pub fn serialize(
        &self,
        extra_fixed: &[u8],
        extra_varlen: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), ()> {
        let mut fixed = vec![];
        let mut varlen = vec![];

        fixed.push(9u8);
        fixed.push(vararg::append_string(&mut varlen, &self.name)?);
        fixed.extend(self.session_id.to_le_bytes());
        fixed.push(vararg::append_string(&mut varlen, &self.serial_number)?);
        fixed.push(vararg::append_string(&mut varlen, &self.firmware_hash)?);
        fixed.push(vararg::checked_u8_size(self.n_streams)?);
        Ok(vararg::extend(fixed, varlen, extra_fixed, extra_varlen)?)
    }
    pub fn make_update2(&self, routing: DeviceRoute) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content: MetadataContent::Device(self.clone()),
                flags: TL_METADATA_UPDATE,
                unknown_fixed: vec![],
                unknown_varlen: vec![],
            }),
            routing: routing,
            ttl: 0,
        }
    }
    pub fn make_update(&self) -> Packet {
        self.make_update2(DeviceRoute::root())
    }
}

impl StreamMetadata {
    pub fn deserialize(
        raw: &[u8],
        full_data: &[u8],
    ) -> Result<(StreamMetadata, Vec<u8>, Vec<u8>), Error> {
        let (fixed, varlen) = vararg::split(raw, full_data)?;
        if fixed.len() < 9 {
            return Err(too_small(full_data));
        }
        let (name, varlen) = vararg::peel_string(varlen, fixed[8], full_data)?;
        if (fixed.len() > 9) && (varlen.len() > 0) {
            return Err(Error::InvalidPayload(full_data.to_vec()));
        }
        Ok((
            StreamMetadata {
                stream_id: fixed[1],
                name: name,
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
    ) -> Result<(Vec<u8>, Vec<u8>), ()> {
        let mut fixed = vec![];
        let mut varlen = vec![];

        fixed.push(9u8);
        fixed.push(self.stream_id);
        fixed.push(vararg::checked_u8_size(self.n_columns)?);
        fixed.push(vararg::checked_u8_size(self.n_segments)?);
        fixed.extend(vararg::checked_u16_size(self.sample_size)?.to_le_bytes());
        fixed.extend(vararg::checked_u16_size(self.buf_samples)?.to_le_bytes());
        fixed.push(vararg::append_string(&mut varlen, &self.name)?);
        Ok(vararg::extend(fixed, varlen, extra_fixed, extra_varlen)?)
    }
    pub fn make_update2(&self, routing: DeviceRoute) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content: MetadataContent::Stream(self.clone()),
                flags: TL_METADATA_UPDATE,
                unknown_fixed: vec![],
                unknown_varlen: vec![],
            }),
            routing: routing,
            ttl: 0,
        }
    }
    pub fn make_update(&self) -> Packet {
        self.make_update2(DeviceRoute::root())
    }
}

impl SegmentMetadata {
    pub fn valid(&self) -> bool {
        (self.flags & TL_METADATA_SEGMENT_VALID) != 0
    }
    pub fn active(&self) -> bool {
        (self.flags & TL_METADATA_SEGMENT_ACTIVE) != 0
    }
    pub fn deserialize(
        raw: &[u8],
        full_data: &[u8],
    ) -> Result<(SegmentMetadata, Vec<u8>, Vec<u8>), Error> {
        let (fixed, varlen) = vararg::split(raw, full_data)?;
        if fixed.len() < 27 {
            return Err(too_small(full_data));
        }
        let (timeref_serial, varlen) = vararg::peel_string(varlen, fixed[5], full_data)?;
        if (fixed.len() > 27) && (varlen.len() > 0) {
            return Err(Error::InvalidPayload(full_data.to_vec()));
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
    ) -> Result<(Vec<u8>, Vec<u8>), ()> {
        let mut fixed = vec![];
        let mut varlen = vec![];

        fixed.push(27u8);
        fixed.push(self.stream_id);
        fixed.push(self.segment_id);
        fixed.push(self.flags);
        fixed.push(self.time_ref_epoch.clone().into());
        fixed.push(vararg::append_string(&mut varlen, &self.time_ref_serial)?);
        fixed.extend(self.time_ref_session_id.to_le_bytes());
        fixed.extend(self.start_time.to_le_bytes());
        fixed.extend(self.sampling_rate.to_le_bytes());
        fixed.extend(self.decimation.to_le_bytes());
        fixed.extend(self.filter_cutoff.to_le_bytes());
        fixed.push(self.filter_type.clone().into());
        Ok(vararg::extend(fixed, varlen, extra_fixed, extra_varlen)?)
    }
    pub fn make_update2(&self, routing: DeviceRoute) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content: MetadataContent::Segment(self.clone()),
                flags: TL_METADATA_UPDATE,
                unknown_fixed: vec![],
                unknown_varlen: vec![],
            }),
            routing: routing,
            ttl: 0,
        }
    }
    pub fn make_update(&self) -> Packet {
        self.make_update2(DeviceRoute::root())
    }
}

impl ColumnMetadata {
    pub fn deserialize(
        raw: &[u8],
        full_data: &[u8],
    ) -> Result<(ColumnMetadata, Vec<u8>, Vec<u8>), Error> {
        let (fixed, varlen) = vararg::split(raw, full_data)?;
        if fixed.len() < 7 {
            return Err(too_small(full_data));
        }
        let (name, varlen) = vararg::peel_string(varlen, fixed[4], full_data)?;
        let (units, varlen) = vararg::peel_string(varlen, fixed[5], full_data)?;
        let (desc, varlen) = vararg::peel_string(varlen, fixed[6], full_data)?;
        if (fixed.len() > 7) && (varlen.len() > 0) {
            return Err(Error::InvalidPayload(full_data.to_vec()));
        }
        Ok((
            ColumnMetadata {
                stream_id: fixed[1],
                index: fixed[2].into(),
                data_type: DataType::from(fixed[3]),
                name: name,
                units: units,
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
    ) -> Result<(Vec<u8>, Vec<u8>), ()> {
        let mut fixed = vec![];
        let mut varlen = vec![];

        fixed.push(7u8);
        fixed.push(self.stream_id);
        fixed.push(vararg::checked_u8_size(self.index)?);
        fixed.push(self.data_type.clone().into());
        fixed.push(vararg::append_string(&mut varlen, &self.name)?);
        fixed.push(vararg::append_string(&mut varlen, &self.units)?);
        fixed.push(vararg::append_string(&mut varlen, &self.description)?);
        Ok(vararg::extend(fixed, varlen, extra_fixed, extra_varlen)?)
    }
    pub fn make_update2(&self, routing: DeviceRoute) -> Packet {
        Packet {
            payload: Payload::Metadata(MetadataPayload {
                content: MetadataContent::Column(self.clone()),
                flags: TL_METADATA_UPDATE,
                unknown_fixed: vec![],
                unknown_varlen: vec![],
            }),
            routing: routing,
            ttl: 0,
        }
    }
    pub fn make_update(&self) -> Packet {
        self.make_update2(DeviceRoute::root())
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
    pub fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<MetadataPayload, Error> {
        if raw.len() < 2 {
            return Err(too_small(full_data));
        }
        let (content, ufixed, uvarlen) = match MetadataType::from(raw[0]) {
            MetadataType::Device => {
                let (dm, uf, uv) = DeviceMetadata::deserialize(&raw[2..], full_data)?;
                (MetadataContent::Device(dm), uf, uv)
            }
            MetadataType::Stream => {
                let (sm, uf, uv) = StreamMetadata::deserialize(&raw[2..], full_data)?;
                (MetadataContent::Stream(sm), uf, uv)
            }
            MetadataType::Segment => {
                let (sm, uf, uv) = SegmentMetadata::deserialize(&raw[2..], full_data)?;
                (MetadataContent::Segment(sm), uf, uv)
            }
            MetadataType::Column => {
                let (cm, uf, uv) = ColumnMetadata::deserialize(&raw[2..], full_data)?;
                (MetadataContent::Column(cm), uf, uv)
            }
            MetadataType::Unknown(mtype) => {
                let (uf, uv) = vararg::split(&raw[2..], full_data)?;
                (MetadataContent::Unknown(mtype), uf.to_vec(), uv.to_vec())
            }
        };
        Ok(MetadataPayload {
            content: content,
            flags: raw[1],
            unknown_fixed: ufixed,
            unknown_varlen: uvarlen,
        })
    }
    pub fn serialize(&self) -> Result<Vec<u8>, ()> {
        let (fixed, varlen, mtype) = match &self.content {
            MetadataContent::Device(dm) => {
                let (f, v) = dm.serialize(&self.unknown_fixed, &self.unknown_varlen)?;
                (f, v, MetadataType::Device.into())
            }
            MetadataContent::Stream(sm) => {
                let (f, v) = sm.serialize(&self.unknown_fixed, &self.unknown_varlen)?;
                (f, v, MetadataType::Stream.into())
            }
            MetadataContent::Segment(sm) => {
                let (f, v) = sm.serialize(&self.unknown_fixed, &self.unknown_varlen)?;
                (f, v, MetadataType::Segment.into())
            }
            MetadataContent::Column(cm) => {
                let (f, v) = cm.serialize(&self.unknown_fixed, &self.unknown_varlen)?;
                (f, v, MetadataType::Column.into())
            }
            MetadataContent::Unknown(mtype) => (
                self.unknown_fixed.clone(),
                self.unknown_varlen.clone(),
                MetadataType::Unknown(*mtype),
            ),
        };
        let payload_size = 2 + fixed.len() + varlen.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::Metadata, 0, payload_size as u16);
        ret.push(mtype.into());
        ret.push(self.flags);
        ret.extend(fixed);
        ret.extend(varlen);
        Ok(ret)
    }
}

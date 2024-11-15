use super::{too_small, DataType, Error, TioPktHdr, TioPktType, TIO_PACKET_MAX_PAYLOAD_SIZE};
use num_enum::{FromPrimitive, IntoPrimitive};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum LegacyTimebaseSource {
    Invalid = 0,
    Local = 1,
    Global = 2,
    #[num_enum(catch_all)]
    Unknown(u8),
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum LegacyTimebaseEpoch {
    Invalid = 0,
    Start = 1,
    SysTime = 2,
    Unix = 3,
    GPS = 4,
    #[num_enum(catch_all)]
    Unknown(u8),
}

#[derive(Debug, Clone)]
pub struct LegacyTimebaseInfoPayload {
    pub id: u16,
    pub source: LegacyTimebaseSource,
    pub epoch: LegacyTimebaseEpoch,
    pub start_time: u64,
    pub period_numerator_us: u32,
    pub period_denominator_us: u32,
    pub flags: u32,
    pub stability: f32,
    pub source_id: [u8; 16],
}

#[derive(Debug, Clone)]
pub struct LegacySourceInfoPayload {
    pub id: u16,
    pub timebase_id: u16,
    pub period: u32,
    pub offset: u32,
    _fmt: i32, // originally intended for formatting hints, unused
    pub flags: u16,
    pub channels: u16,
    pub datatype: DataType,
}

#[derive(Debug, Clone)]
pub struct LegacyStreamComponentInfo {
    pub source_id: u16,
    pub flags: u16,
    pub period: u32,
    pub offset: u32,
}

#[derive(Debug, Clone)]
pub struct LegacyStreamInfoPayload {
    pub id: u16,
    pub timebase_id: u16,
    pub period: u32,
    pub offset: u32,
    pub sample_number: u64, // originally intended for formatting hints, unused might be recycled
    pub flags: u16,
    pub components: Vec<LegacyStreamComponentInfo>,
}

#[derive(Debug, Clone)]
pub struct LegacyStreamDataPayload {
    pub sample_n: u32,
    pub data: Vec<u8>,
}

impl LegacyStreamDataPayload {
    pub fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<LegacyStreamDataPayload, Error> {
        if raw.len() < 5 {
            return Err(too_small(full_data));
        }
        Ok(LegacyStreamDataPayload {
            sample_n: u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]),
            data: raw[4..].to_vec(),
        })
    }
    pub fn serialize(&self) -> Result<Vec<u8>, ()> {
        let payload_size = 4 + self.data.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret =
            TioPktHdr::serialize_new(TioPktType::LegacyStreamData, 0, payload_size as u16);
        ret.extend(self.sample_n.to_le_bytes());
        ret.extend(&self.data);
        Ok(ret)
    }
}

pub mod identifiers;
pub mod legacy;
pub mod meta;
pub mod route;
pub mod rpc;
pub mod vararg;

pub use legacy::{
    LegacySourceInfoPayload, LegacyStreamDataPayload, LegacyStreamInfoPayload,
    LegacyTimebaseInfoPayload,
};
pub use meta::{
    ColumnMetadata, DeviceMetadata, MetadataPayload, MetadataType, SegmentMetadata, StreamMetadata,
};
use num_enum::{FromPrimitive, IntoPrimitive};
pub use route::DeviceRoute;
pub use rpc::{RpcErrorCode, RpcErrorPayload, RpcMethod, RpcReplyPayload, RpcRequestPayload};

#[derive(Debug, Clone)]
pub struct GenericPayload {
    pub packet_type: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum LogLevel {
    Critical = 0,
    Error = 1,
    Warning = 2,
    Info = 3,
    Debug = 4,
    #[num_enum(catch_all)]
    Unknown(u8),
}

#[derive(Debug, Clone)]
pub struct LogMessagePayload {
    pub data: u32,
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum HeartbeatPayload {
    Session(u32),
    Any(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct SettingsPayload {
    pub name_len: u8,
    pub flags: u8,
    pub name: String,
    pub reply: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum ProxyStatus {
    SensorDisconnected = 0,
    SensorReconnected = 1,
    FailedToReconnect = 2,
    FailedToConnect = 3,
    #[num_enum(catch_all)]
    Unknown(u8),
}

#[derive(Debug, Clone)]
pub struct ProxyStatusPayload(pub ProxyStatus);

#[derive(Debug, Clone)]
pub struct RpcUpdatePayload(pub RpcMethod);

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum DataType {
    UInt8 = 0x10,
    Int8 = 0x11,
    UInt16 = 0x20,
    Int16 = 0x21,
    UInt24 = 0x30,
    Int24 = 0x31,
    UInt32 = 0x40,
    Int32 = 0x41,
    UInt64 = 0x80,
    Int64 = 0x81,
    Float32 = 0x42,
    Float64 = 0x82,
    #[num_enum(catch_all)]
    Unknown(u8),
}

impl DataType {
    pub fn size(&self) -> usize {
        let raw: u8 = (*self).into();
        (raw >> 4).into()
    }
    pub fn buffer_type(&self) -> BufferType {
        match self {
            DataType::Float32 | DataType::Float64 => BufferType::Float,

            DataType::Int8
            | DataType::Int16
            | DataType::Int24
            | DataType::Int32
            | DataType::Int64 => BufferType::Int,

            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt24
            | DataType::UInt32
            | DataType::UInt64 => BufferType::UInt,

            DataType::Unknown(_) => BufferType::Float,
        }
    }
}

pub enum BufferType {
    Float,
    Int,
    UInt,
}

#[derive(Debug, Clone)]
pub struct StreamDataPayload {
    pub stream_id: u8,
    pub first_sample_n: u32,
    pub segment_id: u8,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum Payload {
    LogMessage(LogMessagePayload),
    RpcRequest(RpcRequestPayload),
    RpcReply(RpcReplyPayload),
    RpcError(RpcErrorPayload),
    Heartbeat(HeartbeatPayload),
    LegacyTimebaseUpdate(LegacyTimebaseInfoPayload),
    LegacySourceUpdate(LegacySourceInfoPayload),
    LegacyStreamUpdate(LegacyStreamInfoPayload),
    LegacyStreamData(LegacyStreamDataPayload),
    Metadata(MetadataPayload),
    Settings(SettingsPayload),
    StreamData(StreamDataPayload),
    ProxyStatus(ProxyStatusPayload),
    RpcUpdate(RpcUpdatePayload),
    Unknown(GenericPayload),
}

#[derive(Debug, Clone)]
pub struct Packet {
    pub payload: Payload,
    pub routing: DeviceRoute,
    pub ttl: usize,
}

#[derive(Debug)]
pub enum Error {
    NeedMore,
    BadName,
    Text(String),
    CRC32(Vec<u8>),
    PacketTooBig(Vec<u8>),
    PacketTooSmall(Vec<u8>),
    InvalidPacketType(Vec<u8>),
    PayloadTooBig(Vec<u8>),
    RoutingTooBig(Vec<u8>),
    PayloadTooSmall(Vec<u8>),
    InvalidPayload(Vec<u8>),
}

#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
enum TioPktType {
    Invalid = 0,
    Log = 1,
    RpcReq = 2,
    RpcRep = 3,
    RpcError = 4,
    Heartbeat = 5,
    LegacyTimebaseUpdate = 6,
    LegacySourceUpdate = 7,
    LegacyStreamUpdate = 8,
    Reserved0 = 9,
    Reserved1 = 10,
    Metadata = 11,
    Settings = 12,
    Reserved2 = 13,
    ProxyStatus = 64,
    RpcUpdate = 65,
    LegacyStreamData = 128,
    #[num_enum(catch_all)]
    UnknownOrStream(u8),
}

static TIO_PTYPE_STREAM0: u8 = 128;

#[repr(C, packed)]
struct TioPktHdr {
    pkt_type: u8,
    routing_size_and_ttl: u8,
    payload_size: u16,
}

pub static TIO_PACKET_HEADER_SIZE: usize = 4;
pub static TIO_PACKET_MAX_ROUTING_SIZE: usize = 8;
pub static TIO_PACKET_MAX_TOTAL_SIZE: usize = 512;
static TIO_PACKET_MAX_PAYLOAD_SIZE: usize =
    TIO_PACKET_MAX_TOTAL_SIZE - TIO_PACKET_HEADER_SIZE - TIO_PACKET_MAX_ROUTING_SIZE;

impl TioPktHdr {
    fn deserialize(raw: &[u8]) -> Result<TioPktHdr, Error> {
        if raw.len() < 1 {
            return Err(Error::NeedMore);
        }

        // Keep the raw packet type for forward compatibility even if it does not match
        // a known type, as long as it's not one of the reserved values
        let packet_type = TioPktType::from(raw[0]);
        let packet_type_valid = match packet_type {
            TioPktType::Invalid | TioPktType::Reserved0 | TioPktType::Reserved1 => false,
            _ => true,
        };
        if !packet_type_valid {
            return Err(Error::InvalidPacketType(raw.to_vec()));
        }

        // If the packet type appears valid, wait to have a full header
        if raw.len() < std::mem::size_of::<TioPktHdr>() {
            return Err(Error::NeedMore);
        }
        let pkt_hdr = TioPktHdr {
            pkt_type: packet_type.into(),
            routing_size_and_ttl: raw[1],
            payload_size: u16::from_le_bytes([raw[2], raw[3]]),
        };

        if pkt_hdr.routing_size() > TIO_PACKET_MAX_ROUTING_SIZE {
            return Err(Error::RoutingTooBig(raw.to_vec()));
        }
        if pkt_hdr.payload_size as usize > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(Error::PayloadTooBig(raw.to_vec()));
        }

        let packet_len = pkt_hdr.packet_size();

        if raw.len() < packet_len {
            return Err(Error::NeedMore);
        }
        Ok(pkt_hdr)
    }

    fn serialize_new(ptype: TioPktType, rsize: u8, psize: u16) -> Vec<u8> {
        TioPktHdr::serialize_new_raw(u8::from(ptype), rsize, psize)
    }

    fn serialize_new_raw(ptype: u8, rsize: u8, psize: u16) -> Vec<u8> {
        let mut ret = vec![ptype, rsize];
        ret.extend(psize.to_le_bytes());
        ret
    }

    fn ptype(&self) -> TioPktType {
        TioPktType::from(self.pkt_type)
    }

    fn stream_id(&self) -> Option<usize> {
        if self.pkt_type >= TIO_PTYPE_STREAM0 {
            Some((self.pkt_type - TIO_PTYPE_STREAM0) as usize)
        } else {
            None
        }
    }

    fn ttl(&self) -> usize {
        (self.routing_size_and_ttl >> 4) as usize
    }

    fn routing_offset(&self) -> usize {
        self.payload_offset() + self.payload_size()
    }

    fn routing_size(&self) -> usize {
        (self.routing_size_and_ttl & 0x0Fu8) as usize
    }

    fn payload_offset(&self) -> usize {
        std::mem::size_of::<TioPktHdr>()
    }

    fn payload_size(&self) -> usize {
        self.payload_size as usize
    }

    fn packet_size(&self) -> usize {
        self.routing_offset() + self.routing_size()
    }
}

fn too_small(full_data: &[u8]) -> Error {
    Error::PayloadTooSmall(full_data.to_vec())
}

impl LogMessagePayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<LogMessagePayload, Error> {
        if raw.len() < 5 {
            return Err(too_small(full_data));
        }
        Ok(LogMessagePayload {
            data: u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]),
            level: LogLevel::from(raw[4]),
            message: String::from_utf8_lossy(&raw[5..]).to_string(),
        })
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        let raw_message = self.message.as_bytes();
        let payload_size = raw_message.len() + 5;
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::Log, 0, payload_size as u16);
        ret.extend(self.data.to_le_bytes());
        ret.push(u8::from(self.level));
        ret.extend(self.message.as_bytes());
        Ok(ret)
    }
}

impl HeartbeatPayload {
    fn deserialize(raw: &[u8], _full_data: &[u8]) -> Result<HeartbeatPayload, Error> {
        if raw.len() == 4 {
            let session = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
            Ok(HeartbeatPayload::Session(session))
        } else {
            Ok(HeartbeatPayload::Any(raw.to_vec()))
        }
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        let payload_size = match self {
            HeartbeatPayload::Session(_) => 4,
            HeartbeatPayload::Any(payload) => payload.len(),
        };
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::Heartbeat, 0, payload_size as u16);
        match self {
            HeartbeatPayload::Session(session) => ret.extend(session.to_le_bytes()),
            HeartbeatPayload::Any(payload) => ret.extend(payload),
        };
        Ok(ret)
    }
}

impl SettingsPayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<SettingsPayload, Error> {
        if raw.len() < 2 {
            return Err(too_small(full_data));
        }
        let name_len = raw[0];
        let flags = raw[1];
        let content = &raw[2..];

        if content.len() < name_len.into() {
            return Err(too_small(full_data));
        }
        let name = String::from_utf8(content[..name_len.into()].to_vec()).map_err(|_| Error::BadName)?;
        let reply = (&content[name_len.into()..]).to_vec();
        let pl = SettingsPayload { name_len, flags, name, reply };
        Ok(pl)
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        let payload_size: usize = 2 + self.name_len as usize + self.reply.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::Settings, 0, payload_size as u16);
        ret.extend(self.name_len.to_le_bytes());
        ret.extend(self.flags.to_le_bytes());
        ret.extend(self.name.clone().into_bytes());
        ret.extend(self.reply.clone());
        Ok(ret)
    }
}

impl StreamDataPayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<StreamDataPayload, Error> {
        if raw.len() < 5 {
            return Err(too_small(full_data));
        }
        Ok(StreamDataPayload {
            stream_id: full_data[0] - TIO_PTYPE_STREAM0,
            first_sample_n: u32::from_le_bytes([raw[0], raw[1], raw[2], 0u8]),
            segment_id: raw[3],
            data: raw[4..].to_vec(),
        })
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        if (self.stream_id < 1) || (self.stream_id > 127) {
            return Err(());
        }
        let sample_ser = self.first_sample_n.to_le_bytes();
        if sample_ser[3] != 0 {
            return Err(());
        }
        let payload_size = 4 + self.data.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(
            TioPktType::UnknownOrStream(TIO_PTYPE_STREAM0 + self.stream_id),
            0,
            payload_size as u16,
        );
        ret.extend([sample_ser[0], sample_ser[1], sample_ser[2], self.segment_id]);
        ret.extend(&self.data);
        Ok(ret)
    }
}

impl ProxyStatusPayload {
    pub fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<ProxyStatusPayload, Error> {
        if raw.is_empty() {
            return Err(too_small(full_data));
        }
        Ok(ProxyStatusPayload(ProxyStatus::from(raw[0])))
    }

    pub fn serialize(&self) -> Result<Vec<u8>, ()> {
        let mut ret = TioPktHdr::serialize_new(TioPktType::ProxyStatus, 0, 1);
        ret.push(u8::from(self.0));
        Ok(ret)
    }
}

const RPC_METHOD_TYPE_ID: u8 = 0;
const RPC_METHOD_TYPE_NAME: u8 = 1;
impl RpcUpdatePayload {
    pub fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<RpcUpdatePayload, Error> {
        if raw.is_empty() {
            return Err(too_small(full_data));
        }
        let method = match raw[0] {
            RPC_METHOD_TYPE_ID => {
                if raw.len() < 3 {
                    return Err(too_small(full_data));
                }
                RpcMethod::Id(u16::from_le_bytes([raw[1], raw[2]]))
            }
            RPC_METHOD_TYPE_NAME => {
                if raw.len() < 3 {
                    return Err(too_small(full_data));
                }
                let name_len = u16::from_le_bytes([raw[1], raw[2]]) as usize;
                if raw.len() < 3 + name_len {
                    return Err(too_small(full_data));
                }
                RpcMethod::Name(String::from_utf8_lossy(&raw[3..3 + name_len]).to_string())
            }
            _ => return Err(Error::InvalidPayload(full_data.to_vec())),
        };
        Ok(RpcUpdatePayload(method))
    }

    pub fn serialize(&self) -> Result<Vec<u8>, ()> {
        let payload_bytes: Vec<u8> = match &self.0 {
            RpcMethod::Id(id) => {
                let mut v = vec![RPC_METHOD_TYPE_ID];
                v.extend(id.to_le_bytes());
                v
            }
            RpcMethod::Name(name) => {
                let name_bytes = name.as_bytes();
                let mut v = vec![RPC_METHOD_TYPE_NAME];
                v.extend((name_bytes.len() as u16).to_le_bytes());
                v.extend(name_bytes);
                v
            }
        };
        let mut ret =
            TioPktHdr::serialize_new(TioPktType::RpcUpdate, 0, payload_bytes.len() as u16);
        ret.extend(payload_bytes);
        Ok(ret)
    }
}

impl GenericPayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<GenericPayload, Error> {
        Ok(GenericPayload {
            packet_type: full_data[0],
            payload: raw.to_vec(),
        })
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        if self.payload.len() > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new_raw(self.packet_type, 0, self.payload.len() as u16);
        ret.extend(&self.payload);
        Ok(ret)
    }
}

impl Payload {
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        match self {
            Payload::LogMessage(p) => p.serialize(),
            Payload::RpcRequest(p) => p.serialize(),
            Payload::RpcReply(p) => p.serialize(),
            Payload::RpcError(p) => p.serialize(),
            Payload::Heartbeat(p) => p.serialize(),
            Payload::Metadata(p) => p.serialize(),
            Payload::Settings(p) => p.serialize(),
            Payload::LegacyStreamData(p) => p.serialize(),
            Payload::StreamData(p) => p.serialize(),
            Payload::ProxyStatus(p) => p.serialize(),
            Payload::RpcUpdate(p) => p.serialize(),
            Payload::Unknown(p) => p.serialize(),
            _ => Err(()),
        }
    }
    fn deserialize(
        hdr: &TioPktHdr,
        raw_payload: &[u8],
        full_data: &[u8],
    ) -> Result<Payload, Error> {
        match hdr.ptype() {
            TioPktType::Invalid
            | TioPktType::Reserved0
            | TioPktType::Reserved1
            | TioPktType::Reserved2 => {
                // This should never happen for how the code is organized, since
                // it should be ruled out by parsing the header first, but handle
                // this case anyway.
                return Err(Error::InvalidPacketType(full_data.to_vec()));
            }
            TioPktType::Log => Ok(Payload::LogMessage(LogMessagePayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::RpcReq => Ok(Payload::RpcRequest(RpcRequestPayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::RpcRep => Ok(Payload::RpcReply(RpcReplyPayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::RpcError => Ok(Payload::RpcError(RpcErrorPayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::Heartbeat => Ok(Payload::Heartbeat(HeartbeatPayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::LegacyTimebaseUpdate
            | TioPktType::LegacySourceUpdate
            | TioPktType::LegacyStreamUpdate => {
                // For now we deserialize these just into generic payloads, so they can
                // be sent around by the proxy. TODO: full ser/sed for legacy types,
                // which would also let us get rid of TioPktHdr::serialize_new_raw,
                // and handle all cases in Payload::serialize().
                Ok(Payload::Unknown(GenericPayload::deserialize(
                    raw_payload,
                    full_data,
                )?))
            }
            TioPktType::LegacyStreamData => Ok(Payload::LegacyStreamData(
                LegacyStreamDataPayload::deserialize(raw_payload, full_data)?,
            )),
            TioPktType::Metadata => Ok(Payload::Metadata(MetadataPayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::Settings => Ok(Payload::Settings(SettingsPayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::ProxyStatus => Ok(Payload::ProxyStatus(ProxyStatusPayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::RpcUpdate => Ok(Payload::RpcUpdate(RpcUpdatePayload::deserialize(
                raw_payload,
                full_data,
            )?)),
            TioPktType::UnknownOrStream(_) => {
                if let Some(_) = hdr.stream_id() {
                    Ok(Payload::StreamData(StreamDataPayload::deserialize(
                        raw_payload,
                        full_data,
                    )?))
                } else {
                    Ok(Payload::Unknown(GenericPayload::deserialize(
                        raw_payload,
                        full_data,
                    )?))
                }
            }
        }
    }
}

impl Packet {
    pub fn deserialize(raw: &[u8]) -> Result<(Packet, usize), Error> {
        let pkt_hdr = TioPktHdr::deserialize(raw)?;
        let pkt_len = pkt_hdr.packet_size();
        let payload_raw = &raw[pkt_hdr.payload_offset()..pkt_hdr.routing_offset()];
        let routing_raw = &raw[pkt_hdr.routing_offset()..pkt_len];
        let payload = Payload::deserialize(&pkt_hdr, payload_raw, raw)?;

        Ok((
            Packet {
                payload: payload,
                routing: DeviceRoute::from_bytes(routing_raw)
                    .expect("routing should have been validated in header deserialization"),
                ttl: pkt_hdr.ttl(),
            },
            pkt_len,
        ))
    }

    pub fn serialize(&self) -> Result<Vec<u8>, ()> {
        let ret = self.payload.serialize()?;
        self.routing.serialize(ret)
    }
}

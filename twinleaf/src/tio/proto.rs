pub mod identifiers;
pub mod legacy;
pub mod meta;
pub mod route;
mod rpc;

use bytes::Bytes;
pub use legacy::{
    LegacySourceInfoPayload, LegacyStreamDataPayload, LegacyStreamInfoPayload,
    LegacyTimebaseInfoPayload,
};
pub use meta::{
    ColumnMetadata, DeviceMetadata, MetadataPayload, MetadataType, SegmentMetadata, StreamMetadata,
};
use num_enum::{FromPrimitive, IntoPrimitive};
pub use route::DeviceRoute;
pub use rpc::{
    RpcAccess, RpcArgs, RpcDecodeError, RpcErrorCode, RpcErrorPayload, RpcMeta, RpcMetaFlags,
    RpcMethod, RpcReply, RpcReplyFixedSize, RpcReplyPayload, RpcRequestPayload, RpcStringLen,
    RpcValue, RpcValueDecodeError, RpcValueEncodeError, RpcValueType,
};

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
pub enum SettingsPayload {
    RpcHash(u32),
    Unknown {
        name: String,
        flags: u8,
        reply: Vec<u8>,
    },
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
    pub fn type_name(&self) -> String {
        self.to_string()
    }

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

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::UInt8 => write!(f, "u8"),
            DataType::Int8 => write!(f, "i8"),
            DataType::UInt16 => write!(f, "u16"),
            DataType::Int16 => write!(f, "i16"),
            DataType::UInt24 => write!(f, "u24"),
            DataType::Int24 => write!(f, "i24"),
            DataType::UInt32 => write!(f, "u32"),
            DataType::Int32 => write!(f, "i32"),
            DataType::UInt64 => write!(f, "u64"),
            DataType::Int64 => write!(f, "i64"),
            DataType::Float32 => write!(f, "f32"),
            DataType::Float64 => write!(f, "f64"),
            DataType::Unknown(n) => write!(f, "raw{}", n),
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
    pub data: Bytes,
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

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("more data needed")]
    NeedMore,
    #[error("bad name")]
    BadName,
    #[error("CRC32 mismatch")]
    CRC32(Vec<u8>),
    #[error("packet too big")]
    PacketTooBig(Vec<u8>),
    #[error("packet too small")]
    PacketTooSmall(Vec<u8>),
    #[error("invalid packet type")]
    InvalidPacketType(Vec<u8>),
    #[error("payload too big")]
    PayloadTooBig(Vec<u8>),
    #[error("routing too big")]
    RoutingTooBig(Vec<u8>),
    #[error("payload is too short: expected at least {expected} bytes, got {actual}")]
    PayloadTooShort { expected: usize, actual: usize },
    #[error("invalid payload")]
    InvalidPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    #[error("encoded payload is {actual} bytes; maximum is {maximum}")]
    PayloadTooLarge { actual: usize, maximum: usize },
    #[error("value {value} exceeds the encoding maximum of {maximum}")]
    ValueTooLarge { value: usize, maximum: usize },
    #[error("stream ID {0} is outside the encodable range 1..=127")]
    InvalidStreamId(u8),
    #[error("sample number {0} does not fit in the 24-bit packet field")]
    SampleNumberTooLarge(u32),
    #[error("this payload variant does not have a wire encoder")]
    UnsupportedPayload,
    #[error("variable metadata extensions require a fixed extension")]
    VariableExtensionWithoutFixed,
    #[error("fixed metadata extension has an invalid length prefix")]
    InvalidFixedExtension,
}

impl EncodeError {
    fn payload_too_large(actual: usize) -> Self {
        Self::PayloadTooLarge {
            actual,
            maximum: TIO_PACKET_MAX_PAYLOAD_SIZE,
        }
    }
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

pub const TIO_PACKET_HEADER_SIZE: usize = 4;
pub const TIO_PACKET_MAX_ROUTING_SIZE: usize = 8;
pub const TIO_PACKET_MAX_TOTAL_SIZE: usize = 512;
const TIO_PACKET_MAX_PAYLOAD_SIZE: usize =
    TIO_PACKET_MAX_TOTAL_SIZE - TIO_PACKET_HEADER_SIZE - TIO_PACKET_MAX_ROUTING_SIZE;
/// Largest TTL representable by the header's high nibble.
const TIO_PACKET_MAX_TTL: usize = 0x0f;

impl TioPktHdr {
    fn deserialize(raw: &[u8]) -> Result<TioPktHdr, DecodeError> {
        if raw.is_empty() {
            return Err(DecodeError::NeedMore);
        }

        // Keep the raw packet type for forward compatibility even if it does not match
        // a known type, as long as it's not one of the reserved values
        let packet_type = TioPktType::from(raw[0]);
        let packet_type_valid = !matches!(
            packet_type,
            TioPktType::Invalid | TioPktType::Reserved0 | TioPktType::Reserved1
        );
        if !packet_type_valid {
            return Err(DecodeError::InvalidPacketType(raw.to_vec()));
        }

        // If the packet type appears valid, wait to have a full header
        if raw.len() < std::mem::size_of::<TioPktHdr>() {
            return Err(DecodeError::NeedMore);
        }
        let pkt_hdr = TioPktHdr {
            pkt_type: packet_type.into(),
            routing_size_and_ttl: raw[1],
            payload_size: u16::from_le_bytes([raw[2], raw[3]]),
        };

        if pkt_hdr.routing_size() > TIO_PACKET_MAX_ROUTING_SIZE {
            return Err(DecodeError::RoutingTooBig(raw.to_vec()));
        }
        if pkt_hdr.payload_size as usize > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(DecodeError::PayloadTooBig(raw.to_vec()));
        }

        let packet_len = pkt_hdr.packet_size();

        if raw.len() < packet_len {
            return Err(DecodeError::NeedMore);
        }
        Ok(pkt_hdr)
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

impl LogMessagePayload {
    fn deserialize(raw: &[u8]) -> Result<LogMessagePayload, DecodeError> {
        if raw.len() < 5 {
            return Err(DecodeError::PayloadTooShort {
                expected: 5,
                actual: raw.len(),
            });
        }
        Ok(LogMessagePayload {
            data: u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]),
            level: LogLevel::from(raw[4]),
            message: String::from_utf8_lossy(&raw[5..]).to_string(),
        })
    }
    fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        output.extend(self.data.to_le_bytes());
        output.push(u8::from(self.level));
        output.extend(self.message.as_bytes());
        Ok(())
    }
}

impl HeartbeatPayload {
    fn deserialize(raw: &[u8]) -> Result<HeartbeatPayload, DecodeError> {
        if raw.len() == 4 {
            let session = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
            Ok(HeartbeatPayload::Session(session))
        } else {
            Ok(HeartbeatPayload::Any(raw.to_vec()))
        }
    }
    fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            HeartbeatPayload::Session(session) => output.extend(session.to_le_bytes()),
            HeartbeatPayload::Any(payload) => output.extend(payload),
        };
        Ok(())
    }
}

impl SettingsPayload {
    fn deserialize(raw: &[u8]) -> Result<SettingsPayload, DecodeError> {
        if raw.len() < 2 {
            return Err(DecodeError::PayloadTooShort {
                expected: 2,
                actual: raw.len(),
            });
        }
        let name_len = raw[0] as usize;
        let flags = raw[1];
        let content = &raw[2..];

        if content.len() < name_len {
            return Err(DecodeError::PayloadTooShort {
                expected: 2 + name_len,
                actual: raw.len(),
            });
        }
        let name =
            String::from_utf8(content[..name_len].to_vec()).map_err(|_| DecodeError::BadName)?;
        let reply = content[name_len..].to_vec();

        match name.as_str() {
            "rpc.hash" => {
                if reply.len() < 4 {
                    return Err(DecodeError::PayloadTooShort {
                        expected: 2 + name_len + 4,
                        actual: raw.len(),
                    });
                }
                let hash = u32::from_le_bytes(reply[..4].try_into().unwrap());
                Ok(SettingsPayload::RpcHash(hash))
            }
            _ => Ok(SettingsPayload::Unknown { name, flags, reply }),
        }
    }
    fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            SettingsPayload::RpcHash(hash) => {
                let name = b"rpc.hash";
                output.push(name.len() as u8);
                output.push(0); // flags
                output.extend(name);
                output.extend(hash.to_le_bytes());
            }
            SettingsPayload::Unknown { name, flags, reply } => {
                if name.len() > u8::MAX.into() {
                    return Err(EncodeError::ValueTooLarge {
                        value: name.len(),
                        maximum: u8::MAX.into(),
                    });
                }
                output.push(name.len() as u8);
                output.push(*flags);
                output.extend(name.as_bytes());
                output.extend(reply);
            }
        }
        Ok(())
    }
}

impl StreamDataPayload {
    fn deserialize(raw: &[u8], stream_id: u8) -> Result<StreamDataPayload, DecodeError> {
        Self::deserialize_bytes(Bytes::copy_from_slice(raw), stream_id)
    }
    fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        let sample_ser = self.first_sample_n.to_le_bytes();
        if sample_ser[3] != 0 {
            return Err(EncodeError::SampleNumberTooLarge(self.first_sample_n));
        }
        output.extend([sample_ser[0], sample_ser[1], sample_ser[2], self.segment_id]);
        output.extend(&self.data);
        Ok(())
    }
}

impl StreamDataPayload {
    fn deserialize_bytes(raw: Bytes, stream_id: u8) -> Result<Self, DecodeError> {
        if raw.len() < 5 {
            return Err(DecodeError::PayloadTooShort {
                expected: 5,
                actual: raw.len(),
            });
        }
        Ok(Self {
            stream_id,
            first_sample_n: u32::from_le_bytes([raw[0], raw[1], raw[2], 0u8]),
            segment_id: raw[3],
            data: raw.slice(4..),
        })
    }
}

impl ProxyStatusPayload {
    pub fn deserialize(raw: &[u8]) -> Result<ProxyStatusPayload, DecodeError> {
        if raw.is_empty() {
            return Err(DecodeError::PayloadTooShort {
                expected: 1,
                actual: 0,
            });
        }
        Ok(ProxyStatusPayload(ProxyStatus::from(raw[0])))
    }

    fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        output.push(u8::from(self.0));
        Ok(())
    }
}

const RPC_METHOD_TYPE_ID: u8 = 0;
const RPC_METHOD_TYPE_NAME: u8 = 1;
impl RpcUpdatePayload {
    pub fn deserialize(raw: &[u8]) -> Result<RpcUpdatePayload, DecodeError> {
        if raw.is_empty() {
            return Err(DecodeError::PayloadTooShort {
                expected: 1,
                actual: 0,
            });
        }
        let method = match raw[0] {
            RPC_METHOD_TYPE_ID => {
                if raw.len() < 3 {
                    return Err(DecodeError::PayloadTooShort {
                        expected: 3,
                        actual: raw.len(),
                    });
                }
                RpcMethod::Id(u16::from_le_bytes([raw[1], raw[2]]))
            }
            RPC_METHOD_TYPE_NAME => {
                if raw.len() < 3 {
                    return Err(DecodeError::PayloadTooShort {
                        expected: 3,
                        actual: raw.len(),
                    });
                }
                let name_len = u16::from_le_bytes([raw[1], raw[2]]) as usize;
                if raw.len() < 3 + name_len {
                    return Err(DecodeError::PayloadTooShort {
                        expected: 3 + name_len,
                        actual: raw.len(),
                    });
                }
                RpcMethod::Name(String::from_utf8_lossy(&raw[3..3 + name_len]).to_string())
            }
            _ => return Err(DecodeError::InvalidPayload),
        };
        Ok(RpcUpdatePayload(method))
    }

    fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        match &self.0 {
            RpcMethod::Id(id) => {
                output.push(RPC_METHOD_TYPE_ID);
                output.extend(id.to_le_bytes());
            }
            RpcMethod::Name(name) => {
                let name_bytes = name.as_bytes();
                output.push(RPC_METHOD_TYPE_NAME);
                output.extend((name_bytes.len() as u16).to_le_bytes());
                output.extend(name_bytes);
            }
        }
        Ok(())
    }
}

impl GenericPayload {
    fn deserialize(raw: &[u8], packet_type: u8) -> Result<GenericPayload, DecodeError> {
        Ok(GenericPayload {
            packet_type,
            payload: raw.to_vec(),
        })
    }
    fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        output.extend(&self.payload);
        Ok(())
    }
}

impl Payload {
    fn packet_type(&self) -> Result<u8, EncodeError> {
        let packet_type = match self {
            Payload::LogMessage(_) => TioPktType::Log.into(),
            Payload::RpcRequest(_) => TioPktType::RpcReq.into(),
            Payload::RpcReply(_) => TioPktType::RpcRep.into(),
            Payload::RpcError(_) => TioPktType::RpcError.into(),
            Payload::Heartbeat(_) => TioPktType::Heartbeat.into(),
            Payload::Metadata(_) => TioPktType::Metadata.into(),
            Payload::Settings(_) => TioPktType::Settings.into(),
            Payload::LegacyStreamData(_) => TioPktType::LegacyStreamData.into(),
            Payload::StreamData(payload) => {
                if !(1..=127).contains(&payload.stream_id) {
                    return Err(EncodeError::InvalidStreamId(payload.stream_id));
                }
                TIO_PTYPE_STREAM0 + payload.stream_id
            }
            Payload::ProxyStatus(_) => TioPktType::ProxyStatus.into(),
            Payload::RpcUpdate(_) => TioPktType::RpcUpdate.into(),
            Payload::Unknown(payload) => payload.packet_type,
            _ => return Err(EncodeError::UnsupportedPayload),
        };
        Ok(packet_type)
    }

    fn encode_body(&self, output: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            Payload::LogMessage(p) => p.encode_body(output),
            Payload::RpcRequest(p) => p.encode_body(output),
            Payload::RpcReply(p) => p.encode_body(output),
            Payload::RpcError(p) => p.encode_body(output),
            Payload::Heartbeat(p) => p.encode_body(output),
            Payload::Metadata(p) => p.encode_body(output),
            Payload::Settings(p) => p.encode_body(output),
            Payload::LegacyStreamData(p) => p.encode_body(output),
            Payload::StreamData(p) => p.encode_body(output),
            Payload::ProxyStatus(p) => p.encode_body(output),
            Payload::RpcUpdate(p) => p.encode_body(output),
            Payload::Unknown(p) => p.encode_body(output),
            _ => Err(EncodeError::UnsupportedPayload),
        }
    }
    fn deserialize(hdr: &TioPktHdr, raw_payload: &[u8]) -> Result<Payload, DecodeError> {
        match hdr.ptype() {
            TioPktType::Invalid
            | TioPktType::Reserved0
            | TioPktType::Reserved1
            | TioPktType::Reserved2 => {
                // This should never happen for how the code is organized, since
                // it should be ruled out by parsing the header first, but handle
                // this case anyway.
                Err(DecodeError::InvalidPacketType(vec![hdr.pkt_type]))
            }
            TioPktType::Log => Ok(Payload::LogMessage(LogMessagePayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::RpcReq => Ok(Payload::RpcRequest(RpcRequestPayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::RpcRep => Ok(Payload::RpcReply(RpcReplyPayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::RpcError => Ok(Payload::RpcError(RpcErrorPayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::Heartbeat => Ok(Payload::Heartbeat(HeartbeatPayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::LegacyTimebaseUpdate
            | TioPktType::LegacySourceUpdate
            | TioPktType::LegacyStreamUpdate => {
                // For now we deserialize these just into generic payloads, so they can
                // be sent around by the proxy. TODO: fully decode legacy metadata.
                Ok(Payload::Unknown(GenericPayload::deserialize(
                    raw_payload,
                    hdr.pkt_type,
                )?))
            }
            TioPktType::LegacyStreamData => Ok(Payload::LegacyStreamData(
                LegacyStreamDataPayload::deserialize(raw_payload)?,
            )),
            TioPktType::Metadata => Ok(Payload::Metadata(MetadataPayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::Settings => Ok(Payload::Settings(SettingsPayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::ProxyStatus => Ok(Payload::ProxyStatus(ProxyStatusPayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::RpcUpdate => Ok(Payload::RpcUpdate(RpcUpdatePayload::deserialize(
                raw_payload,
            )?)),
            TioPktType::UnknownOrStream(_) => {
                if let Some(stream_id) = hdr.stream_id() {
                    Ok(Payload::StreamData(StreamDataPayload::deserialize(
                        raw_payload,
                        stream_id as u8,
                    )?))
                } else {
                    Ok(Payload::Unknown(GenericPayload::deserialize(
                        raw_payload,
                        hdr.pkt_type,
                    )?))
                }
            }
        }
    }
}

impl Packet {
    pub fn rpc_request(name: &str, arg: &[u8], id: u16, routing: DeviceRoute) -> Self {
        Self {
            payload: Payload::RpcRequest(RpcRequestPayload {
                id,
                method: RpcMethod::Name(name.into()),
                arg: arg.to_vec(),
            }),
            routing,
            ttl: 0,
        }
    }

    pub fn rpc_error(id: u16, error: RpcErrorCode, routing: DeviceRoute) -> Self {
        Self {
            payload: Payload::RpcError(RpcErrorPayload {
                id,
                error,
                extra: Vec::new(),
            }),
            routing,
            ttl: 0,
        }
    }

    pub fn heartbeat(payload: Vec<u8>, routing: DeviceRoute) -> Self {
        Self {
            payload: Payload::Heartbeat(HeartbeatPayload::Any(payload)),
            routing,
            ttl: 0,
        }
    }

    /// Deserialize from caller-owned shared storage. Stream sample bytes are a
    /// zero-copy [`Bytes`] slice of `raw`.
    pub fn deserialize_bytes(raw: &Bytes) -> Result<(Packet, usize), DecodeError> {
        let pkt_hdr = TioPktHdr::deserialize(raw.as_ref())?;
        let pkt_len = pkt_hdr.packet_size();
        let payload_range = pkt_hdr.payload_offset()..pkt_hdr.routing_offset();
        let payload_raw = &raw[payload_range.clone()];
        let routing_raw = &raw[pkt_hdr.routing_offset()..pkt_len];
        let payload = match pkt_hdr.ptype() {
            TioPktType::UnknownOrStream(_) => match pkt_hdr.stream_id() {
                Some(stream_id) => Payload::StreamData(StreamDataPayload::deserialize_bytes(
                    raw.slice(payload_range),
                    stream_id as u8,
                )?),
                None => Payload::deserialize(&pkt_hdr, payload_raw)?,
            },
            _ => Payload::deserialize(&pkt_hdr, payload_raw)?,
        };

        Ok((
            Packet {
                payload,
                routing: DeviceRoute::from_bytes(routing_raw)
                    .expect("routing should have been validated in header deserialization"),
                ttl: pkt_hdr.ttl(),
            },
            pkt_len,
        ))
    }

    pub fn deserialize(raw: &[u8]) -> Result<(Packet, usize), DecodeError> {
        let pkt_len = TioPktHdr::deserialize(raw)?.packet_size();
        Self::deserialize_bytes(&Bytes::copy_from_slice(&raw[..pkt_len]))
    }

    pub fn serialize(&self) -> Result<Vec<u8>, EncodeError> {
        let packet_type = self.payload.packet_type()?;
        let mut ret = vec![packet_type, 0, 0, 0];
        self.payload.encode_body(&mut ret)?;
        let payload_size = ret.len() - TIO_PACKET_HEADER_SIZE;
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(EncodeError::payload_too_large(payload_size));
        }
        if self.ttl > TIO_PACKET_MAX_TTL {
            return Err(EncodeError::ValueTooLarge {
                value: self.ttl,
                maximum: TIO_PACKET_MAX_TTL,
            });
        }
        ret[2..4].copy_from_slice(&(payload_size as u16).to_le_bytes());
        ret[1] = ((self.ttl as u8) << 4) | self.routing.len() as u8;
        ret.extend(self.routing.iter().rev());
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DeviceRoute, EncodeError, HeartbeatPayload, Packet, Payload, SettingsPayload,
        TIO_PACKET_MAX_PAYLOAD_SIZE, TIO_PACKET_MAX_TTL,
    };

    fn packet(payload: Payload) -> Packet {
        Packet {
            payload,
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    #[test]
    fn encode_reports_payload_size_limit() {
        let actual = TIO_PACKET_MAX_PAYLOAD_SIZE + 1;
        let error = packet(Payload::Heartbeat(HeartbeatPayload::Any(vec![0; actual])))
            .serialize()
            .unwrap_err();

        assert_eq!(
            error,
            EncodeError::PayloadTooLarge {
                actual,
                maximum: TIO_PACKET_MAX_PAYLOAD_SIZE,
            }
        );
    }

    #[test]
    fn rpc_request_encoding_preserves_wire_layout() {
        let route = "/1/2".parse::<DeviceRoute>().unwrap();
        let encoded = Packet::rpc_request("x", &[1, 2], 0x1234, route)
            .serialize()
            .unwrap();

        assert_eq!(
            encoded,
            [
                2, 2, 7, 0, // RPC request header, two routing hops
                0x34, 0x12, // request ID
                1, 0x80, b'x', // by-name method
                1, 2, // arguments
                2, 1, // routing is reversed on the wire
            ]
        );
    }

    #[test]
    fn ttl_survives_a_serialize_roundtrip() {
        let sent = Packet {
            payload: Payload::Heartbeat(HeartbeatPayload::Session(1)),
            routing: "/1/2".parse::<DeviceRoute>().unwrap(),
            ttl: 3,
        };
        let (received, _) = Packet::deserialize(&sent.serialize().unwrap()).unwrap();

        assert_eq!(received.ttl, 3);
        assert_eq!(received.routing, sent.routing);
    }

    #[test]
    fn encode_rejects_a_ttl_that_does_not_fit_the_header_nibble() {
        let error = Packet {
            payload: Payload::Heartbeat(HeartbeatPayload::Session(1)),
            routing: DeviceRoute::root(),
            ttl: TIO_PACKET_MAX_TTL + 1,
        }
        .serialize()
        .unwrap_err();

        assert_eq!(
            error,
            EncodeError::ValueTooLarge {
                value: TIO_PACKET_MAX_TTL + 1,
                maximum: TIO_PACKET_MAX_TTL,
            }
        );
    }

    #[test]
    fn encode_rejects_settings_names_that_do_not_fit_the_length_field() {
        let error = packet(Payload::Settings(SettingsPayload::Unknown {
            name: "x".repeat(usize::from(u8::MAX) + 1),
            flags: 0,
            reply: Vec::new(),
        }))
        .serialize()
        .unwrap_err();

        assert_eq!(
            error,
            EncodeError::ValueTooLarge {
                value: usize::from(u8::MAX) + 1,
                maximum: usize::from(u8::MAX),
            }
        );
    }
}

use num_enum::{FromPrimitive, IntoPrimitive};

#[derive(Debug, Clone)]
pub struct GenericPayload {
    pub packet_type: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum LogLevel {
    CRITICAL = 0,
    ERROR = 1,
    WARNING = 2,
    INFO = 3,
    DEBUG = 4,
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
pub enum RpcMethod {
    Id(u16),
    Name(String),
}

#[derive(Debug, Clone)]
pub struct RpcRequestPayload {
    pub id: u16,
    pub method: RpcMethod,
    pub arg: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RpcReplyPayload {
    pub id: u16,
    pub reply: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum RpcErrorCode {
    NoError = 0,
    Undefined = 1,
    NotFound = 2,
    MalformedRequest = 3,
    WrongSizeArgs = 4,
    InvalidArgs = 5,
    ReadOnly = 6,
    WriteOnly = 7,
    Timeout = 8,
    Busy = 9,
    WrongDeviceState = 10,
    LoadFailed = 11,
    LoadRpcFailed = 12,
    SaveFailed = 13,
    SaveWriteFailed = 14,
    Internal = 15,
    OutOfMemory = 16,
    OutOfRange = 17,
    #[num_enum(catch_all)]
    Unknown(u16),
}

#[derive(Debug, Clone)]
pub struct RpcErrorPayload {
    pub id: u16,
    pub error: RpcErrorCode,
    pub extra: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum HeartbeatPayload {
    Session(u32),
    Any(Vec<u8>),
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum TimebaseSource {
    Invalid = 0,
    Local = 1,
    Global = 2,
    #[num_enum(catch_all)]
    Unknown(u8),
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum TimebaseEpoch {
    Invalid = 0,
    Start = 1,
    SysTime = 2,
    Unix = 3,
    GPS = 4,
    #[num_enum(catch_all)]
    Unknown(u8),
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[derive(FromPrimitive, IntoPrimitive)]
pub enum DataType {
    UINT8 = 0x10,
    INT8 = 0x11,
    UINT16 = 0x20,
    INT16 = 0x21,
    UINT24 = 0x30,
    INT24 = 0x31,
    UINT32 = 0x40,
    INT32 = 0x41,
    UINT64 = 0x80,
    INT64 = 0x81,
    FLOAT32 = 0x42,
    FLOAT64 = 0x82,
    #[num_enum(catch_all)]
    Unknown(u8),
}

#[derive(Debug, Clone)]
pub struct LegacyTimebaseInfoPayload {
    pub id: u16,
    pub source: TimebaseSource,
    pub epoch: TimebaseEpoch,
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

#[derive(Debug, Clone)]
pub struct StreamDataPayload {
    pub stream_id: u8,
    pub first_sample_n: u32,
    pub metadata_id: u8,
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
    StreamData(StreamDataPayload),
    Unknown(GenericPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceRoute {
    route: Vec<u8>,
}

impl DeviceRoute {
    pub fn root() -> DeviceRoute {
        DeviceRoute { route: vec![] }
    }

    fn from_bytes(bytes: &[u8]) -> Result<DeviceRoute, ()> {
        if bytes.len() > TIO_PACKET_MAX_ROUTING_SIZE {
            Err(())
        } else {
            let mut route = bytes.to_vec();
            route.reverse();
            Ok(DeviceRoute { route })
        }
    }

    pub fn from_str(route_str: &str) -> Result<DeviceRoute, ()> {
        let mut ret = DeviceRoute::root();
        let stripped = match route_str.strip_prefix("/") {
            Some(s) => s,
            None => route_str,
        };
        if stripped.len() > 0 {
            for segment in stripped.split('/') {
                if ret.route.len() >= TIO_PACKET_MAX_ROUTING_SIZE {
                    return Err(());
                }
                if let Ok(n) = segment.parse() {
                    ret.route.push(n);
                } else {
                    return Err(());
                }
            }
        }
        Ok(ret)
    }

    pub fn len(&self) -> usize {
        self.route.len()
    }

    pub fn iter(&self) -> std::slice::Iter<u8> {
        self.route.iter()
    }

    pub fn serialize(&self, mut rest_of_packet: Vec<u8>) -> Result<Vec<u8>, ()> {
        if (self.route.len() > TIO_PACKET_MAX_ROUTING_SIZE)
            || (rest_of_packet.len() < std::mem::size_of::<TioPktHdr>())
        {
            Err(())
        } else {
            rest_of_packet[1] |= self.route.len() as u8;
            for hop in self.route.iter().rev() {
                rest_of_packet.push(*hop);
            }
            Ok(rest_of_packet)
        }
    }

    // Returns the relative route from this to other_route (which is absolute).
    // Error if other route is not in the subtree rooted by this route.
    pub fn relative_route(&self, other_route: &DeviceRoute) -> Result<DeviceRoute, ()> {
        if (self.len() <= other_route.len()) && (self.route == other_route.route[0..self.len()]) {
            Ok(DeviceRoute {
                route: other_route.route[self.len()..].to_vec(),
            })
        } else {
            Err(())
        }
    }

    pub fn absolute_route(&self, other_route: &DeviceRoute) -> DeviceRoute {
        let mut route = self.route.clone();
        route.extend_from_slice(&other_route.route);
        DeviceRoute { route }
    }
}

use std::fmt::{Display, Formatter};

impl Display for DeviceRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.route.len() == 0 {
            write!(f, "/")?;
        } else {
            for segment in &self.route {
                write!(f, "/{}", segment)?;
            }
        }
        Ok(())
    }
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
    Reserved2 = 13,
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

static TIO_PACKET_HEADER_SIZE: usize = 4;
static TIO_PACKET_MAX_ROUTING_SIZE: usize = 8;
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
            TioPktType::Invalid
            | TioPktType::Reserved0
            | TioPktType::Reserved1
            | TioPktType::Reserved2 => false,
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

impl RpcRequestPayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<RpcRequestPayload, Error> {
        if raw.len() < 4 {
            return Err(too_small(full_data));
        }
        let id = u16::from_le_bytes([raw[0], raw[1]]);
        let method = u16::from_le_bytes([raw[2], raw[3]]);
        let (method, arg_start) = if (method & 0x8000) != 0 {
            let arg_start = (method & 0x7FFF) as usize + 4;
            if arg_start > TIO_PACKET_MAX_PAYLOAD_SIZE {
                return Err(Error::InvalidPayload(full_data.to_vec()));
            }
            if raw.len() < arg_start {
                return Err(too_small(full_data));
            }
            (
                RpcMethod::Name(String::from_utf8_lossy(&raw[4..arg_start]).to_string()),
                arg_start,
            )
        } else {
            (RpcMethod::Id(method), 4)
        };
        Ok(RpcRequestPayload {
            id: id,
            method: method,
            arg: raw[arg_start..].to_vec(),
        })
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        let method_name_len = if let RpcMethod::Name(method_name) = &self.method {
            method_name.as_bytes().len() as u16
        } else {
            0
        };
        let payload_size = 4 + (method_name_len as usize) + self.arg.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::RpcReq, 0, payload_size as u16);
        ret.extend(self.id.to_le_bytes());
        match &self.method {
            RpcMethod::Id(method) => {
                ret.extend(method.to_le_bytes());
            }
            RpcMethod::Name(method) => {
                ret.extend((method_name_len | 0x8000).to_le_bytes());
                ret.extend(method.as_bytes())
            }
        }
        ret.extend_from_slice(&self.arg);
        Ok(ret)
    }
}

impl RpcReplyPayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<RpcReplyPayload, Error> {
        if raw.len() < 2 {
            return Err(too_small(full_data));
        }
        let id = u16::from_le_bytes([raw[0], raw[1]]);
        Ok(RpcReplyPayload {
            id: id,
            reply: raw[2..].to_vec(),
        })
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        let payload_size = 2 + self.reply.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::RpcRep, 0, payload_size as u16);
        ret.extend(self.id.to_le_bytes());
        ret.extend_from_slice(&self.reply);
        Ok(ret)
    }
}

impl RpcErrorPayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<RpcErrorPayload, Error> {
        if raw.len() < 4 {
            return Err(too_small(full_data));
        }
        Ok(RpcErrorPayload {
            id: u16::from_le_bytes([raw[0], raw[1]]),
            error: RpcErrorCode::from(u16::from_le_bytes([raw[2], raw[3]])),
            extra: raw[4..].to_vec(),
        })
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
        let payload_size = 4 + self.extra.len();
        if payload_size > TIO_PACKET_MAX_PAYLOAD_SIZE {
            return Err(());
        }
        let mut ret = TioPktHdr::serialize_new(TioPktType::RpcError, 0, payload_size as u16);
        ret.extend(self.id.to_le_bytes());
        ret.extend(u16::from(self.error).to_le_bytes());
        ret.extend_from_slice(&self.extra);
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

impl LegacyStreamDataPayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<LegacyStreamDataPayload, Error> {
        if raw.len() < 5 {
            return Err(too_small(full_data));
        }
        Ok(LegacyStreamDataPayload {
            sample_n: u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]),
            data: raw[4..].to_vec(),
        })
    }
    fn serialize(&self) -> Result<Vec<u8>, ()> {
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

impl StreamDataPayload {
    fn deserialize(raw: &[u8], full_data: &[u8]) -> Result<StreamDataPayload, Error> {
        if raw.len() < 5 {
            return Err(too_small(full_data));
        }
        Ok(StreamDataPayload {
            stream_id: full_data[0] - TIO_PTYPE_STREAM0,
            first_sample_n: u32::from_le_bytes([raw[0], raw[1], raw[2], 0u8]),
            metadata_id: raw[3],
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
        ret.extend([
            sample_ser[0],
            sample_ser[1],
            sample_ser[2],
            self.metadata_id,
        ]);
        ret.extend(&self.data);
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
            Payload::LegacyStreamData(p) => p.serialize(),
            Payload::StreamData(p) => p.serialize(),
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

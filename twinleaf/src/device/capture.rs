use std::time::{Duration, Instant};

use crate::device::RpcClient;
use crate::tio;
use crate::tio::proto::DataType;
use crate::tio::proxy;

pub const CAPTURE_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const CAPTURE_TRIGGER_INDEX: i16 = -1;
pub const CAPTURE_STATUS_INDEX: i16 = -2;
pub const CAPTURE_METADATA_INDEX: i16 = -3;
pub const CAPTURE_METADATA_VERSION: u8 = 1;
pub const CAPTURE_METADATA_FIXED_LEN: usize = 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureStatus {
    Idle,
    Capturing,
    Done,
    Error,
    Unknown(u8),
}

impl CaptureStatus {
    pub fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Idle,
            1 => Self::Capturing,
            2 => Self::Done,
            4 => Self::Error,
            value => Self::Unknown(value),
        }
    }

    pub fn as_raw(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Capturing => 1,
            Self::Done => 2,
            Self::Error => 4,
            Self::Unknown(value) => value,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CaptureMetadata {
    pub size: u32,
    pub blocksize: u16,
    pub data_type: DataType,
    pub length: u32,
    pub y_calibration: f32,
    pub x_offset: f32,
    pub x_stride: f32,
    pub name: String,
    pub units: String,
    pub x_name: String,
    pub x_units: String,
}

impl CaptureMetadata {
    pub fn data_type_label(&self) -> String {
        self.data_type.type_name()
    }

    pub fn x_value_f64(&self, index: usize) -> f64 {
        f64::from(self.x_offset) + index as f64 * f64::from(self.x_stride)
    }

    pub fn x_values_f64(&self) -> Vec<f64> {
        (0..self.length as usize)
            .map(|index| self.x_value_f64(index))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CaptureReadout {
    pub metadata: CaptureMetadata,
    pub data: Vec<u8>,
}

impl CaptureReadout {
    pub fn values_f64(&self) -> Result<Vec<f64>, CaptureError> {
        decode_capture_values(&self.data, &self.metadata)
    }

    pub fn x_values_f64(&self) -> Vec<f64> {
        self.metadata.x_values_f64()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("capture RPC failed: {0}")]
    Rpc(#[from] proxy::RpcError),
    #[error("capture status reply should be 1 byte, got {actual}")]
    InvalidStatusLength { actual: usize },
    #[error("capture status reported an error")]
    DeviceError,
    #[error("capture status is idle; capture never started")]
    CaptureNotStarted,
    #[error("capture status reported unknown value {0}")]
    UnknownStatus(u8),
    #[error("timed out waiting for capture data; last status was {last_status:?}")]
    Timeout { last_status: CaptureStatus },
    #[error("timed out waiting for capture block {index}")]
    BlockTimeout { index: i16 },
    #[error("capture metadata reply is empty")]
    EmptyMetadata,
    #[error("capture metadata fixed_len {fixed_len} is too small")]
    MetadataFixedLenTooSmall { fixed_len: usize },
    #[error("capture metadata reply is shorter than fixed_len: {actual} < {fixed_len}")]
    MetadataTooShort { actual: usize, fixed_len: usize },
    #[error("unsupported capture metadata version {version}; expected {expected}")]
    UnsupportedMetadataVersion { version: u8, expected: u8 },
    #[error("capture metadata varlen string asks for {requested} bytes, only {remaining} remain")]
    MetadataStringTooShort { requested: usize, remaining: usize },
    #[error("capture metadata contains non-UTF8 text: {0}")]
    MetadataText(#[from] std::str::Utf8Error),
    #[error("capture metadata has {0} trailing varlen bytes")]
    MetadataTrailingBytes(usize),
    #[error("capture metadata reported blocksize 0")]
    ZeroBlocksize,
    #[error("capture requires too many blocks for i16 block indices: {blocks}")]
    TooManyBlocks { blocks: usize },
    #[error("capture metadata has zero-sized data type")]
    ZeroSizeDataType,
    #[error("capture data is too short for metadata length: {actual} < {required}")]
    DataTooShort { actual: usize, required: usize },
    #[error("unsupported capture data type 0x{0:02x}")]
    UnsupportedDataType(u8),
}

pub trait CaptureRpc {
    fn capture_raw_rpc(&self, name: &str, arg: &[u8]) -> Result<Vec<u8>, proxy::RpcError>;
}

impl CaptureRpc for RpcClient {
    fn capture_raw_rpc(&self, name: &str, arg: &[u8]) -> Result<Vec<u8>, proxy::RpcError> {
        self.raw_rpc(self.root_route(), name, arg)
    }
}

pub fn trigger_capture<R: CaptureRpc + ?Sized>(
    rpc: &R,
    rpc_name: &str,
) -> Result<Vec<u8>, CaptureError> {
    capture_rpc_i16(rpc, rpc_name, CAPTURE_TRIGGER_INDEX).map_err(Into::into)
}

pub fn read_capture_status<R: CaptureRpc + ?Sized>(
    rpc: &R,
    rpc_name: &str,
) -> Result<CaptureStatus, CaptureError> {
    let reply = capture_rpc_i16(rpc, rpc_name, CAPTURE_STATUS_INDEX)?;
    if reply.len() != 1 {
        return Err(CaptureError::InvalidStatusLength {
            actual: reply.len(),
        });
    }
    Ok(CaptureStatus::from_raw(reply[0]))
}

pub fn read_capture_metadata<R: CaptureRpc + ?Sized>(
    rpc: &R,
    rpc_name: &str,
) -> Result<CaptureMetadata, CaptureError> {
    let reply = capture_rpc_i16(rpc, rpc_name, CAPTURE_METADATA_INDEX)?;
    parse_capture_metadata(&reply)
}

pub fn read_capture<R: CaptureRpc + ?Sized>(
    rpc: &R,
    rpc_name: &str,
    timeout: Duration,
) -> Result<CaptureReadout, CaptureError> {
    trigger_capture(rpc, rpc_name)?;
    wait_capture_done(rpc, rpc_name, timeout)?;

    let metadata = read_capture_metadata(rpc, rpc_name)?;
    if metadata.blocksize == 0 {
        return Err(CaptureError::ZeroBlocksize);
    }

    let capture_size = metadata.size as usize;
    let blocks = capture_size.div_ceil(usize::from(metadata.blocksize));
    if blocks > i16::MAX as usize + 1 {
        return Err(CaptureError::TooManyBlocks { blocks });
    }

    let mut data = Vec::with_capacity(capture_size);
    for index in 0..blocks {
        let block = read_capture_block(rpc, rpc_name, index as i16, timeout)?;
        data.extend(block);
    }
    data.truncate(capture_size);

    Ok(CaptureReadout { metadata, data })
}

pub fn wait_capture_done<R: CaptureRpc + ?Sized>(
    rpc: &R,
    rpc_name: &str,
    timeout: Duration,
) -> Result<(), CaptureError> {
    let started = Instant::now();
    loop {
        let status = read_capture_status(rpc, rpc_name)?;
        match status {
            CaptureStatus::Done => return Ok(()),
            CaptureStatus::Idle => return Err(CaptureError::CaptureNotStarted),
            CaptureStatus::Error => return Err(CaptureError::DeviceError),
            CaptureStatus::Unknown(value) => return Err(CaptureError::UnknownStatus(value)),
            CaptureStatus::Capturing => {
                if started.elapsed() >= timeout {
                    return Err(CaptureError::Timeout {
                        last_status: status,
                    });
                }
                std::thread::sleep(CAPTURE_POLL_INTERVAL);
            }
        }
    }
}

pub fn read_capture_block<R: CaptureRpc + ?Sized>(
    rpc: &R,
    rpc_name: &str,
    index: i16,
    timeout: Duration,
) -> Result<Vec<u8>, CaptureError> {
    let started = Instant::now();
    loop {
        match capture_rpc_i16(rpc, rpc_name, index) {
            Ok(reply) => return Ok(reply),
            Err(proxy::RpcError::ExecError(err))
                if matches!(err.error, tio::proto::RpcErrorCode::Busy)
                    && started.elapsed() < timeout =>
            {
                std::thread::sleep(CAPTURE_POLL_INTERVAL);
            }
            Err(proxy::RpcError::ExecError(err))
                if matches!(err.error, tio::proto::RpcErrorCode::Busy) =>
            {
                return Err(CaptureError::BlockTimeout { index });
            }
            Err(err) => return Err(CaptureError::Rpc(err)),
        }
    }
}

fn capture_rpc_i16<R: CaptureRpc + ?Sized>(
    rpc: &R,
    rpc_name: &str,
    arg: i16,
) -> Result<Vec<u8>, proxy::RpcError> {
    rpc.capture_raw_rpc(rpc_name, &arg.to_le_bytes())
}

pub fn parse_capture_metadata(raw: &[u8]) -> Result<CaptureMetadata, CaptureError> {
    if raw.is_empty() {
        return Err(CaptureError::EmptyMetadata);
    }
    let fixed_len = raw[0] as usize;
    if fixed_len < CAPTURE_METADATA_FIXED_LEN {
        return Err(CaptureError::MetadataFixedLenTooSmall { fixed_len });
    }
    if raw.len() < fixed_len {
        return Err(CaptureError::MetadataTooShort {
            actual: raw.len(),
            fixed_len,
        });
    }

    let fixed = &raw[..fixed_len];
    if fixed[1] != CAPTURE_METADATA_VERSION {
        return Err(CaptureError::UnsupportedMetadataVersion {
            version: fixed[1],
            expected: CAPTURE_METADATA_VERSION,
        });
    }

    let mut varlen = &raw[fixed_len..];
    let name = peel_capture_string(&mut varlen, fixed[26] as usize)?;
    let units = peel_capture_string(&mut varlen, fixed[27] as usize)?;
    let x_name = peel_capture_string(&mut varlen, fixed[28] as usize)?;
    let x_units = peel_capture_string(&mut varlen, fixed[29] as usize)?;
    if !varlen.is_empty() {
        return Err(CaptureError::MetadataTrailingBytes(varlen.len()));
    }

    Ok(CaptureMetadata {
        size: u32::from_le_bytes(fixed[4..8].try_into().unwrap()),
        blocksize: u16::from_le_bytes(fixed[8..10].try_into().unwrap()),
        data_type: DataType::from(fixed[2]),
        length: u32::from_le_bytes(fixed[10..14].try_into().unwrap()),
        y_calibration: f32::from_le_bytes(fixed[14..18].try_into().unwrap()),
        x_offset: f32::from_le_bytes(fixed[18..22].try_into().unwrap()),
        x_stride: f32::from_le_bytes(fixed[22..26].try_into().unwrap()),
        name,
        units,
        x_name,
        x_units,
    })
}

fn peel_capture_string(raw: &mut &[u8], len: usize) -> Result<String, CaptureError> {
    if raw.len() < len {
        return Err(CaptureError::MetadataStringTooShort {
            requested: len,
            remaining: raw.len(),
        });
    }
    let (head, rest) = raw.split_at(len);
    *raw = rest;
    Ok(std::str::from_utf8(head)?.to_string())
}

pub fn decode_capture_values(raw: &[u8], meta: &CaptureMetadata) -> Result<Vec<f64>, CaptureError> {
    let entry_size = meta.data_type.size();
    if entry_size == 0 {
        return Err(CaptureError::ZeroSizeDataType);
    }
    let required = meta.length as usize * entry_size;
    if raw.len() < required {
        return Err(CaptureError::DataTooShort {
            actual: raw.len(),
            required,
        });
    }

    let scale = f64::from(meta.y_calibration);
    raw[..required]
        .chunks_exact(entry_size)
        .map(|chunk| {
            let value = match meta.data_type {
                DataType::UInt8 => f64::from(chunk[0]),
                DataType::Int8 => f64::from(chunk[0] as i8),
                DataType::UInt16 => f64::from(u16::from_le_bytes(chunk.try_into().unwrap())),
                DataType::Int16 => f64::from(i16::from_le_bytes(chunk.try_into().unwrap())),
                DataType::UInt24 => f64::from(read_u24(chunk)),
                DataType::Int24 => f64::from(read_i24(chunk)),
                DataType::UInt32 => f64::from(u32::from_le_bytes(chunk.try_into().unwrap())),
                DataType::Int32 => f64::from(i32::from_le_bytes(chunk.try_into().unwrap())),
                DataType::UInt64 => u64::from_le_bytes(chunk.try_into().unwrap()) as f64,
                DataType::Int64 => i64::from_le_bytes(chunk.try_into().unwrap()) as f64,
                DataType::Float32 => f64::from(f32::from_le_bytes(chunk.try_into().unwrap())),
                DataType::Float64 => f64::from_le_bytes(chunk.try_into().unwrap()),
                DataType::Unknown(value) => return Err(CaptureError::UnsupportedDataType(value)),
            };
            Ok(value * scale)
        })
        .collect()
}

fn read_u24(raw: &[u8]) -> u32 {
    u32::from(raw[0]) | (u32::from(raw[1]) << 8) | (u32::from(raw[2]) << 16)
}

fn read_i24(raw: &[u8]) -> i32 {
    let value = read_u24(raw) as i32;
    if value & 0x0080_0000 != 0 {
        value | !0x00ff_ffff
    } else {
        value
    }
}

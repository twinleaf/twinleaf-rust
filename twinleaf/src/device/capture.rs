//! One-shot captures read through a device's array RPC.
//!
//! A capturing RPC takes an `i16` selector. `-1` triggers a capture, `-2`
//! reads its [`CaptureStatus`], `-3` reads its [`CaptureMetadata`], and `0`
//! and up read the data one block at a time. [`read_capture`] runs the whole
//! sequence.

use super::connection::Device;
use super::rpc::CallError;
use crate::data::ColumnData;
use crate::proto::capture as wire;
use crate::proto::data::DataType;
use crate::proto::rpc::RpcError;
use std::time::{Duration, Instant};

const CAPTURE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CAPTURE_TRIGGER_INDEX: i16 = -1;
const CAPTURE_STATUS_INDEX: i16 = -2;
const CAPTURE_METADATA_INDEX: i16 = -3;

/// Where a capture stands, as its status selector reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureStatus {
    /// Nothing has been triggered.
    Idle,
    /// A capture is in progress.
    Capturing,
    /// A capture finished and its blocks can be read.
    Done,
    /// The device failed the capture.
    Error,
    /// A status byte this build does not know.
    Unknown(u8),
}

impl CaptureStatus {
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Idle,
            1 => Self::Capturing,
            2 => Self::Done,
            4 => Self::Error,
            value => Self::Unknown(value),
        }
    }
}

/// What one capture holds and how to read it.
#[derive(Clone, Debug, PartialEq)]
pub struct CaptureMetadata {
    /// Bytes of capture data the block selectors serve.
    pub size: u32,
    /// Bytes one block selector returns.
    pub blocksize: u16,
    /// Element type of the data.
    pub data_type: DataType,
    /// Elements captured.
    pub length: u32,
    /// Multiplier taking a raw element to its value in `units`.
    pub y_calibration: f32,
    /// x of element zero, in `x_units`.
    pub x_offset: f32,
    /// x step between consecutive elements.
    pub x_stride: f32,
    /// What was captured.
    pub name: String,
    /// Units of a calibrated element.
    pub units: String,
    /// What x measures.
    pub x_name: String,
    /// Units of `x_offset` and `x_stride`.
    pub x_units: String,
}

impl CaptureMetadata {
    /// Creates owned metadata from a parsed reply.
    fn from_wire(meta: wire::CaptureMetadata<'_>) -> Self {
        Self {
            size: meta.data_size,
            blocksize: meta.block_size,
            data_type: meta.data_type,
            length: meta.length,
            y_calibration: meta.y_calibration,
            x_offset: meta.x_offset,
            x_stride: meta.x_stride,
            name: meta.name.to_string(),
            units: meta.units.to_string(),
            x_name: meta.x_name.to_string(),
            x_units: meta.x_units.to_string(),
        }
    }

    /// Returns the element type's name.
    pub fn data_type_label(&self) -> String {
        self.data_type.to_string()
    }

    /// Returns the x value of element `index`, in `x_units`.
    pub fn x_value_f64(&self, index: usize) -> f64 {
        f64::from(self.x_offset) + index as f64 * f64::from(self.x_stride)
    }
}

/// A finished capture, with its metadata and every block concatenated and
/// cut to `size`.
#[derive(Clone, Debug, PartialEq)]
pub struct CaptureReadout {
    /// How to read `data`.
    pub metadata: CaptureMetadata,
    /// The elements, little-endian.
    pub data: Vec<u8>,
}

impl CaptureReadout {
    /// Decodes every element as a calibrated `f64`.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureError::DataTooShort`] if `data` holds fewer than
    /// `length` elements, [`CaptureError::ZeroSizeDataType`] for an element
    /// type with no size, and [`CaptureError::UnsupportedDataType`] for one
    /// that does not decode as a number.
    pub fn values_f64(&self) -> Result<Vec<f64>, CaptureError> {
        decode_capture_values(&self.data, &self.metadata)
    }
}

/// Why a capture could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CaptureError {
    /// A capture RPC failed.
    #[error("capture RPC failed: {0}")]
    Rpc(#[from] CallError),
    /// The status reply was not one byte.
    #[error("capture status reply should be 1 byte, got {actual}")]
    InvalidStatusLength {
        /// Bytes received.
        actual: usize,
    },
    /// The device reported the capture failed.
    #[error("capture status reported an error")]
    DeviceError,
    /// The device reported idle after being triggered.
    #[error("capture status is idle; capture never started")]
    CaptureNotStarted,
    /// The status byte is not one this build knows.
    #[error("capture status reported unknown value {0}")]
    UnknownStatus(u8),
    /// The capture did not finish within the timeout.
    #[error("timed out waiting for capture data; last status was {last_status:?}")]
    Timeout {
        /// The status seen last.
        last_status: CaptureStatus,
    },
    /// A block stayed busy past the timeout.
    #[error("timed out waiting for capture block {index}")]
    BlockTimeout {
        /// The block that stayed busy.
        index: i16,
    },
    /// The metadata reply did not parse. Carries its length.
    #[error("capture metadata reply is not a valid metadata record ({0} bytes)")]
    InvalidMetadata(usize),
    /// The metadata layout is one this build does not read.
    #[error("unsupported capture metadata version {version}; expected {expected}")]
    UnsupportedMetadataVersion {
        /// The layout the device sent.
        version: u8,
        /// The layout this build reads.
        expected: u8,
    },
    /// The metadata declares blocks of zero bytes.
    #[error("capture metadata reported blocksize 0")]
    ZeroBlocksize,
    /// More blocks than an `i16` selector can address.
    #[error("capture requires too many blocks for i16 block indices: {blocks}")]
    TooManyBlocks {
        /// Blocks the capture needs.
        blocks: usize,
    },
    /// The element type has no size.
    #[error("capture metadata has zero-sized data type")]
    ZeroSizeDataType,
    /// Fewer bytes than `length` elements need.
    #[error("capture data is too short for metadata length: {actual} < {required}")]
    DataTooShort {
        /// Bytes read.
        actual: usize,
        /// Bytes `length` elements take.
        required: usize,
    },
    /// An element type this build cannot decode.
    #[error("unsupported capture data type 0x{0:02x}")]
    UnsupportedDataType(u8),
}

fn trigger_capture(device: &Device, rpc_name: &str) -> Result<Vec<u8>, CaptureError> {
    capture_rpc_i16(device, rpc_name, CAPTURE_TRIGGER_INDEX).map_err(Into::into)
}

fn read_capture_status(device: &Device, rpc_name: &str) -> Result<CaptureStatus, CaptureError> {
    let reply = capture_rpc_i16(device, rpc_name, CAPTURE_STATUS_INDEX)?;
    if reply.len() != 1 {
        return Err(CaptureError::InvalidStatusLength {
            actual: reply.len(),
        });
    }
    Ok(CaptureStatus::from_raw(reply[0]))
}

fn read_capture_metadata(device: &Device, rpc_name: &str) -> Result<CaptureMetadata, CaptureError> {
    let reply = capture_rpc_i16(device, rpc_name, CAPTURE_METADATA_INDEX)?;
    parse_capture_metadata(&reply)
}

/// Triggers a capture on `rpc_name`, waits up to `timeout` for it to finish,
/// and reads every block. Blocks until done.
///
/// Polls every 100 ms. A block the device reports busy is retried for up to
/// `timeout`.
///
/// # Errors
///
/// Returns [`CaptureError::Rpc`] if a call fails. Returns
/// [`CaptureError::CaptureNotStarted`], [`CaptureError::DeviceError`], or
/// [`CaptureError::UnknownStatus`] if the status is not a capture in progress
/// or done, and [`CaptureError::Timeout`] or [`CaptureError::BlockTimeout`] if
/// the capture or a block takes longer than `timeout`. Returns
/// [`CaptureError::InvalidStatusLength`], [`CaptureError::InvalidMetadata`],
/// [`CaptureError::UnsupportedMetadataVersion`],
/// [`CaptureError::ZeroBlocksize`], or [`CaptureError::TooManyBlocks`] if a
/// reply is malformed or describes a capture this build cannot read.
pub fn read_capture(
    device: &Device,
    rpc_name: &str,
    timeout: Duration,
) -> Result<CaptureReadout, CaptureError> {
    trigger_capture(device, rpc_name)?;
    wait_capture_done(device, rpc_name, timeout)?;

    let metadata = read_capture_metadata(device, rpc_name)?;
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
        let block = read_capture_block(device, rpc_name, index as i16, timeout)?;
        data.extend(block);
    }
    data.truncate(capture_size);

    Ok(CaptureReadout { metadata, data })
}

fn wait_capture_done(
    device: &Device,
    rpc_name: &str,
    timeout: Duration,
) -> Result<(), CaptureError> {
    let started = Instant::now();
    loop {
        let status = read_capture_status(device, rpc_name)?;
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

fn read_capture_block(
    device: &Device,
    rpc_name: &str,
    index: i16,
    timeout: Duration,
) -> Result<Vec<u8>, CaptureError> {
    let started = Instant::now();
    loop {
        match capture_rpc_i16(device, rpc_name, index) {
            Ok(reply) => return Ok(reply),
            Err(CallError::DeviceError(err))
                if matches!(err.error, RpcError::Busy) && started.elapsed() < timeout =>
            {
                std::thread::sleep(CAPTURE_POLL_INTERVAL);
            }
            Err(CallError::DeviceError(err)) if matches!(err.error, RpcError::Busy) => {
                return Err(CaptureError::BlockTimeout { index });
            }
            Err(err) => return Err(CaptureError::Rpc(err)),
        }
    }
}

fn capture_rpc_i16(device: &Device, rpc_name: &str, arg: i16) -> Result<Vec<u8>, CallError> {
    device.raw_rpc(rpc_name, &arg.to_le_bytes())
}

fn parse_capture_metadata(raw: &[u8]) -> Result<CaptureMetadata, CaptureError> {
    let meta = wire::CaptureMetadata::parse(raw).ok_or(CaptureError::InvalidMetadata(raw.len()))?;
    if meta.version != wire::METADATA_VERSION {
        return Err(CaptureError::UnsupportedMetadataVersion {
            version: meta.version,
            expected: wire::METADATA_VERSION,
        });
    }
    Ok(CaptureMetadata::from_wire(meta))
}

fn decode_capture_values(raw: &[u8], meta: &CaptureMetadata) -> Result<Vec<f64>, CaptureError> {
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
            ColumnData::from_le_bytes(chunk, meta.data_type)
                .try_as_f64()
                .map(|value| value * scale)
                .ok_or(CaptureError::UnsupportedDataType(meta.data_type.value()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(data_type: DataType, length: u32, y_calibration: f32) -> CaptureMetadata {
        CaptureMetadata {
            size: length * data_type.size() as u32,
            blocksize: 64,
            data_type,
            length,
            y_calibration,
            x_offset: 0.0,
            x_stride: 1.0,
            name: String::new(),
            units: String::new(),
            x_name: String::new(),
            x_units: String::new(),
        }
    }

    /// Each element decodes as a sample column of its type does, scaled by
    /// the calibration, and the data past `length` is ignored.
    #[test]
    fn capture_values_decode_as_sample_columns_do() {
        let raw = [0xff, 0xff, 0xff, 0x01, 0x00, 0x80, 0x99];
        let values = decode_capture_values(&raw, &metadata(DataType::I24, 2, 0.5)).unwrap();
        assert_eq!(values, [-0.5, -4194303.5]);

        let values = decode_capture_values(&raw, &metadata(DataType::U8, 3, 2.0)).unwrap();
        assert_eq!(values, [510.0, 510.0, 510.0]);
    }
}

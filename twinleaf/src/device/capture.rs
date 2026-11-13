//! One-shot captures read through a device's array RPC.
//!
//! A capturing RPC takes an `i16` selector. `-1` triggers a capture, `-2`
//! reads its [`CaptureStatus`], `-3` reads its [`CaptureMetadata`], and `0`
//! and up read the data one block at a time. A [`Capture`] takes each of
//! those steps on its own and waits for none, for a caller that shows how
//! far a capture is or stops it part way. [`read_capture`] runs the whole
//! sequence and blocks until it is done.

use super::connection::Device;
use crate::data::ColumnData;
use crate::proto::capture as wire;
use crate::proto::data::DataType;
use crate::proto::rpc::RpcError;
use crate::session::rpc::{CallError, PendingReply, ReplyFuture};
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::task::{Context, Poll};
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

    /// Returns how many blocks hold the data, block zero the first.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureError::ZeroBlocksize`] for blocks of no bytes, and
    /// [`CaptureError::TooManyBlocks`] for more blocks than the 32768 an
    /// `i16` selector reaches.
    pub fn block_count(&self) -> Result<u16, CaptureError> {
        if self.blocksize == 0 {
            return Err(CaptureError::ZeroBlocksize);
        }
        let blocks = (self.size as usize).div_ceil(usize::from(self.blocksize));
        match blocks <= i16::MAX as usize + 1 {
            true => Ok(blocks as u16),
            false => Err(CaptureError::TooManyBlocks { blocks }),
        }
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
    /// Makes a readout of `data`, a capture's blocks one after another from
    /// block zero, cut to the `size` its `metadata` gives.
    pub fn new(metadata: CaptureMetadata, mut data: Vec<u8>) -> CaptureReadout {
        data.truncate(metadata.size as usize);
        CaptureReadout { metadata, data }
    }

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

impl CaptureError {
    /// Returns whether the device refused a step because a capture is in
    /// progress on the RPC.
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            CaptureError::Rpc(CallError::DeviceError(refused))
                if matches!(refused.error, RpcError::Busy)
        )
    }
}

/// One capture RPC of a device, whose steps are taken one at a time.
///
/// Each step submits one call and returns its [`PendingCapture`], to wait on
/// or `.await`, so a caller can say how far a capture is, give it up between
/// two steps, and hold no thread while the device works. A capture runs as
/// [`read_capture`] runs it. Trigger it, ask its status until it is
/// [`CaptureStatus::Done`], read its metadata, read
/// [`CaptureMetadata::block_count`] blocks from block zero, and make a
/// [`CaptureReadout`] of them. A caller that stops between two steps leaves
/// the device's capture as it stands, and the next trigger starts another.
///
/// # Examples
///
/// ```no_run
/// use twinleaf::device::capture::{Capture, CaptureReadout, CaptureStatus};
/// use twinleaf::{Connection, DeviceRoute};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let connection = Connection::connect()?;
/// let capture = Capture::new(&connection.device(DeviceRoute::root()), "test.capture");
/// capture.trigger().await?;
/// while capture.status().await? == CaptureStatus::Capturing {
///     tokio::time::sleep(std::time::Duration::from_millis(20)).await;
/// }
/// let metadata = capture.metadata().await?;
/// let mut data = Vec::new();
/// for index in 0..metadata.block_count()? {
///     data.extend(capture.block(index).await?);
///     println!("{} of {} bytes", data.len(), metadata.size);
/// }
/// let readout = CaptureReadout::new(metadata, data);
/// println!("{:?}", readout.values_f64()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Capture {
    device: Device,
    rpc_name: String,
}

impl Capture {
    /// Names the capture RPC `rpc_name` of `device`. Nothing is sent.
    pub fn new(device: &Device, rpc_name: &str) -> Capture {
        Capture {
            device: device.clone(),
            rpc_name: rpc_name.to_string(),
        }
    }

    /// Returns the name of the RPC.
    pub fn rpc_name(&self) -> &str {
        &self.rpc_name
    }

    /// Starts a capture.
    ///
    /// # Errors
    ///
    /// Resolves to [`CaptureError::Rpc`] if the call fails. A device that is
    /// capturing already refuses, which [`CaptureError::is_busy`] tells.
    pub fn trigger(&self) -> PendingCapture<()> {
        self.select(CAPTURE_TRIGGER_INDEX, |_| Ok(()))
    }

    /// Asks where the capture stands.
    ///
    /// # Errors
    ///
    /// Resolves to [`CaptureError::Rpc`] if the call fails, and to
    /// [`CaptureError::InvalidStatusLength`] if the reply is not one byte.
    pub fn status(&self) -> PendingCapture<CaptureStatus> {
        self.select(CAPTURE_STATUS_INDEX, |reply| match reply[..] {
            [status] => Ok(CaptureStatus::from_raw(status)),
            _ => Err(CaptureError::InvalidStatusLength {
                actual: reply.len(),
            }),
        })
    }

    /// Reads what the capture holds and how to read it.
    ///
    /// # Errors
    ///
    /// Resolves to [`CaptureError::Rpc`] if the call fails, and to
    /// [`CaptureError::InvalidMetadata`] or
    /// [`CaptureError::UnsupportedMetadataVersion`] if the reply is not
    /// metadata this build reads.
    pub fn metadata(&self) -> PendingCapture<CaptureMetadata> {
        self.select(CAPTURE_METADATA_INDEX, |reply| {
            parse_capture_metadata(&reply)
        })
    }

    /// Reads block `index` of the data, counted from zero.
    ///
    /// # Errors
    ///
    /// Resolves to [`CaptureError::Rpc`] if the call fails. A device that is
    /// still capturing refuses, which [`CaptureError::is_busy`] tells. A
    /// block past the 32768 an `i16` selector reaches resolves to
    /// [`CaptureError::TooManyBlocks`], and nothing is sent for it.
    pub fn block(&self, index: u16) -> PendingCapture<Vec<u8>> {
        match i16::try_from(index) {
            Ok(selector) => self.select(selector, Ok),
            Err(_) => PendingCapture {
                reply: Err(CaptureError::TooManyBlocks {
                    blocks: usize::from(index) + 1,
                }),
                parse: Ok,
            },
        }
    }

    /// Submits the call with `selector`, whose reply `parse` reads.
    fn select<T>(
        &self,
        selector: i16,
        parse: fn(Vec<u8>) -> Result<T, CaptureError>,
    ) -> PendingCapture<T> {
        PendingCapture {
            reply: Ok(self.device.submit(&self.rpc_name, &selector.to_le_bytes())),
            parse,
        }
    }
}

/// A step of a [`Capture`] whose reply has not arrived yet.
///
/// It resolves once, with what the step read or why it could not. Block on
/// it with [`wait`](Self::wait) or `.await` it. Dropping it abandons the
/// reply, and the step still goes ahead.
#[must_use = "the step is in flight; wait on its reply"]
#[derive(Debug)]
pub struct PendingCapture<T> {
    /// The call's reply, or why no call was sent.
    reply: Result<PendingReply, CaptureError>,
    parse: fn(Vec<u8>) -> Result<T, CaptureError>,
}

impl<T> PendingCapture<T> {
    /// Blocks until the step resolves and returns what it read.
    ///
    /// # Errors
    ///
    /// Returns the step's [`CaptureError`], as the method that made it says.
    pub fn wait(self) -> Result<T, CaptureError> {
        (self.parse)(self.reply?.wait()?)
    }
}

impl<T> IntoFuture for PendingCapture<T> {
    type Output = Result<T, CaptureError>;
    type IntoFuture = CaptureFuture<T>;

    fn into_future(self) -> CaptureFuture<T> {
        CaptureFuture {
            reply: self.reply.map(IntoFuture::into_future).map_err(Some),
            parse: self.parse,
        }
    }
}

/// A [`PendingCapture`] being awaited.
#[derive(Debug)]
pub struct CaptureFuture<T> {
    /// The reply being awaited, or why no call was sent, until it is told.
    reply: Result<ReplyFuture, Option<CaptureError>>,
    parse: fn(Vec<u8>) -> Result<T, CaptureError>,
}

impl<T> Future for CaptureFuture<T> {
    type Output = Result<T, CaptureError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        match &mut this.reply {
            Ok(reply) => Pin::new(reply).poll(cx).map(|reply| (this.parse)(reply?)),
            Err(unsent) => {
                let unsent = unsent.take().expect("a step is polled until it resolves");
                Poll::Ready(Err(unsent))
            }
        }
    }
}

/// Triggers a capture on `rpc_name`, waits up to `timeout` for it to finish,
/// and reads every block. Blocks until done. It takes the steps of a
/// [`Capture`] one after another.
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
    let capture = Capture::new(device, rpc_name);
    capture.trigger().wait()?;
    wait_capture_done(&capture, timeout)?;

    let metadata = capture.metadata().wait()?;
    let mut data = Vec::with_capacity(metadata.size as usize);
    for index in 0..metadata.block_count()? {
        data.extend(read_capture_block(&capture, index, timeout)?);
    }
    Ok(CaptureReadout::new(metadata, data))
}

fn wait_capture_done(capture: &Capture, timeout: Duration) -> Result<(), CaptureError> {
    let started = Instant::now();
    loop {
        let status = capture.status().wait()?;
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
    capture: &Capture,
    index: u16,
    timeout: Duration,
) -> Result<Vec<u8>, CaptureError> {
    let started = Instant::now();
    loop {
        match capture.block(index).wait() {
            Err(busy) if busy.is_busy() && started.elapsed() < timeout => {
                std::thread::sleep(CAPTURE_POLL_INTERVAL);
            }
            Err(busy) if busy.is_busy() => {
                return Err(CaptureError::BlockTimeout {
                    index: index as i16,
                });
            }
            answered => return answered,
        }
    }
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
    use crate::device::tests::fixtures::{open_through, proxy};
    use crate::proto::DeviceRoute;
    use crate::tio::tests::device::FakeDevice;

    /// The capture of a fake device, `fake.capture`, opened through a proxy
    /// of the test's own, with both.
    fn fake_capture() -> (FakeDevice, crate::proxy::Proxy, Capture) {
        let device = FakeDevice::start();
        let proxy = proxy();
        let connection = open_through(&proxy, &device.url).unwrap();
        let capture = Capture::new(&connection.device(DeviceRoute::root()), "fake.capture");
        (device, proxy, capture)
    }

    /// Each step reads what its selector serves, waited on or awaited, and
    /// the blocks in order are the capture, cut to its size.
    #[test]
    fn each_step_reads_what_its_selector_serves() {
        let (device, _proxy, capture) = fake_capture();
        assert_eq!(capture.rpc_name(), "fake.capture");

        capture.trigger().wait().unwrap();
        assert_eq!(capture.status().wait().unwrap(), CaptureStatus::Done);
        let metadata = capture.metadata().wait().unwrap();
        assert_eq!((metadata.name.as_str(), metadata.length), ("Fake", 5));
        assert_eq!(metadata.block_count().unwrap(), 2);
        let blocks = [capture.block(0), capture.block(1)].map(|block| block.wait().unwrap());
        assert_eq!(blocks, [vec![1, 2, 3, 4], vec![5, 0, 0, 0]]);
        let readout = CaptureReadout::new(metadata.clone(), blocks.concat());
        assert_eq!(readout.data, [1, 2, 3, 4, 5]);
        assert_eq!(readout.values_f64().unwrap(), [0.5, 1.0, 1.5, 2.0, 2.5]);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let awaited = runtime.block_on(async {
            capture.trigger().await.unwrap();
            assert_eq!(capture.status().await.unwrap(), CaptureStatus::Done);
            assert_eq!(capture.block(1).await.unwrap(), [5, 0, 0, 0]);
            capture.metadata().await.unwrap()
        });
        assert_eq!(awaited, metadata);
        assert_eq!(device.calls("fake.capture"), 9);

        let whole = read_capture(&capture.device, "fake.capture", crate::tio::tests::WAIT);
        assert_eq!(whole.unwrap(), readout);
    }

    /// A block the device refuses as busy says so, and a step the device
    /// refuses otherwise does not. A block no selector reaches is refused
    /// with nothing sent, waited on or awaited.
    #[test]
    fn a_refused_step_says_whether_the_device_is_busy() {
        let (device, _proxy, capture) = fake_capture();

        let busy = capture.block(7).wait().unwrap_err();
        assert!(busy.is_busy(), "{busy}");
        let past = capture.block(2).wait().unwrap_err();
        assert!(
            matches!(past, CaptureError::Rpc(_)) && !past.is_busy(),
            "{past}"
        );
        assert_eq!(device.calls("fake.capture"), 2);

        for index in [32768, u16::MAX] {
            let blocks = usize::from(index) + 1;
            assert!(matches!(
                capture.block(index).wait(),
                Err(CaptureError::TooManyBlocks { blocks: told }) if told == blocks
            ));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert!(matches!(
            runtime.block_on(capture.block(32768).into_future()),
            Err(CaptureError::TooManyBlocks { blocks: 32769 })
        ));
        assert_eq!(device.calls("fake.capture"), 2);
    }

    /// The blocks of a capture are its size over its block size, rounded
    /// up, and a capture with blocks of no bytes or more blocks than a
    /// selector reaches has none to count.
    #[test]
    fn the_blocks_are_the_size_over_the_block_size_rounded_up() {
        let sized = |size: u32, blocksize: u16| CaptureMetadata {
            size,
            blocksize,
            ..metadata(DataType::U8, size, 1.0)
        };
        assert_eq!(sized(0, 64).block_count().unwrap(), 0);
        assert_eq!(sized(64, 64).block_count().unwrap(), 1);
        assert_eq!(sized(65, 64).block_count().unwrap(), 2);
        assert_eq!(sized(3000, 256).block_count().unwrap(), 12);
        assert_eq!(sized(32768, 1).block_count().unwrap(), 32768);
        assert!(matches!(
            sized(32769, 1).block_count(),
            Err(CaptureError::TooManyBlocks { blocks: 32769 })
        ));
        assert!(matches!(
            sized(8, 0).block_count(),
            Err(CaptureError::ZeroBlocksize)
        ));
    }

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
        let raw = [0xff, 0xff, 0x01, 0x80, 0x99];
        let values = decode_capture_values(&raw, &metadata(DataType::I16, 2, 0.5)).unwrap();
        assert_eq!(values, [-0.5, -16383.5]);

        let values = decode_capture_values(&raw, &metadata(DataType::U8, 3, 2.0)).unwrap();
        assert_eq!(values, [510.0, 510.0, 2.0]);
    }

    /// A capture of a type samples do not decode, such as 24-bit integers,
    /// is refused rather than misread.
    #[test]
    fn capture_values_of_undecoded_types_are_refused() {
        let raw = [0xff; 6];
        assert!(matches!(
            decode_capture_values(&raw, &metadata(DataType::I24, 2, 1.0)),
            Err(CaptureError::UnsupportedDataType(code)) if code == DataType::I24.value()
        ));
    }
}

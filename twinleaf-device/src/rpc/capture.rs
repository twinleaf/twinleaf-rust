//! The capture RPC: one buffer a host triggers, then reads back in blocks.
//!
//! The argument is an `i16` selector: `-1` triggers a capture, `-2` reports
//! its status, `-3` reports its [`CaptureMetadata`], and `n >= 0` reads block
//! `n` of its data. No argument reports status. While a capture is in
//! progress, triggering and reading are refused as busy.

use twinleaf_proto::capture::CaptureMetadata;
pub use twinleaf_proto::capture::{Selector, Status};
use twinleaf_proto::rpc::RpcError;

use crate::rpc::{put, Reply};

/// A capture as its RPC serves it.
#[derive(Clone, Copy, Debug)]
pub struct Capture<'a> {
    /// Where the capture stands.
    pub status: Status,
    /// The captured bytes, read in blocks of `metadata.block_size`.
    pub data: &'a [u8],
    /// What the metadata selector reports.
    pub metadata: CaptureMetadata<'a>,
}

impl Capture<'_> {
    /// Answer a selector into `out`. A trigger that is allowed gets an empty
    /// reply; starting the capture is the device's to do.
    pub fn reply(&self, selector: Selector, out: &mut Reply) -> Result<(), RpcError> {
        let busy = self.status == Status::Capturing;
        match selector {
            Selector::Trigger | Selector::Block(_) if busy => Err(RpcError::Busy),
            Selector::Trigger => Ok(()),
            Selector::Status => put(out, &[self.status.code()]),
            Selector::Metadata => {
                let mut buf = [0u8; crate::rpc::REPLY_MAX];
                let len = self
                    .metadata
                    .write(&mut buf)
                    .map_err(|_| RpcError::NoBufs)?;
                put(out, &buf[..len])
            }
            Selector::Block(index) => put(out, self.block(index).ok_or(RpcError::Invalid)?),
        }
    }

    /// Block `index` of the data, `None` past its end.
    pub fn block(&self, index: u16) -> Option<&[u8]> {
        let size = usize::from(self.metadata.block_size);
        let start = usize::from(index) * size;
        let end = (start + size).min(self.data.len());
        (start < end).then(|| &self.data[start..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf_proto::capture::METADATA_VERSION;
    use twinleaf_proto::data::DataType;

    fn capture(status: Status, data: &[u8]) -> Capture<'_> {
        Capture {
            status,
            data,
            metadata: CaptureMetadata {
                version: METADATA_VERSION,
                data_type: DataType::U8,
                data_size: data.len() as u32,
                block_size: 4,
                length: data.len() as u32,
                y_calibration: 1.0,
                x_offset: 0.0,
                x_stride: 0.5,
                name: "n",
                units: "u",
                x_name: "x",
                x_units: "s",
            },
        }
    }

    #[test]
    fn blocks_cover_the_data_and_stop_at_its_end() {
        let data: Vec<u8> = (0..10).collect();
        let capture = capture(Status::Done, &data);
        assert_eq!(capture.block(0), Some(&[0, 1, 2, 3][..]));
        assert_eq!(capture.block(1), Some(&[4, 5, 6, 7][..]));
        assert_eq!(capture.block(2), Some(&[8, 9][..]));
        assert_eq!(capture.block(3), None);

        let mut out = Reply::new();
        assert_eq!(capture.reply(Selector::Block(2), &mut out), Ok(()));
        assert_eq!(out.as_slice(), &[8, 9]);
        assert_eq!(
            capture.reply(Selector::Block(3), &mut out),
            Err(RpcError::Invalid)
        );
    }

    #[test]
    fn a_capture_in_progress_refuses_triggers_and_reads() {
        let capture = capture(Status::Capturing, &[]);
        let mut out = Reply::new();
        assert_eq!(
            capture.reply(Selector::Trigger, &mut out),
            Err(RpcError::Busy)
        );
        assert_eq!(
            capture.reply(Selector::Block(0), &mut out),
            Err(RpcError::Busy)
        );
        assert_eq!(capture.reply(Selector::Status, &mut out), Ok(()));
        assert_eq!(out.as_slice(), &[Status::Capturing as u8]);

        let capture = super::Capture {
            status: Status::Idle,
            ..capture
        };
        assert_eq!(capture.reply(Selector::Trigger, &mut out), Ok(()));
    }

    #[test]
    fn metadata_round_trips() {
        let data = [0u8; 12];
        let capture = capture(Status::Done, &data);
        let mut out = Reply::new();
        capture.reply(Selector::Metadata, &mut out).unwrap();
        assert_eq!(CaptureMetadata::parse(&out), Some(capture.metadata));
    }
}

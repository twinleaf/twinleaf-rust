//! The `dev.metadata` reply: the records a device describes itself with.
//!
//! An empty argument asks for the bootstrap set: the device record, then each
//! stream with its current segment and its columns, as far as one reply holds.
//! Otherwise each selector names one record, in request order. Either way the
//! reply stops at the first record that does not fit, and the host asks again
//! for the rest.

use twinleaf_proto::data::{
    self, Metadata, MetadataQuery, MetadataQueryError, MetadataSelector, MetadataType,
    CURRENT_SEGMENT, METADATA_REPLY_FRAME_HEADER,
};
use twinleaf_proto::rpc::RpcError;

/// The records a device has. `None` is a stream or index it does not have.
pub trait Records {
    /// The device record.
    fn device(&self) -> data::Device<'_>;
    /// Every stream id, in bootstrap order.
    fn stream_ids(&self) -> impl Iterator<Item = u8>;
    /// The shape of a stream.
    fn stream(&self, stream_id: u8) -> Option<data::Stream<'_>>;
    /// A segment of a stream, with [`CURRENT_SEGMENT`] naming the one acquiring.
    fn segment(&self, stream_id: u8, index: u8) -> Option<data::Segment<'_>>;
    /// A column of a stream.
    fn column(&self, stream_id: u8, index: u8) -> Option<data::Column<'_>>;
}

/// Answer `dev.metadata` into `out`, the reply length on success.
pub fn reply(records: &impl Records, arg: &[u8], out: &mut [u8]) -> Result<usize, RpcError> {
    let query = MetadataQuery::parse(arg).map_err(|error| match error {
        MetadataQueryError::Misaligned => RpcError::ArgsSize,
        MetadataQueryError::TooManySelectors => RpcError::Invalid,
    })?;
    let mut reply = Reply { out, len: 0 };
    if query.is_bootstrap() {
        bootstrap(records, &mut reply)?;
        return Ok(reply.len);
    }
    for selector in query.selectors() {
        let record = select(records, selector).ok_or(RpcError::Invalid)?;
        if !reply.append(record)? {
            break;
        }
    }
    Ok(reply.len)
}

/// The record a selector names.
pub fn select(records: &impl Records, selector: MetadataSelector) -> Option<Metadata<'_>> {
    let MetadataSelector {
        mtype,
        stream_id,
        index,
    } = selector;
    match mtype {
        MetadataType::Device => Some(Metadata::Device(records.device())),
        MetadataType::Stream => records.stream(stream_id).map(Metadata::Stream),
        MetadataType::Segment => records.segment(stream_id, index).map(Metadata::Segment),
        MetadataType::Column => records.column(stream_id, index).map(Metadata::Column),
        MetadataType::Unknown(_) => None,
    }
}

/// A listed stream the device cannot describe is an internal error.
fn bootstrap(records: &impl Records, reply: &mut Reply<'_>) -> Result<(), RpcError> {
    if !reply.append(Metadata::Device(records.device()))? {
        return Ok(());
    }
    for stream_id in records.stream_ids() {
        let stream = records.stream(stream_id).ok_or(RpcError::Internal)?;
        let segment = records
            .segment(stream_id, CURRENT_SEGMENT)
            .ok_or(RpcError::Internal)?;
        let columns = (0..stream.n_columns).map(|index| {
            records
                .column(stream_id, index)
                .map(Metadata::Column)
                .ok_or(RpcError::Internal)
        });
        let records = [Ok(Metadata::Stream(stream)), Ok(Metadata::Segment(segment))]
            .into_iter()
            .chain(columns);
        for record in records {
            if !reply.append(record?)? {
                return Ok(());
            }
        }
    }
    Ok(())
}

struct Reply<'a> {
    out: &'a mut [u8],
    len: usize,
}

impl Reply<'_> {
    /// Append one `[type][length][record]` frame, whole or not at all: proto's
    /// writers shorten strings to the room left, and a reply must never carry
    /// a shortened record. `Ok(false)` means it did not fit.
    fn append(&mut self, record: Metadata<'_>) -> Result<bool, RpcError> {
        let len = record.record_len();
        if len > usize::from(u8::MAX) {
            return Err(RpcError::Internal);
        }
        let end = self.len + METADATA_REPLY_FRAME_HEADER + len;
        if end > self.out.len() {
            return Ok(false);
        }
        record.write_reply_frame(&mut self.out[self.len..end]);
        self.len = end;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf_proto::data::{
        DataType, FilterType, MetadataReply, SegmentFlags, MAX_METADATA_REPLY_SIZE,
    };
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::{ColumnId, SegmentId, SessionId, StreamId};

    struct Fixture {
        name: &'static str,
    }

    impl Records for Fixture {
        fn device(&self) -> data::Device<'_> {
            data::Device {
                session: SessionId::new(7),
                n_streams: 2,
                name: self.name,
                serial: "S1",
                firmware: "fw",
            }
        }

        fn stream_ids(&self) -> impl Iterator<Item = u8> {
            [1, 2].into_iter()
        }

        fn stream(&self, stream_id: u8) -> Option<data::Stream<'_>> {
            matches!(stream_id, 1 | 2).then_some(data::Stream {
                stream_id: StreamId::new(stream_id),
                n_columns: 2,
                n_segments: 4,
                sample_size: 8,
                buf_samples: 0,
                name: "s",
            })
        }

        fn segment(&self, stream_id: u8, index: u8) -> Option<data::Segment<'_>> {
            let segment_id = match index {
                CURRENT_SEGMENT => 3,
                0..=3 => index,
                _ => return None,
            };
            self.stream(stream_id)?;
            Some(data::Segment {
                stream_id: StreamId::new(stream_id),
                segment_id: SegmentId::new(segment_id),
                flags: SegmentFlags::VALID,
                epoch: Epoch::UNIX,
                timeref_serial: "S1",
                timeref_session: SessionId::new(7),
                start_time: 0,
                sampling_rate: 10,
                decimation: 1,
                filter_cutoff: 0.0,
                filter_type: FilterType::NONE,
            })
        }

        fn column(&self, stream_id: u8, index: u8) -> Option<data::Column<'_>> {
            self.stream(stream_id)?;
            (index < 2).then_some(data::Column {
                stream_id: StreamId::new(stream_id),
                index: ColumnId::new(index),
                data_type: DataType::F32,
                name: "c",
                units: "",
                description: "",
            })
        }
    }

    fn kinds(reply: &[u8]) -> Vec<MetadataType> {
        MetadataReply::parse(reply)
            .unwrap()
            .map(|(kind, _)| kind)
            .collect()
    }

    #[test]
    fn bootstrap_replies_in_sweep_order() {
        let mut out = [0u8; MAX_METADATA_REPLY_SIZE];
        let len = reply(&Fixture { name: "d" }, &[], &mut out).unwrap();
        use MetadataType::*;
        assert_eq!(
            kinds(&out[..len]),
            [Device, Stream, Segment, Column, Column, Stream, Segment, Column, Column]
        );
    }

    #[test]
    fn a_full_reply_stops_at_a_frame_boundary() {
        let fixture = Fixture { name: "d" };
        let mut whole = [0u8; MAX_METADATA_REPLY_SIZE];
        let device_frame =
            reply(&fixture, &MetadataSelector::device().encode(), &mut whole).unwrap();

        let mut out = [0u8; MAX_METADATA_REPLY_SIZE];
        let len = reply(&fixture, &[], &mut out[..device_frame + 3]).unwrap();
        assert_eq!(len, device_frame);
        assert_eq!(kinds(&out[..len]), [MetadataType::Device]);
    }

    #[test]
    fn selectors_answer_in_request_order() {
        let mut arg = Vec::new();
        arg.extend(MetadataSelector::column(2, 1).encode());
        arg.extend(MetadataSelector::segment(1, CURRENT_SEGMENT).encode());
        arg.extend(MetadataSelector::device().encode());
        let mut out = [0u8; MAX_METADATA_REPLY_SIZE];
        let len = reply(&Fixture { name: "d" }, &arg, &mut out).unwrap();
        use MetadataType::*;
        assert_eq!(kinds(&out[..len]), [Column, Segment, Device]);
        let (_, segment) = MetadataReply::parse(&out[..len]).unwrap().nth(1).unwrap();
        assert_eq!(data::Segment::parse(segment).unwrap().segment_id.value(), 3);
    }

    #[test]
    fn a_bad_request_is_refused() {
        let fixture = Fixture { name: "d" };
        let mut out = [0u8; MAX_METADATA_REPLY_SIZE];
        assert_eq!(reply(&fixture, &[1, 2], &mut out), Err(RpcError::ArgsSize));
        let too_many: Vec<u8> = (0..17)
            .flat_map(|_| MetadataSelector::device().encode())
            .collect();
        assert_eq!(reply(&fixture, &too_many, &mut out), Err(RpcError::Invalid));
        let unknown = MetadataSelector::stream(9).encode();
        assert_eq!(reply(&fixture, &unknown, &mut out), Err(RpcError::Invalid));
        let no_such_segment = MetadataSelector::segment(1, 4).encode();
        assert_eq!(
            reply(&fixture, &no_such_segment, &mut out),
            Err(RpcError::Invalid)
        );
    }

    #[test]
    fn a_record_longer_than_a_frame_is_an_internal_error() {
        let fixture = Fixture {
            name: Box::leak("n".repeat(300).into_boxed_str()),
        };
        let mut out = [0u8; MAX_METADATA_REPLY_SIZE];
        assert_eq!(reply(&fixture, &[], &mut out), Err(RpcError::Internal));
    }
}

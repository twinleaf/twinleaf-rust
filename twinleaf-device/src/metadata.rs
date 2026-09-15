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

use crate::rpc::Reply;

/// The streams a device has. `None` is a stream or index it does not have.
pub trait Streams {
    /// Every stream id, in bootstrap order.
    fn ids(&self) -> impl Iterator<Item = u8>;
    /// The shape of a stream.
    fn stream(&self, stream_id: u8) -> Option<data::Stream<'_>>;
    /// A segment of a stream, with [`CURRENT_SEGMENT`] naming the one acquiring.
    fn segment(&self, stream_id: u8, index: u8) -> Option<data::Segment<'_>>;
    /// A column of a stream.
    fn column(&self, stream_id: u8, index: u8) -> Option<data::Column<'_>>;

    /// The same segment, handed to `f` under the borrow it was read through.
    ///
    /// This is what every reply below asks for, so a device that keeps a
    /// stream behind a lock describes it from the record itself rather than
    /// from a copy taken out of one.
    fn with_segment<R>(
        &self,
        stream_id: u8,
        index: u8,
        f: impl FnOnce(data::Segment<'_>) -> R,
    ) -> Option<R> {
        Some(f(self.segment(stream_id, index)?))
    }
}

/// A device with no streams.
impl Streams for () {
    fn ids(&self) -> impl Iterator<Item = u8> {
        core::iter::empty()
    }

    fn stream(&self, _stream_id: u8) -> Option<data::Stream<'_>> {
        None
    }

    fn segment(&self, _stream_id: u8, _index: u8) -> Option<data::Segment<'_>> {
        None
    }

    fn column(&self, _stream_id: u8, _index: u8) -> Option<data::Column<'_>> {
        None
    }
}

/// Answer `dev.metadata` into `out` for the device `device` describes.
pub fn reply(
    device: data::Device<'_>,
    streams: &(impl Streams + ?Sized),
    arg: &[u8],
    out: &mut Reply,
) -> Result<(), RpcError> {
    let query = MetadataQuery::parse(arg).map_err(|error| match error {
        MetadataQueryError::Misaligned => RpcError::ArgsSize,
        MetadataQueryError::TooManySelectors => RpcError::Invalid,
    })?;
    if query.is_bootstrap() {
        return bootstrap(device, streams, out);
    }
    for selector in query.selectors() {
        let appended = match selector.mtype {
            MetadataType::Segment => streams
                .with_segment(selector.stream_id, selector.index, |segment| {
                    append(out, Metadata::Segment(segment))
                })
                .ok_or(RpcError::Invalid)?,
            MetadataType::Device
            | MetadataType::Stream
            | MetadataType::Column
            | MetadataType::Unknown(_) => append(
                out,
                select(device, streams, selector).ok_or(RpcError::Invalid)?,
            ),
        };
        if !appended? {
            break;
        }
    }
    Ok(())
}

/// The record a selector names.
pub fn select<'a>(
    device: data::Device<'a>,
    streams: &'a (impl Streams + ?Sized),
    selector: MetadataSelector,
) -> Option<Metadata<'a>> {
    let MetadataSelector {
        mtype,
        stream_id,
        index,
    } = selector;
    match mtype {
        MetadataType::Device => Some(Metadata::Device(device)),
        MetadataType::Stream => streams.stream(stream_id).map(Metadata::Stream),
        MetadataType::Segment => streams.segment(stream_id, index).map(Metadata::Segment),
        MetadataType::Column => streams.column(stream_id, index).map(Metadata::Column),
        MetadataType::Unknown(_) => None,
    }
}

/// A listed stream the device cannot describe is an internal error.
fn bootstrap(
    device: data::Device<'_>,
    streams: &(impl Streams + ?Sized),
    out: &mut Reply,
) -> Result<(), RpcError> {
    if !append(out, Metadata::Device(device))? {
        return Ok(());
    }
    for stream_id in streams.ids() {
        let stream = streams.stream(stream_id).ok_or(RpcError::Internal)?;
        let columns = stream.n_columns;
        if !append(out, Metadata::Stream(stream))? {
            return Ok(());
        }
        let segment = streams
            .with_segment(stream_id, CURRENT_SEGMENT, |segment| {
                append(out, Metadata::Segment(segment))
            })
            .ok_or(RpcError::Internal)?;
        if !segment? {
            return Ok(());
        }
        for index in 0..columns {
            let column = streams.column(stream_id, index).ok_or(RpcError::Internal)?;
            if !append(out, Metadata::Column(column))? {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Append one `[type][length][record]` frame, whole or not at all: proto's
/// writers shorten strings to the room left, and a reply must never carry a
/// shortened record. `Ok(false)` means it did not fit.
fn append(out: &mut Reply, record: Metadata<'_>) -> Result<bool, RpcError> {
    let len = record.record_len();
    if len > usize::from(u8::MAX) {
        return Err(RpcError::Internal);
    }
    let start = out.len();
    if out
        .resize_default(start + METADATA_REPLY_FRAME_HEADER + len)
        .is_err()
    {
        return Ok(false);
    }
    record.write_reply_frame(&mut out[start..]);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::REPLY_MAX;
    use twinleaf_proto::data::{DataType, FilterType, MetadataReply, SegmentFlags};
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::{ColumnId, SegmentId, SessionId, StreamId};

    struct Fixture;

    fn device(name: &str) -> data::Device<'_> {
        data::Device {
            session: SessionId::new(7),
            n_streams: 2,
            name,
            serial: "S1",
            firmware: "fw",
        }
    }

    impl Streams for Fixture {
        fn ids(&self) -> impl Iterator<Item = u8> {
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

    /// The same device, with its segments behind a lock: it has no record to
    /// hand out, so [`Streams::with_segment`] is the only way into one.
    struct Locked(Fixture);

    impl Streams for Locked {
        fn ids(&self) -> impl Iterator<Item = u8> {
            self.0.ids()
        }

        fn stream(&self, stream_id: u8) -> Option<data::Stream<'_>> {
            self.0.stream(stream_id)
        }

        fn segment(&self, _stream_id: u8, _index: u8) -> Option<data::Segment<'_>> {
            None
        }

        fn column(&self, stream_id: u8, index: u8) -> Option<data::Column<'_>> {
            self.0.column(stream_id, index)
        }

        fn with_segment<R>(
            &self,
            stream_id: u8,
            index: u8,
            f: impl FnOnce(data::Segment<'_>) -> R,
        ) -> Option<R> {
            Some(f(self.0.segment(stream_id, index)?))
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
        let mut out = Reply::new();
        reply(device("d"), &Fixture, &[], &mut out).unwrap();
        use MetadataType::*;
        assert_eq!(
            kinds(&out),
            [Device, Stream, Segment, Column, Column, Stream, Segment, Column, Column]
        );
    }

    #[test]
    fn a_full_reply_stops_at_a_frame_boundary() {
        let frame = |selector: MetadataSelector| {
            let mut one = Reply::new();
            reply(device("d"), &Fixture, &selector.encode(), &mut one).unwrap();
            one.len()
        };
        let room_for_one = REPLY_MAX - frame(MetadataSelector::stream(1)) + 1;

        let mut out = Reply::new();
        let padding = room_for_one - frame(MetadataSelector::device());
        out.resize_default(padding).unwrap();
        bootstrap(device("d"), &Fixture, &mut out).unwrap();
        assert_eq!(kinds(&out[padding..]), [MetadataType::Device]);
    }

    #[test]
    fn selectors_answer_in_request_order() {
        let mut arg = Vec::new();
        arg.extend(MetadataSelector::column(2, 1).encode());
        arg.extend(MetadataSelector::segment(1, CURRENT_SEGMENT).encode());
        arg.extend(MetadataSelector::device().encode());
        let mut out = Reply::new();
        reply(device("d"), &Fixture, &arg, &mut out).unwrap();
        use MetadataType::*;
        assert_eq!(kinds(&out), [Column, Segment, Device]);
        let (_, segment) = MetadataReply::parse(&out).unwrap().nth(1).unwrap();
        assert_eq!(data::Segment::parse(segment).unwrap().segment_id.value(), 3);
    }

    #[test]
    fn a_bad_request_is_refused() {
        let mut out = Reply::new();
        assert_eq!(
            reply(device("d"), &Fixture, &[1, 2], &mut out),
            Err(RpcError::ArgsSize)
        );
        let too_many: Vec<u8> = (0..17)
            .flat_map(|_| MetadataSelector::device().encode())
            .collect();
        assert_eq!(
            reply(device("d"), &Fixture, &too_many, &mut out),
            Err(RpcError::Invalid)
        );
        let unknown = MetadataSelector::stream(9).encode();
        assert_eq!(
            reply(device("d"), &Fixture, &unknown, &mut out),
            Err(RpcError::Invalid)
        );
        let no_such_segment = MetadataSelector::segment(1, 4).encode();
        assert_eq!(
            reply(device("d"), &Fixture, &no_such_segment, &mut out),
            Err(RpcError::Invalid)
        );
    }

    /// Every reply a device gives is the reply it gives when its records have
    /// to be read in place, which is what lets a firmware keep its streams
    /// behind a lock.
    #[test]
    fn a_locked_device_replies_what_a_handed_out_record_does() {
        let queries = [
            Vec::new(),
            MetadataSelector::segment(1, CURRENT_SEGMENT)
                .encode()
                .into(),
            MetadataSelector::segment(2, 0).encode().into(),
        ];
        for query in queries {
            let (mut handed, mut locked) = (Reply::new(), Reply::new());
            reply(device("d"), &Fixture, &query, &mut handed).unwrap();
            reply(device("d"), &Locked(Fixture), &query, &mut locked).unwrap();
            assert_eq!(handed, locked);
            assert!(kinds(&locked).contains(&MetadataType::Segment));
        }
    }

    #[test]
    fn a_record_longer_than_a_frame_is_an_internal_error() {
        let long = "n".repeat(300);
        let mut out = Reply::new();
        assert_eq!(
            reply(device(&long), &Fixture, &[], &mut out),
            Err(RpcError::Internal)
        );
    }
}

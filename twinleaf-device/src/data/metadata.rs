//! The `dev.metadata` reply: the records a device describes itself with.
//!
//! An empty argument asks for the bootstrap set: the device record, then each
//! stream with its current segment and its columns, as far as one reply holds.
//! Otherwise each selector names one record, in request order. Either way the
//! reply stops at the first record that does not fit, and the host asks again
//! for the rest. A [`Carousel`] repeats the records a session fixes on control.

use twinleaf_proto::data::{
    self, Metadata, MetadataFlags, MetadataRequest, MetadataRequestError, MetadataSelector,
    MetadataType, CURRENT_SEGMENT, METADATA_REPLY_FRAME_HEADER,
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
    let request = MetadataRequest::parse(arg).map_err(|error| match error {
        MetadataRequestError::Misaligned => RpcError::ArgsSize,
        MetadataRequestError::TooManySelectors => RpcError::Invalid,
    })?;
    if request.is_bootstrap() {
        return bootstrap(device, streams, out);
    }
    for selector in request.selectors() {
        let MetadataSelector {
            mtype,
            stream_id,
            index,
        } = selector;
        let appended = match mtype {
            MetadataType::Device => append(out, Metadata::Device(device))?,
            MetadataType::Stream => {
                let stream = streams.stream(stream_id).ok_or(RpcError::Invalid)?;
                append(out, Metadata::Stream(stream))?
            }
            MetadataType::Segment => {
                let segment = streams.segment(stream_id, index).ok_or(RpcError::Invalid)?;
                append(out, Metadata::Segment(segment))?
            }
            MetadataType::Column => {
                let column = streams.column(stream_id, index).ok_or(RpcError::Invalid)?;
                append(out, Metadata::Column(column))?
            }
            MetadataType::Unknown(_) => return Err(RpcError::Invalid),
        };
        if !appended {
            break;
        }
    }
    Ok(())
}

/// Round-robin cursor over a device's metadata: the device, then for each
/// stream its shape, its current segment unless it skips segments, and its columns.
#[derive(Debug, Clone, Copy, Default)]
pub struct Sweep {
    position: Position,
    skips_segments: bool,
}

impl Sweep {
    /// A sweep starting at the device record.
    pub const fn new() -> Self {
        Self {
            position: Position::Device,
            skips_segments: false,
        }
    }

    /// A sweep of the records a session fixes: the device's, and each stream's and its columns'.
    pub const fn layout() -> Self {
        Self {
            position: Position::Device,
            skips_segments: true,
        }
    }

    /// The next record, and whether it closes a pass. `None` is a stream the
    /// device lists but cannot describe.
    pub fn step<'a>(
        &mut self,
        device: data::Device<'a>,
        streams: &'a (impl Streams + ?Sized),
    ) -> Option<(Metadata<'a>, bool)> {
        let id = |index: u8| streams.ids().nth(usize::from(index));
        let columns = |index: u8| Some(streams.stream(id(index)?)?.n_columns);
        let gone = match self.position {
            Position::Device => false,
            Position::Stream(s) | Position::Segment(s) => id(s).is_none(),
            Position::Column(s, c) => c >= columns(s).unwrap_or(0),
        };
        let here = match gone {
            true => Position::Device,
            false => self.position,
        };
        let record = match here {
            Position::Device => Metadata::Device(device),
            Position::Stream(s) => Metadata::Stream(streams.stream(id(s)?)?),
            Position::Segment(s) => Metadata::Segment(streams.segment(id(s)?, CURRENT_SEGMENT)?),
            Position::Column(s, c) => Metadata::Column(streams.column(id(s)?, c)?),
        };
        self.position = match here {
            Position::Device if id(0).is_some() => Position::Stream(0),
            Position::Device => Position::Device,
            Position::Stream(s) if !self.skips_segments => Position::Segment(s),
            Position::Stream(s) | Position::Segment(s) if columns(s)? > 0 => Position::Column(s, 0),
            Position::Column(s, c) if c + 1 < columns(s)? => Position::Column(s, c + 1),
            Position::Stream(s) | Position::Segment(s) | Position::Column(s, _) => {
                match id(s + 1) {
                    Some(_) => Position::Stream(s + 1),
                    None => Position::Device,
                }
            }
        };
        Some((record, matches!(self.position, Position::Device)))
    }
}

/// Where a sweep stands, by zero-based stream and column position.
#[derive(Debug, Clone, Copy, Default)]
enum Position {
    #[default]
    Device,
    Stream(u8),
    Segment(u8),
    Column(u8, u8),
}

/// The device, stream and column records a device repeats on control, one every pass time over
/// the records in a pass, so a listener that missed them learns the device within a pass.
#[derive(Debug, Clone, Copy, Default)]
pub struct Carousel {
    sweep: Sweep,
    next_ns: u64,
}

impl Carousel {
    /// A carousel whose first record, the device's, is due a step after `now_ns`.
    pub fn new(
        now_ns: u64,
        pass_ns: u64,
        device: data::Device<'_>,
        streams: &(impl Streams + ?Sized),
    ) -> Self {
        Self {
            sweep: Sweep::layout(),
            next_ns: now_ns + step(pass_ns, device, streams),
        }
    }

    /// When its next record is due, never while `pass_ns` is zero.
    pub fn deadline(&self, pass_ns: u64) -> u64 {
        match pass_ns {
            0 => u64::MAX,
            _ => self.next_ns,
        }
    }

    /// Hand `send` each record due by `now_ns` under PERIODIC, with LAST on the one closing a
    /// pass, owing at most a pass and waiting at most a step, and none while `pass_ns` is zero.
    pub fn turn(
        &mut self,
        now_ns: u64,
        pass_ns: u64,
        device: data::Device<'_>,
        streams: &(impl Streams + ?Sized),
        send: &mut dyn FnMut(Metadata<'_>, MetadataFlags),
    ) {
        if pass_ns == 0 {
            self.next_ns = now_ns;
            return;
        }
        let step = step(pass_ns, device, streams);
        self.next_ns = self
            .next_ns
            .clamp(now_ns.saturating_sub(pass_ns), now_ns + step);
        while self.next_ns <= now_ns {
            self.next_ns += step;
            let Some((record, last)) = self.sweep.step(device, streams) else {
                return;
            };
            let flags = match last {
                true => MetadataFlags::PERIODIC | MetadataFlags::LAST,
                false => MetadataFlags::PERIODIC,
            };
            send(record, flags);
        }
    }
}

/// Nanoseconds between a carousel's records: a pass's time over the records in it.
fn step(pass_ns: u64, device: data::Device<'_>, streams: &(impl Streams + ?Sized)) -> u64 {
    let mut pass = Sweep::layout();
    let records = core::iter::from_fn(|| pass.step(device, streams))
        .position(|(_, last)| last)
        .map_or(1, |at| at as u64 + 1);
    (pass_ns / records).max(1)
}

/// The bootstrap set, as far as one reply holds. A listed stream the device
/// cannot describe is an internal error.
fn bootstrap(
    device: data::Device<'_>,
    streams: &(impl Streams + ?Sized),
    out: &mut Reply,
) -> Result<(), RpcError> {
    let mut sweep = Sweep::new();
    loop {
        let (record, last) = sweep.step(device, streams).ok_or(RpcError::Internal)?;
        if !append(out, record)? || last {
            return Ok(());
        }
    }
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

    /// A device whose streams are numbered 1..=n with the given column counts.
    struct Shape(Vec<u8>);

    fn fixture() -> Shape {
        Shape(vec![2, 2])
    }

    fn device(name: &str) -> data::Device<'_> {
        data::Device {
            session: SessionId::new(7),
            n_streams: 2,
            name,
            serial: "S1",
            firmware: "fw",
            protocol: 0,
        }
    }

    impl Streams for Shape {
        fn ids(&self) -> impl Iterator<Item = u8> {
            1..=self.0.len() as u8
        }

        fn stream(&self, stream_id: u8) -> Option<data::Stream<'_>> {
            let n_columns = *self.0.get(usize::from(stream_id.checked_sub(1)?))?;
            Some(data::Stream {
                stream_id: StreamId::new(stream_id),
                n_columns,
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
            (index < self.stream(stream_id)?.n_columns).then_some(data::Column {
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

    /// A record as its type, stream and index.
    fn named(record: Metadata<'_>) -> (MetadataType, u8, u8) {
        match record {
            Metadata::Device(_) => (MetadataType::Device, 0, 0),
            Metadata::Stream(s) => (MetadataType::Stream, s.stream_id.value(), 0),
            Metadata::Segment(s) => (
                MetadataType::Segment,
                s.stream_id.value(),
                s.segment_id.value(),
            ),
            Metadata::Column(c) => (MetadataType::Column, c.stream_id.value(), c.index.value()),
        }
    }

    fn walk(mut sweep: Sweep, shape: &[u8], steps: usize) -> Vec<(MetadataType, u8, u8, bool)> {
        let streams = Shape(shape.to_vec());
        (0..steps)
            .map(|_| {
                let (record, last) = sweep.step(device("d"), &streams).unwrap();
                let (kind, stream, index) = named(record);
                (kind, stream, index, last)
            })
            .collect()
    }

    const PASS: u64 = 5_000_000_000;

    /// What a carousel begun at zero sends turned at each of `at`, as type, stream, index and flags.
    fn carousel(
        shape: &[u8],
        at: impl IntoIterator<Item = u64>,
    ) -> Vec<(MetadataType, u8, u8, u8)> {
        let streams = Shape(shape.to_vec());
        let mut carousel = Carousel::new(0, PASS, device("d"), &streams);
        let mut sent = Vec::new();
        at.into_iter().for_each(|now| {
            carousel.turn(now, PASS, device("d"), &streams, &mut |record, flags| {
                let (kind, stream, index) = named(record);
                sent.push((kind, stream, index, flags.bits()));
            })
        });
        sent
    }

    #[test]
    fn a_sweep_walks_the_device_then_each_stream_with_its_columns() {
        use MetadataType::*;
        #[rustfmt::skip]
        assert_eq!(walk(Sweep::new(), &[2, 3], 11), vec![
            (Device, 0, 0, false),
            (Stream, 1, 0, false),
            (Segment, 1, 3, false),
            (Column, 1, 0, false),
            (Column, 1, 1, false),
            (Stream, 2, 0, false),
            (Segment, 2, 3, false),
            (Column, 2, 0, false),
            (Column, 2, 1, false),
            (Column, 2, 2, true),
            (Device, 0, 0, false),
        ]);
    }

    #[test]
    fn a_sweep_of_a_device_with_no_streams_is_the_device_record() {
        assert_eq!(
            walk(Sweep::new(), &[], 2),
            vec![(MetadataType::Device, 0, 0, true); 2]
        );
    }

    #[test]
    fn a_sweep_skips_a_stream_with_no_columns() {
        use MetadataType::*;
        #[rustfmt::skip]
        assert_eq!(walk(Sweep::new(), &[0, 1], 6), vec![
            (Device, 0, 0, false),
            (Stream, 1, 0, false),
            (Segment, 1, 3, false),
            (Stream, 2, 0, false),
            (Segment, 2, 3, false),
            (Column, 2, 0, true),
        ]);
    }

    #[test]
    fn a_layout_sweep_skips_every_segment() {
        use MetadataType::*;
        #[rustfmt::skip]
        assert_eq!(walk(Sweep::layout(), &[2, 0, 1], 8), vec![
            (Device, 0, 0, false),
            (Stream, 1, 0, false),
            (Column, 1, 0, false),
            (Column, 1, 1, false),
            (Stream, 2, 0, false),
            (Stream, 3, 0, false),
            (Column, 3, 0, true),
            (Device, 0, 0, false),
        ]);
    }

    /// Turned a step at a time, a carousel sends the layout sweep's pass once, LAST on its end.
    #[test]
    fn a_carousel_pass_is_the_layout_sweep_once() {
        use MetadataType::*;
        let (periodic, last) = (
            MetadataFlags::PERIODIC.bits(),
            (MetadataFlags::PERIODIC | MetadataFlags::LAST).bits(),
        );
        let step = PASS / 8;
        #[rustfmt::skip]
        assert_eq!(carousel(&[2, 3], (1..=9).map(|k| k * step)), vec![
            (Device, 0, 0, periodic),
            (Stream, 1, 0, periodic),
            (Column, 1, 0, periodic),
            (Column, 1, 1, periodic),
            (Stream, 2, 0, periodic),
            (Column, 2, 0, periodic),
            (Column, 2, 1, periodic),
            (Column, 2, 2, last),
            (Device, 0, 0, periodic),
        ]);
    }

    /// However many records a pass holds, and however coarsely it is turned, a pass takes the
    /// pass time.
    #[test]
    fn a_carousel_paces_a_pass_to_the_pass_time() {
        for shape in [&[][..], &[1], &[2, 3], &[4, 0, 9, 1]] {
            let records = 1 + shape
                .iter()
                .map(|&columns| 1 + usize::from(columns))
                .sum::<usize>();
            for turns in [1, 3, 10, 100] {
                let sent = carousel(shape, (1..=turns).map(|k| k * PASS / turns));
                assert_eq!(sent.len(), records, "{shape:?} turned {turns} times");
                assert!(sent[records - 1].3 & MetadataFlags::LAST.bits() != 0);
            }
        }
    }

    /// A carousel left unturned owes at most a pass, and none while its pass time is zero.
    #[test]
    fn a_carousel_owes_at_most_a_pass() {
        assert_eq!(carousel(&[2, 3], [100 * PASS]).len(), 9);
        let streams = Shape(vec![2, 3]);
        let mut carousel = Carousel::new(0, 0, device("d"), &streams);
        let mut sent = 0;
        carousel.turn(PASS, 0, device("d"), &streams, &mut |_, _| sent += 1);
        assert_eq!((sent, carousel.deadline(0)), (0, u64::MAX));
        carousel.turn(PASS + 1, PASS, device("d"), &streams, &mut |_, _| sent += 1);
        assert_eq!(sent, 1);
    }

    /// A pass time shortened while a record waits brings that record within the new step.
    #[test]
    fn a_shorter_pass_time_waits_at_most_its_step() {
        let streams = Shape(vec![2, 3]);
        let mut carousel = Carousel::new(0, 100 * PASS, device("d"), &streams);
        let mut sent = 0;
        carousel.turn(0, PASS, device("d"), &streams, &mut |_, _| sent += 1);
        assert_eq!((sent, carousel.deadline(PASS)), (0, PASS / 8));
    }

    #[test]
    fn a_sweep_restarts_when_its_position_disappears() {
        let mut sweep = Sweep::new();
        let two = Shape(vec![2, 3]);
        for _ in 0..4 {
            sweep.step(device("d"), &two).unwrap();
        }
        let one = Shape(vec![1]);
        let (record, last) = sweep.step(device("d"), &one).unwrap();
        assert!(matches!(record, Metadata::Device(_)));
        assert!(!last);
    }

    #[test]
    fn bootstrap_replies_in_sweep_order() {
        let mut out = Reply::new();
        reply(device("d"), &fixture(), &[], &mut out).unwrap();
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
            reply(device("d"), &fixture(), &selector.encode(), &mut one).unwrap();
            one.len()
        };
        let room_for_one = REPLY_MAX - frame(MetadataSelector::stream(1)) + 1;

        let mut out = Reply::new();
        let padding = room_for_one - frame(MetadataSelector::device());
        out.resize_default(padding).unwrap();
        bootstrap(device("d"), &fixture(), &mut out).unwrap();
        assert_eq!(kinds(&out[padding..]), [MetadataType::Device]);
    }

    #[test]
    fn selectors_answer_in_request_order() {
        let mut arg = Vec::new();
        arg.extend(MetadataSelector::column(2, 1).encode());
        arg.extend(MetadataSelector::segment(1, CURRENT_SEGMENT).encode());
        arg.extend(MetadataSelector::device().encode());
        let mut out = Reply::new();
        reply(device("d"), &fixture(), &arg, &mut out).unwrap();
        use MetadataType::*;
        assert_eq!(kinds(&out), [Column, Segment, Device]);
        let (_, segment) = MetadataReply::parse(&out).unwrap().nth(1).unwrap();
        assert_eq!(data::Segment::parse(segment).unwrap().segment_id.value(), 3);
    }

    #[test]
    fn a_bad_request_is_refused() {
        let mut out = Reply::new();
        assert_eq!(
            reply(device("d"), &fixture(), &[1, 2], &mut out),
            Err(RpcError::ArgsSize)
        );
        let too_many: Vec<u8> = (0..17)
            .flat_map(|_| MetadataSelector::device().encode())
            .collect();
        assert_eq!(
            reply(device("d"), &fixture(), &too_many, &mut out),
            Err(RpcError::Invalid)
        );
        let unknown = MetadataSelector::stream(9).encode();
        assert_eq!(
            reply(device("d"), &fixture(), &unknown, &mut out),
            Err(RpcError::Invalid)
        );
        let no_such_segment = MetadataSelector::segment(1, 4).encode();
        assert_eq!(
            reply(device("d"), &fixture(), &no_such_segment, &mut out),
            Err(RpcError::Invalid)
        );
    }

    #[test]
    fn a_record_longer_than_a_frame_is_an_internal_error() {
        let long = "n".repeat(300);
        let mut out = Reply::new();
        assert_eq!(
            reply(device(&long), &fixture(), &[], &mut out),
            Err(RpcError::Internal)
        );
    }
}

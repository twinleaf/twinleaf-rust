//! A data stream: what it carries, what it is acquiring, and what it sends.
//!
//! The definition is static, the segment ring is the acquisition, and the
//! publisher turns issued samples into packets. A slice of streams is what a
//! device describes itself with.

use twinleaf_proto::data::{self, DataType, CURRENT_SEGMENT};
use twinleaf_proto::{ColumnId, SegmentId, StreamId};

use crate::metadata::Streams;
use crate::publisher::{Publisher, MAX_SAMPLE_BYTES};
use crate::segments::{Busy, Params, Segment, Segments, Timeref};
use crate::Sink;

/// One column of a stream's sample.
pub struct ColumnDef {
    /// Column name.
    pub name: &'static str,
    /// Units of the value.
    pub units: &'static str,
    /// How the value is encoded.
    pub data_type: DataType,
    /// What the column is.
    pub description: &'static str,
}

/// What a stream carries: its name and the columns of one sample.
pub struct StreamDef {
    /// Stream name.
    pub name: &'static str,
    /// Columns, in the order they are packed into a sample.
    pub columns: &'static [ColumnDef],
}

impl StreamDef {
    /// Bytes of one packed sample.
    pub const fn sample_size(&self) -> usize {
        let mut size = 0;
        let mut index = 0;
        while index < self.columns.len() {
            size += self.columns[index].data_type.size();
            index += 1;
        }
        size
    }
}

/// One data stream with `N` segments.
pub struct Stream<const N: usize> {
    id: StreamId,
    def: &'static StreamDef,
    segments: Segments<N>,
    publisher: Publisher,
}

impl<const N: usize> Stream<N> {
    /// A stopped stream, `None` if one of its samples does not fit a packet.
    pub fn new(id: StreamId, def: &'static StreamDef, params: Params) -> Option<Self> {
        (1..=MAX_SAMPLE_BYTES)
            .contains(&def.sample_size())
            .then(|| Self {
                id,
                def,
                segments: Segments::new(params),
                publisher: Publisher::new(),
            })
    }

    /// Send a packet once it holds `bytes` of samples rather than when full.
    pub fn batched(mut self, bytes: usize) -> Self {
        self.publisher = Publisher::with_capacity(bytes);
        self
    }

    /// Sample bytes in the packet being filled.
    pub fn staged(&self) -> usize {
        self.publisher.staged()
    }

    /// The stream id.
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// What the stream carries.
    pub fn def(&self) -> &'static StreamDef {
        self.def
    }

    /// Begin acquiring, with sample zero at `timeref`.
    pub fn start(&mut self, timeref: Timeref) -> Result<(), Busy> {
        self.segments.start(timeref)
    }

    /// Stop acquiring.
    pub fn stop(&mut self) {
        self.segments.stop();
    }

    /// Acquire with `params` from the next segment on.
    pub fn retune(&mut self, params: Params) {
        self.segments.retune(params);
    }

    /// Begin a new segment with the same parameters.
    pub fn rollover(&mut self) {
        self.segments.rollover();
    }

    /// Mark every segment opened from here on as begun while the time
    /// reference's pulses were absent.
    pub fn set_holdover(&mut self, holdover: bool) {
        self.segments.set_holdover(holdover);
    }

    /// Publish one sample of [`StreamDef::sample_size`] bytes, packed in
    /// column order.
    pub fn push(&mut self, sample: &[u8], out: &mut impl Sink) {
        let Some(issued) = self.segments.issue() else {
            return;
        };
        let record = self
            .segments
            .get(issued.segment)
            .expect("the segment issuing is in the ring")
            .record(self.id);
        self.publisher.push(issued, record, sample, out);
    }

    /// Advance past `count` samples without publishing them.
    pub fn skip(&mut self, count: u32) {
        self.segments.skip(count);
    }

    /// Send the packet being filled, if there is one.
    pub fn flush(&mut self, out: &mut impl Sink) {
        self.publisher.flush(out);
    }

    /// The segment a host asks about: the one a change is pending on, or the
    /// one acquiring.
    pub fn current(&self) -> &Segment {
        self.segments.current()
    }

    /// The stream record. There is no retransmission buffer, so it offers no
    /// buffered samples.
    pub fn record(&self) -> data::Stream<'_> {
        data::Stream {
            stream_id: self.id,
            n_columns: self.def.columns.len() as u8,
            n_segments: N as u8,
            sample_size: self.def.sample_size() as u16,
            buf_samples: 0,
            name: self.def.name,
        }
    }

    /// A segment record, with [`CURRENT_SEGMENT`] naming the one acquiring.
    pub fn segment(&self, index: u8) -> Option<data::Segment<'_>> {
        match index {
            CURRENT_SEGMENT => Some(self.segments.current().record(self.id)),
            index => Some(self.segments.get(SegmentId::new(index))?.record(self.id)),
        }
    }

    /// A column record.
    pub fn column(&self, index: u8) -> Option<data::Column<'_>> {
        let column = self.def.columns.get(usize::from(index))?;
        Some(data::Column {
            stream_id: self.id,
            index: ColumnId::new(index),
            data_type: column.data_type,
            name: column.name,
            units: column.units,
            description: column.description,
        })
    }
}

/// A device's streams, in the order it describes them.
impl<const N: usize> Streams for [Stream<N>] {
    fn ids(&self) -> impl Iterator<Item = u8> {
        self.iter().map(|stream| stream.id.value())
    }

    fn stream(&self, stream_id: u8) -> Option<data::Stream<'_>> {
        Some(find(self, stream_id)?.record())
    }

    fn column(&self, stream_id: u8, index: u8) -> Option<data::Column<'_>> {
        find(self, stream_id)?.column(index)
    }

    fn with_segment<R>(
        &self,
        stream_id: u8,
        index: u8,
        f: impl FnOnce(data::Segment<'_>) -> R,
    ) -> Option<R> {
        Some(f(find(self, stream_id)?.segment(index)?))
    }
}

fn find<const N: usize>(streams: &[Stream<N>], stream_id: u8) -> Option<&Stream<N>> {
    streams.iter().find(|stream| stream.id.value() == stream_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segments::SegmentState;
    use core::num::NonZeroU32;
    use twinleaf_proto::data::{MetadataType, SegmentFlags};
    use twinleaf_proto::packet::{PacketType, PacketView};
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::SessionId;

    static SINE: StreamDef = StreamDef {
        name: "sine",
        columns: &[
            ColumnDef {
                name: "sine",
                units: "V",
                data_type: DataType::F64,
                description: "Noisy sine wave",
            },
            ColumnDef {
                name: "cosine",
                units: "V",
                data_type: DataType::F64,
                description: "Noisy quadrature wave",
            },
        ],
    };

    static STATUS: StreamDef = StreamDef {
        name: "status",
        columns: &[ColumnDef {
            name: "status",
            units: "",
            data_type: DataType::U8,
            description: "",
        }],
    };

    #[derive(Default)]
    struct Sent(Vec<Vec<u8>>);

    impl Sink for Sent {
        fn send(&mut self, packet: &[u8]) {
            self.0.push(packet.to_vec());
        }
    }

    fn stream_packet(stream_id: u8) -> PacketType {
        PacketType::stream(stream_id).unwrap()
    }

    impl Sent {
        fn types(&self) -> Vec<PacketType> {
            self.0
                .iter()
                .map(|packet| PacketView::parse_prefix(packet).unwrap().0.header.ptype)
                .collect()
        }
    }

    fn params(rate: u32) -> Params {
        Params {
            rate: NonZeroU32::new(rate).unwrap(),
            decimation: NonZeroU32::MIN,
            cutoff: 0.0,
            enabled: true,
        }
    }

    fn streams() -> [Stream<4>; 2] {
        [
            Stream::new(StreamId::new(1), &SINE, params(10)).unwrap(),
            Stream::new(StreamId::new(2), &STATUS, params(10)).unwrap(),
        ]
    }

    fn timeref() -> Timeref {
        Timeref::new(Epoch::UNIX, 1000, SessionId::new(3), "S1").unwrap()
    }

    fn started() -> [Stream<4>; 2] {
        let timeref = timeref();
        let mut streams = streams();
        streams
            .iter_mut()
            .for_each(|stream| stream.start(timeref.clone()).unwrap());
        streams
    }

    #[test]
    fn a_sample_that_does_not_fit_a_packet_has_no_stream() {
        const WIDE_COLUMN: ColumnDef = ColumnDef {
            name: "c",
            units: "",
            data_type: DataType::F64,
            description: "",
        };
        static WIDE: StreamDef = StreamDef {
            name: "wide",
            columns: &[WIDE_COLUMN; 64],
        };
        static EMPTY: StreamDef = StreamDef {
            name: "empty",
            columns: &[],
        };
        assert_eq!(SINE.sample_size(), 16);
        assert!(Stream::<4>::new(StreamId::new(1), &WIDE, params(10)).is_none());
        assert!(Stream::<4>::new(StreamId::new(1), &EMPTY, params(10)).is_none());
    }

    #[test]
    fn a_stream_publishes_a_record_and_then_its_samples() {
        let mut streams = started();
        let mut sent = Sent::default();
        streams[0].push(&[0; 16], &mut sent);
        streams[0].push(&[1; 16], &mut sent);
        streams[0].flush(&mut sent);
        assert_eq!(sent.types(), [PacketType::METADATA, stream_packet(1)]);
        let (view, _) = PacketView::parse_prefix(&sent.0[1]).unwrap();
        let samples = data::Samples::parse(view.header, view.payload).unwrap();
        assert_eq!(samples.stream_id, StreamId::new(1));
        assert_eq!(samples.data.len(), 32);
    }

    #[test]
    fn a_batched_stream_sends_short_packets() {
        let mut stream = Stream::<4>::new(StreamId::new(1), &SINE, params(10))
            .unwrap()
            .batched(32);
        stream.start(timeref()).unwrap();
        let mut sent = Sent::default();
        (0..3).for_each(|_| stream.push(&[0; 16], &mut sent));
        assert_eq!(stream.staged(), 16);
        assert_eq!(sent.types(), [PacketType::METADATA, stream_packet(1)]);
    }

    #[test]
    fn a_stopped_stream_publishes_nothing() {
        let mut streams = streams();
        let mut sent = Sent::default();
        streams[0].push(&[0; 16], &mut sent);
        streams[0].flush(&mut sent);
        assert!(sent.0.is_empty());
    }

    #[test]
    fn a_skipped_sample_leaves_a_gap_and_no_packet_spans_it() {
        let mut streams = started();
        let mut sent = Sent::default();
        streams[1].push(&[0], &mut sent);
        streams[1].skip(1);
        streams[1].push(&[2], &mut sent);
        streams[1].flush(&mut sent);

        let numbers: Vec<u32> = sent.0[1..]
            .iter()
            .map(|packet| {
                let (view, _) = PacketView::parse_prefix(packet).unwrap();
                data::Samples::parse(view.header, view.payload)
                    .unwrap()
                    .first
                    .value()
            })
            .collect();
        assert_eq!(sent.types()[0], PacketType::METADATA);
        assert_eq!(numbers, [0, 2]);
    }

    #[test]
    fn a_rollover_records_the_new_segment_before_its_first_sample() {
        let mut streams = started();
        let mut sent = Sent::default();
        (0..10).for_each(|_| streams[0].push(&[0; 16], &mut sent));
        streams[0].rollover();
        (0..11).for_each(|_| streams[0].push(&[0; 16], &mut sent));
        streams[0].flush(&mut sent);

        assert_eq!(
            sent.types(),
            [
                PacketType::METADATA,
                stream_packet(1),
                PacketType::METADATA,
                stream_packet(1),
            ]
        );
        let (view, _) = PacketView::parse_prefix(&sent.0[3]).unwrap();
        let samples = data::Samples::parse(view.header, view.payload).unwrap();
        assert_eq!(samples.segment_id.value(), 1);
        assert_eq!(samples.first.value(), 0);
        assert_eq!(streams[0].current().timeref().start_time, 1001);
    }

    /// D8: no segment spans a change of traceability, and the one begun
    /// without it is flagged.
    #[test]
    fn a_holdover_rollover_flags_the_segment_it_opens() {
        let mut streams = started();
        let mut sent = Sent::default();
        (0..10).for_each(|_| streams[0].push(&[0; 16], &mut sent));
        streams[0].set_holdover(true);
        streams[0].rollover();
        (0..11).for_each(|_| streams[0].push(&[0; 16], &mut sent));

        assert_eq!(streams[0].segment(0).unwrap().flags, SegmentFlags::VALID);
        assert_eq!(
            streams[0].segment(1).unwrap().flags,
            SegmentFlags::VALID | SegmentFlags::ACTIVE | SegmentFlags::HOLDOVER
        );
        assert_eq!(streams[0].current().timeref().start_time, 1001);
    }

    #[test]
    fn a_slice_of_streams_describes_a_device() {
        let mut streams = started();
        streams[0].push(&[0; 16], &mut Sent::default());
        let streams = &streams[..];

        assert_eq!(streams.ids().collect::<Vec<u8>>(), [1, 2]);
        let record = streams.stream(1).unwrap();
        assert_eq!(record.n_columns, 2);
        assert_eq!(record.n_segments, 4);
        assert_eq!(record.sample_size, 16);
        assert_eq!(record.buf_samples, 0);
        assert_eq!(record.name, "sine");

        let held = |stream_id, index| {
            streams.with_segment(stream_id, index, |record| {
                (record.segment_id.value(), record.flags)
            })
        };
        assert_eq!(
            held(1, CURRENT_SEGMENT),
            Some((0, SegmentFlags::VALID | SegmentFlags::ACTIVE))
        );
        assert_eq!(held(1, 0), held(1, CURRENT_SEGMENT));
        assert_eq!(held(1, 1), None);
        assert_eq!(held(1, 4), None);
        assert_eq!(held(9, CURRENT_SEGMENT), None);

        assert_eq!(streams.column(2, 0).unwrap().name, "status");
        assert_eq!(streams.column(2, 1), None);
        assert_eq!(streams.stream(9), None);
    }

    #[test]
    fn a_bootstrap_sweep_reaches_every_stream() {
        let streams = started();
        let mut out = crate::rpc::Reply::new();
        let device = data::Device {
            session: SessionId::new(3),
            n_streams: 2,
            name: "d",
            serial: "S1",
            firmware: "fw",
        };
        crate::metadata::reply(device, &streams[..], &[], &mut out).unwrap();
        let kinds: Vec<MetadataType> = data::MetadataReply::parse(&out)
            .unwrap()
            .map(|(kind, _)| kind)
            .collect();
        use MetadataType::*;
        assert_eq!(
            kinds,
            [Device, Stream, Segment, Column, Column, Stream, Segment, Column]
        );
    }

    #[test]
    fn a_stopped_stream_keeps_its_segment_readable() {
        let mut streams = started();
        streams[0].push(&[0; 16], &mut Sent::default());
        streams[0].stop();
        assert_eq!(streams[0].current().state(), SegmentState::Next);
        assert_eq!(streams[0].segment(0).unwrap().segment_id.value(), 0);
        assert_eq!(streams[0].segment(0).unwrap().flags, SegmentFlags::VALID);
    }
}

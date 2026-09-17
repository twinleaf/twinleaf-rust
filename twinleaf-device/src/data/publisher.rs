//! Sample packets: the one packet a stream is filling, and what closes it.
//!
//! A packet carries consecutive samples of one segment, so a new segment, a
//! gap in sample numbers, a full packet, or a segment's last sample all send
//! what is staged. The record of a segment goes out before its first sample.

use twinleaf_proto::data::{self, Metadata, MetadataFlags, SAMPLE_HEADER_SIZE};
use twinleaf_proto::packet::Packet;
use twinleaf_proto::{SampleNumber, SegmentId, StreamId};

use super::segments::Issued;
use crate::Sink;

/// Sample bytes one packet carries.
pub const MAX_SAMPLE_BYTES: usize = Packet::MAX_PAYLOAD - SAMPLE_HEADER_SIZE;

/// The packet a stream is filling.
pub struct Publisher {
    buf: [u8; Packet::MAX_SIZE],
    open: Option<Open>,
    capacity: usize,
}

/// What the staged packet holds so far.
#[derive(Clone, Copy)]
struct Open {
    stream: StreamId,
    segment: SegmentId,
    first: SampleNumber,
    samples: u32,
    len: usize,
}

impl Publisher {
    /// A publisher with nothing staged, closing a packet when it is full.
    pub const fn new() -> Self {
        Self::with_capacity(MAX_SAMPLE_BYTES)
    }

    /// A publisher closing a packet once it holds `capacity` sample bytes; a
    /// packet always takes at least one sample.
    pub const fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: [0; Packet::MAX_SIZE],
            open: None,
            capacity: if capacity < MAX_SAMPLE_BYTES {
                capacity
            } else {
                MAX_SAMPLE_BYTES
            },
        }
    }

    /// Sample bytes in the packet being filled.
    pub fn staged(&self) -> usize {
        self.open.map_or(0, |open| open.len)
    }

    /// Stage one issued sample, sending packets as they close. A sample too
    /// large for a packet is never published.
    pub fn push(
        &mut self,
        issued: Issued,
        segment: data::Segment<'_>,
        sample: &[u8],
        out: &mut impl Sink,
    ) {
        if let Some(number) = issued.output {
            self.stage(issued, segment, number, sample, out);
        }
        if issued.last {
            self.flush(out);
        }
    }

    /// Send the staged packet, if there is one.
    pub fn flush(&mut self, out: &mut impl Sink) {
        let Some(open) = self.open.take() else {
            return;
        };
        let len = data::Samples::write_header(
            &mut self.buf,
            open.stream,
            open.segment,
            open.first,
            open.len,
        )
        .expect("a segment rolls over before its sample numbers leave the packet field");
        out.send(&self.buf[..len]);
    }

    fn stage(
        &mut self,
        issued: Issued,
        segment: data::Segment<'_>,
        number: SampleNumber,
        sample: &[u8],
        out: &mut impl Sink,
    ) {
        if sample.is_empty() || sample.len() > MAX_SAMPLE_BYTES {
            return;
        }
        if !self.takes(segment.stream_id, issued.segment, number, sample.len()) {
            self.flush(out);
        }
        if issued.first {
            record(segment, out);
        }
        self.write(segment.stream_id, issued.segment, number, sample);
    }

    /// Whether the staged packet ends right before `number` of the same run
    /// and still has room for it.
    fn takes(
        &self,
        stream: StreamId,
        segment: SegmentId,
        number: SampleNumber,
        bytes: usize,
    ) -> bool {
        self.open.is_some_and(|open| {
            open.stream == stream
                && open.segment == segment
                && open.first.value() + open.samples == number.value()
                && open.len + bytes <= self.capacity
        })
    }

    fn write(&mut self, stream: StreamId, segment: SegmentId, number: SampleNumber, sample: &[u8]) {
        let open = self.open.get_or_insert(Open {
            stream,
            segment,
            first: number,
            samples: 0,
            len: 0,
        });
        let start = data::Samples::DATA_OFFSET + open.len;
        self.buf[start..start + sample.len()].copy_from_slice(sample);
        open.samples += 1;
        open.len += sample.len();
    }
}

/// Nothing staged.
impl Default for Publisher {
    fn default() -> Self {
        Self::new()
    }
}

/// Send a segment's record, which its first sample follows.
fn record(segment: data::Segment<'_>, out: &mut impl Sink) {
    let mut buf = [0u8; Packet::MAX_SIZE];
    let flags = MetadataFlags::UPDATE | MetadataFlags::LAST;
    let len = Metadata::Segment(segment)
        .write(flags, &mut buf)
        .expect("a segment record fits a packet");
    out.send(&buf[..len]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf_proto::data::{FilterType, MetadataType, SegmentFlags};
    use twinleaf_proto::packet::{PacketType, PacketView};
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::SessionId;

    const STREAM: StreamId = StreamId::new(1);

    #[derive(Default)]
    struct Sent(Vec<Vec<u8>>);

    impl Sink for Sent {
        fn send(&mut self, packet: &[u8]) {
            self.0.push(packet.to_vec());
        }
    }

    impl Sent {
        /// Every packet as either a segment record or the samples it carries.
        fn packets(&self) -> Vec<Result<MetadataType, (u8, u32, usize)>> {
            self.0
                .iter()
                .map(|packet| {
                    let (view, _) = PacketView::parse_prefix(packet).unwrap();
                    match view.header.ptype {
                        PacketType::METADATA => {
                            Ok(data::split_metadata(view.payload).unwrap().0.into())
                        }
                        _ => {
                            let samples = data::Samples::parse(view.header, view.payload).unwrap();
                            Err((
                                samples.segment_id.value(),
                                samples.first.value(),
                                samples.data.len(),
                            ))
                        }
                    }
                })
                .collect()
        }
    }

    fn segment(segment_id: u8) -> data::Segment<'static> {
        data::Segment {
            stream_id: STREAM,
            segment_id: SegmentId::new(segment_id),
            flags: SegmentFlags::VALID | SegmentFlags::ACTIVE,
            epoch: Epoch::UNIX,
            timeref_serial: "S1",
            timeref_session: SessionId::new(3),
            start_time: 1000,
            sampling_rate: 10,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: FilterType::NONE,
        }
    }

    fn issued(segment_id: u8, output: u32, first: bool, last: bool) -> Issued {
        Issued {
            segment: SegmentId::new(segment_id),
            output: Some(SampleNumber::new(output)),
            first,
            last,
        }
    }

    fn push(publisher: &mut Publisher, issued: Issued, out: &mut Sent) {
        publisher.push(issued, segment(issued.segment.value()), &[7; 4], out);
    }

    #[test]
    fn a_segment_record_precedes_its_first_sample() {
        let mut publisher = Publisher::new();
        let mut sent = Sent::default();
        push(&mut publisher, issued(0, 0, true, false), &mut sent);
        push(&mut publisher, issued(0, 1, false, false), &mut sent);
        publisher.flush(&mut sent);
        assert_eq!(sent.packets(), [Ok(MetadataType::Segment), Err((0, 0, 8))]);
    }

    #[test]
    fn a_packet_never_spans_two_segments() {
        let mut publisher = Publisher::new();
        let mut sent = Sent::default();
        push(&mut publisher, issued(0, 0, true, false), &mut sent);
        push(&mut publisher, issued(1, 0, true, false), &mut sent);
        publisher.flush(&mut sent);
        assert_eq!(
            sent.packets(),
            [
                Ok(MetadataType::Segment),
                Err((0, 0, 4)),
                Ok(MetadataType::Segment),
                Err((1, 0, 4)),
            ]
        );
    }

    #[test]
    fn a_gap_in_sample_numbers_starts_a_new_packet() {
        let mut publisher = Publisher::new();
        let mut sent = Sent::default();
        push(&mut publisher, issued(0, 0, true, false), &mut sent);
        push(&mut publisher, issued(0, 2, false, false), &mut sent);
        publisher.flush(&mut sent);
        assert_eq!(
            sent.packets(),
            [Ok(MetadataType::Segment), Err((0, 0, 4)), Err((0, 2, 4))]
        );
    }

    #[test]
    fn a_full_packet_goes_out_without_a_flush() {
        let mut publisher = Publisher::new();
        let mut sent = Sent::default();
        let per_packet = (MAX_SAMPLE_BYTES / 4) as u32;
        (0..per_packet + 1).for_each(|number| {
            push(
                &mut publisher,
                issued(0, number, number == 0, false),
                &mut sent,
            );
        });
        assert_eq!(
            sent.packets(),
            [
                Ok(MetadataType::Segment),
                Err((0, 0, usize::try_from(per_packet).unwrap() * 4)),
            ]
        );
    }

    #[test]
    fn a_packet_closes_at_the_capacity_chosen() {
        let mut publisher = Publisher::with_capacity(8);
        let mut sent = Sent::default();
        (0..3).for_each(|number| {
            push(
                &mut publisher,
                issued(0, number, number == 0, false),
                &mut sent,
            )
        });
        assert_eq!(publisher.staged(), 4);
        assert_eq!(sent.packets(), [Ok(MetadataType::Segment), Err((0, 0, 8))]);
    }

    #[test]
    fn the_last_sample_of_a_segment_flushes() {
        let mut publisher = Publisher::new();
        let mut sent = Sent::default();
        push(&mut publisher, issued(0, 0, true, false), &mut sent);
        push(&mut publisher, issued(0, 1, false, true), &mut sent);
        assert_eq!(sent.packets(), [Ok(MetadataType::Segment), Err((0, 0, 8))]);

        let mut sent = Sent::default();
        push(&mut publisher, issued(0, 2, false, false), &mut sent);
        publisher.push(
            Issued {
                segment: SegmentId::new(0),
                output: None,
                first: false,
                last: true,
            },
            segment(0),
            &[7; 4],
            &mut sent,
        );
        assert_eq!(sent.packets(), [Err((0, 2, 4))]);
    }

    #[test]
    fn an_empty_flush_sends_nothing() {
        let mut publisher = Publisher::new();
        let mut sent = Sent::default();
        publisher.flush(&mut sent);
        publisher.push(
            Issued {
                segment: SegmentId::new(0),
                output: None,
                first: false,
                last: true,
            },
            segment(0),
            &[7; 4],
            &mut sent,
        );
        assert!(sent.0.is_empty());
    }

    #[test]
    fn a_sample_too_large_for_a_packet_is_never_published() {
        let mut publisher = Publisher::new();
        let mut sent = Sent::default();
        let sample = [0u8; MAX_SAMPLE_BYTES + 1];
        publisher.push(issued(0, 0, true, false), segment(0), &sample, &mut sent);
        publisher.push(issued(0, 1, true, false), segment(0), &[], &mut sent);
        publisher.flush(&mut sent);
        assert!(sent.0.is_empty());
    }
}

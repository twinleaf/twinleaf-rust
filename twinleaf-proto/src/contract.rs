//! The laws a conforming device's packets obey on the wire, and [`check`],
//! which finds the packets of a trace that break them.
//!
//! A trace is every packet a host exchanged with a tree of devices since it
//! first heard them, in the order it saw them. A row is a sample packet, and
//! a segment is a segment id with the `start_time` and time reference of its
//! stream's latest record of it. Each route is checked alone, and starts over at a heartbeat
//! naming a new session. A packet whose payload does not parse is not checked.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::num::NonZeroU16;

use crate::data::{self, Metadata, Samples};
use crate::heartbeat::{Heartbeat, SESSION_PAYLOAD_SIZE, SETTINGS_PAYLOAD_SIZE};
use crate::packet::{Header, PacketType, PacketView};
use crate::rpc::{ErrorReply, Method, Reply, Request};
use crate::settings::Setting;
use crate::sync::Epoch;
use crate::{DeviceRoute, RpcRequestId, SegmentId, SessionId, StreamId};

/// Which end of the link sent a packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sender {
    /// The device the route names, or a hub forwarding its packet.
    Device,
    /// The host, asking the device the route names.
    Host,
}

/// One packet of a trace.
#[derive(Debug, Clone, Copy)]
pub struct Observed<'a> {
    /// When the host saw it, in nanoseconds.
    pub arrival_ns: u64,
    /// The device it came from or went to.
    pub route: DeviceRoute,
    /// Which end sent it.
    pub sender: Sender,
    /// The packet.
    pub packet: PacketView<'a>,
}

/// A law every conforming device obeys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Law {
    /// Every packet a device sends has a TTL of 0.
    F1,
    /// Every heartbeat a device sends is its session, alone in 4 bytes or with
    /// its settings sequence in 8.
    I1,
    /// A segment's first row comes while its stream's latest record is that
    /// segment's.
    S1,
    /// A segment's later rows come only while its stream's latest record is
    /// that segment's, so a record never overtakes rows written before it.
    S2,
    /// Rows of one segment with no row of another between them have strictly
    /// increasing sample numbers.
    S3,
    /// A segment's rows never number back after a row of another segment of
    /// its stream, so no second acquisition reuses its id, start and reference.
    S4,
    /// A record that moves a segment id's start or reference between two of its rows, with no
    /// record of another id between them, restarts the count, so the next row numbers no higher.
    S5,
    /// After a host asks `dev.stop` or `dev.restart` by name, `dev.running` 0
    /// is announced before the first record of a segment not recorded before.
    R1,
    /// An announcement numbered at most the settings sequence the device had
    /// shown before a request never comes after that request's reply.
    A1,
}

/// A packet of a trace that breaks a law.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The law it breaks.
    pub law: Law,
    /// Its place in the trace.
    pub index: usize,
    /// What the packet is.
    pub detail: String,
}

/// Every packet of `trace` that breaks a law, in trace order.
pub fn check(trace: &[Observed<'_>]) -> Vec<Violation> {
    let mut devices = BTreeMap::<DeviceRoute, Device>::new();
    trace
        .iter()
        .enumerate()
        .flat_map(|(index, seen)| {
            let device = devices.entry(seen.route).or_default();
            let broken = match seen.sender {
                Sender::Host => {
                    device.asked(seen.packet);
                    [None, None]
                }
                Sender::Device => [
                    (!f1(seen.packet.header)).then(|| {
                        (
                            Law::F1,
                            format!("a packet of TTL {}", seen.packet.header.ttl),
                        )
                    }),
                    device.heard(seen.packet),
                ],
            };
            broken
                .into_iter()
                .flatten()
                .map(move |(law, detail)| Violation { law, index, detail })
        })
        .collect()
}

fn f1(header: Header) -> bool {
    header.ttl == 0
}

fn i1(payload: &[u8]) -> bool {
    matches!(payload.len(), SESSION_PAYLOAD_SIZE | SETTINGS_PAYLOAD_SIZE)
}

fn s1(stream: &Stream, row: Row) -> bool {
    let Some(segment) = stream.segment(row) else {
        return false;
    };
    stream.last(segment).is_some() || stream.latest.as_ref() == Some(segment)
}

fn s2(stream: &Stream, row: Row) -> bool {
    let Some(segment) = stream.segment(row) else {
        return true;
    };
    stream.last(segment).is_none() || stream.latest.as_ref() == Some(segment)
}

fn s3(stream: &Stream, row: Row) -> bool {
    let Some(segment) = stream.segment(row) else {
        return true;
    };
    let Some(last) = stream.last(segment) else {
        return true;
    };
    stream.latest.as_ref() != Some(segment)
        || stream.last_row.as_ref() != Some(segment)
        || row.first > last
}

fn s4(stream: &Stream, row: Row) -> bool {
    let Some(segment) = stream.segment(row) else {
        return true;
    };
    let Some(last) = stream.last(segment) else {
        return true;
    };
    stream.latest.as_ref() != Some(segment)
        || stream.last_row.as_ref() == Some(segment)
        || row.first > last
}

fn s5(stream: &Stream, row: Row) -> bool {
    let Some(segment) = stream.segment(row) else {
        return true;
    };
    let Some(before) = stream
        .last_row
        .as_ref()
        .filter(|before| !stream.crossed && before.id == segment.id && *before != segment)
    else {
        return true;
    };
    let Some(last) = stream.last(before) else {
        return true;
    };
    stream.last(segment).is_some() || stream.latest.as_ref() != Some(segment) || row.first <= last
}

fn r1(stop: Option<Stop>) -> bool {
    stop.is_none_or(|stop| stop.announced)
}

fn a1(answered: Option<u32>, seq: u32) -> bool {
    answered.is_none_or(|answered| seq > answered)
}

/// A segment as a row names it, its id with the start and time reference of its latest record.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Segment {
    id: SegmentId,
    start_time: u32,
    epoch: Epoch,
    timeref_session: SessionId,
    timeref_serial: String,
}

/// One row, its segment id and the sample numbers of its first and last samples.
#[derive(Clone, Copy)]
struct Row {
    segment: SegmentId,
    first: u32,
    last: u32,
}

/// What a stream's records and rows have said so far.
#[derive(Default)]
struct Stream {
    sample_size: Option<NonZeroU16>,
    records: BTreeMap<SegmentId, Segment>,
    latest: Option<Segment>,
    /// Every segment recorded, with the last sample number of its rows.
    runs: BTreeMap<Segment, Option<u32>>,
    last_row: Option<Segment>,
    /// Whether a record of another segment id came since the last row.
    crossed: bool,
}

impl Stream {
    fn segment(&self, row: Row) -> Option<&Segment> {
        self.records.get(&row.segment)
    }

    fn last(&self, segment: &Segment) -> Option<u32> {
        self.runs.get(segment).copied().flatten()
    }
}

/// A `dev.stop` or `dev.restart` asked, and whether `dev.running` 0 has been announced since.
#[derive(Clone, Copy)]
struct Stop {
    id: RpcRequestId,
    announced: bool,
}

/// What one route has said so far.
#[derive(Default)]
struct Device {
    session: Option<SessionId>,
    streams: BTreeMap<StreamId, Stream>,
    stop: Option<Stop>,
    /// The highest settings sequence the device has shown.
    shown: Option<u32>,
    /// Requests not yet answered, with the sequence shown before each.
    asked: BTreeMap<RpcRequestId, Option<u32>>,
    /// The highest sequence shown before a request that has been replied.
    answered: Option<u32>,
}

impl Device {
    fn asked(&mut self, packet: PacketView<'_>) {
        if packet.header.ptype != PacketType::RPC_REQ {
            return;
        }
        let Some(request) = Request::parse(packet.payload) else {
            return;
        };
        self.asked.insert(request.id, self.shown);
        if let Method::ByName(b"dev.stop" | b"dev.restart") = request.method {
            self.stop = Some(Stop {
                id: request.id,
                announced: false,
            });
        }
    }

    fn heard(&mut self, packet: PacketView<'_>) -> Option<(Law, String)> {
        match packet.header.ptype {
            PacketType::HEARTBEAT => {
                if let Some(Heartbeat::Session { session, settings }) =
                    Heartbeat::parse(packet.payload)
                {
                    self.beat(session, settings);
                }
                (!i1(packet.payload)).then(|| {
                    (
                        Law::I1,
                        format!("a heartbeat of {} bytes", packet.payload.len()),
                    )
                })
            }
            PacketType::SETTING => self.announced(Setting::parse(packet.payload)?),
            PacketType::METADATA => match Metadata::parse(packet.payload)?.0 {
                Metadata::Segment(record) => self.recorded(record),
                Metadata::Stream(record) => {
                    self.streams
                        .entry(record.stream_id)
                        .or_default()
                        .sample_size = NonZeroU16::new(record.sample_size);
                    None
                }
                Metadata::Device(_) | Metadata::Column(_) => None,
            },
            PacketType::RPC_REP => {
                let reply = Reply::parse(packet.payload)?;
                let shown = self.asked.remove(&reply.req_id)?;
                self.answered = self.answered.max(shown);
                None
            }
            PacketType::RPC_ERROR => {
                let error = ErrorReply::parse(packet.payload)?;
                self.asked.remove(&error.req_id);
                self.stop = self.stop.filter(|stop| stop.id != error.req_id);
                None
            }
            _ => self.row(Samples::parse(packet.header, packet.payload)?),
        }
    }

    fn beat(&mut self, session: SessionId, settings: Option<u32>) {
        match self.session {
            Some(known) if known != session => *self = Self::default(),
            Some(_) | None => {}
        }
        self.session = Some(session);
        self.shown = self.shown.max(settings);
    }

    fn announced(&mut self, setting: Setting<'_>) -> Option<(Law, String)> {
        if let (b"dev.running", [0], Some(stop)) = (setting.name, setting.reply, &mut self.stop) {
            stop.announced = true;
        }
        let seq = setting.seq?;
        let broken = !a1(self.answered, seq);
        self.shown = self.shown.max(Some(seq));
        broken.then(|| {
            let name = String::from_utf8_lossy(setting.name);
            (Law::A1, format!("{name} announced as setting {seq}"))
        })
    }

    fn recorded(&mut self, record: data::Segment<'_>) -> Option<(Law, String)> {
        let segment = Segment {
            id: record.segment_id,
            start_time: record.start_time,
            epoch: record.epoch,
            timeref_session: record.timeref_session,
            timeref_serial: record.timeref_serial.into(),
        };
        let stream = self.streams.entry(record.stream_id).or_default();
        stream.crossed |= stream
            .last_row
            .as_ref()
            .is_some_and(|last| last.id != segment.id);
        stream.records.insert(segment.id, segment.clone());
        stream.latest = Some(segment.clone());
        if stream.runs.contains_key(&segment) {
            return None;
        }
        stream.runs.insert(segment, None);
        (!r1(self.stop.take())).then(|| {
            let (stream, id, start) = (
                record.stream_id.value(),
                record.segment_id.value(),
                record.start_time,
            );
            (
                Law::R1,
                format!("the record of stream {stream} segment {id} from {start}"),
            )
        })
    }

    fn row(&mut self, samples: Samples<'_>) -> Option<(Law, String)> {
        let stream = self.streams.entry(samples.stream_id).or_default();
        let count = stream
            .sample_size
            .map_or(1, |size| samples.data.len() / usize::from(size.get()));
        let first = samples.first.value();
        let row = Row {
            segment: samples.segment_id,
            first,
            last: first + count.saturating_sub(1) as u32,
        };
        let laws = [
            (Law::S1, s1 as fn(&Stream, Row) -> bool),
            (Law::S2, s2),
            (Law::S3, s3),
            (Law::S4, s4),
            (Law::S5, s5),
        ];
        let broken = laws.into_iter().find(|(_, holds)| !holds(stream, row));
        stream.last_row = stream.segment(row).cloned();
        stream.crossed = false;
        if let Some(segment) = &stream.last_row {
            stream.runs.insert(segment.clone(), Some(row.last));
        }
        broken.map(|(law, _)| {
            let (stream, id) = (samples.stream_id.value(), row.segment.value());
            (
                law,
                format!("stream {stream} segment {id} rows {first}..={}", row.last),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{FilterType, MetadataFlags, SegmentFlags};
    use crate::packet::Packet;
    use crate::rpc::{self, RpcError};
    use crate::sync::Epoch;
    use crate::SampleNumber;

    /// A packet the device at `/` sent or was sent.
    type Step = (DeviceRoute, Sender, Vec<u8>);

    fn written(write: impl FnOnce(&mut [u8]) -> Option<usize>) -> Vec<u8> {
        let mut buf = [0; Packet::MAX_SIZE];
        let len = write(&mut buf).unwrap();
        buf[..len].to_vec()
    }

    fn device(packet: Vec<u8>) -> Step {
        (DeviceRoute::root(), Sender::Device, packet)
    }

    fn beat(payload: &[u8]) -> Step {
        device(written(|buf| Heartbeat::Any(payload).write(buf)))
    }

    fn session(session: u32, settings: u32) -> Step {
        let beat = Heartbeat::Session {
            session: SessionId::new(session),
            settings: Some(settings),
        };
        device(written(|buf| beat.write(buf)))
    }

    fn record(segment: u8, start_time: u32) -> Step {
        record_on(segment, start_time, 3)
    }

    /// A segment's record on the time reference of session `reference`.
    fn record_on(segment: u8, start_time: u32, reference: u32) -> Step {
        let record = Metadata::Segment(data::Segment {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new(segment),
            flags: SegmentFlags::VALID | SegmentFlags::ACTIVE,
            epoch: Epoch::UNIX,
            timeref_serial: "S1",
            timeref_session: SessionId::new(reference),
            start_time,
            sampling_rate: 10,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: FilterType::NONE,
        });
        device(written(|buf| record.write(MetadataFlags::UPDATE, buf)))
    }

    /// The record of stream 1, two bytes a sample, so that each row carries two.
    fn stream() -> Step {
        let record = Metadata::Stream(data::Stream {
            stream_id: StreamId::new(1),
            n_columns: 1,
            n_segments: 4,
            sample_size: 2,
            buf_samples: 0,
            name: "s",
        });
        device(written(|buf| record.write(MetadataFlags::UPDATE, buf)))
    }

    fn row(segment: u8, first: u32) -> Step {
        let samples = Samples {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new(segment),
            first: SampleNumber::new(first),
            data: &[0; 4],
        };
        device(written(|buf| samples.write(buf)))
    }

    fn announce(name: &str, seq: u32, value: &[u8]) -> Step {
        let setting = Setting {
            name: name.as_bytes(),
            seq: Some(seq),
            reply: value,
        };
        device(written(|buf| setting.write(buf)))
    }

    fn ask(id: u16, name: &str) -> Step {
        let method = Method::ByName(name.as_bytes());
        let packet = written(|buf| rpc::write_request(buf, RpcRequestId::new(id), method, &[]));
        (DeviceRoute::root(), Sender::Host, packet)
    }

    fn reply(id: u16) -> Step {
        device(written(|buf| {
            rpc::write_reply(buf, RpcRequestId::new(id), &[])
        }))
    }

    fn refuse(id: u16) -> Step {
        device(written(|buf| {
            rpc::write_error(buf, RpcRequestId::new(id), RpcError::State)
        }))
    }

    fn at(route: &str, (_, sender, packet): Step) -> Step {
        (route.parse().unwrap(), sender, packet)
    }

    fn ttl(ttl: u8, (route, sender, mut packet): Step) -> Step {
        packet[1] |= ttl << 4;
        (route, sender, packet)
    }

    /// The laws `steps` break, each with the place of the packet that breaks it.
    fn broken(steps: &[Step]) -> Vec<(Law, usize)> {
        let trace: Vec<Observed<'_>> = steps
            .iter()
            .map(|(route, sender, packet)| Observed {
                arrival_ns: 0,
                route: *route,
                sender: *sender,
                packet: PacketView::parse_prefix(packet).unwrap().0,
            })
            .collect();
        check(&trace)
            .into_iter()
            .map(|violation| (violation.law, violation.index))
            .collect()
    }

    /// Each law holds of a trace that keeps it and names the one packet of a
    /// trace that breaks it, and no other law fires on either.
    #[test]
    fn each_law_fires_on_the_packet_that_breaks_it_and_only_there() {
        let cases: Vec<(Law, Vec<Step>, Vec<Step>, usize)> = vec![
            (Law::F1, vec![session(1, 0)], vec![ttl(1, session(1, 0))], 0),
            (
                Law::I1,
                vec![
                    (DeviceRoute::root(), Sender::Host, beat(&[]).2),
                    beat(&[0; 4]),
                    beat(&[0; 8]),
                ],
                vec![beat(&[0; 4]), beat(&[])],
                1,
            ),
            (
                Law::S1,
                vec![session(1, 0), record(0, 100), session(1, 0), row(0, 0)],
                vec![row(0, 0)],
                0,
            ),
            (
                Law::S1,
                vec![record(0, 100), at("/1", record(0, 100)), row(0, 0)],
                vec![record(0, 100), at("/1", row(0, 0))],
                1,
            ),
            (
                Law::S1,
                vec![record(0, 100), record(1, 101), record(0, 100), row(0, 0)],
                vec![record(0, 100), record(1, 101), row(0, 0)],
                2,
            ),
            (
                Law::S2,
                vec![
                    record(0, 100),
                    row(0, 0),
                    row(0, 1),
                    record(1, 101),
                    row(1, 0),
                ],
                vec![record(0, 100), row(0, 0), record(1, 101), row(0, 1)],
                3,
            ),
            (
                Law::S3,
                vec![stream(), record(0, 100), row(0, 0), row(0, 2)],
                vec![stream(), record(0, 100), row(0, 0), row(0, 1)],
                3,
            ),
            (
                Law::S4,
                vec![
                    session(1, 0),
                    record(0, 100),
                    row(0, 0),
                    record(1, 101),
                    row(1, 0),
                    record(0, 102),
                    row(0, 0),
                    record(1, 101),
                    row(1, 1),
                    session(2, 0),
                    record(0, 100),
                    row(0, 0),
                ],
                vec![
                    record(0, 100),
                    row(0, 5),
                    record(1, 101),
                    row(1, 0),
                    record(0, 100),
                    row(0, 5),
                ],
                5,
            ),
            (
                Law::S4,
                vec![
                    record(0, 100),
                    row(0, 5),
                    record(1, 101),
                    row(1, 0),
                    record_on(0, 100, 4),
                    row(0, 0),
                ],
                vec![
                    record(0, 100),
                    row(0, 5),
                    record(1, 101),
                    row(1, 0),
                    record(0, 100),
                    row(0, 0),
                ],
                5,
            ),
            (
                Law::S5,
                vec![
                    record(0, 100),
                    row(0, 4),
                    record(0, 105),
                    row(0, 4),
                    record(0, 100),
                    row(0, 9),
                    record(1, 101),
                    row(1, 12),
                ],
                vec![record(0, 100), row(0, 4), record(0, 105), row(0, 5)],
                3,
            ),
            (
                Law::S5,
                vec![
                    record(0, 100),
                    row(0, 4),
                    record(1, 101),
                    record(0, 105),
                    row(0, 5),
                ],
                vec![
                    record(0, 100),
                    row(0, 4),
                    record(0, 101),
                    record(0, 105),
                    row(0, 5),
                ],
                4,
            ),
            (
                Law::R1,
                vec![
                    ask(1, "dev.stop"),
                    announce("dev.running", 1, &[0]),
                    reply(1),
                    record(0, 100),
                    ask(2, "dev.stop"),
                    refuse(2),
                    record(1, 101),
                ],
                vec![
                    ask(1, "dev.restart"),
                    announce("dev.running", 1, &[1]),
                    reply(1),
                    record(0, 100),
                ],
                3,
            ),
            (
                Law::A1,
                vec![
                    announce("dev.name", 1, b"a"),
                    session(1, 1),
                    ask(1, "dev.name"),
                    reply(1),
                    announce("dev.name", 2, b"b"),
                ],
                vec![
                    session(1, 2),
                    ask(1, "dev.name"),
                    reply(1),
                    announce("dev.name", 2, b"b"),
                ],
                3,
            ),
        ];
        for (law, lawful, breaking, index) in cases {
            assert_eq!(
                broken(&lawful),
                [],
                "{law:?} holds of a trace that keeps it"
            );
            assert_eq!(
                broken(&breaking),
                [(law, index)],
                "{law:?} fires where it is broken"
            );
        }
    }
}

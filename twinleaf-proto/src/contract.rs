//! The laws a conforming device's packets obey on the wire, and [`check`],
//! which finds the packets of a trace that break them.
//!
//! A device sends each packet on the lane [`lane_of`] names. Each lane keeps its write order and
//! lanes keep none between them, so each law reads one lane of one route, its control or one
//! stream's data, and takes from control only what a device fixes for a session, the sample size
//! of a stream's record and the stream count of its device record.
//!
//! A trace is every packet a host exchanged with a tree of devices since it
//! first heard them, in the order it saw them. A row is a sample packet, and
//! a segment is a segment id with the `start_time` and time reference of its
//! stream's latest record of it. Until its first row a segment's record may
//! go out again with another start, reference or rate, as a change made
//! before the segment issues lands on it. A stream is checked from its first record, as a trace
//! may begin mid-stream, and a record without [`SegmentFlags::VALID`] is none. Each route is
//! checked alone. Its control starts over at its first heartbeat and at each naming a new session, as what came before may be of another, and each stream's
//! data at a [`SegmentFlags::START`] record opening another segment, as each begins its lane
//! afresh while the other lanes may still carry what came before. A device keeps the laws of the [`Law::revision`] its record declares and those before.
//! A packet whose payload does not parse is not checked, except against [`Law::F2`], which reads
//! only its header and payload length.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::num::NonZeroU16;

use crate::data::{self, FilterType, Metadata, Samples, SegmentFlags, MIN_SAMPLE_PAYLOAD};
use crate::heartbeat::{Heartbeat, SESSION_PAYLOAD_SIZE, SETTINGS_PAYLOAD_SIZE};
use crate::packet::{lane_of, Header, Lane, PacketType, PacketView};
use crate::rpc::{ErrorReply, Reply, Request};
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
    /// Every sample packet a device sends carries at least one sample byte.
    F2,
    /// Every heartbeat a device sends is its session, alone in 4 bytes or with
    /// its settings sequence in 8.
    I1,
    /// An announcement numbered at most the settings sequence the device had
    /// shown before a request never comes after that request's reply.
    A1,
    /// Each numbered announcement of a session numbers past the one before it, so a device opens
    /// a session with its heartbeat before it announces anything.
    A2,
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
    /// its stream, so no switch back reuses its id, start and reference.
    S4,
    /// A record that moves a segment id's start or reference between two of its rows, with no
    /// record of another id between them, restarts the count, so the next row numbers no higher.
    S5,
    /// A segment's first active record without [`SegmentFlags::START`], on the time reference of
    /// the segment its stream's last row was in, starts no earlier than that segment, whose rows
    /// number below the whole output periods between the two starts at its rate and decimation.
    S6,
    /// No row of a segment comes after its closing record, one without [`SegmentFlags::ACTIVE`].
    D1,
    /// A segment's closing record is its opening record with [`SegmentFlags::ACTIVE`] cleared.
    D2,
    /// A record opening a segment while its stream has none open carries [`SegmentFlags::START`],
    /// and one with it while a segment is open ends that segment unclosed.
    D3,
    /// Every stream a record or a row names is numbered at most the `n_streams` of the device's
    /// latest device record.
    M1,
}

impl Law {
    /// The protocol revision from which a device keeps this law, 0 for one C firmware keeps too.
    pub const fn revision(self) -> u8 {
        match self {
            Self::F1 | Self::I1 | Self::A1 | Self::A2 => 0,
            Self::F2
            | Self::S1
            | Self::S2
            | Self::S3
            | Self::S4
            | Self::S5
            | Self::S6
            | Self::D1
            | Self::D2
            | Self::D3
            | Self::M1 => 1,
        }
    }
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

/// Every packet of `trace` that breaks a law its device keeps, in trace order.
pub fn check(trace: &[Observed<'_>]) -> Vec<Violation> {
    let mut routes = BTreeMap::<DeviceRoute, Route>::new();
    trace
        .iter()
        .enumerate()
        .flat_map(|(index, seen)| {
            let route = routes.entry(seen.route).or_default();
            let broken = match seen.sender {
                Sender::Host => {
                    route.control.asked(seen.packet);
                    [None, None, None]
                }
                Sender::Device => [
                    (!f1(seen.packet.header)).then(|| {
                        (
                            Law::F1,
                            format!("a packet of TTL {}", seen.packet.header.ttl),
                        )
                    }),
                    (!f2(seen.packet.header)).then(|| {
                        (
                            Law::F2,
                            format!(
                                "a sample packet of {} payload bytes",
                                seen.packet.header.payload_size
                            ),
                        )
                    }),
                    route.heard(seen.packet),
                ],
            };
            let protocol = route.control.protocol;
            broken
                .into_iter()
                .flatten()
                .filter(move |(law, _)| law.revision() <= protocol)
                .map(move |(law, detail)| Violation { law, index, detail })
        })
        .collect()
}

fn f1(header: Header) -> bool {
    header.ttl == 0
}

fn f2(header: Header) -> bool {
    header.ptype.sample_stream().is_none() || usize::from(header.payload_size) >= MIN_SAMPLE_PAYLOAD
}

fn i1(payload: &[u8]) -> bool {
    matches!(payload.len(), SESSION_PAYLOAD_SIZE | SETTINGS_PAYLOAD_SIZE)
}

fn a1(answered: Option<u32>, seq: u32) -> bool {
    answered.is_none_or(|answered| seq > answered)
}

fn a2(numbered: Option<u32>, seq: u32) -> bool {
    numbered.is_none_or(|last| (seq.wrapping_sub(last) as i32) > 0)
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

fn d1(stream: &Stream, row: Row) -> bool {
    !stream.closed || stream.segment(row) != stream.latest.as_ref()
}

fn d2(stream: &Stream, record: &Record) -> bool {
    let opening = Record {
        flags: record.flags | SegmentFlags::ACTIVE,
        ..record.clone()
    };
    record.active()
        || stream
            .opened
            .as_ref()
            .is_none_or(|opened| *opened == opening)
}

fn d3(stream: &Stream, record: &Record) -> bool {
    stream.latest.is_none() || !record.active() || stream.open().is_some() || record.starts()
}

/// The rows of a segment with no rate or decimation number against nothing, so only the order of
/// the two starts is read.
fn s6(stream: &Stream, record: &Record) -> bool {
    let next = &record.segment;
    let Some(before) = stream.last_row.as_ref() else {
        return true;
    };
    if !record.active()
        || record.starts()
        || stream.runs.contains_key(next)
        || before.time_reference() != next.time_reference()
    {
        return true;
    }
    let Some(seconds) = next.start_time.checked_sub(before.start_time) else {
        return false;
    };
    let (Some(last), Some(&(rate, decimation))) = (stream.last(before), stream.periods.get(before))
    else {
        return true;
    };
    rate == 0
        || decimation == 0
        || u64::from(last) < u64::from(seconds) * u64::from(rate) / u64::from(decimation)
}

fn m1(n_streams: Option<u8>, stream: StreamId) -> bool {
    n_streams.is_none_or(|n_streams| stream.value() <= n_streams)
}

/// A segment as a row names it, its id with the start and time reference of its latest record.
/// Its fields are the ones [`data::SegmentView::is_same_segment`] compares.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Segment {
    id: SegmentId,
    start_time: u32,
    epoch: Epoch,
    timeref_session: SessionId,
    timeref_serial: String,
}

impl Segment {
    /// The segment `record` names.
    fn of(record: data::Segment<'_>) -> Self {
        Self {
            id: record.segment_id,
            start_time: record.start_time,
            epoch: record.epoch,
            timeref_session: record.timeref_session,
            timeref_serial: record.timeref_serial.into(),
        }
    }

    fn time_reference(&self) -> data::TimeReference<'_> {
        data::TimeReference {
            epoch: self.epoch,
            session: self.timeref_session,
            serial: &self.timeref_serial,
        }
    }
}

/// A segment record, the segment it names with everything else it says.
#[derive(Clone, PartialEq)]
struct Record {
    segment: Segment,
    flags: SegmentFlags,
    sampling_rate: u32,
    decimation: u32,
    filter_cutoff: f32,
    filter_type: FilterType,
}

impl Record {
    fn active(&self) -> bool {
        self.flags.contains(SegmentFlags::ACTIVE)
    }

    fn starts(&self) -> bool {
        self.flags.contains(SegmentFlags::START)
    }
}

/// One row, its segment id and the sample numbers of its first and last samples.
#[derive(Clone, Copy)]
struct Row {
    segment: SegmentId,
    first: u32,
    last: u32,
}

/// What a stream's segment records and rows have said so far, with the sample size its record set.
#[derive(Default)]
struct Stream {
    sample_size: Option<NonZeroU16>,
    records: BTreeMap<SegmentId, Segment>,
    latest: Option<Segment>,
    /// Every segment recorded, with the last sample number of its rows.
    runs: BTreeMap<Segment, Option<u32>>,
    /// Every segment recorded, with the sampling rate and decimation of its latest record.
    periods: BTreeMap<Segment, (u32, u32)>,
    last_row: Option<Segment>,
    /// Whether a record of another segment id came since the last row.
    crossed: bool,
    /// The latest record that keeps its segment active.
    opened: Option<Record>,
    /// Whether the latest record closed its segment.
    closed: bool,
}

impl Stream {
    fn open(&self) -> Option<&Record> {
        self.opened.as_ref().filter(|_| !self.closed)
    }

    fn segment(&self, row: Row) -> Option<&Segment> {
        self.records.get(&row.segment)
    }

    fn last(&self, segment: &Segment) -> Option<u32> {
        self.runs.get(segment).copied().flatten()
    }

    fn recorded(&mut self, record: data::Segment<'_>) -> Option<(Law, String)> {
        if !record.flags.contains(SegmentFlags::VALID) {
            return None;
        }
        let what = format!(
            "the record of stream {} segment {} from {}",
            record.stream_id.value(),
            record.segment_id.value(),
            record.start_time
        );
        let record = Record {
            segment: Segment::of(record),
            flags: record.flags,
            sampling_rate: record.sampling_rate,
            decimation: record.decimation,
            filter_cutoff: record.filter_cutoff,
            filter_type: record.filter_type,
        };
        if record.active()
            && record.starts()
            && self
                .open()
                .is_none_or(|open| open.segment != record.segment)
        {
            *self = Self {
                sample_size: self.sample_size,
                ..Self::default()
            };
        }
        let broken = [
            (Law::D2, d2(self, &record)),
            (Law::D3, d3(self, &record)),
            (Law::S6, s6(self, &record)),
        ]
        .into_iter()
        .find_map(|(law, holds)| (!holds).then_some(law));
        let segment = record.segment.clone();
        self.periods
            .insert(segment.clone(), (record.sampling_rate, record.decimation));
        self.crossed |= self
            .last_row
            .as_ref()
            .is_some_and(|last| last.id != segment.id);
        self.records.insert(segment.id, segment.clone());
        self.latest = Some(segment.clone());
        self.runs.entry(segment).or_insert(None);
        self.closed = !record.active();
        if record.active() {
            self.opened = Some(record);
        }
        broken.map(|law| (law, what))
    }

    fn row(&mut self, samples: Samples<'_>) -> Option<(Law, String)> {
        self.latest.as_ref()?;
        let count = self
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
            (Law::D1, d1),
        ];
        let broken = laws.into_iter().find(|(_, holds)| !holds(self, row));
        self.last_row = self.segment(row).cloned();
        self.crossed = false;
        if let Some(segment) = &self.last_row {
            self.runs.insert(segment.clone(), Some(row.last));
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

/// What one route has said on its control lane so far.
#[derive(Default)]
struct Control {
    session: Option<SessionId>,
    /// The protocol revision the device's record declares, 0 until it declares one.
    protocol: u8,
    /// The stream count of the device's latest record, unknown until it sends one.
    n_streams: Option<u8>,
    /// The highest settings sequence the device has shown.
    shown: Option<u32>,
    /// Requests not yet answered, with the sequence shown before each.
    asked: BTreeMap<RpcRequestId, Option<u32>>,
    /// The highest sequence shown before a request that has been replied.
    answered: Option<u32>,
    /// The number of the session's last numbered announcement.
    numbered: Option<u32>,
}

impl Control {
    fn asked(&mut self, packet: PacketView<'_>) {
        if packet.header.ptype != PacketType::RPC_REQ {
            return;
        }
        if let Some(request) = Request::parse(packet.payload) {
            self.asked.insert(request.id, self.shown);
        }
    }

    fn announced(&mut self, setting: Setting<'_>) -> Option<(Law, String)> {
        let seq = setting.seq?;
        let broken = [
            (Law::A2, a2(self.numbered, seq)),
            (Law::A1, a1(self.answered, seq)),
        ]
        .into_iter()
        .find_map(|(law, holds)| (!holds).then_some(law));
        self.numbered = Some(seq);
        self.shown = self.shown.max(Some(seq));
        broken.map(|law| {
            let name = String::from_utf8_lossy(setting.name);
            (law, format!("{name} announced as setting {seq}"))
        })
    }

    /// Whether a packet naming `stream` breaks [`Law::M1`].
    fn named(&self, stream: StreamId) -> Option<(Law, String)> {
        (!m1(self.n_streams, stream)).then(|| {
            let (stream, n_streams) = (stream.value(), self.n_streams.unwrap_or_default());
            (
                Law::M1,
                format!("stream {stream} of a device of {n_streams} streams"),
            )
        })
    }
}

/// What one route has said so far, on its control lane and on each stream's data.
#[derive(Default)]
struct Route {
    control: Control,
    streams: BTreeMap<StreamId, Stream>,
}

impl Route {
    fn heard(&mut self, packet: PacketView<'_>) -> Option<(Law, String)> {
        match lane_of(packet)? {
            Lane::Control => self.controlled(packet),
            Lane::Data => self.data(packet),
            Lane::Event => None,
        }
    }

    fn controlled(&mut self, packet: PacketView<'_>) -> Option<(Law, String)> {
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
            PacketType::SETTING => self.control.announced(Setting::parse(packet.payload)?),
            PacketType::METADATA => match Metadata::parse(packet.payload)?.0 {
                Metadata::Device(record) => {
                    self.control.protocol = record.protocol;
                    self.control.n_streams = Some(record.n_streams);
                    None
                }
                Metadata::Stream(record) => {
                    self.streams
                        .entry(record.stream_id)
                        .or_default()
                        .sample_size = NonZeroU16::new(record.sample_size);
                    self.control.named(record.stream_id)
                }
                Metadata::Column(record) => self.control.named(record.stream_id),
                Metadata::Segment(_) => None,
            },
            PacketType::RPC_REP => {
                let reply = Reply::parse(packet.payload)?;
                let shown = self.control.asked.remove(&reply.req_id)?;
                self.control.answered = self.control.answered.max(shown);
                None
            }
            PacketType::RPC_ERROR => {
                let error = ErrorReply::parse(packet.payload)?;
                self.control.asked.remove(&error.req_id);
                None
            }
            _ => None,
        }
    }

    fn data(&mut self, packet: PacketView<'_>) -> Option<(Law, String)> {
        match packet.header.ptype {
            PacketType::METADATA => match Metadata::parse(packet.payload)?.0 {
                Metadata::Segment(record) => {
                    let named = self.control.named(record.stream_id);
                    let broken = self
                        .streams
                        .entry(record.stream_id)
                        .or_default()
                        .recorded(record);
                    named.or(broken)
                }
                Metadata::Device(_) | Metadata::Stream(_) | Metadata::Column(_) => None,
            },
            _ => {
                let samples = Samples::parse(packet.header, packet.payload)?;
                let named = self.control.named(samples.stream_id);
                let broken = self
                    .streams
                    .entry(samples.stream_id)
                    .or_default()
                    .row(samples);
                named.or(broken)
            }
        }
    }

    fn beat(&mut self, session: SessionId, settings: Option<u32>) {
        match self.control.session {
            Some(known) if known != session => self.control = Control::default(),
            Some(_) => {}
            None => {
                self.control = Control {
                    protocol: self.control.protocol,
                    n_streams: self.control.n_streams,
                    ..Control::default()
                }
            }
        }
        self.control.session = Some(session);
        self.control.shown = self.control.shown.max(settings);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::MetadataFlags;
    use crate::packet::{Packet, WriteError};
    use crate::rpc::{self, Method};
    use crate::SampleNumber;

    /// A packet the device at `/` sent or was sent.
    type Step = (DeviceRoute, Sender, Vec<u8>);

    fn written(write: impl FnOnce(&mut [u8]) -> Result<usize, WriteError>) -> Vec<u8> {
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

    /// The device record of a device that keeps protocol revision `protocol`.
    fn declare(protocol: u8) -> Step {
        let record = Metadata::Device(data::Device {
            session: SessionId::new(1),
            n_streams: 1,
            name: "d",
            serial: "S",
            firmware: "f",
            protocol,
        });
        device(written(|buf| record.write(MetadataFlags::UPDATE, buf)))
    }

    /// A segment's record with `flags`, on the time reference of session `reference`.
    fn segment(segment: u8, start_time: u32, reference: u32, flags: SegmentFlags) -> Step {
        segment_in(Epoch::UNIX, segment, start_time, reference, flags)
    }

    /// A segment's record with `flags`, on the time reference of session `reference` in `epoch`.
    fn segment_in(
        epoch: Epoch,
        segment: u8,
        start_time: u32,
        reference: u32,
        flags: SegmentFlags,
    ) -> Step {
        let record =
            Metadata::Segment(segment_record(epoch, segment, start_time, reference, flags));
        device(written(|buf| record.write(MetadataFlags::UPDATE, buf)))
    }

    /// The record [`segment_in`] writes.
    fn segment_record(
        epoch: Epoch,
        segment: u8,
        start_time: u32,
        reference: u32,
        flags: SegmentFlags,
    ) -> data::Segment<'static> {
        data::Segment {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new(segment),
            flags,
            epoch,
            timeref_serial: "S1",
            timeref_session: SessionId::new(reference),
            start_time,
            sampling_rate: 10,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: FilterType::NONE,
        }
    }

    /// The record of a segment a switch opened.
    fn record(id: u8, start_time: u32) -> Step {
        record_on(id, start_time, 3)
    }

    fn record_on(id: u8, start_time: u32, reference: u32) -> Step {
        segment(
            id,
            start_time,
            reference,
            SegmentFlags::VALID | SegmentFlags::ACTIVE,
        )
    }

    /// The record of a segment a start opened.
    fn start(id: u8, start_time: u32) -> Step {
        let flags = SegmentFlags::VALID | SegmentFlags::ACTIVE | SegmentFlags::START;
        segment(id, start_time, 3, flags)
    }

    /// The closing record of a segment a start opened.
    fn close(id: u8, start_time: u32) -> Step {
        segment(id, start_time, 3, SegmentFlags::VALID | SegmentFlags::START)
    }

    /// The record of stream 1, two bytes a sample, so that each row carries two.
    fn stream() -> Step {
        stream_of(1)
    }

    /// The record of stream `stream`, two bytes a sample.
    fn stream_of(stream: u8) -> Step {
        let record = Metadata::Stream(data::Stream {
            stream_id: StreamId::new(stream),
            n_columns: 1,
            n_segments: 4,
            sample_size: 2,
            buf_samples: 0,
            name: "s",
        });
        device(written(|buf| record.write(MetadataFlags::UPDATE, buf)))
    }

    fn row(segment: u8, first: u32) -> Step {
        row_of(1, segment, first)
    }

    /// A row of stream `stream`, two bytes for each of two samples.
    fn row_of(stream: u8, segment: u8, first: u32) -> Step {
        let samples = Samples {
            stream_id: StreamId::new(stream),
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

    /// Rows 0 to 29 of a segment at 10 rows a second from second 100, then `next`, and its row 0.
    fn held(next: Step) -> Vec<Step> {
        vec![
            declare(1),
            stream(),
            record(0, 100),
            row(0, 0),
            row(0, 28),
            next,
            row(1, 0),
        ]
    }

    /// Each law holds of a trace that keeps it and names the one packet of a
    /// trace that breaks it, and no other law fires on either.
    #[test]
    fn each_law_fires_on_the_packet_that_breaks_it_and_only_there() {
        let v1 = || declare(1);
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
            (
                Law::A2,
                vec![
                    session(1, 0),
                    announce("dev.name", 5, b"a"),
                    session(2, 0),
                    announce("dev.name", 1, b"b"),
                ],
                vec![
                    session(1, 0),
                    announce("dev.name", 5, b"a"),
                    announce("dev.name", 1, b"b"),
                    session(2, 0),
                ],
                2,
            ),
            (
                Law::S1,
                vec![
                    session(1, 0),
                    v1(),
                    row(5, 0),
                    record(0, 100),
                    session(1, 0),
                    row(0, 0),
                ],
                vec![v1(), record(0, 100), row(1, 0)],
                2,
            ),
            (
                Law::S1,
                vec![
                    v1(),
                    at("/1", v1()),
                    record(0, 100),
                    at("/1", record(0, 100)),
                    row(0, 0),
                ],
                vec![
                    v1(),
                    at("/1", v1()),
                    record(0, 100),
                    at("/1", record(1, 100)),
                    at("/1", row(0, 0)),
                ],
                4,
            ),
            (
                Law::S1,
                vec![
                    v1(),
                    record(0, 100),
                    record(1, 101),
                    record(0, 100),
                    row(0, 0),
                ],
                vec![v1(), record(0, 100), record(1, 101), row(0, 0)],
                3,
            ),
            (
                Law::S2,
                vec![
                    v1(),
                    record(0, 100),
                    row(0, 0),
                    row(0, 1),
                    record(1, 101),
                    row(1, 0),
                ],
                vec![v1(), record(0, 100), row(0, 0), record(1, 101), row(0, 1)],
                4,
            ),
            (
                Law::S3,
                vec![v1(), stream(), record(0, 100), row(0, 0), row(0, 2)],
                vec![v1(), stream(), record(0, 100), row(0, 0), row(0, 1)],
                4,
            ),
            (
                Law::S4,
                vec![
                    session(1, 0),
                    v1(),
                    record(0, 100),
                    row(0, 0),
                    record(1, 101),
                    row(1, 0),
                    record(0, 102),
                    row(0, 0),
                    record(1, 101),
                    row(1, 1),
                    session(2, 0),
                    v1(),
                    start(0, 100),
                    row(0, 0),
                ],
                vec![
                    v1(),
                    record(0, 100),
                    row(0, 5),
                    record(1, 101),
                    row(1, 0),
                    record(0, 100),
                    row(0, 5),
                ],
                6,
            ),
            (
                Law::S4,
                vec![
                    v1(),
                    record(0, 100),
                    row(0, 5),
                    record(1, 101),
                    row(1, 0),
                    record_on(0, 100, 4),
                    row(0, 0),
                ],
                vec![
                    v1(),
                    record(0, 100),
                    row(0, 5),
                    record(1, 101),
                    row(1, 0),
                    record(0, 100),
                    row(0, 0),
                ],
                6,
            ),
            (
                Law::S5,
                vec![
                    v1(),
                    record(0, 100),
                    row(0, 4),
                    record(0, 105),
                    row(0, 4),
                    record(0, 100),
                    row(0, 9),
                    record(1, 101),
                    row(1, 12),
                ],
                vec![v1(), record(0, 100), row(0, 4), record(0, 105), row(0, 5)],
                4,
            ),
            (
                Law::S5,
                vec![
                    v1(),
                    record(0, 100),
                    row(0, 4),
                    record(1, 101),
                    record(0, 105),
                    row(0, 5),
                ],
                vec![
                    v1(),
                    record(0, 100),
                    row(0, 4),
                    record(0, 101),
                    record(0, 105),
                    row(0, 5),
                ],
                5,
            ),
            (
                Law::D1,
                vec![
                    v1(),
                    start(0, 100),
                    row(0, 0),
                    close(0, 100),
                    start(1, 101),
                    row(1, 0),
                ],
                vec![v1(), start(0, 100), row(0, 0), close(0, 100), row(0, 2)],
                4,
            ),
            (
                Law::D2,
                vec![v1(), start(0, 100), row(0, 0), close(0, 100), close(0, 100)],
                vec![
                    v1(),
                    start(0, 100),
                    row(0, 0),
                    segment(0, 100, 3, SegmentFlags::VALID),
                ],
                3,
            ),
            (
                Law::D3,
                vec![
                    v1(),
                    record(1, 99),
                    segment(1, 99, 3, SegmentFlags::VALID),
                    start(0, 100),
                    record(1, 101),
                    start(2, 102),
                    close(2, 102),
                ],
                vec![
                    v1(),
                    record(1, 99),
                    segment(1, 99, 3, SegmentFlags::VALID),
                    record(0, 100),
                ],
                3,
            ),
            (
                Law::D3,
                vec![
                    v1(),
                    close(3, 99),
                    start(0, 100),
                    close(0, 100),
                    start(1, 101),
                ],
                vec![v1(), start(0, 100), close(0, 100), record(1, 101)],
                3,
            ),
            (Law::S6, held(record(1, 103)), held(record(1, 102)), 5),
            (Law::S6, held(start(1, 102)), held(record(1, 102)), 5),
            (Law::S6, held(record_on(1, 102, 4)), held(record(1, 102)), 5),
            (
                Law::S6,
                held(segment_in(
                    Epoch::SYSTIME,
                    1,
                    99,
                    3,
                    SegmentFlags::VALID | SegmentFlags::ACTIVE,
                )),
                held(record(1, 99)),
                5,
            ),
            (
                Law::M1,
                vec![
                    row_of(2, 0, 0),
                    stream_of(2),
                    v1(),
                    stream(),
                    record(0, 100),
                    row(0, 0),
                ],
                vec![v1(), stream_of(2)],
                1,
            ),
            (
                Law::M1,
                vec![row_of(2, 0, 0), v1(), record(0, 100), row(0, 0)],
                vec![v1(), record(0, 100), row_of(2, 0, 0)],
                2,
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

    /// A device that declares no revision, as C firmware, is held only to the laws of revision 0.
    #[test]
    fn a_device_without_a_revision_keeps_only_the_first_laws() {
        let breaking = |declared: Vec<Step>| {
            let rows = [record(0, 100), row(1, 0), ttl(1, row(1, 2))];
            broken(&[declared, rows.to_vec()].concat())
        };
        assert_eq!(breaking(vec![]), [(Law::F1, 2)]);
        assert_eq!(breaking(vec![declare(0)]), [(Law::F1, 3)]);
        assert_eq!(
            breaking(vec![declare(1)]),
            [(Law::S1, 2), (Law::F1, 3), (Law::S1, 3)]
        );
    }

    /// A sample packet with no sample bytes breaks `F2` for a device of revision 1, and a device of
    /// revision 0 is not held to it.
    #[test]
    fn a_sample_packet_with_no_samples_breaks_f2_from_revision_1() {
        let empty = device(vec![
            PacketType::stream(1).unwrap().value(),
            0,
            4,
            0,
            0,
            0,
            0,
            0,
        ]);
        let trace = |declared: Step| broken(&[declared, stream(), record(0, 100), empty.clone()]);
        assert_eq!(trace(declare(1)), [(Law::F2, 3)]);
        assert_eq!(trace(declare(0)), []);
    }

    /// A heartbeat of the next session that overtakes the last session's rows, as a hop that keeps
    /// each lane's order and no other delivers it, leaves the stream to be checked on to its start.
    #[test]
    fn a_heartbeat_overtaking_the_last_sessions_rows_keeps_the_contract() {
        let trace = |late: Step| {
            broken(&[
                session(1, 0),
                declare(1),
                stream(),
                start(0, 100),
                row(0, 0),
                session(2, 0),
                declare(1),
                late,
                start(0, 105),
                row(0, 0),
            ])
        };
        assert_eq!(trace(row(0, 2)), []);
        assert_eq!(trace(row(0, 0)), [(Law::S3, 7)]);
    }

    /// A trace that begins with the last session's announcements numbers the next session's
    /// afresh from its first heartbeat.
    #[test]
    fn announcements_before_the_first_heartbeat_are_of_a_session_of_their_own() {
        let steps = [
            announce("dev.name", 1, b"a"),
            session(2, 0),
            announce("dev.name", 1, b"b"),
        ];
        assert_eq!(broken(&steps), []);
    }

    /// A segment's record sent again before its first row, with the start a
    /// relabel moved it to, names the segment its rows then come in.
    #[test]
    fn a_record_replaced_before_its_first_row_keeps_the_contract() {
        let steps = [
            session(1, 0),
            declare(1),
            stream(),
            start(0, 100),
            start(0, 103),
            row(0, 0),
            row(0, 2),
            close(0, 103),
        ];
        assert_eq!(broken(&steps), []);
    }

    /// The contract's segment and the view's test of the same segment agree.
    /// Records that differ in one of id, start time, epoch and the time
    /// reference's session and serial name other segments to both.
    #[test]
    fn a_segment_is_the_one_the_view_names_the_same() {
        let base = segment_record(
            Epoch::UNIX,
            0,
            100,
            3,
            SegmentFlags::VALID | SegmentFlags::ACTIVE,
        );
        let bytes = |record: data::Segment<'_>| {
            let metadata = Metadata::Segment(record);
            let mut buf = alloc::vec![0; metadata.record_len()];
            let (_, len) = metadata.write_record(&mut buf).unwrap();
            buf.truncate(len);
            buf
        };
        let others = [
            data::Segment {
                segment_id: SegmentId::new(1),
                ..base
            },
            data::Segment {
                start_time: 101,
                ..base
            },
            data::Segment {
                epoch: Epoch::SYSTIME,
                ..base
            },
            data::Segment {
                timeref_session: SessionId::new(4),
                ..base
            },
            data::Segment {
                timeref_serial: "S2",
                ..base
            },
        ];
        let base_bytes = bytes(base);
        let base_view = data::SegmentView::new_checked(&base_bytes).unwrap();
        for (index, other) in others.into_iter().chain([base]).enumerate() {
            let other_bytes = bytes(other);
            let other_view = data::SegmentView::new_checked(&other_bytes).unwrap();
            let same = index == others.len();
            assert_eq!(
                Segment::of(base) == Segment::of(other),
                same,
                "pair {index}"
            );
            assert_eq!(base_view.is_same_segment(&other_view), same, "pair {index}");
        }
    }
}

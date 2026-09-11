//! A device's standing part: who it is, the RPCs every device answers, its
//! heartbeat, and what a host learns on connecting.
//!
//! Nothing here waits. The runtime that owns the transport and the clock calls
//! [`Device::connected`] when a host appears, [`Device::handle`] with each
//! packet, and [`Device::tick`] once [`Device::deadline`] has passed, and
//! every packet the device sends goes to the runtime's [`Sink`]. An RPC the
//! device does not answer itself comes back as a [`Call`] for the application
//! to answer.

use heapless::String;
use twinleaf_proto::data::{self, Metadata, MetadataFlags, CURRENT_SEGMENT};
use twinleaf_proto::heartbeat::Heartbeat;
use twinleaf_proto::packet::{Packet, PacketType, PacketView};
use twinleaf_proto::route;
use twinleaf_proto::rpc::{self, Method, Request, RpcError};
use twinleaf_proto::settings::Setting;
use twinleaf_proto::{RpcRequestId, SessionId};

use crate::metadata::{self, Streams};
use crate::rpc::{self as table, put, Reply, RpcSpec};

/// Where a device's packets go.
pub trait Sink {
    /// Send one complete packet.
    fn send(&mut self, packet: &[u8]);
}

/// Milliseconds between heartbeats.
pub const HEARTBEAT_INTERVAL: u64 = 200;

/// What a device says it is: `dev.name`, `dev.desc`, and the device record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// Model name.
    pub name: String<32>,
    /// Human readable description with revision, serial, and build.
    pub desc: String<96>,
    /// Serial number.
    pub serial: String<32>,
    /// Firmware build identifier.
    pub firmware: String<32>,
}

impl Identity {
    /// An identity, if every string fits its field.
    pub fn new(name: &str, desc: &str, serial: &str, firmware: &str) -> Option<Self> {
        Some(Self {
            name: name.try_into().ok()?,
            desc: desc.try_into().ok()?,
            serial: serial.try_into().ok()?,
            firmware: firmware.try_into().ok()?,
        })
    }
}

/// An RPC request the application answers, with [`Call::reply`].
#[derive(Debug, Clone, Copy)]
pub struct Call<'p, 't> {
    /// Request id, returned with the reply.
    pub id: RpcRequestId,
    /// Method name, from the table.
    pub name: &'t str,
    /// Argument bytes.
    pub args: &'p [u8],
    routing: &'p [u8],
}

impl Call<'_, '_> {
    /// Send the reply or error the application answers with.
    pub fn reply(&self, result: Result<&[u8], RpcError>, out: &mut impl Sink) {
        answer(out, self.routing, self.id, result);
    }
}

/// What [`Device::handle`] did with a packet.
#[derive(Debug)]
pub enum Handled<'p, 't> {
    /// Answered, or nothing to answer.
    Done,
    /// An RPC for the application.
    Rpc(Call<'p, 't>),
}

/// The standing part of a device.
pub struct Device<'t> {
    /// Who the device is.
    pub identity: Identity,
    /// Which boot this is.
    pub session: SessionId,
    table: &'t [RpcSpec],
    hash: u32,
    next_beat: u64,
}

impl<'t> Device<'t> {
    /// A device answering `table`, whose hash is fixed from here on.
    pub fn new(identity: Identity, session: SessionId, table: &'t [RpcSpec]) -> Self {
        Self {
            identity,
            session,
            table,
            hash: table::hash(table),
            next_beat: 0,
        }
    }

    /// The RPC table.
    pub fn table(&self) -> &'t [RpcSpec] {
        self.table
    }

    /// `rpc.hash`.
    pub fn hash(&self) -> u32 {
        self.hash
    }

    /// The device record: identity, session, and how many streams.
    pub fn record(&self, streams: &impl Streams) -> data::Device<'_> {
        data::Device {
            session: self.session,
            n_streams: streams.ids().count() as u8,
            name: &self.identity.name,
            serial: &self.identity.serial,
            firmware: &self.identity.firmware,
        }
    }

    /// Tell a host that has just connected everything it needs: the table
    /// hash, a heartbeat, and every metadata record.
    pub fn connected(&mut self, streams: &impl Streams, now: u64, out: &mut impl Sink) {
        let hash = self.hash.to_le_bytes();
        let setting = Setting {
            name: b"rpc.hash",
            flags: 0,
            reply: &hash,
        };
        send(out, &[], |buf| setting.write(buf));
        self.beat(now, out);
        let mut records = self.records(streams).peekable();
        while let Some(record) = records.next() {
            let flags = match records.peek() {
                Some(_) => MetadataFlags::UPDATE,
                None => MetadataFlags::UPDATE | MetadataFlags::LAST,
            };
            send(out, &[], |buf| record.write(flags, buf));
        }
    }

    /// Act on one packet from the host.
    pub fn handle<'p>(
        &mut self,
        streams: &impl Streams,
        packet: PacketView<'p>,
        out: &mut impl Sink,
    ) -> Handled<'p, 't> {
        if packet.header.ptype != PacketType::RPC_REQ {
            return Handled::Done;
        }
        let routing = packet.routing;
        let Some(request) = Request::parse(packet.payload) else {
            answer(out, routing, RpcRequestId::new(0), Err(RpcError::Malformed));
            return Handled::Done;
        };
        let index = match request.method {
            Method::ById(id) => Some(usize::from(id.value())).filter(|&i| i < self.table.len()),
            Method::ByName(name) => self
                .table
                .iter()
                .position(|spec| spec.name.as_bytes() == name),
        };
        let Some(index) = index else {
            answer(out, routing, request.id, Err(RpcError::NotFound));
            return Handled::Done;
        };
        let (name, args) = (self.table[index].name, request.args);
        let mut reply = Reply::new();
        let result = match name {
            "rpc.name" => table::name(self.table, args, &mut reply),
            "rpc.id" => table::id(self.table, args, &mut reply),
            "rpc.info" => table::info(self.table, args, &mut reply),
            "rpc.list" => table::list(self.table, args, false, &mut reply),
            "rpc.listinfo" => table::list(self.table, args, true, &mut reply),
            "rpc.hash" => read(args, &self.hash.to_le_bytes(), &mut reply),
            "dev.name" => read(args, self.identity.name.as_bytes(), &mut reply),
            "dev.desc" => read(args, self.identity.desc.as_bytes(), &mut reply),
            "dev.serial" => read(args, self.identity.serial.as_bytes(), &mut reply),
            "dev.firmware.serial" => read(args, self.identity.firmware.as_bytes(), &mut reply),
            "dev.session" => read(args, &self.session.to_le_bytes(), &mut reply),
            "dev.metadata" => metadata::reply(self.record(streams), streams, args, &mut reply),
            _ => {
                return Handled::Rpc(Call {
                    id: request.id,
                    name,
                    args,
                    routing,
                })
            }
        };
        answer(out, routing, request.id, result.map(|()| reply.as_slice()));
        Handled::Done
    }

    /// Send whatever is due at `now`.
    pub fn tick(&mut self, now: u64, out: &mut impl Sink) {
        if now >= self.next_beat {
            self.beat(now, out);
        }
    }

    /// When [`Device::tick`] next has something to send.
    pub fn deadline(&self) -> u64 {
        self.next_beat
    }

    fn beat(&mut self, now: u64, out: &mut impl Sink) {
        let beat = Heartbeat::Session(self.session);
        send(out, &[], |buf| beat.write(buf));
        self.next_beat = now + HEARTBEAT_INTERVAL;
    }

    /// Every metadata record, in sweep order.
    fn records<'s, S: Streams>(
        &'s self,
        streams: &'s S,
    ) -> impl Iterator<Item = Metadata<'s>> + 's {
        let per_stream = streams.ids().flat_map(move |id| {
            let stream = streams.stream(id);
            let columns = stream.map_or(0, |stream| stream.n_columns);
            stream
                .map(Metadata::Stream)
                .into_iter()
                .chain(streams.segment(id, CURRENT_SEGMENT).map(Metadata::Segment))
                .chain(
                    (0..columns)
                        .filter_map(move |index| streams.column(id, index).map(Metadata::Column)),
                )
        });
        core::iter::once(Metadata::Device(self.record(streams))).chain(per_stream)
    }
}

/// A read-only property: an argument is a write.
fn read(args: &[u8], value: &[u8], out: &mut Reply) -> Result<(), RpcError> {
    if !args.is_empty() {
        return Err(RpcError::ReadOnly);
    }
    put(out, value)
}

fn answer(out: &mut impl Sink, routing: &[u8], id: RpcRequestId, result: Result<&[u8], RpcError>) {
    match result {
        Ok(value) => send(out, routing, |buf| rpc::write_reply(buf, id, value)),
        Err(error) => send(out, routing, |buf| rpc::write_error(buf, id, error)),
    }
}

/// Send one packet back along `routing`. Every writer is bounded by the
/// packet, so a packet that does not fit is a bug.
fn send(out: &mut impl Sink, routing: &[u8], write: impl FnOnce(&mut [u8]) -> Option<usize>) {
    let mut buf = [0u8; Packet::MAX_SIZE];
    let mut len = write(&mut buf).expect("a device packet fits a packet");
    for &hop in routing {
        len = route::push_hop(&mut buf, hop).expect("a request's route fits its reply");
    }
    out.send(&buf[..len]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{Access, Value};
    use twinleaf_proto::data::{DataType, FilterType, MetadataType, SegmentFlags};
    use twinleaf_proto::rpc::Answer;
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::{ColumnId, SegmentId, StreamId};

    static TABLE: [RpcSpec; 5] = [
        RpcSpec::std("rpc.list", Access::RW),
        RpcSpec::prop("rpc.hash", Value::Uint(4), Access::READ),
        RpcSpec::prop("dev.name", Value::String, Access::READ),
        RpcSpec::std("dev.metadata", Access::RW),
        RpcSpec::prop("app.gain", Value::Uint(1), Access::RW),
    ];

    struct OneStream;

    impl Streams for OneStream {
        fn ids(&self) -> impl Iterator<Item = u8> {
            core::iter::once(1)
        }

        fn stream(&self, stream_id: u8) -> Option<data::Stream<'_>> {
            (stream_id == 1).then_some(data::Stream {
                stream_id: StreamId::new(1),
                n_columns: 1,
                n_segments: 4,
                sample_size: 4,
                buf_samples: 0,
                name: "s",
            })
        }

        fn segment(&self, stream_id: u8, _index: u8) -> Option<data::Segment<'_>> {
            (stream_id == 1).then_some(data::Segment {
                stream_id: StreamId::new(1),
                segment_id: SegmentId::new(0),
                flags: SegmentFlags::VALID,
                epoch: Epoch::UNIX,
                timeref_serial: "S",
                timeref_session: SessionId::new(9),
                start_time: 0,
                sampling_rate: 10,
                decimation: 1,
                filter_cutoff: 0.0,
                filter_type: FilterType::NONE,
            })
        }

        fn column(&self, stream_id: u8, index: u8) -> Option<data::Column<'_>> {
            (stream_id == 1 && index == 0).then_some(data::Column {
                stream_id: StreamId::new(1),
                index: ColumnId::new(0),
                data_type: DataType::F32,
                name: "c",
                units: "",
                description: "",
            })
        }
    }

    #[derive(Default)]
    struct Sent(Vec<Vec<u8>>);

    impl Sink for Sent {
        fn send(&mut self, packet: &[u8]) {
            self.0.push(packet.to_vec());
        }
    }

    impl Sent {
        fn views(&self) -> Vec<PacketView<'_>> {
            self.0
                .iter()
                .map(|packet| PacketView::parse_prefix(packet).unwrap().0)
                .collect()
        }
    }

    fn device() -> Device<'static> {
        let identity = Identity::new("dev", "Dev R1 (S) [fw]", "S", "fw").unwrap();
        Device::new(identity, SessionId::new(9), &TABLE)
    }

    fn request(method: Method<'_>, args: &[u8], routing: &[u8]) -> Vec<u8> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let mut len = rpc::write_request(&mut buf, RpcRequestId::new(7), method, args).unwrap();
        for &hop in routing {
            len = route::push_hop(&mut buf, hop).unwrap();
        }
        buf[..len].to_vec()
    }

    fn answered(sent: &Sent) -> Answer<'_> {
        let [view] = sent.views()[..] else {
            panic!("one packet");
        };
        Answer::parse(view.header.ptype, view.payload).unwrap()
    }

    #[test]
    fn connecting_sends_the_hash_a_heartbeat_and_the_sweep() {
        let mut device = device();
        let mut sent = Sent::default();
        device.connected(&OneStream, 1000, &mut sent);

        let views = sent.views();
        let types: Vec<_> = views.iter().map(|view| view.header.ptype).collect();
        assert_eq!(
            types,
            [
                PacketType::SETTING,
                PacketType::HEARTBEAT,
                PacketType::METADATA,
                PacketType::METADATA,
                PacketType::METADATA,
                PacketType::METADATA,
            ]
        );
        let setting = Setting::parse(views[0].payload).unwrap();
        assert_eq!(setting.name, b"rpc.hash");
        assert_eq!(setting.reply, device.hash().to_le_bytes());
        assert_eq!(
            Heartbeat::parse(views[1].payload),
            Some(Heartbeat::Session(SessionId::new(9)))
        );
        let kinds: Vec<_> = views[2..]
            .iter()
            .map(|view| data::split_metadata(view.payload).unwrap())
            .map(|(kind, flags, _)| {
                (
                    MetadataType::from(kind),
                    flags.contains(MetadataFlags::LAST),
                )
            })
            .collect();
        use MetadataType::*;
        assert_eq!(
            kinds,
            [
                (Device, false),
                (Stream, false),
                (Segment, false),
                (Column, true)
            ]
        );
        assert_eq!(device.deadline(), 1000 + HEARTBEAT_INTERVAL);
    }

    #[test]
    fn heartbeats_are_due_every_interval() {
        let mut device = device();
        let mut sent = Sent::default();
        device.connected(&OneStream, 1000, &mut sent);
        sent.0.clear();

        device.tick(1100, &mut sent);
        assert!(sent.0.is_empty());
        device.tick(1200, &mut sent);
        assert_eq!(sent.views()[0].header.ptype, PacketType::HEARTBEAT);
        assert_eq!(device.deadline(), 1400);
    }

    #[test]
    fn standard_rpcs_are_answered_by_name_or_id() {
        let mut device = device();
        let mut sent = Sent::default();
        let packet = request(Method::ByName(b"dev.name"), &[], &[]);
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        assert!(matches!(
            device.handle(&OneStream, view, &mut sent),
            Handled::Done
        ));
        assert!(
            matches!(answered(&sent), Answer::Reply(reply) if reply.req_id.value() == 7 && reply.value == b"dev")
        );

        let mut sent = Sent::default();
        let packet = request(Method::ById(twinleaf_proto::RpcMethodId::new(1)), &[], &[]);
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        device.handle(&OneStream, view, &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Reply(reply) if reply.value == device.hash().to_le_bytes())
        );

        let mut sent = Sent::default();
        let packet = request(Method::ByName(b"dev.metadata"), &[], &[]);
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        device.handle(&OneStream, view, &mut sent);
        let Answer::Reply(reply) = answered(&sent) else {
            panic!("a reply");
        };
        assert_eq!(data::MetadataReply::parse(reply.value).unwrap().count(), 4);
    }

    #[test]
    fn bad_requests_are_refused() {
        let mut device = device();
        let mut sent = Sent::default();
        let packet = request(Method::ByName(b"dev.nope"), &[], &[]);
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        device.handle(&OneStream, view, &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Error(error) if error.error() == RpcError::NotFound)
        );

        let mut sent = Sent::default();
        let packet = request(Method::ByName(b"dev.name"), &[1], &[]);
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        device.handle(&OneStream, view, &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Error(error) if error.error() == RpcError::ReadOnly)
        );

        let mut sent = Sent::default();
        let mut packet = request(Method::ByName(b"dev.name"), &[], &[]);
        packet.truncate(Packet::MAX_SIZE.min(6));
        packet[2..4].copy_from_slice(&2u16.to_le_bytes());
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        device.handle(&OneStream, view, &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Error(error) if error.error() == RpcError::Malformed)
        );
    }

    #[test]
    fn an_application_rpc_is_handed_back_with_its_route() {
        let mut device = device();
        let mut sent = Sent::default();
        let packet = request(Method::ByName(b"app.gain"), &[3], &[2, 1]);
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        let Handled::Rpc(call) = device.handle(&OneStream, view, &mut sent) else {
            panic!("an application call");
        };
        assert!(sent.0.is_empty());
        assert_eq!(
            (call.name, call.args, call.id.value()),
            ("app.gain", &[3][..], 7)
        );

        call.reply(Err(RpcError::Busy), &mut sent);
        let [view] = sent.views()[..] else {
            panic!("one packet");
        };
        assert_eq!(view.routing, [2, 1]);
        assert!(
            matches!(Answer::parse(view.header.ptype, view.payload), Some(Answer::Error(error)) if error.error() == RpcError::Busy)
        );
    }

    #[test]
    fn other_packets_are_ignored() {
        let mut device = device();
        let mut sent = Sent::default();
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = Heartbeat::Any(&[]).write(&mut buf).unwrap();
        let (view, _) = PacketView::parse_prefix(&buf[..len]).unwrap();
        assert!(matches!(
            device.handle(&OneStream, view, &mut sent),
            Handled::Done
        ));
        assert!(sent.0.is_empty());
    }
}

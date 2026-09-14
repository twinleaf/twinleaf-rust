//! A device's standing part: who it is, the RPCs every device answers, its
//! heartbeat, its log threshold, and what a host learns on connecting.
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
use twinleaf_proto::log::{LogLevel, LogMessage, MAX_MESSAGE_SIZE};
use twinleaf_proto::packet::{Packet, PacketType, PacketView};
use twinleaf_proto::route;
use twinleaf_proto::rpc::{self, Method, Request, RpcError};
use twinleaf_proto::settings::Setting as Announcement;
use twinleaf_proto::{RpcRequestId, SessionId};

use crate::conf::{self, Image};
use crate::metadata::{self, Streams};
use crate::rpc::{self as table, put, Reply, RpcSpec};
use crate::settings::{Changed, Persisted, Scalar, Setting};
use crate::Sink;

/// Nanoseconds between heartbeats.
pub const HEARTBEAT_INTERVAL: u64 = 200_000_000;

/// The level `dev.loglevel` boots at, as tl-chibi's `logThreshold` does.
pub const DEFAULT_LOGLEVEL: LogLevel = LogLevel::INFO;

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
    loglevel: Setting<u8>,
    settings_version: u32,
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
            loglevel: Setting::new("dev.loglevel", DEFAULT_LOGLEVEL.value()).persistent(),
            settings_version: 0,
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

    /// The threshold `dev.loglevel` holds, for a runtime that gates its own logging on it.
    pub fn loglevel(&self) -> LogLevel {
        LogLevel::new(self.loglevel.get())
    }

    /// The device record: identity, session, and how many streams.
    pub fn record(&self, streams: &(impl Streams + ?Sized)) -> data::Device<'_> {
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
    pub fn connected(
        &mut self,
        streams: &(impl Streams + ?Sized),
        now_ns: u64,
        out: &mut impl Sink,
    ) {
        announcement(out, "rpc.hash", &self.hash.to_le_bytes());
        self.beat(now_ns, out);
        let mut records = self.records(streams).peekable();
        while let Some(record) = records.next() {
            let flags = match records.peek() {
                Some(_) => MetadataFlags::UPDATE,
                None => MetadataFlags::UPDATE | MetadataFlags::LAST,
            };
            send(out, &[], |buf| record.write(flags, buf));
        }
    }

    /// Act on one packet from the host. A method the table declares an action
    /// refuses an argument, as libtio firmware does.
    pub fn handle<'p>(
        &mut self,
        streams: &(impl Streams + ?Sized),
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
        let spec = &self.table[index];
        let (name, args) = (spec.name, request.args);
        if spec.method == table::Method::Action && !args.is_empty() {
            answer(out, routing, request.id, Err(RpcError::ArgsSize));
            return Handled::Done;
        }
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
            "dev.loglevel" => write(
                &mut self.settings_version,
                &mut self.loglevel,
                args,
                &mut reply,
                out,
            ),
            "settings.version" => read(args, &self.settings_version.to_le_bytes(), &mut reply),
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

    /// Send `message` as a LOG packet, unless `level` is beneath the
    /// threshold `dev.loglevel` holds. A message too long for one packet is
    /// truncated at a character boundary.
    pub fn log(&self, level: LogLevel, data: u32, message: &str, out: &mut impl Sink) {
        if level.value() > self.loglevel.get() {
            return;
        }
        let end = (0..=message.len().min(MAX_MESSAGE_SIZE))
            .rev()
            .find(|&end| message.is_char_boundary(end))
            .expect("zero is a character boundary");
        let entry = LogMessage {
            level,
            data,
            message: &message.as_bytes()[..end],
        };
        send(out, &[], |buf| entry.write(buf));
    }

    /// Answer a setting's RPC, announcing and counting the value a write left.
    pub fn apply<T: Scalar>(
        &mut self,
        setting: &mut Setting<T>,
        args: &[u8],
        out: &mut impl Sink,
    ) -> Result<Reply, RpcError> {
        let mut reply = Reply::new();
        write(&mut self.settings_version, setting, args, &mut reply, out)?;
        Ok(reply)
    }

    /// Announce and count a setting the application answered itself, with the
    /// value its reply carries.
    pub fn announce(&mut self, name: &str, reply: &[u8], out: &mut impl Sink) {
        announce(&mut self.settings_version, out, name, reply);
    }

    /// Answer `dev.conf.save`: the log threshold the device keeps itself and
    /// every setting the application persists, for the platform to write.
    pub fn save<'i, const N: usize>(
        &mut self,
        settings: &mut [&mut dyn Persisted],
        image: &'i mut Image<N>,
    ) -> Result<&'i [u8], RpcError> {
        image.clear();
        conf::encode(&mut [&mut self.loglevel], image)?;
        conf::encode(settings, image)?;
        Ok(image)
    }

    /// Answer `dev.conf.load`, or a boot: take every stored value the image
    /// names, announcing each one that moved. A name no setting answers is
    /// left behind, and a value refused leaves [`RpcError::Load`].
    pub fn load(
        &mut self,
        settings: &mut [&mut dyn Persisted],
        image: &[u8],
        out: &mut impl Sink,
    ) -> Result<(), RpcError> {
        let Self {
            loglevel,
            settings_version,
            ..
        } = self;
        let mut outcome = Ok(());
        for entry in conf::entries(image) {
            let entry = entry?;
            let found = core::iter::once(&mut *loglevel as &mut dyn Persisted)
                .chain(settings.iter_mut().map(|setting| &mut **setting))
                .find(|setting| setting.name().as_bytes() == entry.name);
            let Some(setting) = found else {
                continue;
            };
            let mut reply = Reply::new();
            match setting.load(entry.value, &mut reply) {
                Ok(Changed::Unchanged) => {}
                Ok(Changed::Changed) => announce(settings_version, out, setting.name(), &reply),
                Err(_) => outcome = Err(RpcError::Load),
            }
        }
        outcome
    }

    /// Power-cycle: a new session, and the heartbeat, log threshold, and
    /// `settings.version` a boot starts from.
    pub fn reboot(&mut self, session: SessionId) {
        self.session = session;
        self.next_beat = 0;
        self.loglevel.reset();
        self.settings_version = 0;
    }

    /// Send whatever is due at `now_ns`.
    pub fn tick(&mut self, now_ns: u64, out: &mut impl Sink) {
        if now_ns >= self.next_beat {
            self.beat(now_ns, out);
        }
    }

    /// When [`Device::tick`] next has something to send.
    pub fn deadline(&self) -> u64 {
        self.next_beat
    }

    fn beat(&mut self, now_ns: u64, out: &mut impl Sink) {
        let beat = Heartbeat::Session(self.session);
        send(out, &[], |buf| beat.write(buf));
        self.next_beat = now_ns + HEARTBEAT_INTERVAL;
    }

    /// Every metadata record, in sweep order.
    fn records<'s, S: Streams + ?Sized>(
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

/// Answer a setting's RPC, announcing the value a write left and counting it
/// in `settings.version`.
fn write<T: Scalar>(
    version: &mut u32,
    setting: &mut Setting<T>,
    args: &[u8],
    reply: &mut Reply,
    out: &mut impl Sink,
) -> Result<(), RpcError> {
    match setting.rpc(args, reply)? {
        Changed::Unchanged => {}
        Changed::Changed => announce(version, out, setting.name(), reply),
    }
    Ok(())
}

/// Count a change and announce it.
fn announce(version: &mut u32, out: &mut impl Sink, name: &str, reply: &[u8]) {
    *version = version.wrapping_add(1);
    announcement(out, name, reply);
}

/// Send a SETTING packet carrying a value as the RPC of `name` replies it.
fn announcement(out: &mut impl Sink, name: &str, reply: &[u8]) {
    let setting = Announcement {
        name: name.as_bytes(),
        flags: 0,
        reply,
    };
    send(out, &[], |buf| setting.write(buf));
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
    use crate::rpc::{Access, Kind};
    use twinleaf_proto::data::{DataType, FilterType, MetadataType, SegmentFlags};
    use twinleaf_proto::rpc::Answer;
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::{ColumnId, SegmentId, StreamId};

    static TABLE: [RpcSpec; 8] = [
        RpcSpec::std("rpc.list", Access::RW),
        RpcSpec::prop("rpc.hash", Kind::Uint(4), Access::READ),
        RpcSpec::prop("dev.name", Kind::String, Access::READ),
        RpcSpec::prop("dev.loglevel", Kind::Uint(1), Access::RW),
        RpcSpec::prop("settings.version", Kind::Uint(4), Access::READ),
        RpcSpec::std("dev.metadata", Access::RW),
        RpcSpec::prop("app.gain", Kind::Uint(1), Access::RW),
        RpcSpec::action("app.go"),
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

    /// Hand the device one request by name, with no route.
    fn deliver(device: &mut Device<'_>, name: &[u8], args: &[u8], sent: &mut Sent) {
        let packet = request(Method::ByName(name), args, &[]);
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        device.handle(&OneStream, view, sent);
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
        device.connected(&OneStream, 1_000_000_000, &mut sent);

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
        let setting = Announcement::parse(views[0].payload).unwrap();
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
        assert_eq!(device.deadline(), 1_000_000_000 + HEARTBEAT_INTERVAL);
    }

    #[test]
    fn heartbeats_are_due_every_interval() {
        let mut device = device();
        let mut sent = Sent::default();
        device.connected(&OneStream, 1_000_000_000, &mut sent);
        sent.0.clear();

        device.tick(1_100_000_000, &mut sent);
        assert!(sent.0.is_empty());
        device.tick(1_200_000_000, &mut sent);
        assert_eq!(sent.views()[0].header.ptype, PacketType::HEARTBEAT);
        assert_eq!(device.deadline(), 1_400_000_000);
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
        deliver(&mut device, b"dev.metadata", &[], &mut sent);
        let Answer::Reply(reply) = answered(&sent) else {
            panic!("a reply");
        };
        assert_eq!(data::MetadataReply::parse(reply.value).unwrap().count(), 4);
    }

    #[test]
    fn bad_requests_are_refused() {
        let mut device = device();
        let mut sent = Sent::default();
        deliver(&mut device, b"dev.nope", &[], &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Error(error) if error.error() == RpcError::NotFound)
        );

        let mut sent = Sent::default();
        deliver(&mut device, b"dev.name", &[1], &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Error(error) if error.error() == RpcError::ReadOnly)
        );

        let mut sent = Sent::default();
        deliver(&mut device, b"app.go", &[1], &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Error(error) if error.error() == RpcError::ArgsSize)
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
    fn a_message_is_logged_at_the_threshold_and_dropped_beneath_it() {
        let device = device();
        let mut sent = Sent::default();
        device.log(LogLevel::DEBUG, 1, "chatter", &mut sent);
        assert!(sent.0.is_empty());

        device.log(LogLevel::INFO, 7, "up", &mut sent);
        device.log(LogLevel::CRITICAL, 8, "vbus", &mut sent);
        let types: Vec<_> = sent.views().iter().map(|view| view.header.ptype).collect();
        assert_eq!(types, [PacketType::LOG, PacketType::LOG]);
        let logged: Vec<_> = sent
            .views()
            .iter()
            .map(|view| LogMessage::parse(view.payload).unwrap())
            .map(|entry| (entry.level, entry.data, entry.message.to_vec()))
            .collect();
        assert_eq!(
            logged,
            [
                (LogLevel::INFO, 7, b"up".to_vec()),
                (LogLevel::CRITICAL, 8, b"vbus".to_vec())
            ]
        );
    }

    #[test]
    fn a_message_too_long_for_a_packet_is_truncated_at_a_character_boundary() {
        let device = device();
        let mut sent = Sent::default();
        let message = "é".repeat(MAX_MESSAGE_SIZE);
        device.log(LogLevel::ERROR, 0, &message, &mut sent);

        let [view] = sent.views()[..] else {
            panic!("one packet");
        };
        let entry = LogMessage::parse(view.payload).unwrap();
        assert_eq!(entry.message.len(), MAX_MESSAGE_SIZE - 1);
        assert_eq!(
            core::str::from_utf8(entry.message),
            Ok(&message[..MAX_MESSAGE_SIZE - 1])
        );
    }

    #[test]
    fn writing_dev_loglevel_raises_the_threshold_and_announces_it() {
        let mut device = device();
        let mut sent = Sent::default();
        deliver(&mut device, b"dev.loglevel", &[], &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Reply(reply) if reply.value == [DEFAULT_LOGLEVEL.value()])
        );

        let mut sent = Sent::default();
        deliver(
            &mut device,
            b"dev.loglevel",
            &[LogLevel::DEBUG.value()],
            &mut sent,
        );
        let views = sent.views();
        assert_eq!(views[0].header.ptype, PacketType::SETTING);
        let setting = Announcement::parse(views[0].payload).unwrap();
        assert_eq!(setting.name, b"dev.loglevel");
        assert_eq!(setting.reply, [LogLevel::DEBUG.value()]);

        let mut sent = Sent::default();
        device.log(LogLevel::DEBUG, 0, "now heard", &mut sent);
        assert_eq!(sent.0.len(), 1);

        let mut sent = Sent::default();
        deliver(&mut device, b"settings.version", &[], &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Reply(reply) if reply.value == 1u32.to_le_bytes())
        );
    }

    #[test]
    fn a_reboot_restores_the_threshold_and_the_settings_version_a_boot_starts_from() {
        let mut device = device();
        let mut sent = Sent::default();
        deliver(
            &mut device,
            b"dev.loglevel",
            &[LogLevel::DEBUG.value()],
            &mut sent,
        );

        device.reboot(SessionId::new(10));
        assert_eq!(device.session, SessionId::new(10));

        let mut sent = Sent::default();
        deliver(&mut device, b"dev.loglevel", &[], &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Reply(reply) if reply.value == [DEFAULT_LOGLEVEL.value()])
        );

        let mut sent = Sent::default();
        deliver(&mut device, b"settings.version", &[], &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Reply(reply) if reply.value == 0u32.to_le_bytes())
        );
    }

    #[test]
    fn an_applied_setting_is_announced_once_per_write() {
        let mut device = device();
        let mut gain = Setting::new("app.gain", 1u8);
        let mut sent = Sent::default();

        let reply = device.apply(&mut gain, &[], &mut sent).unwrap();
        assert_eq!(reply.as_slice(), [1]);
        assert!(sent.0.is_empty());

        let reply = device.apply(&mut gain, &[9], &mut sent).unwrap();
        assert_eq!(reply.as_slice(), [9]);
        assert_eq!(gain.get(), 9);
        let [view] = sent.views()[..] else {
            panic!("one packet");
        };
        assert_eq!(view.header.ptype, PacketType::SETTING);
        let setting = Announcement::parse(view.payload).unwrap();
        assert_eq!(setting.name, b"app.gain");
        assert_eq!(setting.reply, [9]);
    }

    #[test]
    fn a_saved_configuration_comes_back_with_every_value_it_moved_announced() {
        let mut device = device();
        let mut gain = Setting::new("app.gain", 1u8).persistent();
        let mut sent = Sent::default();
        deliver(
            &mut device,
            b"dev.loglevel",
            &[LogLevel::DEBUG.value()],
            &mut sent,
        );
        device.apply(&mut gain, &[9], &mut sent).unwrap();

        let mut image = Image::<64>::new();
        let stored = device.save(&mut [&mut gain], &mut image).unwrap().to_vec();

        device.reboot(SessionId::new(10));
        gain.reset();

        let mut sent = Sent::default();
        assert_eq!(device.load(&mut [&mut gain], &stored, &mut sent), Ok(()));
        assert_eq!(gain.get(), 9);
        let announced: Vec<_> = sent
            .views()
            .iter()
            .map(|view| Announcement::parse(view.payload).unwrap())
            .map(|setting| (setting.name.to_vec(), setting.reply.to_vec()))
            .collect();
        assert_eq!(
            announced,
            [
                (b"dev.loglevel".to_vec(), vec![LogLevel::DEBUG.value()]),
                (b"app.gain".to_vec(), vec![9]),
            ]
        );

        let mut sent = Sent::default();
        assert_eq!(device.load(&mut [&mut gain], &stored, &mut sent), Ok(()));
        assert!(sent.0.is_empty());

        let mut sent = Sent::default();
        deliver(&mut device, b"settings.version", &[], &mut sent);
        assert!(
            matches!(answered(&sent), Answer::Reply(reply) if reply.value == 2u32.to_le_bytes())
        );
    }

    #[test]
    fn a_stored_value_no_setting_takes_is_left_behind_or_left_as_a_load_error() {
        let mut device = device();
        let mut rate = Setting::new("app.rate", 1.0f64)
            .checked(|value| (value > 0.0).then_some(value).ok_or(RpcError::Invalid))
            .persistent();
        let mut sent = Sent::default();

        let unknown = [&[7u8, 1][..], b"app.old", &[3]].concat();
        assert_eq!(device.load(&mut [&mut rate], &unknown, &mut sent), Ok(()));

        let refused = [&[8u8, 8][..], b"app.rate", &(-1.0f64).to_le_bytes()].concat();
        assert_eq!(
            device.load(&mut [&mut rate], &refused, &mut sent),
            Err(RpcError::Load)
        );
        assert_eq!(rate.get(), 1.0);

        let truncated = &refused[..refused.len() - 1];
        assert_eq!(
            device.load(&mut [&mut rate], truncated, &mut sent),
            Err(RpcError::Load)
        );
        assert!(sent.0.is_empty());
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

//! The control machine every platform drives.
//!
//! [`Control`] owns [`Device`] and the board's own table entries, and nothing
//! else: no clock, no executor, no lanes. A platform hands it one [`Input`]
//! with the monotonic nanosecond it happened at, and gets back the packets it
//! wrote into the two sinks — the lane of the port that asked, and the lane
//! every port drains — and an [`Actions`] naming every decision it cannot
//! carry out itself.
//!
//! A board's own entries are a [`Group`]: one ordered list read three ways, as
//! the table's order, as the persist list, and as the name a request names.

use twinleaf_proto::log::LogLevel;
use twinleaf_proto::packet::{Packet, PacketView};
use twinleaf_proto::rpc::{self as wire, RpcError};
use twinleaf_proto::{RpcRequestId, SessionId};

use crate::conf::{self, Image};
use crate::device::{Call, Device, Handled};
use crate::metadata::Streams;
use crate::rpc::{self as table, read, Access, Reply, RpcSpec};
use crate::settings::{Changed, Persisted, Scalar, Setting};
use crate::Sink;

/// As much flash as a stored configuration may take.
pub const IMAGE_MAX: usize = 1024;

/// The bytes a stored configuration is built in and read back into.
pub type Entries = Image<IMAGE_MAX>;

/// Nanoseconds in the hour `dev.uptime` counts.
const NANOS_PER_HOUR: u64 = 3_600_000_000_000;

/// One thing that happens to a device.
pub enum Input<'p> {
    /// A packet a control port framed, addressed to this device.
    Packet(PacketView<'p>),
    /// The stored configuration, with the `dev.conf.load` waiting for it, or
    /// `None` for the restore a boot does unasked.
    Loaded(Option<Pending<'p>>, Result<&'p [u8], RpcError>),
}

/// An RPC whose answer comes from somewhere else, and where that answer goes.
#[derive(Clone, Copy)]
pub struct Pending<'p> {
    /// The request id the answer carries.
    pub id: RpcRequestId,
    routing: &'p [u8],
}

impl<'p> Pending<'p> {
    /// Who to answer for `call`.
    pub const fn of(call: &Call<'p, '_>) -> Self {
        Self {
            id: call.id,
            routing: call.routing,
        }
    }

    /// The answer to a request that carried no route, which every request a
    /// platform holds across an await is.
    pub const fn local(id: RpcRequestId) -> Self {
        Self { id, routing: &[] }
    }
}

/// What a platform's flash answers.
#[allow(clippy::large_enum_variant)]
pub enum FlashOp {
    /// One chunk of the update package, or, when empty, a read of the offset.
    Upload(Reply),
    /// `dev.firmware.upgrade`.
    Upgrade,
    /// `dev.firmware.abort`.
    Abort,
    /// `dev.conf.save`, with the image the settings encoded to.
    ConfSave(Entries),
    /// `dev.conf.load`, answered with [`Input::Loaded`].
    ConfLoad,
    /// `dev.conf.reset`.
    ConfReset,
}

/// What a host asked of the acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncRequest {
    /// `dev.start`: begin acquiring on the next scheduled edge.
    Start,
    /// `dev.stop`: stop acquiring.
    Stop,
    /// `dev.restart`: stop and stage a new segment.
    Restart,
}

/// An RPC whose answer comes from something the machine does not own.
#[allow(clippy::large_enum_variant)]
pub enum Deferred {
    /// The flash: `dev.firmware.*` and `dev.conf.*`.
    Flash(FlashOp),
    /// The acquisition: `dev.start`, `dev.stop`, and `dev.restart`.
    Acquire(SyncRequest),
}

/// What the machine decided and the platform carries out.
///
/// Every field is consumed once, so a platform takes it apart in one `let`
/// rather than reaching into it.
pub struct Actions<'p> {
    /// An RPC no table entry answers, for the platform to answer itself.
    pub call: Option<Call<'p, 'static>>,
    /// An RPC handed to whoever answers it, with who to answer.
    pub deferred: Option<(Pending<'p>, Deferred)>,
    /// The reply is written: flush the port that asked, then reset.
    pub reboot: bool,
    /// What the platform's own logging is gated on from here on.
    pub loglevel: LogLevel,
    /// What the machine has to say on the platform's own log.
    pub log: Option<(LogLevel, &'static str)>,
}

impl<'p> Actions<'p> {
    fn defer(&mut self, pending: Pending<'p>, work: Deferred) {
        self.deferred = Some((pending, work));
    }
}

/// Where a device's packets go.
///
/// Two lanes rather than one sink: a reply belongs to the port that asked and
/// outranks everything queued for it, while a setting the device itself moved
/// belongs to every port. A platform with one port is [`OneLane`].
pub trait Lanes {
    /// The ordered lane of the port that asked: its replies, and the
    /// announcement a write puts in front of its reply.
    fn reply(&mut self, packet: &[u8]);

    /// The lane every port drains: what the device announced or logged on its
    /// own account.
    fn event(&mut self, packet: &[u8]);
}

/// Both lanes over one sink, for a platform that has one port.
pub struct OneLane<S: Sink>(pub S);

impl<S: Sink> Lanes for OneLane<S> {
    fn reply(&mut self, packet: &[u8]) {
        self.0.send(packet);
    }

    fn event(&mut self, packet: &[u8]) {
        self.0.send(packet);
    }
}

/// One lane, as the machine writes packets into it.
struct Lane<'a, L: Lanes + ?Sized> {
    lanes: &'a mut L,
    event: bool,
}

impl<L: Lanes + ?Sized> Sink for Lane<'_, L> {
    fn send(&mut self, packet: &[u8]) {
        match self.event {
            true => self.lanes.event(packet),
            false => self.lanes.reply(packet),
        }
    }
}

/// The lane of the port that asked.
fn to<L: Lanes + ?Sized>(lanes: &mut L) -> Lane<'_, L> {
    Lane {
        lanes,
        event: false,
    }
}

/// The lane every port drains.
fn events<L: Lanes + ?Sized>(lanes: &mut L) -> Lane<'_, L> {
    Lane { lanes, event: true }
}

/// The hardware half of a device's identity, which only a platform knows.
///
/// A field left `None` answers [`RpcError::State`], as any standard RPC a
/// platform does not implement does.
#[derive(Clone, Copy, Debug, Default)]
pub struct Platform {
    /// `dev.model`.
    pub model: Option<&'static str>,
    /// `dev.mcu.model`.
    pub mcu: Option<&'static str>,
    /// `dev.uid`.
    pub uid: Option<&'static [u8]>,
    /// `dev.revision`.
    pub hw_rev: Option<u16>,
}

impl Platform {
    /// A platform that says nothing about its hardware.
    pub const UNKNOWN: Self = Self {
        model: None,
        mcu: None,
        uid: None,
        hw_rev: None,
    };
}

/// The one owner of a device and its settings.
pub struct Control<S: Group> {
    device: Device<'static>,
    settings: S,
    platform: Platform,
    /// What `sync.status` answers, as the platform last published it.
    time_status: u8,
    /// The nanosecond the device came up, which `dev.systime` and `dev.uptime`
    /// count from.
    booted_ns: u64,
}

impl<S: Group> Control<S> {
    /// A device answering `settings` beyond what `device` answers itself,
    /// come up at `now_ns`.
    pub fn new(device: Device<'static>, settings: S, platform: Platform, now_ns: u64) -> Self {
        Self {
            device,
            settings,
            platform,
            time_status: 0,
            booted_ns: now_ns,
        }
    }

    /// The standing part of the device, for what only a platform sends: the
    /// connect burst, the heartbeat, and its own log.
    pub fn device(&self) -> &Device<'static> {
        &self.device
    }

    /// The same, to send through.
    pub fn device_mut(&mut self) -> &mut Device<'static> {
        &mut self.device
    }

    /// The board's own entries, for the tasks that read them.
    pub fn settings(&self) -> &S {
        &self.settings
    }

    /// The same, for a platform that resets them at a reboot.
    pub fn settings_mut(&mut self) -> &mut S {
        &mut self.settings
    }

    /// Publish what this device's own time is now worth, for `sync.status`.
    pub fn set_time_status(&mut self, code: u8) {
        self.time_status = code;
    }

    /// Power-cycle: a new session, and the clock `dev.uptime` counts from.
    pub fn reboot(&mut self, session: SessionId, now_ns: u64) {
        self.device.reboot(session);
        self.booted_ns = now_ns;
    }

    /// Act on one input, writing what it answers into the lane of the port
    /// that asked and what it announces into the lane every port drains.
    pub fn handle<'p>(
        &mut self,
        streams: &(impl Streams + ?Sized),
        input: Input<'p>,
        now_ns: u64,
        lanes: &mut (impl Lanes + ?Sized),
    ) -> Actions<'p> {
        let since_boot_ns = now_ns.saturating_sub(self.booted_ns);
        let mut actions = Actions {
            call: None,
            deferred: None,
            reboot: false,
            loglevel: self.device.loglevel(),
            log: None,
        };
        match input {
            Input::Loaded(pending, image) => {
                let outcome = image.and_then(|image| self.apply(image, lanes));
                self.settings.publish();
                actions.log = Some(match outcome {
                    Ok(()) => (LogLevel::INFO, "restored the saved configuration"),
                    Err(RpcError::State) => (
                        LogLevel::INFO,
                        "no saved configuration; using compiled defaults",
                    ),
                    Err(_) => (
                        LogLevel::WARNING,
                        "some saved settings could not be restored",
                    ),
                });
                if let Some(pending) = pending {
                    answer(pending, outcome.map(|()| &[][..]), &mut to(lanes));
                }
            }
            Input::Packet(packet) => {
                let handled = self.device.handle(streams, packet, &mut to(lanes));
                if let Handled::Rpc(call) = handled {
                    self.dispatch(call, since_boot_ns, lanes, &mut actions);
                }
            }
        }
        actions.loglevel = self.device.loglevel();
        actions
    }

    /// Answer one RPC the device did not answer itself.
    fn dispatch<'p>(
        &mut self,
        call: Call<'p, 'static>,
        since_boot_ns: u64,
        lanes: &mut (impl Lanes + ?Sized),
        actions: &mut Actions<'p>,
    ) {
        let pending = Pending::of(&call);
        let hw_rev = self.platform.hw_rev.map(u16::to_le_bytes);
        let mut reply = Reply::new();
        let result = match call.name {
            "dev.model" => declared(
                &mut reply,
                call.args,
                self.platform.model.map(str::as_bytes),
            ),
            "dev.mcu.model" => {
                declared(&mut reply, call.args, self.platform.mcu.map(str::as_bytes))
            }
            "dev.uid" => declared(&mut reply, call.args, self.platform.uid),
            "dev.revision" => declared(
                &mut reply,
                call.args,
                hw_rev.as_ref().map(|bytes| &bytes[..]),
            ),
            "dev.systime" => read(&mut reply, call.args, &since_boot_ns.to_le_bytes()),
            "dev.uptime" => {
                let hours = (since_boot_ns / NANOS_PER_HOUR) as u32;
                read(&mut reply, call.args, &hours.to_le_bytes())
            }
            "sync.status" => read(&mut reply, call.args, &[self.time_status]),
            "rpc.match" => table::match_name(self.device.table(), call.args, &mut reply),
            "dev.reboot" => {
                self.device.log(
                    LogLevel::WARNING,
                    0,
                    "rebooting at a host's request",
                    &mut events(lanes),
                );
                actions.reboot = true;
                actions.log = Some((LogLevel::INFO, "reboot requested"));
                Ok(())
            }
            "dev.firmware.upload" => match Reply::from_slice(call.args) {
                Ok(chunk) => {
                    return actions.defer(pending, Deferred::Flash(FlashOp::Upload(chunk)))
                }
                Err(_) => Err(RpcError::ArgsSize),
            },
            "dev.firmware.upgrade" => {
                return actions.defer(pending, Deferred::Flash(FlashOp::Upgrade))
            }
            "dev.firmware.abort" => return actions.defer(pending, Deferred::Flash(FlashOp::Abort)),
            "dev.conf.load" => return actions.defer(pending, Deferred::Flash(FlashOp::ConfLoad)),
            "dev.conf.reset" => return actions.defer(pending, Deferred::Flash(FlashOp::ConfReset)),
            "dev.conf.save" => {
                let mut image = Entries::new();
                match self.save(&mut image) {
                    Ok(()) => {
                        return actions.defer(pending, Deferred::Flash(FlashOp::ConfSave(image)))
                    }
                    Err(error) => Err(error),
                }
            }
            "dev.start" => return actions.defer(pending, Deferred::Acquire(SyncRequest::Start)),
            "dev.stop" => return actions.defer(pending, Deferred::Acquire(SyncRequest::Stop)),
            "dev.restart" => {
                return actions.defer(pending, Deferred::Acquire(SyncRequest::Restart))
            }
            name => match self.entry(name, call.args, &mut reply, lanes) {
                Some(result) => result,
                None => return actions.call = Some(call),
            },
        };
        answer(pending, result.map(|()| reply.as_slice()), &mut to(lanes));
    }

    /// Answer one of the board's own entries, announcing a write to the asking
    /// port ahead of its reply. `None` is a name no entry carries.
    fn entry(
        &mut self,
        name: &str,
        args: &[u8],
        out: &mut Reply,
        lanes: &mut (impl Lanes + ?Sized),
    ) -> Option<Result<(), RpcError>> {
        let Self {
            device, settings, ..
        } = self;
        let mut found = None;
        let mut moved = false;
        settings.entries(&mut |entry| {
            if found.is_some() {
                return;
            }
            match entry {
                Entry::Setting(cell) if cell.name() == name => {
                    found = Some(match cell.rpc(args, out) {
                        Ok(Changed::Changed) => {
                            moved = true;
                            device.announce(name, out, &mut to(&mut *lanes));
                            Ok(())
                        }
                        Ok(Changed::Unchanged) => Ok(()),
                        Err(error) => Err(error),
                    });
                }
                Entry::Reading(reading) if reading.spec().name == name => {
                    found = Some(match args {
                        [] => reading.read(out),
                        _ => Err(RpcError::ReadOnly),
                    });
                }
                Entry::Action(action) if action.spec().name == name => {
                    moved = true;
                    let mut lane = events(&mut *lanes);
                    found = Some(action.run(&mut Announcer {
                        device,
                        events: &mut lane,
                    }));
                }
                Entry::Setting(_) | Entry::Reading(_) | Entry::Action(_) => {}
            }
        });
        if moved {
            settings.publish();
        }
        found
    }

    /// Every persistent cell, as `dev.conf.save` hands them to the flash.
    fn save(&mut self, image: &mut Entries) -> Result<(), RpcError> {
        let mut outcome = Ok(());
        self.settings.entries(&mut |entry| {
            let Entry::Setting(cell) = entry else {
                return;
            };
            if !cell.spec().access.contains(Access::PERSISTENT) {
                return;
            }
            if let Err(error) = conf::encode([cell as &mut dyn Persisted], image) {
                outcome = Err(error);
            }
        });
        outcome
    }

    /// Give every setting the value `image` stores under its name, announcing
    /// each one that moved where every port sees it.
    fn apply(&mut self, image: &[u8], lanes: &mut (impl Lanes + ?Sized)) -> Result<(), RpcError> {
        let Self {
            device, settings, ..
        } = self;
        let mut outcome = Ok(());
        settings.entries(&mut |entry| {
            let Entry::Setting(cell) = entry else {
                return;
            };
            let mut lane = events(&mut *lanes);
            let loaded = conf::load([cell as &mut dyn Persisted], image, |name, reply| {
                device.announce(name, reply, &mut lane)
            });
            if loaded.is_err() {
                outcome = loaded;
            }
        });
        outcome
    }
}

/// Answer a deferred RPC where the request that asked it says.
pub fn answer(pending: Pending<'_>, result: Result<&[u8], RpcError>, to: &mut impl Sink) {
    let mut buf = [0u8; Packet::MAX_SIZE];
    let mut len = match result {
        Ok(value) => wire::write_reply(&mut buf, pending.id, value),
        Err(error) => wire::write_error(&mut buf, pending.id, error),
    }
    .expect("a reply fits a packet");
    for &hop in pending.routing {
        len = twinleaf_proto::route::push_hop(&mut buf, hop).expect("a request's route fits");
    }
    to.send(&buf[..len]);
}

/// A read-only property a platform may not have.
fn declared(out: &mut Reply, args: &[u8], value: Option<&[u8]>) -> Result<(), RpcError> {
    read(out, args, value.ok_or(RpcError::State)?)
}

// The table a device answers: what the device answers itself, then one group
// per stream, then the board's own. A group is a single ordered list, walked
// by handing each entry to a visitor, so an entry may borrow the group it came
// from and still be the only borrow alive.

/// A group of table entries, in the order their ids follow one another.
///
/// Entries are visited one at a time rather than collected, so an entry may
/// borrow the group it came from and still be the only borrow alive. What a
/// task reads is assembled from several cells, so publication is the group's
/// rather than the cell's.
pub trait Group {
    /// Every entry this group adds, in table order.
    fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>));

    /// Hand every task that reads one of these what they now say.
    fn publish(&self) {}
}

/// A device that adds nothing to the standard table and the streams'.
pub struct NoSettings;

impl Group for NoSettings {
    fn entries(&mut self, _visit: &mut dyn FnMut(Entry<'_>)) {}
}

impl<T: Scalar> Group for Setting<T> {
    fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
        visit(Entry::Setting(self));
    }
}

/// One entry a board adds to the RPC table, and what answers it.
pub enum Entry<'a> {
    /// A value a host reads and writes; a write is announced.
    Setting(&'a mut dyn Cell),
    /// A number the device computes when asked.
    Reading(&'a dyn Reading),
    /// Something the device does, which may move several settings.
    Action(&'a mut dyn Action),
}

impl Entry<'_> {
    /// The table entry it declares.
    pub fn spec(&self) -> RpcSpec {
        match self {
            Self::Setting(cell) => cell.spec(),
            Self::Reading(reading) => reading.spec(),
            Self::Action(action) => action.spec(),
        }
    }
}

/// A setting cell the control machine reads, writes, announces, and persists.
pub trait Cell: Persisted {
    /// Its table entry.
    fn spec(&self) -> RpcSpec;

    /// Answer its RPC into `out`, saying whether the value moved.
    fn rpc(&mut self, args: &[u8], out: &mut Reply) -> Result<Changed, RpcError>;
}

impl<T: Scalar> Cell for Setting<T> {
    fn spec(&self) -> RpcSpec {
        Setting::spec(self)
    }

    fn rpc(&mut self, args: &[u8], out: &mut Reply) -> Result<Changed, RpcError> {
        Setting::rpc(self, args, out)
    }
}

/// A read-only number, such as a hub diagnostic.
pub trait Reading {
    /// Its table entry.
    fn spec(&self) -> RpcSpec;

    /// Compute it into `out`.
    fn read(&self, out: &mut Reply) -> Result<(), RpcError>;
}

/// An action, which may move several settings at once, announcing each on the
/// lane every port drains and counting it in `settings.version`.
pub trait Action {
    /// Its table entry.
    fn spec(&self) -> RpcSpec;

    /// Carry it out, announcing every setting it moved through `to`.
    fn run(&mut self, to: &mut Announcer<'_>) -> Result<(), RpcError>;
}

/// Where a setting the device itself moved is announced: the lane every port
/// drains, because one call may move a whole group.
pub struct Announcer<'a> {
    device: &'a mut Device<'static>,
    events: &'a mut dyn Sink,
}

impl Announcer<'_> {
    /// Move a setting, announcing it and counting it in `settings.version`.
    pub fn set<T: Scalar>(&mut self, setting: &mut Setting<T>, value: T) -> Result<(), RpcError> {
        let mut args = Reply::new();
        value.encode(&mut args)?;
        let mut announced = Reply::new();
        if setting.rpc(&args, &mut announced)? == Changed::Changed {
            self.device
                .announce(setting.name(), &announced, &mut self.events);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::Identity;
    use crate::rpc::{put, Kind, STANDARD};
    use crate::segments::Params;
    use crate::stream::{ColumnDef, Stream, StreamDef};
    use core::num::NonZeroU32;
    use twinleaf_proto::data::{MetadataReply, MetadataType};
    use twinleaf_proto::packet::{Header, PacketType};
    use twinleaf_proto::rpc::{Answer, Method};
    use twinleaf_proto::settings::Setting as Announcement;
    use twinleaf_proto::StreamId;

    const SESSION: SessionId = SessionId::new(0xBAE5_B410);
    /// A round second, so `dev.systime` counts from something visible.
    const BOOT: u64 = 1_800_000_000_000_000_000;

    static AUX: StreamDef = StreamDef {
        name: "aux",
        columns: &[ColumnDef {
            name: "vbus",
            units: "V",
            data_type: twinleaf_proto::data::DataType::F32,
            description: "Input voltage",
        }],
    };

    /// A value the device computes when asked, and the action that puts the
    /// setting behind it back.
    struct Counter {
        value: Setting<u32>,
    }

    impl Group for Counter {
        fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
            visit(Entry::Setting(&mut self.value));
            visit(Entry::Reading(&Doubled(self.value.get())));
            visit(Entry::Action(&mut Rewind(self)));
        }
    }

    struct Doubled(u32);

    impl Reading for Doubled {
        fn spec(&self) -> RpcSpec {
            RpcSpec::prop("board.doubled", Kind::Uint(4), Access::READ)
        }

        fn read(&self, out: &mut Reply) -> Result<(), RpcError> {
            put(out, &self.0.wrapping_mul(2).to_le_bytes())
        }
    }

    struct Rewind<'a>(&'a mut Counter);

    impl Action for Rewind<'_> {
        fn spec(&self) -> RpcSpec {
            RpcSpec::action("board.rewind")
        }

        fn run(&mut self, to: &mut Announcer<'_>) -> Result<(), RpcError> {
            to.set(&mut self.0.value, 0)
        }
    }

    /// Two plain settings, then a group of its own: the whole vocabulary.
    struct Board {
        first: Setting<u32>,
        second: Setting<u32>,
        counter: Counter,
        published: core::cell::Cell<u32>,
    }

    impl Board {
        fn new() -> Self {
            Self {
                first: Setting::new("board.first", 7).persistent(),
                second: Setting::new("board.second", 0).persistent(),
                counter: Counter {
                    value: Setting::new("board.count", 5).persistent(),
                },
                published: core::cell::Cell::new(0),
            }
        }
    }

    impl Group for Board {
        fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
            Group::entries(&mut self.first, visit);
            Group::entries(&mut self.second, visit);
            Group::entries(&mut self.counter, visit);
        }

        fn publish(&self) {
            self.published.set(self.published.get() + 1);
        }
    }

    /// The names a board adds after the standard table.
    const BOARD_NAMES: &[&str] = &[
        "board.first",
        "board.second",
        "board.count",
        "board.doubled",
        "board.rewind",
    ];

    /// Every packet one lane took.
    #[derive(Default)]
    struct Sent(Vec<Vec<u8>>);

    impl Sink for Sent {
        fn send(&mut self, packet: &[u8]) {
            self.0.push(packet.to_vec());
        }
    }

    impl Sent {
        /// What the last packet said, as a host reads it.
        fn answer(&self) -> Result<&[u8], RpcError> {
            let [.., reply] = &self.0[..] else {
                panic!("nothing was answered");
            };
            let header = Header::parse_prefix(reply).unwrap();
            match Answer::parse(header.ptype, &reply[header.payload_range()]) {
                Some(Answer::Reply(reply)) => Ok(reply.value),
                Some(Answer::Error(error)) => Err(error.error()),
                None => panic!("neither a reply nor an error"),
            }
        }

        fn value(&self) -> &[u8] {
            self.answer().expect("a reply, not an error")
        }

        fn error(&self) -> RpcError {
            self.answer().expect_err("an error, not a reply")
        }

        /// The settings announced, in order.
        fn announced(&self) -> Vec<(String, Vec<u8>)> {
            self.0
                .iter()
                .map(|packet| {
                    Header::parse_prefix(packet)
                        .map(|header| (header, packet))
                        .unwrap()
                })
                .filter(|(header, _)| header.ptype == PacketType::SETTING)
                .map(|(header, packet)| {
                    Announcement::parse(&packet[header.payload_range()]).unwrap()
                })
                .map(|setting| {
                    (
                        setting.name_str().unwrap().to_owned(),
                        setting.reply.to_vec(),
                    )
                })
                .collect()
        }
    }

    /// The two lanes a port has.
    #[derive(Default)]
    struct Both {
        to: Sent,
        events: Sent,
    }

    impl Lanes for Both {
        fn reply(&mut self, packet: &[u8]) {
            self.to.send(packet);
        }

        fn event(&mut self, packet: &[u8]) {
            self.events.send(packet);
        }
    }

    /// One control machine, the streams it describes, and the two lanes.
    struct Harness {
        control: Control<Board>,
        streams: [Stream<4>; 1],
        lanes: Both,
        reboot: bool,
        deferred: Option<(RpcRequestId, Deferred)>,
        logged: Vec<(LogLevel, &'static str)>,
        loglevel: LogLevel,
    }

    fn table() -> &'static [RpcSpec] {
        let mut specs: Vec<RpcSpec> = STANDARD.to_vec();
        Board::new().entries(&mut |entry| specs.push(entry.spec()));
        Box::leak(specs.into_boxed_slice())
    }

    fn harness() -> Harness {
        let identity = Identity::new(
            "COMM-USB",
            "Twinleaf COMM-USB R8 (52373720) [2026-07-24/abc123-DEV]",
            "52373720",
            "2026-07-24/abc123-DEV",
        )
        .unwrap();
        let params = Params {
            rate: NonZeroU32::new(10).unwrap(),
            decimation: NonZeroU32::new(4).unwrap(),
            cutoff: 0.0,
            enabled: true,
        };
        Harness {
            control: Control::new(
                Device::new(identity, SESSION, table()),
                Board::new(),
                Platform {
                    model: Some("COMM-USB"),
                    mcu: Some("STM32G484xE"),
                    uid: Some(&[0xAB; 12]),
                    hw_rev: Some(8),
                },
                BOOT,
            ),
            streams: [Stream::new(StreamId::new(1), &AUX, params).unwrap()],
            lanes: Both::default(),
            reboot: false,
            deferred: None,
            logged: Vec::new(),
            loglevel: crate::device::DEFAULT_LOGLEVEL,
        }
    }

    /// One request by name, as `tio-tool` sends it.
    fn asking(name: &str, args: &[u8]) -> Vec<u8> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = wire::write_request(
            &mut buf,
            RpcRequestId::new(7),
            Method::ByName(name.as_bytes()),
            args,
        )
        .unwrap();
        buf[..len].to_vec()
    }

    impl Harness {
        /// Drive one input, carrying out what comes back the way a platform
        /// does: a name no entry answers is `NotFound`.
        fn drive(&mut self, input: Input<'_>, now_ns: u64) {
            let Harness {
                control,
                streams,
                lanes,
                reboot,
                deferred,
                logged,
                loglevel,
            } = self;
            let Actions {
                call,
                deferred: handed,
                reboot: restart,
                loglevel: threshold,
                log,
            } = control.handle(&streams[..], input, now_ns, lanes);
            if let Some(call) = call {
                answer(Pending::of(&call), Err(RpcError::NotFound), &mut lanes.to);
            }
            *deferred = handed.map(|(pending, work)| (pending.id, work));
            *reboot = restart;
            *loglevel = threshold;
            logged.extend(log);
        }

        /// Ask by name, and collect what came back on the port's own lane.
        fn ask(&mut self, name: &str, args: &[u8]) -> &Sent {
            self.ask_at(name, args, BOOT)
        }

        fn ask_at(&mut self, name: &str, args: &[u8], now_ns: u64) -> &Sent {
            self.lanes.to = Sent::default();
            let packet = asking(name, args);
            let (view, _) = PacketView::parse_prefix(&packet).unwrap();
            self.drive(Input::Packet(view), now_ns);
            &self.lanes.to
        }

        fn table(&self) -> &'static [RpcSpec] {
            self.control.device().table()
        }
    }

    #[test]
    fn answers_identity_rpcs_by_name() {
        let mut device = harness();
        assert_eq!(device.ask("dev.name", &[]).value(), b"COMM-USB");
        assert_eq!(device.ask("dev.serial", &[]).value(), b"52373720");
        assert_eq!(
            device.ask("dev.session", &[]).value(),
            &SESSION.to_le_bytes()
        );
    }

    /// The half of the identity the platform holds rather than the device.
    #[test]
    fn answers_the_platforms_half_of_the_identity() {
        let mut device = harness();
        assert_eq!(device.ask("dev.uid", &[]).value(), &[0xAB; 12]);
        assert_eq!(device.ask("dev.model", &[]).value(), b"COMM-USB");
        assert_eq!(device.ask("dev.mcu.model", &[]).value(), b"STM32G484xE");
        assert_eq!(device.ask("dev.revision", &[]).value(), &8u16.to_le_bytes());
        assert_eq!(
            device.ask("dev.mcu.model", b"STM32F4").error(),
            RpcError::ReadOnly,
            "the part number is not settable"
        );
    }

    /// A platform that says nothing about its hardware answers the state
    /// error, as it does for any standard RPC it does not implement.
    #[test]
    fn hardware_a_platform_does_not_name_is_the_state_error() {
        let mut device = harness();
        device.control = Control::new(
            Device::new(
                Identity::new("d", "d", "S", "fw").unwrap(),
                SESSION,
                table(),
            ),
            Board::new(),
            Platform::UNKNOWN,
            BOOT,
        );
        for name in ["dev.uid", "dev.model", "dev.mcu.model", "dev.revision"] {
            assert_eq!(device.ask(name, &[]).error(), RpcError::State, "{name}");
        }
    }

    #[test]
    fn systime_and_uptime_count_from_the_first_call_and_refuse_a_write() {
        let mut device = harness();
        assert_eq!(
            device.ask_at("dev.systime", &[], BOOT).value(),
            &0u64.to_le_bytes()
        );
        let later = BOOT + 2 * 3_600_000_000_000 + 1_500_000_000;
        assert_eq!(
            device.ask_at("dev.systime", &[], later).value(),
            &(later - BOOT).to_le_bytes()
        );
        assert_eq!(
            device.ask_at("dev.uptime", &[], later).value(),
            &2u32.to_le_bytes()
        );
        assert_eq!(device.ask("dev.systime", b"0").error(), RpcError::ReadOnly);
        assert_eq!(device.ask("dev.uptime", b"0").error(), RpcError::ReadOnly);
    }

    #[test]
    fn rpc_list_reports_the_method_count() {
        let mut device = harness();
        let count = u16::from_le_bytes(device.ask("rpc.list", &[]).value().try_into().unwrap());
        assert_eq!(count as usize, device.table().len());
    }

    #[test]
    fn rpc_match_completes_over_the_whole_table() {
        let mut device = harness();
        assert_eq!(device.ask("rpc.match", b"board.f").value(), b"board.first");
    }

    #[test]
    fn an_unknown_method_is_an_rpc_error_not_a_dropped_packet() {
        let mut device = harness();
        assert_eq!(device.ask("dev.nonsense", &[]).error(), RpcError::NotFound);
    }

    #[test]
    fn writing_a_read_only_rpc_is_rejected() {
        let mut device = harness();
        assert_eq!(device.ask("dev.name", b"nope").error(), RpcError::ReadOnly);
    }

    /// The bootstrap reply a host reads a stream's shape from.
    #[test]
    fn metadata_describes_the_segment_ring() {
        let mut device = harness();
        let answered = device.ask("dev.metadata", &[]);
        let records: Vec<(MetadataType, &[u8])> = MetadataReply::parse(answered.value())
            .expect("a well-framed reply")
            .collect();
        let types: Vec<MetadataType> = records.iter().map(|(mtype, _)| *mtype).collect();
        assert_eq!(
            types,
            [
                MetadataType::Device,
                MetadataType::Stream,
                MetadataType::Segment,
                MetadataType::Column
            ]
        );

        let stream = twinleaf_proto::data::Stream::parse(records[1].1).unwrap();
        assert_eq!(stream.name, "aux");
        assert_eq!(stream.n_segments, 4);
        let segment = twinleaf_proto::data::Segment::parse(records[2].1).unwrap();
        assert_eq!(segment.decimation, 4, "the declared default");
        assert_eq!(segment.sampling_rate, 10);
    }

    /// tl-chibi's `dev.loglevel` (`lib/tlfirmware.c:766-773`): a write reaches
    /// the gate the platform's own logging is checked against.
    #[test]
    fn writing_dev_loglevel_carries_the_threshold_back_to_the_platform() {
        let mut device = harness();
        assert_eq!(
            device.ask("dev.loglevel", &[]).value(),
            [LogLevel::INFO.value()]
        );
        assert_eq!(device.loglevel, LogLevel::INFO);
        device.ask("dev.loglevel", &[LogLevel::DEBUG.value()]);
        assert_eq!(device.loglevel, LogLevel::DEBUG);
    }

    #[test]
    fn reboot_replies_before_asking_the_port_to_restart() {
        let mut device = harness();
        let answered = device.ask("dev.reboot", &[]);
        assert!(
            answered.value().is_empty(),
            "an action RPC returns no value"
        );
        assert!(device.reboot);
        assert_eq!(
            device.lanes.events.0.len(),
            1,
            "a host that asked for a reboot is told why the device went away"
        );

        // With arguments it is a malformed call, not a restart.
        let answered = device.ask("dev.reboot", b"now");
        assert_eq!(answered.error(), RpcError::ArgsSize);
        assert!(!device.reboot);
    }

    /// A packet the machine never answers proves nothing and says nothing.
    #[test]
    fn traffic_that_is_not_a_request_is_ignored() {
        let mut device = harness();
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = twinleaf_proto::heartbeat::Heartbeat::Session(SESSION)
            .write(&mut buf)
            .unwrap();
        let (view, _) = PacketView::parse_prefix(&buf[..len]).unwrap();
        device.drive(Input::Packet(view), BOOT);
        assert!(device.lanes.to.0.is_empty());
    }

    /// Ids are positions in the table and are on the wire, so the standard
    /// entries come first and the board's own follow.
    #[test]
    fn board_entries_follow_the_standard_table() {
        let device = harness();
        let names: Vec<&str> = device.table().iter().map(|spec| spec.name).collect();
        assert_eq!(names[0], "dev.metadata", "the standard table comes first");
        assert_eq!(names[STANDARD.len() - 1], "sync.status");
        assert_eq!(&names[STANDARD.len()..], BOARD_NAMES);
    }

    #[test]
    fn a_board_setting_reads_and_writes_its_own_state() {
        let mut device = harness();
        assert_eq!(device.ask("board.second", &[]).value(), &0u32.to_le_bytes());
        assert_eq!(
            device.ask("board.second", &42u32.to_le_bytes()).value(),
            &42u32.to_le_bytes(),
            "a write answers with the value it left"
        );
        assert_eq!(
            device.ask("board.second", &[]).value(),
            &42u32.to_le_bytes()
        );
        // The neighbouring entry is untouched, so dispatch reached the right one.
        assert_eq!(device.ask("board.first", &[]).value(), &7u32.to_le_bytes());
    }

    /// A write reaches the host as the SETTING its own RPC replies, ahead of
    /// that reply, and it is counted in `settings.version`.
    #[test]
    fn writing_a_setting_announces_it_to_the_port_that_asked_and_counts_it() {
        let mut device = harness();
        let version = |device: &mut Harness| {
            u32::from_le_bytes(
                device
                    .ask("settings.version", &[])
                    .value()
                    .try_into()
                    .unwrap(),
            )
        };
        assert_eq!(version(&mut device), 0);

        let answered = device.ask("board.second", &42u32.to_le_bytes());
        assert_eq!(answered.0.len(), 2, "an announcement, then the reply");
        assert_eq!(
            answered.announced(),
            [("board.second".to_owned(), 42u32.to_le_bytes().to_vec())]
        );
        assert!(
            device.lanes.events.0.is_empty(),
            "a write one port asked for is not an event"
        );
        assert_eq!(version(&mut device), 1, "the write is counted");

        // Reading it back neither announces nor counts.
        assert_eq!(device.ask("board.second", &[]).0.len(), 1);
        assert_eq!(version(&mut device), 1);
    }

    #[test]
    fn a_reading_is_computed_when_asked_and_refuses_a_write() {
        let mut device = harness();
        assert_eq!(
            device.ask("board.doubled", &[]).value(),
            &10u32.to_le_bytes()
        );
        device.ask("board.count", &21u32.to_le_bytes());
        assert_eq!(
            device.ask("board.doubled", &[]).value(),
            &42u32.to_le_bytes()
        );
        assert_eq!(
            device.ask("board.doubled", &[1]).error(),
            RpcError::ReadOnly
        );
    }

    /// An action may move a whole group, so what it moved goes on the lane
    /// every port drains rather than into one reply.
    #[test]
    fn an_action_announces_what_it_moved_where_every_port_sees_it() {
        let mut device = harness();
        device.ask("board.count", &21u32.to_le_bytes());
        device.lanes.events = Sent::default();

        let answered = device.ask("board.rewind", &[]);
        assert_eq!(answered.0.len(), 1, "only the reply went to the port");
        assert!(answered.value().is_empty());
        assert_eq!(
            device.lanes.events.announced(),
            [("board.count".to_owned(), 0u32.to_le_bytes().to_vec())]
        );
        assert_eq!(device.ask("board.count", &[]).value(), &0u32.to_le_bytes());
    }

    /// A move is published to the tasks that read it, and a read is not.
    #[test]
    fn a_move_publishes_the_group_and_a_read_does_not() {
        let mut device = harness();
        let published = |device: &Harness| device.control.settings().published.get();
        device.ask("board.first", &[]);
        assert_eq!(published(&device), 0);
        device.ask("board.first", &1u32.to_le_bytes());
        assert_eq!(published(&device), 1);
        device.ask("board.rewind", &[]);
        assert_eq!(published(&device), 2);
    }

    /// A name the table declares and no entry answers comes back for the
    /// platform to answer, which is what lets one keep an RPC of its own.
    #[test]
    fn a_name_no_entry_answers_comes_back_to_the_platform() {
        let mut specs: Vec<RpcSpec> = table().to_vec();
        specs.push(RpcSpec::std("board.capture", Access::READ));
        let mut device = harness();
        device.control = Control::new(
            Device::new(
                Identity::new("d", "d", "S", "fw").unwrap(),
                SESSION,
                Box::leak(specs.into_boxed_slice()),
            ),
            Board::new(),
            Platform::UNKNOWN,
            BOOT,
        );
        assert_eq!(
            device.ask("board.capture", &[]).error(),
            RpcError::NotFound,
            "the harness answers what it is handed back"
        );
    }

    /// The walk that declares the table and the walk that answers a name are
    /// the same walk, so a name declared and never answered is a table entry a
    /// host cannot call.
    #[test]
    fn every_name_declared_is_one_that_is_answered() {
        let mut device = harness();
        for name in BOARD_NAMES {
            assert_ne!(
                device.ask(name, &[]).answer().err(),
                Some(RpcError::NotFound),
                "{name} is declared and not answered"
            );
        }
    }

    /// `tio upgrade` sends these by name; each must reach the flash with the
    /// request that asked, not be answered here.
    #[test]
    fn flash_and_configuration_entries_reach_the_flash() {
        let mut device = harness();
        for name in [
            "dev.firmware.upload",
            "dev.firmware.upgrade",
            "dev.firmware.abort",
            "dev.conf.save",
            "dev.conf.load",
            "dev.conf.reset",
        ] {
            let answered = device.ask(name, &[]);
            assert!(answered.0.is_empty(), "{name} was answered here");
            let (id, work) = device.deferred.take().expect(name);
            assert_eq!(id, RpcRequestId::new(7));
            assert!(matches!(work, Deferred::Flash(_)), "{name}");
        }
    }

    #[test]
    fn acquisition_entries_reach_whatever_owns_the_acquisition() {
        let mut device = harness();
        for (name, expected) in [
            ("dev.start", SyncRequest::Start),
            ("dev.stop", SyncRequest::Stop),
            ("dev.restart", SyncRequest::Restart),
        ] {
            let answered = device.ask(name, &[]);
            assert!(answered.0.is_empty(), "{name} was answered here");
            let Some((_, Deferred::Acquire(request))) = device.deferred.take() else {
                panic!("{name} did not reach the acquisition");
            };
            assert_eq!(request, expected);
        }
    }

    /// `sync.status` is whatever the platform last published, not a deferral.
    #[test]
    fn sync_status_answers_what_the_platform_published() {
        let mut device = harness();
        assert_eq!(device.ask("sync.status", &[]).value(), [0]);
        device.control.set_time_status(3);
        assert_eq!(device.ask("sync.status", &[]).value(), [3]);
        assert_eq!(device.ask("sync.status", &[1]).error(), RpcError::ReadOnly);
    }

    #[test]
    fn dev_conf_save_hands_the_flash_every_persistent_value() {
        let mut device = harness();
        device.ask("board.second", &42u32.to_le_bytes());
        device.ask("dev.conf.save", &[]);

        let Some((_, Deferred::Flash(FlashOp::ConfSave(image)))) = device.deferred.take() else {
            panic!("a save carries the image");
        };
        let stored: Vec<(&[u8], &[u8])> = conf::entries(&image)
            .map(|entry| entry.unwrap())
            .map(|entry| (entry.name, entry.value))
            .collect();
        let names: Vec<&[u8]> = stored.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            names,
            [&b"board.first"[..], b"board.second", b"board.count"],
            "every persistent cell, in table order"
        );
        assert_eq!(stored[1].1, 42u32.to_le_bytes());
    }

    /// The boot restore: every stored value is taken, announced where every
    /// port sees it, and handed to the tasks that read it.
    #[test]
    fn a_restore_moves_every_stored_setting_and_publishes_it() {
        let mut device = harness();
        let mut image = Entries::new();
        let mut stored = Setting::new("board.second", 42u32).persistent();
        conf::encode([&mut stored as &mut dyn Persisted], &mut image).unwrap();

        device.drive(Input::Loaded(None, Ok(&image)), BOOT);
        assert_eq!(
            device.lanes.events.announced(),
            [("board.second".to_owned(), 42u32.to_le_bytes().to_vec())]
        );
        assert!(
            device.lanes.to.0.is_empty(),
            "nothing asked for the restore"
        );
        assert_eq!(
            device.logged,
            [(LogLevel::INFO, "restored the saved configuration")]
        );
        assert_eq!(device.control.settings().published.get(), 1);
        assert_eq!(
            device.ask("board.second", &[]).value(),
            &42u32.to_le_bytes()
        );
    }

    /// A blank device owes its tasks the compiled defaults just the same.
    #[test]
    fn a_device_with_nothing_stored_keeps_its_compiled_defaults() {
        let mut device = harness();
        device.drive(Input::Loaded(None, Err(RpcError::State)), BOOT);
        assert!(device.lanes.events.0.is_empty());
        assert_eq!(
            device.logged,
            [(
                LogLevel::INFO,
                "no saved configuration; using compiled defaults"
            )]
        );
        assert_eq!(device.control.settings().published.get(), 1);
        assert_eq!(device.ask("board.first", &[]).value(), &7u32.to_le_bytes());
    }

    /// `dev.conf.load` is two hops: the platform reads the slot, the machine
    /// applies what it names, announces it, and only then replies.
    #[test]
    fn a_loaded_configuration_is_applied_and_then_answered() {
        let mut device = harness();
        let mut image = Entries::new();
        let mut stored = Setting::new("board.first", 99u32).persistent();
        conf::encode([&mut stored as &mut dyn Persisted], &mut image).unwrap();

        let pending = Pending::local(RpcRequestId::new(7));
        device.drive(Input::Loaded(Some(pending), Ok(&image)), BOOT);
        assert!(
            device.lanes.to.answer().expect("a reply").is_empty(),
            "an action RPC returns no value"
        );
        assert_eq!(
            device.lanes.events.announced(),
            [("board.first".to_owned(), 99u32.to_le_bytes().to_vec())]
        );
        assert_eq!(device.ask("board.first", &[]).value(), &99u32.to_le_bytes());
    }

    /// A stored value a setting refuses leaves the load error, and every
    /// other stored value is still in force.
    #[test]
    fn a_configuration_that_does_not_load_is_still_applied_as_far_as_it_goes() {
        let mut device = harness();
        let refused = [&[11u8, 1][..], b"board.second", &[3]].concat();
        device.drive(
            Input::Loaded(Some(Pending::local(RpcRequestId::new(7))), Ok(&refused)),
            BOOT,
        );
        assert_eq!(device.lanes.to.error(), RpcError::Load);
        assert_eq!(
            device.logged,
            [(
                LogLevel::WARNING,
                "some saved settings could not be restored"
            )]
        );
    }
}

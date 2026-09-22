//! A device: who it is, the table it answers, and the machine that answers it.
//!
//! [`Device`] owns its identity, its RPC table, and the board's own entries,
//! and nothing else: no clock, no executor, no lanes. A platform hands it one
//! [`Input`] with the monotonic nanosecond it happened at, and gets back the
//! packets it wrote into the two sinks — the lane of the port that asked, and
//! the lane every port drains — and an [`Actions`] naming every decision it
//! cannot carry out itself.
//!
//! A request is resolved to a table position once. A position the standard
//! table holds is a [`Std`] the device answers itself; a position past it is
//! the board's own, and a [`Group`] is walked to it. A board's own entries are
//! one ordered list read three ways: as the table's order, as the persist
//! list, and as the position a request reaches.

use heapless::String;
use twinleaf_proto::data::{self, MetadataFlags};
use twinleaf_proto::heartbeat::Heartbeat;
use twinleaf_proto::log::{LogLevel, LogMessage, MAX_MESSAGE_SIZE};
use twinleaf_proto::packet::{Packet, PacketType, PacketView};
use twinleaf_proto::route;
use twinleaf_proto::rpc::{self, Method, Request, RpcError};
use twinleaf_proto::settings::Setting as Announcement;
use twinleaf_proto::{RpcRequestId, SessionId};

use crate::data::metadata::{self, Streams};
use crate::rpc::{self as table, put, read, Access, Reply, RpcSpec, Std, STANDARD};
use crate::rpc::{Changed, Persisted, Scalar, Setting, Text};
use crate::storage::conf::{self, Image};
use crate::Sink;

/// Nanoseconds between heartbeats.
pub const HEARTBEAT_INTERVAL: u64 = 200_000_000;

/// Nanoseconds a changed configuration waits before it saves itself, as
/// tl-chibi's `dev.conf.autosave` defaults to.
pub const AUTOSAVE_INTERVAL: u64 = 60_000_000_000;

/// The level `dev.loglevel` boots at, as tl-chibi's `logThreshold` does.
pub const DEFAULT_LOGLEVEL: LogLevel = LogLevel::INFO;

/// As much flash as a stored configuration may take.
pub const IMAGE_MAX: usize = 1024;

/// As big a cell as `dev.priv.password` keeps, NUL and all, as tl-chibi's is.
pub const PASSWORD_MAX: usize = 16;

/// The bytes a stored configuration is built in and read back into.
pub type Entries = Image<IMAGE_MAX>;

/// Nanoseconds in the hour `dev.uptime` counts.
const NANOS_PER_HOUR: u64 = 3_600_000_000_000;

/// What a device says it is: every `dev.*` identity RPC, and the device record.
///
/// A hardware fact left `None` answers [`RpcError::State`], as any standard
/// RPC a platform does not implement does.
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
    /// `dev.model`.
    pub model: Option<&'static str>,
    /// `dev.mcu.model`.
    pub mcu: Option<&'static str>,
    /// `dev.uid`.
    pub uid: Option<&'static [u8]>,
    /// `dev.revision`.
    pub hw_rev: Option<u16>,
}

impl Identity {
    /// An identity naming no hardware, if every string fits its field.
    pub fn new(name: &str, desc: &str, serial: &str, firmware: &str) -> Option<Self> {
        Some(Self {
            name: name.try_into().ok()?,
            desc: desc.try_into().ok()?,
            serial: serial.try_into().ok()?,
            firmware: firmware.try_into().ok()?,
            model: None,
            mcu: None,
            uid: None,
            hw_rev: None,
        })
    }

    /// The same identity, with the hardware a platform reads out of its board.
    pub fn hardware(
        self,
        model: &'static str,
        mcu: &'static str,
        uid: &'static [u8],
        hw_rev: u16,
    ) -> Self {
        Self {
            model: Some(model),
            mcu: Some(mcu),
            uid: Some(uid),
            hw_rev: Some(hw_rev),
            ..self
        }
    }
}

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

impl Pending<'_> {
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

/// An RPC whose answer comes from something the device does not own.
#[allow(clippy::large_enum_variant)]
pub enum Deferred {
    /// The flash: `dev.firmware.*` and `dev.conf.*`.
    Flash(FlashOp),
    /// The acquisition: `dev.start`, `dev.stop`, and `dev.restart`.
    Acquire(SyncRequest),
}

/// What the device decided and the platform carries out.
///
/// Every field is consumed once, so a platform takes it apart in one `let`
/// rather than reaching into it.
pub struct Actions<'p> {
    /// An RPC no table entry answers, for the platform to answer itself: who
    /// to answer, the name the table declares, and the argument bytes.
    pub call: Option<(Pending<'p>, &'static str, &'p [u8])>,
    /// An RPC handed to whoever answers it, with who to answer.
    pub deferred: Option<(Pending<'p>, Deferred)>,
    /// The reply is written: flush the port that asked, then reset.
    pub reboot: bool,
    /// What the platform's own logging is gated on from here on.
    pub loglevel: LogLevel,
    /// What the device has to say on the platform's own log.
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

/// One lane, as the device writes packets into it.
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

/// The one owner of a device and its settings.
pub struct Device<S: Group> {
    /// Who the device is.
    pub identity: Identity,
    /// Which boot this is.
    pub session: SessionId,
    table: &'static [RpcSpec],
    /// `rpc.hash` over the whole table, and over what a locked session sees.
    full_hash: u32,
    public_hash: u32,
    /// Whether `dev.priv` has unlocked this session: not persisted, and false
    /// at every boot.
    privileged: bool,
    /// The password `dev.priv` takes, which is never announced or broadcast.
    password: Setting<Text<PASSWORD_MAX>>,
    settings: S,
    next_beat: u64,
    /// The nanosecond the device came up, which `dev.systime` and `dev.uptime`
    /// count from.
    booted_ns: u64,
    /// Whole powered-on hours restored once at boot.
    lifetime_hours: u32,
    loglevel: Setting<u8>,
    settings_version: u32,
    /// When a persistent cell last moved with no save since.
    dirty_since: Option<u64>,
    /// What `sync.status` answers, as the platform last published it.
    time_status: u8,
}

impl<S: Group> Device<S> {
    /// A device come up at `now_ns`, answering `table`: [`STANDARD`] first,
    /// then what `settings` declares, and `dev.priv` taking `password` until
    /// `dev.priv.password` overrides it.
    pub fn new(
        identity: Identity,
        session: SessionId,
        table: &'static [RpcSpec],
        settings: S,
        password: &str,
        now_ns: u64,
    ) -> Self {
        Self {
            identity,
            session,
            table,
            full_hash: table::hash(table, true),
            public_hash: table::hash(table, false),
            privileged: false,
            password: Setting::new("dev.priv.password", Text::from(password)).persistent(),
            settings,
            next_beat: 0,
            booted_ns: now_ns,
            lifetime_hours: 0,
            loglevel: Setting::new("dev.loglevel", DEFAULT_LOGLEVEL.value()),
            settings_version: 0,
            dirty_since: None,
            time_status: 0,
        }
    }

    /// Restores lifetime powered-on hours without changing the current boot clock.
    pub fn restore_uptime(&mut self, hours: u32) {
        self.lifetime_hours = hours;
    }

    /// The RPC table.
    pub fn table(&self) -> &'static [RpcSpec] {
        self.table
    }

    /// `rpc.hash`, over the entries this session sees.
    pub fn hash(&self) -> u32 {
        match self.privileged {
            true => self.full_hash,
            false => self.public_hash,
        }
    }

    /// The board's own entries, for the tasks that read them.
    pub fn settings(&self) -> &S {
        &self.settings
    }

    /// The same, for a platform that resets them at a reboot.
    pub fn settings_mut(&mut self) -> &mut S {
        &mut self.settings
    }

    /// The threshold `dev.loglevel` holds, for a runtime that gates its own logging on it.
    pub fn loglevel(&self) -> LogLevel {
        LogLevel::new(self.loglevel.get())
    }

    /// Publish what this device's own time is now worth, for `sync.status`.
    pub fn set_time_status(&mut self, code: u8) {
        self.time_status = code;
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
        announcement(out, "rpc.hash", &self.hash().to_le_bytes());
        self.beat(now_ns, out);
        let record = self.record(streams);
        let mut sweep = metadata::Sweep::new();
        while let Some((item, last)) = sweep.step(record, streams) {
            let flags = match last {
                true => MetadataFlags::UPDATE | MetadataFlags::LAST,
                false => MetadataFlags::UPDATE,
            };
            send(out, &[], |buf| item.write(flags, buf));
            if last {
                return;
            }
        }
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
        let mut actions = Actions {
            call: None,
            deferred: None,
            reboot: false,
            loglevel: self.loglevel(),
            log: None,
        };
        match input {
            Input::Loaded(pending, image) => {
                let outcome = image.and_then(|image| self.restore(image, lanes));
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
            Input::Packet(packet) if packet.header.ptype == PacketType::RPC_REQ => {
                self.request(streams, packet, now_ns, lanes, &mut actions)
            }
            Input::Packet(_) => {}
        }
        actions.loglevel = self.loglevel();
        actions
    }

    /// Answer one request, resolved to a table position once. A method the
    /// table declares an action refuses an argument, as libtio firmware does.
    fn request<'p>(
        &mut self,
        streams: &(impl Streams + ?Sized),
        packet: PacketView<'p>,
        now_ns: u64,
        lanes: &mut (impl Lanes + ?Sized),
        actions: &mut Actions<'p>,
    ) {
        let routing = packet.routing;
        let Some(request) = Request::parse(packet.payload) else {
            let pending = Pending {
                id: RpcRequestId::new(0),
                routing,
            };
            return answer(pending, Err(RpcError::Malformed), &mut to(lanes));
        };
        let pending = Pending {
            id: request.id,
            routing,
        };
        let table = self.table;
        let index = match request.method {
            Method::ById(id) => Some(usize::from(id.value())).filter(|&i| i < table.len()),
            Method::ByName(name) => table.iter().position(|spec| spec.name.as_bytes() == name),
        };
        let Some(index) =
            index.filter(|&index| self.privileged || !table[index].access.is_privileged())
        else {
            return answer(pending, Err(RpcError::NotFound), &mut to(lanes));
        };
        let spec = &table[index];
        let args = request.args;
        if spec.method == table::Method::Action && !args.is_empty() {
            return answer(pending, Err(RpcError::ArgsSize), &mut to(lanes));
        }
        let since_boot_ns = now_ns.saturating_sub(self.booted_ns);
        let hw_rev = self.identity.hw_rev.map(u16::to_le_bytes);
        let mut reply = Reply::new();
        let result = match Std::at(index) {
            None => match self.entry(index - STANDARD.len(), args, now_ns, &mut reply, lanes) {
                Some(result) => result,
                None => return actions.call = Some((pending, spec.name, args)),
            },
            Some(std) => match std {
                Std::Metadata => metadata::reply(self.record(streams), streams, args, &mut reply),
                Std::Systime => read(&mut reply, args, &since_boot_ns.to_le_bytes()),
                Std::Reboot => {
                    self.log(
                        LogLevel::WARNING,
                        0,
                        "rebooting at a host's request",
                        &mut events(lanes),
                    );
                    actions.reboot = true;
                    actions.log = Some((LogLevel::INFO, "reboot requested"));
                    Ok(())
                }
                Std::Loglevel => write(
                    &mut self.settings_version,
                    &mut self.loglevel,
                    args,
                    &mut reply,
                    &mut to(lanes),
                ),
                Std::Name => read(&mut reply, args, self.identity.name.as_bytes()),
                Std::Model => declared(&mut reply, args, self.identity.model.map(str::as_bytes)),
                Std::Uid => declared(&mut reply, args, self.identity.uid),
                Std::Serial => read(&mut reply, args, self.identity.serial.as_bytes()),
                Std::Revision => {
                    declared(&mut reply, args, hw_rev.as_ref().map(|bytes| &bytes[..]))
                }
                Std::Desc => read(&mut reply, args, self.identity.desc.as_bytes()),
                Std::Session => read(&mut reply, args, &self.session.to_le_bytes()),
                Std::Mcu => declared(&mut reply, args, self.identity.mcu.map(str::as_bytes)),
                Std::FirmwareSerial => read(&mut reply, args, self.identity.firmware.as_bytes()),
                Std::ConfLoad => return actions.defer(pending, Deferred::Flash(FlashOp::ConfLoad)),
                Std::ConfSave => {
                    let mut image = Entries::new();
                    match self.save(&mut image) {
                        Ok(()) => {
                            self.dirty_since = None;
                            return actions
                                .defer(pending, Deferred::Flash(FlashOp::ConfSave(image)));
                        }
                        Err(error) => Err(error),
                    }
                }
                Std::ConfReset => {
                    return actions.defer(pending, Deferred::Flash(FlashOp::ConfReset))
                }
                Std::Uptime => {
                    let hours = self.lifetime_hours.saturating_add(
                        u32::try_from(since_boot_ns / NANOS_PER_HOUR).unwrap_or(u32::MAX),
                    );
                    read(&mut reply, args, &hours.to_le_bytes())
                }
                Std::Upload => match Reply::from_slice(args) {
                    Ok(chunk) => {
                        return actions.defer(pending, Deferred::Flash(FlashOp::Upload(chunk)))
                    }
                    Err(_) => Err(RpcError::ArgsSize),
                },
                Std::Upgrade => return actions.defer(pending, Deferred::Flash(FlashOp::Upgrade)),
                Std::RpcName => table::name(table, self.privileged, args, &mut reply),
                Std::RpcId => table::id(table, self.privileged, args, &mut reply),
                Std::RpcInfo => table::info(table, self.privileged, args, &mut reply),
                Std::RpcList => table::list(table, self.privileged, args, false, &mut reply),
                Std::RpcListInfo => table::list(table, self.privileged, args, true, &mut reply),
                Std::RpcMatch => table::match_name(table, self.privileged, args, &mut reply),
                Std::RpcHash => read(&mut reply, args, &self.hash().to_le_bytes()),
                Std::Start => return actions.defer(pending, Deferred::Acquire(SyncRequest::Start)),
                Std::Stop => return actions.defer(pending, Deferred::Acquire(SyncRequest::Stop)),
                Std::Restart => {
                    return actions.defer(pending, Deferred::Acquire(SyncRequest::Restart))
                }
                Std::Abort => return actions.defer(pending, Deferred::Flash(FlashOp::Abort)),
                Std::SettingsVersion => {
                    read(&mut reply, args, &self.settings_version.to_le_bytes())
                }
                Std::SyncStatus => read(&mut reply, args, &[self.time_status]),
                Std::Priv => self.unlock(args, &mut reply, &mut events(lanes)),
                Std::PrivLock => {
                    self.privileged = false;
                    announcement(&mut events(lanes), "rpc.hash", &self.hash().to_le_bytes());
                    Ok(())
                }
                Std::PrivPassword => match self.password.rpc(args, &mut reply) {
                    Ok(Changed::Changed) => {
                        self.dirty_since = Some(now_ns);
                        Ok(())
                    }
                    Ok(Changed::Unchanged) => Ok(()),
                    Err(error) => Err(error),
                },
            },
        };
        answer(pending, result.map(|()| reply.as_slice()), &mut to(lanes));
    }

    /// `dev.priv`: a read says whether the session is unlocked, a write takes
    /// the password, and a wrong one conceals the method from a locked session
    /// as tl-chibi does. A board that names no password opens no session.
    fn unlock(
        &mut self,
        args: &[u8],
        out: &mut Reply,
        events: &mut impl Sink,
    ) -> Result<(), RpcError> {
        let password = self.password.get();
        if args.is_empty() {
            return put(out, &[u8::from(self.privileged)]);
        }
        if password.as_bytes().is_empty() {
            return Err(RpcError::State);
        }
        if args != password.as_bytes() {
            return Err(match self.privileged {
                true => RpcError::Invalid,
                false => RpcError::NotFound,
            });
        }
        self.privileged = true;
        announcement(events, "rpc.hash", &self.hash().to_le_bytes());
        Ok(())
    }

    /// Answer the board's own entry at `position`, announcing a write to the
    /// asking port ahead of its reply. `None` is a position it does not reach.
    fn entry(
        &mut self,
        position: usize,
        args: &[u8],
        now_ns: u64,
        out: &mut Reply,
        lanes: &mut (impl Lanes + ?Sized),
    ) -> Option<Result<(), RpcError>> {
        let Self {
            settings,
            settings_version,
            ..
        } = self;
        let mut seen = 0;
        let mut found = None;
        let mut moved = false;
        let mut dirty = false;
        settings.entries(&mut |entry| {
            let at = seen;
            seen += 1;
            if at != position {
                return;
            }
            match entry {
                Entry::Setting(cell) => {
                    found = Some(match cell.rpc(args, out) {
                        Ok(Changed::Changed) => {
                            moved = true;
                            dirty |= cell.spec().access.contains(Access::PERSISTENT);
                            announce(settings_version, &mut to(&mut *lanes), cell.name(), out);
                            Ok(())
                        }
                        Ok(Changed::Unchanged) => Ok(()),
                        Err(error) => Err(error),
                    });
                }
                Entry::Reading(reading) => {
                    found = Some(match args {
                        [] => reading.read(out),
                        _ => Err(RpcError::ReadOnly),
                    });
                }
                Entry::Action(action) => {
                    moved = true;
                    let mut lane = events(&mut *lanes);
                    found = Some(action.run(&mut Announcer {
                        version: settings_version,
                        dirty: &mut dirty,
                        events: &mut lane,
                    }));
                }
            }
        });
        if moved {
            settings.publish();
        }
        if dirty {
            self.dirty_since = Some(now_ns);
        }
        found
    }

    /// Every persistent cell, as `dev.conf.save` hands them to the flash.
    fn save(&mut self, image: &mut Entries) -> Result<(), RpcError> {
        let mut outcome = conf::encode([&mut self.password as &mut dyn Persisted], image);
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
    fn restore(&mut self, image: &[u8], lanes: &mut (impl Lanes + ?Sized)) -> Result<(), RpcError> {
        let Self {
            settings,
            settings_version,
            password,
            ..
        } = self;
        let mut outcome = conf::load([password as &mut dyn Persisted], image, |_, _| {});
        settings.entries(&mut |entry| {
            let Entry::Setting(cell) = entry else {
                return;
            };
            let mut lane = events(&mut *lanes);
            let loaded = conf::load([cell as &mut dyn Persisted], image, |name, reply| {
                announce(settings_version, &mut lane, name, reply)
            });
            if loaded.is_err() {
                outcome = loaded;
            }
        });
        outcome
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

    /// Power-cycle: a new session, and the heartbeat, log threshold,
    /// `settings.version`, and uptime a boot starts from.
    pub fn reboot(&mut self, session: SessionId, now_ns: u64) {
        self.session = session;
        self.next_beat = 0;
        self.booted_ns = now_ns;
        self.loglevel.reset();
        self.password.reset();
        self.privileged = false;
        self.settings_version = 0;
        self.dirty_since = None;
    }

    /// Send whatever is due at `now_ns`, and hand back the configuration a
    /// change left unsaved for a period, owed until [`Device::saved`].
    pub fn tick(&mut self, now_ns: u64, out: &mut impl Sink) -> Option<Entries> {
        if now_ns >= self.next_beat {
            self.beat(now_ns, out);
        }
        if now_ns < self.dirty_since? + AUTOSAVE_INTERVAL {
            return None;
        }
        let mut image = Entries::new();
        match self.save(&mut image) {
            Ok(()) => Some(image),
            Err(_) => {
                self.dirty_since = Some(now_ns);
                None
            }
        }
    }

    /// The configuration reached the flash, so nothing is owed until a cell
    /// moves again.
    pub fn saved(&mut self) {
        self.dirty_since = None;
    }

    /// When [`Device::tick`] next has something to send or save.
    pub fn deadline(&self) -> u64 {
        let save = self
            .dirty_since
            .map_or(u64::MAX, |at| at + AUTOSAVE_INTERVAL);
        self.next_beat.min(save)
    }

    fn beat(&mut self, now_ns: u64, out: &mut impl Sink) {
        let beat = Heartbeat::Session(self.session);
        send(out, &[], |buf| beat.write(buf));
        self.next_beat = now_ns + HEARTBEAT_INTERVAL;
    }
}

/// Answer a request where the request that asked says.
pub fn answer(pending: Pending<'_>, result: Result<&[u8], RpcError>, to: &mut impl Sink) {
    match result {
        Ok(value) => send(to, pending.routing, |buf| {
            rpc::write_reply(buf, pending.id, value)
        }),
        Err(error) => send(to, pending.routing, |buf| {
            rpc::write_error(buf, pending.id, error)
        }),
    }
}

/// A read-only property a platform may not have.
fn declared(out: &mut Reply, args: &[u8], value: Option<&[u8]>) -> Result<(), RpcError> {
    read(out, args, value.ok_or(RpcError::State)?)
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

/// A setting cell the device reads, writes, announces, and persists.
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
    version: &'a mut u32,
    dirty: &'a mut bool,
    events: &'a mut dyn Sink,
}

impl Announcer<'_> {
    /// Move a setting, announcing it and counting it in `settings.version`.
    pub fn set<T: Scalar>(&mut self, setting: &mut Setting<T>, value: T) -> Result<(), RpcError> {
        let mut args = Reply::new();
        value.encode(&mut args)?;
        let mut announced = Reply::new();
        if setting.rpc(&args, &mut announced)? == Changed::Changed {
            *self.dirty |= Setting::spec(setting).access.contains(Access::PERSISTENT);
            announce(self.version, &mut self.events, setting.name(), &announced);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::filter::Butterworth4;
    use crate::data::Params;
    use crate::data::{ColumnDef, Stream, StreamDef};
    use crate::rpc::{put, Kind};
    use core::num::NonZeroU32;
    use std::string::String;
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

    /// Two plain settings, then a group of its own: the whole vocabulary, and
    /// the two tiers a developer build adds to it.
    struct Board {
        first: Setting<u32>,
        second: Setting<u32>,
        counter: Counter,
        quiet: Setting<u32>,
        secret: Setting<u32>,
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
                quiet: Setting::new("board.quiet", 9).hidden(),
                secret: Setting::new("board.secret", 11).privileged(),
                published: core::cell::Cell::new(0),
            }
        }
    }

    impl Group for Board {
        fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
            Group::entries(&mut self.first, visit);
            Group::entries(&mut self.second, visit);
            Group::entries(&mut self.counter, visit);
            Group::entries(&mut self.quiet, visit);
            Group::entries(&mut self.secret, visit);
        }

        fn publish(&self) {
            self.published.set(self.published.get() + 1);
        }
    }

    /// More settings than an image holds, for the save that cannot encode.
    struct Fat(Vec<Setting<u32>>);

    impl Group for Fat {
        fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
            self.0
                .iter_mut()
                .for_each(|cell| Group::entries(cell, visit));
        }

        fn publish(&self) {}
    }

    /// The names a board adds after the standard table.
    const BOARD_NAMES: &[&str] = &[
        "board.first",
        "board.second",
        "board.count",
        "board.doubled",
        "board.rewind",
        "board.quiet",
        "board.secret",
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
        fn views(&self) -> Vec<PacketView<'_>> {
            self.0
                .iter()
                .map(|packet| PacketView::parse_prefix(packet).unwrap().0)
                .collect()
        }

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

    /// One device, the streams it describes, and the two lanes.
    struct Harness {
        device: Device<Board>,
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

    /// The developer password the tests' board names.
    const PASSWORD: &str = "895895";

    fn identity() -> Identity {
        Identity::new(
            "COMM-USB",
            "Twinleaf COMM-USB R8 (52373720) [2026-07-24/abc123-DEV]",
            "52373720",
            "2026-07-24/abc123-DEV",
        )
        .unwrap()
    }

    fn device(table: &'static [RpcSpec]) -> Device<Board> {
        let identity = identity().hardware("COMM-USB", "STM32G484xE", &[0xAB; 12], 8);
        Device::new(identity, SESSION, table, Board::new(), PASSWORD, BOOT)
    }

    fn harness() -> Harness {
        let params = Params {
            rate: NonZeroU32::new(10).unwrap(),
            decimation: NonZeroU32::new(4).unwrap(),
            enabled: true,
        };
        Harness {
            device: device(table()),
            streams: [Stream::new(
                StreamId::new(1),
                &AUX,
                Box::leak(Box::new(Butterworth4::<1>::new())),
                params,
            )
            .unwrap()],
            lanes: Both::default(),
            reboot: false,
            deferred: None,
            logged: Vec::new(),
            loglevel: DEFAULT_LOGLEVEL,
        }
    }

    /// One request, as `tio-tool` sends it.
    fn asking(method: Method<'_>, args: &[u8], routing: &[u8]) -> Vec<u8> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let mut len = rpc::write_request(&mut buf, RpcRequestId::new(7), method, args).unwrap();
        for &hop in routing {
            len = route::push_hop(&mut buf, hop).unwrap();
        }
        buf[..len].to_vec()
    }

    impl Harness {
        /// Drive one input, carrying out what comes back the way a platform
        /// does: a name no entry answers is `NotFound`.
        fn drive(&mut self, input: Input<'_>, now_ns: u64) {
            let Harness {
                device,
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
            } = device.handle(&streams[..], input, now_ns, lanes);
            if let Some((pending, _, _)) = call {
                answer(pending, Err(RpcError::NotFound), &mut lanes.to);
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
            self.send(&asking(Method::ByName(name.as_bytes()), args, &[]), now_ns)
        }

        /// Hand the device one packet, and collect what came back on the
        /// port's own lane.
        fn send(&mut self, packet: &[u8], now_ns: u64) -> &Sent {
            self.lanes.to = Sent::default();
            let (view, _) = PacketView::parse_prefix(packet).unwrap();
            self.drive(Input::Packet(view), now_ns);
            &self.lanes.to
        }

        /// The connect burst, as a platform sends it.
        fn connect(&mut self, now_ns: u64) -> &Sent {
            self.lanes.to = Sent::default();
            self.device
                .connected(&self.streams[..], now_ns, &mut self.lanes.to);
            &self.lanes.to
        }

        fn table(&self) -> &'static [RpcSpec] {
            self.device.table()
        }

        /// How many entries `rpc.list` says this session has.
        fn listed(&mut self) -> usize {
            let count = self.ask("rpc.list", &[]).value().try_into().unwrap();
            usize::from(u16::from_le_bytes(count))
        }

        /// Every name `rpc.list` enumerates for this session.
        fn listing(&mut self) -> Vec<String> {
            let mut names = Vec::new();
            for at in 0..self.listed() as u16 {
                let name = self.ask("rpc.list", &at.to_le_bytes()).value().to_vec();
                names.push(String::from_utf8(name).unwrap());
            }
            names
        }

        /// Drive the clock to `now_ns` and store what the device decided to
        /// save on its own account, as a platform whose flash took it does.
        fn tick(&mut self, now_ns: u64) -> Option<Entries> {
            let image = self.device.tick(now_ns, &mut Sent::default())?;
            self.device.saved();
            Some(image)
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

    /// Ids are positions in the table and are on the wire, so a host that read
    /// the table once asks by id from then on.
    #[test]
    fn answers_a_standard_rpc_by_id() {
        let mut device = harness();
        let id = STANDARD
            .iter()
            .position(|spec| spec.name == "rpc.hash")
            .unwrap();
        let hash = device.device.hash().to_le_bytes();
        let asked = asking(
            Method::ById(twinleaf_proto::RpcMethodId::new(id as u16)),
            &[],
            &[],
        );
        assert_eq!(device.send(&asked, BOOT).value(), hash);
    }

    /// The half of the identity the platform holds rather than the device.
    #[test]
    fn answers_the_platforms_half_of_the_identity() {
        let mut device = harness();
        assert_eq!(device.ask("dev.uid", &[]).value(), &[0xAB; 12]);
        assert_eq!(device.ask("dev.model", &[]).value(), b"COMM-USB");
        assert_eq!(device.ask("dev.revision", &[]).value(), &8u16.to_le_bytes());
        assert_eq!(
            device.ask("dev.mcu.model", &[]).error(),
            RpcError::NotFound,
            "the part number is a developer entry"
        );
        device.ask("dev.priv", PASSWORD.as_bytes());
        assert_eq!(device.ask("dev.mcu.model", &[]).value(), b"STM32G484xE");
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
        device.device = Device::new(identity(), SESSION, table(), Board::new(), PASSWORD, BOOT);
        device.ask("dev.priv", PASSWORD.as_bytes());
        for name in ["dev.uid", "dev.model", "dev.mcu.model", "dev.revision"] {
            assert_eq!(device.ask(name, &[]).error(), RpcError::State, "{name}");
        }
    }

    #[test]
    fn restored_lifetime_hours_keep_the_boot_clock_separate_and_saturate() {
        let mut device = harness();
        device.device.restore_uptime(123);
        let later = BOOT + 2 * NANOS_PER_HOUR + 1;
        assert_eq!(
            device.ask_at("dev.uptime", &[], later).value(),
            &125u32.to_le_bytes()
        );
        assert_eq!(
            device.ask_at("dev.systime", &[], later).value(),
            &(later - BOOT).to_le_bytes()
        );
        device.device.restore_uptime(u32::MAX);
        assert_eq!(
            device.ask_at("dev.uptime", &[], later).value(),
            &u32::MAX.to_le_bytes()
        );
        let mut rebooted = harness();
        rebooted.device.restore_uptime(125);
        assert_eq!(
            rebooted.ask("dev.uptime", &[]).value(),
            &125u32.to_le_bytes()
        );
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
        assert_eq!(
            device.listed(),
            device.table().len() - 6,
            "`dev.mcu.model`, the three `dev.priv` entries, and the board's own two"
        );
        device.ask("dev.priv", PASSWORD.as_bytes());
        assert_eq!(device.listed(), device.table().len());
    }

    /// The developer tier: a locked session calls a hidden entry but never
    /// sees it, and cannot reach a privileged one at all.
    #[test]
    fn a_locked_session_hides_what_a_board_marked_developer() {
        let mut device = harness();
        assert_eq!(device.ask("board.quiet", &[]).value(), &9u32.to_le_bytes());
        assert_eq!(device.ask("board.secret", &[]).error(), RpcError::NotFound);
        assert_eq!(device.ask("dev.priv.lock", &[]).error(), RpcError::NotFound);
        assert_eq!(
            device.ask("dev.priv.password", &[]).error(),
            RpcError::NotFound
        );

        for name in ["board.quiet", "board.secret", "dev.priv", "dev.priv.lock"] {
            assert_eq!(
                device.ask("rpc.id", name.as_bytes()).error(),
                RpcError::Invalid,
                "{name} does not exist to a locked session"
            );
            assert_eq!(
                device.ask("rpc.info", name.as_bytes()).error(),
                RpcError::Invalid
            );
        }
        let listed = device.listing();
        assert!(!listed.iter().any(|name| name.starts_with("dev.priv")));
        assert!(!listed.contains(&"board.quiet".to_string()));
        assert!(!listed.contains(&"board.secret".to_string()));
        assert!(listed.contains(&"board.first".to_string()));

        let public = table::hash(device.table(), false);
        assert_eq!(
            device.ask("rpc.hash", &[]).value(),
            public.to_le_bytes(),
            "the hash a locked session caches covers the table it was shown"
        );
        assert_eq!(device.ask("dev.priv", &[]).value(), &[0]);
    }

    /// An id is a declaration position, so a hidden entry keeps its own and
    /// the enumeration simply steps over it.
    #[test]
    fn a_hidden_entry_keeps_its_id_and_answers_by_it() {
        let mut device = harness();
        let id = device
            .table()
            .iter()
            .position(|spec| spec.name == "board.quiet")
            .unwrap() as u16;
        let asked = asking(Method::ById(twinleaf_proto::RpcMethodId::new(id)), &[], &[]);
        assert_eq!(device.send(&asked, BOOT).value(), &9u32.to_le_bytes());
        assert_eq!(
            device.ask("rpc.name", &id.to_le_bytes()).error(),
            RpcError::Invalid
        );
    }

    #[test]
    fn the_password_unlocks_the_session_and_locks_it_again() {
        let mut device = harness();
        assert_eq!(device.ask("dev.priv", b"wrong").error(), RpcError::NotFound);
        assert_eq!(device.ask("dev.priv", PASSWORD.as_bytes()).value(), b"");
        assert_eq!(device.ask("dev.priv", &[]).value(), &[1]);

        assert_eq!(
            device.ask("board.secret", &[]).value(),
            &11u32.to_le_bytes()
        );
        assert_eq!(device.listed(), device.table().len());
        assert!(device.listing().contains(&"board.secret".to_string()));
        let full = table::hash(device.table(), true);
        assert_eq!(device.ask("rpc.hash", &[]).value(), full.to_le_bytes());
        assert_eq!(
            device.lanes.events.announced().last().unwrap(),
            &("rpc.hash".to_string(), full.to_le_bytes().to_vec()),
            "an unlocked session is told the table it may now cache"
        );
        assert_eq!(
            device.ask("dev.priv", b"wrong").error(),
            RpcError::Invalid,
            "a session that already knows the method is told it typed it wrong"
        );

        assert_eq!(device.ask("dev.priv.lock", &[]).value(), b"");
        assert_eq!(device.ask("dev.priv", &[]).value(), &[0]);
        assert_eq!(device.ask("board.secret", &[]).error(), RpcError::NotFound);
    }

    /// The board's password is a compiled default the developer overrides,
    /// stored like any other persistent setting.
    #[test]
    fn a_written_password_is_saved_and_unlocks_the_next_boot() {
        let mut device = harness();
        device.ask("dev.priv", PASSWORD.as_bytes());
        assert_eq!(
            device.ask("dev.priv.password", &[]).value(),
            PASSWORD.as_bytes()
        );
        assert_eq!(
            device.ask("dev.priv.password", b"hunter2").value(),
            b"hunter2"
        );
        device.ask("dev.conf.save", &[]);
        let Some((_, Deferred::Flash(FlashOp::ConfSave(image)))) = device.deferred.take() else {
            panic!("a save carries the image");
        };

        device.device.reboot(SessionId::new(2), BOOT);
        assert_eq!(device.ask("dev.priv", &[]).value(), &[0], "a boot locks");
        device.drive(Input::Loaded(None, Ok(&image)), BOOT);
        assert_eq!(
            device.ask("dev.priv", PASSWORD.as_bytes()).error(),
            RpcError::NotFound,
            "the compiled default no longer opens it"
        );
        assert_eq!(device.ask("dev.priv", b"hunter2").value(), b"");
        assert!(
            !device
                .lanes
                .events
                .announced()
                .iter()
                .any(|(name, _)| name == "dev.priv.password"),
            "a password is never broadcast"
        );
    }

    /// A device that names no password has no developer session to open, as
    /// the proxy's virtual hub does not.
    #[test]
    fn a_device_with_no_password_answers_the_state_error() {
        let mut device = harness();
        device.device = Device::new(identity(), SESSION, table(), Board::new(), "", BOOT);
        assert_eq!(device.ask("dev.priv", b"895895").error(), RpcError::State);
        assert_eq!(device.ask("dev.priv", &[]).value(), &[0]);
    }

    #[test]
    fn rpc_match_completes_over_the_whole_table() {
        let mut device = harness();
        assert_eq!(device.ask("rpc.match", b"board.f").value(), b"board.first");
    }

    #[test]
    fn bad_requests_are_refused() {
        let mut device = harness();
        assert_eq!(device.ask("dev.nonsense", &[]).error(), RpcError::NotFound);
        assert_eq!(device.ask("dev.name", b"nope").error(), RpcError::ReadOnly);
        assert_eq!(
            device.ask("board.rewind", &[1]).error(),
            RpcError::ArgsSize,
            "an action takes no argument"
        );

        let mut packet = asking(Method::ByName(b"dev.name"), &[], &[]);
        packet.truncate(Packet::MAX_SIZE.min(6));
        packet[2..4].copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(device.send(&packet, BOOT).error(), RpcError::Malformed);
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

    /// What a host learns the moment it connects, ending on the record that
    /// closes the sweep.
    #[test]
    fn connecting_sends_the_hash_a_heartbeat_and_the_sweep() {
        let mut device = harness();
        let hash = device.device.hash().to_le_bytes();
        let views = device.connect(BOOT).views();
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
        assert_eq!(setting.reply, hash);
        assert_eq!(
            Heartbeat::parse(views[1].payload),
            Some(Heartbeat::Session(SESSION))
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
        assert_eq!(device.device.deadline(), BOOT + HEARTBEAT_INTERVAL);
    }

    #[test]
    fn heartbeats_are_due_every_interval() {
        let mut device = harness();
        device.connect(BOOT);
        let mut sent = Sent::default();

        assert!(device
            .device
            .tick(BOOT + HEARTBEAT_INTERVAL / 2, &mut sent)
            .is_none());
        assert!(sent.0.is_empty());
        assert!(device
            .device
            .tick(BOOT + HEARTBEAT_INTERVAL, &mut sent)
            .is_none());
        assert_eq!(sent.views()[0].header.ptype, PacketType::HEARTBEAT);
        assert_eq!(device.device.deadline(), BOOT + 2 * HEARTBEAT_INTERVAL);
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

    /// The write is announced to the port that asked and counted, and what it
    /// left is what the device's own logging is gated on.
    #[test]
    fn writing_dev_loglevel_raises_the_threshold_and_announces_it() {
        let mut device = harness();
        let announced = device
            .ask("dev.loglevel", &[LogLevel::DEBUG.value()])
            .announced();
        assert_eq!(
            announced,
            [("dev.loglevel".to_owned(), vec![LogLevel::DEBUG.value()])]
        );

        let mut sent = Sent::default();
        device
            .device
            .log(LogLevel::DEBUG, 0, "now heard", &mut sent);
        assert_eq!(sent.0.len(), 1);

        assert_eq!(
            device.ask("settings.version", &[]).value(),
            1u32.to_le_bytes()
        );
    }

    #[test]
    fn a_message_is_logged_at_the_threshold_and_dropped_beneath_it() {
        let device = harness();
        let mut sent = Sent::default();
        device.device.log(LogLevel::DEBUG, 1, "chatter", &mut sent);
        assert!(sent.0.is_empty());

        device.device.log(LogLevel::INFO, 7, "up", &mut sent);
        device.device.log(LogLevel::CRITICAL, 8, "vbus", &mut sent);
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
        let device = harness();
        let mut sent = Sent::default();
        let message = "é".repeat(MAX_MESSAGE_SIZE);
        device.device.log(LogLevel::ERROR, 0, &message, &mut sent);

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
    fn an_applied_setting_is_announced_once_per_change() {
        let mut device = harness();
        let mut gain = Setting::new("app.gain", 1u8);
        let mut sent = Sent::default();

        let reply = device.device.apply(&mut gain, &[], &mut sent).unwrap();
        assert_eq!(reply.as_slice(), [1]);
        assert!(sent.0.is_empty());

        let reply = device.device.apply(&mut gain, &[9], &mut sent).unwrap();
        assert_eq!(reply.as_slice(), [9]);
        assert_eq!(gain.get(), 9);
        assert_eq!(
            sent.announced(),
            [("app.gain".to_owned(), vec![9])],
            "the write is announced with the value it left"
        );
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

    /// A power cycle is a new session, and the threshold, `settings.version`,
    /// and uptime a boot starts from.
    #[test]
    fn a_reboot_restores_what_a_boot_starts_from() {
        let mut device = harness();
        device.ask("dev.loglevel", &[LogLevel::DEBUG.value()]);

        let later = BOOT + 3 * NANOS_PER_HOUR;
        device.device.reboot(SessionId::new(10), later);
        assert_eq!(device.device.session, SessionId::new(10));
        assert_eq!(
            device.ask_at("dev.loglevel", &[], later).value(),
            [DEFAULT_LOGLEVEL.value()]
        );
        assert_eq!(
            device.ask_at("settings.version", &[], later).value(),
            0u32.to_le_bytes()
        );
        assert_eq!(
            device.ask_at("dev.uptime", &[], later).value(),
            0u32.to_le_bytes()
        );
    }

    /// A packet the device never answers proves nothing and says nothing.
    #[test]
    fn traffic_that_is_not_a_request_is_ignored() {
        let mut device = harness();
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = Heartbeat::Session(SESSION).write(&mut buf).unwrap();
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
        assert_eq!(names[STANDARD.len() - 1], "dev.priv.password");
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
        let types: Vec<_> = answered
            .views()
            .iter()
            .map(|view| view.header.ptype)
            .collect();
        assert_eq!(
            types,
            [PacketType::SETTING, PacketType::RPC_REP],
            "an announcement, then the reply"
        );
        assert_eq!(
            answered.announced(),
            [("board.second".to_owned(), 42u32.to_le_bytes().to_vec())]
        );
        assert!(
            device.lanes.events.0.is_empty(),
            "a write one port asked for is not an event"
        );
        assert_eq!(version(&mut device), 1, "the write is counted");

        // Repeating an accepted value is also quiet.
        assert_eq!(device.ask("board.second", &42u32.to_le_bytes()).0.len(), 1);
        assert_eq!(version(&mut device), 1);

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
        let published = |device: &Harness| device.device.settings().published.get();
        device.ask("board.first", &[]);
        assert_eq!(published(&device), 0);
        device.ask("board.first", &1u32.to_le_bytes());
        assert_eq!(published(&device), 1);
        device.ask("board.rewind", &[]);
        assert_eq!(published(&device), 2);
    }

    /// A name the table declares and no entry answers comes back for the
    /// platform to answer, which is what lets one keep an RPC of its own. It
    /// carries the route it arrived on, so the answer goes back down it.
    #[test]
    fn a_name_no_entry_answers_comes_back_to_the_platform_with_its_route() {
        let mut specs: Vec<RpcSpec> = table().to_vec();
        specs.push(RpcSpec::std("board.capture", Access::READ));
        let mut device = harness();
        device.device = Device::new(
            identity(),
            SESSION,
            Box::leak(specs.into_boxed_slice()),
            Board::new(),
            PASSWORD,
            BOOT,
        );

        let packet = asking(Method::ByName(b"board.capture"), &[3], &[2, 1]);
        let (view, _) = PacketView::parse_prefix(&packet).unwrap();
        let actions = device.device.handle(
            &device.streams[..],
            Input::Packet(view),
            BOOT,
            &mut device.lanes,
        );
        let Some((pending, name, args)) = actions.call else {
            panic!("an application call");
        };
        assert_eq!(
            (name, args, pending.id.value()),
            ("board.capture", &[3][..], 7)
        );
        assert!(device.lanes.to.0.is_empty(), "nothing was answered here");

        answer(pending, Err(RpcError::Busy), &mut device.lanes.to);
        let [view] = device.lanes.to.views()[..] else {
            panic!("one packet");
        };
        assert_eq!(view.routing, [2, 1]);
        assert_eq!(device.lanes.to.error(), RpcError::Busy);
    }

    /// The walk that declares the table and the walk that answers a position
    /// are the same walk, so a name declared and never answered is a table
    /// entry a host cannot call.
    #[test]
    fn every_name_declared_is_one_that_is_answered() {
        let mut device = harness();
        device.ask("dev.priv", PASSWORD.as_bytes());
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
        device.device.set_time_status(3);
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
            [
                &b"dev.priv.password"[..],
                b"board.first",
                b"board.second",
                b"board.count"
            ],
            "every persistent cell, in table order"
        );
        assert_eq!(stored[2].1, 42u32.to_le_bytes());
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
        assert_eq!(device.device.settings().published.get(), 1);
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
        assert_eq!(device.device.settings().published.get(), 1);
        assert_eq!(device.ask("board.first", &[]).value(), &7u32.to_le_bytes());
    }

    /// `dev.conf.load` is two hops: the platform reads the slot, the device
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

    /// tl-chibi's autosave (`lib/firmware/tlfw_persist.c:305`): a written
    /// setting is saved a period after the write, once, and the heartbeat is
    /// still the sooner deadline.
    #[test]
    fn a_written_setting_saves_itself_a_period_later() {
        let mut device = harness();
        assert!(device.tick(BOOT).is_none());
        assert_eq!(device.device.deadline(), BOOT + HEARTBEAT_INTERVAL);

        device.ask_at("board.second", &42u32.to_le_bytes(), BOOT);
        assert_eq!(
            device.device.deadline(),
            BOOT + HEARTBEAT_INTERVAL,
            "the heartbeat comes first"
        );
        assert!(device.tick(BOOT + AUTOSAVE_INTERVAL - 1).is_none());
        assert_eq!(
            device.device.deadline(),
            BOOT + AUTOSAVE_INTERVAL,
            "and then the save the write owes"
        );

        let image = device.tick(BOOT + AUTOSAVE_INTERVAL).expect("the save");
        let stored: Vec<(&[u8], &[u8])> = conf::entries(&image)
            .map(|entry| entry.unwrap())
            .map(|entry| (entry.name, entry.value))
            .collect();
        assert_eq!(stored[2], (&b"board.second"[..], &42u32.to_le_bytes()[..]));
        assert!(
            device.tick(BOOT + 3 * AUTOSAVE_INTERVAL).is_none(),
            "one save, not a save every period"
        );
    }

    /// Each further change puts the save off by another period, so a host
    /// writing a whole configuration stores it once.
    #[test]
    fn a_second_change_puts_the_save_off_by_another_period() {
        let mut device = harness();
        let half = AUTOSAVE_INTERVAL / 2;
        device.ask_at("board.first", &1u32.to_le_bytes(), BOOT);
        device.ask_at("board.second", &2u32.to_le_bytes(), BOOT + half);
        assert!(device.tick(BOOT + AUTOSAVE_INTERVAL).is_none());
        assert!(device.tick(BOOT + half + AUTOSAVE_INTERVAL).is_some());
    }

    /// An action that moves a stored cell owes the save a write does.
    #[test]
    fn an_action_that_moves_a_stored_setting_owes_a_save() {
        let mut device = harness();
        device.ask_at("board.rewind", &[], BOOT);
        assert!(device.tick(BOOT + AUTOSAVE_INTERVAL).is_some());
    }

    /// tl-chibi stores no `dev.loglevel`, so writing it owes nothing.
    #[test]
    fn a_write_to_a_setting_that_is_not_stored_owes_no_save() {
        let mut device = harness();
        device.ask_at("dev.loglevel", &[LogLevel::DEBUG.value()], BOOT);
        assert!(device.tick(BOOT + 2 * AUTOSAVE_INTERVAL).is_none());
    }

    /// `dev.conf.save` stores what the write left, so the autosave it owed is
    /// cancelled rather than repeated.
    #[test]
    fn an_explicit_save_cancels_the_one_a_change_owed() {
        let mut device = harness();
        device.ask_at("board.second", &42u32.to_le_bytes(), BOOT);
        device.ask_at("dev.conf.save", &[], BOOT + 1);
        let Some((_, Deferred::Flash(FlashOp::ConfSave(_)))) = device.deferred.take() else {
            panic!("the save reached the flash");
        };
        assert!(device.tick(BOOT + 2 * AUTOSAVE_INTERVAL).is_none());
    }

    /// A restore says what the flash already stores, whether a boot or a host
    /// asked for it, so tl-chibi's `tl_persist_load` leaves nothing owing.
    #[test]
    fn a_restored_configuration_owes_no_save() {
        let mut device = harness();
        let mut image = Entries::new();
        let mut stored = Setting::new("board.second", 42u32).persistent();
        conf::encode([&mut stored as &mut dyn Persisted], &mut image).unwrap();

        device.drive(Input::Loaded(None, Ok(&image)), BOOT);
        assert!(device.tick(BOOT + 2 * AUTOSAVE_INTERVAL).is_none());

        let later = BOOT + 2 * AUTOSAVE_INTERVAL;
        let pending = Pending::local(RpcRequestId::new(7));
        device.drive(Input::Loaded(Some(pending), Ok(&image)), later);
        assert!(device.tick(later + 2 * AUTOSAVE_INTERVAL).is_none());
    }

    /// A read leaves `Changed::Unchanged`, which tl-chibi does not stamp.
    #[test]
    fn a_read_of_a_stored_setting_owes_no_save() {
        let mut device = harness();
        assert_eq!(
            device.ask_at("board.first", &[], BOOT).value(),
            &7u32.to_le_bytes()
        );
        assert!(device.tick(BOOT + 2 * AUTOSAVE_INTERVAL).is_none());
    }

    /// The save is owed until the flash takes it, so a platform too busy to
    /// store one is offered it again rather than losing the change.
    #[test]
    fn a_save_the_flash_did_not_take_is_offered_again() {
        let mut device = harness();
        device.ask_at("board.second", &42u32.to_le_bytes(), BOOT);
        let mut sent = Sent::default();
        let due = BOOT + AUTOSAVE_INTERVAL;
        assert!(device.device.tick(due, &mut sent).is_some(), "the save");
        assert!(
            device.device.tick(due + 1, &mut sent).is_some(),
            "still owed"
        );
        device.device.saved();
        assert!(device.device.tick(due + 2, &mut sent).is_none());
    }

    /// A save that does not fit its image is owed for another period, as
    /// tl-chibi's systick retries the one its storage refused.
    #[test]
    fn a_save_too_large_for_its_image_is_owed_for_another_period() {
        let cells = (0..IMAGE_MAX / 16)
            .map(|at| Box::leak(format!("board.filler{at:03}").into_boxed_str()))
            .map(|name| Setting::new(name, 0u32).persistent())
            .collect();
        let mut board = Fat(cells);
        let mut specs = STANDARD.to_vec();
        board.entries(&mut |entry| specs.push(entry.spec()));
        let mut device = Device::new(
            identity(),
            SESSION,
            Box::leak(specs.into_boxed_slice()),
            board,
            PASSWORD,
            BOOT,
        );

        let asked = asking(Method::ByName(b"board.filler000"), &1u32.to_le_bytes(), &[]);
        let (view, _) = PacketView::parse_prefix(&asked).unwrap();
        let none: [Stream<4>; 0] = [];
        let mut lanes = Both::default();
        device.handle(&none[..], Input::Packet(view), BOOT, &mut lanes);
        assert_eq!(lanes.to.value(), &1u32.to_le_bytes(), "the write it owes");

        let mut sent = Sent::default();
        assert!(
            device.tick(BOOT + AUTOSAVE_INTERVAL, &mut sent).is_none(),
            "the image has no room for them"
        );
        assert!(device
            .tick(BOOT + 2 * AUTOSAVE_INTERVAL - 1, &mut sent)
            .is_none());
        assert_eq!(
            device.deadline(),
            BOOT + 2 * AUTOSAVE_INTERVAL,
            "owed for another period"
        );
    }
}

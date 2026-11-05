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
use twinleaf_proto::data::{self, Metadata, MetadataFlags};
use twinleaf_proto::heartbeat::Heartbeat;
use twinleaf_proto::log::{LogLevel, LogMessage, MAX_MESSAGE_SIZE};
use twinleaf_proto::packet::{Packet, PacketType, PacketView};
use twinleaf_proto::route;
use twinleaf_proto::rpc::{self, Method, Request, RpcError};
use twinleaf_proto::settings::Setting as Announcement;
use twinleaf_proto::{RpcRequestId, SessionId};

use crate::data::metadata::{self, Streams};
use crate::rpc::{self as table, put, read, Access, Kind, Reply, RpcSpec, Std, STANDARD};
use crate::rpc::{finite, Atomic, Changed, Persisted, Scalar, Setting, Text, UID_SIZE};
use crate::storage::conf::{self, Image};
use crate::Sink;

/// Nanoseconds between heartbeats.
pub const HEARTBEAT_INTERVAL: u64 = 200_000_000;

/// Nanoseconds a changed configuration waits before it saves itself.
pub const AUTOSAVE_INTERVAL: u64 = 60_000_000_000;

/// The level `dev.loglevel` boots at.
pub const DEFAULT_LOGLEVEL: LogLevel = LogLevel::INFO;

/// Seconds `dev.autostart` waits at boot before acquiring.
pub const DEFAULT_AUTOSTART: u8 = 5;

/// Milliseconds `dev.metadata.broadcast.period` boots at for a pass of the description.
pub const DEFAULT_BROADCAST_PERIOD: u16 = 5_000;

/// As much flash as a stored configuration may take.
pub const IMAGE_MAX: usize = 1024;

/// Why a locked session's call to a privileged entry is refused.
pub const LOCKED: &str = "privileged: unlock with dev.priv";

/// As big a cell as `dev.priv.password` keeps, NUL and all.
pub const PASSWORD_MAX: usize = 16;

/// The bytes a stored configuration is built in and read back into.
pub type Entries = Image<IMAGE_MAX>;

/// Nanoseconds in the second `dev.uptime` counts.
const NANOS_PER_SECOND: u64 = 1_000_000_000;
/// Nanoseconds in the millisecond `dev.metadata.broadcast.period` counts.
const NANOS_PER_MILLI: u64 = 1_000_000;
/// Nanoseconds in the hour `dev.lifetime` counts.
const NANOS_PER_HOUR: u64 = 3_600 * NANOS_PER_SECOND;

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
    pub uid: Option<&'static [u8; UID_SIZE]>,
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
        uid: &'static [u8; UID_SIZE],
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

/// Where a device's control and event packets go, each on the lane
/// [`lane_of`](twinleaf_proto::lane_of) names. A stream sends its data through a [`Sink`] of its
/// own.
///
/// Control is every port's announcements, heartbeats and device, stream and column records in one
/// write order, with each port's own replies merged in so that an announcement written before a
/// reply leaves ahead of it. A port that falls behind is cut off, with a counted drop and a gap in the sequence, and
/// resynchronises from a snapshot, so no port delays the device or another port. Events are
/// logs, in no order against anything. A platform with one port is [`OneLane`].
pub trait Lanes {
    /// The control lane of the port that asked: its replies.
    fn reply(&mut self, packet: &[u8]);

    /// Every port's control lane: an announcement, a heartbeat or a device, stream or column
    /// record, behind every one before it.
    fn announce(&mut self, packet: &[u8]);

    /// Every port's event lane: a log.
    fn event(&mut self, packet: &[u8]);
}

/// Both lanes over one sink, for a platform that has one port.
pub struct OneLane<S: Sink>(pub S);

impl<S: Sink> Lanes for OneLane<S> {
    fn reply(&mut self, packet: &[u8]) {
        self.0.send(packet);
    }

    fn announce(&mut self, packet: &[u8]) {
        self.0.send(packet);
    }

    fn event(&mut self, packet: &[u8]) {
        self.0.send(packet);
    }
}

/// Which of [`Lanes`] a [`Lane`] writes into.
#[derive(Clone, Copy)]
enum Route {
    Reply,
    Announce,
    Event,
}

/// One lane, as the device writes packets into it.
struct Lane<'a, L: Lanes + ?Sized> {
    lanes: &'a mut L,
    route: Route,
}

impl<L: Lanes + ?Sized> Sink for Lane<'_, L> {
    fn send(&mut self, packet: &[u8]) {
        match self.route {
            Route::Reply => self.lanes.reply(packet),
            Route::Announce => self.lanes.announce(packet),
            Route::Event => self.lanes.event(packet),
        }
    }
}

/// The lane of the port that asked.
fn to<L: Lanes + ?Sized>(lanes: &mut L) -> Lane<'_, L> {
    Lane {
        lanes,
        route: Route::Reply,
    }
}

/// Every port's control lane.
fn announcements<L: Lanes + ?Sized>(lanes: &mut L) -> Lane<'_, L> {
    Lane {
        lanes,
        route: Route::Announce,
    }
}

/// Every port's event lane.
fn events<L: Lanes + ?Sized>(lanes: &mut L) -> Lane<'_, L> {
    Lane {
        lanes,
        route: Route::Event,
    }
}

/// The one owner of a device and its settings.
pub struct Device<S: Group> {
    /// Who the device is.
    pub identity: Identity,
    /// Which boot this is.
    pub session: SessionId,
    table: &'static [RpcSpec],
    /// `rpc.hash`, fixed for the image.
    hash: u32,
    /// Whether `dev.priv` has unlocked this session: not persisted, and false
    /// at every boot.
    privileged: bool,
    /// The password `dev.priv` takes, which is never announced or broadcast.
    password: Setting<Text<PASSWORD_MAX>>,
    /// Seconds after boot the acquisition starts on its own, or never at zero.
    autostart: Setting<u8>,
    /// Nanoseconds the wire carrying a parent's PPS delays it.
    sync_delay: Setting<f32>,
    /// Milliseconds the carousel takes for a pass, or never at zero.
    broadcast_period: Setting<u16>,
    carousel: metadata::Carousel,
    settings: S,
    next_beat: u64,
    /// The nanosecond the device came up, which `dev.systime`, `dev.uptime`
    /// and `dev.lifetime` count from.
    booted_ns: u64,
    /// Whole powered-on hours restored once at boot.
    lifetime_hours: u32,
    /// `dev.lifetime`, moved to the restored hours plus this boot's at each refresh.
    lifetime: Setting<u32>,
    loglevel: Setting<u8>,
    settings_version: u32,
    /// When a persistent cell last moved with no save since.
    dirty_since: Option<u64>,
    /// What the platform last published for `sync.status`.
    time_status: u8,
    /// `sync.status`, moved to what the platform published at each refresh.
    sync_status: Setting<u8>,
}

impl<S: Group> Device<S> {
    /// A device come up at `now_ns`, answering `table`: [`STANDARD`] first,
    /// then what `settings` declares, and `dev.priv` taking `password` until
    /// `dev.priv.password` overrides it. Its session opens on control with a
    /// heartbeat and then the device record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: Identity,
        session: SessionId,
        table: &'static [RpcSpec],
        settings: S,
        password: &str,
        now_ns: u64,
        streams: &(impl Streams + ?Sized),
        lanes: &mut (impl Lanes + ?Sized),
    ) -> Self {
        let mut device = Self {
            identity,
            session,
            table,
            hash: table::hash(table),
            privileged: false,
            password: Setting::new("dev.priv.password", Text::from(password)).persistent(),
            autostart: Setting::new("dev.autostart", DEFAULT_AUTOSTART).persistent(),
            sync_delay: Setting::new("sync.delay", 0.0).checked(finite).persistent(),
            broadcast_period: Setting::new(
                "dev.metadata.broadcast.period",
                DEFAULT_BROADCAST_PERIOD,
            )
            .persistent(),
            carousel: metadata::Carousel::default(),
            settings,
            next_beat: 0,
            booted_ns: now_ns,
            lifetime_hours: 0,
            lifetime: Setting::new("dev.lifetime", 0).read_only(),
            loglevel: Setting::new("dev.loglevel", DEFAULT_LOGLEVEL.value()),
            settings_version: 0,
            dirty_since: None,
            time_status: 0,
            sync_status: Setting::new("sync.status", 0).read_only(),
        };
        device.open(now_ns, streams, lanes);
        device
    }

    /// Restores lifetime powered-on hours without changing the current boot clock.
    pub fn restore_lifetime(&mut self, hours: u32) {
        self.lifetime_hours = hours;
    }

    /// The RPC table.
    pub fn table(&self) -> &'static [RpcSpec] {
        self.table
    }

    /// `rpc.hash`, which the table fixes for the image.
    pub fn hash(&self) -> u32 {
        self.hash
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

    /// `dev.autostart`, for the platform's acquisition to wait out.
    pub fn autostart(&self) -> u8 {
        self.autostart.get()
    }

    /// `sync.delay`, for the platform's synchronizer to take off a parent's edges.
    pub fn sync_delay_ns(&self) -> f32 {
        self.sync_delay.get()
    }

    /// Publish what this device's own time is now worth, for `sync.status`
    /// to announce at the next refresh.
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
            protocol: twinleaf_proto::PROTOCOL,
        }
    }

    /// Tell a host that has just connected everything it needs: a heartbeat,
    /// and every metadata record.
    pub fn connected(
        &mut self,
        streams: &(impl Streams + ?Sized),
        now_ns: u64,
        out: &mut impl Sink,
    ) {
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
                let repaired = self.settings.repair(&mut Announcer {
                    version: &mut self.settings_version,
                    dirty: &mut false,
                    events: &mut announcements(&mut *lanes),
                });
                self.settings.publish();
                actions.log = Some(match (outcome, repaired) {
                    (Ok(()), Err(Refusal { why: "", .. })) => (
                        LogLevel::WARNING,
                        "a saved group broke its rule and is back at its defaults",
                    ),
                    (Ok(()), Err(Refusal { why, .. })) => (LogLevel::WARNING, why),
                    (Ok(()), Ok(())) => (LogLevel::INFO, "restored the saved configuration"),
                    (Err(RpcError::State), _) => (
                        LogLevel::INFO,
                        "no saved configuration; using compiled defaults",
                    ),
                    (Err(_), _) => (
                        LogLevel::WARNING,
                        "some saved settings could not be restored",
                    ),
                });
                if let Some(pending) = pending {
                    answer(pending, outcome.map(|()| &[][..]), &mut to(lanes));
                }
            }
            Input::Packet(packet) if packet.header.ptype == PacketType::RPC_REQ => {
                self.refresh(now_ns, &mut announcements(&mut *lanes));
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
        let Some(index) = index else {
            return answer(pending, Err(RpcError::NotFound), &mut to(lanes));
        };
        let spec = &table[index];
        if spec.access.is_privileged() && !self.privileged {
            let locked = Refusal {
                error: RpcError::State,
                why: LOCKED,
            };
            return answer(pending, Err(locked), &mut to(lanes));
        }
        let args = request.args;
        if spec.method == table::Method::Action && !args.is_empty() {
            return answer(pending, Err(RpcError::ArgsSize), &mut to(lanes));
        }
        let since_boot_ns = now_ns.saturating_sub(self.booted_ns);
        let hw_rev = self.identity.hw_rev.map(u16::to_le_bytes);
        let mut reply = Reply::new();
        let result: Result<(), Refusal> = match Std::at(index) {
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
                    &mut announcements(lanes),
                )
                .map(drop),
                Std::Name => read(&mut reply, args, self.identity.name.as_bytes()),
                Std::Model => declared(&mut reply, args, self.identity.model.map(str::as_bytes)),
                Std::Uid => declared(&mut reply, args, self.identity.uid.map(|uid| &uid[..])),
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
                Std::Uptime => read(
                    &mut reply,
                    args,
                    &u32::try_from(since_boot_ns / NANOS_PER_SECOND)
                        .unwrap_or(u32::MAX)
                        .to_le_bytes(),
                ),
                Std::Lifetime => self.lifetime.rpc(args, &mut reply).map(drop),
                Std::Upload => match Reply::from_slice(args) {
                    Ok(chunk) => {
                        return actions.defer(pending, Deferred::Flash(FlashOp::Upload(chunk)))
                    }
                    Err(_) => Err(RpcError::ArgsSize),
                },
                Std::Upgrade => return actions.defer(pending, Deferred::Flash(FlashOp::Upgrade)),
                Std::RpcName => table::name(table, args, &mut reply),
                Std::RpcId => table::id(table, args, &mut reply),
                Std::RpcInfo => table::info(table, args, &mut reply),
                Std::RpcList => table::list(table, args, false, &mut reply),
                Std::RpcListInfo => table::list(table, args, true, &mut reply),
                Std::RpcMatch => table::match_name(table, args, &mut reply),
                Std::RpcHash => read(&mut reply, args, &self.hash().to_le_bytes()),
                Std::Autostart => write(
                    &mut self.settings_version,
                    &mut self.autostart,
                    args,
                    &mut reply,
                    &mut announcements(lanes),
                )
                .map(|changed| {
                    if changed == Changed::Changed {
                        self.dirty_since = Some(now_ns);
                    }
                }),
                Std::SyncDelay => write(
                    &mut self.settings_version,
                    &mut self.sync_delay,
                    args,
                    &mut reply,
                    &mut announcements(lanes),
                )
                .map(|changed| {
                    if changed == Changed::Changed {
                        self.dirty_since = Some(now_ns);
                    }
                }),
                Std::BroadcastPeriod => write(
                    &mut self.settings_version,
                    &mut self.broadcast_period,
                    args,
                    &mut reply,
                    &mut announcements(lanes),
                )
                .map(|changed| {
                    if changed == Changed::Changed {
                        self.dirty_since = Some(now_ns);
                    }
                }),
                Std::Start => return actions.defer(pending, Deferred::Acquire(SyncRequest::Start)),
                Std::Stop => return actions.defer(pending, Deferred::Acquire(SyncRequest::Stop)),
                Std::Restart => {
                    return actions.defer(pending, Deferred::Acquire(SyncRequest::Restart))
                }
                Std::Abort => return actions.defer(pending, Deferred::Flash(FlashOp::Abort)),
                Std::SettingsVersion => {
                    read(&mut reply, args, &self.settings_version.to_le_bytes())
                }
                Std::SyncStatus => self.sync_status.rpc(args, &mut reply).map(drop),
                Std::Priv => self.unlock(args, &mut reply),
                Std::PrivLock => {
                    self.privileged = false;
                    Ok(())
                }
                Std::PrivPassword => match args {
                    [] => Err(RpcError::WriteOnly),
                    _ => match self.password.rpc(args, &mut Reply::new()) {
                        Ok(Changed::Changed) => {
                            self.dirty_since = Some(now_ns);
                            Ok(())
                        }
                        Ok(Changed::Unchanged) => Ok(()),
                        Err(error) => Err(error),
                    },
                },
            }
            .map_err(Refusal::from),
        };
        answer(pending, result.map(|()| reply.as_slice()), &mut to(lanes));
    }

    /// `dev.priv`: a read says whether the session is unlocked, and a write
    /// takes the password. A board that names no password opens no session.
    fn unlock(&mut self, args: &[u8], out: &mut Reply) -> Result<(), RpcError> {
        let password = self.password.get();
        if args.is_empty() {
            return put(out, &[u8::from(self.privileged)]);
        }
        if password.as_bytes().is_empty() {
            return Err(RpcError::State);
        }
        if args != password.as_bytes() {
            return Err(RpcError::Invalid);
        }
        self.privileged = true;
        Ok(())
    }

    /// Answer the board's own entry at `position`, announcing a write to the
    /// asking port ahead of its reply. `None` is a position it does not reach.
    /// A write the group's rule refuses is taken back before anyone sees it.
    fn entry(
        &mut self,
        position: usize,
        args: &[u8],
        now_ns: u64,
        out: &mut Reply,
        lanes: &mut (impl Lanes + ?Sized),
    ) -> Option<Result<(), Refusal>> {
        let Self {
            settings,
            settings_version,
            ..
        } = self;
        let mut before = Reply::new();
        let mut found = None;
        let mut wrote = None;
        let mut moved = false;
        let mut dirty = false;
        nth(settings, position, &mut |entry| match entry {
            Entry::Setting(cell) => {
                found = Some(
                    cell.save(&mut before)
                        .and_then(|()| cell.rpc(args, out))
                        .map(|changed| {
                            if changed == Changed::Changed {
                                wrote = Some((cell.name(), cell.spec().access));
                            }
                        }),
                );
            }
            Entry::Diagnostic(diagnostic) => {
                found = Some(match args {
                    [] => diagnostic.read(out),
                    _ => Err(RpcError::ReadOnly),
                });
            }
            Entry::Action(action) => {
                moved = true;
                let mut lane = announcements(&mut *lanes);
                found = Some(action.run(&mut Announcer {
                    version: settings_version,
                    dirty: &mut dirty,
                    events: &mut lane,
                }));
            }
        });
        if let Some((name, access)) = wrote {
            if let Err(refusal) = settings.check() {
                let mut restored = Ok(Changed::Unchanged);
                nth(settings, position, &mut |entry| {
                    if let Entry::Setting(cell) = entry {
                        restored = cell.load(&before, &mut Reply::new());
                    }
                });
                return Some(restored.map_err(Refusal::from).and(Err(refusal)));
            }
            moved = true;
            dirty |= access.contains(Access::PERSISTENT);
            announce(settings_version, &mut announcements(&mut *lanes), name, out);
        }
        if moved {
            settings.publish();
        }
        if dirty {
            self.dirty_since = Some(now_ns);
        }
        found.map(|result| result.map_err(Refusal::from))
    }

    /// Every persistent cell, as `dev.conf.save` hands them to the flash.
    fn save(&mut self, image: &mut Entries) -> Result<(), RpcError> {
        let mut outcome = conf::encode(
            [
                &mut self.password as &mut dyn Persisted,
                &mut self.autostart,
                &mut self.loglevel,
                &mut self.sync_delay,
                &mut self.broadcast_period,
            ],
            image,
        );
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
            autostart,
            loglevel,
            sync_delay,
            broadcast_period,
            ..
        } = self;
        let mut outcome =
            conf::load([password as &mut dyn Persisted], image, |_, _| {}).and(conf::load(
                [
                    autostart as &mut dyn Persisted,
                    loglevel,
                    sync_delay,
                    broadcast_period,
                ],
                image,
                |name, reply| {
                    announce(
                        settings_version,
                        &mut announcements(&mut *lanes),
                        name,
                        reply,
                    )
                },
            ));
        settings.entries(&mut |entry| {
            let Entry::Setting(cell) = entry else {
                return;
            };
            let mut lane = announcements(&mut *lanes);
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
    /// `settings.version`, and uptime a boot starts from, opened as [`Device::new`] opens one.
    pub fn reboot(
        &mut self,
        session: SessionId,
        now_ns: u64,
        streams: &(impl Streams + ?Sized),
        lanes: &mut (impl Lanes + ?Sized),
    ) {
        self.session = session;
        self.booted_ns = now_ns;
        self.loglevel.reset();
        self.password.reset();
        self.autostart.reset();
        self.sync_delay.reset();
        self.broadcast_period.reset();
        self.privileged = false;
        self.settings_version = 0;
        self.dirty_since = None;
        self.open(now_ns, streams, lanes);
    }

    /// Open the session on control, a heartbeat ahead of the device record, and start the
    /// carousel's pass over.
    fn open(
        &mut self,
        now_ns: u64,
        streams: &(impl Streams + ?Sized),
        lanes: &mut (impl Lanes + ?Sized),
    ) {
        let mut control = announcements(lanes);
        self.beat(now_ns, &mut control);
        send(&mut control, &[], |buf| {
            Metadata::Device(self.record(streams)).write(MetadataFlags::UPDATE, buf)
        });
        self.carousel =
            metadata::Carousel::new(now_ns, self.pass_ns(), self.record(streams), streams);
    }

    /// Nanoseconds the carousel takes for a pass, or zero for none.
    fn pass_ns(&self) -> u64 {
        u64::from(self.broadcast_period.get()) * NANOS_PER_MILLI
    }

    /// Send whatever is due at `now_ns`, the reports that moved as announcements, a heartbeat
    /// and the carousel's records, and hand back the configuration a change left unsaved for a
    /// period, owed until [`Device::saved`].
    pub fn tick(
        &mut self,
        now_ns: u64,
        streams: &(impl Streams + ?Sized),
        lanes: &mut (impl Lanes + ?Sized),
    ) -> Option<Entries> {
        let mut control = announcements(lanes);
        if now_ns >= self.next_beat {
            self.refresh(now_ns, &mut control);
            self.beat(now_ns, &mut control);
        }
        let mut carousel = self.carousel;
        carousel.turn(
            now_ns,
            self.pass_ns(),
            self.record(streams),
            streams,
            &mut |record, flags| send(&mut control, &[], |buf| record.write(flags, buf)),
        );
        self.carousel = carousel;
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
        self.next_beat
            .min(save)
            .min(self.carousel.deadline(self.pass_ns()))
    }

    /// Bring the reports up to date, announcing into `out` what moved.
    fn refresh(&mut self, now_ns: u64, out: &mut dyn Sink) {
        let hours = u32::try_from(now_ns.saturating_sub(self.booted_ns) / NANOS_PER_HOUR)
            .unwrap_or(u32::MAX)
            .saturating_add(self.lifetime_hours);
        let mut to = Announcer {
            version: &mut self.settings_version,
            dirty: &mut false,
            events: out,
        };
        self.settings.refresh(&mut to);
        let _ = to.set(&mut self.lifetime, hours);
        let _ = to.set(&mut self.sync_status, self.time_status);
    }

    fn beat(&mut self, now_ns: u64, out: &mut impl Sink) {
        let beat = Heartbeat::Session {
            session: self.session,
            settings: Some(self.settings_version),
        };
        send(out, &[], |buf| beat.write(buf));
        self.next_beat = now_ns + HEARTBEAT_INTERVAL;
    }
}

/// Answer a request where the request that asked says.
pub fn answer(pending: Pending<'_>, result: Result<&[u8], impl Into<Refusal>>, to: &mut impl Sink) {
    match result.map_err(Into::into) {
        Ok(value) => send(to, pending.routing, |buf| {
            rpc::write_reply(buf, pending.id, value)
        }),
        Err(Refusal { error, why }) => send(to, pending.routing, |buf| {
            rpc::write_error_with(buf, pending.id, error, why.as_bytes())
        }),
    }
}

/// Visit the entry at `position` in `group`'s table order, if it reaches that far.
fn nth(group: &mut (impl Group + ?Sized), position: usize, visit: &mut dyn FnMut(Entry<'_>)) {
    let mut seen = 0;
    group.entries(&mut |entry| {
        if seen == position {
            visit(entry);
        }
        seen += 1;
    });
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
) -> Result<Changed, RpcError> {
    let changed = setting.rpc(args, reply)?;
    match changed {
        Changed::Unchanged => {}
        Changed::Changed => announce(version, out, setting.name(), reply),
    }
    Ok(changed)
}

/// Count a change and send it as a SETTING packet numbered with the count,
/// carrying the value as the RPC of `name` replies it.
fn announce(version: &mut u32, out: &mut impl Sink, name: &str, reply: &[u8]) {
    *version = version.wrapping_add(1);
    let setting = Announcement {
        name: name.as_bytes(),
        seq: Some(*version),
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

    /// Move the read-only settings to what the tasks behind them now say,
    /// announcing each that moved.
    fn refresh(&mut self, to: &mut Announcer<'_>) {
        let _ = to;
    }

    /// Refuse a combination of these settings no hardware can run, naming what limits it.
    fn check(&self) -> Result<(), Refusal> {
        Ok(())
    }

    /// Put a group whose restored settings its rule refuses back at its defaults.
    /// A group of groups repairs each child, so one broken rule resets only its own.
    fn repair(&mut self, to: &mut Announcer<'_>) -> Result<(), Refusal> {
        let refused = self.check();
        let mut reset = Ok(());
        if refused.is_err() {
            self.entries(&mut |entry| {
                if let Entry::Setting(cell) = entry {
                    reset = reset.and(to.reset(cell));
                }
            });
        }
        refused.and(reset.map_err(Refusal::from))
    }
}

/// Why a write was refused, as its error reply carries it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// The code a host matches on.
    pub error: RpcError,
    /// The reason a host shows beside it, naming the setting that limits it.
    pub why: &'static str,
}

impl From<RpcError> for Refusal {
    fn from(error: RpcError) -> Self {
        Self { error, why: "" }
    }
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

    fn publish(&self) {
        Setting::publish(self);
    }
}

/// One entry a board adds to the RPC table, and what answers it.
pub enum Entry<'a> {
    /// A value a host reads and writes; a write is announced.
    Setting(&'a mut dyn Cell),
    /// A developer's number, read when asked and never announced.
    Diagnostic(&'a dyn Diagnostic),
    /// Something the device does, which may move several settings.
    Action(&'a mut dyn Action),
}

impl Entry<'_> {
    /// The table entry it declares, a diagnostic's always privileged and unannounced.
    pub fn spec(&self) -> RpcSpec {
        match self {
            Self::Setting(cell) => cell.spec(),
            Self::Diagnostic(diagnostic) => {
                RpcSpec::prop(diagnostic.name(), diagnostic.kind(), Access::READ).privileged()
            }
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

    /// Go back to its compiled default, encoding it into `out`.
    fn reset(&mut self, out: &mut Reply) -> Result<Changed, RpcError>;
}

impl<T: Scalar> Cell for Setting<T> {
    fn spec(&self) -> RpcSpec {
        Setting::spec(self)
    }

    fn rpc(&mut self, args: &[u8], out: &mut Reply) -> Result<Changed, RpcError> {
        Setting::rpc(self, args, out)
    }

    fn reset(&mut self, out: &mut Reply) -> Result<Changed, RpcError> {
        let before = self.get();
        Setting::reset(self);
        self.get().encode(out)?;
        Ok(match before == self.get() {
            true => Changed::Unchanged,
            false => Changed::Changed,
        })
    }
}

/// A diagnostic `read` computes when asked, typed so its entry and reply agree;
/// see [the five channels](crate#channels) for when a value is one.
pub struct Probe<T: Scalar> {
    name: &'static str,
    read: Read<T>,
}

/// Where a probe's value comes from.
enum Read<T: Scalar> {
    Computed(fn() -> T),
    Atomic(&'static dyn Atomic<T>),
}

impl<T: Scalar> Probe<T> {
    /// `name`, answered with what `read` returns.
    pub const fn new(name: &'static str, read: fn() -> T) -> Self {
        Self {
            name,
            read: Read::Computed(read),
        }
    }

    /// `name`, answered with what `atomic` holds.
    pub const fn atomic(name: &'static str, atomic: &'static dyn Atomic<T>) -> Self {
        Self {
            name,
            read: Read::Atomic(atomic),
        }
    }
}

impl<T: Scalar> Diagnostic for Probe<T> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn kind(&self) -> Kind {
        T::KIND
    }

    fn read(&self, out: &mut Reply) -> Result<(), RpcError> {
        match self.read {
            Read::Computed(read) => read(),
            Read::Atomic(atomic) => atomic.load(),
        }
        .encode(out)
    }
}

impl<T: Scalar> Group for Probe<T> {
    fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
        visit(Entry::Diagnostic(self));
    }
}

/// A read-only number for developers, such as a hub counter, whose entry the
/// device makes privileged so no board can make one public.
pub trait Diagnostic {
    /// Its RPC name.
    fn name(&self) -> &'static str;

    /// What it replies with.
    fn kind(&self) -> Kind;

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
    /// Move a setting, read-only or not, announcing it and counting it in `settings.version`.
    pub fn set<T: Scalar>(&mut self, setting: &mut Setting<T>, value: T) -> Result<(), RpcError> {
        if setting.put(value)? == Changed::Changed {
            let mut announced = Reply::new();
            setting.get().encode(&mut announced)?;
            *self.dirty |= Setting::spec(setting).access.contains(Access::PERSISTENT);
            announce(self.version, &mut self.events, setting.name(), &announced);
        }
        Ok(())
    }

    /// Put a cell back at its compiled default, announcing it if it moved.
    pub fn reset(&mut self, cell: &mut dyn Cell) -> Result<(), RpcError> {
        let mut announced = Reply::new();
        if cell.reset(&mut announced)? == Changed::Changed {
            *self.dirty |= cell.spec().access.contains(Access::PERSISTENT);
            announce(self.version, &mut self.events, cell.name(), &announced);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::filter::Butterworth4;
    use crate::data::Params;
    use crate::data::{ColumnDef, DataStream, StreamDef};
    use crate::rpc::put;
    use core::num::NonZeroU32;
    use core::sync::atomic::{AtomicU32, Ordering::Relaxed};
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
            visit(Entry::Diagnostic(&Doubled(self.value.get())));
            visit(Entry::Action(&mut Rewind(self)));
        }

        fn check(&self) -> Result<(), Refusal> {
            match self.value.get() {
                0..=100 => Ok(()),
                _ => Err(Refusal {
                    error: RpcError::Range,
                    why: "board.count is at most 100",
                }),
            }
        }
    }

    struct Doubled(u32);

    impl Diagnostic for Doubled {
        fn name(&self) -> &'static str {
            "board.doubled"
        }

        fn kind(&self) -> Kind {
            Kind::Uint(4)
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

    /// Two plain settings, then a group of its own: the whole vocabulary, a
    /// developer's privileged setting, and a report.
    struct Board {
        first: Setting<u32>,
        second: Setting<u32>,
        counter: Counter,
        secret: Setting<u32>,
        /// What the board's own task last measured, which hosts only read.
        live: Setting<u32>,
        published: core::cell::Cell<u32>,
        /// Where `board.second` publishes, as a task's static would be.
        taken: &'static AtomicU32,
    }

    std::thread_local! {
        /// The board's task's measurement, behind `board.live`, one per test thread.
        static LIVE: core::cell::Cell<u32> = const { core::cell::Cell::new(3) };
    }

    impl Board {
        fn new() -> Self {
            let taken = Box::leak(Box::new(AtomicU32::new(0)));
            Self {
                first: Setting::new("board.first", 7).persistent(),
                second: Setting::new("board.second", 0)
                    .persistent()
                    .publishes(taken),
                counter: Counter {
                    value: Setting::new("board.count", 5).persistent(),
                },
                secret: Setting::new("board.secret", 11).privileged(),
                live: Setting::new("board.live", 3).read_only(),
                published: core::cell::Cell::new(0),
                taken,
            }
        }
    }

    impl Group for Board {
        fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
            Group::entries(&mut self.first, visit);
            Group::entries(&mut self.second, visit);
            Group::entries(&mut self.counter, visit);
            Group::entries(&mut self.secret, visit);
            Group::entries(&mut self.live, visit);
        }

        fn publish(&self) {
            self.published.set(self.published.get() + 1);
            Group::publish(&self.second);
        }

        fn refresh(&mut self, to: &mut Announcer<'_>) {
            let _ = to.set(&mut self.live, LIVE.get());
        }

        fn check(&self) -> Result<(), Refusal> {
            self.counter.check()
        }

        fn repair(&mut self, to: &mut Announcer<'_>) -> Result<(), Refusal> {
            self.counter.repair(to)
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
        "board.secret",
        "board.live",
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

        fn announce(&mut self, packet: &[u8]) {
            self.to.send(packet);
            self.events.send(packet);
        }

        fn event(&mut self, packet: &[u8]) {
            self.events.send(packet);
        }
    }

    /// One device, the streams it describes, and the two lanes.
    struct Harness {
        device: Device<Board>,
        streams: [DataStream<4>; 1],
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

    /// A device come up at boot with no streams, its session opened where no port hears it.
    fn opened<S: Group>(
        identity: Identity,
        table: &'static [RpcSpec],
        settings: S,
        password: &str,
    ) -> Device<S> {
        let none: [DataStream<4>; 0] = [];
        let mut nowhere = OneLane(Sent::default());
        Device::new(
            identity,
            SESSION,
            table,
            settings,
            password,
            BOOT,
            &none[..],
            &mut nowhere,
        )
    }

    fn device(table: &'static [RpcSpec]) -> Device<Board> {
        let identity = identity().hardware("COMM-USB", "STM32G484xE", &[0xAB; 12], 8);
        opened(identity, table, Board::new(), PASSWORD)
    }

    fn harness() -> Harness {
        let params = Params {
            rate: NonZeroU32::new(10).unwrap(),
            decimation: NonZeroU32::new(4).unwrap(),
            enabled: true,
        };
        Harness {
            device: device(table()),
            streams: [DataStream::new(
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

        /// Boot the device again into `session`, its opening on the lanes.
        fn boot(&mut self, session: SessionId, now_ns: u64) {
            self.device
                .reboot(session, now_ns, &self.streams[..], &mut self.lanes);
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
            let image =
                self.device
                    .tick(now_ns, &self.streams[..], &mut OneLane(Sent::default()))?;
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
        device.device = opened(identity(), table(), Board::new(), PASSWORD);
        device.ask("dev.priv", PASSWORD.as_bytes());
        for name in ["dev.uid", "dev.model", "dev.mcu.model", "dev.revision"] {
            assert_eq!(device.ask(name, &[]).error(), RpcError::State, "{name}");
        }
    }

    #[test]
    fn restored_lifetime_hours_keep_the_boot_clock_separate_and_saturate() {
        let mut device = harness();
        device.device.restore_lifetime(123);
        let later = BOOT + 2 * NANOS_PER_HOUR + 1;
        assert_eq!(
            device.ask_at("dev.lifetime", &[], later).value(),
            &125u32.to_le_bytes()
        );
        assert_eq!(
            device.ask_at("dev.systime", &[], later).value(),
            &(later - BOOT).to_le_bytes()
        );
        device.device.restore_lifetime(u32::MAX);
        assert_eq!(
            device.ask_at("dev.lifetime", &[], later).value(),
            &u32::MAX.to_le_bytes()
        );
        let mut rebooted = harness();
        rebooted.device.restore_lifetime(125);
        assert_eq!(
            rebooted.ask("dev.lifetime", &[]).value(),
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
            &7_201u32.to_le_bytes()
        );
        assert_eq!(
            device.ask_at("dev.lifetime", &[], later).value(),
            &2u32.to_le_bytes()
        );
        assert_eq!(device.ask("dev.systime", b"0").error(), RpcError::ReadOnly);
        assert_eq!(device.ask("dev.uptime", b"0").error(), RpcError::ReadOnly);
        assert_eq!(device.ask("dev.lifetime", b"0").error(), RpcError::ReadOnly);
    }

    /// The table is fixed for the image, so a locked session lists every
    /// entry and reads the hash an unlocked one does.
    #[test]
    fn every_session_lists_the_whole_table_under_one_hash() {
        let mut device = harness();
        let hash = table::hash(device.table()).to_le_bytes();
        assert_eq!(device.listed(), device.table().len());
        assert!(device.listing().contains(&"board.secret".to_string()));
        assert_eq!(device.ask("rpc.hash", &[]).value(), hash);
        let info = device.ask("rpc.info", b"board.secret").value().to_vec();
        assert_eq!(
            u16::from_le_bytes(info.try_into().unwrap()),
            0x8000 | (4 << 4) | 0x0100 | 0x0200 | 0x2000 | 0x4000,
            "a board's privileged setting says so and is announced"
        );

        device.ask("dev.priv", PASSWORD.as_bytes());
        assert_eq!(device.listed(), device.table().len());
        assert_eq!(device.ask("rpc.hash", &[]).value(), hash);
    }

    /// A locked session is refused a privileged entry with the state error,
    /// and `dev.priv` itself stays callable.
    #[test]
    fn a_locked_session_is_refused_what_needs_privilege() {
        let mut device = harness();
        for name in [
            "board.secret",
            "board.doubled",
            "dev.priv.lock",
            "dev.priv.password",
        ] {
            let refused = device.ask(name, &[]);
            assert_eq!(refused.error(), RpcError::State, "{name}");
            let packet = &refused.0[0];
            let header = Header::parse_prefix(packet).unwrap();
            let Some(Answer::Error(error)) =
                Answer::parse(header.ptype, &packet[header.payload_range()])
            else {
                panic!("{name} is refused");
            };
            assert_eq!(error.message, LOCKED.as_bytes(), "the refusal says why");
        }
        assert_eq!(device.ask("dev.priv", &[]).value(), &[0]);
        assert_eq!(
            device.ask("dev.mcu.model", &[]).value(),
            b"STM32G484xE",
            "the MCU is a public report"
        );
    }

    #[test]
    fn the_password_unlocks_the_session_and_locks_it_again() {
        let mut device = harness();
        assert_eq!(device.ask("dev.priv", b"wrong").error(), RpcError::Invalid);
        assert_eq!(device.ask("dev.priv", PASSWORD.as_bytes()).value(), b"");
        assert_eq!(device.ask("dev.priv", &[]).value(), &[1]);

        assert_eq!(
            device.ask("board.secret", &[]).value(),
            &11u32.to_le_bytes()
        );
        assert_eq!(
            device.lanes.events.announced(),
            [],
            "unlocking announces nothing"
        );

        assert_eq!(device.ask("dev.priv.lock", &[]).value(), b"");
        assert_eq!(device.ask("dev.priv", &[]).value(), &[0]);
        assert_eq!(device.ask("board.secret", &[]).error(), RpcError::State);
    }

    /// An unlocked session writes a privileged setting as it writes any other.
    #[test]
    fn an_unlocked_session_writes_a_privileged_setting() {
        let mut device = harness();
        device.ask("dev.priv", PASSWORD.as_bytes());
        let written = device.ask("board.secret", &12u32.to_le_bytes());
        assert_eq!(written.value(), &12u32.to_le_bytes());
        assert_eq!(
            written.announced(),
            [("board.secret".to_string(), 12u32.to_le_bytes().to_vec())]
        );
    }

    /// The password is written, never read back, and a write answers nothing.
    #[test]
    fn the_password_is_write_only() {
        let mut device = harness();
        device.ask("dev.priv", PASSWORD.as_bytes());
        assert_eq!(
            device.ask("dev.priv.password", &[]).error(),
            RpcError::WriteOnly
        );
        let written = device.ask("dev.priv.password", b"hunter2");
        assert_eq!(written.value(), b"");
        assert_eq!(written.announced(), []);
    }

    /// `dev.autostart` boots at five seconds, and a written value
    /// is announced and restored at the next boot.
    #[test]
    fn a_written_autostart_is_announced_and_restored_at_the_next_boot() {
        let mut device = harness();
        assert_eq!(
            device.ask("dev.autostart", &[]).value(),
            [DEFAULT_AUTOSTART]
        );
        assert_eq!(
            device.ask("dev.autostart", &[0]).announced(),
            [("dev.autostart".to_owned(), vec![0])]
        );
        device.ask("dev.conf.save", &[]);
        let Some((_, Deferred::Flash(FlashOp::ConfSave(image)))) = device.deferred.take() else {
            panic!("a save carries the image");
        };

        device.boot(SessionId::new(2), BOOT);
        assert_eq!(device.device.autostart(), DEFAULT_AUTOSTART);
        device.drive(Input::Loaded(None, Ok(&image)), BOOT);
        assert_eq!(device.device.autostart(), 0);
    }

    /// A read-only setting refuses a host's write, and what its task measures
    /// reaches a read at once and every port once per change.
    #[test]
    fn a_read_only_setting_follows_its_task_and_announces_each_change() {
        let mut device = harness();
        assert_eq!(device.ask("board.live", &[]).value(), 3u32.to_le_bytes());
        assert_eq!(
            device.ask("board.live", &9u32.to_le_bytes()).error(),
            RpcError::ReadOnly
        );
        LIVE.set(4);
        assert_eq!(device.ask("board.live", &[]).value(), 4u32.to_le_bytes());
        device.ask("board.live", &[]);
        assert_eq!(
            device.lanes.events.announced(),
            [("board.live".to_owned(), 4u32.to_le_bytes().to_vec())],
            "every port hears it, once per change"
        );
    }

    /// `sync.delay` boots at zero, and a written value is announced and
    /// restored at the next boot.
    #[test]
    fn a_written_sync_delay_is_announced_and_restored_at_the_next_boot() {
        let mut device = harness();
        let delay = 12.5f32.to_le_bytes();
        assert_eq!(
            device.ask("sync.delay", &delay).announced(),
            [("sync.delay".to_owned(), delay.to_vec())]
        );
        device.ask("dev.conf.save", &[]);
        let Some((_, Deferred::Flash(FlashOp::ConfSave(image)))) = device.deferred.take() else {
            panic!("a save carries the image");
        };

        device.boot(SessionId::new(2), BOOT);
        assert_eq!(device.device.sync_delay_ns(), 0.0);
        device.drive(Input::Loaded(None, Ok(&image)), BOOT);
        assert_eq!(device.device.sync_delay_ns(), 12.5);
        assert_eq!(
            device.ask("sync.delay", &f32::NAN.to_le_bytes()).error(),
            RpcError::Invalid
        );
    }

    /// The board's password is a compiled default the developer overrides,
    /// stored like any other persistent setting.
    #[test]
    fn a_written_password_is_saved_and_unlocks_the_next_boot() {
        let mut device = harness();
        device.ask("dev.priv", PASSWORD.as_bytes());
        assert_eq!(device.ask("dev.priv.password", b"hunter2").value(), b"");
        device.ask("dev.conf.save", &[]);
        let Some((_, Deferred::Flash(FlashOp::ConfSave(image)))) = device.deferred.take() else {
            panic!("a save carries the image");
        };

        device.boot(SessionId::new(2), BOOT);
        assert_eq!(device.ask("dev.priv", &[]).value(), &[0], "a boot locks");
        device.drive(Input::Loaded(None, Ok(&image)), BOOT);
        assert_eq!(
            device.ask("dev.priv", PASSWORD.as_bytes()).error(),
            RpcError::Invalid,
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
        device.device = opened(identity(), table(), Board::new(), "");
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

    /// Every port's control lane alone.
    #[derive(Default)]
    struct Control(Sent);

    impl Lanes for Control {
        fn reply(&mut self, _: &[u8]) {
            panic!("no request was asked");
        }

        fn announce(&mut self, packet: &[u8]) {
            self.0.send(packet);
        }

        fn event(&mut self, _: &[u8]) {
            panic!("nothing was logged");
        }
    }

    /// The carousel steps a pass of the device's and every stream's records, a stopped one too,
    /// through every port's control lane in the pass time `dev.metadata.broadcast.period` names.
    #[test]
    fn the_carousel_describes_every_stream_on_control() {
        let mut device = harness();
        device.ask("dev.metadata.broadcast.period", &300u16.to_le_bytes());
        let mut control = Control::default();
        (1..=4).for_each(|tick| {
            let now = BOOT + tick * 100 * NANOS_PER_MILLI;
            device.device.tick(now, &device.streams[..], &mut control);
        });
        let records: Vec<_> = control
            .0
            .views()
            .iter()
            .filter(|view| view.header.ptype == PacketType::METADATA)
            .map(|view| data::split_metadata(view.payload).unwrap())
            .map(|(kind, flags, _)| (MetadataType::from(kind), flags))
            .collect();
        use MetadataType::*;
        let periodic = MetadataFlags::PERIODIC;
        assert_eq!(
            records,
            [
                (Device, periodic),
                (Stream, periodic),
                (Column, periodic | MetadataFlags::LAST)
            ]
        );
    }

    /// What a host learns the moment it connects, ending on the record that
    /// closes the sweep.
    #[test]
    fn connecting_sends_a_heartbeat_and_the_sweep() {
        let mut device = harness();
        let views = device.connect(BOOT).views();
        let types: Vec<_> = views.iter().map(|view| view.header.ptype).collect();
        assert_eq!(
            types,
            [
                PacketType::HEARTBEAT,
                PacketType::METADATA,
                PacketType::METADATA,
                PacketType::METADATA,
                PacketType::METADATA,
            ]
        );
        assert_eq!(
            Heartbeat::parse(views[0].payload),
            Some(Heartbeat::Session {
                session: SESSION,
                settings: Some(0)
            })
        );
        let kinds: Vec<_> = views[1..]
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
            .tick(
                BOOT + HEARTBEAT_INTERVAL / 2,
                &device.streams[..],
                &mut OneLane(&mut sent)
            )
            .is_none());
        assert!(sent.0.is_empty());
        assert!(device
            .device
            .tick(
                BOOT + HEARTBEAT_INTERVAL,
                &device.streams[..],
                &mut OneLane(&mut sent)
            )
            .is_none());
        assert_eq!(sent.views()[0].header.ptype, PacketType::HEARTBEAT);
        assert_eq!(device.device.deadline(), BOOT + 2 * HEARTBEAT_INTERVAL);
    }

    /// A heartbeat carries the sequence number of the last announcement, so a
    /// host that missed one learns it within an interval.
    #[test]
    fn a_heartbeat_carries_the_settings_sequence() {
        let mut device = harness();
        device.ask("board.first", &8u32.to_le_bytes());
        let mut sent = Sent::default();
        device.device.tick(
            BOOT + HEARTBEAT_INTERVAL,
            &device.streams[..],
            &mut OneLane(&mut sent),
        );
        assert_eq!(
            Heartbeat::parse(sent.views()[0].payload),
            Some(Heartbeat::Session {
                session: SESSION,
                settings: Some(1)
            })
        );
    }

    /// A write to `dev.loglevel` reaches the gate the platform's own logging is
    /// checked against.
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
        device.boot(SessionId::new(10), later);
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
        let beat = Heartbeat::Session {
            session: SESSION,
            settings: Some(0),
        };
        let len = beat.write(&mut buf).unwrap();
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
        assert_eq!(names[STANDARD.len() - 1], "dev.metadata.broadcast.period");
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
        let answered_announced = answered.announced();
        assert_eq!(
            answered_announced,
            [("board.second".to_owned(), 42u32.to_le_bytes().to_vec())]
        );
        assert_eq!(
            Announcement::parse(answered.views()[0].payload)
                .unwrap()
                .seq,
            Some(1),
            "numbered with the count it makes"
        );
        assert_eq!(
            device.lanes.events.announced(),
            answered_announced,
            "every port hears a write"
        );
        assert_eq!(version(&mut device), 1, "the write is counted");

        // Repeating an accepted value is also quiet.
        assert_eq!(device.ask("board.second", &42u32.to_le_bytes()).0.len(), 1);
        assert_eq!(version(&mut device), 1);

        // Reading it back neither announces nor counts.
        assert_eq!(device.ask("board.second", &[]).0.len(), 1);
        assert_eq!(version(&mut device), 1);
    }

    /// Two control ports, as a board with a host USB port and a parent UART
    /// has: an announcement reaches both in one order, and the port that
    /// asked hears it before its reply.
    struct Ports {
        ports: [Sent; 2],
        asking: usize,
    }

    impl Lanes for Ports {
        fn reply(&mut self, packet: &[u8]) {
            self.ports[self.asking].send(packet);
        }

        fn announce(&mut self, packet: &[u8]) {
            self.ports.iter_mut().for_each(|port| port.send(packet));
        }

        fn event(&mut self, packet: &[u8]) {
            self.ports.iter_mut().for_each(|port| port.send(packet));
        }
    }

    /// A session opens on every port with its heartbeat and then the device record, and every
    /// later heartbeat shares their lane.
    #[test]
    fn a_session_opens_on_every_port_with_its_heartbeat_and_record() {
        let mut device = harness();
        let rebooted = SessionId::new(SESSION.value() + 1);
        let mut lanes = Ports {
            ports: Default::default(),
            asking: 1,
        };
        device
            .device
            .reboot(rebooted, BOOT, &device.streams[..], &mut lanes);
        for port in &lanes.ports {
            let views = port.views();
            assert_eq!(
                Heartbeat::parse(views[0].payload),
                Some(Heartbeat::Session {
                    session: rebooted,
                    settings: Some(0),
                })
            );
            assert!(matches!(
                twinleaf_proto::data::Metadata::parse(views[1].payload),
                Some((twinleaf_proto::data::Metadata::Device(record), _)) if record.session == rebooted
            ));
        }

        device.boot(rebooted, BOOT);
        device.device.tick(
            BOOT + HEARTBEAT_INTERVAL,
            &device.streams[..],
            &mut device.lanes,
        );
        let control: Vec<PacketType> = device
            .lanes
            .to
            .views()
            .iter()
            .map(|view| view.header.ptype)
            .collect();
        assert_eq!(
            control,
            [
                PacketType::HEARTBEAT,
                PacketType::METADATA,
                PacketType::HEARTBEAT
            ]
        );
    }

    #[test]
    fn every_port_hears_each_announcement_in_one_sequence() {
        let mut device = harness();
        let mut lanes = Ports {
            ports: Default::default(),
            asking: 0,
        };
        for (port, name, value) in [(0, "board.first", 8u32), (1, "board.second", 9)] {
            lanes.asking = port;
            let packet = asking(Method::ByName(name.as_bytes()), &value.to_le_bytes(), &[]);
            let (view, _) = PacketView::parse_prefix(&packet).unwrap();
            device
                .device
                .handle(&device.streams[..], Input::Packet(view), BOOT, &mut lanes);
        }
        let heard = |port: &Sent| -> Vec<(PacketType, Option<u32>)> {
            port.views()
                .iter()
                .map(|view| {
                    let seq = (view.header.ptype == PacketType::SETTING)
                        .then(|| Announcement::parse(view.payload).unwrap().seq)
                        .flatten();
                    (view.header.ptype, seq)
                })
                .collect()
        };
        let [first, second] = &lanes.ports;
        assert_eq!(
            heard(first),
            [
                (PacketType::SETTING, Some(1)),
                (PacketType::RPC_REP, None),
                (PacketType::SETTING, Some(2)),
            ]
        );
        assert_eq!(
            heard(second),
            [
                (PacketType::SETTING, Some(1)),
                (PacketType::SETTING, Some(2)),
                (PacketType::RPC_REP, None),
            ]
        );
    }

    #[test]
    fn a_diagnostic_is_computed_when_asked_and_refuses_a_write() {
        let mut device = harness();
        device.ask("dev.priv", PASSWORD.as_bytes());
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
        let types: Vec<_> = answered
            .views()
            .iter()
            .map(|view| view.header.ptype)
            .collect();
        assert_eq!(types, [PacketType::SETTING, PacketType::RPC_REP]);
        assert!(answered.value().is_empty());
        assert_eq!(
            device.lanes.events.announced(),
            [("board.count".to_owned(), 0u32.to_le_bytes().to_vec())]
        );
        assert_eq!(device.ask("board.count", &[]).value(), &0u32.to_le_bytes());
    }

    /// A write the group's rule refuses is taken back unannounced, and its
    /// error reply carries the reason after the code.
    #[test]
    fn a_refused_write_is_taken_back_and_says_why() {
        let mut device = harness();
        let refused = device.ask("board.count", &101u32.to_le_bytes());
        let header = Header::parse_prefix(&refused.0[0]).unwrap();
        let Some(Answer::Error(error)) =
            Answer::parse(header.ptype, &refused.0[0][header.payload_range()])
        else {
            panic!("an error, not a reply");
        };
        assert_eq!(error.error(), RpcError::Range);
        assert_eq!(error.message, b"board.count is at most 100");
        assert_eq!(refused.announced(), []);
        assert_eq!(device.device.settings().published.get(), 0);
        assert!(device.tick(BOOT + 2 * AUTOSAVE_INTERVAL).is_none());
        assert_eq!(device.ask("board.count", &[]).value(), &5u32.to_le_bytes());
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
        device.ask("board.second", &9u32.to_le_bytes());
        assert_eq!(device.device.settings().taken.load(Relaxed), 9);
    }

    /// A probe over an atomic answers what the atomic holds when asked.
    #[test]
    fn a_probe_reads_its_atomic_when_asked() {
        static COUNT: AtomicU32 = AtomicU32::new(3);
        let probe = Probe::atomic("board.count", &COUNT);
        let read = |probe: &Probe<u32>| {
            let mut out = Reply::new();
            probe.read(&mut out).map(|()| out.to_vec())
        };
        assert_eq!(read(&probe), Ok(3u32.to_le_bytes().to_vec()));
        COUNT.store(4, Relaxed);
        assert_eq!(read(&probe), Ok(4u32.to_le_bytes().to_vec()));
    }

    /// A name the table declares and no entry answers comes back for the
    /// platform to answer, which is what lets one keep an RPC of its own. It
    /// carries the route it arrived on, so the answer goes back down it.
    #[test]
    fn a_name_no_entry_answers_comes_back_to_the_platform_with_its_route() {
        let mut specs: Vec<RpcSpec> = table().to_vec();
        specs.push(RpcSpec::std("board.capture", Access::READ));
        let mut device = harness();
        device.device = opened(
            identity(),
            Box::leak(specs.into_boxed_slice()),
            Board::new(),
            PASSWORD,
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
            assert_eq!(
                answered.0.len(),
                answered.announced().len(),
                "{name} was answered here"
            );
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

    /// A stop or restart is handed to the platform, which closes each stream's segment on its
    /// data, and announces nothing.
    #[test]
    fn a_stop_or_restart_is_handed_over_and_announces_nothing() {
        for name in ["dev.stop", "dev.restart"] {
            let mut device = harness();
            assert!(
                device.ask(name, &[]).0.is_empty(),
                "{name} is the platform's to answer"
            );
            assert!(matches!(
                device.deferred.take(),
                Some((_, Deferred::Acquire(_)))
            ));
        }
    }

    /// The standard reports that move are announced as they move, at the
    /// refresh a heartbeat or a request makes, so a host may cache them.
    #[test]
    fn a_moving_standard_report_is_announced_at_the_next_refresh() {
        let mut device = harness();
        device.device.set_time_status(3);
        let mut sent = Sent::default();
        device.device.tick(
            BOOT + NANOS_PER_HOUR,
            &device.streams[..],
            &mut OneLane(&mut sent),
        );
        assert_eq!(
            sent.announced(),
            [
                ("dev.lifetime".to_owned(), 1u32.to_le_bytes().to_vec()),
                ("sync.status".to_owned(), vec![3]),
            ]
        );
        assert_eq!(
            Heartbeat::parse(sent.views()[2].payload).and_then(|beat| beat.settings()),
            Some(2),
            "the heartbeat after them counts them"
        );
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
                b"dev.autostart",
                b"dev.loglevel",
                b"sync.delay",
                b"dev.metadata.broadcast.period",
                b"board.first",
                b"board.second",
                b"board.count"
            ],
            "every persistent cell, in table order"
        );
        assert_eq!(stored[6].1, 42u32.to_le_bytes());
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
        assert_eq!(
            device.lanes.to.announced().len(),
            device.lanes.to.0.len(),
            "nothing asked for the restore, so nothing is answered"
        );
        assert_eq!(
            device.logged,
            [(LogLevel::INFO, "restored the saved configuration")]
        );
        assert_eq!(device.device.settings().published.get(), 1);
        assert_eq!(device.device.settings().taken.load(Relaxed), 42);
        assert_eq!(
            device.ask("board.second", &[]).value(),
            &42u32.to_le_bytes()
        );
    }

    /// A stored image the rule refuses puts back only the group that broke
    /// it, so a setting stored beside it survives.
    #[test]
    fn a_restore_the_rule_refuses_resets_only_the_group_that_broke_it() {
        let mut device = harness();
        let mut image = Entries::new();
        let mut second = Setting::new("board.second", 42u32).persistent();
        let mut count = Setting::new("board.count", 500u32).persistent();
        conf::encode([&mut second as &mut dyn Persisted, &mut count], &mut image).unwrap();

        device.drive(Input::Loaded(None, Ok(&image)), BOOT);
        assert_eq!(
            device.lanes.events.announced(),
            [
                ("board.second".to_owned(), 42u32.to_le_bytes().to_vec()),
                ("board.count".to_owned(), 500u32.to_le_bytes().to_vec()),
                ("board.count".to_owned(), 5u32.to_le_bytes().to_vec()),
            ]
        );
        assert_eq!(
            device.logged,
            [(LogLevel::WARNING, "board.count is at most 100")]
        );
        assert_eq!(
            device.ask("board.second", &[]).value(),
            &42u32.to_le_bytes()
        );
        assert_eq!(device.ask("board.count", &[]).value(), &5u32.to_le_bytes());
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

    /// A written setting is saved a period after the write, once, and the
    /// heartbeat is still the sooner deadline.
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
        assert_eq!(stored[6], (&b"board.second"[..], &42u32.to_le_bytes()[..]));
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

    /// A save stores `dev.loglevel` with whatever else it writes, but writing it owes none.
    #[test]
    fn a_write_to_dev_loglevel_owes_no_save() {
        let mut device = harness();
        device.ask_at("dev.loglevel", &[LogLevel::DEBUG.value()], BOOT);
        assert!(device.tick(BOOT + 2 * AUTOSAVE_INTERVAL).is_none());
    }

    /// A stored `dev.loglevel` is the threshold the platform boots on.
    #[test]
    fn a_restored_loglevel_reaches_the_platform() {
        let mut device = harness();
        let mut image = Entries::new();
        let mut stored = Setting::new("dev.loglevel", LogLevel::DEBUG.value()).persistent();
        conf::encode([&mut stored as &mut dyn Persisted], &mut image).unwrap();

        device.drive(Input::Loaded(None, Ok(&image)), BOOT);
        assert_eq!(device.loglevel, LogLevel::DEBUG);
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
    /// asked for it, so it leaves nothing owing.
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

    /// A read leaves `Changed::Unchanged`, so it owes no save.
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
        assert!(
            device
                .device
                .tick(due, &device.streams[..], &mut OneLane(&mut sent))
                .is_some(),
            "the save"
        );
        assert!(
            device
                .device
                .tick(due + 1, &device.streams[..], &mut OneLane(&mut sent))
                .is_some(),
            "still owed"
        );
        device.device.saved();
        assert!(device
            .device
            .tick(due + 2, &device.streams[..], &mut OneLane(&mut sent))
            .is_none());
    }

    /// A save that does not fit its image is owed for another period, and
    /// tried again when that period ends.
    #[test]
    fn a_save_too_large_for_its_image_is_owed_for_another_period() {
        let cells = (0..IMAGE_MAX / 16)
            .map(|at| Box::leak(format!("board.filler{at:03}").into_boxed_str()))
            .map(|name| Setting::new(name, 0u32).persistent())
            .collect();
        let mut board = Fat(cells);
        let mut specs = STANDARD.to_vec();
        board.entries(&mut |entry| specs.push(entry.spec()));
        let mut device = opened(
            identity(),
            Box::leak(specs.into_boxed_slice()),
            board,
            PASSWORD,
        );

        let asked = asking(Method::ByName(b"board.filler000"), &1u32.to_le_bytes(), &[]);
        let (view, _) = PacketView::parse_prefix(&asked).unwrap();
        let none: [DataStream<4>; 0] = [];
        let mut lanes = Both::default();
        device.handle(&none[..], Input::Packet(view), BOOT, &mut lanes);
        assert_eq!(lanes.to.value(), &1u32.to_le_bytes(), "the write it owes");

        let mut sent = Sent::default();
        assert!(
            device
                .tick(BOOT + AUTOSAVE_INTERVAL, &none[..], &mut OneLane(&mut sent))
                .is_none(),
            "the image has no room for them"
        );
        assert!(device
            .tick(
                BOOT + 2 * AUTOSAVE_INTERVAL - 1,
                &none[..],
                &mut OneLane(&mut sent)
            )
            .is_none());
        assert_eq!(
            device.deadline(),
            BOOT + 2 * AUTOSAVE_INTERVAL,
            "owed for another period"
        );
    }

    /// A device and its streams on a [`Sim`](crate::sim::Sim), alone or as the hub of children,
    /// read by hosts on its ports that each merge its lanes in an order of their own.
    mod contract {
        use super::*;
        use crate::data::Timeref;
        use crate::sim::{self, Port, Sim};
        use proptest::prelude::*;
        use std::collections::VecDeque;
        use twinleaf_proto::contract::{self, Observed, Sender};
        use twinleaf_proto::data::{Metadata, SegmentFlags};
        use twinleaf_proto::packet::Lane;
        use twinleaf_proto::sync::Epoch;
        use twinleaf_proto::DeviceRoute;

        const RATE: u32 = 10;
        const SECOND: u64 = 1_000_000_000;
        /// What a child's port holds, so the hop above it never laps it.
        const LOSSLESS: usize = 1 << 20;

        impl sim::Board<4> for Board {
            fn streams(&self) -> Vec<DataStream<4>> {
                streams().into()
            }
        }

        /// What a host asks or a platform does next. A port is a host's, taken modulo the hosts,
        /// and a device is the hub at `/` or the child at its index, taken modulo the devices.
        #[derive(Debug, Clone)]
        enum Step {
            /// A device's every stream takes this many samples, a period apart.
            Sample(usize, u32),
            /// A device's every stream loses this many samples.
            Skip(usize, u32),
            /// A host asks a device this RPC with these argument bytes, and it is read at once.
            Ask(usize, usize, &'static str, Vec<u8>),
            /// A host asks a device this RPC with these argument bytes, and it is in flight.
            Send(usize, usize, &'static str, Vec<u8>),
            /// The request a host has had in flight longest is read.
            Deliver(usize),
            /// A host asks a device for `dev.restart` this many times, one after another.
            Restarts(usize, usize, usize),
            /// A device's stream acquires at this rate with this decimation, enabled or not, from
            /// its next segment.
            Retune(usize, usize, u32, u32, bool),
            /// A device's stream begins a new segment.
            Rollover(usize, usize),
            /// A device's time reference is replaced by one this many seconds ahead, or behind.
            Relabel(usize, i32),
            /// The clock moves this many nanoseconds on.
            Wait(u64),
            /// A host reads the next packet of each lane named, in turn.
            Read(usize, Vec<Lane>),
            /// The hub forwards the next packet of each lane of a child's port named, in turn.
            Forward(usize, Vec<Lane>),
            /// A host lets its port go and opens it again.
            Reconnect(usize),
            /// A device boots again into its next session.
            Reboot(usize),
        }

        fn lanes() -> impl Strategy<Value = Vec<Lane>> {
            let lane = prop::sample::select(&[Lane::Control, Lane::Data, Lane::Event][..]);
            prop::collection::vec(lane, 1..16)
        }

        fn step() -> impl Strategy<Value = Step> {
            let control = || prop::sample::select(&["dev.start", "dev.stop", "dev.restart"][..]);
            let at = || (0..4usize, 0..3usize);
            prop_oneof![
                4 => (0..3usize, 1..30u32).prop_map(|(at, count)| Step::Sample(at, count)),
                1 => (0..3usize, 1..20u32).prop_map(|(at, count)| Step::Skip(at, count)),
                4 => (at(), control()).prop_map(|((port, at), name)| Step::Ask(port, at, name, Vec::new())),
                2 => (at(), control()).prop_map(|((port, at), name)| Step::Send(port, at, name, Vec::new())),
                2 => (0..4usize).prop_map(Step::Deliver),
                1 => (at(), 2..6usize).prop_map(|((port, at), count)| Step::Restarts(port, at, count)),
                1 => (at(), any::<u32>()).prop_map(|((port, at), value)| {
                    Step::Ask(port, at, "board.first", value.to_le_bytes().to_vec())
                }),
                2 => (0..3usize, 0..2usize, prop::sample::select(&[5, RATE, 20][..]), 1..=3u32, any::<bool>())
                    .prop_map(|(at, stream, rate, decimation, enabled)| {
                        Step::Retune(at, stream, rate, decimation, enabled)
                    }),
                1 => (0..3usize, 0..2usize).prop_map(|(at, stream)| Step::Rollover(at, stream)),
                1 => (0..3usize, -5..=5i32).prop_map(|(at, shift)| Step::Relabel(at, shift)),
                2 => (0..2 * HEARTBEAT_INTERVAL).prop_map(Step::Wait),
                6 => (0..4usize, lanes()).prop_map(|(port, lanes)| Step::Read(port, lanes)),
                6 => (0..3usize, lanes()).prop_map(|(at, lanes)| Step::Forward(at, lanes)),
                1 => (0..4usize).prop_map(Step::Reconnect),
                1 => (0..3usize).prop_map(Step::Reboot),
            ]
        }

        fn params(rate: u32, decimation: u32, enabled: bool) -> Params {
            Params {
                rate: NonZeroU32::new(rate).unwrap(),
                decimation: NonZeroU32::new(decimation).unwrap(),
                enabled,
            }
        }

        /// Two streams, one sending a packet every four samples and one only when full.
        fn streams() -> [DataStream<4>; 2] {
            let stream = |id| {
                let filter = Box::leak(Box::new(Butterworth4::<1>::new()));
                DataStream::new(StreamId::new(id), &AUX, filter, params(RATE, 1, true)).unwrap()
            };
            [stream(1).batched(16), stream(2)]
        }

        fn booted(session: u32, ports: Vec<Port>) -> Sim<Board, 4> {
            let identity = identity().hardware("COMM-USB", "STM32G484xE", &[0xAB; 12], 8);
            let mut sim = Sim::new(
                identity,
                SessionId::new(session),
                table(),
                Board::new(),
                PASSWORD,
                BOOT,
                ports,
            );
            sim.reference = Timeref::new(Epoch::UNIX, 0, SessionId::new(1), "REF");
            sim
        }

        /// One host on a port of the hub: what it heard and asked in the order it did, cut where
        /// it lost its link or fell behind, and its requests still in flight.
        #[derive(Default)]
        struct Host {
            heard: Vec<(Sender, Vec<u8>)>,
            cut: Vec<Vec<(Sender, Vec<u8>)>>,
            in_flight: VecDeque<Vec<u8>>,
            next_id: u16,
            dropped: u64,
        }

        /// The hub and its children, every host, and the clock.
        struct World {
            devices: Vec<Sim<Board, 4>>,
            hosts: Vec<Host>,
            now: u64,
            reference: u32,
            /// Whether every host reads all it can in write order after each step.
            eager: bool,
        }

        impl World {
            /// A hub with `children`, booted with `ports` hosts on ports of `capacity` packets a
            /// lane.
            fn new(children: usize, ports: usize, capacity: usize) -> Self {
                let devices = (0..=children)
                    .map(|at| match at {
                        0 => booted(1, (0..ports).map(|_| Port::new(capacity)).collect()),
                        child => booted(0x100 * child as u32, vec![Port::new(LOSSLESS)]),
                    })
                    .collect();
                let hosts = (0..ports)
                    .map(|port| Host {
                        next_id: (port as u16) << 12,
                        ..Host::default()
                    })
                    .collect();
                Self {
                    devices,
                    hosts,
                    now: BOOT,
                    reference: 1,
                    eager: false,
                }
            }

            /// One device and one host, every host reading eagerly.
            fn alone() -> Self {
                Self {
                    eager: true,
                    ..Self::new(0, 1, LOSSLESS)
                }
            }

            fn take(&mut self, step: &Step) {
                let devices = self.devices.len();
                let hosts = self.hosts.len();
                match *step {
                    Step::Sample(at, count) => (0..count).for_each(|_| {
                        self.now += SECOND / u64::from(RATE);
                        let device = &mut self.devices[at % devices];
                        (0..2).for_each(|stream| device.push(stream, &1.0f32.to_le_bytes()));
                        self.tick();
                    }),
                    Step::Skip(at, count) => {
                        self.now += u64::from(count) * SECOND / u64::from(RATE);
                        let device = &mut self.devices[at % devices];
                        (0..2).for_each(|stream| device.skip(stream, count));
                    }
                    Step::Ask(port, at, name, ref args) => {
                        self.send(port % hosts, at % devices, name, args);
                        while !self.hosts[port % hosts].in_flight.is_empty() {
                            self.deliver(port % hosts);
                        }
                    }
                    Step::Send(port, at, name, ref args) => {
                        self.send(port % hosts, at % devices, name, args)
                    }
                    Step::Deliver(port) => self.deliver(port % hosts),
                    Step::Restarts(port, at, count) => (0..count)
                        .for_each(|_| self.take(&Step::Ask(port, at, "dev.restart", Vec::new()))),
                    Step::Retune(at, stream, rate, decimation, enabled) => {
                        self.devices[at % devices].retune(stream, params(rate, decimation, enabled))
                    }
                    Step::Rollover(at, stream) => self.devices[at % devices].rollover(stream),
                    Step::Relabel(at, shift) => {
                        self.reference += 1;
                        let reference = SessionId::new(self.reference);
                        self.devices[at % devices].relabel(i64::from(shift), reference, self.now);
                    }
                    Step::Wait(ns) => {
                        self.now += ns;
                        self.tick();
                    }
                    Step::Read(port, ref lanes) => lanes
                        .iter()
                        .for_each(|&lane| self.read(port % hosts, |port| port.lane(lane).pop())),
                    Step::Forward(at, ref lanes) => match at % devices {
                        0 => {}
                        child => lanes.iter().for_each(|&lane| self.forward(child, lane)),
                    },
                    Step::Reconnect(port) => self.reconnect(port % hosts),
                    Step::Reboot(at) => {
                        let device = &mut self.devices[at % devices];
                        let session = SessionId::new(device.device.session.value().wrapping_add(1));
                        device.reboot(session, self.now);
                        if at % devices == 0 {
                            self.hosts.iter_mut().for_each(|host| {
                                host.cut.push(core::mem::take(&mut host.heard));
                                host.in_flight.clear();
                            });
                        }
                    }
                }
                if self.eager {
                    (0..hosts).for_each(|port| self.drain(port));
                }
            }

            fn tick(&mut self) {
                let now = self.now;
                self.devices.iter_mut().for_each(|device| device.tick(now));
            }

            /// A host's request to a device, put in flight.
            fn send(&mut self, port: usize, at: usize, name: &str, args: &[u8]) {
                let host = &mut self.hosts[port];
                let mut buf = [0u8; Packet::MAX_SIZE];
                let method = Method::ByName(name.as_bytes());
                let id = RpcRequestId::new(host.next_id);
                host.next_id = host.next_id.wrapping_add(1);
                let mut len = rpc::write_request(&mut buf, id, method, args).unwrap();
                if at > 0 {
                    len = route::push_hop(&mut buf, at as u8).unwrap();
                }
                host.heard.push((Sender::Host, buf[..len].to_vec()));
                host.in_flight.push_back(buf[..len].to_vec());
            }

            /// The request a host has had in flight longest reaches the device its route names,
            /// through the hub's hop for a child, and a call no entry answers is refused.
            fn deliver(&mut self, port: usize) {
                let Some(mut request) = self.hosts[port].in_flight.pop_front() else {
                    return;
                };
                let (at, from) = match route::pop_hop(&mut request) {
                    Ok((hop, len)) => {
                        request.truncate(len);
                        (usize::from(hop), 0)
                    }
                    Err(_) => (0, port),
                };
                let device = &mut self.devices[at];
                if let Some(call) = device.request(from, &request, self.now) {
                    device.answer(&call, Err(RpcError::NotFound));
                }
            }

            /// The hub sends up the next packet of a child's lane, wearing the child's hop.
            fn forward(&mut self, child: usize, lane: Lane) {
                let Some(mut packet) = self.devices[child].ports[0].lane(lane).pop() else {
                    return;
                };
                packet.resize(Packet::MAX_SIZE, 0);
                let len = route::push_hop(&mut packet, child as u8).unwrap();
                self.devices[0].forward(&packet[..len]);
            }

            /// A host reads the packet `next` takes from its port, reconnecting first when the
            /// port lapped it.
            fn read(&mut self, port: usize, next: impl FnOnce(&mut Port) -> Option<Vec<u8>>) {
                if self.devices[0].ports[port].dropped() != self.hosts[port].dropped {
                    self.reconnect(port);
                }
                if let Some(packet) = next(&mut self.devices[0].ports[port]) {
                    self.hosts[port].heard.push((Sender::Device, packet));
                }
            }

            /// A host reads everything its port holds, in write order.
            fn drain(&mut self, port: usize) {
                while !self.devices[0].ports[port].is_empty() {
                    self.read(port, Port::written);
                }
            }

            /// A host lets its port go and opens it again, starting a trace of its own.
            fn reconnect(&mut self, port: usize) {
                let host = &mut self.hosts[port];
                host.cut.push(core::mem::take(&mut host.heard));
                host.in_flight.clear();
                let hub = &mut self.devices[0];
                hub.disconnect(port);
                hub.connect(port, self.now);
                host.dropped = hub.ports[port].dropped();
            }

            /// The hub forwards all its children hold, and every host reads all its port holds.
            fn settle(&mut self) {
                (1..self.devices.len()).for_each(|child| {
                    while !self.devices[child].ports[0].is_empty() {
                        [Lane::Control, Lane::Data, Lane::Event]
                            .into_iter()
                            .for_each(|lane| self.forward(child, lane));
                    }
                });
                (0..self.hosts.len()).for_each(|port| self.drain(port));
            }

            /// Every packet the first host heard, as views.
            fn views(&self) -> Vec<PacketView<'_>> {
                self.hosts[0]
                    .heard
                    .iter()
                    .map(|(_, packet)| PacketView::parse_prefix(packet).unwrap().0)
                    .collect()
            }

            /// The segment records of `stream` the first host heard, in order.
            fn records(&self, stream: u8) -> Vec<(u8, SegmentFlags)> {
                self.views()
                    .into_iter()
                    .filter_map(|view| match Metadata::parse(view.payload) {
                        Some((Metadata::Segment(record), _))
                            if view.header.ptype == PacketType::METADATA =>
                        {
                            Some(record)
                        }
                        Some(_) | None => None,
                    })
                    .filter(|record| record.stream_id.value() == stream)
                    .map(|record| (record.segment_id.value(), record.flags))
                    .collect()
            }

            /// What breaks the contract in any trace a host heard.
            fn broken(&mut self) -> Vec<contract::Violation> {
                self.settle();
                self.hosts
                    .iter()
                    .flat_map(|host| host.cut.iter().chain([&host.heard]))
                    .flat_map(|heard| contract::check(&observed(heard)))
                    .collect()
            }

            /// Every trace keeps the contract.
            fn keeps(&mut self) {
                assert_eq!(self.broken(), []);
            }
        }

        /// A host's trace, each packet on the route its hops name.
        fn observed(heard: &[(Sender, Vec<u8>)]) -> Vec<Observed<'_>> {
            heard
                .iter()
                .map(|(sender, packet)| {
                    let packet = PacketView::parse_prefix(packet).unwrap().0;
                    Observed {
                        arrival_ns: 0,
                        route: DeviceRoute::from_wire(packet.routing).unwrap(),
                        sender: *sender,
                        packet,
                    }
                })
                .collect()
        }

        proptest! {
            /// Whatever hosts ask and the platforms do, starting, stopping, retuning, rolling
            /// over, relabelling and rebooting, on a device alone or a hub whose hop keeps each
            /// route's lanes in order and interleaves them freely, with requests read at once or
            /// after a while in flight, every host that merges its lanes in any order and falls
            /// behind or not hears the contract kept on every route.
            #[test]
            fn a_device_keeps_the_contract(
                children in 0..3usize,
                ports in 1..4usize,
                capacity in prop::sample::select(&[8, 64, LOSSLESS][..]),
                steps in prop::collection::vec(step(), 1..60),
            ) {
                let mut world = World::new(children, ports, capacity);
                steps.iter().for_each(|step| world.take(step));
                prop_assert_eq!(world.broken(), []);
            }
        }

        /// A device opens its session with a heartbeat and then its record, which declares the
        /// revision the contract holds it to.
        #[test]
        fn a_device_declares_the_revision_it_keeps_as_it_boots() {
            let mut world = World::alone();
            world.settle();
            let views = world.views();
            assert_eq!(views[0].header.ptype, PacketType::HEARTBEAT);
            let Some((Metadata::Device(record), _)) = Metadata::parse(views[1].payload) else {
                panic!("the device record follows the heartbeat");
            };
            assert_eq!(record.protocol, twinleaf_proto::PROTOCOL);
            assert_eq!(record.n_streams, 2);
        }

        /// A stop sends each stream's staged rows and then the record that closes its segment, and
        /// a restart's next segment says a start opened it, where a switch's does not.
        #[test]
        fn a_stop_closes_each_stream_after_its_last_row() {
            let mut world = World::alone();
            world.take(&Step::Ask(0, 0, "dev.start", Vec::new()));
            world.take(&Step::Sample(0, 15));
            world.take(&Step::Rollover(0, 1));
            world.take(&Step::Sample(0, 15));
            world.take(&Step::Ask(0, 0, "dev.restart", Vec::new()));
            world.take(&Step::Ask(0, 0, "dev.stop", Vec::new()));
            let open = SegmentFlags::VALID | SegmentFlags::ACTIVE;
            let start = open | SegmentFlags::START;
            let closed = SegmentFlags::VALID | SegmentFlags::START;
            let records = world.records(2);
            assert_eq!(records.first(), Some(&(0, start)));
            assert!(records.contains(&(1, open)), "a switch opened segment 1");
            assert!(
                records.contains(&(1, SegmentFlags::VALID)),
                "the restart closed it"
            );
            assert_eq!(records[records.len() - 2..], [(2, start), (2, closed)]);
            let views = world.views();
            let last = |stream: u8| {
                views
                    .iter()
                    .rposition(|view| view.header.ptype == PacketType::stream(stream).unwrap())
            };
            let closes = views
                .iter()
                .rposition(|view| view.header.ptype == PacketType::METADATA)
                .unwrap();
            assert!(
                last(2).is_some_and(|row| row < closes),
                "the rows went first"
            );
            world.keeps();
        }

        /// A segment that reaches the sample numbers a packet carries rolls over at its next
        /// whole second and keeps the contract across it.
        #[test]
        fn the_wire_limit_rollover_keeps_the_contract() {
            let mut world = World::alone();
            world.take(&Step::Ask(0, 0, "dev.start", Vec::new()));
            world.take(&Step::Sample(0, 5));
            world.take(&Step::Skip(0, 15_999_990));
            world.take(&Step::Sample(0, 30));
            let rows = world
                .views()
                .into_iter()
                .filter_map(|view| twinleaf_proto::data::Samples::parse(view.header, view.payload));
            assert!(
                rows.map(|row| row.segment_id.value()).any(|id| id == 1),
                "it rolled over"
            );
            world.keeps();
        }

        /// Restarts that take no samples each open a segment, so the ring comes back to the first's id
        /// with a count that starts past zero, which is lawful as each record goes out as it opens.
        #[test]
        fn restarts_that_take_no_samples_keep_the_contract() {
            let mut world = World::alone();
            [
                Step::Ask(0, 0, "dev.start", Vec::new()),
                Step::Sample(0, 1),
                Step::Relabel(0, 0),
                Step::Skip(0, 10),
                Step::Restarts(0, 0, 3),
                Step::Skip(0, 1),
                Step::Sample(0, 5),
            ]
            .iter()
            .for_each(|step| world.take(step));
            world.keeps();
        }

        /// A time reference four seconds behind lands a stream's fifth segment on the first's id
        /// and start, which is lawful as the reference is another.
        #[test]
        fn a_reference_that_steps_back_keeps_the_contract() {
            let mut world = World::alone();
            world.take(&Step::Ask(0, 0, "dev.start", Vec::new()));
            world.take(&Step::Sample(0, 10));
            world.take(&Step::Relabel(0, -4));
            (0..3).for_each(|_| {
                world.take(&Step::Sample(0, 10));
                world.take(&Step::Rollover(0, 0));
            });
            world.take(&Step::Sample(0, 10));
            world.keeps();
        }

        /// A device that reboots while no host connects opens its new session with its
        /// heartbeat and record, so the settings it numbers from one again follow them.
        #[test]
        fn a_reboot_no_host_connects_to_opens_its_session_with_its_heartbeat() {
            let mut world = World::alone();
            world.take(&Step::Ask(0, 0, "dev.start", Vec::new()));
            world.take(&Step::Sample(0, 5));
            world.take(&Step::Ask(0, 0, "board.first", 8u32.to_le_bytes().to_vec()));
            world.take(&Step::Sample(0, 5));
            world.take(&Step::Reboot(0));
            world.take(&Step::Ask(0, 0, "board.first", 9u32.to_le_bytes().to_vec()));
            let after: Vec<PacketType> =
                world.views().iter().map(|view| view.header.ptype).collect();
            assert_eq!(
                after,
                [
                    PacketType::HEARTBEAT,
                    PacketType::METADATA,
                    PacketType::RPC_REQ,
                    PacketType::SETTING,
                    PacketType::RPC_REP
                ]
            );
            world.keeps();
        }

        /// A stop in flight while a retune switches segments at a whole second reaches the
        /// device after the new segment's record went out, which keeps the contract as the
        /// host saw it.
        #[test]
        fn a_stop_in_flight_across_a_segment_switch_keeps_the_contract() {
            let mut world = World::alone();
            world.take(&Step::Ask(0, 0, "dev.start", Vec::new()));
            world.take(&Step::Sample(0, 5));
            world.take(&Step::Retune(0, 0, RATE, 2, true));
            world.take(&Step::Send(0, 0, "dev.stop", Vec::new()));
            world.take(&Step::Sample(0, 10));
            world.take(&Step::Deliver(0));
            let views = world.views();
            let asked = views
                .iter()
                .rposition(|view| view.header.ptype == PacketType::RPC_REQ)
                .unwrap();
            let crossed = views[asked..].iter().any(|view| {
                view.header.ptype == PacketType::METADATA
                    && matches!(
                        Metadata::parse(view.payload),
                        Some((Metadata::Segment(_), _))
                    )
            });
            assert!(crossed, "a record went out while the stop was in flight");
            world.keeps();
        }

        /// A child that reboots while its last session's rows still wait at the hub has its new
        /// heartbeat heard ahead of them, which keeps the contract on its route.
        #[test]
        fn a_childs_heartbeat_overtaking_its_last_rows_at_the_hub_keeps_the_contract() {
            let mut world = World::new(1, 1, LOSSLESS);
            world.take(&Step::Ask(0, 1, "dev.start", Vec::new()));
            world.take(&Step::Sample(1, 20));
            world.take(&Step::Forward(1, vec![Lane::Control; 8]));
            world.take(&Step::Forward(1, vec![Lane::Data; 64]));
            world.take(&Step::Reboot(1));
            world.take(&Step::Forward(1, vec![Lane::Control; 2]));
            world.take(&Step::Read(0, vec![Lane::Control; 64]));
            world.take(&Step::Ask(0, 1, "dev.start", Vec::new()));
            world.take(&Step::Sample(1, 20));
            assert!(
                !world.devices[0].ports[0].data.is_empty(),
                "rows wait at the hub"
            );
            world.keeps();
        }

        /// A host that falls behind its port is cut off with the drop counted, and the trace it
        /// starts as it opens the port again keeps the contract.
        #[test]
        fn a_host_that_falls_behind_reconnects_and_keeps_the_contract() {
            let mut world = World::new(0, 1, 8);
            world.take(&Step::Ask(0, 0, "dev.start", Vec::new()));
            world.take(&Step::Sample(0, 60));
            world.take(&Step::Read(0, vec![Lane::Data; 4]));
            assert_eq!(world.hosts[0].cut.len(), 1, "the lap cut the trace");
            assert!(
                world.devices[0].ports[0].data.dropped > 0,
                "and counted its drops"
            );
            world.take(&Step::Sample(0, 10));
            world.keeps();
        }
    }
}

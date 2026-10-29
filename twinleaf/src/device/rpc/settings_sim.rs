//! A deterministic simulation of a `twinleaf-device` board behind faulty
//! links and the hosts caching its settings, which checks every
//! [`SettingsCache`] against what the device announced.
//!
//! Time is simulated and every choice comes from one seed, so a case replays
//! exactly. Each host waits on its calls as a state machine, and the cache's
//! own [`SettingsCache::snapshot_with`] runs on a thread that runs only while
//! the world waits on its next call.

use super::error::{CallError, RpcErrorPayload};
use super::registry::{RpcDescriptor, RpcRegistry};
use super::reply::PendingReply;
use super::settings::{CacheAction, SettingsCache};
use crate::proto::heartbeat::Heartbeat;
use crate::proto::packet::{PacketType, PacketView};
use crate::proto::rpc::{self as wire, Method, Request, RpcError, RpcMetaFlags};
use crate::proto::settings::Setting as Announcement;
use crate::proto::{DeviceRoute, RpcRequestId, SessionId};
use crate::tio::packet::Payload;
use crate::tio::rpc::RpcReply;
use crate::tio::Packet;
use crate::DeviceEvent;
use proptest::prelude::*;
use proptest::test_runner::TestRunner;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::sync::OnceLock;
use std::time::Instant;
use twinleaf_device::device::{
    self, Action, Actions, Announcer, Deferred, Entries, Entry, FlashOp, Group, Identity, Input,
    Lanes, Probe, Refusal, SyncRequest,
};
use twinleaf_device::rpc::{finite, RpcSpec, Setting, STANDARD};
use twinleaf_device::sync::AcquisitionState;
use twinleaf_device::{Device, Sink};

const MS: u64 = 1_000_000;

/// How long a host waits for a reply.
const TIMEOUT: u64 = 250 * MS;

/// How long a quiet period lasts, room for a heartbeat, a timed-out call,
/// another heartbeat and a whole snapshot at the slowest link.
const QUIET: u64 = 4_000 * MS;

const PASSWORD: &str = "sim";

/// Values a float setting is written with, a negative zero, a NaN and an
/// infinity among them.
const FLOATS: [f32; 7] = [0.0, -0.0, 1.5, -2.25, f32::NAN, f32::INFINITY, 1e-40];

/// The board's own entries. Its settings have a clamp, a group rule, a check
/// and a privilege, two reports track what it measures, and an action moves
/// two settings.
struct Board {
    gain: Setting<u8>,
    lo: Setting<u16>,
    hi: Setting<u16>,
    offset: Setting<f32>,
    mode: Setting<bool>,
    secret: Setting<u8>,
    level: Setting<u8>,
    temp: Setting<i16>,
    probe: Probe<u8>,
    measured: (u8, i16),
}

impl Board {
    fn new(measured: (u8, i16)) -> Board {
        Board {
            gain: Setting::new("test.gain", 10)
                .checked(|gain| Ok(gain.min(100)))
                .persistent(),
            lo: Setting::new("test.lo", 0),
            hi: Setting::new("test.hi", 100),
            offset: Setting::new("test.offset", 0.0)
                .checked(finite)
                .persistent(),
            mode: Setting::new("test.mode", false),
            secret: Setting::new("test.secret", 0).privileged(),
            level: Setting::new("test.level", 0).read_only(),
            temp: Setting::new("test.temp", 0).read_only(),
            probe: Probe::new("test.probe", || 7),
            measured,
        }
    }
}

/// `test.zero`, which puts the gain and the offset back to zero.
struct Zero<'a>(&'a mut Setting<u8>, &'a mut Setting<f32>);

impl Action for Zero<'_> {
    fn spec(&self) -> RpcSpec {
        RpcSpec::action("test.zero")
    }

    fn run(&mut self, to: &mut Announcer<'_>) -> Result<(), RpcError> {
        to.set(self.0, 0)?;
        to.set(self.1, 0.0)
    }
}

impl Group for Board {
    fn entries(&mut self, visit: &mut dyn FnMut(Entry<'_>)) {
        visit(Entry::Setting(&mut self.gain));
        visit(Entry::Setting(&mut self.lo));
        visit(Entry::Setting(&mut self.hi));
        visit(Entry::Setting(&mut self.offset));
        visit(Entry::Setting(&mut self.mode));
        visit(Entry::Setting(&mut self.secret));
        visit(Entry::Setting(&mut self.level));
        visit(Entry::Setting(&mut self.temp));
        visit(Entry::Diagnostic(&self.probe));
        visit(Entry::Action(&mut Zero(&mut self.gain, &mut self.offset)));
    }

    fn refresh(&mut self, to: &mut Announcer<'_>) {
        let _ = to.set(&mut self.level, self.measured.0);
        let _ = to.set(&mut self.temp, self.measured.1);
    }

    fn check(&self) -> Result<(), Refusal> {
        match self.lo.get() <= self.hi.get() {
            true => Ok(()),
            false => Err(Refusal {
                error: RpcError::Range,
                why: "test.lo is above test.hi",
            }),
        }
    }
}

/// The standard table and the board's entries.
fn table() -> &'static [RpcSpec] {
    static TABLE: OnceLock<Vec<RpcSpec>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut specs = STANDARD.to_vec();
        Board::new((0, 0)).entries(&mut |entry| specs.push(entry.spec()));
        specs
    })
}

fn flags(spec: &RpcSpec) -> RpcMetaFlags {
    RpcMetaFlags::from_meta(spec.metadata())
}

/// Whether a host may cache the entry, readable, announced and unprivileged.
fn eligible(spec: &RpcSpec) -> bool {
    let flags = flags(spec);
    flags.contains(RpcMetaFlags::READABLE | RpcMetaFlags::ANNOUNCED)
        && !flags.contains(RpcMetaFlags::PRIVILEGED)
        && !flags.contains(RpcMetaFlags::CAPTURE)
}

fn ahead(seq: u32, applied: u32) -> bool {
    (seq.wrapping_sub(applied) as i32) > 0
}

/// A call a host makes.
#[derive(Debug, Clone, Copy)]
enum Target {
    Loglevel,
    Autostart,
    SyncDelay,
    Gain,
    Lo,
    Hi,
    Offset,
    Mode,
    Secret,
    Level,
    Zero,
    Start,
    Stop,
    Unlock,
    Lock,
    Save,
    Reboot,
}

impl Target {
    /// The RPC and argument of this call, from one byte of choice.
    fn call(self, pick: u8) -> (&'static str, Vec<u8>) {
        let float = FLOATS[usize::from(pick) % FLOATS.len()]
            .to_le_bytes()
            .to_vec();
        match self {
            Target::Loglevel => ("dev.loglevel", vec![pick % 8]),
            Target::Autostart => ("dev.autostart", vec![pick]),
            Target::SyncDelay => ("sync.delay", float),
            Target::Gain => ("test.gain", vec![pick]),
            Target::Lo => ("test.lo", u16::from(pick).to_le_bytes().to_vec()),
            Target::Hi => ("test.hi", u16::from(pick).to_le_bytes().to_vec()),
            Target::Offset => ("test.offset", float),
            Target::Mode => ("test.mode", vec![pick % 3]),
            Target::Secret => ("test.secret", vec![pick]),
            Target::Level => ("test.level", vec![pick]),
            Target::Zero => ("test.zero", Vec::new()),
            Target::Start => ("dev.start", Vec::new()),
            Target::Stop => ("dev.stop", Vec::new()),
            Target::Unlock => ("dev.priv", PASSWORD.as_bytes().to_vec()),
            Target::Lock => ("dev.priv.lock", Vec::new()),
            Target::Save => ("dev.conf.save", Vec::new()),
            Target::Reboot => ("dev.reboot", Vec::new()),
        }
    }
}

/// One thing that happens to the system.
#[derive(Debug, Clone)]
enum Op {
    /// A host calls once.
    Call {
        client: u8,
        target: Target,
        pick: u8,
    },
    /// A host sends a burst of writes without waiting, as a slider does.
    Slider {
        client: u8,
        target: Target,
        picks: Vec<u8>,
    },
    /// Two hosts call the same entry at the same instant.
    Race { target: Target, picks: (u8, u8) },
    /// What the board measures moves.
    Measure { level: u8, temp: i16 },
    /// The device loses power and comes back.
    PowerCycle,
    /// Nothing moves and no link loses a packet, after which every cache is exact.
    Quiet,
}

/// How each link mistreats packets.
#[derive(Debug, Clone)]
struct Faults {
    /// Packets dropped, per mille.
    drop: u32,
    /// Packets duplicated, per mille.
    duplicate: u32,
    /// Whether a packet may overtake one sent before it.
    reorder: bool,
    delay_ms: u64,
    jitter_ms: u64,
    /// Whether heartbeats interleave freely with replies and announcements,
    /// as [`Lanes`] allows.
    free_events: bool,
}

/// What the firmware puts on the wire.
#[derive(Debug, Clone)]
struct Firmware {
    /// Whether it has `settings.version`.
    counter: bool,
    /// Whether its heartbeat carries the sequence.
    beat_settings: bool,
    /// Whether it numbers its announcements.
    numbered: bool,
    /// Whether each session's sequence starts a few short of wrapping.
    wrap: bool,
}

#[derive(Debug, Clone)]
struct Case {
    seed: u64,
    ports: usize,
    clients: Vec<usize>,
    faults: Faults,
    firmware: Firmware,
    ops: Vec<(u64, Op)>,
}

/// What a run counted.
#[derive(Debug, Default, Clone, Copy)]
struct Stats {
    runs: u64,
    sim_ms: u64,
    dropped: u64,
    snapshots: u64,
    failed_snapshots: u64,
    reloads: u64,
    exact_checks: u64,
    stale_session_checks: u64,
    quiet_checks: u64,
    own_writes_held: u64,
    own_writes_missed: u64,
}

impl Stats {
    fn add(&mut self, other: &Stats) {
        self.runs += other.runs;
        self.sim_ms += other.sim_ms;
        self.dropped += other.dropped;
        self.snapshots += other.snapshots;
        self.failed_snapshots += other.failed_snapshots;
        self.reloads += other.reloads;
        self.exact_checks += other.exact_checks;
        self.stale_session_checks += other.stale_session_checks;
        self.quiet_checks += other.quiet_checks;
        self.own_writes_held += other.own_writes_held;
        self.own_writes_missed += other.own_writes_missed;
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let z = (self.0 ^ (self.0 >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, per_mille: u32) -> bool {
        self.below(1000) < u64::from(per_mille)
    }
}

/// Which of the device's lanes a packet went out on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lane {
    Reply,
    Announce,
    Event,
}

/// What one input to the device wrote.
#[derive(Default)]
struct Out(Vec<(Lane, Vec<u8>)>);

impl Lanes for Out {
    fn reply(&mut self, packet: &[u8]) {
        self.0.push((Lane::Reply, packet.to_vec()));
    }

    fn announce(&mut self, packet: &[u8]) {
        self.0.push((Lane::Announce, packet.to_vec()));
    }

    fn event(&mut self, packet: &[u8]) {
        self.0.push((Lane::Event, packet.to_vec()));
    }
}

impl Sink for Out {
    fn send(&mut self, packet: &[u8]) {
        self.reply(packet);
    }
}

/// Every value a session's cacheable entries held, by the wire sequence
/// number it held them at.
struct Oracle {
    first: u32,
    states: Vec<BTreeMap<String, Vec<u8>>>,
}

impl Oracle {
    fn at(&self, version: u32) -> Option<&BTreeMap<String, Vec<u8>>> {
        self.states.get(version.wrapping_sub(self.first) as usize)
    }

    fn latest(&self) -> (u32, &BTreeMap<String, Vec<u8>>) {
        let last = self.states.len() - 1;
        (self.first.wrapping_add(last as u32), &self.states[last])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owed {
    Nothing,
    Snapshot,
    Reload,
}

/// One host connection and its cache.
struct Client {
    port: usize,
    cache: Option<SettingsCache>,
    /// The session the registry behind the cache was read in.
    session: Option<SessionId>,
    owed: Owed,
    inbox: VecDeque<DeviceEvent>,
    /// The calls it waits on. A host does nothing else meanwhile.
    calling: Option<Calling>,
    /// Counts calls, so a timeout meant for an earlier one is ignored.
    generation: u64,
    ops: VecDeque<Op>,
    /// The last `settings.version` the last snapshot read, which the cache is
    /// exact from once it has applied that far.
    settled: Option<u32>,
    /// Whether the cache was told the device is legacy since it was built.
    legacy: bool,
    /// Whether it heard another session's heartbeat since it was built.
    foreign: bool,
    rebuilt: u64,
}

/// A host's calls in flight, each reply with how many events were queued
/// ahead of it, and what the host does once they are in.
struct Calling {
    ids: Vec<u16>,
    replies: BTreeMap<u16, (Result<Vec<u8>, CallError>, usize)>,
    then: Then,
}

enum Then {
    Writes(Vec<&'static str>),
    Registry,
    Snapshot(Snapshotting),
}

/// A snapshot running [`SettingsCache::snapshot_with`] on its own thread,
/// which runs only while the world waits on its next step.
struct Snapshotting {
    reply: flume::Sender<Result<Vec<u8>, CallError>>,
    next: flume::Receiver<Step>,
    reading: String,
    settled: Option<u32>,
}

enum Step {
    Call(String, Vec<u8>),
    Done(SettingsCache, Result<(), CallError>),
}

enum Wake {
    Timeout(usize, u64),
    Tick,
    Boot,
    ToDevice(usize, Vec<u8>),
    ToHost(usize, Vec<u8>),
    Op(Op),
    Check,
}

struct World<'c> {
    case: &'c Case,
    now: u64,
    rng: Rng,
    queue: BTreeMap<(u64, u64), Wake>,
    order: u64,
    device: Device<Board>,
    boots: u32,
    offset: u32,
    stored: Option<Entries>,
    oracle: Oracle,
    off: bool,
    tick_at: u64,
    /// When each port's link, down and up, is next clear, for links that keep order.
    clear: Vec<[u64; 2]>,
    quiet_until: u64,
    routes: BTreeMap<u16, usize>,
    next_id: u16,
    clients: Vec<Client>,
    failure: Option<String>,
    trace: VecDeque<String>,
    stats: Stats,
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Down,
    Up,
}

impl<'c> World<'c> {
    fn new(case: &'c Case) -> World<'c> {
        let identity = Identity::new("sim", "Simulated device", "SIM0001", "sim-fw")
            .expect("the identity fits");
        let device = Device::new(
            identity,
            SessionId::new(0),
            table(),
            Board::new((0, 0)),
            PASSWORD,
            0,
        );
        let clients = case
            .clients
            .iter()
            .map(|&port| Client {
                port: port % case.ports,
                cache: None,
                session: None,
                owed: Owed::Reload,
                inbox: VecDeque::new(),
                calling: None,
                generation: 0,
                ops: VecDeque::new(),
                settled: None,
                legacy: false,
                foreign: false,
                rebuilt: 0,
            })
            .collect();
        let mut world = World {
            case,
            now: 0,
            rng: Rng(case.seed),
            queue: BTreeMap::new(),
            order: 0,
            device,
            boots: 0,
            offset: 0,
            stored: None,
            oracle: Oracle {
                first: 0,
                states: vec![BTreeMap::new()],
            },
            off: true,
            tick_at: 0,
            clear: vec![[0; 2]; case.ports],
            quiet_until: 0,
            routes: BTreeMap::new(),
            next_id: 0,
            clients,
            failure: None,
            trace: VecDeque::new(),
            stats: Stats {
                runs: 1,
                ..Stats::default()
            },
        };
        world.boot();
        world
    }

    fn note(&mut self, line: String) {
        if self.trace.len() == 400 {
            self.trace.pop_front();
        }
        self.trace
            .push_back(format!("{:>9.3} ms  {line}", self.now as f64 / MS as f64));
    }

    fn fail(&mut self, violation: String) {
        if self.failure.is_none() {
            let trace: Vec<&str> = self.trace.iter().map(String::as_str).collect();
            self.failure = Some(format!(
                "{violation}\nat {} ms, seed {}\nlast events:\n{}",
                self.now / MS,
                self.case.seed,
                trace.join("\n")
            ));
        }
    }

    fn schedule(&mut self, at: u64, wake: Wake) {
        self.order += 1;
        self.queue.insert((at, self.order), wake);
    }

    /// Runs the world until nothing is left before `deadline`.
    fn run_until(&mut self, deadline: u64) {
        while self.failure.is_none() {
            let Some(entry) = self.queue.first_entry() else {
                break;
            };
            if entry.key().0 > deadline {
                break;
            }
            let ((at, _), wake) = entry.remove_entry();
            self.now = at;
            self.dispatch(wake);
        }
        self.now = self.now.max(deadline);
    }

    fn dispatch(&mut self, wake: Wake) {
        match wake {
            Wake::Timeout(c, generation) => {
                if self.clients[c].generation == generation {
                    self.finish(c);
                }
            }
            Wake::Tick => self.tick(),
            Wake::Boot => self.boot(),
            Wake::ToDevice(port, packet) => self.device_receives(port, &packet),
            Wake::ToHost(port, packet) => self.proxy_receives(port, &packet),
            Wake::Op(op) => self.op(op),
            Wake::Check => self.quiet_check(),
        }
    }

    /// Sends `packet` along one direction of a port's link, which may drop,
    /// duplicate, delay or reorder it outside a quiet period.
    fn send(&mut self, port: usize, direction: Direction, packet: Vec<u8>) {
        let faults = &self.case.faults;
        let faulty = self.now >= self.quiet_until;
        if faulty && self.rng.chance(faults.drop) {
            self.stats.dropped += 1;
            return;
        }
        let copies = 1 + usize::from(faulty && self.rng.chance(faults.duplicate));
        for _ in 0..copies {
            let delay = faults.delay_ms * MS + self.rng.below(faults.jitter_ms * MS + 1);
            let at = match faults.reorder {
                true => self.now + delay,
                false => {
                    let clear = &mut self.clear[port][direction as usize];
                    let at = (self.now + delay).max(*clear);
                    *clear = at;
                    at
                }
            };
            let wake = match direction {
                Direction::Down => Wake::ToDevice(port, packet.clone()),
                Direction::Up => Wake::ToHost(port, packet.clone()),
            };
            self.schedule(at, wake);
        }
    }

    /// Puts what the device wrote on the wire as the firmware would, a reply
    /// to the port that asked and the rest to every port, in the order it was
    /// written unless events fall freely between the rest.
    fn distribute(&mut self, out: Out, asking: Option<usize>, version: Option<RpcRequestId>) {
        let wired: Vec<(Lane, Vec<u8>)> = out
            .0
            .into_iter()
            .map(|(lane, packet)| (lane, self.wire(&packet, version)))
            .collect();
        for port in 0..self.case.ports {
            let lane = wired
                .iter()
                .filter(|(lane, _)| *lane != Lane::Reply || asking == Some(port));
            let (mut events, mut ordered): (VecDeque<_>, VecDeque<_>) =
                match self.case.faults.free_events {
                    true => lane.partition(|(lane, _)| *lane == Lane::Event),
                    false => (VecDeque::new(), lane.collect()),
                };
            while let Some((_, packet)) = match (events.is_empty(), ordered.is_empty()) {
                (false, false) if self.rng.chance(500) => events.pop_front(),
                (false, true) => events.pop_front(),
                (true, _) | (false, false) => ordered.pop_front(),
            } {
                self.send(port, Direction::Up, packet.clone());
            }
        }
    }

    /// The packet the firmware sends for what the device wrote, its sequence
    /// numbers offset toward a wrap, or left off where the firmware lacks them.
    fn wire(&self, packet: &[u8], version: Option<RpcRequestId>) -> Vec<u8> {
        let Ok((view, _)) = PacketView::parse_prefix(packet) else {
            return packet.to_vec();
        };
        let firmware = &self.case.firmware;
        let offset = |seq: u32| seq.wrapping_add(self.offset);
        let mut buf = [0u8; 512];
        let written = match view.header.ptype {
            PacketType::SETTING => Announcement::parse(view.payload).and_then(|setting| {
                Announcement {
                    seq: setting.seq.filter(|_| firmware.numbered).map(offset),
                    ..setting
                }
                .write(&mut buf)
            }),
            PacketType::HEARTBEAT => match Heartbeat::parse(view.payload) {
                Some(Heartbeat::Session { session, settings }) => Heartbeat::Session {
                    session,
                    settings: settings.filter(|_| firmware.beat_settings).map(offset),
                }
                .write(&mut buf),
                Some(Heartbeat::Any(_)) | None => None,
            },
            PacketType::RPC_REP => wire::Reply::parse(view.payload)
                .filter(|reply| Some(reply.req_id) == version)
                .and_then(|reply| Some((reply.req_id, u32::decode_reply(reply.value).ok()?)))
                .and_then(|(id, seq)| wire::write_reply(&mut buf, id, &offset(seq).to_le_bytes())),
            _ => None,
        };
        written.map_or_else(|| packet.to_vec(), |len| buf[..len].to_vec())
    }

    /// Counts each announcement into the oracle, which must see them
    /// numbered one after another.
    fn record(&mut self, out: &Out) {
        for (_, packet) in out.0.iter().filter(|(lane, _)| *lane == Lane::Announce) {
            let Some(setting) = PacketView::parse_prefix(packet)
                .ok()
                .and_then(|(view, _)| Announcement::parse(view.payload))
            else {
                return self.fail("(device) an announcement does not parse".into());
            };
            let name = setting.name_str().unwrap_or_default().to_string();
            let (latest, state) = self.oracle.latest();
            let expected = latest.wrapping_add(1);
            let seq = setting.seq.map(|seq| seq.wrapping_add(self.offset));
            if seq != Some(expected) {
                return self.fail(format!(
                    "(device) {name} was announced as {seq:?}, not {expected}"
                ));
            }
            let mut next = state.clone();
            if table()
                .iter()
                .any(|spec| spec.name == name && eligible(spec))
            {
                next.insert(name.clone(), setting.reply.to_vec());
            }
            self.oracle.states.push(next);
            self.note(format!(
                "device announces #{expected} {name} = {:?}",
                setting.reply
            ));
        }
    }

    /// Reads every cacheable entry and `settings.version` past the ports, as
    /// a request from nowhere, returning the raw count and every value read.
    fn read_all(&mut self, out: &mut Out) -> (u32, BTreeMap<String, Vec<u8>>) {
        let names: Vec<&str> = table()
            .iter()
            .filter(|spec| eligible(spec))
            .map(|spec| spec.name)
            .chain(["settings.version"])
            .collect();
        let replies: Vec<(String, Result<Vec<u8>, ()>)> = names
            .iter()
            .map(|name| {
                let request = Packet::rpc_request(name, &[], 0, DeviceRoute::root())
                    .expect("a read fits a packet");
                let (view, _) = PacketView::parse_prefix(request.as_bytes()).expect("it parses");
                let mut lanes = Out::default();
                let _ = self
                    .device
                    .handle(&(), Input::Packet(view), self.now, &mut lanes);
                let (replies, rest): (Vec<_>, Vec<_>) = lanes
                    .0
                    .into_iter()
                    .partition(|(lane, _)| *lane == Lane::Reply);
                out.0.extend(rest);
                let reply = replies
                    .first()
                    .and_then(|(_, packet)| Packet::from_slice_prefix(packet).ok())
                    .and_then(|(packet, _)| match packet.payload() {
                        Payload::RpcReply(reply) => Some(reply.value.to_vec()),
                        _ => None,
                    })
                    .ok_or(());
                (name.to_string(), reply)
            })
            .collect();
        let version = replies
            .last()
            .and_then(|(_, reply)| reply.as_deref().ok())
            .and_then(|reply| u32::decode_reply(reply).ok())
            .unwrap_or_default();
        let values = replies[..replies.len() - 1]
            .iter()
            .filter_map(|(name, reply)| Some((name.clone(), reply.clone().ok()?)))
            .collect();
        (version, values)
    }

    /// Brings the device up in a new session, back at its defaults and then
    /// at what it stored.
    fn boot(&mut self) {
        self.boots += 1;
        let session = SessionId::new(self.boots.wrapping_mul(0x9E37_79B9) ^ self.case.seed as u32);
        self.device.reboot(session, self.now);
        self.device.set_acquisition(AcquisitionState::Idle);
        self.device.settings_mut().entries(&mut |entry| {
            if let Entry::Setting(cell) = entry {
                let _ = cell.reset(&mut twinleaf_device::rpc::Reply::new());
            }
        });
        self.offset = match self.case.firmware.wrap {
            true => u32::MAX - self.rng.below(6) as u32,
            false => 0,
        };
        let mut out = Out::default();
        if let Some(stored) = self.stored.clone() {
            let _ = self
                .device
                .handle(&(), Input::Loaded(None, Ok(&stored)), self.now, &mut out);
        }
        let (version, values) = self.read_all(&mut out);
        self.oracle = Oracle {
            first: version.wrapping_add(self.offset),
            states: vec![values],
        };
        self.off = false;
        self.note(format!(
            "device boots session {:#x} at #{}",
            session.value(),
            self.oracle.first
        ));
        self.distribute(out, None, None);
        self.tick_at = self.now;
        self.schedule(self.now, Wake::Tick);
    }

    fn power_off(&mut self) {
        self.off = true;
        let downtime = match self.case.faults.reorder {
            true => 100 + self.rng.below(400),
            false => self.rng.below(400),
        };
        self.note("device powers off".into());
        self.schedule(self.now + downtime * MS, Wake::Boot);
    }

    fn tick(&mut self) {
        if self.off || self.now != self.tick_at {
            return;
        }
        let mut out = Out::default();
        if let Some(image) = self.device.tick(self.now, &mut out) {
            self.stored = Some(image);
            self.device.saved();
        }
        self.record(&out);
        self.distribute(out, None, None);
        self.tick_at = self.device.deadline();
        self.schedule(self.tick_at, Wake::Tick);
    }

    fn device_receives(&mut self, port: usize, packet: &[u8]) {
        if self.off {
            return;
        }
        let Ok((view, _)) = PacketView::parse_prefix(packet) else {
            return;
        };
        let version = Request::parse(view.payload)
            .filter(|request| request.method == Method::ByName(b"settings.version"))
            .map(|request| request.id);
        let mut out = Out::default();
        if let (Some(id), false) = (version, self.case.firmware.counter) {
            device::answer(
                twinleaf_device::device::Pending::local(id),
                Err::<&[u8], _>(RpcError::NotFound),
                &mut out,
            );
            return self.distribute(out, Some(port), None);
        }
        let actions = self
            .device
            .handle(&(), Input::Packet(view), self.now, &mut out);
        let reboot = self.carry_out(actions, &mut out);
        self.record(&out);
        self.distribute(out, Some(port), version);
        if reboot {
            self.power_off();
        }
    }

    /// Does what the device left to its platform, saying whether it reboots.
    fn carry_out(&mut self, actions: Actions<'_>, out: &mut Out) -> bool {
        let Actions {
            call,
            deferred,
            reboot,
            ..
        } = actions;
        let done = Ok::<&[u8], RpcError>(&[]);
        if let Some((pending, _, _)) = call {
            device::answer(pending, Err::<&[u8], _>(RpcError::NotFound), out);
        }
        match deferred {
            None => {}
            Some((pending, Deferred::Flash(FlashOp::ConfSave(image)))) => {
                self.stored = Some(image);
                device::answer(pending, done, out);
            }
            Some((pending, Deferred::Flash(FlashOp::ConfLoad))) => {
                let stored = self.stored.clone();
                let image = stored.as_deref().ok_or(RpcError::State);
                let _ = self
                    .device
                    .handle(&(), Input::Loaded(Some(pending), image), self.now, out);
            }
            Some((pending, Deferred::Flash(FlashOp::ConfReset))) => {
                self.stored = None;
                device::answer(pending, done, out);
            }
            Some((
                pending,
                Deferred::Flash(FlashOp::Upload(_) | FlashOp::Upgrade | FlashOp::Abort),
            )) => device::answer(pending, Err::<&[u8], _>(RpcError::State), out),
            Some((pending, Deferred::Acquire(request))) => {
                self.device.set_acquisition(match request {
                    SyncRequest::Start | SyncRequest::Restart => AcquisitionState::Running,
                    SyncRequest::Stop => AcquisitionState::Idle,
                });
                device::answer(pending, done, out);
            }
        }
        reboot
    }

    /// The proxy end of a port, which hands a reply to the host that asked
    /// and anything else to every host on the port as the event its pump makes.
    fn proxy_receives(&mut self, port: usize, packet: &[u8]) {
        let Ok((packet, _)) = Packet::from_slice_prefix(packet) else {
            return;
        };
        let event = match packet.payload() {
            Payload::RpcReply(reply) => {
                return self.answered(reply.req_id.value(), Ok(reply.value.to_vec()))
            }
            Payload::RpcError(error) => {
                let refused = CallError::DeviceError(RpcErrorPayload::from_wire(error));
                return self.answered(error.req_id.value(), Err(refused));
            }
            Payload::Heartbeat(beat) => DeviceEvent::Heartbeat {
                session_id: beat.session(),
                settings: beat.settings(),
            },
            Payload::Setting(setting) => match setting.name_str() {
                Some(name) => DeviceEvent::Setting {
                    name: name.to_string(),
                    seq: setting.seq,
                    reply: setting.reply.to_vec(),
                },
                None => return,
            },
            _ => return,
        };
        let listening: Vec<usize> = (0..self.clients.len())
            .filter(|&c| self.clients[c].port == port)
            .collect();
        for &c in &listening {
            self.clients[c].inbox.push_back(event.clone());
        }
        for c in listening {
            self.serve(c);
        }
    }

    fn answered(&mut self, id: u16, reply: Result<Vec<u8>, CallError>) {
        let Some(&c) = self.routes.get(&id) else {
            return;
        };
        let client = &mut self.clients[c];
        let queued = client.inbox.len();
        let Some(calling) = client.calling.as_mut() else {
            return;
        };
        calling.replies.insert(id, (reply, queued));
        if calling
            .ids
            .iter()
            .all(|id| calling.replies.contains_key(id))
        {
            self.finish(c);
        }
    }

    /// Host `c` sends each of `calls` at once, then waits for every reply or
    /// its timeout before doing `then`.
    fn start_call(&mut self, c: usize, calls: &[(&str, Vec<u8>)], then: Then) {
        let ids = calls
            .iter()
            .map(|(name, arg)| {
                self.next_id = self.next_id.wrapping_add(1);
                let id = self.next_id;
                self.routes.insert(id, c);
                let request = Packet::rpc_request(name, arg, id, DeviceRoute::root())
                    .expect("a request fits a packet");
                self.send(
                    self.clients[c].port,
                    Direction::Down,
                    request.as_bytes().to_vec(),
                );
                id
            })
            .collect();
        let client = &mut self.clients[c];
        client.generation += 1;
        client.calling = Some(Calling {
            ids,
            replies: BTreeMap::new(),
            then,
        });
        let timeout = Wake::Timeout(c, client.generation);
        self.schedule(self.now + TIMEOUT, timeout);
    }

    /// Host `c`'s calls are answered or timed out.
    fn finish(&mut self, c: usize) {
        let client = &mut self.clients[c];
        let Some(Calling {
            ids,
            mut replies,
            then,
        }) = client.calling.take()
        else {
            return;
        };
        client.generation += 1;
        let queued = client.inbox.len();
        let mut replies: Vec<_> = ids
            .iter()
            .map(|id| {
                self.routes.remove(id);
                replies
                    .remove(id)
                    .unwrap_or((Err(CallError::Timeout), queued))
            })
            .collect();
        match then {
            Then::Writes(names) => self.wrote(c, &names, replies),
            Then::Registry => self.registry(c, replies),
            Then::Snapshot(snapshotting) => {
                let (reply, _) = replies.pop().expect("a snapshot calls one at a time");
                self.step_snapshot(c, snapshotting, reply);
            }
        }
        self.serve(c);
    }

    /// Host `c` handles what is queued for it while it waits on nothing.
    fn serve(&mut self, c: usize) {
        while self.failure.is_none() && self.clients[c].calling.is_none() {
            if !self.clients[c].inbox.is_empty() {
                self.drain(c, usize::MAX);
            } else if let Some(op) = self.clients[c].ops.pop_front() {
                self.client_op(c, op);
            } else {
                break;
            }
        }
    }

    /// Hands up to `count` queued events to host `c`'s cache, then does once
    /// what they asked, returning how many it took.
    fn drain(&mut self, c: usize, count: usize) -> usize {
        let mut asked = false;
        let mut taken = 0;
        while taken < count && self.clients[c].calling.is_none() {
            let Some(event) = self.clients[c].inbox.pop_front() else {
                break;
            };
            asked |= self.observe(c, event);
            taken += 1;
        }
        if asked {
            self.settle(c);
            self.check(c);
        }
        taken
    }

    /// Hands one event to host `c`'s cache, saying whether it asks for work.
    fn observe(&mut self, c: usize, event: DeviceEvent) -> bool {
        let client = &mut self.clients[c];
        let heartbeat = match &event {
            DeviceEvent::Heartbeat {
                session_id: Some(session),
                settings,
            } => {
                client.legacy |= settings.is_none();
                client.foreign |= client.session.is_some_and(|known| known != *session);
                true
            }
            DeviceEvent::Setting { seq: None, .. } => {
                client.legacy = true;
                false
            }
            DeviceEvent::Heartbeat {
                session_id: None, ..
            }
            | DeviceEvent::Setting { seq: Some(_), .. }
            | DeviceEvent::Metadata(_)
            | DeviceEvent::MetadataUnavailable
            | DeviceEvent::NewHash(_) => false,
        };
        let action = client
            .cache
            .as_mut()
            .map_or(CacheAction::Keep, |cache| cache.observe(&event));
        client.owed = match (client.owed, action) {
            (Owed::Reload, _) | (_, CacheAction::Reload) => Owed::Reload,
            (Owed::Snapshot, _) | (_, CacheAction::Snapshot) => Owed::Snapshot,
            (Owed::Nothing, CacheAction::Keep) => Owed::Nothing,
        };
        let applied = client.cache.as_ref().and_then(SettingsCache::version);
        let line = match &event {
            DeviceEvent::Heartbeat {
                session_id,
                settings,
            } => {
                format!(
                    "heartbeat {:x?} {settings:?}",
                    session_id.map(SessionId::value)
                )
            }
            DeviceEvent::Setting { name, seq, .. } => format!("{name} {seq:?}"),
            DeviceEvent::Metadata(_)
            | DeviceEvent::MetadataUnavailable
            | DeviceEvent::NewHash(_) => "another event".to_string(),
        };
        let client = &mut self.clients[c];
        let asked = action != CacheAction::Keep || (heartbeat && client.owed != Owed::Nothing);
        self.note(format!(
            "host {c} observes {line}: {action:?}, at {applied:?}"
        ));
        self.check(c);
        asked
    }

    fn settle(&mut self, c: usize) {
        match self.clients[c].owed {
            Owed::Nothing => {}
            Owed::Snapshot => self.start_snapshot(c),
            Owed::Reload => {
                let calls = [
                    ("dev.name", vec![]),
                    ("rpc.hash", vec![]),
                    ("dev.session", vec![]),
                ];
                self.start_call(c, &calls, Then::Registry);
            }
        }
    }

    /// Builds a new cache from the registry the replies describe, as a hit
    /// in the registry's disk cache does, and snapshots it. What was queued
    /// before may be another session's, so it is dropped.
    fn registry(&mut self, c: usize, replies: Vec<(Result<Vec<u8>, CallError>, usize)>) {
        let decoded = |reply: &Result<Vec<u8>, CallError>| match reply {
            Ok(reply) => u32::decode_reply(reply).map_err(CallError::InvalidReply),
            Err(error) => Err(error.clone()),
        };
        let read = match replies.as_slice() {
            [(Ok(_), _), (hash, _), (session, _)] => {
                decoded(hash).and_then(|hash| Ok((hash, decoded(session)?)))
            }
            [(Err(error), _), _, _] => Err(error.clone()),
            _ => Err(CallError::ResponseLost),
        };
        let (hash, session) = match read {
            Ok((hash, session)) => (hash, SessionId::new(session)),
            Err(error) => {
                return self.note(format!("host {c} could not read the registry: {error}"))
            }
        };
        let firmware = &self.case.firmware;
        let mut registry = RpcRegistry::new(
            table()
                .iter()
                .filter(|spec| firmware.counter || spec.name != "settings.version")
                .map(|spec| RpcDescriptor::from_meta(spec.metadata(), spec.name.to_string()))
                .collect(),
        );
        registry.hash = Some(hash);
        registry.session = Some(session);
        let client = &mut self.clients[c];
        client.inbox.clear();
        client.cache = Some(SettingsCache::new(&registry));
        client.session = Some(session);
        client.owed = Owed::Snapshot;
        client.settled = None;
        client.legacy = false;
        client.foreign = false;
        client.rebuilt += 1;
        self.stats.reloads += 1;
        self.note(format!(
            "host {c} reloads in session {:#x}",
            session.value()
        ));
        self.start_snapshot(c);
    }

    /// Starts the cache's own snapshot, each of its calls one of host `c`'s.
    fn start_snapshot(&mut self, c: usize) {
        let Some(mut cache) = self.clients[c].cache.take() else {
            return;
        };
        let (reply, replies) = flume::bounded(1);
        let (steps, next) = flume::bounded(1);
        std::thread::spawn(move || {
            let taken = cache.snapshot_with(|name, arg| {
                let _ = steps.send(Step::Call(name.to_string(), arg.to_vec()));
                let (resolve, reply) = flume::bounded(1);
                let _ = resolve.send(replies.recv().unwrap_or(Err(CallError::ResponseLost)));
                PendingReply { reply }
            });
            let _ = steps.send(Step::Done(cache, taken));
        });
        let snapshotting = Snapshotting {
            reply,
            next,
            reading: String::new(),
            settled: None,
        };
        self.await_snapshot(c, snapshotting);
    }

    fn step_snapshot(
        &mut self,
        c: usize,
        mut snapshotting: Snapshotting,
        reply: Result<Vec<u8>, CallError>,
    ) {
        if snapshotting.reading == "settings.version" {
            snapshotting.settled = reply
                .as_deref()
                .ok()
                .and_then(|v| u32::decode_reply(v).ok());
        }
        let _ = snapshotting.reply.send(reply);
        self.await_snapshot(c, snapshotting);
    }

    /// Lets the snapshot thread run to its next call or its end.
    fn await_snapshot(&mut self, c: usize, mut snapshotting: Snapshotting) {
        match snapshotting.next.recv() {
            Ok(Step::Call(name, arg)) => {
                snapshotting.reading.clone_from(&name);
                self.start_call(c, &[(&name, arg)], Then::Snapshot(snapshotting));
            }
            Ok(Step::Done(cache, taken)) => {
                let client = &mut self.clients[c];
                client.cache = Some(cache);
                client.rebuilt += 1;
                match &taken {
                    Ok(()) => {
                        client.owed = Owed::Nothing;
                        client.settled = snapshotting.settled;
                        self.stats.snapshots += 1;
                    }
                    Err(_) => self.stats.failed_snapshots += 1,
                }
                self.note(format!(
                    "host {c} snapshot {taken:?}, read up to {:?}",
                    snapshotting.settled
                ));
                self.check(c);
            }
            Err(_) => self.fail("a snapshot thread ended without a result".into()),
        }
    }

    /// What every cache promises at every moment. It holds no privileged or
    /// legacy value, nothing after another session's heartbeat, and at the
    /// version it claims in its own session the values the device held.
    fn check(&mut self, c: usize) {
        let client = &self.clients[c];
        let Some(cache) = &client.cache else {
            return;
        };
        let firmware = &self.case.firmware;
        let held = |spec: &&RpcSpec| cache.get(spec.name).is_some();
        if let Some(spec) = table()
            .iter()
            .filter(|spec| flags(spec).contains(RpcMetaFlags::PRIVILEGED))
            .find(held)
        {
            return self.fail(format!("(d) host {c} caches privileged {}", spec.name));
        }
        if !firmware.counter || client.legacy {
            if let Some(spec) = table().iter().find(held) {
                return self.fail(format!(
                    "(e) host {c} caches {} of legacy firmware",
                    spec.name
                ));
            }
        }
        let Some(version) = cache.version() else {
            return;
        };
        if client.foreign {
            return self.fail(format!(
                "(c) host {c} serves #{version} after hearing another session"
            ));
        }
        if client.session != Some(self.device.session) {
            self.stats.stale_session_checks += 1;
            return;
        }
        let Some(state) = self.oracle.at(version) else {
            return self.fail(format!(
                "(b) host {c} claims #{version}, which the session never reached"
            ));
        };
        let unsettled = client
            .settled
            .filter(|&settled| ahead(settled, version))
            .map_or(0, |settled| settled.wrapping_sub(version));
        let wrong = table().iter().filter(|spec| eligible(spec)).find(|spec| {
            let held = cache.get(spec.name);
            !(0..=unsettled).any(|k| {
                self.oracle
                    .at(version.wrapping_add(k))
                    .is_some_and(|state| state.get(spec.name).map(Vec::as_slice) == held)
            })
        });
        if let Some(spec) = wrong {
            let (name, held) = (spec.name, cache.get(spec.name).map(<[u8]>::to_vec));
            let device = state.get(name).cloned();
            return self.fail(format!(
                "(b) host {c} holds {name} = {held:?} at #{version}, where the device held {device:?}"
            ));
        }
        self.stats.exact_checks += 1;
    }

    /// The end of a quiet period, by which the device has announced all it
    /// holds and every cache holds exactly that.
    fn quiet_check(&mut self) {
        self.stats.quiet_checks += 1;
        if self.off {
            return self.fail("(a) the device is off at the end of a quiet period".into());
        }
        let mut out = Out::default();
        let (raw, values) = self.read_all(&mut out);
        if out.0.iter().any(|(lane, _)| *lane == Lane::Announce) {
            return self.fail("(a) the device still had a change to announce".into());
        }
        let (version, state) = self.oracle.latest();
        if raw.wrapping_add(self.offset) != version || *state != values {
            return self.fail(format!(
                "(device) the device holds #{} {values:?}, but announced #{version} {state:?}",
                raw.wrapping_add(self.offset)
            ));
        }
        let state = state.clone();
        for c in 0..self.clients.len() {
            let client = &self.clients[c];
            let Some(cache) = client.cache.as_ref().filter(|_| client.calling.is_none()) else {
                return self.fail(format!("(a) host {c} has no cache after a quiet period"));
            };
            let firmware = &self.case.firmware;
            let exact = !firmware.counter || client.legacy;
            let wrong = table().iter().filter(|spec| eligible(spec)).find(|spec| {
                let expected = state.get(spec.name).map(Vec::as_slice).filter(|_| !exact);
                cache.get(spec.name) != expected
            });
            match (wrong, cache.version()) {
                (Some(spec), _) => {
                    let (name, held) = (spec.name, cache.get(spec.name).map(<[u8]>::to_vec));
                    return self.fail(format!(
                        "(a) host {c} holds {name} = {held:?} after a quiet period, the device {:?}",
                        state.get(name)
                    ));
                }
                (None, Some(held)) if held != version => {
                    return self.fail(format!(
                        "(a) host {c} is at #{held} after a quiet period, the device at #{version}"
                    ));
                }
                (None, Some(_) | None) => {}
            }
        }
    }

    fn op(&mut self, op: Op) {
        self.note(format!("op {op:?}"));
        let clients = self.clients.len();
        match op {
            Op::Call { client, .. } | Op::Slider { client, .. } => {
                let c = usize::from(client) % clients;
                self.clients[c].ops.push_back(op);
                self.serve(c);
            }
            Op::Race { target, picks } => {
                let (first, second) = (0, 1 % clients);
                self.clients[first].ops.push_back(Op::Call {
                    client: 0,
                    target,
                    pick: picks.0,
                });
                self.clients[second].ops.push_back(Op::Call {
                    client: 1,
                    target,
                    pick: picks.1,
                });
                self.serve(first);
                self.serve(second);
            }
            Op::Measure { level, temp } => self.device.settings_mut().measured = (level, temp),
            Op::PowerCycle => {
                if !self.off {
                    self.power_off();
                }
            }
            Op::Quiet => {
                self.quiet_until = self.now + QUIET;
                for client in &mut self.clients {
                    client.ops.clear();
                }
                self.schedule(self.quiet_until, Wake::Check);
            }
        }
    }

    fn client_op(&mut self, c: usize, op: Op) {
        let (target, picks) = match op {
            Op::Call { target, pick, .. } => (target, vec![pick]),
            Op::Slider { target, picks, .. } => (target, picks),
            Op::Race { .. } | Op::Measure { .. } | Op::PowerCycle | Op::Quiet => return,
        };
        let calls: Vec<(&str, Vec<u8>)> = picks.iter().map(|&pick| target.call(pick)).collect();
        let names = calls.iter().map(|(name, _)| *name).collect();
        self.start_call(c, &calls, Then::Writes(names));
    }

    /// Host `c` sees each of its writes in its cache once the events queued
    /// ahead of its reply are in, unless it snapshots meanwhile.
    fn wrote(
        &mut self,
        c: usize,
        names: &[&str],
        replies: Vec<(Result<Vec<u8>, CallError>, usize)>,
    ) {
        let mut seen = 0;
        for (name, (reply, queued)) in names.iter().zip(replies) {
            let rebuilt = self.clients[c].rebuilt;
            seen += self.drain(c, queued.saturating_sub(seen));
            let unbroken = self.clients[c].rebuilt == rebuilt && self.clients[c].calling.is_none();
            if let (Ok(reply), true) = (reply, unbroken) {
                self.own_write(c, name, &reply);
            }
        }
    }

    fn own_write(&mut self, c: usize, name: &str, reply: &[u8]) {
        let client = &self.clients[c];
        let Some(cache) = &client.cache else {
            return;
        };
        let current = client.session == Some(self.device.session) && !self.off;
        let settled = cache.version().is_some_and(|version| {
            client
                .settled
                .is_none_or(|settled| !ahead(settled, version))
        });
        let cached = table()
            .iter()
            .any(|spec| spec.name == name && eligible(spec));
        if !(current && settled && cached && !client.legacy && self.case.firmware.counter) {
            return;
        }
        let faults = &self.case.faults;
        match (
            cache.get(name) == Some(reply),
            faults.drop == 0 && !faults.reorder,
        ) {
            (true, _) => self.stats.own_writes_held += 1,
            (false, false) => self.stats.own_writes_missed += 1,
            (false, true) => {
                let held = cache.get(name).map(<[u8]>::to_vec);
                self.fail(format!(
                    "(f) host {c} wrote {name} = {reply:?} but its cache holds {held:?}"
                ));
            }
        }
    }
}

/// Runs one case to its last quiet period, returning what it counted or the
/// first invariant it broke.
fn run(case: &Case) -> Result<Stats, String> {
    let mut world = World::new(case);
    let mut at = 0;
    for (gap_ms, op) in case.ops.iter().chain([&(0, Op::Quiet)]) {
        at += gap_ms * MS;
        world.schedule(at, Wake::Op(op.clone()));
        if let Op::Quiet = op {
            at += QUIET + 1;
        }
    }
    world.run_until(at);
    world.stats.sim_ms = world.now / MS;
    match world.failure {
        Some(failure) => Err(failure),
        None => Ok(world.stats),
    }
}

fn target() -> impl Strategy<Value = Target> {
    prop_oneof![
        1 => Just(Target::Loglevel),
        1 => Just(Target::Autostart),
        1 => Just(Target::SyncDelay),
        3 => Just(Target::Gain),
        2 => Just(Target::Lo),
        2 => Just(Target::Hi),
        2 => Just(Target::Offset),
        1 => Just(Target::Mode),
        1 => Just(Target::Secret),
        1 => Just(Target::Level),
        1 => Just(Target::Zero),
        1 => Just(Target::Start),
        1 => Just(Target::Stop),
        1 => Just(Target::Unlock),
        1 => Just(Target::Lock),
        1 => Just(Target::Save),
        1 => Just(Target::Reboot),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (any::<u8>(), target(), any::<u8>())
            .prop_map(|(client, target, pick)| Op::Call { client, target, pick }),
        2 => (any::<u8>(), target(), prop::collection::vec(any::<u8>(), 2..8))
            .prop_map(|(client, target, picks)| Op::Slider { client, target, picks }),
        2 => (target(), any::<(u8, u8)>()).prop_map(|(target, picks)| Op::Race { target, picks }),
        4 => (0u8..4, -2i16..2).prop_map(|(level, temp)| Op::Measure { level, temp }),
        1 => Just(Op::PowerCycle),
        1 => Just(Op::Quiet),
    ]
}

fn gap() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0u64), 0u64..50, 0u64..400]
}

fn faults() -> impl Strategy<Value = Faults> {
    (
        prop_oneof![Just(0u32), Just(10), Just(50), Just(200)],
        prop_oneof![Just(0u32), Just(20), Just(100)],
        any::<bool>(),
        0u64..10,
        0u64..20,
        any::<bool>(),
    )
        .prop_map(
            |(drop, duplicate, reorder, delay_ms, jitter_ms, free_events)| Faults {
                drop,
                duplicate,
                reorder,
                delay_ms,
                jitter_ms,
                free_events,
            },
        )
}

fn firmware() -> impl Strategy<Value = Firmware> {
    prop_oneof![
        8 => any::<bool>().prop_map(|wrap| Firmware {
            counter: true,
            beat_settings: true,
            numbered: true,
            wrap,
        }),
        1 => Just(Firmware {
            counter: false,
            beat_settings: false,
            numbered: false,
            wrap: false,
        }),
        1 => any::<(bool, bool, bool)>().prop_map(|(counter, beat_settings, numbered)| Firmware {
            counter,
            beat_settings,
            numbered,
            wrap: false,
        }),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    (
        any::<u64>(),
        1usize..=2,
        prop::collection::vec(0usize..2, 1..=3),
        faults(),
        firmware(),
        prop::collection::vec((gap(), op()), 1..40),
    )
        .prop_map(|(seed, ports, clients, faults, firmware, ops)| Case {
            seed,
            ports,
            clients,
            faults,
            firmware,
            ops,
        })
}

fn config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// Runs `cases` cases, printing what they counted, and panics with the
/// minimal failing case and the invariant it broke.
fn exercise(cases: u32) {
    let totals = RefCell::new(Stats::default());
    let started = Instant::now();
    let outcome = TestRunner::new(config(cases)).run(&case(), |case| {
        let stats = run(&case).map_err(TestCaseError::fail)?;
        totals.borrow_mut().add(&stats);
        Ok(())
    });
    eprintln!("{:?} in {:?}", totals.borrow(), started.elapsed());
    if let Err(failure) = outcome {
        panic!("{failure}");
    }
}

/// Every cache stays exact through faults, races, reboots and wraps.
#[test]
fn every_cache_stays_exact() {
    exercise(2_000);
}

/// The same at many more cases, as many as `CACHE_SIM_CASES` sets.
#[test]
#[ignore = "long-running; run with --ignored"]
fn every_cache_stays_exact_at_length() {
    let cases = std::env::var("CACHE_SIM_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok());
    exercise(cases.unwrap_or(100_000));
}

//! A deterministic simulation of a `twinleaf-device` board behind faulty
//! links and the hosts caching its settings, which checks every
//! [`SettingsCache`] against a written spec.
//!
//! Time is simulated. Each host waits on its calls as a state machine and
//! handles its events one at a time, as the engine's worker does. It takes
//! a snapshot only when its cache asks for one. It loads the registry at its
//! first event, and again when the cache asks, a heartbeat of another
//! session comes or its link reconnects to a device that rebooted, keeping
//! the events queued meanwhile for the cache the load builds. After a load
//! that fails it drops its events for `RELOAD_DELAY` and loads again at the
//! first event after. The cache's own [`SettingsCache::snapshot_with`] is a
//! future the world polls as each read is answered or times out. The world
//! sends each read the moment the snapshot submits it, so the snapshot's
//! window of reads in flight slides as in the engine, and once the snapshot
//! ends the reads still in flight are let go.
//!
//! The spec comes from the docs of [`SettingsCache`], [`CacheAction`] and
//! their methods, the docs of the engine's worker, and the device's docs of
//! its channels, and not from the cache's code. Each law names its kind, its
//! source and a wrong implementation it rules out, and each failure names
//! the law it broke. A failure marked as the model's is a fault of the
//! simulation or the device, not of the cache, and one marked as the
//! harness's is a quiet period that never ended.
//!
//! # Observations
//!
//! What each host's cache says through [`SettingsCache::version`],
//! [`SettingsCache::values`] and [`SettingsCache::get`] after every event
//! and snapshot, the [`CacheAction`] each [`SettingsCache::observe`]
//! returns, and the outcome of each snapshot with the calls it submitted, in
//! order. On the device side, every announcement it writes, and at the end
//! of each quiet period every cacheable value and the counter, read past
//! the links.
//!
//! # The model
//!
//! For each session of the device, the cacheable values it held after each
//! announcement, by position. The device raises its counter by one on every
//! announcement, as the doc of `SETTINGS_VERSION` says, so position `i` of a
//! session is the count `i` past the count it booted at. The model compares
//! positions and never compares counts across a wrap. The cacheable entries
//! are the standard table's settings and reports, which the device's
//! `Channel` docs say a host caches, and the board's settings and reports
//! but the privileged one.
//!
//! Every packet a host receives carries, beside it in the simulation, the
//! session the device was in when it sent the packet and its position then.
//! For each host the model keeps what its cache claims. That is a position
//! of the session it read its registry in, or nothing, or unknown once the
//! cache was handed what another session announced, which no cache can tell
//! apart. A snapshot sets the claim from the counts it read. An announcement
//! of the cache's session one past the claim moves it there, one at or
//! behind it leaves it, and one further on drops it to nothing. A heartbeat
//! ahead of the claim drops it too. A heartbeat of another session, or an
//! event that shows legacy firmware, drops it until the host reads the
//! registry again.
//!
//! # Laws
//!
//! 1. Only cacheable entries, invariant. A cache never holds an entry outside
//!    the cacheable set. Source, the doc of [`SettingsCache`], "every
//!    readable, announced entry that is neither a capture nor privileged".
//!    Rules out holding every announced name, such as the privileged
//!    `test.secret`.
//! 2. Snapshot reads, postcondition. A snapshot that succeeds reads the
//!    counter, every cacheable entry once and the counter again, and reads
//!    them all again while the two counts differ, giving up only after more
//!    than one try. It claims the first count of its last pair. A cache that
//!    caches nothing reads nothing. Source, the doc of
//!    [`SettingsCache::snapshot_with`], "until both reads agree. If they
//!    never do, the last read is kept at the first version", and the doc of
//!    `SNAPSHOT_ATTEMPTS`. Rules out a snapshot that skips the second read,
//!    claims the second count, reads a privileged entry, or reads again once
//!    the counts agree.
//! 3. The claim, model based. After every event and snapshot, a cache whose
//!    claim the model knows claims exactly the count at the model's position,
//!    or claims no count and holds nothing. Each [`SettingsCache::observe`]
//!    returns [`CacheAction::Keep`] while the claim stays or moves on,
//!    [`CacheAction::Snapshot`] when an announcement past a gap or a
//!    heartbeat ahead drops it, [`CacheAction::Snapshot`] again for every
//!    numbered announcement and every heartbeat of its session with the count
//!    while it claims nothing, and [`CacheAction::Reload`] at a heartbeat of
//!    another session. Source, the doc of [`SettingsCache`], "kept by the
//!    numbered announcements after it", "An announcement numbered at or
//!    before the last one applied is already held and changes nothing",
//!    "One numbered past the next, or a heartbeat ahead of the last one
//!    applied, drops every value until the next snapshot" and "While it
//!    waits for a snapshot, every numbered announcement and every heartbeat
//!    of its session that carries the count asks for one", the docs of
//!    [`CacheAction`], and the doc of the engine worker's `snapshot`, where a
//!    cache whose snapshot failed "asks for another at the next heartbeat".
//!    Rules out an announcement applied past a gap, a heartbeat ahead
//!    ignored, a duplicate that drops the values, a drop that says keep, a
//!    cache that never asks again after a failed snapshot, and counts
//!    compared without the wrap.
//! 4. Values at the claim, model based. A cache that claims position `p` of
//!    its session holds, for every cacheable entry, what the device held at
//!    `p`. After a snapshot whose counts never agreed, it may hold what the
//!    device held at any position from `p` to the last count the snapshot
//!    read. Source, the doc of [`SettingsCache`], "exactly as the device
//!    announced them", and the doc of [`SettingsCache::snapshot_with`], "the
//!    last read is kept at the first version, and the announcements between
//!    the two, still to be observed, bring it up to date". Rules out an
//!    announcement's value stored under another entry, or a snapshot that
//!    keeps values read before the count moved.
//! 5. Another session, postcondition. Once a cache that caches anything hears
//!    a heartbeat of another session, it returns [`CacheAction::Reload`] and
//!    claims and holds nothing until its host reads the registry again.
//!    Source, the doc of [`SettingsCache`], "a new session drops the cache",
//!    and the doc of [`CacheAction::Reload`], "Read the registry again and
//!    build a new cache". Rules out values kept across a reboot. Law 3
//!    checks it.
//! 6. Legacy firmware, invariant. A cache of a device without
//!    `settings.version` holds nothing. Once its host is handed an
//!    announcement without a sequence number or a heartbeat without the
//!    count, the cache holds nothing until the registry is read again.
//!    Source, the doc of [`SettingsCache`], "is never cached" and "at most
//!    one heartbeat old", which a heartbeat without the count cannot show.
//!    Rules out caching what an unnumbered announcement says.
//! 7. Quiet, completeness. A quiet period is one in which nothing moves and
//!    no link loses, duplicates or holds back a packet, and every packet
//!    comes after those sent before it. It ends once the device has beaten
//!    since its last request, no packet is in flight, and every host waits on
//!    nothing, has nothing queued, owes no load and holds a cache at the
//!    device's count or one that caches nothing. Then every cache claims the
//!    device's count and holds exactly its cacheable values, or, under law
//!    6, holds nothing. A quiet period still open after `QUIET_LIMIT` is
//!    checked all the same, so a cache stuck short of the device fails this
//!    law. Source, the doc of [`SettingsCache`], "at most one heartbeat old",
//!    and the doc of [`CacheAction::Keep`], "as exact as the events so far
//!    show". Rules out a cache that never snapshots or always holds nothing,
//!    which passes laws 1 to 6.
//! 8. Own write, a completeness check on the model. A host's write is
//!    answered, and the events queued ahead of the reply are in. Let `w` be
//!    the device's position when it sent the reply. If the cache claims a
//!    position at or past `w`, and the device held the written value at
//!    every position from `w` to the end of the cache's window of law 4, the
//!    cache holds the written value. Given laws 3 and 4 a cache cannot break
//!    it alone. It fails when the model loses track of what the cache was
//!    handed, or when the device's reply and its announcement disagree. A
//!    claim short of `w` is allowed, as a value is "at most one heartbeat
//!    old" in the doc of [`SettingsCache`], but never on links that lose
//!    nothing and keep order, which deliver the announcements ahead of the
//!    reply sent after them. The floors of [`Class::OwnHeld`],
//!    [`Class::OwnHeldFaulty`] and [`Class::OwnOrdered`] make sure the cases
//!    reach it. Rules out a model whose claims drift from the device.
//! 9. A failed snapshot, postcondition. Every snapshot starts from a cache
//!    that claims and holds nothing, which the world checks, as a new cache
//!    holds nothing and one that asks for a snapshot has dropped its values
//!    under law 3. A snapshot that ends at an error, a timeout among them,
//!    leaves the cache so, and law 3 keeps it so until a snapshot succeeds.
//!    What a timed out write did reaches the cache through its announcement
//!    or the next snapshot, under laws 3, 4 and 7. Source, the doc of
//!    [`SettingsCache::version`], "`None` while the cache waits for a
//!    snapshot", the doc of [`SettingsCache`], "read in one snapshot", and
//!    the doc of the engine worker's `snapshot`, "A failed snapshot is
//!    logged, and the cache asks for another at the next heartbeat". Rules
//!    out a snapshot that keeps what it read before the error, or claims the
//!    count it read first.
//!
//! # Generator obligations
//!
//! Every case is valid by construction. Hosts and ports are taken modulo
//! their counts and no op has a precondition, so a shrunk case stays valid.
//! Each case draws how its links lose, duplicate, delay, reorder and hold
//! back packets past the timeout, whether heartbeats fall freely among the
//! other packets, and its firmware, legacy or wrapping or neither. Each op
//! carries its own seed, which every random choice draws from until the
//! next op. Shrinking away one op keeps the seeds of the others, though the
//! ops after it move earlier, as each op's time counts from the one before.
//! On links that reorder, the device stays off longer than any packet
//! takes, so no packet of one session arrives after the next began.
//! Wrapping firmware boots each session at most eight announcements short
//! of the wrap, after it loads its stored settings, so caches apply
//! announcements across it.
//!
//! The classes counted are those of [`Class`]. With
//! `TWINLEAF_SETTINGS_CLASSES` set, [`exercise`] prints each class's total
//! and the share of cases that reached it. The default run draws from a
//! fixed seed, so its floors are met or missed the same way every time. A
//! run of at least [`FLOOR_CASES`] cases fails when a class falls short of
//! its floor, which covers the reloads, the wraps, the replies that come
//! after their call timed out, the snapshots that fail at one, and the own
//! writes of law 8. It fails too when the checks that skip laws 3 and 4 for
//! an unknown claim number more than one in ten of the checks of law 4.
//!
//! # Exclusions
//!
//! - A packet of one session that arrives after the next session began, on a
//!   link that reorders. Announcements carry no session, so no cache can
//!   tell them apart, and a TCP link closes at a reboot.
//! - A link that does not reconnect when the device reboots, as a serial
//!   link may not. Every link here reconnects, so its hosts load the
//!   registry again behind the old session's packets. Without that, a host
//!   whose load spans the reboot keeps the old session's announcements
//!   queued for a cache read in the new one, which applies those numbered
//!   next after its snapshot. It then claims a count ahead of the device
//!   and holds values the new session never had, and every heartbeat after
//!   is behind it, so it stays so until the device counts past it.
//! - The world hands a heartbeat of another session to the cache, which the
//!   engine's worker does not, so that law 5 can check what the cache says.
//! - The time a host takes to catch up, which the engine does not bound. A
//!   host takes at most one snapshot for each event it was handed past the
//!   count its snapshot read, which after a reboot may be every event the old
//!   session left queued, and while snapshots fail every heartbeat asks for
//!   another. A quiet period waits for the hosts however long that takes, up
//!   to `QUIET_LIMIT`.
//! - A reply that comes after its call timed out. The world routes replies
//!   by request id and lets a late one go before any cache sees it, as the
//!   RPC layer does, so the late fault exercises the timeouts and the
//!   announcements of writes done anyway, not the routing.
//! - Captures, as the board declares none.
//! - How many times a snapshot tries before it gives up, which the unit tests
//!   of `settings.rs` pin.
//! - A cache that caches nothing never asks for a reload, so a firmware
//!   update that adds `settings.version` waits for the host to reconnect.
//!   Each case runs one firmware.
//! - Firmware that numbers its heartbeats but not its announcements is
//!   cached until the first unnumbered announcement shows it, as no cache
//!   can tell sooner.
//!
//! # Examples that stay
//!
//! The unit tests of `settings.rs`, which pin the reads of one snapshot, a
//! refused read, a counter that never settles, the wrap of a heartbeat and
//! a device that is never read.

use crate::device::rpc::{
    poll_once, CallError, PendingReply, RpcDescriptor, RpcErrorPayload, RpcRegistry,
};
use crate::device::SettingChange;
use crate::proto::heartbeat::Heartbeat;
use crate::proto::packet::{PacketType, PacketView};
use crate::proto::rpc::{self as wire, Method, Request, RpcError};
use crate::proto::settings::Setting as Announcement;
use crate::proto::{DeviceRoute, RpcRequestId, SessionId};
use crate::proxy::engine::settings::{CacheAction, DeviceEvent, SettingsCache};
use crate::tio::packet::Payload;
use crate::tio::rpc::RpcReply;
use crate::tio::Packet;
use proptest::prelude::*;
use proptest::test_runner::{RngSeed, TestRunner};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::OnceLock;
use std::task::Poll;
use twinleaf_device::device::{
    self, Action, Actions, Announcer, Deferred, Entries, Entry, FlashOp, Group, Identity, Input,
    Lanes, Probe, Refusal, SyncRequest,
};
use twinleaf_device::rpc::{finite, Channel, RpcSpec, Setting, Std, STANDARD};
use twinleaf_device::sync::AcquisitionState;
use twinleaf_device::{Device, Sink};

const MS: u64 = 1_000_000;

/// How long a host waits for a reply.
const TIMEOUT: u64 = 250 * MS;

/// How long a host waits after a failed load of the registry before it loads
/// again, as the engine's `RELOAD_DELAY` says.
const RELOAD_DELAY: u64 = 5_000 * MS;

/// The longest a quiet period may last before the harness gives up on the
/// hosts catching up. A quiet period ends once they have, and this guards
/// the harness only.
const QUIET_LIMIT: u64 = 300_000 * MS;

const PASSWORD: &str = "sim";

/// The counter a snapshot reads before and after the values.
const COUNTER: &str = "settings.version";

/// The variable that has [`exercise`] print its class counts.
const CLASSES: &str = "TWINLEAF_SETTINGS_CLASSES";

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

/// The entries a host caches. They are the standard table's settings and
/// reports, which [`Channel`] says a host caches, and the board's settings
/// and reports that a session without privilege may read.
fn cacheable() -> &'static BTreeSet<&'static str> {
    static CACHEABLE: OnceLock<BTreeSet<&'static str>> = OnceLock::new();
    CACHEABLE.get_or_init(|| {
        let mut names: BTreeSet<&'static str> = STANDARD
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                Std::at(*index)
                    .is_some_and(|std| matches!(std.channel(), Channel::Setting | Channel::Report))
            })
            .map(|(_, spec)| spec.name)
            .collect();
        Board::new((0, 0)).entries(&mut |entry| {
            let setting = matches!(entry, Entry::Setting(_));
            let spec = entry.spec();
            if setting && !spec.access.is_privileged() {
                names.insert(spec.name);
            }
        });
        names
    })
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

/// An op, how long after the one before it comes, and the seed of every
/// random choice until the next.
#[derive(Debug, Clone)]
struct Step {
    gap_ms: u64,
    seed: u64,
    op: Op,
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
    /// Packets held back past the timeout, per mille.
    late: u32,
    /// Whether heartbeats interleave freely with replies and announcements,
    /// as [`Lanes`] allows.
    free_events: bool,
}

impl Faults {
    /// The longest a packet takes along a link.
    fn longest(&self) -> u64 {
        let late = match self.late {
            0 => 0,
            _ => TIMEOUT + TIMEOUT / 2,
        };
        (self.delay_ms + self.jitter_ms) * MS + late
    }

    /// Whether the links lose nothing and keep order.
    fn ordered(&self) -> bool {
        self.drop == 0 && !self.reorder
    }
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
    /// The seed of the device's sessions and of the choices before the
    /// first op.
    seed: u64,
    ports: usize,
    clients: Vec<usize>,
    faults: Faults,
    firmware: Firmware,
    steps: Vec<Step>,
}

/// What a run counts, each class with the least total 1000 cases reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    /// A packet a link lost.
    Dropped,
    /// A packet a link held back past the timeout.
    Late,
    /// A reply that came after its call timed out.
    LateReply,
    /// A reply to a snapshot's read that came after the read timed out.
    LateRead,
    /// A snapshot that succeeded.
    Snapshot,
    /// A snapshot that read the counter more than twice.
    Retried,
    /// A snapshot that gave up while its counts still differed.
    GaveUp,
    /// A snapshot that ended at an error.
    Failed,
    /// A snapshot that ended at a timeout.
    TimedOut,
    /// A host that read the registry again and built a new cache.
    Reload,
    /// A cache that applied an announcement across the wrap of the count.
    Wrap,
    /// A cache dropped by an announcement past a gap.
    Gap,
    /// A cache dropped by a heartbeat ahead of it.
    Behind,
    /// A cache dropped by a heartbeat of another session.
    Foreign,
    /// A cache handed what another session announced.
    Unknown,
    /// A check that skips laws 3 and 4, as the cache's claim is unknown.
    Skipped,
    /// A check of law 4.
    Exact,
    /// The end of a quiet period.
    Quiet,
    /// An own write law 8 holds strictly.
    OwnHeld,
    /// The same, on links that lose or reorder packets.
    OwnHeldFaulty,
    /// An own write the device changed again before the cache's window.
    OwnOverwritten,
    /// An own write ahead of the cache's claim, on links that lose or reorder.
    OwnShort,
    /// An own write on links that lose nothing and keep order, where law 8
    /// allows no shortfall.
    OwnOrdered,
    /// An own write answered while the cache claims nothing.
    OwnNothing,
}

impl Class {
    const ALL: [Class; 24] = [
        Class::Dropped,
        Class::Late,
        Class::LateReply,
        Class::LateRead,
        Class::Snapshot,
        Class::Retried,
        Class::GaveUp,
        Class::Failed,
        Class::TimedOut,
        Class::Reload,
        Class::Wrap,
        Class::Gap,
        Class::Behind,
        Class::Foreign,
        Class::Unknown,
        Class::Skipped,
        Class::Exact,
        Class::Quiet,
        Class::OwnHeld,
        Class::OwnHeldFaulty,
        Class::OwnOverwritten,
        Class::OwnShort,
        Class::OwnOrdered,
        Class::OwnNothing,
    ];

    /// The least total 1000 cases reach. Each is about three tenths of the
    /// least that 2000 cases reached under ten random seeds. The classes at
    /// zero are counted to be seen, and no law needs them.
    fn floor(self) -> u64 {
        match self {
            Class::Dropped => 4_500,
            Class::Late => 800,
            Class::LateReply => 770,
            Class::LateRead => 370,
            Class::Snapshot => 3_300,
            Class::Retried => 110,
            Class::GaveUp => 2,
            Class::Failed => 2_000,
            Class::TimedOut => 2_000,
            Class::Reload => 3_000,
            Class::Wrap => 140,
            Class::Gap => 270,
            Class::Behind => 140,
            Class::Foreign => 90,
            Class::Unknown => 0,
            Class::Skipped => 0,
            Class::Exact => 19_000,
            Class::Quiet => 1_200,
            Class::OwnHeld => 1_000,
            Class::OwnHeldFaulty => 700,
            Class::OwnOverwritten => 0,
            Class::OwnShort => 200,
            Class::OwnOrdered => 330,
            Class::OwnNothing => 0,
        }
    }
}

/// How often a run reached each [`Class`].
#[derive(Debug, Default, Clone, Copy)]
struct Stats([u64; Class::ALL.len()]);

impl Stats {
    fn bump(&mut self, class: Class) {
        self.0[class as usize] += 1;
    }

    fn get(&self, class: Class) -> u64 {
        self.0[class as usize]
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

/// Every value a session's cacheable entries held, by position.
struct Oracle {
    /// The count the session booted at, as it goes on the wire.
    first: u32,
    /// The first position whose values are known, where the first read
    /// after the boot found them. The reports that read refreshed were
    /// announced before it.
    known: usize,
    /// The values at each position from `known` on.
    states: Vec<BTreeMap<String, Vec<u8>>>,
}

impl Oracle {
    /// The position of the wire count `count`, if the session reached it.
    fn position(&self, count: u32) -> Option<usize> {
        let position = count.wrapping_sub(self.first) as usize;
        (position <= self.latest()).then_some(position)
    }

    /// The wire count at `position`.
    fn count(&self, position: usize) -> u32 {
        self.first.wrapping_add(position as u32)
    }

    fn latest(&self) -> usize {
        self.known + self.states.len() - 1
    }

    /// The values at `position`, which no law asks for before `known`, as
    /// every claim and every reply comes after the first read.
    fn state(&self, position: usize) -> &BTreeMap<String, Vec<u8>> {
        &self.states[position - self.known]
    }

    /// The values at each position from `from` to `to`.
    fn window(&self, from: usize, to: usize) -> &[BTreeMap<String, Vec<u8>>] {
        &self.states[from - self.known..=to - self.known]
    }
}

/// The session the device was in when it sent a packet, and its position
/// once it had announced everything the input that wrote the packet moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stamp {
    session: SessionId,
    position: usize,
}

/// What law 3 says a cache claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Claim {
    /// The count at this position of the session its registry was read in.
    At(usize),
    /// No count and no value.
    Nothing,
    /// Whatever the cache could not tell from another session's announcement.
    Unknown,
}

/// One host connection and its cache.
struct Client {
    port: usize,
    cache: Option<SettingsCache>,
    /// The session the registry behind the cache was read in.
    session: Option<SessionId>,
    /// Whether the host owes a load of the registry, as the engine's worker
    /// does at its first event, at a new session and after a failed load.
    reload: bool,
    /// When the wait after a failed load ends. Until then the host drops its
    /// events, and it loads again at the first event after.
    reload_at: Option<u64>,
    inbox: VecDeque<(DeviceEvent, Stamp)>,
    /// What it waits on. A host does nothing else meanwhile.
    calling: Option<Calling>,
    /// Counts calls, so a timeout meant for an earlier one is ignored.
    generation: u64,
    ops: VecDeque<Op>,
    /// What the cache claims.
    claim: Claim,
    /// The last count the last snapshot read, the end of the window its
    /// values were read in.
    settled: Option<u32>,
    /// Whether the cache was handed an event of legacy firmware since it
    /// was built.
    legacy: bool,
    /// Whether the cache was handed a heartbeat of another session since it
    /// was built.
    foreign: bool,
    rebuilt: u64,
}

/// What a host waits on.
enum Calling {
    /// Calls sent at once, the answers so far, and what the host does once
    /// every one is in or their timeout comes.
    Calls {
        ids: Vec<u16>,
        answers: BTreeMap<u16, Answer>,
        then: Then,
    },
    /// A snapshot, whose reads each wait on their own timeout.
    Snapshot(Snapshotting),
}

/// A call's reply, how many events were queued ahead of it, and the stamp
/// of the packet that brought it.
struct Answer {
    reply: Result<Vec<u8>, CallError>,
    queued: usize,
    stamp: Option<Stamp>,
}

enum Then {
    Writes(Vec<&'static str>),
    Registry,
}

/// One read a snapshot made, the count it read if it read the counter, and
/// the session that answered.
struct Read {
    name: String,
    count: Option<u32>,
    session: Option<SessionId>,
}

/// A read a snapshot has in flight, its place in the order submitted, and
/// what resolves its reply.
struct Flight {
    id: u16,
    index: usize,
    name: String,
    resolve: crate::chan::Sender<Result<Vec<u8>, CallError>>,
}

/// A snapshot [`SettingsCache::snapshot_with`] takes, which the world polls
/// whenever one of its reads is answered or times out. The snapshot keeps
/// its window of reads in flight, as it does in the engine, and the world
/// sends each read the moment it is submitted. Once the snapshot ends, the
/// reads still in flight are let go.
struct Snapshotting {
    /// The snapshot, which hands the cache back with its outcome.
    snapshot: Pin<Box<dyn Future<Output = Taken>>>,
    /// The reads submitted and not sent yet.
    submitted: Rc<RefCell<VecDeque<Submitted>>>,
    /// The reads sent and not answered.
    flights: Vec<Flight>,
    /// How many reads were sent.
    sent: usize,
    /// The reads answered, each with its place in the order submitted.
    reads: Vec<(usize, Read)>,
}

type Taken = (SettingsCache, Result<(), CallError>);

type Submitted = (
    String,
    Vec<u8>,
    crate::chan::Sender<Result<Vec<u8>, CallError>>,
);

enum Wake {
    Timeout(usize, u64),
    ReadTimeout(usize, u16),
    Tick,
    Boot,
    ToDevice(usize, Vec<u8>),
    ToHost(usize, Vec<u8>, Stamp),
    /// The port's link reconnects to the device, which has rebooted.
    Reconnect(usize, Stamp),
    Op(Op, u64),
}

/// What law 3 says follows one event.
struct Expected {
    claim: Claim,
    action: Option<CacheAction>,
    class: Option<Class>,
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
    oracles: BTreeMap<SessionId, Oracle>,
    off: bool,
    tick_at: u64,
    /// When each port's link, down and up, is next clear, for links that keep order.
    clear: Vec<[u64; 2]>,
    quiet_until: u64,
    /// Whether the device has beaten, and so refreshed its reports, since
    /// the quiet period began and since the last request it took, so it has
    /// announced every report that moved.
    refreshed: bool,
    routes: BTreeMap<u16, usize>,
    /// The calls that timed out, each saying whether a snapshot made it.
    timed_out: BTreeMap<u16, bool>,
    next_id: u16,
    clients: Vec<Client>,
    failure: Option<String>,
    trace: VecDeque<String>,
    stats: Stats,
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Down,
    Up(Stamp),
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
                reload: true,
                reload_at: None,
                inbox: VecDeque::new(),
                calling: None,
                generation: 0,
                ops: VecDeque::new(),
                claim: Claim::Nothing,
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
            oracles: BTreeMap::new(),
            off: true,
            tick_at: 0,
            clear: vec![[0; 2]; case.ports],
            quiet_until: 0,
            refreshed: false,
            routes: BTreeMap::new(),
            timed_out: BTreeMap::new(),
            next_id: 0,
            clients,
            failure: None,
            trace: VecDeque::new(),
            stats: Stats::default(),
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
                "{violation}\nat {} ms\nlast events:\n{}",
                self.now / MS,
                trace.join("\n")
            ));
        }
    }

    fn schedule(&mut self, at: u64, wake: Wake) {
        self.order += 1;
        self.queue.insert((at, self.order), wake);
    }

    /// The oracle of `session`.
    fn oracle(&self, session: SessionId) -> Option<&Oracle> {
        self.oracles.get(&session)
    }

    /// The oracle of the session the device runs.
    fn current(&self) -> &Oracle {
        self.oracle(self.device.session)
            .expect("the device booted into a session with an oracle")
    }

    /// Whether host `c`'s cache still caches, as one of firmware with the
    /// counter does until an event shows it legacy or of another session.
    fn live(&self, c: usize) -> bool {
        let client = &self.clients[c];
        self.case.firmware.counter && !client.legacy && !client.foreign
    }

    /// Where the device stands now.
    fn stamp(&self) -> Stamp {
        Stamp {
            session: self.device.session,
            position: self.current().latest(),
        }
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
            Wake::ReadTimeout(c, id) => {
                let answer = Answer {
                    reply: Err(CallError::Timeout),
                    queued: 0,
                    stamp: None,
                };
                self.read_answered(c, id, answer);
            }
            Wake::Tick => self.tick(),
            Wake::Boot => self.boot(),
            Wake::ToDevice(port, packet) => self.device_receives(port, &packet),
            Wake::ToHost(port, packet, stamp) => self.proxy_receives(port, &packet, stamp),
            Wake::Reconnect(port, stamp) => self.hand(port, DeviceEvent::NewHash(None), stamp),
            Wake::Op(op, seed) => {
                self.rng = Rng(seed);
                self.op(op);
            }
        }
    }

    /// Sends `packet` along one direction of a port's link, which delays it,
    /// and outside a quiet period may drop, duplicate, hold back or reorder
    /// it. A packet sent during a quiet period comes after every packet sent
    /// before it along the same direction.
    fn send(&mut self, port: usize, direction: Direction, packet: Vec<u8>) {
        let faults = &self.case.faults;
        let faulty = self.now >= self.quiet_until;
        if faulty && self.rng.chance(faults.drop) {
            self.stats.bump(Class::Dropped);
            return;
        }
        let copies = 1 + usize::from(faulty && self.rng.chance(faults.duplicate));
        for _ in 0..copies {
            let mut delay = faults.delay_ms * MS + self.rng.below(faults.jitter_ms * MS + 1);
            if faulty && self.rng.chance(faults.late) {
                delay += TIMEOUT + self.rng.below(TIMEOUT / 2);
                self.stats.bump(Class::Late);
            }
            let lane = usize::from(matches!(direction, Direction::Up(_)));
            let clear = &mut self.clear[port][lane];
            let at = match faults.reorder && faulty {
                true => self.now + delay,
                false => (self.now + delay).max(*clear),
            };
            *clear = (*clear).max(at);
            let wake = match direction {
                Direction::Down => Wake::ToDevice(port, packet.clone()),
                Direction::Up(stamp) => Wake::ToHost(port, packet.clone(), stamp),
            };
            self.schedule(at, wake);
        }
    }

    /// Puts what the device wrote on the wire as the firmware would, a reply
    /// to the port that asked and the rest to every port, in the order it was
    /// written unless events fall freely between the rest.
    fn distribute(&mut self, out: Out, asking: Option<usize>, version: Option<RpcRequestId>) {
        let stamp = self.stamp();
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
                self.send(port, Direction::Up(stamp), packet.clone());
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

    /// Counts each announcement into the oracle of the device's session,
    /// which must see them numbered one after another.
    fn record(&mut self, out: &Out) {
        for (_, packet) in out.0.iter().filter(|(lane, _)| *lane == Lane::Announce) {
            let Some(setting) = PacketView::parse_prefix(packet)
                .ok()
                .and_then(|(view, _)| Announcement::parse(view.payload))
            else {
                return self.fail("(model) an announcement does not parse".into());
            };
            let name = setting.name_str().unwrap_or_default().to_string();
            let seq = setting.seq.map(|seq| seq.wrapping_add(self.offset));
            let oracle = self
                .oracles
                .get_mut(&self.device.session)
                .expect("the device booted into a session with an oracle");
            let expected = oracle.count(oracle.latest() + 1);
            if seq != Some(expected) {
                return self.fail(format!(
                    "(model) {name} was announced as {seq:?}, not {expected}"
                ));
            }
            let mut next = oracle.state(oracle.latest()).clone();
            if cacheable().contains(name.as_str()) {
                next.insert(name.clone(), setting.reply.to_vec());
            }
            oracle.states.push(next);
            self.note(format!(
                "device announces #{expected} {name} = {:?}",
                setting.reply
            ));
        }
    }

    /// Reads every cacheable entry and `settings.version` past the ports, as
    /// a request from nowhere, returning the raw count and every value read.
    fn read_all(&mut self, out: &mut Out) -> (u32, BTreeMap<String, Vec<u8>>) {
        let names: Vec<&str> = cacheable().iter().copied().chain([COUNTER]).collect();
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
    /// at what it stored. Wrapping firmware starts the session's count at
    /// most eight short of the wrap once the stored settings are loaded.
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
        let mut early = Out::default();
        let (booted, values) = self.read_all(&mut early);
        let known = early
            .0
            .iter()
            .filter(|(lane, _)| *lane == Lane::Announce)
            .count();
        let mut out = Out::default();
        if let Some(stored) = self.stored.clone() {
            let _ = self
                .device
                .handle(&(), Input::Loaded(None, Ok(&stored)), self.now, &mut out);
        }
        let (loaded, _) = self.read_all(&mut out);
        self.offset = match self.case.firmware.wrap {
            true => u32::MAX
                .wrapping_sub(loaded)
                .wrapping_sub(self.rng.below(8) as u32),
            false => 0,
        };
        let first = booted.wrapping_sub(known as u32).wrapping_add(self.offset);
        self.oracles.insert(
            session,
            Oracle {
                first,
                known,
                states: vec![values],
            },
        );
        self.off = false;
        self.note(format!(
            "device boots session {:#x} at #{first}, read at #{}",
            session.value(),
            booted.wrapping_add(self.offset)
        ));
        let stamp = self.stamp();
        for port in 0..self.case.ports {
            let clear = &mut self.clear[port][1];
            let at = self.now.max(*clear);
            *clear = at;
            self.schedule(at, Wake::Reconnect(port, stamp));
        }
        self.distribute(early, None, None);
        self.record(&out);
        self.distribute(out, None, None);
        self.tick_at = self.now;
        self.schedule(self.now, Wake::Tick);
    }

    /// Turns the device off, for longer than any packet takes on links that
    /// reorder.
    fn power_off(&mut self) {
        self.off = true;
        let faults = &self.case.faults;
        let downtime = match faults.reorder {
            true => faults.longest() + (100 + self.rng.below(400)) * MS,
            false => self.rng.below(400) * MS,
        };
        self.note("device powers off".into());
        self.schedule(self.now + downtime, Wake::Boot);
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
        self.refreshed |= out.0.iter().any(|(_, packet)| {
            PacketView::parse_prefix(packet)
                .is_ok_and(|(view, _)| view.header.ptype == PacketType::HEARTBEAT)
        });
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
        self.refreshed = false;
        let version = Request::parse(view.payload)
            .filter(|request| request.method == Method::ByName(COUNTER.as_bytes()))
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
    fn proxy_receives(&mut self, port: usize, packet: &[u8], stamp: Stamp) {
        let Ok((packet, _)) = Packet::from_slice_prefix(packet) else {
            return;
        };
        let event = match packet.payload() {
            Payload::RpcReply(reply) => {
                return self.answered(reply.req_id.value(), Ok(reply.value.to_vec()), stamp)
            }
            Payload::RpcError(error) => {
                let refused = CallError::DeviceError(RpcErrorPayload::from_wire(error));
                return self.answered(error.req_id.value(), Err(refused), stamp);
            }
            Payload::Heartbeat(beat) => DeviceEvent::Heartbeat {
                session_id: beat.session(),
                settings: beat.settings(),
            },
            Payload::Setting(setting) => match setting.name_str() {
                Some(name) => DeviceEvent::Setting(SettingChange::new(
                    name.to_string(),
                    setting.seq,
                    setting.reply.to_vec(),
                    None,
                )),
                None => return,
            },
            _ => return,
        };
        self.hand(port, event, stamp);
    }

    /// Queues `event` for every host on `port`, as the proxy's pump does.
    fn hand(&mut self, port: usize, event: DeviceEvent, stamp: Stamp) {
        let listening: Vec<usize> = (0..self.clients.len())
            .filter(|&c| self.clients[c].port == port)
            .collect();
        for &c in &listening {
            self.clients[c].inbox.push_back((event.clone(), stamp));
        }
        for c in listening {
            self.serve(c);
        }
    }

    /// Hands a reply to the host that waits on its call. A reply after its
    /// call timed out is let go.
    fn answered(&mut self, id: u16, reply: Result<Vec<u8>, CallError>, stamp: Stamp) {
        let Some(&c) = self.routes.get(&id) else {
            if let Some(read) = self.timed_out.remove(&id) {
                self.stats.bump(Class::LateReply);
                if read {
                    self.stats.bump(Class::LateRead);
                }
                self.note(format!("a reply to call {id} comes after it timed out"));
            }
            return;
        };
        let client = &mut self.clients[c];
        let answer = Answer {
            reply,
            queued: client.inbox.len(),
            stamp: Some(stamp),
        };
        match client.calling.as_mut() {
            Some(Calling::Calls { ids, answers, .. }) => {
                answers.insert(id, answer);
                if ids.iter().all(|id| answers.contains_key(id)) {
                    self.finish(c);
                }
            }
            Some(Calling::Snapshot(_)) => self.read_answered(c, id, answer),
            None => {}
        }
    }

    /// Host `c` sends each of `calls` at once, then waits for every reply or
    /// its timeout before doing `then`.
    /// Puts host `c`'s call of `name` on the wire, returning its request id.
    fn send_call(&mut self, c: usize, name: &str, arg: &[u8]) -> u16 {
        self.next_id = self.next_id.wrapping_add(1);
        let id = self.next_id;
        self.routes.insert(id, c);
        self.timed_out.remove(&id);
        let request = Packet::rpc_request(name, arg, id, DeviceRoute::root())
            .expect("a request fits a packet");
        self.send(
            self.clients[c].port,
            Direction::Down,
            request.as_bytes().to_vec(),
        );
        id
    }

    /// Host `c` sends each of `calls` at once, then waits for every reply or
    /// its timeout before doing `then`.
    fn start_call(&mut self, c: usize, calls: &[(&str, Vec<u8>)], then: Then) {
        let ids = calls
            .iter()
            .map(|(name, arg)| self.send_call(c, name, arg))
            .collect();
        let client = &mut self.clients[c];
        client.generation += 1;
        client.calling = Some(Calling::Calls {
            ids,
            answers: BTreeMap::new(),
            then,
        });
        let timeout = Wake::Timeout(c, client.generation);
        self.schedule(self.now + TIMEOUT, timeout);
    }

    /// Host `c`'s calls are answered or timed out.
    fn finish(&mut self, c: usize) {
        let client = &mut self.clients[c];
        let Some(Calling::Calls {
            ids,
            mut answers,
            then,
        }) = client.calling.take()
        else {
            return;
        };
        client.generation += 1;
        let queued = client.inbox.len();
        let mut taken = Vec::with_capacity(ids.len());
        for id in ids {
            self.routes.remove(&id);
            let answer = answers.remove(&id).unwrap_or_else(|| {
                self.timed_out.insert(id, false);
                Answer {
                    reply: Err(CallError::Timeout),
                    queued,
                    stamp: None,
                }
            });
            taken.push(answer);
        }
        match then {
            Then::Writes(names) => self.wrote(c, &names, taken),
            Then::Registry => self.registry(c, taken),
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

    /// Hands up to `count` queued events to host `c`'s cache one at a time
    /// and does what each asks before the next, as the engine's worker does.
    /// It snapshots only when the cache asks. It loads the registry with an
    /// event queued and keeps that event for the cache the load builds, and
    /// loads again at once when the cache asks or a heartbeat of another
    /// session comes. After a failed load it drops every event until
    /// [`RELOAD_DELAY`] has passed and loads again at the first event after.
    /// Returns how many events it took.
    fn drain(&mut self, c: usize, count: usize) -> usize {
        let mut taken = 0;
        while taken < count && self.failure.is_none() && self.clients[c].calling.is_none() {
            let client = &self.clients[c];
            if client.reload && client.reload_at.is_none() && !client.inbox.is_empty() {
                self.read_registry(c);
                break;
            }
            let Some((event, stamp)) = self.clients[c].inbox.pop_front() else {
                break;
            };
            taken += 1;
            if let DeviceEvent::NewHash(_) = event {
                let client = &mut self.clients[c];
                client.reload = true;
                client.reload_at = None;
                self.read_registry(c);
                continue;
            }
            if let Some(at) = self.clients[c].reload_at {
                if self.now >= at {
                    self.clients[c].reload_at = None;
                    self.read_registry(c);
                }
                continue;
            }
            let foreign = match &event {
                DeviceEvent::Heartbeat {
                    session_id: Some(session),
                    ..
                } => self.clients[c]
                    .session
                    .is_some_and(|known| known != *session),
                _ => false,
            };
            let action = self.observe(c, event, stamp);
            let client = &mut self.clients[c];
            client.reload |= foreign || action == CacheAction::Reload;
            if client.reload {
                self.read_registry(c);
            } else if action == CacheAction::Snapshot {
                self.start_snapshot(c);
            }
        }
        taken
    }

    /// What law 3 says host `c`'s cache claims once it takes `event`, which a
    /// packet stamped `stamp` brought, and the action it returns where the
    /// spec fixes one.
    fn expect(&self, c: usize, event: &DeviceEvent, stamp: Stamp) -> Result<Expected, String> {
        let client = &self.clients[c];
        let live = self.live(c);
        let expected = |claim, action, class| Expected {
            claim,
            action,
            class,
        };
        let position = |count: u32| {
            self.oracle(stamp.session)
                .and_then(|oracle| oracle.position(count))
                .ok_or_else(|| {
                    format!(
                        "(model) session {:#x} never reached #{count}",
                        stamp.session.value()
                    )
                })
        };
        let keep = Some(CacheAction::Keep);
        Ok(match event {
            DeviceEvent::Heartbeat {
                session_id: Some(session),
                settings,
            } => {
                if client.session != Some(*session) {
                    let action = match live {
                        true => CacheAction::Reload,
                        false => CacheAction::Keep,
                    };
                    let class = live.then_some(Class::Foreign);
                    return Ok(expected(Claim::Nothing, Some(action), class));
                }
                let Some(count) = settings else {
                    return Ok(expected(Claim::Nothing, keep, None));
                };
                match client.claim {
                    Claim::At(p) if position(*count)? > p => expected(
                        Claim::Nothing,
                        Some(CacheAction::Snapshot),
                        Some(Class::Behind),
                    ),
                    Claim::At(p) => expected(Claim::At(p), keep, None),
                    Claim::Nothing if live => {
                        expected(Claim::Nothing, Some(CacheAction::Snapshot), None)
                    }
                    claim => expected(claim, None, None),
                }
            }
            DeviceEvent::Setting(SettingChange { seq: None, .. }) => {
                expected(Claim::Nothing, keep, None)
            }
            DeviceEvent::Setting(SettingChange {
                seq: Some(count), ..
            }) => match client.claim {
                Claim::At(p) if client.session == Some(stamp.session) => match position(*count)? {
                    q if q <= p => expected(Claim::At(p), keep, None),
                    q if q == p + 1 => expected(Claim::At(q), keep, None),
                    _ => expected(
                        Claim::Nothing,
                        Some(CacheAction::Snapshot),
                        Some(Class::Gap),
                    ),
                },
                Claim::At(_) | Claim::Unknown => {
                    expected(Claim::Unknown, None, Some(Class::Unknown))
                }
                Claim::Nothing if live => {
                    expected(Claim::Nothing, Some(CacheAction::Snapshot), None)
                }
                Claim::Nothing => expected(Claim::Nothing, None, None),
            },
            DeviceEvent::Heartbeat {
                session_id: None, ..
            }
            | DeviceEvent::NewHash(_) => expected(client.claim, keep, None),
        })
    }

    /// Hands one event to host `c`'s cache, returning what it asks.
    fn observe(&mut self, c: usize, event: DeviceEvent, stamp: Stamp) -> CacheAction {
        let expected = match self.clients[c].cache.is_some() {
            true => match self.expect(c, &event, stamp) {
                Ok(expected) => Some(expected),
                Err(failure) => {
                    self.fail(failure);
                    return CacheAction::Keep;
                }
            },
            false => None,
        };
        let client = &mut self.clients[c];
        match &event {
            DeviceEvent::Heartbeat {
                session_id: Some(session),
                settings,
            } => {
                client.legacy |= settings.is_none();
                client.foreign |= client.session.is_some_and(|known| known != *session);
            }
            DeviceEvent::Setting(SettingChange { seq: None, .. }) => client.legacy = true,
            DeviceEvent::Heartbeat {
                session_id: None, ..
            }
            | DeviceEvent::Setting(SettingChange { seq: Some(_), .. })
            | DeviceEvent::NewHash(_) => {}
        }
        let before = client.cache.as_ref().and_then(SettingsCache::version);
        let action = client
            .cache
            .as_mut()
            .map_or(CacheAction::Keep, |cache| cache.observe(&event));
        let applied = client.cache.as_ref().and_then(SettingsCache::version);
        if let Some(expected) = &expected {
            client.claim = expected.claim;
        }
        if let (Some(before), Some(applied)) = (before, applied) {
            if applied < before {
                self.stats.bump(Class::Wrap);
            }
        }
        let line = match &event {
            DeviceEvent::Heartbeat {
                session_id,
                settings,
            } => format!(
                "heartbeat {:x?} {settings:?}",
                session_id.map(SessionId::value)
            ),
            DeviceEvent::Setting(SettingChange { name, seq, .. }) => format!("{name} {seq:?}"),
            DeviceEvent::NewHash(_) => "another event".to_string(),
        };
        self.note(format!(
            "host {c} observes {line}: {action:?}, at {applied:?}"
        ));
        if let Some(expected) = expected {
            if let Some(class) = expected.class {
                self.stats.bump(class);
            }
            if expected.action.is_some_and(|wanted| wanted != action) {
                self.fail(format!(
                    "law 3, host {c} returned {action:?} for {line}, not {:?}",
                    expected.action
                ));
            }
        }
        self.check(c);
        action
    }

    /// Host `c` reads what identifies the device's registry.
    fn read_registry(&mut self, c: usize) {
        let calls = [
            ("dev.name", vec![]),
            ("rpc.hash", vec![]),
            ("dev.session", vec![]),
        ];
        self.start_call(c, &calls, Then::Registry);
    }

    /// Builds a new cache from the registry the replies describe, as a hit
    /// in the registry's disk cache does, and snapshots it. What was queued
    /// meanwhile stays queued for it, as in the engine. A failed load drops
    /// the cache and waits [`RELOAD_DELAY`] before the next.
    fn registry(&mut self, c: usize, answers: Vec<Answer>) {
        let decoded = |reply: &Result<Vec<u8>, CallError>| match reply {
            Ok(reply) => u32::decode_reply(reply).map_err(CallError::InvalidReply),
            Err(error) => Err(error.clone()),
        };
        let read = match answers.as_slice() {
            [Answer { reply: Ok(_), .. }, hash, session] => decoded(&hash.reply)
                .and_then(|hash| Ok((hash, decoded(&session.reply)?, session.stamp))),
            [Answer {
                reply: Err(error), ..
            }, _, _] => Err(error.clone()),
            _ => Err(CallError::ResponseLost),
        };
        let (hash, session) = match read {
            Ok((hash, session, stamp)) => {
                let session = SessionId::new(session);
                if stamp.is_some_and(|stamp| stamp.session != session) {
                    return self.fail(format!(
                        "(model) session {:#x} answered dev.session with {:#x}",
                        stamp.map_or(0, |stamp| stamp.session.value()),
                        session.value()
                    ));
                }
                (hash, session)
            }
            Err(error) => {
                let client = &mut self.clients[c];
                client.cache = None;
                client.claim = Claim::Nothing;
                client.reload_at = Some(self.now + RELOAD_DELAY);
                return self.note(format!("host {c} could not read the registry: {error}"));
            }
        };
        let firmware = &self.case.firmware;
        let mut registry = RpcRegistry::new(
            table()
                .iter()
                .filter(|spec| firmware.counter || spec.name != COUNTER)
                .map(|spec| RpcDescriptor::from_meta(spec.metadata(), spec.name.to_string()))
                .collect(),
        );
        registry.hash = Some(hash);
        registry.session = Some(session);
        let client = &mut self.clients[c];
        client.cache = Some(SettingsCache::new(&registry));
        client.session = Some(session);
        client.reload = false;
        client.claim = Claim::Nothing;
        client.settled = None;
        client.legacy = false;
        client.foreign = false;
        client.rebuilt += 1;
        self.stats.bump(Class::Reload);
        self.note(format!(
            "host {c} reloads in session {:#x}",
            session.value()
        ));
        self.check(c);
        self.start_snapshot(c);
    }

    /// Starts the cache's own snapshot, each of its calls one of host `c`'s.
    fn start_snapshot(&mut self, c: usize) {
        let Some(mut cache) = self.clients[c].cache.take() else {
            return;
        };
        if cache.version().is_some() || !cache.values().is_empty() {
            let (claimed, held) = (cache.version(), cache.values().len());
            self.fail(format!(
                "law 9, host {c} snapshots a cache that claims {claimed:?} with {held} values"
            ));
        }
        let submitted = Rc::new(RefCell::new(VecDeque::new()));
        let submit = {
            let submitted = Rc::clone(&submitted);
            move |name: &str, arg: &[u8]| {
                let (resolve, reply) = crate::chan::bounded(1);
                let call = (name.to_string(), arg.to_vec(), resolve);
                submitted.borrow_mut().push_back(call);
                PendingReply { reply }
            }
        };
        let snapshot = Box::pin(async move {
            let taken = cache.snapshot_with(submit).await;
            (cache, taken)
        });
        let snapshotting = Snapshotting {
            snapshot,
            submitted,
            flights: Vec::new(),
            sent: 0,
            reads: Vec::new(),
        };
        self.advance_snapshot(c, snapshotting);
    }

    /// Hands host `c`'s snapshot the answer to its read `id`, if the read is
    /// still in flight, and lets the snapshot go on. A read that timed out
    /// is counted, so its reply, if it comes, is known to be late.
    fn read_answered(&mut self, c: usize, id: u16, answer: Answer) {
        let in_flight = match &self.clients[c].calling {
            Some(Calling::Snapshot(snapshotting)) => {
                snapshotting.flights.iter().any(|flight| flight.id == id)
            }
            _ => false,
        };
        if !in_flight {
            return;
        }
        let Some(Calling::Snapshot(mut snapshotting)) = self.clients[c].calling.take() else {
            return;
        };
        let at = snapshotting
            .flights
            .iter()
            .position(|flight| flight.id == id)
            .expect("the read is in flight");
        let flight = snapshotting.flights.remove(at);
        self.routes.remove(&id);
        if answer.stamp.is_none() {
            self.timed_out.insert(id, true);
        }
        let count = match flight.name == COUNTER {
            true => answer
                .reply
                .as_deref()
                .ok()
                .and_then(|reply| u32::decode_reply(reply).ok()),
            false => None,
        };
        let read = Read {
            name: flight.name,
            count,
            session: answer.stamp.map(|stamp| stamp.session),
        };
        snapshotting.reads.push((flight.index, read));
        let _ = flight.resolve.send(answer.reply);
        self.advance_snapshot(c, snapshotting);
        if self.clients[c].calling.is_none() {
            self.serve(c);
        }
    }

    /// Polls host `c`'s snapshot and sends each read it submitted, each with
    /// its own timeout, or takes its outcome once it has ended and lets the
    /// reads still in flight go.
    fn advance_snapshot(&mut self, c: usize, mut snapshotting: Snapshotting) {
        if let Poll::Ready(taken) = poll_once(snapshotting.snapshot.as_mut()) {
            for flight in snapshotting.flights.drain(..) {
                self.routes.remove(&flight.id);
            }
            let mut reads = std::mem::take(&mut snapshotting.reads);
            reads.sort_by_key(|(index, _)| *index);
            let reads: Vec<Read> = reads.into_iter().map(|(_, read)| read).collect();
            return self.snapshot_done(c, &reads, taken);
        }
        let submitted: Vec<Submitted> = snapshotting.submitted.borrow_mut().drain(..).collect();
        for (name, arg, resolve) in submitted {
            let id = self.send_call(c, &name, &arg);
            self.schedule(self.now + TIMEOUT, Wake::ReadTimeout(c, id));
            snapshotting.flights.push(Flight {
                id,
                index: snapshotting.sent,
                name,
                resolve,
            });
            snapshotting.sent += 1;
        }
        if snapshotting.flights.is_empty() {
            return self.fail("(model) a snapshot waits on no call".into());
        }
        self.clients[c].calling = Some(Calling::Snapshot(snapshotting));
    }

    /// Hands host `c` its cache back from a snapshot that read `reads` and
    /// ended at `taken`, checking laws 2 and 9.
    fn snapshot_done(&mut self, c: usize, reads: &[Read], (cache, taken): Taken) {
        let client = &mut self.clients[c];
        client.cache = Some(cache);
        client.rebuilt += 1;
        match &taken {
            Ok(()) => {
                self.stats.bump(Class::Snapshot);
                let claim = self.snapshot_claim(c, reads);
                let client = &mut self.clients[c];
                client.claim = claim;
                client.settled = reads.last().and_then(|read| read.count);
            }
            Err(error) => {
                self.stats.bump(Class::Failed);
                if matches!(error, CallError::Timeout) {
                    self.stats.bump(Class::TimedOut);
                }
                client.claim = Claim::Nothing;
                client.settled = None;
                let cache = client.cache.as_ref().expect("the cache was handed back");
                if cache.version().is_some() || !cache.values().is_empty() {
                    let (claimed, held) = (cache.version(), cache.values().len());
                    self.fail(format!(
                        "law 9, host {c}'s snapshot failed at {error}, but it claims {claimed:?} with {held} values"
                    ));
                }
            }
        }
        let claim = self.clients[c].claim;
        self.note(format!("host {c} snapshot {taken:?}, claiming {claim:?}"));
        self.check(c);
    }

    /// What law 2 says a snapshot that succeeded claims, from the reads it
    /// made.
    fn snapshot_claim(&mut self, c: usize, reads: &[Read]) -> Claim {
        let session = self.clients[c].session;
        if !self.live(c) {
            if !reads.is_empty() {
                self.fail(format!(
                    "law 2, host {c} read {} entries of a device it cannot keep exact",
                    reads.len()
                ));
            }
            return Claim::Nothing;
        }
        let span = cacheable().len() + 2;
        let pairs: Vec<(u32, u32)> = reads
            .chunks(span)
            .filter_map(|attempt| {
                let (first, rest) = attempt.split_first()?;
                let (last, between) = rest.split_last()?;
                let mut names: Vec<&str> = between.iter().map(|read| read.name.as_str()).collect();
                names.sort_unstable();
                let whole = first.name == COUNTER
                    && last.name == COUNTER
                    && names.iter().copied().eq(cacheable().iter().copied());
                whole.then_some((first.count?, last.count?))
            })
            .collect();
        if pairs.is_empty() || pairs.len() * span != reads.len() {
            let names: Vec<&str> = reads.iter().map(|read| read.name.as_str()).collect();
            self.fail(format!("law 2, host {c}'s snapshot read {names:?}"));
            return Claim::Unknown;
        }
        let (&(before, after), earlier) = pairs.split_last().expect("a pair at least");
        if earlier.iter().any(|(before, after)| before == after) {
            self.fail(format!(
                "law 2, host {c}'s snapshot read again after its counts agreed, {pairs:?}"
            ));
            return Claim::Unknown;
        }
        if !earlier.is_empty() {
            self.stats.bump(Class::Retried);
        }
        if before != after {
            if earlier.is_empty() {
                self.fail(format!(
                    "law 2, host {c}'s snapshot gave up at its first try, {pairs:?}"
                ));
                return Claim::Unknown;
            }
            self.stats.bump(Class::GaveUp);
        }
        let mut answered = reads.iter().filter_map(|read| read.session);
        if answered.any(|answered| Some(answered) != session) {
            return Claim::Unknown;
        }
        match session
            .and_then(|session| self.oracle(session))
            .and_then(|oracle| oracle.position(before))
        {
            Some(position) => Claim::At(position),
            None => {
                self.fail(format!(
                    "law 2, host {c}'s snapshot claims #{before}, which its session never reached"
                ));
                Claim::Unknown
            }
        }
    }

    /// What every cache promises at every moment, laws 1, 3, 4 and 6.
    fn check(&mut self, c: usize) {
        let client = &self.clients[c];
        let Some(cache) = &client.cache else {
            return;
        };
        if client.claim == Claim::Unknown {
            self.stats.bump(Class::Skipped);
        }
        if let Some(name) = cache
            .values()
            .keys()
            .find(|name| !cacheable().contains(name.as_str()))
        {
            return self.fail(format!("law 1, host {c} caches {name}"));
        }
        if (!self.case.firmware.counter || client.legacy) && !cache.values().is_empty() {
            let names: Vec<&String> = cache.values().keys().collect();
            return self.fail(format!(
                "law 6, host {c} caches {names:?} of legacy firmware"
            ));
        }
        let claimed = cache.version();
        let p = match client.claim {
            Claim::Unknown => return,
            Claim::Nothing if claimed.is_none() && cache.values().is_empty() => return,
            Claim::Nothing => {
                let held = cache.values().len();
                return self.fail(format!(
                    "law 3, host {c} claims {claimed:?} with {held} values, where it holds nothing"
                ));
            }
            Claim::At(p) => p,
        };
        let oracle = client
            .session
            .and_then(|session| self.oracle(session))
            .expect("a claim has the oracle of its session");
        let count = oracle.count(p);
        if claimed != Some(count) {
            return self.fail(format!(
                "law 3, host {c} claims {claimed:?}, where it is at #{count}"
            ));
        }
        let last = client
            .settled
            .and_then(|settled| oracle.position(settled))
            .map_or(p, |settled| settled.max(p));
        let wrong = cacheable().iter().find(|name| {
            let held = cache.get(name);
            !oracle
                .window(p, last)
                .iter()
                .any(|state| state.get(**name).map(Vec::as_slice) == held)
        });
        if let Some(name) = wrong {
            let held = cache.get(name).map(<[u8]>::to_vec);
            let device = oracle.state(p).get(*name).cloned();
            return self.fail(format!(
                "law 4, host {c} holds {name} = {held:?} at #{count}, where the device held {device:?}"
            ));
        }
        self.stats.bump(Class::Exact);
    }

    /// Runs a quiet period until every host has caught up, then checks law
    /// 7 and lets the faults resume. Past [`QUIET_LIMIT`] it checks law 7
    /// all the same, and fails as the harness's fault if the law holds.
    fn settle(&mut self) {
        while self.failure.is_none() && !self.caught_up() {
            let Some(entry) = self.queue.first_entry() else {
                break;
            };
            if entry.key().0 > self.quiet_until {
                self.quiet_check();
                return self.fail(format!(
                    "(harness) the hosts did not catch up within {} ms of quiet",
                    QUIET_LIMIT / MS
                ));
            }
            let ((at, _), wake) = entry.remove_entry();
            self.now = at;
            self.dispatch(wake);
        }
        if self.failure.is_none() {
            self.quiet_check();
            self.quiet_until = self.now;
        }
    }

    /// Whether the device is up, refreshed and has no packet in flight, and
    /// every host waits on nothing, has nothing queued and owes no load, with
    /// its cache at the device's count or caching nothing.
    fn caught_up(&self) -> bool {
        let moving = self.queue.values().any(|wake| {
            matches!(
                wake,
                Wake::Boot | Wake::ToDevice(..) | Wake::ToHost(..) | Wake::Reconnect(..)
            )
        });
        if self.off || moving || !self.refreshed {
            return false;
        }
        let oracle = self.current();
        let count = oracle.count(oracle.latest());
        self.clients.iter().all(|client| {
            let idle = client.calling.is_none()
                && client.inbox.is_empty()
                && !client.reload
                && client.reload_at.is_none();
            let current = match self.case.firmware.counter && !client.legacy {
                true => {
                    client.session == Some(self.device.session)
                        && client.cache.as_ref().and_then(SettingsCache::version) == Some(count)
                }
                false => client.cache.is_some(),
            };
            idle && current
        })
    }

    /// The end of a quiet period, by which the device has announced all it
    /// holds and every cache holds exactly that, law 7.
    fn quiet_check(&mut self) {
        self.stats.bump(Class::Quiet);
        if self.off {
            return self.fail("(model) the device is off at the end of a quiet period".into());
        }
        let mut out = Out::default();
        let (raw, values) = self.read_all(&mut out);
        if out.0.iter().any(|(lane, _)| *lane == Lane::Announce) {
            return self.fail("(model) the device still had a change to announce".into());
        }
        let oracle = self.current();
        let (count, state) = (oracle.count(oracle.latest()), oracle.state(oracle.latest()));
        if raw.wrapping_add(self.offset) != count || *state != values {
            return self.fail(format!(
                "(model) the device holds #{} {values:?}, but announced #{count} {state:?}",
                raw.wrapping_add(self.offset)
            ));
        }
        let nothing = BTreeMap::new();
        let failure = self.clients.iter().enumerate().find_map(|(c, client)| {
            let Some(cache) = client.cache.as_ref().filter(|_| client.calling.is_none()) else {
                return Some(format!("law 7, host {c} has no cache after a quiet period"));
            };
            let (claim, held) = match self.case.firmware.counter && !client.legacy {
                true => (Some(count), state),
                false => (None, &nothing),
            };
            (cache.version() != claim || cache.values() != held).then(|| {
                format!(
                    "law 7, host {c} claims {:?} with {:?} after a quiet period, the device #{count} {state:?}",
                    cache.version(),
                    cache.values()
                )
            })
        });
        if let Some(failure) = failure {
            self.fail(failure);
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
                self.quiet_until = self.now + QUIET_LIMIT;
                self.refreshed = false;
                for client in &mut self.clients {
                    client.ops.clear();
                }
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

    /// Host `c` checks each of its writes against its cache once the events
    /// queued ahead of its reply are in, unless it snapshots meanwhile.
    fn wrote(&mut self, c: usize, names: &[&str], answers: Vec<Answer>) {
        let mut seen = 0;
        for (name, answer) in names.iter().zip(answers) {
            let rebuilt = self.clients[c].rebuilt;
            seen += self.drain(c, answer.queued.saturating_sub(seen));
            let unbroken = self.clients[c].rebuilt == rebuilt && self.clients[c].calling.is_none();
            if let (Ok(reply), Some(stamp), true) = (answer.reply, answer.stamp, unbroken) {
                self.own_write(c, name, &reply, stamp);
            }
        }
    }

    /// Law 8, for host `c`'s write of `reply` to `name`, answered from where
    /// `stamp` says the device stood.
    fn own_write(&mut self, c: usize, name: &str, reply: &[u8], stamp: Stamp) {
        let client = &self.clients[c];
        let Some(cache) = &client.cache else {
            return;
        };
        if !cacheable().contains(name) || !self.live(c) || client.session != Some(stamp.session) {
            return;
        }
        let oracle = self
            .oracle(stamp.session)
            .expect("every session has an oracle");
        let w = stamp.position;
        if oracle.state(w).get(name).map(Vec::as_slice) != Some(reply) {
            let device = oracle.state(w).get(name).cloned();
            return self.fail(format!(
                "(model) host {c} wrote {name} = {reply:?} at #{}, where the device held {device:?}",
                oracle.count(w)
            ));
        }
        let ordered = self.case.faults.ordered();
        let class = match client.claim {
            Claim::At(p) if p >= w => {
                let last = client
                    .settled
                    .and_then(|settled| oracle.position(settled))
                    .map_or(p, |settled| settled.max(p));
                let unchanged = oracle
                    .window(w, last)
                    .iter()
                    .all(|state| state.get(name).map(Vec::as_slice) == Some(reply));
                match (unchanged, cache.get(name) == Some(reply)) {
                    (false, _) => Class::OwnOverwritten,
                    (true, true) => Class::OwnHeld,
                    (true, false) => {
                        let held = cache.get(name).map(<[u8]>::to_vec);
                        return self.fail(format!(
                            "law 8, host {c} wrote {name} = {reply:?} at #{}, but at #{} its cache holds {held:?}",
                            oracle.count(w),
                            oracle.count(p)
                        ));
                    }
                }
            }
            Claim::At(p) if ordered => {
                return self.fail(format!(
                    "law 8, host {c} wrote {name} at #{}, but its cache is at #{} on links that keep order",
                    oracle.count(w),
                    oracle.count(p)
                ));
            }
            Claim::At(_) => Class::OwnShort,
            Claim::Nothing => Class::OwnNothing,
            Claim::Unknown => return,
        };
        self.stats.bump(class);
        if class == Class::OwnHeld && !ordered {
            self.stats.bump(Class::OwnHeldFaulty);
        }
        if ordered && matches!(client.claim, Claim::At(_)) {
            self.stats.bump(Class::OwnOrdered);
        }
    }
}

/// Runs one case to its last quiet period, returning what it counted or the
/// first law it broke.
fn run(case: &Case) -> Result<Stats, String> {
    let mut world = World::new(case);
    let mut at = 0;
    let last = Step {
        gap_ms: 0,
        seed: 0,
        op: Op::Quiet,
    };
    for step in case.steps.iter().chain([&last]) {
        at += step.gap_ms * MS;
        world.schedule(at, Wake::Op(step.op.clone(), step.seed));
        if let Op::Quiet = step.op {
            world.run_until(at);
            world.settle();
            at = world.now;
        }
        if world.failure.is_some() {
            break;
        }
    }
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

fn step() -> impl Strategy<Value = Step> {
    (gap(), any::<u64>(), op()).prop_map(|(gap_ms, seed, op)| Step { gap_ms, seed, op })
}

fn faults() -> impl Strategy<Value = Faults> {
    (
        prop_oneof![Just(0u32), Just(10), Just(50), Just(200)],
        prop_oneof![Just(0u32), Just(20), Just(100)],
        any::<bool>(),
        0u64..10,
        0u64..20,
        prop_oneof![2 => Just(0u32), 1 => Just(5), 1 => Just(30)],
        any::<bool>(),
    )
        .prop_map(
            |(drop, duplicate, reorder, delay_ms, jitter_ms, late, free_events)| Faults {
                drop,
                duplicate,
                reorder,
                delay_ms,
                jitter_ms,
                late,
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
        prop::collection::vec(step(), 1..40),
    )
        .prop_map(|(seed, ports, clients, faults, firmware, steps)| Case {
            seed,
            ports,
            clients,
            faults,
            firmware,
            steps,
        })
}

/// The seed the default run draws its cases from, so that its floors are met
/// or missed the same way every time. `PROPTEST_RNG_SEED` replaces it.
const SEED: u64 = 0x5E77_1265;

fn config(cases: u32, seed: Option<u64>) -> ProptestConfig {
    let mut config = ProptestConfig {
        cases,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    };
    if let (Some(seed), None) = (seed, std::env::var_os("PROPTEST_RNG_SEED")) {
        config.rng_seed = RngSeed::Fixed(seed);
    }
    config
}

/// The fewest cases a run holds its classes to their floors at. Below it,
/// [`Class::GaveUp`] and [`Class::Retried`] may fall short by chance.
const FLOOR_CASES: u32 = 2_000;

/// Runs `cases` cases, drawn from `seed` if given, and panics with the
/// minimal failing case and the law it broke. From [`FLOOR_CASES`] cases on,
/// it also panics when a class falls short of its floor, or when the checks
/// that skip laws 3 and 4 for an unknown claim number more than one in ten
/// of the checks of law 4, against at most one in fifteen under ten random
/// seeds. With [`CLASSES`] set, it prints each class's total and the share
/// of cases that reached it.
fn exercise(cases: u32, seed: Option<u64>) {
    let totals = RefCell::new(Stats::default());
    let reached = RefCell::new(Stats::default());
    let outcome = TestRunner::new(config(cases, seed)).run(&case(), |case| {
        let stats = run(&case).map_err(TestCaseError::fail)?;
        let (mut totals, mut reached) = (totals.borrow_mut(), reached.borrow_mut());
        for class in Class::ALL {
            totals.0[class as usize] += stats.get(class);
            reached.0[class as usize] += u64::from(stats.get(class) > 0);
        }
        Ok(())
    });
    let (totals, reached) = (totals.into_inner(), reached.into_inner());
    if std::env::var_os(CLASSES).is_some() {
        for class in Class::ALL {
            eprintln!(
                "{:<16} {:>9} in all, in {:>5.1}% of {cases} cases",
                format!("{class:?}"),
                totals.get(class),
                100.0 * reached.get(class) as f64 / f64::from(cases)
            );
        }
    }
    if let Err(failure) = outcome {
        panic!("{failure}");
    }
    if cases < FLOOR_CASES {
        return;
    }
    let short: Vec<String> = Class::ALL
        .iter()
        .filter(|class| totals.get(**class) < class.floor() * u64::from(cases) / 1000)
        .map(|class| format!("{class:?} {}", totals.get(*class)))
        .collect();
    assert!(
        short.is_empty(),
        "classes short of their floor over {cases} cases: {short:?}"
    );
    let (skipped, checked) = (totals.get(Class::Skipped), totals.get(Class::Exact));
    assert!(
        10 * skipped <= checked,
        "{skipped} checks skipped laws 3 and 4, against {checked} checks of law 4"
    );
}

/// Every cache keeps to the spec through faults, races, reboots and wraps.
#[test]
fn every_cache_keeps_to_the_spec() {
    exercise(2_000, Some(SEED));
}

/// The same at many more cases, as many as `CACHE_SIM_CASES` sets, from a
/// random seed.
#[test]
#[ignore = "long-running; run with --ignored"]
fn every_cache_keeps_to_the_spec_at_length() {
    let cases = std::env::var("CACHE_SIM_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok());
    exercise(cases.unwrap_or(100_000), None);
}

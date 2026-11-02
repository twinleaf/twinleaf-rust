//! The model of section 2, the spans, boots and connections the checker
//! reads from the log of one case.

use super::hosts::{Event, Logged, Seen, Shown, Stats};
use super::laws::status_is;
use super::schedule::{Action, Case, Host, Pair, Way};
use super::{LONGEST_BLIP, QUIET, SOCKET_LOSS};
use crate::device::RecvError;
use crate::proxy::turmoil::{CHILD_REBOOT_STEP, TABLE_HASH};
use crate::tio::packet::LinkStatus;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

/// A span of virtual time from `from` up to `to`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Span {
    pub(super) from: u64,
    pub(super) to: u64,
}

impl Span {
    /// Whether it meets the closed span from `from` to `to`.
    fn meets(self, from: u64, to: u64) -> bool {
        self.from <= to && from < self.to
    }

    /// Whether it holds the moment `at`.
    pub(super) fn contains(self, at: u64) -> bool {
        self.from <= at && at < self.to
    }

    /// How long it lasts.
    pub(super) fn length(self) -> u64 {
        self.to.saturating_sub(self.from)
    }
}

/// `spans` sorted, with the ones that meet joined.
pub(super) fn merged(mut spans: Vec<Span>) -> Vec<Span> {
    spans.sort_by_key(|span| span.from);
    let mut out: Vec<Span> = Vec::new();
    for span in spans {
        match out.last_mut() {
            Some(last) if span.from <= last.to => last.to = last.to.max(span.to),
            _ => out.push(span),
        }
    }
    out
}

/// Whether any of `spans` meets the closed span from `from` to `to`.
pub(super) fn meets(spans: &[Span], from: u64, to: u64) -> bool {
    spans.iter().any(|span| span.meets(from, to))
}

/// How much of the span from `from` up to `to` the merged `spans` cover.
pub(super) fn measure(spans: &[Span], from: u64, to: u64) -> u64 {
    let cover = |span: &Span| span.to.min(to).saturating_sub(span.from.max(from));
    spans.iter().map(cover).sum()
}

/// A boot of the device, from the device's log and the driver's.
#[derive(Clone, Copy, Debug)]
pub(super) struct BootSeen {
    pub(super) index: u32,
    pub(super) session: u32,
    pub(super) start: u64,
    /// When the device was killed, or never.
    pub(super) end: u64,
}

/// A connection the device accepted.
#[derive(Clone, Copy, Debug)]
pub(super) struct DeviceConn {
    pub(super) accepted: u64,
    /// When the device logged its end.
    pub(super) logged: Option<u64>,
    /// When it ended, the earliest of its logged end, the device's next
    /// accept, and the device's death.
    pub(super) end: u64,
}

/// One connection an app opened, with what it saw on it.
#[derive(Debug)]
pub(super) struct AppConn<'a> {
    pub(super) from: u64,
    pub(super) closing: Option<u64>,
    /// When it ended, by a receiver's end or by the app's death.
    pub(super) to: u64,
    pub(super) why: Option<RecvError>,
    pub(super) events: Vec<(u64, &'a Seen)>,
}

impl AppConn<'_> {
    /// The last presence of the root, or of the hub child, before `at`.
    pub(super) fn presence_before(&self, at: u64, child: bool) -> Option<Shown> {
        let presences = self.events.iter().filter(|(when, _)| *when < at);
        presences
            .filter_map(|(_, seen)| match seen {
                Seen::Presence {
                    child: of,
                    presence,
                } if *of == child => Some(*presence),
                _ => None,
            })
            .next_back()
    }

    /// When the first event from `from` to `to` that `wanted` accepts came.
    pub(super) fn heard(&self, from: u64, to: u64, wanted: impl Fn(&Seen) -> bool) -> Option<u64> {
        let events = self
            .events
            .iter()
            .filter(|(at, _)| *at >= from && *at <= to);
        events
            .filter(|(_, seen)| wanted(seen))
            .map(|(at, _)| *at)
            .next()
    }
}

/// Who sent a `SensorDisconnected` an app heard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Source {
    /// The app's own pump, at its socket's loss.
    Pump,
    /// The proxy, at the device's loss.
    Proxy,
    /// Either, as the fault log cannot tell.
    Either,
}

/// One loss of the device by the proxy, as the apps heard it.
#[derive(Clone, Copy, Debug)]
pub(super) struct Episode {
    /// The earliest `SensorDisconnected` any app heard.
    pub(super) start: u64,
    /// The app that heard it.
    pub(super) first: u8,
    /// The first `SensorReconnected` or `FailedToReconnect` after it.
    pub(super) until: u64,
}

/// What an app heard that starts or ends a loss of the device.
#[derive(Clone, Copy, Debug)]
enum Heard {
    /// A link status at the root.
    Status(LinkStatus),
    /// `Alive` with a session at the root, on a connection opened at
    /// `opened`.
    Alive { opened: u64 },
}

/// An RPC the device was asked.
#[derive(Clone, Copy, Debug)]
pub(super) struct Asked<'a> {
    pub(super) at: u64,
    /// The index of the boot that was asked.
    pub(super) boot: u32,
    /// The number of the device's connection it came on.
    pub(super) connection: u32,
    /// Whether it was asked at the hub child.
    pub(super) child: bool,
    pub(super) name: &'a str,
    pub(super) args: &'a [u8],
    /// The hash it answered to `rpc.hash`.
    pub(super) hash: Option<u32>,
}

/// A kill of a running host.
#[derive(Clone, Copy, Debug)]
pub(super) struct Kill {
    pub(super) at: u64,
    /// Its place in the log, which orders the actions of one millisecond.
    pub(super) seq: usize,
    pub(super) host: Host,
    /// Whether a link of the host was cut then, as in a power cut.
    pub(super) cut_off: bool,
}

/// A restart of a host.
#[derive(Clone, Copy, Debug)]
pub(super) struct Bounce {
    pub(super) at: u64,
    /// Its place in the log. A kill the bounce makes shares it.
    pub(super) seq: usize,
    pub(super) host: Host,
}

/// A slow link.
#[derive(Clone, Copy, Debug)]
struct Slow {
    pair: Pair,
    span: Span,
    /// Its largest latency, in milliseconds.
    latency: u64,
}

/// The state the checker keeps, from the log of one case.
pub(super) struct Model<'a> {
    pub(super) case: &'a Case,
    /// For each direction of each link, when it was cut.
    cuts: BTreeMap<Way, Vec<Span>>,
    /// For each link, when turmoil held what it carried.
    holds: BTreeMap<Pair, Vec<Span>>,
    /// For each link, when it was failing.
    fails: BTreeMap<Pair, Vec<Span>>,
    slows: Vec<Slow>,
    /// When each host was down. A kill with no down time is 1 ms.
    downs: BTreeMap<Host, Vec<Span>>,
    pub(super) kills: Vec<Kill>,
    pub(super) bounces: Vec<Bounce>,
    /// When each app stayed away with its connection closed.
    away: BTreeMap<u8, Vec<Span>>,
    /// When the hub child was silent.
    pub(super) silences: Vec<Span>,
    pub(super) boots: Vec<BootSeen>,
    /// The connections the device accepted, in the order of their numbers.
    pub(super) device_conns: Vec<DeviceConn>,
    /// When a connection of the device was tainted.
    pub(super) taints: Vec<Span>,
    pub(super) asked: Vec<Asked<'a>>,
    /// What each app saw.
    pub(super) events: Vec<Vec<(u64, &'a Seen)>>,
    /// What the proxy host counted.
    pub(super) stats: Vec<(u64, &'a Stats)>,
    /// turmoil's own count of the proxy host's streams.
    pub(super) streams: Vec<(u64, usize)>,
}

impl<'a> Model<'a> {
    pub(super) fn of(case: &'a Case, log: &'a [Logged]) -> Model<'a> {
        let mut model = Model {
            case,
            cuts: BTreeMap::new(),
            holds: BTreeMap::new(),
            fails: BTreeMap::new(),
            slows: Vec::new(),
            downs: BTreeMap::new(),
            kills: Vec::new(),
            bounces: Vec::new(),
            away: BTreeMap::new(),
            silences: Vec::new(),
            boots: Vec::new(),
            device_conns: Vec::new(),
            taints: Vec::new(),
            asked: Vec::new(),
            events: vec![Vec::new(); usize::from(case.swarm.apps)],
            stats: Vec::new(),
            streams: Vec::new(),
        };
        let mut open = Opens::default();
        let mut logged_ends = BTreeMap::new();
        // The places in the log of each boot and each accept.
        let (mut booted, mut accepted) = (Vec::new(), Vec::new());
        for (seq, Logged { at, event }) in log.iter().enumerate() {
            let at = *at;
            match event {
                Event::Did(action) => model.did(at, seq, *action, &mut open),
                Event::Boot { index, session } => {
                    model.boots.push(BootSeen {
                        index: *index,
                        session: *session,
                        start: at,
                        end: u64::MAX,
                    });
                    booted.push(seq);
                }
                Event::Accepted => {
                    model.device_conns.push(DeviceConn {
                        accepted: at,
                        logged: None,
                        end: u64::MAX,
                    });
                    accepted.push(seq);
                }
                Event::Ended { connection } => {
                    logged_ends.insert(*connection, at);
                }
                Event::Asked {
                    index,
                    connection,
                    child,
                    name,
                    args,
                    hash,
                } => model.asked.push(Asked {
                    at,
                    boot: *index,
                    connection: *connection,
                    child: *child,
                    name,
                    args,
                    hash: *hash,
                }),
                Event::App(app, seen) => model.events[usize::from(*app)].push((at, seen)),
                Event::Proxy(stats) => model.stats.push((at, stats)),
                Event::Streams(streams) => model.streams.push((at, *streams)),
                Event::Began(_) | Event::WarmedUp | Event::Healed => {}
            }
        }
        open.close_all(&mut model);
        // A kill ends what the device logged before it in the log. A bounce
        // logs its kill and the next boot in one millisecond, the kill first.
        let device_kills: Vec<(usize, u64)> = model
            .kills
            .iter()
            .filter(|kill| kill.host == Host::Device)
            .map(|kill| (kill.seq, kill.at))
            .collect();
        let killed_after = |seq: usize| {
            device_kills
                .iter()
                .find(|&&(kill, _)| kill > seq)
                .map_or(u64::MAX, |&(_, at)| at)
        };
        for (boot, &seq) in model.boots.iter_mut().zip(&booted) {
            boot.end = killed_after(seq);
        }
        let accepts: Vec<u64> = model
            .device_conns
            .iter()
            .map(|conn| conn.accepted)
            .collect();
        for (connection, conn) in model.device_conns.iter_mut().enumerate() {
            conn.logged = logged_ends.get(&(connection as u32)).copied();
            let next = accepts.get(connection + 1).copied().unwrap_or(u64::MAX);
            let killed = killed_after(accepted[connection]);
            conn.end = conn.logged.unwrap_or(u64::MAX).min(next).min(killed);
        }
        model.taints = model.find_taints();
        model
    }

    /// Takes the driver's `action` at `at`, the log's entry `seq`, into the
    /// spans.
    fn did(&mut self, at: u64, seq: usize, action: Action, open: &mut Opens) {
        match action {
            Action::Hold(pair) => open.start(Opened::Hold(pair), at),
            Action::Release(pair) | Action::Repair(pair) => {
                open.end(self, Opened::Hold(pair), at);
                for way in pair.ways() {
                    open.end(self, Opened::Cut(way), at);
                }
            }
            Action::Cut(pair) => {
                for way in pair.ways() {
                    open.start(Opened::Cut(way), at);
                }
            }
            Action::CutOneWay(way) => open.start(Opened::Cut(way), at),
            Action::RepairOneWay(way) => open.end(self, Opened::Cut(way), at),
            Action::Latency(pair, ms) => {
                open.end(self, Opened::Slow(pair, 0), at);
                if ms != self.case.swarm.latency {
                    open.start(Opened::Slow(pair, ms), at);
                }
            }
            Action::FailRate(pair, 0) => open.end(self, Opened::Fail(pair), at),
            Action::FailRate(pair, _) => open.start(Opened::Fail(pair), at),
            Action::Crash(host) => {
                let cut_off = Self::is_cut_off(host, open);
                self.kills.push(Kill {
                    at,
                    seq,
                    host,
                    cut_off,
                });
                open.start(Opened::Down(host), at);
            }
            Action::Bounce(host) => {
                if !open.is_open(Opened::Down(host)) {
                    let cut_off = Self::is_cut_off(host, open);
                    self.kills.push(Kill {
                        at,
                        seq,
                        host,
                        cut_off,
                    });
                    self.downs.entry(host).or_default().push(Span {
                        from: at,
                        to: at + 1,
                    });
                }
                open.end(self, Opened::Down(host), at);
                self.bounces.push(Bounce { at, seq, host });
            }
            Action::Silence(true) => open.start(Opened::Silent, at),
            Action::Silence(false) => open.end(self, Opened::Silent, at),
            Action::Away(app, true) => open.start(Opened::Away(app), at),
            Action::Away(app, false) => open.end(self, Opened::Away(app), at),
        }
    }

    /// Whether a link of `host` is cut now, as in a power cut. The proxy's
    /// is the device link.
    fn is_cut_off(host: Host, open: &Opens) -> bool {
        let pair = match host {
            Host::Device | Host::Proxy => Pair::Device,
            Host::App(app) => Pair::App(app),
        };
        pair.ways()
            .iter()
            .any(|&way| open.is_open(Opened::Cut(way)))
    }

    fn close(&mut self, opened: Opened, span: Span) {
        match opened {
            Opened::Cut(way) => self.cuts.entry(way).or_default().push(span),
            Opened::Hold(pair) => self.holds.entry(pair).or_default().push(span),
            Opened::Fail(pair) => self.fails.entry(pair).or_default().push(span),
            Opened::Slow(pair, latency) => self.slows.push(Slow {
                pair,
                span,
                latency,
            }),
            Opened::Down(host) => self.downs.entry(host).or_default().push(span),
            Opened::Away(app) => self.away.entry(app).or_default().push(span),
            Opened::Silent => self.silences.push(span),
        }
    }

    /// The apps of the case.
    pub(super) fn apps(&self) -> Range<u8> {
        0..self.case.swarm.apps
    }

    /// The spans in which `way` was cut.
    pub(super) fn way_cuts(&self, way: Way) -> &[Span] {
        self.cuts.get(&way).map_or(&[], Vec::as_slice)
    }

    /// The spans in which `pair` is cut either way.
    pub(super) fn cut(&self, pair: Pair) -> Vec<Span> {
        let ways = pair.ways();
        merged(
            ways.iter()
                .flat_map(|&way| self.way_cuts(way))
                .copied()
                .collect(),
        )
    }

    /// The spans in which `pair` is failing.
    pub(super) fn failing(&self, pair: Pair) -> Vec<Span> {
        self.fails.get(&pair).cloned().unwrap_or_default()
    }

    /// The spans in which `host` is down.
    pub(super) fn down(&self, host: Host) -> Vec<Span> {
        self.downs.get(&host).cloned().unwrap_or_default()
    }

    /// The spans in which app `app` is down or away.
    pub(super) fn gone(&self, app: u8) -> Vec<Span> {
        let mut spans = self.down(Host::App(app));
        spans.extend(self.away.get(&app).into_iter().flatten());
        merged(spans)
    }

    /// The spans in which app `app`'s own link is unusable, the proxy aside.
    pub(super) fn own_link(&self, app: u8) -> Vec<Span> {
        merged([self.cut(Pair::App(app)), self.failing(Pair::App(app))].concat())
    }

    /// The spans in which the device is down or its link cut. Unlike a
    /// failing link, these let no connection through.
    pub(super) fn device_dead(&self) -> Vec<Span> {
        merged([self.cut(Pair::Device), self.down(Host::Device)].concat())
    }

    /// The spans in which the device cannot be reached over its link, the
    /// proxy aside.
    pub(super) fn device_out(&self) -> Vec<Span> {
        merged([self.device_dead(), self.failing(Pair::Device)].concat())
    }

    /// The spans in which `way` cannot carry a segment, as it is cut or
    /// failing or a host at either end is down.
    fn bad(&self, way: Way) -> Vec<Span> {
        let mut spans = self.way_cuts(way).to_vec();
        spans.extend(self.fails.get(&way.pair).into_iter().flatten());
        spans.extend(self.downs.get(&way.pair.host()).into_iter().flatten());
        spans.extend(self.downs.get(&Host::Proxy).into_iter().flatten());
        merged(spans)
    }

    /// The spans in which any of `pairs` is unusable either way.
    pub(super) fn unusable(&self, pairs: &[Pair]) -> Vec<Span> {
        let ways = pairs.iter().flat_map(|pair| pair.ways());
        merged(ways.flat_map(|way| self.bad(way)).collect())
    }

    /// The slack `d` of an obligation on `pairs` that runs from `from` for
    /// `length`. The holds it counts reach back by the longest blip.
    pub(super) fn slack(&self, pairs: &[Pair], from: u64, length: u64) -> u64 {
        let to = from.saturating_add(length).saturating_add(QUIET);
        let slowest = self
            .slows
            .iter()
            .filter(|slow| pairs.contains(&slow.pair) && slow.span.meets(from, to));
        let latency = slowest
            .map(|slow| slow.latency)
            .fold(self.case.swarm.latency, u64::max);
        let held = pairs
            .iter()
            .flat_map(|pair| self.holds.get(pair).into_iter().flatten().copied());
        let held = measure(
            &merged(held.collect()),
            from.saturating_sub(LONGEST_BLIP),
            to,
        );
        2 * latency + 200 + held
    }

    /// The slack of an obligation on app `app`'s path.
    pub(super) fn slack_of(&self, app: u8, from: u64, length: u64) -> u64 {
        self.slack(&[Pair::App(app), Pair::Device], from, length)
    }

    /// The spans in which a connection of the device was tainted. Each span
    /// in which the direction toward the device was cut or failing taints a
    /// connection live in it, from the span's start, or the accept, to the
    /// connection's end. A request the device read that the proxy sent after
    /// the span ended shows that nothing the proxy sent before it was lost,
    /// as the device reads in order, so the taint ends there. A request read
    /// later than `d` after the span ended was sent after it.
    fn find_taints(&self) -> Vec<Span> {
        let mut bad = self.way_cuts(Pair::Device.way(false)).to_vec();
        bad.extend(self.fails.get(&Pair::Device).into_iter().flatten());
        let mut taints = Vec::new();
        for (number, conn) in self.device_conns.iter().enumerate() {
            let live = bad
                .iter()
                .filter(|span| span.meets(conn.accepted, conn.end.saturating_sub(1)));
            for span in live {
                let from = span.from.max(conn.accepted);
                let sent_after = span
                    .to
                    .saturating_add(self.slack(&[Pair::Device], span.to, 0));
                let read = self
                    .asked
                    .iter()
                    .find(|asked| asked.connection == number as u32 && asked.at > sent_after);
                let to = read.map_or(conn.end, |asked| asked.at.min(conn.end));
                if from < to {
                    taints.push(Span { from, to });
                }
            }
        }
        merged(taints)
    }

    /// The spans in which a connection of app `app` was tainted, as the
    /// device's are. Each span in which the direction from the proxy to the
    /// app was cut or failing taints the app's connection live in it, from
    /// the span's start until the app loses its socket, which it hears as a
    /// `SensorDisconnected` later than `d` after the taint began. A segment
    /// lost toward the app leaves a hole the app never reads past, so it
    /// hears nothing more on that socket. What the app hears that the proxy
    /// sent after the span ended shows there is no hole, and ends the taint
    /// too. That is a batch with a sample the device sent after the span
    /// ended, at one a second from its boot, or a reply to a request the
    /// device read after it.
    pub(super) fn app_taints(&self, app: u8) -> Vec<Span> {
        let pair = Pair::App(app);
        let mut bad = self.way_cuts(pair.way(false)).to_vec();
        bad.extend(self.fails.get(&pair).into_iter().flatten());
        let sent_after = |seen: &Seen, after: u64| match seen {
            Seen::Batch(batch) => self
                .boot_of(batch.session)
                .is_some_and(|boot| boot.start + u64::from(batch.last) * 1_000 > after),
            Seen::Done(number, Ok(_)) => self
                .echo(&super::hosts::nonce(app, *number))
                .is_some_and(|asked| asked.at > after),
            _ => false,
        };
        let mut taints = Vec::new();
        for conn in self.conns_of(app) {
            let live = bad
                .iter()
                .filter(|span| span.meets(conn.from, conn.to.saturating_sub(1)));
            for span in live {
                let from = span.from.max(conn.from);
                let d = self.slack(&[pair], from, 0);
                let ended = conn.events.iter().find(|(at, seen)| {
                    (*at > from + d && status_is(seen, LinkStatus::SensorDisconnected))
                        || (*at >= from && sent_after(seen, span.to))
                });
                let to = ended.map_or(conn.to, |(at, _)| (*at).min(conn.to));
                if from < to {
                    taints.push(Span { from, to });
                }
            }
        }
        merged(taints)
    }

    /// The spans in which the device refuses the proxy's connects, as it
    /// still serves a connection the proxy has let go. The proxy surely lets
    /// a connection go when it goes down, and when the device link is cut
    /// toward the proxy, while the connection lives, for longer than the
    /// quiet time and `d`. The span runs from the kill, or from the later of
    /// the cut and the accept. The device ends the connection once it reads
    /// the proxy's close, once a write fails at the proxy's reset, or once a
    /// write has waited 20 s, so every such span ends.
    pub(super) fn device_refuses(&self) -> Vec<Span> {
        let kills: Vec<u64> = self
            .kills
            .iter()
            .filter(|kill| kill.host == Host::Proxy)
            .map(|kill| kill.at)
            .collect();
        let cuts = merged(self.way_cuts(Pair::Device.way(true)).to_vec());
        let refusing = |conn: &DeviceConn| {
            let killed = kills
                .iter()
                .copied()
                .filter(|&at| at >= conn.accepted && at < conn.end);
            let unheard = cuts.iter().filter_map(|cut| {
                let from = cut.from.max(conn.accepted);
                let quiet = QUIET + self.slack(&[Pair::Device], from, QUIET);
                (from < conn.end && cut.to.saturating_sub(from) > quiet).then_some(from)
            });
            let from = killed.chain(unheard).min()?;
            Some(Span { from, to: conn.end })
        };
        merged(self.device_conns.iter().filter_map(refusing).collect())
    }

    /// When the last tainted connection, or the last connection the device
    /// served after the proxy let it go, ended, or `None` while one lasts.
    pub(super) fn taint_ends(&self) -> Option<u64> {
        let refusing = self.device_refuses();
        let ends = self.taints.iter().chain(&refusing).map(|span| span.to);
        let last = ends.max().unwrap_or(0);
        (last != u64::MAX).then_some(last)
    }

    /// The boot in force at `at`, the last one started by then.
    pub(super) fn boot_at(&self, at: u64) -> Option<BootSeen> {
        self.boots
            .iter()
            .rev()
            .find(|boot| boot.start <= at)
            .copied()
    }

    /// The boot whose session is `session`.
    pub(super) fn boot_of(&self, session: u32) -> Option<BootSeen> {
        self.boots
            .iter()
            .find(|boot| boot.session == session)
            .copied()
    }

    /// How many times the hub child has rebooted by `at`, once at the end of
    /// each of its silences, as the fake device counts.
    fn child_reboots(&self, at: u64) -> u32 {
        let ended = |span: &&Span| span.to <= at;
        self.silences.iter().filter(ended).count() as u32
    }

    /// The hash the device answers at the hub child, or at the root, at
    /// `at`. The child's grows with each of its reboots.
    pub(super) fn hash_at(&self, child: bool, at: u64) -> Option<u32> {
        let boot = self.boot_at(at)?;
        let reboots = if child { self.child_reboots(at) } else { 0 };
        Some(TABLE_HASH + boot.index + CHILD_REBOOT_STEP * reboots)
    }

    /// The session the device has at the hub child, or at the root, at
    /// `at`.
    pub(super) fn session_at(&self, child: bool, at: u64) -> Option<u32> {
        let boot = self.boot_at(at)?;
        let reboots = if child { self.child_reboots(at) } else { 0 };
        Some(boot.session + CHILD_REBOOT_STEP * reboots)
    }

    /// The sessions the device has at the hub child, or at the root, in the
    /// order they start, each with its start. One starts at each boot, and
    /// at the child at the end of each silence.
    pub(super) fn sessions(&self, child: bool) -> Vec<(u64, u32)> {
        let mut starts: Vec<u64> = self.boots.iter().map(|boot| boot.start).collect();
        if child {
            starts.extend(self.silences.iter().map(|span| span.to));
        }
        starts.sort();
        starts.dedup();
        let session = |at: u64| Some((at, self.session_at(child, at)?));
        starts.into_iter().filter_map(session).collect()
    }

    /// The `test.echo` the device was first asked with `nonce`.
    pub(super) fn echo(&self, nonce: &[u8]) -> Option<&Asked<'a>> {
        self.asked
            .iter()
            .find(|asked| asked.name == "test.echo" && asked.args == nonce)
    }

    /// The connections app `app` opened. Each takes what its receivers
    /// yield until the app opens again or dies, and ends at the first end
    /// of one of them, or at the app's death.
    pub(super) fn conns_of(&self, app: u8) -> Vec<AppConn<'a>> {
        let deaths: Vec<u64> = self
            .down(Host::App(app))
            .iter()
            .map(|span| span.from)
            .collect();
        let mut conns: Vec<AppConn<'a>> = Vec::new();
        let mut taking = false;
        for &(at, seen) in &self.events[usize::from(app)] {
            if let Some(current) = conns.last_mut().filter(|_| taking) {
                if let Some(&died) = deaths
                    .iter()
                    .find(|&&death| death >= current.from && death < at)
                {
                    current.to = current.to.min(died);
                    taking = false;
                }
            }
            let current = conns.last_mut().filter(|_| taking);
            match (seen, current) {
                (Seen::Opening, _) => taking = false,
                (Seen::Opened, _) => {
                    taking = true;
                    conns.push(AppConn {
                        from: at,
                        closing: None,
                        to: u64::MAX,
                        why: None,
                        events: Vec::new(),
                    });
                }
                (Seen::Stopped(_, why), Some(current)) if current.to == u64::MAX => {
                    current.to = at;
                    current.why = Some(*why);
                }
                (Seen::Stopped(..), _) => {}
                (Seen::Closing, Some(current)) => current.closing = Some(at),
                (_, Some(current)) => current.events.push((at, seen)),
                (_, None) => {}
            }
        }
        if let Some(current) = conns.last_mut().filter(|_| taking) {
            let died = deaths
                .iter()
                .copied()
                .filter(|&death| death >= current.from)
                .min();
            if let Some(died) = died {
                current.to = current.to.min(died);
            }
        }
        conns
    }

    /// Who sent the `SensorDisconnected` app `app` heard at `at`, as
    /// section 1 says.
    pub(super) fn source(&self, app: u8, at: u64) -> Source {
        let d = self.slack_of(app, at.saturating_sub(SOCKET_LOSS), SOCKET_LOSS);
        let mut own = self.cut(Pair::App(app));
        own.extend(self.fails.get(&Pair::App(app)).into_iter().flatten());
        own.extend(self.holds.get(&Pair::App(app)).into_iter().flatten());
        own.extend(self.down(Host::Proxy));
        let pump = meets(&merged(own), at.saturating_sub(SOCKET_LOSS + d), at);
        let mut device = self.cut(Pair::Device);
        device.extend(self.fails.get(&Pair::Device).into_iter().flatten());
        device.extend(self.holds.get(&Pair::Device).into_iter().flatten());
        device.extend(self.down(Host::Device));
        // A tainted connection ends at a write that fails, which the proxy
        // hears within its last second.
        let ended = self.taints.iter().filter(|span| span.to != u64::MAX);
        device.extend(ended.map(|span| Span {
            from: span.to.saturating_sub(1_000),
            to: span.to + 1,
        }));
        let proxy = meets(&merged(device), at.saturating_sub(QUIET + d + 1_000), at);
        match (pump, proxy) {
            (true, false) => Source::Pump,
            (false, true) => Source::Proxy,
            _ => Source::Either,
        }
    }

    /// What every app heard that starts or ends a loss of the device, in
    /// order. These are the root link statuses, and each `Alive` with a
    /// session at the root, with when its connection opened.
    fn heard(&self) -> Vec<(u64, u8, Heard)> {
        let mut heard = Vec::new();
        for app in self.apps() {
            for conn in self.conns_of(app) {
                for &(at, seen) in &conn.events {
                    match seen {
                        Seen::Link { status, root: true } => {
                            heard.push((at, app, Heard::Status(*status)));
                        }
                        Seen::Presence {
                            child: false,
                            presence: Shown::Alive(Some(_)),
                        } => heard.push((at, app, Heard::Alive { opened: conn.from })),
                        _ => {}
                    }
                }
            }
        }
        heard.sort_by_key(|heard| heard.0);
        heard
    }

    /// The proxy's losses of the device, each started by a
    /// `SensorDisconnected` some app heard whose source `starts` accepts.
    /// One ends at the first `SensorReconnected` or `FailedToReconnect` any
    /// app hears, at the first `Alive` with a session an app hears on a
    /// connection it opened since the loss began once the device has
    /// accepted a connection since, and with the link, once no app holds it
    /// or the proxy goes down. An app that hears a loss within its slack of
    /// an episode's end, and heard none since the episode began, hears that
    /// episode's. Once the link has ended, at a kill of the proxy or a
    /// `FailedToReconnect` any app hears, no loss starts until the device
    /// accepts a connection again, as what an app hears until then comes
    /// from the link that ended, from a link that has not reached the device,
    /// or from the app's own socket.
    pub(super) fn episodes(&self, starts: impl Fn(Source) -> bool) -> Vec<Episode> {
        let mut episodes: Vec<Episode> = Vec::new();
        let mut open: Option<(u64, u8)> = None;
        let mut heard_since = BTreeSet::new();
        let proxy_kills = self
            .kills
            .iter()
            .filter(|kill| kill.host == Host::Proxy)
            .map(|kill| kill.at);
        let mut ends: Vec<u64> = self
            .stats
            .iter()
            .filter(|(_, stats)| stats.users == 0)
            .map(|(at, _)| *at)
            .collect();
        ends.extend(proxy_kills.clone());
        ends.sort();
        let ended = |from: u64, to: u64| ends.iter().copied().find(|&end| end > from && end <= to);
        let heard = self.heard();
        let mut links_end: Vec<u64> = proxy_kills.collect();
        links_end.extend(heard.iter().filter_map(|(at, _, heard)| {
            matches!(heard, Heard::Status(LinkStatus::FailedToReconnect)).then_some(*at)
        }));
        let accepted = |from: u64, to: u64| {
            self.device_conns
                .iter()
                .any(|conn| conn.accepted > from && conn.accepted <= to)
        };
        // Whether the link had ended by `at` with no connection to the
        // device since.
        let gone = |at: u64| {
            let last = links_end.iter().copied().filter(|&end| end <= at).max();
            last.is_some_and(|end| !accepted(end, at))
        };
        for (at, app, heard) in heard {
            if let Some(((start, first), end)) =
                open.and_then(|(start, first)| Some(((start, first), ended(start, at)?)))
            {
                episodes.push(Episode {
                    start,
                    first,
                    until: end,
                });
                open = None;
            }
            let late = |episode: &Episode| {
                at <= episode.until + self.slack_of(app, episode.start, at - episode.start)
            };
            match heard {
                Heard::Status(LinkStatus::SensorDisconnected) if open.is_some() => {
                    heard_since.insert(app);
                }
                Heard::Status(LinkStatus::SensorDisconnected)
                    if episodes.last().is_some_and(late) && !heard_since.contains(&app) =>
                {
                    heard_since.insert(app);
                }
                Heard::Status(LinkStatus::SensorDisconnected)
                    if !gone(at) && starts(self.source(app, at)) =>
                {
                    open = Some((at, app));
                    heard_since = BTreeSet::from([app]);
                }
                Heard::Status(LinkStatus::SensorReconnected | LinkStatus::FailedToReconnect) => {
                    if let Some((start, first)) = open.take() {
                        episodes.push(Episode {
                            start,
                            first,
                            until: at,
                        });
                    }
                }
                Heard::Alive { opened } => {
                    let back = |&(start, _): &(u64, u8)| opened > start && accepted(start, at);
                    if let Some((start, first)) = open.filter(back) {
                        episodes.push(Episode {
                            start,
                            first,
                            until: at,
                        });
                        open = None;
                    }
                }
                Heard::Status(_) => {}
            }
        }
        if let Some((start, first)) = open {
            episodes.push(Episode {
                start,
                first,
                until: ended(start, u64::MAX).unwrap_or(u64::MAX),
            });
        }
        episodes
    }

    /// The proxy's counts in force at `at`.
    pub(super) fn stats_at(&self, at: u64) -> Option<&'a Stats> {
        self.stats
            .iter()
            .rev()
            .find(|(when, _)| *when <= at)
            .map(|(_, stats)| *stats)
    }

    /// The ids of app `app`'s clients at the proxy at `at`.
    pub(super) fn clients_at(&self, app: u8, at: u64) -> Vec<u64> {
        self.stats_at(at)
            .map_or_else(Vec::new, |stats| stats.clients_of(app).collect())
    }

    /// Whether the proxy listed none of `clients` at some count after
    /// `from` and by `by`.
    pub(super) fn dropped(&self, clients: &[u64], from: u64, by: u64) -> bool {
        self.stats.iter().any(|(at, stats)| {
            *at > from
                && *at <= by
                && clients
                    .iter()
                    .all(|id| !stats.clients.iter().any(|client| client.0 == *id))
        })
    }

    /// When the device accepted a connection whose first packets toward the
    /// proxy were lost, on which no app heard the device for 10 s before
    /// `end`, if it did. A worker gives up such a connection over TCP, so a
    /// failure that finds one names it.
    pub(super) fn unheard(&self, end: u64) -> Option<u64> {
        let mut toward = self.way_cuts(Pair::Device.way(true)).to_vec();
        toward.extend(self.fails.get(&Pair::Device).into_iter().flatten());
        let heard = |from: u64, to: u64| {
            self.events.iter().flatten().any(|(at, seen)| {
                *at > from
                    && *at < to
                    && (status_is(seen, LinkStatus::SensorReconnected)
                        || matches!(seen, Seen::Batch(_)))
            })
        };
        let silent = |conn: &&DeviceConn| {
            let to = conn.end.min(end);
            meets(&toward, conn.accepted, conn.accepted + 1_000)
                && to >= conn.accepted + 10_000
                && !heard(conn.accepted, to)
        };
        self.device_conns
            .iter()
            .find(silent)
            .map(|conn| conn.accepted)
    }

    /// Whether some app heard what `wanted` accepts.
    pub(super) fn any_heard(&self, wanted: impl Fn(&Seen) -> bool) -> bool {
        self.events.iter().flatten().any(|(_, seen)| wanted(seen))
    }
}

/// The spans the model is still building, each by when it opened.
#[derive(Default)]
struct Opens(BTreeMap<Opened, u64>);

/// A span that opens and closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Opened {
    Cut(Way),
    Hold(Pair),
    Fail(Pair),
    /// A slow link and its latency. Its end matches any latency.
    Slow(Pair, u64),
    Down(Host),
    Away(u8),
    Silent,
}

impl Opens {
    fn start(&mut self, opened: Opened, at: u64) {
        self.0.entry(opened).or_insert(at);
    }

    fn is_open(&self, opened: Opened) -> bool {
        self.0.contains_key(&opened)
    }

    fn end(&mut self, model: &mut Model<'_>, opened: Opened, at: u64) {
        let key = match opened {
            Opened::Slow(pair, _) => self
                .0
                .keys()
                .copied()
                .find(|key| matches!(key, Opened::Slow(p, _) if *p == pair)),
            _ => self.0.contains_key(&opened).then_some(opened),
        };
        if let Some(key) = key {
            let from = self.0.remove(&key).expect("an open span");
            model.close(key, Span { from, to: at });
        }
    }

    fn close_all(&mut self, model: &mut Model<'_>) {
        for (key, from) in std::mem::take(&mut self.0) {
            model.close(key, Span { from, to: u64::MAX });
        }
    }
}

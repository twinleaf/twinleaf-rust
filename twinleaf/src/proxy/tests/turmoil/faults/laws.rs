//! The laws of section 3, judged over the model of one run. Each law is a
//! method that adds what broke it to a list. A timing law adds obligations,
//! each discharged at once by [`Obligation::discharge`], as section 2 says.

use super::driver::Run;
use super::hosts::{nonce, Batch, Bound, Changed, Seen, Shown, Stats};
use super::model::{measure, meets, merged, AppConn, Model, Source, Span};
use super::schedule::{plan, within_margin, Case, Host, Pair, Step};
use super::{
    BOUND, CALL_BOUND, CALL_TIMEOUT, CLOSE_TIMEOUT, IDLE_CLOSE, KEEPALIVE_QUIET, MOSTLY_UNUSABLE,
    QUIET, RECONNECT, RECONNECT_INTERVAL, RECONNECT_TRY, REOPEN_FOR, REOPEN_WAIT, SAMPLE_GAP,
    SOCKET_LOSS, WINDOW,
};
use crate::session::RecvError;
use crate::tio::packet::LinkStatus;
use std::collections::{BTreeMap, BTreeSet};

/// A law a case broke, with the time and the event it broke it at.
#[derive(Clone, Debug)]
pub(super) struct Broken {
    pub(super) law: &'static str,
    pub(super) at: u64,
    pub(super) what: String,
}

/// Adds the law `$law`, broken at `$at`, to `$out`, with what broke it as
/// the rest of the arguments format it.
macro_rules! broke {
    ($out:expr, $law:expr, $at:expr => $($what:tt)+) => {
        $out.push(Broken {
            law: $law,
            at: $at,
            what: format!($($what)+),
        })
    };
}

/// The laws whose source is an open question. Each is checked and counted,
/// and fails no case. The laws name these open questions by number.
/// 1. S6 (b) and (c). May the proxy send a request again to a device that
///    reconnected, or does a request reach the device at most once?
/// 2. S1. The docs promise order and loss only. The user chose "never
///    twice", which also needs the fake device of section 1.
/// 3. S9. Does the module doc of `proxy` on the `Stopped` status mean a
///    crashed proxy sends no `Stopped`, or only that one may be lost?
/// 4. L6 (c). Letting a client that hangs up mid open go at once is stated
///    only in a test's doc. Should the module doc say it?
/// 5. L9. No doc bounds when a call left by a lost device or socket fails.
///    Is "when the loss is noticed" the contract?
/// 6. A link that loses what it sends to the device, while the device still
///    talks, keeps the device `Alive` and lets every call time out until the
///    proxy's writes fill the window and drain out, up to a minute here.
///    Should the proxy notice within a documented time? The law
///    `one way gap` counts it.
/// 7. L13. Does the worker let go of a lost device's port at once? The
///    module doc of `proxy::worker` says it reopens a device whose port
///    closes, fails or goes quiet, and `CLOSE_TIMEOUT` in `tio/transport/mod.rs`
///    bounds how long a closed port takes to let go, but no doc says the
///    port the device was lost on is closed when it is lost. A device that
///    serves one connection at a time refuses the worker's next connect
///    until it is.
pub(super) const OPEN: [&str; 7] = ["S1", "S6", "S9", "L6c", "L9 bound", "one way gap", "L13"];

/// Whether `seen` is the root's `SensorDisconnected`.
pub(super) fn disconnected(seen: &Seen) -> bool {
    status_is(seen, LinkStatus::SensorDisconnected)
}

/// Whether `seen` is the root status `wanted`.
pub(super) fn status_is(seen: &Seen, wanted: LinkStatus) -> bool {
    matches!(seen, Seen::Link { status, root: true } if *status == wanted)
}

/// The root presence `seen` shows, if it is one.
pub(super) fn root_presence(seen: &Seen) -> Option<Shown> {
    match seen {
        Seen::Presence {
            child: false,
            presence,
        } => Some(*presence),
        _ => None,
    }
}

/// Whether `presence` is `Alive`.
fn alive(presence: Option<Shown>) -> bool {
    matches!(presence, Some(Shown::Alive(_)))
}

/// An event an app must see on its connection by a due time, which a law
/// adds at a fault.
struct Obligation<'o> {
    law: &'static str,
    /// The connection the app must see it on.
    conn: &'o AppConn<'o>,
    /// When the fault that added it came.
    from: u64,
    due: u64,
    /// The spans in which a precondition stops holding. One that meets the
    /// window from `from` to `due` drops the obligation.
    unless: Vec<&'o Vec<Span>>,
    met: Met<'o>,
    /// What breaks the law if the obligation is not met.
    what: String,
}

/// What meets an obligation.
enum Met<'o> {
    /// An event on the connection that the closure accepts. The connection's
    /// end by the due time drops the obligation.
    Seen(Box<dyn Fn(&Seen) -> bool + 'o>),
    /// What the closure finds in the connection's whole course, its end
    /// included.
    Course(Box<dyn Fn(&AppConn<'_>) -> bool + 'o>),
    /// What the closure finds in what the proxy counted. The connection's
    /// end by the due time drops the obligation.
    Counted(Box<dyn Fn() -> bool + 'o>),
}

impl Obligation<'_> {
    /// Drops the obligation if a precondition stopped holding, and adds its
    /// law to `out` if it was not met by its due time.
    fn discharge(self, out: &mut Vec<Broken>) {
        let (conn, from, due) = (self.conn, self.from, self.due);
        if self.unless.iter().any(|spans| meets(spans, from, due)) {
            return;
        }
        let met = match &self.met {
            Met::Seen(wanted) => conn.to <= due || conn.heard(from, due, wanted).is_some(),
            Met::Course(judged) => judged(conn),
            Met::Counted(found) => conn.to <= due || found(),
        };
        if !met {
            out.push(Broken {
                law: self.law,
                at: due,
                what: self.what,
            });
        }
    }
}

/// An app open and `Alive` on an untainted connection when the device link
/// was cut toward the proxy for longer than the quiet time and `d`. L6 (a)
/// and L10 each add an obligation for it.
struct AtDeviceCut<'j, 'a> {
    app: u8,
    conn: &'j AppConn<'a>,
    /// When the cut began.
    from: u64,
    due: u64,
}

/// Judges one run of a case against the laws.
pub(super) struct Judge<'a> {
    pub(super) model: Model<'a>,
    /// The connections each app opened.
    pub(super) app_conns: Vec<Vec<AppConn<'a>>>,
    pub(super) run: &'a Run,
    /// `H' + B`.
    due: u64,
}

impl<'a> Judge<'a> {
    pub(super) fn new(case: &'a Case, run: &'a Run) -> Judge<'a> {
        let model = Model::of(case, &run.log);
        let app_conns = model.apps().map(|app| model.conns_of(app)).collect();
        Judge {
            model,
            app_conns,
            run,
            due: run.settled + BOUND,
        }
    }

    /// Every law the run broke, in the order of their times.
    pub(super) fn judge(&self) -> Vec<Broken> {
        let mut out = Vec::new();
        self.samples(&mut out);
        self.presence(&mut out);
        self.replies(&mut out);
        self.endings(&mut out);
        self.recovery(&mut out);
        self.device_cut_seen(&mut out);
        self.app_cut_seen(&mut out);
        self.killed_app_dropped(&mut out);
        self.idle_link_closed(&mut out);
        self.silent_child_lost(&mut out);
        self.given_up(&mut out);
        self.ended_after_loss(&mut out);
        self.return_heard(&mut out);
        self.calls_end(&mut out);
        self.one_way_gaps(&mut out);
        self.losses_shared(&mut out);
        self.samples_follow(&mut out);
        self.apps_try_again(&mut out);
        self.blips(&mut out);
        self.lost_closed(&mut out);
        out.sort_by_key(|broken| broken.at);
        out
    }

    /// The connection app `app` has open at `at`.
    fn conn_at(&self, app: u8, at: u64) -> Option<&AppConn<'a>> {
        self.app_conns[usize::from(app)]
            .iter()
            .find(|conn| conn.from <= at && at < conn.to)
    }

    /// S1 to S4, at every batch.
    fn samples(&self, out: &mut Vec<Broken>) {
        for app in self.model.apps() {
            let mut highest = BTreeMap::<u32, u32>::new();
            for conn in &self.app_conns[usize::from(app)] {
                let mut previous: Option<&Batch> = None;
                for &(at, seen) in &conn.events {
                    let Seen::Batch(batch) = seen else { continue };
                    if !batch.steps {
                        broke!(out, "S1", at =>
                            "app {app} took a batch that does not step by one {batch:?}");
                    }
                    if let Some(&high) = highest.get(&batch.session) {
                        if batch.first <= high {
                            broke!(out, "S1", at =>
                                "app {app} took sample {} of session {} again after {high}",
                                batch.first, batch.session);
                        }
                    }
                    let high = highest.entry(batch.session).or_insert(batch.last);
                    *high = (*high).max(batch.last);
                    if let Some(odd) = &batch.odd {
                        broke!(out, "S4", at =>
                            "app {app} took the value {odd} under session {}", batch.session);
                    }
                    if let Some(before) = previous {
                        Self::continuity(app, at, before, batch, out);
                    }
                    previous = Some(batch);
                }
            }
        }
    }

    /// S2 and S3, between `before` and `batch`, two batches of one
    /// connection of app `app`.
    fn continuity(app: u8, at: u64, before: &Batch, batch: &Batch, out: &mut Vec<Broken>) {
        let change = batch
            .boundary
            .as_ref()
            .and_then(|bound| bound.change.as_ref());
        if before.session == batch.session {
            let next = before.last.wrapping_add(1);
            if batch.first == next && batch.boundary.is_some() {
                broke!(out, "S2", at =>
                    "app {app} took samples {} to {} with the boundary {:?}",
                    before.last, batch.first, batch.boundary);
            } else if batch.first > next {
                let gap = u64::from(batch.first - next);
                let exact = matches!(&batch.boundary, Some(Bound { change: None, lost: Some(lost) }) if *lost == gap);
                if !exact {
                    broke!(out, "S2", at =>
                        "app {app} lost {gap} samples after {} told as {:?}",
                        before.last, batch.boundary);
                }
            }
        } else {
            let wanted = Changed::Session {
                old: before.session,
                new: batch.session,
            };
            if change != Some(&wanted) {
                broke!(out, "S3", at =>
                    "app {app} went from session {} to {} with {:?}",
                    before.session, batch.session, batch.boundary);
            }
        }
        if change == Some(&Changed::Initial) {
            broke!(out, "S3", at => "app {app} took a batch told as Initial after its first");
        }
    }

    /// S5, at every presence of the root and of the hub child. Order is the
    /// order in which the device's sessions at that route started.
    fn presence(&self, out: &mut Vec<Broken>) {
        for (app, child) in self
            .model
            .apps()
            .flat_map(|app| [(app, false), (app, true)])
        {
            let route = if child { "the child" } else { "the root" };
            let sessions = self.model.sessions(child);
            let mut heard: Option<usize> = None;
            for conn in &self.app_conns[usize::from(app)] {
                for &(at, seen) in &conn.events {
                    let Seen::Presence {
                        child: of,
                        presence: Shown::Alive(Some(session)),
                    } = *seen
                    else {
                        continue;
                    };
                    if of != child {
                        continue;
                    }
                    let started = sessions
                        .iter()
                        .rposition(|&(start, of)| of == session && start <= at);
                    let Some(index) = started else {
                        broke!(out, "S5", at =>
                            "app {app} heard {route} Alive in session {session}, which it had not started");
                        continue;
                    };
                    if let Some(heard) = heard.filter(|&heard| index < heard) {
                        broke!(out, "S5", at =>
                            "app {app} heard {route} Alive in session {session} after session {}",
                            sessions[heard].1);
                    }
                    heard = Some(heard.map_or(index, |heard| heard.max(index)));
                    let current = sessions
                        .iter()
                        .rposition(|&(start, _)| start <= at)
                        .expect("a session");
                    let (start, now) = sessions[current];
                    let d = self.model.slack_of(app, at.saturating_sub(QUIET), QUIET);
                    if index != current && start + QUIET + d < at {
                        broke!(out, "S5", at =>
                            "app {app} heard {route} Alive in session {session} after session {now} started at {start}");
                    }
                }
            }
        }
    }

    /// S6, at every reply and every request the device logged.
    fn replies(&self, out: &mut Vec<Broken>) {
        let mut echoes = BTreeMap::<&[u8], u64>::new();
        for asked in &self.model.asked {
            if asked.name == "test.echo" && echoes.insert(asked.args, asked.at).is_some() {
                broke!(out, "S6", asked.at =>
                    "the device was asked the nonce {:?} twice", asked.args);
            }
        }
        for app in self.model.apps() {
            let mut sent = BTreeMap::new();
            for &(at, seen) in &self.model.events[usize::from(app)] {
                match seen {
                    Seen::Sent(number) => {
                        sent.insert(*number, at);
                    }
                    Seen::Done(number, Ok((session, from, echoed))) => {
                        if (*from, *echoed) != (u32::from(app), *number) {
                            broke!(out, "S6", at =>
                                "app {app} call {number} took the reply to app {from} call {echoed}");
                            continue;
                        }
                        let boot = self.model.boot_of(*session);
                        let asked = self.model.echo(&nonce(app, *number));
                        let since = sent.get(number).copied().unwrap_or(0);
                        let fits = matches!((boot, asked), (Some(boot), Some(asked)) if asked.boot == boot.index && asked.at >= since);
                        if !fits {
                            broke!(out, "S6", at =>
                                "app {app} call {number} took a reply in session {session} that boot did not give");
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// S8 and S9, at every end and every status.
    fn endings(&self, out: &mut Vec<Broken>) {
        let lenient = self.model.episodes(|_| true);
        let reconnect = self.model.case.swarm.reconnect_timeout;
        let device_out = self.model.device_out();
        let refusing = self.model.device_refuses();
        for app in self.model.apps() {
            let path = self.model.unusable(&[Pair::App(app), Pair::Device]);
            let path = merged([path, refusing.clone()].concat());
            for conn in &self.app_conns[usize::from(app)] {
                let end = conn.to;
                match conn.why {
                    Some(RecvError::Disconnected) => {
                        let gave_up = conn.heard(conn.from, end.saturating_add(1_000), |seen| {
                            status_is(seen, LinkStatus::FailedToReconnect)
                        });
                        if gave_up.is_none() {
                            let alive = conn.events.iter().filter(|(at, seen)| {
                                *at <= end && matches!(root_presence(seen), Some(Shown::Alive(_)))
                            });
                            let since = alive.map(|(at, _)| *at).next_back().unwrap_or(conn.from);
                            let lost = conn.events.iter().find(|(at, seen)| {
                                *at >= since
                                    && *at <= end
                                    && disconnected(seen)
                                    && self.model.source(app, *at) != Source::Proxy
                            });
                            let d = self.model.slack_of(
                                app,
                                end.saturating_sub(REOPEN_FOR),
                                REOPEN_FOR,
                            );
                            let late = lost.is_some_and(|(at, _)| end + d >= at + REOPEN_FOR);
                            let unusable = measure(&path, end.saturating_sub(REOPEN_FOR), end);
                            if !late || unusable < MOSTLY_UNUSABLE {
                                broke!(out, "S8", end =>
                                    "app {app}'s connection ended with Disconnected, after a socket loss at {:?}, its path unusable for {unusable} ms of the 30 s before",
                                    lost.map(|lost| lost.0));
                            }
                        }
                    }
                    Some(RecvError::Closed) if conn.closing.is_none_or(|closing| closing > end) => {
                        broke!(out, "S8", end =>
                            "app {app}'s connection ended with Closed, though the app did not close it");
                    }
                    Some(RecvError::Stopped) => {
                        broke!(out, "S9", end => "app {app}'s connection ended with Stopped");
                    }
                    _ => {}
                }
                for &(at, seen) in &conn.events {
                    if status_is(seen, LinkStatus::Stopped) {
                        broke!(out, "S9", at => "app {app} heard Stopped");
                    }
                    if !status_is(seen, LinkStatus::FailedToReconnect) {
                        continue;
                    }
                    if !reconnect {
                        broke!(out, "S8", at =>
                            "app {app} heard FailedToReconnect with no reconnect timeout");
                        continue;
                    }
                    // The loss the apps heard first stands for the proxy's,
                    // unless the app that heard it may have heard its own
                    // socket's loss or heard the proxy's late. Then the
                    // outage of the device link stands for it too.
                    let episode = lenient
                        .iter()
                        .find(|episode| episode.start <= at && at <= episode.until);
                    let outage = device_out.iter().rfind(|span| span.from <= at);
                    let outage = outage.map_or(0, |span| span.from.saturating_sub(RECONNECT_TRY));
                    let lost = match episode {
                        Some(episode)
                            if self.model.source(episode.first, episode.start) == Source::Proxy =>
                        {
                            episode.start
                        }
                        Some(episode) => episode.start.min(outage),
                        None => outage,
                    };
                    let d = self.model.slack_of(app, lost, RECONNECT);
                    if at + d < lost + RECONNECT {
                        broke!(out, "S8", at =>
                            "app {app} heard FailedToReconnect {} ms after the loss at {lost}",
                            at - lost);
                    }
                }
            }
        }
    }

    /// L1 to L5, after the heal.
    fn recovery(&self, out: &mut Vec<Broken>) {
        let Some(boot) = self.model.boot_at(self.due) else {
            return broke!(out, "L1", self.due => "no boot of the device");
        };
        for app in self.model.apps() {
            self.app_back(app, boot.session, out);
        }
        self.leaks(out);
    }

    /// L1 to L4 for app `app`, with `session` the current boot's.
    fn app_back(&self, app: u8, session: u32, out: &mut Vec<Broken>) {
        let (due, end) = (self.due, self.run.end);
        let Some(conn) = self.conn_at(app, due) else {
            return broke!(out, "L1", due => "app {app} has no open connection");
        };
        let presence = conn.presence_before(due + 1, false);
        if presence != Some(Shown::Alive(Some(session))) {
            broke!(out, "L1", due =>
                "app {app}'s presence is {presence:?}, not Alive in session {session}");
        }
        let child = self.model.session_at(true, due);
        if let Some(child) = child.filter(|_| self.model.case.swarm.child) {
            let presence = conn.presence_before(due + 1, true);
            if presence != Some(Shown::Alive(Some(child))) {
                broke!(out, "L1", due =>
                    "app {app}'s presence of the child is {presence:?}, not Alive in session {child}");
            }
        }
        let d = self.model.slack_of(app, due, WINDOW);
        if !self.model.case.swarm.withheld {
            let batches: Vec<(u64, u32)> = self.app_conns[usize::from(app)]
                .iter()
                .flat_map(|conn| conn.events.iter())
                .filter_map(|(at, seen)| match seen {
                    Seen::Batch(batch) => Some((*at, batch.session)),
                    _ => None,
                })
                .collect();
            let before = batches
                .iter()
                .rfind(|(at, of)| *at <= due && *of == session);
            match before {
                None => broke!(out, "L2", due => "app {app} has no batch of session {session}"),
                Some(&(last, _)) => {
                    let mut times: Vec<u64> = batches
                        .iter()
                        .map(|(at, _)| *at)
                        .filter(|&at| at >= last && at <= end)
                        .collect();
                    times.push(end);
                    let gap = times
                        .windows(2)
                        .find(|pair| pair[1] - pair[0] > SAMPLE_GAP + d);
                    if let Some(pair) = gap {
                        broke!(out, "L2", pair[1] =>
                            "app {app} took no batch from {} to {}", pair[0], pair[1]);
                    }
                }
            }
        }
        let events = &self.model.events[usize::from(app)];
        let done = |number: u32| {
            events.iter().find_map(|(at, seen)| match seen {
                Seen::Done(of, outcome) if *of == number => Some((*at, outcome)),
                _ => None,
            })
        };
        let sent = events.iter().filter_map(|(at, seen)| match seen {
            Seen::Sent(number) if *at >= due => Some((*at, *number)),
            _ => None,
        });
        for (index, (at, number)) in sent.enumerate() {
            match done(number) {
                Some((_, Ok((of, from, echoed))))
                    if *of == session && (*from, *echoed) == (u32::from(app), number) => {}
                // A call sent within 2 s and `d` of the end may still wait
                // for its reply.
                None if index > 0 && at + 2_000 + d > end => {}
                other => broke!(out, "L3", at => "app {app}'s call {number} ended {other:?}"),
            }
        }
        let asked = events.iter().find_map(|(at, seen)| match seen {
            Seen::NameSent(second) if *at >= due => Some(*second),
            _ => None,
        });
        let named = asked.and_then(|second| {
            events.iter().find_map(|(_, seen)| match seen {
                Seen::Named(of, named) if *of == second => Some(named.clone()),
                _ => None,
            })
        });
        if named != Some(Ok("FAKE".to_string())) {
            broke!(out, "L3", due => "app {app}'s dev.name gave {named:?}");
        }
        let routes = match self.model.case.swarm.child {
            true => vec![false, true],
            false => vec![false],
        };
        for child in routes {
            let mut table = conn.events.iter().filter(|(at, _)| *at <= due).filter_map(
                |(_, seen)| match seen {
                    Seen::Table { child: of, table } if *of == child => Some(table),
                    _ => None,
                },
            );
            let table = table.next_back();
            let hash = self.model.hash_at(child, due);
            let session = self.model.session_at(child, due).unwrap_or(session);
            let answered = self
                .model
                .asked
                .iter()
                .rfind(|asked| asked.at <= due && asked.child == child && asked.hash.is_some())
                .and_then(|asked| asked.hash);
            let fits = matches!(table, Some(Ok((Some(got), Some(of)))) if Some(*got) == hash && Some(*got) == answered && *of == session);
            if !fits {
                let route = if child { "child" } else { "root" };
                broke!(out, "L4", due =>
                    "app {app}'s table at the {route} is {table:?}, the device's is {hash:?} as last answered {answered:?} in session {session}");
            }
        }
    }

    /// L5, after the heal.
    fn leaks(&self, out: &mut Vec<Broken>) {
        let (due, end) = (self.due, self.run.end);
        let apps = usize::from(self.model.case.swarm.apps);
        let warm_tasks = self
            .model
            .stats_at(self.run.warmed)
            .map(|stats| stats.tasks);
        let mut checked: Vec<(u64, &Stats)> = self
            .model
            .stats
            .iter()
            .filter(|(at, _)| *at > due && *at <= end)
            .copied()
            .collect();
        if let Some(stats) = self.model.stats_at(due) {
            checked.insert(0, (due, stats));
        }
        for (at, stats) in checked {
            let fits = stats.streams == apps + 1
                && stats.clients.len() == apps
                && stats.users == apps
                && Some(stats.tasks) == warm_tasks;
            if !fits {
                broke!(out, "L5", at =>
                    "the proxy counts {stats:?} with {apps} apps open, and {warm_tasks:?} tasks after the warm up");
                break;
            }
        }
        let streams_at = |at: u64| {
            self.model
                .streams
                .iter()
                .rev()
                .find(|(when, _)| *when <= at)
                .map(|(_, streams)| *streams)
        };
        if streams_at(end) > streams_at(due) {
            broke!(out, "L5", end =>
                "turmoil's count of the proxy's streams grew from {:?} to {:?}",
                streams_at(due), streams_at(end));
        }
    }

    /// Each app open and `Alive` on an untainted connection at a cut of the
    /// device link toward the proxy, or the device's power cut, which cuts
    /// it too, that lasts past the quiet time and `d`.
    fn device_cuts(&self) -> Vec<AtDeviceCut<'_, 'a>> {
        let mut found = Vec::new();
        for cut in self.model.way_cuts(Pair::Device.way(true)) {
            for app in self.model.apps() {
                let d = self.model.slack_of(app, cut.from, QUIET);
                if cut.length() <= QUIET + d {
                    continue;
                }
                let Some(conn) = self.conn_at(app, cut.from) else {
                    continue;
                };
                let ready = alive(conn.presence_before(cut.from, false));
                let tainted = self
                    .model
                    .taints
                    .iter()
                    .any(|span| span.contains(cut.from.saturating_sub(1)));
                if ready && !tainted {
                    let due = cut.from + QUIET + d;
                    found.push(AtDeviceCut {
                        app,
                        conn,
                        from: cut.from,
                        due,
                    });
                }
            }
        }
        found
    }

    /// L6 (a), during the chaos.
    fn device_cut_seen(&self, out: &mut Vec<Broken>) {
        let proxy_down = self.model.down(Host::Proxy);
        for cut in self.device_cuts() {
            let own = self.model.own_link(cut.app);
            let gone = self.model.gone(cut.app);
            let tainted = self.model.app_taints(cut.app);
            Obligation {
                law: "L6a",
                conn: cut.conn,
                from: cut.from,
                due: cut.due,
                unless: vec![&own, &proxy_down, &gone, &tainted],
                met: Met::Seen(Box::new(|seen| root_presence(seen) == Some(Shown::Lost))),
                what: format!(
                    "app {} did not see the device Lost after the cut at {}",
                    cut.app, cut.from
                ),
            }
            .discharge(out);
        }
    }

    /// L6 (b), during the chaos. Its two halves share their preconditions.
    fn app_cut_seen(&self, out: &mut Vec<Broken>) {
        let proxy_down = self.model.down(Host::Proxy);
        for app in self.model.apps() {
            let failing = self.model.failing(Pair::App(app));
            let gone = self.model.gone(app);
            for cut in self.model.cut(Pair::App(app)) {
                let d = self.model.slack_of(app, cut.from, SOCKET_LOSS);
                if cut.length() <= SOCKET_LOSS + d {
                    continue;
                }
                let due = cut.from + SOCKET_LOSS + d;
                let Some(conn) = self.conn_at(app, cut.from) else {
                    continue;
                };
                if !alive(conn.presence_before(cut.from, false))
                    || meets(&failing, cut.from.saturating_sub(SOCKET_LOSS), due)
                    || meets(&proxy_down, cut.from, due)
                    || meets(&gone, cut.from, due)
                {
                    continue;
                }
                let from = cut.from;
                Obligation {
                    law: "L6b",
                    conn,
                    from,
                    due,
                    unless: Vec::new(),
                    met: Met::Course(Box::new(move |conn| {
                        let heard = conn
                            .events
                            .iter()
                            .any(|(at, seen)| *at > from && *at <= due && disconnected(seen));
                        heard || (conn.to > from && conn.to <= due)
                    })),
                    what: format!(
                        "app {app} did not lose its socket after its link was cut at {from}"
                    ),
                }
                .discharge(out);
                let clients = self.model.clients_at(app, from);
                let mut dropped_by = from + KEEPALIVE_QUIET + d + 100;
                // A cut toward the proxy that begins after the cut from it
                // stops the app's close from reaching the proxy, which then
                // lets the app go only once it has heard nothing for its
                // quiet time.
                let toward = self.model.way_cuts(Pair::App(app).way(true));
                if let Some(later) = toward
                    .iter()
                    .find(|span| span.from > from && span.from <= dropped_by)
                {
                    dropped_by = later.from + KEEPALIVE_QUIET + d + 100;
                }
                if !clients.is_empty() && !self.model.dropped(&clients, from, dropped_by) {
                    broke!(out, "L6b", dropped_by =>
                        "the proxy kept app {app}'s clients {clients:?} after its link was cut at {from}");
                }
            }
        }
    }

    /// L6 (c), during the chaos.
    fn killed_app_dropped(&self, out: &mut Vec<Broken>) {
        let proxy_down = self.model.down(Host::Proxy);
        for kill in &self.model.kills {
            let (Host::App(app), at) = (kill.host, kill.at) else {
                continue;
            };
            let d = self.model.slack_of(app, at, 1_000);
            let by = at + 1_000 + d + 100;
            let clients = self.model.clients_at(app, at);
            if kill.cut_off || clients.is_empty() || meets(&proxy_down, at, by) {
                continue;
            }
            if !self.model.dropped(&clients, at, by) {
                broke!(out, "L6c", by =>
                    "the proxy kept app {app}'s clients {clients:?} after it was killed at {at}");
            }
        }
    }

    /// L6 (d), during the chaos.
    fn idle_link_closed(&self, out: &mut Vec<Broken>) {
        let device_link = self.model.unusable(&[Pair::Device]);
        let mut users = None;
        for &(at, stats) in &self.model.stats {
            let was = users.replace(stats.users);
            if stats.users != 0 || was.is_none_or(|was| was == 0) {
                continue;
            }
            let d = self.model.slack(&[Pair::Device], at, IDLE_CLOSE);
            let due = at + IDLE_CLOSE + d + 100;
            let held = self
                .model
                .stats
                .iter()
                .any(|(when, stats)| *when >= at && *when <= due && stats.users != 0);
            if held || meets(&device_link, at, due) || meets(&self.model.taints, at, due) {
                continue;
            }
            let Some(conn) = self
                .model
                .device_conns
                .iter()
                .rev()
                .find(|conn| conn.accepted <= at && conn.end > at)
            else {
                continue;
            };
            if !conn.logged.is_some_and(|ended| ended <= due) {
                broke!(out, "L6d", due =>
                    "the device kept the proxy's connection after no app held it from {at}");
            }
        }
    }

    /// L6 (e), during the chaos.
    fn silent_child_lost(&self, out: &mut Vec<Broken>) {
        if !self.model.case.swarm.child {
            return;
        }
        for silence in &self.model.silences {
            for app in self.model.apps() {
                let d = self.model.slack_of(app, silence.from, QUIET);
                if silence.length() <= QUIET + d {
                    continue;
                }
                let Some(conn) = self.conn_at(app, silence.from) else {
                    continue;
                };
                if !alive(conn.presence_before(silence.from, true)) {
                    continue;
                }
                let path = self.model.unusable(&[Pair::App(app), Pair::Device]);
                let gone = self.model.gone(app);
                let tainted = self.model.app_taints(app);
                let child_lost = Seen::Presence {
                    child: true,
                    presence: Shown::Lost,
                };
                Obligation {
                    law: "L6e",
                    conn,
                    from: silence.from,
                    due: silence.from + QUIET + d,
                    unless: vec![&path, &self.model.taints, &gone, &tainted],
                    met: Met::Seen(Box::new(move |seen| *seen == child_lost)),
                    what: format!(
                        "app {app} did not see the child Lost after it went silent at {}",
                        silence.from
                    ),
                }
                .discharge(out);
            }
        }
    }

    /// L7 (a), during the chaos.
    fn given_up(&self, out: &mut Vec<Broken>) {
        if !self.model.case.swarm.reconnect_timeout {
            return;
        }
        let proxy_down = self.model.down(Host::Proxy);
        let dead = self.model.device_dead();
        let failing = self.model.failing(Pair::Device);
        for episode in self.model.episodes(|source| source == Source::Proxy) {
            let lost = episode.start;
            for app in self.model.apps() {
                let d = self.model.slack_of(app, lost, RECONNECT);
                let due = lost + RECONNECT + d;
                if measure(&dead, lost, due) < due - lost {
                    continue;
                }
                let Some(conn) = self.conn_at(app, lost) else {
                    continue;
                };
                let own = self.model.own_link(app);
                let gone = self.model.gone(app);
                let gave_up = conn.heard(lost, due, |seen| {
                    status_is(seen, LinkStatus::FailedToReconnect)
                });
                Obligation {
                    law: "L7a",
                    conn,
                    from: lost,
                    due,
                    unless: vec![&failing, &proxy_down, &own, &gone],
                    met: Met::Course(Box::new(move |conn| {
                        gave_up.is_some()
                            && conn.to <= due
                            && conn.why == Some(RecvError::Disconnected)
                    })),
                    what: format!("app {app} heard FailedToReconnect at {gave_up:?} and its connection ended at {} with {:?}, after the loss at {lost}", conn.to, conn.why),
                }
                .discharge(out);
            }
        }
    }

    /// L7 (b), during the chaos.
    fn ended_after_loss(&self, out: &mut Vec<Broken>) {
        let proxy_down = self.model.down(Host::Proxy);
        for app in self.model.apps() {
            let failing = self.model.failing(Pair::App(app));
            let gone = self.model.gone(app);
            let mut own = self.model.cut(Pair::App(app));
            own.extend(proxy_down.iter().copied());
            for span in merged(own) {
                let Some(conn) = self.conn_at(app, span.from) else {
                    continue;
                };
                let d = self
                    .model
                    .slack_of(app, span.from, SOCKET_LOSS + REOPEN_FOR);
                let mut losses = conn.events.iter().filter(|(at, seen)| {
                    *at >= span.from && *at <= span.from + SOCKET_LOSS + d && disconnected(seen)
                });
                let Some(&(lost, _)) = losses.next_back() else {
                    continue;
                };
                let due = lost + REOPEN_FOR + d;
                if span.to < due {
                    continue;
                }
                Obligation {
                    law: "L7b",
                    conn,
                    from: span.from,
                    due,
                    unless: vec![&failing, &gone],
                    met: Met::Course(Box::new(move |conn| {
                        conn.to <= due && conn.why == Some(RecvError::Disconnected)
                    })),
                    what: format!(
                        "app {app}'s connection lasted to {} with {:?} after its socket loss at {lost}",
                        conn.to, conn.why
                    ),
                }
                .discharge(out);
            }
        }
    }

    /// L8, during the chaos and after the heal.
    fn return_heard(&self, out: &mut Vec<Broken>) {
        // The device is out of the proxy's reach while its link is, and
        // while it refuses the proxy for a connection the proxy let go.
        let device_out = merged([self.model.device_out(), self.model.device_refuses()].concat());
        let proxy_down = self.model.down(Host::Proxy);
        let failing = self.model.failing(Pair::Device);
        // The device listens again at once after a plain kill, so the proxy
        // reaches it at its next attempt, and otherwise after a connect
        // timeout and the interval after it.
        let wait_after = |span: &Span| {
            let mut plain_kills = self
                .model
                .kills
                .iter()
                .filter(|kill| kill.host == Host::Device && !kill.cut_off);
            match plain_kills.any(|kill| kill.at == span.from) {
                true => RECONNECT_INTERVAL,
                false => RECONNECT_TRY,
            }
        };
        for app in self.model.apps() {
            let own = self.model.own_link(app);
            let gone = self.model.gone(app);
            let tainted = self.model.app_taints(app);
            for conn in &self.app_conns[usize::from(app)] {
                for &(at, seen) in &conn.events {
                    if !disconnected(seen) || self.model.source(app, at) != Source::Proxy {
                        continue;
                    }
                    let mut back = at;
                    let mut wait = RECONNECT_TRY;
                    if let Some(span) = device_out.iter().rfind(|span| span.from <= at) {
                        back = back.max(span.to);
                        wait = wait_after(span);
                    }
                    // The device serves one connection at a time, so it
                    // takes the proxy's next only once the one the proxy
                    // lost has ended, and refuses each attempt before. The
                    // one lost is the last the device accepted more than
                    // `d` before the loss was heard, as one accepted later
                    // may be the proxy's next.
                    let heard_after = self.model.slack_of(app, at, 0);
                    let lost = self
                        .model
                        .device_conns
                        .iter()
                        .rfind(|conn| conn.accepted + heard_after <= at);
                    if let Some(end) = lost.map(|conn| conn.end).filter(|&end| end > back) {
                        back = end;
                        wait = RECONNECT_INTERVAL;
                    }
                    let mut due = back.saturating_add(
                        wait + self.model.slack_of(app, at, back.saturating_sub(at) + wait),
                    );
                    while let Some(span) = device_out
                        .iter()
                        .find(|span| span.from > back && span.from <= due)
                    {
                        back = span.to;
                        wait = wait_after(span);
                        let d = self.model.slack_of(app, at, back.saturating_sub(at) + wait);
                        due = back.saturating_add(wait + d);
                    }
                    if at < self.due {
                        due = due.min(self.due);
                    }
                    if meets(&failing, at.saturating_sub(6_000), due) || due > self.run.end {
                        continue;
                    }
                    Obligation {
                        law: "L8",
                        conn,
                        from: at,
                        due,
                        unless: vec![&own, &proxy_down, &gone, &tainted],
                        met: Met::Seen(Box::new(|seen| {
                            status_is(seen, LinkStatus::SensorReconnected)
                                || status_is(seen, LinkStatus::FailedToReconnect)
                        })),
                        what: format!("app {app} heard no SensorReconnected after the SensorDisconnected at {at}"),
                    }
                    .discharge(out);
                }
            }
        }
    }

    /// L9, during the chaos and after the heal.
    fn calls_end(&self, out: &mut Vec<Broken>) {
        for app in self.model.apps() {
            let events = &self.model.events[usize::from(app)];
            let deaths = self.model.down(Host::App(app));
            for &(at, seen) in events {
                let Seen::Sent(number) = seen else { continue };
                let d = self.model.slack_of(app, at, CALL_TIMEOUT);
                let due = at + CALL_TIMEOUT + d;
                let done = events.iter().find_map(|(when, seen)| match seen {
                    Seen::Done(of, _) if of == number => Some(*when),
                    _ => None,
                });
                match done {
                    Some(done) if done <= due => {
                        if done - at > CALL_BOUND && !meets(&self.model.taints, at, done) {
                            broke!(out, "L9 bound", done =>
                                "app {app}'s call {number} took {} ms", done - at);
                        }
                    }
                    _ if due > self.run.end || meets(&deaths, at, due) => {}
                    _ => {
                        broke!(out, "L9", due =>
                            "app {app}'s call {number} sent at {at} ended at {done:?}")
                    }
                }
            }
        }
    }

    /// The one way gap of section 5, which no law bounds, open question 6.
    /// A connection whose writes to the device are lost while some app still
    /// takes its samples, 10 s after the loss began, is counted.
    fn one_way_gaps(&self, out: &mut Vec<Broken>) {
        for taint in &self.model.taints {
            let late = taint.from + 10_000;
            let sampled =
                self.model.events.iter().flatten().find(|(at, seen)| {
                    *at > late && *at < taint.to && matches!(seen, Seen::Batch(_))
                });
            if let Some(&(at, _)) = sampled {
                broke!(out, "one way gap", at =>
                    "the device was still heard {} ms after its connection began to lose what it was sent",
                    at - taint.from);
            }
        }
    }

    /// L10, during the chaos.
    fn losses_shared(&self, out: &mut Vec<Broken>) {
        let proxy_down = self.model.down(Host::Proxy);
        for cut in self.device_cuts() {
            let own = self.model.own_link(cut.app);
            let gone = self.model.gone(cut.app);
            let tainted = self.model.app_taints(cut.app);
            Obligation {
                law: "L10",
                conn: cut.conn,
                from: cut.from,
                due: cut.due,
                unless: vec![&own, &proxy_down, &gone, &tainted],
                met: Met::Seen(Box::new(disconnected)),
                what: format!(
                    "app {} did not hear SensorDisconnected after the cut at {}",
                    cut.app, cut.from
                ),
            }
            .discharge(out);
        }
        let episodes = self.model.episodes(|source| source == Source::Proxy);
        for episode in &episodes {
            let first = episode.start;
            for app in self.model.apps() {
                let d = self.model.slack_of(app, first, 0);
                let (from, to) = (first.saturating_sub(d), first + d);
                let Some(conn) = self.conn_at(app, first) else {
                    continue;
                };
                if !alive(conn.presence_before(from, false)) || conn.from > from {
                    continue;
                }
                let own = self.model.unusable(&[Pair::App(app)]);
                let gone = self.model.gone(app);
                let tainted = self.model.app_taints(app);
                Obligation {
                    law: "L10",
                    conn,
                    from,
                    due: to,
                    unless: vec![&own, &gone, &tainted],
                    met: Met::Seen(Box::new(disconnected)),
                    what: format!("app {app} did not hear the loss another app heard at {first}"),
                }
                .discharge(out);
            }
        }
        for app in self.model.apps() {
            let own = self.model.unusable(&[Pair::App(app)]);
            let tainted = self.model.app_taints(app);
            for conn in &self.app_conns[usize::from(app)] {
                let opened = conn.from;
                let Some(episode) = episodes
                    .iter()
                    .find(|episode| episode.start < opened && opened < episode.until)
                else {
                    continue;
                };
                let to = opened + self.model.slack_of(app, opened, 0);
                if episode.until <= to {
                    continue;
                }
                // The status a mount hears as it is added reaches the
                // session before the open returns, ahead of any receiver,
                // so the app is told by its presence.
                let told =
                    |seen: &Seen| disconnected(seen) || root_presence(seen) == Some(Shown::Lost);
                Obligation {
                    law: "L10",
                    conn,
                    from: opened,
                    due: to,
                    unless: vec![&own, &tainted],
                    met: Met::Seen(Box::new(told)),
                    what: format!("app {app} opened at {opened} while the device was out since {} and was not told", episode.start),
                }
                .discharge(out);
            }
        }
    }

    /// L11, during the chaos.
    fn samples_follow(&self, out: &mut Vec<Broken>) {
        if self.model.case.swarm.withheld {
            return;
        }
        for app in self.model.apps() {
            let path = self.model.unusable(&[Pair::App(app), Pair::Device]);
            let gone = self.model.gone(app);
            let tainted = self.model.app_taints(app);
            for conn in &self.app_conns[usize::from(app)] {
                let mut was = None;
                for &(at, seen) in &conn.events {
                    let Some(presence) = root_presence(seen) else {
                        continue;
                    };
                    let returned = matches!(presence, Shown::Alive(_)) && was == Some(Shown::Lost);
                    was = Some(presence);
                    if !returned {
                        continue;
                    }
                    let boot = match presence {
                        Shown::Alive(Some(session)) => self.model.boot_of(session),
                        _ => self.model.boot_at(at),
                    };
                    let Some(boot) = boot else { continue };
                    let d = self.model.slack_of(app, at, SAMPLE_GAP);
                    let due = at + SAMPLE_GAP + d;
                    // An `Alive` for a boot that ends by the due time, which
                    // S5 allows for a while after the next boot starts,
                    // promises no samples of it.
                    if due > self.run.end || boot.end <= due {
                        continue;
                    }
                    let session = boot.session;
                    Obligation {
                        law: "L11",
                        conn,
                        from: at,
                        due,
                        unless: vec![&path, &self.model.taints, &gone, &tainted],
                        met: Met::Seen(Box::new(move |seen| {
                            matches!(seen, Seen::Batch(batch) if batch.session == session)
                        })),
                        what: format!("app {app} heard the device back at {at} and took no batch of session {session}"),
                    }
                    .discharge(out);
                }
            }
        }
    }

    /// L12, during the chaos. Each kill of the proxy pairs with the first
    /// bounce of the proxy after it in the log, as the driver may bounce the
    /// proxy and kill it again within one millisecond. A kill after that
    /// bounce, before the due time, takes the proxy down again and drops the
    /// obligation.
    fn apps_try_again(&self, out: &mut Vec<Broken>) {
        let proxy_kills: Vec<_> = self
            .model
            .kills
            .iter()
            .filter(|kill| kill.host == Host::Proxy)
            .collect();
        for kill in proxy_kills.iter().filter(|kill| !kill.cut_off) {
            let killed = kill.at;
            let back = self
                .model
                .bounces
                .iter()
                .find(|bounce| bounce.host == Host::Proxy && bounce.seq >= kill.seq);
            let Some(back) = back else {
                continue;
            };
            let again: Vec<Span> = proxy_kills
                .iter()
                .filter(|later| later.seq > back.seq)
                .map(|later| Span {
                    from: later.at,
                    to: later.at + 1,
                })
                .collect();
            let back = back.at;
            for app in self.model.apps() {
                let d = self.model.slack_of(app, back, REOPEN_WAIT);
                let due = back + REOPEN_WAIT + d;
                let Some(conn) = self.conn_at(app, killed.saturating_sub(1)) else {
                    continue;
                };
                let held = !self
                    .model
                    .clients_at(app, killed.saturating_sub(1))
                    .is_empty();
                if !held || due + d >= killed + REOPEN_FOR {
                    continue;
                }
                let path = merged([self.model.own_link(app), self.model.device_out()].concat());
                let gone = self.model.gone(app);
                let stats = &self.model.stats;
                Obligation {
                    law: "L12",
                    conn,
                    from: killed,
                    due,
                    unless: vec![&path, &gone, &again],
                    met: Met::Counted(Box::new(move || {
                        stats.iter().any(|(at, stats)| {
                            *at > back && *at <= due + 100 && stats.clients_of(app).next().is_some()
                        })
                    })),
                    what: format!(
                        "app {app} did not open again after the proxy was back at {back}"
                    ),
                }
                .discharge(out);
            }
        }
    }

    /// L13, an open law, open question 7. Once the proxy has lost the
    /// device, or gone down, the device logs the end of the connection the
    /// proxy held within the close timeout and `d`, while the direction
    /// toward the device can carry the proxy's close and the connection is
    /// untainted. A loss counts from when the first app heard it, and its
    /// connection is the last the device accepted more than that app's slack
    /// before, as one accepted later may be the proxy's next.
    fn lost_closed(&self, out: &mut Vec<Broken>) {
        let mut bad = self.model.way_cuts(Pair::Device.way(false)).to_vec();
        bad.extend(self.model.failing(Pair::Device));
        bad.extend(self.model.down(Host::Device));
        let bad = merged(bad);
        // When the proxy let go, and how long before that its connection
        // was accepted at the latest.
        let mut let_go: Vec<(u64, u64)> = self
            .model
            .kills
            .iter()
            .filter(|kill| kill.host == Host::Proxy)
            .map(|kill| (kill.at, 0))
            .collect();
        let losses = self.model.episodes(|source| source == Source::Proxy);
        let_go.extend(losses.iter().map(|episode| {
            let heard = self.model.slack_of(episode.first, episode.start, 0);
            (episode.start, heard)
        }));
        for (at, heard) in let_go {
            let d = self.model.slack(&[Pair::Device], at, CLOSE_TIMEOUT);
            let held = self
                .model
                .device_conns
                .iter()
                .rfind(|conn| conn.accepted + heard <= at);
            let Some(conn) = held.filter(|conn| conn.end > at) else {
                continue;
            };
            let due = at + CLOSE_TIMEOUT + d;
            let from = at.saturating_sub(heard.max(d));
            if due > self.run.end || meets(&bad, from, due) || meets(&self.model.taints, from, due)
            {
                continue;
            }
            if conn.end > due {
                broke!(out, "L13", due =>
                    "the device kept the connection it accepted at {} after the proxy let it go at {at}",
                    conn.accepted);
            }
        }
    }

    /// M1 and M3, at the end of the case.
    fn blips(&self, out: &mut Vec<Broken>) {
        let case = self.model.case;
        let steps = plan(case).steps;
        let slowest = steps.iter().filter_map(|(step, _, _)| match step {
            Step::SlowLink { max_ms, .. } => Some(*max_ms),
            _ => None,
        });
        let latency = slowest.fold(case.swarm.latency, u64::max);
        let within = |step: &Step| match *step {
            Step::Wait | Step::SlowLink { .. } => true,
            Step::Blip { pair, ms } => within_margin(pair, ms, latency),
            _ => false,
        };
        let (law, spared): (&'static str, Vec<u8>) =
            if steps.iter().all(|(step, _, _)| within(step)) {
                ("M1", self.model.apps().collect())
            } else if case.swarm.apps >= 2 {
                let touched: BTreeSet<Option<u8>> = steps
                    .iter()
                    .filter(|(step, _, _)| *step != Step::Wait)
                    .map(|(step, _, _)| step.app())
                    .collect();
                match touched.iter().collect::<Vec<_>>().as_slice() {
                    [Some(app)] => (
                        "M3",
                        self.model.apps().filter(|other| other != app).collect(),
                    ),
                    _ => return,
                }
            } else {
                return;
            };
        let end = self.run.end;
        let accepted = self.model.device_conns.len();
        if accepted != 1 {
            broke!(out, law, end => "the device accepted {accepted} connections");
        }
        // The first boot's session, which no blip may change.
        let first = Shown::Alive(Some(7));
        for app in spared {
            let events = &self.model.events[usize::from(app)];
            let opened = events
                .iter()
                .filter(|(_, seen)| matches!(seen, Seen::Opened | Seen::OpenFailed(_)))
                .count();
            let clients: BTreeSet<u64> = self
                .model
                .stats
                .iter()
                .flat_map(|(_, stats)| stats.clients_of(app))
                .collect();
            if opened != 1 || clients.len() != 1 {
                broke!(out, law, end =>
                    "app {app} opened {opened} times, as {} clients", clients.len());
            }
            let mut batches = 0;
            for &(at, seen) in events {
                match seen {
                    Seen::Presence {
                        child: false,
                        presence,
                    } if at > self.run.warmed && *presence != first => {
                        broke!(out, law, at => "app {app} saw the presence {presence:?}");
                    }
                    Seen::Batch(batch) => {
                        if batches > 0 && batch.boundary.is_some() {
                            broke!(out, law, at =>
                                "app {app} took a batch with the boundary {:?}", batch.boundary);
                        }
                        batches += 1;
                    }
                    Seen::Done(number, Err(error)) => {
                        broke!(out, law, at => "app {app}'s call {number} failed with {error}");
                    }
                    _ => {}
                }
            }
        }
    }
}

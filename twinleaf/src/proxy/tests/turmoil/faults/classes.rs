//! The classes of section 4, which count what the generated cases reach.

use super::hosts::{nonce, Seen, Shown};
use super::laws::{root_presence, status_is, Judge};
use super::model::{meets, merged};
use super::schedule::{plan, within_margin, Host, Pair, Step};
use super::QUIET;
use crate::session::RecvError;
use crate::tio::packet::LinkStatus;
use std::collections::BTreeSet;

/// Every class a nightly run must reach, with `TWINLEAF_TURMOIL_SHARES` set.
pub(super) const CLASSES: [&str; 28] = [
    "C1 device cut past the quiet time",
    "C2 FailedToReconnect",
    "C3 app cut 6 to 20 s and back",
    "C4 app outage of 45 s or more ending it",
    "C5 proxy kill short",
    "C5 proxy kill long",
    "C6 proxy power cut",
    "C7 device kill",
    "C8 device power cut",
    "C9 crash while an app opens",
    "C10 app leaves while its open waits",
    "C11 old samples after a reboot in a blip",
    "C12 call pending at a device cut",
    "C12 call whose boot died before its reply",
    "C13 one app's faults alone",
    "C14 child silent and heard again",
    "C15 heal with an app cut off",
    "C15 heal with the proxy down",
    "C15 heal with the device down",
    "C15 heal with a tainted connection",
    "C16 blip within its margin on the device link",
    "C16 blip within its margin on an app link",
    "C17 one way cut from the device",
    "C17 one way cut to the device",
    "C17 one way cut from an app",
    "C17 one way cut to an app",
    "C18 open again while the device is out",
    "C19 lagged",
];

/// The classes the run that `judge` judges reached.
pub(super) fn classes(judge: &Judge<'_>) -> BTreeSet<&'static str> {
    let (model, run) = (&judge.model, judge.run);
    let case = model.case;
    let mut reached = BTreeSet::new();
    let mut reach = |class: &'static str, reached_it: bool| {
        if reached_it {
            reached.insert(class);
        }
    };
    let device_cuts = model.way_cuts(Pair::Device.way(true));
    reach(
        "C1 device cut past the quiet time",
        device_cuts
            .iter()
            .any(|cut| cut.length() > QUIET + model.slack(&[Pair::Device], cut.from, QUIET)),
    );
    reach(
        "C2 FailedToReconnect",
        model.any_heard(|seen| status_is(seen, LinkStatus::FailedToReconnect)),
    );
    for app in model.apps() {
        let events = &model.events[usize::from(app)];
        let alive_after = |at: u64| {
            events.iter().any(|(when, seen)| {
                *when > at && matches!(root_presence(seen), Some(Shown::Alive(_)))
            })
        };
        for cut in model.cut(Pair::App(app)) {
            reach(
                "C3 app cut 6 to 20 s and back",
                (6_000..=20_000).contains(&cut.length()) && alive_after(cut.to),
            );
        }
        let mut outages = model.own_link(app);
        outages.extend(model.down(Host::Proxy));
        for outage in merged(outages) {
            let ended = judge.app_conns[usize::from(app)].iter().any(|conn| {
                conn.why == Some(RecvError::Disconnected)
                    && conn.to >= outage.from
                    && conn.to <= outage.to.saturating_add(10_000)
            });
            reach(
                "C4 app outage of 45 s or more ending it",
                outage.length() >= 45_000 && ended,
            );
        }
    }
    let device_out = model.device_out();
    for kill in &model.kills {
        let at = kill.at;
        let back = model
            .bounces
            .iter()
            .find(|bounce| bounce.host == kill.host && bounce.at >= at)
            .map_or(u64::MAX, |bounce| bounce.at);
        match (kill.host, kill.cut_off) {
            (Host::Proxy, false) if back - at < 30_000 => reach("C5 proxy kill short", true),
            (Host::Proxy, false) => reach("C5 proxy kill long", true),
            (Host::Proxy, true) => reach("C6 proxy power cut", true),
            (Host::Device, false) => reach("C7 device kill", true),
            (Host::Device, true) => reach("C8 device power cut", true),
            _ => {}
        }
        let opening = |app: u8| {
            let events = &model.events[usize::from(app)];
            let last = events.iter().rfind(|(when, seen)| {
                *when <= at && matches!(seen, Seen::Opening | Seen::Opened | Seen::OpenFailed(_))
            });
            matches!(last, Some((_, Seen::Opening)))
        };
        if matches!(kill.host, Host::Proxy | Host::Device) {
            reach("C9 crash while an app opens", model.apps().any(opening));
        }
        if let Host::App(app) = kill.host {
            reach(
                "C10 app leaves while its open waits",
                opening(app) && meets(&device_out, at, at),
            );
        }
    }
    let steps = plan(case).steps;
    for (step, start, _) in &steps {
        match *step {
            Step::RebootInBlip { app, hold_ms, .. } => {
                let old = model.boot_at(*start).map(|boot| boot.session);
                let events = &model.events[usize::from(app)];
                let late = events.iter().any(|(at, seen)| {
                    *at >= run.warmed + start + hold_ms
                        && matches!(seen, Seen::Batch(batch) if Some(batch.session) == old)
                });
                reach("C11 old samples after a reboot in a blip", late);
            }
            Step::Blip { pair, ms } => reach(
                match pair {
                    Pair::Device => "C16 blip within its margin on the device link",
                    Pair::App(_) => "C16 blip within its margin on an app link",
                },
                within_margin(pair, ms, case.swarm.latency),
            ),
            Step::CutOneWay { way, .. } => reach(
                match (way.pair, way.to_proxy) {
                    (Pair::Device, true) => "C17 one way cut from the device",
                    (Pair::Device, false) => "C17 one way cut to the device",
                    (Pair::App(_), true) => "C17 one way cut from an app",
                    (Pair::App(_), false) => "C17 one way cut to an app",
                },
                true,
            ),
            _ => {}
        }
    }
    let cut_ats: Vec<u64> = model.cut(Pair::Device).iter().map(|cut| cut.from).collect();
    for app in model.apps() {
        let events = &model.events[usize::from(app)];
        for &(at, seen) in events {
            let Seen::Sent(number) = seen else { continue };
            let done = events
                .iter()
                .find_map(|(when, seen)| {
                    matches!(seen, Seen::Done(of, _) if of == number).then_some(*when)
                })
                .unwrap_or(u64::MAX);
            reach(
                "C12 call pending at a device cut",
                cut_ats.iter().any(|&cut| at < cut && cut < done),
            );
            let asked = model.echo(&nonce(app, *number));
            let died = asked
                .and_then(|asked| model.boots.iter().find(|boot| boot.index == asked.boot))
                .is_some_and(|boot| boot.end < done);
            reach("C12 call whose boot died before its reply", died);
        }
        let lagged = events
            .iter()
            .any(|(_, seen)| matches!(seen, Seen::Lagged(..)));
        reach("C19 lagged", lagged);
    }
    let touched: BTreeSet<Option<u8>> = steps
        .iter()
        .filter(|(step, _, _)| *step != Step::Wait)
        .map(|(step, _, _)| step.app())
        .collect();
    reach(
        "C13 one app's faults alone",
        case.swarm.apps >= 2 && touched.len() == 1 && touched.iter().all(Option::is_some),
    );
    if case.swarm.child {
        for silence in &model.silences {
            let long = silence.length() > QUIET + model.slack(&[Pair::Device], silence.from, QUIET);
            let child_alive = |seen: &Seen| {
                matches!(
                    seen,
                    Seen::Presence {
                        child: true,
                        presence: Shown::Alive(_)
                    }
                )
            };
            let back = model
                .events
                .iter()
                .flatten()
                .any(|(at, seen)| *at > silence.to && child_alive(seen));
            reach("C14 child silent and heard again", long && back);
        }
    }
    let healed = run.healed.saturating_sub(1);
    reach(
        "C15 heal with an app cut off",
        model
            .apps()
            .any(|app| model.own_link(app).iter().any(|cut| cut.contains(healed))),
    );
    reach(
        "C15 heal with the proxy down",
        model
            .down(Host::Proxy)
            .iter()
            .any(|span| span.contains(healed)),
    );
    reach(
        "C15 heal with the device down",
        model
            .down(Host::Device)
            .iter()
            .any(|span| span.contains(healed)),
    );
    reach(
        "C15 heal with a tainted connection",
        model.taints.iter().any(|span| span.contains(healed)),
    );
    // A client the proxy lists for the first time while the device is out.
    let mut known = BTreeSet::new();
    for &(at, stats) in &model.stats {
        for &(id, app) in &stats.clients {
            let new = known.insert(id);
            let out_now = device_out.iter().any(|span| span.contains(at));
            reach(
                "C18 open again while the device is out",
                new && app.is_some() && at > run.warmed && out_now,
            );
        }
    }
    reached
}

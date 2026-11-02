//! The fixed examples of section 7, each a schedule the interpreter runs and
//! the checker judges, with checks of its own.

use super::driver::{simulate, Run};
use super::hosts::{Event, Seen};
use super::laws::{status_is, Broken, Judge, OPEN};
use super::schedule::{Action, Case, Down, Host, Kinds, Pair, Step, Swarm};
use crate::device::RecvError;
use crate::tio::packet::LinkStatus;

/// The swarm of a fixed example with `apps` apps.
fn plain(apps: u8) -> Swarm {
    Swarm {
        apps,
        reconnect_timeout: false,
        child: false,
        withheld: false,
        random_order: false,
        latency: 100,
        tcp_capacity: 64,
        kinds: Kinds::default(),
    }
}

/// The schedule of `step` alone, at the start of the chaos.
fn once(step: Step) -> Vec<(u64, Step)> {
    vec![(0, step)]
}

/// How many seeds each fixed example runs with.
const FIXED_SEEDS: usize = 2;

/// Runs the fixed case of `swarm` and `schedule` with each seed, and fails
/// at the first law a run breaks. Returns each run, for the example's own
/// checks.
fn kept(swarm: Swarm, schedule: Vec<(u64, Step)>) -> Vec<Run> {
    let mut runs = Vec::new();
    for seed in crate::proxy::turmoil::seeds().into_iter().take(FIXED_SEEDS) {
        let case = Case {
            seed,
            swarm: swarm.clone(),
            schedule: schedule.clone(),
        };
        let run = match simulate(&case) {
            Ok(run) => run,
            Err(why) => panic!("the case of seed {seed} was not judged: {why:?}"),
        };
        let broken = Judge::new(&case, &run).judge();
        if let Some(broken) = broken.iter().find(|broken| !OPEN.contains(&broken.law)) {
            let Broken { law, at, what } = broken;
            panic!(
                "{law} broke at {at} ms in this example with TWINLEAF_TURMOIL_SEED={seed}: {what}"
            );
        }
        runs.push(run);
    }
    runs
}

/// When the driver applied `action` in `run`.
fn applied(run: &Run, action: Action) -> u64 {
    let did = run
        .log
        .iter()
        .find(|logged| logged.event == Event::Did(action));
    did.expect("the action").at
}

/// The times app `app` saw what `wanted` accepts in `run`.
fn times_seen(run: &Run, app: u8, wanted: impl Fn(&Seen) -> bool) -> Vec<u64> {
    let events = run.log.iter().filter_map(|logged| match &logged.event {
        Event::App(of, seen) if *of == app && wanted(seen) => Some(logged.at),
        _ => None,
    });
    events.collect()
}

/// The scenarios of `proxy/turmoil/scenarios.rs` as fixed schedules keep the laws.
#[test]
fn the_scenarios_as_fixed_schedules_keep_the_laws() {
    let mut child = plain(1);
    child.child = true;
    let mut withheld = child.clone();
    withheld.withheld = true;
    kept(
        plain(1),
        once(Step::Cut {
            pair: Pair::Device,
            ms: 6_000,
        }),
    );
    kept(
        plain(1),
        once(Step::RebootDevice {
            style: Down::Kill,
            down_ms: 0,
        }),
    );
    kept(
        plain(1),
        once(Step::CrashProxy {
            style: Down::Kill,
            down_ms: 2_000,
        }),
    );
    kept(
        plain(1),
        once(Step::Cut {
            pair: Pair::App(0),
            ms: 8_000,
        }),
    );
    let slow = Step::SlowLink {
        pair: Pair::Device,
        max_ms: 1_000,
    };
    kept(plain(1), vec![(0, slow), (30_000, Step::Wait)]);
    kept(withheld, once(Step::ChildSilent { ms: 60_000 }));
    kept(child, once(Step::ChildSilent { ms: 5_000 }));
}

/// A device whose link toward the proxy is cut for 10 s is lost, and the
/// proxy connects to it again during the cut, as turmoil completes a
/// handshake whose answer crosses the cut. The device's first packets on
/// that connection are lost, and turmoil sends nothing again. The worker
/// gives the connection up once `LIVENESS_TIMEOUT_TCP` has passed from the
/// start of the attempt, as it would a device that accepts and stays silent
/// over real TCP, and connects again.
#[test]
fn a_reopened_connection_the_device_never_answers_on_is_given_up() {
    let way = Pair::Device.way(true);
    kept(plain(1), once(Step::CutOneWay { way, ms: 10_000 }));
}

/// A proxy killed for 27 s, short of the session's 30 s, has each app open
/// its device again, and no connection ends.
#[test]
fn a_proxy_killed_for_27_s_has_each_app_open_again() {
    let schedule = once(Step::CrashProxy {
        style: Down::Kill,
        down_ms: 27_000,
    });
    for run in kept(plain(2), schedule) {
        for app in 0..2 {
            assert_eq!(
                times_seen(&run, app, |seen| *seen == Seen::Opened).len(),
                1,
                "app {app} opened again"
            );
            let ended = times_seen(&run, app, |seen| matches!(seen, Seen::Stopped(..)));
            assert_eq!(ended, Vec::<u64>::new(), "app {app}'s connection ended");
        }
    }
}

/// A proxy killed for 33 s, past the session's 30 s, ends each app's
/// connection with `Disconnected` 30 s after the kill, and the app opens a
/// new one.
#[test]
fn a_proxy_killed_for_33_s_ends_each_connection_after_30_s() {
    let schedule = once(Step::CrashProxy {
        style: Down::Kill,
        down_ms: 33_000,
    });
    for run in kept(plain(2), schedule) {
        let killed = applied(&run, Action::Crash(Host::Proxy));
        for app in 0..2 {
            let ended = times_seen(&run, app, |seen| {
                matches!(seen, Seen::Stopped(_, RecvError::Disconnected))
            });
            let after = ended.first().map(|at| at.saturating_sub(killed));
            assert!(
                after.is_some_and(|after| (29_500..=31_000).contains(&after)),
                "app {app}'s connection ended {after:?} ms after the kill"
            );
            assert_eq!(
                times_seen(&run, app, |seen| *seen == Seen::Opened).len(),
                2,
                "app {app} did not open again"
            );
        }
    }
}

/// With a reconnect timeout of 10 s, a device killed for 8 s is not given
/// up.
#[test]
fn a_device_killed_for_8_s_within_the_reconnect_timeout_is_kept() {
    let mut swarm = plain(1);
    swarm.reconnect_timeout = true;
    let schedule = once(Step::RebootDevice {
        style: Down::Kill,
        down_ms: 8_000,
    });
    for run in kept(swarm, schedule) {
        let gave_up = times_seen(&run, 0, |seen| {
            status_is(seen, LinkStatus::FailedToReconnect)
        });
        assert_eq!(gave_up, Vec::<u64>::new(), "the proxy gave the device up");
    }
}

/// With a reconnect timeout of 10 s, a device killed for 12 s is given up
/// 10 s after the kill.
#[test]
fn a_device_killed_for_12_s_past_the_reconnect_timeout_is_given_up() {
    let mut swarm = plain(1);
    swarm.reconnect_timeout = true;
    let schedule = once(Step::RebootDevice {
        style: Down::Kill,
        down_ms: 12_000,
    });
    for run in kept(swarm, schedule) {
        let killed = applied(&run, Action::Crash(Host::Device));
        let gave_up = times_seen(&run, 0, |seen| {
            status_is(seen, LinkStatus::FailedToReconnect)
        });
        let after = gave_up.first().map(|at| at.saturating_sub(killed));
        assert!(
            after.is_some_and(|after| (10_000..=10_500).contains(&after)),
            "the proxy gave the device up {after:?} ms after the kill"
        );
    }
}

/// With a reconnect timeout of 10 s and three apps that call the device each
/// second, a device killed for 20 s is given up, and every app hears
/// `FailedToReconnect` 10 s after the kill and keeps the laws, wherever the
/// kill falls against the apps' calls. An app whose request reaches the
/// proxy after the worker gave up and before the engine heard it must hear
/// the status too, and not see its connection closed without it. The kill
/// comes 860 to 960 ms into the chaos, in each case of its own, which
/// spreads it over a second of calls.
#[test]
fn every_app_hears_a_device_given_up_wherever_its_requests_land() {
    let seed = crate::proxy::turmoil::seeds()[0];
    let offsets: Vec<u64> = (860..=960).collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let failures = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..super::report::threads().min(offsets.len()) {
            scope.spawn(|| loop {
                let taken = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let Some(&offset) = offsets.get(taken) else {
                    break;
                };
                if let Some(failure) = given_up_at(seed, offset) {
                    crate::chan::lock(&failures).push(format!("offset {offset}: {failure}"));
                }
            });
        }
    });
    let mut failures = failures.into_inner().expect("the failures");
    failures.sort();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Runs the case of [`every_app_hears_a_device_given_up_wherever_its_requests_land`]
/// with the kill `offset` ms into the chaos, and returns what went wrong, if
/// anything did.
fn given_up_at(seed: u64, offset: u64) -> Option<String> {
    const APPS: u8 = 3;
    let mut swarm = plain(APPS);
    swarm.reconnect_timeout = true;
    let step = Step::RebootDevice {
        style: Down::Kill,
        down_ms: 20_000,
    };
    let case = Case {
        seed,
        swarm,
        schedule: vec![(offset, step)],
    };
    let run = match simulate(&case) {
        Ok(run) => run,
        Err(why) => return Some(format!("the case was not judged: {why:?}")),
    };
    let killed = applied(&run, Action::Crash(Host::Device));
    for app in 0..APPS {
        let gave_up = times_seen(&run, app, |seen| {
            status_is(seen, LinkStatus::FailedToReconnect)
        });
        let after = gave_up.first().map(|at| at.saturating_sub(killed));
        if !after.is_some_and(|after| (10_000..=10_600).contains(&after)) {
            return Some(format!(
                "app {app} heard FailedToReconnect {after:?} ms after the kill"
            ));
        }
    }
    let broken = Judge::new(&case, &run).judge();
    let broken = broken.iter().find(|broken| !OPEN.contains(&broken.law));
    broken.map(|Broken { law, at, what }| format!("{law} broke at {at} ms: {what}"))
}

/// A hub child that reboots in a silence of 1.5 s, too short to be a loss,
/// comes back in a new session on the same connection, and every app hears
/// it in that session and loads its new table, as S5, L1 and L4 say.
#[test]
fn a_hub_child_that_reboots_unnoticed_is_heard_in_its_new_session() {
    let mut swarm = plain(2);
    swarm.child = true;
    kept(swarm, once(Step::ChildSilent { ms: 1_500 }));
}

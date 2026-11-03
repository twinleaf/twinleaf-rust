#![doc = include_str!("spec.md")]

mod classes;
mod driver;
mod examples;
mod hosts;
mod laws;
mod model;
mod report;
mod schedule;

pub(super) use hosts::{Event, Logged};

use super::REPLAYED;
use classes::CLASSES;
use proptest::strategy::{Strategy, ValueTree};
use report::{case_seeds, exercise, threads, Totals};
use schedule::{drawn, drawn_case, runner, valid, Case};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

// The phases of a case.

/// The longest warm up, in milliseconds.
const WARM_UP: u64 = 15_000;
/// The longest chaos, in milliseconds.
const CHAOS: u64 = 60_000;
/// The latest `H'` may come after the heal, in milliseconds.
const SETTLE: u64 = 70_000;
/// The bound `B`, in milliseconds.
const BOUND: u64 = 30_000;
/// The window `W`, in milliseconds.
const WINDOW: u64 = 10_000;
/// How long a case may run in virtual time.
const DURATION: Duration = Duration::from_secs(200);

// The hosts.

/// The most apps a case has.
const MOST_APPS: usize = 3;
/// The hosts of the apps.
const APPS: [&str; MOST_APPS] = ["app0", "app1", "app2"];
/// How long an app's open waits.
const OPEN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long an app's call waits for its reply, in milliseconds.
const CALL_TIMEOUT: u64 = 20_000;
/// The `reconnect_timeout` a case may draw, in milliseconds.
const RECONNECT: u64 = 10_000;

// The timers of section 6, in milliseconds, as the laws count them.

/// The device's quiet time over TCP and the engine's, after which the proxy
/// loses a silent device or hub child.
const QUIET: u64 = 3_000;
/// How long the proxy's worker waits between attempts to reach the device.
const RECONNECT_INTERVAL: u64 = 1_000;
/// A connect timeout and the reconnect interval after it, the longest the
/// proxy takes to reach a device whose link is usable again.
const RECONNECT_TRY: u64 = 4_000;
/// The keepalive's quiet time, after which the proxy lets a silent client go.
const KEEPALIVE_QUIET: u64 = 5_000;
/// How long an app takes to see its socket lost, the keepalive's quiet time
/// and the close timeout after it.
const SOCKET_LOSS: u64 = 6_000;
/// How long a closed port takes at most to write what was queued and let go
/// of its link.
const CLOSE_TIMEOUT: u64 = 1_000;
/// How long the device takes to see its connection end once no app holds
/// the link, the idle time and a second for the close.
const IDLE_CLOSE: u64 = 6_000;
/// The longest an app waits before it opens again once the proxy is back,
/// the backoff's cap with its jitter.
const REOPEN_WAIT: u64 = 1_250;
/// The reopen limit, after which a session whose socket was lost ends.
const REOPEN_FOR: u64 = 30_000;
/// How much of the reopen limit an app's path must have been unusable for
/// before its connection may end with `Disconnected`, as S8 says.
const MOSTLY_UNUSABLE: u64 = 23_000;
/// The longest gap between two batches an app takes, two samples.
const SAMPLE_GAP: u64 = 2_000;
/// How long a call may take unless the device connection is tainted, as L9
/// says.
const CALL_BOUND: u64 = 10_000;
/// The longest blip, in milliseconds.
const LONGEST_BLIP: u64 = 2_500;

/// Generated schedules of faults keep every law whose source is settled.
/// The laws of open questions are counted, and a run prints them with the
/// classes its cases reached and the real time they took. A failing case
/// is shrunk, and its seed reruns it alone. Cases run on several threads,
/// and a failure stops the run once the cases under way end.
///
/// Each law has mutants the suite must catch, applied by hand and run with
/// this test. Where a constant changes, the mutant uses a value a person
/// might plausibly write, as well as an extreme one. A survivor gets one
/// answer. A law is missing, the generator cannot reach the case, or the
/// mutant is equivalent, with the reason.
/// - S1. In `Mounts::feed` in `proxy/engine/mod.rs`, queue each packet twice,
///   `mount.outbox.stream(shown.clone())` before `mount.outbox.stream(shown)`.
/// - S2. In `StreamTracker::detect_boundary` in `data/state.rs`, count one
///   more, `first_sample_n.value() - expected.value() + 1`. In the same
///   function, drop `opened &&` from the condition `opened && self.relearned`
///   of the `Restarted` branch, so a gap after a link loss is told as
///   `Restarted`.
/// - S3. In `StreamTracker::detect_boundary`, delete the `SessionChanged`
///   branch, `if device.session() != self.last_session_id`.
/// - S4. In `Outbox::update` in `proxy/outbox/mod.rs`, put a metadata update on
///   `Queue::Urgent`, in the arm `Update::Metadata(_) => Queue::Stream`.
/// - S5. In `Engine::take` in `proxy/engine/mod.rs`, store the session plus one
///   in `store.set(route, PRESENCE, Presence::Alive { session })`. In the
///   heartbeat arm `beat.session().or(held.flatten())` of the same function,
///   prefer the held session to the heartbeat's,
///   `held.flatten().or(beat.session())`.
/// - S6. In `Worker::dispatch_device_packet` in `proxy/worker.rs`, answer
///   with `original_id ^ 1` in `self.answer(port, pkt.with_rpc_id(original_id))`.
///   For the retry, a small patch that keeps each request packet in
///   `RpcMapEntry`, deletes the `self.cancel_active_rpcs()` under
///   `if self.device.is_none()` in `Worker::turn`, and sends every request
///   still in `rpc_map` in the arm `Ok(device) => self.device = Some(device)`
///   of `Worker::turn`, where it adopts the device.
/// - S8. Make `REOPEN_FOR` in `session/mod.rs` 25 s and 3 s. In
///   `Worker::device_received` in `proxy/worker.rs`, delete
///   `self.reconnect_deadline = None`. In `Worker::lose_device`, set
///   `self.reconnect_deadline` to `now + timeout / 2` for `now + timeout`.
/// - S9. In `Outbox::end` in `proxy/outbox/mod.rs`, also set the stop due, so a
///   client let go hears `Stopped`.
/// - L1. In `Worker::turn` in `proxy/worker.rs`, delete the liveness check
///   `let liveness = self.device.as_ref().and_then(Device::liveness_deadline)`.
///   In the same function, wait 100 s in
///   `self.next_attempt = Instant::now() + RECONNECT_INTERVAL`.
/// - L2. In `Mounts::feed` in `proxy/engine/mod.rs`, feed only the first mount,
///   `list.mounts.iter().take(1)` for the loop `for mount in &list.mounts`.
///   In `Outbox::queue` in `proxy/outbox/mod.rs`, compare with 0 in the loop
///   `while queues.sample_bytes > SAMPLE_BUDGET`.
/// - L3. The first S6 mutant, with one call in flight.
/// - L4. Delete `self.reconnect(&mut store, route)` in `Engine::apply_status`
///   in `proxy/engine/mod.rs` for the root, and in the `Presence::Lost` arm of
///   `Engine::take` for the child.
/// - L5. At the end of `Worker::lose_device` in `proxy/worker.rs`, add
///   `std::mem::forget(device)`, so the lost port's socket and task stay.
/// - L6. (a) Make `LIVENESS_TIMEOUT_TCP` in `proxy/worker.rs` 4000 ms and
///   30000 ms. (b) Make the `quiet` of `CLIENT_KEEPALIVE` in
///   `session/control.rs` 7 s. (c) In `Client::serve` in `proxy/client.rs`,
///   guard the arm `received = self.incoming.next()` of the `select!` with
///   `if self.opening.is_none()`. (d) Make `IDLE` in `proxy/mod.rs` 8 s.
///   (e) Make `QUIET` in `proxy/engine/mod.rs` 4 s.
/// - L7. (a) In `Worker::lose_device` in `proxy/worker.rs`, set
///   `self.reconnect_deadline` to `now + timeout * 3 / 2`. In `Worker::packet`
///   in `session/mod.rs`, delete `self.reopen = None` on
///   `FailedToReconnect`. (b) Make `REOPEN_FOR` in `session/mod.rs` 35 s.
/// - L8. In `Worker::device_received` in `proxy/worker.rs`, delete
///   `self.status_to_engine(packet::LinkStatus::SensorReconnected)`. In
///   `Worker::turn`, wait 5 s in
///   `self.next_attempt = Instant::now() + RECONNECT_INTERVAL`.
/// - L9. Empty `Worker::cancel_active_rpcs` in `proxy/worker.rs`. Delete the
///   loop `for pending in std::mem::take(&mut self.pending).into_values()`
///   in `Worker::lose` in `session/mod.rs`, which fails pending calls.
/// - L10. In `Mounts::feed` in `proxy/engine/mod.rs`, put link statuses on the
///   first mount only.
/// - L11. In `Engine::apply_status` in `proxy/engine/mod.rs`, never forget the
///   subtree on `SensorDisconnected`, at `let forget = matches!(status, ...)`,
///   so samples of a new boot wait on metadata the parser thinks it has. If
///   it survives, record whether the records sent first make it equivalent.
/// - L12. In `open_again` in `session/mod.rs`, cap the backoff at 5 s in
///   `wait = (wait * 2).min(Duration::from_secs(1))`.
/// - M1. In `Device::liveness_timeout` in `proxy/worker.rs`, use
///   `LIVENESS_TIMEOUT` for TCP in the arm
///   `transport::TransportKind::Tcp => LIVENESS_TIMEOUT_TCP`.
/// - M2. In `jittered` in `session/mod.rs`, draw from the nanoseconds of
///   `SystemTime::now()` for `RandomState::new().hash_one(())`.
/// - M3. In `Mounts::feed` in `proxy/engine/mod.rs`, feed only the last mount
///   added, so an app that opens again starves the others.
/// - I2. In `Engine::next_wake` in `proxy/engine/mod.rs`, make a pending retry
///   due now, at
///   `let retry = self.schedule.next_retry(asked(&self.heard, &self.demand))`.
///
/// The restart check `std::mem::take(&mut device.restarted)` in
/// `Worker::turn` in `proxy/worker.rs`, and the `self.cancel_active_rpcs()`
/// it calls, are equivalent here, as section 5 says.
#[test]
fn generated_fault_schedules_keep_the_laws() {
    let seeds = case_seeds();
    let all = std::env::var("TWINLEAF_TURMOIL_ALL").is_ok();
    let totals = std::sync::Mutex::new(Totals::default());
    let failed = std::sync::Mutex::new(Vec::<(u64, String)>::new());
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let started = std::time::Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads().min(seeds.len()) {
            scope.spawn(|| {
                while !stop.load(Ordering::SeqCst) {
                    let Some(&seed) = seeds.get(next.fetch_add(1, Ordering::SeqCst)) else {
                        break;
                    };
                    if let Some(failure) = exercise(seed, &totals) {
                        crate::chan::lock(&failed).push((seed, failure));
                        stop.store(!all, Ordering::SeqCst);
                    }
                }
            });
        }
    });
    let totals = totals.into_inner().expect("the totals");
    totals.print();
    eprintln!("  the run took {:?} of real time", started.elapsed());
    let mut failed = failed.into_inner().expect("the failures");
    failed.sort_by_key(|(seed, _)| *seed);
    if let [(_, failure)] = failed.as_slice() {
        panic!("{failure}");
    }
    let shown = |failure: &String| {
        failure
            .split(".\nminimal failing input")
            .next()
            .unwrap_or_default()
            .to_string()
    };
    let firsts: Vec<String> = failed.iter().map(|(_, failure)| shown(failure)).collect();
    assert!(
        failed.is_empty(),
        "{} cases failed\n{}",
        failed.len(),
        firsts.join("\n")
    );
    if std::env::var("TWINLEAF_TURMOIL_SHARES").is_ok() {
        let missed: Vec<&str> = CLASSES
            .iter()
            .copied()
            .filter(|class| !totals.classes.contains_key(class))
            .collect();
        assert!(missed.is_empty(), "no case reached {missed:?}");
    }
    assert!(
        totals.unjudged.is_empty(),
        "the harness could not judge {:?}",
        totals.unjudged
    );
}

/// The schedule steps of `Step` are valid in every generated case, and in
/// every case shrinking leaves.
#[test]
fn generated_and_shrunk_schedules_are_valid() {
    for seed in 1..=256 {
        let mut runner = runner(seed, 0);
        let mut tree = drawn().new_tree(&mut runner).expect("a case");
        for _ in 0..64 {
            let (swarm, schedule) = tree.current();
            let case = Case {
                seed,
                swarm,
                schedule,
            };
            if let Err(why) = valid(&case) {
                panic!("{why} in {case:#?}");
            }
            if !tree.simplify() {
                break;
            }
        }
    }
}

/// How many cases the replay check looks through for ones in which an app
/// opens again.
const LOOKED_THROUGH: u64 = 64;

/// A generated case in which an app opens again, run twice with one seed,
/// gives the same turmoil trace and the same log.
#[test]
fn a_generated_case_run_twice_with_one_seed_runs_the_same_way() {
    let mut replayed = 0;
    for seed in 1..=LOOKED_THROUGH {
        if replayed == REPLAYED {
            break;
        }
        let case = drawn_case(seed);
        let (first, Ok(first_run)) = driver::simulate_traced(&case) else {
            continue;
        };
        if !first_run.reopened(case.swarm.apps) {
            continue;
        }
        let (second, Ok(second_run)) = driver::simulate_traced(&case) else {
            panic!("the second run of the case of seed {seed} was not judged");
        };
        let longest = first.len().max(second.len());
        if let Some(at) = (0..longest).find(|&at| first.get(at) != second.get(at)) {
            panic!(
                "the runs of seed {seed} differ at trace event {at}\nfirst  {:?}\nsecond {:?}",
                first.get(at),
                second.get(at)
            );
        }
        let longest = first_run.log.len().max(second_run.log.len());
        if let Some(at) = (0..longest).find(|&at| first_run.log.get(at) != second_run.log.get(at)) {
            panic!(
                "the runs of seed {seed} differ at log event {at}\nfirst  {:?}\nsecond {:?}",
                first_run.log.get(at),
                second_run.log.get(at)
            );
        }
        replayed += 1;
    }
    assert_eq!(
        replayed, REPLAYED,
        "too few of {LOOKED_THROUGH} cases have an app open again"
    );
}

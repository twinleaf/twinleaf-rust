//! The schedule of a case, the generator that draws it, and the validity
//! property of section 4.

use super::{APPS, CHAOS, LONGEST_BLIP};
use proptest::prelude::*;
use proptest::strategy::{Union, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;

/// A link a step acts on, between the proxy and the device or an app.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::proxy::turmoil) enum Pair {
    Device,
    App(u8),
}

impl Pair {
    /// The host at the far end from the proxy.
    pub(super) fn far(self) -> &'static str {
        match self {
            Pair::Device => "device",
            Pair::App(app) => APPS[usize::from(app)],
        }
    }

    /// The host of that far end.
    pub(super) fn host(self) -> Host {
        match self {
            Pair::Device => Host::Device,
            Pair::App(app) => Host::App(app),
        }
    }

    /// The direction of this link toward the proxy, or away from it.
    pub(super) fn way(self, to_proxy: bool) -> Way {
        Way {
            pair: self,
            to_proxy,
        }
    }

    /// Both directions of this link.
    pub(super) fn ways(self) -> [Way; 2] {
        [self.way(true), self.way(false)]
    }
}

/// A host a step crashes or bounces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::proxy::turmoil) enum Host {
    Device,
    Proxy,
    App(u8),
}

impl Host {
    pub(super) fn name(self) -> &'static str {
        match self {
            Host::Device => "device",
            Host::Proxy => "proxy",
            Host::App(app) => APPS[usize::from(app)],
        }
    }
}

/// One direction of a link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::proxy::turmoil) struct Way {
    pub(super) pair: Pair,
    /// Whether it runs toward the proxy.
    pub(super) to_proxy: bool,
}

impl Way {
    /// The hosts it runs from and to.
    pub(super) fn ends(self) -> (&'static str, &'static str) {
        match self.to_proxy {
            true => (self.pair.far(), "proxy"),
            false => ("proxy", self.pair.far()),
        }
    }
}

/// How an app goes away.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Away {
    Close,
    Kill,
    CutThenKill,
}

/// How a host goes down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Down {
    Kill,
    PowerCut,
}

/// One step of a schedule. Each carries its own end, in milliseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Step {
    Wait,
    SlowLink { pair: Pair, max_ms: u64 },
    Blip { pair: Pair, ms: u64 },
    Cut { pair: Pair, ms: u64 },
    CutOneWay { way: Way, ms: u64 },
    AppAway { app: u8, style: Away, ms: u64 },
    ChildSilent { ms: u64 },
    RebootDevice { style: Down, down_ms: u64 },
    RebootInBlip { app: u8, hold_ms: u64, at_ms: u64 },
    CrashProxy { style: Down, down_ms: u64 },
    FailRate { pair: Pair, permille: u32, ms: u64 },
}

/// What a step claims while it runs, so that no other step that sets the
/// same thing overlaps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Claim {
    Link(Pair),
    Host(Host),
    Child,
}

impl Step {
    /// How long it runs.
    fn length(&self) -> u64 {
        match *self {
            Step::Wait | Step::SlowLink { .. } => 0,
            Step::Blip { ms, .. }
            | Step::Cut { ms, .. }
            | Step::CutOneWay { ms, .. }
            | Step::AppAway { ms, .. }
            | Step::ChildSilent { ms }
            | Step::FailRate { ms, .. } => ms,
            Step::RebootDevice { down_ms, .. } | Step::CrashProxy { down_ms, .. } => down_ms,
            Step::RebootInBlip { hold_ms, .. } => hold_ms,
        }
    }

    /// The app whose own link or host it touches, if it touches one and
    /// nothing else.
    pub(super) fn app(&self) -> Option<u8> {
        match *self {
            Step::SlowLink { pair, .. }
            | Step::Blip { pair, .. }
            | Step::Cut { pair, .. }
            | Step::FailRate { pair, .. }
            | Step::CutOneWay {
                way: Way { pair, .. },
                ..
            } => match pair {
                Pair::App(app) => Some(app),
                Pair::Device => None,
            },
            Step::AppAway { app, .. } => Some(app),
            _ => None,
        }
    }

    /// What it claims while it runs, in a case of `apps` apps.
    fn claims(&self, apps: u8) -> Vec<Claim> {
        match *self {
            Step::Wait | Step::SlowLink { .. } => Vec::new(),
            Step::Blip { pair, .. } | Step::Cut { pair, .. } | Step::FailRate { pair, .. } => {
                vec![Claim::Link(pair)]
            }
            Step::CutOneWay { way, .. } => vec![Claim::Link(way.pair)],
            Step::AppAway { app, style, .. } => match style {
                Away::CutThenKill => vec![Claim::Host(Host::App(app)), Claim::Link(Pair::App(app))],
                _ => vec![Claim::Host(Host::App(app))],
            },
            Step::ChildSilent { .. } => vec![Claim::Child],
            Step::RebootDevice { style, .. } => match style {
                Down::Kill => vec![Claim::Host(Host::Device)],
                Down::PowerCut => vec![Claim::Host(Host::Device), Claim::Link(Pair::Device)],
            },
            Step::RebootInBlip { app, .. } => {
                vec![Claim::Link(Pair::App(app)), Claim::Host(Host::Device)]
            }
            Step::CrashProxy { style, .. } => {
                let mut claimed = vec![Claim::Host(Host::Proxy)];
                if style == Down::PowerCut {
                    claimed.push(Claim::Link(Pair::Device));
                    claimed.extend((0..apps).map(|app| Claim::Link(Pair::App(app))));
                }
                claimed
            }
        }
    }

    /// The actions that carry it out when it starts at `at`, in a case of
    /// `apps` apps.
    fn actions(&self, at: u64, apps: u8) -> Vec<(u64, Action)> {
        match *self {
            Step::Wait => Vec::new(),
            Step::SlowLink { pair, max_ms } => vec![(at, Action::Latency(pair, max_ms))],
            Step::Blip { pair, ms } => {
                vec![(at, Action::Hold(pair)), (at + ms, Action::Release(pair))]
            }
            Step::Cut { pair, ms } => {
                vec![(at, Action::Cut(pair)), (at + ms, Action::Repair(pair))]
            }
            Step::CutOneWay { way, ms } => {
                vec![
                    (at, Action::CutOneWay(way)),
                    (at + ms, Action::RepairOneWay(way)),
                ]
            }
            Step::AppAway { app, style, ms } => {
                let host = Host::App(app);
                match style {
                    Away::Close => vec![
                        (at, Action::Away(app, true)),
                        (at + ms, Action::Away(app, false)),
                    ],
                    Away::Kill => vec![(at, Action::Crash(host)), (at + ms, Action::Bounce(host))],
                    Away::CutThenKill => vec![
                        (at, Action::Cut(Pair::App(app))),
                        (at, Action::Crash(host)),
                        (at + ms, Action::Repair(Pair::App(app))),
                        (at + ms, Action::Bounce(host)),
                    ],
                }
            }
            Step::ChildSilent { ms } => vec![
                (at, Action::Silence(true)),
                (at + ms, Action::Silence(false)),
            ],
            Step::RebootDevice { style, down_ms } => {
                down(Host::Device, style, at, down_ms, &[Pair::Device])
            }
            Step::RebootInBlip {
                app,
                hold_ms,
                at_ms,
            } => vec![
                (at, Action::Hold(Pair::App(app))),
                (at + at_ms, Action::Bounce(Host::Device)),
                (at + hold_ms, Action::Release(Pair::App(app))),
            ],
            Step::CrashProxy { style, down_ms } => {
                down(Host::Proxy, style, at, down_ms, &pairs(apps))
            }
            Step::FailRate { pair, permille, ms } => vec![
                (at, Action::FailRate(pair, permille)),
                (at + ms, Action::FailRate(pair, 0)),
                (at + ms, Action::Repair(pair)),
            ],
        }
    }
}

/// Every link of a case of `apps` apps, the device's first.
pub(super) fn pairs(apps: u8) -> Vec<Pair> {
    std::iter::once(Pair::Device)
        .chain((0..apps).map(Pair::App))
        .collect()
}

/// Whether a blip of `ms` on `pair` is within its margin with `latency`, as
/// M1 says.
pub(super) fn within_margin(pair: Pair, ms: u64, latency: u64) -> bool {
    match pair {
        Pair::Device => ms + 300 + latency < 3_000,
        Pair::App(_) => ms + latency < 3_000,
    }
}

/// The actions that take `host` down at `at` for `down_ms` by `style`. A
/// power cut cuts `pairs` first.
fn down(host: Host, style: Down, at: u64, down_ms: u64, pairs: &[Pair]) -> Vec<(u64, Action)> {
    if style == Down::Kill && down_ms == 0 {
        return vec![(at, Action::Bounce(host))];
    }
    let mut actions = Vec::new();
    if style == Down::PowerCut {
        actions.extend(pairs.iter().map(|&pair| (at, Action::Cut(pair))));
    }
    actions.push((at, Action::Crash(host)));
    if style == Down::PowerCut {
        actions.extend(
            pairs
                .iter()
                .map(|&pair| (at + down_ms, Action::Repair(pair))),
        );
    }
    actions.push((at + down_ms, Action::Bounce(host)));
    actions
}

/// Which step kinds a case's schedule draws from, `Wait` aside.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Kinds {
    slow_link: bool,
    blip: bool,
    cut: bool,
    cut_one_way: bool,
    app_away: bool,
    child_silent: bool,
    reboot_device: bool,
    reboot_in_blip: bool,
    crash_proxy: bool,
    fail_rate: bool,
}

impl Kinds {
    /// The kinds `on` enables, in the order of [`Step`].
    fn of(on: [bool; 10]) -> Kinds {
        let [slow_link, blip, cut, cut_one_way, app_away, child_silent, reboot_device, reboot_in_blip, crash_proxy, fail_rate] =
            on;
        Kinds {
            slow_link,
            blip,
            cut,
            cut_one_way,
            app_away,
            child_silent,
            reboot_device,
            reboot_in_blip,
            crash_proxy,
            fail_rate,
        }
    }
}

/// What a case draws besides its schedule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Swarm {
    pub(super) apps: u8,
    pub(super) reconnect_timeout: bool,
    pub(super) child: bool,
    pub(super) withheld: bool,
    pub(super) random_order: bool,
    /// The largest latency of every link, in milliseconds.
    pub(super) latency: u64,
    pub(super) tcp_capacity: usize,
    pub(super) kinds: Kinds,
}

/// One generated case.
#[derive(Clone, Debug)]
pub(super) struct Case {
    /// The seed that drew it, and that seeds its simulation.
    pub(super) seed: u64,
    pub(super) swarm: Swarm,
    /// Each step after its gap, in milliseconds.
    pub(super) schedule: Vec<(u64, Step)>,
}

/// One change the driver makes to the simulation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::proxy::turmoil) enum Action {
    Hold(Pair),
    Release(Pair),
    Cut(Pair),
    Repair(Pair),
    CutOneWay(Way),
    RepairOneWay(Way),
    Latency(Pair, u64),
    FailRate(Pair, u32),
    Crash(Host),
    Bounce(Host),
    Silence(bool),
    Away(u8, bool),
}

/// A schedule laid out in time from the start of the chaos.
#[derive(Debug)]
pub(super) struct Plan {
    /// Each action and when it is due, in order.
    pub(super) actions: Vec<(u64, Action)>,
    /// Each step that runs, with when it starts and ends.
    pub(super) steps: Vec<(Step, u64, u64)>,
    /// When the chaos ends.
    pub(super) heal: u64,
}

/// Lays out `case`'s schedule. A step starts after its gap, and once every
/// step before it that claims what it claims has ended. A step that would
/// start after the chaos is left out, and the heal ends one that runs past
/// it.
pub(super) fn plan(case: &Case) -> Plan {
    let apps = case.swarm.apps;
    let mut free = BTreeMap::<Claim, u64>::new();
    let mut nominal = 0;
    let mut actions = Vec::new();
    let mut steps = Vec::new();
    let mut heal = 0;
    for (gap, step) in &case.schedule {
        nominal += gap;
        let claims = step.claims(apps);
        let start = claims
            .iter()
            .filter_map(|claim| free.get(claim))
            .fold(nominal, |a, &b| a.max(b));
        if start >= CHAOS {
            continue;
        }
        let end = start + step.length();
        for claim in claims {
            free.insert(claim, end);
        }
        let due = step
            .actions(start, apps)
            .into_iter()
            .filter(|(at, _)| *at < CHAOS);
        actions.extend(due);
        steps.push((step.clone(), start, end.min(CHAOS)));
        heal = heal.max(end.min(CHAOS)).max(nominal.min(CHAOS));
    }
    actions.sort_by_key(|(at, _)| *at);
    Plan {
        actions,
        steps,
        heal,
    }
}

// The generator, and the bands of section 4.

/// An app's short outage.
const SHORT_APP_OUTAGE: RangeInclusive<u64> = 300..=20_000;
/// An app's long outage.
const LONG_APP_OUTAGE: RangeInclusive<u64> = 45_000..=60_000;
/// A device's short outage, with a reconnect timeout.
const SHORT_DEVICE_OUTAGE: RangeInclusive<u64> = 300..=3_000;
/// A device's long outage, with a reconnect timeout.
const LONG_DEVICE_OUTAGE: RangeInclusive<u64> = 16_000..=60_000;
/// A device's outage, without a reconnect timeout.
const DEVICE_OUTAGE: RangeInclusive<u64> = 300..=60_000;
/// A slow link's largest latency.
const SLOW: RangeInclusive<u64> = 100..=1_000;
/// A blip.
const BLIP: RangeInclusive<u64> = 100..=LONGEST_BLIP;
/// A hub child's silence.
const SILENCE: RangeInclusive<u64> = 1_000..=20_000;

/// An app's outage, at most 20 s or at least 45 s.
fn app_outage() -> BoxedStrategy<u64> {
    prop_oneof![SHORT_APP_OUTAGE, LONG_APP_OUTAGE].boxed()
}

/// A device's outage, at most 3 s or at least 16 s with a reconnect
/// timeout, and anything from 300 ms to 60 s without one.
fn device_outage(reconnect_timeout: bool) -> BoxedStrategy<u64> {
    match reconnect_timeout {
        true => prop_oneof![SHORT_DEVICE_OUTAGE, LONG_DEVICE_OUTAGE].boxed(),
        false => DEVICE_OUTAGE.boxed(),
    }
}

/// An outage of `pair`.
fn outage(pair: Pair, swarm: &Swarm) -> BoxedStrategy<u64> {
    match pair {
        Pair::Device => device_outage(swarm.reconnect_timeout),
        Pair::App(_) => app_outage(),
    }
}

/// A link of a case of `apps` apps.
fn pair(apps: u8) -> BoxedStrategy<Pair> {
    prop_oneof![Just(Pair::Device), (0..apps).prop_map(Pair::App)].boxed()
}

/// A step of a kind `swarm` enables, simplest first.
fn step(swarm: &Swarm) -> BoxedStrategy<Step> {
    let apps = swarm.apps;
    let app = 0..apps;
    let on = swarm.kinds;
    let mut kinds = vec![Just(Step::Wait).boxed()];
    if on.slow_link {
        let slow = (pair(apps), SLOW);
        kinds.push(
            slow.prop_map(|(pair, max_ms)| Step::SlowLink { pair, max_ms })
                .boxed(),
        );
    }
    if on.blip {
        let blip = (pair(apps), BLIP);
        kinds.push(blip.prop_map(|(pair, ms)| Step::Blip { pair, ms }).boxed());
    }
    if on.cut {
        let swarm = swarm.clone();
        let cut = pair(apps).prop_flat_map(move |pair| (Just(pair), outage(pair, &swarm)));
        kinds.push(cut.prop_map(|(pair, ms)| Step::Cut { pair, ms }).boxed());
    }
    if on.cut_one_way {
        let swarm = swarm.clone();
        let way = (pair(apps), any::<bool>()).prop_map(|(pair, to_proxy)| pair.way(to_proxy));
        let cut = way.prop_flat_map(move |way| (Just(way), outage(way.pair, &swarm)));
        kinds.push(
            cut.prop_map(|(way, ms)| Step::CutOneWay { way, ms })
                .boxed(),
        );
    }
    if on.app_away {
        let style = prop_oneof![Just(Away::Close), Just(Away::Kill), Just(Away::CutThenKill)];
        let away = (app.clone(), style, app_outage());
        kinds.push(
            away.prop_map(|(app, style, ms)| Step::AppAway { app, style, ms })
                .boxed(),
        );
    }
    if on.child_silent {
        kinds.push(SILENCE.prop_map(|ms| Step::ChildSilent { ms }).boxed());
    }
    if on.reboot_device {
        let style = prop_oneof![Just(Down::Kill), Just(Down::PowerCut)];
        let down = prop_oneof![Just(0u64), device_outage(swarm.reconnect_timeout)];
        let reboot = (style, down).prop_map(|(style, down_ms)| Step::RebootDevice {
            style,
            down_ms: lasting(style, down_ms),
        });
        kinds.push(reboot.boxed());
    }
    if on.reboot_in_blip && swarm.tcp_capacity == 4 {
        let blip = (app.clone(), 1_000u64..=LONGEST_BLIP)
            .prop_flat_map(|(app, hold_ms)| (Just(app), Just(hold_ms), 0..=hold_ms / 2));
        let reboot = blip.prop_map(|(app, hold_ms, at_ms)| Step::RebootInBlip {
            app,
            hold_ms,
            at_ms,
        });
        kinds.push(reboot.boxed());
    }
    if on.crash_proxy {
        let style = prop_oneof![Just(Down::Kill), Just(Down::PowerCut)];
        let down = prop_oneof![Just(0u64), app_outage()];
        let crash = (style, down).prop_map(|(style, down_ms)| Step::CrashProxy {
            style,
            down_ms: lasting(style, down_ms),
        });
        kinds.push(crash.boxed());
    }
    if on.fail_rate {
        let swarm = swarm.clone();
        let failing = (pair(apps), 50u32..=500).prop_flat_map(move |(pair, permille)| {
            (Just(pair), Just(permille), outage(pair, &swarm))
        });
        let failing =
            failing.prop_map(|(pair, permille, ms)| Step::FailRate { pair, permille, ms });
        kinds.push(failing.boxed());
    }
    Union::new(kinds).boxed()
}

/// The down time `down_ms` drew, made at least 300 ms for a power cut, as
/// only a kill has no down time.
fn lasting(style: Down, down_ms: u64) -> u64 {
    match style {
        Down::PowerCut => down_ms.max(300),
        Down::Kill => down_ms,
    }
}

/// The gap before a step, under 500 ms for a third of them, so that steps
/// land inside opens.
fn gap() -> BoxedStrategy<u64> {
    prop_oneof![1 => 0u64..=500, 2 => 0u64..=10_000].boxed()
}

/// A case's swarm parameters.
fn swarm() -> impl Strategy<Value = Swarm> {
    let apps = prop_oneof![1 => Just(1u8), 2 => 2u8..=3];
    let latency = prop_oneof![Just(100u64), Just(500u64)];
    let capacity = prop_oneof![Just(64usize), Just(4usize)];
    let kinds = proptest::array::uniform10(any::<bool>());
    let drawn = (
        apps,
        any::<bool>(),
        any::<bool>(),
        proptest::bool::weighted(0.125),
        any::<bool>(),
        latency,
        capacity,
        kinds,
    );
    drawn.prop_map(
        |(apps, reconnect_timeout, child, withheld, random_order, latency, tcp_capacity, kinds)| {
            Swarm {
                apps,
                reconnect_timeout,
                child,
                withheld,
                random_order,
                latency,
                tcp_capacity,
                kinds: Kinds::of(kinds),
            }
        },
    )
}

/// A case's swarm parameters and schedule.
pub(super) fn drawn() -> impl Strategy<Value = (Swarm, Vec<(u64, Step)>)> {
    swarm().prop_flat_map(|swarm| {
        let schedule = proptest::collection::vec((gap(), step(&swarm)), 0..=12);
        (Just(swarm), schedule)
    })
}

/// The generator of the case of `seed`, which draws the same case each
/// time.
pub(super) fn runner(seed: u64, shrinks: u32) -> TestRunner {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    let config = Config {
        cases: 1,
        failure_persistence: None,
        max_shrink_iters: shrinks,
        ..Config::default()
    };
    TestRunner::new_with_rng(config, TestRng::from_seed(RngAlgorithm::ChaCha, &bytes))
}

/// The case the seed `seed` draws, before any shrinking.
pub(super) fn drawn_case(seed: u64) -> Case {
    let mut runner = runner(seed, 0);
    let (swarm, schedule) = drawn().new_tree(&mut runner).expect("a case").current();
    Case {
        seed,
        swarm,
        schedule,
    }
}

/// Checks V1 to V4 of `case`.
pub(super) fn valid(case: &Case) -> Result<(), String> {
    let swarm = &case.swarm;
    for (_, step) in &case.schedule {
        if !in_band(step, swarm) {
            return Err(format!("V3 the step {step:?} lies outside its band"));
        }
    }
    let mut busy = BTreeSet::new();
    let mut down = BTreeSet::new();
    for &(at, action) in &plan(case).actions {
        if at >= CHAOS {
            return Err(format!(
                "V4 the action {action:?} at {at} ms comes after the chaos"
            ));
        }
        match action {
            Action::Hold(pair)
            | Action::Cut(pair)
            | Action::FailRate(pair, 1..)
            | Action::CutOneWay(Way { pair, .. }) => {
                if !busy.insert(pair) {
                    return Err(format!(
                        "V1 the action {action:?} at {at} ms overlaps another on its link"
                    ));
                }
            }
            Action::Release(pair)
            | Action::Repair(pair)
            | Action::FailRate(pair, 0)
            | Action::RepairOneWay(Way { pair, .. }) => {
                busy.remove(&pair);
            }
            Action::Crash(host) => {
                if !down.insert(host) {
                    return Err(format!(
                        "V2 the action {action:?} at {at} ms crashes a host that is down"
                    ));
                }
            }
            Action::Bounce(host) => {
                down.remove(&host);
            }
            _ => {}
        }
    }
    Ok(())
}

/// Whether the durations of `step` lie in their bands for `swarm`.
fn in_band(step: &Step, swarm: &Swarm) -> bool {
    let app = |ms: u64| SHORT_APP_OUTAGE.contains(&ms) || LONG_APP_OUTAGE.contains(&ms);
    let device = |ms: u64| match swarm.reconnect_timeout {
        true => SHORT_DEVICE_OUTAGE.contains(&ms) || LONG_DEVICE_OUTAGE.contains(&ms),
        false => DEVICE_OUTAGE.contains(&ms),
    };
    let outage = |pair: Pair, ms: u64| match pair {
        Pair::Device => device(ms),
        Pair::App(_) => app(ms),
    };
    match *step {
        Step::Wait => true,
        Step::SlowLink { max_ms, .. } => SLOW.contains(&max_ms),
        Step::Blip { ms, .. } => BLIP.contains(&ms),
        Step::Cut { pair, ms } | Step::FailRate { pair, ms, .. } => outage(pair, ms),
        Step::CutOneWay { way, ms } => outage(way.pair, ms),
        Step::AppAway { ms, .. } => app(ms),
        Step::ChildSilent { ms } => SILENCE.contains(&ms),
        Step::RebootDevice { style, down_ms } => {
            (style == Down::Kill && down_ms == 0) || device(down_ms)
        }
        Step::RebootInBlip { hold_ms, at_ms, .. } => BLIP.contains(&hold_ms) && at_ms < hold_ms,
        Step::CrashProxy { style, down_ms } => {
            (style == Down::Kill && down_ms == 0) || app(down_ms)
        }
    }
}

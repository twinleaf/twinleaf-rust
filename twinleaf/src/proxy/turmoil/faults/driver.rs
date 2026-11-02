//! The driver, which runs a case through its phases, and the check that
//! runs a case and judges it.

use super::classes::classes;
use super::hosts::{self, Control, Event, Logged, Seen};
use super::laws::{Broken, Judge, OPEN};
use super::model::Model;
use super::report::dump;
use super::schedule::{pairs, plan, Action, Case, Host};
use super::{APPS, BOUND, DURATION, RECONNECT, SETTLE, WARM_UP, WINDOW};
use crate::proxy::turmoil::{device, traced, Traced, Tracer, World, SPIN_LIMIT};
use crate::seam::turmoil::seeded;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

/// What a run of a case left.
pub(super) struct Run {
    pub(super) log: Vec<Logged>,
    /// When the warm up ended.
    pub(super) warmed: u64,
    /// When the chaos was healed, `H`.
    pub(super) healed: u64,
    /// `H'`.
    pub(super) settled: u64,
    /// When the run ended.
    pub(super) end: u64,
}

impl Run {
    /// Whether some app of the `apps` opened twice, or came to the proxy as
    /// a second client.
    pub(super) fn reopened(&self, apps: u8) -> bool {
        (0..apps).any(|app| {
            let opened = self
                .log
                .iter()
                .filter(|logged| logged.event == Event::App(app, Seen::Opened))
                .count();
            let clients: BTreeSet<u64> = self
                .log
                .iter()
                .filter_map(|logged| match &logged.event {
                    Event::Proxy(stats) => Some(stats.clients_of(app).collect::<Vec<_>>()),
                    _ => None,
                })
                .flatten()
                .collect();
            opened > 1 || clients.len() > 1
        })
    }
}

/// How long the proxy host's counts hold before the warm up ends, in
/// milliseconds.
const STEADY: u64 = 1_000;

/// Why a case was not judged.
#[derive(Debug)]
pub(super) enum Unjudged {
    /// The warm up never ended.
    Cold,
    /// The harness broke its own bound.
    Harness(String),
    /// A step failed.
    Step(String),
}

/// Builds the simulation of `case` with its hosts.
fn simulation<'a>(case: &Case, world: &Arc<World>, control: &Arc<Control>) -> ::turmoil::Sim<'a> {
    Tracer::install();
    let swarm = &case.swarm;
    world
        .withholds_metadata
        .store(swarm.withheld, Ordering::SeqCst);
    world.has_child.store(swarm.child, Ordering::SeqCst);
    let mut builder = ::turmoil::Builder::new();
    builder
        .simulation_duration(DURATION)
        .rng_seed(case.seed)
        .max_message_latency(Duration::from_millis(swarm.latency))
        .tcp_capacity(swarm.tcp_capacity);
    if swarm.random_order {
        builder.enable_random_order();
    }
    let mut sim = builder.build();
    let serving = world.clone();
    sim.host("device", move || device(serving.clone()));
    let hosting = world.clone();
    let reconnect_timeout = swarm
        .reconnect_timeout
        .then_some(Duration::from_millis(RECONNECT));
    sim.host("proxy", move || {
        hosts::proxy(hosting.clone(), reconnect_timeout)
    });
    let observing = world.clone();
    sim.host("observer", move || hosts::observer(observing.clone()));
    for index in 0..swarm.apps {
        let (world, control, child) = (world.clone(), control.clone(), swarm.child);
        sim.host(APPS[usize::from(index)], move || {
            hosts::app(world.clone(), control.clone(), index, child)
        });
    }
    sim
}

/// A simulation, with what its hosts share and the hosts it has taken
/// down.
struct Driver<'a> {
    sim: ::turmoil::Sim<'a>,
    world: Arc<World>,
    control: Arc<Control>,
    down: BTreeSet<Host>,
}

impl Driver<'_> {
    /// The virtual time in milliseconds.
    fn now(&self) -> u64 {
        self.sim.elapsed().as_millis() as u64
    }

    /// Takes one step.
    fn step(&mut self) -> Result<(), Unjudged> {
        self.sim
            .step()
            .map(drop)
            .map_err(|error| Unjudged::Step(error.to_string()))
    }

    /// Steps until the virtual time reaches `at`.
    fn step_until(&mut self, at: u64) -> Result<(), Unjudged> {
        while self.now() < at {
            self.step()?;
        }
        Ok(())
    }

    /// Writes `event` to the log now.
    fn write(&self, event: Event) {
        self.world.write_at(self.sim.elapsed(), event);
    }

    /// Whether each of the first `apps` apps is warm.
    fn warm(&self, apps: usize) -> bool {
        self.control.warm[..apps]
            .iter()
            .all(|warm| warm.load(Ordering::SeqCst))
    }

    /// Whether each app of `case` has the tables of its last connection
    /// `Ok`, and the proxy host's counts have held for [`STEADY`].
    fn steady(&self, case: &Case) -> bool {
        let now = self.now();
        let log = crate::chan::lock(&self.world.log);
        let counted = log.iter().rev().find_map(|logged| match logged.event {
            Event::Proxy(_) => Some(logged.at),
            _ => None,
        });
        if counted.is_none_or(|at| at + STEADY > now) {
            return false;
        }
        let routes: &[bool] = match case.swarm.child {
            true => &[false, true],
            false => &[false],
        };
        (0..case.swarm.apps).all(|app| {
            let opened = log
                .iter()
                .rposition(|logged| logged.event == Event::App(app, Seen::Opened));
            let Some(opened) = opened else {
                return false;
            };
            routes.iter().all(|&route| {
                let table = log[opened..]
                    .iter()
                    .rev()
                    .find_map(|logged| match &logged.event {
                        Event::App(of, Seen::Table { child, table })
                            if *of == app && *child == route =>
                        {
                            Some(table)
                        }
                        _ => None,
                    });
                matches!(table, Some(Ok(_)))
            })
        })
    }

    /// Applies `action` and writes it to the log.
    fn apply(&mut self, action: Action) {
        let sim = &mut self.sim;
        match action {
            Action::Hold(pair) => sim.hold(pair.far(), "proxy"),
            Action::Release(pair) => sim.release(pair.far(), "proxy"),
            Action::Cut(pair) => sim.partition(pair.far(), "proxy"),
            Action::Repair(pair) => sim.repair(pair.far(), "proxy"),
            Action::CutOneWay(way) => {
                let (from, to) = way.ends();
                sim.partition_oneway(from, to);
            }
            Action::RepairOneWay(way) => {
                let (from, to) = way.ends();
                sim.repair_oneway(from, to);
            }
            Action::Latency(pair, ms) => {
                sim.set_link_max_message_latency(pair.far(), "proxy", Duration::from_millis(ms));
            }
            Action::FailRate(pair, permille) => {
                sim.set_link_fail_rate(pair.far(), "proxy", f64::from(permille) / 1000.0);
            }
            Action::Crash(host) => {
                sim.crash(host.name());
                self.down.insert(host);
            }
            Action::Bounce(host) => {
                sim.bounce(host.name());
                self.down.remove(&host);
            }
            Action::Silence(silent) => self.world.silence_child(silent),
            Action::Away(app, away) => {
                self.control.away[usize::from(app)].store(away, Ordering::SeqCst);
            }
        }
        self.write(Event::Did(action));
    }

    /// Heals everything `case`'s schedule did.
    fn heal(&mut self, case: &Case) {
        let pairs = pairs(case.swarm.apps);
        for &pair in &pairs {
            self.apply(Action::Release(pair));
        }
        for &pair in &pairs {
            self.apply(Action::Repair(pair));
        }
        for &pair in &pairs {
            self.apply(Action::FailRate(pair, 0));
            self.apply(Action::Latency(pair, case.swarm.latency));
        }
        self.apply(Action::Silence(false));
        for app in 0..case.swarm.apps {
            if self.control.away[usize::from(app)].load(Ordering::SeqCst) {
                self.apply(Action::Away(app, false));
            }
        }
        for host in self.down.clone() {
            self.apply(Action::Bounce(host));
        }
    }
}

/// Runs `case` through its phases on a thread of its own, whose randomness
/// follows the case's seed, and returns its log.
pub(super) fn simulate(case: &Case) -> Result<Run, Unjudged> {
    seeded(case.seed, || run(case))
}

/// Runs `case` as [`simulate`] does, and returns the events turmoil made
/// besides its log.
pub(super) fn simulate_traced(case: &Case) -> (Vec<Traced>, Result<Run, Unjudged>) {
    seeded(case.seed, || {
        let mut ran = None;
        let trace = traced(|| ran = Some(run(case)));
        (trace, ran.expect("the case ran"))
    })
}

/// Runs `case` through its phases on this thread, and returns its log.
fn run(case: &Case) -> Result<Run, Unjudged> {
    let world = Arc::new(World::default());
    let control = Arc::new(Control::default());
    let mut driver = Driver {
        sim: simulation(case, &world, &control),
        world,
        control,
        down: BTreeSet::new(),
    };
    let began = format!("seed {} {:?}", case.seed, case.swarm);
    driver.world.write_at(Duration::ZERO, Event::Began(began));
    while !driver.warm(usize::from(case.swarm.apps)) {
        if driver.now() >= WARM_UP {
            return Err(Unjudged::Cold);
        }
        driver.step()?;
    }
    // The warm up ends once every app has its tables and the proxy's counts
    // have held, as the proxy starts tasks while it loads the tables.
    while !driver.steady(case) {
        if driver.now() >= WARM_UP {
            return Err(Unjudged::Cold);
        }
        driver.step()?;
    }
    let warmed = driver.now();
    driver.write(Event::WarmedUp);
    let plan = plan(case);
    for &(at, action) in &plan.actions {
        driver.step_until(warmed + at)?;
        driver.apply(action);
    }
    driver.step_until(warmed + plan.heal)?;
    driver.heal(case);
    let healed = driver.now();
    driver.write(Event::Healed);
    let settled = loop {
        let now = driver.now();
        if now.is_multiple_of(100) {
            let log = crate::chan::lock(&driver.world.log);
            match Model::of(case, &log).taint_ends() {
                Some(end) if end <= now => break end.max(healed),
                _ if now > healed + SETTLE => {
                    let run = Run {
                        log: log.clone(),
                        warmed,
                        healed,
                        settled: now,
                        end: now,
                    };
                    drop(log);
                    if std::env::var("TWINLEAF_TURMOIL_DUMP").is_ok() {
                        dump(case, &run);
                    }
                    let mut why = format!("a tainted connection, or one the device served after the proxy let it go, outlived the heal by {SETTLE} ms");
                    if let Some(silent) = Model::of(case, &run.log).unheard(run.end) {
                        why += &format!(", the proxy kept a connection from {silent} ms on which it never heard the device");
                    }
                    return Err(Unjudged::Harness(why));
                }
                _ => {}
            }
        }
        driver.step()?;
    };
    let end = settled + BOUND + WINDOW;
    driver.step_until(end)?;
    let log = crate::chan::lock(&driver.world.log).clone();
    Ok(Run {
        log,
        warmed,
        healed,
        settled,
        end,
    })
}

/// What one run of a case gave.
pub(super) struct Checked {
    pub(super) broken: Vec<Broken>,
    pub(super) classes: BTreeSet<&'static str>,
    /// Why the harness could not judge the case.
    pub(super) unjudged: Option<String>,
    pub(super) real: Duration,
}

/// Runs `f`, and ends the test process if it takes longer than
/// [`SPIN_LIMIT`] of real time, as a host that spins never gives a step
/// back.
fn watched<T>(f: impl FnOnce() -> T) -> T {
    let (done, finished) = std::sync::mpsc::channel::<()>();
    let watchdog = std::thread::spawn(move || {
        let waited = finished.recv_timeout(SPIN_LIMIT);
        if waited == Err(std::sync::mpsc::RecvTimeoutError::Timeout) {
            use std::io::Write;
            let spun = format!("a turmoil case spun for {SPIN_LIMIT:?} of real time");
            let _ = writeln!(std::io::stderr(), "{spun}");
            std::process::exit(101);
        }
    });
    let given = f();
    let _ = done.send(());
    watchdog.join().expect("the watchdog");
    given
}

/// Runs `case` and judges it.
pub(super) fn check(case: &Case) -> Checked {
    let started = std::time::Instant::now();
    let simulated =
        watched(|| std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| simulate(case))));
    let real = started.elapsed();
    let broke = |law: &'static str, what: String| Checked {
        broken: vec![Broken { law, at: 0, what }],
        classes: BTreeSet::new(),
        unjudged: None,
        real,
    };
    let unjudged = |why: String| Checked {
        broken: Vec::new(),
        classes: BTreeSet::new(),
        unjudged: Some(why),
        real,
    };
    match simulated {
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()));
            broke("I1", format!("a host panicked with {message:?}"))
        }
        Ok(Err(Unjudged::Cold)) => broke("L1", "the warm up did not end within 15 s".to_string()),
        Ok(Err(Unjudged::Harness(why))) => unjudged(why),
        // turmoil fails a step with `server socket buffer full` when more
        // connects reach a listener than its backlog of `tcp_capacity`
        // holds, which I1 counts as a harness error.
        Ok(Err(Unjudged::Step(why))) if why.contains("buffer full") => unjudged(why),
        Ok(Err(Unjudged::Step(why))) => broke("I1", format!("a step failed with {why}")),
        Ok(Ok(run)) => {
            if std::env::var("TWINLEAF_TURMOIL_DUMP").is_ok() {
                dump(case, &run);
            }
            let judge = Judge::new(case, &run);
            let classes = classes(&judge);
            let mut broken = judge.judge();
            let first = broken.iter_mut().find(|broken| !OPEN.contains(&broken.law));
            if let (Some(first), Some(silent)) = (first, judge.model.unheard(run.end)) {
                first.what += &format!("\nthe proxy kept a connection from {silent} ms on which it never heard the device");
            }
            Checked {
                broken,
                classes,
                unjudged: None,
                real,
            }
        }
    }
}

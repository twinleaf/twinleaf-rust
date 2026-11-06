//! Scenarios over turmoil's simulated network. A device, the proxy and an
//! app each run on a host of their own, on a virtual clock, and the test
//! cuts, delays and restarts the links between them. The device is the
//! one of `fake_device`, over TCP, and samples once a second. The scenarios
//! are in `scenarios`, and the generated fault schedules in `faults`.
//!
//! Each scenario runs with several seeds, each on a thread of its own. A
//! seed sets its messages' delays and every random number the run draws, as
//! `seam/turmoil` says, so a scenario run twice with one seed runs the same
//! way, as one test checks. `TWINLEAF_TURMOIL_SEED` runs one seed alone.

#[cfg(not(tokio_unstable))]
compile_error!(
    "the turmoil tests need `--cfg tokio_unstable`, under which turmoil seeds each host's runtime"
);

mod fake_device;
mod faults;
mod scenarios;
mod trace;

use fake_device::device;
use trace::{traced, Traced, Tracer};

use crate::data::{Change, SampleBatch};
use crate::device::{Connection, Device};
use crate::proto::DeviceRoute;
use crate::proxy::{server, HostOptions, Listen};
use crate::seam::turmoil::{polls, seeded};
use crate::session::ProxyAddress;
use crate::session::Purpose;
use crate::session::{Presence, Receiver, RecvError};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout, Instant};

const DEVICE_PORT: u16 = 7855;
const PROXY_PORT: u16 = 7856;
const TABLE_HASH: u32 = 0x5157_0001;

/// How much the hub child's session and table hash grow at each of its
/// reboots.
const CHILD_REBOOT_STEP: u32 = 0x100;

/// How long one step of a simulation may take in real time. A step takes
/// well under a millisecond. A spawned task that spins fails a run at once,
/// as [`polls`] says, and a host's own future that spins never returns from
/// its step.
const SPIN_LIMIT: Duration = Duration::from_secs(60);

/// What the hosts of one scenario share, and how its device behaves.
#[derive(Default)]
struct World {
    /// How many times the device has started. Its session is 7 on its first
    /// boot and one more on each restart.
    boots: AtomicU32,
    /// Set for a device that never sends its metadata, so the proxy keeps
    /// asking for it.
    withholds_metadata: AtomicBool,
    /// Set for a device that has a hub child at `/1`, which heartbeats with
    /// its own session.
    has_child: AtomicBool,
    /// Set while the hub child sends nothing.
    child_silent: AtomicBool,
    /// Every RPC the device was asked, by route and name.
    calls: Mutex<Vec<(DeviceRoute, Vec<u8>)>>,
    /// What the app has reached, for the test to act on between steps.
    phase: AtomicU8,
    /// Set once the proxy listens, which the app waits for before it opens.
    proxy_listens: AtomicBool,
    /// How many connections the device accepted.
    accepted: AtomicU32,
    /// How many times the hub child has rebooted, once at the end of each of
    /// its silences.
    child_reboots: AtomicU32,
    /// What the hosts and the driver of a case saw, in the order they saw
    /// it, as the checker of `faults` reads it.
    log: Mutex<Vec<faults::Logged>>,
}

impl World {
    fn reach(&self, phase: u8) {
        self.phase.store(phase, Ordering::SeqCst);
    }

    /// Silences the hub child, or lets it be heard again. A child heard
    /// again after a silence has rebooted, as a silence stands for its
    /// reboot.
    fn silence_child(&self, silent: bool) {
        if self.child_silent.swap(silent, Ordering::SeqCst) && !silent {
            self.child_reboots.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Writes `event` to the log at the virtual time of the host that saw
    /// it.
    fn write(&self, event: faults::Event) {
        let at = ::turmoil::sim_elapsed().unwrap_or_default();
        self.write_at(at, event);
    }

    /// Writes `event` to the log at `at`, as the driver does between steps.
    fn write_at(&self, at: Duration, event: faults::Event) {
        let at = at.as_millis() as u64;
        crate::chan::lock(&self.log).push(faults::Logged { at, event });
    }

    /// How many of the RPCs that load a table `route` was asked.
    fn table_calls(&self, route: DeviceRoute) -> usize {
        let calls = crate::chan::lock(&self.calls);
        let loads =
            |(at, name): &&(DeviceRoute, Vec<u8>)| *at == route && name.starts_with(b"rpc.");
        calls.iter().filter(loads).count()
    }
}

fn child() -> DeviceRoute {
    "/1".parse().expect("a route")
}

/// Hosts the proxy on [`PROXY_PORT`] until its host stops.
async fn proxy(world: Arc<World>) -> ::turmoil::Result {
    let options = HostOptions {
        listen: Listen::AllInterfaces,
        port: PROXY_PORT,
        name: "simulated".to_string(),
        ..HostOptions::default()
    };
    let _proxy = server::host(options).await?;
    world.proxy_listens.store(true, Ordering::SeqCst);
    std::future::pending::<()>().await;
    Ok(())
}

/// How many seeds each scenario runs with.
const SEEDS: u64 = 8;

/// The seeds a scenario runs with, `TWINLEAF_TURMOIL_SEED` alone when it is
/// set, and otherwise 1 to [`SEEDS`].
fn seeds() -> Vec<u64> {
    match std::env::var("TWINLEAF_TURMOIL_SEED") {
        Ok(seed) => vec![seed.parse().expect("a seed")],
        Err(_) => (1..=SEEDS).collect(),
    }
}

/// Runs `scenario` with each of the [`seeds`], each run on a thread of its
/// own whose randomness follows its seed. A run that fails names its seed.
fn each_seed(scenario: impl Fn(u64) + Sync) {
    for seed in seeds() {
        let _seed = Seed(seed);
        seeded(seed, || scenario(seed));
    }
}

/// Names the seed of a scenario that fails, so it can be run alone.
struct Seed(u64);

impl Drop for Seed {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("the scenario failed with TWINLEAF_TURMOIL_SEED={}", self.0);
        }
    }
}

/// A simulation of `world` with the device and the proxy on their hosts,
/// whose network follows `seed`. It installs the [`Tracer`].
fn simulation(world: &Arc<World>, seed: u64) -> ::turmoil::Sim<'static> {
    Tracer::install();
    let mut sim = ::turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(120))
        .rng_seed(seed)
        .build();
    let serving = world.clone();
    sim.host("device", move || device(serving.clone()));
    let hosting = world.clone();
    sim.host("proxy", move || proxy(hosting.clone()));
    sim
}

/// Steps `sim` until the app reaches `phase`.
fn step_until(sim: &mut ::turmoil::Sim<'_>, world: &World, phase: u8) {
    while world.phase.load(Ordering::SeqCst) < phase {
        assert!(
            !sim.step().expect("a step"),
            "the app finished before phase {phase}"
        );
    }
}

/// A watch from a thread of its own on the steps of a simulation. It ends
/// the test process when one step takes longer than [`SPIN_LIMIT`] of real
/// time, as a host's own future that spins never gives its step back, and
/// says how to run the simulation alone. A run is as slow as the machine it
/// runs on, so the watch bounds each step and not the run. Dropping it ends
/// the watch.
struct Watchdog {
    stepped: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Watchdog {
    /// Watches a simulation that `rerun`, set in the environment, runs alone.
    fn new(rerun: String) -> Watchdog {
        let (stepped, steps) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || loop {
            match steps.recv_timeout(SPIN_LIMIT) {
                Ok(()) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // The test harness captures `eprintln!`, and loses what
                    // it holds when the process exits.
                    use std::io::Write;
                    let spun = format!(
                        "a turmoil step spun for {SPIN_LIMIT:?} of real time, run it alone with {rerun}"
                    );
                    let _ = writeln!(std::io::stderr(), "{spun}");
                    std::process::exit(101);
                }
            }
        });
        Watchdog {
            stepped: Some(stepped),
            thread: Some(thread),
        }
    }

    /// Takes one step of `sim`, and returns whether the simulation ended.
    fn step(&self, sim: &mut ::turmoil::Sim<'_>) -> ::turmoil::Result<bool> {
        let ended = sim.step();
        if let Some(stepped) = &self.stepped {
            let _ = stepped.send(());
        }
        ended
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        drop(self.stepped.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Runs `sim`, the scenario of `seed`, to its end under a [`Watchdog`].
fn run_to_end(sim: &mut ::turmoil::Sim<'_>, seed: u64) {
    let watchdog = Watchdog::new(format!("TWINLEAF_TURMOIL_SEED={seed}"));
    while !watchdog.step(sim).expect("a step") {}
}

/// Opens the device through the proxy, as an app does, once the proxy
/// listens.
async fn open(world: &World) -> std::io::Result<Connection> {
    while !world.proxy_listens.load(Ordering::SeqCst) {
        sleep(Duration::from_millis(10)).await;
    }
    let proxy = ProxyAddress {
        url: format!("tcp://proxy:{PROXY_PORT}"),
        ensure: None,
    };
    let device = format!("tcp://device:{DEVICE_PORT}");
    Connection::open_at_async(&proxy, &device, Purpose::Use, Duration::from_secs(10)).await
}

/// Waits up to `within` for a presence `matches` accepts, and returns how
/// long it took.
async fn wait_for(
    presence: &Receiver<Presence>,
    within: Duration,
    matches: impl Fn(&Presence) -> bool,
) -> Duration {
    let started = Instant::now();
    let waiting = async {
        loop {
            match presence.recv_async().await {
                Ok(seen) if matches(&seen) => return,
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(error) => panic!("the presence ended with {error:?}"),
            }
        }
    };
    match timeout(within, waiting).await {
        Ok(()) => started.elapsed(),
        Err(_) => panic!("no matching presence within {within:?}"),
    }
}

fn alive(presence: &Presence) -> bool {
    matches!(presence, Presence::Alive { .. })
}

fn lost(presence: &Presence) -> bool {
    matches!(presence, Presence::Lost { .. })
}

/// Asserts the device answers an RPC within 5 s, asking again for up to
/// 40 s.
async fn answers(device: &Device) {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        match timeout(Duration::from_secs(5), device.submit("dev.name", &[])).await {
            Ok(Ok(name)) => return assert_eq!(name, b"FAKE"),
            other => assert!(Instant::now() < deadline, "no answer: {other:?}"),
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// What an app's samples showed, checked as they arrive.
#[derive(Default)]
struct Watched {
    /// The session and the last sample number of the last batch.
    last: Option<(u32, u32)>,
    /// Each boundary after the first batch.
    boundaries: Vec<String>,
    /// The samples the boundaries counted as lost.
    lost: u64,
}

impl Watched {
    /// Takes every batch queued on `samples`. A gap in a session's sample
    /// numbers comes with a loss that counts it exactly, and a new session
    /// says so.
    fn take(&mut self, samples: &Receiver<SampleBatch>) {
        loop {
            let batch = match samples.try_recv() {
                Ok(Some(batch)) => batch,
                Ok(None) => return,
                Err(RecvError::Lagged(_)) => continue,
                Err(error) => panic!("the samples ended with {error:?}"),
            };
            let session = batch.metadata().device().session().value();
            let numbers = batch.sample_numbers();
            let (first, last) = (numbers[0].value(), numbers[numbers.len() - 1].value());
            let boundary = batch.boundary();
            match self.last {
                Some((was, before)) if was == session && first != before + 1 => {
                    let counted = boundary
                        .and_then(|b| b.loss.as_ref())
                        .map(|loss| loss.samples);
                    let gap = u64::from(first - before - 1);
                    assert_eq!(
                        counted,
                        Some(gap),
                        "samples {before} to {first}: {boundary:?}"
                    );
                    self.lost += gap;
                }
                Some((was, _)) if was != session => {
                    let change = boundary.and_then(|b| b.change.as_ref());
                    let changed = matches!(change, Some(Change::SessionChanged { .. }));
                    assert!(changed, "session {was} to {session}: {boundary:?}");
                }
                _ => {}
            }
            if self.last.is_some() && boundary.is_some() {
                self.boundaries.push(format!("{boundary:?}"));
            }
            self.last = Some((session, last));
        }
    }

    /// Watches `samples` for `period`, checking each batch.
    async fn watch(&mut self, samples: &Receiver<SampleBatch>, period: Duration) {
        let until = Instant::now() + period;
        while Instant::now() < until {
            self.take(samples);
            sleep(Duration::from_millis(100)).await;
        }
        self.take(samples);
    }
}

/// An app's open device, its presence and its samples.
struct App {
    _connection: Connection,
    device: Device,
    presence: Receiver<Presence>,
    samples: Receiver<SampleBatch>,
    watched: Watched,
}

impl App {
    /// Opens the device, and waits until it is alive and its samples flow.
    async fn open(world: &World) -> std::io::Result<App> {
        let connection = open(world).await?;
        let device = connection.device(DeviceRoute::root());
        let presence = device.presence_updates();
        let samples = connection.samples();
        let mut app = App {
            device,
            presence,
            samples,
            watched: Watched::default(),
            _connection: connection,
        };
        wait_for(&app.presence, Duration::from_secs(10), alive).await;
        app.watch(Duration::from_secs(5)).await;
        assert!(app.watched.last.is_some(), "no samples in 5 s");
        Ok(app)
    }

    async fn watch(&mut self, period: Duration) {
        self.watched.watch(&self.samples, period).await;
    }
}

/// How many seeds the determinism check runs its scenario twice with.
const REPLAYED: usize = 4;

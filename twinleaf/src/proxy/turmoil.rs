//! Scenarios over turmoil's simulated network. A device, the proxy and an
//! app each run on a host of their own, on a virtual clock, and the test
//! cuts, delays and restarts the links between them. The device is the
//! fake device of the proxy's fixtures, over TCP, and samples once a second.
//!
//! Each scenario runs with several seeds. A seed sets its messages' delays,
//! the order each host's `select!` tries its branches in, and what
//! `crate::rand` draws, so a scenario run twice with one seed runs the same
//! way, as one test checks. `TWINLEAF_TURMOIL_SEED` runs one seed alone.
//! The command is in `crate::net`.

#[cfg(not(tokio_unstable))]
compile_error!(
    "the turmoil tests need `--cfg tokio_unstable`, under which turmoil seeds each host's runtime"
);

mod faults;

use super::control::Purpose;
use super::fixtures::answer;
use super::{server, HostOptions, Listen};
use crate::chan::sleep_until;
use crate::data::{Change, SampleBatch};
use crate::device::fixtures::{description, metadata_records};
use crate::device::runtime::ProxyAddress;
use crate::device::{Connection, Device, Presence, Receiver, RecvError};
use crate::net::{TcpListener, TcpStream};
use crate::proto::rpc::Method;
use crate::proto::DeviceRoute;
use crate::tio::packet::{Packet, Payload};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, timeout, Instant};

const DEVICE_PORT: u16 = 7855;
const PROXY_PORT: u16 = 7856;
const TABLE_HASH: u32 = 0x5157_0001;

/// How much the hub child's session and table hash grow at each of its
/// reboots.
const CHILD_REBOOT_STEP: u32 = 0x100;

/// How long a test lets the simulation after its last phase take in real
/// time. The simulation runs in about a second. A spawned task that spins
/// fails it at once, as [`polls`](crate::chan::polls) says, and a host's
/// own future that spins never returns from a step.
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

/// One boot of the device.
struct Boot {
    /// How many boots came before it.
    index: u32,
    /// Its session, 7 on the first boot and one more on each later one.
    session: u32,
    /// When it started.
    born: Instant,
    /// One more than the highest sample number it has sent on any
    /// connection.
    unsent: AtomicU32,
}

impl Boot {
    /// How many times the device at `route` has rebooted in this boot, none
    /// at the root.
    fn reboots(route: DeviceRoute, world: &World) -> u32 {
        match route == DeviceRoute::root() {
            true => 0,
            false => world.child_reboots.load(Ordering::SeqCst),
        }
    }

    /// The session of the device at `route` now. The root's is the boot's,
    /// and the hub child's grows by [`CHILD_REBOOT_STEP`] at each of its
    /// reboots.
    fn session(&self, route: DeviceRoute, world: &World) -> u32 {
        self.session + CHILD_REBOOT_STEP * Self::reboots(route, world)
    }

    /// The hash of the RPC table at `route` now. It changes with each boot,
    /// and at the hub child with each of its reboots, as a device's table
    /// changes only with new firmware, which comes with a new session. So a
    /// table read before either never matches the device after it.
    fn table_hash(&self, route: DeviceRoute, world: &World) -> u32 {
        TABLE_HASH + self.index + CHILD_REBOOT_STEP * Self::reboots(route, world)
    }
}

/// Serves the device on [`DEVICE_PORT`] until its host stops. Like the
/// firmware, it serves one connection at a time. It listens only while it
/// serves none, so a connect that comes while a connection lives is refused.
async fn device(world: Arc<World>) -> ::turmoil::Result {
    let index = world.boots.fetch_add(1, Ordering::SeqCst);
    let boot = Arc::new(Boot {
        index,
        session: 7 + index,
        born: Instant::now(),
        unsent: AtomicU32::new(0),
    });
    world.write(faults::Event::Boot {
        index,
        session: boot.session,
    });
    loop {
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, DEVICE_PORT)).await?;
        let (stream, _) = listener.accept().await?;
        drop(listener);
        let connection = world.accepted.fetch_add(1, Ordering::SeqCst);
        world.write(faults::Event::Accepted);
        serve(stream, boot.clone(), connection, world.clone()).await;
    }
}

/// How long what the device wrote may go unread by its peer before the
/// device drops the connection. The firmware drops a peer that has not
/// acknowledged what it sent for 20 s. turmoil has no acknowledgements, and
/// gives a write's credit back only once the peer reads the segment, so the
/// device counts the 20 s from the last moment its peer read while some of
/// what it wrote was unread. It therefore also drops a peer that
/// acknowledges and stops reading, which the firmware never does. The proxy
/// reads its connection to the device for as long as it holds it, so no
/// case reaches that. The firmware's keepalive of 5 s probes only an idle
/// connection, and the device sends a heartbeat every 300 ms.
const UNACKNOWLEDGED: Duration = Duration::from_secs(20);

/// What the peer of a connection of the device has read of what the device
/// wrote, which the device takes as acknowledged.
struct Acknowledged {
    local: SocketAddr,
    peer: SocketAddr,
    /// How many bytes the device has written.
    written: u64,
    /// How many of them the peer had read when the device last looked.
    read: u64,
    /// When the peer last read, or the device last wrote with nothing
    /// unread.
    since: Instant,
}

impl Acknowledged {
    fn of(local: SocketAddr, peer: SocketAddr) -> Acknowledged {
        Acknowledged {
            local,
            peer,
            written: 0,
            read: 0,
            since: Instant::now(),
        }
    }

    /// Takes in what the peer has read since the device last looked.
    fn look(&mut self) {
        let read = crate::net::bytes_read(self.peer, self.local);
        if read != self.read {
            self.read = read;
            self.since = Instant::now();
        }
    }

    /// Counts `bytes` the device is about to write.
    fn write(&mut self, bytes: usize) {
        self.look();
        if self.read >= self.written {
            self.since = Instant::now();
        }
        self.written += bytes as u64;
    }

    /// When the device drops its peer, unless the peer reads before then,
    /// or never while nothing is unread.
    fn deadline(&mut self) -> Instant {
        self.look();
        match self.written > self.read {
            true => self.since + UNACKNOWLEDGED,
            false => Instant::now() + UNACKNOWLEDGED,
        }
    }

    /// Whether the peer has left what the device wrote unread for
    /// [`UNACKNOWLEDGED`].
    fn given_up(&mut self) -> bool {
        self.look();
        self.written > self.read && self.since + UNACKNOWLEDGED <= Instant::now()
    }
}

/// The device's `dev.metadata` reply in `session`.
fn metadata_reply(session: u32) -> Vec<u8> {
    let mut reply = vec![0u8; crate::proto::data::MAX_METADATA_REPLY_SIZE];
    let records = metadata_records(session);
    let written = records.iter().fold(0, |written, record| {
        let frame = record.write_reply_frame(&mut reply[written..]);
        written + frame.expect("a record that fits")
    });
    reply.truncate(written);
    reply
}

/// Answers the RPCs that arrive on `reading`, the device's connection
/// `connection` in `boot`, through `replies`, until the connection ends.
/// `test.echo` replies with the boot's session as four little endian bytes,
/// then its arguments.
async fn answer_calls(
    mut reading: impl AsyncReadExt + Unpin,
    replies: tokio::sync::mpsc::UnboundedSender<Packet>,
    boot: &Boot,
    connection: u32,
    world: &World,
) {
    let mut received = Vec::new();
    let mut chunk = [0; 512];
    loop {
        let n = match reading.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        received.extend_from_slice(&chunk[..n]);
        while let Ok((packet, used)) = Packet::from_slice_prefix(&received) {
            received.drain(..used);
            let Payload::RpcRequest(request) = packet.payload() else {
                continue;
            };
            let Method::ByName(name) = request.method else {
                continue;
            };
            let route = packet.route();
            crate::chan::lock(&world.calls).push((route, name.to_vec()));
            let withheld = world.withholds_metadata.load(Ordering::SeqCst);
            let hash = boot.table_hash(route, world);
            let value = match name {
                b"dev.session" => Some(boot.session(route, world).to_le_bytes().to_vec()),
                b"dev.metadata" if withheld => None,
                b"dev.metadata" => Some(metadata_reply(boot.session)),
                b"test.echo" => Some([&boot.session.to_le_bytes()[..], request.args].concat()),
                _ => answer(name, request.args, hash),
            };
            world.write(faults::Event::Asked {
                index: boot.index,
                connection,
                child: route != DeviceRoute::root(),
                name: String::from_utf8_lossy(name).into_owned(),
                args: request.args.to_vec(),
                hash: (name == b"rpc.hash").then_some(hash),
            });
            if let Some(value) = value {
                let reply = Packet::rpc_reply(request.id.value(), &value, route);
                let _ = replies.send(reply.expect("a reply that fits"));
            }
        }
    }
}

/// Serves `connection` of `boot`. It gets the device's records, unless the
/// device withholds them, then a heartbeat of the root and of the hub child,
/// each with its session, every 300 ms, and a sample each second. Its first
/// sample number is the larger of the whole seconds since the boot and one
/// more than the highest the boot has sent, and it counts on its own from
/// there. Each sample's one column holds the session. The connection ends
/// at the end of what it reads, at a write that fails, or once its peer has
/// left what it wrote unread for [`UNACKNOWLEDGED`].
async fn serve(stream: TcpStream, boot: Arc<Boot>, connection: u32, world: Arc<World>) {
    let root = DeviceRoute::root();
    let session = boot.session;
    let (Ok(local), Ok(peer)) = (stream.local_addr(), stream.peer_addr()) else {
        return world.write(faults::Event::Ended { connection });
    };
    let mut acknowledged = Acknowledged::of(local, peer);
    let (reading, mut writing) = stream.into_split();
    let (replies, mut queued) = tokio::sync::mpsc::unbounded_channel();
    let (read_end, mut ended) = crate::chan::oneshot::channel::<()>();
    let answering = crate::chan::spawn({
        let (boot, world) = (boot.clone(), world.clone());
        async move {
            answer_calls(reading, replies, &boot, connection, &world).await;
            let _ = read_end.send(());
        }
    });
    let end = || world.write(faults::Event::Ended { connection });
    let records = match world.withholds_metadata.load(Ordering::SeqCst) {
        true => Vec::new(),
        false => description(root, session),
    };
    let mut beat = tokio::time::interval(Duration::from_millis(300));
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut next = (boot.born.elapsed().as_secs() as u32).max(boot.unsent.load(Ordering::SeqCst));
    let mut outgoing = records;
    loop {
        for packet in outgoing.drain(..) {
            acknowledged.write(packet.as_bytes().len());
            loop {
                // A write that waits for credit writes nothing, so one
                // dropped at the deadline is made again in full.
                let deadline = acknowledged.deadline();
                tokio::select! {
                    written = writing.write_all(packet.as_bytes()) => match written {
                        Ok(()) => break,
                        Err(_) => {
                            end();
                            return answering.abort();
                        }
                    },
                    _ = sleep_until(deadline) => if acknowledged.given_up() {
                        end();
                        return answering.abort();
                    },
                    _ = &mut ended => return end(),
                }
            }
        }
        let deadline = acknowledged.deadline();
        tokio::select! {
            _ = &mut ended => return end(),
            _ = sleep_until(deadline) => if acknowledged.given_up() {
                end();
                return answering.abort();
            },
            _ = beat.tick() => {
                let child_beats = world.has_child.load(Ordering::SeqCst)
                    && !world.child_silent.load(Ordering::SeqCst);
                if child_beats {
                    outgoing.push(Packet::heartbeat_session(boot.session(child(), &world), child()));
                }
                outgoing.push(Packet::heartbeat_session(session, root));
            }
            Some(reply) = queued.recv() => outgoing.push(reply),
            _ = tick.tick(), if boot.born.elapsed().as_secs() as u32 >= next => {
                let column = (session as f32).to_le_bytes();
                let sample = Packet::samples(1, 0, next, &column, root);
                outgoing.push(sample.expect("a sample that fits"));
                boot.unsent.fetch_max(next + 1, Ordering::SeqCst);
                next += 1;
            }
        }
    }
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
/// whose randomness comes from `seed`. It seeds this thread's
/// `crate::rand`, which every host draws from, and installs the [`Tracer`].
fn simulation(world: &Arc<World>, seed: u64) -> ::turmoil::Sim<'static> {
    Tracer::install();
    crate::rand::seed(seed);
    crate::net::forget_live_streams();
    crate::chan::forget_live_tasks();
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

/// Runs `sim` to its end. Fails the test process if the run takes longer
/// than [`SPIN_LIMIT`], as a host that spins outside a spawned task never
/// gives a step back. A run that fails ends the watchdog with it.
fn run_to_end(sim: &mut ::turmoil::Sim<'_>) {
    let (done, finished) = std::sync::mpsc::channel::<()>();
    let watchdog = std::thread::spawn(move || {
        let waited = finished.recv_timeout(SPIN_LIMIT);
        if waited == Err(std::sync::mpsc::RecvTimeoutError::Timeout) {
            // The test harness captures `eprintln!`, and loses what it holds
            // when the process exits.
            use std::io::Write;
            let spun = format!("a turmoil simulation spun for {SPIN_LIMIT:?} of real time");
            let _ = writeln!(std::io::stderr(), "{spun}");
            std::process::exit(101);
        }
    });
    while !sim.step().expect("a step") {}
    let _ = done.send(());
    watchdog.join().expect("the watchdog");
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

/// A device whose link to the proxy is cut, with no close reaching either
/// end, is lost within the proxy's quiet time and comes back once the link
/// is whole. The app's samples count the seconds lost.
#[test]
fn a_device_cut_off_without_a_close_is_lost_and_comes_back() {
    for seed in seeds() {
        let _seed = Seed(seed);
        let world = Arc::new(World::default());
        let mut sim = simulation(&world, seed);
        let opening = world.clone();
        sim.client("app", async move {
            let mut app = App::open(&opening).await?;
            ::turmoil::partition("device", "proxy");
            wait_for(&app.presence, Duration::from_secs(10), lost).await;
            app.watch(Duration::from_secs(3)).await;
            ::turmoil::repair("device", "proxy");
            wait_for(&app.presence, Duration::from_secs(30), alive).await;
            app.watch(Duration::from_secs(5)).await;
            answers(&app.device).await;
            assert!(app.watched.lost > 0, "{:?}", app.watched.boundaries);
            Ok(())
        });
        sim.run().expect("the scenario");
    }
}

/// A device that restarts under an app comes back in a new session, and the
/// app's samples say the session changed.
#[test]
fn a_device_that_restarts_comes_back_in_a_new_session() {
    for seed in seeds() {
        let _seed = Seed(seed);
        let world = Arc::new(World::default());
        let mut sim = simulation(&world, seed);
        let app = world.clone();
        sim.client("app", async move {
            let mut opened = App::open(&app).await?;
            app.reach(1);
            let rebooted =
                |p: &Presence| matches!(p, Presence::Alive { session: Some(s) } if s.value() == 8);
            wait_for(&opened.presence, Duration::from_secs(30), rebooted).await;
            opened.watch(Duration::from_secs(5)).await;
            answers(&opened.device).await;
            assert_eq!(opened.watched.last.map(|(session, _)| session), Some(8));
            Ok(())
        });
        step_until(&mut sim, &world, 1);
        sim.bounce("device");
        sim.run().expect("the scenario");
    }
}

/// Runs the scenario of a proxy that crashes under an app that has the
/// device open, and is back 2 s later, with `seed`. The app opens the device
/// again through it, and its samples count the seconds lost.
fn proxy_crash(seed: u64) {
    let world = Arc::new(World::default());
    let mut sim = simulation(&world, seed);
    let app = world.clone();
    sim.client("app", async move {
        let mut opened = App::open(&app).await?;
        app.reach(1);
        wait_for(&opened.presence, Duration::from_secs(10), |p| !alive(p)).await;
        wait_for(&opened.presence, Duration::from_secs(30), alive).await;
        opened.watch(Duration::from_secs(5)).await;
        answers(&opened.device).await;
        assert!(opened.watched.lost > 0, "{:?}", opened.watched.boundaries);
        Ok(())
    });
    step_until(&mut sim, &world, 1);
    sim.crash("proxy");
    let crashed = sim.elapsed();
    while sim.elapsed() < crashed + Duration::from_secs(2) {
        sim.step().expect("a step");
    }
    sim.bounce("proxy");
    sim.run().expect("the scenario");
}

/// The proxy crashes under an app that has the device open, and is back
/// 2 s later. The app opens the device again through it, and its samples
/// count the seconds lost.
#[test]
fn an_app_opens_its_device_again_once_a_crashed_proxy_is_back() {
    for seed in seeds() {
        let _seed = Seed(seed);
        proxy_crash(seed);
    }
}

/// How many seeds the determinism check runs its scenario twice with.
const REPLAYED: usize = 4;

/// The crashed proxy's scenario, run twice with one seed, has turmoil do
/// the same things in the same order, on the same hosts at the same virtual
/// times. The scenario's sends, deliveries and receives, its crash and its
/// restart are all on the trace. A failure names the first event that
/// differs.
#[test]
fn a_scenario_run_twice_with_one_seed_runs_the_same_way() {
    for seed in seeds().into_iter().take(REPLAYED) {
        let _seed = Seed(seed);
        let first = traced(|| proxy_crash(seed));
        let second = traced(|| proxy_crash(seed));
        assert!(!first.is_empty(), "the trace recorded nothing");
        let longest = first.len().max(second.len());
        if let Some(at) = (0..longest).find(|&at| first.get(at) != second.get(at)) {
            panic!(
                "the runs differ at event {at} of {} and {}\nfirst  {:?}\nsecond {:?}",
                first.len(),
                second.len(),
                first.get(at),
                second.get(at),
            );
        }
    }
}

/// One event of turmoil's, as a trace keeps it.
#[derive(Debug, PartialEq)]
struct Traced {
    /// How many steps of the simulation had finished when the step it
    /// happened in began, which is its virtual time in milliseconds.
    step: u64,
    /// The host it happened on, or `sim` for the simulation's own.
    host: String,
    /// The event's message and its fields, as turmoil gave them.
    what: String,
}

thread_local! {
    /// What this thread records while [`traced`] runs a scenario on it.
    static TRACE: std::cell::RefCell<Option<Trace>> = const { std::cell::RefCell::new(None) };
}

/// The events one thread records, and what it needs to place them.
#[derive(Default)]
struct Trace {
    events: Vec<Traced>,
    /// The name of each span, whose id is its place here plus one. A host's
    /// span on each step reuses the id of its first.
    spans: Vec<String>,
    /// The ids of the spans entered, innermost last.
    entered: Vec<u64>,
    /// How many steps had finished when the one that runs now began.
    step: u64,
}

/// Returns the events turmoil makes on this thread while `scenario` runs,
/// in order.
fn traced(scenario: impl FnOnce()) -> Vec<Traced> {
    Tracer::install();
    TRACE.with(|trace| *trace.borrow_mut() = Some(Trace::default()));
    scenario();
    let trace = TRACE.with(|trace| trace.borrow_mut().take());
    trace.expect("the thread's trace").events
}

/// The process's `tracing` subscriber, which hands each of turmoil's events
/// to the [`Trace`] of the thread it happens on, if that thread records one.
/// Turmoil marks the start of each step with an event, and runs each host
/// inside a span that names it.
///
/// It is the global default rather than a thread's own, as `tracing` asks
/// each new callsite of the default of the thread that reaches it first.
/// A scenario on another thread would then turn off turmoil's callsites
/// for every thread.
struct Tracer;

impl Tracer {
    /// Makes the tracer the process's subscriber, once. Every scenario
    /// installs it before turmoil runs, so turmoil's callsites are on from
    /// the start.
    fn install() {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(|| {
            ::tracing::subscriber::set_global_default(Tracer).expect("no other subscriber");
            // A callsite reached while the tracer was not yet the default
            // took the interest of none.
            ::tracing::callsite::rebuild_interest_cache();
        });
    }
}

/// The fields of one event or span, written out in order.
#[derive(Default)]
struct Fields {
    message: String,
    name: String,
    rest: String,
}

impl ::tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &::tracing::field::Field, value: &str) {
        match field.name() {
            "name" => self.name = value.to_string(),
            _ => self.record_debug(field, &value),
        }
    }

    fn record_debug(&mut self, field: &::tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            name => {
                let _ = write!(self.rest, " {name}={value:?}");
            }
        }
    }
}

/// Runs `f` on this thread's trace, if it records one.
fn with_trace(f: impl FnOnce(&mut Trace)) {
    TRACE.with(|trace| {
        if let Some(trace) = trace.borrow_mut().as_mut() {
            f(trace);
        }
    });
}

impl ::tracing::Subscriber for Tracer {
    fn enabled(&self, metadata: &::tracing::Metadata<'_>) -> bool {
        metadata.target().starts_with("turmoil")
    }

    fn new_span(&self, span: &::tracing::span::Attributes<'_>) -> ::tracing::span::Id {
        let mut fields = Fields::default();
        span.record(&mut fields);
        let mut id = 1;
        with_trace(|trace| {
            let spans = &mut trace.spans;
            let at = match spans.iter().position(|name| *name == fields.name) {
                Some(at) => at,
                None => {
                    spans.push(fields.name);
                    spans.len() - 1
                }
            };
            id = at as u64 + 1;
        });
        ::tracing::span::Id::from_u64(id)
    }

    fn record(&self, _: &::tracing::span::Id, _: &::tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &::tracing::span::Id, _: &::tracing::span::Id) {}

    fn event(&self, event: &::tracing::Event<'_>) {
        with_trace(|trace| {
            let mut fields = Fields::default();
            event.record(&mut fields);
            if let Some(step) = fields.message.strip_prefix("step ") {
                trace.step = step.parse().expect("a step number");
                return;
            }
            let host = match trace.entered.last() {
                Some(&id) => trace.spans[id as usize - 1].clone(),
                None => "sim".to_string(),
            };
            trace.events.push(Traced {
                step: trace.step,
                host,
                what: fields.message + &fields.rest,
            });
        });
    }

    fn enter(&self, span: &::tracing::span::Id) {
        with_trace(|trace| trace.entered.push(span.into_u64()));
    }

    fn exit(&self, _: &::tracing::span::Id) {
        with_trace(|trace| {
            trace.entered.pop();
        });
    }
}

/// The link between the app and the proxy is cut for 8 s with no close.
/// The app gives up the quiet proxy, opens the device again once the link
/// is whole, and its samples count the seconds lost.
#[test]
fn an_app_cut_off_from_its_proxy_opens_its_device_again() {
    for seed in seeds() {
        let _seed = Seed(seed);
        let world = Arc::new(World::default());
        let mut sim = simulation(&world, seed);
        let opening = world.clone();
        sim.client("app", async move {
            let mut app = App::open(&opening).await?;
            ::turmoil::partition("app", "proxy");
            let cut = Instant::now();
            wait_for(&app.presence, Duration::from_secs(8), lost).await;
            app.watch(Duration::from_secs(8).saturating_sub(cut.elapsed()))
                .await;
            ::turmoil::repair("app", "proxy");
            answers(&app.device).await;
            app.watch(Duration::from_secs(5)).await;
            assert!(app.watched.lost > 0, "{:?}", app.watched.boundaries);
            Ok(())
        });
        sim.run().expect("the scenario");
    }
}

/// Over a link to the device whose messages each take up to 1 s, the proxy
/// keeps its one connection to the device for 30 s, the device answers
/// every RPC, and no sample is lost.
#[test]
fn a_device_on_a_slow_link_stays_alive_and_answers() {
    for seed in seeds() {
        let _seed = Seed(seed);
        let world = Arc::new(World::default());
        let mut sim = simulation(&world, seed);
        let opening = world.clone();
        sim.client("app", async move {
            let mut app = App::open(&opening).await?;
            let started = Instant::now();
            while started.elapsed() < Duration::from_secs(30) {
                let asked = Instant::now();
                let answered =
                    timeout(Duration::from_secs(5), app.device.submit("dev.name", &[])).await;
                let named = matches!(&answered, Ok(Ok(name)) if name == b"FAKE");
                assert!(named, "{answered:?} after {:?}", asked.elapsed());
                app.watch(Duration::from_millis(500)).await;
            }
            assert_eq!(app.watched.boundaries, Vec::<String>::new());
            let accepted = opening.accepted.load(Ordering::SeqCst);
            assert_eq!(accepted, 1, "the proxy connected to the device again");
            Ok(())
        });
        sim.set_link_max_message_latency("device", "proxy", Duration::from_secs(1));
        sim.run().expect("the scenario");
    }
}

/// The device behind a hub, which never sends its metadata, goes quiet
/// while the proxy waits to ask for it again, and the link stays up. The
/// proxy goes on idling for the next 60 s. No task spins, as
/// [`polls`](crate::chan::polls) fails a run in which one does, and the
/// hosts' tasks are polled fewer times than those 60 s have ticks of 1 ms,
/// so none wakes on every tick.
#[test]
fn a_hub_child_lost_while_its_metadata_is_retried_leaves_the_proxy_idle() {
    const IDLE: Duration = Duration::from_secs(60);
    for seed in seeds() {
        let _seed = Seed(seed);
        let world = Arc::new(World::default());
        world.withholds_metadata.store(true, Ordering::SeqCst);
        world.has_child.store(true, Ordering::SeqCst);
        let mut sim = simulation(&world, seed);
        let app = world.clone();
        sim.client("app", async move {
            let connection = open(&app).await?;
            let child = connection.device(child());
            let presence = child.presence_updates();
            let _metadata = child.metadata_updates();
            wait_for(&presence, Duration::from_secs(10), alive).await;
            sleep(Duration::from_secs(5)).await;
            app.silence_child(true);
            wait_for(&presence, Duration::from_secs(10), lost).await;
            app.reach(1);
            sleep(IDLE).await;
            drop(connection);
            Ok(())
        });
        step_until(&mut sim, &world, 1);
        let before = crate::chan::polls::count();
        run_to_end(&mut sim);
        let polls = crate::chan::polls::count() - before;
        let ticks = IDLE.as_millis() as u64;
        assert!(
            polls < ticks,
            "the tasks were polled {polls} times in {ticks} ticks"
        );
    }
}

/// The device behind a hub goes quiet past the proxy's quiet time while the
/// link stays up, and is heard again after its reboot. The proxy loads
/// its table again.
#[test]
fn a_hub_child_heard_again_after_going_quiet_loads_its_table_again() {
    for seed in seeds() {
        let _seed = Seed(seed);
        let world = Arc::new(World::default());
        world.has_child.store(true, Ordering::SeqCst);
        let mut sim = simulation(&world, seed);
        let app = world.clone();
        sim.client("app", async move {
            let connection = open(&app).await?;
            let child_device = connection.device(child());
            let presence = child_device.presence_updates();
            let _settings = child_device.settings_updates();
            wait_for(&presence, Duration::from_secs(10), alive).await;
            sleep(Duration::from_secs(5)).await;
            let before = app.table_calls(child());
            assert!(before > 0, "the child's table was never loaded");
            app.silence_child(true);
            wait_for(&presence, Duration::from_secs(10), lost).await;
            app.silence_child(false);
            wait_for(&presence, Duration::from_secs(10), alive).await;
            sleep(Duration::from_secs(5)).await;
            let after = app.table_calls(child());
            assert!(
                after > before,
                "the table read before the silence is still in force"
            );
            Ok(())
        });
        sim.run().expect("the scenario");
    }
}

//! Scenarios of a device, the proxy and an app, each run with every seed.

use super::*;
/// A device whose link to the proxy is cut, with no close reaching either
/// end, is lost within the proxy's quiet time and comes back once the link
/// is whole. The app's samples count the seconds lost.
#[test]
fn a_device_cut_off_without_a_close_is_lost_and_comes_back() {
    each_seed(|seed| {
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
    });
}

/// A device that restarts under an app comes back in a new session, and the
/// app's samples say the session changed.
#[test]
fn a_device_that_restarts_comes_back_in_a_new_session() {
    each_seed(|seed| {
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
    });
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
    each_seed(proxy_crash);
}

/// The crashed proxy's scenario, run twice with one seed, has turmoil do
/// the same things in the same order, on the same hosts at the same virtual
/// times. The scenario's sends, deliveries and receives, its crash and its
/// restart are all on the trace. A failure names the first event that
/// differs.
#[test]
fn a_scenario_run_twice_with_one_seed_runs_the_same_way() {
    for seed in seeds().into_iter().take(REPLAYED) {
        let _seed = Seed(seed);
        let first = seeded(seed, || traced(|| proxy_crash(seed)));
        let second = seeded(seed, || traced(|| proxy_crash(seed)));
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

/// The link between the app and the proxy is cut for 8 s with no close.
/// The app gives up the quiet proxy, opens the device again once the link
/// is whole, and its samples count the seconds lost.
#[test]
fn an_app_cut_off_from_its_proxy_opens_its_device_again() {
    each_seed(|seed| {
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
    });
}

/// Over a link to the device whose messages each take up to 1 s, the proxy
/// keeps its one connection to the device for 30 s, the device answers
/// every RPC, and no sample is lost.
#[test]
fn a_device_on_a_slow_link_stays_alive_and_answers() {
    each_seed(|seed| {
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
    });
}

/// The device behind a hub, which never sends its metadata, goes quiet
/// while the proxy waits to ask for it again, and the link stays up. The
/// proxy goes on idling for the next 60 s. No task spins, as
/// [`polls`] fails a run in which one does, and the
/// hosts' tasks are polled fewer times than those 60 s have ticks of 1 ms,
/// so none wakes on every tick.
#[test]
fn a_hub_child_lost_while_its_metadata_is_retried_leaves_the_proxy_idle() {
    const IDLE: Duration = Duration::from_secs(60);
    each_seed(|seed| {
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
        let before = polls();
        run_to_end(&mut sim);
        let polls = polls() - before;
        let ticks = IDLE.as_millis() as u64;
        assert!(
            polls < ticks,
            "the tasks were polled {polls} times in {ticks} ticks"
        );
    });
}

/// The device behind a hub goes quiet past the proxy's quiet time while the
/// link stays up, and is heard again after its reboot. The proxy loads
/// its table again.
#[test]
fn a_hub_child_heard_again_after_going_quiet_loads_its_table_again() {
    each_seed(|seed| {
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
    });
}

//! The app's API over a proxy on this machine.

use crate::device::Connection;
use crate::proto::{DeviceRoute, RouteError};
use crate::proxy::tests::fixtures::options;
use crate::proxy::{host, HostOptions, Proxy};
use crate::rt;
use crate::session::rpc::{Outcome, Refusal, RpcValue};
use crate::session::{Layout, Mount, ProxyAddress, Session};
use crate::tio::tests::device::FakeDevice;
use crate::tio::tests::WAIT;

/// The layout that mounts each of `devices` at a prefix of its own, and
/// the prefixes.
fn each_at_its_own_prefix(devices: &[FakeDevice]) -> (Layout, Vec<DeviceRoute>) {
    let prefixes: Vec<DeviceRoute> = (1..=devices.len())
        .map(|place| format!("/{place}").parse().unwrap())
        .collect();
    let mounts = devices.iter().zip(&prefixes).map(|(device, prefix)| Mount {
        url: device.url.clone(),
        source: DeviceRoute::root(),
        prefix: *prefix,
    });
    let layout = Layout {
        mounts: mounts.collect(),
    };
    (layout, prefixes)
}

/// A proxy hosted and a connection opened through the async entry points
/// run every task on the runtime they are awaited on, here one of a
/// single thread, and never reach the crate's runtime. The proxy keeps
/// the tables and settings of several devices there.
#[test]
fn a_proxy_and_a_connection_run_on_the_runtime_they_are_awaited_on() {
    let devices: Vec<FakeDevice> = (0..3).map(|_| FakeDevice::start()).collect();
    let (layout, prefixes) = each_at_its_own_prefix(&devices);
    let caller = std::thread::spawn(move || {
        rt::forbid_crate_runtime();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .on_thread_start(rt::forbid_crate_runtime)
            .build()
            .unwrap();
        runtime.block_on(async {
            let proxy = host(options()).await.unwrap();
            let address = ProxyAddress {
                url: proxy.endpoint(),
                ensure: None,
            };
            let session = Session::mount_async(&address, &layout, WAIT);
            let connection = Connection::over(session.await.unwrap());
            for prefix in &prefixes {
                let device = connection.device(*prefix);
                assert_eq!(device.submit("dev.name", &[]).await.unwrap(), b"FAKE");
                let followed = device.settings_updates();
                let settings = tokio::time::timeout(WAIT, followed.recv_async()).await;
                let settings = settings.expect("the settings in time").unwrap();
                assert_eq!(settings.get("test.amplitude"), Some(&[1u8][..]));
            }
            connection.close();
            proxy.shutdown_async().await;
        });
    });
    if let Err(panic) = caller.join() {
        std::panic::resume_unwind(panic);
    }
}

/// The synchronous API works from inside an application's tokio runtime
/// of one thread, where `block_on` panics. It runs on the crate's runtime
/// and blocks the application's thread until each call is done.
#[tokio::test]
async fn the_sync_api_works_inside_an_applications_runtime() {
    let device = FakeDevice::start();
    let proxy = Proxy::host(HostOptions {
        default: Some(Layout::single(&device.url)),
        ..options()
    })
    .unwrap();
    let connection = Connection::open(&proxy.endpoint()).unwrap();

    let name: String = connection
        .device(DeviceRoute::root())
        .get("dev.name")
        .unwrap();

    assert_eq!(name, "FAKE");
    drop(connection);
    drop(proxy);
}

/// A connection that holds a layout as a background proxy's default follows the tree it holds,
/// its routes heard and not ended, until it closes and gives the default back.
#[test]
fn a_held_default_follows_the_tree_it_holds() {
    let device = FakeDevice::start();
    let proxy = Proxy::host(HostOptions {
        stoppable: true,
        ..options()
    })
    .unwrap();
    let address = ProxyAddress {
        url: proxy.endpoint(),
        ensure: None,
    };
    let held = Connection::hold_default_at(&address, &Layout::single(&device.url), WAIT).unwrap();
    let routes = held.tree(DeviceRoute::root()).routes_updates();
    assert_eq!(routes.recv_timeout(WAIT), Ok(vec![DeviceRoute::root()]));
    let user = Connection::open(&proxy.endpoint()).unwrap();
    assert_eq!(
        user.device(DeviceRoute::root())
            .get::<String>("dev.name")
            .unwrap(),
        "FAKE"
    );
    assert!(matches!(
        routes.recv_timeout(std::time::Duration::from_millis(500)),
        Err(crate::session::RecvError::Timeout)
    ));
    held.close();
    proxy.shutdown();
}

/// Two fake devices, each at a prefix of its own behind a proxy of the
/// test's own, a connection to it, and the prefixes.
fn two_devices() -> (Vec<FakeDevice>, Proxy, Connection, Vec<DeviceRoute>) {
    let devices: Vec<FakeDevice> = (0..2).map(|_| FakeDevice::start()).collect();
    let (layout, prefixes) = each_at_its_own_prefix(&devices);
    let proxy = Proxy::host(HostOptions {
        default: Some(layout),
        ..options()
    })
    .unwrap();
    let connection = Connection::open(&proxy.endpoint()).unwrap();
    (devices, proxy, connection, prefixes)
}

/// Waits until the proxy has loaded the table of each of `prefixes` and
/// read its settings, so no read of them is still to come. Holds no
/// receiver of a table once it returns.
fn settle(connection: &Connection, prefixes: &[DeviceRoute]) {
    for prefix in prefixes {
        connection.device(*prefix).settings().unwrap();
    }
}

/// A call every device takes goes to each of them, and each says what it
/// answered, in the order of the routes.
#[tokio::test]
async fn a_call_each_device_takes_reaches_every_device() {
    let (devices, proxy, connection, prefixes) = two_devices();
    settle(&connection, &prefixes);
    let before: Vec<usize> = devices.iter().map(|d| d.calls("test.amplitude")).collect();
    let tree = connection.tree(DeviceRoute::root());
    let order = [prefixes[1], prefixes[0]];

    let results = tree.call_each("test.amplitude", Some("3"), &order).await;

    let routes: Vec<DeviceRoute> = results.iter().map(|(route, _)| *route).collect();
    assert_eq!(routes, order);
    for (_, outcome) in &results {
        assert!(
            matches!(outcome, Outcome::Ok(Some(RpcValue::U64(1)))),
            "{results:?}"
        );
    }
    for (device, before) in devices.iter().zip(before) {
        assert_eq!(device.calls("test.amplitude"), before + 1);
    }
    drop(connection);
    drop(proxy);
}

/// The awaited table of a device is the one the blocking read returns.
#[tokio::test]
async fn a_tables_awaited_read_is_the_blocking_reads_table() {
    let (_devices, proxy, connection, prefixes) = two_devices();
    let device = connection.device(prefixes[0]);
    let awaited = device.rpc_registry_async().await.unwrap();
    assert!(awaited.find("test.amplitude").is_some());
    let blocking = tokio::task::spawn_blocking(move || device.rpc_registry().unwrap());
    let names = |table: &crate::session::rpc::RpcRegistry| -> Vec<String> {
        table.iter().map(|desc| desc.full_name.clone()).collect()
    };
    assert_eq!(names(&awaited), names(&blocking.await.unwrap()));
    drop(connection);
    drop(proxy);
}

/// A client that follows no device's table, and has asked nothing of the
/// devices yet, gets its call made. The call waits for each table.
#[tokio::test]
async fn a_client_that_holds_no_table_gets_its_call_made() {
    let (_devices, proxy, connection, prefixes) = two_devices();
    let tree = connection.tree(DeviceRoute::root());

    let results = tree.call_each("test.amplitude", Some("3"), &prefixes).await;

    let routes: Vec<DeviceRoute> = results.iter().map(|(route, _)| *route).collect();
    assert_eq!(routes, prefixes);
    for (_, outcome) in &results {
        assert!(
            matches!(outcome, Outcome::Ok(Some(RpcValue::U64(1)))),
            "{results:?}"
        );
    }
    drop(connection);
    drop(proxy);
}

/// A device that cannot take a call stops it for every device. It says
/// why, the others are skipped, and nothing is sent.
#[tokio::test]
async fn a_device_that_refuses_a_call_stops_it_for_every_device() {
    let (devices, proxy, connection, prefixes) = two_devices();
    settle(&connection, &prefixes);
    let before: Vec<usize> = devices.iter().map(|d| d.calls("test.amplitude")).collect();
    let tree = connection.tree(prefixes[0]);

    let results = tree
        .call_each("test.amplitude", Some("3"), &[prefixes[0], prefixes[1]])
        .await;
    assert!(
        matches!(&results[0], (route, Outcome::Skipped) if *route == prefixes[0]),
        "{results:?}"
    );
    assert!(
        matches!(
            &results[1],
            (route, Outcome::Refused(Refusal::OutsideView(RouteError::OutsideSubtree)))
                if *route == prefixes[1]
        ),
        "{results:?}"
    );

    let first = [prefixes[0]];
    let refused = tree.call_each("test.amplitude", Some("300"), &first).await;
    assert!(
        matches!(&refused[0].1, Outcome::Refused(Refusal::DoesNotFit(_))),
        "{refused:?}"
    );
    let unnamed = tree.call_each("test.nothing", Some("3"), &first).await;
    assert!(
        matches!(&unnamed[..], [(route, Outcome::Refused(Refusal::NoSuchRpc))] if *route == prefixes[0]),
        "{unnamed:?}"
    );

    for (device, before) in devices.iter().zip(before) {
        assert_eq!(device.calls("test.amplitude"), before, "nothing is sent");
    }
    drop(connection);
    drop(proxy);
}

/// A route named twice in one call refuses the call for every device. Each
/// place it is named says so, the others are skipped, and nothing is sent.
#[tokio::test]
async fn a_route_named_twice_stops_the_call_for_every_device() {
    let (devices, proxy, connection, prefixes) = two_devices();
    settle(&connection, &prefixes);
    let before: Vec<usize> = devices.iter().map(|d| d.calls("test.amplitude")).collect();
    let tree = connection.tree(DeviceRoute::root());
    let routes = [prefixes[0], prefixes[1], prefixes[0]];

    let results = tree.call_each("test.amplitude", Some("3"), &routes).await;

    assert!(
        matches!(
            &results[..],
            [
                (_, Outcome::Refused(Refusal::NamedTwice)),
                (_, Outcome::Skipped),
                (_, Outcome::Refused(Refusal::NamedTwice)),
            ]
        ),
        "{results:?}"
    );
    let said: Vec<DeviceRoute> = results.iter().map(|(route, _)| *route).collect();
    assert_eq!(said, routes);
    for (device, before) in devices.iter().zip(before) {
        assert_eq!(device.calls("test.amplitude"), before, "nothing is sent");
    }
    drop(connection);
    drop(proxy);
}

/// A device lost once its table is known is sent the call all the same.
/// Its outcome is an error or unknown, and every other device takes it.
#[tokio::test]
async fn a_device_lost_after_its_table_loaded_fails_alone() {
    let (mut devices, proxy, connection, prefixes) = two_devices();
    settle(&connection, &prefixes);
    let before = devices[0].calls("test.amplitude");
    let lost = connection.device(prefixes[1]);
    let presence = lost.presence_updates();
    drop(devices.pop());
    let start = std::time::Instant::now();
    loop {
        let update = presence.recv_async().await.unwrap();
        if matches!(update, crate::session::Presence::Lost { .. }) {
            break;
        }
        assert!(start.elapsed() < WAIT, "the device was never lost");
    }
    let tree =
        (connection.tree(DeviceRoute::root())).with_timeout(std::time::Duration::from_millis(500));

    let results = tree.call_each("test.amplitude", Some("3"), &prefixes).await;

    assert!(
        matches!(&results[0], (route, Outcome::Ok(_)) if *route == prefixes[0]),
        "{results:?}"
    );
    assert!(
        matches!(&results[1], (route, Outcome::Error(_) | Outcome::Unknown(_)) if *route == prefixes[1]),
        "{results:?}"
    );
    assert_eq!(devices[0].calls("test.amplitude"), before + 1);
    drop(connection);
    drop(proxy);
}

//! The app's API over a proxy on this machine.

use crate::device::Connection;
use crate::proto::DeviceRoute;
use crate::proxy::tests::fixtures::options;
use crate::proxy::{host, HostOptions, Proxy};
use crate::rt;
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

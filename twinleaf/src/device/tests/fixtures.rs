//! Test fixtures for `device`, a proxy on a free port and a connection
//! through it.

use crate::device::Connection;
use crate::proto::DeviceRoute;
use crate::proxy::{tests::fixtures, Proxy};
use crate::session::{Layout, Mount, ProxyAddress, Purpose, Session};
use crate::tio::tests::WAIT;
use std::io;
use std::sync::Mutex;

/// Held by each test that sets the variables a proxy is found or started by,
/// such as `TWINLEAF_PROXY`, `TWINLEAF_TIO` and `TWINLEAF_RUNTIME_DIR`. Every
/// test in the process shares them.
pub(crate) static ENVIRONMENT: Mutex<()> = Mutex::new(());

/// Hosts a proxy on a free port of this machine, with no default.
pub(crate) fn proxy() -> Proxy {
    Proxy::host(fixtures::options()).unwrap()
}

/// Hosts a proxy on a free port of this machine, with the whole of the device
/// at `url` as its default.
pub(crate) fn proxy_serving(url: &str) -> Proxy {
    let proxy = proxy();
    proxy.set_default(Some(Layout {
        mounts: vec![Mount {
            url: url.to_string(),
            source: DeviceRoute::root(),
            prefix: DeviceRoute::root(),
        }],
    }));
    proxy
}

/// Opens `url` through `proxy`, as [`Connection::open`] does through the
/// proxy `TWINLEAF_PROXY` names. The proxy opens the fake devices' loopback
/// addresses as it opens any device.
pub(crate) fn open_through(proxy: &Proxy, url: &str) -> io::Result<Connection> {
    let proxy = ProxyAddress {
        url: proxy.endpoint(),
        ensure: None,
    };
    Session::open(&proxy, url, Purpose::Use, WAIT).map(Connection::over)
}

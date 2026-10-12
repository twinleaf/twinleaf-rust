//! Test fixtures for this module, a proxy on a free port and the fake device
//! of the proxy's own tests.

use super::runtime::ProxyAddress;
use super::session::Session;
use super::Connection;
use crate::proto::DeviceRoute;
pub(crate) use crate::proxy::fixtures::{attached, FakeDevice, WAIT};
use crate::proxy::{fixtures, Layout, Mount, Proxy, Purpose};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
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

/// A port of `127.0.0.1` that refuses every connection. A socket holds it
/// bound without listening, and no other test, in this process or another,
/// can take the port while it lives.
pub(crate) struct ClosedPort(socket2::Socket);

impl ClosedPort {
    /// Binds a free port without listening.
    pub(crate) fn new() -> ClosedPort {
        use socket2::{Domain, Socket, Type};
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        let address = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        socket.bind(&address.into()).unwrap();
        ClosedPort(socket)
    }

    /// Returns the port number.
    pub(crate) fn port(&self) -> u16 {
        self.0.local_addr().unwrap().as_socket().unwrap().port()
    }

    /// Returns its URL, such as `tcp://127.0.0.1:40000`.
    pub(crate) fn url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.port())
    }

    /// Starts listening on the port, which nothing else can have taken.
    #[cfg(unix)]
    pub(crate) fn listen(self) -> TcpListener {
        self.0.listen(16).unwrap();
        self.0.into()
    }
}

/// How many ports [`on_free_port`] tries.
const PORT_ATTEMPTS: usize = 5;

/// Runs `scenario` on a port of `127.0.0.1` that was free a moment before,
/// for a test whose proxy binds the port itself.
///
/// Another process can take the port in that moment. When `scenario` returns
/// `None` and the port is held elsewhere, the scenario runs again on another
/// port. Returns `None` if the port was free and the scenario still failed,
/// or every port tried was taken.
pub(crate) fn on_free_port<T>(mut scenario: impl FnMut(u16) -> Option<T>) -> Option<T> {
    for _ in 0..PORT_ATTEMPTS {
        let port = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if let Some(done) = scenario(port) {
            return Some(done);
        }
        let taken = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
            .is_err_and(|error| error.kind() == io::ErrorKind::AddrInUse);
        if !taken {
            return None;
        }
    }
    None
}

//! The proxy's server, the task at the root of the proxy. It accepts
//! clients, runs the task of each, keeps the client table, and owns the
//! registry of links. [`Shared`] holds what every client's task reads.

use super::client::{Client, SOCKET_QUEUE};
use super::link::{Links, Owner};
use super::table::{Notice, Shared, TableChange};
use super::worker;
use super::{answers_async, endpoint_at, ClientInfo, HostOptions, Listen, IDLE, LEAVE_TIMEOUT};
use crate::chan::oneshot;
use crate::net::{Listeners, TcpStream};
use crate::rt::{sleep_until_some, Supervisor};
use crate::session::{
    call_at, decode, encode, older_build, Introduction, Layout, HELLO, HELLO_TIMEOUT,
};
use crate::tio::transport;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Instant};
use tokio_util::task::JoinMap;

/// How long the server waits after an accept fails before it accepts again.
// An accept that fails for want of file descriptors fails again at once.
const ACCEPT_RETRY: Duration = Duration::from_millis(20);

/// A running proxy. Dropping it stops it, and the drop blocks until the
/// proxy has stopped, as [`shutdown`](Proxy::shutdown) does, for at most
/// 1.5 s and the moment its aborts take.
pub struct Proxy {
    /// The server's mailbox, which keeps the proxy running.
    server: mpsc::UnboundedSender<Notice>,
    /// The receiver that disconnects once the proxy has stopped.
    stopped: crate::chan::Receiver<()>,
    /// Whether a drop waits for the stop, as for a proxy [`Proxy::host`]
    /// started on the crate's runtime.
    drop_waits: bool,
    /// The client table, as the server publishes it.
    clients: watch::Receiver<Vec<ClientInfo>>,
    default: watch::Sender<Option<Layout>>,
    endpoint: String,
    #[cfg(test)]
    #[cfg_attr(all(test, turmoil), allow(dead_code))]
    pub(super) links: Links,
}

impl Proxy {
    /// Starts a proxy with `options`. Returns once it accepts clients.
    ///
    /// The proxy runs on the crate's own runtime. This may be called from
    /// inside an application's tokio runtime, and blocks that thread until
    /// the proxy accepts clients or fails to start.
    ///
    /// # Errors
    ///
    /// Returns an [`io::ErrorKind::AddrInUse`] error if `options.port` is
    /// nonzero and anything already answers on that port of `127.0.0.1`.
    /// Nothing is bound then. The message says whether a Twinleaf proxy
    /// already runs there. Also returns any error from binding the port or
    /// listening on it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use twinleaf::proxy::{HostOptions, Layout, Mount, Proxy};
    /// use twinleaf::DeviceRoute;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let proxy = Proxy::host(HostOptions {
    ///     default: Some(Layout {
    ///         mounts: vec![Mount {
    ///             url: "serial:///dev/ttyACM0".to_string(),
    ///             source: DeviceRoute::root(),
    ///             prefix: DeviceRoute::root(),
    ///         }],
    ///     }),
    ///     ..HostOptions::default()
    /// })?;
    /// println!("Serving at {}", proxy.endpoint());
    /// proxy.wait();
    /// # Ok(())
    /// # }
    /// ```
    pub fn host(options: HostOptions) -> io::Result<Proxy> {
        let mut proxy = Supervisor::new().run("proxy host", host(options))?;
        proxy.drop_waits = true;
        Ok(proxy)
    }

    /// Returns the URL a client on this machine connects to, such as
    /// `tcp://127.0.0.1:7855`.
    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    /// Replaces the layout `default` opens and `auto` prefers, or clears it
    /// with `None`. Clients that already opened one keep it.
    pub fn set_default(&self, layout: Option<Layout>) {
        self.default.send_replace(layout);
    }

    /// Returns the clients connected now, in the order they connected. A
    /// proxy that has stopped has none.
    pub fn clients(&self) -> Vec<ClientInfo> {
        match self.clients.has_changed() {
            Ok(_) => self.clients.borrow().clone(),
            Err(_stopped) => Vec::new(),
        }
    }

    /// Blocks until the proxy has stopped, by `proxy.stop`, by idling out, or
    /// by [`shutdown`](Self::shutdown).
    pub fn wait(&self) {
        let _ = self.stopped.recv();
    }

    /// Stops the proxy and blocks until it has stopped. Each client hears the
    /// status [`Stopped`](crate::tio::packet::LinkStatus::Stopped), then its
    /// connection closes. The proxy stops accepting clients and closes every
    /// link.
    ///
    /// The stop waits at most 1.5 s for the clients to close, and in the
    /// same 1.5 s for each link to let go of its device. Then it aborts the
    /// tasks of the clients and links that still run, and returns. So it
    /// blocks for at most 1.5 s and the moment the aborts take. An open of a
    /// serial device in flight on a blocking thread runs to its end after
    /// that. The engine that keeps a link's state, and the tasks that read
    /// its devices' settings, end once their link has ended, and the stop
    /// does not wait for them.
    ///
    /// Calling it again does nothing. It may be called from inside an
    /// application's tokio runtime, and blocks that thread as it would any
    /// other.
    pub fn shutdown(&self) {
        let _ = self.server.send(Notice::Stop);
        self.wait();
    }

    /// Stops the proxy as [`shutdown`](Self::shutdown) does, and waits for
    /// it without blocking. A proxy hosted on its caller's runtime, as the
    /// crate's async [`host`] starts it, is stopped this way, since its drop
    /// only asks it to stop.
    pub(crate) async fn shutdown_async(&self) {
        let _ = self.server.send(Notice::Stop);
        let _ = self.stopped.recv_async().await;
    }
}

impl fmt::Debug for Proxy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Proxy")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl Drop for Proxy {
    /// Stops the proxy. A proxy [`Proxy::host`] started is waited for as
    /// [`shutdown`](Proxy::shutdown) waits. Any other proxy runs on its
    /// caller's runtime and is only asked to stop, since on a runtime of one
    /// thread a wait would block the proxy itself.
    fn drop(&mut self) {
        let _ = self.server.send(Notice::Stop);
        if self.drop_waits {
            self.wait();
        }
    }
}

/// Binds the listeners for `options`, and starts the server, which serves
/// clients until the proxy stops. Returns the proxy's handle. Must be
/// awaited on a tokio runtime, which runs every task of the proxy.
///
/// Returns an [`io::ErrorKind::AddrInUse`] error if anything already answers
/// on a nonzero port. The message calls it a Twinleaf proxy if it answers
/// `proxy.hello`. Also returns any error from binding the listeners or
/// registering them with the runtime.
pub(crate) async fn host(mut options: HostOptions) -> io::Result<Proxy> {
    let tasks = Supervisor::current();
    if options.port != 0 && answers_async(options.port).await {
        let endpoint = endpoint_at(options.port);
        let address = SocketAddr::from((Ipv4Addr::LOCALHOST, options.port));
        let hello = encode(&Introduction::of_this_process(&options.name));
        let called = call_at(address, &endpoint, HELLO, &hello, HELLO_TIMEOUT, &tasks).await;
        let message = match called {
            Ok(reply) => match decode::<Introduction>(&reply) {
                Ok(host) => {
                    format!("a Twinleaf proxy already runs at {endpoint}, hosted by {host}")
                }
                Err(_) => format!("a Twinleaf proxy already runs at {endpoint}"),
            },
            Err(_) => older_build(&endpoint).to_string(),
        };
        return Err(io::Error::new(io::ErrorKind::AddrInUse, message));
    }
    let (v4, v6) = addresses(options.listen, LOOPBACK_SHARES_PORT);
    let listeners = Listeners::bind(v4, v6, options.port).await?;
    let port = listeners.port()?;
    let default = watch::channel(options.default.take());
    let registry = Links::new(
        &tasks,
        options.reconnect_timeout,
        options.dump,
        default.1.clone(),
    );
    let (proxy, mut server) = Server::new(
        options,
        endpoint_at(port),
        tasks.clone(),
        registry,
        default.0,
    );
    server.listeners = listeners;
    log::info!(
        "Proxy hosted by {} at {}",
        server.shared.options.name,
        proxy.endpoint
    );
    tasks.spawn(format_args!("proxy {}", proxy.endpoint), server.run());
    Ok(proxy)
}

/// The proxy's server, which runs as the task "proxy <endpoint>". It accepts
/// clients and spawns the task of each. It is the only owner of the client
/// table, which it publishes on every change.
pub(super) struct Server {
    shared: Arc<Shared>,
    /// The listening sockets. They are dropped once the proxy stops
    /// accepting.
    listeners: Listeners,
    notices: mpsc::UnboundedReceiver<Notice>,
    clients: BTreeMap<u64, ClientInfo>,
    /// The publisher of `clients`.
    table: watch::Sender<Vec<ClientInfo>>,
    /// The task of each client.
    sessions: JoinMap<u64, Option<()>>,
    /// The lifeline of each client's task, dropped to tell it the proxy stops.
    lifelines: BTreeMap<u64, oneshot::Sender<Infallible>>,
    registry: Owner,
    /// Disconnects once the server has ended.
    _stopped: crate::chan::Sender<()>,
    next_id: u64,
    /// When a proxy that exits once idle exits, if no client connects
    /// before.
    idle_at: Instant,
    /// When accepting resumes after an accept failed.
    retry_at: Option<Instant>,
}

impl Server {
    /// Returns the handle of a proxy at `endpoint` hosted with `options`, whose
    /// clients run on `tasks` and links in `registry`, with its server to run.
    /// Its default layout is the one `default` holds, which the links watch
    /// too.
    pub(super) fn new(
        options: HostOptions,
        endpoint: String,
        tasks: Supervisor,
        (links, registry): (Links, Owner),
        default: watch::Sender<Option<Layout>>,
    ) -> (Proxy, Server) {
        let (notices, mailbox) = mpsc::unbounded_channel();
        let (table, clients) = watch::channel(Vec::new());
        let (stopped, waited) = crate::chan::bounded(0);
        let proxy = Proxy {
            server: notices.clone(),
            stopped: waited,
            drop_waits: false,
            clients,
            default: default.clone(),
            endpoint: endpoint.clone(),
            #[cfg(test)]
            links: links.clone(),
        };
        let shared = Arc::new(Shared {
            options,
            endpoint,
            links,
            tasks,
            server: notices.downgrade(),
            default,
            holder: Mutex::new(None),
        });
        let server = Server {
            shared,
            listeners: Listeners::default(),
            notices: mailbox,
            clients: BTreeMap::new(),
            table,
            sessions: JoinMap::new(),
            lifelines: BTreeMap::new(),
            registry,
            _stopped: stopped,
            next_id: 1,
            idle_at: Instant::now() + IDLE,
            retry_at: None,
        };
        (proxy, server)
    }

    /// Accepts clients and keeps the table until a stop, the last handle's
    /// drop, the idle exit or the end of the registry. Then stops the proxy.
    async fn run(mut self) {
        loop {
            let accepting = self.retry_at.is_none();
            let idle = self.shared.options.idle_exit && self.clients.is_empty();
            tokio::select! {
                // A connection waiting goes before the idle exit.
                biased;
                notice = self.notices.recv() => match notice {
                    Some(Notice::Update { id, update, done }) => self.update(id, update, done),
                    Some(Notice::Stop) | None => break,
                },
                accepted = self.listeners.accept(), if accepting => match accepted {
                    Ok((stream, peer)) => self.serve(stream, peer),
                    Err(error) => {
                        log::warn!("Error accepting client: {error}");
                        self.retry_at = Some(Instant::now() + ACCEPT_RETRY);
                    }
                },
                Some((id, _outcome)) = self.sessions.join_next() => self.joined(id),
                _ = &mut self.registry.task => {
                    log::error!("Stopping the proxy at {}, as its links ended", self.shared.endpoint);
                    break;
                }
                () = sleep_until_some(self.retry_at) => self.retry_at = None,
                () = sleep_until_some(idle.then_some(self.idle_at)) => {
                    if self.idle_out().is_break() {
                        break;
                    }
                }
            }
        }
        self.stop().await;
    }

    /// Stops accepting, and drops each client's lifeline and the registry of
    /// links together. Waits [`LEAVE_TIMEOUT`] for the clients, as the
    /// registry does for its links.
    async fn stop(mut self) {
        self.listeners.close();
        self.notices.close();
        while self.notices.try_recv().is_ok() {}
        self.lifelines.clear();
        let sessions = &mut self.sessions;
        let departing = async {
            let departed = async { while sessions.join_next().await.is_some() {} };
            if timeout(LEAVE_TIMEOUT, departed).await.is_err() {
                log::warn!("{} clients did not close in time", sessions.len());
                sessions.shutdown().await;
            }
        };
        tokio::join!(departing, self.registry.close());
        log::info!("Proxy at {} stopped", self.shared.endpoint);
    }

    /// Changes client `id`'s entry in the table, and answers on `done`.
    fn update(&mut self, id: u64, update: TableChange, done: oneshot::Sender<()>) {
        if let Some(info) = self.clients.get_mut(&id) {
            match update {
                TableChange::Hello(client) => {
                    info.version = Some(client.version_text());
                    info.own_process = client.pid == std::process::id();
                    info.pid = Some(client.pid);
                    info.name = Some(client.name);
                }
                TableChange::Target(target) => info.target = Some(target),
            }
            self.publish();
        }
        let _ = done.send(());
    }

    /// Removes the client whose task ended. The idle time starts when the
    /// last client's task leaves.
    fn joined(&mut self, id: u64) {
        self.clients.remove(&id);
        self.lifelines.remove(&id);
        self.publish();
        if self.clients.is_empty() {
            self.idle_at = Instant::now() + IDLE;
        }
    }

    /// Publishes the client table.
    fn publish(&self) {
        self.table
            .send_replace(self.clients.values().cloned().collect());
    }

    /// Adds the client at `peer` connected on `stream` to the table, and
    /// spawns its task.
    fn serve(&mut self, stream: TcpStream, peer: SocketAddr) {
        // What a client sends goes to its ports, so it is queued as deep as
        // they are. What it is sent waits in its outbox.
        let (deliver, incoming) = crate::chan::bounded(worker::client_tx_channel_size());
        let tasks = &self.shared.tasks;
        let port = match transport::Port::from_tcp(stream, deliver, SOCKET_QUEUE, tasks) {
            Ok(port) => port,
            Err(error) => {
                log::warn!("Failed to set up client {peer}: {error}");
                return;
            }
        };
        log::debug!("Accepted client from {}", peer);
        let id = self.next_id;
        self.next_id += 1;
        let info = ClientInfo {
            id,
            peer,
            pid: None,
            name: None,
            version: None,
            target: None,
            own_process: false,
        };
        self.clients.insert(id, info);
        self.publish();
        let (lifeline, alive) = oneshot::channel();
        self.lifelines.insert(id, lifeline);
        let session = Client::new(self.shared.clone(), id, peer, port, incoming, alive);
        let task = format_args!("proxy client {id}");
        self.shared
            .tasks
            .spawn_in_map(&mut self.sessions, id, task, session.run());
    }

    /// Breaks to exit a proxy that has had no client for [`IDLE`]. Each
    /// connection already waiting on a listener is served instead.
    fn idle_out(&mut self) -> ControlFlow<()> {
        let waiting = self.listeners.waiting();
        if waiting.is_empty() {
            log::info!("no clients for {:?}; exiting", IDLE);
            return ControlFlow::Break(());
        }
        for accepted in waiting {
            match accepted {
                Ok((stream, peer)) => self.serve(stream, peer),
                Err(error) => log::warn!("Error accepting client: {error}"),
            }
        }
        ControlFlow::Continue(())
    }
}

/// Whether a socket on every interface shares its port with one on
/// `127.0.0.1` on this OS. It does on macOS and Windows.
const LOOPBACK_SHARES_PORT: bool = cfg!(any(target_os = "macos", windows));

/// Returns the addresses a proxy binds for `listen`, its IPv4 addresses in
/// the order they are bound and then its IPv6 address. `loopback_shares_port`
/// says whether a socket on every interface shares its port with one on
/// `127.0.0.1`, as [`LOOPBACK_SHARES_PORT`] says of this OS. Where it does,
/// a proxy on every interface holds `127.0.0.1` too, so that no other proxy
/// binds it. Windows hands each connection to `127.0.0.1` to the socket bound
/// there rather than to the one on every interface.
fn addresses(listen: Listen, loopback_shares_port: bool) -> (&'static [Ipv4Addr], Ipv6Addr) {
    match (listen, loopback_shares_port) {
        (Listen::Loopback, _) => (&[Ipv4Addr::LOCALHOST], Ipv6Addr::LOCALHOST),
        (Listen::AllInterfaces, true) => (
            &[Ipv4Addr::LOCALHOST, Ipv4Addr::UNSPECIFIED],
            Ipv6Addr::UNSPECIFIED,
        ),
        (Listen::AllInterfaces, false) => (&[Ipv4Addr::UNSPECIFIED], Ipv6Addr::UNSPECIFIED),
    }
}

#[cfg(test)]
#[cfg(not(turmoil))]
mod tests {
    use super::*;
    use crate::proxy::tests::fixtures::options;
    use crate::proxy::DumpFlags;
    use crate::rt;
    use crate::tio::tests::{assert_took, binds_every_interface, PAUSED_WAIT};
    use socket2::SockRef;
    use std::net::TcpListener;
    use std::thread;
    use tokio::time::timeout;

    /// A proxy on loopback binds `127.0.0.1` and `::1` on every OS. One on
    /// every interface binds `127.0.0.1` beside every interface where the
    /// two share a port, and every interface alone elsewhere.
    #[test]
    fn each_mode_binds_its_addresses_on_each_os() {
        for shares in [false, true] {
            assert_eq!(
                addresses(Listen::Loopback, shares),
                (&[Ipv4Addr::LOCALHOST][..], Ipv6Addr::LOCALHOST)
            );
        }
        assert_eq!(
            addresses(Listen::AllInterfaces, true),
            (
                &[Ipv4Addr::LOCALHOST, Ipv4Addr::UNSPECIFIED][..],
                Ipv6Addr::UNSPECIFIED
            )
        );
        assert_eq!(
            addresses(Listen::AllInterfaces, false),
            (&[Ipv4Addr::UNSPECIFIED][..], Ipv6Addr::UNSPECIFIED)
        );
    }

    /// IPv4 is held on sockets of their own, the first as the lock and the
    /// endpoint, and IPv6 on another that takes no IPv4, in either mode.
    #[test]
    fn ipv4_and_ipv6_are_bound_apart() {
        assert_bound_apart(Listen::Loopback);
        if binds_every_interface("ipv4_and_ipv6_are_bound_apart with Listen::AllInterfaces") {
            assert_bound_apart(Listen::AllInterfaces);
        }
    }

    /// Binds the listeners of a proxy for `listen` on `port`, on the crate's
    /// runtime.
    fn bind(listen: Listen, port: u16) -> io::Result<Listeners> {
        let (v4, v6) = addresses(listen, LOOPBACK_SHARES_PORT);
        rt::handle().block_on(Listeners::bind(v4, v6, port))
    }

    /// Asserts that `bind` holds the [`addresses`] of `listen`, IPv4 on
    /// sockets of their own and IPv6 on one that takes no IPv4.
    fn assert_bound_apart(listen: Listen) {
        let (v4, v6) = addresses(listen, LOOPBACK_SHARES_PORT);
        let listeners = bind(listen, 0).unwrap();
        let listeners = listeners.sockets();
        let port = listeners[0].local_addr().unwrap().port();
        let bound: Vec<_> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
        let expected = v4.iter().map(|v4| SocketAddr::from((*v4, port)));
        assert_eq!(bound[..v4.len()], expected.collect::<Vec<_>>()[..]);
        if TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).is_err() {
            eprintln!("skipped IPv6: this host cannot bind ::1");
            return;
        }
        assert_eq!(listeners.len(), v4.len() + 1, "{listen:?}");
        assert_eq!(bound[v4.len()], (v6, port).into());
        assert!(SockRef::from(&listeners[v4.len()]).only_v6().unwrap());
    }

    /// A proxy on every interface and one on loopback never both hold the
    /// port, whichever binds first.
    #[test]
    fn a_proxy_on_every_interface_and_one_on_loopback_exclude_each_other() {
        if !binds_every_interface(
            "a_proxy_on_every_interface_and_one_on_loopback_exclude_each_other",
        ) {
            return;
        }
        for (first, second) in [
            (Listen::AllInterfaces, Listen::Loopback),
            (Listen::Loopback, Listen::AllInterfaces),
        ] {
            let held = bind(first, 0).unwrap();
            let port = held.port().unwrap();
            let refused = bind(second, port).unwrap_err();
            assert_eq!(
                refused.kind(),
                io::ErrorKind::AddrInUse,
                "{second:?} after {first:?}"
            );
        }
    }

    /// A connection that waits on a listener as the proxy idles out is
    /// served, though the runtime has not yet seen it arrive. The proxy
    /// keeps running.
    #[test]
    fn a_connection_waiting_as_the_proxy_idles_out_is_served() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut options = HostOptions {
            idle_exit: true,
            ..options()
        };
        let tasks = Supervisor::new();
        let default = watch::channel(options.default.take());
        let registry = Links::new(&tasks, None, DumpFlags::default(), default.1.clone());
        let endpoint = endpoint_at(address.port());
        let (proxy, mut server) = Server::new(options, endpoint, tasks, registry, default.0);
        let _runtime = rt::handle().enter();
        server.listeners = Listeners::from_std(vec![listener]).unwrap();
        let _client = std::net::TcpStream::connect(address).unwrap();
        // A connection may reach the listener's queue only after connect
        // returns, as with the mirrored networking of WSL2.
        thread::sleep(Duration::from_millis(100));

        let idled = server.idle_out();

        assert!(idled.is_continue());
        assert_eq!(proxy.clients().len(), 1);
        assert_eq!(server.listeners.sockets().len(), 1);
        drop(server);
    }

    /// Starts a proxy hosted with `options` on the test's runtime, and
    /// returns its handle.
    fn started(mut options: HostOptions) -> Proxy {
        // Nothing connects to it, so the paused clock never waits on it.
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let endpoint = endpoint_at(listener.local_addr().unwrap().port());
        listener.set_nonblocking(true).unwrap();
        let tasks = Supervisor::current();
        let default = watch::channel(options.default.take());
        let registry = Links::new(&tasks, None, DumpFlags::default(), default.1.clone());
        let (proxy, mut server) =
            Server::new(options, endpoint, tasks.clone(), registry, default.0);
        server.listeners = Listeners::from_std(vec![listener]).unwrap();
        tasks.spawn("a test proxy", server.run());
        proxy
    }

    /// Waits until `proxy` has stopped.
    async fn stopped(proxy: &Proxy) {
        let waited = timeout(PAUSED_WAIT, proxy.stopped.recv_async()).await;
        assert_eq!(waited, Ok(Err(crate::chan::RecvError::Disconnected)));
    }

    /// A proxy that exits once idle, and that no client connects to, stops
    /// once [`IDLE`] has passed. Its waiters hear it then, and its table is
    /// empty.
    #[tokio::test(start_paused = true)]
    async fn a_proxy_no_client_connects_to_exits_once_idle() {
        let started_at = Instant::now();
        let options = HostOptions {
            idle_exit: true,
            ..options()
        };
        let proxy = started(options);

        stopped(&proxy).await;

        assert_took(started_at.elapsed(), IDLE, "the idle exit");
        assert!(proxy.clients().is_empty());
    }

    /// A stop with no client and no link to wait for ends the proxy at once.
    #[tokio::test(start_paused = true)]
    async fn a_stop_with_nothing_to_wait_for_ends_the_proxy_at_once() {
        let proxy = started(options());
        let stopping = Instant::now();

        proxy
            .server
            .send(Notice::Stop)
            .unwrap_or_else(|_| panic!("the server ended"));
        stopped(&proxy).await;

        assert_eq!(stopping.elapsed(), Duration::ZERO);
        assert!(proxy.clients().is_empty());
    }
}

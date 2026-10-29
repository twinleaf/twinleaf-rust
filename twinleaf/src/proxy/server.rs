//! The proxy's server, the task at the root of the proxy. It accepts
//! clients, runs the task of each, keeps the client table, and owns the
//! registry of links. [`Shared`] holds what every client's task reads.

use super::client::Session;
use super::control::{self, Introduction, HELLO, HELLO_TIMEOUT};
use super::link::{self, Links, Owner};
use super::{
    answers, endpoint_at, ClientInfo, HostOptions, Layout, Listen, Proxy, IDLE, LEAVE_TIMEOUT,
};
use crate::rt::{self, sleep_until_some, Supervisor};
use crate::tio::rpc::{RpcArgs, RpcReply};
use crate::tio::transport;
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::future::{self, Future};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{timeout, Instant};
use tokio_util::task::JoinMap;

/// How long the server waits after an accept fails before it accepts again.
// An accept that fails for want of file descriptors fails again at once.
const ACCEPT_RETRY: Duration = Duration::from_millis(20);

/// What the server is told, by the proxy's handle and its clients' tasks.
pub(super) enum Notice {
    /// A change to client `id`'s entry in the table. The server answers on
    /// `done` once the table shows it.
    Update {
        id: u64,
        update: Update,
        done: oneshot::Sender<()>,
    },
    /// A stop of the proxy.
    Stop,
    /// A panic of the server.
    #[cfg(test)]
    Panic,
}

/// A change to a client's entry in the table.
pub(super) enum Update {
    /// The client introduced itself with `proxy.hello`.
    Hello(Introduction),
    /// The client opened this target.
    Target(String),
}

/// What every client's task reads, none of which keeps the proxy running.
pub(super) struct Shared {
    /// How the proxy is hosted. Its default layout is held in `default`.
    pub(super) options: HostOptions,
    pub(super) endpoint: String,
    pub(super) links: Links,
    /// The spawner of the clients' transports and blocking steps.
    pub(super) tasks: Supervisor,
    /// The server's mailbox.
    pub(super) server: mpsc::WeakUnboundedSender<Notice>,
    /// The default layout, which the host sets and a client may hold.
    pub(super) default: watch::Sender<Option<Layout>>,
    /// The id and address of the client holding the default with `proxy.default`.
    pub(super) holder: Mutex<Option<(u64, String)>>,
}

impl Shared {
    /// Returns the layout `default` opens.
    pub(super) fn default(&self) -> Option<Layout> {
        self.default.borrow().clone()
    }
}

/// Binds the listeners for `options`, and starts the server, which serves
/// clients until the proxy stops. Returns the proxy's handle.
///
/// Returns an [`io::ErrorKind::AddrInUse`] error if anything already answers
/// on a nonzero port. The message calls it a Twinleaf proxy if it answers
/// `proxy.hello`. Also returns any error from binding the listeners or
/// registering them with the crate's runtime.
///
/// # Panics
///
/// Panics if the crate's runtime cannot start.
pub(super) fn host(mut options: HostOptions) -> io::Result<Proxy> {
    if options.port != 0 && answers(options.port) {
        let endpoint = endpoint_at(options.port);
        let hello = Introduction::of_this_process(&options.name).encode_args();
        let message = match control::call(&endpoint, HELLO, &hello, HELLO_TIMEOUT) {
            Ok(reply) => match Introduction::decode_reply(&reply) {
                Ok(host) => {
                    format!("a Twinleaf proxy already runs at {endpoint}, hosted by {host}")
                }
                Err(_) => format!("a Twinleaf proxy already runs at {endpoint}"),
            },
            Err(_) => control::not_a_proxy(&endpoint).to_string(),
        };
        return Err(io::Error::new(io::ErrorKind::AddrInUse, message));
    }
    let listeners = bind(options.listen, options.port)?;
    let port = listeners[0].local_addr()?.port();
    let listeners = {
        let _runtime = rt::handle().enter();
        let listening = listeners.into_iter().map(|listener| {
            listener.set_nonblocking(true)?;
            TcpListener::from_std(listener)
        });
        listening.collect::<io::Result<Vec<_>>>()?
    };
    let tasks = Supervisor::new();
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
    listeners: Vec<TcpListener>,
    notices: mpsc::UnboundedReceiver<Notice>,
    clients: BTreeMap<u64, ClientInfo>,
    /// The publisher of `clients`.
    table: watch::Sender<Vec<ClientInfo>>,
    /// The task of each client.
    sessions: JoinMap<u64, Option<()>>,
    /// The lifeline of each client's task, dropped to tell it the proxy stops.
    lifelines: HashMap<u64, oneshot::Sender<Infallible>>,
    registry: Owner,
    /// Disconnects once the server has ended.
    _stopped: flume::Sender<()>,
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
        let (stopped, waited) = flume::bounded(0);
        let proxy = Proxy {
            server: notices.clone(),
            stopped: waited,
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
            listeners: Vec::new(),
            notices: mailbox,
            clients: BTreeMap::new(),
            table,
            sessions: JoinMap::new(),
            lifelines: HashMap::new(),
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
                    #[cfg(test)]
                    Some(Notice::Panic) => panic!("a test asked the server to panic"),
                },
                accepted = accept(&self.listeners), if accepting => match accepted {
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
        self.listeners.clear();
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
    fn update(&mut self, id: u64, update: Update, done: oneshot::Sender<()>) {
        if let Some(info) = self.clients.get_mut(&id) {
            match update {
                Update::Hello(client) => {
                    info.version = Some(client.version_text());
                    info.own_process = client.pid == std::process::id();
                    info.pid = Some(client.pid);
                    info.name = Some(client.name);
                }
                Update::Target(target) => info.target = Some(target),
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
        // A client from the proxy perspective is a port in reverse, i.e. what it receives
        // is what a client transmits, and vice-versa. Therefore, the channel size settings
        // for rx and tx are inverted. Also, we use the proxy port channel size setting
        // instead of the physical ports setting.
        let (deliver, incoming) = flume::bounded(link::client_tx_channel_size());
        let size = link::client_rx_channel_size();
        let port = match transport::Port::from_tcp(stream, deliver, size, &self.shared.tasks) {
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
        let session = Session::new(self.shared.clone(), id, peer, port, incoming, alive);
        let task = format_args!("proxy client {id}");
        self.shared
            .tasks
            .spawn_in_map(&mut self.sessions, id, task, session.run());
    }

    /// Breaks to exit a proxy that has had no client for [`IDLE`]. Each
    /// connection already waiting on a listener is served instead.
    fn idle_out(&mut self) -> ControlFlow<()> {
        let mut waiting = Vec::new();
        for listener in &self.listeners {
            loop {
                match SockRef::from(listener).accept() {
                    Ok((socket, _)) => waiting.push(socket),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        log::warn!("Error accepting client: {error}");
                        break;
                    }
                }
            }
        }
        if waiting.is_empty() {
            log::info!("no clients for {:?}; exiting", IDLE);
            return ControlFlow::Break(());
        }
        for socket in waiting {
            let stream = std::net::TcpStream::from(socket);
            let accepted = stream.set_nonblocking(true).and_then(|()| {
                let peer = stream.peer_addr()?;
                Ok((TcpStream::from_std(stream)?, peer))
            });
            match accepted {
                Ok((stream, peer)) => self.serve(stream, peer),
                Err(error) => log::warn!("Error accepting client: {error}"),
            }
        }
        ControlFlow::Continue(())
    }
}

/// Waits for a connection on any of `listeners`. Never returns while there is
/// none.
fn accept(
    listeners: &[TcpListener],
) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + '_ {
    future::poll_fn(move |cx| {
        for listener in listeners {
            if let Poll::Ready(accepted) = listener.poll_accept(cx) {
                return Poll::Ready(accepted);
            }
        }
        Poll::Pending
    })
}

/// Binds the listening sockets for `listen` on `port`. The IPv4 sockets come
/// first, on `127.0.0.1` or every interface, and the first holds the proxy's
/// port. On macOS and Windows, where a socket on every interface shares its
/// port with one on `127.0.0.1`, a proxy on every interface holds
/// `127.0.0.1` too, so that no other proxy binds it. Windows hands each
/// connection to `127.0.0.1` to the socket bound there rather than to the
/// one on every interface. An IPv6 only socket follows on the same
/// port, on `::1` or every interface. A host without IPv6 gets the IPv4
/// sockets alone. Returns an error if an IPv4 socket cannot be bound.
fn bind(listen: Listen, port: u16) -> io::Result<Vec<std::net::TcpListener>> {
    let (v4, v6): (&[Ipv4Addr], _) = match listen {
        Listen::Loopback => (&[Ipv4Addr::LOCALHOST], Ipv6Addr::LOCALHOST),
        Listen::AllInterfaces if cfg!(any(target_os = "macos", windows)) => (
            &[Ipv4Addr::LOCALHOST, Ipv4Addr::UNSPECIFIED],
            Ipv6Addr::UNSPECIFIED,
        ),
        Listen::AllInterfaces => (&[Ipv4Addr::UNSPECIFIED], Ipv6Addr::UNSPECIFIED),
    };
    let first = std::net::TcpListener::bind((v4[0], port))?;
    let port = first.local_addr()?.port();
    let rest = v4[1..]
        .iter()
        .map(|v4| std::net::TcpListener::bind((*v4, port)));
    let mut listeners = std::iter::once(Ok(first))
        .chain(rest)
        .collect::<io::Result<Vec<_>>>()?;
    match bind_v6_only((v6, port).into()) {
        Ok(v6) => listeners.push(v6),
        Err(error) => log::debug!("Not listening on [{v6}]:{port}: {error}"),
    }
    Ok(listeners)
}

/// Listens on the IPv6 `address` like [`std::net::TcpListener::bind`], with
/// a socket that takes no IPv4.
fn bind_v6_only(address: SocketAddr) -> io::Result<std::net::TcpListener> {
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_only_v6(true)?;
    #[cfg(not(windows))]
    socket.set_reuse_address(true)?;
    socket.bind(&address.into())?;
    socket.listen(128)?;
    Ok(socket.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::fixtures::{assert_took, options, PAUSED_WAIT};
    use crate::proxy::DumpFlags;
    use std::net::TcpListener;
    use std::thread;
    use tokio::time::timeout;

    /// IPv4 is held on sockets of their own, the first as the lock and the
    /// endpoint, and IPv6 on another that takes no IPv4, in either mode.
    #[test]
    fn ipv4_and_ipv6_are_bound_apart() {
        let everywhere: &[Ipv4Addr] = if cfg!(any(target_os = "macos", windows)) {
            &[Ipv4Addr::LOCALHOST, Ipv4Addr::UNSPECIFIED]
        } else {
            &[Ipv4Addr::UNSPECIFIED]
        };
        for (listen, v4, v6) in [
            (
                Listen::Loopback,
                &[Ipv4Addr::LOCALHOST][..],
                Ipv6Addr::LOCALHOST,
            ),
            (Listen::AllInterfaces, everywhere, Ipv6Addr::UNSPECIFIED),
        ] {
            let listeners = bind(listen, 0).unwrap();
            let port = listeners[0].local_addr().unwrap().port();
            let bound: Vec<_> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
            let expected = v4.iter().map(|v4| SocketAddr::from((*v4, port)));
            assert_eq!(bound[..v4.len()], expected.collect::<Vec<_>>()[..]);
            if TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).is_err() {
                eprintln!("skipped IPv6: this host cannot bind ::1");
                continue;
            }
            assert_eq!(listeners.len(), v4.len() + 1, "{listen:?}");
            assert_eq!(bound[v4.len()], (v6, port).into());
            assert!(SockRef::from(&listeners[v4.len()]).only_v6().unwrap());
        }
    }

    /// A proxy on every interface and one on loopback never both hold the
    /// port, whichever binds first.
    #[test]
    fn a_proxy_on_every_interface_and_one_on_loopback_exclude_each_other() {
        for (first, second) in [
            (Listen::AllInterfaces, Listen::Loopback),
            (Listen::Loopback, Listen::AllInterfaces),
        ] {
            let held = bind(first, 0).unwrap();
            let port = held[0].local_addr().unwrap().port();
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
        server.listeners = vec![tokio::net::TcpListener::from_std(listener).unwrap()];
        let _client = std::net::TcpStream::connect(address).unwrap();
        // A connection may reach the listener's queue only after connect
        // returns, as with the mirrored networking of WSL2.
        thread::sleep(Duration::from_millis(100));

        let idled = server.idle_out();

        assert!(idled.is_continue());
        assert_eq!(proxy.clients().len(), 1);
        assert_eq!(server.listeners.len(), 1);
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
        server.listeners = vec![tokio::net::TcpListener::from_std(listener).unwrap()];
        tasks.spawn("a test proxy", server.run());
        proxy
    }

    /// Waits until `proxy` has stopped.
    async fn stopped(proxy: &Proxy) {
        let waited = timeout(PAUSED_WAIT, proxy.stopped.recv_async()).await;
        assert_eq!(waited, Ok(Err(flume::RecvError::Disconnected)));
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

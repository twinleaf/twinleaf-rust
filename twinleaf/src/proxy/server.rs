//! The proxy's shared state, its server and its supervisor. [`Shared`] holds
//! what the proxy's tasks share. The server is the task that accepts
//! clients, spawns a task for each, and keeps the client table. The
//! supervisor is the task that runs the server and stops the proxy in order.

use super::client::Session;
use super::control::{self, Introduction, HELLO, HELLO_TIMEOUT};
use super::link::{self, Links};
use super::{answers, endpoint_at, ClientInfo, HostOptions, Layout, Listen, IDLE};
use crate::rt::{self, sleep_until_some, Supervisor};
use crate::tio::rpc::{RpcArgs, RpcReply};
use crate::tio::transport;
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::collections::BTreeMap;
use std::future::{self, Future};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{timeout, timeout_at, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::JoinMap;

/// How long stopping waits for clients to close their transports.
const DEPARTURE_TIMEOUT: Duration = Duration::from_secs(1);

/// How long a stop may take in all. Stopping then ends the proxy's tasks
/// still running.
const STOP_TIMEOUT: Duration = Duration::from_secs(2);

/// How long stopping waits for the tasks it ended.
// An ended task finishes at its next await, but work on a blocking thread
// runs to its end.
const ENDED_GRACE: Duration = Duration::from_millis(100);

/// How long the server waits after an accept fails before it accepts again.
// An accept that fails for want of file descriptors fails again at once.
const ACCEPT_RETRY: Duration = Duration::from_millis(20);

/// What a client's task tells the server.
pub(super) enum Notice {
    /// A change to client `id`'s entry in the table. The server answers on
    /// `done` once the table shows it.
    Update {
        id: u64,
        update: Update,
        done: oneshot::Sender<()>,
    },
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

/// What the proxy's tasks share.
pub(super) struct Shared {
    /// How the proxy is hosted. Its default layout is held in `default`.
    pub(super) options: HostOptions,
    pub(super) links: Links,
    /// The spawner of the proxy's tasks. The proxy has stopped once each has
    /// ended.
    pub(super) tasks: Supervisor,
    /// The server's mailbox.
    pub(super) notices: mpsc::UnboundedSender<Notice>,
    /// Cancelled once the proxy begins to stop. Each client hears the stop,
    /// and the server stops accepting.
    pub(super) stop: CancellationToken,
    endpoint: String,
    default: watch::Sender<Option<Layout>>,
    /// The client table, as the server publishes it.
    clients: watch::Receiver<Vec<ClientInfo>>,
}

impl Shared {
    /// Binds the listeners for `options`, and starts the proxy's tasks, which
    /// serve clients until the proxy stops. Returns what the tasks share, and
    /// the receiver that disconnects once the proxy has stopped.
    ///
    /// Returns an [`io::ErrorKind::AddrInUse`] error if anything already
    /// answers on a nonzero port. The message calls it a Twinleaf proxy if it
    /// answers `proxy.hello`. Also returns any error from binding the
    /// listeners or registering them with the crate's runtime.
    ///
    /// # Panics
    ///
    /// Panics if the crate's runtime cannot start.
    pub(super) fn host(options: HostOptions) -> io::Result<(Arc<Shared>, flume::Receiver<()>)> {
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
        let links = Links::new(&tasks, options.reconnect_timeout, options.dump);
        let (shared, server) = Shared::new(options, endpoint_at(port), tasks, links);
        let (stopped, waited) = flume::bounded(0);
        // The supervisor waits for every task on `tasks`, so it runs apart
        // from them.
        let task = format_args!("proxy {}", shared.endpoint);
        Supervisor::new().spawn(task, supervise(server, listeners, stopped));
        log::info!(
            "Proxy hosted by {} at {}",
            shared.options.name,
            shared.endpoint
        );
        Ok((shared, waited))
    }

    /// Returns the state a proxy at `endpoint` hosted with `options` shares,
    /// whose tasks run on `tasks` and whose links `links` holds. Returns it
    /// with the server that serves it, still to run.
    pub(super) fn new(
        mut options: HostOptions,
        endpoint: String,
        tasks: Supervisor,
        links: Links,
    ) -> (Arc<Shared>, Server) {
        let (notices, mailbox) = mpsc::unbounded_channel();
        let (table, clients) = watch::channel(Vec::new());
        let shared = Arc::new(Shared {
            links,
            tasks,
            notices,
            stop: CancellationToken::new(),
            endpoint,
            default: watch::Sender::new(options.default.take()),
            options,
            clients,
        });
        let server = Server {
            shared: shared.clone(),
            listeners: Vec::new(),
            notices: mailbox,
            clients: BTreeMap::new(),
            table,
            sessions: JoinMap::new(),
            next_id: 1,
            idle_at: Instant::now() + IDLE,
            retry_at: None,
            stopping: false,
            abort_at: None,
        };
        (shared, server)
    }

    pub(super) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the layout `default` opens.
    pub(super) fn default(&self) -> Option<Layout> {
        self.default.borrow().clone()
    }

    pub(super) fn set_default(&self, layout: Option<Layout>) {
        self.default.send_replace(layout);
    }

    /// Returns every client in the table, in the order they connected.
    pub(super) fn clients(&self) -> Vec<ClientInfo> {
        self.clients.borrow().clone()
    }
}

/// Supervises the proxy, and stops it in order. Spawns `server` to accept
/// clients on `listeners`, and waits until the proxy begins to stop or its
/// server or registry of links ends. The stop then has [`STOP_TIMEOUT`] in
/// all. It waits for the server to end, which it does once its clients have
/// heard the stop and left. It closes every link, and waits for every task of
/// the proxy to end. Once the time runs out, it ends the tasks still running
/// and waits [`ENDED_GRACE`] for them. `stopped` disconnects then, or if the
/// supervisor panics.
async fn supervise(server: Server, listeners: Vec<TcpListener>, stopped: flume::Sender<()>) {
    let shared = server.shared.clone();
    shared.tasks.spawn("proxy server", server.run(listeners));
    tokio::select! {
        // A server that ends once the proxy stops has not failed.
        biased;
        () = shared.stop.cancelled() => {}
        () = shared.notices.closed() => {
            log::error!("Stopping the proxy at {}, as its server ended", shared.endpoint);
        }
        () = shared.links.ended() => {
            log::error!("Stopping the proxy at {}, as its links ended", shared.endpoint);
        }
    }
    shared.stop.cancel();
    let deadline = Instant::now() + STOP_TIMEOUT;
    let _ = timeout_at(deadline, shared.notices.closed()).await;
    let left = deadline.saturating_duration_since(Instant::now());
    if !shared.links.close_all(left).await {
        log::warn!("Some links did not close within the stop's {STOP_TIMEOUT:?}");
    }
    if timeout_at(deadline, shared.tasks.wait()).await.is_err() {
        shared.tasks.end_all();
        if timeout(ENDED_GRACE, shared.tasks.wait()).await.is_err() {
            log::warn!("Work of the proxy on a blocking thread outlived its stop");
        }
    }
    log::info!("Proxy at {} stopped", shared.endpoint);
    drop(stopped);
}

/// The proxy's server, which runs as the task "proxy server". It accepts
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
    next_id: u64,
    /// When a proxy that exits once idle exits, if no client connects
    /// before.
    idle_at: Instant,
    /// When accepting resumes after an accept failed.
    retry_at: Option<Instant>,
    /// Whether the server has heard the proxy's stop.
    stopping: bool,
    /// When the clients that have not left are aborted, once the proxy
    /// stops.
    abort_at: Option<Instant>,
}

impl Server {
    /// Accepts clients on `listeners` and keeps the table until the proxy
    /// stops and its clients have left. Once the proxy stops, aborts the
    /// clients still connected after [`DEPARTURE_TIMEOUT`]. Begins the stop
    /// if the proxy idles out.
    async fn run(mut self, listeners: Vec<TcpListener>) {
        self.listeners = listeners;
        loop {
            if self.stopping && self.sessions.is_empty() {
                self.clients.clear();
                self.publish();
                return;
            }
            let accepting = self.retry_at.is_none();
            let idle = self.shared.options.idle_exit && self.clients.is_empty();
            let idle_at = (idle && !self.listeners.is_empty()).then_some(self.idle_at);
            tokio::select! {
                // The stop ends accepting at once. A connection waiting goes
                // before the idle exit.
                biased;
                () = self.shared.stop.cancelled(), if !self.stopping => {
                    self.stopping = true;
                    self.listeners.clear();
                    self.abort_at = Some(Instant::now() + DEPARTURE_TIMEOUT);
                }
                accepted = accept(&self.listeners), if accepting => match accepted {
                    Ok((stream, peer)) => self.serve(stream, peer),
                    Err(error) => {
                        log::warn!("Error accepting client: {error}");
                        self.retry_at = Some(Instant::now() + ACCEPT_RETRY);
                    }
                },
                notice = self.notices.recv() => match notice {
                    Some(notice) => self.notice(notice),
                    None => return,
                },
                Some((id, _outcome)) = self.sessions.join_next() => self.joined(id),
                () = sleep_until_some(self.retry_at) => self.retry_at = None,
                () = sleep_until_some(idle_at) => self.idle_out(),
                () = sleep_until_some(self.abort_at) => {
                    log::warn!("{} clients did not close in time", self.clients.len());
                    self.sessions.abort_all();
                    self.abort_at = None;
                }
            }
        }
    }

    /// Handles a notice from a client's task.
    fn notice(&mut self, notice: Notice) {
        match notice {
            Notice::Update { id, update, done } => {
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
            #[cfg(test)]
            Notice::Panic => panic!("a test asked the server to panic"),
        }
    }

    /// Removes the client whose task ended. The idle time starts when the
    /// last client's task leaves.
    fn joined(&mut self, id: u64) {
        self.clients.remove(&id);
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
        let stop = self.shared.stop.child_token();
        let session = Session::new(self.shared.clone(), id, peer, port, incoming, stop);
        let task = format_args!("proxy client {id}");
        self.shared
            .tasks
            .spawn_in_map(&mut self.sessions, id, task, session.run());
    }

    /// Exits a proxy that has had no client for [`IDLE`]. Each connection
    /// already waiting on a listener is served instead.
    fn idle_out(&mut self) {
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
            self.shared.stop.cancel();
            return;
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

/// Binds the listening sockets for `listen` on `port`. The IPv4 socket comes
/// first, on `127.0.0.1` or every interface, and holds the proxy's port. An
/// IPv6 only socket follows on the same port, on `::1` or every interface. A
/// host without IPv6 gets the IPv4 socket alone. Returns an error if the IPv4
/// socket cannot be bound.
fn bind(listen: Listen, port: u16) -> io::Result<Vec<std::net::TcpListener>> {
    let (v4, v6) = match listen {
        Listen::Loopback => (Ipv4Addr::LOCALHOST, Ipv6Addr::LOCALHOST),
        Listen::AllInterfaces => (Ipv4Addr::UNSPECIFIED, Ipv6Addr::UNSPECIFIED),
    };
    let v4 = std::net::TcpListener::bind((v4, port))?;
    let port = v4.local_addr()?.port();
    let mut listeners = vec![v4];
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

    /// IPv4 is held on a socket of its own, as the lock and the endpoint,
    /// and IPv6 on another that takes no IPv4, in either mode.
    #[test]
    fn ipv4_and_ipv6_are_bound_apart() {
        for (listen, v4, v6) in [
            (Listen::Loopback, Ipv4Addr::LOCALHOST, Ipv6Addr::LOCALHOST),
            (
                Listen::AllInterfaces,
                Ipv4Addr::UNSPECIFIED,
                Ipv6Addr::UNSPECIFIED,
            ),
        ] {
            let listeners = bind(listen, 0).unwrap();
            let port = listeners[0].local_addr().unwrap().port();
            assert_eq!(listeners[0].local_addr().unwrap(), (v4, port).into());
            if TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).is_err() {
                eprintln!("skipped IPv6: this host cannot bind ::1");
                continue;
            }
            assert_eq!(listeners.len(), 2, "{listen:?}");
            assert_eq!(listeners[1].local_addr().unwrap(), (v6, port).into());
            assert!(SockRef::from(&listeners[1]).only_v6().unwrap());
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
        let options = HostOptions {
            idle_exit: true,
            ..options()
        };
        let tasks = Supervisor::new();
        let links = Links::new(&tasks, None, DumpFlags::default());
        let endpoint = endpoint_at(address.port());
        let (shared, mut server) = Shared::new(options, endpoint, tasks, links);
        let _runtime = rt::handle().enter();
        server.listeners = vec![tokio::net::TcpListener::from_std(listener).unwrap()];
        let _client = std::net::TcpStream::connect(address).unwrap();
        // A connection may reach the listener's queue only after connect
        // returns, as with the mirrored networking of WSL2.
        thread::sleep(Duration::from_millis(100));

        server.idle_out();

        assert_eq!(shared.clients().len(), 1);
        assert_eq!(server.listeners.len(), 1);
        assert!(!shared.stop.is_cancelled());
    }

    /// Starts a proxy hosted with `options` on the test's runtime. Returns
    /// what its tasks share, and the receiver that disconnects once it has
    /// stopped.
    fn started(options: HostOptions) -> (Arc<Shared>, flume::Receiver<()>) {
        // Nothing connects to it, so the paused clock never waits on it.
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let endpoint = endpoint_at(listener.local_addr().unwrap().port());
        listener.set_nonblocking(true).unwrap();
        let listeners = vec![tokio::net::TcpListener::from_std(listener).unwrap()];
        let tasks = Supervisor::current();
        let links = Links::new(&tasks, None, DumpFlags::default());
        let (shared, server) = Shared::new(options, endpoint, tasks, links);
        let (stopped, waited) = flume::bounded(0);
        let supervisor = supervise(server, listeners, stopped);
        Supervisor::current().spawn("a test proxy", supervisor);
        (shared, waited)
    }

    /// Waits until `waited` hears that its proxy stopped.
    async fn stopped(waited: flume::Receiver<()>) {
        let waited = timeout(PAUSED_WAIT, waited.recv_async()).await;
        assert_eq!(waited, Ok(Err(flume::RecvError::Disconnected)));
    }

    /// A proxy that exits once idle, and that no client connects to, stops
    /// once [`IDLE`] has passed. Its waiters hear it then.
    #[tokio::test(start_paused = true)]
    async fn a_proxy_no_client_connects_to_exits_once_idle() {
        let started_at = Instant::now();
        let options = HostOptions {
            idle_exit: true,
            ..options()
        };
        let (shared, waited) = started(options);

        stopped(waited).await;

        assert_took(started_at.elapsed(), IDLE, "the idle exit");
        assert!(shared.stop.is_cancelled());
        assert!(shared.clients().is_empty());
    }

    /// A task of the proxy that never ends holds up its stop for
    /// [`STOP_TIMEOUT`] in all, and is then ended.
    #[tokio::test(start_paused = true)]
    async fn a_task_that_never_ends_is_ended_once_the_stop_runs_out_of_time() {
        let (shared, waited) = started(options());
        let (held, mut dropped) = tokio::sync::oneshot::channel::<()>();
        shared.tasks.spawn("a stuck task", async move {
            let _held = held;
            future::pending::<()>().await
        });
        let stopping = Instant::now();

        shared.stop.cancel();
        stopped(waited).await;

        assert_took(stopping.elapsed(), STOP_TIMEOUT, "the stop");
        assert!(dropped.try_recv().is_err(), "the stuck task was not ended");
    }
}

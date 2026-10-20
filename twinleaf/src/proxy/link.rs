//! The proxy's links. A [`Link`] is one connection to a device, shared by
//! every client that mounts it. [`Links`] is the handle of the registry, a
//! task that holds the links and counts the [`Lease`] each client holds on a
//! link it uses. The registry closes a link that a client chose once the link
//! has had no users for [`IDLE`]. It closes a link that only probes have used
//! as soon as its last lease drops.
//!
//! A link's worker reconnects a device that drops out, and the clients using
//! it hear this as link statuses. A link with users closes when the proxy
//! stops or its worker stops. The worker stops when its first open fails,
//! which refuses the opens waiting on it. It also stops when a reconnect
//! gives up, and then its clients hear
//! [`FailedToReconnect`](crate::tio::packet::LinkStatus::FailedToReconnect)
//! and are disconnected. The next open of the device makes a new link, and
//! the registry forgets the old one once its task ends.
//!
//! The registry holds the only sender of each worker's commands, and closes
//! a link by dropping it. A new link to a device opens only once the task of
//! every earlier link to it has ended, and with it the worker's hold on the
//! device, however long that takes. Links to other devices open meanwhile.
//! The registry runs until its [`Owner`] drops. It then closes every link
//! and waits [`LEAVE_TIMEOUT`] for their tasks before it ends.

use super::auto::Candidate;
use super::control::Refusal;
use super::worker::{self, Command, Opener, Readiness, Subscriber};
use super::{DumpFlags, IDLE, LEAVE_TIMEOUT};
use crate::proto::rpc::RpcError;
use crate::proto::DeviceRoute;
use crate::rt::{sleep_until_some, Supervisor};
use crate::tio::packet::Packet;
use crate::tio::transport;
use futures_util::future::{Fuse, FusedFuture};
use futures_util::FutureExt;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use tokio_util::task::JoinMap;

/// How long a new link may take to deliver its first packet.
pub(super) const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// What opens the device of each new link. It takes the link's URL and
/// returns the opener its worker opens the device with.
pub(super) type Opens = Box<dyn FnMut(&str) -> Opener + Send>;

/// The number of a link, unique among the links of one registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct LinkId(u64);

/// A client's port on a link. It carries the packets of one subtree of the
/// device, with routes relative to that subtree.
pub(super) struct Port {
    sender: PortSender,
    rx: mpsc::Receiver<Packet>,
}

/// The sending half of a client's port, with routes relative to its scope.
pub(super) struct PortSender {
    tx: mpsc::Sender<Packet>,
    scope: DeviceRoute,
}

/// Why a port could not take a packet. Each carries the packet back.
#[derive(Debug, Clone, thiserror::Error)]
pub(super) enum SendError {
    /// The port's queue is full.
    #[error("channel full")]
    WouldBlock(Packet),
    /// The link's worker is gone.
    #[error("proxy disconnected")]
    ProxyDisconnected(Packet),
    /// The route lies outside the port's scope.
    #[error("route exceeds port scope")]
    InvalidRoute(Packet),
}

impl PortSender {
    /// Sends `packet` to the port without blocking.
    ///
    /// Returns the packet back in a [`SendError`] if the queue is full, the
    /// worker is gone, or the route is outside the port's subtree.
    #[allow(clippy::result_large_err)] // Preserve ownership when submission fails.
    pub(super) fn try_send(&self, packet: Packet) -> Result<(), SendError> {
        if self.scope.absolute_route(&packet.route()).is_err() {
            return Err(SendError::InvalidRoute(packet));
        }
        match self.tx.try_send(packet) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(pkt)) => Err(SendError::WouldBlock(pkt)),
            Err(mpsc::error::TrySendError::Closed(pkt)) => Err(SendError::ProxyDisconnected(pkt)),
        }
    }
}

impl Port {
    /// Separates outgoing routing from the channel of packets the link delivers.
    pub(super) fn into_split(self) -> (PortSender, mpsc::Receiver<Packet>) {
        (self.sender, self.rx)
    }
}

/// Returns the depth of the queue the proxy fills for each of its ports.
/// `TWINLEAF_PROXY_INTERFACE_RX_BUFSIZE` may raise it above the default.
pub(super) fn client_rx_channel_size() -> usize {
    let min_size = transport::DEFAULT_RX_CHANNEL_SIZE;
    if let Ok(req) = env::var("TWINLEAF_PROXY_INTERFACE_RX_BUFSIZE") {
        std::cmp::max(req.parse().unwrap_or(0), min_size)
    } else {
        min_size
    }
}

/// Returns the depth of the queue each port fills for the proxy.
/// `TWINLEAF_PROXY_INTERFACE_TX_BUFSIZE` may raise it above the default.
pub(super) fn client_tx_channel_size() -> usize {
    let min_size = transport::DEFAULT_TX_CHANNEL_SIZE;
    if let Ok(req) = env::var("TWINLEAF_PROXY_INTERFACE_TX_BUFSIZE") {
        std::cmp::max(req.parse().unwrap_or(0), min_size)
    } else {
        min_size
    }
}

/// One device's connection, shared by every client that mounts it.
#[derive(Clone)]
pub(super) struct Link {
    id: LinkId,
    url: Arc<str>,
    /// The worker's commands, which the registry alone keeps open.
    commands: mpsc::WeakSender<Command>,
    /// The link's readiness, as its worker reports it.
    readiness: watch::Receiver<Readiness>,
}

impl Link {
    /// Returns the link's number.
    pub(super) fn id(&self) -> LinkId {
        self.id
    }

    /// Returns the URL the link was opened with.
    pub(super) fn url(&self) -> &str {
        &self.url
    }

    /// Returns the link's readiness.
    pub(super) fn readiness(&self) -> Readiness {
        self.readiness.borrow().clone()
    }

    /// Waits until the link's device has answered.
    ///
    /// # Errors
    ///
    /// Returns why the first open failed, or that the worker ended, if either
    /// comes first.
    pub(super) async fn ready(&self) -> Result<(), String> {
        let mut readiness = self.readiness.clone();
        let settled = readiness.wait_for(|readiness| *readiness != Readiness::Pending);
        let settled = settled.await.map(|settled| settled.clone());
        match settled {
            Ok(Readiness::Ready) => Ok(()),
            Ok(Readiness::Failed(reason)) => Err(reason),
            Ok(Readiness::Pending) | Err(_) => Err("the link closed".to_string()),
        }
    }

    /// Queues a client's port on the subtree at `source` for the worker to
    /// adopt, without blocking.
    ///
    /// Returns an error message if the link is closed, the worker's queue is
    /// full, or the worker is gone.
    pub(super) fn open_port(&self, source: DeviceRoute) -> Result<Port, String> {
        let Some(commands) = self.commands.upgrade() else {
            return Err(format!("the link to {} closed", self.url));
        };
        let (to_worker, from_port) = mpsc::channel(client_tx_channel_size());
        let (to_port, from_worker) = mpsc::channel(client_rx_channel_size());
        let subscriber = Subscriber::new(to_port, from_port, source);
        let reason = match commands.try_send(Command::OpenPort(subscriber)) {
            Ok(()) => {
                return Ok(Port {
                    sender: PortSender {
                        tx: to_worker,
                        scope: source,
                    },
                    rx: from_worker,
                })
            }
            // Transient, as the worker still runs.
            Err(mpsc::error::TrySendError::Full(_)) => "the proxy is busy",
            Err(mpsc::error::TrySendError::Closed(_)) => "failed to set up new proxy client",
        };
        Err(format!("could not open a port on {}: {reason}", self.url))
    }
}

/// A client's use of a link. The registry counts the link's leases, and a
/// lease that drops releases its use.
pub(super) struct Lease {
    link: Link,
    /// The registry's mailbox, which a lease does not keep open.
    requests: mpsc::WeakUnboundedSender<Request>,
}

impl Lease {
    /// Returns the link the lease uses.
    pub(super) fn link(&self) -> &Link {
        &self.link
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(requests) = self.requests.upgrade() {
            let _ = requests.send(Request::Release { link: self.link.id });
        }
    }
}

/// A request of the registry.
enum Request {
    /// A lease on the link to `url`, filed under `key`.
    Acquire {
        url: String,
        key: String,
        reply: oneshot::Sender<Result<Lease, Refusal>>,
    },
    /// A lease on `link`, an open link that `auto` chose.
    Share {
        link: Link,
        reply: oneshot::Sender<Result<Lease, Refusal>>,
    },
    /// The open links whose device has answered.
    Answered {
        reply: oneshot::Sender<Vec<Candidate>>,
    },
    /// The links a `proxy.open` bound.
    Choose { links: Vec<LinkId> },
    /// The end of a lease on `link`.
    Release { link: LinkId },
    /// A test's look at the registry.
    #[cfg(test)]
    Inspect(Box<dyn FnOnce(&mut Registry) + Send>),
}

/// The refusal of a request made once the registry is ending.
fn stopping() -> Refusal {
    Refusal::new(RpcError::State, "the proxy is stopping")
}

/// A handle of the registry of links, which does not keep it running.
/// Clones share one registry.
#[derive(Clone)]
pub(super) struct Links {
    requests: mpsc::WeakUnboundedSender<Request>,
}

/// The registry's task and the one sender that keeps it running. Dropping
/// the owner ends the registry, which closes every link first.
pub(super) struct Owner {
    _requests: mpsc::UnboundedSender<Request>,
    /// The registry's task, which resolves to `None` if it panicked.
    pub(super) task: Fuse<JoinHandle<Option<()>>>,
}

impl Owner {
    /// Ends the registry, and waits until it has ended unless it already
    /// had.
    pub(super) async fn close(self) {
        let Owner { _requests, task } = self;
        drop(_requests);
        if !task.is_terminated() {
            let _ = task.await;
        }
    }
}

impl Links {
    /// Starts a registry with no links, whose tasks run on `tasks`. Their
    /// workers retry a lost device for `reconnect_timeout`, or forever with
    /// `None`, and log the traffic `dump` asks for.
    pub(super) fn new(
        tasks: &Supervisor,
        reconnect_timeout: Option<Duration>,
        dump: DumpFlags,
    ) -> (Links, Owner) {
        let opening = tasks.clone();
        let opens = Box::new(move |url: &str| worker::opener(&opening, url));
        Links::on(tasks, reconnect_timeout, dump, opens)
    }

    /// Starts a registry as [`new`](Self::new) does, with each new link's
    /// device opened as `opens` says.
    pub(super) fn on(
        tasks: &Supervisor,
        reconnect_timeout: Option<Duration>,
        dump: DumpFlags,
        opens: Opens,
    ) -> (Links, Owner) {
        let (requests, mailbox) = mpsc::unbounded_channel();
        let links = Links {
            requests: requests.downgrade(),
        };
        let registry = Registry {
            requests: mailbox,
            leases: links.requests.clone(),
            links: HashMap::new(),
            waiting: HashMap::new(),
            workers: JoinMap::new(),
            next_id: 0,
            tasks: tasks.clone(),
            reconnect_timeout,
            dump,
            opens,
        };
        let task = tasks.spawn("proxy links", registry.run()).fuse();
        let owner = Owner {
            _requests: requests,
            task,
        };
        (links, owner)
    }

    /// Sends the registry the request `ask` makes with a reply sender, and
    /// returns the receiver of the reply. The reply never comes if the
    /// registry is ending.
    fn ask<T>(&self, ask: impl FnOnce(oneshot::Sender<T>) -> Request) -> oneshot::Receiver<T> {
        let (reply, replied) = oneshot::channel();
        if let Some(requests) = self.requests.upgrade() {
            let _ = requests.send(ask(reply));
        }
        replied
    }

    /// Returns a lease on the link to `url`, whose device key is `key`. Opens
    /// a new link if none is open or the open one has stopped. A new link
    /// opens only once the task of every earlier link to the device has
    /// ended.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] if the registry is ending.
    pub(super) async fn acquire(&self, url: &str, key: String) -> Result<Lease, Refusal> {
        let url = url.to_string();
        let acquired = self.ask(|reply| Request::Acquire { url, key, reply });
        acquired.await.unwrap_or_else(|_| Err(stopping()))
    }

    /// Returns a lease on `link`, an open link that `auto` chose.
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] if the link has closed in the meantime, or if
    /// the registry is ending.
    pub(super) async fn share(&self, link: &Link) -> Result<Lease, Refusal> {
        let link = link.clone();
        let shared = self.ask(|reply| Request::Share { link, reply });
        shared.await.unwrap_or_else(|_| Err(stopping()))
    }

    /// Returns the open links whose device has answered, idle ones included,
    /// in order of URL.
    pub(super) async fn answered(&self) -> Vec<Candidate> {
        let answered = self.ask(|reply| Request::Answered { reply });
        answered.await.unwrap_or_default()
    }

    /// Marks `links` as chosen by a client. A chosen link is a candidate for
    /// `auto`, and stays open for [`IDLE`] after its last user leaves.
    pub(super) fn choose(&self, links: Vec<LinkId>) {
        if let Some(requests) = self.requests.upgrade() {
            let _ = requests.send(Request::Choose { links });
        }
    }
}

/// The registry's record of one link whose task has not ended.
struct Entry {
    link: Link,
    /// The only sender of the worker's commands, dropped to close the link.
    commands: Option<mpsc::Sender<Command>>,
    /// The key the link is filed under.
    key: String,
    /// The number of leases on the link.
    users: usize,
    /// When the last lease dropped.
    idle_since: Instant,
    /// Whether a `proxy.open` has bound the link. It is `false` while only
    /// probes have. A link that is not chosen closes as soon as it has no
    /// users.
    chosen: bool,
}

impl Entry {
    /// Returns `true` while the link is open and its worker takes ports.
    fn open(&self) -> bool {
        self.commands
            .as_ref()
            .is_some_and(|commands| !commands.is_closed())
    }

    /// Returns when a chosen open link with no users is due to close, which
    /// is [`IDLE`] after its last lease dropped.
    fn due(&self) -> Option<Instant> {
        let idle = self.commands.is_some() && self.chosen && self.users == 0;
        idle.then(|| self.idle_since + IDLE)
    }
}

/// An acquire held until the earlier links to its device have ended.
struct Waiter {
    url: String,
    reply: oneshot::Sender<Result<Lease, Refusal>>,
}

/// The registry, which runs as the task "proxy links". It is the only owner
/// of the links' users, choices and idle times.
struct Registry {
    requests: mpsc::UnboundedReceiver<Request>,
    /// The mailbox each lease releases its link through. It does not keep
    /// the registry running.
    leases: mpsc::WeakUnboundedSender<Request>,
    /// Every link whose task has not ended. At most one is filed under
    /// each device key.
    links: HashMap<LinkId, Entry>,
    /// The acquires of each device key held until its earlier links end.
    waiting: HashMap<String, Vec<Waiter>>,
    /// The task of each link.
    workers: JoinMap<LinkId, Option<()>>,
    next_id: u64,
    /// The spawner of the links' tasks.
    tasks: Supervisor,
    reconnect_timeout: Option<Duration>,
    dump: DumpFlags,
    opens: Opens,
}

impl Registry {
    /// Serves requests until its owner drops, then closes every link.
    async fn run(mut self) {
        loop {
            let due = self.links.values().filter_map(Entry::due).min();
            tokio::select! {
                request = self.requests.recv() => match request {
                    Some(request) => self.handle(request),
                    None => break,
                },
                Some((id, _outcome)) = self.workers.join_next() => self.ended(id),
                () = sleep_until_some(due) => self.expire(),
            }
        }
        self.close_all().await;
    }

    /// Handles one request.
    fn handle(&mut self, request: Request) {
        match request {
            Request::Acquire { url, key, reply } => self.acquire(url, key, reply),
            Request::Share { link, reply } => {
                let _ = reply.send(self.share(&link));
            }
            Request::Answered { reply } => {
                let _ = reply.send(self.answered());
            }
            Request::Choose { links } => {
                for id in links {
                    if let Some(entry) = self.links.get_mut(&id) {
                        entry.chosen = true;
                    }
                }
            }
            Request::Release { link } => self.release(link),
            #[cfg(test)]
            Request::Inspect(inspect) => inspect(self),
        }
    }

    /// Answers an acquire of `url` under `key` on `reply`. A link whose
    /// worker has stopped is closed. An acquire of a device whose earlier
    /// link still runs waits until it ends.
    fn acquire(
        &mut self,
        url: String,
        key: String,
        reply: oneshot::Sender<Result<Lease, Refusal>>,
    ) {
        let Some((&id, entry)) = self.links.iter().find(|(_, entry)| entry.key == key) else {
            let id = self.start(&url, key);
            let _ = reply.send(Ok(self.lease(id)));
            return;
        };
        if entry.open() {
            let _ = reply.send(Ok(self.lease(id)));
            return;
        }
        self.links
            .get_mut(&id)
            .expect("a link found is held")
            .commands = None;
        let waiter = Waiter { url, reply };
        self.waiting.entry(key).or_default().push(waiter);
    }

    /// Returns a lease on `link`, an open link that `auto` chose.
    fn share(&mut self, link: &Link) -> Result<Lease, Refusal> {
        match self.links.get(&link.id) {
            Some(entry) if entry.open() => Ok(self.lease(link.id)),
            Some(_) | None => {
                let message = format!("the link to {} closed", link.url);
                Err(Refusal::new(RpcError::Load, message))
            }
        }
    }

    /// Returns the open links whose device has answered, idle ones included,
    /// in order of URL.
    fn answered(&self) -> Vec<Candidate> {
        let mut answered: Vec<_> = self
            .links
            .values()
            .filter(|entry| entry.open() && entry.link.readiness() == Readiness::Ready)
            .map(|entry| Candidate {
                link: entry.link.clone(),
                chosen: entry.chosen,
            })
            .collect();
        answered.sort_by(|a, b| a.link.url.cmp(&b.link.url));
        answered
    }

    /// Returns a new lease on link `id`.
    fn lease(&mut self, id: LinkId) -> Lease {
        let entry = self.links.get_mut(&id).expect("a link leased is held");
        entry.users += 1;
        Lease {
            link: entry.link.clone(),
            requests: self.leases.clone(),
        }
    }

    /// Counts the end of a lease on link `id`. A link with no users left is
    /// closed at once if no client chose it, and starts its idle time if one
    /// did.
    fn release(&mut self, id: LinkId) {
        let Some(entry) = self.links.get_mut(&id) else {
            return;
        };
        entry.users = entry.users.saturating_sub(1);
        if entry.users > 0 || entry.commands.is_none() {
            return;
        }
        if entry.chosen {
            entry.idle_since = Instant::now();
            return;
        }
        if entry.open() {
            log::debug!("Closing {}, as its probes are done", entry.link.url);
        }
        entry.commands = None;
    }

    /// Closes each chosen link that has had no users for [`IDLE`].
    fn expire(&mut self) {
        let now = Instant::now();
        let due = self
            .links
            .values_mut()
            .filter(|entry| entry.due().is_some_and(|due| due <= now));
        for entry in due {
            log::info!("Closing {}, unused for {:?}", entry.link.url, IDLE);
            entry.commands = None;
        }
    }

    /// Opens a new link to `url` under `key`, and spawns its worker.
    fn start(&mut self, url: &str, key: String) -> LinkId {
        log::info!("Opening {url}");
        let id = LinkId(self.next_id);
        self.next_id += 1;
        let open = (self.opens)(url);
        let (commands, readiness, run) = worker::new(url, self.reconnect_timeout, self.dump, open);
        self.tasks
            .spawn_in_map(&mut self.workers, id, format_args!("proxy link {url}"), run);
        let link = Link {
            id,
            url: url.into(),
            commands: commands.downgrade(),
            readiness,
        };
        let entry = Entry {
            link,
            commands: Some(commands),
            key,
            users: 0,
            idle_since: Instant::now(),
            chosen: false,
        };
        self.links.insert(id, entry);
        id
    }

    /// Forgets the link whose task ended, and opens a new link for the
    /// acquires that waited for its device.
    fn ended(&mut self, id: LinkId) {
        let Some(entry) = self.links.remove(&id) else {
            return;
        };
        if let Readiness::Failed(reason) = entry.link.readiness() {
            log::debug!("Closed {}: {reason}", entry.link.url);
        }
        self.serve(entry.key);
    }

    /// Opens a new link for the acquires of `key` that waited, once no
    /// earlier link to the device runs. Every waiter shares the link.
    fn serve(&mut self, key: String) {
        if self.links.values().any(|entry| entry.key == key) {
            return;
        }
        let Some(waiters) = self.waiting.remove(&key) else {
            return;
        };
        let mut waiters = waiters
            .into_iter()
            .filter(|waiter| !waiter.reply.is_closed());
        let Some(first) = waiters.next() else {
            return;
        };
        let id = self.start(&first.url, key);
        for waiter in [first].into_iter().chain(waiters) {
            let _ = waiter.reply.send(Ok(self.lease(id)));
        }
    }

    /// Refuses every acquire waiting, closes every link, and waits at most
    /// [`LEAVE_TIMEOUT`] for their tasks. A blocking open runs to its end.
    async fn close_all(mut self) {
        self.waiting.clear();
        for entry in self.links.values_mut() {
            entry.commands = None;
        }
        let joined = timeout(LEAVE_TIMEOUT, async {
            while let Some((id, _outcome)) = self.workers.join_next().await {
                self.ended(id);
            }
        });
        if joined.await.is_err() {
            log::warn!(
                "{} links still held their devices {LEAVE_TIMEOUT:?} after the proxy stopped",
                self.workers.len()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::fixtures::{
        assert_took, fakes, opened, FakeDevice, First, Opened, PAUSED_WAIT, UNREACHABLE, WAIT,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use tokio::sync::Notify;

    impl Link {
        /// Creates a link to `url` with no worker. Returns it with the
        /// sender that keeps its commands open, and their receiver.
        pub(crate) fn idle(url: &str) -> (Link, mpsc::Sender<Command>, mpsc::Receiver<Command>) {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);
            let (commands, receiver) = mpsc::channel(16);
            let link = Link {
                id: LinkId(NEXT_ID.fetch_add(1, Ordering::Relaxed)),
                url: url.into(),
                commands: commands.downgrade(),
                readiness: watch::channel(Readiness::Pending).1,
            };
            (link, commands, receiver)
        }
    }

    impl Registry {
        /// Returns the URLs of the open links, in order.
        fn urls(&self) -> Vec<String> {
            let open = self.links.values().filter(|entry| entry.commands.is_some());
            let mut urls: Vec<_> = open.map(|entry| entry.link.url.to_string()).collect();
            urls.sort();
            urls
        }
    }

    impl Links {
        /// Has the registry run `inspect`. Returns the receiver of what it
        /// returns, which disconnects if the registry has ended.
        fn inspect<T: Send + 'static>(
            &self,
            inspect: impl FnOnce(&mut Registry) -> T + Send + 'static,
        ) -> oneshot::Receiver<T> {
            self.ask(|reply| {
                Request::Inspect(Box::new(move |registry| {
                    let _ = reply.send(inspect(registry));
                }))
            })
        }

        /// Closes the link to `url`, as a link left idle is closed. Returns
        /// the receiver of whether one was open.
        pub(crate) fn close(&self, url: &str) -> oneshot::Receiver<bool> {
            let key = worker::device_key(url);
            self.inspect(move |registry| {
                let entries = registry.links.values_mut();
                let open = entries.filter(|entry| entry.key == key && entry.commands.is_some());
                open.map(|entry| entry.commands = None).count() > 0
            })
        }

        /// Makes the registry panic.
        pub(crate) fn panic(&self) {
            drop(self.inspect(|_| panic!("a test asked the registry to panic")));
        }

        /// Returns a lease on the link to `url` as [`acquire`](Self::acquire)
        /// does, blocking until the registry answers.
        pub(crate) fn acquire_blocking(&self, url: &str) -> Result<Lease, Refusal> {
            let (url, key) = (url.to_string(), worker::device_key(url));
            let acquired = self.ask(|reply| Request::Acquire { url, key, reply });
            acquired.blocking_recv().unwrap_or_else(|_| Err(stopping()))
        }

        /// Returns the URLs of every open link, in order.
        pub(crate) fn urls(&self) -> Vec<String> {
            let urls = self.inspect(|registry| registry.urls());
            urls.blocking_recv().unwrap_or_default()
        }

        /// Returns the total number of leases on the open links.
        pub(crate) fn users(&self) -> usize {
            let users = self.inspect(|registry| {
                let open = registry
                    .links
                    .values()
                    .filter(|entry| entry.commands.is_some());
                open.map(|entry| entry.users).sum()
            });
            users.blocking_recv().unwrap_or_default()
        }

        /// Files the link to `url` under `key`, where `url` no longer finds
        /// it. A replug that moves the device to another port leaves a link
        /// this way.
        ///
        /// # Panics
        ///
        /// Panics if no link to `url` is open.
        pub(crate) fn rekey(&self, url: &str, key: &str) {
            let (from, to) = (worker::device_key(url), key.to_string());
            let rekeyed = self.inspect(move |registry| {
                let mut open = registry
                    .links
                    .values_mut()
                    .filter(|entry| entry.commands.is_some());
                let entry = open.find(|entry| entry.key == from);
                entry.map(|entry| entry.key = to).is_some()
            });
            assert!(rekeyed.blocking_recv().unwrap_or(false), "no link to rekey");
        }

        /// Blocks for at most `timeout` until no link's task is running.
        /// Returns `true` if none is running, as when the registry has
        /// ended.
        pub(crate) fn wait_finished(&self, timeout: Duration) -> bool {
            let deadline = std::time::Instant::now() + timeout;
            while let Ok(false) = self
                .inspect(|registry| registry.links.is_empty())
                .blocking_recv()
            {
                if std::time::Instant::now() >= deadline {
                    return false;
                }
                thread::sleep(Duration::from_millis(10));
            }
            true
        }
    }

    /// A link to nothing whose worker never runs, with the sender that
    /// keeps its commands open and their far end.
    fn idle_link() -> (Link, mpsc::Sender<Command>, mpsc::Receiver<Command>) {
        Link::idle(UNREACHABLE)
    }

    /// A worker stuck acquiring a transport it will never open must not hold
    /// up a client waiting for its port.
    #[test]
    fn opening_a_port_does_not_wait_for_a_worker_that_never_answers() {
        let (link, _open, mut commands) = idle_link();

        let _port = link
            .open_port(DeviceRoute::root())
            .expect("the port is queued");

        assert!(matches!(commands.try_recv(), Ok(Command::OpenPort(_))));
    }

    /// A congested command lane says nothing about whether the worker lives,
    /// so it must never be reported as the worker being gone.
    #[test]
    fn a_full_command_lane_is_distinct_from_a_stopped_worker() {
        let (link, _open, commands) = idle_link();
        while link.open_port(DeviceRoute::root()).is_ok() {}

        let busy = link.open_port(DeviceRoute::root()).err().unwrap();
        assert!(busy.ends_with("the proxy is busy"), "{busy}");
        drop(commands);
        let gone = link.open_port(DeviceRoute::root()).err().unwrap();
        assert!(
            gone.ends_with("failed to set up new proxy client"),
            "{gone}"
        );
    }

    /// A port's routes are relative to its scope, and together they reach no
    /// deeper than a route can.
    #[test]
    fn a_port_rejects_routes_that_reach_too_deep() {
        let (link, _open, _commands) = idle_link();
        let (port, _received) = link.open_port("/1".parse().unwrap()).unwrap().into_split();

        let too_deep = Packet::heartbeat("/2/3/4/5/6/7/8/9".parse().unwrap());
        assert!(matches!(
            port.try_send(too_deep),
            Err(SendError::InvalidRoute(_))
        ));
        assert!(port
            .try_send(Packet::heartbeat("/2".parse().unwrap()))
            .is_ok());
    }

    /// The device a test opens.
    const DEVICE: &str = "tcp://device";
    /// Another device a test opens.
    const OTHER: &str = "tcp://other";

    /// A registry on the test's runtime whose links open fake devices, the
    /// first link to each URL in `first` as it says. Returns it with the
    /// owner and the receiver of each device port it opens.
    fn registry(first: Vec<(&str, First)>) -> (Links, Owner, mpsc::UnboundedReceiver<Opened>) {
        let (opens, opened) = fakes(first);
        let (links, owner) = Links::on(&Supervisor::current(), None, DumpFlags::default(), opens);
        (links, owner, opened)
    }

    /// Returns a lease on the link to `url`.
    async fn acquire(links: &Links, url: &str) -> Lease {
        let acquired = links.acquire(url, worker::device_key(url)).await;
        acquired.unwrap_or_else(|refusal| panic!("{url} was refused: {}", refusal.message))
    }

    /// Returns the URLs of the open links, in order.
    async fn urls(links: &Links) -> Vec<String> {
        let urls = links.inspect(|registry| registry.urls());
        urls.await.expect("the registry answers")
    }

    /// Waits until the port of `device` lets go of it, and returns when.
    async fn released(device: &mut Opened) -> Instant {
        let gone = timeout(PAUSED_WAIT, device.end.recv()).await;
        let gone = gone.expect("the device stayed open");
        assert!(gone.is_none(), "the device was sent a packet");
        Instant::now()
    }

    /// A link that only probes have used gets no grace. It closes as soon as
    /// its last user leaves.
    #[tokio::test(start_paused = true)]
    async fn a_link_only_probes_used_closes_as_soon_as_they_leave() {
        let (links, _owner, mut devices) = registry(Vec::new());
        let probe = acquire(&links, DEVICE).await;
        let mut device = opened(&mut devices).await;
        assert_eq!(urls(&links).await, [DEVICE]);

        drop(probe);
        let left = Instant::now();

        assert_took(
            released(&mut device).await - left,
            Duration::ZERO,
            "closing",
        );
        assert!(urls(&links).await.is_empty());
    }

    /// A probe that overlaps an open shares its link. The link counts as
    /// chosen, so it keeps its grace after both leave. It closes once it has
    /// had no user for [`IDLE`].
    #[tokio::test(start_paused = true)]
    async fn a_link_a_probe_shared_with_an_open_keeps_its_grace() {
        let (links, _owner, mut devices) = registry(Vec::new());
        let probe = acquire(&links, DEVICE).await;
        let open = acquire(&links, DEVICE).await;
        assert_eq!(probe.link().id(), open.link().id());
        links.choose(vec![open.link().id()]);
        let mut device = opened(&mut devices).await;

        drop(probe);
        drop(open);
        let left = Instant::now();

        tokio::time::sleep(IDLE / 2).await;
        assert_eq!(urls(&links).await, [DEVICE]);
        assert_took(released(&mut device).await - left, IDLE, "closing");
        assert!(urls(&links).await.is_empty());
    }

    /// A new link waits while the task of an earlier link to its device
    /// runs, here one whose first open cannot be dropped, however long that
    /// takes. A link to another device opens meanwhile.
    #[tokio::test(start_paused = true)]
    async fn a_new_link_waits_for_an_earlier_worker_of_its_own_device_alone() {
        let gate = Arc::new(Notify::new());
        let (links, _owner, mut devices) = registry(vec![(DEVICE, First::Waits(gate.clone()))]);
        drop(acquire(&links, DEVICE).await);

        let reopening = tokio::spawn({
            let links = links.clone();
            async move { acquire(&links, DEVICE).await }
        });
        let _other = acquire(&links, OTHER).await;
        // Its device stays, as a device that hung up would be reopened.
        let other = opened(&mut devices).await;
        assert_eq!(other.url, OTHER);
        tokio::time::sleep(LEAVE_TIMEOUT * 4).await;
        assert!(
            !reopening.is_finished(),
            "the link opened as the worker ran"
        );
        assert_eq!(urls(&links).await, [OTHER]);

        gate.notify_one();

        let mut lingering = opened(&mut devices).await;
        assert_eq!(lingering.url, DEVICE);
        released(&mut lingering).await;
        let reopened = reopening.await.unwrap();
        assert_eq!(reopened.link().url(), DEVICE);
        assert_eq!(opened(&mut devices).await.url, DEVICE);
        assert_eq!(urls(&links).await, [DEVICE, OTHER]);
    }

    /// A registry whose owner drops lets each worker write what was queued for
    /// its device first, though the device takes half a second to read it.
    #[tokio::test(start_paused = true)]
    async fn an_ending_registry_lets_its_workers_write_what_was_queued() {
        let (links, owner, mut devices) = registry(Vec::new());
        let lease = acquire(&links, DEVICE).await;
        let mut device = opened(&mut devices).await;
        device
            .end
            .send(&Packet::heartbeat(DeviceRoute::root()))
            .await;
        assert_eq!(lease.link().ready().await, Ok(()));
        let (port, _received) = lease
            .link()
            .open_port(DeviceRoute::root())
            .unwrap()
            .into_split();
        let queued = 2000;
        for _ in 0..queued {
            port.try_send(Packet::heartbeat(DeviceRoute::root()))
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop((port, lease));
        let ending = Instant::now();

        let ended = tokio::spawn(owner.close());
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut written = 0;
        while let Some(_pkt) = timeout(PAUSED_WAIT, device.end.recv()).await.unwrap() {
            written += 1;
        }

        assert_eq!(written, queued);
        ended.await.unwrap();
        assert!(ending.elapsed() < LEAVE_TIMEOUT, "{:?}", ending.elapsed());
    }

    /// A link whose task panics has stopped, and the next open of its
    /// device makes a new link.
    #[tokio::test(start_paused = true)]
    async fn a_panic_in_a_link_task_frees_its_key() {
        let (links, _owner, mut devices) = registry(vec![(DEVICE, First::Panics)]);
        let first = acquire(&links, DEVICE).await;
        let failed = first.link().ready().await;
        assert_eq!(failed, Err("the link closed".to_string()));

        let second = acquire(&links, DEVICE).await;

        assert_ne!(second.link().id(), first.link().id());
        assert_eq!(opened(&mut devices).await.url, DEVICE);
        assert_eq!(urls(&links).await, [DEVICE]);
    }

    /// Blocks until `link` has settled, and returns how.
    fn settled(link: &Link) -> Readiness {
        let deadline = std::time::Instant::now() + WAIT;
        while link.readiness() == Readiness::Pending {
            assert!(
                std::time::Instant::now() < deadline,
                "the link never settled"
            );
            thread::sleep(Duration::from_millis(10));
        }
        link.readiness()
    }

    /// A device that takes one connection at a time opens again at once
    /// after its probe-only link closes. The new link waits for the old
    /// worker to let go, so the device refuses nothing.
    #[test]
    fn a_device_reopened_as_its_link_closes_waits_for_the_old_worker() {
        let device = FakeDevice::start();
        device.exclusive();
        let (links, owner) = Links::new(&Supervisor::new(), None, DumpFlags::default());
        let probe = links.acquire_blocking(&device.url).unwrap();
        assert_eq!(settled(probe.link()), Readiness::Ready);
        drop(probe);

        let open = links.acquire_blocking(&device.url).unwrap();

        assert_eq!(settled(open.link()), Readiness::Ready);
        assert_eq!((device.accepted(), device.refused()), (2, 0));
        drop(open);
        assert!(links.wait_finished(WAIT));
        crate::rt::handle().block_on(owner.close());
    }

    #[test]
    fn nothing_opens_once_the_registry_has_ended() {
        let (links, owner) = Links::new(&Supervisor::new(), None, DumpFlags::default());

        crate::rt::handle().block_on(owner.close());

        let refusal = links.acquire_blocking(UNREACHABLE).err().unwrap();
        assert_eq!(refusal.code, RpcError::State);
        assert!(links.urls().is_empty());
        assert!(links.wait_finished(Duration::ZERO));
    }
}

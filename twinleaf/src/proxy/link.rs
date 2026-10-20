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
//! A new link to a device opens only once the task of every earlier link to
//! it has ended, and with it the worker's hold on the device. Links to other
//! devices open meanwhile. The registry aborts a task that still runs
//! [`RELEASE_TIMEOUT`] after its link closed.

use super::auto::Candidate;
use super::control::Refusal;
use super::worker::{self, Command, Opener, Readiness, Subscriber};
use super::{DumpFlags, IDLE};
use crate::proto::rpc::RpcError;
use crate::proto::DeviceRoute;
use crate::rt::{sleep_until_some, Supervisor};
use crate::tio::packet::Packet;
use crate::tio::transport::{self, CLOSE_TIMEOUT};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::JoinMap;

/// How long a new link may take to deliver its first packet.
pub(super) const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the task of a closed link may run before the registry aborts it.
// A serial port opens once at a time, and so do devices that take one TCP
// connection. The worker may take up to CLOSE_TIMEOUT to close its
// transport.
const RELEASE_TIMEOUT: Duration = CLOSE_TIMEOUT.saturating_add(Duration::from_millis(500));

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
    /// The sender of the worker's commands.
    commands: mpsc::Sender<Command>,
    /// The link's readiness, as its worker reports it.
    readiness: watch::Receiver<Readiness>,
    /// Cancelled once the link is closed, which stops the worker.
    closed: CancellationToken,
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

    /// Returns `true` if the link's worker has stopped.
    fn stopped(&self) -> bool {
        matches!(self.readiness(), Readiness::Failed(_))
    }

    /// Waits until the link's device has answered.
    ///
    /// # Errors
    ///
    /// Returns why the link's worker stopped, if it stops first.
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
        if self.closed.is_cancelled() {
            return Err(format!("the link to {} closed", self.url));
        }
        let (to_worker, from_port) = mpsc::channel(client_tx_channel_size());
        let (to_port, from_worker) = mpsc::channel(client_rx_channel_size());
        let subscriber = Subscriber::new(to_port, from_port, source);
        let reason = match self.commands.try_send(Command::OpenPort(subscriber)) {
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
    /// The registry's mailbox.
    requests: mpsc::UnboundedSender<Request>,
}

impl Lease {
    /// Returns the link the lease uses.
    pub(super) fn link(&self) -> &Link {
        &self.link
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let _ = self.requests.send(Request::Release { link: self.link.id });
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
    /// The close of every link, waiting at most `within` for their tasks.
    CloseAll {
        within: Duration,
        reply: oneshot::Sender<bool>,
    },
    /// A test's look at the registry.
    #[cfg(test)]
    Inspect(Box<dyn FnOnce(&mut Registry) + Send>),
}

/// The refusal of a request made once every link is closed.
fn stopping() -> Refusal {
    Refusal::new(RpcError::State, "the proxy is stopping")
}

/// The handle of the registry of links. Clones share one registry.
#[derive(Clone)]
pub(super) struct Links {
    requests: mpsc::UnboundedSender<Request>,
}

impl Links {
    /// Starts a registry with no links, whose tasks run on `tasks`. Their
    /// workers retry a lost device for `reconnect_timeout`, or forever with
    /// `None`, and log the traffic `dump` asks for.
    pub(super) fn new(
        tasks: &Supervisor,
        reconnect_timeout: Option<Duration>,
        dump: DumpFlags,
    ) -> Links {
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
    ) -> Links {
        let (requests, mailbox) = mpsc::unbounded_channel();
        let registry = Registry {
            requests: mailbox,
            leases: requests.downgrade(),
            links: HashMap::new(),
            waiting: HashMap::new(),
            workers: JoinMap::new(),
            closed: false,
            next_id: 0,
            tasks: tasks.clone(),
            reconnect_timeout,
            dump,
            opens,
        };
        tasks.spawn("proxy links", registry.run());
        Links { requests }
    }

    /// Sends the registry the request `ask` makes with a reply sender, and
    /// returns the receiver of the reply. The reply never comes if the
    /// registry has ended.
    fn ask<T>(&self, ask: impl FnOnce(oneshot::Sender<T>) -> Request) -> oneshot::Receiver<T> {
        let (reply, replied) = oneshot::channel();
        let _ = self.requests.send(ask(reply));
        replied
    }

    /// Returns a lease on the link to `url`, whose device key is `key`. Opens
    /// a new link if none is open or the open one has stopped. A new link
    /// opens only once the task of every earlier link to the device has
    /// ended, or was aborted after [`RELEASE_TIMEOUT`].
    ///
    /// # Errors
    ///
    /// Returns a [`Refusal`] if every link is closed.
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
    /// every link is closed.
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
        let _ = self.requests.send(Request::Choose { links });
    }

    /// Closes every link, and waits until their tasks have ended. Aborts the
    /// tasks still running after `within`. No link opens after this, and the
    /// registry ends. Returns `true` if every task ended in time, as when the
    /// registry has ended.
    pub(super) async fn close_all(&self, within: Duration) -> bool {
        let closed = self.ask(|reply| Request::CloseAll { within, reply });
        closed.await.unwrap_or(true)
    }

    /// Waits until the registry has ended.
    pub(super) async fn ended(&self) {
        self.requests.closed().await;
    }
}

/// The registry's record of one link whose task has not ended.
struct Entry {
    link: Link,
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
    /// When the registry aborts the link's task, once the link is closed and
    /// until the task is aborted.
    abort_at: Option<Instant>,
    /// Whether the link is closed.
    closed: bool,
}

impl Entry {
    /// Returns when the link is due to close or its task to be aborted, if
    /// either is due. A chosen open link with no users is due [`IDLE`] after
    /// its last lease dropped.
    fn due(&self) -> Option<Instant> {
        let idle = !self.closed && self.chosen && self.users == 0;
        let idle = idle.then(|| self.idle_since + IDLE);
        idle.into_iter().chain(self.abort_at).min()
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
    /// Set once every link is closed. No link opens after that.
    closed: bool,
    next_id: u64,
    /// The spawner of the links' tasks.
    tasks: Supervisor,
    reconnect_timeout: Option<Duration>,
    dump: DumpFlags,
    opens: Opens,
}

impl Registry {
    /// Serves requests until every link is closed, or until every handle and
    /// lease is gone, which closes every link.
    async fn run(mut self) {
        while !self.closed {
            let due = self.links.values().filter_map(Entry::due).min();
            tokio::select! {
                request = self.requests.recv() => match request {
                    Some(request) => self.handle(request).await,
                    None => break,
                },
                Some((id, _outcome)) = self.workers.join_next() => self.ended(id),
                () = sleep_until_some(due) => self.expire(),
            }
        }
        self.close_all(CLOSE_TIMEOUT).await;
    }

    /// Handles one request.
    async fn handle(&mut self, request: Request) {
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
            Request::CloseAll { within, reply } => {
                let _ = reply.send(self.close_all(within).await);
            }
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
        if self.closed {
            let _ = reply.send(Err(stopping()));
            return;
        }
        let Some((&id, entry)) = self.links.iter().find(|(_, entry)| entry.key == key) else {
            let id = self.start(&url, key);
            let _ = reply.send(self.lease(id));
            return;
        };
        if !entry.closed && !entry.link.stopped() {
            let _ = reply.send(self.lease(id));
            return;
        }
        self.close(id);
        let waiter = Waiter { url, reply };
        self.waiting.entry(key).or_default().push(waiter);
    }

    /// Returns a lease on `link`, an open link that `auto` chose.
    fn share(&mut self, link: &Link) -> Result<Lease, Refusal> {
        if self.closed {
            return Err(stopping());
        }
        match self.links.get(&link.id) {
            Some(entry) if !entry.closed => self.lease(link.id),
            _ => {
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
            .filter(|entry| !entry.closed && entry.link.readiness() == Readiness::Ready)
            .map(|entry| Candidate {
                link: entry.link.clone(),
                chosen: entry.chosen,
            })
            .collect();
        answered.sort_by(|a, b| a.link.url.cmp(&b.link.url));
        answered
    }

    /// Returns a new lease on link `id`.
    ///
    /// Returns a [`Refusal`] if every handle of the registry is gone.
    fn lease(&mut self, id: LinkId) -> Result<Lease, Refusal> {
        let requests = self.leases.upgrade().ok_or_else(stopping)?;
        let entry = self.links.get_mut(&id).expect("a link leased is held");
        entry.users += 1;
        Ok(Lease {
            link: entry.link.clone(),
            requests,
        })
    }

    /// Counts the end of a lease on link `id`. A link with no users left is
    /// closed at once if no client chose it, and starts its idle time if one
    /// did.
    fn release(&mut self, id: LinkId) {
        let Some(entry) = self.links.get_mut(&id) else {
            return;
        };
        entry.users = entry.users.saturating_sub(1);
        if entry.users > 0 || entry.closed {
            return;
        }
        if entry.chosen {
            entry.idle_since = Instant::now();
            return;
        }
        if !entry.link.stopped() {
            log::debug!("Closing {}, as its probes are done", entry.link.url);
        }
        self.close(id);
    }

    /// Closes each chosen link that has had no users for [`IDLE`], and
    /// aborts each task still running [`RELEASE_TIMEOUT`] after its link
    /// closed.
    fn expire(&mut self) {
        let now = Instant::now();
        let due: Vec<LinkId> = self
            .links
            .iter()
            .filter(|(_, entry)| entry.due().is_some_and(|due| due <= now))
            .map(|(&id, _)| id)
            .collect();
        for id in due {
            let entry = &self.links[&id];
            if !entry.closed {
                log::info!("Closing {}, unused for {:?}", entry.link.url, IDLE);
                self.close(id);
                continue;
            }
            let entry = self.links.get_mut(&id).expect("a link due is held");
            log::warn!(
                "Aborted the link to {}, whose worker still held the device {:?} after it closed",
                entry.link.url,
                RELEASE_TIMEOUT
            );
            self.workers.abort(&id);
            entry.abort_at = None;
        }
    }

    /// Opens a new link to `url` under `key`, and spawns its worker.
    fn start(&mut self, url: &str, key: String) -> LinkId {
        log::info!("Opening {url}");
        let id = LinkId(self.next_id);
        self.next_id += 1;
        let closed = CancellationToken::new();
        let open = (self.opens)(url);
        let (commands, readiness, run) =
            worker::new(url, self.reconnect_timeout, self.dump, open, closed.clone());
        self.tasks
            .spawn_in_map(&mut self.workers, id, format_args!("proxy link {url}"), run);
        let link = Link {
            id,
            url: url.into(),
            commands,
            readiness,
            closed,
        };
        let entry = Entry {
            link,
            key,
            users: 0,
            idle_since: Instant::now(),
            chosen: false,
            abort_at: None,
            closed: false,
        };
        self.links.insert(id, entry);
        id
    }

    /// Closes link `id`, which stops its worker. The registry aborts its task
    /// if it still runs [`RELEASE_TIMEOUT`] from now.
    fn close(&mut self, id: LinkId) {
        let entry = self.links.get_mut(&id).expect("a link closed is held");
        if entry.closed {
            return;
        }
        entry.closed = true;
        entry.abort_at = Some(Instant::now() + RELEASE_TIMEOUT);
        entry.link.closed.cancel();
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
            let _ = waiter.reply.send(self.lease(id));
        }
    }

    /// Closes every link and refuses every acquire still waiting. No link
    /// opens after this. Waits at most `within` for the links' tasks to end,
    /// then aborts the rest. Returns `true` if every task ended in time.
    async fn close_all(&mut self, within: Duration) -> bool {
        self.closed = true;
        for waiter in self.waiting.drain().flat_map(|(_, waiters)| waiters) {
            let _ = waiter.reply.send(Err(stopping()));
        }
        let ids: Vec<_> = self.links.keys().copied().collect();
        for id in ids {
            self.close(id);
        }
        let ended = timeout(within, self.join_all()).await.is_ok();
        if !ended {
            self.workers.abort_all();
            self.join_all().await;
        }
        ended
    }

    /// Waits until every link's task has ended.
    async fn join_all(&mut self) {
        while let Some((id, _outcome)) = self.workers.join_next().await {
            self.ended(id);
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
        /// receiver of the commands it sends.
        pub(crate) fn idle(url: &str) -> (Link, mpsc::Receiver<Command>) {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);
            let (commands, receiver) = mpsc::channel(16);
            let link = Link {
                id: LinkId(NEXT_ID.fetch_add(1, Ordering::Relaxed)),
                url: url.into(),
                commands,
                readiness: watch::channel(Readiness::Pending).1,
                closed: CancellationToken::new(),
            };
            (link, receiver)
        }
    }

    impl Registry {
        /// Returns the URLs of the open links, in order.
        fn urls(&self) -> Vec<String> {
            let open = self.links.values().filter(|entry| !entry.closed);
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

        /// Closes every link as [`close_all`](Self::close_all) does,
        /// blocking until it is done.
        pub(crate) fn close_all_blocking(&self, within: Duration) -> bool {
            let closed = self.ask(|reply| Request::CloseAll { within, reply });
            closed.blocking_recv().unwrap_or(true)
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
                let open = registry.links.values().filter(|entry| !entry.closed);
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
                let mut open = registry.links.values_mut().filter(|entry| !entry.closed);
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

    /// A link to nothing whose worker never runs, with the far end of the
    /// commands it sends.
    fn idle_link() -> (Link, mpsc::Receiver<Command>) {
        Link::idle(UNREACHABLE)
    }

    /// A worker stuck acquiring a transport it will never open must not hold
    /// up a client waiting for its port.
    #[test]
    fn opening_a_port_does_not_wait_for_a_worker_that_never_answers() {
        let (link, mut commands) = idle_link();

        let _port = link
            .open_port(DeviceRoute::root())
            .expect("the port is queued");

        assert!(matches!(commands.try_recv(), Ok(Command::OpenPort(_))));
    }

    /// A congested command lane says nothing about whether the worker lives,
    /// so it must never be reported as the worker being gone.
    #[test]
    fn a_full_command_lane_is_distinct_from_a_stopped_worker() {
        let (link, commands) = idle_link();
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
        let (link, _commands) = idle_link();
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
    /// receiver of each device port it opens.
    fn registry(first: Vec<(&str, First)>) -> (Links, mpsc::UnboundedReceiver<Opened>) {
        let (opens, opened) = fakes(first);
        let links = Links::on(&Supervisor::current(), None, DumpFlags::default(), opens);
        (links, opened)
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
        let (links, mut devices) = registry(Vec::new());
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
        let (links, mut devices) = registry(Vec::new());
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
    /// runs, here one still in its first open. A link to another device
    /// opens meanwhile.
    #[tokio::test(start_paused = true)]
    async fn a_new_link_waits_for_an_earlier_worker_of_its_own_device_alone() {
        let gate = Arc::new(Notify::new());
        let (links, mut devices) = registry(vec![(DEVICE, First::Waits(gate.clone()))]);
        drop(acquire(&links, DEVICE).await);

        let reopening = tokio::spawn({
            let links = links.clone();
            async move { acquire(&links, DEVICE).await }
        });
        let _other = acquire(&links, OTHER).await;
        // Its device stays, as a device that hung up would be reopened.
        let other = opened(&mut devices).await;
        assert_eq!(other.url, OTHER);
        tokio::time::sleep(RELEASE_TIMEOUT / 2).await;
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

    /// A worker that never lets go holds up a new link to its device for
    /// [`RELEASE_TIMEOUT`] from its close. Then its task is aborted, and the
    /// new link opens and is ready once its device answers.
    #[tokio::test(start_paused = true)]
    async fn a_new_link_opens_once_a_worker_that_never_lets_go_is_aborted() {
        let (links, mut devices) = registry(vec![(DEVICE, First::Hangs)]);
        drop(acquire(&links, DEVICE).await);
        let closed = Instant::now();

        let reopened = acquire(&links, DEVICE).await;

        assert_took(closed.elapsed(), RELEASE_TIMEOUT, "the abort");
        let mut device = opened(&mut devices).await;
        assert_eq!(device.url, DEVICE);
        let answer = Packet::heartbeat(DeviceRoute::root());
        device.end.send(&answer).await;
        assert_eq!(reopened.link().ready().await, Ok(()));
        assert_eq!(urls(&links).await, [DEVICE]);
    }

    /// A link whose task panics has stopped, and the next open of its
    /// device makes a new link.
    #[tokio::test(start_paused = true)]
    async fn a_panic_in_a_link_task_frees_its_key() {
        let (links, mut devices) = registry(vec![(DEVICE, First::Panics)]);
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
        let links = Links::new(&Supervisor::new(), None, DumpFlags::default());
        let probe = links.acquire_blocking(&device.url).unwrap();
        assert_eq!(settled(probe.link()), Readiness::Ready);
        drop(probe);

        let open = links.acquire_blocking(&device.url).unwrap();

        assert_eq!(settled(open.link()), Readiness::Ready);
        assert_eq!((device.accepted(), device.refused()), (2, 0));
        drop(open);
        assert!(links.close_all_blocking(WAIT));
        assert!(links.wait_finished(WAIT));
    }

    #[test]
    fn nothing_opens_once_every_link_is_closed() {
        let links = Links::new(&Supervisor::new(), None, DumpFlags::default());

        links.close_all_blocking(WAIT);

        let refusal = links.acquire_blocking(UNREACHABLE).err().unwrap();
        assert_eq!(refusal.code, RpcError::State);
        assert!(links.urls().is_empty());
        assert!(links.wait_finished(Duration::ZERO));
    }
}

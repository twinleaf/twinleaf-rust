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

use super::engine::{self, Mount, Mounts};
use super::outbox::Outbox;
use super::worker::{self, OpenPort, Opener, Readiness};
use super::{DumpFlags, Layout, IDLE, LEAVE_TIMEOUT};
use crate::chan::{oneshot, JoinHandle};
use crate::proto::rpc::RpcError;
use crate::proto::DeviceRoute;
use crate::rt::{sleep_until_some, Supervisor};
use crate::session::Refusal;
use crate::session::Store;
use crate::tio::packet::Packet;
use futures_util::future::{Fuse, FusedFuture};
use futures_util::FutureExt;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Instant};
use tokio_util::task::JoinMap;

/// What opens the device of each new link. It takes the link's URL and
/// returns the opener its worker opens the device with.
pub(super) type Opens = Box<dyn FnMut(&str) -> Opener + Send>;

/// The number of a link, unique among the links of one registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct LinkId(u64);

/// A link the proxy has open whose device has answered, as `auto` weighs
/// it.
#[derive(Clone)]
pub(super) struct Candidate {
    /// The link.
    pub(super) link: Link,
    /// Whether a `proxy.open` has bound it. It is `false` while only probes
    /// have.
    pub(super) chosen: bool,
}

/// One device's connection, shared by every client that mounts it.
#[derive(Clone)]
pub(super) struct Link {
    id: LinkId,
    url: Arc<str>,
    /// The worker's commands, which the registry alone keeps open.
    commands: mpsc::WeakSender<OpenPort>,
    /// The link's readiness, as its worker reports it.
    readiness: watch::Receiver<Readiness>,
    /// The state of the link's devices, which its engine keeps.
    store: Arc<Mutex<Store>>,
    /// The clients' mounts of the link, which its engine feeds.
    mounts: Arc<Mounts>,
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

    /// Hands the port of `mount` to the worker to adopt, without blocking.
    /// The worker reads what the client sends from `from_port`.
    fn adopt(&self, mount: Arc<Mount>, from_port: mpsc::Receiver<Packet>) -> Result<(), String> {
        let Some(commands) = self.commands.upgrade() else {
            return Err(format!("the link to {} closed", self.url));
        };
        let open = OpenPort {
            mount,
            from_client: from_port,
        };
        let reason = match commands.try_send(open) {
            Ok(()) => return Ok(()),
            // Transient, as the worker still runs.
            Err(mpsc::error::TrySendError::Full(_)) => "the proxy is busy",
            Err(mpsc::error::TrySendError::Closed(_)) => "failed to set up new proxy client",
        };
        Err(format!("could not open a port on {}: {reason}", self.url))
    }

    /// Returns the state of the link's devices.
    pub(super) fn store(&self) -> Arc<Mutex<Store>> {
        Arc::clone(&self.store)
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

    /// Mounts the subtree at `source` of the link for the client of
    /// `outbox`, which sees it at `prefix`. Queues a port for the worker to
    /// adopt, without blocking, that takes what the client sends and queues
    /// the answers to its requests in `outbox`. The link's engine feeds the
    /// client every packet of the subtree until the mount drops.
    ///
    /// Returns an error message if the link is closed, the worker's queue is
    /// full, or the worker is gone.
    pub(super) fn mount(
        self,
        source: DeviceRoute,
        prefix: DeviceRoute,
        outbox: &Arc<Outbox>,
    ) -> Result<Mounted, String> {
        let (port, from_port) = mpsc::channel(worker::client_tx_channel_size());
        // The engine feeds the mount before the worker adopts its port. An
        // engine that ends lets the client go, and a port asked for after
        // that is refused, as the worker ended first.
        let mount = Mount::new(source, prefix, outbox);
        self.link.mounts.add(Arc::clone(&mount));
        // From here the guard removes the mount, if the worker refuses its
        // port too.
        let mounted = Mounted {
            mount,
            port,
            lease: self,
        };
        let link = mounted.lease.link();
        link.adopt(Arc::clone(&mounted.mount), from_port)?;
        Ok(mounted)
    }
}

/// A client's mount of a link, with its lease on the link. Dropping it ends
/// the mount.
pub(super) struct Mounted {
    pub(super) mount: Arc<Mount>,
    /// Takes what the client sends to the device, at the device's routes.
    pub(super) port: mpsc::Sender<Packet>,
    pub(super) lease: Lease,
}

impl Drop for Mounted {
    fn drop(&mut self) {
        self.lease.link.mounts.remove(&self.mount);
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
    /// `None`, log the traffic `dump` asks for, and name routes as the
    /// proxy's `default` layout shows them.
    pub(super) fn new(
        tasks: &Supervisor,
        reconnect_timeout: Option<Duration>,
        dump: DumpFlags,
        default: watch::Receiver<Option<Layout>>,
    ) -> (Links, Owner) {
        let opening = tasks.clone();
        let opens = Box::new(move |url: &str| worker::opener(&opening, url));
        Links::on(tasks, reconnect_timeout, dump, default, opens)
    }

    /// Starts a registry as [`new`](Self::new) does, with each new link's
    /// device opened as `opens` says.
    pub(super) fn on(
        tasks: &Supervisor,
        reconnect_timeout: Option<Duration>,
        dump: DumpFlags,
        default: watch::Receiver<Option<Layout>>,
        opens: Opens,
    ) -> (Links, Owner) {
        let (requests, mailbox) = mpsc::unbounded_channel();
        let links = Links {
            requests: requests.downgrade(),
        };
        let registry = Registry {
            requests: mailbox,
            leases: links.requests.clone(),
            links: BTreeMap::new(),
            waiting: BTreeMap::new(),
            workers: JoinMap::new(),
            next_id: 0,
            tasks: tasks.clone(),
            reconnect_timeout,
            dump,
            default,
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
    commands: Option<mpsc::Sender<OpenPort>>,
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
    /// is [`IDLE`] after its last lease dropped. A link to a device the
    /// proxy's `default` layout mounts stays open while the proxy runs.
    fn due(&self, default: Option<&Layout>) -> Option<Instant> {
        let served = default.is_some_and(|layout| {
            layout
                .mounts
                .iter()
                .any(|mount| *mount.url == *self.link.url)
        });
        let idle = self.commands.is_some() && self.chosen && self.users == 0 && !served;
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
    /// Every link whose task has not ended, in the order they opened. At
    /// most one is filed under each device key.
    links: BTreeMap<LinkId, Entry>,
    /// The acquires of each device key held until its earlier links end.
    waiting: BTreeMap<String, Vec<Waiter>>,
    /// The task of each link.
    workers: JoinMap<LinkId, Option<()>>,
    next_id: u64,
    /// The spawner of the links' tasks.
    tasks: Supervisor,
    reconnect_timeout: Option<Duration>,
    dump: DumpFlags,
    opens: Opens,
    /// The proxy's default layout, which each link names its routes by.
    default: watch::Receiver<Option<Layout>>,
}

impl Registry {
    /// Serves requests until its owner drops, then closes every link.
    async fn run(mut self) {
        loop {
            let due = {
                let default = self.default.borrow();
                let due = self
                    .links
                    .values()
                    .filter_map(|entry| entry.due(default.as_ref()));
                due.min()
            };
            tokio::select! {
                request = self.requests.recv() => match request {
                    Some(request) => self.handle(request),
                    None => break,
                },
                Some((id, _outcome)) = self.workers.join_next() => self.ended(id),
                () = sleep_until_some(due) => self.expire(),
                // A device the new default leaves out may be due already.
                Ok(()) = self.default.changed() => {}
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

    /// Closes each chosen link that has had no users for [`IDLE`], unless the
    /// default layout mounts its device.
    fn expire(&mut self) {
        let now = Instant::now();
        let default = self.default.borrow();
        let due = self
            .links
            .values_mut()
            .filter(|entry| entry.due(default.as_ref()).is_some_and(|due| due <= now));
        for entry in due {
            log::info!("Closing {}, unused for {:?}", entry.link.url, IDLE);
            entry.commands = None;
        }
    }

    /// Opens a new link to `url` under `key`, and spawns its worker and its
    /// engine. The registry holds the worker's task. No one holds the
    /// engine's task. It ends once the worker's task has ended, as the
    /// worker's end closes the port the engine reads, and the engine's
    /// settings tasks end with it.
    fn start(&mut self, url: &str, key: String) -> LinkId {
        log::info!("Opening {url}");
        let id = LinkId(self.next_id);
        self.next_id += 1;
        let open = (self.opens)(url);
        let default = self.default.clone();
        let started = worker::new(
            url,
            self.reconnect_timeout,
            self.dump,
            default,
            open,
            &self.tasks,
        );
        let name = format_args!("proxy link {url}");
        self.tasks
            .spawn_in_map(&mut self.workers, id, name, started.run);
        let link = Link {
            id,
            url: url.into(),
            commands: started.commands.downgrade(),
            readiness: started.readiness,
            store: Arc::new(Mutex::new(Store::new(true))),
            mounts: Mounts::new(url),
        };
        let engine = engine::serve(
            link.store(),
            Arc::clone(&link.mounts),
            started.engine,
            &self.tasks,
        );
        self.tasks.spawn(format_args!("proxy state {url}"), engine);
        let entry = Entry {
            link,
            commands: Some(started.commands),
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

/// The default layout of a proxy that has none, for a test.
#[cfg(test)]
pub(super) fn no_default() -> watch::Receiver<Option<Layout>> {
    watch::channel(None).1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::tests::fixtures::{
        assert_took, fakes, opened, FakeDevice, First, Opened, PAUSED_WAIT, UNREACHABLE, WAIT,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Barrier;
    use std::thread;

    impl Link {
        /// Creates a link to `url` with no worker. Returns it with the
        /// sender that keeps its commands open, and their receiver.
        pub(crate) fn idle(url: &str) -> (Link, mpsc::Sender<OpenPort>, mpsc::Receiver<OpenPort>) {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);
            let (commands, receiver) = mpsc::channel(16);
            let link = Link {
                id: LinkId(NEXT_ID.fetch_add(1, Ordering::Relaxed)),
                url: url.into(),
                commands: commands.downgrade(),
                readiness: watch::channel(Readiness::Pending).1,
                store: Arc::new(Mutex::new(Store::new(true))),
                mounts: Mounts::new(url),
            };
            (link, commands, receiver)
        }

        /// Queues the port of a mount of the whole device for the worker to
        /// adopt, as a client's mount does.
        fn open_port(&self) -> Result<(), String> {
            let mount = Mount::new(DeviceRoute::root(), DeviceRoute::root(), &Arc::default());
            self.adopt(mount, mpsc::channel(1).1)
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

    #[cfg_attr(all(test, turmoil), allow(dead_code))]
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
            self.counting_users().blocking_recv().unwrap_or_default()
        }

        /// Returns the total number of leases on the open links, as
        /// [`users`](Self::users) does, without blocking.
        #[cfg(all(test, turmoil))]
        pub(crate) async fn users_async(&self) -> usize {
            self.counting_users().await.unwrap_or_default()
        }

        /// Has the registry count the leases on the open links. Returns the
        /// receiver of the count.
        fn counting_users(&self) -> oneshot::Receiver<usize> {
            self.inspect(|registry| {
                let open = registry
                    .links
                    .values()
                    .filter(|entry| entry.commands.is_some());
                open.map(|entry| entry.users).sum()
            })
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
    fn idle_link() -> (Link, mpsc::Sender<OpenPort>, mpsc::Receiver<OpenPort>) {
        Link::idle(UNREACHABLE)
    }

    /// A worker stuck acquiring a transport it will never open must not hold
    /// up a client waiting for its port.
    #[test]
    fn opening_a_port_does_not_wait_for_a_worker_that_never_answers() {
        let (link, _open, mut commands) = idle_link();

        link.open_port().expect("the port is queued");

        assert!(commands.try_recv().is_ok());
    }

    /// A congested command lane says nothing about whether the worker lives,
    /// so it must never be reported as the worker being gone.
    #[test]
    fn a_full_command_lane_is_distinct_from_a_stopped_worker() {
        let (link, _open, commands) = idle_link();
        while link.open_port().is_ok() {}

        let busy = link.open_port().err().unwrap();
        assert!(busy.ends_with("the proxy is busy"), "{busy}");
        drop(commands);
        let gone = link.open_port().err().unwrap();
        assert!(
            gone.ends_with("failed to set up new proxy client"),
            "{gone}"
        );
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
        let (links, owner) = Links::on(
            &Supervisor::current(),
            None,
            DumpFlags::default(),
            no_default(),
            opens,
        );
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

    /// Has `device` act as a live device for at most `long`. It beats each
    /// second, from 300 ms on so that no beat meets a whole second of the
    /// test, and leaves what it is sent unanswered. A TCP device that stays
    /// silent is reopened, so a test that keeps one port open needs this.
    /// Returns when the port lets go of the device, if it does.
    async fn live(device: &mut Opened, long: Duration) -> Option<Instant> {
        let start = Instant::now();
        let mut beats =
            tokio::time::interval_at(start + Duration::from_millis(300), Duration::from_secs(1));
        loop {
            tokio::select! {
                biased;
                sent = device.end.recv() => if sent.is_none() {
                    return Some(Instant::now());
                },
                () = crate::chan::sleep_until(start + long) => return None,
                _ = beats.tick() => {
                    device.end.send(&Packet::heartbeat(DeviceRoute::root())).await;
                }
            }
        }
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

        assert_eq!(live(&mut device, IDLE / 2).await, None);
        assert_eq!(urls(&links).await, [DEVICE]);
        let closed = live(&mut device, PAUSED_WAIT).await.expect("it closed");
        assert_took(closed - left, IDLE, "closing");
        assert!(urls(&links).await.is_empty());
    }

    /// A device the proxy's default layout mounts stays open with no user
    /// for as long as the default mounts it, and closes once a new default
    /// leaves it out.
    #[tokio::test(start_paused = true)]
    async fn a_device_the_default_mounts_stays_open_while_it_does() {
        let (opens, mut devices) = fakes(Vec::new());
        let (default, watched) = watch::channel(Some(Layout::single(DEVICE)));
        let (links, _owner) = Links::on(
            &Supervisor::current(),
            None,
            DumpFlags::default(),
            watched,
            opens,
        );
        let open = acquire(&links, DEVICE).await;
        links.choose(vec![open.link().id()]);
        let mut device = opened(&mut devices).await;
        drop(open);

        assert_eq!(live(&mut device, IDLE * 4).await, None);
        assert_eq!(urls(&links).await, [DEVICE]);

        default.send_replace(Some(Layout::single(OTHER)));
        let replaced = Instant::now();
        assert_took(
            released(&mut device).await - replaced,
            Duration::ZERO,
            "closing",
        );
        assert!(urls(&links).await.is_empty());
    }

    /// A new link waits while the task of an earlier link to its device
    /// runs, here one whose first open blocks a thread, however long that
    /// takes. A link to another device opens meanwhile.
    #[tokio::test(start_paused = true)]
    async fn a_new_link_waits_for_an_earlier_worker_of_its_own_device_alone() {
        let gate = Arc::new(Barrier::new(2));
        let (links, _owner, mut devices) = registry(vec![(DEVICE, First::Blocks(gate.clone()))]);
        drop(acquire(&links, DEVICE).await);

        let reopening = crate::chan::spawn({
            let links = links.clone();
            async move { acquire(&links, DEVICE).await }
        });
        let _other = acquire(&links, OTHER).await;
        // Its device stays, as a device that hung up would be reopened.
        let other = opened(&mut devices).await;
        assert_eq!(other.url, OTHER);
        tokio::time::advance(LEAVE_TIMEOUT * 4).await;
        assert!(
            !reopening.is_finished(),
            "the link opened as the worker ran"
        );
        assert_eq!(urls(&links).await, [OTHER]);

        gate.wait();

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
        let root = DeviceRoute::root();
        let mounted = lease.mount(root, root, &Arc::default()).unwrap();
        let queued = 2000;
        for _ in 0..queued {
            mounted.port.try_send(Packet::heartbeat(root)).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(mounted);
        let ending = Instant::now();

        let ended = crate::chan::spawn(owner.close());
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
        let (links, owner) =
            Links::new(&Supervisor::new(), None, DumpFlags::default(), no_default());
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
        let (links, owner) =
            Links::new(&Supervisor::new(), None, DumpFlags::default(), no_default());

        crate::rt::handle().block_on(owner.close());

        let refusal = links.acquire_blocking(UNREACHABLE).err().unwrap();
        assert_eq!(refusal.code, RpcError::State);
        assert!(links.urls().is_empty());
        assert!(links.wait_finished(Duration::ZERO));
    }
}

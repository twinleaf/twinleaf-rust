//! The task that serves each client connection. It answers the client's
//! control requests and routes packets between the client and its mounts.
//! [`Session::run`] runs it.

use super::auto::{self, Choice};
use super::control::encode;
use super::control::{self, Purpose, Refusal, Request, Update, Watch};
use super::link::{self, Lease, Link};
use super::server::{self, Notice, Shared};
use super::worker;
use super::Layout;
use crate::device::store::{lock, Store, CHANGES, METADATA, PRESENCE, REGISTRY, SETTINGS};
use crate::device::stream::Scope;
use crate::device::{Receiver, RecvError, SettingChange};
use crate::proto::rpc::{Method, RpcError};
use crate::proto::{DeviceRoute, PacketType, TCP_DEFAULT_PORT};
use crate::rt::{finish, BoxFuture};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use crate::tio::transport::{self, is_local_tcp, ReceiveResult, TransportEvent, CLOSE_TIMEOUT};
use futures_util::future;
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::oneshot::{self, error::TryRecvError};
use tokio::time::{timeout, timeout_at, Instant};
use tokio_stream::{wrappers::ReceiverStream, StreamMap, StreamNotifyClose};

/// Why a client's connection ended. The task serving the client logs it
/// once.
#[derive(Debug, thiserror::Error)]
pub(super) enum ClientError {
    /// The client closed its connection, or the connection failed.
    #[error("it hung up")]
    HungUp,
    /// The proxy stopped and told the client.
    #[error("the proxy stopped")]
    Stopped,
    /// The client fell behind a proxy that kicks slow clients.
    #[error("it fell behind")]
    TooSlow,
    /// The link to the device at this URL, which the client had opened,
    /// ended without giving up on it.
    #[error("the link to {0} ended unexpectedly")]
    LinkEnded(String),
    /// The proxy gave up reconnecting to the device at this URL, which the
    /// client had opened.
    #[error("the proxy gave up on {0}")]
    GaveUp(String),
    /// A packet it sent could not be queued for its device.
    #[error("a packet it sent could not reach its device")]
    ForwardFailed,
}

/// Returns `true` if `pkt` is an RPC request, reply or error.
fn is_rpc(pkt: &Packet) -> bool {
    matches!(
        pkt.ptype(),
        PacketType::RPC_REQ | PacketType::RPC_REP | PacketType::RPC_ERROR
    )
}

/// A record of whether a client is keeping up. The first dropped packet and
/// the recovery are each logged once.
#[derive(Default)]
struct SlowTracker {
    is_slow: bool,
    dropped: usize,
}

impl SlowTracker {
    /// Counts a packet dropped for the client at `addr`.
    fn packet_dropped(&mut self, addr: &str) {
        if !log::log_enabled!(log::Level::Debug) {
            return;
        }
        if !self.is_slow {
            self.is_slow = true;
            log::debug!("Client {} is not keeping up and is dropping packets", addr);
        }
        self.dropped += 1;
    }

    /// Notes a packet delivered to the client at `addr`.
    fn packet_delivered(&mut self, addr: &str) {
        if self.is_slow {
            log::debug!(
                "Client {} resuming after having dropped {} packets",
                addr,
                self.dropped
            );
            self.is_slow = false;
            self.dropped = 0;
        }
    }
}

/// One mount a client opened. It holds the port on the device and the prefix
/// where the client sees it.
struct Mounted {
    prefix: DeviceRoute,
    /// The subtree of the device the mount shows at its prefix.
    source: DeviceRoute,
    port: link::PortSender,
    /// Whether the last link status the mount heard is `FailedToReconnect`.
    gave_up: bool,
}

/// What a client opened. It holds a lease on each mount's link until dropped.
struct Binding {
    target: String,
    mounts: Vec<Mounted>,
    /// Packets from mounted links, keyed by their stable mount index.
    incoming: StreamMap<usize, StreamNotifyClose<ReceiverStream<Packet>>>,
    /// The lease on the link of each mount, in the order of `mounts`.
    leases: Vec<Lease>,
}

impl Binding {
    /// Returns why the port of mount `index` closed, [`ClientError::GaveUp`]
    /// if the mount last heard `FailedToReconnect`.
    fn closed(&self, index: usize) -> ClientError {
        let url = self.leases[index].link().url().to_string();
        match self.mounts[index].gave_up {
            true => ClientError::GaveUp(url),
            false => ClientError::LinkEnded(url),
        }
    }
}

/// The default a client holds with `proxy.default`. Dropping it gives the
/// default back to the layout it replaced.
struct Held {
    shared: Arc<Shared>,
    replaced: Option<Layout>,
}

impl Drop for Held {
    fn drop(&mut self) {
        *self
            .shared
            .holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        self.shared.default.send_replace(self.replaced.take());
    }
}

/// An open in flight. It resolves to the id of the control request it
/// answers and the outcome of the binding.
type Opening = BoxFuture<'static, (u16, Result<Binding, Refusal>)>;

/// A client's connection and what it opened.
pub(super) struct Session {
    shared: Arc<Shared>,
    id: u64,
    addr: String,
    client: transport::Port,
    /// The packets the client sends.
    incoming: flume::r#async::RecvStream<'static, ReceiveResult>,
    /// Closes once the server drops its lifeline, as the proxy stops.
    alive: oneshot::Receiver<Infallible>,
    /// Packets the client sent that wait their turn while an open waits.
    backlog: VecDeque<Packet>,
    slow: SlowTracker,
    binding: Option<Binding>,
    opening: Option<Opening>,
    held: Option<Held>,
    /// The updates of each device the client watches, by the id of its
    /// `proxy.watch`, each ending with `None`.
    watches: StreamMap<u16, StreamNotifyClose<BoxStream<'static, Update>>>,
}

impl Session {
    /// Creates the session of client `id`, connected from `peer`. The client
    /// is reached through `client`, whose packets arrive on `incoming`. The
    /// session ends once `alive` closes.
    pub(super) fn new(
        shared: Arc<Shared>,
        id: u64,
        peer: SocketAddr,
        client: transport::Port,
        incoming: flume::Receiver<ReceiveResult>,
        alive: oneshot::Receiver<Infallible>,
    ) -> Session {
        Session {
            shared,
            id,
            addr: peer.to_string(),
            client,
            incoming: incoming.into_stream(),
            alive,
            backlog: VecDeque::new(),
            slow: SlowTracker::default(),
            binding: None,
            opening: None,
            held: None,
            watches: StreamMap::new(),
        }
    }

    /// Serves the client until it leaves or the proxy stops. Then closes the
    /// connection, and logs why it ended.
    pub(super) async fn run(mut self) {
        let served = self.serve().await;
        let peer = self.addr.clone();
        self.close().await;
        if let Err(error) = served {
            let level = match error {
                ClientError::HungUp | ClientError::Stopped => log::Level::Debug,
                ClientError::GaveUp(_) => log::Level::Info,
                _ => log::Level::Warn,
            };
            log::log!(level, "Closed client {peer}: {error}");
        }
    }

    /// Closes the connection once what was queued for the client is written,
    /// such as the status that says why it ends. Waits at most
    /// [`CLOSE_TIMEOUT`].
    async fn close(self) {
        let Session {
            client, incoming, ..
        } = self;
        // The transport writes the queue before it ends. It would end at
        // once if what the client sends found no receiver.
        client.close(CLOSE_TIMEOUT).await;
        drop(incoming);
    }

    /// Forwards packets between the client and its mounts until the
    /// connection ends, keeping the client's link alive. Returns why it
    /// ended.
    async fn serve(&mut self) -> Result<(), ClientError> {
        self.client.keep_alive(control::PROXY_KEEPALIVE).await;
        loop {
            if self.stopping() {
                return Err(self.stopped().await);
            }
            // What the client sent while an open waited goes first, in
            // order.
            while self.opening.is_none() {
                let Some(pkt) = self.backlog.pop_front() else {
                    break;
                };
                self.client_packet(pkt).await?;
            }
            let mounted = self
                .binding
                .as_ref()
                .is_some_and(|binding| !binding.incoming.is_empty());
            // State goes out ahead of the devices' packets.
            tokio::select! {
                biased;
                _ = &mut self.alive => return Err(self.stopped().await),
                Some((id, update)) = self.watches.next(), if !self.watches.is_empty() => {
                    let replies = match update {
                        Some(update) => control::parts(id, &update),
                        None => {
                            let ended = "the proxy no longer follows this device";
                            vec![control::answer(id, Err(Refusal::new(RpcError::State, ended)))]
                        }
                    };
                    self.send_state(replies).await?
                }
                received = self.incoming.next() => {
                    let Some(received) = received else { return Err(ClientError::HungUp); };
                    if let Some(pkt) = self.delivered(received)? {
                        self.backlog.push_back(pkt);
                    }
                }
                Some((index, pkt)) = async {
                    self.binding.as_mut().expect("a mount is bound").incoming.next().await
                }, if mounted => {
                    let Some(pkt) = pkt else {
                        let binding = self.binding.as_ref();
                        return Err(binding.expect("a mount is bound").closed(index));
                    };
                    self.mount_packet(pkt, index)?;
                }
                (request, outcome) = finish(&mut self.opening) => {
                    self.opened(request, outcome).await?;
                }
            }
        }
    }

    /// Returns the packet in what the client's transport delivered, or `None`
    /// for one that did not decode.
    ///
    /// Returns [`ClientError::HungUp`] if the connection closed or failed.
    fn delivered(&self, received: ReceiveResult) -> Result<Option<Packet>, ClientError> {
        match received {
            Ok(TransportEvent::Packet(pkt)) => Ok(Some(pkt)),
            Ok(event) => {
                log::debug!("Skipped what client {} sent: {event:?}", self.addr);
                Ok(None)
            }
            Err(_) => Err(ClientError::HungUp),
        }
    }

    /// Returns `true` once the proxy stops.
    fn stopping(&mut self) -> bool {
        matches!(self.alive.try_recv(), Err(TryRecvError::Closed))
    }

    /// Tells the client at the root that the proxy stopped, waiting at most
    /// [`CLOSE_TIMEOUT`] for room in its queue, and returns
    /// [`ClientError::Stopped`].
    fn stopped(&self) -> impl Future<Output = ClientError> {
        let stop = self.client.send_async(Packet::link(LinkStatus::Stopped));
        async move {
            let _ = timeout(CLOSE_TIMEOUT, stop).await;
            ClientError::Stopped
        }
    }

    /// Handles a packet from the client. Answers a control request, and
    /// forwards any other packet to the mount its route falls under. Drops a
    /// packet that falls under no mount.
    ///
    /// Returns [`ClientError::ForwardFailed`] if the mount's port cannot take
    /// the packet.
    async fn client_packet(&mut self, pkt: Packet) -> Result<(), ClientError> {
        if self.shared.options.dump.traffic {
            log::info!("{}->{} -- {:?}", self.addr, pkt.route(), pkt.payload());
        }
        match pkt.ptype() {
            PacketType::PROXY_TX => return self.control(&pkt).await,
            PacketType::HEARTBEAT if pkt.route().is_empty() => return Ok(()),
            PacketType::PROXY_RX => {
                log::debug!("Dropping a control reply from client {}", self.addr);
                return Ok(());
            }
            _ => {}
        }
        let Some(binding) = &self.binding else {
            log::debug!(
                "Dropping a packet from client {}, which opened nothing",
                self.addr
            );
            return Ok(());
        };
        let mut dest = None;
        for Mounted { prefix, port, .. } in &binding.mounts {
            if let Ok(relative) = prefix.relative_route(&pkt.route()) {
                dest = Some((relative, port));
                break;
            }
        }
        let Some((relative, port)) = dest else {
            log::debug!(
                "Client {} addressed unmounted route {}",
                self.addr,
                pkt.route()
            );
            return Ok(());
        };
        let sent = port.try_send(pkt.with_route(relative));
        sent.map_err(|_| ClientError::ForwardFailed)
    }

    /// Forwards a packet from mount `index` to the client.
    ///
    /// Returns an error if the client is gone, or if it falls behind a proxy
    /// that kicks slow clients.
    fn mount_packet(&mut self, pkt: Packet, index: usize) -> Result<(), ClientError> {
        let binding = self.binding.as_mut().expect("a mount is bound");
        let mount = &mut binding.mounts[index];
        let Ok(routing) = mount.prefix.absolute_route(&pkt.route()) else {
            log::warn!(
                "Dropping packet for client {}: route {} exceeds max depth",
                self.addr,
                pkt.route()
            );
            return Ok(());
        };
        let status = match pkt.payload() {
            Payload::ProxyRx(ProxyRx::Link(status)) => Some(status),
            _ => None,
        };
        if let Some(status) = status {
            mount.gave_up = status == LinkStatus::FailedToReconnect;
        }
        let pkt = pkt.with_route(routing);
        if self.shared.options.dump.traffic && is_rpc(&pkt) {
            log::info!("{}->{} -- {:?}", pkt.route(), self.addr, pkt.payload());
        }
        match self.client.try_send(pkt) {
            Ok(()) => self.slow.packet_delivered(&self.addr),
            Err(transport::SendError::Full) if !self.shared.options.kick_slow => {
                self.slow.packet_dropped(&self.addr)
            }
            Err(transport::SendError::Full) => return Err(ClientError::TooSlow),
            Err(_) => return Err(ClientError::HungUp),
        }
        Ok(())
    }

    /// Queues the answer to control request `id` for the client.
    ///
    /// Returns [`ClientError::HungUp`] if the client is gone.
    fn answer(&self, id: u16, outcome: Result<Vec<u8>, Refusal>) -> Result<(), ClientError> {
        match self.client.try_send(control::answer(id, outcome)) {
            Ok(()) => Ok(()),
            Err(transport::SendError::Full) => {
                log::debug!("Dropping a control reply to client {}", self.addr);
                Ok(())
            }
            Err(_) => Err(ClientError::HungUp),
        }
    }

    /// Changes the client's entry in the table, and waits until the table
    /// shows it or the server has ended.
    async fn update(&mut self, update: server::Update) {
        let (done, shown) = oneshot::channel();
        let notice = Notice::Update {
            id: self.id,
            update,
            done,
        };
        let server = self.shared.server.upgrade();
        if server.is_some_and(|server| server.send(notice).is_ok()) {
            let _ = shown.await;
        }
    }

    /// Answers the control request in `pkt`. Begins the proxy's stop after
    /// queuing the reply to `proxy.stop`. An open or a probe is answered once
    /// it is done.
    async fn control(&mut self, pkt: &Packet) -> Result<(), ClientError> {
        let Payload::ProxyTx(request) = pkt.payload() else {
            return Ok(());
        };
        let id = request.id.value();
        let decoded = match request.method {
            Method::ByName(name) => Request::decode(name, request.args),
            Method::ById(_) => Err(Refusal::new(
                RpcError::NotFound,
                "the proxy's methods are called by name",
            )),
        };
        let request = match decoded {
            Ok(request) => request,
            Err(refusal) => return self.answer(id, Err(refusal)),
        };
        match request {
            Request::Hello(client) => {
                log::debug!("Client {} is {client}", self.addr);
                self.update(server::Update::Hello(client)).await;
                let host = control::Introduction::of_this_process(&self.shared.options.name);
                self.answer(id, Ok(encode(&host)))
            }
            Request::Open { target } => self.request_open(id, target, Purpose::Use),
            Request::Probe { url } => self.request_open(id, url, Purpose::Probe),
            Request::Mount(layout) => {
                let (shared, addr) = (self.shared.clone(), self.addr.clone());
                let binding = attach(shared, addr, layout.to_string(), layout, None, Purpose::Use);
                self.request(id, binding)
            }
            Request::Default(layout) => self.hold_default(id, layout),
            Request::Watch(watch) => self.watch(id, watch).await,
            Request::Unwatch(watched) => {
                self.watches.remove(&watched);
                Ok(())
            }
            Request::Stop if self.shared.options.stoppable => {
                self.answer(id, Ok(Vec::new()))?;
                log::info!("Stopped by proxy.stop from client {}", self.addr);
                if let Some(server) = self.shared.server.upgrade() {
                    let _ = server.send(Notice::Stop);
                }
                Ok(())
            }
            Request::Stop => {
                let message = format!(
                    "the proxy at {} is hosted by {} and stops only with it",
                    self.shared.endpoint, self.shared.options.name
                );
                self.answer(id, Err(Refusal::new(RpcError::State, message)))
            }
            #[cfg(test)]
            Request::Panic => panic!("client {} asked its task to panic", self.addr),
        }
    }

    /// Follows the device that control request `id` names, answering with
    /// its state now and then each change. A watch under an id the client
    /// already watches with replaces it.
    async fn watch(&mut self, id: u16, watch: Watch) -> Result<(), ClientError> {
        let opened = self.binding.as_ref();
        let opened =
            opened.ok_or_else(|| Refusal::new(RpcError::State, "this connection opened nothing"));
        match opened.and_then(|binding| updates(binding, watch)) {
            Ok(updates) => {
                self.watches.insert(id, StreamNotifyClose::new(updates));
                Ok(())
            }
            Err(refusal) => {
                self.send_state(vec![control::answer(id, Err(refusal))])
                    .await
            }
        }
    }

    /// Sends `packets`, the replies to a watch that carry state or its end.
    /// State is never dropped, so it waits for room in the client's queue,
    /// unless the proxy stops meanwhile.
    ///
    /// Returns [`ClientError::Stopped`] once the proxy stops,
    /// [`ClientError::TooSlow`] if the client makes no room within
    /// the time a quiet client is given, and [`ClientError::HungUp`] if it
    /// is gone.
    async fn send_state(&mut self, packets: Vec<Packet>) -> Result<(), ClientError> {
        for part in packets {
            let quiet = control::PROXY_KEEPALIVE.quiet;
            let sent = tokio::select! {
                biased;
                _ = &mut self.alive => return Err(self.stopped().await),
                sent = timeout(quiet, self.client.send_async(part)) => sent,
            };
            match sent {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(ClientError::HungUp),
                Err(_) => return Err(ClientError::TooSlow),
            }
        }
        Ok(())
    }

    /// Makes `layout` the default until this client leaves, answering control
    /// request `id`. Only a background proxy lets a client hold its default,
    /// and one client at a time.
    fn hold_default(&mut self, id: u16, layout: Layout) -> Result<(), ClientError> {
        if !self.shared.options.stoppable {
            let message = format!(
                "the proxy at {} is hosted by {} and keeps its own default",
                self.shared.endpoint, self.shared.options.name
            );
            return self.answer(id, Err(Refusal::new(RpcError::State, message)));
        }
        let mut holder = self
            .shared
            .holder
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match holder.as_ref() {
            None => {
                *holder = Some((self.id, self.addr.clone()));
                self.held = Some(Held {
                    shared: self.shared.clone(),
                    replaced: self.shared.default(),
                });
            }
            Some((holding, _)) if *holding == self.id => {}
            Some((_, addr)) => {
                let message = format!(
                    "client {addr} holds the default of the proxy at {}",
                    self.shared.endpoint
                );
                drop(holder);
                return self.answer(id, Err(Refusal::new(RpcError::State, message)));
            }
        }
        drop(holder);
        log::info!("Client {} holds {layout} as the default", self.addr);
        self.shared.default.send_replace(Some(layout));
        self.answer(id, Ok(Vec::new()))
    }

    /// Begins the open of `target` for `purpose` that control request `id`
    /// asks for.
    fn request_open(
        &mut self,
        id: u16,
        target: String,
        purpose: Purpose,
    ) -> Result<(), ClientError> {
        let binding = bind(self.shared.clone(), self.addr.clone(), target, purpose);
        self.request(id, binding)
    }

    /// Begins the `binding` that control request `id` asks for. A connection
    /// binds once, so a second request is refused at once.
    fn request(
        &mut self,
        id: u16,
        binding: impl Future<Output = Result<Binding, Refusal>> + Send + 'static,
    ) -> Result<(), ClientError> {
        if let Some(binding) = &self.binding {
            let message = format!("this connection already opened {}", binding.target);
            return self.answer(id, Err(Refusal::new(RpcError::State, message)));
        }
        self.opening = Some(Box::pin(async move { (id, binding.await) }));
        Ok(())
    }

    /// Answers the open that ended with `outcome`. Once the table shows what
    /// the client opened, answers control request `request`.
    ///
    /// Returns [`ClientError::Stopped`] for an open refused while the proxy
    /// stops, which the client hears as the stop.
    async fn opened(
        &mut self,
        request: u16,
        outcome: Result<Binding, Refusal>,
    ) -> Result<(), ClientError> {
        let outcome = match outcome {
            Ok(binding) => {
                let target = binding.target.clone();
                self.update(server::Update::Target(target.clone())).await;
                self.binding = Some(binding);
                Ok(encode(&target))
            }
            Err(_) if self.stopping() => return Err(self.stopped().await),
            Err(refusal) => Err(refusal),
        };
        self.answer(request, outcome)
    }
}

/// Binds a connection of the client at `addr` to `target` for `purpose`.
/// Resolves the target, takes a lease on each mount's link, waits for each
/// link's device to answer, and opens a port on each. A binding for
/// [`Purpose::Use`] marks each link as chosen. A probe's target is a device
/// URL.
///
/// Returns a [`Refusal`] if the target does not resolve, a link is refused,
/// or a link is not ready within [`link::READY_TIMEOUT`].
async fn bind(
    shared: Arc<Shared>,
    addr: String,
    target: String,
    purpose: Purpose,
) -> Result<Binding, Refusal> {
    let (target, layout, chosen) = match purpose {
        Purpose::Use => resolve(&shared, target).await?,
        Purpose::Probe => (target.clone(), Layout::single(&target), None),
    };
    attach(shared, addr, target, layout, chosen, purpose).await
}

/// Binds the connection to `layout`, named `target`, as [`bind`] does once
/// it has resolved its target. A link `auto` chose is shared, and every
/// other mount takes a lease of its own. A mount whose URL is a TCP URL at a
/// loopback address or `localhost`, on the port this proxy listens on, is
/// refused, as its link would be a client of this proxy. Another name of
/// this machine is not recognised.
async fn attach(
    shared: Arc<Shared>,
    addr: String,
    target: String,
    layout: Layout,
    chosen: Option<Link>,
    purpose: Purpose,
) -> Result<Binding, Refusal> {
    let deadline = Instant::now() + link::READY_TIMEOUT;
    let mut leases = Vec::with_capacity(layout.mounts.len());
    for mount in &layout.mounts {
        if names_this_proxy(&mount.url, &shared.endpoint) {
            let message = format!("{} is this proxy, which cannot mount itself", mount.url);
            return Err(Refusal::new(RpcError::Invalid, message));
        }
        let lease = match &chosen {
            Some(link) => shared.links.share(link).await?,
            None => {
                let url = mount.url.clone();
                let key = shared
                    .tasks
                    .spawn_blocking(move || worker::device_key(&url));
                let key = key.await.map_err(|error| failed(&error))?;
                shared.links.acquire(&mount.url, key).await?
            }
        };
        leases.push(lease);
    }
    for lease in &leases {
        let link = lease.link();
        let reason = match timeout_at(deadline, link.ready()).await {
            Ok(Ok(())) => continue,
            Ok(Err(reason)) => reason,
            Err(_) => format!("it sent nothing within {:?}", link::READY_TIMEOUT),
        };
        log::warn!("Could not open {} for client {addr}: {reason}", link.url());
        let message = format!("could not open {}: {reason}", link.url());
        return Err(Refusal::new(RpcError::Load, message));
    }
    let mut mounts = Vec::with_capacity(layout.mounts.len());
    let mut incoming = StreamMap::with_capacity(layout.mounts.len());
    for (mount, lease) in layout.mounts.iter().zip(&leases) {
        let port = lease
            .link()
            .open_port(mount.source)
            .map_err(|reason| Refusal::new(RpcError::Load, reason))?;
        let (port, received) = port.into_split();
        incoming.insert(
            mounts.len(),
            StreamNotifyClose::new(ReceiverStream::new(received)),
        );
        mounts.push(Mounted {
            prefix: mount.prefix,
            source: mount.source,
            port,
            gave_up: false,
        });
    }
    match purpose {
        Purpose::Use => {
            shared
                .links
                .choose(leases.iter().map(|lease| lease.link().id()).collect());
            log::info!("Client {addr} opened {target}");
        }
        Purpose::Probe => log::debug!("Client {addr} probed {target}"),
    }
    Ok(Binding {
        target,
        mounts,
        incoming,
        leases,
    })
}

/// Resolves `target`. Returns the target it resolved to, the layout it
/// opens, and the open link `auto` chose, if any.
///
/// Returns a [`Refusal`] if `auto` finds no single choice, or if the target
/// is `default` and no default is set.
async fn resolve(
    shared: &Shared,
    target: String,
) -> Result<(String, Layout, Option<Link>), Refusal> {
    let default = shared.default();
    match target.as_str() {
        auto::AUTO => {
            let open = shared.links.answered().await;
            // Listing the attached devices enumerates serial ports.
            let resolved = shared.tasks.spawn_blocking(move || {
                match auto::resolve(default.as_ref(), &open, auto::attached) {
                    Ok(Choice::Default(layout)) => {
                        Ok((auto::DEFAULT.to_string(), layout.clone(), None))
                    }
                    Ok(Choice::Open(link)) => {
                        let url = link.url().to_string();
                        Ok((url.clone(), Layout::single(&url), Some(link)))
                    }
                    Ok(Choice::Url(url)) => Ok((url.clone(), Layout::single(&url), None)),
                    Err(message) => Err(Refusal::new(RpcError::Invalid, message)),
                }
            });
            resolved.await.map_err(|error| failed(&error))?
        }
        auto::DEFAULT => match default {
            Some(layout) => Ok((target, layout, None)),
            None => Err(Refusal::new(
                RpcError::Invalid,
                format!("{} set no default", shared.options.name),
            )),
        },
        url => Ok((url.to_string(), Layout::single(url), None)),
    }
}

/// Returns the updates `watch` sends through the mounts of `binding`.
///
/// With `table`, they are the RPC table and settings of the device the
/// watch names, and a table goes out ahead of the first settings read with
/// it. Without, they are the routes heard at or below it, from every mount
/// there, and the presence, metadata and announced settings of the device
/// it names, if a mount shows it. The updates end when the link's engine
/// does.
///
/// Returns a [`Refusal`] if no mount shows what the watch follows.
fn updates(binding: &Binding, watch: Watch) -> Result<BoxStream<'static, Update>, Refusal> {
    let refused = || Refusal::new(RpcError::Invalid, format!("no mount shows {}", watch.route));
    let device = binding.device(watch.route);
    if watch.table {
        let (store, route) = device.ok_or_else(refused)?;
        return Ok(table_updates(&store, route));
    }
    let routes = binding.routes(watch.route).ok_or_else(refused)?;
    Ok(match device {
        Some((store, route)) => stream::select(routes, device_updates(&store, route)).boxed(),
        None => routes,
    })
}

impl Binding {
    /// Returns the store of the link whose mount shows `route`, and the
    /// route there.
    fn device(&self, route: DeviceRoute) -> Option<(Arc<Mutex<Store>>, DeviceRoute)> {
        let mounted = self.mounts.iter().zip(&self.leases);
        mounted.into_iter().find_map(|(mount, lease)| {
            let below = mount.prefix.relative_route(&route).ok()?;
            Some((
                lease.link().store(),
                mount.source.absolute_route(&below).ok()?,
            ))
        })
    }

    /// Returns the updates of the routes heard at or below `route`, as the
    /// client sees them, from every mount there. Each change of any mount's
    /// list sends the whole list, read from every mount's store then.
    /// Returns `None` if no mount is there.
    fn routes(&self, route: DeviceRoute) -> Option<BoxStream<'static, Update>> {
        let mounted = self.mounts.iter().zip(&self.leases);
        let shown: Vec<_> = mounted
            .filter_map(|(mount, lease)| {
                let below = match mount.prefix.relative_route(&route) {
                    Ok(below) => mount.source.absolute_route(&below).ok()?,
                    Err(_) if mount.prefix.starts_with(&route) => mount.source,
                    Err(_) => return None,
                };
                Some((
                    lease.link().store(),
                    Scope::subtree(below),
                    (mount.prefix, mount.source),
                ))
            })
            .collect();
        if shown.is_empty() {
            return None;
        }
        let changes = shown
            .iter()
            .map(|(store, scope, _)| values(lock(store).follow_routes(*scope)));
        let changes = stream::select_all(changes.collect::<Vec<_>>());
        Some(
            changes
                .map(move |_| {
                    let all = shown.iter().flat_map(|(store, scope, (prefix, source))| {
                        let heard = lock(store).routes().clone().into_iter();
                        let heard = heard.filter(|heard| scope.covers(*heard));
                        let below = heard.filter_map(|heard| source.relative_route(&heard).ok());
                        below.filter_map(|below| prefix.absolute_route(&below).ok())
                    });
                    Update::Routes(all.map(|route| route.as_slice().to_vec().into()).collect())
                })
                .boxed(),
        )
    }
}

/// Returns the updates of the presence, metadata and announced settings of
/// the device at `route` that `store` holds.
fn device_updates(store: &Mutex<Store>, route: DeviceRoute) -> BoxStream<'static, Update> {
    let mut store = lock(store);
    let presence = values(store.follow(route, PRESENCE)).map(Update::presence);
    let metadata = values(store.follow(route, METADATA));
    let metadata = metadata.filter_map(|state| future::ready(Update::metadata(&state)));
    let changes = announced(store.follow(route, CHANGES));
    stream::select_all([presence.boxed(), metadata.boxed(), changes]).boxed()
}

/// Returns the updates of the RPC table and settings of the device at
/// `route` that `store` holds. A table goes out ahead of the first settings
/// read with it, and settings read with any other table than the one sent
/// last never go out.
fn table_updates(store: &Mutex<Store>, route: DeviceRoute) -> BoxStream<'static, Update> {
    let mut store = lock(store);
    let followed = (
        store.follow(route, REGISTRY),
        store.follow(route, SETTINGS),
        None,
    );
    let updates = stream::unfold(followed, |(registry, settings, mut table)| async move {
        let update = tokio::select! {
            biased;
            loaded = registry.recv_async() => {
                let loaded = loaded.ok()?;
                table = loaded.as_ref().ok().cloned();
                Some(Update::table(&loaded))
            }
            read = settings.recv_async() => {
                let read = read.ok()?;
                let current = table.as_ref().is_some_and(|sent| Arc::ptr_eq(sent, read.rpc_registry()));
                current.then(|| Update::settings(&read))
            }
        };
        Some((update, (registry, settings, table)))
    });
    updates.filter_map(future::ready).boxed()
}

/// Returns what the latest-value `receiver` yields until it ends.
fn values<T: Send + 'static>(receiver: Receiver<T>) -> BoxStream<'static, T> {
    stream::unfold(receiver, |receiver| async move {
        let value = receiver.recv_async().await.ok()?;
        Some((value, receiver))
    })
    .boxed()
}

/// Returns the updates of each setting `changes` yields, and of each count
/// of those it missed, until it ends.
fn announced(changes: Receiver<SettingChange>) -> BoxStream<'static, Update> {
    stream::unfold(changes, |changes| async move {
        let update = match changes.recv_async().await {
            Ok(change) => Update::setting(change),
            Err(RecvError::Lagged(missed)) => Update::Lagged(missed),
            Err(_) => return None,
        };
        Some((update, changes))
    })
    .boxed()
}

/// Returns `true` if `url` is a TCP URL on this machine whose port is that
/// of `endpoint`, the address this proxy answers at on this machine.
fn names_this_proxy(url: &str, endpoint: &str) -> bool {
    let port = |url: &str| {
        let port = url.rsplit_once(':').and_then(|(_, port)| port.parse().ok());
        port.unwrap_or(TCP_DEFAULT_PORT)
    };
    is_local_tcp(url) && port(url) == port(endpoint)
}

/// Returns the refusal of an open whose blocking step failed with `error`.
fn failed(error: &tokio::task::JoinError) -> Refusal {
    Refusal::new(RpcError::Internal, format!("the open failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::fixtures::{assert_took, fakes, opened, options, Opened, PAUSED_WAIT};
    use crate::proxy::link::{Links, Owner};
    use crate::proxy::DumpFlags;
    use crate::rt::Supervisor;
    use crate::tio::transport::{FakeEnd, TransportKind, DEFAULT_RX_CHANNEL_SIZE};
    use futures_util::FutureExt;
    use std::time::Duration;
    use tokio::sync::{mpsc, watch};

    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    /// The device a test opens.
    const DEVICE: &str = "tcp://device";

    /// A client's session on the test's runtime, over a fake connection, of
    /// a proxy whose devices are fake.
    struct Client {
        /// The client's end of its connection.
        end: FakeEnd,
        /// Each device port the proxy opens.
        devices: mpsc::UnboundedReceiver<Opened>,
        links: Links,
        /// Resolves to why the session ended.
        served: JoinHandle<Option<Result<(), ClientError>>>,
        _registry: Owner,
        _lifeline: oneshot::Sender<Infallible>,
    }

    /// Connects a client to a proxy with no table, whose default is `default`
    /// and whose links give up a lost device after `reconnect_timeout`.
    fn connect(default: Option<Layout>, reconnect_timeout: Option<Duration>) -> Client {
        let tasks = Supervisor::current();
        let (opens, devices) = fakes(Vec::new());
        let (links, registry) = Links::on(
            &tasks,
            reconnect_timeout,
            DumpFlags::default(),
            super::link::no_default(),
            opens,
        );
        let shared = Arc::new(Shared {
            options: options(),
            endpoint: "tcp://127.0.0.1:7855".to_string(),
            links: links.clone(),
            tasks: tasks.clone(),
            server: mpsc::unbounded_channel().0.downgrade(),
            default: watch::channel(default).0,
            holder: std::sync::Mutex::new(None),
        });
        let (deliver, incoming) = flume::bounded(DEFAULT_RX_CHANNEL_SIZE);
        let (port, end) = transport::Port::fake(TransportKind::Tcp, None, deliver);
        let peer = "127.0.0.1:50000".parse().unwrap();
        let (lifeline, alive) = oneshot::channel();
        let mut session = Session::new(shared, 1, peer, port, incoming, alive);
        let served = tasks.spawn("a test client", async move {
            let served = session.serve().await;
            session.close().await;
            served
        });
        Client {
            end,
            devices,
            links,
            served,
            _registry: registry,
            _lifeline: lifeline,
        }
    }

    /// Opens [`DEVICE`] for `client`, and has the device answer. Returns the
    /// device's port.
    async fn opened_and_answered(client: &mut Client) -> Opened {
        client.end.send(&open(DEVICE, 2)).await;
        let mut device = opened(&mut client.devices).await;
        device
            .end
            .send(&Packet::heartbeat(DeviceRoute::root()))
            .await;
        assert_eq!(
            answer(&next(&mut client.end).await),
            (2, Ok(encode(&DEVICE)))
        );
        device
    }

    /// Waits for `client`'s session to end, and returns why.
    async fn ended(client: Client) -> ClientError {
        let served = timeout(PAUSED_WAIT, client.served).await;
        let served = served.expect("the session never ended").unwrap();
        served.expect("the session panicked").unwrap_err()
    }

    /// A session whose link gives up its device ends saying so, once the
    /// client has heard `FailedToReconnect`.
    #[tokio::test(start_paused = true)]
    async fn a_session_whose_link_gives_up_ends_saying_so() {
        let mut client = connect(None, Some(Duration::from_millis(200)));
        let device = opened_and_answered(&mut client).await;

        drop(device);

        let error = ended(client).await;
        assert!(matches!(error, ClientError::GaveUp(_)), "{error}");
        assert_eq!(error.to_string(), format!("the proxy gave up on {DEVICE}"));
    }

    /// A session whose link ends without giving up its device, as one the
    /// registry closes does, is not told the proxy gave up on it.
    #[tokio::test(start_paused = true)]
    async fn a_session_whose_link_ends_without_giving_up_says_so() {
        let mut client = connect(None, Some(Duration::from_millis(200)));
        let _device = opened_and_answered(&mut client).await;

        assert!(client.links.close(DEVICE).await.unwrap());

        let error = ended(client).await;
        assert!(matches!(error, ClientError::LinkEnded(_)), "{error}");
        assert_eq!(
            error.to_string(),
            format!("the link to {DEVICE} ended unexpectedly")
        );
    }

    /// A stopping session whose client reads nothing lets go of it once the
    /// close runs out, though more was queued than its connection holds.
    #[tokio::test(start_paused = true)]
    async fn a_stopping_session_lets_go_of_a_client_that_reads_nothing() {
        let mut client = connect(None, None);
        let mut device = opened_and_answered(&mut client).await;
        for _ in 0..4096 {
            device
                .end
                .send(&Packet::heartbeat(DeviceRoute::root()))
                .await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;

        let stopping = Instant::now();
        drop(client._lifeline);

        let served = timeout(PAUSED_WAIT, client.served).await.unwrap();
        assert!(matches!(served.unwrap(), Some(Err(ClientError::Stopped))));
        assert_took(stopping.elapsed(), CLOSE_TIMEOUT, "the close");
    }

    /// Returns the request to open `target`, with `id`.
    fn open(target: &str, id: u16) -> Packet {
        Packet::proxy_request("proxy.open", &encode(&target), id).unwrap()
    }

    /// Waits for the next packet the client is sent, skipping the proxy's
    /// signs of life and keeping the link alive as a client does meanwhile.
    async fn next(end: &mut FakeEnd) -> Packet {
        let deadline = Instant::now() + PAUSED_WAIT;
        loop {
            assert!(Instant::now() < deadline, "nothing arrived");
            match timeout(Duration::from_secs(1), end.recv()).await {
                Ok(Some(pkt)) if matches!(pkt.payload(), Payload::ProxyRx(ProxyRx::Alive)) => {}
                Ok(Some(pkt)) => return pkt,
                Ok(None) => panic!("the connection closed"),
                Err(_quiet) => end.send(&Packet::heartbeat(DeviceRoute::root())).await,
            }
        }
    }

    /// Returns the id of the control request `pkt` answers, with the answer.
    fn answer(pkt: &Packet) -> (u16, Result<Vec<u8>, (RpcError, String)>) {
        match pkt.payload() {
            Payload::ProxyRx(ProxyRx::Reply(reply)) => {
                (reply.req_id.value(), Ok(reply.value.to_vec()))
            }
            Payload::ProxyRx(ProxyRx::Error(error)) => (
                error.req_id.value(),
                Err((
                    RpcError::from(error.code),
                    String::from_utf8_lossy(error.message).into_owned(),
                )),
            ),
            other => panic!("expected an answer, got {other:?}"),
        }
    }

    /// A client that sends nothing opens nothing, though the proxy has a
    /// default, and is given up once it has been quiet for longer than the
    /// keepalive allows.
    #[tokio::test(start_paused = true)]
    async fn a_client_that_sends_nothing_opens_nothing_and_is_given_up() {
        let mut client = connect(Some(Layout::single(DEVICE)), None);
        let connected = Instant::now();

        while let Some(pkt) = timeout(PAUSED_WAIT, client.end.recv()).await.unwrap() {
            assert!(matches!(pkt.payload(), Payload::ProxyRx(ProxyRx::Alive)));
        }

        assert_eq!(connected.elapsed(), control::PROXY_KEEPALIVE.quiet);
        assert!(client.devices.try_recv().is_err(), "a device was opened");
    }

    /// A control client that sends nothing for longer than the keepalive
    /// allows is given up, though it heard the proxy's signs of life.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_control_client_is_given_up() {
        let mut client = connect(None, None);
        client.end.send(&open(DEVICE, 2)).await;
        let quiet = Instant::now();

        let mut alive = 0;
        while let Some(pkt) = timeout(PAUSED_WAIT, client.end.recv()).await.unwrap() {
            assert!(matches!(pkt.payload(), Payload::ProxyRx(ProxyRx::Alive)));
            alive += 1;
        }

        assert_eq!(quiet.elapsed(), control::PROXY_KEEPALIVE.quiet);
        assert_eq!(alive, 4);
    }

    /// An open of a device that sends nothing is refused once
    /// [`link::READY_TIMEOUT`] has passed, and the refusal says so.
    #[tokio::test(start_paused = true)]
    async fn an_open_of_a_silent_device_is_refused_at_the_ready_timeout() {
        let mut client = connect(None, None);
        client.end.send(&open(DEVICE, 2)).await;
        let asked = Instant::now();

        let refused = answer(&next(&mut client.end).await);

        assert_took(asked.elapsed(), link::READY_TIMEOUT, "the refusal");
        let message = format!("could not open {DEVICE}: it sent nothing within 10s");
        assert_eq!(refused, (2, Err((RpcError::Load, message))));
    }

    /// What a client sends while its open waits is handled once the open is
    /// answered, in the order it was sent. A second open is refused then.
    #[tokio::test(start_paused = true)]
    async fn what_a_client_sends_while_its_open_waits_follows_the_answer_in_order() {
        let mut client = connect(None, None);
        client.end.send(&open(DEVICE, 2)).await;
        let mut device = opened(&mut client.devices).await;
        let beat = |route: &str| Packet::heartbeat(route.parse().unwrap());
        for pkt in [beat("/1"), open(DEVICE, 3), beat("/2")] {
            client.end.send(&pkt).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        device.end.send(&beat("/")).await;

        let opened = answer(&next(&mut client.end).await);
        assert_eq!(opened, (2, Ok(encode(&DEVICE))));
        let message = format!("this connection already opened {DEVICE}");
        let refused = answer(&next(&mut client.end).await);
        assert_eq!(refused, (3, Err((RpcError::State, message))));
        for route in ["/1", "/2"] {
            let forwarded = timeout(PAUSED_WAIT, device.end.recv()).await.unwrap();
            assert_eq!(
                forwarded.map(|pkt| pkt.route()),
                Some(route.parse().unwrap())
            );
        }
    }

    /// Takes what `updates` yields without waiting.
    fn ready(updates: &mut BoxStream<'static, Update>) -> Vec<Update> {
        std::iter::from_fn(|| updates.next().now_or_never().flatten()).collect()
    }

    /// A stop reaches a client whose queue is too full for its state at
    /// once, rather than once the wait for room runs out, and its status
    /// arrives however full the queue is.
    #[tokio::test(start_paused = true)]
    async fn a_stop_reaches_a_client_waiting_to_send_state() {
        let Client {
            mut end,
            mut devices,
            served,
            _lifeline,
            links: _links,
            _registry,
        } = connect(None, None);
        end.send(&open(DEVICE, 2)).await;
        let mut device = opened(&mut devices).await;
        let root = DeviceRoute::root();
        device.end.send(&Packet::heartbeat(root)).await;
        assert_eq!(answer(&next(&mut end).await).0, 2);
        for _ in 0..transport::DEFAULT_TX_CHANNEL_SIZE * 2 {
            device.end.send(&Packet::heartbeat(root)).await;
        }
        let watch = encode(&Watch {
            route: root,
            table: false,
        });
        end.send(&Packet::proxy_request(control::WATCH, &watch, 3).unwrap())
            .await;
        for _ in 0..1000 {
            tokio::task::yield_now().await;
        }

        let stopping = Instant::now();
        drop(_lifeline);
        let heard = async {
            let mut stopped = false;
            while let Some(pkt) = end.recv().await {
                stopped |= matches!(
                    pkt.payload(),
                    Payload::ProxyRx(ProxyRx::Link(LinkStatus::Stopped))
                );
            }
            stopped
        };
        let (served, stopped) = tokio::join!(timeout(PAUSED_WAIT, served), heard);
        let served = served.unwrap().unwrap();
        assert!(
            matches!(served, Some(Err(ClientError::Stopped))),
            "{served:?}"
        );
        assert!(stopping.elapsed() < control::PROXY_KEEPALIVE.quiet);
        assert!(stopped, "the stop never arrived");
    }

    /// A table watch that falls behind a new table sends the new table, and
    /// never the settings read with the table before, which the store's
    /// lanes may still hold.
    #[test]
    fn a_table_watch_never_sends_settings_of_an_older_table() {
        let store = Mutex::new(Store::new(true));
        let route = DeviceRoute::root();
        let table = |hash| {
            Arc::new(crate::device::rpc::RpcRegistry::from_entries(
                Vec::new(),
                Some(hash),
                None,
            ))
        };
        let (old, new) = (table(1), table(2));
        let settings = crate::device::Settings::new(Arc::clone(&old), Some(1), Default::default());
        lock(&store).set_registry(route, Ok(Arc::clone(&old)));
        lock(&store).set_settings(route, Arc::new(settings));
        let mut updates = table_updates(&store, route);

        lock(&store).set_registry(route, Ok(Arc::clone(&new)));

        assert_eq!(ready(&mut updates), [Update::table(&Ok(new))]);
    }

    /// A watch whose client reads nothing while a device's presence changes
    /// sends only the newest presence once it is read again, as the store's
    /// latest values coalesce what it missed.
    #[test]
    fn a_slow_watch_converges_to_the_newest_presence() {
        let store = Mutex::new(Store::new(true));
        let route = DeviceRoute::root();
        let mut updates = device_updates(&store, route);
        let first = ready(&mut updates);
        assert_eq!(
            first,
            [
                Update::Presence(Err(None)),
                Update::Metadata(Some(Vec::new()))
            ]
        );

        for session in 0..1000 {
            let alive = crate::device::Presence::Alive {
                session: Some(crate::proto::SessionId::new(session)),
            };
            lock(&store).set(route, PRESENCE, alive);
        }

        assert_eq!(ready(&mut updates), [Update::Presence(Ok(Some(999)))]);
    }

    /// A watch that falls behind the device's announcements says how many
    /// it missed, and goes on with the next one.
    #[test]
    fn a_watch_that_misses_announcements_says_how_many() {
        let store = Mutex::new(Store::new(true));
        let route = DeviceRoute::root();
        let mut updates = device_updates(&store, route);
        ready(&mut updates);
        let change = |seq: usize| SettingChange {
            name: "test.amplitude".to_string(),
            seq: u32::try_from(seq).ok(),
            reply: vec![1],
        };
        let missed = 10;
        let sent = crate::device::store::CHANGE_QUEUE_LEN + missed;
        for seq in 0..sent {
            lock(&store).set(route, CHANGES, change(seq));
        }
        assert_eq!(
            ready(&mut updates).len(),
            crate::device::store::CHANGE_QUEUE_LEN
        );

        lock(&store).set(route, CHANGES, change(sent));
        assert_eq!(
            ready(&mut updates),
            [Update::Lagged(missed as u64), Update::setting(change(sent))]
        );
    }
}

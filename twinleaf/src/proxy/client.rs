//! The task that serves each client connection. It answers the client's
//! control requests, forwards what the client sends to its mounts, follows
//! the client's watches, and writes the client's [`Outbox`] to its socket.
//! [`Session::run`] runs it.
//!
//! A device watch follows the device's presence, announced settings and
//! routes here, and the RPC table each announced setting was recorded
//! under, and these updates go on the outbox's urgent queue. The
//! device's metadata update says how to decode its samples, so the engine of
//! the device's link queues it on the stream instead, as it sets it. A table
//! watch follows the RPC table and settings here.

use super::auto::{self, Choice};
use super::control::encode;
use super::control::{self, Purpose, Refusal, Request, Update, Watch};
use super::engine::Mount;
use super::link::{self, Link, Mounted};
use super::outbox::Outbox;
use super::outbox::Taken;
use super::server::{self, Notice, Shared};
use super::worker;
use super::Layout;
use crate::chan::oneshot::{self, error::TryRecvError};
use crate::chan::{lock, StreamMap};
use crate::device::store::{Store, CHANGES, PRESENCE, REGISTRY, SETTINGS};
use crate::device::stream::Scope;
use crate::device::{Receiver, RecvError, SettingChange};
use crate::proto::rpc::{Method, RpcError};
use crate::proto::{DeviceRoute, PacketType, SessionId, TCP_DEFAULT_PORT};
use crate::rt::{finish, BoxFuture};
use crate::tio::packet::{Packet, Payload};
use crate::tio::transport::{self, is_local_tcp, ReceiveResult, TransportEvent, CLOSE_TIMEOUT};
use futures_util::future;
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{timeout_at, Instant};
use tokio_stream::StreamNotifyClose;

/// How many packets a client's socket queues. The outbox holds what waits
/// for the client, and this queue only keeps the socket's task busy while
/// the next batch is taken.
pub(super) const SOCKET_QUEUE: usize = 64;

/// Why a client's connection ended. The task serving the client logs it
/// once.
#[derive(Debug, Clone, thiserror::Error)]
pub(super) enum ClientError {
    /// The client closed its connection, or the connection failed. A write
    /// to a client that stops reading fails so once the transport has
    /// waited its drain timeout, `DRAIN_TIMEOUT`, for one packet to go out,
    /// and the connection of a client quiet for longer than the keepalive
    /// allows fails so too.
    #[error("it hung up")]
    HungUp,
    /// The proxy stopped and told the client.
    #[error("the proxy stopped")]
    Stopped,
    /// The client let more pile up than its outbox holds, its
    /// [`HARD_LIMIT`](super::outbox::HARD_LIMIT).
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

/// What a client opened. Each mount holds a lease on its link until dropped.
struct Binding {
    target: String,
    mounts: Vec<Mounted>,
}

/// The default a client holds with `proxy.default`. Dropping it gives the
/// default back to the layout it replaced.
struct Held {
    shared: Arc<Shared>,
    replaced: Option<Layout>,
}

impl Drop for Held {
    fn drop(&mut self) {
        *lock(&self.shared.holder) = None;
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
    incoming: crate::chan::RecvStream<'static, ReceiveResult>,
    /// Closes once the server drops its lifeline, as the proxy stops.
    alive: oneshot::Receiver<Infallible>,
    /// Packets the client sent that wait their turn while an open waits.
    backlog: VecDeque<Packet>,
    /// What goes to the client.
    outbox: Arc<Outbox>,
    /// The updates of the client's watches but metadata, by the id of each
    /// `proxy.watch`. Each ends with `None`.
    watches: StreamMap<u16, StreamNotifyClose<BoxStream<'static, Update>>>,
    /// The write of what was last taken from the outbox, while it lasts.
    writing: Option<BoxFuture<'static, Result<(), ClientError>>>,
    binding: Option<Binding>,
    opening: Option<Opening>,
    held: Option<Held>,
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
        incoming: crate::chan::Receiver<ReceiveResult>,
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
            outbox: Arc::default(),
            watches: StreamMap::new(),
            writing: None,
            binding: None,
            opening: None,
            held: None,
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
    /// such as the status that says why it ends. The write under way goes
    /// first, then everything the outbox still holds, the status that the
    /// proxy stopped last. Waits at most [`CLOSE_TIMEOUT`].
    async fn close(mut self) {
        // The links and watches stop feeding the client first.
        self.binding = None;
        self.watches.clear();
        let deadline = Instant::now() + CLOSE_TIMEOUT;
        let flushed = async {
            loop {
                if self.writing.is_none() {
                    // The client is let go already, whatever the outbox says.
                    let _end = self.write();
                }
                // Nothing was left to write.
                if self.writing.is_none() {
                    return;
                }
                if finish(&mut self.writing).await.is_err() {
                    return;
                }
            }
        };
        let _ = timeout_at(deadline, flushed).await;
        // The transport writes its queue before it ends. It would end at
        // once if what the client sends found no receiver, so `incoming`
        // stays until then.
        let left = deadline.saturating_duration_since(Instant::now());
        self.client.close(left).await;
    }

    /// Forwards what the client sends to its mounts, follows its watches,
    /// and writes its outbox, until the connection ends, keeping the client's
    /// link alive. Returns why it ended.
    async fn serve(&mut self) -> Result<(), ClientError> {
        self.client.keep_alive(control::PROXY_KEEPALIVE).await;
        loop {
            if self.stopping() {
                return Err(self.stopped());
            }
            // What the client sent while an open waited goes first, in
            // order.
            while self.opening.is_none() {
                let Some(pkt) = self.backlog.pop_front() else {
                    break;
                };
                self.client_packet(pkt).await?;
            }
            if self.writing.is_none() {
                if let Some(end) = self.write() {
                    return Err(end);
                }
            }
            let outbox = Arc::clone(&self.outbox);
            tokio::select! {
                _ = &mut self.alive => return Err(self.stopped()),
                received = self.incoming.next() => {
                    let Some(received) = received else { return Err(ClientError::HungUp); };
                    if let Some(pkt) = self.delivered(received)? {
                        self.backlog.push_back(pkt);
                    }
                }
                Some((id, update)) = self.watches.next(), if !self.watches.is_empty() => {
                    self.watched(id, update);
                }
                () = outbox.ready.notified(), if self.writing.is_none() => {}
                written = finish(&mut self.writing) => written?,
                (request, outcome) = finish(&mut self.opening) => {
                    self.opened(request, outcome).await?;
                }
            }
        }
    }

    /// Starts writing what the outbox holds for the client, if it holds
    /// anything. Each packet waits for room in the socket's queue of
    /// [`SOCKET_QUEUE`] packets, and a write fails as
    /// [`ClientError::HungUp`] once the connection has.
    ///
    /// Returns why the client must be let go once that is written, if the
    /// outbox says so.
    fn write(&mut self) -> Option<ClientError> {
        let Taken {
            packets: queued,
            dropped,
            end,
        } = self.outbox.take();
        if dropped > 0 {
            log::debug!(
                "Client {} is not keeping up and lost {dropped} samples",
                self.addr
            );
        }
        if queued.is_empty() {
            return end;
        }
        if self.shared.options.dump.traffic {
            for pkt in queued.iter().filter(|pkt| is_rpc(pkt)) {
                log::info!("{}->{} -- {:?}", pkt.route(), self.addr, pkt.payload());
            }
        }
        let sends: Vec<_> = queued
            .into_iter()
            .map(|pkt| self.client.send_async(pkt))
            .collect();
        self.writing = Some(Box::pin(async {
            for send in sends {
                send.await.map_err(|_| ClientError::HungUp)?;
            }
            Ok(())
        }));
        end
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

    /// Stops feeding the client, queues the status that the proxy stopped
    /// behind what was queued for it, and returns [`ClientError::Stopped`].
    /// The close writes it as it writes the rest.
    fn stopped(&mut self) -> ClientError {
        self.binding = None;
        self.watches.clear();
        self.outbox.stop();
        ClientError::Stopped
    }

    /// Queues `update` of watch `id` for the client, whole, or ends a watch
    /// whose updates have ended and queues its refusal.
    fn watched(&mut self, id: u16, update: Option<Update>) {
        match update {
            Some(update) => self.outbox.update(id, &update),
            None => {
                self.unwatch(id);
                let ended = "the proxy no longer follows this device";
                self.answer(id, Err(Refusal::new(RpcError::State, ended)));
            }
        }
    }

    /// Handles a packet from the client. Answers a control request, and
    /// forwards any other packet to the mount its route falls under. Drops a
    /// packet that falls under no mount.
    ///
    /// Returns [`ClientError::ForwardFailed`] if the mount's port is full, or
    /// the packet's route reaches too deep on the device. Drops a packet for
    /// a link that is ending, whose end lets the client go with the reason.
    async fn client_packet(&mut self, pkt: Packet) -> Result<(), ClientError> {
        if self.shared.options.dump.traffic {
            log::info!("{}->{} -- {:?}", self.addr, pkt.route(), pkt.payload());
        }
        match pkt.ptype() {
            PacketType::PROXY_TX => {
                self.control(&pkt).await;
                return Ok(());
            }
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
        let dest = binding.mounts.iter().find_map(|mounted| {
            let route = mounted.mount.to_device(pkt.route())?;
            Some((mounted, route))
        });
        let Some((mounted, route)) = dest else {
            log::debug!(
                "Client {} addressed unmounted route {}",
                self.addr,
                pkt.route()
            );
            return Ok(());
        };
        let route = route.map_err(|_| ClientError::ForwardFailed)?;
        match mounted.port.try_send(pkt.with_route(route)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(ClientError::ForwardFailed),
            // The link's worker lets its ports go as the link ends, a moment
            // before the link tells its mounts why and lets their clients
            // go. The client stays to hear that.
            Err(TrySendError::Closed(_)) => {
                log::debug!(
                    "Dropping a packet from client {} to a link that is ending",
                    self.addr
                );
                Ok(())
            }
        }
    }

    /// Queues the answer to control request `id` for the client.
    fn answer(&self, id: u16, outcome: Result<Vec<u8>, Refusal>) {
        self.outbox.urgent(control::answer(id, outcome));
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
    async fn control(&mut self, pkt: &Packet) {
        let Payload::ProxyTx(request) = pkt.payload() else {
            return;
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
                let outbox = Arc::clone(&self.outbox);
                let target = layout.to_string();
                let binding = attach(shared, addr, target, layout, None, Purpose::Use, outbox);
                self.request(id, binding)
            }
            Request::Default(layout) => self.hold_default(id, layout),
            Request::Watch(watch) => self.watch(id, watch),
            Request::Unwatch(watched) => self.unwatch(watched),
            Request::Stop if self.shared.options.stoppable => {
                self.answer(id, Ok(Vec::new()));
                log::info!("Stopped by proxy.stop from client {}", self.addr);
                if let Some(server) = self.shared.server.upgrade() {
                    let _ = server.send(Notice::Stop);
                }
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
    fn watch(&mut self, id: u16, watch: Watch) {
        let Some(binding) = &self.binding else {
            let refusal = Refusal::new(RpcError::State, "this connection opened nothing");
            return self.answer(id, Err(refusal));
        };
        match binding.watch(id, watch) {
            Ok(updates) => {
                self.watches.insert(id, StreamNotifyClose::new(updates));
            }
            Err(refusal) => self.answer(id, Err(refusal)),
        }
    }

    /// Stops watch `id`.
    fn unwatch(&mut self, id: u16) {
        self.watches.remove(&id);
        if let Some(binding) = &self.binding {
            binding.unwatch_metadata(id);
        }
    }

    /// Makes `layout` the default until this client leaves, answering control
    /// request `id`. Only a background proxy lets a client hold its default,
    /// and one client at a time.
    fn hold_default(&mut self, id: u16, layout: Layout) {
        if !self.shared.options.stoppable {
            let message = format!(
                "the proxy at {} is hosted by {} and keeps its own default",
                self.shared.endpoint, self.shared.options.name
            );
            return self.answer(id, Err(Refusal::new(RpcError::State, message)));
        }
        let mut holder = lock(&self.shared.holder);
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
    fn request_open(&mut self, id: u16, target: String, purpose: Purpose) {
        let (shared, addr) = (self.shared.clone(), self.addr.clone());
        let binding = bind(shared, addr, target, purpose, Arc::clone(&self.outbox));
        self.request(id, binding)
    }

    /// Begins the `binding` that control request `id` asks for. A connection
    /// binds once, so a second request is refused at once.
    fn request(
        &mut self,
        id: u16,
        binding: impl Future<Output = Result<Binding, Refusal>> + Send + 'static,
    ) {
        if let Some(binding) = &self.binding {
            let message = format!("this connection already opened {}", binding.target);
            return self.answer(id, Err(Refusal::new(RpcError::State, message)));
        }
        self.opening = Some(Box::pin(async move { (id, binding.await) }));
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
            Err(_) if self.stopping() => return Err(self.stopped()),
            Err(refusal) => Err(refusal),
        };
        self.answer(request, outcome);
        Ok(())
    }
}

/// Binds a connection of the client at `addr` to `target` for `purpose`.
/// Resolves the target, takes a lease on each mount's link, waits for each
/// link's device to answer, and mounts each for the client of `outbox`. A
/// binding for [`Purpose::Use`] marks each link as chosen. A probe's target
/// is a device URL.
///
/// Returns a [`Refusal`] if the target does not resolve, a link is refused,
/// or a link is not ready within [`link::READY_TIMEOUT`].
async fn bind(
    shared: Arc<Shared>,
    addr: String,
    target: String,
    purpose: Purpose,
    outbox: Arc<Outbox>,
) -> Result<Binding, Refusal> {
    let (target, layout, chosen) = match purpose {
        Purpose::Use => resolve(&shared, target).await?,
        Purpose::Probe => (target.clone(), Layout::single(&target), None),
    };
    attach(shared, addr, target, layout, chosen, purpose, outbox).await
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
    outbox: Arc<Outbox>,
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
    for (mount, lease) in layout.mounts.iter().zip(leases) {
        let mounted = lease.mount(mount.source, mount.prefix, &outbox);
        mounts.push(mounted.map_err(|reason| Refusal::new(RpcError::Load, reason))?);
    }
    match purpose {
        Purpose::Use => {
            let chosen = mounts.iter().map(|mounted| mounted.lease.link().id());
            shared.links.choose(chosen.collect());
            log::info!("Client {addr} opened {target}");
        }
        Purpose::Probe => log::debug!("Client {addr} probed {target}"),
    }
    Ok(Binding { target, mounts })
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

impl Binding {
    /// Starts watch `id`, in place of any watch the client had under that
    /// id, and returns the updates the client's task follows for it.
    ///
    /// With `table`, the updates are the RPC table and settings of the device
    /// the watch names, and a table goes out ahead of the first settings read
    /// with it. Without, they are the routes heard at or below it, from every
    /// mount there, and the presence and announced settings of the device it
    /// names, if a mount shows it, each setting after the table it was
    /// recorded under. The engine of that mount's link then queues the
    /// device's metadata updates for the watch itself. The updates end when
    /// the link's engine does.
    ///
    /// Returns a [`Refusal`] if no mount shows what the watch follows, and
    /// leaves the watch the client had under that id as it was.
    fn watch(&self, id: u16, watch: Watch) -> Result<BoxStream<'static, Update>, Refusal> {
        let refused = || Refusal::new(RpcError::Invalid, format!("no mount shows {}", watch.route));
        let device = self.device(watch.route);
        if watch.table {
            let (mounted, route) = device.ok_or_else(refused)?;
            self.unwatch_metadata(id);
            return Ok(table_updates(&mounted.lease.link().store(), route));
        }
        let routes = self.routes(watch.route).ok_or_else(refused)?;
        self.unwatch_metadata(id);
        let Some((mounted, route)) = device else {
            return Ok(routes);
        };
        let store = mounted.lease.link().store();
        mounted.mount.watch_metadata(id, route, &store);
        let device = device_updates(&store, route);
        Ok(stream::select(routes, device).boxed())
    }

    /// Stops the metadata updates of watch `id` on every mount.
    fn unwatch_metadata(&self, id: u16) {
        for mounted in &self.mounts {
            mounted.mount.unwatch(id);
        }
    }

    /// Returns the mount that shows `route`, and the route on its link.
    fn device(&self, route: DeviceRoute) -> Option<(&Mounted, DeviceRoute)> {
        self.mounts.iter().find_map(|mounted| {
            let on_link = mounted.mount.to_device(route)?.ok()?;
            Some((mounted, on_link))
        })
    }

    /// Returns the updates of the routes heard at or below `route`, as the
    /// client sees them, from every mount there. Each change of any mount's
    /// list sends the whole list, read from every mount's store then.
    /// Returns `None` if no mount is there.
    fn routes(&self, route: DeviceRoute) -> Option<BoxStream<'static, Update>> {
        let shown: Vec<(Arc<Mount>, Arc<Mutex<Store>>, Scope)> = self
            .mounts
            .iter()
            .filter_map(|mounted| {
                let subtree = mounted.mount.subtree_at(route)?;
                let store = mounted.lease.link().store();
                Some((Arc::clone(&mounted.mount), store, Scope::subtree(subtree)))
            })
            .collect();
        if shown.is_empty() {
            return None;
        }
        let changes = shown
            .iter()
            .map(|(_, store, scope)| values(lock(store).follow_routes(*scope)));
        let changes = stream::select_all(changes.collect::<Vec<_>>());
        let lists = changes.map(move |_| {
            let all = shown.iter().flat_map(|(mount, store, scope)| {
                let heard = lock(store).routes().clone().into_iter();
                let heard = heard.filter(|heard| scope.covers(*heard));
                heard.filter_map(|heard| mount.to_client(heard))
            });
            Update::Routes(all.map(|route| route.as_slice().to_vec().into()).collect())
        });
        Some(lists.boxed())
    }
}

/// Returns the updates of the presence and announced settings of the device
/// at `route` that `store` holds, each setting after the table it was
/// recorded under.
pub(super) fn device_updates(
    store: &Mutex<Store>,
    route: DeviceRoute,
) -> BoxStream<'static, Update> {
    let mut store = lock(store);
    let presence = values(store.follow(route, PRESENCE)).map(Update::presence);
    let changes = announced(store.follow(route, CHANGES));
    stream::select_all([presence.boxed(), changes]).boxed()
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
/// of those it missed, until it ends. The table a setting was recorded under
/// goes out ahead of it, unless it has the hash and session of the table of
/// the setting before.
fn announced(changes: Receiver<SettingChange>) -> BoxStream<'static, Update> {
    // The hash and session of the table named last, or `None` for no
    // table, once one is named.
    type Identity = Option<(Option<u32>, Option<SessionId>)>;
    let named: Option<Identity> = None;
    stream::unfold((changes, named), move |(changes, mut named)| async move {
        let updates = match changes.recv_async().await {
            Ok(change) => {
                let table = change.rpc_registry();
                let identity = table.map(|table| (table.hash, table.session));
                let name = (named != Some(identity)).then(|| Update::recorded(table));
                named = Some(identity);
                name.into_iter().chain([Update::setting(change)]).collect()
            }
            Err(RecvError::Lagged(missed)) => vec![Update::Lagged(missed)],
            Err(_) => return None,
        };
        Some((stream::iter(updates), (changes, named)))
    })
    .flatten()
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
    use crate::device::rpc::RpcRegistry;
    use crate::device::session::{Inlet, Received, Session as AppSession};
    use crate::device::stream::{End, Queued};
    use crate::proxy::link::{Links, Owner};
    use crate::proxy::tests::fixtures::{assert_took, fakes, opened, options, Opened, PAUSED_WAIT};
    use crate::proxy::DumpFlags;
    use crate::rt::Supervisor;
    use crate::tio::packet::{LinkStatus, ProxyRx};
    use crate::tio::transport::tests::fake::{self, FakeEnd};
    use crate::tio::transport::{TransportKind, DEFAULT_RX_CHANNEL_SIZE};
    use std::time::Duration;
    use tokio::sync::{mpsc, watch};

    use crate::chan::JoinHandle;
    use futures_util::FutureExt;
    use tokio::time::timeout;

    /// The device a test opens.
    const DEVICE: &str = "tcp://device";

    /// What resolves to why a client's session ended.
    type Served = JoinHandle<Option<Result<(), ClientError>>>;

    /// A client's session on the test's runtime, over a fake connection, of
    /// a proxy whose devices are fake.
    struct Client {
        /// The client's end of its connection.
        end: FakeEnd,
        /// Each device port the proxy opens.
        devices: mpsc::UnboundedReceiver<Opened>,
        links: Links,
        /// Resolves to why the session ended.
        served: Served,
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
        let (end, served, lifeline) = join(&links, default);
        Client {
            end,
            devices,
            links,
            served,
            _registry: registry,
            _lifeline: lifeline,
        }
    }

    /// Connects a client to the proxy whose links are `links` and whose
    /// default is `default`. Returns the client's end of its connection,
    /// what resolves to why its session ended, and the lifeline whose drop
    /// stops the proxy for it.
    fn join(
        links: &Links,
        default: Option<Layout>,
    ) -> (FakeEnd, Served, oneshot::Sender<Infallible>) {
        let tasks = Supervisor::current();
        let shared = Arc::new(Shared {
            options: options(),
            endpoint: "tcp://127.0.0.1:7855".to_string(),
            links: links.clone(),
            tasks: tasks.clone(),
            server: mpsc::unbounded_channel().0.downgrade(),
            default: watch::channel(default).0,
            holder: std::sync::Mutex::new(None),
        });
        let (deliver, incoming) = crate::chan::bounded(DEFAULT_RX_CHANNEL_SIZE);
        let (port, end) = fake::port_queue(TransportKind::Tcp, None, deliver, SOCKET_QUEUE);
        let peer = "127.0.0.1:50000".parse().unwrap();
        let (lifeline, alive) = oneshot::channel();
        let mut session = Session::new(shared, 1, peer, port, incoming, alive);
        let served = tasks.spawn("a test client", async move {
            let served = session.serve().await;
            session.close().await;
            served
        });
        (end, served, lifeline)
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

        let gave_up = |pkt: &Packet| pkt.link_status() == Some(LinkStatus::FailedToReconnect);
        read_until(&mut client.end, gave_up).await;
        let error = ended(client).await;
        assert!(matches!(error, ClientError::GaveUp(_)), "{error}");
        assert_eq!(error.to_string(), format!("the proxy gave up on {DEVICE}"));
    }

    /// A packet whose route on its device would be deeper than a route can
    /// be cannot reach the device, and its client is let go.
    #[tokio::test(start_paused = true)]
    async fn a_packet_too_deep_for_its_device_lets_its_client_go() {
        let mut client = connect(None, None);
        let source: DeviceRoute = "/1/2/3/4/5/6".parse().unwrap();
        let layout = Layout {
            mounts: vec![crate::proxy::Mount {
                url: DEVICE.to_string(),
                source,
                prefix: DeviceRoute::root(),
            }],
        };
        let mount = Packet::proxy_request(control::MOUNT, &encode(&layout), 2).unwrap();
        client.end.send(&mount).await;
        let mut device = opened(&mut client.devices).await;
        let root = DeviceRoute::root();
        device.end.send(&Packet::heartbeat(root)).await;
        assert_eq!(answer(&next(&mut client.end).await).0, 2);

        let too_deep = Packet::heartbeat("/1/2/3".parse().unwrap());
        client.end.send(&too_deep).await;

        let error = ended(client).await;
        assert!(matches!(error, ClientError::ForwardFailed), "{error}");
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

    /// A packet of 100 samples of stream 1 at the root, numbered from
    /// `first`.
    fn hundred(first: u32) -> Packet {
        Packet::samples(1, 0, first, &[0; 400], DeviceRoute::root()).unwrap()
    }

    /// The bytes of the payload of a [`big_beat`].
    const BIG_BEAT: usize = 496;

    /// A heartbeat at the root with a payload of [`BIG_BEAT`] bytes that
    /// carries `number`. It is no sample.
    fn big_beat(number: u32) -> Packet {
        let mut raw = vec![PacketType::HEARTBEAT.value(), 0];
        raw.extend((BIG_BEAT as u16).to_le_bytes());
        raw.extend(number.to_le_bytes());
        raw.resize(4 + BIG_BEAT, 0);
        Packet::from_slice_prefix(&raw).unwrap().0
    }

    /// Returns the number a heartbeat carries, its session or the number of
    /// a [`big_beat`].
    fn beat_number(pkt: &Packet) -> Option<u32> {
        let Payload::Heartbeat(beat) = pkt.payload() else {
            return None;
        };
        let bytes = pkt.payload_bytes();
        match beat.session() {
            Some(session) => Some(session.value()),
            None if bytes.len() == BIG_BEAT => {
                Some(u32::from_le_bytes(bytes[..4].try_into().ok()?))
            }
            None => None,
        }
    }

    /// Reads what `end` is sent until a packet `done` picks, and returns the
    /// numbers of the heartbeats and the first sample numbers of the samples
    /// it read, in order.
    async fn read_until(end: &mut FakeEnd, done: impl Fn(&Packet) -> bool) -> (Vec<u32>, Vec<u32>) {
        let (mut beats, mut samples) = (Vec::new(), Vec::new());
        loop {
            let pkt = next(end).await;
            beats.extend(beat_number(&pkt));
            if let Payload::Samples(data) = pkt.payload() {
                samples.push(data.first.value());
            }
            if done(&pkt) {
                return (beats, samples);
            }
        }
    }

    /// A client that stops reading for less time than a write may take stays
    /// connected and reads what it was sent. Once it stops reading for longer
    /// than a write may take, it is let go as hung up, well before the
    /// keepalive would give it up.
    #[tokio::test(start_paused = true)]
    async fn a_client_that_stops_reading_for_longer_than_a_write_may_take_is_let_go() {
        let mut client = connect(None, None);
        let mut device = opened_and_answered(&mut client).await;
        // More samples than the outbox holds, with a numbered heartbeat
        // ahead of every 60 packets of them.
        let (beats, per) = (60, 60);
        for beat in 0..beats {
            let route = DeviceRoute::root();
            device
                .end
                .send(&Packet::heartbeat_session(beat, route))
                .await;
            for first in 0..per {
                device.end.send(&hundred((beat * per + first) * 100)).await;
            }
        }
        // The client reads again before a write runs out of time.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let last = |pkt: &Packet| match pkt.payload() {
            Payload::Heartbeat(beat) => beat.session().map(|s| s.value()) == Some(beats - 1),
            _ => false,
        };
        read_until(&mut client.end, last).await;
        let rest = |pkt: &Packet| match pkt.payload() {
            Payload::Samples(data) => data.first.value() == (beats * per - 1) * 100,
            _ => false,
        };
        read_until(&mut client.end, rest).await;

        client
            .end
            .send(&Packet::heartbeat(DeviceRoute::root()))
            .await;
        let stalled = Instant::now();
        for first in 0..1000 {
            device.end.send(&hundred(first * 100)).await;
        }
        let error = ended(client).await;
        assert!(matches!(error, ClientError::HungUp), "{error}");
        assert!(stalled.elapsed() < control::PROXY_KEEPALIVE.quiet);
    }

    /// A proxy that stops queues its status behind everything a stalled
    /// client was not yet sent. The client hears all of it, and the stop
    /// last, once it reads again within the close.
    #[tokio::test(start_paused = true)]
    async fn a_stop_goes_out_behind_what_a_stalled_client_was_not_yet_sent() {
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
        for beat in 0..2000 {
            device
                .end
                .send(&Packet::heartbeat_session(beat, root))
                .await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        drop(_lifeline);
        let stop = |pkt: &Packet| pkt.link_status() == Some(LinkStatus::Stopped);
        let (beats, _) = read_until(&mut end, stop).await;

        assert_eq!(beats, (0..2000).collect::<Vec<_>>());
        let served = timeout(PAUSED_WAIT, served).await.unwrap().unwrap();
        assert!(
            matches!(served, Some(Err(ClientError::Stopped))),
            "{served:?}"
        );
    }

    /// Connects another client to the proxy whose links are `links`, and
    /// opens [`DEVICE`] for it, which the link has open already. Returns what
    /// [`join`] returns.
    async fn joined(links: &Links) -> (FakeEnd, Served, oneshot::Sender<Infallible>) {
        let (mut end, served, lifeline) = join(links, None);
        end.send(&open(DEVICE, 2)).await;
        assert_eq!(answer(&next(&mut end).await), (2, Ok(encode(&DEVICE))));
        (end, served, lifeline)
    }

    /// A client that falls behind loses samples, the oldest first, and
    /// nothing else, and holds up neither its link nor another client of the
    /// link, which hears every packet the device sent. Once more than the
    /// hard limit of other packets waits for it, it is let go as too slow,
    /// and the link and the other client go on.
    #[tokio::test(start_paused = true)]
    async fn a_client_that_falls_behind_loses_only_samples_and_holds_up_no_other() {
        let mut behind = connect(None, None);
        let mut device = opened_and_answered(&mut behind).await;
        let (mut other, other_served, _other_lifeline) = joined(&behind.links).await;
        let limit = super::super::outbox::HARD_LIMIT;
        let beats = (limit / BIG_BEAT / 100 + 10) as u32 * 100;
        // The clock stands still while packets flow, so the client that
        // reads nothing falls behind before any write runs out of time.
        let (mut heard, mut samples) = (Vec::new(), Vec::new());
        for beat in 0..beats {
            device.end.send(&big_beat(beat)).await;
            if beat % 4 == 0 {
                device.end.send(&hundred(beat * 100)).await;
            }
            if beat % 100 == 99 {
                let caught_up = |pkt: &Packet| beat_number(pkt) == Some(beat);
                let (more, more_samples) = read_until(&mut other, caught_up).await;
                heard.extend(more);
                samples.extend(more_samples);
            }
        }
        let sent = (0..beats).step_by(4).map(|beat| beat * 100);
        let whole = heard.into_iter().eq(0..beats) && samples.into_iter().eq(sent);
        assert!(whole, "the other client lost a packet");

        let (mut kept, mut kept_samples) = (Vec::new(), Vec::new());
        while let Some(pkt) = timeout(PAUSED_WAIT, behind.end.recv()).await.unwrap() {
            kept.extend(beat_number(&pkt));
            if let Payload::Samples(data) = pkt.payload() {
                kept_samples.push(data.first.value());
            }
        }
        let in_order = kept.iter().copied().eq(0..kept.len() as u32);
        assert!(in_order, "a beat was lost");
        let refused_early = kept.len() * big_beat(0).as_bytes().len() <= limit;
        assert!(!refused_early, "beats were refused below the hard limit");
        let gaps = kept_samples
            .windows(2)
            .filter(|pair| pair[1] - pair[0] > 400);
        assert_eq!(gaps.count(), 1, "the oldest samples waiting were lost");
        let served = timeout(PAUSED_WAIT, &mut behind.served).await.unwrap();
        assert!(matches!(served, Ok(Some(Err(ClientError::TooSlow)))));

        device.end.send(&big_beat(beats)).await;
        read_until(&mut other, |pkt| beat_number(pkt) == Some(beats)).await;
        assert!(!other_served.is_finished());
    }

    /// Each client of a device that drops out and comes back hears that the
    /// device was lost and then that it is back, ahead of anything the device
    /// sends once back. It stays attached, and its requests reach the device
    /// again.
    #[tokio::test(start_paused = true)]
    async fn the_clients_of_a_device_that_comes_back_hear_so_ahead_of_its_data() {
        let mut first = connect(None, None);
        let device = opened_and_answered(&mut first).await;
        let (mut second, second_served, _second_lifeline) = joined(&first.links).await;

        drop(device);
        let mut back = opened(&mut first.devices).await;
        let root = DeviceRoute::root();
        back.end.send(&Packet::heartbeat_session(2, root)).await;
        back.end.send(&hundred(0)).await;

        for end in [&mut first.end, &mut second] {
            let mut heard = Vec::new();
            while heard.last().map(String::as_str) != Some("samples") {
                let pkt = next(end).await;
                heard.push(
                    match (pkt.link_status(), beat_number(&pkt), pkt.payload()) {
                        (Some(status), ..) => format!("{status:?}"),
                        (_, Some(beat), _) => format!("beat {beat}"),
                        (.., Payload::Samples(_)) => "samples".to_string(),
                        // The heartbeat the device answered the open with.
                        (.., Payload::Heartbeat(_)) => continue,
                        (.., other) => format!("{other:?}"),
                    },
                );
            }
            let expected = [
                "SensorDisconnected",
                "SensorReconnected",
                "beat 2",
                "samples",
            ];
            assert_eq!(heard, expected);
        }
        let request = Packet::rpc_request("dev.name", &[], 7, root).unwrap();
        second.send(&request).await;
        let asked = timeout(PAUSED_WAIT, back.end.recv())
            .await
            .unwrap()
            .unwrap();
        let Payload::RpcRequest(asked) = asked.payload() else {
            panic!("the device was asked {:?}", asked.payload());
        };
        let reply = Packet::rpc_reply(asked.id.value(), b"FAKE", root).unwrap();
        back.end.send(&reply).await;
        let answered = next(&mut second).await;
        assert!(
            matches!(answered.payload(), Payload::RpcReply(reply) if reply.req_id.value() == 7)
        );
        assert!(!first.served.is_finished() && !second_served.is_finished());
    }

    /// Runs an app's session over `end`, a client's connection, as its
    /// socket. The session opens again once its socket closes without a
    /// stop. Returns the session and the inlet of its pump.
    fn app_over(mut end: FakeEnd) -> (AppSession, Inlet) {
        let (app, proxy) = AppSession::test_pair_reopening(&Supervisor::current());
        let inlet = app.attach(usize::MAX);
        crate::chan::spawn(async move {
            loop {
                tokio::select! {
                    from_proxy = end.recv() => match from_proxy {
                        Some(pkt) => drop(proxy.deliver.send(Ok(TransportEvent::Packet(pkt)))),
                        None => return,
                    },
                    from_app = proxy.sent.recv_async() => match from_app {
                        Ok(pkt) => end.send(&pkt).await,
                        Err(_) => return,
                    },
                }
            }
        });
        (app, inlet)
    }

    /// A graceful stop of either end ends both, and neither end takes it for
    /// a lost connection, so nothing opens again. A proxy that stops lets
    /// its client go as stopped, and the app hears the stop last and ends
    /// its session as stopped. An app that closes its session ends it as
    /// closed, and the proxy lets its client go as hung up.
    #[tokio::test(start_paused = true)]
    async fn a_graceful_stop_of_either_end_ends_both_and_is_never_a_lost_connection() {
        for proxy_stops in [true, false] {
            let mut client = connect(None, None);
            let mut device = opened_and_answered(&mut client).await;
            let (app, inlet) = app_over(client.end);
            device
                .end
                .send(&Packet::heartbeat_session(5, DeviceRoute::root()))
                .await;
            while !matches!(
                timeout(PAUSED_WAIT, inlet.recv_async()).await.unwrap(),
                Ok(Queued::Item(Received::Packet(pkt))) if beat_number(&pkt) == Some(5)
            ) {}

            if proxy_stops {
                drop(client._lifeline);
            } else {
                app.close();
            }

            let mut rest = Vec::new();
            while let Ok(item) = timeout(PAUSED_WAIT, inlet.recv_async()).await.unwrap() {
                rest.push(item);
            }
            let lost = |item: &Queued<Received>| matches!(item, Queued::Item(Received::SocketLost));
            assert!(!rest.iter().any(lost), "{rest:?}");
            let (end, error) = match proxy_stops {
                true => (End::Stopped, ClientError::Stopped),
                false => (End::Closed, ClientError::HungUp),
            };
            assert!(matches!(rest.pop(), Some(Queued::Ended(ended)) if ended == end));
            if proxy_stops {
                let stop = matches!(rest.pop(), Some(Queued::Item(Received::Packet(pkt)))
                    if pkt.link_status() == Some(LinkStatus::Stopped));
                assert!(stop, "the stop was not the last the app heard");
            }
            let served = timeout(PAUSED_WAIT, client.served).await.unwrap().unwrap();
            let served = served.expect("the session panicked").unwrap_err();
            assert_eq!(served.to_string(), error.to_string());
        }
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
        assert_eq!(ready(&mut updates), [Update::Presence(Err(None))]);

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
        let change = |seq: usize| {
            let seq = u32::try_from(seq).ok();
            SettingChange::new("test.amplitude".to_string(), seq, vec![1], None)
        };
        let missed = 10;
        let sent = crate::device::store::CHANGE_QUEUE_LEN + missed;
        for seq in 0..sent {
            lock(&store).set(route, CHANGES, change(seq));
        }
        let first = ready(&mut updates);
        assert_eq!(first[0], Update::Recorded(None));
        assert_eq!(first.len(), crate::device::store::CHANGE_QUEUE_LEN + 1);

        lock(&store).set(route, CHANGES, change(sent));
        assert_eq!(
            ready(&mut updates),
            [Update::Lagged(missed as u64), Update::setting(change(sent))]
        );
    }

    /// A watch names the table each announced setting was recorded under
    /// ahead of the first setting recorded under it, and names no table
    /// ahead of a setting recorded while none was in force. A table loaded
    /// again with the hash and session of the one named is not named again.
    #[test]
    fn a_watch_names_each_table_ahead_of_the_settings_recorded_under_it() {
        let store = Mutex::new(Store::new(true));
        let route = DeviceRoute::root();
        let mut updates = device_updates(&store, route);
        ready(&mut updates);
        let table = |hash| Arc::new(RpcRegistry::from_entries(Vec::new(), Some(hash), None));
        let (old, new) = (table(1), table(2));
        let record = |seq| {
            let name = "test.amplitude".to_string();
            lock(&store).record_change(route, name, Some(seq), vec![1])
        };

        let before = record(0);
        lock(&store).set_registry(route, Ok(Arc::clone(&old)));
        let (first, second) = (record(1), record(2));
        lock(&store).set_registry(route, Ok(Arc::clone(&new)));
        let after = record(3);
        lock(&store).set_registry(route, Ok(table(2)));
        let reloaded = record(4);

        assert_eq!(
            ready(&mut updates),
            [
                Update::recorded(None),
                Update::setting(before),
                Update::recorded(Some(&old)),
                Update::setting(first),
                Update::setting(second),
                Update::recorded(Some(&new)),
                Update::setting(after),
                Update::setting(reloaded),
            ]
        );
    }
}

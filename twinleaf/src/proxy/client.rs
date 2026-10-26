//! The task that serves each client connection. It answers the client's
//! control requests and routes packets between the client and its mounts.
//! [`Session::run`] runs it.

use super::auto::{self, Choice};
use super::control::{self, Purpose, Refusal, Request};
use super::link::{self, Lease, Link};
use super::server::{Notice, Shared, Update};
use super::worker;
use super::Layout;
use crate::proto::rpc::{Method, RpcError};
use crate::proto::{DeviceRoute, PacketType};
use crate::rt::{finish, sleep_until_some, BoxFuture};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use crate::tio::rpc::RpcArgs;
use crate::tio::transport::{self, ReceiveResult, TransportEvent, CLOSE_TIMEOUT};
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::{self, error::TryRecvError};
use tokio::time::{timeout_at, Instant};
use tokio_stream::{wrappers::ReceiverStream, StreamMap, StreamNotifyClose};

/// How long a new client has to send a control request before it is taken
/// for a legacy client.
const CLASSIFY: Duration = Duration::from_millis(250);

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
    /// The proxy refused what a legacy client opened. A control client hears
    /// the refusal and stays connected.
    #[error("auto opened nothing for it: {}", .0.message)]
    Refused(Refusal),
}

impl From<Refusal> for ClientError {
    fn from(refusal: Refusal) -> ClientError {
        ClientError::Refused(refusal)
    }
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

/// An open in flight. It resolves to the id of the control request it
/// answers, or `None` for a legacy client's open of `auto`, and the outcome
/// of the binding.
type Opening = BoxFuture<'static, (Option<u16>, Result<Binding, Refusal>)>;

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
    /// When a client that has sent nothing is taken for a legacy client, or
    /// `None` once the client is classified.
    classify_until: Option<Instant>,
    /// Packets the client sent that wait their turn. They wait while the
    /// client is classified and while an open waits.
    backlog: VecDeque<Packet>,
    slow: SlowTracker,
    binding: Option<Binding>,
    opening: Option<Opening>,
    /// Whether the client is a legacy client, whose first packet was not a
    /// control request. A legacy client reads link statuses in the legacy
    /// form. It is `true` until the client is classified.
    legacy: bool,
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
            classify_until: Some(Instant::now() + CLASSIFY),
            backlog: VecDeque::new(),
            slow: SlowTracker::default(),
            binding: None,
            opening: None,
            legacy: true,
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

    /// Classifies the client, then forwards packets between the client and
    /// its mounts until the connection ends. Returns why it ended.
    async fn serve(&mut self) -> Result<(), ClientError> {
        loop {
            if self.stopping() {
                return Err(self.stopped());
            }
            // What the client sent while it was classified or an open waited
            // goes first, in order.
            while self.classify_until.is_none() && self.opening.is_none() {
                let Some(pkt) = self.backlog.pop_front() else {
                    break;
                };
                self.client_packet(pkt).await?;
            }
            let mounted = self
                .binding
                .as_ref()
                .is_some_and(|binding| !binding.incoming.is_empty());
            tokio::select! {
                biased;
                _ = &mut self.alive => return Err(self.stopped()),
                received = self.incoming.next() => {
                    let Some(received) = received else { return Err(ClientError::HungUp); };
                    if let Some(pkt) = self.delivered(received)? {
                        if self.classify_until.is_some() {
                            self.classify(Some(&pkt)).await;
                        }
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
                () = sleep_until_some(self.classify_until) => self.classify(None).await,
            }
        }
    }

    /// Classifies the client by its first packet `first` within [`CLASSIFY`]
    /// of connecting, or by `None` if it sent none. A client whose first
    /// packet is a control request is a control client, and any other is a
    /// legacy client. Packets that do not decode are skipped. A legacy
    /// client opens `auto` at once, and its packets wait until the open is
    /// done. A control client's link is kept alive from then on.
    async fn classify(&mut self, first: Option<&Packet>) {
        self.classify_until = None;
        self.legacy = first.is_none_or(|pkt| pkt.ptype() != PacketType::PROXY_TX);
        match self.legacy {
            true => self.open(
                None,
                bind(
                    self.shared.clone(),
                    self.addr.clone(),
                    auto::AUTO.to_string(),
                    Purpose::Use,
                ),
            ),
            false => self.client.keep_alive(control::PROXY_KEEPALIVE).await,
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

    /// Tells the client at the root that the proxy stopped, and returns
    /// [`ClientError::Stopped`].
    fn stopped(&self) -> ClientError {
        let _ = self.client.try_send(self.status(LinkStatus::Stopped));
        ClientError::Stopped
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
            PacketType::HEARTBEAT if !self.legacy && pkt.route().is_empty() => return Ok(()),
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
        // Only a status the link itself produced is put in this client's
        // form. A device's own type 64 packet is USER traffic, so it passes
        // untouched.
        let pkt = match status {
            Some(status) if self.legacy => Packet::legacy_status(status),
            Some(_) | None => pkt,
        }
        .with_route(routing);
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

    /// Returns a packet carrying `status` in the form this client reads.
    fn status(&self, status: LinkStatus) -> Packet {
        match self.legacy {
            true => Packet::legacy_status(status),
            false => Packet::link(status),
        }
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
    async fn update(&mut self, update: Update) {
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
                self.update(Update::Hello(client)).await;
                let host = control::Introduction::of_this_process(&self.shared.options.name);
                self.answer(id, Ok(host.encode_args()))
            }
            Request::Open { target } => self.request_open(id, target, Purpose::Use),
            Request::Probe { url } => self.request_open(id, url, Purpose::Probe),
            Request::Mount(layout) => {
                let (shared, addr) = (self.shared.clone(), self.addr.clone());
                let binding = attach(shared, addr, layout.to_string(), layout, None, Purpose::Use);
                self.request(id, binding)
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
        self.open(Some(id), binding);
        Ok(())
    }

    /// Begins `binding` the connection, to answer control request `request`,
    /// or for a legacy client with `None`.
    fn open(
        &mut self,
        request: Option<u16>,
        binding: impl Future<Output = Result<Binding, Refusal>> + Send + 'static,
    ) {
        self.opening = Some(Box::pin(async move { (request, binding.await) }));
    }

    /// Answers the open that ended with `outcome`. Once the table shows what
    /// the client opened, answers control request `request`. A legacy client
    /// hears no answer.
    ///
    /// Returns [`ClientError::Refused`] if a legacy client's open is refused,
    /// and [`ClientError::Stopped`] for an open refused while the proxy
    /// stops, which the client hears as the stop.
    async fn opened(
        &mut self,
        request: Option<u16>,
        outcome: Result<Binding, Refusal>,
    ) -> Result<(), ClientError> {
        let outcome = match outcome {
            Ok(binding) => {
                let target = binding.target.clone();
                self.update(Update::Target(target.clone())).await;
                self.binding = Some(binding);
                Ok(target.encode_args())
            }
            Err(_) if self.stopping() => return Err(self.stopped()),
            Err(refusal) => Err(refusal),
        };
        match request {
            Some(id) => self.answer(id, outcome),
            None => outcome.map(drop).map_err(ClientError::Refused),
        }
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
/// other mount takes a lease of its own.
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
            default: watch::channel(default).1,
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
            (2, Ok(DEVICE.encode_args()))
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
        Packet::proxy_request("proxy.open", &target.encode_args(), id).unwrap()
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

    /// A client that sends nothing is a legacy client once [`CLASSIFY`] has
    /// passed, and opens `auto` then.
    #[tokio::test(start_paused = true)]
    async fn a_client_that_sends_nothing_opens_auto_once_it_is_classified() {
        let connected = Instant::now();
        let mut client = connect(Some(Layout::single(DEVICE)), None);

        let device = opened(&mut client.devices).await;

        assert_eq!(device.url, DEVICE);
        assert_took(device.at - connected, CLASSIFY, "classifying");
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

    /// A legacy client hears nothing in the control protocol, signs of life
    /// included, and stays connected however long it is quiet.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_legacy_client_is_kept() {
        let mut client = connect(Some(Layout::single(DEVICE)), None);
        let mut device = opened(&mut client.devices).await;
        let beat = Packet::heartbeat(DeviceRoute::root());
        device.end.send(&beat).await;

        let deadline = Instant::now() + control::PROXY_KEEPALIVE.quiet * 4;
        while let Ok(heard) = timeout_at(deadline, client.end.recv()).await {
            let pkt = heard.expect("the legacy client was kept");
            assert_ne!(pkt.ptype(), PacketType::PROXY_RX, "{:?}", pkt.payload());
        }
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
        assert_eq!(opened, (2, Ok(DEVICE.encode_args())));
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
}

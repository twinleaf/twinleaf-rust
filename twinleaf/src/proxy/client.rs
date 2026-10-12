//! The task that serves each client connection. It answers the client's
//! control requests and routes packets between the client and its mounts.
//! [`Session::run`] runs it.

use super::auto::{self, Choice};
use super::control::{self, Purpose, Refusal, Request};
use super::link::{self, Lease, Link};
use super::server::{Notice, Shared, Update};
use super::worker::{self, Readiness};
use super::Layout;
use crate::proto::rpc::{Method, RpcError};
use crate::proto::{DeviceRoute, PacketType};
use crate::rt::{finish, sleep_until_some, BoxFuture};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use crate::tio::rpc::RpcArgs;
use crate::tio::transport::{self, ReceiveResult, RecvError, CLOSE_TIMEOUT};
use std::collections::VecDeque;
use std::future::{self, Future};
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};
use tokio_util::sync::CancellationToken;

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
    /// A link the client opened closed, for a reason other than
    /// [`ClientError::GaveUp`].
    #[error("a link it opened closed")]
    LinkClosed,
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
    port: link::Port,
}

/// What a client opened. It holds a lease on each mount's link until dropped.
struct Binding {
    target: String,
    mounts: Vec<Mounted>,
    /// The lease on the link of each mount, in the order of `mounts`.
    leases: Vec<Lease>,
}

impl Binding {
    /// Returns why the port of mount `index` closed. It is
    /// [`ClientError::GaveUp`] if the proxy gave up on the device, and
    /// [`ClientError::LinkClosed`] for any other reason.
    fn closed(&self, index: usize) -> ClientError {
        let link = self.leases[index].link();
        match link.readiness() {
            Readiness::Failed(_) => ClientError::GaveUp(link.url().to_string()),
            _ => ClientError::LinkClosed,
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
    incoming: flume::Receiver<ReceiveResult>,
    /// Cancelled once the proxy stops.
    stop: CancellationToken,
    /// When a client that has sent nothing is taken for a legacy client, or
    /// `None` once the client is classified.
    classify_until: Option<Instant>,
    /// Packets the client sent that wait their turn. They wait while the
    /// client is classified and while an open waits.
    backlog: VecDeque<Packet>,
    slow: SlowTracker,
    binding: Option<Binding>,
    opening: Option<Opening>,
    /// Where in the mounts the next wait for a mount's packet starts.
    next_mount: usize,
    /// Whether the client is a legacy client, whose first packet was not a
    /// control request. A legacy client reads link statuses in the legacy
    /// form. It is `true` until the client is classified.
    legacy: bool,
}

impl Session {
    /// Creates the session of client `id`, connected from `peer`. The client
    /// is reached through `client`, whose packets arrive on `incoming`. The
    /// session ends once `stop` is cancelled.
    pub(super) fn new(
        shared: Arc<Shared>,
        id: u64,
        peer: SocketAddr,
        client: transport::Port,
        incoming: flume::Receiver<ReceiveResult>,
        stop: CancellationToken,
    ) -> Session {
        Session {
            shared,
            id,
            addr: peer.to_string(),
            client,
            incoming,
            stop,
            classify_until: Some(Instant::now() + CLASSIFY),
            backlog: VecDeque::new(),
            slow: SlowTracker::default(),
            binding: None,
            opening: None,
            next_mount: 0,
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
            if self.stop.is_cancelled() {
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
            tokio::select! {
                () = self.stop.cancelled() => return Err(self.stopped()),
                received = self.incoming.recv_async() => {
                    let Ok(received) = received else {
                        return Err(ClientError::HungUp);
                    };
                    if let Some(pkt) = self.delivered(received)? {
                        if self.classify_until.is_some() {
                            self.classify(Some(&pkt));
                        }
                        self.backlog.push_back(pkt);
                    }
                }
                (index, pkt) = next_mounted(&mut self.binding, &mut self.next_mount) => {
                    let Some(pkt) = pkt else {
                        let binding = self.binding.as_ref();
                        return Err(binding.expect("a mount is bound").closed(index));
                    };
                    self.mount_packet(pkt, index)?;
                }
                (request, outcome) = finish(&mut self.opening) => {
                    self.opened(request, outcome).await?;
                }
                () = sleep_until_some(self.classify_until) => self.classify(None),
            }
        }
    }

    /// Classifies the client by its first packet `first` within [`CLASSIFY`]
    /// of connecting, or by `None` if it sent none. A client whose first
    /// packet is a control request is a control client, and any other is a
    /// legacy client. Packets that do not decode are skipped. A legacy
    /// client opens `auto` at once, and its packets wait until the open is
    /// done.
    fn classify(&mut self, first: Option<&Packet>) {
        self.classify_until = None;
        self.legacy = first.is_none_or(|pkt| pkt.ptype() != PacketType::PROXY_TX);
        if self.legacy {
            self.open(None, auto::AUTO.to_string(), Purpose::Use);
        }
    }

    /// Returns the packet in what the client's transport delivered, or `None`
    /// for one that did not decode.
    ///
    /// Returns [`ClientError::HungUp`] if the connection closed or failed.
    fn delivered(&self, received: ReceiveResult) -> Result<Option<Packet>, ClientError> {
        match received {
            Ok(pkt) => Ok(Some(pkt)),
            Err(error @ (RecvError::Protocol(_) | RecvError::Text(_))) => {
                log::debug!("Skipped what client {} sent: {error}", self.addr);
                Ok(None)
            }
            Err(RecvError::IO(_) | RecvError::Disconnected) => Err(ClientError::HungUp),
        }
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
        for Mounted { prefix, port } in &binding.mounts {
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
        let binding = self.binding.as_ref().expect("a mount is bound");
        let prefix = binding.mounts[index].prefix;
        let Ok(routing) = prefix.absolute_route(&pkt.route()) else {
            log::warn!(
                "Dropping packet for client {}: route {} exceeds max depth",
                self.addr,
                pkt.route()
            );
            return Ok(());
        };
        // Only a status the link itself produced is put in this client's
        // form. A device's own type 64 packet is USER traffic, so it passes
        // untouched.
        let pkt = match pkt.payload() {
            Payload::ProxyRx(ProxyRx::Link(status)) if self.legacy => Packet::legacy_status(status),
            _ => pkt,
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
    /// shows it.
    async fn update(&mut self, update: Update) {
        let (done, shown) = oneshot::channel();
        let notice = Notice::Update {
            id: self.id,
            update,
            done,
        };
        if self.shared.notices.send(notice).is_ok() {
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
            Request::Stop if self.shared.options.stoppable => {
                self.answer(id, Ok(Vec::new()))?;
                log::info!("Stopped by proxy.stop from client {}", self.addr);
                self.shared.stop.cancel();
                Ok(())
            }
            Request::Stop => {
                let message = format!(
                    "the proxy at {} is hosted by {} and stops only with it",
                    self.shared.endpoint(),
                    self.shared.options.name
                );
                self.answer(id, Err(Refusal::new(RpcError::State, message)))
            }
            #[cfg(test)]
            Request::Panic => panic!("client {} asked its task to panic", self.addr),
        }
    }

    /// Begins the open of `target` for `purpose` that control request `id`
    /// asks for. A connection binds once, so a second open is refused at
    /// once.
    fn request_open(
        &mut self,
        id: u16,
        target: String,
        purpose: Purpose,
    ) -> Result<(), ClientError> {
        if let Some(binding) = &self.binding {
            let message = format!("this connection already opened {}", binding.target);
            return self.answer(id, Err(Refusal::new(RpcError::State, message)));
        }
        self.open(Some(id), target, purpose);
        Ok(())
    }

    /// Begins binding the connection to `target` for `purpose`, to answer
    /// control request `request`, or for a legacy client with `None`.
    fn open(&mut self, request: Option<u16>, target: String, purpose: Purpose) {
        let binding = bind(self.shared.clone(), self.addr.clone(), target, purpose);
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
            Err(_) if self.stop.is_cancelled() => return Err(self.stopped()),
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
    for (mount, lease) in layout.mounts.iter().zip(&leases) {
        let port = lease
            .link()
            .open_port(mount.source)
            .map_err(|reason| Refusal::new(RpcError::Load, reason))?;
        mounts.push(Mounted {
            prefix: mount.prefix,
            port,
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

/// Waits for the next packet from a mount of `binding`. Returns the mount's
/// index with the packet, or with `None` once the mount's port has closed.
/// Never returns while nothing is bound. Each wait starts at the mount after
/// the one that last delivered, at the position `next` keeps.
fn next_mounted<'a>(
    binding: &'a mut Option<Binding>,
    next: &'a mut usize,
) -> impl Future<Output = (usize, Option<Packet>)> + 'a {
    future::poll_fn(move |cx| {
        let Some(binding) = binding.as_mut() else {
            return Poll::Pending;
        };
        let count = binding.mounts.len();
        for offset in 0..count {
            let index = (*next + offset) % count;
            if let Poll::Ready(pkt) = binding.mounts[index].port.poll_recv(cx) {
                *next = index + 1;
                return Poll::Ready((index, pkt));
            }
        }
        Poll::Pending
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::fixtures::{
        assert_took, fakes, opened, options, FakeDevice, Opened, PAUSED_WAIT, WAIT,
    };
    use crate::proxy::link::Links;
    use crate::proxy::{DumpFlags, HostOptions};
    use crate::rt::Supervisor;
    use crate::tio::transport::{FakeEnd, TransportKind, DEFAULT_RX_CHANNEL_SIZE};
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    /// Wait until `link` is no longer `from`, and return what it is.
    fn settled(link: &Link, from: Readiness) -> Readiness {
        let deadline = std::time::Instant::now() + WAIT;
        loop {
            let readiness = link.readiness();
            if readiness != from {
                return readiness;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the link stayed {from:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// A port closes once its link's worker stops. When the proxy gave up on
    /// the device, its client ends saying so, rather than that a link
    /// closed.
    #[test]
    fn a_port_closed_by_a_device_given_up_says_so() {
        let device = FakeDevice::start();
        let links = Links::new(
            &Supervisor::new(),
            Some(Duration::from_millis(200)),
            DumpFlags::default(),
        );
        let binding = Binding {
            target: device.url.clone(),
            mounts: Vec::new(),
            leases: vec![links.acquire_blocking(&device.url).unwrap()],
        };
        assert_eq!(
            settled(binding.leases[0].link(), Readiness::Pending),
            Readiness::Ready
        );
        assert!(matches!(binding.closed(0), ClientError::LinkClosed));

        device.die();

        assert!(matches!(
            settled(binding.leases[0].link(), Readiness::Ready),
            Readiness::Failed(_)
        ));
        let error = binding.closed(0);
        assert!(matches!(error, ClientError::GaveUp(_)));
        assert_eq!(
            error.to_string(),
            format!("the proxy gave up on {}", device.url)
        );
        drop(binding);
        links.close_all_blocking(WAIT);
        assert!(links.wait_finished(WAIT));
    }

    /// The device a test opens.
    const DEVICE: &str = "tcp://device";

    /// A client's session on the test's runtime, over a fake connection, of
    /// a proxy whose devices are fake.
    struct Client {
        /// The client's end of its connection.
        end: FakeEnd,
        /// Each device port the proxy opens.
        devices: mpsc::UnboundedReceiver<Opened>,
    }

    /// Connects a client to a proxy whose default layout is `default`. The
    /// proxy's table takes no updates.
    fn connect(default: Option<Layout>) -> Client {
        let tasks = Supervisor::current();
        let (opens, devices) = fakes(Vec::new());
        let links = Links::on(&tasks, None, DumpFlags::default(), opens);
        let options = HostOptions {
            default,
            ..options()
        };
        let endpoint = "tcp://127.0.0.1:7855".to_string();
        let (shared, server) = Shared::new(options, endpoint, tasks.clone(), links);
        drop(server);
        let (deliver, incoming) = flume::bounded(DEFAULT_RX_CHANNEL_SIZE);
        let (port, end) = transport::Port::fake(TransportKind::Tcp, None, deliver);
        let peer = "127.0.0.1:50000".parse().unwrap();
        let stop = shared.stop.child_token();
        let session = Session::new(shared, 1, peer, port, incoming, stop);
        tasks.spawn("a test client", session.run());
        Client { end, devices }
    }

    /// Returns the request to open `target`, with `id`.
    fn open(target: &str, id: u16) -> Packet {
        Packet::proxy_request("proxy.open", &target.encode_args(), id).unwrap()
    }

    /// Waits for the next packet the client is sent.
    async fn next(end: &mut FakeEnd) -> Packet {
        let next = timeout(PAUSED_WAIT, end.recv()).await;
        next.expect("nothing arrived")
            .expect("the connection closed")
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
        let mut client = connect(Some(Layout::single(DEVICE)));

        let device = opened(&mut client.devices).await;

        assert_eq!(device.url, DEVICE);
        assert_took(device.at - connected, CLASSIFY, "classifying");
    }

    /// An open of a device that sends nothing is refused once
    /// [`link::READY_TIMEOUT`] has passed, and the refusal says so.
    #[tokio::test(start_paused = true)]
    async fn an_open_of_a_silent_device_is_refused_at_the_ready_timeout() {
        let mut client = connect(None);
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
        let mut client = connect(None);
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

//! An app's [`Session`] with its proxy, over one socket.
//!
//! The session carries the open that says what it is for, the RPCs its views
//! make, and the packets its stream pump reads.
//!
//! [`Session::open`] connects to the proxy and introduces this process with
//! `proxy.hello`. It then asks the proxy to open a target with `proxy.open`,
//! or to look a device over with `proxy.probe`, and returns once the proxy
//! has answered. It fails with the proxy's refusal, or when the peer does not
//! answer the hello as a proxy of this build does.
//!
//! One task then owns the socket, on the runtime the open ran on. It sends
//! each call, and a reply completes the call it answers. Every other packet
//! goes to the pump, in the order it arrived. The session ends when the proxy
//! stops, and every call still waiting fails.
//!
//! A socket that closes without a stop, as when the proxy crashes, does not
//! end a session a handshake opened, unless the proxy gave a device up
//! before it closed. The session fails every call in flight, tells the pump
//! the socket is lost, and opens the target the proxy resolved the first
//! time again through the same address. It never starts a proxy for that,
//! so a stopped proxy stays stopped. It ends once [`REOPEN_FOR`] has passed
//! without an open.
//!
//! A device that drops out does not end the session. The proxy retries the
//! device, and the pump sees the link status
//! [`SensorDisconnected`](LinkStatus::SensorDisconnected) and then
//! [`SensorReconnected`](LinkStatus::SensorReconnected). Calls made in
//! between fail. The proxy closes the socket when it gives the device up,
//! after [`FailedToReconnect`](LinkStatus::FailedToReconnect), which ends
//! the session. It also closes the socket when the link to the device ends
//! otherwise, when the session stops reading what it is sent, and when the
//! session is quiet for longer than the keepalive allows. The session takes
//! such a close as a socket that closes without a stop.

use super::runtime::ProxyAddress;
use super::stream::{End, Lane, Queued};
use crate::device::rpc::{CallError, RpcErrorPayload};
use crate::proto::{DeviceRoute, RouteError};
use crate::proxy::{encode, handshake, Layout, Purpose, DEFAULT_LAYOUT, MAX_RPC_TIMEOUT, MOUNT};
use crate::rt::{finish, sleep_until_some, BoxFuture, Supervisor};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use crate::tio::transport::{self, ReceiveResult, TransportEvent};
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// The target an empty target means.
const AUTO: &str = "auto";

/// How long a session whose socket closed without a stop tries to open
/// again before it ends.
const REOPEN_FOR: Duration = Duration::from_secs(30);

/// The RPC timeout a view starts with.
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_millis(3000);
/// The shortest RPC timeout a view may set. The longest is
/// [`MAX_RPC_TIMEOUT`], how long the proxy holds a request.
const MIN_RPC_TIMEOUT: Duration = Duration::from_millis(100);

/// What a call resolves to, the reply's bytes or why there are none.
pub(crate) type RawCallResult = Result<Vec<u8>, CallError>;

/// What a call does with its outcome, run exactly once. It may run on the
/// session's task, and must not block.
pub(crate) type Completion = Box<dyn FnOnce(RawCallResult) + Send>;

/// Where a session writes, its socket, a port on a link of the proxy that
/// hosts it, or a channel a test reads.
enum Socket {
    Transport(transport::Port),
    Port(mpsc::Sender<Packet>),
    #[cfg(test)]
    Channel(crate::chan::Sender<Packet>),
}

impl Socket {
    fn send(&self, packet: Packet) -> Result<(), CallError> {
        match self {
            Socket::Transport(port) => port.try_send(packet).map_err(|error| match error {
                transport::SendError::Full => CallError::RequestNotSubmitted,
                _ => CallError::ResponseLost,
            }),
            Socket::Port(port) => port.try_send(packet).map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => CallError::RequestNotSubmitted,
                mpsc::error::TrySendError::Closed(_) => CallError::ResponseLost,
            }),
            #[cfg(test)]
            Socket::Channel(sent) => sent.send(packet).map_err(|_| CallError::ResponseLost),
        }
    }

    /// Returns what sends `packet` once the socket has room, and resolves to
    /// `false` once the socket has closed. It holds no borrow of the socket.
    fn send_later(&self, packet: Packet) -> BoxFuture<'static, bool> {
        match self {
            Socket::Transport(port) => Box::pin(port.send_async(packet).map(|sent| sent.is_ok())),
            Socket::Port(port) => {
                let port = port.clone();
                Box::pin(async move { port.send(packet).await.is_ok() })
            }
            #[cfg(test)]
            Socket::Channel(sent) => Box::pin(std::future::ready(sent.send(packet).is_ok())),
        }
    }
}

/// A call on its way to the session's task.
struct Call {
    request: Packet,
    timeout: Duration,
    complete: Completion,
}

/// A call waiting for its reply.
struct Pending {
    complete: Completion,
    deadline: Instant,
}

/// What the session's task is asked to do.
enum Command {
    Call(Call),
    /// Feeds the pump through this inlet. A second attach is dropped.
    Attach(Lane<Received>),
    /// Sends a packet that expects no reply the session would complete.
    Send(Packet),
    /// Ends the session, as its owner closed it.
    Close,
}

/// The pump's end of a session. It carries every packet from the proxy but
/// the replies to calls, in the order they arrived, and the loss of each
/// socket. The replies to the pump's control requests and the losses always
/// go in. A gap stands where the session had no room for other packets, and
/// the stop comes last.
pub(crate) type Inlet = crate::chan::Receiver<Queued<Received>>;

/// What the session hands its pump.
#[derive(Debug, Clone)]
pub(crate) enum Received {
    /// A packet from the proxy.
    Packet(Packet),
    /// The socket was lost, and the session opens its target again.
    SocketLost,
}

/// One session with a proxy. Every clone is the same session, and dropping
/// the last ends the session's task, which closes the socket.
#[derive(Clone)]
pub(crate) struct Session {
    commands: mpsc::UnboundedSender<Command>,
    /// The spawner of the session's task, which the tasks of its views run
    /// on too.
    tasks: Supervisor,
}

impl Session {
    /// Connects to `proxy` and asks it to open `target` for `purpose`,
    /// waiting at most `timeout` for its answer. An empty target means
    /// `auto`. Blocks until the proxy answers or the open fails.
    ///
    /// When nothing answers at the machine's own address, finds or starts the
    /// proxy there first. Fails with the proxy's message when it refuses the
    /// open. Fails saying to update the peer when it gives no introduction
    /// within two seconds, as a proxy from an older build gives none.
    pub(crate) fn open(
        proxy: &ProxyAddress,
        target: &str,
        purpose: Purpose,
        timeout: Duration,
    ) -> io::Result<Session> {
        let (proxy, target) = (proxy.clone(), target.to_string());
        Supervisor::new().run("twinleaf session open", async move {
            Session::open_async(&proxy, &target, purpose, timeout).await
        })
    }

    /// Opens a session as [`open`](Self::open) does, without blocking. Must
    /// be awaited on a tokio runtime, which runs the session's tasks.
    pub(crate) async fn open_async(
        proxy: &ProxyAddress,
        target: &str,
        purpose: Purpose,
        timeout: Duration,
    ) -> io::Result<Session> {
        let target = match target {
            "" => AUTO,
            target => target,
        };
        let request = (purpose.method(), encode(&target));
        Session::request_async(proxy, request, &format!("open {target}"), timeout).await
    }

    /// Opens a session that sees `layout`, as [`open_async`](Self::open_async)
    /// opens a target.
    pub(crate) async fn mount_async(
        proxy: &ProxyAddress,
        layout: &Layout,
        timeout: Duration,
    ) -> io::Result<Session> {
        let request = (MOUNT, encode(&layout));
        Session::request_async(proxy, request, &format!("mount {layout}"), timeout).await
    }

    /// Opens a session that holds `layout` as the proxy's default for as long
    /// as it lasts, and opens nothing itself.
    pub(crate) async fn hold_default_async(
        proxy: &ProxyAddress,
        layout: &Layout,
        timeout: Duration,
    ) -> io::Result<Session> {
        let request = (DEFAULT_LAYOUT, encode(&layout));
        let goal = format!("hold {layout} as the default");
        Session::request_async(proxy, request, &goal, timeout).await
    }

    /// Connects to `proxy` and makes the control request `(method, args)`
    /// that binds the connection, which fails as unable to `goal`.
    async fn request_async(
        proxy: &ProxyAddress,
        (method, args): (&'static str, Vec<u8>),
        goal: &str,
        timeout: Duration,
    ) -> io::Result<Session> {
        let tasks = Supervisor::current();
        let (opened, answer) = bind(proxy, (method, &args), goal, timeout, &tasks).await?;
        // An open again opens the target the proxy resolved.
        let resolved = (method == Purpose::Use.method()).then_some(answer);
        let again = ProxyAddress {
            ensure: None,
            ..proxy.clone()
        };
        let reopen = Some((again, method, resolved.unwrap_or(args)));
        Ok(Session::start(opened, reopen, &tasks))
    }

    /// Starts a session inside the proxy over a port on one of its links,
    /// which sends on `sent` and delivers what `received` yields. `peer`
    /// names the link in the log.
    pub(crate) fn over_port(
        sent: mpsc::Sender<Packet>,
        received: impl futures_util::Stream<Item = Packet> + Send + 'static,
        peer: String,
        tasks: &Supervisor,
    ) -> Session {
        let received = received
            .map(|packet| Ok(TransportEvent::Packet(packet)))
            .boxed();
        Session::start((Socket::Port(sent), received, peer), None, tasks)
    }

    /// Starts the session's task on `tasks`, which reads `received` and
    /// writes to `socket`. `peer` names the proxy in the log, such as `the
    /// proxy at tcp://127.0.0.1:7855`. With `reopen`, the proxy and the
    /// control request that open it again, the session outlives a socket
    /// that closes without a stop.
    fn start(
        (socket, received, peer): Opened,
        reopen: Option<(ProxyAddress, &'static str, Vec<u8>)>,
        tasks: &Supervisor,
    ) -> Session {
        let (commands, mailbox) = mpsc::unbounded_channel();
        let worker = Worker {
            socket,
            received,
            peer,
            pending: BTreeMap::new(),
            next_id: 0,
            next_deadline: None,
            inlet: None,
            queued: VecDeque::new(),
            sending: None,
            reopen,
            reopening: None,
            tasks: tasks.clone(),
        };
        tasks.spawn("twinleaf session", worker.run(mailbox));
        Session {
            commands,
            tasks: tasks.clone(),
        }
    }

    /// Returns the spawner of the session's task.
    pub(crate) fn tasks(&self) -> &Supervisor {
        &self.tasks
    }

    /// Sends `packet`, such as a control request whose replies the pump
    /// reads, once the socket has room. It is lost only with the socket.
    pub(crate) fn send(&self, packet: Packet) {
        let _ = self.commands.send(Command::Send(packet));
    }

    /// Calls `name` at `route` with encoded `args`, giving up after
    /// `timeout`. `complete` runs exactly once with the outcome. It runs on
    /// the calling thread if the request does not encode or the session has
    /// ended, and on the session's task for any other outcome.
    pub(crate) fn submit_with(
        &self,
        route: DeviceRoute,
        name: &str,
        args: &[u8],
        timeout: Duration,
        complete: Completion,
    ) {
        let Ok(request) = Packet::rpc_request(name, args, 0, route) else {
            return complete(Err(CallError::RequestNotSubmitted));
        };
        let call = Call {
            request,
            timeout,
            complete,
        };
        if let Err(mpsc::error::SendError(Command::Call(call))) =
            self.commands.send(Command::Call(call))
        {
            (call.complete)(Err(CallError::ResponseLost));
        }
    }

    /// Ends the session for every handle on it, which closes its socket.
    pub(crate) fn close(&self) {
        let _ = self.commands.send(Command::Close);
    }

    /// Attaches the pump, with room for `capacity` packets in its inlet, and
    /// returns the inlet. The first attach is live, and any later one has
    /// already ended.
    pub(crate) fn attach(&self, capacity: usize) -> Inlet {
        let (inlet, packets) = Lane::new(capacity);
        let _ = self.commands.send(Command::Attach(inlet));
        packets
    }
}

/// Connects to `proxy`. A TCP address connects without blocking, and any
/// other opens on a blocking thread of `tasks`. If the machine's own address
/// refuses the connection or times out, finds or starts the proxy there and
/// connects again. Finding or starting the proxy runs on a task of its own,
/// which finishes even if this future is dropped.
async fn connect(
    proxy: &ProxyAddress,
    tasks: &Supervisor,
) -> io::Result<(transport::Port, crate::chan::Receiver<ReceiveResult>)> {
    let attempt = || async {
        let (sender, received) = crate::chan::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
        let port = transport::Port::open(&proxy.url, sender, tasks).await?;
        io::Result::Ok((port, received))
    };
    let connected = match (attempt().await, proxy.ensure) {
        (Err(error), Some(ensure))
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut
            ) =>
        {
            // The proxy is the machine's, not this open's, so it is found or
            // started on a task of its own, which finishes if this open is
            // given up. A proxy it hosts then stays up.
            let ensuring = tasks.spawn("twinleaf proxy start", ensure());
            let ensured = match ensuring.await {
                Ok(Some(ensured)) => ensured,
                // The supervisor logged the panic.
                Ok(None) => Err(io::Error::other("starting the proxy panicked")),
                Err(cancelled) => Err(io::Error::other(cancelled)),
            };
            ensured?;
            attempt().await
        }
        (connected, _) => connected,
    };
    connected.map_err(|error| proxy.unreachable(error))
}

/// A session's socket, what it receives, and the proxy as the log names it.
type Opened = (Socket, BoxStream<'static, ReceiveResult>, String);

/// Connects to `proxy` and makes the control request `(method, args)`, which
/// fails as unable to `goal` if the proxy does not answer within `timeout`.
/// Returns the session's socket and the proxy's answer.
async fn bind(
    proxy: &ProxyAddress,
    (method, args): (&str, &[u8]),
    goal: &str,
    timeout: Duration,
    tasks: &Supervisor,
) -> io::Result<(Opened, Vec<u8>)> {
    let (port, received) = connect(proxy, tasks).await?;
    let url = &proxy.url;
    let (host, opened) = handshake(&port, &received, url, method, args, goal, timeout).await?;
    let peer = match host {
        Some(host) => format!("the proxy at {url}, hosted by {host},"),
        None => format!("the proxy at {url}"),
    };
    log::debug!("{peer} opened {}", String::from_utf8_lossy(&opened));
    let received = received.into_stream().boxed();
    Ok(((Socket::Transport(port), received, peer), opened))
}

/// Opens a session through `proxy` again with the control request
/// `(method, args)`, for at most [`REOPEN_FOR`]. The wait between two tries
/// doubles from 250 ms up to 1 s, so a proxy that is back is tried within
/// 1.25 s, and each is made up
/// to a quarter longer or shorter at random, so the clients of one proxy
/// do not all try at once. The new socket's task runs on `tasks`. Returns
/// `None` if no try succeeded.
async fn open_again(
    (proxy, method, args): (ProxyAddress, &'static str, Vec<u8>),
    tasks: Supervisor,
) -> Option<Opened> {
    let tries = async {
        let mut wait = Duration::from_millis(250);
        loop {
            match bind(&proxy, (method, &args), "open it again", REOPEN_FOR, &tasks).await {
                Ok((opened, _)) => return opened,
                Err(error) => log::debug!("could not open the session again: {error}"),
            }
            tokio::time::sleep(jittered(wait)).await;
            wait = (wait * 2).min(Duration::from_secs(1));
        }
    };
    tokio::time::timeout(REOPEN_FOR, tries).await.ok()
}

/// Returns `wait` made up to a quarter longer or shorter at random.
fn jittered(wait: Duration) -> Duration {
    use std::hash::BuildHasher;
    // The standard library's random keys, as std has no generator of its own.
    let random = std::collections::hash_map::RandomState::new().hash_one(());
    wait.mul_f64(0.75 + random as f64 / u64::MAX as f64 / 2.0)
}

/// Why a session's task ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// The proxy stopped.
    Stopped,
    /// The socket closed without a stop.
    SocketClosed,
    /// Every handle on the session is gone.
    HandlesGone,
    /// Its owner closed it.
    Closed,
}

/// The session's task. It owns the socket, sends calls and completes them,
/// feeds the pump, and fails the calls that run out of time.
struct Worker {
    socket: Socket,
    received: BoxStream<'static, ReceiveResult>,
    /// The proxy, as the log names it.
    peer: String,
    /// The calls waiting for a reply, by the id each went out with.
    pending: BTreeMap<u16, Pending>,
    next_id: u16,
    /// When the earliest pending call runs out of time, as last looked up.
    next_deadline: Option<Instant>,
    /// The pump's inlet, once it attached. Until then the session drops what
    /// it would queue.
    inlet: Option<Lane<Received>>,
    /// The packets the pump sent, in order, which wait for room in the
    /// socket and for the socket to be open, and the send of the first of
    /// them under way.
    queued: VecDeque<Packet>,
    sending: Option<BoxFuture<'static, bool>>,
    /// The proxy and the control request that open the session again once
    /// its socket closes without a stop, or `None` for a session that ends
    /// then.
    reopen: Option<(ProxyAddress, &'static str, Vec<u8>)>,
    /// The open in progress after a socket closed.
    reopening: Option<BoxFuture<'static, Option<Opened>>>,
    /// The spawner of the session's task, which a socket opened again runs
    /// its task on too.
    tasks: Supervisor,
}

impl Worker {
    /// Runs the session, taking `commands` ahead of the socket, until its
    /// socket closes, the proxy stops or every handle is gone.
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        let ending = loop {
            if self.sending.is_none() && self.reopening.is_none() {
                self.sending = self
                    .queued
                    .pop_front()
                    .map(|packet| self.socket.send_later(packet));
            }
            tokio::select! {
                biased;
                command = commands.recv() => match command {
                    Some(Command::Call(call)) => self.call(call),
                    Some(Command::Attach(inlet)) => {
                        self.inlet.get_or_insert(inlet);
                    }
                    Some(Command::Send(packet)) => self.queued.push_back(packet),
                    Some(Command::Close) => break Ending::Closed,
                    None => break Ending::HandlesGone,
                },
                received = self.received.next(), if self.reopening.is_none() => {
                    let ending = received.map_or(Some(Ending::SocketClosed), |received| {
                        self.delivered(received)
                    });
                    match ending {
                        Some(Ending::SocketClosed) if self.lose() => {}
                        Some(ending) => break ending,
                        None => {}
                    }
                }
                opened = finish(&mut self.reopening) => {
                    let Some(opened) = opened else { break Ending::SocketClosed };
                    (self.socket, self.received, self.peer) = opened;
                    log::info!("the session with {} is open again", self.peer);
                }
                sent = finish(&mut self.sending) => if !sent {
                    log::debug!("could not send to {}, which closed", self.peer);
                },
                () = sleep_until_some(self.next_deadline) => self.expire(),
            }
        };
        match ending {
            // A port inside the proxy closes with its link.
            Ending::SocketClosed if matches!(self.socket, Socket::Port(_)) => {
                log::debug!("{} closed", self.peer)
            }
            Ending::SocketClosed => log::warn!("{} closed the connection", self.peer),
            ending => log::debug!("the session with {} ended: {ending:?}", self.peer),
        }
        let end = match ending {
            Ending::Stopped => Some(End::Stopped),
            Ending::Closed => Some(End::Closed),
            Ending::SocketClosed | Ending::HandlesGone => None,
        };
        if let (Some(inlet), Some(end)) = (self.inlet.take(), end) {
            inlet.end(end);
        }
        // A call sent before the mailbox closed is failed here, and a later
        // one is failed by its caller.
        commands.close();
        while let Ok(command) = commands.try_recv() {
            match command {
                Command::Call(call) => (call.complete)(Err(CallError::ResponseLost)),
                Command::Attach(..) | Command::Send(_) | Command::Close => {}
            }
        }
        for pending in std::mem::take(&mut self.pending).into_values() {
            (pending.complete)(Err(CallError::ResponseLost));
        }
    }

    /// Fails every call in flight, tells the pump the socket is lost, and
    /// opens the session again, unless it is one that ends then. Returns
    /// whether it opens again.
    fn lose(&mut self) -> bool {
        let Some(reopen) = self.reopen.clone() else {
            return false;
        };
        log::warn!("{} closed the connection, so it is opened again", self.peer);
        for pending in std::mem::take(&mut self.pending).into_values() {
            (pending.complete)(Err(CallError::ResponseLost));
        }
        if let Some(inlet) = &mut self.inlet {
            inlet.send_always(Received::SocketLost);
        }
        self.reopening = Some(Box::pin(open_again(reopen, self.tasks.clone())));
        true
    }

    /// Sends `call` with an id no pending call has, and keeps it until its
    /// reply or its deadline. Fails it if it cannot be sent.
    fn call(&mut self, call: Call) {
        let Some(id) = self.free_id() else {
            return (call.complete)(Err(CallError::RequestNotSubmitted));
        };
        if let Err(error) = self.socket.send(call.request.with_rpc_id(id)) {
            return (call.complete)(Err(error));
        }
        let deadline = Instant::now() + call.timeout;
        let complete = call.complete;
        self.pending.insert(id, Pending { complete, deadline });
        self.next_deadline = Some(
            self.next_deadline
                .map_or(deadline, |next| next.min(deadline)),
        );
    }

    /// Returns the next id from the counter that no pending call has, or
    /// `None` if every id is taken.
    fn free_id(&mut self) -> Option<u16> {
        for _ in 0..=u16::MAX {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if !self.pending.contains_key(&id) {
                return Some(id);
            }
        }
        None
    }

    /// Handles one transport event, leaving queued calls and deadlines a turn.
    fn delivered(&mut self, received: ReceiveResult) -> Option<Ending> {
        match received {
            Ok(TransportEvent::Packet(packet)) => {
                if !self.packet(packet) {
                    return Some(Ending::Stopped);
                }
            }
            Ok(TransportEvent::Text(text)) => log::debug!("the proxy sent text: {text}"),
            Ok(TransportEvent::RejectedFrame(error)) => {
                log::debug!("the proxy sent a packet that did not decode: {error}");
            }
            Err(_) => return Some(Ending::SocketClosed),
        }
        None
    }

    /// Takes one packet from the proxy. Returns `false` if the proxy
    /// stopped.
    fn packet(&mut self, packet: Packet) -> bool {
        let answer = match packet.payload() {
            Payload::RpcReply(reply) => Some((reply.req_id.value(), Ok(reply.value.to_vec()))),
            Payload::RpcError(error) => Some((
                error.req_id.value(),
                Err(CallError::DeviceError(RpcErrorPayload::from_wire(error))),
            )),
            _ => None,
        };
        if let Some((id, outcome)) = answer {
            match self.pending.remove(&id) {
                Some(pending) => (pending.complete)(outcome),
                None => log::debug!("dropping a reply to {id}, which no call waits for"),
            }
            return true;
        }
        match packet.payload() {
            Payload::ProxyRx(ProxyRx::Alive) => return true,
            Payload::ProxyRx(ProxyRx::Reply(_) | ProxyRx::Error(_)) => {
                if let Some(inlet) = &mut self.inlet {
                    inlet.send_always(Received::Packet(packet));
                }
                return true;
            }
            _ => {}
        }
        let stopped =
            packet.route().is_empty() && packet.link_status() == Some(LinkStatus::Stopped);
        // The proxy closes the socket once it gives a device up, and the
        // session is not opened again then.
        if packet.link_status() == Some(LinkStatus::FailedToReconnect) {
            self.reopen = None;
        }
        self.forward(packet);
        !stopped
    }

    /// Queues a packet for the pump, once the pump has attached, after a gap
    /// for those the inlet had no room for.
    fn forward(&mut self, packet: Packet) {
        if let Some(inlet) = &mut self.inlet {
            inlet.send(Received::Packet(packet));
        }
    }

    /// Fails every call whose deadline has passed.
    fn expire(&mut self) {
        let now = Instant::now();
        let expired: Vec<u16> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(pending) = self.pending.remove(&id) {
                (pending.complete)(Err(CallError::Timeout));
            }
        }
        self.next_deadline = self.pending.values().map(|pending| pending.deadline).min();
    }
}

/// A view's share of a session. It holds the subtree its RPCs may reach, how
/// deep, and how long each may take.
#[derive(Clone)]
pub(crate) struct Handle {
    session: Session,
    scope: DeviceRoute,
    depth: usize,
    timeout: Duration,
}

impl Handle {
    /// Returns a handle on all of `session`, reaching every route at the
    /// default timeout.
    pub(crate) fn root(session: Session) -> Handle {
        Handle {
            session,
            scope: DeviceRoute::root(),
            depth: DeviceRoute::MAX_HOPS,
            timeout: DEFAULT_RPC_TIMEOUT,
        }
    }

    /// Returns the root route of this handle's subtree.
    pub(crate) fn scope(&self) -> DeviceRoute {
        self.scope
    }

    /// Returns how many levels below its root this handle reaches.
    pub(crate) fn depth(&self) -> usize {
        self.depth
    }

    /// Returns the session.
    pub(crate) fn session(&self) -> &Session {
        &self.session
    }

    /// Submits a call and returns a channel that receives its outcome. See
    /// [`submit_with`](Self::submit_with) to run something else with it.
    pub(crate) fn submit(
        &self,
        route: DeviceRoute,
        name: &str,
        args: &[u8],
    ) -> crate::chan::Receiver<RawCallResult> {
        let (resolve, reply) = crate::chan::bounded(1);
        self.submit_with(
            route,
            name,
            args,
            Box::new(move |outcome| {
                let _ = resolve.send(outcome);
            }),
        );
        reply
    }

    /// Submits a call. `complete` runs exactly once with the outcome. It runs
    /// on the calling thread if the route lies outside this handle, the
    /// request does not encode or the session has ended, and on the
    /// session's task for any other outcome.
    pub(crate) fn submit_with(
        &self,
        route: DeviceRoute,
        name: &str,
        args: &[u8],
        complete: Completion,
    ) {
        let relative = match self.scope.relative_route(&route) {
            Ok(relative) => relative,
            Err(error) => return complete(Err(CallError::InvalidRoute(error))),
        };
        if relative.len() > self.depth {
            return complete(Err(CallError::InvalidRoute(RouteError::OutsideSubtree)));
        }
        self.session
            .submit_with(route, name, args, self.timeout, complete);
    }

    /// Returns the same handle narrowed to exactly `scope`. Fails with
    /// [`RouteError::OutsideSubtree`] if this handle does not cover `scope`.
    pub(crate) fn scoped(&self, scope: DeviceRoute) -> Result<Handle, RouteError> {
        Ok(Handle {
            depth: 0,
            ..self.subtree(scope)?
        })
    }

    /// Returns the same handle narrowed to the subtree at `scope`, keeping
    /// the depth that remains below it. Fails with
    /// [`RouteError::OutsideSubtree`] if this handle does not cover `scope`.
    pub(crate) fn subtree(&self, scope: DeviceRoute) -> Result<Handle, RouteError> {
        let below = self.scope.relative_route(&scope)?.len();
        let Some(depth) = self.depth.checked_sub(below) else {
            return Err(RouteError::OutsideSubtree);
        };
        Ok(Handle {
            session: self.session.clone(),
            scope,
            depth,
            timeout: self.timeout,
        })
    }

    /// Returns the same handle reaching no deeper than `depth`. A `depth`
    /// beyond its own has no effect.
    pub(crate) fn to_depth(&self, depth: usize) -> Handle {
        Handle {
            depth: depth.min(self.depth),
            ..self.clone()
        }
    }

    /// Returns the same handle with an RPC timeout of `timeout`, clamped to
    /// between [`MIN_RPC_TIMEOUT`] and [`MAX_RPC_TIMEOUT`].
    pub(crate) fn with_timeout(&self, timeout: Duration) -> Handle {
        Handle {
            timeout: timeout.clamp(MIN_RPC_TIMEOUT, MAX_RPC_TIMEOUT),
            ..self.clone()
        }
    }
}

/// The far end of a session whose socket is a pair of channels.
#[cfg(test)]
pub(crate) struct TestProxy {
    /// What the session wrote, in order.
    pub(crate) sent: crate::chan::Receiver<Packet>,
    /// Hands the session packets as its socket would. Dropping it closes
    /// the socket.
    pub(crate) deliver: crate::chan::Sender<ReceiveResult>,
}

#[cfg(test)]
impl TestProxy {
    /// Hand the session `packet`, as from the proxy.
    pub(crate) fn send(&self, packet: Packet) {
        self.deliver
            .send(Ok(TransportEvent::Packet(packet)))
            .expect("the session reads its socket");
    }

    /// The next call the session wrote, as its route, its method name, its
    /// arguments, and its id.
    pub(crate) fn call(&self) -> (DeviceRoute, String, Vec<u8>, u16) {
        let packet = self
            .sent
            .recv_timeout(Duration::from_secs(5))
            .expect("the session writes a call");
        let Payload::RpcRequest(request) = packet.payload() else {
            panic!("expected an RPC request, got {:?}", packet.payload());
        };
        let crate::proto::rpc::Method::ByName(name) = request.method else {
            panic!("a session calls by name");
        };
        (
            packet.route(),
            String::from_utf8_lossy(name).into_owned(),
            request.args.to_vec(),
            request.id.value(),
        )
    }

    /// Answer call `id` at `route` with `value`.
    pub(crate) fn reply(&self, id: u16, value: &[u8], route: DeviceRoute) {
        self.send(Packet::rpc_reply(id, value, route).expect("the reply fits"));
    }

    /// Refuse call `id` at `route` with `error`.
    pub(crate) fn refuse(&self, id: u16, error: crate::proto::rpc::RpcError, route: DeviceRoute) {
        self.send(Packet::rpc_error(id, error, route));
    }
}

#[cfg(test)]
impl Session {
    /// A session whose socket is a pair of channels, with no handshake. Its
    /// task runs on the crate's runtime.
    pub(crate) fn test_pair() -> (Session, TestProxy) {
        Session::test_pair_on(&Supervisor::new())
    }

    /// [`test_pair`](Self::test_pair) with the session's task on `tasks`.
    pub(crate) fn test_pair_on(tasks: &Supervisor) -> (Session, TestProxy) {
        Session::test_pair_with(tasks, None)
    }

    /// [`test_pair_on`](Self::test_pair_on) for a session that opens again
    /// once its socket closes without a stop, through an address that never
    /// answers. The pump hears each such loss.
    pub(crate) fn test_pair_reopening(tasks: &Supervisor) -> (Session, TestProxy) {
        let nowhere = ProxyAddress {
            url: crate::proxy::tests::fixtures::UNREACHABLE.to_string(),
            ensure: None,
        };
        let reopen = (nowhere, Purpose::Use.method(), encode(&"auto"));
        Session::test_pair_with(tasks, Some(reopen))
    }

    /// A session whose socket is a pair of channels. With `reopen`, it opens
    /// again as [`start`](Self::start) says.
    fn test_pair_with(
        tasks: &Supervisor,
        reopen: Option<(ProxyAddress, &'static str, Vec<u8>)>,
    ) -> (Session, TestProxy) {
        let (sender, sent) = crate::chan::unbounded();
        let (deliver, received) = crate::chan::unbounded();
        let peer = "the test's proxy".to_string();
        let received = received.into_stream().boxed();
        let session = Session::start((Socket::Channel(sender), received, peer), reopen, tasks);
        (session, TestProxy { sent, deliver })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::tests::fixtures::{self, FakeDevice};
    use crate::proto::rpc::{Method, RpcError};
    use crate::proto::PacketType;
    use crate::proxy::tests::fixtures::assert_took;
    use crate::proxy::CLIENT_KEEPALIVE;
    use crate::proxy::{client_name, decode, Introduction};
    use crate::rt::BoxFuture;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Instant;

    /// The longest any step of a test waits.
    const WAIT: Duration = Duration::from_secs(5);

    /// What a fake proxy is asked to open. Nothing is ever dialed there.
    const TARGET: &str = "tcp://192.0.2.1:9";

    /// A proxy that a test drives packet by packet.
    struct FakeProxy {
        listener: TcpListener,
    }

    impl FakeProxy {
        fn start() -> FakeProxy {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            FakeProxy { listener }
        }

        fn url(&self) -> String {
            format!("tcp://{}", self.listener.local_addr().unwrap())
        }

        /// The proxy, named so that it is never started.
        fn address(&self) -> ProxyAddress {
            ProxyAddress {
                url: self.url(),
                ensure: None,
            }
        }

        /// The next connection, within [`WAIT`].
        fn accept(&self) -> ProxyEnd {
            let deadline = Instant::now() + WAIT;
            loop {
                match self.listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        return ProxyEnd {
                            stream,
                            received: Vec::new(),
                        };
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "no connection within {WAIT:?}");
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accepting failed: {error}"),
                }
            }
        }

        /// Whether another connection arrives within `within`.
        fn arrives(&self, within: Duration) -> bool {
            let deadline = Instant::now() + within;
            while Instant::now() < deadline {
                if self.listener.accept().is_ok() {
                    return true;
                }
                thread::sleep(Duration::from_millis(10));
            }
            false
        }

        /// Open a session through this proxy on another thread, for the
        /// test to answer.
        fn open(&self, target: &str, purpose: Purpose) -> thread::JoinHandle<io::Result<Session>> {
            let address = self.address();
            let target = target.to_string();
            thread::spawn(move || Session::open(&address, &target, purpose, WAIT))
        }
    }

    /// The proxy's end of one session's socket.
    struct ProxyEnd {
        stream: TcpStream,
        received: Vec<u8>,
    }

    impl ProxyEnd {
        /// The next packet the session sends within `within`, if any.
        fn next_within(&mut self, within: Duration) -> Option<Packet> {
            let deadline = Instant::now() + within;
            loop {
                match Packet::from_slice_prefix(&self.received) {
                    Ok((packet, used)) => {
                        self.received.drain(..used);
                        return Some(packet);
                    }
                    Err(crate::tio::packet::DecodeError::NeedMore) => {}
                    Err(error) => panic!("the session sent an undecodable packet: {error}"),
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return None;
                }
                self.stream.set_read_timeout(Some(remaining)).unwrap();
                let mut chunk = [0; 512];
                match self.stream.read(&mut chunk) {
                    Ok(0) => return None,
                    Ok(n) => self.received.extend_from_slice(&chunk[..n]),
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) =>
                    {
                        return None
                    }
                    Err(_) => return None,
                }
            }
        }

        fn expect(&mut self) -> Packet {
            self.next_within(WAIT)
                .expect("the session sent nothing more")
        }

        /// The control request the session sends next, as its method, its
        /// arguments, and its id.
        fn expect_control(&mut self) -> (String, Vec<u8>, u16) {
            control(&self.expect())
        }

        /// Expect `proxy.hello`, then `method` naming `target`, and answer
        /// neither. Returns the id of each.
        fn expect_handshake(&mut self, method: &str, target: &str) -> (u16, u16) {
            let (name, args, hello) = self.expect_control();
            assert_eq!(name, "proxy.hello");
            assert_eq!(
                decode::<Introduction>(&args).unwrap(),
                Introduction::of_this_process(client_name())
            );
            let (name, args, open) = self.expect_control();
            assert_eq!(name, method);
            assert_eq!(decode::<String>(&args).unwrap(), target);
            (hello, open)
        }

        /// Expect the handshake opening `target`, and accept it.
        fn accept_open(&mut self, target: &str) {
            let (hello, open) = self.expect_handshake("proxy.open", target);
            self.send(&Packet::proxy_reply(hello, &encode(&fake_host())).unwrap());
            self.send(&Packet::proxy_reply(open, &encode(&target)).unwrap());
        }

        /// The next call the session sends, as its method name and its id.
        fn expect_call(&mut self) -> (String, u16) {
            let packet = loop {
                let packet = self.expect();
                if !matches!(packet.payload(), Payload::Heartbeat(_)) {
                    break packet;
                }
            };
            let Payload::RpcRequest(request) = packet.payload() else {
                panic!("expected an RPC request, got {:?}", packet.payload());
            };
            let Method::ByName(name) = request.method else {
                panic!("a session calls by name");
            };
            (
                String::from_utf8_lossy(name).into_owned(),
                request.id.value(),
            )
        }

        fn send(&mut self, packet: &Packet) {
            self.stream.write_all(packet.as_bytes()).unwrap();
        }
    }

    /// The control request `packet` carries, as its method, its arguments,
    /// and its id.
    fn control(packet: &Packet) -> (String, Vec<u8>, u16) {
        let Payload::ProxyTx(request) = packet.payload() else {
            panic!("expected a control request, got {:?}", packet.payload());
        };
        let Method::ByName(name) = request.method else {
            panic!("a control request names its method");
        };
        (
            String::from_utf8_lossy(name).into_owned(),
            request.args.to_vec(),
            request.id.value(),
        )
    }

    /// The introduction a fake proxy answers `proxy.hello` with.
    fn fake_host() -> Introduction {
        Introduction::of_this_process("a fake proxy")
    }

    /// A session through a fake proxy that opened [`TARGET`], and the
    /// proxy's end of it.
    fn opened(proxy: &FakeProxy) -> (Session, ProxyEnd) {
        let opening = proxy.open(TARGET, Purpose::Use);
        let mut end = proxy.accept();
        end.accept_open(TARGET);
        (opening.join().unwrap().unwrap(), end)
    }

    /// The next packet on `inlet`, within [`WAIT`].
    fn next(inlet: &Inlet) -> Result<Packet, crate::chan::RecvTimeoutError> {
        match inlet.recv_timeout(WAIT)? {
            Queued::Item(Received::Packet(packet)) => Ok(packet),
            Queued::Item(Received::SocketLost) => panic!("the session lost its socket"),
            Queued::Gap(dropped) => panic!("the session dropped {dropped} packets"),
            Queued::Ended(end) => panic!("the session ended its inlet: {end:?}"),
        }
    }

    #[test]
    fn open_says_hello_then_opens_its_target_and_returns_once_answered() {
        let proxy = FakeProxy::start();
        let opening = proxy.open(TARGET, Purpose::Use);
        let mut end = proxy.accept();

        let (hello, open) = end.expect_handshake("proxy.open", TARGET);
        end.send(&Packet::proxy_reply(hello, &encode(&fake_host())).unwrap());
        assert!(
            end.next_within(Duration::from_millis(200)).is_none(),
            "the session sent more before the open was answered"
        );
        assert!(
            !opening.is_finished(),
            "the open returned before its answer"
        );
        end.send(&Packet::proxy_reply(open, &encode(&TARGET)).unwrap());

        let root = Handle::root(opening.join().unwrap().unwrap());
        let reply = root.submit(DeviceRoute::root(), "dev.name", &[]);
        let (name, id) = end.expect_call();
        assert_eq!(name, "dev.name");
        end.send(&Packet::rpc_reply(id, b"FAKE", DeviceRoute::root()).unwrap());
        assert_eq!(reply.recv_timeout(WAIT).unwrap().unwrap(), b"FAKE");
    }

    /// The proxy's own words are the error, such as the candidates an
    /// ambiguous `auto` could mean.
    #[test]
    fn a_refused_open_fails_with_the_proxys_message() {
        let proxy = FakeProxy::start();
        let opening = proxy.open("", Purpose::Use);
        let mut end = proxy.accept();

        let (hello, open) = end.expect_handshake("proxy.open", "auto");
        end.send(&Packet::proxy_reply(hello, &encode(&fake_host())).unwrap());
        let refusal = Packet::proxy_error(open, RpcError::Invalid, "auto finds two devices");
        end.send(&refusal.unwrap());

        let error = opening.join().unwrap().err().expect("the open was refused");
        assert_eq!(error.to_string(), "auto finds two devices");
    }

    /// A peer that answers the hello with no introduction is a proxy from an
    /// older build, and the open fails before the peer answers it.
    #[test]
    fn an_open_through_a_proxy_that_gives_no_introduction_fails() {
        let proxy = FakeProxy::start();
        let opening = proxy.open(TARGET, Purpose::Use);
        let mut end = proxy.accept();

        let (hello, _) = end.expect_handshake("proxy.open", TARGET);
        end.send(&Packet::proxy_reply(hello, &encode(&0u8)).unwrap());

        let error = opening.join().unwrap().err().expect("the open failed");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let message = error.to_string();
        assert!(message.contains("older build"), "{message}");
    }

    #[test]
    fn a_probe_asks_the_proxy_to_probe() {
        let proxy = FakeProxy::start();
        let opening = proxy.open(TARGET, Purpose::Probe);
        let mut end = proxy.accept();

        let (hello, probe) = end.expect_handshake("proxy.probe", TARGET);
        end.send(&Packet::proxy_reply(hello, &encode(&fake_host())).unwrap());
        end.send(&Packet::proxy_reply(probe, &encode(&TARGET)).unwrap());

        assert!(opening.join().unwrap().is_ok());
    }

    /// A stop while the open waits is the proxy going away, not a refusal.
    #[test]
    fn a_stop_while_the_open_waits_fails_the_open() {
        let proxy = FakeProxy::start();
        let opening = proxy.open(TARGET, Purpose::Use);
        let mut end = proxy.accept();

        let (hello, _) = end.expect_handshake("proxy.open", TARGET);
        end.send(&Packet::proxy_reply(hello, &encode(&fake_host())).unwrap());
        end.send(&Packet::link(LinkStatus::Stopped));

        let error = opening.join().unwrap().err().expect("the open failed");
        assert!(error.to_string().contains("stopped"), "{error}");
    }

    #[test]
    fn concurrent_rpcs_complete_out_of_order() {
        let proxy = FakeProxy::start();
        let (session, mut end) = opened(&proxy);
        let root = Handle::root(session);
        let first_route: DeviceRoute = "/1".parse().unwrap();
        let second_route: DeviceRoute = "/2".parse().unwrap();

        let first = root.submit(first_route, "dev.name", &[]);
        let second = root.submit(second_route, "dev.desc", &[]);
        let calls = [end.expect_call(), end.expect_call()];
        assert_ne!(calls[0].1, calls[1].1, "each call has an id of its own");
        for ((_, id), route) in calls.iter().zip([first_route, second_route]).rev() {
            let value = format!("{route}");
            end.send(&Packet::rpc_reply(*id, value.as_bytes(), route).unwrap());
        }

        assert_eq!(first.recv_timeout(WAIT).unwrap().unwrap(), b"/1");
        assert_eq!(second.recv_timeout(WAIT).unwrap().unwrap(), b"/2");
    }

    /// A call unanswered within its view's timeout fails, and the reply that
    /// comes late finds nothing to complete.
    #[test]
    fn an_rpc_unanswered_in_time_times_out() {
        let proxy = FakeProxy::start();
        let (session, mut end) = opened(&proxy);
        let root = Handle::root(session).with_timeout(Duration::from_millis(150));

        let started = Instant::now();
        let reply = root.submit(DeviceRoute::root(), "dev.name", &[]);
        let (_, id) = end.expect_call();

        assert!(matches!(
            reply.recv_timeout(WAIT),
            Ok(Err(CallError::Timeout))
        ));
        assert!(started.elapsed() >= Duration::from_millis(150));
        end.send(&Packet::rpc_reply(id, b"late", DeviceRoute::root()).unwrap());
        let again = root.submit(DeviceRoute::root(), "dev.name", &[]);
        let (_, id) = end.expect_call();
        end.send(&Packet::rpc_reply(id, b"FAKE", DeviceRoute::root()).unwrap());
        assert_eq!(again.recv_timeout(WAIT).unwrap().unwrap(), b"FAKE");
    }

    /// A device's refusal reaches the call as the device's error.
    #[test]
    fn an_rpc_error_completes_its_call() {
        let (session, proxy) = Session::test_pair();
        let root = Handle::root(session);
        let reply = root.submit(DeviceRoute::root(), "dev.nothing", &[]);
        let (route, name, _, id) = proxy.call();
        assert_eq!(name, "dev.nothing");

        proxy.refuse(id, RpcError::NotFound, route);

        assert!(matches!(
            reply.recv_timeout(WAIT),
            Ok(Err(CallError::DeviceError(RpcErrorPayload {
                error: RpcError::NotFound,
                ..
            })))
        ));
    }

    /// A stop ends the session, and fails every call still waiting and
    /// every call made after it.
    #[test]
    fn a_stop_fails_every_call_waiting_and_every_later_one() {
        let proxy = FakeProxy::start();
        let (session, mut end) = opened(&proxy);
        let root = Handle::root(session);
        let waiting = root.submit(DeviceRoute::root(), "dev.name", &[]);
        end.expect_call();

        end.send(&Packet::link(LinkStatus::Stopped));

        assert!(matches!(
            waiting.recv_timeout(WAIT),
            Ok(Err(CallError::ResponseLost))
        ));
        assert!(matches!(
            root.submit(DeviceRoute::root(), "dev.name", &[])
                .recv_timeout(WAIT),
            Ok(Err(CallError::ResponseLost))
        ));
    }

    /// A packet the pump sends, such as a watch, waits for room in a full
    /// socket rather than being dropped.
    #[tokio::test]
    async fn a_packet_sent_to_a_full_socket_waits_for_room() {
        let tasks = Supervisor::current();
        let (sent, mut written) = mpsc::channel(1);
        let (_deliver, received) = mpsc::channel(1);
        let session = Session::over_port(
            sent.clone(),
            tokio_stream::wrappers::ReceiverStream::new(received),
            "a full port".to_string(),
            &tasks,
        );
        sent.send(Packet::heartbeat(DeviceRoute::root()))
            .await
            .unwrap();

        session.send(Packet::heartbeat_session(9, DeviceRoute::root()));
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            written.recv().await.is_some(),
            "the packet that filled the port"
        );
        let waited = tokio::time::timeout(WAIT, written.recv()).await.unwrap();
        let waited = waited.expect("the packet sent");
        let Payload::Heartbeat(beat) = waited.payload() else {
            panic!("expected the heartbeat, got {:?}", waited.payload());
        };
        assert_eq!(beat.session(), Some(crate::proto::SessionId::new(9)));
    }

    /// A packet waiting for room in a full socket leaves the session
    /// responsive, so a close ends it at once.
    #[tokio::test]
    async fn a_packet_waiting_for_room_leaves_the_session_responsive() {
        let tasks = Supervisor::current();
        let (sent, _written) = mpsc::channel(1);
        let (_deliver, received) = mpsc::channel(1);
        let session = Session::over_port(
            sent.clone(),
            tokio_stream::wrappers::ReceiverStream::new(received),
            "a full port".to_string(),
            &tasks,
        );
        sent.send(Packet::heartbeat(DeviceRoute::root()))
            .await
            .unwrap();
        let inlet = session.attach(64);
        session.send(Packet::heartbeat(DeviceRoute::root()));

        session.close();

        let ended = tokio::time::timeout(WAIT, inlet.recv_async()).await;
        assert!(
            matches!(ended, Ok(Ok(Queued::Ended(End::Closed)))),
            "{ended:?}"
        );
    }

    /// A socket that drops without a stop fails the call in flight, and the
    /// session opens the target the proxy resolved the first time again on
    /// a new socket, not `auto`. The pump hears that the socket was lost.
    #[test]
    fn a_dropped_socket_fails_pending_calls_and_opens_the_resolved_target_again() {
        let proxy = FakeProxy::start();
        let opening = proxy.open("auto", Purpose::Use);
        let mut end = proxy.accept();
        let (hello, open) = end.expect_handshake("proxy.open", "auto");
        end.send(&Packet::proxy_reply(hello, &encode(&fake_host())).unwrap());
        end.send(&Packet::proxy_reply(open, &encode(&TARGET)).unwrap());
        let session = opening.join().unwrap().unwrap();
        let inlet = session.attach(64);
        let root = Handle::root(session);
        let waiting = root.submit(DeviceRoute::root(), "dev.name", &[]);
        end.expect_call();

        drop(end);

        assert!(matches!(
            waiting.recv_timeout(WAIT),
            Ok(Err(CallError::ResponseLost))
        ));
        assert!(
            matches!(
                inlet.recv_timeout(WAIT),
                Ok(Queued::Item(Received::SocketLost))
            ),
            "the loss"
        );
        let mut end = proxy.accept();
        end.accept_open(TARGET);
        // A call made before the session is open again fails at once.
        let reply = loop {
            let reply = root.submit(DeviceRoute::root(), "dev.name", &[]);
            match reply.recv_timeout(Duration::from_millis(100)) {
                Ok(Err(CallError::ResponseLost)) => {}
                Err(_) => break reply,
                other => panic!("the call ended as {other:?}"),
            }
        };
        let (name, id) = end.expect_call();
        assert_eq!(name, "dev.name");
        end.send(&Packet::rpc_reply(id, b"FAKE", DeviceRoute::root()).unwrap());
        assert_eq!(reply.recv_timeout(WAIT).unwrap().unwrap(), b"FAKE");
    }

    /// Whether [`record_start`] ran.
    static STARTED: AtomicBool = AtomicBool::new(false);

    /// Notes that it was asked to start a proxy, and fails.
    fn record_start() -> BoxFuture<'static, io::Result<()>> {
        STARTED.store(true, Ordering::SeqCst);
        Box::pin(async { Err(io::Error::other("no proxy is started here")) })
    }

    /// A session opened again after its socket closed never starts a
    /// proxy, though the open that made it would, so a stopped proxy whose
    /// stop a client missed stays stopped.
    #[test]
    fn a_session_opened_again_never_starts_a_proxy() {
        let proxy = FakeProxy::start();
        let address = ProxyAddress {
            url: proxy.url(),
            ensure: Some(record_start),
        };
        let opening = thread::spawn(move || Session::open(&address, TARGET, Purpose::Use, WAIT));
        let mut end = proxy.accept();
        end.accept_open(TARGET);
        let _session = opening.join().unwrap().unwrap();

        drop((end, proxy));
        thread::sleep(Duration::from_secs(1));

        assert!(!STARTED.load(Ordering::SeqCst), "a proxy was started");
    }

    /// The proxy closes the socket once it gives the device up, so a socket
    /// that drops after that ends the session, and nothing opens it again.
    #[test]
    fn a_socket_dropped_after_its_device_was_given_up_ends_the_session() {
        let proxy = FakeProxy::start();
        let (session, mut end) = opened(&proxy);
        let inlet = session.attach(64);

        end.send(&Packet::link(LinkStatus::FailedToReconnect));
        drop(end);

        let status = next(&inlet).map(|packet| packet.link_status());
        assert_eq!(status, Ok(Some(LinkStatus::FailedToReconnect)));
        assert_eq!(
            next(&inlet).err(),
            Some(crate::chan::RecvTimeoutError::Disconnected)
        );
        assert!(
            !proxy.arrives(Duration::from_secs(1)),
            "the session opened again after its device was given up"
        );
    }

    /// The wait between two tries to open again is never more than a
    /// quarter longer or shorter than the backoff, and not always the same.
    #[test]
    fn the_wait_to_open_again_is_jittered_by_a_quarter() {
        let wait = Duration::from_secs(4);
        let waits: Vec<_> = (0..100).map(|_| jittered(wait)).collect();
        assert!(waits
            .iter()
            .all(|jittered| *jittered >= wait * 3 / 4 && *jittered <= wait * 5 / 4));
        assert!(
            waits.iter().any(|jittered| *jittered != waits[0]),
            "no jitter"
        );
    }

    /// A rejected frame leaves an RPC pending, but a terminal transport error
    /// fails pending calls even if the delivery channel's sender stays alive.
    #[test]
    fn rejected_frames_preserve_calls_and_transport_errors_fail_them() {
        let (session, proxy) = Session::test_pair();
        let root = Handle::root(session);
        let waiting = root.submit(DeviceRoute::root(), "dev.name", &[]);
        let (_, _, _, id) = proxy.call();
        proxy
            .deliver
            .send(Ok(TransportEvent::RejectedFrame(
                crate::tio::packet::DecodeError::CRC32,
            )))
            .unwrap();
        proxy.reply(id, b"sensor", DeviceRoute::root());
        assert_eq!(waiting.recv_timeout(WAIT).unwrap().unwrap(), b"sensor");

        let waiting = root.submit(DeviceRoute::root(), "dev.name", &[]);
        proxy.call();
        proxy
            .deliver
            .send(Err(transport::RecvError::LostFraming(
                crate::tio::packet::DecodeError::InvalidPacketType,
            )))
            .unwrap();
        assert!(matches!(
            waiting.recv_timeout(WAIT),
            Ok(Err(CallError::ResponseLost))
        ));
    }

    /// A peer that never answers the hello, such as a proxy from an older
    /// build, fails every open, `auto` too, saying to update it.
    #[test]
    fn a_peer_that_never_answers_as_a_proxy_fails_every_open() {
        let legacy = FakeDevice::start();
        let address = ProxyAddress {
            url: legacy.url.clone(),
            ensure: None,
        };
        for target in ["auto", TARGET] {
            let opened = Session::open(&address, target, Purpose::Use, Duration::from_millis(500));

            let error = opened.err().expect("a legacy proxy opens nothing");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                format!(
                    "{} is a proxy from an older build of twinleaf, or no proxy; update tio, or stop it",
                    legacy.url
                )
            );
        }
    }

    /// Whether [`slow_refusal`] has begun.
    static SLOW_ENSURE_BEGAN: AtomicBool = AtomicBool::new(false);

    /// Whether [`slow_refusal`] ran to its end.
    static SLOW_ENSURE_ENDED: AtomicBool = AtomicBool::new(false);

    /// Finding or starting a proxy, which takes a while, then fails.
    fn slow_refusal() -> BoxFuture<'static, io::Result<()>> {
        Box::pin(async {
            SLOW_ENSURE_BEGAN.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(200)).await;
            SLOW_ENSURE_ENDED.store(true, Ordering::SeqCst);
            Err(io::Error::other("no proxy could start"))
        })
    }

    /// An open given up while it finds or starts the proxy leaves that work
    /// to finish, so a proxy it hosts stays up for the opens after it.
    #[test]
    fn an_open_given_up_leaves_the_proxy_it_starts_to_finish() {
        let closed = fixtures::ClosedPort::new();
        let machine = ProxyAddress {
            url: closed.url(),
            ensure: Some(slow_refusal),
        };

        let gave_up = Supervisor::new().run("a test open", async move {
            let opening = Session::open_async(&machine, TARGET, Purpose::Use, WAIT);
            let began = async {
                while !SLOW_ENSURE_BEGAN.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::select! {
                _ = opening => Ok(false),
                () = began => Ok(true),
            }
        });
        assert!(gave_up.unwrap(), "the open ended before it was given up");

        let deadline = Instant::now() + WAIT;
        while !SLOW_ENSURE_ENDED.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "the proxy's start was dropped with the open"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// How often [`counted_refusal`] ran.
    static ENSURED: AtomicUsize = AtomicUsize::new(0);

    /// Finding or starting a proxy, which fails and counts that it ran.
    fn counted_refusal() -> BoxFuture<'static, io::Result<()>> {
        Box::pin(async {
            ENSURED.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("no proxy could start"))
        })
    }

    /// Only the machine's own address starts a proxy, and only once nothing
    /// answered there.
    #[test]
    fn only_a_refused_machine_address_finds_or_starts_the_proxy() {
        let closed = fixtures::ClosedPort::new();
        let named = ProxyAddress {
            url: closed.url(),
            ensure: None,
        };
        let machine = ProxyAddress {
            url: closed.url(),
            ensure: Some(counted_refusal),
        };

        let error = Session::open(&named, TARGET, Purpose::Use, WAIT).err();
        assert_eq!(
            error.map(|error| error.kind()),
            Some(io::ErrorKind::ConnectionRefused)
        );
        assert_eq!(ENSURED.load(Ordering::SeqCst), 0);

        let error = Session::open(&machine, TARGET, Purpose::Use, WAIT).err();
        assert_eq!(
            error.map(|error| error.to_string()).as_deref(),
            Some("no proxy could start")
        );
        assert_eq!(ENSURED.load(Ordering::SeqCst), 1);

        let proxy = FakeProxy::start();
        let answering = ProxyAddress {
            url: proxy.url(),
            ensure: Some(counted_refusal),
        };
        let opening = thread::spawn(move || Session::open(&answering, TARGET, Purpose::Use, WAIT));
        proxy.accept().accept_open(TARGET);
        assert!(opening.join().unwrap().is_ok());
        assert_eq!(ENSURED.load(Ordering::SeqCst), 1);
    }

    /// A session beats while the proxy waits on its device to open, as the
    /// proxy hangs up on a client quiet longer than the wait may take.
    #[test]
    fn a_session_beats_while_its_open_waits() {
        let proxy = FakeProxy::start();
        let address = ProxyAddress {
            url: proxy.url(),
            ensure: None,
        };
        let opening = thread::spawn(move || Session::open(&address, TARGET, Purpose::Use, WAIT));
        let mut end = proxy.accept();
        let (hello, open) = end.expect_handshake("proxy.open", TARGET);
        end.send(&Packet::proxy_reply(hello, &encode(&fake_host())).unwrap());

        let beat = end.next_within(CLIENT_KEEPALIVE.every * 2);
        assert!(
            matches!(
                beat.as_ref().map(Packet::payload),
                Some(Payload::Heartbeat(_))
            ),
            "the session sent {beat:?} while its open waited"
        );

        end.send(&Packet::proxy_reply(open, &encode(&TARGET)).unwrap());
        assert!(opening.join().unwrap().is_ok());
    }

    /// Once the proxy has said hello, a session beats while idle, and gives
    /// up a proxy that goes quiet without closing its connection. The session
    /// opens over an in-memory socket on the paused clock, as [`bind`] opens
    /// one over TCP, so each beat and the give up come at exactly their time.
    #[tokio::test(start_paused = true)]
    async fn a_session_beats_and_gives_up_a_quiet_proxy() {
        let tasks = Supervisor::current();
        let (deliver, received) = crate::chan::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
        let (port, mut end) = transport::Port::fake(transport::TransportKind::Tcp, None, deliver);
        let args = encode(&TARGET);
        let open = Purpose::Use.method();
        let opening = handshake(&port, &received, "fake", open, &args, "open", WAIT);
        let answering = async {
            let (_, _, hello) = control(&end.recv().await.expect("the hello"));
            let (_, _, open) = control(&end.recv().await.expect("the open"));
            let host = Packet::proxy_reply(hello, &encode(&fake_host())).unwrap();
            end.send(&host).await;
            end.send(&Packet::proxy_reply(open, &args).unwrap()).await;
        };
        let (opened, ()) = tokio::join!(opening, answering);
        opened.unwrap();
        // The proxy sends nothing after the reply to the open, so the session
        // has heard nothing since.
        let quiet = tokio::time::Instant::now();
        let peer = "the fake proxy".to_string();
        let socket = (
            Socket::Transport(port),
            received.into_stream().boxed(),
            peer,
        );
        let _session = Session::start(socket, None, &tasks);

        let mut beats = Vec::new();
        let beating = async {
            while let Some(packet) = end.recv().await {
                assert!(matches!(packet.payload(), Payload::Heartbeat(_)));
                beats.push(quiet.elapsed());
            }
        };
        let gave_up = tokio::time::timeout(CLIENT_KEEPALIVE.quiet * 2, beating).await;

        assert!(gave_up.is_ok(), "the session beat on at {beats:?}");
        assert_took(quiet.elapsed(), CLIENT_KEEPALIVE.quiet, "the give up");
        assert!(beats.len() >= 4, "the session beat at {beats:?}");
        for (beat, at) in beats.iter().enumerate() {
            let due = CLIENT_KEEPALIVE.every * (beat as u32 + 1);
            assert_took(*at, due, format!("heartbeat {beat}"));
        }
    }

    /// Nothing reads the inlet before the pump attaches, so the session drops
    /// what it would queue. Once the pump reads it, a full inlet drops what
    /// does not fit and says so.
    #[test]
    fn a_full_inlet_drops_packets_and_says_so() {
        let (session, proxy) = Session::test_pair();
        let root = Handle::root(session.clone());
        // A reply comes after every packet sent ahead of it, so its arrival
        // says the session took those.
        let settled = |label: &[u8]| {
            let reply = root.submit(DeviceRoute::root(), "dev.name", &[]);
            let (route, _, _, id) = proxy.call();
            proxy.reply(id, label, route);
            assert_eq!(reply.recv_timeout(WAIT).unwrap().unwrap(), label);
        };
        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        settled(b"before");
        let inlet = session.attach(2);

        for _ in 0..3 {
            proxy.send(Packet::heartbeat(DeviceRoute::root()));
        }
        settled(b"after");
        assert_eq!(inlet.drain().count(), 2, "the inlet holds what fits");
        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        settled(b"drained");

        assert!(matches!(inlet.try_recv(), Ok(Queued::Gap(1))));
        assert!(matches!(inlet.try_recv(), Ok(Queued::Item(_))));
        assert_eq!(
            next(&session.attach(2)).err(),
            Some(crate::chan::RecvTimeoutError::Disconnected),
            "a second pump gets an inlet that has ended"
        );
    }

    /// A reply to the pump's control request goes into a full inlet, in the
    /// order it arrived among the packets around it, after the count of the
    /// packets the inlet missed before it.
    #[test]
    fn a_control_reply_always_reaches_the_pump_in_order() {
        let (session, proxy) = Session::test_pair();
        let inlet = session.attach(1);
        let root = Handle::root(session);
        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        proxy.send(Packet::proxy_reply(7, b"state").unwrap());
        let reply = root.submit(DeviceRoute::root(), "dev.name", &[]);
        let (route, _, _, id) = proxy.call();
        proxy.reply(id, b"taken", route);
        assert_eq!(reply.recv_timeout(WAIT).unwrap().unwrap(), b"taken");

        let kinds: Vec<_> = inlet
            .drain()
            .map(|queued| match queued {
                Queued::Item(Received::Packet(packet)) => Some(packet.ptype()),
                Queued::Gap(1) => None,
                other => panic!("expected a packet or one missed, got {other:?}"),
            })
            .collect();
        assert_eq!(
            kinds,
            [
                Some(PacketType::HEARTBEAT),
                None,
                Some(PacketType::PROXY_RX)
            ]
        );
    }

    /// The session's task ends once nothing holds the session, which
    /// closes its socket.
    #[test]
    fn dropping_every_handle_closes_the_socket() {
        let proxy = FakeProxy::start();
        let (session, mut end) = opened(&proxy);

        drop(session);

        end.stream.set_read_timeout(Some(WAIT)).unwrap();
        loop {
            match end.stream.read(&mut [0; 64]) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    panic!("the socket stayed open")
                }
                Err(_) => break,
            }
        }
    }

    #[test]
    fn a_handle_rejects_routes_outside_its_scope_or_depth() {
        let (session, proxy) = Session::test_pair();
        let scope: DeviceRoute = "/1".parse().unwrap();
        let handle = Handle::root(session).subtree(scope).unwrap().to_depth(1);

        let outside: DeviceRoute = "/2".parse().unwrap();
        assert!(matches!(
            handle.submit(outside, "dev.name", b"").recv(),
            Ok(Err(CallError::InvalidRoute(RouteError::OutsideSubtree)))
        ));
        let too_deep: DeviceRoute = "/1/2/3".parse().unwrap();
        assert!(matches!(
            handle.submit(too_deep, "dev.name", b"").recv(),
            Ok(Err(CallError::InvalidRoute(RouteError::OutsideSubtree)))
        ));
        assert!(proxy.sent.is_empty());

        let inside: DeviceRoute = "/1/2".parse().unwrap();
        let _reply = handle.submit(inside, "dev.name", b"arg");
        let (route, name, args, _) = proxy.call();
        assert_eq!(
            (route, name.as_str(), args.as_slice()),
            (inside, "dev.name", &b"arg"[..])
        );
    }

    /// A call no reply answers fails at exactly its timeout, and not before.
    #[tokio::test(start_paused = true)]
    async fn a_call_fails_at_exactly_its_timeout() {
        let tasks = Supervisor::current();
        let (session, _proxy) = Session::test_pair_on(&tasks);
        let root = Handle::root(session);
        let started = tokio::time::Instant::now();

        let reply = root.submit(DeviceRoute::root(), "dev.name", &[]);

        assert!(matches!(
            reply.recv_async().await,
            Ok(Err(CallError::Timeout))
        ));
        assert_took(started.elapsed(), DEFAULT_RPC_TIMEOUT, "the call");
    }

    /// The call that runs out of time first fails first, though it was made
    /// after one that waits longer. Each other call fails at its own
    /// timeout.
    #[tokio::test(start_paused = true)]
    async fn the_earliest_of_several_deadlines_wins() {
        let tasks = Supervisor::current();
        let (session, _proxy) = Session::test_pair_on(&tasks);
        let root = Handle::root(session);
        let started = tokio::time::Instant::now();
        let call = |timeout| {
            root.with_timeout(timeout)
                .submit(DeviceRoute::root(), "dev.name", &[])
        };
        let (short, middle, long) = (
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(3),
        );

        let calls = [call(long), call(short), call(middle)];

        let [long_reply, short_reply, middle_reply] = calls;
        assert!(matches!(
            short_reply.recv_async().await,
            Ok(Err(CallError::Timeout))
        ));
        assert_took(started.elapsed(), short, "the earliest call");
        assert!(matches!(
            long_reply.try_recv(),
            Err(crate::chan::TryRecvError::Empty)
        ));
        assert!(matches!(
            middle_reply.recv_async().await,
            Ok(Err(CallError::Timeout))
        ));
        assert_took(started.elapsed(), middle, "the middle call");
        assert!(matches!(
            long_reply.recv_async().await,
            Ok(Err(CallError::Timeout))
        ));
        assert_took(started.elapsed(), long, "the latest call");
    }
}

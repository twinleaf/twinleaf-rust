//! An app's [`Session`] with its proxy, over one socket.
//!
//! The session carries the open that says what it is for, the RPCs its views
//! make, and the packets its stream pump reads.
//!
//! [`Session::open`] connects to the proxy and introduces this process with
//! `proxy.hello`. It then asks the proxy to open a target with `proxy.open`,
//! or to look a device over with `proxy.probe`, and returns once the proxy
//! has answered. It fails with the proxy's refusal, or when the peer does not
//! answer as a 2.0 proxy does. [`Session::direct`] connects to a loopback
//! target with no handshake, as a client older than tio 2.0 does.
//!
//! Either way one task on the crate's runtime then owns the socket. It sends
//! each call, and a reply completes the call it answers. Every other packet
//! goes to the pump. The session ends when its socket closes or the proxy
//! stops, and every call still waiting fails. The session does not
//! reconnect.
//!
//! A device that drops out does not end the session. The proxy retries the
//! device, and the pump sees the link status
//! [`SensorDisconnected`](LinkStatus::SensorDisconnected) and then
//! [`SensorReconnected`](LinkStatus::SensorReconnected). Calls made in
//! between fail. The proxy closes the socket only when it gives the device
//! up, after [`FailedToReconnect`](LinkStatus::FailedToReconnect). A proxy
//! that crashes also closes the socket, which ends the session as a stop
//! does.

use super::runtime::ProxyAddress;
use super::stream::{End, Lane, Queued};
use crate::device::rpc::{CallError, RpcArgs, RpcErrorPayload};
use crate::proto::{DeviceRoute, RouteError};
use crate::proxy::{handshake, Layout, Purpose, CLIENT_KEEPALIVE, MAX_RPC_TIMEOUT, MOUNT};
use crate::rt::{sleep_until_some, Supervisor};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use crate::tio::transport::{self, ReceiveResult, TransportEvent};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// The target an empty target means.
const AUTO: &str = "auto";

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

/// Where a session writes, its socket or a channel a test reads.
enum Socket {
    Transport(transport::Port),
    #[cfg(test)]
    Channel(flume::Sender<Packet>),
}

impl Socket {
    fn send(&self, packet: Packet) -> Result<(), CallError> {
        match self {
            Socket::Transport(port) => port.try_send(packet).map_err(|error| match error {
                transport::SendError::Full => CallError::RequestNotSubmitted,
                _ => CallError::ResponseLost,
            }),
            #[cfg(test)]
            Socket::Channel(sent) => sent.send(packet).map_err(|_| CallError::ResponseLost),
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
    /// Feeds the pump through this inlet. A second inlet is dropped.
    Attach(Lane<Packet>),
    /// Ends the session, as its owner closed it.
    Close,
}

/// The pump's end of a session. It carries every packet from the proxy but
/// replies, in order, a gap where the session had no room, and the stop.
pub(crate) type Inlet = flume::Receiver<Queued<Packet>>;

/// One session with a proxy. Every clone is the same session, and dropping
/// the last ends the session's task, which closes the socket.
#[derive(Clone)]
pub(crate) struct Session {
    commands: mpsc::UnboundedSender<Command>,
}

impl Session {
    /// Connects to `proxy` and asks it to open `target` for `purpose`,
    /// waiting at most `timeout` for its answer. An empty target means
    /// `auto`. Blocks until the proxy answers or the open fails.
    ///
    /// When nothing answers at the machine's own address, finds or starts the
    /// proxy there first. Fails with the proxy's message when it refuses the
    /// open. Fails with how to reach the peer when it does not answer as a
    /// 2.0 proxy within two seconds, as a proxy older than tio 2.0 does not.
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
    /// be awaited on a tokio runtime.
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
        let request = (purpose.method(), target.encode_args());
        Session::request_async(proxy, request, &format!("open {target}"), timeout).await
    }

    /// Opens a session that sees `layout`, as [`open_async`](Self::open_async)
    /// opens a target.
    pub(crate) async fn mount_async(
        proxy: &ProxyAddress,
        layout: &Layout,
        timeout: Duration,
    ) -> io::Result<Session> {
        let request = (MOUNT, layout.encode_args());
        Session::request_async(proxy, request, &format!("mount {layout}"), timeout).await
    }

    /// Connects to `proxy` and makes the control request `(method, args)`
    /// that binds the connection, which fails as unable to `goal`.
    async fn request_async(
        proxy: &ProxyAddress,
        (method, args): (&str, Vec<u8>),
        goal: &str,
        timeout: Duration,
    ) -> io::Result<Session> {
        let tasks = Supervisor::new();
        let (port, received) = connect(proxy, &tasks).await?;
        let url = &proxy.url;
        let (host, opened) = handshake(&port, &received, url, method, &args, goal, timeout).await?;
        port.keep_alive(CLIENT_KEEPALIVE).await;
        log::debug!(
            "the proxy at {url} opened {}",
            String::from_utf8_lossy(&opened)
        );
        let peer = match host {
            Some(host) => format!("the proxy at {url}, hosted by {host},"),
            None => format!("the proxy at {url}"),
        };
        let socket = Socket::Transport(port);
        Ok(Session::start(socket, received, peer, &tasks))
    }

    /// Connects to the proxy at the loopback `url` with no handshake, as a
    /// client older than tio 2.0 does. Blocks while it connects.
    pub(crate) fn direct(url: &str) -> io::Result<Session> {
        let (sender, received) = flume::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
        let port = transport::Port::new(url, sender).map_err(|error| {
            io::Error::new(error.kind(), format!("cannot reach {url}: {error}"))
        })?;
        let socket = Socket::Transport(port);
        let peer = format!("the proxy at {url}");
        Ok(Session::start(socket, received, peer, &Supervisor::new()))
    }

    /// Starts the session's task on `tasks`, which reads `received` and
    /// writes to `socket`. `peer` names the proxy in the log, such as `the
    /// proxy at tcp://127.0.0.1:7855`.
    fn start(
        socket: Socket,
        received: flume::Receiver<ReceiveResult>,
        peer: String,
        tasks: &Supervisor,
    ) -> Session {
        let (commands, mailbox) = mpsc::unbounded_channel();
        let worker = Worker {
            socket,
            received: received.into_stream(),
            peer,
            pending: HashMap::new(),
            next_id: 0,
            next_deadline: None,
            inlet: None,
        };
        tasks.spawn("twinleaf session", worker.run(mailbox));
        Session { commands }
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

    /// Attaches the pump, with room for `capacity` packets. The first inlet
    /// is live, and any later one has already ended.
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
) -> io::Result<(transport::Port, flume::Receiver<ReceiveResult>)> {
    let attempt = || async {
        let (sender, received) = flume::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
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

impl fmt::Display for Ending {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Ending::Stopped => "the proxy stopped",
            Ending::SocketClosed => "its socket closed",
            Ending::HandlesGone => "every handle on it is gone",
            Ending::Closed => "its owner closed it",
        })
    }
}

/// The session's task. It owns the socket, sends calls and completes them,
/// feeds the pump, and fails the calls that run out of time.
struct Worker {
    socket: Socket,
    received: flume::r#async::RecvStream<'static, ReceiveResult>,
    /// The proxy, as the log names it.
    peer: String,
    /// The calls waiting for a reply, by the id each went out with.
    pending: HashMap<u16, Pending>,
    next_id: u16,
    /// When the earliest pending call runs out of time, as last looked up.
    next_deadline: Option<Instant>,
    /// The pump's inlet, once it attached. Until then the session drops what
    /// it would queue.
    inlet: Option<Lane<Packet>>,
}

impl Worker {
    /// Runs the session, taking `commands` ahead of the socket, until its
    /// socket closes, the proxy stops or every handle is gone.
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        let ending = loop {
            tokio::select! {
                biased;
                command = commands.recv() => match command {
                    Some(Command::Call(call)) => self.call(call),
                    Some(Command::Attach(inlet)) => {
                        self.inlet.get_or_insert(inlet);
                    }
                    Some(Command::Close) => break Ending::Closed,
                    None => break Ending::HandlesGone,
                },
                received = self.received.next() => {
                    let Some(received) = received else { break Ending::SocketClosed; };
                    if let Some(ending) = self.delivered(received) {
                        break ending;
                    }
                }
                () = sleep_until_some(self.next_deadline) => self.expire(),
            }
        };
        match ending {
            Ending::SocketClosed => log::warn!("{} closed the connection", self.peer),
            ending => log::debug!("the session with {} ended: {ending}", self.peer),
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
                Command::Attach(_) | Command::Close => {}
            }
        }
        for (_, pending) in self.pending.drain() {
            (pending.complete)(Err(CallError::ResponseLost));
        }
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
        if let Payload::ProxyRx(ProxyRx::Alive) = packet.payload() {
            return true;
        }
        let stopped =
            packet.route().is_empty() && packet.link_status() == Some(LinkStatus::Stopped);
        self.forward(packet);
        !stopped
    }

    /// Queues a packet for the pump, once the pump has attached, after a gap
    /// for those the inlet had no room for.
    fn forward(&mut self, packet: Packet) {
        if let Some(inlet) = &mut self.inlet {
            inlet.send(packet);
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
    ) -> flume::Receiver<RawCallResult> {
        let (resolve, reply) = flume::bounded(1);
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
    pub(crate) sent: flume::Receiver<Packet>,
    /// Hands the session packets as its socket would. Dropping it closes
    /// the socket.
    pub(crate) deliver: flume::Sender<ReceiveResult>,
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
        let (sender, sent) = flume::unbounded();
        let (deliver, received) = flume::unbounded();
        let peer = "the test's proxy".to_string();
        let session = Session::start(Socket::Channel(sender), received, peer, tasks);
        (session, TestProxy { sent, deliver })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::fixtures::{self, FakeDevice};
    use crate::device::rpc::RpcReply;
    use crate::proto::rpc::{Method, RpcError};
    use crate::proxy::fixtures::assert_took;
    use crate::proxy::{client_name, Introduction};
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
            let packet = self.expect();
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

        /// Expect `proxy.hello`, then `method` naming `target`, and answer
        /// neither. Returns the id of each.
        fn expect_handshake(&mut self, method: &str, target: &str) -> (u16, u16) {
            let (name, args, hello) = self.expect_control();
            assert_eq!(name, "proxy.hello");
            assert_eq!(
                Introduction::decode_reply(&args).unwrap(),
                Introduction::of_this_process(client_name())
            );
            let (name, args, open) = self.expect_control();
            assert_eq!(name, method);
            assert_eq!(String::decode_reply(&args).unwrap(), target);
            (hello, open)
        }

        /// Expect the handshake opening `target`, and accept it.
        fn accept_open(&mut self, target: &str) {
            let (hello, open) = self.expect_handshake("proxy.open", target);
            self.send(&Packet::proxy_reply(hello, &fake_host().encode_args()).unwrap());
            self.send(&Packet::proxy_reply(open, &target.encode_args()).unwrap());
        }

        /// The next call the session sends, as its method name and its id.
        fn expect_call(&mut self) -> (String, u16) {
            let packet = self.expect();
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
    fn next(inlet: &Inlet) -> Result<Packet, flume::RecvTimeoutError> {
        match inlet.recv_timeout(WAIT)? {
            Queued::Item(packet) => Ok(packet),
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
        end.send(&Packet::proxy_reply(hello, &fake_host().encode_args()).unwrap());
        assert!(
            end.next_within(Duration::from_millis(200)).is_none(),
            "the session sent more before the open was answered"
        );
        assert!(
            !opening.is_finished(),
            "the open returned before its answer"
        );
        end.send(&Packet::proxy_reply(open, &TARGET.encode_args()).unwrap());

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
        end.send(&Packet::proxy_reply(hello, &fake_host().encode_args()).unwrap());
        let refusal = Packet::proxy_error(open, RpcError::Invalid, "auto finds two devices");
        end.send(&refusal.unwrap());

        let error = opening.join().unwrap().err().expect("the open was refused");
        assert_eq!(error.to_string(), "auto finds two devices");
    }

    /// A proxy older than this build's control protocol is named, and the
    /// open fails before the proxy answers it.
    #[test]
    fn an_open_through_an_older_proxy_fails_and_names_it() {
        let proxy = FakeProxy::start();
        let opening = proxy.open(TARGET, Purpose::Use);
        let mut end = proxy.accept();

        let (hello, _) = end.expect_handshake("proxy.open", TARGET);
        let older = Introduction {
            protocol: 0,
            ..fake_host()
        };
        end.send(&Packet::proxy_reply(hello, &older.encode_args()).unwrap());

        let error = opening.join().unwrap().err().expect("the open failed");
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        let message = error.to_string();
        assert!(message.contains(&older.to_string()), "{message}");
        assert!(message.contains("protocol 0"), "{message}");
    }

    #[test]
    fn a_probe_asks_the_proxy_to_probe() {
        let proxy = FakeProxy::start();
        let opening = proxy.open(TARGET, Purpose::Probe);
        let mut end = proxy.accept();

        let (hello, probe) = end.expect_handshake("proxy.probe", TARGET);
        end.send(&Packet::proxy_reply(hello, &fake_host().encode_args()).unwrap());
        end.send(&Packet::proxy_reply(probe, &TARGET.encode_args()).unwrap());

        assert!(opening.join().unwrap().is_ok());
    }

    /// A stop while the open waits is the proxy going away, not a refusal.
    #[test]
    fn a_stop_while_the_open_waits_fails_the_open() {
        let proxy = FakeProxy::start();
        let opening = proxy.open(TARGET, Purpose::Use);
        let mut end = proxy.accept();

        let (hello, _) = end.expect_handshake("proxy.open", TARGET);
        end.send(&Packet::proxy_reply(hello, &fake_host().encode_args()).unwrap());
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

    /// A stop reaches the pump, then ends its inlet, and the session ends
    /// with every call still waiting, and nothing reconnects.
    #[test]
    fn a_stop_ends_the_session() {
        let proxy = FakeProxy::start();
        let (session, mut end) = opened(&proxy);
        let inlet = session.attach(64);
        let root = Handle::root(session);
        let waiting = root.submit(DeviceRoute::root(), "dev.name", &[]);
        end.expect_call();

        end.send(&Packet::heartbeat(DeviceRoute::root()));
        end.send(&Packet::link(LinkStatus::Stopped));

        assert!(matches!(
            next(&inlet).unwrap().payload(),
            Payload::Heartbeat(_)
        ));
        assert_eq!(
            next(&inlet).unwrap().link_status(),
            Some(LinkStatus::Stopped)
        );
        assert!(matches!(
            inlet.recv_timeout(WAIT),
            Ok(Queued::Ended(End::Stopped))
        ));
        assert_eq!(
            inlet.recv_timeout(WAIT).err(),
            Some(flume::RecvTimeoutError::Disconnected)
        );
        assert!(matches!(
            waiting.recv_timeout(WAIT),
            Ok(Err(CallError::ResponseLost))
        ));
        assert!(matches!(
            root.submit(DeviceRoute::root(), "dev.name", &[])
                .recv_timeout(WAIT),
            Ok(Err(CallError::ResponseLost))
        ));
        assert!(
            !proxy.arrives(Duration::from_secs(2)),
            "the session reconnected after the proxy stopped"
        );
    }

    /// A socket that drops ends the session as a stop does, and nothing
    /// reconnects in its place.
    #[test]
    fn a_dropped_socket_fails_pending_calls_and_ends_the_stream_for_good() {
        let proxy = FakeProxy::start();
        let (session, mut end) = opened(&proxy);
        let inlet = session.attach(64);
        let root = Handle::root(session);
        let waiting = root.submit(DeviceRoute::root(), "dev.name", &[]);
        end.expect_call();

        drop(end);

        assert!(matches!(
            waiting.recv_timeout(WAIT),
            Ok(Err(CallError::ResponseLost))
        ));
        assert_eq!(
            next(&inlet).err(),
            Some(flume::RecvTimeoutError::Disconnected)
        );
        assert!(
            !proxy.arrives(Duration::from_secs(2)),
            "the session reconnected after its socket dropped"
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

    /// A loopback target is a proxy already, so its first packet from the
    /// session is the app's own, with no hello ahead of it.
    #[test]
    fn a_loopback_target_is_reached_directly_with_no_hello() {
        let proxy = FakeProxy::start();
        let url = proxy.url();
        let connecting = thread::spawn(move || Session::direct(&url));
        let mut end = proxy.accept();
        let root = Handle::root(connecting.join().unwrap().unwrap());

        let reply = root.submit(DeviceRoute::root(), "dev.name", &[]);

        let (name, id) = end.expect_call();
        assert_eq!(name, "dev.name");
        end.send(&Packet::rpc_reply(id, b"FAKE", DeviceRoute::root()).unwrap());
        assert_eq!(reply.recv_timeout(WAIT).unwrap().unwrap(), b"FAKE");
    }

    /// A peer that never answers as a 2.0 proxy does, as a proxy older than
    /// tio 2.0 does not, fails every open, `auto` too, saying how to reach
    /// it.
    #[test]
    fn a_peer_that_never_answers_as_a_proxy_fails_every_open() {
        let legacy = FakeDevice::start();
        let address = ProxyAddress {
            url: legacy.url.clone(),
            ensure: None,
        };
        let port = legacy.url.rsplit(':').next().unwrap();

        for target in ["auto", TARGET] {
            let opened = Session::open(&address, target, Purpose::Use, Duration::from_millis(500));

            let error = opened.err().expect("a legacy proxy opens nothing");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                format!(
                    "{} is not a Twinleaf 2.0 proxy; reach it with -r tcp://localhost:{port}",
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

        thread::sleep(Duration::from_millis(400));
        assert!(
            SLOW_ENSURE_ENDED.load(Ordering::SeqCst),
            "the proxy's start was dropped with the open"
        );
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

    /// Once the proxy has said hello, a session beats while idle, and gives
    /// up a proxy that goes quiet without closing its connection.
    #[test]
    fn a_session_beats_and_gives_up_a_quiet_proxy() {
        let proxy = FakeProxy::start();
        let address = ProxyAddress {
            url: proxy.url(),
            ensure: None,
        };
        let opening = thread::spawn(move || Session::open(&address, TARGET, Purpose::Use, WAIT));
        let mut end = proxy.accept();
        end.accept_open(TARGET);
        let _session = opening.join().unwrap().unwrap();
        let quiet = Instant::now();

        let mut beats = 0;
        while let Some(packet) = end.next_within(CLIENT_KEEPALIVE.every * 2) {
            assert!(matches!(packet.payload(), Payload::Heartbeat(_)));
            beats += 1;
        }

        let given_up = quiet.elapsed();
        assert!(beats >= 3, "the session sent {beats} heartbeats");
        assert!(
            (CLIENT_KEEPALIVE.quiet..CLIENT_KEEPALIVE.quiet + CLIENT_KEEPALIVE.every)
                .contains(&given_up),
            "the session gave the proxy up after {given_up:?}"
        );
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
            Some(flume::RecvTimeoutError::Disconnected),
            "a second pump gets an inlet that has ended"
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
            Err(flume::TryRecvError::Empty)
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

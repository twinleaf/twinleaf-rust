//! The stream pump: one private thread per subscription root that drains a
//! data port, keeps metadata discovery going, and fans batches and events out
//! to owned receivers.

use crate::data::coalesce::BatchCoalescer;
use crate::data::{DeviceMetadataSnapshot, MetadataQuery, PacketParser, SampleBatch, SessionId};
use crate::device::RpcMethod;
use crate::tio;
use crate::tio::proto::{self, DeviceRoute};
use crate::tio::proxy;

use std::collections::{HashMap, HashSet, VecDeque};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel;
use twinleaf_proto::data as wire;
use twinleaf_proto::rpc as wire_rpc;

/// Batches a subscriber may fall behind by before the pump starts merging.
const BATCH_QUEUE_LEN: usize = 1024;

/// Events a subscriber may fall behind by. Events are rare next to samples, so
/// this is deep enough that only an abandoned receiver reaches it.
const EVENT_QUEUE_LEN: usize = 4096;

/// Rows the pump merges into one batch while a subscriber is not receiving.
const LAG_MERGE_ROWS: usize = 4096;

/// Subscriptions the tree may queue before the pump takes them.
const SUBSCRIBE_QUEUE_LEN: usize = 16;

/// Wait before the first metadata retry, and the ceiling it doubles up to.
const METADATA_RETRY_FIRST: Duration = Duration::from_millis(250);
const METADATA_RETRY_MAX: Duration = Duration::from_secs(8);

/// The one setting a host reads as more than opaque bytes: its value is the
/// device's RPC table hash, a u32le.
fn rpc_hash(setting: &twinleaf_proto::settings::Setting<'_>) -> Option<u32> {
    if setting.name != b"rpc.hash" {
        return None;
    }
    let value = setting.reply.get(..4)?;
    Some(u32::from_le_bytes(value.try_into().ok()?))
}

/// Device-level events produced by the live connection engine.
///
/// These events arrive via both direct serial and tio-proxy connections. A
/// direct serial connection closes after `SensorDisconnected`; a tio-proxy TCP
/// connection can remain open, making the status event the disconnection
/// signal shared by both transports.
#[derive(Debug, Clone)]
pub enum DeviceEvent {
    /// Connection status changed.
    ///
    /// A direct serial channel closes after `SensorDisconnected`, while a
    /// tio-proxy connection can stay open. Consumers should therefore handle
    /// this event rather than depending on a later receive error.
    Status(proto::ProxyStatus),
    /// Another client completed an RPC that can invalidate a cached value.
    RpcInvalidated(RpcMethod),
    /// Device heartbeat, including the session id for the standard format.
    Heartbeat { session_id: Option<SessionId> },
    /// The parser has collected complete metadata for this device.
    MetadataReady(DeviceMetadataSnapshot),
    /// The device answered `dev.metadata` with `NotFound`: its firmware cannot
    /// describe its streams, so nothing on this route will ever decode.
    MetadataUnavailable,
    /// `Some(hash)` comes from a settings packet; `None` requests a refresh
    /// after reconnection.
    NewHash(Option<u32>),
}

/// Events from a DeviceTree (multi-device monitoring).
#[derive(Debug, Clone)]
pub enum TreeEvent {
    /// First packet received from this route.
    RouteDiscovered(DeviceRoute),

    /// Event from a specific device.
    Device {
        route: DeviceRoute,
        event: DeviceEvent,
    },
}

/// A discovered route paired with its device's `dev.name` (`None` if the device
/// didn't answer). Returned by [`DeviceTree::named_routes`](super::DeviceTree::named_routes).
#[derive(Debug, Clone)]
pub struct NamedRoute {
    pub route: DeviceRoute,
    pub name: Option<String>,
}

/// Why [`Receiver::recv`] returned no item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecvError {
    /// The subscriber fell behind and the pump merged or shed this many items.
    /// Recoverable: receive again for the items that follow the gap.
    #[error("skipped {0} items")]
    Lagged(u64),
    /// The pump stopped. Terminal.
    #[error("the stream ended")]
    Disconnected,
}

/// Why [`Receiver::try_recv`] returned no item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TryRecvError {
    #[error("no item available")]
    Empty,
    #[error("skipped {0} items")]
    Lagged(u64),
    #[error("the stream ended")]
    Disconnected,
}

/// Why [`Receiver::recv_timeout`] returned no item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecvTimeoutError {
    #[error("timed out waiting for an item")]
    Timeout,
    #[error("skipped {0} items")]
    Lagged(u64),
    #[error("the stream ended")]
    Disconnected,
}

/// An owned subscription to one subtree's stream.
///
/// Progress is internal: the pump keeps draining the device whether or not
/// anything is receiving here. Dropping this releases the pump's fan-out slot,
/// and the pump stops once the last receiver and the tree that started it are
/// both gone.
pub struct Receiver<T> {
    items: channel::Receiver<Result<T, RecvError>>,
    _lifeline: channel::Sender<()>,
}

impl<T> Receiver<T> {
    /// Block until the next item.
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.items.recv() {
            Ok(item) => item,
            Err(channel::RecvError) => Err(RecvError::Disconnected),
        }
    }

    /// The next item if one is already queued.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.items.try_recv() {
            Ok(Ok(item)) => Ok(item),
            Ok(Err(RecvError::Lagged(skipped))) => Err(TryRecvError::Lagged(skipped)),
            Ok(Err(RecvError::Disconnected)) | Err(channel::TryRecvError::Disconnected) => {
                Err(TryRecvError::Disconnected)
            }
            Err(channel::TryRecvError::Empty) => Err(TryRecvError::Empty),
        }
    }

    /// Block for at most `timeout`.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.resolve(self.items.recv_timeout(timeout))
    }

    /// Block until `deadline`, which can be reused across a loop without
    /// extending the caller's overall budget.
    pub fn recv_deadline(&self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        self.resolve(self.items.recv_deadline(deadline))
    }

    fn resolve(
        &self,
        received: Result<Result<T, RecvError>, channel::RecvTimeoutError>,
    ) -> Result<T, RecvTimeoutError> {
        match received {
            Ok(Ok(item)) => Ok(item),
            Ok(Err(RecvError::Lagged(skipped))) => Err(RecvTimeoutError::Lagged(skipped)),
            Ok(Err(RecvError::Disconnected)) | Err(channel::RecvTimeoutError::Disconnected) => {
                Err(RecvTimeoutError::Disconnected)
            }
            Err(channel::RecvTimeoutError::Timeout) => Err(RecvTimeoutError::Timeout),
        }
    }

    /// Blocking iterator over the subscription, ending when the stream does.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter { receiver: self }
    }

    /// The underlying channel, to `select!` on alongside other sources. Its
    /// items are one value, or the [`RecvError::Lagged`] count of what the
    /// subscriber missed before it.
    pub fn receiver(&self) -> &channel::Receiver<Result<T, RecvError>> {
        &self.items
    }
}

/// Blocking iterator over a borrowed [`Receiver`].
pub struct Iter<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<T> Iterator for Iter<'_, T> {
    type Item = Result<T, RecvError>;

    fn next(&mut self) -> Option<Result<T, RecvError>> {
        match self.receiver.recv() {
            Ok(item) => Some(Ok(item)),
            Err(RecvError::Lagged(skipped)) => Some(Err(RecvError::Lagged(skipped))),
            Err(RecvError::Disconnected) => None,
        }
    }
}

impl<'a, T> IntoIterator for &'a Receiver<T> {
    type Item = Result<T, RecvError>;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

/// Blocking iterator over an owned [`Receiver`].
pub struct IntoIter<T> {
    receiver: Receiver<T>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = Result<T, RecvError>;

    fn next(&mut self) -> Option<Result<T, RecvError>> {
        self.receiver.iter().next()
    }
}

impl<T> IntoIterator for Receiver<T> {
    type Item = Result<T, RecvError>;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter { receiver: self }
    }
}

/// One subscriber's queue: what it will take, and what it has missed.
struct Sink<T> {
    items: channel::Sender<Result<T, RecvError>>,
    /// Disconnects when the subscriber drops its receiver, waking the pump.
    alive: channel::Receiver<()>,
    skipped: u64,
}

impl<T> Sink<T> {
    fn new(capacity: usize) -> (Sink<T>, Receiver<T>) {
        let (items, received) = channel::bounded(capacity);
        let (lifeline, alive) = channel::bounded(0);
        (
            Sink {
                items,
                alive,
                skipped: 0,
            },
            Receiver {
                items: received,
                _lifeline: lifeline,
            },
        )
    }

    /// Offer one item, counting it as lag if the subscriber has no room.
    /// False once the subscriber is gone.
    fn send(&mut self, item: T) -> bool {
        if !self.report_lag() {
            return false;
        }
        match self.items.try_send(Ok(item)) {
            Ok(()) => true,
            Err(channel::TrySendError::Full(_)) => {
                self.skipped += 1;
                true
            }
            Err(channel::TrySendError::Disconnected(_)) => false,
        }
    }

    /// Tell the subscriber what it missed, once its queue has room again.
    fn report_lag(&mut self) -> bool {
        if self.skipped == 0 {
            return true;
        }
        match self.items.try_send(Err(RecvError::Lagged(self.skipped))) {
            Ok(()) => {
                self.skipped = 0;
                true
            }
            Err(channel::TrySendError::Full(_)) => true,
            Err(channel::TrySendError::Disconnected(_)) => false,
        }
    }

    fn is_live(&self) -> bool {
        !matches!(
            self.alive.try_recv(),
            Err(channel::TryRecvError::Disconnected)
        )
    }
}

/// A subscriber to sample batches, plus the backlog it falls behind into.
///
/// While its queue is full the backlog merges whole batches rather than
/// forwarding them one by one, so catching up costs the consumer fewer, larger
/// batches instead of a burst of tiny ones.
struct BatchSink {
    sink: Sink<SampleBatch>,
    backlog: BatchCoalescer,
}

impl BatchSink {
    fn new(sink: Sink<SampleBatch>) -> BatchSink {
        BatchSink {
            sink,
            backlog: BatchCoalescer::new(Some(LAG_MERGE_ROWS)),
        }
    }

    /// False once the subscriber is gone.
    fn publish(&mut self, batch: &SampleBatch) -> bool {
        self.backlog.push_batch(batch);
        self.flush()
    }

    /// Hand over everything the subscriber will take right now.
    fn flush(&mut self) -> bool {
        if !self.sink.items.is_full() {
            self.backlog.finish_buffered_batch();
        }
        while let Some(ready) = self.backlog.next_completed_batch() {
            if !self.sink.send(ready) {
                return false;
            }
        }
        self.sink.report_lag()
    }
}

/// Why a route is not being asked for metadata right now.
enum Discovery {
    /// The last query failed; ask again once `until` passes.
    Backoff { until: Instant, delay: Duration },
    /// The device answered `NotFound`: its firmware has no `dev.metadata`.
    Unsupported,
}

/// Everything one subscription root's stream needs to stay live: the parser,
/// the routes and metadata already announced, and where discovery stands.
struct StreamState {
    root_route: DeviceRoute,
    parser: PacketParser,
    known_routes: HashSet<DeviceRoute>,
    metadata_announced: HashSet<DeviceRoute>,
    discovery: HashMap<DeviceRoute, Discovery>,
    batches: VecDeque<SampleBatch>,
    events: VecDeque<TreeEvent>,
}

impl StreamState {
    fn new(root_route: DeviceRoute) -> StreamState {
        StreamState {
            root_route,
            parser: PacketParser::new(root_route, false),
            known_routes: HashSet::new(),
            metadata_announced: HashSet::new(),
            discovery: HashMap::new(),
            batches: VecDeque::new(),
            events: VecDeque::new(),
        }
    }

    fn pop_batch(&mut self) -> Option<SampleBatch> {
        self.batches.pop_front()
    }

    fn pop_event(&mut self) -> Option<TreeEvent> {
        self.events.pop_front()
    }

    /// Emit [`DeviceEvent::MetadataReady`] the first time a route's metadata is
    /// complete.
    fn announce_metadata(&mut self, route: DeviceRoute) {
        if self.metadata_announced.contains(&route) {
            return;
        }
        let Some(snapshot) = self.parser.metadata(route) else {
            return;
        };
        self.metadata_announced.insert(route);
        self.events.push_back(TreeEvent::Device {
            route,
            event: DeviceEvent::MetadataReady(snapshot),
        });
    }

    /// Bring a new event subscriber up to date with what the pump already knows.
    fn replay(&self, sink: &mut Sink<TreeEvent>) -> bool {
        self.known_routes
            .iter()
            .all(|route| sink.send(TreeEvent::RouteDiscovered(*route)))
            && self
                .metadata_announced
                .iter()
                .filter_map(|route| Some((*route, self.parser.metadata(*route)?)))
                .all(|(route, snapshot)| {
                    sink.send(TreeEvent::Device {
                        route,
                        event: DeviceEvent::MetadataReady(snapshot),
                    })
                })
    }

    /// Routes whose metadata may be asked for again right now.
    fn metadata_routes(&self, now: Instant) -> Vec<DeviceRoute> {
        self.known_routes
            .iter()
            .copied()
            .filter(|route| match self.discovery.get(route) {
                None => true,
                Some(Discovery::Backoff { until, .. }) => *until <= now,
                Some(Discovery::Unsupported) => false,
            })
            .collect()
    }

    /// The earliest moment a backed-off route wants to be asked again.
    fn next_retry(&self) -> Option<Instant> {
        self.discovery
            .values()
            .filter_map(|state| match state {
                Discovery::Backoff { until, .. } => Some(*until),
                Discovery::Unsupported => None,
            })
            .min()
    }

    fn apply_metadata_reply(&mut self, query: MetadataQuery, reply: &[u8]) {
        let route = query.route;
        self.parser.apply_metadata_reply(query, reply);
        self.discovery.remove(&route);
        self.announce_metadata(route);
    }

    /// Give up on a query and hold its route off for a growing delay.
    fn back_off(&mut self, query: MetadataQuery) {
        let route = query.route;
        self.parser.fail_metadata_query(query);
        let delay = match self.discovery.get(&route) {
            None => METADATA_RETRY_FIRST,
            Some(Discovery::Backoff { delay, .. }) => (*delay * 2).min(METADATA_RETRY_MAX),
            Some(Discovery::Unsupported) => return,
        };
        self.discovery.insert(
            route,
            Discovery::Backoff {
                until: Instant::now() + delay,
                delay,
            },
        );
    }

    /// Stop asking a route whose firmware does not implement `dev.metadata`.
    fn give_up(&mut self, query: MetadataQuery) {
        let route = query.route;
        self.parser.fail_metadata_query(query);
        if matches!(
            self.discovery.insert(route, Discovery::Unsupported),
            Some(Discovery::Unsupported)
        ) {
            return;
        }
        log::warn!("{route} has no dev.metadata; its streams cannot be decoded");
        self.events.push_back(TreeEvent::Device {
            route,
            event: DeviceEvent::MetadataUnavailable,
        });
    }

    fn process_packet(&mut self, pkt: &tio::Packet) {
        let packet_route = pkt.route();
        let Ok(absolute_route) = self.root_route.absolute_route(&packet_route) else {
            log::warn!(
                "dropping packet whose route {} exceeds root scope {}",
                packet_route,
                self.root_route
            );
            return;
        };

        if self.known_routes.insert(absolute_route) {
            self.events
                .push_back(TreeEvent::RouteDiscovered(absolute_route));
        }

        match pkt.payload() {
            proto::Payload::ProxyStatus(status) => {
                self.events.push_back(TreeEvent::Device {
                    route: absolute_route,
                    event: DeviceEvent::Status(status),
                });

                // Forget our metadata on disconnect
                if matches!(status, proto::ProxyStatus::SensorDisconnected) {
                    self.metadata_announced = HashSet::new();
                    self.discovery = HashMap::new();
                    self.parser.reset();
                }

                // We might have new hash(es) on reconnect
                if matches!(status, proto::ProxyStatus::SensorReconnected) {
                    for route in self.known_routes.iter() {
                        self.events.push_back(TreeEvent::Device {
                            route: *route,
                            event: DeviceEvent::NewHash(None),
                        });
                    }
                }

                return;
            }
            proto::Payload::RpcUpdate(method) => {
                self.events.push_back(TreeEvent::Device {
                    route: absolute_route,
                    event: DeviceEvent::RpcInvalidated(RpcMethod::from_wire(method)),
                });
                return;
            }
            proto::Payload::Heartbeat(beat) => {
                self.events.push_back(TreeEvent::Device {
                    route: absolute_route,
                    event: DeviceEvent::Heartbeat {
                        session_id: beat.session().map(|session| session.value()),
                    },
                });
            }
            proto::Payload::Setting(setting) => {
                if let Some(hash) = rpc_hash(&setting) {
                    self.events.push_back(TreeEvent::Device {
                        route: absolute_route,
                        event: DeviceEvent::NewHash(Some(hash)),
                    });
                }
            }
            _ => {}
        }

        if let Err(error) = self.parser.push_packet(pkt) {
            log::warn!("dropping invalid stream packet: {error}");
        }
        while let Some(batch) = self.parser.pop_batch() {
            self.batches.push_back(batch);
        }
        self.announce_metadata(absolute_route);
    }
}

/// A subscription the tree has minted and the pump has not taken yet.
enum PumpCommand {
    Batches(Sink<SampleBatch>),
    Events(Sink<TreeEvent>),
}

/// The tree's end of a running pump.
pub(super) struct PumpHandle {
    commands: channel::Sender<PumpCommand>,
}

impl PumpHandle {
    /// Open a same-proxy data port through `endpoint` and start pumping it.
    pub(super) fn start(endpoint: &proxy::RpcEndpoint) -> Result<PumpHandle, proxy::PortError> {
        let data = endpoint.open_port(true, true)?;
        let (commands, received) = channel::bounded(SUBSCRIBE_QUEUE_LEN);
        let pump = Pump {
            state: StreamState::new(endpoint.scope()),
            endpoint: endpoint.clone(),
            data,
            commands: received,
            metadata_calls: Vec::new(),
            batches: Vec::new(),
            events: Vec::new(),
        };
        thread::Builder::new()
            .name("twinleaf-stream".into())
            .spawn(move || pump.run())
            .expect("failed to spawn the stream pump thread");
        Ok(PumpHandle { commands })
    }

    /// `None` once the pump has stopped, so the caller can start a new one.
    pub(super) fn batches(&self) -> Option<Receiver<SampleBatch>> {
        let (sink, receiver) = Sink::new(BATCH_QUEUE_LEN);
        self.commands
            .try_send(PumpCommand::Batches(sink))
            .is_ok()
            .then_some(receiver)
    }

    /// `None` once the pump has stopped, so the caller can start a new one.
    pub(super) fn events(&self) -> Option<Receiver<TreeEvent>> {
        let (sink, receiver) = Sink::new(EVENT_QUEUE_LEN);
        self.commands
            .try_send(PumpCommand::Events(sink))
            .is_ok()
            .then_some(receiver)
    }
}

/// The pump itself: it owns the data port, the parser, and the metadata queries
/// in flight, and runs whether or not anything is receiving from it.
struct Pump {
    endpoint: proxy::RpcEndpoint,
    data: proxy::Port,
    commands: channel::Receiver<PumpCommand>,
    state: StreamState,
    metadata_calls: Vec<(MetadataQuery, channel::Receiver<proxy::RawCallResult>)>,
    batches: Vec<BatchSink>,
    events: Vec<Sink<TreeEvent>>,
}

impl Pump {
    fn run(mut self) {
        let mut commands_open = true;
        loop {
            if commands_open && !self.drain_commands() {
                commands_open = false;
            }
            let connected = self.drain_input();
            self.publish();
            if !connected {
                return self.close();
            }
            // Nothing left to serve and nobody left to ask.
            if !commands_open && self.batches.is_empty() && self.events.is_empty() {
                return;
            }
            self.submit_metadata_queries();
            self.wait(commands_open);
        }
    }

    /// False once the tree that started the pump is gone.
    fn drain_commands(&mut self) -> bool {
        loop {
            match self.commands.try_recv() {
                Ok(PumpCommand::Batches(sink)) => self.batches.push(BatchSink::new(sink)),
                Ok(PumpCommand::Events(mut sink)) => {
                    if self.state.replay(&mut sink) {
                        self.events.push(sink);
                    }
                }
                Err(channel::TryRecvError::Empty) => return true,
                Err(channel::TryRecvError::Disconnected) => return false,
            }
        }
    }

    /// Take what the port had queued on entry and no more, so a device that
    /// keeps writing cannot starve new subscriptions or publishing.
    ///
    /// False once the proxy link is gone.
    fn drain_input(&mut self) -> bool {
        self.drain_metadata_replies();
        for _ in 0..self.data.receiver().len().max(1) {
            match self.data.try_recv() {
                Ok(packet) => self.state.process_packet(&packet),
                Err(proxy::RecvError::WouldBlock) => return true,
                Err(proxy::RecvError::ProxyDisconnected) => return false,
            }
        }
        true
    }

    fn drain_metadata_replies(&mut self) {
        let mut waiting = Vec::with_capacity(self.metadata_calls.len());
        for (query, pending) in std::mem::take(&mut self.metadata_calls) {
            match pending.try_recv() {
                Ok(result) => self.complete_metadata(query, result),
                Err(channel::TryRecvError::Empty) => waiting.push((query, pending)),
                Err(channel::TryRecvError::Disconnected) => {
                    self.complete_metadata(query, Err(proxy::RawCallError::ProxyClosed))
                }
            }
        }
        self.metadata_calls = waiting;
    }

    /// Apply a finished `dev.metadata` call, or decide when to ask again.
    fn complete_metadata(&mut self, query: MetadataQuery, result: proxy::RawCallResult) {
        match result {
            Ok(reply) => self.state.apply_metadata_reply(query, &reply),
            Err(proxy::RawCallError::Device {
                error: wire_rpc::RpcError::NotFound,
                ..
            }) => self.state.give_up(query),
            Err(proxy::RawCallError::Device { .. })
            | Err(proxy::RawCallError::InvalidRoute)
            | Err(proxy::RawCallError::RequestNotSubmitted)
            | Err(proxy::RawCallError::Timeout)
            | Err(proxy::RawCallError::DeviceDisconnected)
            | Err(proxy::RawCallError::ProxyClosed) => self.state.back_off(query),
        }
    }

    /// Ask each route that is due for whatever metadata the parser still wants.
    fn submit_metadata_queries(&mut self) {
        for route in self.state.metadata_routes(Instant::now()) {
            for query in self.state.parser.take_metadata_queries_for(route) {
                match self
                    .endpoint
                    .submit(route, wire::METADATA_RPC_METHOD, &query.args())
                {
                    Ok(pending) => self.metadata_calls.push((query, pending)),
                    Err(error) => self.complete_metadata(query, Err(error)),
                }
            }
        }
    }

    fn publish(&mut self) {
        while let Some(batch) = self.state.pop_batch() {
            self.batches.retain_mut(|sink| sink.publish(&batch));
        }
        while let Some(event) = self.state.pop_event() {
            self.events.retain_mut(|sink| sink.send(event.clone()));
        }
        self.batches
            .retain_mut(|sink| sink.flush() && sink.sink.is_live());
        self.events
            .retain_mut(|sink| sink.report_lag() && sink.is_live());
    }

    /// Hand over whatever the backlogs still hold before the sinks drop.
    fn close(&mut self) {
        for sink in &mut self.batches {
            sink.backlog.finish_buffered_batch();
            sink.flush();
        }
    }

    /// Sleep until something the pump cares about happens.
    fn wait(&self, commands_open: bool) {
        let mut select = channel::Select::new();
        select.recv(self.data.receiver());
        for (_, pending) in &self.metadata_calls {
            select.recv(pending);
        }
        for sink in &self.batches {
            select.recv(&sink.sink.alive);
        }
        for sink in &self.events {
            select.recv(&sink.alive);
        }
        if commands_open {
            select.recv(&self.commands);
        }
        match self.state.next_retry() {
            Some(deadline) => {
                let _ = select.ready_deadline(deadline);
            }
            None => {
                select.ready();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tio::proto::{DataType, Packet};
    use crate::tio::proxy_core::ProxyCommand;
    use twinleaf_proto::rpc::Method;
    use twinleaf_proto::sync::Epoch;
    use twinleaf_proto::SessionId as WireSessionId;

    /// A pump over a port with no proxy behind it: the far end that delivers
    /// packets to it, and the far end that answers its RPCs.
    fn pump() -> (
        PumpHandle,
        channel::Sender<Packet>,
        channel::Receiver<ProxyCommand>,
        proxy::RpcEndpoint,
    ) {
        let (endpoint, commands) = proxy::RpcEndpoint::test_pair(DeviceRoute::root(), 1);
        let (data, _sent, deliver) = proxy::Port::test_pair();
        let (subscriptions, received) = channel::bounded(SUBSCRIBE_QUEUE_LEN);
        let pump = Pump {
            state: StreamState::new(DeviceRoute::root()),
            endpoint: endpoint.clone(),
            data,
            commands: received,
            metadata_calls: Vec::new(),
            batches: Vec::new(),
            events: Vec::new(),
        };
        thread::spawn(move || pump.run());
        (
            PumpHandle {
                commands: subscriptions,
            },
            deliver,
            commands,
            endpoint,
        )
    }

    /// A batch subscription too small for a paused consumer to keep up with.
    fn tiny_subscription(handle: &PumpHandle) -> Receiver<SampleBatch> {
        let (sink, receiver) = Sink::new(2);
        handle
            .commands
            .try_send(PumpCommand::Batches(sink))
            .expect("the pump takes the subscription");
        receiver
    }

    fn samples(first: u32, route: DeviceRoute) -> Packet {
        Packet::samples(1, 0, first, &[0; 4], route).expect("valid samples")
    }

    /// The four records that let the parser decode one f32 stream.
    fn metadata_records() -> [wire::Metadata<'static>; 4] {
        [
            wire::Metadata::Device(wire::Device {
                session: WireSessionId::new(1),
                n_streams: 1,
                name: "d",
                serial: "s",
                firmware: "f",
            }),
            wire::Metadata::Stream(wire::Stream {
                stream_id: 1,
                n_columns: 1,
                n_segments: 1,
                sample_size: 4,
                buf_samples: 128,
                name: "stream",
            }),
            wire::Metadata::Segment(wire::Segment {
                stream_id: 1,
                segment_id: 0,
                flags: wire::SegmentFlags::default(),
                epoch: Epoch::UNIX,
                timeref_serial: "clock",
                timeref_session: WireSessionId::new(7),
                start_time: 0,
                sampling_rate: 1,
                decimation: 1,
                filter_cutoff: 0.0,
                filter_type: wire::FilterType::NONE,
            }),
            wire::Metadata::Column(wire::Column {
                stream_id: 1,
                index: 0,
                data_type: DataType::F32,
                name: "col",
                units: "",
                description: "",
            }),
        ]
    }

    fn describe(deliver: &channel::Sender<Packet>, route: DeviceRoute) {
        for record in metadata_records() {
            deliver
                .send(
                    Packet::metadata(record, wire::MetadataFlags::default(), route)
                        .expect("one record fits"),
                )
                .expect("the pump is listening");
        }
    }

    /// The `dev.metadata` call the pump submitted: its route, its args, and the
    /// one-shot that answers it.
    fn metadata_call(
        commands: &channel::Receiver<ProxyCommand>,
    ) -> (DeviceRoute, Vec<u8>, channel::Sender<proxy::RawCallResult>) {
        let ProxyCommand::Call {
            request, result, ..
        } = commands
            .recv_timeout(Duration::from_secs(5))
            .expect("the pump submits the query")
        else {
            panic!("expected a direct RPC command");
        };
        let route = request.route();
        let proto::Payload::RpcRequest(rpc) = request.payload() else {
            panic!("expected an RPC request");
        };
        assert_eq!(
            rpc.method,
            Method::ByName(wire::METADATA_RPC_METHOD.as_bytes())
        );
        assert_eq!(
            rpc.id.value(),
            0,
            "the proxy, not the pump, allocates request ids"
        );
        (route, rpc.args.to_vec(), result)
    }

    /// One `dev.metadata` reply frame describing a one-stream device.
    fn device_reply() -> Vec<u8> {
        let mut record = [0u8; 64];
        let len = metadata_records()[0]
            .write_reply_frame(&mut record)
            .expect("one record fits");
        record[..len].to_vec()
    }

    #[test]
    fn a_paused_consumer_never_stalls_the_pump_and_learns_it_lagged() {
        let (handle, deliver, commands, endpoint) = pump();
        let batches = tiny_subscription(&handle);
        let root = DeviceRoute::root();
        describe(&deliver, root);

        // Nothing is receiving, and the port's own queue is far smaller than
        // this: a pump that only ran while a consumer pulled would block here.
        let mut sample_number = 0..;
        for first in sample_number.by_ref().take(2 * LAG_MERGE_ROWS) {
            deliver
                .send_timeout(samples(first, root), Duration::from_secs(5))
                .expect("the pump keeps draining while nothing receives");
        }

        // An RPC issued while the pump is buried in samples still completes.
        let pending = endpoint
            .submit(root, "dev.name", b"")
            .expect("the endpoint accepts the call");
        let result = loop {
            let ProxyCommand::Call {
                request, result, ..
            } = commands
                .recv_timeout(Duration::from_secs(5))
                .expect("the call reaches the proxy")
            else {
                panic!("expected a direct RPC command");
            };
            let proto::Payload::RpcRequest(rpc) = request.payload() else {
                panic!("expected an RPC request");
            };
            if rpc.method == Method::ByName(b"dev.name") {
                break result;
            }
        };
        result.send(Ok(b"ASM".to_vec())).unwrap();
        assert_eq!(
            pending
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            b"ASM"
        );

        let mut lagged = 0u64;
        for first in sample_number.take(100) {
            let _ = deliver.send_timeout(samples(first, root), Duration::from_secs(5));
            match batches.recv_timeout(Duration::from_secs(1)) {
                Ok(_) => {}
                Err(RecvTimeoutError::Lagged(skipped)) => {
                    lagged += skipped;
                    break;
                }
                Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        assert!(lagged > 0, "the consumer is told how much it missed");
    }

    #[test]
    fn metadata_discovery_completes_over_independent_calls() {
        let (handle, deliver, commands, _endpoint) = pump();
        let events = handle.events().expect("the pump takes the subscription");
        let child: DeviceRoute = "/1".parse().unwrap();
        deliver.send(samples(0, DeviceRoute::root())).unwrap();
        deliver.send(samples(0, child)).unwrap();

        // Both routes bootstrap at once; answering them out of order must still
        // reach the right parser state.
        let first = metadata_call(&commands);
        let second = metadata_call(&commands);
        assert_ne!(first.0, second.0, "one query per route");
        assert!(first.1.is_empty() && second.1.is_empty(), "both bootstrap");
        second.2.send(Ok(device_reply())).unwrap();
        first.2.send(Ok(device_reply())).unwrap();

        let wanted: Vec<Vec<u8>> = (0..2).map(|_| metadata_call(&commands).1).collect();
        assert!(
            wanted
                .iter()
                .all(|args| *args == wire::MetadataSelector::stream(1).encode()),
            "each reply reached its own route, which now wants only its stream"
        );
        drop(events);
    }

    #[test]
    fn a_route_without_dev_metadata_is_asked_once() {
        let (handle, deliver, commands, _endpoint) = pump();
        let events = handle.events().expect("the pump takes the subscription");
        deliver.send(samples(0, DeviceRoute::root())).unwrap();

        let (_, _, result) = metadata_call(&commands);
        result
            .send(Err(proxy::RawCallError::Device {
                error: wire_rpc::RpcError::NotFound,
                message: Vec::new(),
            }))
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        let reported = std::iter::from_fn(|| events.recv_deadline(deadline).ok()).any(|event| {
            matches!(
                event,
                TreeEvent::Device {
                    event: DeviceEvent::MetadataUnavailable,
                    ..
                }
            )
        });
        assert!(reported, "the refusal is reported, not retried forever");
        assert!(
            commands.recv_timeout(Duration::from_millis(500)).is_err(),
            "discovery does not re-arm for a route with no dev.metadata"
        );
        drop(deliver);
    }

    #[test]
    fn dropping_the_last_receiver_stops_the_pump() {
        let (handle, deliver, commands, _endpoint) = pump();
        let batches = handle.batches().expect("the pump takes the subscription");
        drop(handle);
        drop(batches);

        // A silent device gives the pump nothing to notice a dead subscriber
        // by, so it has to wake on the receiver itself going away.
        for _ in 0..500 {
            if deliver
                .send(Packet::heartbeat(DeviceRoute::root()))
                .is_err()
            {
                drop(commands);
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("the pump kept running with no subscribers");
    }

    #[test]
    fn a_disconnect_terminates_every_receiver() {
        let (handle, deliver, _commands, _endpoint) = pump();
        let batches = handle.batches().expect("the pump takes the subscription");
        let events = handle.events().expect("the pump takes the subscription");
        drop(deliver);

        assert!(batches.iter().last().is_none_or(|last| last.is_err()));
        assert!(matches!(events.recv(), Err(RecvError::Disconnected)));
    }
}

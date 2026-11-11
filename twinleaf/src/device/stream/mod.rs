//! The stream pump, one task per connection.
//!
//! The pump takes what the session's inlet carries in the order it arrived,
//! and sends packets and sample batches to owned [`Receiver`]s, each filtered
//! to the subtree its view covers. The state of each device comes from the
//! proxy alone. The pump watches each device a receiver follows with
//! `proxy.watch`, and keeps what the proxy sends in the connection's
//! [`Store`]. The proxy sends a metadata update ahead of every packet its
//! device sent after the metadata changed. The update reaches the parser
//! too, which decodes the samples after it with it. The proxy's other
//! updates may overtake the device's packets, as each is the latest value of
//! one field of the store.

#[cfg(test)]
mod tests;

use crate::chan::lock;
use crate::data::{PacketParser, SampleBatch};
use crate::proto::DeviceRoute;
use crate::session::{
    encode, end_sinks, prune, pruned, BatchLane, End, Field, Handle, Inlet, Joined, Lane, Queued,
    Received, Receiver, Scope, Sink, Store, Update, Watch, UNWATCH, WATCH,
};
use crate::tio;
use crate::tio::packet::{self, LinkStatus, Packet, ProxyRx};
use crate::tio::transport;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc;

/// How many batches a subscriber's queue holds before the pump drops more.
const BATCH_QUEUE_LEN: usize = 1024;

/// How many packets a raw tap's queue holds, enough for a recorder's disk
/// stall.
const PACKET_QUEUE_LEN: usize = 8192;

/// How many packets the inlet holds, as many as a socket's own queue.
const INLET_LEN: usize = transport::DEFAULT_RX_CHANNEL_SIZE;

/// A subscription to the pump's batches or packets, on its way to the pump.
enum Tap {
    Batches(Sink<BatchLane>),
    Packets(Sink<Lane<tio::Packet>>),
}

/// The connection's one stream, shared by every view. Its pump starts at the
/// first subscription and owns the session's inlet and the parser.
pub(super) struct Stream {
    root: Handle,
    store: Arc<Mutex<Store>>,
    pump: OnceLock<mpsc::UnboundedSender<Tap>>,
}

impl Stream {
    /// Creates the stream of the session behind `root`, with no pump yet.
    pub(super) fn new(root: Handle) -> Stream {
        Stream {
            root,
            store: Arc::new(Mutex::new(Store::new(false))),
            pump: OnceLock::new(),
        }
    }

    /// Subscribes to the sample batches in `scope`.
    pub(super) fn batches(self: &Arc<Stream>, scope: Scope) -> Receiver<SampleBatch> {
        let (sink, receiver) = Sink::batches(BATCH_QUEUE_LEN, scope);
        let _ = self.pump().send(Tap::Batches(sink));
        receiver.tied_to(self)
    }

    /// Subscribes to the packets in `scope`.
    pub(super) fn packets(self: &Arc<Stream>, scope: Scope) -> Receiver<tio::Packet> {
        let (sink, receiver) = Sink::new(PACKET_QUEUE_LEN, scope);
        let _ = self.pump().send(Tap::Packets(sink));
        receiver.tied_to(self)
    }

    /// Subscribes to the sorted list of the routes heard in `scope`.
    pub(super) fn routes(self: &Arc<Stream>, scope: Scope) -> Receiver<Vec<DeviceRoute>> {
        self.pump();
        lock(&self.store).follow_routes(scope).tied_to(self)
    }

    /// Subscribes to the `field` of the device at `route`.
    pub(super) fn state<T: Clone>(
        self: &Arc<Stream>,
        route: DeviceRoute,
        field: Field<T>,
    ) -> Receiver<T> {
        self.pump();
        lock(&self.store).follow(route, field).tied_to(self)
    }

    /// Returns the sender of the pump's new taps, starting the pump on the
    /// first call, on the runtime of the session's task. Only one pump takes
    /// the session's inlet.
    fn pump(&self) -> &mpsc::UnboundedSender<Tap> {
        self.pump.get_or_init(|| {
            let (taps, received) = mpsc::unbounded_channel();
            let session = self.root.session();
            let attached = session.attach(INLET_LEN);
            let pump = Pump::new(&self.root, attached, Arc::clone(&self.store), received);
            session.tasks().spawn("twinleaf stream", pump.run());
            taps
        })
    }
}

/// The pump itself. It owns the session's inlet, the parser and the
/// watches, and runs whether or not anything receives from it.
struct Pump {
    root: Handle,
    /// What the session carries, with the replies to the pump's control
    /// requests, and `None` each time the session's socket is lost, which
    /// ends every watch.
    inlet: Inlet,
    parser: PacketParser,
    store: Arc<Mutex<Store>>,
    taps: mpsc::UnboundedReceiver<Tap>,
    batches: Vec<Sink<BatchLane>>,
    packets: Vec<Sink<Lane<tio::Packet>>>,
    /// Each watch the pump sent, by its id, and what has arrived of its
    /// next update.
    watches: BTreeMap<u16, (Watch, Joined)>,
    next_id: u16,
}

impl Pump {
    fn new(
        root: &Handle,
        inlet: Inlet,
        store: Arc<Mutex<Store>>,
        taps: mpsc::UnboundedReceiver<Tap>,
    ) -> Pump {
        Pump {
            root: root.clone(),
            inlet,
            parser: PacketParser::new(false),
            store,
            taps,
            batches: Vec::new(),
            packets: Vec::new(),
            watches: BTreeMap::new(),
            next_id: 0,
        }
    }

    /// Runs until the session ends or the stream is dropped. Then every
    /// device is lost, and every receiver ends, saying why the session
    /// ended, as does each new one until the stream drops.
    async fn run(mut self) {
        let followed = Arc::clone(&lock(&self.store).followed);
        let end = loop {
            // A tap takes the packets that arrive after it was made, so new
            // taps and pruning go ahead of the inlet.
            tokio::select! {
                biased;
                tap = self.taps.recv() => match tap {
                    Some(Tap::Batches(sink)) => self.batches.push(sink),
                    Some(Tap::Packets(sink)) => self.packets.push(sink),
                    None => return,
                },
                () = followed.notified() => {}
                () = pruned(|cx| {
                    let store = lock(&self.store).prune_gone(cx);
                    store | prune(&mut self.batches, cx) | prune(&mut self.packets, cx)
                }) => {}
                received = self.inlet.recv_async() => {
                    let Ok(first) = received else { break None };
                    if let Some(end) = self.take_input(first) {
                        break Some(end);
                    }
                }
            }
            self.reconcile();
        };
        {
            let mut store = lock(&self.store);
            store.mark_lost();
            store.end_all(end);
        }
        end_batches(&mut self.batches, end);
        end_sinks(&mut self.packets, end);
        while let Some(tap) = self.taps.recv().await {
            match tap {
                Tap::Batches(sink) => end_batches(&mut vec![sink], end),
                Tap::Packets(sink) => end_sinks(&mut vec![sink], end),
            }
        }
    }

    /// Watches each device a receiver follows that is not watched yet, and
    /// stops each watch no receiver follows, dropping what it kept. A device
    /// whose batches a tap takes counts as followed, as its metadata decodes
    /// them.
    fn reconcile(&mut self) {
        let session = self.root.session();
        let mut store = lock(&self.store);
        let mut wanted = store.demand();
        for sink in &self.batches {
            let covered = store
                .routes()
                .iter()
                .filter(|route| sink.scope.covers(**route));
            let routes = std::iter::once(sink.scope.route).chain(covered.copied());
            wanted.extend(routes.map(|route| Watch {
                route,
                table: false,
            }));
        }
        self.watches.retain(|id, (watch, _)| {
            if wanted.remove(watch) {
                return true;
            }
            if let Ok(request) = Packet::proxy_request(UNWATCH, &encode(id), *id) {
                session.send(request);
            }
            store.clear(*watch);
            false
        });
        for watch in wanted {
            let id = loop {
                self.next_id = self.next_id.wrapping_add(1);
                if !self.watches.contains_key(&self.next_id) {
                    break self.next_id;
                }
            };
            match Packet::proxy_request(WATCH, &encode(&watch), id) {
                Ok(request) => session.send(request),
                Err(error) => log::warn!("could not watch {}: {error}", watch.route),
            }
            self.watches.insert(id, (watch, Joined::default()));
        }
    }

    /// Drops every watch, as the session's socket is lost, and makes every
    /// device lost. The raw taps and the parser get a disconnect status at
    /// the root, as a link's loss gives them. The parser learns each device's
    /// metadata again and keeps its last rows, so the samples after report
    /// what the outage lost, or a new session, against them. The watches go
    /// out again at once, and the session sends them once it is open again.
    fn lose(&mut self) {
        // A recording marks the outage as it marks a link's loss.
        let lost = Packet::link(LinkStatus::SensorDisconnected);
        self.tap(&lost);
        lock(&self.store).mark_lost();
        self.watches.clear();
        self.parse(&lost);
    }

    /// Takes a reply to one of the pump's watches, a part of an update or a
    /// refusal. A refused table watch is a table that did not load. A
    /// refused device watch ends what follows the device, and the batches of
    /// a tap on it.
    fn take_reply(&mut self, reply: &Packet) {
        let packet::Payload::ProxyRx(reply) = reply.payload() else {
            return;
        };
        let (id, value) = match reply {
            ProxyRx::Reply(reply) => (reply.req_id.value(), reply.value),
            ProxyRx::Error(error) => {
                let Some((watch, _)) = self.watches.get(&error.req_id.value()) else {
                    return;
                };
                let Watch { route, table } = *watch;
                let message = String::from_utf8_lossy(error.message);
                log::warn!("the proxy follows no state of {route}: {message}");
                let mut store = lock(&self.store);
                if table {
                    let refused = Update::Table(Err((error.code, error.message.to_vec().into())));
                    return store.apply(route, refused, &mut self.parser);
                }
                store.end_device(route);
                self.batches.retain(|sink| sink.scope.route != route);
                self.watches.remove(&error.req_id.value());
                return;
            }
            ProxyRx::Link(_) | ProxyRx::Alive | ProxyRx::Dropped { .. } | ProxyRx::Unknown(..) => {
                return
            }
        };
        let Some((watch, joined)) = self.watches.get_mut(&id) else {
            return;
        };
        if let Some(update) = joined.join(value) {
            lock(&self.store).apply(watch.route, update, &mut self.parser);
        }
    }

    /// Takes `first` from the inlet, and what else the inlet held then, up
    /// to the end.
    fn take_input(&mut self, first: Queued<Received>) -> Option<End> {
        let inlet = self.inlet.clone();
        let held = inlet.try_iter().take(inlet.len());
        std::iter::once(first)
            .chain(held)
            .find_map(|queued| self.take(queued))
    }

    /// Takes one entry of the inlet, a packet, a reply to a watch or the
    /// loss of the socket, or reports the gap the session left. Returns why
    /// the session ended, at its end. The proxy's count of the samples it
    /// dropped for this connection goes to the parser as a packet does, and
    /// a gap is told to it, so the samples after say where the missing ones
    /// went. The packets of a route a tap covers
    /// reach the parser, but for the device's own metadata records, as the
    /// parser takes metadata from the proxy's metadata updates alone. Until
    /// the first metadata update of a device, the parser waits for metadata
    /// and drops the device's samples.
    fn take(&mut self, queued: Queued<Received>) -> Option<End> {
        let packet = &match queued {
            Queued::Item(Received::Packet(packet)) => packet,
            Queued::Item(Received::SocketLost) => {
                self.lose();
                return None;
            }
            // The parser marks the gap in the samples, as this connection's.
            Queued::Gap(lost) => {
                log::warn!("the stream's inlet overflowed and {lost} packets were lost");
                self.parser.note_dropped_packets();
                return None;
            }
            Queued::Ended(end) => return Some(end),
        };
        if let packet::Payload::ProxyRx(ProxyRx::Reply(_) | ProxyRx::Error(_)) = packet.payload() {
            self.take_reply(packet);
            return None;
        }
        self.tap(packet);
        let covered = self
            .batches
            .iter()
            .any(|sink| sink.scope.covers(packet.route()));
        if !covered || matches!(packet.payload(), packet::Payload::Metadata(..)) {
            return None;
        }
        self.parse(packet);
        None
    }

    /// Hands `packet` to the parser, and offers each batch it completes to
    /// the batch sinks.
    fn parse(&mut self, packet: &Packet) {
        if let Err(error) = self.parser.push_packet(packet) {
            log::warn!("dropping invalid stream packet: {error}");
        }
        while let Some(batch) = self.parser.pop_batch() {
            let scope = Scope::point(batch.route());
            self.batches
                .retain_mut(|sink| sink.offer(scope, || batch.clone()));
        }
        self.batches.retain_mut(|sink| sink.lane.report_lag());
    }

    /// Offers a packet to the raw taps, unparsed and with its absolute route.
    /// A status reaches every tap its subtree overlaps.
    fn tap(&mut self, packet: &tio::Packet) {
        let scope = match packet.link_status() {
            Some(_) => Scope::subtree(packet.route()),
            None => Scope::point(packet.route()),
        };
        self.packets
            .retain_mut(|sink| sink.offer(scope, || packet.clone()));
    }
}

/// Ends each batch sink of `sinks`, saying why if `end` does.
fn end_batches(sinks: &mut Vec<Sink<BatchLane>>, end: Option<End>) {
    for sink in sinks.drain(..) {
        if let Some(end) = end {
            sink.lane.end(end);
        }
    }
}

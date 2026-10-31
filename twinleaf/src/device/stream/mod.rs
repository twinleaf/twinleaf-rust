//! The stream pump, one task per connection.
//!
//! The pump drains the session's inlet into the parser, and sends packets
//! and sample batches to owned [`Receiver`]s, each filtered to the subtree
//! its view covers. The state of each device comes from the proxy alone. The
//! pump watches each device a receiver follows with `proxy.watch`, and keeps
//! what the proxy sends in the connection's [`Store`]. The metadata in it
//! reaches the parser too, which takes metadata from nothing else and
//! decodes the samples with it.

mod subscription;

pub(crate) use subscription::{End, Lane, Queued, Scope, Sink};
pub use subscription::{Receiver, RecvError};

use super::session::{Handle, Inlet};
use super::store::{end_sinks, lock, prune, pruned, Field, Store};
use crate::data::{PacketParser, SampleBatch};
use crate::proto::DeviceRoute;
use crate::proxy::{encode, Joined, Update, Watch, UNWATCH, WATCH};
use crate::rt::Supervisor;
use crate::tio;
use crate::tio::packet::{self, LinkStatus, Packet, ProxyRx};
use crate::tio::transport;
use std::collections::HashMap;
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
    Batches(Sink<SampleBatch>),
    Packets(Sink<tio::Packet>),
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
        let (sink, receiver) = Sink::new(BATCH_QUEUE_LEN, scope);
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
    /// first call. Only one pump takes the session's inlet.
    fn pump(&self) -> &mpsc::UnboundedSender<Tap> {
        self.pump.get_or_init(|| {
            let (taps, received) = mpsc::unbounded_channel();
            let attached = self.root.session().attach(INLET_LEN);
            let pump = Pump::new(&self.root, attached, Arc::clone(&self.store), received);
            Supervisor::new().spawn("twinleaf stream", pump.run());
            taps
        })
    }
}

/// The pump itself. It owns the session's inlet, the parser and the
/// watches, and runs whether or not anything receives from it.
struct Pump {
    root: Handle,
    inlet: Inlet,
    /// The replies to the pump's control requests, each a `PROXY_RX`
    /// packet, and `None` each time the session's socket is lost, which
    /// ends every watch. No reply is ever dropped.
    replies: flume::Receiver<Option<Packet>>,
    parser: PacketParser,
    store: Arc<Mutex<Store>>,
    taps: mpsc::UnboundedReceiver<Tap>,
    batches: Vec<Sink<SampleBatch>>,
    packets: Vec<Sink<tio::Packet>>,
    /// Each watch the pump sent, by its id, and what has arrived of its
    /// next update.
    watches: HashMap<u16, (Watch, Joined)>,
    next_id: u16,
}

impl Pump {
    fn new(
        root: &Handle,
        (inlet, replies): (Inlet, flume::Receiver<Option<Packet>>),
        store: Arc<Mutex<Store>>,
        taps: mpsc::UnboundedReceiver<Tap>,
    ) -> Pump {
        Pump {
            root: root.clone(),
            inlet,
            replies,
            parser: PacketParser::new(false),
            store,
            taps,
            batches: Vec::new(),
            packets: Vec::new(),
            watches: HashMap::new(),
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
            // taps, replies and pruning go ahead of the inlet.
            tokio::select! {
                biased;
                tap = self.taps.recv() => match tap {
                    Some(Tap::Batches(sink)) => self.batches.push(sink),
                    Some(Tap::Packets(sink)) => self.packets.push(sink),
                    None => return,
                },
                Ok(reply) = self.replies.recv_async() => match reply {
                    Some(reply) => self.take_reply(&reply),
                    None => self.lose(),
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
        end_sinks(&mut self.batches, end);
        end_sinks(&mut self.packets, end);
        while let Some(tap) = self.taps.recv().await {
            match tap {
                Tap::Batches(sink) => end_sinks(&mut vec![sink], end),
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
    /// device lost. The raw taps get a disconnect status at the root. The watches go out again at once, and the session sends
    /// them once it is open again. The parser starts again, so the samples
    /// after begin a new run.
    fn lose(&mut self) {
        // A recording marks the outage as it marks a link's loss.
        self.tap(&Packet::link(LinkStatus::SensorDisconnected));
        lock(&self.store).mark_lost();
        self.watches.clear();
        self.parser = PacketParser::new(false);
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
                    return refused.apply(route, &mut store, &mut self.parser);
                }
                store.end_device(route);
                self.batches.retain(|sink| sink.scope.route != route);
                self.watches.remove(&error.req_id.value());
                return;
            }
            ProxyRx::Link(_) | ProxyRx::Alive | ProxyRx::Unknown(..) => return,
        };
        let Some((watch, joined)) = self.watches.get_mut(&id) else {
            return;
        };
        if let Some(update) = joined.join(value) {
            update.apply(watch.route, &mut lock(&self.store), &mut self.parser);
        }
    }

    /// Takes `first` from the inlet, and what else the inlet held then, up
    /// to the end.
    fn take_input(&mut self, first: Queued<tio::Packet>) -> Option<End> {
        let inlet = self.inlet.clone();
        let held = inlet.try_iter().take(inlet.len());
        std::iter::once(first)
            .chain(held)
            .find_map(|queued| self.take(queued))
    }

    /// Takes one packet from the inlet, or reports the gap the session left.
    /// Returns why the session ended, at its end. Only the samples of a
    /// route a tap covers reach the parser.
    fn take(&mut self, queued: Queued<tio::Packet>) -> Option<End> {
        let packet = &match queued {
            Queued::Item(packet) => packet,
            // The parser marks the gap in the samples.
            Queued::Gap(lost) => {
                log::warn!("the stream's inlet overflowed and {lost} packets were lost");
                return None;
            }
            Queued::Ended(end) => return Some(end),
        };
        self.tap(packet);
        let covered = self
            .batches
            .iter()
            .any(|sink| sink.scope.covers(packet.route()));
        if !covered || !matches!(packet.payload(), packet::Payload::Samples(_)) {
            return None;
        }
        if let Err(error) = self.parser.push_packet(packet) {
            log::warn!("dropping invalid stream packet: {error}");
        }
        while let Some(batch) = self.parser.pop_batch() {
            let scope = Scope::point(batch.route());
            self.batches
                .retain_mut(|sink| sink.offer(scope, || batch.clone()));
        }
        self.batches.retain_mut(|sink| sink.lane.report_lag());
        None
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::fixtures::{announcement, description, samples, wait_for};
    use crate::device::session::{Session, TestProxy};
    use crate::device::store::{CHANGES, METADATA, PRESENCE, REGISTRY, SETTINGS};
    use crate::device::{MetadataState, Presence};
    use crate::proto::SessionId as WireSessionId;
    use crate::proxy::{decode, Update};
    use crate::tio::packet::Packet;
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    /// The whole tree, as a root tree view covers it.
    fn everything() -> Scope {
        Scope {
            route: DeviceRoute::root(),
            depth: DeviceRoute::MAX_HOPS,
        }
    }

    fn exactly(route: &str) -> Scope {
        Scope {
            route: route.parse().expect("a valid route"),
            depth: 0,
        }
    }

    /// A stream over a session whose socket is a pair of channels, and the
    /// session's far end, which plays the proxy.
    fn stream() -> (Arc<Stream>, TestProxy) {
        let (session, proxy) = Session::test_pair();
        (Arc::new(Stream::new(Handle::root(session))), proxy)
    }

    /// The control request the pump wrote next, skipping anything else, as
    /// its method, id and arguments.
    fn request(proxy: &TestProxy) -> (String, u16, Vec<u8>) {
        loop {
            let packet = proxy
                .sent
                .recv_timeout(Duration::from_secs(5))
                .expect("the pump makes a request");
            let packet::Payload::ProxyTx(request) = packet.payload() else {
                continue;
            };
            let crate::proto::rpc::Method::ByName(name) = request.method else {
                panic!("a control request names its method");
            };
            let name = String::from_utf8_lossy(name).into_owned();
            return (name, request.id.value(), request.args.to_vec());
        }
    }

    /// The watch the pump requested next, as its id and what it follows.
    fn watch_request(proxy: &TestProxy) -> (u16, Watch) {
        let (name, id, args) = request(proxy);
        assert_eq!(name, WATCH);
        (id, decode(&args).unwrap())
    }

    /// The id of the watch of `watch` the pump requested, skipping others.
    fn watch_of(proxy: &TestProxy, watch: Watch) -> u16 {
        loop {
            let (id, requested) = watch_request(proxy);
            if requested == watch {
                return id;
            }
        }
    }

    fn device(route: DeviceRoute) -> Watch {
        Watch {
            route,
            table: false,
        }
    }

    fn table(route: DeviceRoute) -> Watch {
        Watch { route, table: true }
    }

    /// Hands the pump `update` as the proxy's replies to watch `id`.
    fn reply_update(proxy: &TestProxy, id: u16, update: &Update) {
        for part in crate::proxy::parts(id, update) {
            proxy.send(part);
        }
    }

    /// The update of the metadata of the device in `session`, as the proxy
    /// sends it.
    fn metadata_update(session: u32) -> Update {
        let records = description(DeviceRoute::root(), session).into_iter();
        Update::Metadata(Some(
            records
                .map(|packet| packet.as_bytes().to_vec().into())
                .collect(),
        ))
    }

    fn routes_update(routes: &[DeviceRoute]) -> Update {
        Update::Routes(
            routes
                .iter()
                .map(|route| route.as_slice().to_vec().into())
                .collect(),
        )
    }

    fn table_update(hash: u32) -> Update {
        Update::Table(Ok((
            Some(hash),
            None,
            vec![("test.gain".to_string(), 0xA742)],
        )))
    }

    fn settings_update(hash: u32, version: u32) -> Update {
        let values = BTreeMap::from([("test.gain".to_string(), vec![2u8].into())]);
        Update::Settings(Some(hash), Some(version), values)
    }

    fn lost(presence: &Presence) -> bool {
        matches!(presence, Presence::Lost { .. })
    }

    /// Whether the session wrote anything within `within`.
    fn wrote_within(proxy: &TestProxy, within: Duration) -> bool {
        proxy.sent.recv_timeout(within).is_ok()
    }

    /// The pump decodes batches with the metadata the proxy sends for the
    /// routes the proxy lists in a batch receiver's scope, and the samples
    /// it took before it never decode.
    #[test]
    fn batches_decode_with_the_metadata_the_proxy_sends() {
        let (stream, proxy) = stream();
        let root = DeviceRoute::root();
        let batches = stream.batches(everything());
        let packets = stream.packets(Scope::point(root));
        let list = watch_of(&proxy, device(root));
        proxy.send(samples(0, root));
        packets
            .recv_timeout(Duration::from_secs(5))
            .expect("the pump took it");
        reply_update(&proxy, list, &routes_update(&[root]));
        reply_update(&proxy, list, &metadata_update(1));

        proxy.send(samples(1, root));
        let batch = batches
            .recv_timeout(Duration::from_secs(5))
            .expect("a batch");
        assert_eq!(batch.sample_numbers(), [1]);
        assert!(batch.is_initial());
    }

    /// A heartbeat of the session before reaches the pump after the proxy's
    /// metadata of the new one, and the metadata stays in place, as the
    /// parser takes metadata from the proxy alone.
    #[test]
    fn an_old_heartbeat_behind_the_proxys_metadata_leaves_it_in_place() {
        let (stream, proxy) = stream();
        let root = DeviceRoute::root();
        let batches = stream.batches(everything());
        let metadata = stream.state(root, METADATA);
        let list = watch_of(&proxy, device(root));
        reply_update(&proxy, list, &routes_update(&[root]));
        reply_update(&proxy, list, &metadata_update(2));
        wait_for(&metadata, |state| matches!(state, MetadataState::Ready(_)));

        proxy.send(Packet::heartbeat_session(1, root));
        proxy.send(samples(5, root));
        let batch = batches
            .recv_timeout(Duration::from_secs(5))
            .expect("a batch");
        assert_eq!(batch.sample_numbers(), [5]);
    }

    /// A device's metadata going pending resets that device alone, so the
    /// devices below it, whose metadata the proxy does not send again, go
    /// on decoding.
    #[test]
    fn a_parents_pending_metadata_leaves_its_childrens_in_place() {
        let (stream, proxy) = stream();
        let (root, child) = (DeviceRoute::root(), "/1".parse().unwrap());
        let batches = stream.batches(everything());
        let parent = watch_of(&proxy, device(root));
        reply_update(&proxy, parent, &routes_update(&[root, child]));
        let below = watch_of(&proxy, device(child));
        let metadata = stream.state(child, METADATA);
        reply_update(&proxy, below, &metadata_update(1));
        wait_for(&metadata, |state| matches!(state, MetadataState::Ready(_)));

        reply_update(&proxy, parent, &Update::Metadata(Some(Vec::new())));
        let pending = stream.state(root, METADATA);
        wait_for(&pending, |state| matches!(state, MetadataState::Pending));
        proxy.send(samples(5, child));
        let batch = batches
            .recv_timeout(Duration::from_secs(5))
            .expect("a batch");
        assert_eq!(batch.route(), child);
        assert_eq!(batch.sample_numbers(), [5]);
    }

    /// A table the proxy sends again, with the hash and session of the one
    /// held, keeps the settings read with it.
    #[test]
    fn the_same_table_again_keeps_its_settings() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let settings = stream.state(route, SETTINGS);
        let id = watch_of(&proxy, table(route));
        reply_update(&proxy, id, &table_update(1));
        reply_update(&proxy, id, &settings_update(1, 9));
        settings
            .recv_timeout(Duration::from_secs(5))
            .expect("the settings");

        reply_update(&proxy, id, &table_update(1));
        let late = stream.state(route, SETTINGS);
        let held = late
            .recv_timeout(Duration::from_secs(5))
            .expect("the settings kept");
        assert_eq!(held.version(), Some(9));
    }

    /// When the session's socket is lost, every device the pump follows is
    /// lost and every watch goes out again under a new id, which the session
    /// sends once it is open again. The samples after begin a new run.
    #[test]
    fn a_lost_socket_loses_every_device_and_watches_again() {
        let (session, proxy) = Session::test_pair();
        let root = Handle::root(session);
        let (mut inlet, inlet_received) = Lane::new(INLET_LEN);
        let (replies, replies_received) = flume::unbounded();
        let store = Arc::new(Mutex::new(Store::new(false)));
        let (taps, taps_received) = mpsc::unbounded_channel();
        let pump = Pump::new(
            &root,
            (inlet_received, replies_received),
            Arc::clone(&store),
            taps_received,
        );
        Supervisor::new().spawn("twinleaf stream", pump.run());
        let route = DeviceRoute::root();
        let (sink, batches) = Sink::new(BATCH_QUEUE_LEN, everything());
        let _ = taps.send(Tap::Batches(sink));
        let (sink, packets) = Sink::new(PACKET_QUEUE_LEN, everything());
        let _ = taps.send(Tap::Packets(sink));
        let presence = lock(&store).follow(route, PRESENCE);
        let reply = |id: u16, update: &Update| {
            for part in crate::proxy::parts(id, update) {
                replies.send(Some(part)).unwrap();
            }
        };
        let id = watch_of(&proxy, device(route));
        reply(id, &Update::Presence(Ok(Some(7))));
        reply(id, &routes_update(&[route]));
        reply(id, &metadata_update(1));
        let wait = Duration::from_secs(5);
        wait_for(&presence, |presence| {
            matches!(presence, Presence::Alive { .. })
        });
        inlet.send(samples(0, route));
        assert!(batches.recv_timeout(wait).unwrap().is_initial());

        replies.send(None).unwrap();
        wait_for(&presence, lost);
        let marked = wait_for(&packets, |packet| packet.link_status().is_some());
        assert_eq!(
            marked.link_status(),
            Some(packet::LinkStatus::SensorDisconnected)
        );
        let again = watch_of(&proxy, device(route));
        assert_ne!(again, id, "the watch is new");
        reply(again, &routes_update(&[route]));
        reply(again, &metadata_update(1));
        inlet.send(samples(1, route));
        let resumed = batches.recv_timeout(wait).unwrap();
        assert_eq!(resumed.sample_numbers(), [1]);
        assert!(
            resumed.is_initial(),
            "the samples after the outage continue a run"
        );
    }

    /// A disconnect status that reaches the pump after the proxy's newer
    /// metadata leaves that metadata in place, so the samples after it
    /// decode.
    #[test]
    fn a_status_behind_the_proxys_metadata_leaves_it_in_place() {
        let (stream, proxy) = stream();
        let root = DeviceRoute::root();
        let batches = stream.batches(everything());
        let metadata = stream.state(root, METADATA);
        let list = watch_of(&proxy, device(root));
        reply_update(&proxy, list, &routes_update(&[root]));
        reply_update(&proxy, list, &metadata_update(2));
        wait_for(&metadata, |state| matches!(state, MetadataState::Ready(_)));

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        proxy.send(samples(5, root));
        let batch = batches
            .recv_timeout(Duration::from_secs(5))
            .expect("a batch");
        assert_eq!(batch.sample_numbers(), [5]);
    }

    #[test]
    fn a_paused_consumer_never_stalls_the_pump_and_learns_it_lagged() {
        let (stream, proxy) = stream();
        let (root, marker) = (DeviceRoute::root(), "/9".parse().unwrap());
        let (sink, batches) = Sink::new(2, everything());
        let _ = stream.pump().send(Tap::Batches(sink));
        let packets = stream.packets(Scope::point(marker));
        let list = watch_of(&proxy, device(root));
        reply_update(&proxy, list, &routes_update(&[root]));
        reply_update(&proxy, list, &metadata_update(1));

        // Nothing is receiving, and the receiver has room for two batches:
        // a pump that only ran while a consumer pulled would stop there.
        let mut sample_number = 0..;
        for first in sample_number.by_ref().take(4096) {
            proxy.send(samples(first, root));
        }
        proxy.send(Packet::heartbeat(marker));

        // An RPC issued while the pump is buried in samples still completes.
        let pending = stream.root.submit(root, "dev.name", b"");
        let (route, id) = loop {
            let packet = proxy
                .sent
                .recv_timeout(Duration::from_secs(5))
                .expect("the RPC");
            if let packet::Payload::RpcRequest(request) = packet.payload() {
                break (packet.route(), request.id.value());
            }
        };
        proxy.reply(id, b"ASM", route);
        let named = pending.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(named.unwrap(), b"ASM");

        // The consumer stays paused until the pump has taken every sample,
        // which it has once it taps the marker sent after them.
        packets
            .recv_timeout(Duration::from_secs(5))
            .expect("the marker");
        let mut lagged = 0u64;
        for first in sample_number.take(100) {
            proxy.send(samples(first, root));
            match batches.recv_timeout(Duration::from_secs(1)) {
                Ok(_) => {}
                Err(RecvError::Lagged(skipped)) => {
                    lagged += skipped;
                    break;
                }
                Err(_) => break,
            }
        }
        assert!(lagged > 0, "the consumer is told how much it missed");
    }

    /// When the session ends, every device the pump follows is lost, and
    /// then every receiver ends.
    #[test]
    fn a_disconnect_loses_every_device_then_ends_every_receiver() {
        let (stream, proxy) = stream();
        let batches = stream.batches(everything());
        let presence = stream.state(DeviceRoute::root(), PRESENCE);
        let id = watch_of(&proxy, device(DeviceRoute::root()));
        reply_update(&proxy, id, &Update::Presence(Ok(None)));
        wait_for(&presence, |presence| !lost(presence));

        // The socket closes, which ends the session for good.
        drop(proxy);

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut lose = false;
        let ended = loop {
            match presence.recv_deadline(deadline) {
                Ok(now) => lose |= lost(&now),
                Err(ended) => break ended,
            }
        };
        assert!(lose, "the device was not lost with the session");
        assert_eq!(ended, RecvError::Disconnected);
        loop {
            match batches.recv_deadline(deadline) {
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Disconnected) => break,
                Err(other) => panic!("the batches ended with {other:?}"),
            }
        }
    }

    /// An inlet overrun is only logged, as the samples mark the gap. The
    /// stream goes on and keeps what it knew.
    #[test]
    fn the_stream_goes_on_after_an_inlet_overrun() {
        let (session, proxy) = Session::test_pair();
        let root = Handle::root(session);
        let (inlet, packets) = flume::unbounded();
        let (_replies, replied) = flume::unbounded();
        let store = Arc::new(Mutex::new(Store::new(true)));
        let (taps, received) = mpsc::unbounded_channel();
        let pump = Pump::new(&root, (packets, replied), Arc::clone(&store), received);
        Supervisor::new().spawn("twinleaf stream", pump.run());
        let (sink, tapped) = Sink::new(PACKET_QUEUE_LEN, everything());
        taps.send(Tap::Packets(sink)).unwrap();
        let deliver = |packet| inlet.send(Queued::Item(packet)).unwrap();

        deliver(samples(0, DeviceRoute::root()));
        inlet.send(Queued::Gap(1)).unwrap();
        deliver(samples(2, DeviceRoute::root()));

        for first in [0, 2] {
            let packet = tapped.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(
                packet.as_bytes(),
                samples(first, DeviceRoute::root()).as_bytes()
            );
        }
        drop(proxy);
    }

    /// A stop at the root is the session's last packet. The pump takes it,
    /// then ends, though the socket is still open. Every receiver says the
    /// proxy stopped, even one that carries no statuses and one that
    /// subscribed after.
    #[test]
    fn a_stop_at_the_root_is_published_then_ends_the_stream() {
        let (stream, proxy) = stream();
        let root = DeviceRoute::root();
        let presence = stream.state(root, PRESENCE);
        let batches = stream.batches(everything());
        let id = watch_of(&proxy, device(root));
        reply_update(&proxy, id, &Update::Presence(Ok(None)));
        wait_for(&presence, |presence| !lost(presence));

        proxy.send(Packet::link(packet::LinkStatus::Stopped));

        wait_for(&presence, lost);
        let wait = Duration::from_secs(5);
        assert_eq!(presence.recv_timeout(wait).err(), Some(RecvError::Stopped));
        assert_eq!(batches.recv_timeout(wait).err(), Some(RecvError::Stopped));
        let late = stream.batches(everything());
        assert_eq!(late.recv_timeout(wait).err(), Some(RecvError::Stopped));
        let state = stream.state(root, METADATA);
        assert_eq!(state.recv_timeout(wait).err(), Some(RecvError::Stopped));
        drop(proxy);
    }

    /// A subscriber whose queue is full still hears the stop, after what was
    /// queued.
    #[test]
    fn a_full_queue_still_ends_with_the_stop() {
        let (mut sink, receiver) = Sink::new(2, everything());
        assert!((0..3).all(|item| sink.lane.send(item)));

        sink.lane.end(End::Stopped);

        assert_eq!(receiver.try_recv(), Ok(Some(0)));
        assert_eq!(receiver.try_recv(), Ok(Some(1)));
        assert_eq!(receiver.try_recv(), Err(RecvError::Stopped));
        assert_eq!(receiver.try_recv(), Err(RecvError::Disconnected));
    }

    /// The status saying why the link failed may be the last thing the
    /// session queues, so the pump must deliver what the inlet still holds
    /// before ending.
    #[test]
    fn the_last_packets_a_session_queued_are_still_delivered() {
        let (stream, proxy) = stream();
        let packets = stream.packets(everything());
        proxy.send(Packet::heartbeat(DeviceRoute::root()));
        proxy.send(Packet::link(packet::LinkStatus::FailedToReconnect));
        drop(proxy);

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut heard = false;
        let ended = loop {
            match packets.recv_deadline(deadline) {
                Ok(packet) => heard |= packet.link_status().is_some(),
                Err(ended) => break ended,
            }
        };
        assert!(heard, "the link's last status was not delivered");
        assert_eq!(ended, RecvError::Disconnected);
    }

    /// The pump ends once nothing can reach it any more: no view left to
    /// subscribe, and no receiver left holding the stream open.
    #[test]
    fn dropping_the_last_view_and_receiver_ends_the_pump() {
        let (stream, proxy) = stream();
        let batches = stream.batches(everything());
        let _ = watch_request(&proxy);

        drop((stream, batches));

        // A silent device gives the pump nothing else to wake on, so it has
        // to wake on the taps going away. Once the pump is gone nothing holds
        // the session, which closes its socket.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match proxy.sent.recv_deadline(deadline) {
                Ok(_) => {}
                Err(flume::RecvTimeoutError::Disconnected) => break,
                Err(flume::RecvTimeoutError::Timeout) => panic!("the session outlived the pump"),
            }
        }
    }

    /// A raw tap sees the wire, absolutely routed and filtered to its scope,
    /// and asks nothing of anyone. A status filters as the subtree at its
    /// route: it is a recorded stream's reset marker.
    #[test]
    fn a_packet_tap_is_filtered_absolute_and_passive() {
        let (stream, proxy) = stream();
        let taps = stream.packets(everything());
        let scoped = stream.packets(exactly("/1"));
        let root = DeviceRoute::root();
        let child: DeviceRoute = "/1".parse().unwrap();

        proxy.send(Packet::link(packet::LinkStatus::SensorReconnected));
        proxy.send(samples(0, root));
        proxy.send(samples(7, child));

        let status = taps.recv_timeout(Duration::from_secs(5)).expect("a packet");
        assert!(status.link_status().is_some());
        let first = taps.recv_timeout(Duration::from_secs(5)).expect("a packet");
        assert_eq!(first.route(), root);
        assert!(matches!(first.payload(), packet::Payload::Samples(_)));
        let next = taps.recv_timeout(Duration::from_secs(5)).expect("a packet");
        assert_eq!(next.route(), child);

        let status = scoped.recv_timeout(Duration::from_secs(5)).expect("packet");
        assert!(
            status.link_status().is_some(),
            "the whole link's status reaches a tap scoped inside it"
        );
        let only = scoped.recv_timeout(Duration::from_secs(5)).expect("packet");
        assert_eq!(only.route(), child, "a tap only sees what its scope covers");
        assert!(matches!(
            scoped.recv_timeout(Duration::from_millis(200)),
            Err(RecvError::Timeout)
        ));
        assert!(
            !wrote_within(&proxy, Duration::from_millis(500)),
            "a raw tap watches nothing and asks nothing"
        );
    }

    /// A receiver is the subscription: it keeps the pump running on its own,
    /// so a caller need not hold the view it was minted from.
    #[test]
    fn a_receiver_keeps_the_pump_running_without_its_view() {
        let (stream, proxy) = stream();
        let presence = stream.state(DeviceRoute::root(), PRESENCE);
        drop(stream);

        let id = watch_of(&proxy, device(DeviceRoute::root()));
        reply_update(&proxy, id, &Update::Presence(Ok(None)));
        wait_for(&presence, |presence| {
            *presence == Presence::Alive { session: None }
        });
    }

    /// A channel consults a deadline only once it runs dry, so a lane that
    /// stays saturated would deliver items past the caller's bound forever.
    #[test]
    fn a_saturated_subscription_still_honours_an_elapsed_deadline() {
        let (mut sink, receiver) = Sink::new(2, everything());
        assert!(sink.lane.send(1));
        assert!(sink.lane.send(2));

        assert!(matches!(
            receiver.recv_deadline(Instant::now()),
            Err(RecvError::Timeout)
        ));
        assert!(
            receiver.recv().is_ok(),
            "the bound reports the time, it does not shed the queue"
        );
    }

    /// A stream whose session already ended hands back receivers that say
    /// so, rather than ones that wait on a pump that will never run. With no
    /// stop heard, that is only that the stream ended.
    #[test]
    fn subscribing_to_an_ended_session_yields_a_disconnected_receiver() {
        let (stream, proxy) = stream();
        drop(proxy);

        assert!(matches!(
            stream
                .state(DeviceRoute::root(), CHANGES)
                .recv_timeout(Duration::from_secs(5)),
            Err(RecvError::Disconnected)
        ));
        assert!(matches!(
            stream
                .batches(everything())
                .recv_timeout(Duration::from_secs(5)),
            Err(RecvError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn an_async_receive_returns_a_queued_item() {
        let (mut sink, receiver) = Sink::new(1, everything());
        assert!(sink.lane.send(7));

        assert_eq!(receiver.recv_async().await, Ok(7));
    }

    #[tokio::test]
    async fn an_async_receive_waits_for_an_item_another_thread_sends_later() {
        let (mut sink, receiver) = Sink::new(1, everything());
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            sink.lane.send(7)
        });

        assert_eq!(receiver.recv_async().await, Ok(7));
        assert!(sender.join().unwrap());
    }

    /// A latest-value lane keeps only the newest item, so a reader that fell
    /// behind skips to it and is never told it lagged.
    #[test]
    fn a_latest_value_reader_skips_to_the_newest_item() {
        let (mut sink, receiver) = Sink::latest(everything());
        for value in 1..=5 {
            assert!(sink.lane.send(value));
        }

        assert_eq!(receiver.recv(), Ok(5));
        assert_eq!(receiver.try_recv(), Ok(None));

        assert!(sink.lane.send(6));
        assert_eq!(receiver.recv(), Ok(6));
    }

    /// A lane being replaced as fast as another thread can never looks empty
    /// to a reader who has not taken its item, and a reader who takes items
    /// meanwhile ends on the last one sent.
    #[test]
    fn a_latest_value_lane_never_looks_empty_while_it_is_replaced() {
        const SENT: u32 = 100_000;
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(0));
        let sender = std::thread::spawn(move || {
            for value in 1..=SENT {
                sink.lane.send(value);
            }
            sink
        });
        while !sender.is_finished() {
            assert!(!receiver.as_flume().is_empty(), "the lane looked empty");
        }
        let mut sink = sender.join().unwrap();
        assert_eq!(receiver.try_recv(), Ok(Some(SENT)));

        let sender = std::thread::spawn(move || {
            for value in 1..=SENT {
                sink.lane.send(value);
            }
        });
        let mut last = 0;
        while last < SENT {
            let value = receiver.recv().unwrap();
            assert!(value >= last, "{value} came after {last}");
            last = value;
        }
        sender.join().unwrap();
    }

    /// A blocking receive with a deadline times out on an empty lane, and
    /// waits for an item another thread sends while it waits.
    #[test]
    fn a_latest_value_reader_waits_until_a_deadline() {
        let (mut sink, receiver) = Sink::latest(everything());
        assert_eq!(
            receiver.recv_deadline(Instant::now() + Duration::from_millis(20)),
            Err(RecvError::Timeout)
        );

        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            sink.lane.send(7)
        });
        assert_eq!(
            receiver.recv_deadline(Instant::now() + Duration::from_secs(5)),
            Ok(7)
        );
        assert!(sender.join().unwrap());
    }

    #[tokio::test]
    async fn an_async_latest_value_receive_waits_for_the_next_item() {
        let (mut sink, receiver) = Sink::latest(everything());
        let sender = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            sink.lane.send(7)
        });

        assert_eq!(receiver.recv_async().await, Ok(7));
        assert!(sender.join().unwrap());
    }

    /// The sync API may run inside an application's own tokio runtime, where
    /// tokio's `block_on` would panic. A latest-value receive blocks there as
    /// any other receive does.
    #[test]
    fn a_latest_value_receive_blocks_inside_a_tokio_runtime() {
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(3));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("start a runtime");

        let received = runtime.block_on(async { receiver.recv_timeout(Duration::from_secs(5)) });
        assert_eq!(received, Ok(3));
    }

    /// The end has a place of its own, so the last value is still read
    /// before the reader learns why the stream ended.
    #[test]
    fn a_latest_value_lane_delivers_its_last_item_before_its_end() {
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(1));
        sink.lane.end(End::Closed);

        assert_eq!(receiver.recv(), Ok(1));
        assert_eq!(receiver.recv(), Err(RecvError::Closed));
    }

    /// The lane holds its own end of the queue, which must not hide that the
    /// reader is gone.
    #[test]
    fn a_latest_value_lane_reports_a_dropped_reader() {
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(1));
        drop(receiver);

        assert!(!sink.lane.send(2));
    }

    /// An async receive ends as a blocking one does, so it too says the proxy
    /// stopped rather than only that the stream ended.
    #[tokio::test]
    async fn an_async_receive_ends_as_a_blocking_one_does() {
        let (stream, proxy) = stream();
        let awaited = stream.batches(everything());
        let blocked = stream.batches(everything());

        proxy.send(Packet::link(packet::LinkStatus::Stopped));

        assert_eq!(awaited.recv_async().await.err(), Some(RecvError::Stopped));
        assert_eq!(blocked.recv().err(), Some(RecvError::Stopped));
    }

    /// The replies that carry `update` to watch 1, as their values.
    fn parts_of(update: &Update) -> Vec<Vec<u8>> {
        crate::proxy::parts(1, update)
            .iter()
            .map(|packet| match packet.payload() {
                packet::Payload::ProxyRx(ProxyRx::Reply(reply)) => reply.value.to_vec(),
                other => panic!("a part is a reply, not {other:?}"),
            })
            .collect()
    }

    fn watched() -> Joined {
        Joined::default()
    }

    fn large_table(entries: usize) -> Update {
        let entries = (0..entries).map(|index| (format!("test.entry{index}"), 0x8140));
        Update::Table(Ok((Some(1), None, entries.collect())))
    }

    /// An update too large for one reply arrives in parts, and is whole once
    /// its last part is in. An update that lost a part is dropped, and the
    /// next one joins whole.
    #[test]
    fn a_watched_update_joins_from_its_parts_and_one_missing_a_part_is_dropped() {
        let update = large_table(100);
        let parts = parts_of(&update);
        assert!(parts.len() > 2, "the table takes several replies");
        let mut watched = watched();

        let lost_one = parts.iter().enumerate().filter(|(index, _)| *index != 1);
        let joined: Vec<_> = lost_one
            .filter_map(|(_, part)| watched.join(part))
            .collect();
        assert!(joined.is_empty(), "an update that lost a part is dropped");
        let joined: Vec<_> = parts.iter().filter_map(|part| watched.join(part)).collect();
        assert_eq!(joined.len(), 1, "only the last part completes the update");
        assert_eq!(joined[0], update);
    }

    /// An update starts at its first part. The tail of a later one whose
    /// first parts were lost is never spliced onto the one before.
    #[test]
    fn the_tail_of_an_update_is_never_joined_without_its_head() {
        let (first, second) = (large_table(100), large_table(101));
        let (first, second) = (parts_of(&first), parts_of(&second));
        let mut watched = watched();
        let joined: Vec<_> = first.iter().filter_map(|part| watched.join(part)).collect();
        assert_eq!(joined.len(), 1);

        let tail = second.iter().skip(1);
        let spliced: Vec<_> = tail.filter_map(|part| watched.join(part)).collect();
        assert!(spliced.is_empty(), "a tail was joined without its head");
    }

    /// A table receiver taken with a change receiver holds the table by the
    /// time a change arrives. A reader decodes each change with that table.
    #[test]
    fn a_table_followed_with_the_changes_is_held_when_a_change_arrives() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let loaded = stream.state(route, REGISTRY);
        let table_watch = watch_of(&proxy, table(route));
        reply_update(&proxy, table_watch, &table_update(1));
        loaded.recv().unwrap().expect("the table loads");

        let changes = stream.state(route, CHANGES);
        let tables = stream.state(route, REGISTRY);
        let device_watch = watch_of(&proxy, device(route));
        let announced = Update::Setting(
            "test.gain".to_string(),
            Some(2),
            1.5f32.to_le_bytes().to_vec().into(),
        );
        reply_update(&proxy, device_watch, &announced);
        reply_update(&proxy, table_watch, &settings_update(1, 2));

        assert_eq!(changes.recv().unwrap().seq, Some(2));
        let held = tables.try_recv().unwrap().expect("the table is held");
        assert!(held.unwrap().find("test.gain").is_some());
    }

    /// Settings read with a table the pump does not hold are dropped, as the
    /// proxy sends a table ahead of the settings read with it.
    #[test]
    fn settings_read_with_a_table_not_held_are_dropped() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let settings = stream.state(route, SETTINGS);
        let id = watch_of(&proxy, table(route));
        reply_update(&proxy, id, &table_update(1));
        reply_update(&proxy, id, &settings_update(2, 9));
        assert_eq!(
            settings.recv_timeout(Duration::from_millis(300)).err(),
            Some(RecvError::Timeout)
        );

        reply_update(&proxy, id, &settings_update(1, 9));
        let read = settings
            .recv_timeout(Duration::from_secs(5))
            .expect("the settings");
        assert_eq!(
            (read.version(), read.rpc_registry().hash),
            (Some(9), Some(1))
        );
    }

    /// A refused table watch is a table that did not load, so a reader is
    /// told, rather than waiting on a table no one will send.
    #[test]
    fn a_refused_table_watch_is_a_table_that_did_not_load() {
        let (stream, proxy) = stream();
        let route: DeviceRoute = "/4".parse().unwrap();
        let registry = stream.state(route, REGISTRY);
        let id = watch_of(&proxy, table(route));
        let refusal = Packet::proxy_error(
            id,
            crate::proto::rpc::RpcError::Invalid,
            "no mount shows /4",
        );
        proxy.send(refusal.unwrap());

        let refused = registry
            .recv_timeout(Duration::from_secs(5))
            .expect("the refusal");
        assert!(refused.is_err(), "{refused:?}");
    }

    /// The pump takes presence, metadata and setting changes from the proxy
    /// alone. It watches a device once a receiver follows it, before the
    /// device is heard, and a heartbeat, an announcement or a status it
    /// receives changes none of them. A reset of the metadata comes from the
    /// proxy too.
    #[test]
    fn presence_metadata_and_changes_come_from_the_proxy_alone() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let presence = stream.state(route, PRESENCE);
        let metadata = stream.state(route, METADATA);
        let changes = stream.state(route, CHANGES);
        let (id, watch) = watch_request(&proxy);
        assert_eq!(watch, device(route));

        proxy.send(Packet::heartbeat_session(7, route));
        proxy.send(announcement("test.amplitude", Some(1), &[1], route));
        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        let quiet = Duration::from_millis(200);
        assert_eq!(presence.recv_timeout(quiet), Err(RecvError::Timeout));
        assert_eq!(changes.recv_timeout(quiet), Err(RecvError::Timeout));
        assert!(matches!(
            metadata.recv_timeout(quiet),
            Err(RecvError::Timeout)
        ));

        let announced = Update::Setting("test.amplitude".to_string(), Some(1), vec![1].into());
        reply_update(&proxy, id, &Update::Presence(Ok(Some(7))));
        reply_update(&proxy, id, &announced);
        reply_update(&proxy, id, &metadata_update(1));
        assert_eq!(
            presence.recv(),
            Ok(Presence::Alive {
                session: Some(WireSessionId::new(7))
            })
        );
        let change = changes.recv().expect("the proxy's announcement");
        assert_eq!(
            (change.name.as_str(), change.reply),
            ("test.amplitude", vec![1])
        );
        assert!(matches!(metadata.recv(), Ok(MetadataState::Ready(_))));

        reply_update(&proxy, id, &Update::Metadata(Some(Vec::new())));
        assert!(matches!(metadata.recv(), Ok(MetadataState::Pending)));
        reply_update(&proxy, id, &Update::Presence(Err(Some(1000))));
        let Ok(Presence::Lost { since }) = presence.recv() else {
            panic!("the proxy's loss of the device");
        };
        assert!(since.elapsed() >= Duration::from_secs(1));
    }

    /// The routes come from the proxy too, from the list of the watch of the
    /// subtree a receiver follows.
    #[test]
    fn the_routes_come_from_the_proxy() {
        let (stream, proxy) = stream();
        let below: DeviceRoute = "/3".parse().unwrap();
        let routes = stream.routes(Scope::subtree(below));
        let id = watch_of(&proxy, device(below));
        proxy.send(Packet::heartbeat("/3/1".parse().unwrap()));
        assert_eq!(
            routes.recv_timeout(Duration::from_millis(200)),
            Err(RecvError::Timeout)
        );

        let heard: [DeviceRoute; 2] = ["/3".parse().unwrap(), "/3/2".parse().unwrap()];
        reply_update(&proxy, id, &routes_update(&heard));
        assert_eq!(
            routes.recv_timeout(Duration::from_secs(5)),
            Ok(heard.to_vec())
        );
    }

    /// A pump stops a watch once no receiver follows what it watches.
    #[tokio::test(start_paused = true)]
    async fn a_watch_no_receiver_follows_is_stopped() {
        let tasks = Supervisor::current();
        let (session, proxy) = Session::test_pair_on(&tasks);
        let stream = Arc::new(Stream::new(Handle::root(session)));
        let registry = stream.state(DeviceRoute::root(), REGISTRY);
        let watched = loop {
            let request = proxy.sent.recv_async().await.unwrap();
            if let packet::Payload::ProxyTx(request) = request.payload() {
                break request.id.value();
            }
        };

        drop(registry);

        let stopped = proxy.sent.recv_async().await.unwrap();
        let packet::Payload::ProxyTx(request) = stopped.payload() else {
            panic!("expected an unwatch, got {:?}", stopped.payload());
        };
        let crate::proto::rpc::Method::ByName(name) = request.method else {
            panic!("a control request names its method");
        };
        assert_eq!(name, UNWATCH.as_bytes());
        assert_eq!(decode::<u16>(request.args).unwrap(), watched);
    }

    /// A value a stopped watch kept is dropped with it, as no one keeps it
    /// current. A receiver that follows it again waits for the proxy's word
    /// on the new watch.
    #[test]
    fn a_stopped_watch_leaves_no_stale_value() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let presence = stream.state(route, PRESENCE);
        let id = watch_of(&proxy, device(route));
        reply_update(&proxy, id, &Update::Presence(Ok(Some(7))));
        assert!(presence.recv_timeout(Duration::from_secs(5)).is_ok());
        drop(presence);
        let (name, _, _) = request(&proxy);
        assert_eq!(name, UNWATCH);

        let again = stream.state(route, PRESENCE);
        let id = watch_of(&proxy, device(route));
        assert_eq!(
            again.try_recv(),
            Ok(None),
            "the stopped watch's presence was kept"
        );
        reply_update(&proxy, id, &Update::Presence(Err(None)));
        assert_eq!(
            again.recv_timeout(Duration::from_secs(5)),
            Ok(Presence::Connecting)
        );
    }

    /// A device watch the proxy refuses ends the receivers that follow the
    /// device and the batches of a tap on it, rather than leaving them to
    /// wait.
    #[test]
    fn a_refused_device_watch_ends_its_receivers() {
        let (stream, proxy) = stream();
        let route: DeviceRoute = "/4".parse().unwrap();
        let presence = stream.state(route, PRESENCE);
        let batches = stream.batches(exactly("/4"));
        let id = watch_of(&proxy, device(route));
        let refusal = Packet::proxy_error(
            id,
            crate::proto::rpc::RpcError::Invalid,
            "no mount shows /4",
        );
        proxy.send(refusal.unwrap());

        let wait = Duration::from_secs(5);
        assert_eq!(presence.recv_timeout(wait), Err(RecvError::Disconnected));
        assert_eq!(
            batches.recv_timeout(wait).err(),
            Some(RecvError::Disconnected)
        );
    }

    /// A count of announcements the proxy's watch missed reaches each
    /// receiver of the device's changes as a lag, and the next announcement
    /// follows it.
    #[test]
    fn a_lag_of_the_proxys_watch_reaches_the_change_receivers() {
        let (stream, proxy) = stream();
        let route = DeviceRoute::root();
        let changes = stream.state(route, CHANGES);
        let id = watch_of(&proxy, device(route));
        reply_update(&proxy, id, &Update::Lagged(5));
        let announced = Update::Setting("test.amplitude".to_string(), Some(9), vec![1].into());
        reply_update(&proxy, id, &announced);

        let wait = Duration::from_secs(5);
        assert_eq!(changes.recv_timeout(wait), Err(RecvError::Lagged(5)));
        assert_eq!(
            changes.recv_timeout(wait).map(|change| change.seq),
            Ok(Some(9))
        );
    }
}

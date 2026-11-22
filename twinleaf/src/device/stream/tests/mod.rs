#[cfg(all(test, shuttle))]
mod shuttle;

use super::*;
use crate::data::fixtures;
use crate::data::Change;
use crate::proto::SampleNumber;
use crate::proto::SessionId as WireSessionId;
use crate::rt::Supervisor;
use crate::session::rpc::RpcValue;
use crate::session::tests::packets::{announcement, description, samples};
use crate::session::tests::wait_for;
use crate::session::{decode, RecvError, Update};
use crate::session::{LostCause, MetadataState, Presence};
use crate::session::{Session, TestProxy};
use crate::session::{CHANGES, METADATA, PRESENCE, REGISTRY, SETTINGS};
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
    for part in crate::session::parts(id, update) {
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

/// A heartbeat of the session before reaches the pump ahead of the
/// proxy's metadata of the new one, as the proxy sends an update ahead
/// of every packet the device sent after the change. The metadata stays
/// in place, and the samples after it decode with it.
#[test]
fn an_old_heartbeat_ahead_of_the_proxys_new_metadata_leaves_it_in_place() {
    let (stream, proxy) = stream();
    let root = DeviceRoute::root();
    let batches = stream.batches(everything());
    let list = watch_of(&proxy, device(root));
    reply_update(&proxy, list, &routes_update(&[root]));
    reply_update(&proxy, list, &metadata_update(1));

    proxy.send(Packet::heartbeat_session(1, root));
    reply_update(&proxy, list, &metadata_update(2));
    proxy.send(samples(5, root));
    let batch = batches
        .recv_timeout(Duration::from_secs(5))
        .expect("a batch");
    assert_eq!(batch.sample_numbers(), [5]);
    assert_eq!(batch.metadata().device().session().value(), 2);
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
/// lost as the proxy was lost, and every watch goes out again under a
/// new id, which the session sends once it is open again. The samples after
/// count what the outage lost against the samples before.
#[test]
fn a_lost_socket_loses_every_device_and_watches_again() {
    let (session, proxy) = Session::test_pair();
    let root = Handle::root(session);
    let (inlet, inlet_received) = Lane::new(INLET_LEN);
    let inlet = std::cell::RefCell::new(inlet);
    let store = Arc::new(Mutex::new(Store::new(false)));
    let (taps, taps_received) = mpsc::unbounded_channel();
    let pump = Pump::new(&root, inlet_received, Arc::clone(&store), taps_received);
    Supervisor::new().spawn("twinleaf stream", pump.run());
    let route = DeviceRoute::root();
    let (sink, batches) = Sink::batches(BATCH_QUEUE_LEN, everything());
    let _ = taps.send(Tap::Batches(sink));
    let (sink, packets) = Sink::new(PACKET_QUEUE_LEN, everything());
    let _ = taps.send(Tap::Packets(sink));
    let presence = lock(&store).follow(route, PRESENCE);
    let deliver = |packet| inlet.borrow_mut().send(Received::Packet(packet));
    let reply = |id: u16, update: &Update| {
        for part in crate::session::parts(id, update) {
            inlet.borrow_mut().send_always(Received::Packet(part));
        }
    };
    let id = watch_of(&proxy, device(route));
    reply(id, &Update::Presence(Ok(Some(7)), None, None));
    reply(id, &routes_update(&[route]));
    reply(id, &metadata_update(1));
    let wait = Duration::from_secs(5);
    wait_for(&presence, |presence| {
        matches!(presence, Presence::Alive { .. })
    });
    deliver(samples(0, route));
    assert!(batches.recv_timeout(wait).unwrap().is_initial());

    inlet.borrow_mut().send_always(Received::SocketLost);
    let gone = wait_for(&presence, lost);
    assert!(
        matches!(
            gone,
            Presence::Lost {
                cause: Some(LostCause::ProxyLost),
                ..
            }
        ),
        "{gone:?}"
    );
    let marked = wait_for(&packets, |packet| packet.link_status().is_some());
    assert_eq!(
        marked.link_status(),
        Some(packet::LinkStatus::SensorDisconnected)
    );
    let again = watch_of(&proxy, device(route));
    assert_ne!(again, id, "the watch is new");
    reply(again, &routes_update(&[route]));
    reply(again, &metadata_update(1));
    deliver(samples(3, route));
    let resumed = batches.recv_timeout(wait).unwrap();
    assert_eq!(resumed.sample_numbers(), [3]);
    let loss = resumed
        .boundary()
        .and_then(|boundary| boundary.loss.as_ref());
    assert_eq!(
        loss.map(|loss| (loss.samples, loss.expected)),
        Some((2, Some(SampleNumber::new(1)))),
        "{:?}",
        resumed.boundary()
    );
}

/// A device whose link reconnects behind the proxy, which says its
/// metadata is pending and then sends it again, goes on from its samples
/// before. In the same session the samples after count what it lost, and
/// in a new one they say the session changed.
#[test]
fn a_device_that_reconnects_behind_the_proxy_goes_on_from_its_samples_before() {
    for (session, first) in [(1, 3), (2, 0)] {
        let (stream, proxy) = stream();
        let root = DeviceRoute::root();
        let batches = stream.batches(everything());
        let list = watch_of(&proxy, device(root));
        reply_update(&proxy, list, &routes_update(&[root]));
        reply_update(&proxy, list, &metadata_update(1));
        proxy.send(samples(0, root));
        let wait = Duration::from_secs(5);
        assert!(batches.recv_timeout(wait).unwrap().is_initial());

        proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
        reply_update(&proxy, list, &Update::Metadata(Some(Vec::new())));
        reply_update(&proxy, list, &metadata_update(session));
        proxy.send(samples(first, root));
        let after = batches.recv_timeout(wait).unwrap();
        let boundary = after.boundary().cloned();
        let expected = match session {
            1 => fixtures::boundary(
                None,
                Some(fixtures::loss(2, Some(SampleNumber::new(1)), 0, 0, 2, 0)),
            ),
            _ => fixtures::boundary(
                Some(Change::SessionChanged {
                    old: WireSessionId::new(1),
                    new: WireSessionId::new(2),
                }),
                None,
            ),
        };
        assert_eq!(boundary, Some(expected), "session {session}");
    }
}

/// The rows a stream is missing after the proxy says it dropped packets of
/// that stream for this connection count as the proxy's. The proxy's count
/// is for one stream of one device, and with no count the rows count as
/// skipped.
#[test]
fn rows_missing_after_the_proxys_count_of_dropped_samples_are_the_proxys() {
    let (stream, proxy) = stream();
    let root = DeviceRoute::root();
    let batches = stream.batches(everything());
    let list = watch_of(&proxy, device(root));
    reply_update(&proxy, list, &routes_update(&[root]));
    reply_update(&proxy, list, &metadata_update(1));
    proxy.send(samples(0, root));
    proxy.send(Packet::dropped(1, 2).with_route(root));
    proxy.send(samples(3, root));
    proxy.send(samples(6, root));
    proxy.send(Packet::dropped(2, 2).with_route(root));
    proxy.send(samples(9, root));
    let wait = Duration::from_secs(5);
    let mut lost = Vec::new();
    while lost.len() < 3 {
        let batch = batches.recv_timeout(wait).expect("a batch");
        if let Some(loss) = batch.boundary().and_then(|b| b.loss.clone()) {
            assert_eq!(
                (loss.samples, loss.dropped_samples, loss.link_samples),
                (2, 0, 0)
            );
            lost.push((loss.proxy_samples, loss.skipped_samples()));
        }
    }
    assert_eq!(lost, [(2, 0), (0, 2), (0, 2)]);
}

/// The rows a stream is missing after the session's inlet had no room for
/// packets count as dropped by this connection.
#[test]
fn rows_missing_after_the_inlet_overflowed_are_this_connections() {
    let (stream, _proxy) = stream();
    let root = DeviceRoute::root();
    let (_inlet, received) = crate::session::Lane::new(1);
    let (_taps, taps) = mpsc::unbounded_channel();
    let store = Arc::new(Mutex::new(Store::new(false)));
    let mut pump = Pump::new(&stream.root, received, store, taps);
    for packet in description(root, 1) {
        pump.parser.push_packet(&packet).expect("a record");
    }
    let (sink, batches) = Sink::batches(8, everything());
    pump.batches.push(sink);
    let packet = |first| Queued::Item(Received::Packet(samples(first, root)));
    assert!(pump.take(packet(0)).is_none());
    assert!(pump.take(Queued::Gap(3)).is_none());
    assert!(pump.take(packet(4)).is_none());
    assert!(pump.take(packet(7)).is_none());
    let lost: Vec<_> = std::iter::from_fn(|| batches.try_recv().ok().flatten())
        .filter_map(|batch| batch.boundary().and_then(|b| b.loss.clone()))
        .map(|loss| (loss.samples, loss.dropped_samples, loss.skipped_samples()))
        .collect();
    assert_eq!(lost, [(3, 3, 0), (2, 0, 2)]);
}

/// The rows a child is missing after the socket was lost count as the
/// link's, though the only batch tap covers that child alone.
#[test]
fn a_lost_socket_is_blamed_on_the_link_under_a_childs_tap() {
    let (stream, _proxy) = stream();
    let child: DeviceRoute = "/1".parse().unwrap();
    let (_inlet, received) = crate::session::Lane::new(1);
    let (_taps, taps) = mpsc::unbounded_channel();
    let store = Arc::new(Mutex::new(Store::new(false)));
    let mut pump = Pump::new(&stream.root, received, store, taps);
    let (sink, batches) = Sink::batches(8, exactly("/1"));
    pump.batches.push(sink);
    let packet = |first| Queued::Item(Received::Packet(samples(first, child)));
    for packet in description(child, 1) {
        pump.parser.push_packet(&packet).expect("a record");
    }
    assert!(pump.take(packet(0)).is_none());
    assert!(pump.take(Queued::Item(Received::SocketLost)).is_none());
    for packet in description(child, 1) {
        pump.parser.push_packet(&packet).expect("a record");
    }
    assert!(pump.take(packet(3)).is_none());
    let lost: Vec<_> = std::iter::from_fn(|| batches.try_recv().ok().flatten())
        .filter_map(|batch| batch.boundary().and_then(|b| b.loss.clone()))
        .map(|loss| (loss.samples, loss.link_samples))
        .collect();
    assert_eq!(lost, [(2, 2)]);
}

/// A disconnect status reaches the pump ahead of the proxy's metadata of
/// the device once it is back, which stays in place, so the samples after
/// it decode.
#[test]
fn a_status_ahead_of_the_proxys_metadata_leaves_it_in_place() {
    let (stream, proxy) = stream();
    let root = DeviceRoute::root();
    let batches = stream.batches(everything());
    let list = watch_of(&proxy, device(root));
    reply_update(&proxy, list, &routes_update(&[root]));
    reply_update(&proxy, list, &metadata_update(1));

    proxy.send(Packet::link(packet::LinkStatus::SensorDisconnected));
    reply_update(&proxy, list, &metadata_update(2));
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
    let (sink, batches) = Sink::batches(2, everything());
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
    reply_update(&proxy, id, &Update::Presence(Ok(None), None, None));
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
    let (inlet, packets) = crate::chan::unbounded();
    let store = Arc::new(Mutex::new(Store::new(true)));
    let (taps, received) = mpsc::unbounded_channel();
    let pump = Pump::new(&root, packets, Arc::clone(&store), received);
    Supervisor::new().spawn("twinleaf stream", pump.run());
    let (sink, tapped) = Sink::new(PACKET_QUEUE_LEN, everything());
    taps.send(Tap::Packets(sink)).unwrap();
    let deliver = |packet| inlet.send(Queued::Item(Received::Packet(packet))).unwrap();

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
    reply_update(&proxy, id, &Update::Presence(Ok(None), None, None));
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

/// The pump ends once nothing can reach it any more, with no view left to
/// subscribe and no receiver left holding the stream open.
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
            Err(crate::chan::RecvTimeoutError::Disconnected) => break,
            Err(crate::chan::RecvTimeoutError::Timeout) => {
                panic!("the session outlived the pump")
            }
        }
    }
}

/// A raw tap sees the wire, absolutely routed and filtered to its scope,
/// and asks nothing of anyone. A status filters as the subtree at its
/// route, as it is a recorded stream's reset marker.
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

/// A receiver is the subscription. It keeps the pump running on its own,
/// so a caller need not hold the view it was minted from.
#[test]
fn a_receiver_keeps_the_pump_running_without_its_view() {
    let (stream, proxy) = stream();
    let presence = stream.state(DeviceRoute::root(), PRESENCE);
    drop(stream);

    let id = watch_of(&proxy, device(DeviceRoute::root()));
    reply_update(&proxy, id, &Update::Presence(Ok(None), None, None));
    wait_for(&presence, |presence| {
        *presence == Presence::Alive { session: None }
    });
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

/// A change receiver alone follows the device's table too, and each
/// change carries the table the proxy named ahead of it, which is the
/// pump's own copy of the device's table. A reader decodes the change
/// with it at once.
#[test]
fn a_change_carries_the_table_the_proxy_named_for_it() {
    let (stream, proxy) = stream();
    let route = DeviceRoute::root();
    let changes = stream.state(route, CHANGES);
    let device_watch = watch_of(&proxy, device(route));
    let table_watch = watch_of(&proxy, table(route));
    reply_update(&proxy, table_watch, &table_update(1));
    let Update::Table(Ok(entries)) = table_update(1) else {
        unreachable!("a table update holds a table");
    };
    reply_update(&proxy, device_watch, &Update::Recorded(Some(entries)));
    let announced = Update::Setting(
        "test.gain".to_string(),
        Some(2),
        1.5f32.to_le_bytes().to_vec().into(),
    );
    reply_update(&proxy, device_watch, &announced);

    let change = changes.recv().unwrap();
    assert_eq!(change.value().unwrap(), RpcValue::F64(1.5));
    let held = stream.state(route, REGISTRY).try_recv().unwrap();
    let held = held.expect("the table is held").unwrap();
    assert!(Arc::ptr_eq(change.rpc_registry().unwrap(), &held));
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
    assert_eq!(changes.recv_timeout(quiet).err(), Some(RecvError::Timeout));
    assert!(matches!(
        metadata.recv_timeout(quiet),
        Err(RecvError::Timeout)
    ));

    let announced = Update::Setting("test.amplitude".to_string(), Some(1), vec![1].into());
    reply_update(&proxy, id, &Update::Presence(Ok(Some(7)), None, None));
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
    reply_update(&proxy, id, &Update::Presence(Err(Some(1000)), None, None));
    let Ok(Presence::Lost { since, .. }) = presence.recv() else {
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
    reply_update(&proxy, id, &Update::Presence(Ok(Some(7)), None, None));
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
    reply_update(&proxy, id, &Update::Presence(Err(None), None, None));
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
    assert_eq!(changes.recv_timeout(wait).err(), Some(RecvError::Lagged(5)));
    assert_eq!(
        changes.recv_timeout(wait).map(|change| change.seq),
        Ok(Some(9))
    );
}

mod settings_sim;

use super::*;
use crate::data::DeviceMetadataSnapshot;
use crate::device::rpc::RpcValue;
use crate::device::session::TestProxy;
use crate::device::store::{Field, CHANGES, REGISTRY, SETTINGS};
use crate::device::stream::{Receiver, RecvError, Scope};
use crate::device::tests::fixtures::{
    announcement, broadcast, description, device_reply, samples, segment_record, wait_for,
};
use crate::device::RegistryState;
use crate::proto::data as wire;
use crate::proto::rpc as wire_rpc;
use crate::proto::SessionId;
use crate::proxy::control::Joined;
use crate::rt::Supervisor;
use crate::tio::packet::{Payload, ProxyRx};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant as StdInstant;

/// Feeds a client the whole link, with watch 1 following the metadata
/// of the device at the root, from the engine whose store is `store`.
fn client_of(store: &Mutex<Store>, mounts: &Mounts) -> Arc<Outbox> {
    let (outbox, root) = (Arc::<Outbox>::default(), DeviceRoute::root());
    let mount = Mount::new(root, root, &outbox);
    mount.watch_metadata(1, root, store);
    mounts.add(mount);
    outbox
}

/// Takes what `outbox` holds, as a client's task does, pausing between
/// takes for a while that `seed` picks, until a packet `done` picks has
/// gone. Returns everything taken, in order.
fn take_until(outbox: &Outbox, seed: u64, done: impl Fn(&Packet) -> bool) -> Vec<Packet> {
    let deadline = StdInstant::now() + Duration::from_secs(20);
    let (mut taken, mut pause) = (Vec::new(), seed);
    loop {
        assert!(
            StdInstant::now() < deadline,
            "the last packet never went out"
        );
        let queued = outbox.take().packets;
        outbox.check();
        let finished = queued.iter().any(&done);
        taken.extend(queued);
        if finished {
            return taken;
        }
        pause = (pause * 7 + 3) % 11;
        std::thread::sleep(Duration::from_micros(pause * 200));
    }
}

/// A metadata update's place in what a client took, the id and start of
/// each segment it holds, and its device's session.
type Renewal = (usize, Vec<(u8, u32)>, u32);

/// Each metadata update in `taken`, as the client joins them.
fn segment_updates(taken: &[Packet]) -> Vec<Renewal> {
    let mut joined = Joined::default();
    let mut updates = Vec::new();
    for (place, packet) in taken.iter().enumerate() {
        let Payload::ProxyRx(ProxyRx::Reply(reply)) = packet.payload() else {
            continue;
        };
        let Some(Update::Metadata(Some(records))) = joined.join(reply.value) else {
            continue;
        };
        let (mut segments, mut session) = (Vec::new(), 0);
        for record in records {
            let (record, _) = Packet::from_slice_prefix(&record).unwrap();
            match record.payload() {
                Payload::Metadata(wire::Metadata::Segment(segment), _) => {
                    segments.push((segment.segment_id.value(), segment.start_time))
                }
                Payload::Metadata(wire::Metadata::Device(device), _) => {
                    session = device.session.value()
                }
                _ => {}
            }
        }
        updates.push((place, segments, session));
    }
    updates
}

/// Whatever the interleaving of segment records and samples, and however
/// the client's task takes from its outbox meanwhile, the client hears
/// each record ahead of every sample the device sent after it. A sample
/// that goes out goes out under the record it was sent under.
#[test]
fn a_client_hears_each_record_ahead_of_every_sample_sent_after_it() {
    let root = DeviceRoute::root();
    let done = route("/9");
    for seed in 1..=4u64 {
        let (store, mounts, proxy) = engine_with_mounts(&Supervisor::new());
        let outbox = client_of(&store, &mounts);
        let taking = Arc::clone(&outbox);
        let taker =
            std::thread::spawn(move || take_until(&taking, seed, |packet| packet.route() == done));
        describe(&proxy, root, 1);
        let mut random = seed;
        let mut next = move || {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            random >> 60
        };
        let (mut segment, mut start, mut first) = (0, 0, 0);
        let mut sent = HashMap::new();
        for _ in 0..3000 {
            match next() {
                0 => {
                    (segment, start) = (segment ^ 1, start + 1);
                    proxy.send(broadcast(record(segment, start), root));
                }
                1 => proxy.send(broadcast(record(segment, start), root)),
                _ => {
                    sent.insert(first, (segment, start));
                    proxy.send(Packet::samples(1, segment, first, &[0; 400], root).unwrap());
                    first += 100;
                }
            }
        }
        proxy.send(Packet::heartbeat(done));
        let taken = taker.join().unwrap();

        let updates = segment_updates(&taken);
        let mut held = HashMap::new();
        let mut updates = updates.into_iter().peekable();
        let mut decoded = 0;
        for (place, packet) in taken.iter().enumerate() {
            while let Some((_, segments, _)) = updates.next_if(|(at, ..)| *at <= place) {
                held.extend(segments);
            }
            let Payload::Samples(data) = packet.payload() else {
                continue;
            };
            let (segment, start) = sent[&data.first.value()];
            assert_eq!(
                held.get(&segment),
                Some(&start),
                "seed {seed}: sample {} went out under another record",
                data.first.value()
            );
            decoded += 1;
        }
        assert!(decoded > 0, "seed {seed}: no sample went out");
    }
}

/// A mount added while its link has lost the device hears so first, at
/// the root of its subtree, and then hears the device come back along
/// with the mount that was there.
#[test]
fn a_mount_added_while_its_device_is_lost_hears_so_first() {
    let root = DeviceRoute::root();
    let mounts = Mounts::new("tcp://device");
    let (early, late) = (Arc::<Outbox>::default(), Arc::<Outbox>::default());
    mounts.add(Mount::new(root, root, &early));
    mounts.feed(&Packet::heartbeat(root));
    mounts.feed(&Packet::link(LinkStatus::SensorDisconnected));

    mounts.add(Mount::new(route("/1"), route("/2"), &late));
    mounts.feed(&Packet::link(LinkStatus::SensorReconnected));

    let heard = |outbox: &Outbox| -> Vec<_> {
        let taken = outbox.take().packets;
        taken
            .iter()
            .map(|pkt| (pkt.route(), pkt.link_status()))
            .collect()
    };
    let (lost, back) = (
        Some(LinkStatus::SensorDisconnected),
        Some(LinkStatus::SensorReconnected),
    );
    assert_eq!(heard(&early), [(root, None), (root, lost), (root, back)]);
    assert_eq!(heard(&late), [(route("/2"), lost), (route("/2"), back)]);
}

/// The segment record of `segment` that starts at `start`.
fn record(segment: u8, start: u32) -> wire::Metadata<'static> {
    wire::Metadata::Segment(wire::Segment {
        start_time: start,
        ..segment_record(segment)
    })
}

/// A heartbeat of the old session goes out ahead of the metadata of the
/// new one, and the samples the device sent after it go out after it,
/// so a client never meets a stale heartbeat behind newer metadata.
#[test]
fn an_old_sessions_heartbeat_goes_out_ahead_of_the_new_sessions_metadata() {
    let root = DeviceRoute::root();
    let (store, mounts, proxy) = engine_with_mounts(&Supervisor::new());
    let outbox = client_of(&store, &mounts);
    describe(&proxy, root, 1);
    proxy.send(Packet::heartbeat_session(1, root));
    proxy.send(Packet::heartbeat_session(2, root));
    describe(&proxy, root, 2);
    proxy.send(samples(5, root));

    let is_sample = |packet: &Packet| matches!(packet.payload(), Payload::Samples(_));
    let taken = take_until(&outbox, 0, is_sample);
    let beat = taken.iter().position(|packet| match packet.payload() {
        Payload::Heartbeat(beat) => beat.session().map(SessionId::value) == Some(1),
        _ => false,
    });
    let updates = segment_updates(&taken);
    let renewed = updates.iter().find(|(_, _, session)| *session == 2);
    let sample = taken.iter().position(is_sample);
    let renewed = renewed.map(|(place, ..)| *place);
    assert!(
        beat < renewed && renewed < sample,
        "{beat:?} {renewed:?} {sample:?}"
    );
    assert!(beat.is_some());
}

/// An engine over a session whose socket is a pair of channels, with
/// its tasks on `tasks`. Returns its store and the session's far end,
/// which plays the link.
fn engine_on(tasks: &Supervisor) -> (Arc<Mutex<Store>>, TestProxy) {
    let (store, _, proxy) = engine_with_mounts(tasks);
    (store, proxy)
}

/// An engine as [`engine_on`] starts it, with the mounts it feeds.
fn engine_with_mounts(tasks: &Supervisor) -> (Arc<Mutex<Store>>, Arc<Mounts>, TestProxy) {
    let (session, proxy) = Session::test_pair_on(tasks);
    let store = Arc::new(Mutex::new(Store::new(true)));
    let mounts = Mounts::new("tcp://device");
    let serving = serve(Arc::clone(&store), Arc::clone(&mounts), session, tasks);
    tasks.spawn("proxy state", serving);
    (store, mounts, proxy)
}

fn engine() -> (Arc<Mutex<Store>>, TestProxy) {
    engine_on(&Supervisor::new())
}

fn follow<T: Clone>(store: &Mutex<Store>, route: DeviceRoute, field: Field<T>) -> Receiver<T> {
    lock(store).follow(route, field)
}

fn presence(store: &Mutex<Store>, route: DeviceRoute) -> Receiver<Presence> {
    follow(store, route, PRESENCE)
}

fn metadata(store: &Mutex<Store>, route: DeviceRoute) -> Receiver<MetadataState> {
    follow(store, route, METADATA)
}

fn route(text: &str) -> DeviceRoute {
    text.parse().expect("a valid route")
}

fn describe(proxy: &TestProxy, route: DeviceRoute, session: u32) {
    for packet in description(route, session) {
        proxy.send(packet);
    }
}

/// The route, args and id of the `dev.metadata` call the engine
/// submitted.
fn metadata_call(proxy: &TestProxy) -> (DeviceRoute, Vec<u8>, u16) {
    let (route, name, args, id) = proxy.call();
    assert_eq!(name, wire::METADATA_RPC_METHOD);
    (route, args, id)
}

/// Whether the session wrote anything within `within`.
fn wrote_within(proxy: &TestProxy, within: Duration) -> bool {
    proxy.sent.recv_timeout(within).is_ok()
}

/// The current segment of the one stream a snapshot describes.
fn current_segment(snapshot: &DeviceMetadataSnapshot) -> u8 {
    snapshot
        .stream(crate::proto::StreamId::new(1))
        .expect("the described stream")
        .segment()
        .segment_id()
        .value()
}

fn snapshot(state: &MetadataState) -> Option<&DeviceMetadataSnapshot> {
    match state {
        MetadataState::Ready(snapshot) => Some(snapshot),
        MetadataState::Pending | MetadataState::Unsupported => None,
    }
}

fn lost(presence: &Presence) -> bool {
    matches!(presence, Presence::Lost { .. })
}

fn alive(presence: &Presence) -> bool {
    matches!(presence, Presence::Alive { .. })
}

#[test]
fn metadata_discovery_completes_over_independent_calls() {
    let (store, proxy) = engine();
    let child = route("/1");
    let _followed = [
        metadata(&store, DeviceRoute::root()),
        metadata(&store, child),
    ];
    proxy.send(samples(0, DeviceRoute::root()));
    proxy.send(samples(0, child));

    // Both routes bootstrap at once, and answering them out of order
    // must still reach the right parser state.
    let first = metadata_call(&proxy);
    let second = metadata_call(&proxy);
    assert_ne!(first.0, second.0, "one query per route");
    assert!(first.1.is_empty() && second.1.is_empty(), "both bootstrap");
    proxy.reply(second.2, &device_reply(), second.0);
    proxy.reply(first.2, &device_reply(), first.0);

    let wanted: Vec<Vec<u8>> = (0..2).map(|_| metadata_call(&proxy).1).collect();
    assert!(
        wanted
            .iter()
            .all(|args| *args == wire::MetadataSelector::stream(1).encode()),
        "each reply reached its own route, which now wants only its stream"
    );
}

/// A segment UPDATE replaces the current segment of a route that never
/// stopped being complete, so completeness alone would miss the change.
#[test]
fn a_live_segment_rollover_republishes_metadata_and_replays_the_latest() {
    let root = DeviceRoute::root();
    let (store, proxy) = engine();
    let followed = metadata(&store, root);
    describe(&proxy, root, 1);
    wait_for(&followed, |state| {
        snapshot(state).is_some_and(|snapshot| current_segment(snapshot) == 0)
    });

    proxy.send(broadcast(wire::Metadata::Segment(segment_record(1)), root));
    wait_for(&followed, |state| {
        snapshot(state).is_some_and(|snapshot| current_segment(snapshot) == 1)
    });

    let late = metadata(&store, root);
    let first = late
        .recv_timeout(Duration::from_secs(5))
        .expect("the state");
    assert_eq!(snapshot(&first).map(current_segment), Some(1));
}

/// A session change makes the route rediscover. Receivers are told what
/// it found instead of keeping what the old session said.
#[test]
fn a_new_session_republishes_metadata_once_the_route_rediscovers() {
    let root = DeviceRoute::root();
    let (store, proxy) = engine();
    let followed = metadata(&store, root);
    describe(&proxy, root, 1);
    wait_for(&followed, |state| {
        snapshot(state).is_some_and(|snapshot| snapshot.device().session().value() == 1)
    });

    proxy.send(Packet::heartbeat_session(2, root));
    describe(&proxy, root, 2);
    wait_for(&followed, |state| {
        snapshot(state).is_some_and(|snapshot| snapshot.device().session().value() == 2)
    });
}

/// Devices re-broadcast their metadata. Saying again what a receiver
/// already has is not news.
#[test]
fn re_broadcasting_identical_metadata_does_not_republish_it() {
    let root = DeviceRoute::root();
    let (store, proxy) = engine();
    let followed = metadata(&store, root);
    describe(&proxy, root, 1);
    wait_for(&followed, |state| snapshot(state).is_some());

    describe(&proxy, root, 1);
    assert!(
        matches!(
            followed.recv_timeout(Duration::from_millis(500)),
            Err(RecvError::Timeout)
        ),
        "identical metadata was republished"
    );
}

#[test]
fn a_route_without_dev_metadata_is_asked_once() {
    let (store, proxy) = engine();
    let followed = metadata(&store, DeviceRoute::root());
    proxy.send(samples(0, DeviceRoute::root()));

    let (route, _, id) = metadata_call(&proxy);
    proxy.refuse(id, wire_rpc::RpcError::NotFound, route);

    wait_for(&followed, |state| {
        matches!(state, MetadataState::Unsupported)
    });
    assert!(
        !wrote_within(&proxy, Duration::from_millis(500)),
        "discovery does not re-arm for a route with no dev.metadata"
    );
}

/// A refusal answering a query the reset already overtook belongs to the
/// session that ended, and must not condemn the route discovering now.
#[test]
fn a_refusal_from_before_a_reset_does_not_condemn_the_route() {
    let root = DeviceRoute::root();
    let (store, proxy) = engine();
    let followed = presence(&store, root);
    let routes = lock(&store).follow_routes(Scope::subtree(root));
    proxy.send(samples(0, root));
    let (stale_route, _, stale) = metadata_call(&proxy);

    proxy.send(Packet::link(LinkStatus::SensorDisconnected));
    wait_for(&followed, lost);
    proxy.refuse(stale, wire_rpc::RpcError::NotFound, stale_route);

    // The refusal reaches the engine ahead of the packet sent after it.
    let child = route("/1");
    proxy.send(samples(0, child));
    wait_for(&routes, |routes| routes.contains(&child));
    let late = metadata(&store, root);
    assert!(
        matches!(
            late.recv_timeout(Duration::from_secs(5)),
            Ok(MetadataState::Pending)
        ),
        "the stale refusal condemned a route that had moved on"
    );
}

/// A link's status reaches the devices it serves. Another device's
/// heartbeat is not their business.
#[test]
fn a_device_follows_its_link_but_not_another_routes_heartbeat() {
    let (root, child) = (DeviceRoute::root(), route("/1"));
    let (store, proxy) = engine();
    let (other, device) = (presence(&store, root), presence(&store, child));
    proxy.send(Packet::heartbeat(child));
    wait_for(&device, alive);

    proxy.send(Packet::heartbeat(root));
    wait_for(&other, alive);
    assert_eq!(
        device.recv_timeout(Duration::from_millis(200)),
        Err(RecvError::Timeout),
        "the device heard another route's heartbeat"
    );

    proxy.send(Packet::link(LinkStatus::SensorDisconnected));
    let next = device.recv_timeout(Duration::from_secs(5));
    assert!(next.as_ref().is_ok_and(lost), "got {next:?}");
}

/// A status carries a fabricated route, since `FailedToConnect` means
/// nothing is there, so it must never mint a device. An upstream proxy
/// older than `PROXY_RX` sends it as a legacy status, at the root.
#[test]
fn a_status_packet_discovers_no_route() {
    let (store, proxy) = engine();
    let routes = lock(&store).follow_routes(Scope::subtree(DeviceRoute::root()));
    assert_eq!(routes.recv(), Ok(Vec::new()));
    let child = route("/1");
    proxy.send(Packet::legacy_status(LinkStatus::FailedToConnect));
    proxy.send(Packet::link(LinkStatus::FailedToConnect).with_route(route("/2")));
    proxy.send(Packet::heartbeat(child));

    let heard = wait_for(&routes, |routes| !routes.is_empty());
    assert_eq!(heard, [child], "only the heartbeat's route exists");
}

/// Behind `tio proxy --mount`, a status arrives on its mount's prefix, so
/// one sensor bouncing redescribes its own subtree and no one else's.
#[test]
fn a_mounts_disconnect_resets_only_its_own_subtree() {
    let (steady, bounced) = (route("/1"), route("/2"));
    let (store, proxy) = engine();
    let (steady_metadata, bounced_metadata) = (metadata(&store, steady), metadata(&store, bounced));
    let (steady_presence, bounced_presence) = (presence(&store, steady), presence(&store, bounced));
    describe(&proxy, steady, 1);
    describe(&proxy, bounced, 1);
    wait_for(&steady_metadata, |state| snapshot(state).is_some());
    wait_for(&bounced_metadata, |state| snapshot(state).is_some());
    wait_for(&steady_presence, alive);
    wait_for(&bounced_presence, alive);

    proxy.send(Packet::link(LinkStatus::SensorDisconnected).with_route(bounced));
    wait_for(&bounced_metadata, |state| {
        matches!(state, MetadataState::Pending)
    });
    wait_for(&bounced_presence, lost);

    describe(&proxy, steady, 1);
    describe(&proxy, bounced, 1);
    wait_for(&bounced_metadata, |state| snapshot(state).is_some());
    assert!(
        matches!(steady_metadata.try_recv(), Ok(None)),
        "the steady mount was reset too"
    );

    proxy.send(Packet::link(LinkStatus::SensorReconnected).with_route(bounced));
    wait_for(&bounced_presence, |presence| {
        *presence == Presence::Connecting
    });
    assert!(
        matches!(steady_presence.try_recv(), Ok(None)),
        "the steady mount reconnected too"
    );
}

/// A mount that stopped on purpose ends its session as a disconnected one
/// does. Its own subtree redescribes and no one else's does.
#[test]
fn a_mounts_stop_resets_only_its_own_subtree() {
    let (steady, stopped) = (route("/1"), route("/2"));
    let (store, proxy) = engine();
    let (steady_metadata, stopped_metadata) = (metadata(&store, steady), metadata(&store, stopped));
    describe(&proxy, steady, 1);
    describe(&proxy, stopped, 1);
    wait_for(&steady_metadata, |state| snapshot(state).is_some());
    wait_for(&stopped_metadata, |state| snapshot(state).is_some());

    proxy.send(Packet::link(LinkStatus::Stopped).with_route(stopped));
    wait_for(&stopped_metadata, |state| {
        matches!(state, MetadataState::Pending)
    });

    describe(&proxy, steady, 1);
    describe(&proxy, stopped, 1);
    wait_for(&stopped_metadata, |state| snapshot(state).is_some());
    assert!(
        matches!(steady_metadata.try_recv(), Ok(None)),
        "the steady mount was reset too"
    );
}

/// An upstream proxy announces a mount's status as a link packet, and
/// one that predates `PROXY_RX` announces it as a legacy status. The
/// engine takes either the same way.
#[test]
fn a_link_packet_is_the_same_status_as_a_legacy_one() {
    let mount = route("/2");
    let (store, proxy) = engine();
    let followed = presence(&store, mount);
    let heard = |status: Packet| {
        proxy.send(Packet::heartbeat(mount));
        wait_for(&followed, alive);
        proxy.send(status.with_route(mount));
        followed
            .recv_timeout(Duration::from_secs(5))
            .expect("the engine publishes")
    };

    let legacy = heard(Packet::legacy_status(LinkStatus::SensorDisconnected));
    let link = heard(Packet::link(LinkStatus::SensorDisconnected));

    assert!(
        lost(&legacy),
        "the legacy status lost the mount, got {legacy:?}"
    );
    assert!(lost(&link), "the link status lost the mount, got {link:?}");
}

/// The affected subtree contains the device, not the other way around. A
/// device below a mount follows that mount's status, and a device on
/// another mount does not.
#[test]
fn a_device_below_the_affected_subtree_follows_its_status() {
    let (pinned, other) = (route("/1/0"), route("/2"));
    let (store, proxy) = engine();
    let (below, elsewhere) = (presence(&store, pinned), presence(&store, other));
    proxy.send(Packet::heartbeat(pinned));
    proxy.send(Packet::heartbeat(other));
    wait_for(&below, alive);
    wait_for(&elsewhere, alive);

    proxy.send(Packet::link(LinkStatus::SensorReconnected).with_route(route("/1")));

    wait_for(&below, |presence| *presence == Presence::Connecting);
    assert_eq!(
        elsewhere.recv_timeout(Duration::from_millis(200)),
        Err(RecvError::Timeout),
        "another mount's status is not this device's business"
    );
}

/// Link state is a level, not an edge, so a receiver that joins during an
/// outage learns what it did not watch happen.
#[test]
fn a_late_receiver_starts_from_the_link_state_it_did_not_watch() {
    let mount = route("/2");
    let (store, proxy) = engine();
    let early = presence(&store, mount);
    proxy.send(samples(0, mount));
    proxy.send(Packet::link(LinkStatus::SensorDisconnected).with_route(mount));
    wait_for(&early, lost);

    let late = presence(&store, mount);
    let first = late
        .recv_timeout(Duration::from_secs(5))
        .expect("the state");
    assert!(lost(&first), "the late receiver starts from {first:?}");
    let routes = lock(&store).follow_routes(Scope::subtree(DeviceRoute::root()));
    assert_eq!(routes.recv_timeout(Duration::from_secs(5)), Ok(vec![mount]));
}

/// A device not heard yet, that a receiver follows, is lost with its
/// link.
#[test]
fn a_followed_device_not_yet_heard_is_lost_with_its_link() {
    let (store, proxy) = engine();
    let unheard = presence(&store, route("/1"));
    proxy.send(Packet::heartbeat(DeviceRoute::root()));
    proxy.send(Packet::link(LinkStatus::SensorDisconnected));

    wait_for(&unheard, lost);
}

#[test]
fn a_route_no_receiver_follows_is_not_asked_for_metadata() {
    let (store, proxy) = engine();
    let (root, elsewhere) = (DeviceRoute::root(), route("/2"));
    let _device = metadata(&store, route("/1"));
    describe(&proxy, root, 1);
    proxy.send(samples(0, root));
    proxy.send(samples(0, elsewhere));

    assert!(
        !wrote_within(&proxy, Duration::from_millis(500)),
        "a route no receiver follows is never asked for metadata"
    );

    let _followed = presence(&store, elsewhere);
    let (route, args, _) = metadata_call(&proxy);
    assert_eq!(route, elsewhere);
    assert!(args.is_empty(), "the newly followed route bootstraps");
}

#[test]
fn overlapping_receivers_share_one_discovery() {
    let (store, proxy) = engine();
    let child = route("/1");
    let _followed = [
        metadata(&store, DeviceRoute::root()),
        metadata(&store, child),
        metadata(&store, child),
    ];
    let _present = presence(&store, child);
    proxy.send(samples(0, DeviceRoute::root()));
    proxy.send(samples(0, child));

    let first = metadata_call(&proxy);
    let second = metadata_call(&proxy);
    assert_ne!(
        first.0, second.0,
        "one bootstrap per route, not per receiver"
    );
    assert!(
        !wrote_within(&proxy, Duration::from_millis(500)),
        "the second receiver started no second discovery"
    );
}

/// A device is connecting until it is heard, alive with the session its
/// heartbeat reports, lost when its link is, and connecting again when
/// the link reconnects. A late receiver starts from the presence now.
#[test]
fn presence_follows_packets_heartbeats_and_link_statuses() {
    let (store, proxy) = engine();
    let root = DeviceRoute::root();
    let followed = presence(&store, root);
    assert_eq!(followed.recv(), Ok(Presence::Connecting));
    proxy.send(Packet::heartbeat_session(7, root));
    assert_eq!(
        followed.recv_timeout(Duration::from_secs(5)),
        Ok(Presence::Alive {
            session: Some(crate::proto::SessionId::new(7))
        })
    );

    proxy.send(Packet::link(LinkStatus::SensorDisconnected));
    assert!(matches!(followed.recv(), Ok(Presence::Lost { .. })));

    proxy.send(Packet::link(LinkStatus::SensorReconnected));
    assert_eq!(followed.recv(), Ok(Presence::Connecting));
    assert_eq!(presence(&store, root).recv(), Ok(Presence::Connecting));
}

/// A device under a hub that goes quiet while the hub keeps sending is
/// lost once it has been silent for [`QUIET`], and alive again once it
/// speaks.
#[tokio::test(start_paused = true)]
async fn a_device_silent_while_its_hub_speaks_is_lost() {
    let tasks = Supervisor::current();
    let (store, proxy) = engine_on(&tasks);
    let (hub, device) = (DeviceRoute::root(), route("/1"));
    let followed = presence(&store, device);
    proxy.send(Packet::heartbeat(device));
    while !followed.recv_async().await.as_ref().is_ok_and(alive) {}
    let heard = Instant::now();

    let beating = async {
        loop {
            proxy.send(Packet::heartbeat(hub));
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    tokio::select! {
        () = beating => unreachable!(),
        now = followed.recv_async() => assert!(now.as_ref().is_ok_and(lost), "{now:?}"),
    }
    assert!(heard.elapsed() >= QUIET, "lost after {:?}", heard.elapsed());

    proxy.send(Packet::heartbeat(device));
    while !followed.recv_async().await.as_ref().is_ok_and(alive) {}
}

/// A device silent for [`QUIET`] is lost then on the paused clock, the
/// moment its presence gives is that one, and how long ago the proxy
/// says it was lost follows the paused clock after.
#[tokio::test(start_paused = true)]
async fn a_quiet_loss_and_its_moment_follow_the_paused_clock() {
    let tasks = Supervisor::current();
    let (store, proxy) = engine_on(&tasks);
    let device = route("/1");
    let followed = presence(&store, device);
    proxy.send(Packet::heartbeat(device));
    while !followed.recv_async().await.as_ref().is_ok_and(alive) {}
    let heard = Instant::now();

    let now = followed.recv_async().await;
    let Ok(Presence::Lost { since }) = now else {
        panic!("the device was not lost, got {now:?}");
    };
    assert_eq!(heard.elapsed(), QUIET);
    assert_eq!(since, (heard + QUIET).into_std());
    tokio::time::sleep(Duration::from_secs(60)).await;
    let lost = Update::presence(Presence::Lost { since });
    assert_eq!(lost, Update::Presence(Err(Some(60_000))));
}

/// A device's metadata is pending until it is complete, and pending
/// again once its link drops it.
#[test]
fn metadata_is_ready_once_complete_and_pending_after_a_disconnect() {
    let (store, proxy) = engine();
    let root = DeviceRoute::root();
    let followed = metadata(&store, root);
    describe(&proxy, root, 1);
    let ready = wait_for(&followed, |state| snapshot(state).is_some());
    assert_eq!(snapshot(&ready).unwrap().device().name(), "d");

    proxy.send(Packet::link(LinkStatus::SensorDisconnected));
    assert!(matches!(
        followed.recv_timeout(Duration::from_secs(5)),
        Ok(MetadataState::Pending)
    ));
}

/// A route list holds every route heard within its scope, sorted, and
/// only those.
#[test]
fn a_route_list_holds_the_routes_heard_within_its_scope() {
    let (store, proxy) = engine();
    let routes = lock(&store).follow_routes(Scope::subtree(route("/1")));
    assert_eq!(routes.recv(), Ok(Vec::new()));

    for heard in ["/1/2", "/2", "/1/0"] {
        proxy.send(Packet::heartbeat(route(heard)));
    }
    let heard = wait_for(&routes, |heard| heard.len() == 2);
    assert_eq!(heard, [route("/1/0"), route("/1/2")]);
    let all = lock(&store).follow_routes(Scope::subtree(DeviceRoute::root()));
    assert_eq!(all.recv().map(|all| all.len()), Ok(3));
}

/// Setting changes arrive one by one, in the order announced, to the
/// receivers of the device that announced them and no other's.
#[test]
fn setting_changes_arrive_in_order_at_their_own_device() {
    let (store, proxy) = engine();
    let (device, other) = (route("/1"), DeviceRoute::root());
    let changes = lock(&store).follow(device, CHANGES);
    let elsewhere = lock(&store).follow(other, CHANGES);

    proxy.send(announcement("test.amplitude", Some(9), &[1], device));
    proxy.send(announcement("rpc.hash", None, &7u32.to_le_bytes(), device));
    let first = changes.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        (first.name.as_str(), first.seq, first.reply),
        ("test.amplitude", Some(9), vec![1])
    );
    let hash = changes.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!((hash.name.as_str(), hash.seq), ("rpc.hash", None));
    assert_eq!(
        elsewhere.recv_timeout(Duration::from_millis(200)).err(),
        Some(RecvError::Timeout),
        "another device's announcement is not this device's change"
    );
}

/// The name, hash and session of the table [`answer_table`] serves.
const TABLE_DEVICE: &str = "state-test-device";
const TABLE_HASH: u32 = 0x5747_e001;

/// Answers the calls the session writes as a device with an announced
/// setting and `settings.version` would, until no call comes for a
/// while. Returns how many times the table was loaded.
fn answer_table(proxy: &TestProxy) -> std::thread::JoinHandle<usize> {
    answer_tables(proxy, Arc::new(AtomicU32::new(TABLE_HASH)))
}

/// Answers as [`answer_table`] does, as a device whose table has the
/// hash that `hash` holds at each call. A table with an odd hash lists
/// the announced setting, and one with an even hash hides it.
fn answer_tables(proxy: &TestProxy, hash: Arc<AtomicU32>) -> std::thread::JoinHandle<usize> {
    let (sent, deliver) = (proxy.sent.clone(), proxy.deliver.clone());
    std::thread::spawn(move || {
        let proxy = TestProxy { sent, deliver };
        let mut loads = 0;
        while let Ok(packet) = proxy.sent.recv_timeout(Duration::from_secs(2)) {
            let asked = answer(&proxy, &packet, hash.load(Ordering::SeqCst));
            loads += usize::from(asked.as_deref() == Some("dev.name"));
        }
        loads
    })
}

/// Answers `packet` as the device of [`answer_tables`] does, with the
/// table of hash `hash`. Returns the name of the RPC it calls, if it is
/// a call by name.
fn answer(proxy: &TestProxy, packet: &Packet, hash: u32) -> Option<String> {
    let listed = [("test.amplitude", 0xA310u16), ("settings.version", 0x8140)];
    let table = &listed[usize::from(hash.is_multiple_of(2))..];
    let packet::Payload::RpcRequest(request) = packet.payload() else {
        return None;
    };
    let wire_rpc::Method::ByName(name) = request.method else {
        return None;
    };
    let (id, route) = (request.id.value(), packet.route());
    let name = std::str::from_utf8(name).expect("a UTF-8 name");
    let reply = match name {
        "dev.name" => TABLE_DEVICE.as_bytes().to_vec(),
        "rpc.hash" => hash.to_le_bytes().to_vec(),
        "dev.session" => 7u32.to_le_bytes().to_vec(),
        "rpc.listinfo" if request.args.is_empty() => (table.len() as u16).to_le_bytes().to_vec(),
        "rpc.listinfo" => {
            let index = u16::from_le_bytes([request.args[0], request.args[1]]);
            let (name, meta) = table[usize::from(index)];
            let mut reply = meta.to_le_bytes().to_vec();
            reply.extend_from_slice(name.as_bytes());
            reply
        }
        "settings.version" => 5u32.to_le_bytes().to_vec(),
        "test.amplitude" => vec![1],
        _ => {
            proxy.refuse(id, wire_rpc::RpcError::NotFound, route);
            return Some(name.to_string());
        }
    };
    proxy.reply(id, &reply, route);
    Some(name.to_string())
}

/// A change the device announces after an `rpc.hash` that names another
/// table carries no table until that table loads, and the new table
/// after. A change recorded before keeps the old table, and decodes with
/// it while the store holds a new one that lacks its entry.
#[test]
fn a_change_after_a_new_table_carries_the_new_table() {
    const SWITCHED: u32 = 0x5747_e101;
    let (store, proxy) = engine();
    let root = DeviceRoute::root();
    let hash = Arc::new(AtomicU32::new(SWITCHED));
    let answering = answer_tables(&proxy, Arc::clone(&hash));
    proxy.send(Packet::heartbeat(root));
    let (tables, changes) = (
        follow(&store, root, REGISTRY),
        follow(&store, root, CHANGES),
    );
    let wait = Duration::from_secs(5);
    let old = tables.recv_timeout(wait).unwrap().expect("the table loads");

    proxy.send(announcement("test.amplitude", Some(6), &[2], root));
    let before = changes.recv_timeout(wait).unwrap();
    hash.store(SWITCHED + 1, Ordering::SeqCst);
    let announced = (SWITCHED + 1).to_le_bytes();
    proxy.send(announcement("rpc.hash", Some(7), &announced, root));
    proxy.send(announcement("test.amplitude", Some(8), &[3], root));
    let new = tables
        .recv_timeout(wait)
        .unwrap()
        .expect("the new table loads");
    proxy.send(announcement("test.amplitude", Some(9), &[4], root));
    let carried: Vec<_> = (0..3)
        .map(|_| changes.recv_timeout(wait).unwrap())
        .map(|change| change.rpc_registry().map(Arc::as_ptr))
        .collect();

    assert_eq!(new.hash, Some(SWITCHED + 1));
    assert!(Arc::ptr_eq(before.rpc_registry().unwrap(), &old));
    assert_eq!(carried, [None, None, Some(Arc::as_ptr(&new))]);
    assert_eq!(before.value().unwrap(), RpcValue::U64(2));
    drop((tables, changes));
    answering.join().expect("the device answers");
}

/// A device heard again after its link reconnects reports no session
/// until its next heartbeat, so a setting it announces before then
/// carries no table, while the store still holds the table read in its
/// session.
#[test]
fn a_change_before_the_first_heartbeat_after_a_reconnect_carries_no_table() {
    let (store, proxy) = engine();
    let root = DeviceRoute::root();
    let answering = answer_table(&proxy);
    proxy.send(Packet::heartbeat(root));
    let (tables, changes) = (
        follow(&store, root, REGISTRY),
        follow(&store, root, CHANGES),
    );
    let wait = Duration::from_secs(5);
    let table = tables.recv_timeout(wait).unwrap().expect("the table loads");

    proxy.send(announcement("test.amplitude", Some(6), &[2], root));
    let before = changes.recv_timeout(wait).unwrap();
    proxy.send(Packet::link(LinkStatus::SensorReconnected));
    proxy.send(announcement("test.amplitude", Some(7), &[3], root));
    let after = changes.recv_timeout(wait).unwrap();

    assert!(Arc::ptr_eq(before.rpc_registry().unwrap(), &table));
    assert!(after.rpc_registry().is_none());
    drop((tables, changes));
    answering.join().expect("the device answers");
}

/// A table whose load was in flight when the link reconnected is held
/// once it lands, but is not in force, so a change recorded then carries
/// no table. The load the reconnect calls for puts its table in force.
#[test]
fn a_table_loaded_across_a_reconnect_is_not_in_force() {
    let (store, proxy) = engine();
    let root = DeviceRoute::root();
    proxy.send(Packet::heartbeat(root));
    let (tables, changes) = (
        follow(&store, root, REGISTRY),
        follow(&store, root, CHANGES),
    );
    let wait = Duration::from_secs(5);
    let deadline = StdInstant::now() + wait;
    let name_of = |packet: &Packet| match packet.payload() {
        packet::Payload::RpcRequest(request) => match request.method {
            wire_rpc::Method::ByName(name) => name.to_vec(),
            wire_rpc::Method::ById(_) => Vec::new(),
        },
        _ => Vec::new(),
    };

    // The load is in flight once the worker asks the device's name.
    let mut asked = Vec::new();
    while asked
        .last()
        .is_none_or(|packet| name_of(packet) != b"dev.name")
    {
        asked.push(proxy.sent.recv_timeout(wait).expect("the worker loads"));
    }
    proxy.send(Packet::link(LinkStatus::SensorReconnected));
    for packet in &asked {
        answer(&proxy, packet, TABLE_HASH);
    }
    let landed = loop {
        assert!(StdInstant::now() < deadline, "the load never landed");
        if let Ok(Some(loaded)) = tables.try_recv() {
            break loaded.expect("the table loads");
        }
        if let Ok(packet) = proxy.sent.recv_timeout(Duration::from_millis(20)) {
            answer(&proxy, &packet, TABLE_HASH);
        }
    };
    proxy.send(announcement("test.amplitude", Some(6), &[2], root));
    let across = changes.recv_timeout(wait).unwrap();

    let answering = answer_table(&proxy);
    let reloaded = tables
        .recv_timeout(wait)
        .unwrap()
        .expect("the table loads again");
    proxy.send(announcement("test.amplitude", Some(7), &[3], root));
    let after = changes.recv_timeout(wait).unwrap();

    assert!(across.rpc_registry().is_none());
    assert!(!Arc::ptr_eq(&landed, &reloaded));
    assert!(Arc::ptr_eq(after.rpc_registry().unwrap(), &reloaded));
    drop((tables, changes));
    answering.join().expect("the device answers");
}

/// A device heard again after it went quiet has reconnected, though its
/// link reported nothing, as a port of a hub that is plugged in again
/// does. A change it announces before its table loads again carries no
/// table, even as its heartbeats report no session, and the table the
/// reconnect calls for is in force once it loads.
#[tokio::test(start_paused = true)]
async fn a_device_heard_again_after_going_quiet_loads_its_table_again() {
    const QUIETED: u32 = 0x5747_e201;
    let tasks = Supervisor::current();
    let (store, proxy) = engine_on(&tasks);
    let device = route("/1");
    let (tables, changes, followed) = (
        follow(&store, device, REGISTRY),
        follow(&store, device, CHANGES),
        presence(&store, device),
    );
    proxy.send(Packet::heartbeat(device));
    let old = answer_until_loaded(&proxy, &tables, QUIETED).await;
    answer_until_idle(&proxy, QUIETED).await;
    proxy.send(announcement("test.amplitude", Some(6), &[2], device));
    let before = changes.recv_async().await.unwrap();

    let quiet = Instant::now();
    while !followed.recv_async().await.as_ref().is_ok_and(lost) {}
    assert!(quiet.elapsed() >= QUIET, "lost after {:?}", quiet.elapsed());
    proxy.send(Packet::heartbeat(device));
    proxy.send(announcement("test.amplitude", Some(7), &[3], device));
    let after_quiet = changes.recv_async().await.unwrap();
    assert!(
        after_quiet.rpc_registry().is_none(),
        "the table read before the device went quiet is in force"
    );
    let reloading = answer_until_loaded(&proxy, &tables, QUIETED);
    let new = tokio::time::timeout(QUIET, reloading)
        .await
        .expect("the table loads again");
    answer_until_idle(&proxy, QUIETED).await;
    proxy.send(announcement("test.amplitude", Some(8), &[4], device));
    let after_reload = changes.recv_async().await.unwrap();

    assert!(Arc::ptr_eq(before.rpc_registry().unwrap(), &old));
    assert!(!Arc::ptr_eq(&old, &new));
    assert!(Arc::ptr_eq(after_reload.rpc_registry().unwrap(), &new));
}

/// Answers each call the session writes as the device of
/// [`answer_tables`] with the table of hash `hash` does, until a table
/// lands in `tables`. Returns the table.
async fn answer_until_loaded(
    proxy: &TestProxy,
    tables: &Receiver<RegistryState>,
    hash: u32,
) -> Arc<RpcRegistry> {
    loop {
        tokio::select! {
            biased;
            table = tables.recv_async() => return table.unwrap().expect("the table loads"),
            packet = proxy.sent.recv_async() => {
                answer(proxy, &packet.unwrap(), hash);
            }
        }
    }
}

/// Answers each call the session writes as [`answer_until_loaded`]
/// does, until none comes for a while.
async fn answer_until_idle(proxy: &TestProxy, hash: u32) {
    let idle = Duration::from_millis(100);
    while let Ok(packet) = tokio::time::timeout(idle, proxy.sent.recv_async()).await {
        answer(proxy, &packet.unwrap(), hash);
    }
}

/// The table is loaded once for every receiver, the settings are read
/// and then kept by each announcement, and a new table hash loads the
/// table again, which drops the settings read with the old one.
#[test]
fn the_table_and_settings_load_once_and_follow_announcements() {
    let (store, proxy) = engine();
    let root = DeviceRoute::root();
    let answering = answer_table(&proxy);
    proxy.send(Packet::heartbeat(root));
    let settings = follow(&store, root, SETTINGS);
    let wait = Duration::from_secs(5);

    let first = settings.recv_timeout(wait).expect("the settings are read");
    assert_eq!(first.version(), Some(5));
    assert_eq!(first.get("test.amplitude"), Some(&[1u8][..]));
    assert_eq!(first.get("settings.version"), None);
    let registry = follow(&store, root, REGISTRY)
        .recv_timeout(wait)
        .expect("the table is shared")
        .expect("the table loaded");
    assert_eq!(registry.hash, Some(TABLE_HASH));

    proxy.send(announcement("test.amplitude", Some(6), &[2], root));
    let next = settings
        .recv_timeout(wait)
        .expect("the announcement is kept");
    assert_eq!(
        (next.version(), next.get("test.amplitude")),
        (Some(6), Some(&[2u8][..]))
    );

    let tables = follow(&store, root, REGISTRY);
    tables.recv_timeout(wait).unwrap().unwrap();
    proxy.send(announcement(
        "rpc.hash",
        Some(7),
        &(TABLE_HASH + 1).to_le_bytes(),
        root,
    ));
    tables
        .recv_timeout(wait)
        .expect("the table loads again")
        .unwrap();
    let stale = follow(&store, root, SETTINGS);
    if let Ok(Some(read)) = stale.try_recv() {
        assert_eq!(
            read.version(),
            Some(5),
            "settings of the old table were kept"
        );
    }
    let reread = settings
        .recv_timeout(wait)
        .expect("the settings are read again");
    assert_eq!(reread.version(), Some(5));
    drop((settings, stale, tables));
    assert_eq!(answering.join().expect("the device answers"), 2);
}

/// A worker keeps its device's table while a receiver follows it and
/// for the linger after, so a receiver that follows again soon finds it
/// at once. Then the worker stops and the table is dropped, so a device
/// no one asks is never walked again and no one is handed a table no
/// one keeps current.
#[tokio::test(start_paused = true)]
async fn a_table_no_receiver_follows_is_dropped_after_its_linger() {
    let tasks = Supervisor::current();
    let (store, proxy) = engine_on(&tasks);
    let root = DeviceRoute::root();
    proxy.send(Packet::heartbeat(root));
    let registry = follow(&store, root, REGISTRY);
    answer_until_loaded(&proxy, &registry, TABLE_HASH).await;
    answer_until_idle(&proxy, TABLE_HASH).await;
    drop(registry);
    let again = follow(&store, root, REGISTRY);
    assert!(
        matches!(again.try_recv(), Ok(Some(Ok(_)))),
        "the table lingers"
    );
    drop(again);

    tokio::time::sleep(LINGER / 2).await;
    assert!(
        lock(&store).value(root, REGISTRY).is_some(),
        "the table was dropped before its linger ended"
    );
    tokio::time::sleep(LINGER).await;
    assert!(
        lock(&store).value(root, REGISTRY).is_none(),
        "the table outlived its worker"
    );
    proxy.send(Packet::link(LinkStatus::SensorReconnected));
    let asked = tokio::time::timeout(RELOAD_DELAY * 2, proxy.sent.recv_async()).await;
    assert!(asked.is_err(), "a stopped worker loaded the table again");
}

/// A table that failed to load is loaded again after [`RELOAD_DELAY`]
/// only once its device is heard, so a worker never asks a device that
/// is not there again and again. The device stays alive over the wait
/// on samples, which its worker does not hear.
#[tokio::test(start_paused = true)]
async fn a_table_that_failed_loads_again_only_once_its_device_is_heard() {
    let tasks = Supervisor::current();
    let (store, proxy) = engine_on(&tasks);
    let device = route("/9");
    let registry = follow(&store, device, REGISTRY);
    proxy.send(Packet::heartbeat(device));
    let idle = Duration::from_millis(500);
    while let Ok(packet) = tokio::time::timeout(idle, proxy.sent.recv_async()).await {
        let packet = packet.unwrap();
        if let packet::Payload::RpcRequest(request) = packet.payload() {
            proxy.refuse(
                request.id.value(),
                wire_rpc::RpcError::NotFound,
                packet.route(),
            );
        }
    }
    let failed = registry.recv_async().await.expect("the failure");
    assert!(failed.is_err(), "{failed:?}");

    let sampling = async {
        loop {
            proxy.send(samples(0, device));
            tokio::time::sleep(QUIET / 2).await;
        }
    };
    tokio::select! {
        () = sampling => unreachable!(),
        asked = tokio::time::timeout(RELOAD_DELAY * 2, proxy.sent.recv_async()) => {
            assert!(asked.is_err(), "a device not heard since was asked again");
        }
    }
    proxy.send(Packet::heartbeat(device));
    let asked = proxy.sent.recv_async().await.unwrap();
    assert_eq!(asked.route(), device);
}

/// A worker's task ends once the engine lets go of its device, sooner
/// than the call it waits on, never answered, times out. Every worker's
/// task ends with the engine. Devices followed and let go again and
/// again leave no task behind.
#[tokio::test(start_paused = true)]
async fn a_workers_task_ends_when_the_engine_lets_go_and_with_the_engine() {
    let (engine_tasks, workers) = (Supervisor::current(), Supervisor::current());
    let (session, proxy) = Session::test_pair_on(&engine_tasks);
    let store = Arc::new(Mutex::new(Store::new(true)));
    let mounts = Mounts::new("tcp://device");
    engine_tasks.spawn(
        "proxy state",
        serve(Arc::clone(&store), mounts, session, &workers),
    );
    let devices = [route("/1"), route("/2"), route("/3")];
    let follow_all = || -> Vec<_> {
        let followed = devices.iter();
        followed
            .map(|device| follow(&store, *device, REGISTRY))
            .collect()
    };

    for _ in 0..3 {
        let followed = follow_all();
        for device in devices {
            proxy.send(Packet::heartbeat(device));
        }
        running(&workers, devices.len()).await;
        drop(followed);
        let released = Instant::now();
        running(&workers, 0).await;
        let took = released.elapsed();
        assert!(
            took < LINGER * 2,
            "the workers ended {took:?} after release"
        );
    }

    let _followed = follow_all();
    running(&workers, devices.len()).await;
    drop(proxy);
    let ended = Instant::now();
    running(&workers, 0).await;
    let took = ended.elapsed();
    assert!(took < LINGER, "the workers ended {took:?} after the engine");
}

/// Waits until `count` of the tasks `tasks` spawned run.
async fn running(tasks: &Supervisor, count: usize) {
    let waited = tokio::time::timeout(Duration::from_secs(60), async {
        while tasks.running() != count {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    assert!(
        waited.await.is_ok(),
        "{} tasks run, not {count}",
        tasks.running()
    );
}

/// A table followed of a device not heard starts no worker, so a watch
/// of a route where nothing is starts no task and asks nothing, until
/// the device is heard.
#[test]
fn a_table_of_a_device_not_heard_is_not_loaded_until_it_is() {
    let (store, proxy) = engine();
    let device = route("/9");
    let _registry = follow(&store, device, REGISTRY);
    assert!(
        !wrote_within(&proxy, Duration::from_millis(500)),
        "a worker asked"
    );

    proxy.send(Packet::heartbeat(device));
    let (asked, ..) = proxy.call();
    assert_eq!(asked, device);
}

/// A device a reconnect of its link left connecting is lost once it has
/// been silent for [`QUIET`], rather than connecting for ever.
#[tokio::test(start_paused = true)]
async fn a_device_left_connecting_by_a_reconnect_is_lost_when_silent() {
    let tasks = Supervisor::current();
    let (store, proxy) = engine_on(&tasks);
    let device = route("/1");
    let followed = presence(&store, device);
    proxy.send(Packet::heartbeat(device));
    while !followed.recv_async().await.as_ref().is_ok_and(alive) {}

    proxy.send(Packet::link(LinkStatus::SensorReconnected));
    assert_eq!(followed.recv_async().await, Ok(Presence::Connecting));
    let reconnected = Instant::now();
    let now = tokio::time::timeout(QUIET * 2, followed.recv_async()).await;
    assert!(
        now.as_ref().is_ok_and(|now| now.as_ref().is_ok_and(lost)),
        "{now:?}"
    );
    assert!(
        reconnected.elapsed() >= QUIET,
        "lost after {:?}",
        reconnected.elapsed()
    );
}

/// A device whose link fails to reconnect is lost at the moment of the
/// status on the paused clock.
#[tokio::test(start_paused = true)]
async fn a_loss_by_status_is_dated_on_the_paused_clock() {
    let tasks = Supervisor::current();
    let (store, proxy) = engine_on(&tasks);
    let device = route("/1");
    let followed = presence(&store, device);
    proxy.send(Packet::heartbeat(device));
    while !followed.recv_async().await.as_ref().is_ok_and(alive) {}
    tokio::time::sleep(QUIET / 3).await;

    proxy.send(Packet::link(LinkStatus::FailedToReconnect).with_route(device));
    let failed = Instant::now();
    let now = followed.recv_async().await;
    let Ok(Presence::Lost { since }) = now else {
        panic!("the device was not lost, got {now:?}");
    };
    assert_eq!(since, failed.into_std());
}

/// The engine follows every packet its link sends, however many, so a
/// status after a flood of samples still reaches the receivers.
#[test]
fn the_engine_loses_none_of_its_input() {
    let (store, proxy) = engine();
    let root = DeviceRoute::root();
    let followed = presence(&store, root);
    let changes = lock(&store).follow(root, CHANGES);
    for first in 0..20_000 {
        proxy.send(samples(first, root));
    }
    for seq in 0..100 {
        proxy.send(announcement("test.amplitude", Some(seq), &[1], root));
    }
    proxy.send(Packet::link(LinkStatus::SensorDisconnected));

    let seqs: Vec<_> = (0..100)
        .map(|_| changes.recv_timeout(Duration::from_secs(5)).unwrap().seq)
        .collect();
    assert_eq!(seqs, (0..100).map(Some).collect::<Vec<_>>());
    wait_for(&followed, lost);
}

/// A route whose metadata query failed is asked again when its backoff
/// ends on the clock the engine sleeps on, and not before.
#[tokio::test(start_paused = true)]
async fn a_failed_metadata_query_is_asked_again_when_its_backoff_ends() {
    let tasks = Supervisor::current();
    let (store, proxy) = engine_on(&tasks);
    let _followed = metadata(&store, DeviceRoute::root());
    tokio::time::sleep(Duration::from_millis(1)).await;
    proxy.send(samples(0, DeviceRoute::root()));

    let asked = proxy.sent.recv_async().await.unwrap();
    let packet::Payload::RpcRequest(request) = asked.payload() else {
        panic!("expected a metadata query, got {:?}", asked.payload());
    };
    proxy.refuse(request.id.value(), wire_rpc::RpcError::Busy, asked.route());
    let refused = Instant::now();

    let again = proxy.sent.recv_async().await.unwrap();
    assert_eq!(again.route(), asked.route());
    assert_eq!(refused.elapsed(), super::metadata::RETRY_FIRST);
}

/// A route whose metadata query is backing off and that the engine
/// stops asking, as its device is lost or no receiver follows it, sets
/// no wake for the retry, so the engine sleeps past the moment it was
/// due. Asked again, it is asked at once as its backoff has passed, and
/// its next failure doubles the delay as the schedule says.
#[tokio::test(start_paused = true)]
async fn a_retry_the_engine_will_not_make_sets_no_wake() {
    for unfollowed in [false, true] {
        let tasks = Supervisor::current();
        let (session, proxy) = Session::test_pair_on(&tasks);
        let store = Arc::new(Mutex::new(Store::new(true)));
        let mounts = Mounts::new("tcp://device");
        let mut engine = Engine::new(&store, &mounts, session, &tasks);
        let device = route("/1");
        let mut followed = Some(presence(&store, device));
        engine.take(&Packet::heartbeat(device));
        engine.discover();
        refuse_busy(&proxy, &mut engine, device).await;
        let retry = Instant::now() + super::metadata::RETRY_FIRST;
        assert_eq!(engine.next_wake(), Some(retry));

        if unfollowed {
            followed = None;
            pruned(|cx| lock(&store).prune_gone(cx)).await;
            engine.follow_demand();
        } else {
            engine.apply_status(device, LinkStatus::FailedToReconnect);
        }
        crate::chan::sleep_until(retry + Duration::from_millis(1)).await;
        let wake = engine.next_wake();
        assert!(
            wake.is_none_or(|wake| wake > Instant::now()),
            "unfollowed {unfollowed}, the engine wakes at once for a retry it will not make"
        );

        if followed.is_none() {
            followed = Some(presence(&store, device));
            engine.follow_demand();
        }
        engine.take(&Packet::heartbeat(device));
        engine.discover();
        let asked_at = Instant::now();
        refuse_busy(&proxy, &mut engine, device).await;
        assert_eq!(
            asked_at,
            Instant::now(),
            "unfollowed {unfollowed}, asked late"
        );
        let doubled = asked_at + super::metadata::RETRY_FIRST * 2;
        assert_eq!(engine.next_wake(), Some(doubled), "unfollowed {unfollowed}");
        drop(followed);
    }
}

/// Refuses the one `dev.metadata` call to `device` that the engine has
/// sent as busy, and has the engine take the refusal.
async fn refuse_busy(proxy: &TestProxy, engine: &mut Engine, device: DeviceRoute) {
    let asking = tokio::time::timeout(Duration::from_secs(1), proxy.sent.recv_async());
    let asked = asking.await.expect("the engine asks").unwrap();
    let packet::Payload::RpcRequest(request) = asked.payload() else {
        panic!("expected a metadata query, got {:?}", asked.payload());
    };
    assert_eq!(asked.route(), device);
    proxy.refuse(request.id.value(), wire_rpc::RpcError::Busy, device);
    let reply = engine.schedule.replies().recv_async().await.unwrap();
    engine.take_metadata_replies(reply);
}

/// A status supersedes the presence of every device in the subtree it
/// covers, so a mount's loss never outlives the reconnect of the whole
/// link that ended it.
#[test]
fn a_broader_status_supersedes_the_narrower_state_it_covers() {
    let mount = route("/2");
    let (store, proxy) = engine();
    let early = presence(&store, mount);
    proxy.send(samples(0, mount));
    wait_for(&early, alive);
    proxy.send(Packet::link(LinkStatus::SensorDisconnected).with_route(mount));
    wait_for(&early, lost);

    proxy.send(Packet::link(LinkStatus::SensorReconnected));
    wait_for(&early, |presence| *presence == Presence::Connecting);
    let below = lock(&store).value(route("/2/1"), PRESENCE);
    assert!(
        below.is_none_or(|below| below == Presence::Connecting),
        "{below:?}"
    );
}

/// Every receiver ends once the engine does, with its link, rather than
/// waiting on state no one will send. The status saying why the link
/// failed may be the last packet the link sends, and it still counts.
#[test]
fn the_receivers_end_with_the_engine() {
    let (store, proxy) = engine();
    let followed = presence(&store, DeviceRoute::root());
    proxy.send(Packet::heartbeat(DeviceRoute::root()));
    proxy.send(Packet::link(LinkStatus::FailedToReconnect));
    drop(proxy);

    let deadline = StdInstant::now() + Duration::from_secs(5);
    let mut heard = false;
    let ended = loop {
        match followed.recv_deadline(deadline) {
            Ok(now) => heard |= lost(&now),
            Err(ended) => break ended,
        }
    };
    assert!(heard, "the link's last status was not taken");
    assert_eq!(ended, RecvError::Disconnected);

    let late = presence(&store, DeviceRoute::root());
    let ended = loop {
        if let Err(ended) = late.recv_timeout(Duration::from_secs(5)) {
            break ended;
        }
    };
    assert_eq!(ended, RecvError::Disconnected);
}

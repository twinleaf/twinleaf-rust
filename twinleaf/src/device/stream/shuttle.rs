//! Properties of the store, the session and the pump under shuttle's
//! schedulers. The session and the pump run as shuttle tasks, and the test's
//! proxy and readers as shuttle threads and tasks.

use super::{RecvError, Scope, Stream};
use crate::chan::{self, lock};
use crate::device::fixtures::{broadcast, metadata_records, segment_record};
use crate::device::session::{Handle, Session, TestProxy};
use crate::device::store::{Store, CHANGES, PRESENCE};
use crate::device::{Presence, SettingChange};
use crate::proto::data as wire;
use crate::proto::{DeviceRoute, SegmentId, SessionId};
use crate::proxy::{decode, parts, Update, Watch, WATCH};
use crate::rt::Supervisor;
use crate::tio::packet::{LinkStatus, Packet, Payload};
use ::shuttle::future::{block_on, spawn};
use ::shuttle::thread;
use std::sync::{Arc, Mutex};

/// How many schedules each scheduler runs per property.
const ITERATIONS: usize = 1000;

/// The presence of a device alive in `session`.
fn alive(session: u32) -> Presence {
    Presence::Alive {
        session: Some(SessionId::new(session)),
    }
}

/// The session `presence` names, or 0 for none.
fn session_of(presence: &Presence) -> u32 {
    match presence {
        Presence::Alive {
            session: Some(session),
        } => session.value(),
        _ => 0,
    }
}

/// The change of a setting counted as `seq`.
fn change(seq: u32) -> SettingChange {
    SettingChange::new("test.gain".to_string(), Some(seq), vec![1], None)
}

/// A receiver that follows a device's presence while the pump updates the
/// store gets the value the store held when it followed, or a newer one, and
/// never an older one after a newer, up to the last. A receiver of the
/// device's setting changes gets each change recorded after it followed, in
/// order, and none recorded before.
#[test]
fn a_new_subscriber_gets_the_held_value_or_a_newer_one_then_every_later_change() {
    const SET: u32 = 3;
    chan::check(ITERATIONS, || {
        let route = DeviceRoute::root();
        let store = Arc::new(Mutex::new(Store::new(false)));
        let pump = thread::spawn({
            let store = Arc::clone(&store);
            move || {
                for n in 1..=SET {
                    let mut store = lock(&store);
                    store.set(route, PRESENCE, alive(n));
                    store.set(route, CHANGES, change(n));
                }
            }
        });

        let (held, presence, changes) = {
            let mut store = lock(&store);
            let held = store.value(route, PRESENCE);
            let held = held.map_or(0, |presence| session_of(&presence));
            let presence = store.follow(route, PRESENCE);
            (held, presence, store.follow(route, CHANGES))
        };
        let mut newest = session_of(&presence.recv().expect("the store is open"));
        assert!(
            newest >= held,
            "presence {newest} came when {held} was held"
        );
        while newest < SET {
            let now = session_of(&presence.recv().expect("the store is open"));
            assert!(now >= newest, "presence {now} came after {newest}");
            newest = now;
        }
        let later: Vec<_> = (held + 1..=SET)
            .map(|_| changes.recv().expect("the store is open").seq)
            .collect();
        assert_eq!(later, (held + 1..=SET).map(Some).collect::<Vec<_>>());
        pump.join().unwrap();
        assert!(
            matches!(changes.try_recv(), Ok(None)),
            "a change came twice"
        );
    });
}

/// A stream over a session whose socket is a pair of channels, the session
/// itself, its far end, and the supervisor of their tasks.
fn stream() -> (Arc<Stream>, Session, TestProxy, Supervisor) {
    let tasks = Supervisor::new();
    let (session, proxy) = Session::test_pair_on(&tasks);
    let stream = Arc::new(Stream::new(Handle::root(session.clone())));
    (stream, session, proxy, tasks)
}

/// Drops what is left of a scenario's stream and session, then waits for
/// their tasks to end.
fn finish<T>(tasks: Supervisor, rest: T) {
    drop(rest);
    block_on(tasks.wait());
}

/// How a scenario ends its session.
#[derive(Clone, Copy)]
enum Ending {
    /// Its owner closes it.
    Close,
    /// The proxy stops.
    Stop,
}

/// Ends a stream's session as `ending` says, while one reader blocks in a
/// receive of the stream's batches and another awaits the device's presence.
/// Returns why each one's receiver ended.
fn wake_on(ending: Ending) -> (RecvError, RecvError) {
    let (stream, session, proxy, tasks) = stream();
    let root = DeviceRoute::root();
    let batches = stream.batches(Scope::subtree(root));
    let presence = stream.state(root, PRESENCE);
    let blocked = thread::spawn(move || loop {
        match batches.recv() {
            Ok(_) | Err(RecvError::Lagged(_)) => {}
            Err(ended) => return ended,
        }
    });
    let awaiting = spawn(async move {
        loop {
            match presence.recv_async().await {
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(ended) => return ended,
            }
        }
    });

    match ending {
        Ending::Close => session.close(),
        Ending::Stop => proxy.send(Packet::link(LinkStatus::Stopped)),
    }

    let ended = (blocked.join().unwrap(), block_on(awaiting).unwrap());
    finish(tasks, (stream, session, proxy));
    ended
}

/// A reader blocked in a receive and one awaiting a receive when the
/// connection closes both wake, and read that it was closed.
#[test]
fn a_reader_waiting_when_the_connection_closes_wakes_to_the_close() {
    chan::check(ITERATIONS, || {
        assert_eq!(
            wake_on(Ending::Close),
            (RecvError::Closed, RecvError::Closed)
        );
    });
}

/// A reader blocked in a receive and one awaiting a receive when the proxy
/// stops both wake, and read that it stopped.
#[test]
fn a_reader_waiting_when_the_proxy_stops_wakes_to_the_stop() {
    chan::check(ITERATIONS, || {
        assert_eq!(
            wake_on(Ending::Stop),
            (RecvError::Stopped, RecvError::Stopped)
        );
    });
}

/// The segments the device moves through, and the samples it sends in each.
const SEGMENTS: [u8; 2] = [0, 1];
const SAMPLES_PER_SEGMENT: u32 = 2;

/// The root device's metadata with `segment` as its stream's current
/// segment, which starts once the samples of the segments before it have
/// passed at 1 Hz, as the proxy sends it.
fn metadata(segment: u8) -> Update {
    let mut records = metadata_records(1);
    records[2] = wire::Metadata::Segment(wire::Segment {
        start_time: u32::from(segment) * SAMPLES_PER_SEGMENT,
        ..segment_record(segment)
    });
    let packets = records
        .into_iter()
        .map(|record| broadcast(record, DeviceRoute::root()));
    Update::Metadata(Some(
        packets
            .map(|packet| packet.as_bytes().to_vec().into())
            .collect(),
    ))
}

/// The id of the pump's watch of the root device, skipping the session's
/// other writes.
fn watch_id(proxy: &TestProxy) -> u16 {
    loop {
        let packet = proxy.sent.recv().expect("the pump watches the device");
        let Payload::ProxyTx(request) = packet.payload() else {
            continue;
        };
        let crate::proto::rpc::Method::ByName(name) = request.method else {
            continue;
        };
        let watch: Watch = match name == WATCH.as_bytes() {
            true => decode(request.args).expect("a watch"),
            false => continue,
        };
        if watch.route == DeviceRoute::root() && !watch.table {
            return request.id.value();
        }
    }
}

/// The proxy sends a device's metadata, then its samples, then a new segment
/// record, then the samples of the new segment, then a stop, all in that
/// order on one socket. Every sample reaches the client decoded with the
/// segment record the proxy sent last before it, and none is dropped.
#[test]
fn every_sample_decodes_with_the_last_record_sent_before_it() {
    chan::check(ITERATIONS, || {
        let (stream, session, proxy, tasks) = stream();
        let root = DeviceRoute::root();
        let batches = stream.batches(Scope::subtree(root));
        let id = watch_id(&proxy);
        let routes = Update::Routes(vec![root.as_slice().to_vec().into()]);
        for part in parts(id, &routes) {
            proxy.send(part);
        }
        let mut sent = Vec::new();
        for segment in SEGMENTS {
            for part in parts(id, &metadata(segment)) {
                proxy.send(part);
            }
            for n in 0..SAMPLES_PER_SEGMENT {
                proxy.send(Packet::samples(1, segment, n, &[0; 4], root).expect("samples"));
                sent.push((SegmentId::new(segment), n));
            }
        }
        proxy.send(Packet::link(LinkStatus::Stopped));

        let mut decoded = Vec::new();
        let ended = loop {
            match batches.recv() {
                Ok(batch) => {
                    let segment = batch.metadata().segment().segment_id();
                    let numbers = batch.sample_numbers().iter();
                    decoded.extend(numbers.map(|number| (segment, number.value())));
                }
                Err(ended) => break ended,
            }
        };
        assert_eq!(ended, RecvError::Stopped);
        assert_eq!(decoded, sent, "the samples decoded under other records");
        finish(tasks, (batches, stream, session, proxy));
    });
}

//! Properties of the store, the session and the pump under shuttle's
//! schedulers. The session and the pump run as shuttle tasks, and the test's
//! proxy and readers as shuttle threads and tasks.

use super::{Receiver, RecvError, Scope, Stream};
use crate::chan::lock;
use crate::data::SampleBatch;
use crate::device::fixtures::{broadcast, metadata_records, segment_record};
use crate::device::session::{Handle, Session, TestProxy};
use crate::device::store::{Store, CHANGES, METADATA, PRESENCE};
use crate::device::{MetadataState, Presence, SettingChange};
use crate::proto::data as wire;
use crate::proto::{DeviceRoute, SegmentId, SessionId};
use crate::proxy::{decode, parts, Update, Watch, WATCH};
use crate::rt::Supervisor;
use crate::seam::shuttle::{check, replaying};
use crate::tio::packet::{LinkStatus, Packet, Payload};
use ::shuttle::future::{block_on, spawn};
use ::shuttle::thread;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
    check(|| {
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
    check(|| {
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
    check(|| {
        assert_eq!(
            wake_on(Ending::Stop),
            (RecvError::Stopped, RecvError::Stopped)
        );
    });
}

/// How many segments the device moves through, and the samples it sends in
/// each. The device's ring holds two segments, so the third takes the slot
/// of the first again, under a record of its own.
const SEGMENTS: u8 = 3;
const SAMPLES_PER_SEGMENT: u32 = 2;

/// The slot in the stream's ring of the device's `nth` segment, and the
/// time that segment starts, once the samples of the segments before it
/// have passed at 1 Hz.
fn segment(nth: u8) -> (SegmentId, u32) {
    (
        SegmentId::new(nth % 2),
        u32::from(nth) * SAMPLES_PER_SEGMENT,
    )
}

/// The root device's metadata with its `nth` segment as its stream's
/// current one, as the proxy sends it.
fn metadata(nth: u8) -> Update {
    let (slot, start_time) = segment(nth);
    let mut records = metadata_records(1);
    records[2] = wire::Metadata::Segment(wire::Segment {
        start_time,
        ..segment_record(slot.value())
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

/// A sample as the proxy sent it or a reader decoded it, as the slot and
/// start time of its segment record and its sample number.
type Sample = (SegmentId, u32, u32);

/// Sends the routes of the watch `id`, then each segment's metadata
/// followed by its samples, as the proxy does on one socket. Returns each
/// sample sent, under the record sent last before it.
fn send_segments(proxy: &TestProxy, id: u16) -> Vec<Sample> {
    let root = DeviceRoute::root();
    let routes = Update::Routes(vec![root.as_slice().to_vec().into()]);
    for part in parts(id, &routes) {
        proxy.send(part);
    }
    let mut sent = Vec::new();
    for nth in 0..SEGMENTS {
        for part in parts(id, &metadata(nth)) {
            proxy.send(part);
        }
        let (slot, start_time) = segment(nth);
        for n in 0..SAMPLES_PER_SEGMENT {
            let samples = Packet::samples(1, slot.value(), n, &[0; 4], root);
            proxy.send(samples.expect("samples"));
            sent.push((slot, start_time, n));
        }
    }
    sent
}

/// Takes batches until the stream ends. Returns each sample under the
/// record it was decoded with, and why the stream ended.
fn read_samples(batches: &Receiver<SampleBatch>) -> (Vec<Sample>, RecvError) {
    let mut decoded = Vec::new();
    loop {
        match batches.recv() {
            Ok(batch) => {
                let record = batch.metadata().segment();
                let (slot, start_time) = (record.segment_id(), record.start_time());
                let numbers = batch.sample_numbers().iter();
                decoded.extend(numbers.map(|number| (slot, start_time, number.value())));
            }
            Err(ended) => return (decoded, ended),
        }
    }
}

/// The proxy sends a device's metadata, then its samples, then a new segment
/// record, then the samples of the new segment, and so on, then a stop, all
/// in that order on one socket. Every sample reaches the client decoded with
/// the segment record the proxy sent last before it, and none is dropped.
#[test]
fn every_sample_decodes_with_the_last_record_sent_before_it() {
    check(|| {
        let (stream, session, proxy, tasks) = stream();
        let batches = stream.batches(Scope::subtree(DeviceRoute::root()));
        let id = watch_id(&proxy);
        let sent = send_segments(&proxy, id);
        proxy.send(Packet::link(LinkStatus::Stopped));

        let (decoded, ended) = read_samples(&batches);
        assert_eq!(ended, RecvError::Stopped);
        assert_eq!(decoded, sent, "the samples decoded under other records");
        finish(tasks, (batches, stream, session, proxy));
    });
}

/// A subscriber that joins on a second thread once the device's metadata
/// has come, while the pump takes the device's samples and new segment
/// records, gets every sample taken after it joined, then the stop. Each is
/// decoded with the record the proxy sent last before it, and never with
/// one the parser held before, such as the record of the same slot from an
/// earlier lap of the ring. What it gets is the samples sent last, so a
/// sample it missed is one the pump took before it joined. Some schedule
/// gives it a sample, and so the samples of the segment that reuses a slot.
#[test]
fn a_late_subscriber_decodes_each_sample_with_the_record_in_force_for_it() {
    /// How many schedules gave the late subscriber a sample.
    static GOT_SAMPLES: AtomicUsize = AtomicUsize::new(0);
    check(|| {
        let (stream, session, proxy, tasks) = stream();
        let root = DeviceRoute::root();
        let early = stream.batches(Scope::subtree(root));
        let id = watch_id(&proxy);
        let late = thread::spawn({
            let stream = Arc::clone(&stream);
            move || {
                let metadata = stream.state(root, METADATA);
                loop {
                    match metadata.recv() {
                        Ok(MetadataState::Ready(_)) => break,
                        Ok(_) | Err(RecvError::Lagged(_)) => {}
                        Err(ended) => panic!("the stream ended with {ended:?} before metadata"),
                    }
                }
                read_samples(&stream.batches(Scope::subtree(root)))
            }
        });
        let sent = send_segments(&proxy, id);
        proxy.send(Packet::link(LinkStatus::Stopped));

        assert_eq!(read_samples(&early), (sent.clone(), RecvError::Stopped));
        let (decoded, ended) = late.join().unwrap();
        assert_eq!(ended, RecvError::Stopped);
        assert!(
            sent.ends_with(&decoded),
            "the late subscriber got {decoded:?} of {sent:?}"
        );
        if !decoded.is_empty() {
            GOT_SAMPLES.fetch_add(1, Ordering::Relaxed);
        }
        finish(tasks, (early, stream, session, proxy));
    });
    if !replaying() {
        assert!(
            GOT_SAMPLES.load(Ordering::Relaxed) > 0,
            "no schedule gave the late subscriber a sample"
        );
    }
}

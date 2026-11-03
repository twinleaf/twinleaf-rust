//! Properties of the lanes under shuttle's schedulers. The pump's end of a
//! lane sends on one thread while its reader takes items on another.

use crate::data::{Change, PacketParser, SampleBatch};
use crate::device::tests::fixtures::{description, samples};
use crate::proto::{DeviceRoute, SessionId};
use crate::seam::shuttle::check;
use crate::session::subscription::{BatchLane, End, Queued, Receiver, RecvError, Scope, Sink};
use ::shuttle::future::block_on;
use ::shuttle::sync::atomic::{AtomicBool, Ordering};
use ::shuttle::thread;
use std::sync::Arc;

/// How many values the pump sends in a scenario.
const SENT: u32 = 4;

fn everything() -> Scope {
    Scope::subtree(DeviceRoute::root())
}

/// Takes items from `receiver` until it ends, checking that none is older
/// than the one before. Returns the last item and the end.
async fn read_to_the_end(receiver: &Receiver<u32>) -> (u32, RecvError) {
    let mut last = 0;
    loop {
        match receiver.recv_async().await {
            Ok(value) => {
                assert!(value >= last, "{value} came after {last}");
                last = value;
            }
            Err(ended) => return (last, ended),
        }
    }
}

/// A reader that awaits a latest-value lane while the pump replaces its value
/// never gets an older value after a newer one. It ends on the last value
/// sent, and reads the end after it.
#[test]
fn a_latest_value_reader_never_goes_back_and_ends_on_the_last_value() {
    check(|| {
        let (mut sink, receiver) = Sink::latest(everything());
        let pump = thread::spawn(move || {
            for value in 1..=SENT {
                assert!(sink.lane.send(value), "the reader is gone");
            }
            sink.lane.end(End::Closed);
        });

        let read = block_on(read_to_the_end(&receiver));

        assert_eq!(read, (SENT, RecvError::Closed));
        pump.join().unwrap();
    });
}

/// A latest-value lane that holds a value never looks empty while the pump
/// replaces it, and a reader that blocks on it while the pump replaces it
/// always wakes, up to the last value sent.
#[test]
fn a_latest_value_lane_never_looks_empty_while_it_holds_a_value() {
    check(|| {
        let (mut sink, receiver) = Sink::latest(everything());
        assert!(sink.lane.send(0));
        let done = Arc::new(AtomicBool::new(false));
        let pump = thread::spawn({
            let done = Arc::clone(&done);
            move || {
                for value in 1..=SENT {
                    sink.lane.send(value);
                }
                done.store(true, Ordering::SeqCst);
                sink
            }
        });
        while !done.load(Ordering::SeqCst) {
            assert!(!receiver.as_flume().is_empty(), "the lane looked empty");
            thread::yield_now();
        }
        let mut sink = pump.join().unwrap();
        assert_eq!(receiver.try_recv(), Ok(Some(SENT)));

        let pump = thread::spawn(move || {
            for value in SENT + 1..=2 * SENT {
                sink.lane.send(value);
            }
            sink
        });
        let mut last = SENT;
        while last < 2 * SENT {
            let value = receiver.recv().expect("the lane is open");
            assert!(value >= last, "{value} came after {last}");
            last = value;
        }
        drop(pump.join().unwrap());
    });
}

/// A reader of a FIFO lane that the pump overfills gets the items in order,
/// and in the place of each run of items it missed a gap that counts them.
/// The items and the gaps account for every item sent, and the stop comes
/// after them all. An item sent while the queue has room for it goes in.
#[test]
fn a_fifo_reader_gets_each_item_or_a_gap_in_its_place_then_the_stop() {
    const ITEMS: u64 = 6;
    check(|| {
        let (mut sink, receiver) = Sink::new(2, everything());
        let pump = thread::spawn(move || {
            for item in 0..ITEMS {
                // The reader only makes room, so room seen here is there
                // when the lane sends.
                let room = sink.lane.has_room();
                assert!(sink.lane.send(item), "the reader is gone");
                if room {
                    assert_eq!(
                        sink.lane.skipped, 0,
                        "item {item} was missed with room for it"
                    );
                }
            }
            sink.lane.end(End::Stopped);
        });

        let mut due = 0;
        loop {
            match receiver.recv() {
                Ok(item) => {
                    assert_eq!(item, due, "item {item} came where {due} was due");
                    due += 1;
                }
                Err(RecvError::Lagged(missed)) => {
                    assert!(missed > 0, "a gap counted no item");
                    due += missed;
                }
                Err(RecvError::Stopped) => break,
                Err(other) => panic!("the lane ended with {other:?} after {due} items"),
            }
        }
        assert_eq!(due, ITEMS, "the items and gaps counted {due} of {ITEMS}");
        pump.join().unwrap();
        assert_eq!(receiver.try_recv(), Err(RecvError::Disconnected));
    });
}

/// The batches of one device that reboots once, one sample each, as the
/// parser makes them. The first three are of session 1 and the rest of
/// session 2.
fn rebooting_batches() -> Vec<SampleBatch> {
    let root = DeviceRoute::root();
    let mut parser = PacketParser::new(false);
    let mut batches = Vec::new();
    for session in [1, 2] {
        for record in description(root, session) {
            parser.push_packet(&record).expect("a record");
        }
        for first in 0..3 {
            parser.push_packet(&samples(first, root)).expect("samples");
            batches.push(parser.pop_batch().expect("a batch"));
        }
    }
    batches
}

/// What a batch lane's reader took, the batches and what its gaps counted.
struct Taken {
    batches: Vec<SampleBatch>,
    gaps: u64,
}

/// A batch lane that drops batches while its reader falls behind carries
/// what they changed and lost onto the next batch of the stream it
/// delivers, whichever thread runs first. Every row sent is in a delivered
/// batch or counted in the loss of one, each loss names the sample due in
/// the place it stands, the gaps just ahead of each batch count the batches
/// its loss says were dropped, and the first batch delivered of the new
/// session says the session changed.
#[test]
fn a_batch_lane_carries_the_change_and_loss_of_dropped_batches_onto_the_next() {
    let sent = Arc::new(rebooting_batches());
    let rows: u64 = sent.iter().map(|batch| batch.len() as u64).sum();
    check(move || {
        let (mut lane, queue) = BatchLane::new(1);
        let batches = Arc::clone(&sent);
        let pump = thread::spawn(move || {
            let (last, rest) = batches.split_last().expect("batches");
            for batch in rest {
                assert!(lane.send(batch.clone()), "the reader is gone");
            }
            // The last batch goes in once the reader makes room for it.
            while !lane.lane.has_room() {
                thread::yield_now();
            }
            assert!(lane.send(last.clone()), "the reader is gone");
            lane.end(End::Stopped);
        });

        let mut taken = Taken {
            batches: Vec::new(),
            gaps: 0,
        };
        // The batches the gaps since the last batch counted.
        let mut ahead = 0;
        loop {
            match queue.recv() {
                Ok(Queued::Item(batch)) => {
                    let loss = batch.boundary().and_then(|b| b.loss.as_ref());
                    let dropped = loss.map_or(0, |loss| loss.dropped_batches);
                    assert_eq!(
                        ahead, dropped,
                        "the gaps ahead of a batch counted other drops"
                    );
                    ahead = 0;
                    taken.batches.push(batch);
                }
                Ok(Queued::Gap(dropped)) => {
                    taken.gaps += dropped;
                    ahead += dropped;
                }
                Ok(Queued::Ended(End::Stopped)) => break,
                other => panic!("the lane ended with {other:?}"),
            }
        }
        assert_eq!(ahead, 0, "a gap came after the last batch");
        pump.join().unwrap();

        let losses = taken
            .batches
            .iter()
            .filter_map(|batch| batch.boundary().and_then(|b| b.loss.clone()));
        let (lost, dropped) = losses.fold((0, 0), |(lost, dropped), loss| {
            (lost + loss.samples, dropped + loss.dropped_batches)
        });
        let delivered: u64 = taken.batches.iter().map(|batch| batch.len() as u64).sum();
        assert_eq!(delivered + lost, rows, "rows went missing");
        let missing = (sent.len() - taken.batches.len()) as u64;
        assert_eq!((taken.gaps, dropped), (missing, missing));

        for batch in &taken.batches {
            let Some(loss) = batch.boundary().and_then(|b| b.loss.as_ref()) else {
                continue;
            };
            if let Some(expected) = loss.expected {
                let first = batch.sample_numbers()[0].value();
                assert_eq!(u64::from(first), u64::from(expected.value()) + loss.samples);
            }
        }
        let session = |batch: &SampleBatch| batch.metadata().device().session();
        let rebooted = taken
            .batches
            .iter()
            .find(|batch| session(batch) == SessionId::new(2))
            .expect("the last batch is of session 2");
        assert_eq!(
            rebooted.boundary().and_then(|b| b.change.clone()),
            Some(Change::SessionChanged {
                old: SessionId::new(1),
                new: SessionId::new(2),
            })
        );
    });
}

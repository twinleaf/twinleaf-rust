mod boundaries;
#[cfg(all(test, shuttle))]
mod shuttle;

use crate::data::{fixtures, PacketParser, SampleBatch};
use crate::proto::{DeviceRoute, SampleNumber};
use crate::session::subscription::{BatchLane, Queued, RecvError, Scope, Sink};
use crate::session::tests::packets::{description, samples};
use crate::tio::packet::Packet;
use std::time::{Duration, Instant};

/// The whole tree.
fn everything() -> Scope {
    Scope::subtree(DeviceRoute::root())
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

#[tokio::test]
async fn an_async_receive_returns_a_queued_item() {
    let (mut sink, receiver) = Sink::new(1, everything());
    assert!(sink.lane.send(7));

    assert_eq!(receiver.recv_async().await, Ok(7));
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

/// The lane holds its own end of the queue, which must not hide that the
/// reader is gone.
#[test]
fn a_latest_value_lane_reports_a_dropped_reader() {
    let (mut sink, receiver) = Sink::latest(everything());
    assert!(sink.lane.send(1));
    drop(receiver);

    assert!(!sink.lane.send(2));
}

/// A parser that has taken the description of the device at each of
/// `routes` in session 1.
fn described(routes: &[DeviceRoute]) -> PacketParser {
    let mut parser = PacketParser::new(false);
    for &route in routes {
        for packet in description(route, 1) {
            parser.push_packet(&packet).expect("a record");
        }
    }
    parser
}

/// The batch `parser` makes of `packet`.
fn parse(parser: &mut PacketParser, packet: &Packet) -> SampleBatch {
    parser.push_packet(packet).expect("valid samples");
    parser.pop_batch().expect("a batch")
}

/// The next batch on `queue`, after the gaps ahead of it, and what those
/// gaps counted.
fn next_batch(queue: &crate::chan::Receiver<Queued<SampleBatch>>) -> (u64, SampleBatch) {
    let mut gaps = 0;
    loop {
        match queue.try_recv().expect("a queued batch") {
            Queued::Item(batch) => return (gaps, batch),
            Queued::Gap(dropped) => gaps += dropped,
            Queued::Ended(end) => panic!("the lane ended with {end:?}"),
        }
    }
}

/// A batch lane carries the rows it dropped of each stream onto that
/// stream's next batch, which counts the stream's share of the gaps and
/// names the sample due. The generations stay the parser's.
#[test]
fn a_batch_lane_carries_each_streams_dropped_batches_onto_its_next_batch() {
    let (a, b): (DeviceRoute, DeviceRoute) = ("/1".parse().unwrap(), "/2".parse().unwrap());
    let mut parser = described(&[a, b]);
    let (mut lane, queue) = BatchLane::new(2);
    for packet in [samples(0, a), samples(0, b)] {
        assert!(lane.send(parse(&mut parser, &packet)));
    }
    for packet in [samples(1, a), samples(2, a), samples(1, b)] {
        assert!(lane.send(parse(&mut parser, &packet)));
    }
    let first: Vec<_> = (0..2).map(|_| next_batch(&queue).1.route()).collect();
    assert_eq!(first, [a, b]);

    let resumed = parse(&mut parser, &samples(3, a));
    assert_eq!(resumed.boundary(), None, "the parser lost nothing");
    let generations = resumed.generations();
    assert!(lane.send(resumed));
    let (gaps_a, batch_a) = next_batch(&queue);
    assert!(lane.send(parse(&mut parser, &samples(2, b))));
    let (gaps_b, batch_b) = next_batch(&queue);

    let loss = |batch: &SampleBatch| batch.boundary().and_then(|b| b.loss.clone());
    let lost = |samples, dropped_batches| {
        fixtures::loss(
            samples,
            Some(SampleNumber::new(1)),
            dropped_batches,
            samples,
            0,
            0,
        )
    };
    assert_eq!(loss(&batch_a), Some(lost(2, 2)));
    assert_eq!(loss(&batch_b), Some(lost(1, 1)));
    assert_eq!(gaps_a + gaps_b, 3, "the gaps count every batch dropped");
    assert_eq!(batch_a.boundary().unwrap().change, None);
    assert_eq!(batch_a.generations(), generations);
}

//! What the proxy queues for one client.
//!
//! An [`Outbox`] holds two queues, the stream and the urgent queue, and the
//! client's task writes them to the client's socket. Each time the writer
//! takes, it takes the whole urgent queue, and then the oldest part of the
//! stream, up to [`TAKE_LIMIT`] bytes of it. The rest of the stream stays
//! queued, where samples may still be dropped and urgent packets queued
//! later still go ahead of it. So an urgent packet waits behind at most
//! [`TAKE_LIMIT`] bytes of the stream and the rest of one update, then the
//! [`SOCKET_QUEUE`](super::client::SOCKET_QUEUE) packets the socket holds.
//!
//! Three parties fill an outbox. The engine of each link the client mounts
//! puts the link's packets on the stream in device order, and each metadata
//! update the client's watches follow at the point in that order where the
//! metadata changed. The worker of each such link puts the answers to the
//! client's requests on the urgent queue, and the link's list of mounts puts
//! the status that the link has lost its device there for a mount added
//! meanwhile. The client's task puts the answers to its control requests and
//! every other update of its watches on the urgent queue, and the status
//! that the proxy stopped behind both queues.
//!
//! The stream keeps the order it was queued in, so a metadata update
//! reaches the client ahead of every sample the device sent after the
//! metadata changed, and the client decodes each sample with the metadata
//! it was sent under. Nothing that says how to decode samples goes on the
//! urgent queue. What goes there may overtake the stream, and keeps its own
//! order. It is an answer, which the client matches by its id, the latest
//! value of a device's presence, RPC table, settings or routes, which the
//! client keeps in its store whatever it overtook, or a setting the device
//! announced, which the client's store takes in the order the urgent queue
//! keeps.
//!
//! The outbox takes a watch's update whole, and splits it into the packets
//! the client joins by the id of its watch, all on one queue in one step, so
//! no other packet comes between them. It puts a metadata update on the
//! stream and any other update on the urgent queue. A take never ends inside
//! an update. The metadata updates of a watch keep their order, as do its
//! other updates, and an update of one kind may overtake one of the other,
//! as each sets its own field of the client's store.
//!
//! Only samples are ever dropped. Once the samples on the stream take more
//! than [`SAMPLE_BUDGET`] bytes, the oldest sample goes. The writer learns
//! how many went, and logs it at debug level. The client is not told. The
//! packets that are not samples are never dropped, so a client whose other
//! packets take more than [`HARD_LIMIT`] bytes is let go as
//! [`ClientError::TooSlow`].
//!
//! A client is let go once something it mounts ends, or once it falls that
//! far behind. [`Outbox::take`] then hands the writer the reason, and the
//! client's task writes what was queued for at most
//! [`CLOSE_TIMEOUT`](crate::tio::transport::CLOSE_TIMEOUT) before it closes
//! the socket. The status that the proxy stopped is never refused. It goes
//! to the writer behind everything queued before it, and nothing goes after
//! it.

use super::client::ClientError;
use super::control::{decode, parts, Part, Update};
use crate::proto::data::FIRST_STREAM_ID;
use crate::rt::lock;
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::Notify;

/// The bytes of samples the stream holds before it drops the oldest. It is
/// one second of samples arriving at 8 Mbit/s, so a client that stalls for
/// less than that behind one fast device loses none.
pub(super) const SAMPLE_BUDGET: usize = 1024 * 1024;

/// The bytes of packets other than samples an outbox holds before its client
/// is let go. A device's heartbeats, statuses and state updates take under
/// 1 KiB a second, so this is over two hours of them. A client that stops
/// reading is let go sooner, as its socket's writes stall. So only a flood
/// of packets that are not samples, into a client that still reads, reaches
/// it.
pub(super) const HARD_LIMIT: usize = 8 * 1024 * 1024;

/// The bytes of the stream the writer takes at once. It is 52 ms of a
/// 10 Mbit/s link, which bounds how long an urgent packet waits behind the
/// stream.
pub(super) const TAKE_LIMIT: usize = 64 * 1024;

/// What waits to go to one client. It wakes the client's task whenever it
/// has something to write.
#[derive(Default)]
pub(super) struct Outbox {
    queues: Mutex<Queues>,
    /// Wakes the client's task to write.
    pub(super) ready: Notify,
}

/// What the writer takes from an [`Outbox`] at once.
pub(super) struct Taken {
    /// The urgent queue, then the oldest part of the stream, and last the
    /// status that the proxy stopped once the stream is empty.
    pub(super) packets: VecDeque<Packet>,
    /// How many samples were dropped since the last take.
    pub(super) dropped: u64,
    /// Why the client must be let go, once what is queued is written.
    pub(super) end: Option<ClientError>,
}

/// What an [`Outbox`] holds behind its lock.
#[derive(Default)]
struct Queues {
    urgent: VecDeque<Packet>,
    stream: VecDeque<Packet>,
    sample_bytes: usize,
    /// The bytes of the packets held that are not samples.
    other_bytes: usize,
    /// How many samples were dropped since the writer last took.
    dropped: u64,
    /// Why the client must be let go.
    end: Option<ClientError>,
    stop: Stop,
}

/// Where an outbox is in the proxy's stop.
#[derive(Default, PartialEq)]
enum Stop {
    #[default]
    Running,
    /// The status that the proxy stopped goes to the writer at the take
    /// that empties the stream.
    Due,
    /// The status went to the writer, and nothing goes after it.
    Sent,
}

/// The queue a packet goes on.
#[derive(Clone, Copy)]
enum Queue {
    Urgent,
    Stream,
}

impl Queues {
    /// Takes `packet` out of the byte counts, as it leaves the outbox.
    fn uncount(&mut self, packet: &Packet) {
        let size = packet.as_bytes().len();
        if is_sample(packet) {
            self.sample_bytes -= size;
        } else {
            self.other_bytes -= size;
        }
    }

    /// Drops the oldest sample on the stream, and counts it.
    fn drop_oldest_sample(&mut self) {
        let at = self.stream.iter().position(is_sample);
        let at = at.expect("a sample on the stream past the budget");
        let oldest = self.stream.remove(at).expect("the sample found");
        self.sample_bytes -= oldest.as_bytes().len();
        self.dropped += 1;
    }
}

/// Returns `true` if `packet` holds samples, as its type says. It reads the
/// type alone, as the eviction scans the stream with it.
fn is_sample(packet: &Packet) -> bool {
    let stream = packet.ptype().stream_id();
    stream.is_some_and(|stream| stream >= FIRST_STREAM_ID)
}

/// Returns the watch id and the part of the update that `packet` carries,
/// if it carries one.
fn part_of(packet: &Packet) -> Option<(u16, Part)> {
    let Payload::ProxyRx(ProxyRx::Reply(reply)) = packet.payload() else {
        return None;
    };
    Some((reply.req_id.value(), decode::<Part>(reply.value).ok()?))
}

impl Outbox {
    /// Queues `packet`, one of the device's, on the stream, behind every
    /// packet queued there before. A sample past the budget drops the oldest
    /// sample.
    pub(super) fn stream(&self, packet: Packet) {
        self.queue([packet], Queue::Stream);
    }

    /// Queues `packet`, an answer or a status, on the urgent queue, ahead of
    /// the stream.
    pub(super) fn urgent(&self, packet: Packet) {
        debug_assert!(!is_sample(&packet), "a sample is never urgent");
        self.queue([packet], Queue::Urgent);
    }

    /// Queues `update` of watch `id` whole, as the packets the client joins
    /// it from, in one step. A metadata update says how to decode the samples
    /// after it, so it goes on the stream, and any other update goes on the
    /// urgent queue.
    pub(super) fn update(&self, id: u16, update: &Update) {
        let queue = match update {
            Update::Metadata(_) => Queue::Stream,
            _ => Queue::Urgent,
        };
        self.queue(parts(id, update), queue);
    }

    /// Queues `packets` on `queue` in one step, unless the client is let go
    /// as too slow or the proxy has stopped.
    fn queue(&self, packets: impl IntoIterator<Item = Packet>, queue: Queue) {
        let mut queues = lock(&self.queues);
        if queues.other_bytes > HARD_LIMIT || queues.stop != Stop::Running {
            return;
        }
        for packet in packets {
            let size = packet.as_bytes().len();
            if is_sample(&packet) {
                queues.sample_bytes += size;
            } else {
                queues.other_bytes += size;
            }
            match queue {
                Queue::Urgent => queues.urgent.push_back(packet),
                Queue::Stream => queues.stream.push_back(packet),
            }
        }
        while queues.sample_bytes > SAMPLE_BUDGET {
            queues.drop_oldest_sample();
        }
        if queues.other_bytes > HARD_LIMIT {
            queues.end.get_or_insert(ClientError::TooSlow);
        }
        drop(queues);
        self.ready.notify_one();
    }

    /// Lets the client go for `error` once what is queued is written, unless
    /// it is let go for another reason already.
    pub(super) fn end(&self, error: ClientError) {
        lock(&self.queues).end.get_or_insert(error);
        self.ready.notify_one();
    }

    /// Queues the status that the proxy stopped behind everything queued,
    /// and refuses what comes after it.
    pub(super) fn stop(&self) {
        let mut queues = lock(&self.queues);
        if queues.stop == Stop::Running {
            queues.stop = Stop::Due;
        }
        drop(queues);
        self.ready.notify_one();
    }

    /// Takes the whole urgent queue, and then the oldest part of the stream,
    /// up to [`TAKE_LIMIT`] bytes of it and at least one packet. A take that
    /// reaches the limit inside an update takes the rest of the update too.
    /// Once the stream is empty, the status that the proxy stopped goes last
    /// if it is due. Says how many samples were dropped since the last take,
    /// and why the client must be let go, once something asked for that.
    pub(super) fn take(&self) -> Taken {
        let mut queues = lock(&self.queues);
        let mut packets = std::mem::take(&mut queues.urgent);
        for packet in &packets {
            queues.uncount(packet);
        }
        let mut taken = 0;
        while let Some(next) = queues.stream.front() {
            let size = next.as_bytes().len();
            let full = taken > 0 && taken + size > TAKE_LIMIT;
            let inside_update = part_of(next).is_some_and(|(_, part)| part.index > 0);
            if full && !inside_update {
                break;
            }
            let packet = queues.stream.pop_front().expect("the packet in front");
            queues.uncount(&packet);
            taken += size;
            packets.push_back(packet);
        }
        if queues.stream.is_empty() && queues.stop == Stop::Due {
            packets.push_back(Packet::link(LinkStatus::Stopped));
            queues.stop = Stop::Sent;
        }
        Taken {
            packets,
            dropped: std::mem::take(&mut queues.dropped),
            end: queues.end.take(),
        }
    }

    /// Checks what the outbox holds. Its byte counts match what it holds,
    /// and the parts of each update follow each other on their queue.
    /// Returns how many packets other than samples it holds, which a test
    /// counts against what it queued, as none is ever dropped. Tests call it
    /// after each step.
    #[cfg(test)]
    pub(super) fn check(&self) -> usize {
        let queues = lock(&self.queues);
        let (mut sample_bytes, mut other_bytes, mut others) = (0, 0, 0);
        for packet in queues.urgent.iter().chain(&queues.stream) {
            let size = packet.as_bytes().len();
            if is_sample(packet) {
                sample_bytes += size;
            } else {
                other_bytes += size;
                others += 1;
            }
        }
        assert_eq!(
            queues.sample_bytes, sample_bytes,
            "the sample bytes are off"
        );
        assert_eq!(queues.other_bytes, other_bytes, "the other bytes are off");
        for queue in [&queues.urgent, &queues.stream] {
            let mut due = None;
            for packet in queue {
                let part = part_of(packet);
                let at = part.as_ref().map(|(id, part)| (*id, part.index));
                if let Some(due) = due.take() {
                    assert_eq!(at, Some(due), "the parts of an update were split");
                }
                if let Some((id, part)) = part {
                    due = (part.index + 1 < part.count).then_some((id, part.index + 1));
                }
            }
            assert_eq!(due, None, "an update ends without its last part");
        }
        others
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::fixtures::{broadcast, segment_record};
    use crate::proto::data::Metadata;
    use crate::proto::{DeviceRoute, SessionId};
    use crate::proxy::Joined;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// A sample of stream 1 at the root, numbered `first`, that takes `size`
    /// bytes of data.
    fn sample(first: u32, size: usize) -> Packet {
        Packet::samples(1, 0, first, &vec![0; size], DeviceRoute::root()).unwrap()
    }

    /// The sample numbers of the samples in `packets`, in order.
    fn numbers(packets: &VecDeque<Packet>) -> Vec<u32> {
        let samples = packets.iter().filter_map(|packet| match packet.payload() {
            Payload::Samples(data) => Some(data.first.value()),
            _ => None,
        });
        samples.collect()
    }

    /// The sessions of the heartbeats in `packets`, in order.
    fn sessions(packets: &VecDeque<Packet>) -> Vec<u32> {
        let beats = packets.iter().filter_map(|packet| match packet.payload() {
            Payload::Heartbeat(beat) => beat.session().map(SessionId::value),
            _ => None,
        });
        beats.collect()
    }

    /// The bytes `packets` take.
    fn bytes<'a>(packets: impl IntoIterator<Item = &'a Packet>) -> usize {
        packets
            .into_iter()
            .map(|packet| packet.as_bytes().len())
            .sum()
    }

    /// Takes from `outbox` as the writer does until it holds nothing, and
    /// returns what it took in order, with the count of samples dropped.
    fn take_all(outbox: &Outbox) -> (VecDeque<Packet>, u64) {
        let (mut packets, mut dropped) = (VecDeque::new(), 0);
        loop {
            let taken = outbox.take();
            if taken.packets.is_empty() {
                assert_eq!(outbox.check(), 0, "the outbox kept something");
                return (packets, dropped);
            }
            packets.extend(taken.packets);
            dropped += taken.dropped;
        }
    }

    /// Samples beyond the budget drop the oldest first, each counted once,
    /// and the stream never holds more samples than the budget.
    #[test]
    fn eviction_takes_the_oldest_sample() {
        let outbox = Outbox::default();
        let size = 400;
        let sent = 2 * SAMPLE_BUDGET / size;
        for first in 0..sent {
            outbox.stream(sample(first as u32, size));
            assert_eq!(outbox.check(), 0);
            assert!(lock(&outbox.queues).sample_bytes <= SAMPLE_BUDGET);
        }

        let (taken, dropped) = take_all(&outbox);
        let kept = numbers(&taken);
        assert!(dropped > 0);
        assert_eq!(
            dropped + kept.len() as u64,
            sent as u64,
            "each drop counts once"
        );
        assert_eq!(u64::from(kept[0]), dropped, "the oldest went first");
        assert!(kept.windows(2).all(|pair| pair[0] + 1 == pair[1]));
    }

    /// No record or heartbeat is ever evicted, however many samples come
    /// between, and the stream keeps the order it was queued in. Each sample
    /// kept still follows the record and heartbeat queued ahead of it.
    #[test]
    fn records_are_never_evicted_and_the_stream_keeps_its_order() {
        let root = DeviceRoute::root();
        let record = broadcast(Metadata::Segment(segment_record(0)), root);
        let outbox = Outbox::default();
        for session in 0..1000 {
            outbox.stream(record.clone());
            outbox.stream(Packet::heartbeat_session(session, root));
            for first in 0..100 {
                outbox.stream(sample(session * 100 + first, 400));
            }
            let queued = 2 * (session as usize + 1);
            assert_eq!(outbox.check(), queued, "a record or heartbeat was dropped");
        }

        let (taken, dropped) = take_all(&outbox);
        assert!(dropped > 0, "samples were dropped meanwhile");
        assert_eq!(sessions(&taken), (0..1000).collect::<Vec<_>>());
        let records = taken
            .iter()
            .filter(|packet| packet.as_bytes() == record.as_bytes())
            .count();
        assert_eq!(records, 1000, "a record was evicted");
        let mut session = None;
        for packet in &taken {
            match packet.payload() {
                Payload::Heartbeat(beat) => session = beat.session().map(SessionId::value),
                Payload::Samples(data) => {
                    assert_eq!(session, Some(data.first.value() / 100), "out of order")
                }
                _ => {}
            }
        }
        assert!(numbers(&taken).windows(2).all(|pair| pair[0] < pair[1]));
    }

    /// What is urgent goes out ahead of the whole stream, each queue in the
    /// order it was queued.
    #[test]
    fn urgent_goes_first() {
        let outbox = Outbox::default();
        let beat = |session| Packet::heartbeat_session(session, DeviceRoute::root());
        outbox.stream(beat(1));
        outbox.urgent(beat(2));
        outbox.stream(beat(3));
        outbox.urgent(beat(4));
        assert_eq!(outbox.check(), 4);

        assert_eq!(sessions(&outbox.take().packets), [2, 4, 1, 3]);
    }

    /// A client that stalled until its samples filled the budget, and then
    /// reads slowly, hears an answer queued after that behind at most
    /// [`TAKE_LIMIT`] bytes of samples, the take being written when it came.
    /// The samples keep their order, and those not yet taken can still be
    /// dropped.
    #[test]
    fn an_urgent_packet_waits_behind_at_most_the_take_limit() {
        let outbox = Outbox::default();
        let size = 400;
        for first in 0..(SAMPLE_BUDGET / size) as u32 {
            outbox.stream(sample(first, size));
        }
        let writing = outbox.take();
        outbox.check();
        assert!(
            bytes(&writing.packets) <= TAKE_LIMIT,
            "the writer took it all"
        );

        let answer = Packet::proxy_reply(7, b"answer").unwrap();
        outbox.urgent(answer.clone());

        let next = outbox.take();
        outbox.check();
        assert_eq!(next.packets[0].as_bytes(), answer.as_bytes());
        let (rest, _) = take_all(&outbox);
        let mut samples = numbers(&writing.packets);
        samples.extend(numbers(&next.packets));
        samples.extend(numbers(&rest));
        assert!(samples.windows(2).all(|pair| pair[0] + 1 == pair[1]));
    }

    /// A take that reaches its limit inside a metadata update takes the
    /// rest of the update, so an urgent update of the same watch cannot come
    /// between its parts.
    #[test]
    fn a_take_never_ends_inside_an_update() {
        let outbox = Outbox::default();
        let metadata = Update::Metadata(Some(vec![vec![7; 4000].into()]));
        let first_part = crate::proxy::parts(1, &metadata)[0].as_bytes().len();
        // The samples leave room for the update's first part, and no more.
        let per = sample(0, 100).as_bytes().len();
        let count = (TAKE_LIMIT - first_part) / per;
        for first in 0..count as u32 {
            outbox.stream(sample(first, 100));
        }
        outbox.update(1, &metadata);
        outbox.stream(sample(count as u32, 100));
        outbox.check();

        let taken = outbox.take().packets;
        let parts = taken.iter().filter(|packet| part_of(packet).is_some());
        assert_eq!(parts.count(), parts_of(&metadata), "the update was split");
        assert!(bytes(&taken) > TAKE_LIMIT, "the take kept to its limit");
        let last = numbers(&taken).last().copied();
        assert_eq!(
            last,
            Some(count as u32 - 1),
            "the take went on past the update"
        );
    }

    /// How many packets `update` takes.
    fn parts_of(update: &Update) -> usize {
        crate::proxy::parts(1, update).len()
    }

    /// However the writer's takes fall among the parts of a watch's
    /// updates, each update reaches it whole, a metadata update on the
    /// stream and a presence update of the same watch on the urgent queue
    /// alike, so the client joins every one.
    #[test]
    fn a_watch_update_reaches_the_writer_whole() {
        let metadata = Update::Metadata(Some(vec![vec![7; 2000].into()]));
        let presence = Update::Presence(Ok(Some(1)));
        assert!(parts_of(&metadata) > 1, "the update takes one part");
        let rounds = 2000;
        let outbox = Arc::new(Outbox::default());
        let taking = Arc::clone(&outbox);
        let taker = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            let (mut joined, mut whole) = (Joined::default(), 0);
            while whole < 2 * rounds && Instant::now() < deadline {
                for packet in taking.take().packets {
                    let Payload::ProxyRx(ProxyRx::Reply(reply)) = packet.payload() else {
                        continue;
                    };
                    if joined.join(reply.value).is_some() {
                        whole += 1;
                    }
                }
            }
            whole
        });

        for _ in 0..rounds {
            outbox.update(1, &metadata);
            outbox.check();
            outbox.update(1, &presence);
            outbox.check();
        }

        assert_eq!(taker.join().unwrap(), 2 * rounds, "an update was split");
    }

    /// An outbox whose packets other than samples take more than the hard
    /// limit lets its client go as too slow, with what it holds, and refuses
    /// what comes next rather than send less.
    #[test]
    fn the_hard_limit_disconnects() {
        let outbox = Outbox::default();
        let status = Packet::link(LinkStatus::SensorReconnected);
        let per = status.as_bytes().len();
        for _ in 0..HARD_LIMIT / per {
            outbox.urgent(status.clone());
        }
        assert!(
            outbox.take().end.is_none(),
            "the outbox holds up to the limit"
        );
        let over = HARD_LIMIT / per + 1;
        for _ in 0..over {
            outbox.stream(status.clone());
        }

        assert_eq!(outbox.check(), over);
        outbox.urgent(status);
        outbox.stream(sample(0, 4));
        assert_eq!(outbox.check(), over, "more was queued");
        let first = outbox.take();
        assert!(matches!(first.end, Some(ClientError::TooSlow)));
        let (rest, _) = take_all(&outbox);
        assert_eq!(
            first.packets.len() + rest.len(),
            over,
            "what it held was kept"
        );
    }

    /// The reason a client is let go comes with what was queued for it,
    /// and at one take only.
    #[test]
    fn an_end_comes_with_what_was_queued() {
        let outbox = Outbox::default();
        outbox.stream(Packet::heartbeat(DeviceRoute::root()));
        outbox.end(ClientError::LinkEnded("tcp://device".to_string()));
        outbox.end(ClientError::HungUp);

        let taken = outbox.take();
        assert_eq!(taken.packets.len(), 1);
        assert!(matches!(taken.end, Some(ClientError::LinkEnded(_))));
        assert!(outbox.take().end.is_none());
    }

    /// The status that the proxy stopped goes behind both queues though the
    /// outbox refuses everything else past the hard limit, comes once, and
    /// nothing goes after it.
    #[test]
    fn stopped_goes_last_and_is_never_refused() {
        let outbox = Outbox::default();
        let status = Packet::link(LinkStatus::SensorReconnected);
        outbox.urgent(Packet::heartbeat(DeviceRoute::root()));
        for _ in 0..=HARD_LIMIT / status.as_bytes().len() {
            outbox.stream(status.clone());
        }

        outbox.stop();
        outbox.urgent(Packet::heartbeat(DeviceRoute::root()));

        let (taken, _) = take_all(&outbox);
        let last = taken.back().and_then(Packet::link_status);
        assert_eq!(last, Some(LinkStatus::Stopped));
        assert!(taken[0].link_status().is_none(), "urgent went first");
        let stops = taken.iter().filter(|packet| packet.link_status() == last);
        assert_eq!(stops.count(), 1);
        outbox.stream(status);
        outbox.stop();
        assert!(outbox.take().packets.is_empty(), "something went after it");
    }
}

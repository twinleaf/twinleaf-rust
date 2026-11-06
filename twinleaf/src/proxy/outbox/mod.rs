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
//! client keeps in its store whatever it overtook, a setting the device
//! announced, or the RPC table the settings after it were recorded under.
//! The client's store takes the settings and those tables in the order the
//! urgent queue keeps.
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
//! [`ClientError::TooSlow`]. The client counts as too slow while the packets
//! other than samples its outbox holds take more than [`HARD_LIMIT`] bytes.
//! Meanwhile the outbox refuses whatever is queued, and once takes bring
//! those packets back under the limit it takes what is queued again. The
//! call that takes them past the limit is taken whole, and asks that the
//! client be let go as [`ClientError::TooSlow`].
//!
//! A client is let go once something it mounts ends, or once it falls that
//! far behind. [`Outbox::take`] then hands the writer the reason. Each take
//! hands the first reason asked for since the last take that handed one, and
//! a reason asked for while another waits is dropped. The client's task
//! writes what was queued for at most
//! [`CLOSE_TIMEOUT`](crate::tio::transport::CLOSE_TIMEOUT) before it closes
//! the socket. The status that the proxy stopped is never refused. It goes
//! to the writer behind everything queued before it, and nothing goes after
//! it.

use crate::chan::lock;
use crate::chan::Notify;
use crate::proto::data::FIRST_STREAM_ID;
use crate::session::{decode, parts, Part, Update};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use std::collections::VecDeque;
use std::sync::Mutex;

/// Why a client's connection ended. The task serving the client logs it
/// once.
#[derive(Debug, Clone, thiserror::Error)]
pub(super) enum ClientError {
    /// The client closed its connection, or the connection failed. A write
    /// to a client that stops reading fails so once the transport has
    /// waited its drain timeout, `DRAIN_TIMEOUT`, for one packet to go out,
    /// and the connection of a client quiet for longer than the keepalive
    /// allows fails so too.
    #[error("it hung up")]
    HungUp,
    /// The proxy stopped and told the client.
    #[error("the proxy stopped")]
    Stopped,
    /// The client let more pile up than its outbox holds, its
    /// [`HARD_LIMIT`].
    #[error("it fell behind")]
    TooSlow,
    /// The link to the device at this URL, which the client had opened,
    /// ended without giving up on it.
    #[error("the link to {0} ended unexpectedly")]
    LinkEnded(String),
    /// The proxy gave up reconnecting to the device at this URL, which the
    /// client had opened.
    #[error("the proxy gave up on {0}")]
    GaveUp(String),
    /// A packet it sent could not be queued for its device.
    #[error("a packet it sent could not reach its device")]
    ForwardFailed,
}

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
    /// another reason already waits for the next take.
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

    /// Takes the whole urgent queue, and then as much of the front of the
    /// stream as fits under [`TAKE_LIMIT`] bytes, and at least one packet of
    /// it. The take stops only at the end of the stream, or at a packet that
    /// would take it past the limit. A take that reaches the limit inside an
    /// update takes the rest of the update too. Once the stream is empty, the
    /// status that the proxy stopped goes last if it is due. Says how many
    /// samples were dropped since the last take, and the first reason to let
    /// the client go asked for since the last take that handed one.
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
mod tests;

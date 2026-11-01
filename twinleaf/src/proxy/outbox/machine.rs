//! State-machine properties of the [`Outbox`].
//!
//! Each case feeds one outbox what its parties queue, samples, other packets
//! of the stream, whole watch updates, answers, floods past [`HARD_LIMIT`],
//! the reasons to let the client go and the proxy's stop. The writer takes
//! between steps, and also on a thread of its own while an update is queued.
//! A model of the urgent queue and the stream follows the outbox's rules, and
//! every take is checked against it. One property checks the order and the
//! other the wait, so each shrinks to a case of its own promise.
//!
//! The order property checks these promises.
//!
//! - A sample goes only while the samples held take more than
//!   [`SAMPLE_BUDGET`] bytes, the oldest first, and each take counts those
//!   gone since the last.
//! - Nothing else is ever dropped.
//! - Each take brings the whole urgent queue in its order, then the front
//!   of the stream in its order.
//! - The parts of each update reach the writer in order with nothing between
//!   them, and no take ends inside an update, even when the take runs while
//!   the update is queued.
//! - The first reason to let the client go wins, and the next take brings it
//!   once. Passing [`HARD_LIMIT`] is such a reason, [`ClientError::TooSlow`].
//! - A take brings something whenever the outbox holds something.
//! - The status that the proxy stopped is never refused, goes out once,
//!   behind everything queued before it, and nothing goes after it.
//! - The byte counts match a recount and the model's.
//! - After a close's drain, the outbox and the model hold nothing.
//!
//! The wait property checks that each take ends its share of the stream at
//! [`TAKE_LIMIT`] bytes or at the end of the update it reached the limit in,
//! and that each urgent packet goes out at the first take after it was
//! queued. So an urgent packet waits behind at most one take of the stream.

use super::super::client::ClientError;
use super::{Outbox, Taken, HARD_LIMIT, SAMPLE_BUDGET, TAKE_LIMIT};
use crate::chan::lock;
use crate::device::fixtures::{broadcast, segment_record};
use crate::proto::data::Metadata;
use crate::proto::DeviceRoute;
use crate::proxy::control::{decode, parts, Part, Update};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use proptest::prelude::*;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use std::collections::VecDeque;
use std::fmt::Arguments;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

/// The most takes the writer makes to empty the outbox at the end of a case.
const DRAIN_TAKES: usize = 100_000;

/// What the value of an answer starts with. It decodes as no [`Part`].
const ANSWER: &[u8] = b"ans";

/// One step of a case.
#[derive(Clone, Debug)]
enum Step {
    /// The engine queues `count` packets of samples, each of `size` bytes of
    /// data.
    Samples { size: usize, count: usize },
    /// The engine queues `count` packets of the stream that are not samples,
    /// segment records if `record` and heartbeats otherwise.
    Other { record: bool, count: usize },
    /// A watch's update of `parts` packets, a metadata update on the stream
    /// if `metadata` and another update on the urgent queue otherwise. With
    /// `racing`, the writer takes on a thread of its own until it is queued.
    Update {
        id: u16,
        metadata: bool,
        parts: usize,
        racing: bool,
    },
    /// `count` answers, each of `size` bytes of value, on the urgent queue.
    Answers { size: usize, count: usize },
    /// Answers until the packets other than samples the outbox holds pass
    /// [`HARD_LIMIT`], and `extra` more after that. Then the proxy stops if
    /// `stop`, as a flood is rare enough that a later step rarely does.
    Flood { extra: usize, stop: bool },
    /// Something asks that the client be let go, as it hung up if `hung_up`
    /// and as its link ended otherwise.
    End { hung_up: bool },
    /// The writer takes.
    Take,
    /// The proxy stops.
    Stop,
}

/// The steps a case is drawn from, any of them after any other.
struct Writer;

impl ReferenceStateMachine for Writer {
    type State = ();
    type Transition = Step;

    fn init_state() -> BoxedStrategy<()> {
        Just(()).boxed()
    }

    fn transitions(_: &()) -> BoxedStrategy<Step> {
        let count = prop_oneof![4 => 1..=8usize, 1 => 100..=3000usize];
        let samples = (4..=480usize, count).prop_map(|(size, count)| Step::Samples { size, count });
        let other =
            (any::<bool>(), 1..=4usize).prop_map(|(record, count)| Step::Other { record, count });
        let parts = prop_oneof![4 => 1..=4usize, 1 => 100..=160usize];
        let update = (1..=3u16, any::<bool>(), parts, any::<bool>()).prop_map(
            |(id, metadata, parts, racing)| Step::Update {
                id,
                metadata,
                parts,
                racing,
            },
        );
        let answers =
            (0..=400usize, 1..=4usize).prop_map(|(size, count)| Step::Answers { size, count });
        // A flood costs as much as the rest of a case, so few cases have one.
        prop_oneof![
            80 => Just(Step::Take),
            60 => samples,
            30 => other,
            40 => update,
            40 => answers,
            5 => any::<bool>().prop_map(|hung_up| Step::End { hung_up }),
            10 => Just(Step::Stop),
            1 => (0..=4usize, any::<bool>()).prop_map(|(extra, stop)| Step::Flood { extra, stop }),
        ]
        .boxed()
    }

    fn apply(state: (), _: &Step) {
        state
    }
}

/// What a packet the harness queued is.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Sample,
    Other,
    /// Part `index` of `count` of the update numbered `update`.
    Part {
        update: usize,
        index: usize,
        count: usize,
    },
}

/// The queue a packet went on.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Queue {
    Urgent,
    Stream,
}

/// A packet the outbox took, as the model keeps it.
#[derive(Debug)]
struct Item {
    key: Key,
    kind: Kind,
    bytes: usize,
    /// How many takes the writer had made when it was queued.
    takes: usize,
}

/// How the harness tells the packets it queued apart.
#[derive(Clone, PartialEq)]
enum Key {
    Sample(u32),
    Beat(u32),
    Answer(u32),
    Bytes(Vec<u8>),
    Stopped,
}

/// Returns the key of `packet`.
fn key(packet: &Packet) -> Key {
    match packet.payload() {
        Payload::Samples(data) => Key::Sample(data.first.value()),
        Payload::Heartbeat(beat) if beat.session().is_some() => {
            Key::Beat(beat.session().expect("a session").value())
        }
        Payload::ProxyRx(ProxyRx::Link(LinkStatus::Stopped)) => Key::Stopped,
        Payload::ProxyRx(ProxyRx::Reply(reply)) if reply.value.starts_with(ANSWER) => {
            let tag = reply.value[ANSWER.len()..ANSWER.len() + 4].try_into();
            Key::Answer(u32::from_le_bytes(tag.expect("a tag")))
        }
        _ => Key::Bytes(packet.as_bytes().to_vec()),
    }
}

/// Shows a key by its tag, and a part or record by its length.
impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Sample(tag) => write!(f, "sample {tag}"),
            Key::Beat(tag) => write!(f, "heartbeat {tag}"),
            Key::Answer(tag) => write!(f, "answer {tag}"),
            Key::Bytes(bytes) => write!(f, "a part or record of {} bytes", bytes.len()),
            Key::Stopped => write!(f, "Stopped"),
        }
    }
}

/// The bytes of an update one part carries.
fn part_bytes() -> usize {
    static BYTES: OnceLock<usize> = OnceLock::new();
    *BYTES.get_or_init(|| {
        let update = Update::Metadata(Some(vec![vec![0; 4096].into()]));
        let first = &parts(0, &update)[0];
        let Payload::ProxyRx(ProxyRx::Reply(reply)) = first.payload() else {
            panic!("a part is a reply");
        };
        decode::<Part>(reply.value).expect("a part").bytes.len()
    })
}

/// Returns an update of `count` packets, metadata if `metadata`, whose
/// parts each hold bytes no other update's parts hold, as its value is
/// numbered by `tag`.
fn tagged_update(tag: u32, metadata: bool, count: usize) -> Update {
    let length = count * part_bytes() - 64;
    let words = (0..).flat_map(|word: u32| tag.to_le_bytes().into_iter().chain(word.to_le_bytes()));
    let value: Vec<u8> = words.take(length).collect();
    if metadata {
        Update::Metadata(Some(vec![value.into()]))
    } else {
        Update::Routes(vec![value.into()])
    }
}

/// The outbox under test, with the model of what it holds. `WAIT` picks
/// the wait property's promises, and the order property's otherwise.
#[derive(Default)]
struct Client<const WAIT: bool> {
    outbox: Outbox,
    urgent: VecDeque<Item>,
    stream: VecDeque<Item>,
    sample_bytes: usize,
    /// The bytes of the packets other than samples held.
    other_bytes: usize,
    /// How many packets other than samples are held.
    others: usize,
    /// The samples dropped since the last take.
    dropped: u64,
    /// Why the client must be let go, at the next take.
    end: Option<ClientError>,
    /// The parts of an urgent update queued while the writer takes, which
    /// the model holds from the take that brings the first of them.
    racing: Vec<(Packet, Kind)>,
    next_tag: u32,
    updates: usize,
    takes: usize,
    stopping: bool,
    stopped: bool,
    /// The update whose part the writer must take next, and that part.
    open: Option<(usize, usize)>,
}

impl<const WAIT: bool> Client<WAIT> {
    /// Asserts a promise of the order property.
    fn order(holds: bool, broken: Arguments) {
        assert!(WAIT || holds, "{broken}");
    }

    /// Asserts a promise of the wait property.
    fn wait(holds: bool, broken: Arguments) {
        assert!(!WAIT || holds, "{broken}");
    }

    fn tag(&mut self) -> u32 {
        self.next_tag += 1;
        self.next_tag
    }

    /// Returns `true` if the outbox refuses what is queued now, as its
    /// client is let go as too slow or the proxy stopped.
    fn refuses(&self) -> bool {
        self.other_bytes > HARD_LIMIT || self.stopping
    }

    /// Adds `packet` to the model, on `queue`, and drops the oldest samples
    /// past the budget.
    fn hold(&mut self, packet: &Packet, kind: Kind, queue: Queue) {
        let bytes = packet.as_bytes().len();
        if kind == Kind::Sample {
            self.sample_bytes += bytes;
        } else {
            self.other_bytes += bytes;
            self.others += 1;
        }
        let item = Item {
            key: key(packet),
            kind,
            bytes,
            takes: self.takes,
        };
        match queue {
            Queue::Urgent => self.urgent.push_back(item),
            Queue::Stream => self.stream.push_back(item),
        }
        while self.sample_bytes > SAMPLE_BUDGET {
            let at = self
                .stream
                .iter()
                .position(|item| item.kind == Kind::Sample);
            let oldest = self
                .stream
                .remove(at.expect("a sample"))
                .expect("the sample");
            self.sample_bytes -= oldest.bytes;
            self.dropped += 1;
        }
    }

    /// Lets the client go as too slow once what it holds passes the limit,
    /// after one caller's packets were taken.
    fn after_queue(&mut self) {
        if self.other_bytes > HARD_LIMIT {
            self.end.get_or_insert(ClientError::TooSlow);
        }
    }

    /// Queues `packet` on `queue` as one caller does.
    fn queue(&mut self, packet: Packet, kind: Kind, queue: Queue) {
        if !self.refuses() {
            self.hold(&packet, kind, queue);
            self.after_queue();
        }
        match queue {
            Queue::Urgent => self.outbox.urgent(packet),
            Queue::Stream => self.outbox.stream(packet),
        }
    }

    fn step(&mut self, step: Step) {
        match step {
            Step::Samples { size, count } => {
                for _ in 0..count {
                    let first = self.tag();
                    let data = vec![0; size];
                    let packet = Packet::samples(1, 0, first, &data, DeviceRoute::root());
                    let packet = packet.expect("samples that fit a packet");
                    self.queue(packet, Kind::Sample, Queue::Stream);
                }
            }
            Step::Other { record, count } => {
                for segment in 0..count as u8 {
                    let packet = if record {
                        broadcast(
                            Metadata::Segment(segment_record(segment)),
                            DeviceRoute::root(),
                        )
                    } else {
                        Packet::heartbeat_session(self.tag(), DeviceRoute::root())
                    };
                    self.queue(packet, Kind::Other, Queue::Stream);
                }
            }
            Step::Update {
                id,
                metadata,
                parts: count,
                racing,
            } => self.update(id, metadata, count, racing),
            Step::Answers { size, count } => {
                for _ in 0..count {
                    self.answer(size);
                }
            }
            Step::Flood { mut extra, stop } => {
                while extra > 0 && !self.stopping {
                    if self.refuses() {
                        extra -= 1;
                    }
                    self.answer(400);
                }
                if stop {
                    self.step(Step::Stop);
                }
            }
            Step::End { hung_up } => {
                let reason = if hung_up {
                    ClientError::HungUp
                } else {
                    ClientError::LinkEnded("tcp://device".to_string())
                };
                self.end.get_or_insert(reason.clone());
                self.outbox.end(reason);
            }
            Step::Take => {
                let taken = self.outbox.take();
                self.took(taken, true);
            }
            Step::Stop => {
                self.stopping = true;
                self.outbox.stop();
            }
        }
    }

    /// Queues an update of watch `id`, while the writer takes on a thread of
    /// its own if `racing`.
    fn update(&mut self, id: u16, metadata: bool, count: usize, racing: bool) {
        let tag = self.tag();
        let update = tagged_update(tag, metadata, count);
        let packets = parts(id, &update);
        assert_eq!(packets.len(), count, "the harness sized the update");
        let bytes: usize = packets.iter().map(|part| part.as_bytes().len()).sum();
        let accepted = !self.refuses();
        // A take beside the update must not move the outbox across the limit
        // where the model cannot tell when.
        let racing = racing && accepted && self.other_bytes + bytes <= HARD_LIMIT;
        if accepted {
            let number = self.updates;
            self.updates += 1;
            for (index, part) in packets.into_iter().enumerate() {
                let kind = Kind::Part {
                    update: number,
                    index,
                    count,
                };
                // Until a take brings it, an urgent update queued beside the
                // takes may come after them.
                if racing && !metadata {
                    self.racing.push((part, kind));
                } else {
                    let queue = if metadata {
                        Queue::Stream
                    } else {
                        Queue::Urgent
                    };
                    self.hold(&part, kind, queue);
                }
            }
            self.after_queue();
        }
        if !racing {
            self.outbox.update(id, &update);
            return;
        }
        let (outbox, started, done) =
            (&self.outbox, AtomicBool::new(false), AtomicBool::new(false));
        let takes = std::thread::scope(|scope| {
            let taker = scope.spawn(|| {
                let mut takes = Vec::new();
                started.store(true, Ordering::Release);
                while !done.load(Ordering::Acquire) {
                    let taken = outbox.take();
                    if !taken.packets.is_empty() || taken.dropped > 0 || taken.end.is_some() {
                        takes.push(taken);
                    }
                }
                takes
            });
            while !started.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            outbox.update(id, &update);
            done.store(true, Ordering::Release);
            taker.join().expect("the writer's thread")
        });
        for taken in takes {
            self.took(taken, false);
        }
        self.hold_racing(self.takes);
    }

    /// Holds the parts of an urgent update queued beside the takes, as
    /// queued after the first `takes` takes.
    fn hold_racing(&mut self, takes: usize) {
        let now = std::mem::replace(&mut self.takes, takes);
        for (part, kind) in std::mem::take(&mut self.racing) {
            self.hold(&part, kind, Queue::Urgent);
        }
        self.takes = now;
    }

    /// Queues an answer whose value takes `size` bytes.
    fn answer(&mut self, size: usize) {
        let tag = self.tag();
        let mut value = ANSWER.to_vec();
        value.extend(tag.to_le_bytes());
        value.resize(value.len().max(size), 0);
        let packet = Packet::proxy_reply(7, &value).expect("an answer that fits");
        self.queue(packet, Kind::Other, Queue::Urgent);
    }

    /// Checks one take against the model, and takes what it brought out of
    /// the model. `alone` says no update was being queued meanwhile.
    fn took(&mut self, taken: Taken, alone: bool) {
        self.takes += 1;
        let held = self.urgent.len() + self.stream.len();
        Self::order(
            !alone || !taken.packets.is_empty() || held == 0,
            format_args!("a take brought nothing while the outbox holds {held} packets"),
        );
        Self::order(
            taken.dropped == self.dropped,
            format_args!(
                "a take says {} samples went, and the budget calls for {}",
                taken.dropped, self.dropped
            ),
        );
        self.dropped = 0;
        let due = self.end.take();
        Self::order(
            format!("{:?}", taken.end) == format!("{due:?}"),
            format_args!(
                "a take brought the end {:?} where {due:?} was due",
                taken.end
            ),
        );
        if let Some((first, _)) = self.racing.first() {
            let first = key(first);
            if taken.packets.iter().any(|packet| key(packet) == first) {
                self.hold_racing(self.takes - 1);
            }
        }
        let mut stream_bytes = 0;
        for packet in taken.packets {
            let key = key(&packet);
            Self::order(
                !self.stopped,
                format_args!("{key:?} went out after the status that the proxy stopped"),
            );
            if key == Key::Stopped {
                Self::order(
                    self.stopping,
                    format_args!("the status that the proxy stopped went out unasked"),
                );
                Self::order(
                    self.urgent.is_empty() && self.stream.is_empty(),
                    format_args!(
                        "the status that the proxy stopped went out ahead of {} urgent and {} stream packets",
                        self.urgent.len(),
                        self.stream.len()
                    ),
                );
                self.stopped = true;
                continue;
            }
            // The whole urgent queue goes first, then the front of the stream.
            let at_front = |queue: &VecDeque<Item>| queue.front().is_some_and(|i| i.key == key);
            let due = self
                .urgent
                .front()
                .or(self.stream.front())
                .map(|item| &item.key);
            let queue = if at_front(&self.urgent) {
                Some(Queue::Urgent)
            } else {
                at_front(&self.stream).then_some(Queue::Stream)
            };
            Self::order(
                queue.is_some() && (queue == Some(Queue::Urgent) || self.urgent.is_empty()),
                format_args!("the writer took {key:?} where {due:?} was due"),
            );
            let item = match queue {
                Some(Queue::Urgent) => self.urgent.pop_front(),
                Some(Queue::Stream) => self.stream.pop_front(),
                None => continue,
            };
            let item = item.expect("the item in front");
            if let Some((update, index)) = self.open.take() {
                let part = match item.kind {
                    Kind::Part { update, index, .. } => Some((update, index)),
                    _ => None,
                };
                Self::order(
                    part == Some((update, index)),
                    format_args!(
                        "the parts of an update were split: the writer took {:?} where part {index} of update {update} was due",
                        item.kind
                    ),
                );
            }
            if let Kind::Part {
                update,
                index,
                count,
            } = item.kind
            {
                self.open = (index + 1 < count).then_some((update, index + 1));
            }
            match queue {
                Some(Queue::Urgent) => {
                    Self::wait(
                        self.takes == item.takes + 1,
                        format_args!(
                            "an urgent packet queued after take {} went out at take {}",
                            item.takes, self.takes
                        ),
                    );
                }
                _ => {
                    let full = stream_bytes > 0 && stream_bytes + item.bytes > TAKE_LIMIT;
                    let rest = matches!(item.kind, Kind::Part { index, .. } if index > 0);
                    Self::wait(
                        !full || rest,
                        format_args!(
                            "a take went on past {stream_bytes} bytes of the stream with {:?}",
                            item.kind
                        ),
                    );
                    stream_bytes += item.bytes;
                }
            }
            if item.kind == Kind::Sample {
                self.sample_bytes -= item.bytes;
            } else {
                self.other_bytes -= item.bytes;
                self.others -= 1;
            }
        }
        Self::order(
            self.open.is_none(),
            format_args!("a take ended inside an update"),
        );
    }

    /// Checks the outbox against the model.
    fn check(&self) {
        let others = self.outbox.check();
        let sample_bytes = lock(&self.outbox.queues).sample_bytes;
        Self::order(
            others == self.others && sample_bytes == self.sample_bytes,
            format_args!(
                "the outbox holds {others} packets that are not samples and {sample_bytes} bytes of samples, and the model {} and {}",
                self.others, self.sample_bytes
            ),
        );
    }

    /// Takes until a take brings nothing, as the client's close does, and
    /// checks that the writer took everything the model holds.
    fn drain(&mut self) {
        for _ in 0..DRAIN_TAKES {
            let taken = self.outbox.take();
            let empty = taken.packets.is_empty();
            self.took(taken, true);
            if empty {
                break;
            }
        }
        Self::order(
            self.urgent.is_empty() && self.stream.is_empty(),
            format_args!(
                "the writer never took {} urgent and {} stream packets the outbox was given",
                self.urgent.len(),
                self.stream.len()
            ),
        );
        Self::order(
            self.stopped || !self.stopping,
            format_args!("the status that the proxy stopped never went out"),
        );
    }
}

/// The outbox's properties, the wait if `WAIT` and the order otherwise.
struct Suite<const WAIT: bool>;

impl<const WAIT: bool> StateMachineTest for Suite<WAIT> {
    type SystemUnderTest = Client<WAIT>;
    type Reference = Writer;

    fn init_test(_: &()) -> Client<WAIT> {
        Client::default()
    }

    fn apply(mut client: Client<WAIT>, _: &(), step: Step) -> Client<WAIT> {
        client.step(step);
        client
    }

    fn check_invariants(client: &Client<WAIT>, _: &()) {
        client.check();
    }

    fn teardown(mut client: Client<WAIT>, _: ()) {
        client.drain();
        client.check();
    }
}

prop_state_machine! {
    /// The writer takes what was queued in the outbox's order, every packet
    /// but the oldest samples past the budget, each update whole and each
    /// end once, whatever the callers queue and however the takes fall.
    #[test]
    fn the_writer_takes_what_was_queued_in_order(sequential 1..40 => Suite<false>);

    /// An urgent packet waits behind at most one take of the stream, which
    /// ends at the take limit or at the end of an update.
    #[test]
    fn an_urgent_packet_waits_behind_at_most_a_take(sequential 1..40 => Suite<true>);
}

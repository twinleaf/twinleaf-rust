//! State-machine properties of the [`Outbox`], checked against a written
//! spec.
//!
//! The spec comes from the docs of `outbox.rs`, its module doc and the docs
//! of its constants and methods, and not from its code. Each law names its
//! kind, its source and a wrong implementation it rules out, and each
//! failure names the law it broke.
//!
//! # Observations
//!
//! What each [`Outbox::take`] hands the writer, its packets, the count of
//! samples dropped since the last take, and the end. After each step, the
//! recount of [`Outbox::check`], the packets other than samples it says the
//! outbox holds, and the outbox's count of the bytes of samples it holds.
//!
//! # The model
//!
//! An urgent queue and a stream of the packets the outbox took, in the order
//! it took them, each known by its key, its kind and its size. Beside them
//! the reason to let the client go that the next take brings, and where the
//! proxy's stop is. The model never chooses a sample to drop. It drops as
//! many as a take says went, the oldest first, and checks that number
//! against the budget. It never computes a take either. It checks the take
//! it is given against laws 1 to 4.
//!
//! # Laws
//!
//! 1. Order, model based. A take brings the whole urgent queue in its order,
//!    then the front of the stream in its order, and nothing else but the
//!    status of law 10. Source, the module doc and [`Outbox::take`]. Rules
//!    out a take that leaves an urgent packet behind, or puts the stream
//!    first.
//! 2. Progress, postcondition. A take brings at least one packet of the
//!    stream while the stream holds one. Source, [`Outbox::take`]. Rules out
//!    a take whose first packet must fit under the limit too.
//! 3. The limit, postcondition. Each packet of a take's share of the stream
//!    after the first fits under [`TAKE_LIMIT`] with those before it, or
//!    continues an update. Source, [`Outbox::take`]. Rules out
//!    `taken + size > 2 * TAKE_LIMIT`.
//! 4. A take goes as far as it may, postcondition. A take leaves packets of
//!    the stream behind only when the next would take its share past
//!    [`TAKE_LIMIT`] and does not continue an update. Source,
//!    [`Outbox::take`], which brings as much of the stream as fits under
//!    [`TAKE_LIMIT`] and stops only at the end of the stream or at a packet
//!    that would pass it, and the module doc, where a take never ends inside
//!    an update. Rules out `taken + size > TAKE_LIMIT / 64`, and a take that
//!    stops after one packet of the stream.
//! 5. Only samples go, the oldest first, model based. The samples a take
//!    says went are the oldest the stream held, and every packet other than
//!    a sample stays until a take brings it. Source, the module doc. Rules
//!    out dropping the newest sample, or a heartbeat.
//! 6. The budget, postcondition. Once the samples a take says went are gone,
//!    the samples held take at most [`SAMPLE_BUDGET`] bytes, and with the
//!    newest of those gone back they would take more. After every step the
//!    outbox's own count of the bytes of samples is within the budget.
//!    Source, the module doc and [`SAMPLE_BUDGET`]. Rules out dropping one
//!    sample too many or too few, and dropping only when the writer takes.
//! 7. Updates whole, invariant. The parts of an update follow each other on
//!    one queue with nothing between them, a metadata update on the stream
//!    and any other on the urgent queue. Source, the module doc and
//!    [`Outbox::update`]. [`Outbox::check`] checks it after every step and
//!    law 1 at every take. Rules out a metadata update queued as urgent.
//! 8. Refusal, model based. A call that queues is refused whole while the
//!    outbox holds more than [`HARD_LIMIT`] bytes of packets other than
//!    samples, or once the proxy stopped, and is taken whole otherwise. Once
//!    takes bring those packets back under the limit, calls are taken again.
//!    Source, the module doc, where a client counts as too slow while its
//!    other packets take more than [`HARD_LIMIT`] bytes and the outbox
//!    refuses what is queued meanwhile, and [`Outbox::stop`]. Rules out
//!    refusing at the limit itself, or queuing after the stop.
//! 9. Ends, postcondition. The first reason to let the client go since the
//!    last take that brought one wins, and the next take brings it. The call
//!    that takes the packets other than samples past [`HARD_LIMIT`] asks for
//!    [`ClientError::TooSlow`]. Source, the module doc, where each take
//!    hands the first reason asked for since the last take that handed one
//!    and the call past the limit asks for [`ClientError::TooSlow`], and
//!    [`Outbox::end`]. Rules out a later reason that replaces the first, and
//!    a client never told it is too slow.
//! 10. Stop, model based. The status that the proxy stopped is never
//!     refused. It goes last at the take that empties the stream, once, and
//!     nothing goes after it. Source, the module doc, [`Outbox::stop`] and
//!     [`Outbox::take`]. Rules out the status ahead of the stream, or twice.
//! 11. Drain, completeness. Taking until a take brings nothing, as the
//!     client's close does, takes every packet the model holds, and the
//!     status that the proxy stopped once the proxy stopped. Source, the
//!     module doc. Rules out a take that brings nothing while the outbox
//!     holds something.
//! 12. A call races the writer, linearizability. A thread makes one call
//!     while the writer queues and takes on another. The takes and the drain
//!     after them must be what the model accepts with that call made whole
//!     at one place among the writer's calls. Each race must reach every
//!     place. The races are an update while the writer takes twice, and the
//!     stop while the writer queues an answer, takes, queues a sample and
//!     takes. Source, the module doc, where the outbox queues an update in
//!     one step, and laws 8 and 10. The `shuttle` module checks it under
//!     shuttle's schedulers, where a failure replays from its schedule. Rules
//!     out an update that locks the outbox once per part, and a stop that
//!     lets a packet queued after it go out behind the status.
//! 13. Wakeups, postcondition. A step that queues a packet the outbox takes,
//!     asks that the client be let go or stops the proxy leaves the writer
//!     woken. Source, [`Outbox`], which wakes the client's task whenever it
//!     has something to write. Rules out a stop that never wakes the writer.
//!
//! # Generator obligations
//!
//! Every step is valid by construction. Samples of 4 to 480 bytes and
//! answers of at most 400 bytes fit a packet, and an update's parts are
//! sized from what one part carries. Each packet of samples is of a stream
//! from 1 to 4, which its tag picks. No step has a precondition, so a shrunk
//! case stays valid. Each case draws its own weight for each kind of step,
//! and about half of the kinds are off in each case. So the stop ends only
//! the cases that draw it, and a flood reaches [`HARD_LIMIT`] in many cases.
//! Sizes drawn at random almost never fill a limit exactly, so some steps
//! of samples fill the stream to [`TAKE_LIMIT`] or the samples held to the
//! budget to the byte, and some floods fill [`HARD_LIMIT`] to the byte, and
//! each stops there. Laws 4, 6, 8 and 9 then hold at the limit itself.
//!
//! The classes counted are the cases where a take left the stream behind at
//! the limit, where a take passed the limit inside an update, where a take
//! said samples went, where an urgent packet overtook the stream, where a
//! take brought [`ClientError::TooSlow`], where a take brought the status
//! that the proxy stopped, and where the samples held, the other packets
//! held or the stream filled their limit exactly. With
//! `TWINLEAF_OUTBOX_CLASSES` set, each case prints the counts so far, and
//! each shuttle race prints how often its call fell at each place.
//!
//! # Exclusions
//!
//! - That the client's task wakes when the outbox wakes it, which is the
//!   task's. Law 13 checks only that the outbox wakes it. The logging of the
//!   drops, which is the task's too.
//! - [`HARD_LIMIT`] in the race, as filling it costs a scheduling point per
//!   packet. The sequential property queues updates across the limit.
//! - More than one writer, as each client has one task.
//! - What the parts carry, which the tests of `control` join.
//!
//! # Examples that stay
//!
//! None. Every law is a property.

use super::super::client::ClientError;
use super::{Outbox, Taken, HARD_LIMIT, SAMPLE_BUDGET, TAKE_LIMIT};
use crate::chan::lock;
use crate::device::fixtures::{broadcast, segment_record};
use crate::proto::data::Metadata;
use crate::proto::DeviceRoute;
use crate::proxy::control::{decode, parts, Part, Update};
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use futures_util::FutureExt;
use proptest::prelude::*;
use proptest::strategy::Union;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

#[cfg(shuttle)]
mod shuttle;

/// The most takes the writer makes to empty the outbox at the end of a case.
const DRAIN_TAKES: usize = 100_000;

/// What the value of an answer starts with. It decodes as no [`Part`].
const ANSWER: &[u8] = b"ans";

/// The environment variable that has each case print its class counts.
const CLASSES: &str = "TWINLEAF_OUTBOX_CLASSES";

/// One step of a case. The kinds are listed simplest first, in the order of
/// a case's weights.
#[derive(Clone, Debug)]
enum Step {
    /// The writer takes.
    Take,
    /// The engine queues samples, as `fill` says.
    Samples {
        size: usize,
        count: usize,
        fill: Fill,
    },
    /// The engine queues `count` packets of the stream that are not samples,
    /// segment records if `record` and heartbeats otherwise.
    Other { record: bool, count: usize },
    /// `count` answers, each of `size` bytes of value, on the urgent queue.
    Answers { size: usize, count: usize },
    /// A watch's update of `parts` packets, a metadata update on the stream
    /// if `metadata` and another update on the urgent queue otherwise.
    Update {
        id: u16,
        metadata: bool,
        parts: usize,
    },
    /// Something asks that the client be let go, as it hung up if `hung_up`
    /// and as its link ended otherwise.
    End { hung_up: bool },
    /// The proxy stops.
    Stop,
    /// Answers until the outbox refuses them, and `extra` more after that.
    /// With `exact`, the answers instead bring the packets other than
    /// samples held to [`HARD_LIMIT`] bytes exactly, and stop there.
    Flood { exact: bool, extra: usize },
}

/// What a step of samples queues.
#[derive(Clone, Copy, Debug)]
enum Fill {
    /// `count` packets of samples, each of `size` bytes of data.
    Count,
    /// Samples until the stream holds [`TAKE_LIMIT`] bytes exactly.
    TakeLimit,
    /// Samples until those held take [`SAMPLE_BUDGET`] bytes exactly.
    Budget,
}

/// How many kinds of [`Step`] there are.
const KINDS: usize = 8;

/// The steps a case is drawn from, any of them after any other, each kind
/// with a weight the case draws.
struct Writer;

impl ReferenceStateMachine for Writer {
    type State = [u32; KINDS];
    type Transition = Step;

    /// Draws a weight for each kind of step, none for about half of them.
    fn init_state() -> BoxedStrategy<[u32; KINDS]> {
        proptest::array::uniform8(prop_oneof![Just(0u32), 1..=4u32]).boxed()
    }

    fn transitions(weights: &[u32; KINDS]) -> BoxedStrategy<Step> {
        let count = prop_oneof![4 => 1..=8usize, 1 => 100..=3000usize];
        let fill = prop_oneof![
            4 => Just(Fill::Count),
            1 => Just(Fill::TakeLimit),
            1 => Just(Fill::Budget),
        ];
        let samples = (4..=480usize, count, fill).prop_map(|(size, count, fill)| Step::Samples {
            size,
            count,
            fill,
        });
        let other =
            (any::<bool>(), 1..=4usize).prop_map(|(record, count)| Step::Other { record, count });
        let answers =
            (0..=400usize, 1..=4usize).prop_map(|(size, count)| Step::Answers { size, count });
        let parts = prop_oneof![4 => 1..=4usize, 1 => 100..=160usize];
        let update =
            (1..=3u16, any::<bool>(), parts).prop_map(|(id, metadata, parts)| Step::Update {
                id,
                metadata,
                parts,
            });
        let end = any::<bool>().prop_map(|hung_up| Step::End { hung_up });
        let flood =
            (any::<bool>(), 1..=4usize).prop_map(|(exact, extra)| Step::Flood { exact, extra });
        let kinds: [BoxedStrategy<Step>; KINDS] = [
            Just(Step::Take).boxed(),
            samples.boxed(),
            other.boxed(),
            answers.boxed(),
            update.boxed(),
            end.boxed(),
            Just(Step::Stop).boxed(),
            flood.boxed(),
        ];
        let mut drawn: Vec<_> = weights
            .iter()
            .zip(kinds)
            .filter(|(weight, _)| **weight > 0)
            .map(|(weight, kind)| (*weight, kind))
            .collect();
        if drawn.is_empty() {
            drawn.push((1, Just(Step::Take).boxed()));
        }
        Union::new_weighted(drawn).boxed()
    }

    fn apply(weights: [u32; KINDS], _: &Step) -> [u32; KINDS] {
        weights
    }
}

/// What a packet the harness queued is.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Sample,
    Other,
    /// Part `index` of an update.
    Part {
        index: usize,
    },
}

/// The queue a packet goes on.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Queue {
    Urgent,
    Stream,
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

/// A packet as the model holds it.
#[derive(Clone)]
struct Item {
    key: Key,
    kind: Kind,
    bytes: usize,
    /// How many packets the model took before it.
    seq: usize,
}

impl Item {
    fn new(packet: &Packet, kind: Kind) -> Item {
        Item {
            key: key(packet),
            kind,
            bytes: packet.as_bytes().len(),
            seq: 0,
        }
    }

    /// Returns `true` if the item is a part of an update after its first.
    fn continues(&self) -> bool {
        matches!(self.kind, Kind::Part { index } if index > 0)
    }
}

/// Where the model is in the proxy's stop.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum Stopping {
    #[default]
    Running,
    /// The status that the proxy stopped is due.
    Due,
    /// The status went to the writer.
    Sent,
}

/// What the model has seen a case reach.
#[derive(Clone, Copy, Default)]
struct Classes {
    limit: bool,
    spanning: bool,
    eviction: bool,
    overtaking: bool,
    too_slow: bool,
    stopped: bool,
    /// The samples held took [`SAMPLE_BUDGET`] bytes exactly.
    at_budget: bool,
    /// The packets other than samples held took [`HARD_LIMIT`] bytes exactly.
    at_hard_limit: bool,
    /// The stream held [`TAKE_LIMIT`] bytes exactly.
    at_take_limit: bool,
}

/// The cases run, and how many reached each class, in the order of
/// [`Classes`].
static COUNTS: [AtomicUsize; 10] = [const { AtomicUsize::new(0) }; 10];

impl Classes {
    /// Counts the case that reached these classes, and prints the counts so
    /// far if [`CLASSES`] is set.
    fn count(self) {
        let reached = [
            true,
            self.limit,
            self.spanning,
            self.eviction,
            self.overtaking,
            self.too_slow,
            self.stopped,
            self.at_budget,
            self.at_hard_limit,
            self.at_take_limit,
        ];
        for (count, reached) in COUNTS.iter().zip(reached) {
            count.fetch_add(usize::from(reached), Ordering::Relaxed);
        }
        if std::env::var_os(CLASSES).is_some() {
            let [cases, limit, spanning, eviction, overtaking, too_slow, stopped, budget, hard, take] =
                COUNTS.each_ref().map(|count| count.load(Ordering::Relaxed));
            eprintln!(
                "outbox classes in {cases} cases, limit {limit}, spanning {spanning}, \
                 eviction {eviction}, overtaking {overtaking}, too slow {too_slow}, \
                 stopped {stopped}, at the budget {budget}, at the hard limit {hard}, at the take limit {take}"
            );
        }
    }
}

/// The model of an outbox.
#[derive(Clone, Default)]
struct Model {
    urgent: VecDeque<Item>,
    stream: VecDeque<Item>,
    sample_bytes: usize,
    /// The bytes of the packets other than samples held.
    other_bytes: usize,
    /// How many packets other than samples are held.
    others: usize,
    /// Why the client must be let go, at the next take.
    end: Option<ClientError>,
    stop: Stopping,
    /// How many packets the model took.
    queued: usize,
}

impl Model {
    /// Returns `true` if a call that queues is taken, by law 8.
    fn accepts(&self) -> bool {
        self.stop == Stopping::Running && self.other_bytes <= HARD_LIMIT
    }

    /// Takes `items` from one call on `queue`, whole or not at all, by laws 7
    /// to 9.
    fn queue(&mut self, items: &[Item], queue: Queue) {
        if !self.accepts() {
            return;
        }
        for item in items {
            let mut item = item.clone();
            item.seq = self.queued;
            self.queued += 1;
            if item.kind == Kind::Sample {
                self.sample_bytes += item.bytes;
            } else {
                self.other_bytes += item.bytes;
                self.others += 1;
            }
            match queue {
                Queue::Urgent => self.urgent.push_back(item),
                Queue::Stream => self.stream.push_back(item),
            }
        }
        if self.other_bytes > HARD_LIMIT {
            self.end.get_or_insert(ClientError::TooSlow);
        }
    }

    /// Asks that the client be let go for `reason`, by law 9.
    fn end(&mut self, reason: ClientError) {
        self.end.get_or_insert(reason);
    }

    /// The proxy stops, by law 10.
    fn stop(&mut self) {
        if self.stop == Stopping::Running {
            self.stop = Stopping::Due;
        }
    }

    /// Takes `item` out of the counts, as it leaves the model.
    fn release(&mut self, item: &Item) {
        if item.kind == Kind::Sample {
            self.sample_bytes -= item.bytes;
        } else {
            self.other_bytes -= item.bytes;
            self.others -= 1;
        }
    }

    /// Drops the `dropped` oldest samples on the stream, as a take says they
    /// went, and checks them against the budget by laws 5 and 6.
    fn drop_oldest(&mut self, dropped: u64) -> Result<(), String> {
        let mut newest = None;
        for _ in 0..dropped {
            let Some(at) = self.stream.iter().position(|i| i.kind == Kind::Sample) else {
                return Err(format!(
                    "law 5, a take says {dropped} samples went, more than the stream held"
                ));
            };
            let gone = self.stream.remove(at).expect("the sample found");
            self.sample_bytes -= gone.bytes;
            newest = Some(gone.bytes);
        }
        if self.sample_bytes > SAMPLE_BUDGET {
            return Err(format!(
                "law 6, a take says {dropped} samples went, and the {} bytes of samples left pass the budget",
                self.sample_bytes
            ));
        }
        if let Some(bytes) = newest {
            if self.sample_bytes + bytes <= SAMPLE_BUDGET {
                return Err(format!(
                    "law 6, a take says {dropped} samples went, and the newest of {bytes} bytes fit beside the {} bytes left",
                    self.sample_bytes
                ));
            }
        }
        Ok(())
    }

    /// Checks `taken` against laws 1 to 6, 9 and 10, takes what it brought
    /// out of the model, and notes the classes it reached in `classes`.
    fn take(&mut self, taken: &Taken, classes: &mut Classes) -> Result<(), String> {
        let due = self.end.take();
        if format!("{:?}", taken.end) != format!("{due:?}") {
            return Err(format!(
                "law 9, a take brought the end {:?} where {due:?} was due",
                taken.end
            ));
        }
        classes.too_slow |= matches!(taken.end, Some(ClientError::TooSlow));
        self.drop_oldest(taken.dropped)?;
        classes.eviction |= taken.dropped > 0;

        let mut packets = taken.packets.iter().map(key).peekable();
        if let (Some(urgent), Some(stream)) = (self.urgent.back(), self.stream.front()) {
            classes.overtaking |= urgent.seq > stream.seq;
        }
        while let Some(item) = self.urgent.pop_front() {
            let brought = packets.next();
            if brought.as_ref() != Some(&item.key) {
                return Err(format!(
                    "law 1, the take brought {brought:?} where urgent {:?} was due",
                    item.key
                ));
            }
            self.release(&item);
        }

        let (mut share, mut brought) = (0, 0);
        while let Some(item) = self.stream.front() {
            if packets.peek() != Some(&item.key) {
                break;
            }
            let past = share + item.bytes > TAKE_LIMIT;
            if brought > 0 && past && !item.continues() {
                return Err(format!(
                    "law 3, a take went on past {share} bytes of the stream with {:?} of {} bytes",
                    item.key, item.bytes
                ));
            }
            classes.spanning |= brought > 0 && past;
            share += item.bytes;
            brought += 1;
            packets.next();
            let item = self.stream.pop_front().expect("the item in front");
            self.release(&item);
        }
        if let Some(next) = packets.peek().filter(|key| **key != Key::Stopped) {
            let due = self.stream.front().map(|item| &item.key);
            return Err(format!(
                "law 1, the take brought {next:?} where {due:?} was due"
            ));
        }
        if let Some(next) = self.stream.front() {
            if brought == 0 {
                return Err(format!(
                    "law 2, a take brought nothing of the stream while it holds {:?}",
                    next.key
                ));
            }
            if next.continues() || share + next.bytes <= TAKE_LIMIT {
                return Err(format!(
                    "law 4, a take stopped at {share} bytes of the stream ahead of {:?} of {} bytes",
                    next.kind, next.bytes
                ));
            }
            classes.limit = true;
        }

        if self.stream.is_empty() && self.stop == Stopping::Due {
            let brought = packets.next();
            if brought != Some(Key::Stopped) {
                return Err(format!(
                    "law 10, the take that emptied the stream brought {brought:?} where Stopped was due"
                ));
            }
            self.stop = Stopping::Sent;
            classes.stopped = true;
        }
        if let Some(extra) = packets.next() {
            return Err(format!(
                "law 10, the take brought {extra:?} with the stop {:?} and the stream holding {}",
                self.stop,
                self.stream.len()
            ));
        }
        Ok(())
    }

    /// Checks that a drain took everything, by law 11.
    fn drained(&self) -> Result<(), String> {
        if !self.urgent.is_empty() || !self.stream.is_empty() {
            return Err(format!(
                "law 11, the drain left {} urgent and {} stream packets",
                self.urgent.len(),
                self.stream.len()
            ));
        }
        if self.stop == Stopping::Due {
            return Err("law 11, the status that the proxy stopped never went out".to_string());
        }
        Ok(())
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

/// Returns the bytes of `packet`, a packet the harness built to measure.
fn packet_bytes<E: std::fmt::Debug>(packet: Result<Packet, E>) -> usize {
    packet.expect("a packet that fits").as_bytes().len()
}

/// Splits `gap` bytes into packets of `least` to `most` bytes each, or into
/// none if `gap` is less than `least`. It needs `most` to be at least twice
/// `least`.
fn split(mut gap: usize, least: usize, most: usize) -> Vec<usize> {
    let mut sizes = Vec::new();
    if gap < least {
        return sizes;
    }
    while gap > most {
        let size = if gap - most >= least {
            most
        } else {
            gap - least
        };
        sizes.push(size);
        gap -= size;
    }
    sizes.push(gap);
    sizes
}

/// Takes from `outbox` until a take brings nothing, as the client's close
/// does, and returns every take.
fn drain(outbox: &Outbox) -> Vec<Taken> {
    let mut takes = Vec::new();
    for _ in 0..DRAIN_TAKES {
        let taken = outbox.take();
        let empty = taken.packets.is_empty();
        takes.push(taken);
        if empty {
            break;
        }
    }
    takes
}

/// The outbox under test, with its model.
#[derive(Default)]
struct Client {
    outbox: Arc<Outbox>,
    model: Model,
    next_tag: u32,
    classes: Classes,
}

impl Client {
    fn tag(&mut self) -> u32 {
        self.next_tag += 1;
        self.next_tag
    }

    /// Queues `packet` on `queue` as one caller does.
    fn queue(&mut self, packet: Packet, kind: Kind, queue: Queue) {
        self.model.queue(&[Item::new(&packet, kind)], queue);
        match queue {
            Queue::Urgent => self.outbox.urgent(packet),
            Queue::Stream => self.outbox.stream(packet),
        }
    }

    /// Returns a packet of samples with `size` bytes of data, of a stream
    /// from 1 to 4 that its tag picks.
    fn sample_packet(&mut self, size: usize) -> Packet {
        let first = self.tag();
        let stream = 1 + (first % 4) as u8;
        let data = vec![0; size];
        let packet = Packet::samples(stream, 0, first, &data, DeviceRoute::root());
        packet.expect("samples that fit a packet")
    }

    /// Queues a packet of samples with `size` bytes of data, and returns its
    /// bytes.
    fn sample(&mut self, size: usize) -> usize {
        let packet = self.sample_packet(size);
        let bytes = packet.as_bytes().len();
        self.queue(packet, Kind::Sample, Queue::Stream);
        bytes
    }

    /// Returns an answer whose value takes `size` bytes.
    fn answer_packet(&mut self, size: usize) -> Packet {
        let tag = self.tag();
        let mut value = ANSWER.to_vec();
        value.extend(tag.to_le_bytes());
        value.resize(value.len().max(size), 0);
        Packet::proxy_reply(7, &value).expect("an answer that fits")
    }

    /// Queues an answer whose value takes `size` bytes.
    fn answer(&mut self, size: usize) {
        let packet = self.answer_packet(size);
        self.queue(packet, Kind::Other, Queue::Urgent);
    }

    /// Returns an update of `count` parts, metadata if `metadata`, the
    /// model's items for its parts, and the queue they go on.
    fn prepare(&mut self, id: u16, metadata: bool, count: usize) -> (Update, Vec<Item>, Queue) {
        let update = tagged_update(self.tag(), metadata, count);
        let packets = parts(id, &update);
        assert_eq!(packets.len(), count, "the harness sized the update");
        let items = packets
            .iter()
            .enumerate()
            .map(|(index, part)| Item::new(part, Kind::Part { index }))
            .collect();
        let queue = if metadata {
            Queue::Stream
        } else {
            Queue::Urgent
        };
        (update, items, queue)
    }

    fn step(&mut self, step: Step) {
        match step {
            Step::Take => {
                let taken = self.outbox.take();
                if let Err(broken) = self.model.take(&taken, &mut self.classes) {
                    panic!("{broken}");
                }
            }
            Step::Samples { size, count, fill } => {
                let (limit, held): (usize, fn(&Outbox) -> usize) = match fill {
                    Fill::Count => {
                        for _ in 0..count {
                            self.sample(size);
                        }
                        return;
                    }
                    Fill::TakeLimit => (TAKE_LIMIT, |outbox| {
                        let queues = lock(&outbox.queues);
                        queues.stream.iter().map(|p| p.as_bytes().len()).sum()
                    }),
                    Fill::Budget => (SAMPLE_BUDGET, |outbox| lock(&outbox.queues).sample_bytes),
                };
                let overhead =
                    packet_bytes(Packet::samples(1, 0, 0, &[0; 4], DeviceRoute::root())) - 4;
                let gap = limit.saturating_sub(held(&self.outbox));
                for bytes in split(gap, overhead + 4, overhead + 480) {
                    self.sample(bytes - overhead);
                }
                let at = held(&self.outbox) == limit;
                match fill {
                    Fill::TakeLimit => self.classes.at_take_limit |= at,
                    _ => self.classes.at_budget |= at,
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
            Step::Answers { size, count } => {
                for _ in 0..count {
                    self.answer(size);
                }
            }
            Step::Update {
                id,
                metadata,
                parts,
            } => {
                let (update, items, queue) = self.prepare(id, metadata, parts);
                self.model.queue(&items, queue);
                self.outbox.update(id, &update);
            }
            Step::End { hung_up } => {
                let reason = if hung_up {
                    ClientError::HungUp
                } else {
                    ClientError::LinkEnded("tcp://device".to_string())
                };
                self.model.end(reason.clone());
                self.outbox.end(reason);
            }
            Step::Stop => {
                self.model.stop();
                self.outbox.stop();
            }
            Step::Flood { exact, mut extra } => {
                if exact {
                    if self.model.accepts() {
                        let overhead = packet_bytes(Packet::proxy_reply(7, &[0; 7])) - 7;
                        let gap = HARD_LIMIT - self.model.other_bytes;
                        for bytes in split(gap, overhead + 7, overhead + 400) {
                            self.answer(bytes - overhead);
                        }
                        self.classes.at_hard_limit |= self.model.other_bytes == HARD_LIMIT;
                    }
                    return;
                }
                while extra > 0 {
                    if !self.model.accepts() {
                        extra -= 1;
                    }
                    self.answer(400);
                }
            }
        }
    }

    /// Checks the outbox's recount against the model, by laws 5 to 8.
    fn check(&self) {
        let others = self.outbox.check();
        assert_eq!(
            others, self.model.others,
            "laws 5 and 8, the outbox holds {others} packets other than samples, and the model {}",
            self.model.others
        );
        let sample_bytes = lock(&self.outbox.queues).sample_bytes;
        assert!(
            sample_bytes <= SAMPLE_BUDGET,
            "law 6, the outbox holds {sample_bytes} bytes of samples"
        );
    }

    /// Drains the outbox, and checks each take and the end by law 11.
    fn drain(&mut self) {
        for taken in drain(&self.outbox) {
            if let Err(broken) = self.model.take(&taken, &mut self.classes) {
                panic!("{broken}");
            }
        }
        if let Err(broken) = self.model.drained() {
            panic!("{broken}");
        }
    }
}

/// The outbox's laws.
struct Suite;

impl StateMachineTest for Suite {
    type SystemUnderTest = Client;
    type Reference = Writer;

    fn init_test(_: &[u32; KINDS]) -> Client {
        Client::default()
    }

    fn apply(mut client: Client, _: &[u32; KINDS], step: Step) -> Client {
        let queued = client.model.queued;
        let asks = matches!(step, Step::End { .. } | Step::Stop);
        let shown = format!("{step:?}");
        client.step(step);
        let woken = client.outbox.ready.notified().now_or_never().is_some();
        assert!(
            woken || !(asks || client.model.queued > queued),
            "law 13, the step {shown} left the writer asleep"
        );
        client
    }

    fn check_invariants(client: &Client, _: &[u32; KINDS]) {
        client.check();
    }

    fn teardown(mut client: Client, _: [u32; KINDS]) {
        client.drain();
        client.check();
        client.classes.count();
    }
}

prop_state_machine! {
    /// The writer takes what was queued as laws 1 to 11 say, whatever the
    /// callers queue and however the takes fall.
    #[test]
    fn the_writer_takes_what_was_queued_as_the_spec_says(sequential 1..40 => Suite);
}

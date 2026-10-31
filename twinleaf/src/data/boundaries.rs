//! State-machine properties of the boundaries a reader of batches learns.
//!
//! Each case simulates one device stream, which takes samples, rolls into
//! its next segment with or without sending the segment's record first,
//! changes its rate or decimation, takes a new time reference, reboots and
//! drops samples. Its packets go through the parser, and the batches through
//! a [`Lane`] as the pump sends them, to a reader whose reads interleave with
//! the device's steps. A lane with little room drops batches, as for a slow
//! app. Every sample carries its position in the device's sequence, so the
//! harness knows what the reader missed and when each sample was taken.
//!
//! The first batch says [`BoundaryReason::Initial`], and a first batch after
//! a gap says only the gap, as the reader missed the start of the stream.
//! Between two batches the reader receives, it learns of each change the
//! device made there and of each sample it lost, and of nothing else.
//!
//! - A reboot comes as a [`BoundaryReason::SessionChanged`] naming both
//!   sessions, and stands for whatever else changed and was lost before it.
//! - Otherwise a new time reference comes as a
//!   [`BoundaryReason::TimeRefSessionChanged`], and a rate change as a
//!   [`BoundaryReason::RateChanged`] naming both rates, each standing for
//!   what else changed and was lost before it.
//! - Otherwise a rollover comes as a [`BoundaryReason::SegmentRollover`]
//!   naming both segments.
//! - A lost sample comes as a [`BoundaryReason::SamplesLost`] naming the
//!   sample due and the sample received, or as a gap the lane reports, so a
//!   loss across a rollover needs a rollover and a loss. The gaps count each
//!   batch the lane dropped.
//! - A batch with nothing of these before it carries no boundary.
//! - Each batch's first timestamp is the time the device took its sample.
//!
//! `reported` turns a batch and the gap ahead of it into the change and the
//! loss the harness compares with what the device did. A boundary that
//! carries a change and a loss apart, as the fix of Problems A and B gives
//! it, changes `reported` alone.
//!
//! Two properties hold. A reader that keeps up learns every change, when no
//! sample is lost next to a new segment, and a reader that lags learns every
//! loss of a device that changes nothing. They catch faults in the parser's
//! rollovers, forced or not, its boundaries and their payloads, its clock,
//! and the lane's gaps.
//!
//! Four properties fail, and are ignored until the parser and the lane
//! change.
//!
//! - Problem A. A lane that drops batches drops the boundaries they carry,
//!   and the gap it reports carries only a count, so a reader that lags
//!   misses the changes in the batches it lost.
//! - Problem B. The parser takes a segment change for a rollover only when
//!   the new segment starts at sample 0 and the old one ended whole, and one
//!   [`BoundaryReason`] cannot say both that the segment rolled over and
//!   that samples were lost, so a loss across a rollover comes as a
//!   [`BoundaryReason::SegmentChanged`].
//! - Problem C. Before a forced rollover the parser evicts a stale record
//!   from the ring only when its time reference is the current one. A
//!   forced rollover onto a slot whose record is from an older time
//!   reference decodes with that record, so it reports a
//!   [`BoundaryReason::TimeRefSessionChanged`] back to the older reference
//!   and stamps the samples from the record's start. The other properties
//!   leave out exactly that rollover.
//! - Problem D. A device switches segments at the next whole second of its
//!   undecimated clock, and drops the group of inputs that switch leaves
//!   incomplete, so a decimated stream's last sample can end short of the
//!   new segment's start, by less than a period. The parser takes that gap
//!   past half a period for a [`BoundaryReason::SegmentChanged`], and for a
//!   forced rollover it infers the start at the whole second before the
//!   last sample ends, where the device switched at the one after. The
//!   other properties switch segments only where a whole group ends on a
//!   whole second.

use super::fixtures;
use super::{BoundaryReason, PacketParser, SampleBatch};
use crate::device::stream::{Lane, Queued};
use crate::proto::data as wire;
use crate::proto::data::DataType;
use crate::proto::{DeviceRoute, SegmentId, SessionId};
use crate::tio;
use proptest::prelude::*;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::RangeInclusive;

/// The segments in the device's ring.
const SEGMENTS: u8 = 4;

/// The rates the device samples at before decimation, in Hz.
const RATES: [u32; 4] = [1, 2, 4, 10];

/// The decimations the device applies.
const DECIMATIONS: RangeInclusive<u32> = 1..=3;

/// The room in the lane of a reader that never falls behind, as much as a
/// case ever sends it.
const ROOMY: usize = 1024;

/// What the device does between the samples on either side.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Change {
    Rollover,
    Rate,
    TimeRef,
    Reboot,
}

/// Where the device drops samples.
#[derive(Clone, Copy, PartialEq)]
enum Drops {
    Never,
    /// Never on either side of a new segment in one session, so every
    /// segment the device opens reaches the parser.
    ApartFromSegments,
    Anywhere,
}

/// What a property's device and reader do.
trait Case: 'static {
    /// The room in the reader's lane.
    const ROOM: RangeInclusive<usize>;
    /// Whether the device rolls over.
    const ROLLS_OVER: bool;
    /// Whether the device changes its rate and time reference, and reboots.
    const RECONFIGURES: bool;
    const DROPS: Drops;
    /// Whether the device rolls over without the record onto a slot whose
    /// record is from an older time reference.
    const STALE_RING: bool = false;
    /// Whether the device switches segments at the next whole second of its
    /// undecimated clock and drops the incomplete group of inputs, rather
    /// than only where a whole group ends on a whole second.
    const SPLITS_GROUPS: bool = false;
}

/// A reader that keeps up, and a device that changes everything and drops
/// samples away from its new segments.
struct KeepsUp;

impl Case for KeepsUp {
    const ROOM: RangeInclusive<usize> = ROOMY..=ROOMY;
    const ROLLS_OVER: bool = true;
    const RECONFIGURES: bool = true;
    const DROPS: Drops = Drops::ApartFromSegments;
}

/// A reader that lags, and a device that changes nothing and drops samples.
struct Lags;

impl Case for Lags {
    const ROOM: RangeInclusive<usize> = 1..=3;
    const ROLLS_OVER: bool = false;
    const RECONFIGURES: bool = false;
    const DROPS: Drops = Drops::Anywhere;
}

/// A reader that lags, and a device that changes everything and drops no
/// sample. Problem A.
struct LagsAcrossChanges;

impl Case for LagsAcrossChanges {
    const ROOM: RangeInclusive<usize> = 1..=3;
    const ROLLS_OVER: bool = true;
    const RECONFIGURES: bool = true;
    const DROPS: Drops = Drops::Never;
}

/// A reader that keeps up, and a device that rolls over and drops samples
/// anywhere, the first sample of a segment among them. Problem B.
struct LosesAtRollover;

impl Case for LosesAtRollover {
    const ROOM: RangeInclusive<usize> = ROOMY..=ROOMY;
    const ROLLS_OVER: bool = true;
    const RECONFIGURES: bool = false;
    const DROPS: Drops = Drops::Anywhere;
}

/// A reader that keeps up, and a device that also rolls over without the
/// record onto a slot of an older time reference. Problem C.
struct RollsOntoAStaleRecord;

impl Case for RollsOntoAStaleRecord {
    const ROOM: RangeInclusive<usize> = ROOMY..=ROOMY;
    const ROLLS_OVER: bool = true;
    const RECONFIGURES: bool = true;
    const DROPS: Drops = Drops::ApartFromSegments;
    const STALE_RING: bool = true;
}

/// A reader that keeps up, and a device that also switches segments inside
/// a group of inputs. Problem D.
struct SplitsAGroup;

impl Case for SplitsAGroup {
    const ROOM: RangeInclusive<usize> = ROOMY..=ROOMY;
    const ROLLS_OVER: bool = true;
    const RECONFIGURES: bool = false;
    const DROPS: Drops = Drops::Never;
    const SPLITS_GROUPS: bool = true;
}

/// One step of a case.
#[derive(Clone, Debug)]
enum Step {
    /// The device takes `count` samples, and sends them in packets of at
    /// most `rows`.
    Samples { count: u32, rows: u32 },
    /// The device drops its next `count` samples.
    Drop { count: u32 },
    /// The device takes samples to its next whole second, and rolls into its
    /// next segment. Unless `forced`, it sends the new segment's record
    /// first, and otherwise the record follows the segment's first packet.
    Rollover { forced: bool },
    /// The device takes samples to its next whole second, and goes on at
    /// `RATES[rate]` decimated by `decimation` in its next segment.
    Rate { rate: usize, decimation: u32 },
    /// The device takes samples to its next whole second, and goes on with
    /// a new time reference in its next segment.
    TimeRef,
    /// The device reboots into a new session, and describes itself again.
    Reboot,
    /// The reader takes up to `count` entries off its lane.
    Read { count: usize },
}

/// A packet the device sends.
#[derive(Clone, Debug)]
enum Sent {
    Device {
        session: u32,
    },
    Stream,
    Segment {
        id: u8,
        start: u32,
        rate: u32,
        decimation: u32,
        timeref: u32,
    },
    Column,
    /// `rows` samples of `segment` from sample `first`, the first of them at
    /// `position` in the device's sequence.
    Samples {
        segment: u8,
        first: u32,
        position: u64,
        rows: u32,
    },
}

impl Sent {
    fn packet(&self) -> tio::Packet {
        let record = match *self {
            Sent::Device { session } => wire::Metadata::Device(wire::Device {
                session: SessionId::new(session),
                ..fixtures::device()
            }),
            Sent::Stream => wire::Metadata::Stream(wire::Stream {
                n_segments: SEGMENTS,
                ..fixtures::stream(1)
            }),
            Sent::Segment {
                id,
                start,
                rate,
                decimation,
                timeref,
            } => wire::Metadata::Segment(wire::Segment {
                segment_id: SegmentId::new(id),
                start_time: start,
                sampling_rate: rate,
                decimation,
                timeref_session: SessionId::new(timeref),
                ..fixtures::segment(1)
            }),
            Sent::Column => wire::Metadata::Column(fixtures::column(1, 0, DataType::U32)),
            Sent::Samples {
                segment,
                first,
                position,
                rows,
            } => {
                let positions = (position..position + u64::from(rows)).map(|at| at as u32);
                let data: Vec<u8> = positions.flat_map(u32::to_le_bytes).collect();
                let packet = tio::Packet::samples(1, segment, first, &data, DeviceRoute::root());
                return packet.expect("samples that fit a packet");
            }
        };
        let flags = wire::MetadataFlags::default();
        tio::Packet::metadata(record, flags, DeviceRoute::root()).expect("a record that fits")
    }
}

/// Where the device was at one position of its sequence.
#[derive(Clone, Copy, Debug)]
struct At {
    session: u32,
    timeref: u32,
    segment: u8,
    /// The rate after decimation, in Hz.
    rate: f64,
    number: u32,
    /// When the sample ended, in seconds after the epoch.
    time: f64,
}

/// The simulated device, and what the reader's lane has room for.
#[derive(Clone, Debug)]
struct Device {
    room: usize,
    /// Whether a segment switch drops an incomplete group of inputs.
    splits_groups: bool,
    session: u32,
    timeref: u32,
    rate: u32,
    decimation: u32,
    segment: u8,
    /// The segment's start, in whole seconds.
    start: u32,
    /// The segment's next sample number.
    next: u32,
    /// The positions since the last change, and the samples sent of them.
    since_change: u64,
    sent_since_change: u64,
    /// Whether the last position was dropped.
    dropped_last: bool,
    /// Whether the last change opened a segment in the same session.
    segmented_last: bool,
    /// Whether the current segment's record waits for its first packet.
    record_due: bool,
    /// The time reference of the record last sent for each slot of the ring
    /// in this session.
    ring: [Option<u32>; SEGMENTS as usize],
    /// Each change, at the position of the first sample after it.
    changes: Vec<(u64, Change)>,
    /// Where the device was at each position, sent or dropped.
    history: Vec<At>,
    /// What the last step sent.
    sent: Vec<Sent>,
}

impl Device {
    fn new(room: usize, splits_groups: bool, rate: u32, decimation: u32) -> Device {
        let mut device = Device {
            room,
            splits_groups,
            session: 1,
            timeref: 7,
            rate,
            decimation,
            segment: 0,
            start: 1000,
            next: 0,
            since_change: 0,
            sent_since_change: 0,
            dropped_last: false,
            segmented_last: false,
            record_due: false,
            ring: [None; SEGMENTS as usize],
            changes: Vec::new(),
            history: Vec::new(),
            sent: Vec::new(),
        };
        device.describe();
        device
    }

    /// The next sample's position in the device's sequence.
    fn position(&self) -> u64 {
        self.history.len() as u64
    }

    /// Sends every record of the device.
    fn describe(&mut self) {
        self.ring = [None; SEGMENTS as usize];
        self.sent.extend([
            Sent::Device {
                session: self.session,
            },
            Sent::Stream,
        ]);
        self.send_record();
        self.sent.push(Sent::Column);
    }

    /// Sends the current segment's record.
    fn send_record(&mut self) {
        self.ring[usize::from(self.segment)] = Some(self.timeref);
        self.record_due = false;
        self.sent.push(Sent::Segment {
            id: self.segment,
            start: self.start,
            rate: self.rate,
            decimation: self.decimation,
            timeref: self.timeref,
        });
    }

    /// Moves past the next sample, sent or dropped.
    fn advance(&mut self) {
        let period = f64::from(self.decimation) / f64::from(self.rate);
        self.history.push(At {
            session: self.session,
            timeref: self.timeref,
            segment: self.segment,
            rate: f64::from(self.rate) / f64::from(self.decimation),
            number: self.next,
            time: f64::from(self.start) + period * f64::from(self.next + 1),
        });
        self.next += 1;
        self.since_change += 1;
    }

    /// Takes `count` samples, in packets of at most `rows`.
    fn take(&mut self, count: u32, rows: u32) {
        let mut left = count;
        while left > 0 {
            let rows = left.min(rows);
            self.sent.push(Sent::Samples {
                segment: self.segment,
                first: self.next,
                position: self.position(),
                rows,
            });
            if self.record_due {
                self.send_record();
            }
            (0..rows).for_each(|_| self.advance());
            self.sent_since_change += u64::from(rows);
            self.dropped_last = false;
            left -= rows;
        }
    }

    /// Returns `true` if the next sample's group of inputs starts on a whole
    /// second of the undecimated clock.
    fn on_a_second(&self) -> bool {
        (self.next * self.decimation).is_multiple_of(self.rate)
    }

    /// Takes samples up to the segment switch. A device that splits groups
    /// switches as `twinleaf_device`'s segment ring does, at the next whole
    /// second of its inputs, after the last whole group before it.
    fn finish_second(&mut self) {
        if self.splits_groups {
            let at = (self.next * self.decimation).div_ceil(self.rate) * self.rate;
            while (self.next + 1) * self.decimation <= at {
                self.take(1, 1);
            }
        } else {
            while !self.on_a_second() {
                self.take(1, 1);
            }
        }
    }

    /// Opens a new segment from the current second on, after `change`.
    fn change(&mut self, change: Change) {
        if self.record_due {
            self.send_record();
        }
        self.start += match change {
            Change::Reboot => (self.next * self.decimation).div_ceil(self.rate) + 10,
            _ => (self.next * self.decimation).div_ceil(self.rate),
        };
        self.next = 0;
        self.changes.push((self.position(), change));
        self.since_change = 0;
        self.sent_since_change = 0;
        self.segmented_last = change != Change::Reboot;
        self.segment = match change {
            Change::Reboot => 0,
            _ => (self.segment + 1) % SEGMENTS,
        };
    }

    /// Returns `true` if `step` is one a device of `C` takes now. Every
    /// segment holds a sample or a dropped one, as a device rolls over once
    /// a second at most.
    fn allows<C: Case>(&self, step: &Step) -> bool {
        // Nothing comes between the last dropped sample and a new segment.
        let after_a_drop =
            C::DROPS == Drops::ApartFromSegments && self.dropped_last && self.on_a_second();
        let segment = self.since_change > 0 && !after_a_drop;
        let next = usize::from((self.segment + 1) % SEGMENTS);
        let stale = self.ring[next].is_some_and(|timeref| timeref != self.timeref);
        match step {
            Step::Samples { .. } | Step::Read { .. } => true,
            Step::Drop { .. } => match C::DROPS {
                Drops::Never => false,
                Drops::ApartFromSegments => !(self.segmented_last && self.sent_since_change == 0),
                Drops::Anywhere => true,
            },
            Step::Rollover { forced } => {
                C::ROLLS_OVER && segment && (!forced || !stale || C::STALE_RING)
            }
            Step::Rate { rate, decimation } => {
                let old = f64::from(self.rate) / f64::from(self.decimation);
                let new = f64::from(RATES[*rate]) / f64::from(*decimation);
                C::RECONFIGURES && segment && old != new
            }
            Step::TimeRef => C::RECONFIGURES && segment,
            Step::Reboot => C::RECONFIGURES && self.since_change > 0,
        }
    }
}

/// The device and reader of `C`, and the steps their cases are drawn from.
struct Pipeline<C>(PhantomData<C>);

impl<C: Case> ReferenceStateMachine for Pipeline<C> {
    type State = Device;
    type Transition = Step;

    fn init_state() -> BoxedStrategy<Device> {
        (C::ROOM, 0..RATES.len(), DECIMATIONS)
            .prop_map(|(room, rate, decimation)| {
                Device::new(room, C::SPLITS_GROUPS, RATES[rate], decimation)
            })
            .boxed()
    }

    fn transitions(_: &Device) -> BoxedStrategy<Step> {
        let samples = (1..=6u32, 1..=3u32).prop_map(|(count, rows)| Step::Samples { count, rows });
        let read = (1..=4usize).prop_map(|count| Step::Read { count });
        let drop = (1..=3u32).prop_map(|count| Step::Drop { count });
        let rollover = any::<bool>().prop_map(|forced| Step::Rollover { forced });
        let rate = (0..RATES.len(), DECIMATIONS)
            .prop_map(|(rate, decimation)| Step::Rate { rate, decimation });
        let mut steps = vec![(6, samples.boxed()), (4, read.boxed())];
        if C::DROPS != Drops::Never {
            steps.push((2, drop.boxed()));
        }
        if C::ROLLS_OVER {
            steps.push((3, rollover.boxed()));
        }
        if C::RECONFIGURES {
            steps.push((1, rate.boxed()));
            steps.push((1, Just(Step::TimeRef).boxed()));
            steps.push((1, Just(Step::Reboot).boxed()));
        }
        proptest::strategy::Union::new_weighted(steps).boxed()
    }

    fn preconditions(device: &Device, step: &Step) -> bool {
        device.allows::<C>(step)
    }

    fn apply(mut device: Device, step: &Step) -> Device {
        device.sent.clear();
        match *step {
            Step::Samples { count, rows } => device.take(count, rows),
            Step::Drop { count } => {
                (0..count).for_each(|_| device.advance());
                device.dropped_last = true;
            }
            Step::Rollover { forced } => {
                device.finish_second();
                device.change(Change::Rollover);
                device.record_due = true;
                if !forced {
                    device.send_record();
                }
            }
            Step::Rate { rate, decimation } => {
                device.finish_second();
                device.change(Change::Rate);
                (device.rate, device.decimation) = (RATES[rate], decimation);
                device.send_record();
            }
            Step::TimeRef => {
                device.finish_second();
                device.change(Change::TimeRef);
                device.timeref += 1;
                device.send_record();
            }
            Step::Reboot => {
                device.change(Change::Reboot);
                device.session += 1;
                device.describe();
            }
            Step::Read { .. } => {}
        }
        device
    }
}

/// A change a reader learns of, with what it names.
#[derive(Clone, Debug, PartialEq)]
enum Told {
    Initial,
    Reboot {
        old: u32,
        new: u32,
    },
    TimeRef {
        old: u32,
        new: u32,
    },
    Rate {
        old: f64,
        new: f64,
    },
    Rollover {
        old: u8,
        new: u8,
    },
    /// A boundary that names no change the device makes.
    Other(String),
}

/// A loss a reader learns of.
#[derive(Clone, Debug, PartialEq)]
enum Loss {
    /// The samples from `expected` up to `received` were lost.
    Samples { expected: u32, received: u32 },
    /// The lane dropped batches.
    Gap,
}

/// Returns the change and the loss that `batch` reports, after a gap if
/// `gap`.
fn reported(batch: &SampleBatch, gap: bool) -> (Option<Told>, Option<Loss>) {
    let mut loss = gap.then_some(Loss::Gap);
    let change = match batch.boundary() {
        None => None,
        Some(BoundaryReason::SamplesLost { expected, received }) => {
            loss.get_or_insert(Loss::Samples {
                expected: expected.value(),
                received: received.value(),
            });
            None
        }
        Some(BoundaryReason::Initial) => Some(Told::Initial),
        Some(BoundaryReason::SessionChanged { old, new }) => Some(Told::Reboot {
            old: old.value(),
            new: new.value(),
        }),
        Some(BoundaryReason::TimeRefSessionChanged { old, new }) => Some(Told::TimeRef {
            old: old.value(),
            new: new.value(),
        }),
        Some(&BoundaryReason::RateChanged { old_rate, new_rate }) => Some(Told::Rate {
            old: old_rate,
            new: new_rate,
        }),
        Some(BoundaryReason::SegmentRollover { old_id, new_id }) => Some(Told::Rollover {
            old: old_id.value(),
            new: new_id.value(),
        }),
        Some(other) => Some(Told::Other(format!("{other:?}"))),
    };
    (change, loss)
}

/// The parser, the lane and the reader under test.
struct Reader {
    parser: PacketParser,
    lane: Lane<SampleBatch>,
    queue: flume::Receiver<Queued<SampleBatch>>,
    /// The first position of each batch sent to the lane and not yet
    /// received or counted as dropped.
    sent: VecDeque<u64>,
    /// The position of the last sample the reader received.
    last: Option<u64>,
    /// The batches the lane's gaps counted since the last batch.
    gap: u64,
}

impl Reader {
    fn new(room: usize) -> Reader {
        let (lane, queue) = Lane::new(room);
        Reader {
            parser: PacketParser::new(false),
            lane,
            queue,
            sent: VecDeque::new(),
            last: None,
            gap: 0,
        }
    }

    /// Hands `packet` to the parser, and its batches to the lane, as the
    /// pump does.
    fn pump(&mut self, packet: &tio::Packet) {
        self.parser
            .push_packet(packet)
            .expect("a packet the parser takes");
        while let Some(batch) = self.parser.pop_batch() {
            self.sent.push_back(batch.columns()[0].to_f64()[0] as u64);
            assert!(self.lane.send(batch), "the reader is there");
        }
        assert!(self.lane.report_lag(), "the reader is there");
    }

    /// Takes up to `count` entries off the lane, and checks each batch
    /// against what `device` did.
    fn read(&mut self, count: usize, device: &Device) {
        for _ in 0..count {
            match self.queue.try_recv() {
                Ok(Queued::Item(batch)) => self.check(&batch, device),
                Ok(Queued::Gap(dropped)) => self.gap += dropped,
                Ok(Queued::Ended(_)) => panic!("the lane ended"),
                Err(_) => return,
            }
        }
    }

    /// Checks that `batch` and any gap ahead of it report what the device
    /// changed and lost since the last batch the reader received.
    fn check(&mut self, batch: &SampleBatch, device: &Device) {
        let positions = batch.columns()[0].to_f64();
        let first = positions[0] as u64;
        let last = positions[positions.len() - 1] as u64;
        assert_eq!(
            last - first + 1,
            batch.len() as u64,
            "a batch holds consecutive samples"
        );
        let dropped = self.sent.iter().take_while(|&&sent| sent < first).count();
        self.sent.drain(..=dropped);
        assert_eq!(
            self.gap, dropped as u64,
            "the gaps ahead of the batch at {first} count the batches dropped"
        );
        let (change, loss) = reported(batch, self.gap > 0);
        let now = device.history[first as usize];
        assert_eq!(
            batch.timestamps()[0],
            now.time,
            "the time of the sample at {first}"
        );
        let Some(previous) = self.last else {
            let due = if self.gap > 0 {
                (None, Some(Loss::Gap))
            } else {
                (Some(Told::Initial), None)
            };
            assert_eq!((change, loss), due, "the first batch, at position {first}");
            self.last = Some(last);
            self.gap = 0;
            return;
        };
        assert!(first > previous, "the batches came out of order");
        let lost = first - previous - 1;
        let was = device.history[previous as usize];
        let between = |kind: Change| {
            device
                .changes
                .iter()
                .any(|&(at, change)| change == kind && previous < at && at <= first)
        };
        let due = if between(Change::Reboot) {
            Some(Told::Reboot {
                old: was.session,
                new: now.session,
            })
        } else if between(Change::TimeRef) {
            Some(Told::TimeRef {
                old: was.timeref,
                new: now.timeref,
            })
        } else if between(Change::Rate) {
            Some(Told::Rate {
                old: was.rate,
                new: now.rate,
            })
        } else if between(Change::Rollover) {
            Some(Told::Rollover {
                old: was.segment,
                new: now.segment,
            })
        } else {
            None
        };
        let after = format!(
            "between positions {previous} and {first}, which lost {lost} samples, the batch after {} dropped batches reports {change:?} and {loss:?}",
            self.gap
        );
        assert_eq!(
            change, due,
            "the change {after}, of the changes {:?}",
            device.changes
        );
        let stands_for_loss = matches!(
            due,
            Some(Told::Reboot { .. } | Told::TimeRef { .. } | Told::Rate { .. })
        );
        if lost == 0 {
            assert_eq!(loss, None, "the loss {after}");
        } else if !stands_for_loss {
            let named = Loss::Samples {
                expected: was.number + 1,
                received: now.number,
            };
            assert!(
                loss == Some(Loss::Gap) || loss == Some(named),
                "the loss {after}"
            );
        }
        self.last = Some(last);
        self.gap = 0;
    }
}

/// A property of the boundaries a reader learns, with the device and the
/// reader of `C`.
struct Boundaries<C>(PhantomData<C>);

impl<C: Case> StateMachineTest for Boundaries<C> {
    type SystemUnderTest = Reader;
    type Reference = Pipeline<C>;

    fn init_test(device: &Device) -> Reader {
        let mut reader = Reader::new(device.room);
        for sent in &device.sent {
            reader.pump(&sent.packet());
        }
        reader
    }

    fn apply(mut reader: Reader, device: &Device, step: Step) -> Reader {
        for sent in &device.sent {
            reader.pump(&sent.packet());
        }
        if let Step::Read { count } = step {
            reader.read(count, device);
        }
        reader
    }

    fn teardown(mut reader: Reader, device: Device) {
        reader.read(usize::MAX, &device);
        assert!(reader.lane.report_lag(), "the reader is there");
        reader.read(usize::MAX, &device);
    }
}

prop_state_machine! {
    /// A reader that keeps up learns of every change and every lost sample,
    /// and of nothing else, when no sample is lost next to a new segment.
    #[test]
    fn a_reader_that_keeps_up_learns_every_change(sequential 1..40 => Boundaries<KeepsUp>);

    /// A reader that lags learns of every sample it lost, the device's and
    /// the lane's, and of nothing else, from a device that changes nothing.
    #[test]
    fn a_reader_that_lags_learns_every_loss(sequential 1..40 => Boundaries<Lags>);

    /// A reader that lags learns of every change in the batches its lane
    /// dropped.
    #[test]
    #[ignore = "fails until the boundary fix; see Problem A in data/boundaries.rs, after which only `reported` changes"]
    fn a_reader_that_lags_learns_every_change(sequential 1..40 => Boundaries<LagsAcrossChanges>);

    /// A loss across a rollover reaches the reader as both a loss and a
    /// rollover.
    #[test]
    #[ignore = "fails until the boundary fix; see Problem B in data/boundaries.rs, after which only `reported` changes"]
    fn a_loss_across_a_rollover_is_a_loss_and_a_rollover(sequential 1..40 => Boundaries<LosesAtRollover>);

    /// A forced rollover onto a slot of an older time reference reaches the
    /// reader as a rollover, stamped from the new segment's start.
    #[test]
    #[ignore = "fails until the parser evicts stale ring records of older time references; see Problem C in data/boundaries.rs (state.rs evicts a record only under the current time reference)"]
    fn a_forced_rollover_onto_an_older_time_reference_is_a_rollover(sequential 1..40 => Boundaries<RollsOntoAStaleRecord>);

    /// A segment switch that drops an incomplete group of inputs reaches the
    /// reader as a rollover, from the whole second the device switched at.
    #[test]
    #[ignore = "fails until validate_rows infers a forced rollover's start as twinleaf-device's segments.rs switches, at the next whole second of the inputs after its last sample rather than floor(L * decimation / rate), and detect_boundary takes the gap that switch leaves for a rollover; see Problem D in data/boundaries.rs"]
    fn a_switch_inside_a_group_is_a_rollover(sequential 1..40 => Boundaries<SplitsAGroup>);
}

//! State-machine properties of the boundaries a reader of batches learns.
//!
//! Each case simulates one device stream, which takes samples, rolls into
//! its next segment with or without sending the segment's record first,
//! changes its rate or decimation, takes a new time reference whose clock
//! starts before or after the old one's last sample, reboots, stops and
//! starts again, drops samples, loses its link for a while, and gives way
//! to another device at its route. It switches segments as
//! `twinleaf_device`'s segment ring does, at the next whole second of its
//! undecimated clock from wherever in a group of inputs it is asked to, and
//! drops the group of inputs that switch leaves incomplete, so a decimated
//! stream's last sample can end short of the new segment's start by less
//! than a period. When a period spans more than a second, several whole
//! seconds can leave the same last sample. A rollover without the record
//! sends the segment's record after its first packet, whose rows the parser
//! loses, as it decodes a segment's rows only with that segment's record.
//! After the first lap of its ring, such a rollover lands on a slot whose
//! record the parser holds from the lap before, and that record can be of
//! an older time reference. A stop announces `dev.running` turning off, and
//! the samples its publisher held of the stopped acquisition follow the
//! announcement. A start begins the segment after the stopped one at a
//! whole second, with or without sending the segment's record first as a
//! rollover does, and announces `dev.running` turning on before or after
//! the segment's first packet. Its packets go through the parser, and the
//! batches through a [`BatchLane`] as the pump sends them, to a reader
//! whose reads interleave with the device's steps. A lane with little room
//! drops batches, as for a slow app. Every sample carries its position in
//! the device's sequence, so the harness knows what the reader missed and
//! when each sample was taken.
//!
//! The first batch the reader receives says
//! [`Change::Initial`](crate::data::Change::Initial), and after a gap it also says
//! the loss, as the reader missed the start of the stream. Between two
//! batches the reader receives, it learns
//! of each change the device made there and of each sample it lost, and of
//! nothing else.
//!
//! - Another device in the place of the last comes as an `Initial`, and
//!   stands for whatever else changed and was lost before it.
//! - A reboot comes as a `SessionChanged` naming the sessions of the two
//!   batches, and stands for whatever else changed and was lost before it.
//! - Otherwise a new time reference comes as a `TimeRefSessionChanged`, and
//!   a rate change as a `RateChanged` naming both rates, each standing for
//!   what else changed and was lost before it.
//! - Otherwise a restart comes as a `Restarted` naming the segments of the
//!   two batches, and stands for what else was lost before it and for any
//!   rollover.
//!   Rows the parser decodes after its link came back, in another segment
//!   than its last rows before, come as a restart too, as a restart while
//!   the link was down may have gone unseen.
//! - Otherwise a rollover comes as a `SegmentRollover` naming the segments
//!   of the two batches, even when segments passed between them.
//! - A lost sample, the rows the parser lost waiting for a record among
//!   them, comes as a [`Loss`](crate::data::Loss). A change other than a
//!   rollover names a loss only of the batches the lane dropped. Next to a
//!   rollover or no change at all, it names the sample due, unless the lane
//!   dropped batches, so a loss across a rollover comes as both. A loss
//!   says it follows dropped batches exactly when the lane reported a gap
//!   ahead of it, and the gaps count each batch the lane dropped.
//! - Next to a rollover or no change at all, a loss names the sample due
//!   and counts the samples lost. Across segments that passed unseen it
//!   counts the whole output periods of their time, so it can exceed the
//!   samples lost by one for each of them whose time ends inside a group
//!   of inputs. Next to a bigger change, a loss names no sample due and
//!   counts no more than that.
//! - A batch with nothing of these before it carries no boundary.
//! - Each batch's first timestamp is the time the device took its sample.
//!
//! `reported` turns a batch and the gap ahead of it into the change and the
//! loss the harness compares with what the device did.
//! The parser's first rows after each outage are the first the device
//! sent after it whose segment record went first, and with the `hdf5`
//! feature each batch of the parser's goes to a new export table exactly
//! when it breaks the stream's run, or the stream starts over after
//! batches before.
//!
//! Four properties hold, each of a reader and a device that between them
//! reach part of what can happen. A reader that keeps up learns every
//! change of a device that drops no sample next to a new segment. A reader
//! that lags learns every loss of a device that changes nothing, and every
//! change of a device that drops nothing, as its lane carries what each
//! batch it drops changed onto the next batch it queues. A reader that
//! keeps up learns a loss across a rollover as both a loss and a rollover,
//! from a device that drops samples anywhere, rolls over with or without
//! the record and restarts. The properties catch faults in the parser's
//! rollovers and restarts, the ring records it keeps or evicts, its
//! boundaries and their payloads, the samples its losses count, its clock,
//! and the lane's gaps and the boundaries and losses it carries over.
//!
//! A lost link reaches the parser as a disconnect status. What the device
//! sends while its link is down reaches no one, and when the link is back
//! the device describes itself again. A device in the place of the last
//! has another serial and the same session number.
//!
//! A case announces every stop. A stop and start no host asked for, with
//! no refresh between them, announce nothing, and their packets are those
//! of a rollover after lost samples.
//! A segment that published nothing sends no record, so the ring can
//! come round to the slot the parser takes as current with no record
//! between. A start there sends its record first, as the parser has no
//! other sign that the slot moved on.

use crate::data::tests::fixtures;
use crate::data::{PacketParser, SampleBatch};
use crate::proto::data as wire;
use crate::proto::data::DataType;
use crate::proto::{DeviceRoute, SegmentId, SessionId};
use crate::session::subscription::{BatchLane, Queued};
use crate::tio;
use proptest::prelude::*;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::RangeInclusive;

/// The segments in the device's ring, the fewest a `twinleaf_device` ring holds.
const SEGMENTS: u8 = 3;

/// The rates the device samples at before decimation, in Hz.
const RATES: [u32; 4] = [1, 2, 4, 10];

/// The decimations the device applies.
const DECIMATIONS: RangeInclusive<u32> = 1..=3;

/// The seconds from where the old time reference puts a segment switch to
/// where the new one puts it. At the least of them the new clock starts
/// before the old one's last sample at every rate and decimation.
const SHIFTS: RangeInclusive<i32> = -7..=2;

/// The serials of the device and of those that take its place in turn.
const SERIALS: [&str; 3] = ["SN123", "SN456", "SN789"];

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
    Restart,
    Replace,
}

/// Where the device drops samples.
#[derive(Clone, Copy, PartialEq)]
enum Drops {
    Never,
    /// Never on either side of a new segment in one session.
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
    /// Whether the device stops acquiring and starts again.
    const RESTARTS: bool;
    const DROPS: Drops;
}

/// A reader that keeps up, and a device that changes everything and drops
/// samples away from its new segments.
struct KeepsUp;

impl Case for KeepsUp {
    const ROOM: RangeInclusive<usize> = ROOMY..=ROOMY;
    const ROLLS_OVER: bool = true;
    const RECONFIGURES: bool = true;
    const RESTARTS: bool = true;
    const DROPS: Drops = Drops::ApartFromSegments;
}

/// A reader that lags, and a device that changes nothing and drops samples.
struct Lags;

impl Case for Lags {
    const ROOM: RangeInclusive<usize> = 1..=3;
    const ROLLS_OVER: bool = false;
    const RECONFIGURES: bool = false;
    const RESTARTS: bool = false;
    const DROPS: Drops = Drops::Anywhere;
}

/// A reader that lags, and a device that changes everything and drops no
/// sample.
struct LagsAcrossChanges;

impl Case for LagsAcrossChanges {
    const ROOM: RangeInclusive<usize> = 1..=3;
    const ROLLS_OVER: bool = true;
    const RECONFIGURES: bool = true;
    const RESTARTS: bool = true;
    const DROPS: Drops = Drops::Never;
}

/// A reader that keeps up, and a device that rolls over, restarts and drops
/// samples anywhere, the first sample of a segment among them.
struct LosesAtRollover;

impl Case for LosesAtRollover {
    const ROOM: RangeInclusive<usize> = ROOMY..=ROOMY;
    const ROLLS_OVER: bool = true;
    const RECONFIGURES: bool = false;
    const RESTARTS: bool = true;
    const DROPS: Drops = Drops::Anywhere;
}

/// One step of a case.
#[derive(Clone, Debug)]
enum Step {
    /// The device takes `count` samples, and sends them in packets of at
    /// most `rows`.
    Samples { count: u32, rows: u32 },
    /// The device drops its next `count` samples.
    Drop { count: u32 },
    /// The device is asked `within` inputs into its next sample's group to
    /// roll into its next segment, takes samples to the switch, and rolls
    /// over. Unless `forced`, it sends the new segment's record first, and
    /// otherwise the record follows the segment's first packet.
    Rollover { forced: bool, within: u32 },
    /// The device is asked `within` inputs into its next sample's group to
    /// go on at `RATES[rate]` decimated by `decimation`, takes samples to
    /// the switch, and goes on so in its next segment.
    Rate {
        rate: usize,
        decimation: u32,
        within: u32,
    },
    /// The device is asked `within` inputs into its next sample's group to
    /// take a new time reference, which puts the switch `shift` seconds
    /// after where the old one puts it, takes samples to the switch, and
    /// goes on with it in its next segment.
    TimeRef { within: u32, shift: i32 },
    /// The device reboots into a new session, describes itself again and
    /// acquires. It announces `dev.running` turning on after its first
    /// packet.
    Reboot,
    /// A host stops the device, which announces `dev.running` off in that
    /// request, whether or not it announced it on, and the on it owed goes
    /// unannounced. Its publisher holds `staged` samples of the stopped
    /// acquisition, and sends them after the announcement.
    Stop { staged: u32 },
    /// The device starts acquiring again in a fresh segment, `pause` whole
    /// seconds after the second its stop fell in. Unless `forced`, it sends
    /// the segment's record first, and otherwise the record follows the
    /// segment's first packet, as for a rollover. It announces `dev.running`
    /// turning on ahead of the segment if `early`, and otherwise after the
    /// segment's first packet.
    Start {
        pause: u32,
        early: bool,
        forced: bool,
    },
    /// The link to the device goes down. The device drops its next `lost`
    /// samples, and if `opens`, rolls into its next segment or starts
    /// again if it was stopped, all unseen. The link comes back, and the
    /// device describes itself again.
    Outage { lost: u32, opens: bool },
    /// The link goes down, and another device with another serial and the
    /// same session number answers at the route, describes itself and
    /// acquires.
    Replace,
    /// The reader takes up to `count` entries off its lane.
    Read { count: usize },
}

/// A packet the device sends.
#[derive(Clone, Debug)]
enum Sent {
    Device {
        session: u32,
        serial: usize,
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
    /// The status a lost link reaches the parser as.
    Disconnect,
    /// `rows` samples of `segment` from sample `first`, the first of them at
    /// `position` in the device's sequence.
    Samples {
        segment: u8,
        first: u32,
        position: u64,
        rows: u32,
    },
    /// The announcement of `dev.running`, numbered `seq` in the device's
    /// settings version.
    Running {
        on: bool,
        seq: u32,
    },
}

impl Sent {
    fn packet(&self) -> tio::Packet {
        let record = match *self {
            Sent::Running { on, seq } => {
                let mut raw = [0u8; 64];
                let announcement = crate::proto::settings::Setting {
                    name: b"dev.running",
                    seq: Some(seq),
                    reply: &[u8::from(on)],
                };
                let len = announcement.write(&mut raw).expect("the announcement fits");
                let (packet, _) = tio::Packet::from_slice_prefix(&raw[..len]).expect("a packet");
                return packet.with_route(DeviceRoute::root());
            }
            Sent::Disconnect => {
                let lost = tio::Packet::link(tio::packet::LinkStatus::SensorDisconnected);
                return lost.with_route(DeviceRoute::root());
            }
            Sent::Device { session, serial } => wire::Metadata::Device(wire::Device {
                session: SessionId::new(session),
                serial: SERIALS[serial],
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
    /// The segment's place among those the device opened, from 0.
    ordinal: usize,
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
    session: u32,
    /// The serial of the device answering, in `SERIALS`.
    serial: usize,
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
    /// The segment of the last record or packet sent, which the parser
    /// takes as current.
    told: u8,
    /// Whether the device is acquiring.
    running: bool,
    /// Whether `dev.running` turning on waits for the current segment's
    /// first packet.
    running_due: bool,
    /// The device's settings version, which counts its announcements.
    settings: u32,
    /// Each change, at the position of the first sample after it.
    changes: Vec<(u64, Change)>,
    /// Whether the time of each segment the device left, in the order it
    /// opened them, ends inside a group of inputs.
    splits: Vec<bool>,
    /// Where the device was at each position, sent or dropped.
    history: Vec<At>,
    /// The outages whose first sample the parser can decode is still to
    /// come.
    resumes_due: usize,
    /// For each outage, the position of the first sample after it that the
    /// parser can decode, one that comes after its segment's record.
    resumes: Vec<u64>,
    /// What the last step sent.
    sent: Vec<Sent>,
}

impl Device {
    fn new(room: usize, rate: u32, decimation: u32) -> Device {
        let mut device = Device {
            room,
            session: 1,
            serial: 0,
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
            told: 0,
            running: true,
            running_due: false,
            settings: 0,
            changes: Vec::new(),
            splits: Vec::new(),
            history: Vec::new(),
            resumes_due: 0,
            resumes: Vec::new(),
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
        self.sent.extend([
            Sent::Device {
                session: self.session,
                serial: self.serial,
            },
            Sent::Stream,
        ]);
        self.send_record();
        self.sent.push(Sent::Column);
    }

    /// Sends the current segment's record.
    fn send_record(&mut self) {
        self.record_due = false;
        self.told = self.segment;
        self.sent.push(Sent::Segment {
            id: self.segment,
            start: self.start,
            rate: self.rate,
            decimation: self.decimation,
            timeref: self.timeref,
        });
    }

    /// Announces `dev.running` as `on`.
    fn announce_running(&mut self, on: bool) {
        self.running_due = false;
        self.settings += 1;
        self.sent.push(Sent::Running {
            on,
            seq: self.settings,
        });
    }

    /// Moves past the next sample, sent or dropped.
    fn advance(&mut self) {
        let period = f64::from(self.decimation) / f64::from(self.rate);
        self.history.push(At {
            session: self.session,
            timeref: self.timeref,
            segment: self.segment,
            ordinal: self.splits.len(),
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
            self.told = self.segment;
            if !self.record_due {
                let at = self.position();
                self.resumes.extend((0..self.resumes_due).map(|_| at));
                self.resumes_due = 0;
            }
            self.sent.push(Sent::Samples {
                segment: self.segment,
                first: self.next,
                position: self.position(),
                rows,
            });
            if self.record_due {
                self.send_record();
            }
            if self.running_due {
                self.announce_running(true);
            }
            (0..rows).for_each(|_| self.advance());
            self.sent_since_change += u64::from(rows);
            self.dropped_last = false;
            left -= rows;
        }
    }

    /// The input a segment switch asked for `within` inputs into the next
    /// sample's group lands on. As in `twinleaf_device`'s segment ring, it is
    /// the next whole second of the inputs from there.
    fn switch_at(&self, within: u32) -> u32 {
        (self.next * self.decimation + within).div_ceil(self.rate) * self.rate
    }

    /// The samples whose groups complete by a switch at input `at`.
    fn samples_to(&self, at: u32) -> u32 {
        at / self.decimation - self.next
    }

    /// Takes the samples whose groups complete by a switch at input `at`,
    /// and drops the inputs of the group that switch leaves incomplete.
    fn finish_segment(&mut self, at: u32) {
        (0..self.samples_to(at)).for_each(|_| self.take(1, 1));
    }

    /// Switches segments after `change`, asked for `within` inputs into the
    /// next sample's group. The switch is set once, as the ring sets it.
    fn switch(&mut self, change: Change, within: u32) {
        let at = self.switch_at(within);
        self.finish_segment(at);
        self.change(change, at / self.rate);
    }

    /// Opens a new segment `seconds` after the current one starts, after
    /// `change`.
    fn change(&mut self, change: Change, seconds: u32) {
        if self.record_due {
            self.send_record();
        }
        let inputs = seconds * self.rate;
        self.splits.push(!inputs.is_multiple_of(self.decimation));
        self.start += seconds;
        self.next = 0;
        self.changes.push((self.position(), change));
        self.since_change = 0;
        self.sent_since_change = 0;
        self.segmented_last = !matches!(change, Change::Reboot | Change::Replace);
        self.segment = match change {
            Change::Reboot | Change::Replace => 0,
            _ => self.next_segment(),
        };
    }

    /// The segment after the current one in the ring.
    fn next_segment(&self) -> u8 {
        (self.segment + 1) % SEGMENTS
    }

    /// Returns `true` if `step` is one a device of `C` takes now. Every
    /// segment holds a sample or a dropped one, as a device rolls over once
    /// a second at most. A stopped device only starts again or reboots. A
    /// stop comes whether or not the device announced that it runs. A start
    /// whose record follows its first packet does not land on the segment
    /// the parser takes as current, whose earlier record the parser would
    /// decode that packet with.
    fn allows<C: Case>(&self, step: &Step) -> bool {
        // Whether a switch asked for `within` inputs into a group comes with
        // nothing between the last dropped sample and the new segment.
        let after_a_drop =
            |within: u32| self.dropped_last && self.samples_to(self.switch_at(within)) == 0;
        let apart = C::DROPS == Drops::ApartFromSegments;
        let segment = |within: u32| {
            self.running
                && within < self.decimation
                && self.since_change > 0
                && !(apart && after_a_drop(within))
        };
        match *step {
            Step::Read { .. } => true,
            Step::Samples { .. } => self.running,
            Step::Drop { .. } => match C::DROPS {
                _ if !self.running => false,
                Drops::Never => false,
                Drops::ApartFromSegments => !(self.segmented_last && self.sent_since_change == 0),
                Drops::Anywhere => true,
            },
            Step::Stop { staged } => {
                C::RESTARTS
                    && self.running
                    && self.since_change > 0
                    && !(apart && self.dropped_last && staged == 0)
            }
            Step::Start { forced, .. } => {
                C::RESTARTS && !self.running && !(forced && self.next_segment() == self.told)
            }
            Step::Rollover { within, .. } => C::ROLLS_OVER && segment(within),
            Step::Rate {
                rate,
                decimation,
                within,
            } => {
                let old = f64::from(self.rate) / f64::from(self.decimation);
                let new = f64::from(RATES[rate]) / f64::from(decimation);
                C::RECONFIGURES && segment(within) && old != new
            }
            Step::TimeRef { within, .. } => C::RECONFIGURES && segment(within),
            Step::Reboot => C::RECONFIGURES && self.since_change > 0,
            // A device that drops samples only apart from its new segments
            // loses none to a rollover while its link is down.
            Step::Outage { lost, opens } => {
                let drops = lost == 0 || self.allows::<C>(&Step::Drop { count: lost });
                let rolls = match C::DROPS {
                    Drops::Anywhere => self.since_change + u64::from(lost) > 0,
                    _ => lost == 0 && segment(0) && self.samples_to(self.switch_at(0)) == 0,
                };
                let opens = !opens
                    || match self.running {
                        true => C::ROLLS_OVER && rolls,
                        false => C::RESTARTS,
                    };
                drops && opens
            }
            Step::Replace => {
                C::RECONFIGURES && self.since_change > 0 && self.serial + 1 < SERIALS.len()
            }
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
            .prop_map(|(room, rate, decimation)| Device::new(room, RATES[rate], decimation))
            .boxed()
    }

    /// Draws only the steps `device` takes now, so no case is rejected.
    fn transitions(device: &Device) -> BoxedStrategy<Step> {
        let samples = (1..=6u32, 1..=3u32).prop_map(|(count, rows)| Step::Samples { count, rows });
        let read = (1..=4usize).prop_map(|count| Step::Read { count });
        let mut steps = vec![(4, read.boxed())];
        if device.allows::<C>(&Step::Samples { count: 1, rows: 1 }) {
            steps.push((6, samples.boxed()));
        }
        if device.allows::<C>(&Step::Drop { count: 1 }) {
            let drop = (1..=3u32).prop_map(|count| Step::Drop { count });
            steps.push((2, drop.boxed()));
        }
        let within = 0..device.decimation;
        let rollovers = within
            .clone()
            .flat_map(|within| [false, true].map(|forced| Step::Rollover { forced, within }));
        let rates = (0..RATES.len()).flat_map(|rate| {
            DECIMATIONS.flat_map(move |decimation| {
                (0..device.decimation).map(move |within| Step::Rate {
                    rate,
                    decimation,
                    within,
                })
            })
        });
        let timerefs =
            within.flat_map(|within| SHIFTS.map(move |shift| Step::TimeRef { within, shift }));
        let stops = (0..=2).map(|staged| Step::Stop { staged });
        let starts = (0..=2).flat_map(|pause| {
            [(false, false), (false, true), (true, false), (true, true)].map(|(early, forced)| {
                Step::Start {
                    pause,
                    early,
                    forced,
                }
            })
        });
        let outages =
            (0..=2u32).flat_map(|lost| [false, true].map(|opens| Step::Outage { lost, opens }));
        let changes: [(u32, Vec<Step>); 8] = [
            (3, rollovers.collect()),
            (1, rates.collect()),
            (1, timerefs.collect()),
            (1, vec![Step::Reboot]),
            (1, stops.collect()),
            (3, starts.collect()),
            (2, outages.collect()),
            (1, vec![Step::Replace]),
        ];
        for (weight, candidates) in changes {
            let allowed: Vec<Step> = candidates
                .into_iter()
                .filter(|step| device.allows::<C>(step))
                .collect();
            if !allowed.is_empty() {
                steps.push((weight, proptest::sample::select(allowed).boxed()));
            }
        }
        proptest::strategy::Union::new_weighted(steps).boxed()
    }

    /// Keeps a shrunk case to the steps its device takes.
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
            Step::Rollover { forced, within } => {
                device.switch(Change::Rollover, within);
                device.record_due = true;
                if !forced {
                    device.send_record();
                }
            }
            Step::Rate {
                rate,
                decimation,
                within,
            } => {
                device.switch(Change::Rate, within);
                (device.rate, device.decimation) = (RATES[rate], decimation);
                device.send_record();
            }
            Step::TimeRef { within, shift } => {
                device.switch(Change::TimeRef, within);
                device.start = device.start.saturating_add_signed(shift);
                device.timeref += 1;
                device.send_record();
            }
            Step::Reboot => {
                let seconds = (device.next * device.decimation).div_ceil(device.rate) + 10;
                device.change(Change::Reboot, seconds);
                device.session += 1;
                device.describe();
                device.running = true;
                device.running_due = true;
            }
            Step::Stop { staged } => {
                device.running_due = false;
                device.take(staged, staged.max(1));
                let held = std::mem::take(&mut device.sent);
                device.running = false;
                // A segment that published nothing never sends its record.
                device.record_due = false;
                device.announce_running(false);
                device.sent.extend(held);
            }
            Step::Start {
                pause,
                early,
                forced,
            } => {
                let seconds = (device.next * device.decimation).div_ceil(device.rate) + pause;
                device.change(Change::Restart, seconds);
                device.running = true;
                device.running_due = true;
                if early {
                    device.announce_running(true);
                }
                device.record_due = true;
                if !forced {
                    device.send_record();
                }
            }
            Step::Outage { lost, opens } => {
                // What the device sends while its link is down reaches no
                // one, so every position it passes is lost, and none is
                // where an outage resumes.
                let from = device.position();
                let due = std::mem::take(&mut device.resumes_due);
                (0..lost).for_each(|_| device.advance());
                if opens && device.running {
                    device.switch(Change::Rollover, 0);
                } else if opens {
                    let seconds = (device.next * device.decimation).div_ceil(device.rate);
                    device.change(Change::Restart, seconds);
                    device.running = true;
                    device.running_due = true;
                }
                if device.position() > from {
                    device.dropped_last = true;
                }
                device.resumes_due = due + 1;
                device.sent = vec![Sent::Disconnect];
                device.describe();
            }
            Step::Replace => {
                let seconds = (device.next * device.decimation).div_ceil(device.rate) + 10;
                device.change(Change::Replace, seconds);
                device.serial += 1;
                device.running = true;
                device.running_due = true;
                device.resumes_due += 1;
                device.sent = vec![Sent::Disconnect];
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
    Restart {
        old: u8,
        new: u8,
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
struct Loss {
    /// The samples it counts.
    samples: u64,
    /// The sample number it names due.
    expected: Option<u32>,
}

/// Returns the change and the loss that `batch` reports, after a gap if
/// `gap`.
fn reported(batch: &SampleBatch, gap: bool) -> (Option<Told>, Option<Loss>) {
    use crate::data::{Change as Reported, Loss as Lost};
    let Some(boundary) = batch.boundary() else {
        assert!(!gap, "the batch after a gap names its loss");
        return (None, None);
    };
    let loss = boundary.loss.as_ref().map(|lost: &Lost| {
        assert!(lost.samples >= 1, "a loss of {lost:?} names no sample");
        assert_eq!(
            lost.dropped_batches > 0,
            gap,
            "a loss of {lost:?} says it follows dropped batches when a gap was reported"
        );
        assert!(
            gap || lost.expected.is_some(),
            "a loss of {lost:?} the parser found names the sample due"
        );
        Loss {
            samples: lost.samples,
            expected: lost.expected.map(|expected| expected.value()),
        }
    });
    assert!(
        loss.is_some() || !gap,
        "the batch after a gap names its loss"
    );
    let change = boundary.change.as_ref().map(|change| match change {
        Reported::Initial => Told::Initial,
        Reported::SessionChanged { old, new } => Told::Reboot {
            old: old.value(),
            new: new.value(),
        },
        Reported::TimeRefSessionChanged { old, new } => Told::TimeRef {
            old: old.value(),
            new: new.value(),
        },
        &Reported::RateChanged { old_rate, new_rate } => Told::Rate {
            old: old_rate,
            new: new_rate,
        },
        Reported::Restarted { old_id, new_id } => Told::Restart {
            old: old_id.value(),
            new: new_id.value(),
        },
        Reported::SegmentRollover { old_id, new_id } => Told::Rollover {
            old: old_id.value(),
            new: new_id.value(),
        },
        other => Told::Other(format!("{other:?}")),
    });
    (change, loss)
}

/// The parser, the lane and the reader under test.
struct Reader {
    parser: PacketParser,
    lane: BatchLane,
    queue: crate::chan::Receiver<Queued<SampleBatch>>,
    /// The first position of each batch sent to the lane and not yet
    /// received or counted as dropped.
    sent: VecDeque<u64>,
    /// The position of the last sample the reader received.
    last: Option<u64>,
    /// The batches the lane's gaps counted since the last batch.
    gap: u64,
    /// The position of the last sample the parser decoded.
    decoded: Option<u64>,
    /// For each time the link was lost, the position of the last sample the
    /// parser decoded before, and of the first it decoded after.
    outages: Vec<(Option<u64>, Option<u64>)>,
    /// The tables an export of the parser's batches puts them in, one table
    /// per stream run, one per device run and one per run of the whole
    /// parse.
    #[cfg(feature = "hdf5")]
    tables: [crate::data::export::RunTables; 3],
}

impl Reader {
    fn new(room: usize) -> Reader {
        #[cfg(feature = "hdf5")]
        use crate::data::export::{RunSplitLevel, RunTables, SplitPolicy};
        let (lane, queue) = BatchLane::new(room);
        Reader {
            parser: PacketParser::new(false),
            lane,
            queue,
            sent: VecDeque::new(),
            last: None,
            gap: 0,
            decoded: None,
            outages: Vec::new(),
            #[cfg(feature = "hdf5")]
            tables: [
                RunSplitLevel::PerStream,
                RunSplitLevel::PerDevice,
                RunSplitLevel::Global,
            ]
            .map(|level| RunTables::new(SplitPolicy::Continuous, level)),
        }
    }

    /// Hands `packet` to the parser, and its batches to the lane, as the
    /// pump does. The parser's first rows after each outage are the first
    /// the device sent after it that it can decode.
    fn pump(&mut self, packet: &tio::Packet, device: &Device) {
        if packet.link_status().is_some() {
            self.outages.push((self.decoded, None));
        }
        self.parser
            .push_packet(packet)
            .expect("a packet the parser takes");
        while let Some(batch) = self.parser.pop_batch() {
            let positions = batch.columns()[0].to_f64();
            let first = positions[0] as u64;
            for (outage, (_, after)) in self.outages.iter_mut().enumerate() {
                if after.is_none() {
                    let resumed = device.resumes.get(outage).copied();
                    assert_eq!(Some(first), resumed, "the first rows after outage {outage}");
                    *after = Some(first);
                }
            }
            #[cfg(feature = "hdf5")]
            self.split(&batch);
            self.decoded = Some(positions[positions.len() - 1] as u64);
            self.sent.push_back(first);
            assert!(self.lane.send(batch), "the reader is there");
        }
        assert!(self.lane.report_lag(), "the reader is there");
    }

    /// Checks that an export moves `batch` to a new table, at the level of
    /// its stream and of its device, exactly when its boundary breaks the
    /// stream's run, or the stream starts over after batches before.
    #[cfg(feature = "hdf5")]
    fn split(&mut self, batch: &SampleBatch) {
        let key = batch.stream_key();
        let breaks = batch
            .boundary()
            .is_some_and(|boundary| match boundary.is_initial() {
                true => self.decoded.is_some(),
                false => !boundary.is_continuous(),
            });
        for tables in &mut self.tables {
            let before = tables.index(key).expect("tables split by run");
            tables.observe(key, batch.generations(), batch.boundary());
            let after = tables.index(key).expect("tables split by run");
            assert_eq!(
                after - before,
                u64::from(breaks),
                "the table of the batch from sample {} after {:?}",
                batch.sample_numbers()[0].value(),
                batch.boundary()
            );
        }
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
        assert_eq!(
            batch.sample_numbers()[0].value(),
            now.number,
            "the number of the sample at {first}"
        );
        // The change the reader is due, the samples it lost, the segments
        // that passed unseen, and the sample number after its last batch.
        let (due, lost, unseen, next) = match self.last {
            None => (
                Some(Told::Initial),
                first,
                &device.splits[..now.ordinal],
                None,
            ),
            Some(previous) => {
                assert!(first > previous, "the batches came out of order");
                let was = device.history[previous as usize];
                let between = |kind: Change| {
                    device
                        .changes
                        .iter()
                        .any(|&(at, change)| change == kind && previous < at && at <= first)
                };
                // Whether the parser's first rows after a lost link are in
                // another segment than its last rows before it.
                let reopened = self.outages.iter().any(|&outage| match outage {
                    (Some(before), Some(after)) => {
                        previous < after
                            && after <= first
                            && device.history[before as usize].ordinal
                                != device.history[after as usize].ordinal
                    }
                    _ => false,
                });
                let due = if between(Change::Replace) {
                    Some(Told::Initial)
                } else if between(Change::Reboot) {
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
                } else if between(Change::Restart) || reopened {
                    Some(Told::Restart {
                        old: was.segment,
                        new: now.segment,
                    })
                } else if between(Change::Rollover) {
                    Some(Told::Rollover {
                        old: was.segment,
                        new: now.segment,
                    })
                } else {
                    None
                };
                let unseen = &device.splits[now.ordinal.min(was.ordinal + 1)..now.ordinal];
                (due, first - previous - 1, unseen, Some(was.number + 1))
            }
        };
        let split = unseen.iter().filter(|&&split| split).count() as u64;
        let since = self
            .last
            .map_or_else(|| "the start".to_string(), |at| format!("position {at}"));
        let after = format!(
            "between {since} and position {first}, which lost {lost} samples across {} unseen segments of which {split} end inside a group, the batch after {} dropped batches reports {change:?} and {loss:?}",
            unseen.len(),
            self.gap
        );
        assert_eq!(
            change, due,
            "the change {after}, of the changes {:?}",
            device.changes
        );
        // Every change but a rollover stands for the loss next to it.
        let stands_for_loss = due
            .as_ref()
            .is_some_and(|due| !matches!(due, Told::Rollover { .. }));
        if lost == 0 || stands_for_loss && self.gap == 0 {
            assert_eq!(loss, None, "the loss {after}");
        } else {
            let loss = loss.unwrap_or_else(|| panic!("the loss {after}"));
            let named = next.filter(|_| !stands_for_loss);
            assert_eq!(loss.expected, named, "the sample due {after}");
            let least = if stands_for_loss { 1 } else { lost };
            assert!(
                (least..=lost + split).contains(&loss.samples),
                "the samples counted {after}"
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
            reader.pump(&sent.packet(), device);
        }
        reader
    }

    fn apply(mut reader: Reader, device: &Device, step: Step) -> Reader {
        for sent in &device.sent {
            reader.pump(&sent.packet(), device);
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
    fn a_reader_that_lags_learns_every_change(sequential 1..40 => Boundaries<LagsAcrossChanges>);

    /// A loss across a rollover reaches the reader as both a loss and a
    /// rollover.
    #[test]
    fn a_loss_across_a_rollover_is_a_loss_and_a_rollover(sequential 1..40 => Boundaries<LosesAtRollover>);

}

/// Takes `steps` with the device and reader of `C`, checking each batch the
/// reader receives as the properties do.
fn run<C: Case>(rate: u32, decimation: u32, steps: &[Step]) {
    let mut device = Device::new(*C::ROOM.start(), rate, decimation);
    let mut reader = Boundaries::<C>::init_test(&device);
    for step in steps {
        assert!(
            device.allows::<C>(step),
            "{step:?} is a step the device takes"
        );
        device = Pipeline::<C>::apply(device, step);
        reader = Boundaries::<C>::apply(reader, &device, step.clone());
    }
    Boundaries::<C>::teardown(reader, device);
}

/// Acquisitions that publish nothing send no record, so the ring comes
/// round to the slot the parser takes as current with nothing between. The
/// record the next start sends there names another start, and its rows
/// come as a restart.
#[test]
fn a_start_that_comes_round_the_ring_to_the_current_slot_is_a_restart() {
    let empty = [
        Step::Stop { staged: 0 },
        Step::Start {
            pause: 0,
            early: true,
            forced: true,
        },
        Step::Drop { count: 1 },
    ];
    let mut steps = vec![Step::Samples { count: 1, rows: 1 }];
    for _ in 1..SEGMENTS {
        steps.extend(empty.clone());
    }
    steps.extend([
        Step::Stop { staged: 0 },
        Step::Start {
            pause: 0,
            early: true,
            forced: false,
        },
        Step::Samples { count: 1, rows: 1 },
        Step::Read { count: 2 },
    ]);
    run::<LosesAtRollover>(1, 1, &steps);
}

/// The ring comes round to the last rows' slot with no record between. A
/// forced rollover drops its first sample and stops before its record goes
/// out, a forced start publishes nothing before the link goes down, and the
/// device rolls into the last rows' slot while it is down and describes
/// that slot again. It drops that segment's first sample, so the rows after
/// the link is back follow on by number from the last rows. They are in
/// another segment after a lost link and a stop, and come as a restart.
#[test]
fn rows_in_the_last_slot_a_lap_later_after_a_lost_link_are_a_restart() {
    let steps = [
        Step::Samples { count: 1, rows: 1 },
        Step::Read { count: 1 },
        Step::Rollover {
            forced: true,
            within: 0,
        },
        Step::Drop { count: 1 },
        Step::Stop { staged: 0 },
        Step::Start {
            pause: 0,
            early: false,
            forced: true,
        },
        Step::Outage {
            lost: 1,
            opens: true,
        },
        Step::Drop { count: 1 },
        Step::Samples { count: 1, rows: 1 },
        Step::Read { count: 1 },
    ];
    run::<LosesAtRollover>(1, 1, &steps);
}

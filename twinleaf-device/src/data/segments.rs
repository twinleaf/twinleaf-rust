//! The segment ring: one entry per contiguous run of samples, and the rule
//! for when a parameter change takes effect.
//!
//! A change lands on a new segment at a whole second of the one issuing, so
//! every segment's sample zero sits at an integer second of its time
//! reference. Sample numbers restart there, which is also how a stream stays
//! clear of the 24-bit sample number a packet carries.

use core::num::NonZeroU32;

use heapless::String;
use twinleaf_proto::data::{self, FilterType, SegmentFlags};
use twinleaf_proto::sync::Epoch;
use twinleaf_proto::{SampleNumber, SegmentId, SessionId, StreamId};

use super::filter;

/// Input samples one segment issues before a rollover is forced, short of a
/// `u32` by more than any plausible rate.
const MAX_INPUT_SAMPLES: u32 = 4_000_000_000;

/// Output samples one segment issues before a rollover is forced, short of
/// the 24-bit sample number a packet carries.
const MAX_OUTPUT_SAMPLES: u32 = 16_000_000;

/// A start refused because the ring is still acquiring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Busy;

/// Where a segment's sample zero sits in time, and whose clock says so. A
/// synced device reports the serial of the device it takes time from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Timeref {
    /// Timescale of `start_time`.
    pub epoch: Epoch,
    /// Time of sample zero, in seconds after `epoch`.
    pub start_time: u32,
    /// Session of the timebase source.
    pub session: SessionId,
    /// Serial of the timebase source.
    pub serial: String<32>,
}

impl Timeref {
    /// A time reference, `None` if the serial does not fit.
    pub fn new(epoch: Epoch, start_time: u32, session: SessionId, serial: &str) -> Option<Self> {
        Some(Self {
            epoch,
            start_time,
            session,
            serial: serial.try_into().ok()?,
        })
    }
}

/// No time reference: what a segment holds until a start supplies one.
impl Default for Timeref {
    fn default() -> Self {
        Self {
            epoch: Epoch::INVALID,
            start_time: 0,
            session: SessionId::new(0),
            serial: String::new(),
        }
    }
}

/// What a segment acquires with.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Params {
    /// Input samples per second.
    pub rate: NonZeroU32,
    /// Input samples averaged into one output sample.
    pub decimation: NonZeroU32,
    /// Whether output samples are published.
    pub enabled: bool,
}

impl Params {
    /// Anti-alias corner in hertz, [`filter::CORNER`] of the output Nyquist
    /// frequency, and zero when nothing is filtered or decimated.
    pub fn cutoff(&self, kind: FilterType) -> f32 {
        match (kind, self.decimation.get()) {
            (FilterType::NONE, _) | (_, 1) => 0.0,
            (_, decimation) => filter::CORNER * 0.5 * (self.rate.get() / decimation) as f32,
        }
    }

    /// The filter a sample's float columns pass through.
    pub fn filter_type(&self, kind: FilterType) -> FilterType {
        if self.cutoff(kind) > 0.0 {
            kind
        } else {
            FilterType::NONE
        }
    }
}

/// Where one entry of the ring stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentState {
    /// No segment: never acquired, or retired without data.
    Invalid,
    /// Prepared to acquire, with no data published yet.
    Next,
    /// Publishing samples.
    Issuing,
    /// Its last sample is issued and the next segment takes over.
    Done,
    /// A past segment, its parameters still readable.
    Inactive,
}

/// One run of samples: what it acquires with, and how far it has got.
#[derive(Clone, Debug, PartialEq)]
pub struct Segment {
    id: SegmentId,
    state: SegmentState,
    params: Params,
    timeref: Timeref,
    holdover: bool,
    next_issue: u32,
    next_output: u32,
}

impl Segment {
    fn new(id: SegmentId, params: Params) -> Self {
        Self {
            id,
            state: SegmentState::Invalid,
            params,
            timeref: Timeref::default(),
            holdover: false,
            next_issue: 0,
            next_output: 0,
        }
    }

    /// Its id, which is also its place in the ring.
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// Where it stands.
    pub fn state(&self) -> SegmentState {
        self.state
    }

    /// What it acquires with.
    pub fn params(&self) -> &Params {
        &self.params
    }

    /// Where its sample zero sits in time.
    pub fn timeref(&self) -> &Timeref {
        &self.timeref
    }

    /// Its metadata record, as part of `stream_id`, filtered by `kind`.
    pub fn record(&self, stream_id: StreamId, kind: FilterType) -> data::Segment<'_> {
        data::Segment {
            stream_id,
            segment_id: self.id,
            flags: self.flags(),
            epoch: self.timeref.epoch,
            timeref_serial: &self.timeref.serial,
            timeref_session: self.timeref.session,
            start_time: self.timeref.start_time,
            sampling_rate: self.params.rate.get(),
            decimation: self.params.decimation.get(),
            filter_cutoff: self.params.cutoff(kind),
            filter_type: self.params.filter_type(kind),
        }
    }

    /// A segment is valid on the wire once it has published a sample, and
    /// active until the next segment takes over.
    fn flags(&self) -> SegmentFlags {
        let holdover = if self.holdover {
            SegmentFlags::HOLDOVER
        } else {
            SegmentFlags::default()
        };
        match self.state {
            SegmentState::Invalid | SegmentState::Next => SegmentFlags::default(),
            SegmentState::Issuing | SegmentState::Done => {
                SegmentFlags::VALID | SegmentFlags::ACTIVE | holdover
            }
            SegmentState::Inactive => SegmentFlags::VALID | holdover,
        }
    }
}

/// One issued sample, as the publisher needs to see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Issued {
    /// Segment the sample belongs to.
    pub segment: SegmentId,
    /// Its output sample number, `None` when nothing is published for it.
    pub output: Option<SampleNumber>,
    /// Whether its segment's record has to go out before it.
    pub first: bool,
    /// Whether it is the last sample of its segment.
    pub last: bool,
}

/// Which entry of the ring is issuing, and which takes over when.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Not acquiring; `next` holds what the next start acquires with.
    Stopped { next: u8 },
    /// Acquiring from `current`, with no change pending.
    Issuing { current: u8 },
    /// Acquiring from `from` until its sample `at`, then from `to`.
    Switching { from: u8, to: u8, at: u32 },
}

/// A stream's segments, and the sample numbers it issues from them.
pub struct Segments<const N: usize> {
    entries: [Segment; N],
    phase: Phase,
    published: u8,
    holdover: bool,
}

impl<const N: usize> Segments<N> {
    const RING: () = assert!(N >= 3 && N <= 7, "a segment ring holds 3 to 7 segments");

    /// A stopped ring whose next segment acquires with `params`.
    pub fn new(params: Params) -> Self {
        let () = Self::RING;
        let mut entries = core::array::from_fn(|id| Segment::new(SegmentId::new(id as u8), params));
        entries[0].state = SegmentState::Next;
        Self {
            entries,
            phase: Phase::Stopped { next: 0 },
            published: 0,
            holdover: false,
        }
    }

    /// Begin acquiring, with sample zero at `timeref`.
    pub fn start(&mut self, timeref: Timeref) -> Result<(), Busy> {
        let Phase::Stopped { next } = self.phase else {
            return Err(Busy);
        };
        let entry = &mut self.entries[usize::from(next)];
        entry.timeref = timeref;
        entry.holdover = self.holdover;
        self.phase = Phase::Issuing { current: next };
        Ok(())
    }

    /// Stop acquiring. The next start begins a fresh segment.
    pub fn stop(&mut self) {
        let next = match self.phase {
            Phase::Stopped { next } => next,
            Phase::Issuing { current } => {
                let issued = self.entries[usize::from(current)].next_issue;
                self.retire(current);
                self.open(current, issued)
            }
            Phase::Switching { from, to, .. } => {
                self.retire(from);
                to
            }
        };
        self.phase = Phase::Stopped { next };
    }

    /// Mark every segment opened from here on as begun while the time
    /// reference's pulses were absent.
    pub fn set_holdover(&mut self, holdover: bool) {
        self.holdover = holdover;
    }

    /// Acquire with `params` from the next segment on.
    pub fn retune(&mut self, params: Params) {
        self.next().params = params;
    }

    /// Begin a new segment with the same parameters, restarting its sample
    /// numbers at zero.
    pub fn rollover(&mut self) {
        self.next();
    }

    /// Name second `from` of the current time reference `to` from the next
    /// segment on, wherever that segment begins.
    pub fn relabel(&mut self, from: u32, to: Timeref) {
        let next = self.next();
        next.timeref = Timeref {
            start_time: to
                .start_time
                .wrapping_add(next.timeref.start_time.wrapping_sub(from)),
            ..to
        };
    }

    /// Issue the next sample number, `None` while stopped.
    pub fn issue(&mut self) -> Option<Issued> {
        self.advance(true)
    }

    /// Advance past `count` samples without issuing them: the one gap a
    /// segment's sample numbers may have.
    pub fn skip(&mut self, count: u32) {
        (0..count).for_each(|_| {
            self.advance(false);
        });
    }

    /// What a host asks about: the segment whose first sample was published
    /// last, which is the one its samples arrive on.
    pub fn current(&self) -> &Segment {
        &self.entries[usize::from(self.published)]
    }

    /// The segment `id` names, `None` if the ring holds none.
    pub fn get(&self, id: SegmentId) -> Option<&Segment> {
        self.entries
            .get(usize::from(id.value()))
            .filter(|segment| segment.state != SegmentState::Invalid)
    }

    fn advance(&mut self, publishing: bool) -> Option<Issued> {
        let current = self.switched()?;
        let entry = &mut self.entries[usize::from(current)];
        let number = entry.next_issue;
        entry.next_issue += 1;
        let kept = (number + 1).is_multiple_of(entry.params.decimation.get());
        let output = kept.then(|| {
            let output = entry.next_output;
            entry.next_output += 1;
            output
        });
        let published = output
            .filter(|_| publishing && entry.params.enabled)
            .map(SampleNumber::new);
        let first = published.is_some() && entry.state == SegmentState::Next;
        if first {
            entry.state = SegmentState::Issuing;
            self.published = current;
        }
        let (issued, generated) = (entry.next_issue, entry.next_output);
        match self.phase {
            Phase::Issuing { .. }
                if issued >= MAX_INPUT_SAMPLES || generated >= MAX_OUTPUT_SAMPLES =>
            {
                self.rollover()
            }
            Phase::Stopped { .. } | Phase::Issuing { .. } | Phase::Switching { .. } => {}
        }
        let last = match self.phase {
            Phase::Switching { at, .. } => at == issued,
            Phase::Stopped { .. } | Phase::Issuing { .. } => false,
        };
        let entry = &mut self.entries[usize::from(current)];
        if last && entry.state == SegmentState::Issuing {
            entry.state = SegmentState::Done;
        }
        Some(Issued {
            segment: entry.id,
            output: published,
            first,
            last,
        })
    }

    /// The entry issuing now, taking a scheduled switch once it is due.
    fn switched(&mut self) -> Option<u8> {
        match self.phase {
            Phase::Stopped { .. } => None,
            Phase::Issuing { current } => Some(current),
            Phase::Switching { from, to, at }
                if self.entries[usize::from(from)].next_issue == at =>
            {
                self.retire(from);
                self.phase = Phase::Issuing { current: to };
                Some(to)
            }
            Phase::Switching { from, .. } => Some(from),
        }
    }

    /// The entry a parameter change lands on, scheduling a switch onto a new
    /// one when the segment issuing has already had samples.
    fn next(&mut self) -> &mut Segment {
        let index = match self.phase {
            Phase::Stopped { next } => next,
            Phase::Switching { to, .. } => to,
            Phase::Issuing { current } if self.entries[usize::from(current)].next_issue == 0 => {
                current
            }
            Phase::Issuing { current } => {
                let entry = &self.entries[usize::from(current)];
                let rate = entry.params.rate.get();
                let at = entry.next_issue.div_ceil(rate) * rate;
                let to = self.open(current, at);
                self.phase = Phase::Switching {
                    from: current,
                    to,
                    at,
                };
                to
            }
        };
        let entry = &mut self.entries[usize::from(index)];
        entry.holdover = self.holdover;
        entry
    }

    /// Prepare the entry after `from` to take over at its sample `at`, which
    /// is a whole number of seconds into it.
    fn open(&mut self, from: u8, at: u32) -> u8 {
        let source = &self.entries[usize::from(from)];
        let params = source.params;
        let mut timeref = source.timeref.clone();
        timeref.start_time = timeref.start_time.wrapping_add(at / params.rate.get());
        let to = (from + 1) % (N as u8);
        let entry = &mut self.entries[usize::from(to)];
        entry.state = SegmentState::Next;
        entry.params = params;
        entry.timeref = timeref;
        entry.holdover = self.holdover;
        entry.next_issue = 0;
        entry.next_output = 0;
        to
    }

    /// A segment stops issuing: it keeps its data, or it never had any.
    fn retire(&mut self, index: u8) {
        let entry = &mut self.entries[usize::from(index)];
        entry.state = match entry.state {
            SegmentState::Invalid | SegmentState::Next => SegmentState::Invalid,
            SegmentState::Issuing | SegmentState::Done | SegmentState::Inactive => {
                SegmentState::Inactive
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(rate: u32) -> Params {
        Params {
            rate: NonZeroU32::new(rate).unwrap(),
            decimation: NonZeroU32::MIN,
            enabled: true,
        }
    }

    fn id(value: u8) -> SegmentId {
        SegmentId::new(value)
    }

    fn timeref(start_time: u32) -> Timeref {
        Timeref::new(Epoch::UNIX, start_time, SessionId::new(3), "S1").unwrap()
    }

    fn started<const N: usize>(params: Params) -> Segments<N> {
        let mut segments = Segments::new(params);
        segments.start(timeref(1000)).unwrap();
        segments
    }

    fn issue<const N: usize>(segments: &mut Segments<N>, count: u32) -> Vec<Issued> {
        (0..count).filter_map(|_| segments.issue()).collect()
    }

    #[test]
    fn a_stopped_ring_issues_nothing_and_an_acquiring_one_refuses_a_start() {
        let mut segments: Segments<4> = Segments::new(params(10));
        assert_eq!(segments.issue(), None);
        assert_eq!(segments.start(timeref(1000)), Ok(()));
        assert_eq!(segments.start(timeref(2000)), Err(Busy));
        assert!(segments.issue().is_some());
        segments.stop();
        assert_eq!(segments.issue(), None);
        assert_eq!(segments.start(timeref(2000)), Ok(()));
    }

    #[test]
    fn ids_advance_by_one_modulo_the_ring() {
        let mut segments: Segments<3> = started(params(10));
        let ids: Vec<u8> = (0..5)
            .map(|_| {
                let id = issue(&mut segments, 10).last().unwrap().segment.value();
                segments.rollover();
                id
            })
            .collect();
        assert_eq!(ids, [0, 1, 2, 0, 1]);
    }

    #[test]
    fn a_switch_lands_on_a_whole_second_of_the_outgoing_segment() {
        let mut segments: Segments<4> = started(params(10));
        issue(&mut segments, 13);
        segments.retune(Params {
            decimation: NonZeroU32::new(2).unwrap(),
            ..params(10)
        });

        let pending = segments.get(id(1)).unwrap();
        assert_eq!(pending.state(), SegmentState::Next);
        assert_eq!(pending.timeref().start_time, 1002);
        assert_eq!(segments.current(), segments.get(id(0)).unwrap());
        assert_eq!(
            segments.current().flags(),
            SegmentFlags::VALID | SegmentFlags::ACTIVE
        );
        assert_eq!(segments.get(id(0)).unwrap().timeref().start_time, 1000);

        let issued = issue(&mut segments, 8);
        let ids: Vec<u8> = issued.iter().map(|issued| issued.segment.value()).collect();
        assert_eq!(ids, [0, 0, 0, 0, 0, 0, 0, 1]);
        assert!(issued[6].last);
        assert_eq!(issued[6].output, Some(SampleNumber::new(19)));
    }

    #[test]
    fn sample_numbers_restart_in_a_new_segment() {
        let mut segments: Segments<4> = started(params(10));
        issue(&mut segments, 25);
        segments.rollover();
        let issued = issue(&mut segments, 6);
        assert_eq!(issued[4].segment.value(), 0);
        assert_eq!(issued[4].output, Some(SampleNumber::new(29)));
        assert_eq!(issued[5].segment.value(), 1);
        assert_eq!(issued[5].output, Some(SampleNumber::new(0)));
        assert!(issued[5].first);
        assert_eq!(segments.current().timeref().start_time, 1003);
    }

    #[test]
    fn a_retune_never_changes_the_segment_issuing() {
        let mut segments: Segments<4> = started(params(10));
        issue(&mut segments, 5);
        segments.retune(params(100));

        assert_eq!(segments.get(id(0)).unwrap().params().rate.get(), 10);
        assert_eq!(segments.get(id(1)).unwrap().params().rate.get(), 100);
        let issued = issue(&mut segments, 5);
        assert!(issued.iter().all(|issued| issued.segment.value() == 0));
        assert_eq!(segments.get(id(0)).unwrap().params().rate.get(), 10);
    }

    #[test]
    fn a_retune_while_stopped_edits_the_segment_in_place() {
        let mut segments: Segments<4> = Segments::new(params(10));
        segments.retune(params(50));
        assert_eq!(segments.current().id().value(), 0);
        assert_eq!(segments.current().params().rate.get(), 50);

        segments.start(timeref(1000)).unwrap();
        segments.retune(params(20));
        assert_eq!(segments.current().id().value(), 0);
        assert_eq!(segments.issue().unwrap().segment.value(), 0);
        assert_eq!(segments.get(id(0)).unwrap().params().rate.get(), 20);
    }

    #[test]
    fn decimation_numbers_output_samples_apart_from_input_samples() {
        let mut segments: Segments<4> = started(Params {
            decimation: NonZeroU32::new(4).unwrap(),
            ..params(8)
        });
        let outputs: Vec<Option<u32>> = issue(&mut segments, 8)
            .iter()
            .map(|issued| issued.output.map(SampleNumber::value))
            .collect();
        assert_eq!(
            outputs,
            [None, None, None, Some(0), None, None, None, Some(1)]
        );
        assert_eq!(segments.get(id(0)).unwrap().next_issue, 8);
        assert_eq!(segments.get(id(0)).unwrap().next_output, 2);
    }

    #[test]
    fn a_rollover_is_forced_before_the_wire_limit() {
        let mut segments: Segments<4> = started(params(1000));
        (0..MAX_OUTPUT_SAMPLES).for_each(|_| {
            segments.issue();
        });
        assert_eq!(segments.get(id(1)).unwrap().state(), SegmentState::Next);

        let issued = segments.issue().unwrap();
        assert_eq!(issued.segment.value(), 1);
        assert_eq!(issued.output, Some(SampleNumber::new(0)));
        const { assert!(MAX_OUTPUT_SAMPLES < SampleNumber::MAX) };
        assert_eq!(
            segments.current().timeref().start_time,
            1000 + MAX_OUTPUT_SAMPLES / 1000
        );
    }

    #[test]
    fn skipping_is_the_only_gap_in_a_segment() {
        let mut segments: Segments<4> = started(params(10));
        issue(&mut segments, 3);
        segments.skip(2);
        let outputs: Vec<u32> = issue(&mut segments, 2)
            .iter()
            .filter_map(|issued| issued.output.map(SampleNumber::value))
            .collect();
        assert_eq!(outputs, [5, 6]);
    }

    #[test]
    fn a_segment_whose_first_samples_are_skipped_still_gets_a_record() {
        let mut segments: Segments<4> = started(params(10));
        segments.skip(1);
        let issued = segments.issue().unwrap();
        assert_eq!(issued.output, Some(SampleNumber::new(1)));
        assert!(issued.first);
        assert!(!segments.issue().unwrap().first);
    }

    #[test]
    fn a_disabled_segment_numbers_samples_without_publishing_them() {
        let mut segments: Segments<4> = started(Params {
            enabled: false,
            ..params(10)
        });
        assert!(issue(&mut segments, 3)
            .iter()
            .all(|issued| issued.output.is_none()));

        segments.retune(params(10));
        issue(&mut segments, 7);
        let issued = segments.issue().unwrap();
        assert_eq!(issued.segment.value(), 1);
        assert_eq!(issued.output, Some(SampleNumber::new(0)));
        assert!(issued.first);
    }

    #[test]
    fn a_segment_reports_where_it_stands() {
        let mut segments: Segments<4> = Segments::new(params(10));
        assert_eq!(segments.current().state(), SegmentState::Next);
        assert_eq!(segments.current().flags(), SegmentFlags::default());
        assert_eq!(segments.get(id(1)), None);
        assert_eq!(segments.get(id(4)), None);

        segments.start(timeref(1000)).unwrap();
        issue(&mut segments, 1);
        assert_eq!(segments.current().state(), SegmentState::Issuing);
        assert_eq!(
            segments.current().flags(),
            SegmentFlags::VALID | SegmentFlags::ACTIVE
        );

        segments.rollover();
        issue(&mut segments, 10);
        assert_eq!(segments.get(id(0)).unwrap().state(), SegmentState::Inactive);
        assert_eq!(segments.get(id(0)).unwrap().flags(), SegmentFlags::VALID);
        assert_eq!(segments.get(id(1)).unwrap().state(), SegmentState::Issuing);
    }

    /// D8: a segment begun while the pulses were absent says so, and the one
    /// the runtime rolls over to when they come back does not.
    #[test]
    fn a_segment_opened_in_holdover_carries_the_flag() {
        let mut segments: Segments<4> = Segments::new(params(10));
        segments.set_holdover(true);
        segments.start(timeref(1000)).unwrap();
        issue(&mut segments, 1);
        assert_eq!(
            segments.current().flags(),
            SegmentFlags::VALID | SegmentFlags::ACTIVE | SegmentFlags::HOLDOVER
        );

        segments.set_holdover(false);
        segments.rollover();
        issue(&mut segments, 10);
        assert_eq!(segments.current().id().value(), 1);
        assert_eq!(
            segments.current().flags(),
            SegmentFlags::VALID | SegmentFlags::ACTIVE
        );
        assert_eq!(
            segments.get(id(0)).unwrap().flags(),
            SegmentFlags::VALID | SegmentFlags::HOLDOVER
        );
    }

    #[test]
    fn holdover_set_before_a_segment_issues_reaches_it_at_the_rollover() {
        let mut segments: Segments<4> = started(params(10));
        segments.set_holdover(true);
        segments.rollover();
        issue(&mut segments, 1);
        assert_eq!(segments.current().id().value(), 0);
        assert_eq!(
            segments.current().flags(),
            SegmentFlags::VALID | SegmentFlags::ACTIVE | SegmentFlags::HOLDOVER
        );
    }

    /// Two streams switch on different seconds yet agree on every edge.
    #[test]
    fn a_relabel_offsets_each_stream_from_its_own_switch() {
        let gps = Timeref::new(Epoch::UNIX, 1_700_000_000, SessionId::new(9), "GPS").unwrap();
        let switched = |rate: u32, samples: u32| {
            let mut segments: Segments<4> = started(params(rate));
            issue(&mut segments, samples);
            segments.relabel(1002, gps.clone());
            let issued = issue(&mut segments, 3 * rate);
            let first = issued.iter().position(|issued| issued.first).unwrap() as u32;
            (samples + first, segments.current().timeref().clone())
        };

        let (slow_at, slow) = switched(10, 25);
        let (fast_at, fast) = switched(1000, 1001);
        assert_eq!((slow_at, fast_at), (30, 2000));
        assert_eq!(slow.start_time, 1_700_000_001);
        assert_eq!(fast.start_time, 1_700_000_000);
        assert_eq!(
            Timeref {
                start_time: 0,
                ..slow
            },
            Timeref {
                start_time: 0,
                ..gps
            }
        );
    }

    #[test]
    fn a_relabel_before_any_sample_renames_the_segment_in_place() {
        let mut segments: Segments<4> = started(params(10));
        segments.relabel(1000, timeref(5000));
        let issued = segments.issue().unwrap();
        assert_eq!(issued.segment.value(), 0);
        assert_eq!(segments.current().timeref().start_time, 5000);
    }

    #[test]
    fn a_stop_retires_the_segment_and_a_start_takes_the_next() {
        let mut segments: Segments<4> = started(params(10));
        issue(&mut segments, 5);
        segments.stop();
        assert_eq!(segments.get(id(0)).unwrap().state(), SegmentState::Inactive);
        assert_eq!(segments.current(), segments.get(id(0)).unwrap());
        assert_eq!(segments.get(id(1)).unwrap().state(), SegmentState::Next);

        segments.start(timeref(2000)).unwrap();
        let issued = segments.issue().unwrap();
        assert_eq!(issued.segment.value(), 1);
        assert_eq!(issued.output, Some(SampleNumber::new(0)));
        assert_eq!(segments.current().timeref().start_time, 2000);
    }

    #[test]
    fn a_record_describes_the_segment_for_its_stream() {
        let mut segments: Segments<4> = started(Params {
            decimation: NonZeroU32::new(2).unwrap(),
            ..params(10)
        });
        issue(&mut segments, 2);
        let record = segments
            .current()
            .record(StreamId::new(2), FilterType::IIR_BW_LPF4);
        assert_eq!(record.stream_id, StreamId::new(2));
        assert_eq!(record.segment_id, SegmentId::new(0));
        assert_eq!(record.flags, SegmentFlags::VALID | SegmentFlags::ACTIVE);
        assert_eq!(record.epoch, Epoch::UNIX);
        assert_eq!(record.timeref_serial, "S1");
        assert_eq!(record.timeref_session, SessionId::new(3));
        assert_eq!(record.start_time, 1000);
        assert_eq!(record.sampling_rate, 10);
        assert_eq!(record.decimation, 2);
        assert_eq!(record.filter_cutoff, 2.0);
        assert_eq!(record.filter_type, FilterType::IIR_BW_LPF4);
    }

    #[test]
    fn random_operations_keep_every_invariant() {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let rates = [5u32, 10, 25];
        let mut segments: Segments<4> = started(params(10));
        let mut previous: Option<u8> = None;
        let mut ended = false;
        let mut switches = 0;

        for _ in 0..20_000 {
            let before = issuing(&segments);
            match next() % 8 {
                0 => segments.retune(params(rates[(next() % 3) as usize])),
                1 => segments.rollover(),
                2 => segments.skip(1 + (next() % 3) as u32),
                3..=7 => {
                    let sample = segments.issue().unwrap();
                    let entry = segments.get(sample.segment).unwrap();
                    let output = sample.output.unwrap().value();
                    assert_eq!(output, entry.next_issue - 1);
                    assert_eq!(segments.current().id(), sample.segment);
                    assert!(output <= SampleNumber::MAX);
                    match previous {
                        Some(id) if id == sample.segment.value() => assert!(!ended),
                        Some(_) => assert!(sample.first || output > 0),
                        None => assert_eq!(output, 0),
                    }
                    previous = Some(sample.segment.value());
                    ended = sample.last;
                }
                8..=u64::MAX => unreachable!("the modulo bounds the case"),
            }
            let after = issuing(&segments);
            if after != before {
                let left = &segments.entries[usize::from(before)];
                let entered = &segments.entries[usize::from(after)];
                let rate = left.params.rate.get();
                assert_eq!(after, (before + 1) % 4);
                assert!(left.next_issue.is_multiple_of(rate));
                assert_eq!(
                    entered.timeref.start_time,
                    left.timeref.start_time + left.next_issue / rate
                );
                switches += 1;
            }
        }
        assert!(switches > 10);
    }

    /// Which entry is issuing samples right now.
    fn issuing<const N: usize>(segments: &Segments<N>) -> u8 {
        match segments.phase {
            Phase::Stopped { next } => next,
            Phase::Issuing { current } => current,
            Phase::Switching { from, .. } => from,
        }
    }
}

//! Electrical PPS qualification and phase measurement.
//!
//! A main timebase that rejects spurs and measures phase, plus a tentative
//! timebase that takes over when the main train disappears.

use super::CounterDomain;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Counter ticks in `ns` nanoseconds of a timer running at `timer_hz`.
pub const fn ticks(ns: u32, timer_hz: u32) -> u32 {
    (ns as u64 * timer_hz as u64 / NANOS_PER_SECOND) as u32
}

/// PPS tracker configuration.
#[derive(Debug, Clone, Copy)]
pub struct PulseConfig {
    /// Largest period error accepted as part of one pulse train.
    pub edge_tolerance_ticks: u32,
    /// Grace after the expected second before declaring a pulse missing.
    pub miss_grace_ns: u32,
    /// Consecutive clean pulses required to report lock.
    pub lock_threshold: u8,
    /// Misses tolerated before considering a tentative alternate pulse train.
    pub missed_pulse_threshold: u8,
    /// Clean pulses required on the alternate train before switching to it.
    pub switchover_threshold: u8,
}

impl PulseConfig {
    /// The edge tolerance is the one board-dependent threshold: it follows the
    /// counter frequency and the oscillator specification.
    pub const fn with_edge_tolerance(edge_tolerance_ticks: u32) -> Self {
        Self {
            edge_tolerance_ticks,
            miss_grace_ns: 10_000_000,
            lock_threshold: 3,
            missed_pulse_threshold: 4,
            switchover_threshold: 4,
        }
    }
}

/// Qualification state of the electrical pulse train.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PulseState {
    /// No external pulse train has been established.
    FreeRunning,
    /// A plausible pulse train exists but has not met the lock threshold.
    Acquiring {
        /// Consecutive clean pulses so far.
        good_pulses: u8,
    },
    /// Enough consecutive clean pulses have been observed.
    Locked,
    /// A previously established pulse train is currently absent.
    Holdover {
        /// Pulses expected and not seen, saturating at 255.
        missed_pulses: u8,
    },
}

impl PulseState {
    /// Whether the train is qualified.
    pub const fn is_locked(self) -> bool {
        matches!(self, Self::Locked)
    }
}

/// What the pulse tracker observed for one second of the main pulse train.
///
/// Exactly one per nominal second, with deliberately no elapsed-seconds count:
/// see [`Synchronizer`](super::Synchronizer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SecondEvent {
    /// An accepted main-train edge closed this second.
    Captured {
        /// Seconds the monotonic gap implies that no prior
        /// [`Synchronizer::poll`](super::Synchronizer::poll) has accounted for.
        /// Nonzero is a divergence, and the capture is not anchored on.
        unaccounted: i64,
    },
    /// The expected pulse did not arrive.
    Missed,
    /// A coherent alternate train replaced the missing main train.
    Switched,
}

/// Where one pulse train is in its life cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// No pulse has ever qualified: the train carries no phase and no counts.
    Empty,
    /// One accepted pulse, so no period has been measured against it yet.
    Fresh,
    /// Consecutive accepted pulses, each closing one second.
    Tracking,
    /// A qualified train whose pulses are currently absent.
    Lapsed { missed: u32 },
}

#[derive(Debug, Clone, Copy)]
struct Train {
    mode: Mode,
    /// Displacement since this train's last accepted pulse, whole seconds plus
    /// a residual under half a period. It accumulates across rejected spurs.
    pending_seconds: u32,
    pending_delta: i64,
    spurious_count: u32,
    good_pulse_count: u32,
    last_delta: i64,
    phase_error: i64,
    last_edge: u32,
    miss_timeout_ns: u64,
}

impl Train {
    const fn new() -> Self {
        Self {
            mode: Mode::Empty,
            pending_seconds: 0,
            pending_delta: 0,
            spurious_count: 0,
            good_pulse_count: 0,
            last_delta: 0,
            phase_error: 0,
            last_edge: 0,
            miss_timeout_ns: 0,
        }
    }

    const fn is_valid(&self) -> bool {
        !matches!(self.mode, Mode::Empty)
    }

    /// Returns the seconds this pulse accounts for beyond the one it closes,
    /// or `None` if it was rejected as spurious.
    fn process_pulse(
        &mut self,
        domain: CounterDomain,
        config: PulseConfig,
        delta: i64,
        seconds: u32,
        pulse_edge: u32,
        pulse_time_ns: u64,
    ) -> Option<i64> {
        if !self.is_valid() {
            *self = Self::new();
        } else {
            self.pending_seconds = self.pending_seconds.saturating_add(seconds);
            self.pending_delta = self.pending_delta.saturating_add(delta);

            let period = domain.period() as i64;
            let half = period / 2;
            if self.pending_delta >= half {
                self.pending_seconds = self.pending_seconds.saturating_add(1);
                self.pending_delta -= period;
            } else if self.pending_delta < -half {
                self.pending_seconds = self.pending_seconds.saturating_sub(1);
                self.pending_delta += period;
            }

            if self.pending_seconds == 0
                || self.pending_delta < -(config.edge_tolerance_ticks as i64)
                || self.pending_delta > config.edge_tolerance_ticks as i64
            {
                self.spurious_count = self.spurious_count.saturating_add(1);
                return None;
            }
        }

        let accounted = match self.mode {
            Mode::Empty => 0,
            Mode::Fresh | Mode::Tracking => 1,
            Mode::Lapsed { missed } => missed as i64 + 1,
        };
        let unaccounted = self.pending_seconds as i64 - accounted;
        self.mode = match self.mode {
            Mode::Empty => Mode::Fresh,
            _ => Mode::Tracking,
        };

        if self.spurious_count == 0 {
            self.good_pulse_count = self.good_pulse_count.saturating_add(1);
        } else {
            self.good_pulse_count = 0;
        }

        self.last_delta = self.pending_delta;
        self.pending_seconds = 0;
        self.pending_delta = 0;
        self.last_edge = pulse_edge;
        self.miss_timeout_ns = pulse_time_ns
            .saturating_add(NANOS_PER_SECOND)
            .saturating_add(config.miss_grace_ns as u64);
        Some(unaccounted)
    }

    /// Account for one expected pulse that did not arrive. Callers only invoke
    /// this on a train that has qualified at least once.
    fn process_miss(&mut self) {
        self.good_pulse_count = 0;
        self.mode = match self.mode {
            Mode::Lapsed { missed } => Mode::Lapsed {
                missed: missed.saturating_add(1),
            },
            _ => Mode::Lapsed { missed: 1 },
        };
        self.miss_timeout_ns = self.miss_timeout_ns.saturating_add(NANOS_PER_SECOND);
    }
}

/// Tracks a main PPS train and a tentative alternate train.
pub(crate) struct PulseTracker {
    domain: CounterDomain,
    config: PulseConfig,
    main: Train,
    tentative: Train,
    last_raw_edge: u32,
    last_raw_time_ns: Option<u64>,
    spurs: u32,
    backward_time: u32,
}

impl PulseTracker {
    /// # Panics
    ///
    /// Both are board constants. A tolerance of half a period or more would
    /// make every pulse in the domain qualify.
    pub(crate) const fn new(domain: CounterDomain, config: PulseConfig) -> Self {
        assert!(
            config.edge_tolerance_ticks < domain.period() / 2,
            "edge tolerance must be under half the counter period"
        );
        assert!(
            config.lock_threshold != 0 && config.switchover_threshold != 0,
            "pulse thresholds must be nonzero"
        );
        Self {
            domain,
            config,
            main: Train::new(),
            tentative: Train::new(),
            last_raw_edge: 0,
            last_raw_time_ns: None,
            spurs: 0,
            backward_time: 0,
        }
    }

    pub(crate) const fn domain(&self) -> CounterDomain {
        self.domain
    }

    pub(crate) fn state(&self) -> PulseState {
        let good = self.main.good_pulse_count;
        match self.main.mode {
            Mode::Empty => PulseState::FreeRunning,
            Mode::Lapsed { missed } => PulseState::Holdover {
                missed_pulses: missed.min(u8::MAX as u32) as u8,
            },
            _ if good >= self.config.lock_threshold as u32 => PulseState::Locked,
            _ => PulseState::Acquiring {
                good_pulses: good.min(u8::MAX as u32) as u8,
            },
        }
    }

    /// The drift-compensated phase anchor.
    pub(crate) fn target_edge(&self) -> Option<u32> {
        if !self.main.is_valid() {
            return None;
        }
        let period = self.domain.period() as i128;
        Some(
            (self.main.last_edge as i128 - self.main.phase_error as i128).rem_euclid(period) as u32,
        )
    }

    /// The measured phase, or `None` while no pulse is closing the second.
    pub(crate) fn phase_error_ns(&self) -> Option<f32> {
        matches!(self.main.mode, Mode::Fresh | Mode::Tracking)
            .then(|| self.domain.ticks_to_ns(self.main.phase_error))
    }

    /// Edges the main train rejected.
    pub(crate) const fn spur_count(&self) -> u32 {
        self.spurs
    }

    pub(crate) const fn backward_time_count(&self) -> u32 {
        self.backward_time
    }

    /// Earliest monotonic time at which [`Self::poll`] can produce an event.
    ///
    /// The miss comparison is strictly `> miss_timeout_ns`, so this is one
    /// nanosecond past that timeout.
    pub(crate) fn next_poll_deadline_ns(&self) -> Option<u64> {
        [self.main, self.tentative]
            .into_iter()
            .filter(Train::is_valid)
            .map(|train| train.miss_timeout_ns.saturating_add(1))
            .min()
    }

    /// Replace any unqualified startup edge with a synthetic local boundary:
    /// keeping one would make missed-pulse deadlines disagree with the free-run
    /// phase established here. A real train still qualifies through a capture.
    pub(crate) fn bootstrap(&mut self, pulse_edge: u32, monotonic_ns: u64) -> SecondEvent {
        self.main = Train::new();
        self.tentative = Train::new();
        self.last_raw_time_ns = None;
        self.capture(pulse_edge, monotonic_ns)
            .expect("an empty pulse tracker accepts its first edge")
    }

    /// Capture a raw rising edge. `monotonic_ns` need not share a frequency
    /// with the captured counter: it only separates a one-second pulse from one
    /// arriving after missing seconds.
    pub(crate) fn capture(&mut self, pulse_edge: u32, monotonic_ns: u64) -> Option<SecondEvent> {
        let monotonic_ns = self.forward_time(monotonic_ns);
        let pulse_edge = self.domain.normalize(pulse_edge as u64);
        let (delta, seconds) = match self.last_raw_time_ns {
            None => (0, 0),
            Some(last_time) => {
                let delta = self.domain.signed_delta(pulse_edge, self.last_raw_edge);
                let elapsed = monotonic_ns - last_time;
                let delta_ns =
                    delta as i128 * NANOS_PER_SECOND as i128 / self.domain.period() as i128;
                let corrected = elapsed as i128 - delta_ns;
                let rounded =
                    (corrected + (NANOS_PER_SECOND / 2) as i128) / NANOS_PER_SECOND as i128;
                (delta, rounded.clamp(0, u32::MAX as i128) as u32)
            }
        };
        self.last_raw_edge = pulse_edge;
        self.last_raw_time_ns = Some(monotonic_ns);

        match self.main.process_pulse(
            self.domain,
            self.config,
            delta,
            seconds,
            pulse_edge,
            monotonic_ns,
        ) {
            Some(unaccounted) => {
                if self.tentative.is_valid() {
                    self.tentative.spurious_count = self.tentative.spurious_count.saturating_add(1);
                    self.tentative.good_pulse_count = 0;
                }
                self.main.phase_error = self.main.phase_error.saturating_add(self.main.last_delta);
                self.main.spurious_count = 0;
                Some(SecondEvent::Captured { unaccounted })
            }
            None => {
                self.spurs = self.spurs.saturating_add(1);
                if self
                    .tentative
                    .process_pulse(
                        self.domain,
                        self.config,
                        delta,
                        seconds,
                        pulse_edge,
                        monotonic_ns,
                    )
                    .is_some()
                {
                    self.tentative.spurious_count = 0;
                }
                None
            }
        }
    }

    /// Account for at most one expected pulse that did not arrive by
    /// `monotonic_ns`. The caller repeats until this returns `None`, so one
    /// second-event is reported per second.
    pub(crate) fn poll(&mut self, monotonic_ns: u64) -> Option<SecondEvent> {
        let monotonic_ns = self.forward_time(monotonic_ns);

        let missed = self.main.is_valid() && monotonic_ns > self.main.miss_timeout_ns;
        if missed {
            self.main.process_miss();
        }

        if self.tentative.is_valid() && monotonic_ns > self.tentative.miss_timeout_ns {
            if self.tentative.spurious_count != 0 {
                self.tentative = Train::new();
            } else {
                self.tentative.process_miss();
            }
        }

        if !missed {
            return None;
        }

        let absent = match self.main.mode {
            Mode::Lapsed { missed } => missed,
            _ => 0,
        };
        let switch = absent > self.config.missed_pulse_threshold as u32
            && self.tentative.good_pulse_count > self.config.switchover_threshold as u32;
        if switch {
            self.main = self.tentative;
            self.main.mode = Mode::Fresh;
            self.tentative = Train::new();
        }
        self.main.spurious_count = 0;
        Some(if switch {
            SecondEvent::Switched
        } else {
            SecondEvent::Missed
        })
    }

    /// Move the phase anchor to the latest observed edge while preserving all
    /// pulse qualification counters. Frequency discipline is separate and
    /// retains its learned correction.
    pub(crate) fn rebase(&mut self) {
        self.main.phase_error = 0;
    }

    /// Clamp a timestamp that went backwards, so a stalled or re-based clock
    /// source makes the affected pulse look like a spur instead of corrupting
    /// the second count.
    fn forward_time(&mut self, monotonic_ns: u64) -> u64 {
        match self.last_raw_time_ns {
            Some(last) if monotonic_ns < last => {
                self.backward_time = self.backward_time.saturating_add(1);
                last
            }
            _ => monotonic_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERIOD: u32 = 1_000_000;

    fn new_tracker() -> PulseTracker {
        PulseTracker::new(
            CounterDomain::new(PERIOD),
            PulseConfig::with_edge_tolerance(100),
        )
    }

    fn captured(event: Option<SecondEvent>) -> i64 {
        match event {
            Some(SecondEvent::Captured { unaccounted }) => unaccounted,
            other => panic!("expected a capture, got {other:?}"),
        }
    }

    #[test]
    fn clean_pulses_qualify_and_accumulate_phase() {
        let mut tracker = new_tracker();
        assert_eq!(captured(tracker.capture(100, 0)), 0);
        assert_eq!(tracker.state(), PulseState::Acquiring { good_pulses: 1 });
        assert_eq!(captured(tracker.capture(110, 1_000_010_000)), 0);
        assert_eq!(captured(tracker.capture(120, 2_000_020_000)), 0);
        assert_eq!(tracker.state(), PulseState::Locked);
        assert_eq!(tracker.main.phase_error, 20);
        assert_eq!(tracker.target_edge(), Some(100));
        assert!((tracker.phase_error_ns().unwrap() - 20_000.0).abs() < 0.1);
    }

    #[test]
    fn a_spur_does_not_move_the_main_train() {
        let mut tracker = new_tracker();
        tracker.capture(100, 0).unwrap();
        assert_eq!(tracker.capture(500_000, 500_000_000), None);
        assert_eq!(captured(tracker.capture(100, 1_000_000_000)), 0);
        assert_eq!(tracker.main.last_delta, 0);
        assert_eq!(tracker.target_edge(), Some(100));
        assert_eq!(tracker.spur_count(), 1);
    }

    #[test]
    fn a_board_tolerance_is_stated_in_nanoseconds() {
        assert_eq!(ticks(50, 98_304_000), 4);
        assert_eq!(ticks(1_000, 98_304_000), 98);
        assert_eq!(
            PulseConfig::with_edge_tolerance(ticks(1_000, 98_304_000)).edge_tolerance_ticks,
            98
        );
    }

    #[test]
    fn missing_pulse_enters_holdover_without_erasing_phase() {
        let mut tracker = new_tracker();
        tracker.capture(100, 0).unwrap();
        tracker.capture(110, 1_000_010_000).unwrap();
        let phase = tracker.main.phase_error;
        assert_eq!(tracker.poll(2_020_000_000), Some(SecondEvent::Missed));
        assert_eq!(tracker.state(), PulseState::Holdover { missed_pulses: 1 });
        assert_eq!(tracker.phase_error_ns(), None);
        assert_eq!(tracker.main.phase_error, phase);
    }

    #[test]
    fn a_single_poll_reports_a_single_second() {
        let mut tracker = new_tracker();
        tracker.capture(100, 0).unwrap();
        for missed in 1..=3 {
            assert_eq!(tracker.poll(4_000_000_000), Some(SecondEvent::Missed));
            assert_eq!(
                tracker.state(),
                PulseState::Holdover {
                    missed_pulses: missed
                }
            );
        }
        assert_eq!(tracker.poll(4_000_000_000), None);
    }

    #[test]
    fn a_resumed_pulse_reconciles_against_the_polled_misses() {
        let mut tracker = new_tracker();
        tracker.capture(100, 0).unwrap();
        while tracker.poll(3_000_000_000).is_some() {}
        assert_eq!(captured(tracker.capture(100, 3_000_000_000)), 0);

        let mut unpolled = new_tracker();
        unpolled.capture(100, 0).unwrap();
        assert_eq!(captured(unpolled.capture(100, 3_000_000_000)), 2);
    }

    #[test]
    fn a_coherent_alternate_train_takes_over_after_enough_misses() {
        let mut tracker = new_tracker();
        for second in 0..3u64 {
            tracker.capture(100, second * 1_000_000_000).unwrap();
        }
        for second in 3..12u64 {
            let now = second * 1_000_000_000;
            assert_eq!(tracker.capture(500_000, now + 500_000_000), None);
            while let Some(event) = tracker.poll(now + 1_000_000_000) {
                if event == SecondEvent::Switched {
                    assert_eq!(tracker.target_edge(), Some(500_000));
                    return;
                }
            }
        }
        panic!("no switchover");
    }

    #[test]
    fn rebase_moves_target_to_the_latest_edge() {
        let mut tracker = new_tracker();
        tracker.capture(100, 0).unwrap();
        tracker.capture(110, 1_000_010_000).unwrap();
        assert_eq!(tracker.target_edge(), Some(100));
        tracker.rebase();
        assert_eq!(tracker.target_edge(), Some(110));
        assert_eq!(tracker.phase_error_ns(), Some(0.0));
    }

    #[test]
    fn time_going_backwards_is_clamped_and_counted() {
        let mut tracker = new_tracker();
        tracker.capture(0, 10).unwrap();
        assert_eq!(tracker.capture(0, 9), None);
        assert_eq!(tracker.backward_time_count(), 1);
        assert_eq!(tracker.state(), PulseState::Acquiring { good_pulses: 1 });
    }

    #[test]
    fn the_poll_deadline_is_exact_and_moves_one_second_per_miss() {
        let mut tracker = new_tracker();
        assert_eq!(tracker.next_poll_deadline_ns(), None);

        tracker.capture(100, 123).unwrap();
        let deadline = 1_010_000_124;
        assert_eq!(tracker.next_poll_deadline_ns(), Some(deadline));
        assert_eq!(tracker.poll(deadline - 1), None);
        assert_eq!(tracker.poll(deadline), Some(SecondEvent::Missed));
        assert_eq!(
            tracker.next_poll_deadline_ns(),
            Some(deadline + NANOS_PER_SECOND)
        );
    }

    #[test]
    fn bootstrap_replaces_an_unqualified_startup_phase() {
        let mut tracker = new_tracker();
        tracker.capture(123, 500_000_000).unwrap();

        assert_eq!(
            tracker.bootstrap(500_000, 1_000_000_000),
            SecondEvent::Captured { unaccounted: 0 }
        );
        assert_eq!(tracker.target_edge(), Some(500_000));
        assert_eq!(tracker.next_poll_deadline_ns(), Some(2_010_000_001));
    }

    /// The pre-refactor implementation, kept verbatim as a differential oracle.
    /// It is the line-for-line port of `tlppstracker.c` this module grew out of,
    /// so the test below holds the rewrite to the C's behavior, not just to a
    /// tidier description of it.
    mod oracle {
        use super::{CounterDomain, PulseConfig, PulseState, SecondEvent, NANOS_PER_SECOND};

        #[derive(Debug, Clone, Copy)]
        pub(super) struct OracleTrain {
            pub(super) valid: bool,
            pub(super) triggered: bool,
            pub(super) resumed: bool,
            pub(super) first: bool,
            pub(super) seconds_since_ref: u32,
            pub(super) delta_since_ref: i64,
            pub(super) spurious_count: u32,
            pub(super) good_pulse_count: u32,
            pub(super) pulse_streak_count: u32,
            pub(super) last_delta: i64,
            pub(super) phase_error: i64,
            pub(super) last_edge: u32,
            pub(super) miss_timeout_ns: u64,
        }

        impl OracleTrain {
            const fn new() -> Self {
                Self {
                    valid: false,
                    triggered: false,
                    resumed: false,
                    first: false,
                    seconds_since_ref: 0,
                    delta_since_ref: 0,
                    spurious_count: 0,
                    good_pulse_count: 0,
                    pulse_streak_count: 0,
                    last_delta: 0,
                    phase_error: 0,
                    last_edge: 0,
                    miss_timeout_ns: 0,
                }
            }

            fn process_pulse(
                &mut self,
                domain: CounterDomain,
                config: PulseConfig,
                delta: i64,
                seconds: u32,
                pulse_edge: u32,
                pulse_time_ns: u64,
            ) -> Option<i64> {
                if !self.valid {
                    *self = Self::new();
                    self.valid = true;
                    self.first = true;
                } else {
                    self.seconds_since_ref = self.seconds_since_ref.saturating_add(seconds);
                    self.delta_since_ref = self.delta_since_ref.saturating_add(delta);

                    let period = domain.period() as i64;
                    let half = period / 2;
                    if self.delta_since_ref >= half {
                        self.seconds_since_ref = self.seconds_since_ref.saturating_add(1);
                        self.delta_since_ref -= period;
                    } else if self.delta_since_ref < -half {
                        self.seconds_since_ref = self.seconds_since_ref.saturating_sub(1);
                        self.delta_since_ref += period;
                    }

                    if self.seconds_since_ref == 0
                        || self.delta_since_ref < -(config.edge_tolerance_ticks as i64)
                        || self.delta_since_ref > config.edge_tolerance_ticks as i64
                    {
                        self.spurious_count = self.spurious_count.saturating_add(1);
                        return None;
                    }
                    self.first = false;
                }

                let unaccounted = if self.triggered {
                    if self.resumed {
                        self.resumed = false;
                        self.pulse_streak_count = 1;
                    }
                    self.seconds_since_ref as i64 - 1
                } else if self.first {
                    self.triggered = true;
                    self.seconds_since_ref as i64
                } else {
                    self.triggered = true;
                    self.resumed = true;
                    self.seconds_since_ref as i64 - (self.pulse_streak_count as i64 + 1)
                };

                self.pulse_streak_count = self.pulse_streak_count.saturating_add(1);

                if self.spurious_count == 0 {
                    self.good_pulse_count = self.good_pulse_count.saturating_add(1);
                } else {
                    self.good_pulse_count = 0;
                }

                self.last_delta = self.delta_since_ref;
                self.seconds_since_ref = 0;
                self.delta_since_ref = 0;
                self.last_edge = pulse_edge;
                self.miss_timeout_ns = pulse_time_ns
                    .saturating_add(NANOS_PER_SECOND)
                    .saturating_add(config.miss_grace_ns as u64);
                Some(unaccounted)
            }

            fn process_miss(&mut self) {
                self.good_pulse_count = 0;
                if self.triggered {
                    self.pulse_streak_count = 0;
                }
                self.pulse_streak_count = self.pulse_streak_count.saturating_add(1);
                self.triggered = false;
                self.resumed = false;
                self.first = false;
                self.miss_timeout_ns = self.miss_timeout_ns.saturating_add(NANOS_PER_SECOND);
            }
        }

        pub(super) struct OracleTracker {
            domain: CounterDomain,
            config: PulseConfig,
            pub(super) main: OracleTrain,
            pub(super) tentative: OracleTrain,
            last_raw_edge: u32,
            last_raw_time_ns: Option<u64>,
            backward_time: u32,
        }

        impl OracleTracker {
            pub(super) const fn new(domain: CounterDomain, config: PulseConfig) -> Self {
                Self {
                    domain,
                    config,
                    main: OracleTrain::new(),
                    tentative: OracleTrain::new(),
                    last_raw_edge: 0,
                    last_raw_time_ns: None,
                    backward_time: 0,
                }
            }

            pub(super) fn state(&self) -> PulseState {
                if !self.main.valid {
                    PulseState::FreeRunning
                } else if !self.main.triggered {
                    PulseState::Holdover {
                        missed_pulses: self.main.pulse_streak_count.min(u8::MAX as u32) as u8,
                    }
                } else if self.main.good_pulse_count >= self.config.lock_threshold as u32 {
                    PulseState::Locked
                } else {
                    PulseState::Acquiring {
                        good_pulses: self.main.good_pulse_count.min(u8::MAX as u32) as u8,
                    }
                }
            }

            pub(super) fn target_edge(&self) -> Option<u32> {
                if !self.main.valid {
                    return None;
                }
                let period = self.domain.period() as i128;
                Some(
                    (self.main.last_edge as i128 - self.main.phase_error as i128).rem_euclid(period)
                        as u32,
                )
            }

            pub(super) fn phase_error_ns(&self) -> Option<f32> {
                self.main
                    .triggered
                    .then(|| self.domain.ticks_to_ns(self.main.phase_error))
            }

            pub(super) const fn backward_time_count(&self) -> u32 {
                self.backward_time
            }

            pub(super) fn next_poll_deadline_ns(&self) -> Option<u64> {
                [self.main, self.tentative]
                    .into_iter()
                    .filter(|train| train.valid)
                    .map(|train| train.miss_timeout_ns.saturating_add(1))
                    .min()
            }

            pub(super) fn bootstrap(&mut self, pulse_edge: u32, monotonic_ns: u64) -> SecondEvent {
                self.main = OracleTrain::new();
                self.tentative = OracleTrain::new();
                self.last_raw_time_ns = None;
                self.capture(pulse_edge, monotonic_ns)
                    .expect("an empty pulse tracker accepts its first edge")
            }

            pub(super) fn capture(
                &mut self,
                pulse_edge: u32,
                monotonic_ns: u64,
            ) -> Option<SecondEvent> {
                let monotonic_ns = self.forward_time(monotonic_ns);
                let pulse_edge = self.domain.normalize(pulse_edge as u64);
                let (delta, seconds) = match self.last_raw_time_ns {
                    None => (0, 0),
                    Some(last_time) => {
                        let delta = self.domain.signed_delta(pulse_edge, self.last_raw_edge);
                        let elapsed = monotonic_ns - last_time;
                        let delta_ns =
                            delta as i128 * NANOS_PER_SECOND as i128 / self.domain.period() as i128;
                        let corrected = elapsed as i128 - delta_ns;
                        let rounded =
                            (corrected + (NANOS_PER_SECOND / 2) as i128) / NANOS_PER_SECOND as i128;
                        (delta, rounded.clamp(0, u32::MAX as i128) as u32)
                    }
                };
                self.last_raw_edge = pulse_edge;
                self.last_raw_time_ns = Some(monotonic_ns);

                match self.main.process_pulse(
                    self.domain,
                    self.config,
                    delta,
                    seconds,
                    pulse_edge,
                    monotonic_ns,
                ) {
                    Some(unaccounted) => {
                        if self.tentative.valid {
                            self.tentative.spurious_count =
                                self.tentative.spurious_count.saturating_add(1);
                            self.tentative.good_pulse_count = 0;
                        }
                        self.main.phase_error =
                            self.main.phase_error.saturating_add(self.main.last_delta);
                        self.main.spurious_count = 0;
                        Some(SecondEvent::Captured { unaccounted })
                    }
                    None => {
                        if self
                            .tentative
                            .process_pulse(
                                self.domain,
                                self.config,
                                delta,
                                seconds,
                                pulse_edge,
                                monotonic_ns,
                            )
                            .is_some()
                        {
                            self.tentative.spurious_count = 0;
                        }
                        None
                    }
                }
            }

            pub(super) fn poll(&mut self, monotonic_ns: u64) -> Option<SecondEvent> {
                let monotonic_ns = self.forward_time(monotonic_ns);

                let missed = self.main.valid && monotonic_ns > self.main.miss_timeout_ns;
                if missed {
                    self.main.process_miss();
                }

                if self.tentative.valid && monotonic_ns > self.tentative.miss_timeout_ns {
                    if self.tentative.spurious_count != 0 {
                        self.tentative = OracleTrain::new();
                    } else {
                        self.tentative.process_miss();
                    }
                }

                if !missed {
                    return None;
                }

                let switch = self.main.pulse_streak_count
                    > self.config.missed_pulse_threshold as u32
                    && self.tentative.good_pulse_count > self.config.switchover_threshold as u32;
                if switch {
                    self.main = self.tentative;
                    self.main.first = true;
                    self.tentative = OracleTrain::new();
                }
                self.main.spurious_count = 0;
                Some(if switch {
                    SecondEvent::Switched
                } else {
                    SecondEvent::Missed
                })
            }

            pub(super) fn rebase(&mut self) {
                self.main.phase_error = 0;
            }

            fn forward_time(&mut self, monotonic_ns: u64) -> u64 {
                match self.last_raw_time_ns {
                    Some(last) if monotonic_ns < last => {
                        self.backward_time = self.backward_time.saturating_add(1);
                        last
                    }
                    _ => monotonic_ns,
                }
            }
        }
    }

    /// Randomized equivalence between the explicit state machine and the
    /// flag-based original.
    mod differential {
        use super::oracle::{OracleTracker, OracleTrain};
        use super::*;

        const SEEDS: u64 = 24;
        const STEPS: u32 = 2000;
        const TOLERANCE: u32 = 100;

        /// xorshift64: deterministic, dependency-free, good enough to shuffle
        /// event order.
        struct Rng(u64);

        impl Rng {
            fn new(seed: u64) -> Self {
                Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
            }

            fn next(&mut self) -> u64 {
                let mut x = self.0;
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                self.0 = x;
                x
            }

            fn below(&mut self, bound: u64) -> u64 {
                self.next() % bound
            }

            fn chance(&mut self, one_in: u64) -> bool {
                self.below(one_in) == 0
            }

            /// Uniform in `[-span, span]`.
            fn spread(&mut self, span: i64) -> i64 {
                self.below(2 * span as u64 + 1) as i64 - span
            }
        }

        /// Both trackers under one set of inputs, with the coverage counters
        /// that prove the interesting transitions were reached.
        struct Pair {
            new: PulseTracker,
            old: OracleTracker,
            captures: u32,
            spurs: u32,
            misses: u32,
            switchovers: u32,
            resumes: u32,
        }

        impl Pair {
            fn new() -> Self {
                let domain = CounterDomain::new(PERIOD);
                let config = PulseConfig::with_edge_tolerance(TOLERANCE);
                Self {
                    new: PulseTracker::new(domain, config),
                    old: OracleTracker::new(domain, config),
                    captures: 0,
                    spurs: 0,
                    misses: 0,
                    switchovers: 0,
                    resumes: 0,
                }
            }

            fn capture(&mut self, edge: u32, now: u64) {
                let lapsed = matches!(self.new.state(), PulseState::Holdover { .. });
                let new = self.new.capture(edge, now);
                let old = self.old.capture(edge, now);
                assert_eq!(new, old, "capture(edge {edge}, {now} ns)");
                match new {
                    Some(SecondEvent::Captured { .. }) => {
                        self.captures += 1;
                        self.resumes += u32::from(lapsed);
                    }
                    _ => self.spurs += 1,
                }
                self.agree("capture");
            }

            fn poll(&mut self, now: u64) -> Option<SecondEvent> {
                let new = self.new.poll(now);
                let old = self.old.poll(now);
                assert_eq!(new, old, "poll({now} ns)");
                match new {
                    Some(SecondEvent::Switched) => self.switchovers += 1,
                    Some(_) => self.misses += 1,
                    None => {}
                }
                self.agree("poll");
                new
            }

            /// One poll per missing second, as `Synchronizer::poll` does.
            fn drain(&mut self, now: u64) {
                while self.poll(now).is_some() {}
            }

            fn bootstrap(&mut self, edge: u32, now: u64) {
                let new = self.new.bootstrap(edge, now);
                let old = self.old.bootstrap(edge, now);
                assert_eq!(new, old, "bootstrap(edge {edge}, {now} ns)");
                self.agree("bootstrap");
            }

            fn rebase(&mut self) {
                self.new.rebase();
                self.old.rebase();
                self.agree("rebase");
            }

            fn agree(&self, what: &str) {
                assert_eq!(self.new.state(), self.old.state(), "state after {what}");
                assert_eq!(
                    self.new.target_edge(),
                    self.old.target_edge(),
                    "target edge after {what}"
                );
                assert_eq!(
                    self.new.phase_error_ns().map(f32::to_bits),
                    self.old.phase_error_ns().map(f32::to_bits),
                    "phase error after {what}"
                );
                assert_eq!(
                    self.new.next_poll_deadline_ns(),
                    self.old.next_poll_deadline_ns(),
                    "poll deadline after {what}"
                );
                assert_eq!(
                    self.new.backward_time_count(),
                    self.old.backward_time_count(),
                    "backward time after {what}"
                );
                same_train(&self.new.main, &self.old.main, "main", what);
                same_train(&self.new.tentative, &self.old.tentative, "tentative", what);
            }
        }

        /// Every flag-based field has a counterpart except `pulse_streak_count`
        /// while triggered and `resumed`, which the original only ever read
        /// back after a miss had overwritten them.
        fn same_train(new: &Train, old: &OracleTrain, which: &str, what: &str) {
            let mode = if !old.valid {
                Mode::Empty
            } else if !old.triggered {
                Mode::Lapsed {
                    missed: old.pulse_streak_count,
                }
            } else if old.first {
                Mode::Fresh
            } else {
                Mode::Tracking
            };
            assert_eq!(new.mode, mode, "{which} mode after {what}");
            assert_eq!(
                new.pending_seconds, old.seconds_since_ref,
                "{which} pending seconds after {what}"
            );
            assert_eq!(
                new.pending_delta, old.delta_since_ref,
                "{which} pending delta after {what}"
            );
            assert_eq!(
                new.spurious_count, old.spurious_count,
                "{which} spurious count after {what}"
            );
            assert_eq!(
                new.good_pulse_count, old.good_pulse_count,
                "{which} good pulses after {what}"
            );
            assert_eq!(
                new.last_delta, old.last_delta,
                "{which} last delta after {what}"
            );
            assert_eq!(
                new.phase_error, old.phase_error,
                "{which} phase error after {what}"
            );
            assert_eq!(new.last_edge, old.last_edge, "{which} edge after {what}");
            assert_eq!(
                new.miss_timeout_ns, old.miss_timeout_ns,
                "{which} miss timeout after {what}"
            );
        }

        /// A simulated PPS environment: a drifting primary train that goes away
        /// for stretches, a coherent alternate half a period out of phase to
        /// take over, spurs, and timestamps that occasionally go backwards.
        struct Scenario {
            rng: Rng,
            now: u64,
            edge: u32,
            alternate: u32,
            outage: u32,
            alternate_on: bool,
        }

        fn normalize(edge: i64) -> u32 {
            edge.rem_euclid(PERIOD as i64) as u32
        }

        impl Scenario {
            fn new(seed: u64) -> Self {
                let mut rng = Rng::new(seed);
                let edge = rng.below(PERIOD as u64) as u32;
                let now = rng.below(4) * NANOS_PER_SECOND;
                Self {
                    rng,
                    now,
                    edge,
                    alternate: (edge + PERIOD / 2) % PERIOD,
                    outage: 0,
                    alternate_on: false,
                }
            }

            /// One simulated second: pulses, spurs, and whatever polling a
            /// distracted board might get around to.
            fn step(&mut self, pair: &mut Pair) {
                let base = self.now;

                if self.outage == 0 && self.rng.chance(12) {
                    self.outage = 6 + self.rng.below(10) as u32;
                    self.alternate_on = true;
                } else if self.alternate_on && self.outage == 0 && self.rng.chance(15) {
                    self.alternate_on = false;
                }

                self.edge = normalize(self.edge as i64 + self.rng.spread(3));
                if self.outage == 0 {
                    let jitter = if self.rng.chance(10) {
                        self.rng.spread(400)
                    } else {
                        self.rng.spread(40)
                    };
                    let at = base.saturating_add_signed(self.rng.spread(450) * 1_000_000);
                    pair.capture(normalize(self.edge as i64 + jitter), at);
                } else {
                    self.outage -= 1;
                }

                if self.alternate_on {
                    let jitter = self.rng.spread(30);
                    let at = base + 500_000_000 + (self.rng.below(41) * 1_000_000) - 20_000_000;
                    pair.capture(normalize(self.alternate as i64 + jitter), at);
                }

                let noise = if self.outage == 0 { 6 } else { 40 };
                if self.rng.chance(noise) {
                    let at = base + self.rng.below(NANOS_PER_SECOND);
                    pair.capture(self.rng.below(PERIOD as u64) as u32, at);
                }
                if self.rng.chance(70) {
                    pair.capture(self.edge, base.saturating_sub(self.rng.below(300_000_000)));
                }

                match self.rng.below(12) {
                    0..=6 => pair.drain(base + 1_020_000_000),
                    7 => {
                        pair.poll(base + 1_020_000_000);
                    }
                    8 => {
                        pair.poll(base + 400_000_000);
                    }
                    9 => pair.drain(base + 1_020_000_000 + self.rng.below(4) * NANOS_PER_SECOND),
                    _ => {}
                }

                if self.rng.chance(60) {
                    pair.rebase();
                }
                if self.rng.chance(300) {
                    pair.bootstrap(self.rng.below(PERIOD as u64) as u32, base + 900_000_000);
                }

                self.now = base + NANOS_PER_SECOND;
            }
        }

        #[test]
        fn the_explicit_state_machine_matches_the_flag_based_original() {
            let (mut captures, mut spurs, mut misses, mut switchovers, mut resumes) =
                (0u64, 0u64, 0u64, 0u64, 0u64);
            for seed in 1..=SEEDS {
                let mut pair = Pair::new();
                let mut scenario = Scenario::new(seed);
                for _ in 0..STEPS {
                    scenario.step(&mut pair);
                }
                assert!(pair.switchovers > 0, "seed {seed} never switched over");
                assert!(pair.resumes > 0, "seed {seed} never resumed a lapsed train");
                assert!(pair.spurs > 0, "seed {seed} never rejected a spur");
                captures += pair.captures as u64;
                spurs += pair.spurs as u64;
                misses += pair.misses as u64;
                switchovers += pair.switchovers as u64;
                resumes += pair.resumes as u64;
            }
            println!(
                "{SEEDS} seeds x {STEPS} steps: {captures} captures, {spurs} spurs, \
                 {misses} misses, {switchovers} switchovers, {resumes} resumes"
            );
        }
    }
}

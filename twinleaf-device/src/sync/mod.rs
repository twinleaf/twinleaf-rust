//! Clock synchronization and acquisition scheduling, free of any hardware.
//!
//! [`Synchronizer`] is the whole interface and the only thing that advances
//! the reference timeline. A runtime turns its hardware events into these
//! calls and performs the [`Actions`] that come back; nothing here knows about
//! a counter peripheral, an executor, or a clock of its own.
//!
//! The pieces it composes are below it: the wrapping-counter arithmetic, the
//! timeref association, the oscillator servo, and the acquisition scheduler.

mod acquisition;
mod pulse;

pub use acquisition::{
    AcquisitionAction, AcquisitionError, AcquisitionMachine, AcquisitionState, ScheduledEdge,
    StartPlan,
};
pub use pulse::{ticks, PulseConfig, PulseState};

use core::mem::{discriminant, Discriminant};

use twinleaf_proto::sync::{Epoch, Timeref, MAX_SERIAL_SIZE};
use twinleaf_proto::SessionId;

use pulse::{PulseTracker, SecondEvent};

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Coherent observations before a new source may replace the active one.
const CONFIRMATIONS: u8 = 3;

/// How long after the second it labels a SYNC packet may still be paired with
/// it. A hub emits within 500 ms of its edge and never replays history.
const REFERENCE_WINDOW_NS: u64 = 900_000_000;

/// The pad byte counts seconds modulo 64.
const SEQUENCE_MASK: u8 = 0x3f;

/// Work for the runtime after one [`Synchronizer`] call.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Actions {
    /// At least one second event was processed. A catch-up poll still reports
    /// one: historical SYNC packets must not follow their output edges.
    pub second: bool,
    /// What the acquisition asks of the sampler.
    pub local: AcquisitionAction,
    /// Frequency correction for the oscillator, present once per second event
    /// on a board that has an actuator.
    pub correction_ppm: Option<f32>,
    /// The SYNC packet to send to every child, on a device that has children.
    pub announce: Option<Announce>,
    /// A counter value for the outgoing PPS compare, when the anchor moved.
    pub set_edge: Option<u32>,
    /// The status to log and report, when it is no longer what was reported.
    pub status_changed: Option<Status>,
}

impl Actions {
    /// Nothing to do.
    pub const NONE: Self = Self {
        second: false,
        local: AcquisitionAction::None,
        correction_ppm: None,
        announce: None,
        set_edge: None,
        status_changed: None,
    };

    pub(crate) const fn local(local: AcquisitionAction) -> Self {
        Self {
            local,
            ..Self::NONE
        }
    }
}

/// One second's SYNC packet, as the only emitter of them produces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Announce {
    /// The second just closed, which a child labels its next edge from.
    pub timeref: Reference,
    /// Sequence in bits 0..=5, [`TimeStatus`] in bits 6..=7.
    pub pad: u8,
}

/// What a device says its own time is worth, in the SYNC pad byte.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeStatus {
    /// A hub that predates the pad byte, 0. It says nothing either way.
    Legacy,
    /// No pulse train has been established, 1.
    FreeRun,
    /// An established pulse train is absent, 2.
    Holdover,
    /// The pulse train is qualified, 3.
    Locked,
}

impl TimeStatus {
    /// The status a pad byte carries.
    pub const fn from_pad(pad: u8) -> Self {
        match pad >> 6 {
            0 => Self::Legacy,
            1 => Self::FreeRun,
            2 => Self::Holdover,
            3..=u8::MAX => Self::Locked,
        }
    }

    /// The two-bit code, which is also what `sync.status` reports.
    pub const fn code(self) -> u8 {
        match self {
            Self::Legacy => 0,
            Self::FreeRun => 1,
            Self::Holdover => 2,
            Self::Locked => 3,
        }
    }

    /// That code shifted into its pad byte field.
    pub const fn bits(self) -> u8 {
        self.code() << 6
    }

    /// Whether seconds from a source in this state are traceable. A legacy
    /// hub said nothing, and is taken at its word.
    pub const fn traceable(self) -> bool {
        matches!(self, Self::Legacy | Self::Locked)
    }

    /// The poorer of two statuses, which is what a hub passes on.
    pub const fn worse(self, other: Self) -> Self {
        if other.rank() < self.rank() {
            other
        } else {
            self
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::FreeRun => 0,
            Self::Holdover => 1,
            Self::Legacy | Self::Locked => 2,
        }
    }
}

/// Snapshot for a diagnostic RPC or status stream. Deliberately kept out of
/// the SYNC packet: a child only needs the PPS edge and timeref to follow.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Status {
    /// Qualification of the electrical pulse train.
    pub pulse: PulseState,
    /// Qualification of packet-provided time.
    pub reference: ReferenceState,
    /// Where the acquisition stands.
    pub acquisition: AcquisitionState,
    /// The second carried by the most recently processed edge.
    pub active: Reference,
    /// Counter value of the drift-compensated second edge.
    pub counter_edge: Option<u32>,
    /// Bumped when the edge anchor moves or the timeref is replaced. A change
    /// restarts an active acquisition.
    pub generation: u32,
    /// Measured phase, absent while no pulse is closing the second.
    pub phase_error_ns: Option<f32>,
    /// `None` on a board with no frequency actuator, so a held zero can never
    /// be mistaken for active discipline.
    pub correction_ppm: Option<f32>,
    /// Whether the servo is against its pull limit.
    pub correction_saturated: bool,
    /// Whether the active seconds are traceable to the source naming them.
    pub traceable: bool,
    /// What this device's pad byte says its own time is worth, and what
    /// `sync.status` reports.
    pub announced: TimeStatus,
    /// What the last accepted SYNC packet said its time was worth.
    pub upstream: TimeStatus,
    /// Edges the main pulse train rejected.
    pub spurs: u32,
    /// SYNC packets that arrived too late to label an edge.
    pub late_references: u32,
    /// Seconds that did not reconcile; see [`Synchronizer::poll`].
    pub divergences: u32,
    /// Acknowledgements that named a superseded start plan.
    pub stale_acks: u32,
    /// Staged starts whose target second never arrived.
    pub superseded_plans: u32,
    /// Timestamps that preceded the previous one.
    pub backward_time: u32,
}

/// One wrapping counter period. The math is unitless: the period may be timer
/// clocks, prescaled ticks, or a simulated counter.
#[derive(Debug, Clone, Copy)]
pub struct CounterDomain {
    period: u32,
}

impl CounterDomain {
    /// # Panics
    ///
    /// On a zero period. Build the domain in a `const` item to get a build
    /// failure instead of a runtime one.
    pub const fn new(period: u32) -> Self {
        assert!(period != 0, "counter period must be nonzero");
        Self { period }
    }

    /// Counter values in one second.
    pub const fn period(self) -> u32 {
        self.period
    }

    /// Bring an arbitrary non-negative value into the counter period.
    pub(crate) const fn normalize(self, value: u64) -> u32 {
        (value % self.period as u64) as u32
    }

    /// Shortest signed displacement from `expected` to `observed`. An exactly
    /// half-period ambiguity resolves negative.
    pub(crate) const fn signed_delta(self, observed: u32, expected: u32) -> i64 {
        let period = self.period as i64;
        let mut delta = observed as i64 - expected as i64;
        let half = period / 2;
        if delta >= half {
            delta -= period;
        } else if delta < -half {
            delta += period;
        }
        delta
    }

    /// Convert counter ticks to nanoseconds without assuming a prescaler.
    pub(crate) fn ticks_to_ns(self, ticks: i64) -> f32 {
        ticks as f32 * (1_000_000_000.0f32 / self.period as f32)
    }
}

#[cfg(test)]
mod counter_tests {
    use super::*;

    #[test]
    #[should_panic(expected = "counter period must be nonzero")]
    fn a_zero_period_is_a_build_error() {
        CounterDomain::new(0);
    }

    #[test]
    fn signed_delta_uses_the_short_path() {
        let domain = CounterDomain::new(100);
        assert_eq!(domain.signed_delta(3, 98), 5);
        assert_eq!(domain.signed_delta(98, 3), -5);
        assert_eq!(domain.signed_delta(50, 0), -50);
    }

    #[test]
    fn conversions_stay_inside_the_period() {
        let domain = CounterDomain::new(1_000_000);
        assert_eq!(domain.normalize(2_500_000), 500_000);
        assert!((domain.ticks_to_ns(20) - 20_000.0).abs() < 0.1);
    }
}

/// Owned form of the source serial carried by a borrowed [`Timeref`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSerial {
    len: u8,
    bytes: [u8; MAX_SERIAL_SIZE],
}

impl SourceSerial {
    /// Truncates rather than rejects: the wire format allows a longer serial
    /// than this field holds, and dropping the packet would lose the timeref.
    pub const fn new(value: &[u8]) -> Self {
        let len = if value.len() > MAX_SERIAL_SIZE {
            MAX_SERIAL_SIZE
        } else {
            value.len()
        };
        let mut bytes = [0; MAX_SERIAL_SIZE];
        let mut i = 0;
        while i < len {
            bytes[i] = value[i];
            i += 1;
        }
        Self {
            len: len as u8,
            bytes,
        }
    }

    /// The serial as it goes back on the wire.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

/// Identity of one timebase instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceIdentity {
    /// Timescale its seconds are counted in.
    pub epoch: Epoch,
    /// Session of the source, so a reboot is a new timebase.
    pub session: SessionId,
    /// Serial of the source.
    pub serial: SourceSerial,
}

impl ReferenceIdentity {
    /// An identity, truncating an oversized serial.
    pub const fn new(epoch: Epoch, session: SessionId, serial: &[u8]) -> Self {
        Self {
            epoch,
            session,
            serial: SourceSerial::new(serial),
        }
    }
}

/// A source identity and the second assigned to one particular edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reference {
    /// Whose timebase this is.
    pub identity: ReferenceIdentity,
    /// The second of the edge it names.
    pub second: u32,
}

impl Reference {
    /// The same source, `seconds` later.
    pub const fn advance(self, seconds: u32) -> Self {
        Self {
            identity: self.identity,
            second: self.second.wrapping_add(seconds),
        }
    }

    /// Borrow this owned reference in the SYNC packet shape.
    pub fn timeref(&self) -> Timeref<'_> {
        Timeref {
            epoch: self.identity.epoch,
            time: self.second,
            session: self.identity.session,
            serial: self.identity.serial.as_bytes(),
        }
    }
}

/// Qualification state of packet-provided time.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceState {
    /// The device's own timeline.
    Local,
    /// A source that has been coherent for this many edges.
    Candidate {
        /// Coherent observations so far, of the three adoption needs.
        confirmations: u8,
    },
    /// A source adopted from upstream.
    Upstream,
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    identity: ReferenceIdentity,
    base_edge: u64,
    base_second: u32,
    confirmations: u8,
    last_observed_edge: u64,
}

impl Candidate {
    fn reference_at(self, edge: u64) -> Reference {
        Reference {
            identity: self.identity,
            second: self
                .base_second
                .wrapping_add(edge.wrapping_sub(self.base_edge) as u32),
        }
    }

    fn is_consistent(self, edge: u64, reference: Reference) -> bool {
        self.identity == reference.identity && self.reference_at(edge).second == reference.second
    }
}

/// Qualifies packet time independently from electrical PPS qualification.
///
/// A type-0 SYNC packet labels the next edge with `time + 1`; a candidate is
/// only adopted on a qualified electrical edge.
pub(crate) struct ReferenceTracker {
    local: ReferenceIdentity,
    active: Reference,
    active_is_upstream: bool,
    edge_sequence: u64,
    candidate: Option<Candidate>,
}

impl ReferenceTracker {
    pub(crate) const fn new(local: Reference) -> Self {
        Self {
            local: local.identity,
            active: local,
            active_is_upstream: false,
            edge_sequence: 0,
            candidate: None,
        }
    }

    pub(crate) const fn active(&self) -> Reference {
        self.active
    }

    /// Whether the active reference came from a source rather than from the
    /// device's own timeline.
    pub(crate) const fn is_upstream(&self) -> bool {
        self.active_is_upstream
    }

    pub(crate) fn state(&self) -> ReferenceState {
        match self.candidate {
            Some(candidate) => ReferenceState::Candidate {
                confirmations: candidate.confirmations,
            },
            None if self.active_is_upstream => ReferenceState::Upstream,
            None => ReferenceState::Local,
        }
    }

    /// Restart the local timeline at `second`, keeping the local identity.
    pub(crate) fn reset_local(&mut self, second: u32) {
        self.active.second = second;
    }

    /// Observe a packet received after the current edge. Its timeref applies to
    /// the next edge.
    pub(crate) fn observe(&mut self, timeref: Timeref<'_>) {
        self.qualify(Reference {
            identity: ReferenceIdentity::new(timeref.epoch, timeref.session, timeref.serial),
            second: timeref.time.wrapping_add(1),
        });
    }

    /// Observe a second the device was told by something other than a packet,
    /// a GPS receiver above all, under its own identity in `epoch`.
    pub(crate) fn observe_local(&mut self, second: u32, epoch: Epoch) {
        self.qualify(Reference {
            identity: ReferenceIdentity {
                epoch,
                ..self.local
            },
            second: second.wrapping_add(1),
        });
    }

    /// Drop the source being qualified, which has proved incoherent.
    pub(crate) fn discard_candidate(&mut self) {
        self.candidate = None;
    }

    /// Advance by one second edge, reporting whether a candidate replaced the
    /// active reference. Adoption needs `pulse_qualified`.
    pub(crate) fn on_edge(&mut self, pulse_qualified: bool) -> bool {
        self.edge_sequence = self.edge_sequence.wrapping_add(1);
        self.active = self.active.advance(1);

        let Some(candidate) = self.candidate else {
            return false;
        };
        if !pulse_qualified || candidate.confirmations < CONFIRMATIONS {
            return false;
        }

        let next = candidate.reference_at(self.edge_sequence);
        self.candidate = None;
        if next == self.active {
            return false;
        }
        self.active = next;
        self.active_is_upstream = true;
        true
    }

    fn qualify(&mut self, reference: Reference) {
        let target_edge = self.edge_sequence.wrapping_add(1);

        if reference == self.active.advance(1) {
            self.candidate = None;
            return;
        }

        self.candidate = Some(match self.candidate {
            Some(mut candidate) if candidate.is_consistent(target_edge, reference) => {
                if candidate.last_observed_edge != target_edge {
                    candidate.confirmations = candidate.confirmations.saturating_add(1);
                    candidate.last_observed_edge = target_edge;
                }
                candidate
            }
            _ => Candidate {
                identity: reference.identity,
                base_edge: target_edge,
                base_second: reference.second,
                confirmations: 1,
                last_observed_edge: target_edge,
            },
        });
    }
}

#[cfg(test)]
mod reference_tests {
    use super::*;

    fn local() -> Reference {
        Reference {
            identity: ReferenceIdentity::new(Epoch::SYSTIME, SessionId::new(1), b"local"),
            second: 10,
        }
    }

    fn packet(time: u32) -> Timeref<'static> {
        Timeref {
            epoch: Epoch::UNIX,
            time,
            session: SessionId::new(99),
            serial: b"parent",
        }
    }

    #[test]
    fn three_coherent_packets_and_pulses_adopt_parent_time() {
        let mut tracker = ReferenceTracker::new(local());

        tracker.observe(packet(100));
        assert_eq!(
            tracker.state(),
            ReferenceState::Candidate { confirmations: 1 }
        );
        assert!(!tracker.on_edge(true));

        tracker.observe(packet(101));
        assert!(!tracker.on_edge(true));

        tracker.observe(packet(102));
        assert!(tracker.on_edge(true));
        assert_eq!(tracker.active().second, 103);
        assert_eq!(tracker.active().identity.epoch, Epoch::UNIX);
        assert_eq!(tracker.active().identity.serial.as_bytes(), b"parent");
        assert_eq!(tracker.state(), ReferenceState::Upstream);
    }

    #[test]
    fn packet_qualification_cannot_replace_pps_qualification() {
        let mut tracker = ReferenceTracker::new(local());
        for time in 100..103 {
            tracker.observe(packet(time));
            assert!(!tracker.on_edge(false));
        }
        assert!(matches!(
            tracker.state(),
            ReferenceState::Candidate { confirmations: 3 }
        ));
        assert!(tracker.on_edge(true));
        assert_eq!(tracker.active().second, 104);
    }

    #[test]
    fn duplicate_packets_for_one_edge_do_not_qualify() {
        let mut tracker = ReferenceTracker::new(local());
        tracker.observe(packet(100));
        tracker.observe(packet(100));
        tracker.observe(packet(100));
        assert_eq!(
            tracker.state(),
            ReferenceState::Candidate { confirmations: 1 }
        );
    }

    #[test]
    fn an_inconsistent_second_restarts_qualification() {
        let mut tracker = ReferenceTracker::new(local());
        tracker.observe(packet(100));
        tracker.on_edge(true);
        tracker.observe(packet(500));
        assert_eq!(
            tracker.state(),
            ReferenceState::Candidate { confirmations: 1 }
        );
    }

    #[test]
    fn an_upstream_reference_advances_through_holdover() {
        let mut tracker = ReferenceTracker::new(local());
        for time in 100..103 {
            tracker.observe(packet(time));
            tracker.on_edge(true);
        }
        assert_eq!(tracker.state(), ReferenceState::Upstream);
        let adopted = tracker.active().second;
        for _ in 0..5 {
            assert!(!tracker.on_edge(false));
        }
        assert_eq!(tracker.active().second, adopted + 5);
        assert_eq!(tracker.state(), ReferenceState::Upstream);
    }

    /// A GPS receiver's second qualifies exactly as a packet's does, under the
    /// device's own serial and the epoch the receiver reports.
    #[test]
    fn a_local_label_qualifies_under_the_devices_own_identity() {
        let mut tracker = ReferenceTracker::new(local());
        for time in 100..103 {
            tracker.observe_local(time, Epoch::UNIX);
            tracker.on_edge(true);
        }
        assert_eq!(tracker.state(), ReferenceState::Upstream);
        assert_eq!(tracker.active().second, 103);
        assert_eq!(tracker.active().identity.epoch, Epoch::UNIX);
        assert_eq!(tracker.active().identity.serial.as_bytes(), b"local");
    }

    #[test]
    fn an_oversized_serial_is_truncated_like_the_c_timeref() {
        let serial = SourceSerial::new(&[7; MAX_SERIAL_SIZE + 4]);
        assert_eq!(serial.as_bytes(), [7; MAX_SERIAL_SIZE]);
    }
}

/// Configuration for the 1 Hz phase-to-frequency PI loop, output in ppm.
///
/// `P = Kp * error`, `I += Kp * Ki * error * dt`, `error = -phase_error_ns`.
/// `Ki` is a ratio in 1/s, not the independent coefficient most PID crates
/// accept.
#[derive(Debug, Clone, Copy)]
pub struct PiConfig {
    /// Proportional gain, ppm per nanosecond of phase error.
    pub kp: f32,
    /// Integral ratio, in 1/s.
    pub ki: f32,
    /// Seconds between observations.
    pub interval_seconds: f32,
    /// Most negative output the actuator accepts.
    pub min_output_ppm: f32,
    /// Most positive output the actuator accepts.
    pub max_output_ppm: f32,
}

impl PiConfig {
    /// Critically damped type-2 loop for the SiT5356: plant 1 ppm -> 1000 ns/s,
    /// wn = 0.1 rad/s, and the part's ±50 ppm pull range.
    pub const SIT5356: Self = Self {
        kp: 2.0e-4,
        ki: 0.05,
        interval_seconds: 1.0,
        min_output_ppm: -50.0,
        max_output_ppm: 50.0,
    };
}

/// A dedicated phase-to-frequency PI servo.
#[derive(Debug, Clone, Copy)]
pub struct PhaseServo {
    config: PiConfig,
    integral_ppm: f32,
    output_ppm: f32,
    saturated: bool,
}

impl PhaseServo {
    /// # Panics
    ///
    /// On unusable gains, which are a board constant. The comparisons double
    /// as NaN traps: each one is false for a NaN operand.
    pub const fn new(config: PiConfig) -> Self {
        assert!(config.kp.is_finite() && config.ki.is_finite(), "PI gains");
        assert!(config.interval_seconds > 0.0, "PI interval");
        assert!(
            config.min_output_ppm <= config.max_output_ppm,
            "PI output range"
        );
        Self {
            config,
            integral_ppm: 0.0,
            output_ppm: 0.0,
            saturated: false,
        }
    }

    /// The correction it is asking for.
    pub const fn output_ppm(&self) -> f32 {
        self.output_ppm
    }

    /// Whether that correction is against a limit.
    pub const fn saturated(&self) -> bool {
        self.saturated
    }

    /// Process one phase observation. A non-finite value means a missing PPS
    /// and holds the correction unchanged.
    pub fn observe(&mut self, phase_error_ns: f32) -> f32 {
        if !phase_error_ns.is_finite() {
            return self.output_ppm;
        }

        let error = -phase_error_ns;
        let proportional = self.config.kp * error;
        self.integral_ppm += self.config.kp * self.config.ki * error * self.config.interval_seconds;

        let requested = proportional + self.integral_ppm;
        self.output_ppm = requested
            .max(self.config.min_output_ppm)
            .min(self.config.max_output_ppm);
        self.saturated = self.output_ppm != requested;
        if self.saturated {
            self.integral_ppm = self.output_ppm - proportional;
        }
        self.output_ppm
    }
}

#[cfg(test)]
mod discipline_tests {
    use super::*;

    #[test]
    fn gain_form_matches_tl_control_pid() {
        let mut servo = PhaseServo::new(PiConfig::SIT5356);
        assert!((servo.observe(1000.0) - -0.21).abs() < 1.0e-6);
        assert!((servo.observe(1000.0) - -0.22).abs() < 1.0e-6);
    }

    #[test]
    fn saturation_back_calculates_the_integrator() {
        let mut servo = PhaseServo::new(PiConfig {
            min_output_ppm: -1.0,
            max_output_ppm: 1.0,
            ..PiConfig::SIT5356
        });
        assert_eq!(servo.observe(-1_000_000.0), 1.0);
        assert!(servo.saturated());
        assert!(servo.observe(1_000.0) < 1.0);
    }

    #[test]
    fn a_missing_pulse_holds_the_frequency_estimate() {
        let mut servo = PhaseServo::new(PiConfig::SIT5356);
        let acquired = servo.observe(-500.0);
        assert_eq!(servo.observe(f32::NAN), acquired);
        assert_eq!(servo.output_ppm(), acquired);
    }

    #[test]
    #[should_panic(expected = "PI output range")]
    fn an_inverted_output_range_is_a_build_error() {
        PhaseServo::new(PiConfig {
            min_output_ppm: 1.0,
            max_output_ppm: -1.0,
            ..PiConfig::SIT5356
        });
    }
}

/// The last sequence accepted from one source, and when.
#[derive(Debug, Clone, Copy)]
struct Sequence {
    identity: ReferenceIdentity,
    sequence: u8,
    seconds: u32,
}

/// The coarse state a change of status is reported for. Holdover's missing
/// pulses and a candidate's confirmations move every second and are not it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Summary {
    pulse: Discriminant<PulseState>,
    reference: Discriminant<ReferenceState>,
    acquisition: AcquisitionState,
    traceable: bool,
}

/// Everything a device needs to follow a PPS input and schedule acquisitions.
///
/// A runtime translates its events into these calls and performs the
/// [`Actions`] that come out; it never interprets a count of elapsed seconds.
pub struct Synchronizer {
    pulses: PulseTracker,
    references: ReferenceTracker,
    servo: Option<PhaseServo>,
    acquisition: AcquisitionMachine,
    anchor: Option<u32>,
    generation: u32,
    divergences: u32,
    late_references: u32,
    upstream: TimeStatus,
    sequence: Option<Sequence>,
    reported: Option<Summary>,
    /// Second events processed since construction. Bootstrap happens on the
    /// first wake, so this tracks uptime closely enough to time the autostart.
    seconds: u32,
    /// Monotonic stamp of the last second event, which the arrival window for
    /// a SYNC packet is measured from.
    last_second_ns: Option<u64>,
}

impl Synchronizer {
    /// `servo` is `None` on a board with no frequency actuator.
    pub const fn new(
        domain: CounterDomain,
        pulses: PulseConfig,
        local: Reference,
        servo: Option<PhaseServo>,
        autostart_seconds: u8,
    ) -> Self {
        Self {
            pulses: PulseTracker::new(domain, pulses),
            references: ReferenceTracker::new(local),
            servo,
            acquisition: AcquisitionMachine::new(autostart_seconds),
            anchor: None,
            generation: 0,
            divergences: 0,
            late_references: 0,
            upstream: TimeStatus::Legacy,
            sequence: None,
            reported: None,
            seconds: 0,
            last_second_ns: None,
        }
    }

    /// What everything here stands at.
    pub fn status(&self) -> Status {
        Status {
            pulse: self.pulses.state(),
            reference: self.references.state(),
            acquisition: self.acquisition.state(),
            active: self.references.active(),
            counter_edge: self.anchor,
            generation: self.generation,
            phase_error_ns: self.pulses.phase_error_ns(),
            correction_ppm: self.servo.map(|servo| servo.output_ppm()),
            correction_saturated: self.servo.is_some_and(|servo| servo.saturated()),
            traceable: self.traceable(),
            announced: self.announced(),
            upstream: self.upstream,
            spurs: self.pulses.spur_count(),
            late_references: self.late_references,
            divergences: self.divergences,
            stale_acks: self.acquisition.stale_acks(),
            superseded_plans: self.acquisition.superseded_plans(),
            backward_time: self.pulses.backward_time_count(),
        }
    }

    /// The counter the pulses are measured against.
    pub const fn domain(&self) -> CounterDomain {
        self.pulses.domain()
    }

    /// Whether the active seconds are traceable to the source naming them. Its
    /// own timescale needs no proof; anything else must be locked to what it
    /// follows.
    pub fn traceable(&self) -> bool {
        let own_timescale = !self.references.is_upstream()
            && self.references.active().identity.epoch != Epoch::UNIX;
        own_timescale || (self.pulses.state().is_locked() && self.upstream.traceable())
    }

    /// Earliest monotonic nanosecond at which [`Self::poll`] can advance the
    /// timeline because an expected PPS did not arrive.
    fn next_poll_deadline_ns(&self) -> Option<u64> {
        self.pulses.next_poll_deadline_ns()
    }

    /// Earliest useful wakeup for a runtime: now while unanchored, since the
    /// bootstrap defines the first second boundary. It is the one source of
    /// the wake that closes a period; nothing else advances seconds.
    pub fn next_wake_deadline_ns(&self, monotonic_ns: u64) -> u64 {
        if self.anchor.is_none() {
            return monotonic_ns;
        }
        self.next_poll_deadline_ns()
            .unwrap_or_else(|| monotonic_ns.saturating_add(NANOS_PER_SECOND))
    }

    /// Process the wakeup returned by [`Self::next_wake_deadline_ns`].
    pub fn wake(&mut self, counter_now: u32, monotonic_ns: u64) -> Actions {
        if self.anchor.is_none() {
            self.bootstrap(counter_now, monotonic_ns)
        } else {
            self.poll(monotonic_ns)
        }
    }

    /// Observe a SYNC timeref that arrived at `arrival_ns`, labelling the
    /// second just closed. One that missed its second's window is dropped and
    /// counted; a sequence that skipped is a divergence, and discards whatever
    /// the source had qualified so far.
    pub fn observe_packet(&mut self, timeref: Timeref<'_>, pad: u8, arrival_ns: u64) {
        let in_window = self
            .last_second_ns
            .is_some_and(|last| arrival_ns.saturating_sub(last) < REFERENCE_WINDOW_NS);
        if !in_window {
            self.late_references = self.late_references.saturating_add(1);
            return;
        }
        let identity = ReferenceIdentity::new(timeref.epoch, timeref.session, timeref.serial);
        let status = TimeStatus::from_pad(pad);
        if !self.sequenced(identity, status, pad & SEQUENCE_MASK) {
            return;
        }
        self.upstream = status;
        self.references.observe(timeref);
    }

    /// Observe a second from the device's own receiver rather than a packet,
    /// labelling the second just closed as [`Self::observe_packet`] does.
    pub fn observe_local(&mut self, second: u32, epoch: Epoch) {
        self.references.observe_local(second, epoch);
    }

    /// Capture an electrical PPS rising edge.
    ///
    /// Spurs return [`Actions::NONE`]: they never reach the timeline.
    pub fn capture(&mut self, pulse_edge: u32, monotonic_ns: u64) -> Actions {
        match self.pulses.capture(pulse_edge, monotonic_ns) {
            Some(event) => self.second(event, monotonic_ns),
            None => Actions::NONE,
        }
    }

    /// Account for PPS edges that have not arrived and advance a free-running
    /// timebase. Calling this before the deadline is safe and does nothing.
    pub fn poll(&mut self, monotonic_ns: u64) -> Actions {
        let mut actions = Actions::NONE;
        while let Some(event) = self.pulses.poll(monotonic_ns) {
            let mut next = self.second(event, monotonic_ns);
            if let (
                AcquisitionAction::Arm(_) | AcquisitionAction::Rearm(_),
                AcquisitionAction::Start(edge),
            ) = (actions.local, next.local)
            {
                next.local = self.acquisition.rearm_from(edge, monotonic_ns);
            }
            if next.local != AcquisitionAction::None {
                actions.local = next.local;
            }
            actions.second |= next.second;
            actions.correction_ppm = next.correction_ppm;
            actions.announce = next.announce.or(actions.announce);
            actions.set_edge = next.set_edge.or(actions.set_edge);
            actions.status_changed = next.status_changed.or(actions.status_changed);
        }
        actions
    }

    /// Establish a local timebase from the monotonic clock, so a board with no
    /// PPS input can still acquire. The calling instant is the first boundary,
    /// so `counter_now` is the edge. Does nothing once an anchor exists.
    pub fn bootstrap(&mut self, counter_now: u32, monotonic_ns: u64) -> Actions {
        if self.anchor.is_some() {
            return Actions::NONE;
        }

        let edge = self.pulses.domain().normalize(counter_now as u64);
        let label = ((monotonic_ns + NANOS_PER_SECOND / 2) / NANOS_PER_SECOND) as u32;
        self.references.reset_local(label.wrapping_sub(1));
        self.anchor = Some(edge);
        self.generation = self.generation.wrapping_add(1);

        let event = self.pulses.bootstrap(edge, monotonic_ns);
        let mut actions = self.second(event, monotonic_ns);
        actions.set_edge = Some(edge);
        actions
    }

    /// `dev.autostart`.
    pub fn autostart_seconds(&self) -> u8 {
        self.acquisition.autostart_seconds()
    }

    /// Set `dev.autostart`.
    pub fn set_autostart_seconds(&mut self, seconds: u8) {
        self.acquisition.set_autostart_seconds(seconds);
    }

    /// `dev.start`.
    pub fn start(&mut self) -> Result<Actions, AcquisitionError> {
        let actions = self.acquisition.request_start()?;
        Ok(self.report(actions))
    }

    /// `dev.stop`.
    pub fn stop(&mut self) -> Result<Actions, AcquisitionError> {
        let actions = self.acquisition.request_stop()?;
        Ok(self.report(actions))
    }

    /// `dev.restart`: rebase the phase anchor onto the pulses as observed now.
    /// Keeps the learned frequency correction, and is deliberately not a
    /// synchronization change: the new segment starts against the new anchor.
    pub fn restart(&mut self) -> Result<Actions, AcquisitionError> {
        let actions = self.acquisition.request_restart()?;
        self.pulses.rebase();
        if let Some(edge) = self.pulses.target_edge() {
            self.anchor = Some(edge);
        }
        Ok(self.report(actions))
    }

    /// Confirm that the peripheral produced the first sample of `plan`.
    pub fn mark_running(&mut self, plan: u16) -> Result<(), AcquisitionError> {
        self.acquisition.mark_running(plan)
    }

    /// Report that `plan`'s staging window was already past when the board
    /// went to program it.
    pub fn missed(&mut self, plan: u16) -> Result<(), AcquisitionError> {
        self.acquisition.missed(plan)
    }

    /// Confirm that the board-specific stop completed.
    pub fn finish_stop(&mut self) -> Result<(), AcquisitionError> {
        self.acquisition.finish_stop()
    }

    /// The one place the reference timeline moves: one second per second event,
    /// never a measured elapsed gap, which would double-count what a
    /// [`Self::poll`] already reported. A gap no poll accounted for is counted
    /// as a divergence and drops the edge to holdover instead of anchoring it.
    fn second(&mut self, event: SecondEvent, monotonic_ns: u64) -> Actions {
        let anchored_at = self.anchor;
        let (steps, qualified) = match event {
            SecondEvent::Captured { unaccounted: 0 } => (1, self.pulses.state().is_locked()),
            SecondEvent::Captured { unaccounted } => {
                self.divergences = self.divergences.saturating_add(1);
                ((1 + unaccounted).max(0) as u32, false)
            }
            SecondEvent::Missed => (1, false),
            SecondEvent::Switched => (1, false),
        };

        self.seconds = self.seconds.saturating_add(steps);
        self.last_second_ns = Some(monotonic_ns);
        for _ in 1..steps {
            self.references.on_edge(false);
        }
        let adopted = steps > 0 && self.references.on_edge(qualified);

        let mut resynchronized = adopted;
        if qualified {
            let target = self.pulses.target_edge();
            if target != self.anchor {
                self.anchor = target;
                resynchronized = true;
            }
        }

        let mut actions = if resynchronized {
            self.generation = self.generation.wrapping_add(1);
            self.acquisition.synchronization_changed()
        } else if let Some(counter_edge) = self.anchor {
            self.acquisition.on_edge(
                ScheduledEdge {
                    reference: self.references.active(),
                    counter_edge,
                },
                monotonic_ns,
            )
        } else {
            Actions::NONE
        };

        self.acquisition.advance_autostart(self.seconds);

        let phase_error_ns = self.pulses.phase_error_ns().unwrap_or(f32::NAN);
        actions.second = true;
        actions.correction_ppm = self.servo.as_mut().map(|s| s.observe(phase_error_ns));
        actions.set_edge = self.anchor.filter(|edge| Some(*edge) != anchored_at);
        actions.announce = self.anchor.map(|_| Announce {
            timeref: self.references.active(),
            pad: self.pad(),
        });
        self.report(actions)
    }

    /// What this device announces its time is worth: the poorer of what its
    /// pulses are worth and what upstream said.
    fn announced(&self) -> TimeStatus {
        let own = match self.pulses.state() {
            PulseState::Locked => TimeStatus::Locked,
            PulseState::Holdover { .. } => TimeStatus::Holdover,
            PulseState::FreeRunning | PulseState::Acquiring { .. } => TimeStatus::FreeRun,
        };
        own.worse(self.upstream)
    }

    /// The pad byte this device announces: its own second count and status.
    fn pad(&self) -> u8 {
        (self.seconds as u8 & SEQUENCE_MASK) | self.announced().bits()
    }

    /// Whether a packet's sequence follows the last one this source sent. The
    /// count is reset whenever the source changes, so a hub reboot is not a
    /// divergence, and a legacy hub sends no sequence to check.
    fn sequenced(&mut self, identity: ReferenceIdentity, status: TimeStatus, sequence: u8) -> bool {
        let previous = self.sequence.filter(|prev| prev.identity == identity);
        self.sequence = Some(Sequence {
            identity,
            sequence,
            seconds: self.seconds,
        });
        let (Some(previous), TimeStatus::FreeRun | TimeStatus::Holdover | TimeStatus::Locked) =
            (previous, status)
        else {
            return true;
        };
        let closed = self.seconds.wrapping_sub(previous.seconds) as u8 & SEQUENCE_MASK;
        if sequence.wrapping_sub(previous.sequence) & SEQUENCE_MASK == closed {
            return true;
        }
        self.divergences = self.divergences.saturating_add(1);
        self.references.discard_candidate();
        false
    }

    /// Report the status once per change of it, which is what a runtime logs
    /// and what rolls its segments over.
    fn report(&mut self, mut actions: Actions) -> Actions {
        let summary = Summary {
            pulse: discriminant(&self.pulses.state()),
            reference: discriminant(&self.references.state()),
            acquisition: self.acquisition.state(),
            traceable: self.traceable(),
        };
        if self.reported != Some(summary) {
            self.reported = Some(summary);
            actions.status_changed = Some(self.status());
        }
        actions
    }
}

#[cfg(test)]
mod synchronizer_tests {
    use super::*;

    const PERIOD: u32 = 1_000_000;
    const EDGE: u32 = 100;

    fn local() -> Reference {
        Reference {
            identity: ReferenceIdentity::new(Epoch::SYSTIME, SessionId::new(1), b"local"),
            second: 10,
        }
    }

    fn synchronizer() -> Synchronizer {
        Synchronizer::new(
            CounterDomain::new(PERIOD),
            PulseConfig::with_edge_tolerance(100),
            local(),
            Some(PhaseServo::new(PiConfig::SIT5356)),
            0,
        )
    }

    fn plain() -> Synchronizer {
        Synchronizer::new(
            CounterDomain::new(PERIOD),
            PulseConfig::with_edge_tolerance(100),
            local(),
            None,
            0,
        )
    }

    fn packet(time: u32) -> Timeref<'static> {
        Timeref {
            epoch: Epoch::UNIX,
            time,
            session: SessionId::new(99),
            serial: b"parent",
        }
    }

    /// A hub's pad byte: its second count and what its own time is worth.
    fn pad(sequence: u8, status: TimeStatus) -> u8 {
        (sequence & SEQUENCE_MASK) | status.bits()
    }

    /// One clean pulse per second, on the same counter edge.
    fn run(sync: &mut Synchronizer, seconds: u64) {
        for second in 0..seconds {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
        }
    }

    #[test]
    fn one_pulse_per_second_advances_the_timeline_once_per_second() {
        let mut sync = synchronizer();
        for second in 0..10 {
            let now = second * NANOS_PER_SECOND;
            assert!(!sync.poll(now).second);
            assert!(sync.capture(EDGE, now).second);
        }
        assert_eq!(sync.status().active.second, local().second + 10);
        assert_eq!(sync.status().pulse, PulseState::Locked);
        assert_eq!(sync.status().divergences, 0);
        assert_eq!(sync.status().counter_edge, Some(EDGE));
    }

    #[test]
    fn non_timeline_actions_do_not_manufacture_a_second_event() {
        let mut sync = synchronizer();
        let actions = sync.start().unwrap();
        assert!(!actions.second);
        assert_eq!(sync.status().acquisition, AcquisitionState::WaitingForEdge);
    }

    #[test]
    fn a_three_second_outage_with_polls_advances_exactly_three_seconds() {
        let mut sync = synchronizer();
        run(&mut sync, 4);
        let before = sync.status().active.second;

        for tenths in 1..40 {
            sync.poll(3 * NANOS_PER_SECOND + tenths * NANOS_PER_SECOND / 10);
        }
        assert_eq!(sync.status().active.second, before + 3);
        assert!(matches!(sync.status().pulse, PulseState::Holdover { .. }));

        sync.capture(EDGE, 7 * NANOS_PER_SECOND);
        assert_eq!(sync.status().active.second, before + 4);
        assert_eq!(sync.status().divergences, 0);
    }

    #[test]
    fn a_resumed_capture_without_polls_diverges_loudly_but_lands_correctly() {
        let mut sync = synchronizer();
        run(&mut sync, 4);
        let before = sync.status().active.second;

        sync.capture(EDGE, 7 * NANOS_PER_SECOND);
        assert_eq!(sync.status().active.second, before + 4);
        assert_eq!(sync.status().divergences, 1);
        assert_eq!(sync.status().counter_edge, Some(EDGE));

        let mut polled = synchronizer();
        run(&mut polled, 4);
        for tenths in 1..40 {
            polled.poll(3 * NANOS_PER_SECOND + tenths * NANOS_PER_SECOND / 10);
        }
        polled.capture(EDGE, 7 * NANOS_PER_SECOND);
        assert_eq!(polled.status().active.second, sync.status().active.second);
        assert_eq!(polled.status().divergences, 0);
    }

    #[test]
    fn a_spur_beside_the_second_boundary_does_not_skew_the_timeline() {
        let mut sync = synchronizer();
        run(&mut sync, 3);
        let before = sync.status().active.second;

        for second in 3..13u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now - NANOS_PER_SECOND / 100);
            sync.capture(PERIOD + EDGE - PERIOD / 100, now - NANOS_PER_SECOND / 100);
            sync.poll(now);
            sync.capture(EDGE, now);
        }
        assert_eq!(sync.status().active.second, before + 10);
        assert_eq!(sync.status().counter_edge, Some(EDGE));
        assert_eq!(sync.status().divergences, 0);
        assert_eq!(sync.status().generation, 1);
        assert_eq!(sync.status().spurs, 10);
    }

    #[test]
    fn packets_observed_between_edges_are_adopted_on_a_qualified_edge() {
        let mut sync = synchronizer();
        run(&mut sync, 3);
        assert_eq!(sync.status().reference, ReferenceState::Local);

        for second in 3..6u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
            sync.observe_packet(packet(500 + second as u32), 0, now);
        }
        let now = 6 * NANOS_PER_SECOND;
        sync.poll(now);
        sync.capture(EDGE, now);

        let status = sync.status();
        assert_eq!(status.reference, ReferenceState::Upstream);
        assert_eq!(status.active.second, 506);
        assert_eq!(status.active.identity.epoch, Epoch::UNIX);
        assert_eq!(status.generation, 2);
    }

    #[test]
    fn a_reference_change_restarts_a_running_acquisition() {
        let mut sync = synchronizer();
        run(&mut sync, 3);
        sync.start().unwrap();

        let mut second = 3u64;
        let mut plan = 0;
        loop {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            let actions = sync.capture(EDGE, now);
            second += 1;
            match actions.local {
                AcquisitionAction::Arm(staged) | AcquisitionAction::Rearm(staged) => {
                    plan = staged.id
                }
                AcquisitionAction::Start(_) => break,
                AcquisitionAction::None | AcquisitionAction::Disarm | AcquisitionAction::Stop => {
                    assert!(second < 8, "never started")
                }
            }
        }
        sync.mark_running(plan).unwrap();
        assert_eq!(sync.status().acquisition, AcquisitionState::Running);

        for _ in 0..3 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
            sync.observe_packet(packet(900 + second as u32), 0, now);
            second += 1;
        }
        let now = second * NANOS_PER_SECOND;
        sync.poll(now);
        let actions = sync.capture(EDGE, now);
        assert_eq!(actions.local, AcquisitionAction::Stop);
        sync.finish_stop().unwrap();
        assert_eq!(sync.status().acquisition, AcquisitionState::WaitingForEdge);
    }

    /// A healthy train never moves the drift-compensated anchor; a switchover
    /// does, and that jump must restart an acquisition.
    #[test]
    fn a_switchover_moves_the_anchor_and_restarts_the_segment() {
        let mut sync = synchronizer();
        run(&mut sync, 3);
        sync.start().unwrap();
        let generation = sync.status().generation;

        let alternate = PERIOD / 2;
        let mut switched = None;
        for second in 3..16u64 {
            let now = second * NANOS_PER_SECOND;
            sync.capture(alternate, now + NANOS_PER_SECOND / 2);
            sync.poll(now + NANOS_PER_SECOND);
            if sync.status().counter_edge == Some(alternate) {
                switched = Some(second);
                break;
            }
        }
        let second = switched.expect("no switchover");
        assert_eq!(sync.status().generation, generation + 1);
        assert_eq!(sync.status().acquisition, AcquisitionState::Stopping);
        assert_eq!(
            sync.status().active.second,
            local().second + second as u32 + 1
        );
        assert_eq!(sync.status().divergences, 0);
    }

    #[test]
    fn restart_rebases_the_anchor_without_a_generation_bump() {
        let mut sync = synchronizer();
        for second in 0..5u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE + 10 * second as u32, now);
        }
        let status = sync.status();
        assert_eq!(status.counter_edge, Some(EDGE));
        assert!(status.phase_error_ns.unwrap() > 0.0);

        sync.restart().unwrap();
        let after = sync.status();
        assert_eq!(after.counter_edge, Some(EDGE + 40));
        assert_eq!(after.phase_error_ns, Some(0.0));
        assert_eq!(after.generation, status.generation);
        assert_eq!(after.correction_ppm, status.correction_ppm);
    }

    #[test]
    fn a_board_with_no_actuator_never_reports_a_correction() {
        let mut sync = plain();
        run(&mut sync, 4);
        assert_eq!(sync.status().correction_ppm, None);
        assert!(sync.status().phase_error_ns.is_some());
    }

    #[test]
    fn a_bootstrapped_root_advances_and_acquires_without_any_pps() {
        let mut sync = plain();
        assert_eq!(sync.status().pulse, PulseState::FreeRunning);

        let bootstrapped = sync.bootstrap(100_000, 5_100_000_000);
        assert!(bootstrapped.second);
        assert_eq!(sync.status().divergences, 0);
        assert_eq!(sync.status().counter_edge, Some(100_000));
        assert_eq!(sync.status().active.second, 5);
        assert_eq!(sync.status().generation, 1);
        assert_eq!(bootstrapped.set_edge, Some(100_000));

        sync.start().unwrap();
        let mut started = false;
        for second in 6..12u64 {
            let actions = sync.poll(second * NANOS_PER_SECOND + 500_000_000);
            assert!(actions.second);
            assert_eq!(actions.set_edge, None);
            assert!(matches!(sync.status().pulse, PulseState::Holdover { .. }));
            assert_eq!(sync.bootstrap(0, second * NANOS_PER_SECOND), Actions::NONE);
            if let AcquisitionAction::Start(edge) = actions.local {
                assert_eq!(edge.counter_edge, 100_000);
                started = true;
            }
        }
        assert!(started, "a free-running board never started");
        assert_eq!(sync.status().active.second, 11);
    }

    /// Staging and target seconds can both fall inside one delayed poll. The
    /// board must still be handed a plan before it is told to start it.
    #[test]
    fn a_catchup_that_spans_a_staged_start_re_arms_instead() {
        let mut sync = plain();
        sync.bootstrap(100_000, 5_100_000_000);
        sync.start().unwrap();
        assert_eq!(sync.status().acquisition, AcquisitionState::WaitingForEdge);

        let actions = sync.poll(7_500_000_000);
        assert!(actions.second);
        let plan = match actions.local {
            AcquisitionAction::Rearm(plan) => plan,
            action => panic!("unexpected action: {action:?}"),
        };
        assert_eq!(sync.status().acquisition, AcquisitionState::Armed);
        assert_eq!(plan.target, plan.staging.next_second());

        let started = sync.poll(8_500_000_000);
        assert_eq!(started.local, AcquisitionAction::Start(plan.target));
        assert_eq!(sync.status().acquisition, AcquisitionState::Starting);
        sync.mark_running(plan.id).unwrap();
        assert_eq!(sync.status().acquisition, AcquisitionState::Running);
    }

    #[test]
    fn a_late_poll_reports_one_second_for_a_whole_catchup() {
        let mut sync = plain();
        sync.bootstrap(100_000, 5_100_000_000);

        let actions = sync.poll(9_500_000_000);
        assert!(actions.second);
        assert_eq!(
            sync.status().pulse,
            PulseState::Holdover { missed_pulses: 4 }
        );
        assert_eq!(sync.status().active.second, 9);
    }

    #[test]
    fn a_board_can_sleep_until_the_explicit_deadline() {
        let mut sync = plain();
        assert_eq!(sync.next_poll_deadline_ns(), None);

        sync.bootstrap(100_000, 5_100_000_000);
        let deadline = 6_110_000_001;
        assert_eq!(sync.next_poll_deadline_ns(), Some(deadline));
        assert_eq!(sync.poll(deadline - 1), Actions::NONE);
        assert!(sync.poll(deadline).second);
        assert_eq!(
            sync.status().pulse,
            PulseState::Holdover { missed_pulses: 1 }
        );
        assert_eq!(
            sync.next_poll_deadline_ns(),
            Some(deadline + NANOS_PER_SECOND)
        );
    }

    #[test]
    fn an_unanchored_board_wakes_now_and_then_tracks_the_pulse_deadline() {
        let mut sync = plain();

        assert_eq!(sync.next_wake_deadline_ns(5_100_000_000), 5_100_000_000);
        assert_eq!(sync.next_wake_deadline_ns(5_600_000_000), 5_600_000_000);

        assert!(sync.wake(100_000, 5_600_000_000).second);
        assert_eq!(sync.status().active.second, 6);
        assert_eq!(sync.status().counter_edge, Some(100_000));
        assert_eq!(
            sync.next_wake_deadline_ns(5_600_000_000),
            sync.next_poll_deadline_ns().unwrap()
        );
    }

    /// Autostart is timed by counted second events, not by a second clock.
    #[test]
    fn autostart_zero_goes_idle_and_nonzero_starts_after_its_delay() {
        let mut disabled = synchronizer();
        run(&mut disabled, 1);
        assert_eq!(disabled.status().acquisition, AcquisitionState::Idle);

        let mut enabled = Synchronizer::new(
            CounterDomain::new(PERIOD),
            PulseConfig::with_edge_tolerance(100),
            local(),
            None,
            4,
        );
        run(&mut enabled, 4);
        assert_eq!(enabled.status().acquisition, AcquisitionState::Autostart);

        let now = 4 * NANOS_PER_SECOND;
        enabled.poll(now);
        enabled.capture(EDGE, now);
        assert_eq!(
            enabled.status().acquisition,
            AcquisitionState::WaitingForEdge
        );

        let staged = (5..10u64).any(|second| {
            let now = second * NANOS_PER_SECOND;
            enabled.poll(now);
            matches!(
                enabled.capture(EDGE, now).local,
                AcquisitionAction::Arm(_) | AcquisitionAction::Rearm(_)
            )
        });
        assert!(staged, "autostart never staged an acquisition");
    }

    #[test]
    fn local_bootstrap_replaces_an_unqualified_startup_edge() {
        let mut sync = plain();

        sync.capture(123, 5_500_000_000);
        assert_eq!(sync.status().counter_edge, None);
        assert_eq!(sync.next_wake_deadline_ns(5_500_000_000), 5_500_000_000);

        let actions = sync.wake(500_000, 6_000_000_000);
        assert!(actions.second);
        assert_eq!(sync.status().divergences, 0);
        assert_eq!(sync.status().counter_edge, Some(500_000));
        assert_eq!(sync.status().active.second, 6);
        assert_eq!(sync.next_poll_deadline_ns(), Some(7_010_000_001));
    }

    /// D4: a packet is paired with the second it labels by its arrival, and a
    /// packet that missed that window labels nothing.
    #[test]
    fn a_packet_outside_its_seconds_window_is_dropped_and_counted() {
        let mut sync = synchronizer();
        run(&mut sync, 4);

        sync.observe_packet(packet(500), 0, 3 * NANOS_PER_SECOND + 899_999_999);
        assert_eq!(
            sync.status().reference,
            ReferenceState::Candidate { confirmations: 1 }
        );
        assert_eq!(sync.status().late_references, 0);

        sync.observe_packet(packet(501), 0, 3 * NANOS_PER_SECOND + 900_000_000);
        assert_eq!(sync.status().late_references, 1);
        assert_eq!(
            sync.status().reference,
            ReferenceState::Candidate { confirmations: 1 }
        );
    }

    /// A packet before the first second event has no window to fall in.
    #[test]
    fn a_packet_before_any_second_event_is_counted_late() {
        let mut sync = synchronizer();
        sync.observe_packet(packet(500), 0, 0);
        assert_eq!(sync.status().late_references, 1);
        assert_eq!(sync.status().reference, ReferenceState::Local);
    }

    /// D5: the sequence in the pad byte catches a slip that the arrival
    /// window cannot, and wrapping at 64 is not one.
    #[test]
    fn a_sequence_that_skipped_discards_the_candidate() {
        let mut sync = synchronizer();
        run(&mut sync, 3);

        for second in 3..5u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
            sync.observe_packet(
                packet(500 + second as u32),
                pad(second as u8, TimeStatus::Locked),
                now,
            );
        }
        assert_eq!(
            sync.status().reference,
            ReferenceState::Candidate { confirmations: 2 }
        );

        let now = 5 * NANOS_PER_SECOND;
        sync.poll(now);
        sync.capture(EDGE, now);
        sync.observe_packet(packet(505), pad(9, TimeStatus::Locked), now);
        assert_eq!(sync.status().reference, ReferenceState::Local);
        assert_eq!(sync.status().divergences, 1);
        assert_eq!(sync.status().upstream, TimeStatus::Locked);
    }

    #[test]
    fn a_sequence_wrapping_at_sixty_four_is_not_a_slip() {
        let mut sync = synchronizer();
        run(&mut sync, 3);

        let mut sequence = 62u8;
        for second in 3..7u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
            sync.observe_packet(
                packet(500 + second as u32),
                pad(sequence, TimeStatus::Locked),
                now,
            );
            sequence = sequence.wrapping_add(1);
        }
        assert_eq!(sync.status().reference, ReferenceState::Upstream);
        assert_eq!(sync.status().divergences, 0);
    }

    /// A legacy hub sends no sequence, so there is nothing to check.
    #[test]
    fn a_legacy_hub_is_paired_by_its_arrival_alone() {
        let mut sync = synchronizer();
        run(&mut sync, 3);

        for second in 3..7u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
            sync.observe_packet(packet(500 + second as u32), pad(7, TimeStatus::Legacy), now);
        }
        assert_eq!(sync.status().reference, ReferenceState::Upstream);
        assert_eq!(sync.status().divergences, 0);
        assert_eq!(sync.status().upstream, TimeStatus::Legacy);
    }

    /// R7: a hub reboot restarts its sequence from zero, and that is not a
    /// divergence because the source is a new one.
    #[test]
    fn a_new_source_starts_its_sequence_over() {
        let mut sync = synchronizer();
        run(&mut sync, 3);
        let now = 3 * NANOS_PER_SECOND;
        sync.poll(now);
        sync.capture(EDGE, now);
        sync.observe_packet(packet(500), pad(40, TimeStatus::Locked), now);

        let rebooted = Timeref {
            session: SessionId::new(100),
            ..packet(700)
        };
        let now = 4 * NANOS_PER_SECOND;
        sync.poll(now);
        sync.capture(EDGE, now);
        sync.observe_packet(rebooted, pad(0, TimeStatus::Locked), now);
        assert_eq!(sync.status().divergences, 0);
        assert_eq!(
            sync.status().reference,
            ReferenceState::Candidate { confirmations: 1 }
        );
    }

    /// D7: a child stays on its parent's timeline through any outage; only a
    /// new session ever takes it off.
    #[test]
    fn a_child_keeps_its_parents_identity_through_a_thousand_missed_pulses() {
        let mut sync = synchronizer();
        run(&mut sync, 3);
        for second in 3..7u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
            sync.observe_packet(packet(500 + second as u32), 0, now);
        }
        let adopted = sync.status();
        assert_eq!(adopted.reference, ReferenceState::Upstream);

        for second in 7..1007u64 {
            sync.poll(second * NANOS_PER_SECOND + NANOS_PER_SECOND / 2);
        }
        let after = sync.status();
        assert_eq!(after.reference, ReferenceState::Upstream);
        assert_eq!(after.active.identity, adopted.active.identity);
        assert_eq!(after.active.second, adopted.active.second + 1000);
        assert_eq!(after.pulse, PulseState::Holdover { missed_pulses: 255 });
        assert!(!after.traceable);
    }

    /// D8: what the recording is flagged by, and what tells the runtime to
    /// roll a segment over.
    #[test]
    fn traceability_follows_the_pulses_and_is_reported_when_it_changes() {
        let mut sync = synchronizer();
        assert!(
            sync.traceable(),
            "the epoch already says a systime device keeps its own time"
        );

        for second in 3..7u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
            sync.observe_packet(packet(500 + second as u32), pad(0, TimeStatus::Legacy), now);
        }
        assert_eq!(sync.status().reference, ReferenceState::Upstream);
        assert!(sync.traceable());

        let lost = (7..10u64)
            .map(|second| sync.poll(second * NANOS_PER_SECOND + NANOS_PER_SECOND / 2))
            .find_map(|actions| actions.status_changed)
            .expect("holdover is a status change");
        assert!(!lost.traceable);
        assert_eq!(lost.pulse, PulseState::Holdover { missed_pulses: 1 });

        let mut hub = synchronizer();
        run(&mut hub, 3);
        for second in 3..7u64 {
            let now = second * NANOS_PER_SECOND;
            hub.poll(now);
            hub.capture(EDGE, now);
            hub.observe_packet(
                packet(500 + second as u32),
                pad(second as u8, TimeStatus::Holdover),
                now,
            );
        }
        assert_eq!(hub.status().pulse, PulseState::Locked);
        assert!(!hub.traceable());

        let mut child = synchronizer();
        run(&mut child, 3);
        for second in 3..7u64 {
            let now = second * NANOS_PER_SECOND;
            child.poll(now);
            child.capture(EDGE, now);
            child.observe_packet(
                Timeref {
                    epoch: Epoch::SYSTIME,
                    ..packet(500 + second as u32)
                },
                0,
                now,
            );
        }
        assert_eq!(child.status().reference, ReferenceState::Upstream);
        assert!(child.traceable());
        child.poll(8 * NANOS_PER_SECOND);
        assert!(!child.traceable());
    }

    /// D6: a GPS second qualifies like a packet, and its pad byte then carries
    /// the poorer of the two statuses down the chain.
    #[test]
    fn a_hub_announces_the_poorer_of_its_own_status_and_upstreams() {
        let mut sync = synchronizer();
        run(&mut sync, 3);
        for second in 3..7u64 {
            let now = second * NANOS_PER_SECOND;
            sync.poll(now);
            sync.capture(EDGE, now);
            sync.observe_local(1_700_000_000 + second as u32, Epoch::UNIX);
        }
        let now = 7 * NANOS_PER_SECOND;
        sync.poll(now);
        let announced = sync
            .capture(EDGE, now)
            .announce
            .expect("an anchored device announces every second");
        assert_eq!(sync.status().reference, ReferenceState::Upstream);
        assert_eq!(announced.timeref.second, 1_700_000_007);
        assert_eq!(TimeStatus::from_pad(announced.pad), TimeStatus::Locked);
        assert_eq!(announced.pad & SEQUENCE_MASK, 8);

        sync.observe_packet(packet(1_700_000_007), pad(1, TimeStatus::Holdover), now);
        let now = 8 * NANOS_PER_SECOND;
        sync.poll(now);
        let announced = sync.capture(EDGE, now).announce.expect("an announcement");
        assert_eq!(TimeStatus::from_pad(announced.pad), TimeStatus::Holdover);
    }

    #[test]
    fn a_status_worse_than_holdover_wins_and_legacy_defers() {
        assert_eq!(
            TimeStatus::Locked.worse(TimeStatus::Holdover),
            TimeStatus::Holdover
        );
        assert_eq!(
            TimeStatus::Holdover.worse(TimeStatus::FreeRun),
            TimeStatus::FreeRun
        );
        assert_eq!(
            TimeStatus::Locked.worse(TimeStatus::Legacy),
            TimeStatus::Locked
        );
        assert!(TimeStatus::Legacy.traceable() && TimeStatus::Locked.traceable());
        assert!(!TimeStatus::FreeRun.traceable() && !TimeStatus::Holdover.traceable());
        assert_eq!(
            TimeStatus::from_pad(TimeStatus::Holdover.bits() | 5),
            TimeStatus::Holdover
        );
    }
}

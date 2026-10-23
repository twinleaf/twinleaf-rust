//! Electrical PPS qualification and phase measurement.
//!
//! Each pulse train is a three-state Kalman tracker of the edge's phase and the
//! oscillator's frequency offset and drift, gating every edge on its innovation. A main
//! train rejects spurs and measures phase; a tentative train, fed the main
//! train's rejected edges, takes over when they prove a coherent train of
//! their own.

use super::CounterDomain;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Accepted edges before a train can lock: two always fit a phase and a
/// frequency exactly, so the third is the first that tests them.
const ACQUISITION_RUN: u32 = 3;

/// Counter ticks in `ns` nanoseconds of a timer running at `timer_hz`.
pub const fn ticks(ns: u32, timer_hz: u32) -> u32 {
    (ns as u64 * timer_hz as u64 / NANOS_PER_SECOND) as u32
}

/// PPS tracker configuration: the gate, the oscillator model, and the priors a
/// new train starts from.
#[derive(Debug, Clone, Copy)]
pub struct PulseConfig {
    /// Grace after the expected second before declaring a pulse missing.
    pub miss_grace_ns: u32,
    /// Innovations accepted, in standard deviations of the predicted edge.
    pub gate_sigmas: f64,
    /// Hard ceiling on the gate however long the holdover, under half a second.
    pub max_gate_ns: u32,
    /// Frequency offset a new train may have from the oscillator, ppb (1σ).
    pub frequency_prior_ppb: f64,
    /// Edge noise a new train is assumed to have until its innovations say, ns (1σ).
    pub edge_noise_prior_ns: f64,
    /// White frequency noise of the oscillator, ns²/s.
    pub white_fm: f64,
    /// Random-walk frequency noise of the oscillator, ns²/s³.
    pub random_walk_fm: f64,
    /// Frequency drift a new train may have from the oscillator, ppb/s (1σ).
    pub drift_prior_ppb_per_second: f64,
    /// How far the innovations' fading mean may stray, in standard deviations
    /// of its own spread, before the covariance is widened to follow.
    pub bias_sigmas: f64,
    /// Seconds of innovations the edge-noise estimate remembers.
    pub noise_memory_seconds: f64,
    /// Factor a rejected edge grows the edge-noise estimate by, so a train
    /// noisier than assumed is still acquired.
    pub rejected_noise_growth: f64,
}

impl PulseConfig {
    /// A TCXO-class oscillator, σy(1 s) = 1e-9 white and σy(100 s) ≈ 6e-10
    /// random walk, following a source within ±50 ppm of it drifting ~10 ppb/s.
    pub const DEFAULT: Self = Self {
        miss_grace_ns: 10_000_000,
        gate_sigmas: 4.0,
        max_gate_ns: 100_000_000,
        frequency_prior_ppb: 50_000.0,
        edge_noise_prior_ns: 1_000.0,
        white_fm: 1.0,
        random_walk_fm: 0.01,
        drift_prior_ppb_per_second: 10.0,
        bias_sigmas: 3.0,
        noise_memory_seconds: 32.0,
        rejected_noise_growth: 2.0,
    };
}

/// Qualification state of the electrical pulse train.
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PulseState {
    /// No external pulse train has been established.
    FreeRunning,
    /// A plausible pulse train whose frequency is not yet measured and
    /// tested, or one just switched onto that has closed no period since.
    Acquiring {
        /// Pulses accepted since the train was established or resumed.
        good_pulses: u8,
    },
    /// The train's frequency is measured and it is steered to with no
    /// re-anchor pending, as linuxptp's servo is locked.
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
    /// The expected pulse did not arrive, or arrived outside the gate.
    Missed,
    /// A coherent alternate train replaced the missing main train. The swap
    /// itself closes this many seconds, the nearest to the new train's phase.
    Switched { seconds: u32 },
}

/// The main train's estimate at the second just closed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PhaseEstimate {
    /// Phase of the edge from the anchor, ns.
    pub phase_ns: f32,
    /// Frequency offset of the unsteered oscillator from the train over the coming second, ppb.
    pub frequency_ppb: f32,
    /// Expected RMS of the phase, `√(θ² + P₀₀)`, 1σ: how far the counter's second is off the train's, ns.
    pub time_error_ns: u64,
    /// Bound on the mean phase, `|θ| + 2√P₀₀`, as IEEE 1588-2019 §7.6.2.6 asks of a clockAccuracy, ns.
    pub accuracy_ns: u64,
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
    /// Seconds closed on the prediction because their edges fell outside the gate.
    Coasting { missed: u32 },
    /// A qualified train whose pulses are currently absent.
    Lapsed { missed: u32 },
}

/// Phase `θ` (ns), unsteered frequency offset `y` (ns/s) and its drift `d`
/// (ns/s²) of a train, their covariance, and its own edge-noise variance `r` (ns²).
#[derive(Debug, Clone, Copy)]
struct Filter {
    x: [f64; 3],
    p: [[f64; 3]; 3],
    r: f64,
    innovations: f64,
    /// The last innovation, and the innovations' fading mean, ns.
    innovation: f64,
    bias: f64,
    /// Integrated steering at the last update, ns.
    steered: f64,
}

impl Filter {
    const EMPTY: Self = Self {
        x: [0.0; 3],
        p: [[0.0; 3]; 3],
        r: 0.0,
        innovations: 0.0,
        innovation: 0.0,
        bias: 0.0,
        steered: 0.0,
    };

    fn fresh(config: PulseConfig, floor: f64, steered: f64) -> Self {
        let r = (config.edge_noise_prior_ns * config.edge_noise_prior_ns).max(floor);
        let (y, d) = (
            config.frequency_prior_ppb,
            config.drift_prior_ppb_per_second,
        );
        Self {
            p: [[r, 0.0, 0.0], [0.0, y * y, 0.0], [0.0, 0.0, d * d]],
            r,
            innovations: 1.0,
            steered,
            ..Self::EMPTY
        }
    }

    /// Propagated `n` seconds, under the steering integrated to `steered`.
    fn predicted(self, config: PulseConfig, n: f64, steered: f64) -> Self {
        let f = [[1.0, n, n * n / 2.0], [0.0, 1.0, n], [0.0, 0.0, 1.0]];
        let (white, walk) = (config.white_fm, config.random_walk_fm);
        let q = [
            [white * n + walk * n * n * n / 3.0, walk * n * n / 2.0, 0.0],
            [walk * n * n / 2.0, walk * n, 0.0],
            [0.0; 3],
        ];
        let fp: [[f64; 3]; 3] = core::array::from_fn(|i| {
            core::array::from_fn(|j| (0..3).map(|k| f[i][k] * self.p[k][j]).sum())
        });
        let mut x: [f64; 3] = core::array::from_fn(|i| (0..3).map(|k| f[i][k] * self.x[k]).sum());
        x[0] += steered - self.steered;
        Self {
            x,
            p: core::array::from_fn(|i| {
                core::array::from_fn(|j| (0..3).map(|k| fp[i][k] * f[j][k]).sum::<f64>() + q[i][j])
            }),
            steered,
            ..self
        }
    }

    /// Updated by the phase `z` inside the gate, its covariance first widened by how far the
    /// innovations' mean has strayed; `r` is half the squared change of innovation, less `P₀₀⁻`.
    fn updated(self, config: PulseConfig, floor: f64, z: f64) -> Option<Self> {
        let innovation = z - self.x[0];
        let s = self.p[0][0] + self.r;
        let gate = (config.gate_sigmas * config.gate_sigmas * s)
            .min(config.max_gate_ns as f64 * config.max_gate_ns as f64);
        if innovation * innovation > gate {
            return None;
        }
        let innovations = (self.innovations + 1.0).min(config.noise_memory_seconds);
        let weight = 1.0 / innovations;
        let bias = (1.0 - weight) * self.bias + weight * innovation;
        let spread = s * weight / (2.0 - weight);
        let widening = (bias * bias / (config.bias_sigmas * config.bias_sigmas * spread)).max(1.0);
        let widened: [[f64; 3]; 3] =
            core::array::from_fn(|i| core::array::from_fn(|j| self.p[i][j] * widening));
        let s = widened[0][0] + self.r;
        let k: [f64; 3] = core::array::from_fn(|i| widened[i][0] / s);
        let change = innovation - self.innovation;
        Some(Self {
            x: core::array::from_fn(|i| self.x[i] + k[i] * innovation),
            p: core::array::from_fn(|i| {
                core::array::from_fn(|j| widened[i][j] - k[i] * widened[0][j])
            }),
            r: ((1.0 - weight) * self.r + weight * (change * change / 2.0 - self.p[0][0]))
                .max(floor),
            innovations,
            innovation,
            bias,
            ..self
        })
    }
}

/// One raw edge, as every train sees it.
#[derive(Debug, Clone, Copy)]
struct Edge {
    /// Displacement from the previous raw edge, ticks.
    delta: i64,
    /// Seconds the monotonic clock puts since the previous raw edge.
    seconds: u32,
    counter: u32,
    at_ns: u64,
    /// Phase the corrections have moved the counter by `at_ns`.
    steered: f64,
}

#[derive(Debug, Clone, Copy)]
struct Train {
    mode: Mode,
    /// Whether an outside pulse has been accepted: the synthetic bootstrap
    /// edge anchors the counter without establishing a train.
    external: bool,
    /// Displacement since this train's last accepted pulse, whole seconds plus
    /// a residual under half a period. It accumulates across rejected spurs.
    pending_seconds: u32,
    pending_delta: i64,
    /// Whether an edge since the last closed second was not this train's.
    rejected: bool,
    /// Pulses accepted since the train was established or resumed.
    run: u32,
    last_delta: i64,
    phase_error: i64,
    last_edge: u32,
    miss_timeout_ns: u64,
    filter: Filter,
}

impl Train {
    const fn new() -> Self {
        Self {
            mode: Mode::Empty,
            external: false,
            pending_seconds: 0,
            pending_delta: 0,
            rejected: false,
            run: 0,
            last_delta: 0,
            phase_error: 0,
            last_edge: 0,
            miss_timeout_ns: 0,
            filter: Filter::EMPTY,
        }
    }

    const fn is_valid(&self) -> bool {
        !matches!(self.mode, Mode::Empty)
    }

    /// Seconds closed since the last accepted pulse.
    const fn unclosed(&self) -> u32 {
        match self.mode {
            Mode::Empty | Mode::Fresh | Mode::Tracking => 0,
            Mode::Coasting { missed } | Mode::Lapsed { missed } => missed,
        }
    }

    /// When the second just closed was due.
    const fn closed_at_ns(&self, config: PulseConfig) -> u64 {
        self.miss_timeout_ns
            .saturating_sub(NANOS_PER_SECOND + config.miss_grace_ns as u64)
    }

    /// The filter at the second just closed.
    fn estimate(&self, config: PulseConfig, steered: f64) -> Filter {
        self.filter
            .predicted(config, self.unclosed() as f64, steered)
    }

    /// linuxptp's servo locked: a run long enough to have measured the
    /// frequency and tested it, with a period closed since any switch onto it.
    const fn is_locked(&self) -> bool {
        matches!(self.mode, Mode::Tracking | Mode::Coasting { .. }) && self.run >= ACQUISITION_RUN
    }

    /// Returns the seconds this pulse accounts for beyond the one it closes,
    /// or `None` if it was rejected as spurious.
    fn process_pulse(
        &mut self,
        domain: CounterDomain,
        config: PulseConfig,
        edge: Edge,
    ) -> Option<i64> {
        let Edge {
            delta,
            seconds,
            counter: pulse_edge,
            at_ns: pulse_time_ns,
            steered,
        } = edge;
        let floor = domain.ticks_to_ns(1) * domain.ticks_to_ns(1);
        if !self.is_valid() {
            *self = Self {
                filter: Filter::fresh(config, floor, steered),
                ..Self::new()
            };
        } else {
            let mut pending_seconds = self.pending_seconds.saturating_add(seconds);
            let mut pending_delta = self.pending_delta.saturating_add(delta);
            let period = domain.period() as i64;
            let half = period / 2;
            if pending_delta >= half {
                pending_seconds = pending_seconds.saturating_add(1);
                pending_delta -= period;
            } else if pending_delta < -half {
                pending_seconds = pending_seconds.saturating_sub(1);
                pending_delta += period;
            }
            (self.pending_seconds, self.pending_delta) = (pending_seconds, pending_delta);

            let phase = domain.ticks_to_ns(self.phase_error.saturating_add(pending_delta));
            if pending_seconds == 0 {
                self.rejected = true;
                return None;
            }
            let predicted = self
                .filter
                .predicted(config, pending_seconds as f64, steered);
            let Some(filter) = predicted.updated(config, floor, phase) else {
                let ceiling = config.max_gate_ns as f64 / config.gate_sigmas;
                self.filter.r =
                    (self.filter.r * config.rejected_noise_growth).min(ceiling * ceiling);
                self.rejected = true;
                return None;
            };
            self.filter = filter;
        }

        let accounted = match self.mode {
            Mode::Empty => 0,
            Mode::Fresh | Mode::Tracking => 1,
            Mode::Coasting { missed } | Mode::Lapsed { missed } => missed as i64 + 1,
        };
        let unaccounted = self.pending_seconds as i64 - accounted;
        self.mode = match self.mode {
            Mode::Empty => Mode::Fresh,
            Mode::Fresh | Mode::Tracking | Mode::Coasting { .. } | Mode::Lapsed { .. } => {
                Mode::Tracking
            }
        };
        self.external = true;
        self.rejected = false;
        self.run = self.run.saturating_add(1);
        self.last_delta = self.pending_delta;
        self.phase_error = self.phase_error.saturating_add(self.pending_delta);
        self.pending_seconds = 0;
        self.pending_delta = 0;
        self.last_edge = pulse_edge;
        self.miss_timeout_ns = pulse_time_ns
            .saturating_add(NANOS_PER_SECOND)
            .saturating_add(config.miss_grace_ns as u64);
        Some(unaccounted)
    }

    /// Account for one expected pulse that did not arrive, or arrived outside
    /// the gate. Callers only invoke this on a train that has qualified once.
    fn process_miss(&mut self) {
        self.mode = match (self.mode, self.rejected) {
            (Mode::Empty | Mode::Fresh | Mode::Tracking, true) => Mode::Coasting { missed: 1 },
            (Mode::Coasting { missed }, true) => Mode::Coasting {
                missed: missed.saturating_add(1),
            },
            (Mode::Empty | Mode::Fresh | Mode::Tracking, false) => Mode::Lapsed { missed: 1 },
            (Mode::Coasting { missed } | Mode::Lapsed { missed }, false)
            | (Mode::Lapsed { missed }, true) => Mode::Lapsed {
                missed: missed.saturating_add(1),
            },
        };
        if let Mode::Lapsed { .. } = self.mode {
            self.run = 0;
        }
        self.rejected = false;
        self.miss_timeout_ns = self.miss_timeout_ns.saturating_add(NANOS_PER_SECOND);
    }

    /// Anchor on the latest accepted edge, keeping the estimate.
    fn rebase(&mut self, domain: CounterDomain) {
        self.filter.x[0] -= domain.ticks_to_ns(self.phase_error);
        self.phase_error = 0;
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
    /// The correction in force, ppb, since `steered_at_ns`.
    steering_ppb: f64,
    /// Phase the corrections have moved the counter by `steered_at_ns`.
    steered_ns: f64,
    steered_at_ns: u64,
}

impl PulseTracker {
    /// # Panics
    ///
    /// Both are board constants. A gate of half a second or more would make
    /// every pulse in the domain qualify.
    pub(crate) const fn new(domain: CounterDomain, config: PulseConfig) -> Self {
        assert!(
            config.max_gate_ns < NANOS_PER_SECOND as u32 / 2,
            "the gate must be under half a second"
        );
        assert!(
            config.gate_sigmas > 0.0
                && config.frequency_prior_ppb > 0.0
                && config.edge_noise_prior_ns >= 0.0
                && config.white_fm >= 0.0
                && config.random_walk_fm >= 0.0
                && config.drift_prior_ppb_per_second >= 0.0
                && config.bias_sigmas > 0.0
                && config.noise_memory_seconds >= 1.0
                && config.rejected_noise_growth >= 1.0,
            "pulse filter parameters"
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
            steering_ppb: 0.0,
            steered_ns: 0.0,
            steered_at_ns: 0,
        }
    }

    pub(crate) const fn domain(&self) -> CounterDomain {
        self.domain
    }

    pub(crate) fn state(&self) -> PulseState {
        if !self.main.external {
            return PulseState::FreeRunning;
        }
        match self.main.mode {
            Mode::Empty => PulseState::FreeRunning,
            Mode::Lapsed { missed } => PulseState::Holdover {
                missed_pulses: missed.min(u8::MAX as u32) as u8,
            },
            Mode::Fresh | Mode::Tracking | Mode::Coasting { .. } if self.main.is_locked() => {
                PulseState::Locked
            }
            Mode::Fresh | Mode::Tracking | Mode::Coasting { .. } => PulseState::Acquiring {
                good_pulses: self.main.run.min(u8::MAX as u32) as u8,
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
            .then(|| self.domain.ticks_to_ns(self.main.phase_error) as f32)
    }

    /// The main train's estimate at the second just closed, while an outside
    /// train is established.
    pub(crate) fn estimate(&self) -> Option<PhaseEstimate> {
        (self.main.external && self.main.is_valid()).then(|| {
            let steered = self.steered(self.main.closed_at_ns(self.config));
            let Filter { x, p, .. } = self.main.estimate(self.config, steered);
            PhaseEstimate {
                phase_ns: x[0] as f32,
                frequency_ppb: (x[1] + x[2] / 2.0) as f32,
                time_error_ns: ((x[0] * x[0] + p[0][0]) as u64).isqrt(),
                accuracy_ns: x[0].abs() as u64 + 2 * (p[0][0] as u64).isqrt(),
            }
        })
    }

    /// Put `correction_ppm` in force from `monotonic_ns`: the filters predict
    /// with it as a known control input.
    pub(crate) fn steer(&mut self, correction_ppm: f32, monotonic_ns: u64) {
        self.steered_ns = self.steered(monotonic_ns);
        self.steered_at_ns = self.steered_at_ns.max(monotonic_ns);
        self.steering_ppb = correction_ppm as f64 * 1_000.0;
    }

    /// Phase the corrections have moved the counter by `monotonic_ns`.
    fn steered(&self, monotonic_ns: u64) -> f64 {
        self.steered_ns
            + self.steering_ppb * monotonic_ns.saturating_sub(self.steered_at_ns) as f64 * 1e-9
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
        let event = self
            .capture(pulse_edge, monotonic_ns)
            .expect("an empty pulse tracker accepts its first edge");
        self.main.external = false;
        event
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
        let edge = Edge {
            delta,
            seconds,
            counter: pulse_edge,
            at_ns: monotonic_ns,
            steered: self.steered(monotonic_ns),
        };

        match self.main.process_pulse(self.domain, self.config, edge) {
            Some(unaccounted) => {
                self.tentative.rejected |= self.tentative.is_valid();
                Some(SecondEvent::Captured { unaccounted })
            }
            None => {
                self.spurs = self.spurs.saturating_add(1);
                self.tentative.process_pulse(self.domain, self.config, edge);
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
            if self.tentative.rejected {
                self.tentative = Train::new();
            } else {
                self.tentative.process_miss();
            }
        }

        if !missed {
            return None;
        }
        if !self.tentative.is_locked() {
            return Some(SecondEvent::Missed);
        }
        let seconds = (self
            .tentative
            .miss_timeout_ns
            .saturating_add(NANOS_PER_SECOND)
            .saturating_sub(self.main.miss_timeout_ns)
            .saturating_add(NANOS_PER_SECOND / 2)
            / NANOS_PER_SECOND) as u32;
        self.main = Train {
            mode: Mode::Fresh,
            rejected: false,
            ..self.tentative
        };
        self.main.rebase(self.domain);
        self.tentative = Train::new();
        Some(SecondEvent::Switched { seconds })
    }

    /// Move the phase anchor to the latest observed edge while preserving all
    /// pulse qualification counters. Frequency discipline is separate and
    /// retains its learned correction.
    pub(crate) fn rebase(&mut self) {
        self.main.rebase(self.domain);
    }

    /// Forget the alternate train, so a switchover qualifies only on pulses after this.
    pub(crate) fn discard_tentative(&mut self) {
        self.tentative = Train::new();
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
pub(super) mod tests {
    use super::*;

    const PERIOD: u32 = 1_000_000;

    fn new_tracker() -> PulseTracker {
        PulseTracker::new(CounterDomain::new(PERIOD), PulseConfig::DEFAULT)
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
    fn a_duration_in_nanoseconds_is_counted_in_ticks() {
        assert_eq!(ticks(50, 98_304_000), 4);
        assert_eq!(ticks(1_000, 98_304_000), 98);
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
    fn a_coherent_alternate_train_takes_over_after_enough_misses_and_locks_on_its_next_edge() {
        let mut tracker = new_tracker();
        for second in 0..3u64 {
            tracker.capture(100, second * 1_000_000_000).unwrap();
        }
        for second in 3..12u64 {
            let now = second * 1_000_000_000;
            assert_eq!(tracker.capture(500_000, now + 500_000_000), None);
            while let Some(event) = tracker.poll(now + 1_000_000_000) {
                if let SecondEvent::Switched { .. } = event {
                    assert_eq!(tracker.target_edge(), Some(500_000));
                    assert!(!tracker.state().is_locked());
                    tracker.capture(500_000, now + 1_500_000_000).unwrap();
                    assert_eq!(tracker.state(), PulseState::Locked);
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

    #[test]
    fn a_bootstrap_edge_never_establishes_a_train() {
        let mut tracker = new_tracker();
        tracker.bootstrap(500_000, 0);
        assert_eq!(tracker.state(), PulseState::FreeRunning);
        assert_eq!(tracker.poll(1_500_000_000), Some(SecondEvent::Missed));
        assert_eq!(tracker.state(), PulseState::FreeRunning);

        assert_eq!(captured(tracker.capture(500_000, 2_020_000_000)), 0);
        assert_eq!(tracker.state(), PulseState::Acquiring { good_pulses: 1 });
    }

    /// Random pulse environments, held to the tracker's own invariants.
    pub(in crate::sync) mod scenarios {
        use super::*;

        const SEEDS: u64 = 24;
        const STEPS: u32 = 2000;

        /// xorshift64: deterministic, dependency-free, good enough to shuffle
        /// event order.
        pub(in crate::sync) struct Rng(u64);

        impl Rng {
            pub(in crate::sync) fn new(seed: u64) -> Self {
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

            /// Standard normal, by Box-Muller.
            pub(in crate::sync) fn gaussian(&mut self) -> f64 {
                let mut uniform = || (self.next() >> 11) as f64 / (1u64 << 53) as f64;
                let (u, v) = (1.0 - uniform(), uniform());
                (-2.0 * u.ln()).sqrt() * (core::f64::consts::TAU * v).cos()
            }
        }

        /// One tracker under the scenario's inputs, with the coverage counters
        /// that prove the interesting transitions were reached.
        struct Harness {
            tracker: PulseTracker,
            /// Whether an edge has been taken, so a target edge is owed.
            anchored: bool,
            /// Whether an outside train is in force, so free-running is over.
            adopted: bool,
            captures: u32,
            spurs: u32,
            misses: u32,
            switchovers: u32,
            resumes: u32,
        }

        impl Harness {
            fn new() -> Self {
                Self {
                    tracker: PulseTracker::new(CounterDomain::new(PERIOD), PulseConfig::DEFAULT),
                    anchored: false,
                    adopted: false,
                    captures: 0,
                    spurs: 0,
                    misses: 0,
                    switchovers: 0,
                    resumes: 0,
                }
            }

            /// An accepted pulse anchors the train on its edge; a rejected one
            /// leaves the anchor and the reported phase exactly where they were.
            fn capture(&mut self, edge: u32, now: u64) {
                let lapsed = matches!(self.tracker.state(), PulseState::Holdover { .. });
                let before = (self.tracker.main.last_edge, self.tracker.target_edge());
                match self.tracker.capture(edge, now) {
                    Some(SecondEvent::Captured { .. }) => {
                        self.captures += 1;
                        self.resumes += u32::from(lapsed);
                        (self.anchored, self.adopted) = (true, true);
                        assert_eq!(
                            self.tracker.main.last_edge, edge,
                            "an accepted pulse anchors on its edge at {now} ns"
                        );
                        assert!(
                            self.tracker.phase_error_ns().is_some(),
                            "an accepted pulse at {now} ns closed no second"
                        );
                    }
                    Some(other) => panic!("capture(edge {edge}, {now} ns) reported {other:?}"),
                    None => {
                        self.spurs += 1;
                        assert_eq!(
                            (self.tracker.main.last_edge, self.tracker.target_edge()),
                            before,
                            "a spur moved the main train at {now} ns"
                        );
                    }
                }
                self.check("capture");
            }

            /// A reported second always pushes the deadline out, so polling
            /// terminates and the tracker cannot report the same second twice.
            fn poll(&mut self, now: u64) -> Option<SecondEvent> {
                let locked = self.tracker.state().is_locked();
                let unclosed = self.tracker.main.unclosed();
                let before = self.tracker.next_poll_deadline_ns();
                let event = self.tracker.poll(now);
                match event {
                    Some(SecondEvent::Switched { .. }) => {
                        self.switchovers += 1;
                        self.adopted = true;
                    }
                    Some(SecondEvent::Missed) => {
                        self.misses += 1;
                        assert!(
                            !locked || self.tracker.main.unclosed() == unclosed + 1,
                            "a miss out of lock closed other than one second at {now} ns"
                        );
                        assert!(
                            self.tracker.phase_error_ns().is_none(),
                            "a missing pulse at {now} ns still measured a phase"
                        );
                    }
                    Some(SecondEvent::Captured { .. }) => panic!("poll({now} ns) captured"),
                    None => {}
                }
                if event.is_some() {
                    assert!(
                        self.tracker.next_poll_deadline_ns() > before,
                        "the poll deadline did not move past a second at {now} ns"
                    );
                }
                self.check("poll");
                event
            }

            /// One poll per missing second, as `Synchronizer::poll` does.
            fn drain(&mut self, now: u64) {
                let deadline = self.tracker.next_poll_deadline_ns();
                let seconds: u64 = core::iter::from_fn(|| self.poll(now))
                    .map(|event| match event {
                        SecondEvent::Switched { seconds } => seconds as u64,
                        SecondEvent::Missed | SecondEvent::Captured { .. } => 1,
                    })
                    .sum();
                match deadline {
                    Some(deadline) => {
                        let counted = deadline
                            - NANOS_PER_SECOND
                            - self.tracker.config.miss_grace_ns as u64
                            - 1;
                        assert!(
                            seconds
                                <= (now.saturating_sub(counted) + NANOS_PER_SECOND / 2)
                                    / NANOS_PER_SECOND,
                            "{seconds} seconds reported from the boundary at {counted} to {now} ns"
                        )
                    }
                    None => assert_eq!(seconds, 0, "a tracker with no deadline reported a second"),
                }
            }

            /// A synthetic edge anchors the counter without establishing a
            /// train, so the tracker still reports itself unsynchronized.
            fn bootstrap(&mut self, edge: u32, now: u64) {
                self.tracker.bootstrap(edge, now);
                (self.anchored, self.adopted) = (true, false);
                assert_eq!(
                    self.tracker.state(),
                    PulseState::FreeRunning,
                    "bootstrap(edge {edge}, {now} ns) established a train"
                );
                self.check("bootstrap");
            }

            fn rebase(&mut self) {
                self.tracker.rebase();
                self.check("rebase");
            }

            fn check(&self, what: &str) {
                match self.tracker.target_edge() {
                    Some(edge) => assert!(
                        self.anchored && edge < PERIOD,
                        "target edge {edge} after {what}"
                    ),
                    None => assert!(!self.anchored, "the anchor was lost after {what}"),
                }
                assert_eq!(
                    self.tracker.state() == PulseState::FreeRunning,
                    !self.adopted,
                    "free-running is exactly no adopted outside train, after {what}"
                );
            }
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
            fn step(&mut self, pair: &mut Harness) {
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
        fn a_random_pulse_environment_holds_every_invariant() {
            let (mut captures, mut spurs, mut misses, mut switchovers, mut resumes) =
                (0u64, 0u64, 0u64, 0u64, 0u64);
            for seed in 1..=SEEDS {
                let mut pair = Harness::new();
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

//! Changing the sample rate by a ratio of whole numbers.
//!
//! Output sample `k` is the input at position `k * down / up`, read through
//! the centred low-pass of [`crate::interp`]. The filter is centred, so the
//! output is not delayed. Sample `k` of the result and sample `k * down / up`
//! of the input are the same instant. What the filter costs is latency. A
//! sample cannot be emitted until the input half a filter-width past it has
//! arrived.

use crate::error::BuildError;
use crate::interp::Interpolator;
use crate::node::{arity, fingerprint, Coupling, Extent, Inputs, Kernel, Node, Stage};
use crate::signal::{gcd, Domains, SignalType, Span};
use std::ops::Range;
use std::sync::Arc;

/// The most either side of a rate ratio may be. Each phase of the ratio has
/// its own set of filter weights, and the filter's length grows with `down`.
const MAX_RATIO_TERM: u64 = 4096;

/// Resamples to `up / down` times the input's rate, low-passing to the lower
/// of the two Nyquist frequencies on the way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resample {
    up: u64,
    down: u64,
}

impl Resample {
    /// Keeps one sample in `factor`, after filtering.
    pub fn decimate(factor: u64) -> Self {
        Self::ratio(1, factor)
    }

    /// Resamples to `up / down` times the rate.
    pub fn ratio(up: u64, down: u64) -> Self {
        let divisor = gcd(up, down).max(1);
        Self {
            up: up / divisor,
            down: down / divisor,
        }
    }

    /// The ratio that takes a signal at `from` Hz to `to` Hz, if it is a
    /// ratio of two whole numbers small enough to tabulate.
    pub fn between(from: f64, to: f64) -> Option<Self> {
        let target = to / from;
        if !(target.is_finite() && target > 0.0) {
            return None;
        }
        // Continued fractions: each step is the best ratio with terms that
        // small.
        let (mut up, mut down) = (1u64, 0u64);
        let (mut up_before, mut down_before) = (0u64, 1u64);
        let mut rest = target;
        for _ in 0..40 {
            let whole = rest.floor();
            let next_up = (whole as u64).checked_mul(up)?.checked_add(up_before)?;
            let next_down = (whole as u64).checked_mul(down)?.checked_add(down_before)?;
            if next_up > MAX_RATIO_TERM || next_down > MAX_RATIO_TERM {
                return None;
            }
            (up_before, down_before, up, down) = (up, down, next_up, next_down);
            if (up as f64 / down as f64 - target).abs() <= 1e-12 * target {
                return Some(Self::ratio(up, down));
            }
            rest = 1.0 / (rest - whole);
            if !rest.is_finite() {
                return None;
            }
        }
        None
    }

    /// The filter this rate change reads its input through.
    pub fn filter(&self) -> Interpolator {
        let band = (self.up as f64 / self.down as f64).min(1.0);
        Interpolator::for_band(0.5 * band)
    }
}

impl Node for Resample {
    fn kind(&self) -> &'static str {
        if self.up == 1 {
            "decimate"
        } else {
            "resample"
        }
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        let Self { up, down } = *self;
        if up == 0 || down == 0 || up > MAX_RATIO_TERM || down > MAX_RATIO_TERM {
            return Err(BuildError::RateRatio {
                kind: self.kind(),
                up,
                down,
            });
        }
        if up == down {
            return Err(BuildError::NoRateChange { kind: self.kind() });
        }
        let filter = self.filter();
        let phases = (0..up)
            .map(|phase| filter.taps(phase as f64 / up as f64))
            .collect();
        Ok(Box::new(ResampleStage {
            ty: SignalType {
                domain: domains.scaled(inputs[0].domain, up, down)?,
                channels: Arc::clone(&inputs[0].channels),
            },
            up,
            down,
            half_width: filter.half_width() as u64,
            phases: Arc::new(phases),
        }))
    }
}

struct ResampleStage {
    ty: SignalType,
    up: u64,
    down: u64,
    half_width: u64,
    /// The filter weights for each remainder of `k * down` by `up`.
    phases: Arc<Vec<Vec<f64>>>,
}

impl ResampleStage {
    /// How many output samples can be computed from input ending at `end`.
    /// They are those whose window, centred on `k * down / up`, ends before
    /// it.
    fn ready(&self, end: u64) -> u64 {
        let reach = end.saturating_sub(self.half_width);
        (u128::from(reach) * u128::from(self.up)).div_ceil(u128::from(self.down)) as u64
    }
}

impl Stage for ResampleStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("resample", self.up, self.down, self.half_width))
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn span(&self, inputs: &[Extent]) -> Span {
        let input = inputs[0];
        // The first output whose window begins inside the run.
        let earliest = (u128::from(input.since + self.half_width) * u128::from(self.up))
            .div_ceil(u128::from(self.down)) as u64;
        let first = earliest.max(self.ready(input.first));
        let end = first.max(self.ready(input.end()));
        Span {
            first,
            frames: (end - first) as usize,
        }
    }

    fn buffers(&self) -> bool {
        true
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(ResampleKernel {
            history: vec![Vec::new(); lane.len()],
            lane,
            up: self.up,
            down: self.down,
            half_width: self.half_width,
            phases: Arc::clone(&self.phases),
            start: 0,
        })
    }
}

struct ResampleKernel {
    lane: Range<usize>,
    up: u64,
    down: u64,
    half_width: u64,
    phases: Arc<Vec<Vec<f64>>>,
    /// The input samples still needed, per lane channel.
    history: Vec<Vec<f64>>,
    /// The input sample every history begins at.
    start: u64,
}

impl ResampleKernel {
    /// The input sample at or just before output `k`, and which set of
    /// weights reaches from there to `k`.
    fn position(&self, k: u64) -> (u64, usize) {
        let position = u128::from(k) * u128::from(self.down);
        let up = u128::from(self.up);
        ((position / up) as u64, (position % up) as usize)
    }
}

impl Kernel for ResampleKernel {
    fn reset(&mut self) {
        self.history.iter_mut().for_each(Vec::clear);
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        if input.frames > 0 {
            if self.history[0].is_empty() {
                self.start = input.first;
            }
            for (history, channel) in self.history.iter_mut().zip(self.lane.clone()) {
                history.extend_from_slice(input.channel(channel));
            }
        }
        if span.frames > 0 {
            for (row, history) in out.chunks_exact_mut(span.frames).zip(&self.history) {
                for (y, k) in row.iter_mut().zip(span.first..) {
                    let (base, phase) = self.position(k);
                    let taps = &self.phases[phase];
                    let from = (base - self.half_width - self.start) as usize;
                    *y = taps
                        .iter()
                        .zip(&history[from..from + taps.len()])
                        .map(|(tap, x)| tap * x)
                        .sum();
                }
            }
        }
        // Drop what no later output reaches back to.
        let (next, _) = self.position(span.first + span.frames as u64);
        let held = self.history[0].len() as u64;
        let keep = next
            .saturating_sub(self.half_width)
            .clamp(self.start, self.start + held);
        for history in &mut self.history {
            history.drain(..(keep - self.start) as usize);
        }
        self.start = keep;
    }
}

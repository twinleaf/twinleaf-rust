//! Taking out of one signal what it shares with another.
//!
//! Each channel of the signal is fitted, sample by sample, as a weighted sum
//! of the reference's channels, and the fit is subtracted. What is left is
//! what the reference does not explain. For two magnetometers that is the
//! gradient without the common field. All the reference's channels go into
//! every fit, so a sensor turned a little against its neighbour is fitted
//! too.
//!
//! The regressors at sample `n` are the reference's channels at `n` and, with
//! more than one tap, at the samples before it. There are `taps` of each,
//! most recent first. One tap fits a gain. More fit a short filter, for
//! coupling that differs with frequency. Before a run has that many samples
//! the missing ones count as zero.
//!
//! The three methods differ in how the weights follow the data.
//!
//! - **Least squares** solves for the weights that fit the last `window`
//!   best, afresh at every sample. It has no memory beyond the window and
//!   no parameter but its length.
//! - **NLMS** nudges the weights along the error at each sample. It is the
//!   cheapest and the slowest to converge, and its step is set so that an
//!   error in the weights decays over one to two times `window`.
//! - **RLS** is least squares with a memory that fades over about `window`
//!   instead of ending. It converges about as fast as the window allows and
//!   costs the square of the number of regressors per sample.
//!
//! NLMS and RLS report the error before the weights are updated with it.
//! None of the three fits an offset. Take offsets out first, or the fit is
//! of them.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Inputs, Kernel, Node, Stage};
use crate::nodes::window::samples_in;
use crate::signal::{Domains, SignalType, Span};
use std::ops::Range;

/// How the weights follow the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    LeastSquares,
    Nlms,
    Rls,
}

/// Subtracts from its first input the best fit of its second.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decorrelate {
    pub method: Method,
    /// How much of the past the fit looks at, in seconds.
    pub window: f64,
    /// How many samples of each reference channel go into the fit.
    pub taps: usize,
}

impl Node for Decorrelate {
    fn kind(&self) -> &'static str {
        "decorrelate"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 2)?;
        let rate = domains
            .get(inputs[0].domain)
            .map(|domain| domain.rate)
            .ok_or(BuildError::UnknownDomain)?;
        Ok(Box::new(DecorrelateStage {
            ty: inputs[0].clone(),
            references: inputs[1].channels.len(),
            method: self.method,
            samples: samples_in(self.window, rate),
            taps: self.taps.max(1),
        }))
    }
}

struct DecorrelateStage {
    ty: SignalType,
    references: usize,
    method: Method,
    samples: usize,
    taps: usize,
}

impl Stage for DecorrelateStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    /// Every output channel reads all of the reference, and none reads
    /// another output channel, so they can still be computed apart.
    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn fingerprint(&self) -> u64 {
        fingerprint((
            "decorrelate",
            self.method,
            self.samples,
            self.taps,
            self.references,
        ))
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        let regressors = Regressors::new(self.references, self.taps);
        let size = regressors.size();
        let channels = lane.len();
        let fit: Box<dyn Fit> = match self.method {
            Method::LeastSquares => Box::new(Windowed::new(size, channels, self.samples)),
            Method::Nlms => Box::new(Nlms::new(size, channels, self.samples)),
            Method::Rls => Box::new(Rls::new(size, channels, self.samples)),
        };
        Box::new(DecorrelateKernel {
            lane,
            regressors,
            fit,
            sample: vec![0.0; channels],
        })
    }
}

/// The regressors of the present sample, which are each reference channel's
/// last `taps` samples, most recent first.
struct Regressors {
    references: usize,
    taps: usize,
    /// `values[k * taps + l]` is reference channel `k`, `l` samples ago.
    values: Vec<f64>,
}

impl Regressors {
    fn new(references: usize, taps: usize) -> Self {
        Self {
            references,
            taps,
            values: vec![0.0; references * taps],
        }
    }

    fn size(&self) -> usize {
        self.values.len()
    }

    fn reset(&mut self) {
        self.values.fill(0.0);
    }

    /// Moves on one sample, taking in each reference channel's new value.
    fn advance(&mut self, latest: impl Iterator<Item = f64>) {
        for (channel, value) in self.values.chunks_exact_mut(self.taps).zip(latest) {
            channel.copy_within(..self.taps - 1, 1);
            channel[0] = value;
        }
        debug_assert_eq!(self.values.len(), self.references * self.taps);
    }
}

/// One way of following the weights. `step` takes the regressors and each
/// lane channel's sample, and leaves in each sample's place what the fit
/// did not explain.
trait Fit: Send {
    fn reset(&mut self);
    fn step(&mut self, regressors: &[f64], samples: &mut [f64]);
}

struct DecorrelateKernel {
    lane: Range<usize>,
    regressors: Regressors,
    fit: Box<dyn Fit>,
    /// The present sample of every lane channel.
    sample: Vec<f64>,
}

impl Kernel for DecorrelateKernel {
    fn reset(&mut self) {
        self.regressors.reset();
        self.fit.reset();
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let (signal, reference) = (inputs.block(0), inputs.block(1));
        let references = reference.channels.len();
        for frame in 0..span.frames {
            self.regressors
                .advance((0..references).map(|k| reference.channel(k)[frame]));
            for (sample, channel) in self.sample.iter_mut().zip(self.lane.clone()) {
                *sample = signal.channel(channel)[frame];
            }
            self.fit.step(&self.regressors.values, &mut self.sample);
            for (index, residual) in self.sample.iter().enumerate() {
                out[index * span.frames + frame] = *residual;
            }
        }
    }
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Least squares over the last `samples` samples.
struct Windowed {
    size: usize,
    samples: usize,
    /// The regressors of the samples in the window, as a ring of rows.
    rows: Vec<f64>,
    /// Each lane channel's samples in the window, as a ring.
    held: Vec<Vec<f64>>,
    filled: usize,
    next: usize,
    /// The sum over the window of each regressor times each other.
    moments: Vec<f64>,
    /// Per lane channel, the sum of each regressor times the sample.
    cross: Vec<Vec<f64>>,
    factor: Vec<f64>,
    weights: Vec<f64>,
}

impl Windowed {
    fn new(size: usize, channels: usize, samples: usize) -> Self {
        Self {
            size,
            samples,
            rows: vec![0.0; samples * size],
            held: vec![vec![0.0; samples]; channels],
            filled: 0,
            next: 0,
            moments: vec![0.0; size * size],
            cross: vec![vec![0.0; size]; channels],
            factor: vec![0.0; size * size],
            weights: vec![0.0; size],
        }
    }

    /// Adds one sample's products to the sums, or with `sign` of -1 takes
    /// them away.
    fn accumulate(&mut self, at: usize, sign: f64) {
        let size = self.size;
        let row = &self.rows[at * size..(at + 1) * size];
        for i in 0..size {
            for j in 0..size {
                self.moments[i * size + j] += sign * row[i] * row[j];
            }
        }
        for (cross, held) in self.cross.iter_mut().zip(&self.held) {
            for (sum, regressor) in cross.iter_mut().zip(row) {
                *sum += sign * held[at] * regressor;
            }
        }
    }

    /// Factors the moments, a little stiffened, as `L Lᵀ`. False if they
    /// are not positive definite even so, as when nothing has been seen.
    fn factorize(&mut self) -> bool {
        let size = self.size;
        let trace: f64 = (0..size).map(|i| self.moments[i * size + i]).sum();
        // Enough to keep two references that say the same thing from
        // dividing by nothing, and far too little to bend a real fit.
        let stiffening = 1e-12 * trace / size as f64 + f64::MIN_POSITIVE;
        for i in 0..size {
            for j in 0..=i {
                let mut sum = self.moments[i * size + j];
                if i == j {
                    sum += stiffening;
                }
                for k in 0..j {
                    sum -= self.factor[i * size + k] * self.factor[j * size + k];
                }
                if i == j {
                    if !(sum > 0.0 && sum.is_finite()) {
                        return false;
                    }
                    self.factor[i * size + i] = sum.sqrt();
                } else {
                    self.factor[i * size + j] = sum / self.factor[j * size + j];
                }
            }
        }
        true
    }

    /// Solves `L Lᵀ w = b` into `self.weights`.
    fn solve(&mut self, channel: usize) {
        let size = self.size;
        for i in 0..size {
            let mut sum = self.cross[channel][i];
            for k in 0..i {
                sum -= self.factor[i * size + k] * self.weights[k];
            }
            self.weights[i] = sum / self.factor[i * size + i];
        }
        for i in (0..size).rev() {
            let mut sum = self.weights[i];
            for k in i + 1..size {
                sum -= self.factor[k * size + i] * self.weights[k];
            }
            self.weights[i] = sum / self.factor[i * size + i];
        }
    }
}

impl Fit for Windowed {
    fn reset(&mut self) {
        self.filled = 0;
        self.next = 0;
        self.moments.fill(0.0);
        self.cross.iter_mut().for_each(|cross| cross.fill(0.0));
    }

    fn step(&mut self, regressors: &[f64], samples: &mut [f64]) {
        let at = self.next;
        if self.filled == self.samples {
            self.accumulate(at, -1.0);
        } else {
            self.filled += 1;
        }
        self.rows[at * self.size..(at + 1) * self.size].copy_from_slice(regressors);
        for (held, sample) in self.held.iter_mut().zip(samples.iter()) {
            held[at] = *sample;
        }
        self.accumulate(at, 1.0);
        self.next = (at + 1) % self.samples;
        // Adding and taking away drifts. Each time the ring comes round
        // the sums are taken afresh, at a sample that is the same however
        // the samples were cut into blocks.
        if self.next == 0 {
            self.moments.fill(0.0);
            self.cross.iter_mut().for_each(|cross| cross.fill(0.0));
            for row in 0..self.filled {
                self.accumulate(row, 1.0);
            }
        }

        if !self.factorize() {
            return;
        }
        for (channel, sample) in samples.iter_mut().enumerate() {
            self.solve(channel);
            *sample -= dot(&self.weights, regressors);
        }
    }
}

/// Normalized least mean squares.
struct Nlms {
    /// Each lane channel's weights.
    weights: Vec<Vec<f64>>,
    step: f64,
    /// The regressors' power, averaged over about the window.
    power: f64,
    seen: usize,
    samples: usize,
}

impl Nlms {
    fn new(size: usize, channels: usize, samples: usize) -> Self {
        Self {
            weights: vec![vec![0.0; size]; channels],
            // An error in the weights decays by a factor of e in about
            // `size / step` samples, which is to be the window.
            step: (size as f64 / samples as f64).min(1.0),
            power: 0.0,
            seen: 0,
            samples,
        }
    }
}

impl Fit for Nlms {
    fn reset(&mut self) {
        self.weights
            .iter_mut()
            .for_each(|weights| weights.fill(0.0));
        self.power = 0.0;
        self.seen = 0;
    }

    fn step(&mut self, regressors: &[f64], samples: &mut [f64]) {
        let energy = dot(regressors, regressors);
        self.seen = (self.seen + 1).min(self.samples);
        self.power += (energy - self.power) / self.seen as f64;
        // Normalized by the regressors' power: the present sample's, or
        // the window's average where that is more. With few regressors a
        // sample where the reference passes through zero would otherwise
        // throw the weights.
        let scale = self.step / (energy.max(self.power) + f64::MIN_POSITIVE);
        for (weights, sample) in self.weights.iter_mut().zip(samples) {
            let error = *sample - dot(weights, regressors);
            for (weight, regressor) in weights.iter_mut().zip(regressors) {
                *weight += scale * error * regressor;
            }
            *sample = error;
        }
    }
}

/// Recursive least squares with a fading memory.
struct Rls {
    size: usize,
    weights: Vec<Vec<f64>>,
    /// The inverse of the regressors' faded moments. Empty until the
    /// reference has said something.
    inverse: Vec<f64>,
    started: bool,
    forgetting: f64,
    gain: Vec<f64>,
    projected: Vec<f64>,
}

impl Rls {
    fn new(size: usize, channels: usize, samples: usize) -> Self {
        Self {
            size,
            weights: vec![vec![0.0; size]; channels],
            inverse: vec![0.0; size * size],
            started: false,
            // A sample `samples` ago weighs 1/e of the present one.
            forgetting: 1.0 - 1.0 / samples.max(2) as f64,
            gain: vec![0.0; size],
            projected: vec![0.0; size],
        }
    }
}

impl Fit for Rls {
    fn reset(&mut self) {
        self.weights
            .iter_mut()
            .for_each(|weights| weights.fill(0.0));
        self.started = false;
    }

    fn step(&mut self, regressors: &[f64], samples: &mut [f64]) {
        let size = self.size;
        if !self.started {
            let energy = dot(regressors, regressors);
            if !(energy > 0.0 && energy.is_finite()) {
                // Nothing to fit yet: the signal passes as it is.
                return;
            }
            // Begin sure of nothing, on the scale of the first sample that
            // says anything, so that the units do not matter.
            let confidence = size as f64 / (0.01 * energy);
            self.inverse.fill(0.0);
            for i in 0..size {
                self.inverse[i * size + i] = confidence;
            }
            self.started = true;
        }

        for i in 0..size {
            self.projected[i] = dot(&self.inverse[i * size..(i + 1) * size], regressors);
        }
        let denominator = self.forgetting + dot(regressors, &self.projected);
        for (gain, projected) in self.gain.iter_mut().zip(&self.projected) {
            *gain = projected / denominator;
        }
        for (weights, sample) in self.weights.iter_mut().zip(samples) {
            let error = *sample - dot(weights, regressors);
            for (weight, gain) in weights.iter_mut().zip(&self.gain) {
                *weight += gain * error;
            }
            *sample = error;
        }
        // The inverse is symmetric, and is kept so: rounding that lets it
        // lean is what makes this method come apart.
        for i in 0..size {
            for j in 0..=i {
                let a = self.inverse[i * size + j] - self.gain[i] * self.projected[j];
                let b = self.inverse[j * size + i] - self.gain[j] * self.projected[i];
                let value = 0.5 * (a + b) / self.forgetting;
                self.inverse[i * size + j] = value;
                self.inverse[j * size + i] = value;
            }
        }
    }
}

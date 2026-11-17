//! Stages that need the whole of a run before they can say anything. They
//! are a filter run both ways, and a trend taken out.
//!
//! Such a stage holds every sample it is given and emits nothing until the
//! run is closed, and then emits all of it at once. A recording's runs are
//! closed at its end and at its gaps. A live graph's never are, so these
//! are for recordings only, and a live graph refuses a spec that asks for
//! one rather than hold samples for ever.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Extent, Inputs, Kernel, Node, Stage};
use crate::nodes::biquad::Section;
use crate::signal::{Domains, SignalType, Span};
use std::ops::Range;
use std::sync::Arc;

/// What is done to each channel of a whole run, in place.
type Apply = dyn Fn(&mut [f64]) + Send + Sync;

struct WholeStage {
    ty: SignalType,
    fingerprint: u64,
    apply: Arc<Apply>,
}

impl Stage for WholeStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Nothing until the run ends, and then every sample of it.
    fn span(&self, inputs: &[Extent]) -> Span {
        let input = inputs[0];
        let frames = match input.closed {
            true => (input.end() - input.since) as usize,
            false => 0,
        };
        Span {
            first: input.since,
            frames,
        }
    }

    fn buffers(&self) -> bool {
        true
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(WholeKernel {
            held: vec![Vec::new(); lane.len()],
            apply: Arc::clone(&self.apply),
            lane,
        })
    }
}

struct WholeKernel {
    lane: Range<usize>,
    /// Every sample of the run so far, per lane channel.
    held: Vec<Vec<f64>>,
    apply: Arc<Apply>,
}

impl Kernel for WholeKernel {
    fn reset(&mut self) {
        self.held.iter_mut().for_each(Vec::clear);
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        for (held, channel) in self.held.iter_mut().zip(self.lane.clone()) {
            held.extend_from_slice(input.channel(channel));
        }
        if span.frames == 0 {
            return;
        }
        for (row, held) in out.chunks_exact_mut(span.frames).zip(&mut self.held) {
            (self.apply)(held);
            row.copy_from_slice(held);
            held.clear();
        }
    }
}

/// Runs every channel through a cascade of sections forwards and then
/// backwards, so that the phase the first pass adds the second takes away.
/// The result is delayed at no frequency, and attenuated twice over.
///
/// The ends are handled as `scipy.signal.sosfiltfilt` handles them. The run
/// is extended at each end by its own reflection about its end sample, and
/// each pass begins from the state the filter would have settled to on a
/// constant at the first sample's value, so that a run does not open or
/// close with the filter's start-up.
#[derive(Debug, Clone, PartialEq)]
pub struct ZeroPhase {
    sections: Vec<Section>,
}

impl ZeroPhase {
    pub fn cascade(sections: Vec<Section>) -> Self {
        Self { sections }
    }
}

impl Node for ZeroPhase {
    fn kind(&self) -> &'static str {
        "zero-phase biquad"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        _domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        let sections = self.sections.clone();
        let bits: Vec<[u64; 5]> = sections
            .iter()
            .map(|s| [s.b0, s.b1, s.b2, s.a1, s.a2].map(f64::to_bits))
            .collect();
        Ok(Box::new(WholeStage {
            ty: inputs[0].clone(),
            fingerprint: fingerprint(("zero-phase biquad", bits)),
            apply: Arc::new(move |run| both_ways(&sections, run)),
        }))
    }
}

/// The state each section settles to on a constant input of one, the
/// sections in cascade. It is what `scipy.signal.sosfilt_zi` computes.
fn settled(sections: &[Section]) -> Vec<[f64; 2]> {
    let mut level = 1.0;
    sections
        .iter()
        .map(|s| {
            let first = (s.b1 + s.b2 - (s.a1 + s.a2) * s.b0) / (1.0 + s.a1 + s.a2);
            let second = s.b2 - s.a2 * s.b0 - s.a2 * first;
            let state = [level * first, level * second];
            // What a constant comes out of this section as, for the next.
            level *= (s.b0 + s.b1 + s.b2) / (1.0 + s.a1 + s.a2);
            state
        })
        .collect()
}

/// One pass of the cascade over `samples`, in place, from `state`.
fn pass(sections: &[Section], state: &mut [[f64; 2]], samples: &mut [f64]) {
    for (section, state) in sections.iter().zip(state) {
        let [mut s1, mut s2] = *state;
        for sample in samples.iter_mut() {
            let x = *sample;
            let y = section.b0 * x + s1;
            s1 = section.b1 * x - section.a1 * y + s2;
            s2 = section.b2 * x - section.a2 * y;
            *sample = y;
        }
        *state = [s1, s2];
    }
}

fn both_ways(sections: &[Section], run: &mut [f64]) {
    if run.is_empty() || sections.is_empty() {
        return;
    }
    // How far to extend: three times the filter's length, as scipy counts
    // it, and never further than the run reaches.
    let first_order = sections
        .iter()
        .filter(|s| s.b2 == 0.0)
        .count()
        .min(sections.iter().filter(|s| s.a2 == 0.0).count());
    let pad = (3 * (2 * sections.len() + 1 - first_order)).min(run.len() - 1);

    // Reflected about each end sample: `2 x[0] - x[pad], …, 2 x[0] - x[1]`.
    let (head, tail) = (run[0], run[run.len() - 1]);
    let mut extended = Vec::with_capacity(run.len() + 2 * pad);
    extended.extend((1..=pad).rev().map(|k| 2.0 * head - run[k]));
    extended.extend_from_slice(run);
    extended.extend((1..=pad).map(|k| 2.0 * tail - run[run.len() - 1 - k]));

    let rest = settled(sections);
    let begin =
        |at: f64| -> Vec<[f64; 2]> { rest.iter().map(|s| [s[0] * at, s[1] * at]).collect() };
    let mut state = begin(extended[0]);
    pass(sections, &mut state, &mut extended);
    extended.reverse();
    let mut state = begin(extended[0]);
    pass(sections, &mut state, &mut extended);
    extended.reverse();
    run.copy_from_slice(&extended[pad..pad + run.len()]);
}

/// Takes out of every channel the polynomial that fits its whole run best,
/// which is its mean, its line, or its parabola.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Detrend {
    /// The polynomial's degree, which is 0, 1 or 2.
    pub degree: usize,
}

impl Node for Detrend {
    fn kind(&self) -> &'static str {
        "detrend"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        _domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        let degree = self.degree.min(2);
        Ok(Box::new(WholeStage {
            ty: inputs[0].clone(),
            fingerprint: fingerprint(("detrend", degree)),
            apply: Arc::new(move |run| remove_trend(degree, run)),
        }))
    }
}

/// Fits and subtracts a polynomial of `degree` by least squares.
///
/// The fit is over time scaled to run from -1 to 1, in the polynomials that
/// are orthogonal over the run's own samples, so that each coefficient is
/// one sum divided by another and nothing has to be solved.
fn remove_trend(degree: usize, run: &mut [f64]) {
    let count = run.len();
    if count == 0 {
        return;
    }
    let at = |n: usize| match count {
        1 => 0.0,
        _ => 2.0 * n as f64 / (count - 1) as f64 - 1.0,
    };
    // The basis: 1; t less its mean, which is zero; t² less its mean and
    // its part along t, which is zero too, the samples being symmetric.
    let mean_square = (0..count).map(|n| at(n) * at(n)).sum::<f64>() / count as f64;
    let basis = |order: usize, t: f64| match order {
        0 => 1.0,
        1 => t,
        _ => t * t - mean_square,
    };
    for order in 0..=degree.min(count - 1) {
        let (mut along, mut norm) = (0.0, 0.0);
        for (n, sample) in run.iter().enumerate() {
            let b = basis(order, at(n));
            along += sample * b;
            norm += b * b;
        }
        if norm > 0.0 {
            let coefficient = along / norm;
            for (n, sample) in run.iter_mut().enumerate() {
                *sample -= coefficient * basis(order, at(n));
            }
        }
    }
}

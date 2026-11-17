//! Biquad cascades in transposed direct form II.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Inputs, Kernel, Node, Stage};
use crate::signal::{Domains, SignalType, Span};
use std::ops::Range;

/// One second-order section, normalized so that `a0` is 1. It computes
/// `y[n] = b0 x[n] + b1 x[n-1] + b2 x[n-2] - a1 y[n-1] - a2 y[n-2]`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Section {
    pub b0: f64,
    pub b1: f64,
    pub b2: f64,
    pub a1: f64,
    pub a2: f64,
}

/// Runs every channel through the same cascade of sections, so that the
/// channels cannot differ in gain or phase. [`crate::design`] makes the
/// sections.
#[derive(Debug, Clone, PartialEq)]
pub struct Biquad {
    sections: Vec<Section>,
}

impl Biquad {
    /// Creates a cascade that applies `sections` in order.
    pub fn cascade(sections: Vec<Section>) -> Self {
        Self { sections }
    }
}

impl Node for Biquad {
    fn kind(&self) -> &'static str {
        "biquad"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        _domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        Ok(Box::new(BiquadStage {
            ty: inputs[0].clone(),
            sections: self.sections.clone(),
        }))
    }
}

struct BiquadStage {
    ty: SignalType,
    sections: Vec<Section>,
}

impl Stage for BiquadStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn fingerprint(&self) -> u64 {
        let sections: Vec<[u64; 5]> = self
            .sections
            .iter()
            .map(|s| [s.b0, s.b1, s.b2, s.a1, s.a2].map(f64::to_bits))
            .collect();
        fingerprint(("biquad", sections))
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(BiquadKernel {
            state: vec![[0.0; 2]; lane.len() * self.sections.len()],
            sections: self.sections.clone(),
            lane,
        })
    }
}

struct BiquadKernel {
    sections: Vec<Section>,
    lane: Range<usize>,
    /// The two delay elements of each section, per lane channel, at
    /// `state[channel * sections.len() + section]`.
    state: Vec<[f64; 2]>,
}

impl Kernel for BiquadKernel {
    fn reset(&mut self) {
        self.state.fill([0.0; 2]);
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        let count = self.sections.len();
        for (row, (index, channel)) in out
            .chunks_exact_mut(span.frames)
            .zip(self.lane.clone().enumerate())
        {
            row.copy_from_slice(input.channel(channel));
            let state = &mut self.state[index * count..(index + 1) * count];
            for (section, delay) in self.sections.iter().zip(state) {
                let [mut s1, mut s2] = *delay;
                for sample in row.iter_mut() {
                    let x = *sample;
                    let y = section.b0 * x + s1;
                    s1 = section.b1 * x - section.a1 * y + s2;
                    s2 = section.b2 * x - section.a2 * y;
                    *sample = y;
                }
                *delay = [s1, s2];
            }
        }
    }
}

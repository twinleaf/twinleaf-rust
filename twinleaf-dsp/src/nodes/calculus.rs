//! Integrals and derivatives in time.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Extent, Inputs, Kernel, Node, Stage};
use crate::signal::{Domains, SignalType, Span};
use std::ops::Range;

fn rate_of(input: &SignalType, domains: &Domains) -> Result<f64, BuildError> {
    domains
        .get(input.domain)
        .map(|domain| domain.rate)
        .ok_or(BuildError::UnknownDomain)
}

/// The running integral of each channel by the trapezoid rule, from zero at
/// the first sample of a run. It computes
/// `y[n] = y[n-1] + (x[n] + x[n-1]) / (2 rate)`.
///
/// An integral keeps whatever offset its input has and turns it into a
/// ramp, and it keeps every rounding it has ever made. Take the offset out
/// first, and do not expect hours of it to mean much.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Integrate;

impl Node for Integrate {
    fn kind(&self) -> &'static str {
        "integrate"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        Ok(Box::new(IntegrateStage {
            ty: inputs[0].clone(),
            step: 0.5 / rate_of(&inputs[0], domains)?,
        }))
    }
}

struct IntegrateStage {
    ty: SignalType,
    /// Half a sample period, which is what each of two samples adds to the
    /// integral.
    step: f64,
}

impl Stage for IntegrateStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("integrate", self.step.to_bits()))
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(IntegrateKernel {
            state: vec![None; lane.len()],
            step: self.step,
            lane,
        })
    }
}

struct IntegrateKernel {
    lane: Range<usize>,
    step: f64,
    /// For each lane channel, the last sample, the sum so far, and what the
    /// sum has lost to rounding. `None` before the first sample of a run.
    state: Vec<Option<(f64, f64, f64)>>,
}

impl Kernel for IntegrateKernel {
    fn reset(&mut self) {
        self.state.fill(None);
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        for (row, (state, channel)) in out
            .chunks_exact_mut(span.frames)
            .zip(self.state.iter_mut().zip(self.lane.clone()))
        {
            for (y, &x) in row.iter_mut().zip(input.channel(channel)) {
                let (sum, lost) = match *state {
                    None => (0.0, 0.0),
                    Some((last, sum, lost)) => {
                        // Summed so that what one addition drops is put
                        // back by the next.
                        let term = (x + last) * self.step - lost;
                        let next = sum + term;
                        (next, (next - sum) - term)
                    }
                };
                *state = Some((x, sum, lost));
                *y = sum;
            }
        }
    }
}

/// The slope of each channel by the central difference. It computes
/// `y[n] = (x[n+1] - x[n-1]) rate / 2`.
///
/// It has no phase error, and it needs the sample after the one it reports,
/// so it runs one sample late and a run loses its first and last sample. A
/// derivative amplifies noise in proportion to frequency, so filter first.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Derivative;

impl Node for Derivative {
    fn kind(&self) -> &'static str {
        "derivative"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        Ok(Box::new(DerivativeStage {
            ty: inputs[0].clone(),
            scale: 0.5 * rate_of(&inputs[0], domains)?,
        }))
    }
}

struct DerivativeStage {
    ty: SignalType,
    scale: f64,
}

impl Stage for DerivativeStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("derivative", self.scale.to_bits()))
    }

    /// Sample `n` can be reported once sample `n + 1` is in, and the first
    /// of a run has no sample before it.
    fn span(&self, inputs: &[Extent]) -> Span {
        let input = inputs[0];
        let first = input.first.saturating_sub(1).max(input.since + 1);
        let end = input.end().saturating_sub(1).max(first);
        Span {
            first,
            frames: (end - first) as usize,
        }
    }

    fn buffers(&self) -> bool {
        true
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(DerivativeKernel {
            held: vec![[0.0; 2]; lane.len()],
            scale: self.scale,
            lane,
        })
    }
}

struct DerivativeKernel {
    lane: Range<usize>,
    scale: f64,
    /// The two samples before this tick's block, per lane channel.
    held: Vec<[f64; 2]>,
}

impl Kernel for DerivativeKernel {
    fn reset(&mut self) {
        self.held.fill([0.0; 2]);
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        if input.frames == 0 {
            return;
        }
        let mut rows = out.chunks_exact_mut(span.frames.max(1));
        for (held, channel) in self.held.iter_mut().zip(self.lane.clone()) {
            let block = input.channel(channel);
            // Sample `n` of the input, which is in the block or is one of
            // the two held from before it.
            let at = |n: u64| match n.checked_sub(input.first) {
                Some(offset) => block[offset as usize],
                None => held[(2 - (input.first - n)) as usize],
            };
            if span.frames > 0 {
                let row = rows.next().expect("a row for each lane channel");
                for (y, n) in row.iter_mut().zip(span.first..) {
                    *y = (at(n + 1) - at(n - 1)) * self.scale;
                }
            }
            *held = match block {
                [.., a, b] => [*a, *b],
                [only] => [held[1], *only],
                [] => *held,
            };
        }
    }
}

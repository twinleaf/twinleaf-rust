//! Statistics over a sliding window.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Inputs, Kernel, Node, Stage};
use crate::signal::{Domains, SignalType, Span};
use std::ops::Range;

/// The root mean square of each channel over the samples of the last
/// `window` seconds, the present one included. At the start of a run, over
/// the samples there are.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rms {
    /// The window, in seconds.
    pub window: f64,
}

impl Node for Rms {
    fn kind(&self) -> &'static str {
        "rms"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        let rate = domains
            .get(inputs[0].domain)
            .map(|domain| domain.rate)
            .ok_or(BuildError::UnknownDomain)?;
        Ok(Box::new(RmsStage {
            ty: inputs[0].clone(),
            samples: samples_in(self.window, rate),
        }))
    }
}

/// How many samples a window of `seconds` holds at `rate`. It is the
/// nearest whole number, and never none.
pub fn samples_in(seconds: f64, rate: f64) -> usize {
    ((seconds * rate).round() as usize).max(1)
}

struct RmsStage {
    ty: SignalType,
    samples: usize,
}

impl Stage for RmsStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("rms", self.samples))
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(RmsKernel {
            squares: vec![Vec::with_capacity(self.samples); lane.len()],
            sums: vec![0.0; lane.len()],
            next: 0,
            samples: self.samples,
            lane,
        })
    }
}

struct RmsKernel {
    lane: Range<usize>,
    samples: usize,
    /// The squares of the last `samples` samples of each lane channel, as
    /// a ring.
    squares: Vec<Vec<f64>>,
    sums: Vec<f64>,
    /// Where in every ring the next square goes.
    next: usize,
}

impl Kernel for RmsKernel {
    fn reset(&mut self) {
        self.squares.iter_mut().for_each(Vec::clear);
        self.sums.fill(0.0);
        self.next = 0;
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        let started = self.next;
        for (row, ((squares, sum), channel)) in out.chunks_exact_mut(span.frames).zip(
            self.squares
                .iter_mut()
                .zip(&mut self.sums)
                .zip(self.lane.clone()),
        ) {
            let mut next = started;
            for (y, &x) in row.iter_mut().zip(input.channel(channel)) {
                let square = x * x;
                if squares.len() < self.samples {
                    squares.push(square);
                    *sum += square;
                } else {
                    *sum += square - squares[next];
                    squares[next] = square;
                }
                next = (next + 1) % self.samples;
                // Adding and taking away drifts. Each time the ring comes
                // round the sum is taken afresh, at a sample that is the
                // same however the samples were cut into blocks.
                if next == 0 {
                    *sum = squares.iter().sum();
                }
                *y = (sum.max(0.0) / squares.len() as f64).sqrt();
            }
        }
        self.next = (started + span.frames) % self.samples;
    }
}

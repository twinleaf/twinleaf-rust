//! Vector magnitude, which takes N channels to one.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Inputs, Kernel, Node, Stage};
use crate::reduce::tree_sum;
use crate::signal::{Channel, ChannelSet, Domains, SignalType, Span};
use std::ops::Range;

/// The Euclidean norm across the channels of each frame. The channels must
/// share a unit, which the result keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Magnitude;

impl Node for Magnitude {
    fn kind(&self) -> &'static str {
        "magnitude"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        _domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        let mut channels = inputs[0].channels.iter();
        let first = channels.next().expect("a channel set is never empty");
        if let Some(other) = channels.find(|c| c.unit != first.unit) {
            return Err(BuildError::UnitMismatch {
                kind: self.kind(),
                channel: other.name.clone(),
                left: other.unit.clone(),
                right: first.unit.clone(),
            });
        }
        Ok(Box::new(MagnitudeStage {
            inputs: inputs[0].channels.len(),
            ty: SignalType {
                domain: inputs[0].domain,
                channels: ChannelSet::new(vec![Channel {
                    name: "magnitude".to_owned(),
                    unit: first.unit.clone(),
                }])?,
            },
        }))
    }
}

struct MagnitudeStage {
    ty: SignalType,
    inputs: usize,
}

impl Stage for MagnitudeStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("magnitude", self.inputs))
    }

    fn coupling(&self) -> Coupling {
        Coupling::Coupled
    }

    fn kernel(&self, _lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(MagnitudeKernel {
            squares: vec![0.0; self.inputs],
        })
    }
}

struct MagnitudeKernel {
    /// One frame's squared samples, summed in a fixed-shape tree.
    squares: Vec<f64>,
}

impl Kernel for MagnitudeKernel {
    fn reset(&mut self) {}

    fn process(&mut self, inputs: &Inputs<'_>, _span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        for (frame, y) in out.iter_mut().enumerate() {
            for (channel, square) in self.squares.iter_mut().enumerate() {
                let x = input.data[channel * input.frames + frame];
                *square = x * x;
            }
            *y = tree_sum(&self.squares).sqrt();
        }
    }
}

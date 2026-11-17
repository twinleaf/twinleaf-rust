//! Elementwise arithmetic. Two signals combine channel by channel, paired by
//! name and never by position.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Inputs, Kernel, Node, Stage};
use crate::signal::{Domains, SignalType, Span};
use std::ops::Range;

/// Multiplies every channel by one factor, or divides by it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Gain {
    factor: f64,
    divide: bool,
}

impl Gain {
    /// Multiplies by `factor`.
    pub fn new(factor: f64) -> Self {
        Self {
            factor,
            divide: false,
        }
    }

    /// Divides by `divisor`, which rounds differently from multiplying by
    /// its reciprocal.
    pub fn dividing(divisor: f64) -> Self {
        Self {
            factor: divisor,
            divide: true,
        }
    }
}

impl Node for Gain {
    fn kind(&self) -> &'static str {
        "gain"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        _domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        Ok(Box::new(GainStage {
            ty: inputs[0].clone(),
            gain: *self,
        }))
    }
}

struct GainStage {
    ty: SignalType,
    gain: Gain,
}

impl Stage for GainStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("gain", self.gain.factor.to_bits(), self.gain.divide))
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(GainKernel {
            gain: self.gain,
            lane,
        })
    }
}

struct GainKernel {
    gain: Gain,
    lane: Range<usize>,
}

impl Kernel for GainKernel {
    fn reset(&mut self) {}

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        let Gain { factor, divide } = self.gain;
        for (row, channel) in out.chunks_exact_mut(span.frames).zip(self.lane.clone()) {
            for (y, x) in row.iter_mut().zip(input.channel(channel)) {
                *y = if divide { x / factor } else { x * factor };
            }
        }
    }
}

/// Adds its two inputs. The output keeps the first input's channels, and each
/// takes the second input's channel of the same name, wherever that is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Add;

/// Subtracts the second input from the first, pairing channels as [`Add`]
/// does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sub;

impl Node for Add {
    fn kind(&self) -> &'static str {
        "add"
    }

    fn bind(&self, inputs: &[SignalType], _: &mut Domains) -> Result<Box<dyn Stage>, BuildError> {
        pairwise(self.kind(), inputs, false)
    }
}

impl Node for Sub {
    fn kind(&self) -> &'static str {
        "sub"
    }

    fn bind(&self, inputs: &[SignalType], _: &mut Domains) -> Result<Box<dyn Stage>, BuildError> {
        pairwise(self.kind(), inputs, true)
    }
}

/// Pairs the channels of two inputs by name.
fn pairwise(
    kind: &'static str,
    inputs: &[SignalType],
    subtract: bool,
) -> Result<Box<dyn Stage>, BuildError> {
    arity(kind, inputs, 2)?;
    let (left, right) = (&inputs[0], &inputs[1]);
    let unpaired = |name: &str| BuildError::UnpairedChannel {
        kind,
        name: name.to_owned(),
    };
    let mut partners = Vec::with_capacity(left.channels.len());
    for channel in left.channels.iter() {
        let partner = right
            .channels
            .position(&channel.name)
            .ok_or_else(|| unpaired(&channel.name))?;
        let other = &right
            .channels
            .get(partner)
            .expect("position is in range")
            .unit;
        if *other != channel.unit {
            return Err(BuildError::UnitMismatch {
                kind,
                channel: channel.name.clone(),
                left: channel.unit.clone(),
                right: other.clone(),
            });
        }
        partners.push(partner);
    }
    if let Some(extra) = right
        .channels
        .iter()
        .find(|c| left.channels.position(&c.name).is_none())
    {
        return Err(unpaired(&extra.name));
    }
    Ok(Box::new(PairStage {
        ty: left.clone(),
        partners,
        subtract,
    }))
}

struct PairStage {
    ty: SignalType,
    /// For each channel of the first input, where the second stores its
    /// partner.
    partners: Vec<usize>,
    subtract: bool,
}

impl Stage for PairStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("pair", &self.partners, self.subtract))
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(PairKernel {
            partners: self.partners[lane.clone()].to_vec(),
            subtract: self.subtract,
            lane,
        })
    }
}

struct PairKernel {
    lane: Range<usize>,
    partners: Vec<usize>,
    subtract: bool,
}

impl Kernel for PairKernel {
    fn reset(&mut self) {}

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let left = inputs.block(0);
        let right = inputs.block(1);
        for ((row, channel), partner) in out
            .chunks_exact_mut(span.frames)
            .zip(self.lane.clone())
            .zip(&self.partners)
        {
            let pairs = left.channel(channel).iter().zip(right.channel(*partner));
            for (y, (a, b)) in row.iter_mut().zip(pairs) {
                *y = if self.subtract { a - b } else { a + b };
            }
        }
    }
}

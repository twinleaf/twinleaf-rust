//! Nodes that change which channels a signal has and what they are called,
//! and no sample.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Inputs, Kernel, Node, Stage};
use crate::signal::{Channel, ChannelSet, Domains, SignalType, Span};
use std::ops::Range;
use std::sync::Arc;

/// Takes some channels of a signal, each under a new name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pick {
    /// The channel to take, and what to call it.
    picks: Vec<(String, String)>,
}

impl Pick {
    pub fn new<F: Into<String>, T: Into<String>>(picks: impl IntoIterator<Item = (F, T)>) -> Self {
        Self {
            picks: picks
                .into_iter()
                .map(|(from, to)| (from.into(), to.into()))
                .collect(),
        }
    }
}

impl Node for Pick {
    fn kind(&self) -> &'static str {
        "pick"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        _domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        let mut from = Vec::with_capacity(self.picks.len());
        let mut channels = Vec::with_capacity(self.picks.len());
        for (source, name) in &self.picks {
            let position =
                inputs[0]
                    .channels
                    .position(source)
                    .ok_or_else(|| BuildError::NoChannel {
                        kind: self.kind(),
                        name: source.clone(),
                    })?;
            let taken = inputs[0]
                .channels
                .get(position)
                .expect("position is in range");
            from.push(position);
            channels.push(Channel {
                name: name.clone(),
                unit: taken.unit.clone(),
            });
        }
        Ok(Box::new(CopyStage {
            ty: SignalType {
                domain: inputs[0].domain,
                channels: ChannelSet::new(channels)?,
            },
            from,
        }))
    }
}

/// Gives a signal's channels new names and units, position by position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relabel {
    channels: Arc<ChannelSet>,
}

impl Relabel {
    pub fn to(channels: Arc<ChannelSet>) -> Self {
        Self { channels }
    }
}

impl Node for Relabel {
    fn kind(&self) -> &'static str {
        "relabel"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        _domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        let found = inputs[0].channels.len();
        if found != self.channels.len() {
            return Err(BuildError::ChannelCount {
                kind: self.kind(),
                expected: self.channels.len(),
                found,
            });
        }
        Ok(Box::new(CopyStage {
            ty: SignalType {
                domain: inputs[0].domain,
                channels: Arc::clone(&self.channels),
            },
            from: (0..found).collect(),
        }))
    }
}

/// Copies input channels to output channels.
struct CopyStage {
    ty: SignalType,
    /// For each output channel, the input channel it is a copy of.
    from: Vec<usize>,
}

impl Stage for CopyStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("copy", &self.from))
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(CopyKernel {
            from: self.from[lane].to_vec(),
        })
    }
}

struct CopyKernel {
    from: Vec<usize>,
}

impl Kernel for CopyKernel {
    fn reset(&mut self) {}

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        for (row, &channel) in out.chunks_exact_mut(span.frames).zip(&self.from) {
            row.copy_from_slice(input.channel(channel));
        }
    }
}

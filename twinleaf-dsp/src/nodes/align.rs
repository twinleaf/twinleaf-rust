//! Merging signals from different domains into one.
//!
//! The arms must run at the same rate on the same time base. What may differ
//! is where each arm's sample 0 falls. `align` takes the first arm's grid as
//! the result's, and reads every arm at those instants.
//!
//! When the arms' grids coincide, that is a shift by whole samples and no
//! sample is changed. When they do not, every arm, the first included, is
//! read through the same centred low-pass of [`crate::interp`], each at its
//! own fraction of a sample. Filtering the first arm too is deliberate. An
//! arm left untouched beside arms that were interpolated would differ from
//! them near the band edge, and a difference between arms is exactly what a
//! gradiometer measures.

use crate::error::BuildError;
use crate::interp::Interpolator;
use crate::node::{fingerprint, Coupling, Extent, Inputs, Kernel, Node, Stage};
use crate::signal::{Channel, ChannelSet, Domains, SignalType, Span};
use std::ops::Range;
use std::sync::Arc;

/// How close to a whole sample an arm's offset must be to count as one. The
/// offset is a difference of two times in seconds, which is not exact.
const ON_GRID: f64 = 1e-6;

/// Merges its inputs into one domain. Each arm's channels are renamed
/// `arm.channel`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Align {
    arms: Vec<String>,
}

impl Align {
    /// Creates an align whose inputs are called `arms`, in order.
    pub fn new<S: Into<String>>(arms: impl IntoIterator<Item = S>) -> Self {
        Self {
            arms: arms.into_iter().map(Into::into).collect(),
        }
    }
}

impl Node for Align {
    fn kind(&self) -> &'static str {
        "align"
    }

    fn merges_domains(&self) -> bool {
        true
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        let kind = self.kind();
        if inputs.len() != self.arms.len() || inputs.len() < 2 {
            return Err(BuildError::Arity {
                kind,
                expected: self.arms.len().max(2),
                found: inputs.len(),
            });
        }
        let domain = |ty: &SignalType| {
            domains
                .get(ty.domain)
                .copied()
                .ok_or(BuildError::UnknownDomain)
        };
        let reference = domain(&inputs[0])?;

        // Where each arm's samples fall on the reference grid: output sample
        // `j` is the arm's sample `j + shift + fraction`.
        let mut offsets = Vec::with_capacity(inputs.len());
        for ty in inputs {
            let arm = domain(ty)?;
            if arm.rate != reference.rate {
                return Err(BuildError::DomainMismatch {
                    kind,
                    left: reference,
                    right: arm,
                });
            }
            if arm.epoch != reference.epoch || arm.session != reference.session {
                return Err(BuildError::Timebase {
                    kind,
                    left: reference,
                    right: arm,
                });
            }
            let offset = (reference.origin - arm.origin) * reference.rate;
            let nearest = offset.round();
            offsets.push(if (offset - nearest).abs() < ON_GRID {
                (nearest as i64, 0.0)
            } else {
                (offset.floor() as i64, offset - offset.floor())
            });
        }

        let filter = offsets
            .iter()
            .any(|(_, fraction)| *fraction != 0.0)
            .then(|| Interpolator::for_band(0.5));
        let arms: Vec<Arm> = offsets
            .iter()
            .map(|&(shift, fraction)| Arm {
                shift,
                taps: Arc::new(match &filter {
                    Some(filter) => filter.taps(fraction),
                    None => vec![1.0],
                }),
            })
            .collect();

        let mut channels = Vec::new();
        let mut from = Vec::new();
        for (arm, (name, ty)) in self.arms.iter().zip(inputs).enumerate() {
            for (index, channel) in ty.channels.iter().enumerate() {
                channels.push(Channel {
                    name: format!("{name}.{}", channel.name),
                    unit: channel.unit.clone(),
                });
                from.push((arm, index));
            }
        }
        Ok(Box::new(AlignStage {
            ty: SignalType {
                domain: domains.add(
                    reference.epoch,
                    reference.session,
                    reference.rate,
                    reference.origin,
                )?,
                channels: ChannelSet::new(channels)?,
            },
            half_width: filter.map_or(0, |filter| filter.half_width() as i128),
            arms,
            from,
        }))
    }
}

struct Arm {
    /// Whole samples from an output index to this arm's.
    shift: i64,
    /// The weights that read the arm at its fraction of a sample.
    taps: Arc<Vec<f64>>,
}

struct AlignStage {
    ty: SignalType,
    /// How far the filter reaches on each side. Zero when the grids coincide.
    half_width: i128,
    arms: Vec<Arm>,
    /// For each output channel, its arm and its channel within that arm.
    from: Vec<(usize, usize)>,
}

impl AlignStage {
    /// The first output index that cannot be computed when each arm's input
    /// ends at `end`.
    fn ready(&self, inputs: &[Extent], end: fn(&Extent) -> u64) -> i128 {
        self.arms
            .iter()
            .zip(inputs)
            .map(|(arm, input)| i128::from(end(input)) - self.half_width - i128::from(arm.shift))
            .min()
            .unwrap_or(0)
    }
}

impl Stage for AlignStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn fingerprint(&self) -> u64 {
        let arms: Vec<(i64, Vec<u64>)> = self
            .arms
            .iter()
            .map(|arm| {
                (
                    arm.shift,
                    arm.taps.iter().map(|tap| tap.to_bits()).collect(),
                )
            })
            .collect();
        fingerprint(("align", arms, &self.from, self.half_width))
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn span(&self, inputs: &[Extent]) -> Span {
        // The first output every arm's run reaches back far enough for.
        let earliest = self
            .arms
            .iter()
            .zip(inputs)
            .map(|(arm, input)| i128::from(input.since) + self.half_width - i128::from(arm.shift))
            .max()
            .unwrap_or(0);
        let first = earliest.max(self.ready(inputs, |input| input.first)).max(0);
        let end = first.max(self.ready(inputs, Extent::end));
        Span {
            first: first as u64,
            frames: (end - first) as usize,
        }
    }

    fn buffers(&self) -> bool {
        true
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(AlignKernel {
            half_width: self.half_width,
            channels: self.from[lane]
                .iter()
                .map(|&(arm, channel)| Held {
                    arm,
                    channel,
                    shift: i128::from(self.arms[arm].shift),
                    taps: Arc::clone(&self.arms[arm].taps),
                    history: Vec::new(),
                    start: 0,
                })
                .collect(),
        })
    }
}

/// One output channel's input, held until every arm has caught up with it.
struct Held {
    arm: usize,
    channel: usize,
    shift: i128,
    taps: Arc<Vec<f64>>,
    history: Vec<f64>,
    /// The arm's sample that `history` begins at.
    start: u64,
}

struct AlignKernel {
    half_width: i128,
    channels: Vec<Held>,
}

impl Kernel for AlignKernel {
    fn reset(&mut self) {
        for held in &mut self.channels {
            held.history.clear();
        }
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let half_width = self.half_width;
        for (index, held) in self.channels.iter_mut().enumerate() {
            let block = inputs.block(held.arm);
            if block.frames > 0 {
                if held.history.is_empty() {
                    held.start = block.first;
                }
                held.history.extend_from_slice(block.channel(held.channel));
            }
            // The arm's sample that the window for output `j` begins at.
            let window = |j: u64| i128::from(j) + held.shift - half_width;
            if span.frames > 0 {
                let row = &mut out[index * span.frames..(index + 1) * span.frames];
                for (y, j) in row.iter_mut().zip(span.first..) {
                    let from = (window(j) - i128::from(held.start)) as usize;
                    *y = held
                        .taps
                        .iter()
                        .zip(&held.history[from..from + held.taps.len()])
                        .map(|(tap, x)| tap * x)
                        .sum();
                }
            }
            let start = i128::from(held.start);
            let keep = window(span.first + span.frames as u64)
                .clamp(start, start + held.history.len() as i128);
            held.history.drain(..(keep - start) as usize);
            held.start = keep as u64;
        }
    }
}

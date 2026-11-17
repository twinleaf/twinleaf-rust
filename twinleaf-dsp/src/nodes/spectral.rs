//! A run's spectrum.

use crate::error::BuildError;
use crate::node::{arity, fingerprint, Coupling, Extent, Inputs, Kernel, Node, Stage};
use crate::nodes::window::samples_in;
use crate::signal::{Domains, SignalType, Span};
use crate::spectrum;
use std::ops::Range;

/// The power spectral density of each channel over the last `window`
/// seconds of a run, by the twinleaf library's Welch estimate.
///
/// What comes out is not a signal in time. Its samples are frequency bins.
/// Sample `k` is the density at `(k + 1)` times the bin spacing, in the
/// channel's unit squared per hertz, one sided. Its domain says so in the
/// only way a domain can. Its rate is bins per hertz, and its origin is the
/// first bin's frequency.
///
/// It needs the whole of a run, and says nothing of a run shorter than its
/// window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Psd {
    /// How much of the end of the run to take, in seconds.
    pub window: f64,
}

impl Node for Psd {
    fn kind(&self) -> &'static str {
        "psd"
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        arity(self.kind(), inputs, 1)?;
        let parent = *domains
            .get(inputs[0].domain)
            .ok_or(BuildError::UnknownDomain)?;
        let samples = samples_in(self.window, parent.rate);
        let (bins, spacing) =
            spectrum::shape(samples, parent.rate).ok_or(BuildError::InvalidRate { rate: 0.0 })?;
        Ok(Box::new(PsdStage {
            ty: SignalType {
                domain: domains.add(parent.epoch, parent.session, 1.0 / spacing, spacing)?,
                channels: inputs[0].channels.clone(),
            },
            samples,
            bins,
            rate: parent.rate,
        }))
    }
}

struct PsdStage {
    ty: SignalType,
    samples: usize,
    bins: usize,
    rate: f64,
}

impl Stage for PsdStage {
    fn output(&self) -> SignalType {
        self.ty.clone()
    }

    fn coupling(&self) -> Coupling {
        Coupling::Independent
    }

    fn fingerprint(&self) -> u64 {
        fingerprint(("psd", self.samples, self.rate.to_bits()))
    }

    /// Nothing until the run ends, and then one spectrum, if the run was
    /// as long as the window.
    fn span(&self, inputs: &[Extent]) -> Span {
        let input = inputs[0];
        let whole = input.closed && (input.end() - input.since) as usize >= self.samples;
        Span {
            first: 0,
            frames: if whole { self.bins } else { 0 },
        }
    }

    fn buffers(&self) -> bool {
        true
    }

    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel> {
        Box::new(PsdKernel {
            held: vec![Vec::new(); lane.len()],
            samples: self.samples,
            rate: self.rate,
            lane,
        })
    }
}

struct PsdKernel {
    lane: Range<usize>,
    samples: usize,
    rate: f64,
    /// The latest samples of each lane channel. There is at least the
    /// window, once the run is that long.
    held: Vec<Vec<f64>>,
}

impl Kernel for PsdKernel {
    fn reset(&mut self) {
        self.held.iter_mut().for_each(Vec::clear);
    }

    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]) {
        let input = inputs.block(0);
        for (held, channel) in self.held.iter_mut().zip(self.lane.clone()) {
            held.extend_from_slice(input.channel(channel));
            // Only the window is ever looked at. Let go of the rest now
            // and then, rather than keep a whole recording.
            if held.len() > 2 * self.samples {
                held.drain(..held.len() - self.samples);
            }
        }
        if span.frames == 0 {
            return;
        }
        for (row, held) in out.chunks_exact_mut(span.frames).zip(&mut self.held) {
            let latest = &held[held.len() - self.samples..];
            let points = spectrum::density(latest, self.rate);
            // A bin with no power in it is left out of the estimate, and
            // is zero here.
            row.fill(0.0);
            let spacing = self.rate / 2.0 / (span.frames + 1) as f64;
            for (frequency, density) in points {
                let bin = (frequency / spacing).round() as usize;
                if (1..=row.len()).contains(&bin) {
                    row[bin - 1] = density * density;
                }
            }
            held.clear();
        }
    }
}

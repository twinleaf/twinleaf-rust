//! What a node is, in three steps.
//!
//! A [`Node`] is the node as written, a kind and its parameters. Binding it
//! to its inputs gives a [`Stage`], which knows its output type and
//! everything else that depends on what it was wired to. A stage is expanded
//! into [`Kernel`]s, one per lane, which hold the signal state and run the
//! inner loops.

use crate::error::BuildError;
use crate::signal::{Block, Domains, SignalType, Span};
use std::ops::Range;

/// Whether a node's channels can be computed apart from one another. It decides
/// how the node is split into parallel tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coupling {
    /// Each output channel depends on no other output channel's state. The
    /// node is split into one kernel per bundle of channels.
    Independent,
    /// Output channels depend on one another. The node stays whole, as one
    /// kernel over every channel.
    Coupled,
}

/// A node of the graph as written. It holds parameters and no signal state.
pub trait Node: Send + Sync {
    /// The node's name in diagnostics, such as `biquad`.
    fn kind(&self) -> &'static str;

    /// Whether the node takes inputs from different domains, covering
    /// different spans. For every other node the graph checks that the inputs
    /// share one domain, and at each tick one span.
    fn merges_domains(&self) -> bool {
        false
    }

    /// Checks the input types and fixes everything that depends on them. A
    /// node that changes the rate, or merges domains, makes its output's
    /// domain here.
    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError>;
}

impl<N: Node + ?Sized> Node for Box<N> {
    fn kind(&self) -> &'static str {
        (**self).kind()
    }

    fn merges_domains(&self) -> bool {
        (**self).merges_domains()
    }

    fn bind(
        &self,
        inputs: &[SignalType],
        domains: &mut Domains,
    ) -> Result<Box<dyn Stage>, BuildError> {
        (**self).bind(inputs, domains)
    }
}

/// A node bound to its inputs.
pub trait Stage: Send + Sync {
    /// The type of the output edge.
    fn output(&self) -> SignalType;

    fn coupling(&self) -> Coupling;

    /// A number that is the same for two stages exactly when they would
    /// compute the same thing from the same inputs. It covers the kind of
    /// stage and everything it was bound with. When a spec is changed while
    /// it runs, a stage whose fingerprint and whose inputs' are unchanged
    /// keeps its state, and the rest start over.
    fn fingerprint(&self) -> u64;

    /// Returns the samples the stage emits this tick. The answer depends on
    /// where its inputs' samples are and never on their values, so that every
    /// span is settled before any kernel runs. Stages that emit a sample for
    /// each sample they are given keep the default.
    fn span(&self, inputs: &[Extent]) -> Span {
        inputs[0].span()
    }

    /// Whether the stage holds samples back, so that its kernels must see
    /// every block even in a tick where it emits nothing.
    fn buffers(&self) -> bool {
        false
    }

    /// Makes the kernel that computes output channels `lane`. For a coupled
    /// stage `lane` is every output channel.
    fn kernel(&self, lane: Range<usize>) -> Box<dyn Kernel>;
}

/// The state and inner loop for one lane of a stage. Kernels of one stage run
/// concurrently, each writing its own part of the output.
pub trait Kernel: Send {
    /// Discards all state, as at a new run.
    fn reset(&mut self);

    /// Computes the samples `span` of this kernel's lane.
    ///
    /// `out` is the lane's part of the output block, channel-major, one row
    /// of `span.frames` samples per lane channel. Its previous contents are
    /// unspecified, and every element must be written. It is empty only for
    /// a stage that buffers.
    fn process(&mut self, inputs: &Inputs<'_>, span: Span, out: &mut [f64]);
}

/// Where one input's samples are. That is this tick's block, and where the
/// run it belongs to began for the stage reading it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    /// The first sample the stage was given since it last started over.
    pub since: u64,
    /// The first sample of this tick's block.
    pub first: u64,
    pub frames: usize,
    /// Whether the run ends with this block, so that no sample will follow
    /// it. A stage that needs the whole of a run before it can say anything
    /// emits then, and until then emits nothing.
    pub closed: bool,
}

impl Extent {
    /// The sample after the last of this tick's block.
    pub fn end(&self) -> u64 {
        self.first + self.frames as u64
    }

    pub fn span(&self) -> Span {
        Span {
            first: self.first,
            frames: self.frames,
        }
    }
}

/// The input blocks of one stage for one tick, complete and read-only.
pub struct Inputs<'a> {
    blocks: &'a [Block],
    indices: &'a [usize],
}

impl<'a> Inputs<'a> {
    pub(crate) fn new(blocks: &'a [Block], indices: &'a [usize]) -> Self {
        Self { blocks, indices }
    }

    /// Returns how many inputs the stage has.
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns whether the stage has no inputs, which no stage in a graph does.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the block on input `index`, in the order the node was wired.
    pub fn block(&self, index: usize) -> &'a Block {
        &self.blocks[self.indices[index]]
    }
}

/// Hashes the parts a stage is made of, for [`Stage::fingerprint`]. A float
/// goes in as its bits.
pub fn fingerprint(parts: impl std::hash::Hash) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    parts.hash(&mut hasher);
    hasher.finish()
}

/// Checks that a node was wired with the number of inputs it takes.
pub fn arity(kind: &'static str, inputs: &[SignalType], expected: usize) -> Result<(), BuildError> {
    if inputs.len() == expected {
        Ok(())
    } else {
        Err(BuildError::Arity {
            kind,
            expected,
            found: inputs.len(),
        })
    }
}

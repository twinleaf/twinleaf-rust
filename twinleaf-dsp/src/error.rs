//! What can go wrong building a graph and running it.

use crate::signal::{Domain, Unit};

/// Why a graph could not be built.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BuildError {
    #[error("a signal needs at least one channel")]
    NoChannels,
    #[error("channel `{name}` appears twice, and channels are matched by name")]
    DuplicateChannel { name: String },
    #[error("a domain's rate must be a positive, finite number of samples per second, not {rate}")]
    InvalidRate { rate: f64 },
    #[error("the domain does not belong to this graph")]
    UnknownDomain,
    #[error("the node does not belong to this graph")]
    UnknownNode,
    #[error("`{kind}` takes {expected} input(s), not {found}")]
    Arity {
        kind: &'static str,
        expected: usize,
        found: usize,
    },
    #[error("`{kind}` cannot combine signals in different domains ({left}) and ({right})")]
    DomainMismatch {
        kind: &'static str,
        left: Domain,
        right: Domain,
    },
    #[error("`{kind}` cannot merge signals that do not share a time base ({left}) and ({right})")]
    Timebase {
        kind: &'static str,
        left: Domain,
        right: Domain,
    },
    #[error(
        "`{kind}` cannot change the rate by {up}/{down}: both terms must be between 1 and 4096"
    )]
    RateRatio {
        kind: &'static str,
        up: u64,
        down: u64,
    },
    #[error("`{kind}` was asked for the rate its input already has")]
    NoRateChange { kind: &'static str },
    #[error("`{kind}` has no channel called `{name}` to take")]
    NoChannel { kind: &'static str, name: String },
    #[error("`{kind}` was given {found} channel(s) to rename {expected}")]
    ChannelCount {
        kind: &'static str,
        expected: usize,
        found: usize,
    },
    #[error("`{kind}` pairs channels by name, and `{name}` has no partner")]
    UnpairedChannel { kind: &'static str, name: String },
    #[error("`{kind}` cannot combine `{channel}` in {left} with {right}")]
    UnitMismatch {
        kind: &'static str,
        channel: String,
        left: Unit,
        right: Unit,
    },
    #[error("source `{name}` is declared twice")]
    DuplicateSource { name: String },
    #[error("output `{name}` is declared twice")]
    DuplicateOutput { name: String },
    #[error("a graph needs at least one output")]
    NoOutputs,
    #[error("{0}")]
    InvalidOptions(&'static str),
    #[error("could not start the thread pool: {0}")]
    Pool(String),
}

/// Why a tick was refused. A refused tick leaves the graph as it was.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TickError {
    #[error("the graph has {expected} source(s), but {found} block(s) were supplied")]
    InputCount { expected: usize, found: usize },
    #[error("the block for source `{name}` is in another domain")]
    WrongDomain { name: String },
    #[error("the block for source `{name}` carries other channels than the source declares")]
    WrongChannels { name: String },
    #[error("the block for source `{name}` holds {found} samples, not the {expected} its shape calls for")]
    WrongLength {
        name: String,
        expected: usize,
        found: usize,
    },
    #[error("source `{name}` jumped from sample {expected} to {found} without starting a new run")]
    Discontinuity {
        name: String,
        expected: u64,
        found: u64,
    },
    #[error("the inputs of `{kind}` cover different spans of samples")]
    SpanMismatch { kind: String },
}

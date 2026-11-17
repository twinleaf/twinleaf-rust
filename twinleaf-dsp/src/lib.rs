//! A DSP flow graph over Twinleaf sensor streams, written as text and served
//! as a TIO device.
//!
//! The README says what a signal graph is, what a spec may call, and how
//! `tio dsp` runs one. This is how the crate is laid out.
//!
//! - The engine is the signal model ([`Block`], [`Domain`], [`SignalType`]),
//!   the [`Node`] and [`Kernel`] traits, and a [`Graph`] that expands nodes
//!   into lane tasks and runs them on a work-stealing pool.
//! - The language is in [`lang`]. A spec is parsed into a lossless syntax
//!   tree and checked into a typed [`lang::Program`], with diagnostics, and
//!   [`lang::outline`] says what an editor needs to know about its text.
//! - Running one takes the rest. [`lower`] makes a program into a graph,
//!   [`run`] feeds it from a recording or from live streams, [`session`]
//!   lets its text be changed while it runs, and [`serve`] publishes it as a
//!   device. [`client`] is the other end of that device's spec.
//! - The command line that `tio dsp` runs is in `cli`, and [`Library`] is
//!   how a build adds functions of its own to it.
//!
//! The engine never reads a clock. A graph's output is a function of the
//! samples it was fed, and not of how they were cut into blocks, how many
//! threads ran it, or how its nodes were bundled into tasks.

#![forbid(unsafe_code)]

#[cfg(feature = "cli")]
pub mod cli;
pub mod client;
pub mod design;
pub mod error;
pub mod figure;
pub mod graph;
pub mod interp;
pub mod lang;
pub mod library;
pub mod lower;
pub mod node;
pub mod nodes;
pub mod publish;
pub mod reduce;
pub mod run;
pub mod serve;
pub mod session;
pub mod signal;
pub mod spectrum;

pub use error::{BuildError, TickError};
pub use graph::{Graph, GraphBuilder, NodeId, Options};
pub use library::Library;
pub use node::{Coupling, Extent, Inputs, Kernel, Node, Stage};
pub use signal::{
    Block, Channel, ChannelSet, Domain, DomainId, Domains, RunId, SignalType, Span, Unit,
};

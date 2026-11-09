//! Computation over what is kept. The stages a processor drives from a
//! buffer, and the stages built in.

pub(super) mod pipeline;

#[cfg(feature = "processing")]
pub mod ops;

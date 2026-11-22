//! Computation over what is kept. The stages a processor drives from a
//! buffer, the stages built in, and the history that reads spans of any
//! length back through them.

pub(super) mod pipeline;

#[cfg(feature = "processing")]
pub(super) mod history;
#[cfg(feature = "processing")]
pub mod ops;

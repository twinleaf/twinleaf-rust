//! Signal processing over one column, as [`ColumnOp`](super::ColumnOp)s for a
//! [`ColumnProcessor`](super::ColumnProcessor) to drive.
//!
//! - [`Fpcs`] decimates a column for drawing and keeps its peaks.
//! - [`FpcsLevels`] decimates a column at several ratios at once, so a plot of
//!   any span finds one about as dense as its pixels.
//! - [`Welch`] estimates a column's amplitude spectral density, and
//!   [`estimate_white_noise_floor`] finds the white noise level in one.
//!
//! Each op takes every sample of one run in order and holds its result. A
//! processor feeds it only the rows it has not seen. It resets the op and
//! replays the column when the run or the op's parameters change.

mod fpcs;
mod fpcs_levels;
mod welch;

pub use fpcs::Fpcs;
pub use fpcs_levels::FpcsLevels;
pub use welch::{estimate_white_noise_floor, NoSpectrum, Spectrum, Welch};

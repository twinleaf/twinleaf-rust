//! Signal processing over one column, as [`ColumnOp`](crate::ColumnOp)s for a
//! [`ColumnProcessor`](crate::ColumnProcessor) to drive.
//!
//! - [`Fpcs`] decimates a column for drawing and keeps its peaks.
//! - [`Pyramid`] keeps decimations of a column at ratios that double, of the
//!   newest rows only at its finest levels, and [`Stretch`] reads any span of
//!   a run from them at a cost set by the points it asks for.
//! - [`Stats`] sums a column's rows up as their count, extremes, mean and
//!   standard deviation.
//! - [`Welch`] estimates a column's amplitude spectral density, and
//!   [`estimate_white_noise_floor`] finds the white noise level in one and
//!   [`largest_peaks`] its largest peaks.
//!
//! Each op takes every sample of one run in order and holds its result. A
//! processor feeds it only the rows it has not seen. It resets the op and
//! replays the column when the run or the op's parameters change. A read
//! of a span, as [`History::rows`](crate::History::rows) returns it,
//! feeds an op with [`ColumnOp::feed_rows`](crate::ColumnOp::feed_rows).

mod fpcs;
mod pyramid;
mod stats;
mod welch;

pub use fpcs::Fpcs;
pub(crate) use pyramid::{level_for, FINE};
pub use pyramid::{split_at_gaps, Level, Pyramid, Slice, Stretch};
#[cfg(test)]
pub(crate) use pyramid::{Point, BLOCK, FINEST};
pub use stats::Stats;
pub use welch::{estimate_white_noise_floor, largest_peaks, NoSpectrum, Spectrum, Welch};

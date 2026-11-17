//! The node library.

mod align;
mod arith;
mod biquad;
mod calculus;
mod decorrelate;
mod magnitude;
mod resample;
mod select;
mod spectral;
mod whole;
mod window;

pub use align::Align;
pub use arith::{Add, Gain, Sub};
pub use biquad::{Biquad, Section};
pub use calculus::{Derivative, Integrate};
pub use decorrelate::{Decorrelate, Method};
pub use magnitude::Magnitude;
pub use resample::Resample;
pub use select::{Pick, Relabel};
pub use spectral::Psd;
pub use whole::{Detrend, ZeroPhase};
pub use window::{samples_in, Rms};

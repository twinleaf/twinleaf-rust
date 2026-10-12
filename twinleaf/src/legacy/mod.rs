//! The 1.x library, kept while the 2.0 library is built beside it.

// The 1.x code keeps the lints it was written under.
#![allow(
    missing_docs,
    missing_debug_implementations,
    clippy::all,
    clippy::pedantic,
    rustdoc::all
)]

pub mod data;
pub mod device;
pub mod firmware;
pub mod tio;

pub use device::Device;
pub use tio::proxy::Interface as ProxyInterface;

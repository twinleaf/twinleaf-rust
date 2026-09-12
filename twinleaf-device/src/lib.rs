#![doc = include_str!("../README.md")]
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(rustdoc::all)]

pub mod calls;
pub mod capture;
pub mod device;
pub mod hub;
pub mod metadata;
pub mod publisher;
pub mod rpc;
pub mod segments;
pub mod settings;
pub mod stream;
pub mod sync;

/// Where a device's packets go.
pub trait Sink {
    /// Send one complete packet.
    fn send(&mut self, packet: &[u8]);
}

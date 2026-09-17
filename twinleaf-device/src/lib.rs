//! What a Twinleaf device does with the packets it receives and the samples it
//! takes, and nothing else: no transport, no allocation, no clock of its own.
//! Every piece is a state machine stepped by whoever owns the wire and the
//! nanosecond — a firmware task, a host tool, or a test — and
//! [twinleaf-proto](https://docs.rs/twinleaf-proto) encodes what crosses it.
//!
//! The map below is the order to read the modules in.
//!
//! - [`device`] — the one machine that answers a request: the device, its
//!   settings, and the [`Actions`](device::Actions) a platform is to perform.
//! - [`rpc`] — what a request finds: the standard table and its `rpc.hash`,
//!   the [`Setting`](rpc::Setting) cells behind properties, and capture.
//! - [`data`] — what a stream carries: its definition, its segment ring, the
//!   sample packets it sends, and the `dev.metadata` records describing it.
//! - [`sync`] — the reference timeline: pulse tracking, the oscillator servo,
//!   and the acquisition scheduler, free of any counter peripheral.
//! - [`hub`] — the child-facing half: routing, presence, and the table of
//!   requests still waiting for an answer.
//! - [`storage`] — what outlives a boot: the configuration image and the
//!   firmware upload cursor.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(rustdoc::all)]

pub mod device;
pub mod rpc;

pub mod data;
pub mod sync;

pub mod hub;
pub mod storage;

pub use device::Device;

/// Where a device's packets go.
pub trait Sink {
    /// Send one complete packet.
    fn send(&mut self, packet: &[u8]);
}

impl<S: Sink + ?Sized> Sink for &mut S {
    fn send(&mut self, packet: &[u8]) {
        (**self).send(packet);
    }
}

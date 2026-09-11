#![doc = include_str!("../README.md")]
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(rustdoc::all)]

pub mod capture;
pub mod metadata;
pub mod rpc;

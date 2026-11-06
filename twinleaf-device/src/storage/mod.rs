//! What outlives a boot: the configuration image a device saves its settings
//! as, and the firmware package it takes an upload of.
//!
//! Inward come the bytes a platform read back from flash; outward go the bytes
//! it is to write and where they go. The flash itself, its keys, and its
//! erase are the platform's. Both modules stay named because each has its own
//! `Image`.

pub mod conf;
pub mod update;

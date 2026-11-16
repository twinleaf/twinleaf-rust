//! Recorded logs. Reading and indexing them, and exporting them to HDF5.

pub(super) mod reader;

#[cfg(feature = "hdf5")]
pub mod export;

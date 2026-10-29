//! The payload codecs, one module for each kind of payload a packet carries.

pub mod capture;
pub mod data;
pub mod heartbeat;
pub mod log;
pub mod rpc;
pub mod settings;
pub mod sync;

/// Returns the `N` bytes of `bytes` from offset `at`, or `None` if they run
/// past the end.
pub(crate) fn array_at<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..)?.first_chunk().copied()
}

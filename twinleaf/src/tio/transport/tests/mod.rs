pub(super) mod fake;
/// The tests a turmoil build leaves out, as they run on the real network.
#[cfg(not(turmoil))]
mod real;

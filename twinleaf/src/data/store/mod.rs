//! What is kept of each stream. The buffer of runs, and the coalescing of
//! batches into the chunks a run holds.

pub(super) mod buffer;
pub(super) mod coalesce;

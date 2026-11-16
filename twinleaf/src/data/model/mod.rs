//! What a batch of samples is. A stream's schema, its typed columns, the
//! continuity of the run it belongs to, and which columns a reader selects.

pub(super) mod continuity;
pub(super) mod filter;
pub(super) mod sample;
pub(super) mod schema;

//! Routed packets into batches. The parser, the state it keeps of each
//! stream, and the decoding of a packet's columns.

pub(super) mod body;
pub(super) mod parser;
pub(super) mod state;

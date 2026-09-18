//! What a stream carries: its definition, the segments it is acquiring, the
//! packets it sends, and the records it describes itself with.
//!
//! Inward come samples, one per [`Stream::push`], and the [`Params`] a setting
//! changed; outward go packets to a [`Sink`](crate::Sink) and the
//! [`metadata`] reply a host asks for. A board implements [`Streams`] over the
//! streams it has; nothing here decides when a sample is taken.

mod filter;
mod publisher;
mod segments;
mod stream;

pub mod metadata;

pub use filter::{Butterworth, Filter, CORNER, MAX_COLUMNS};
pub use metadata::Streams;
pub use publisher::{Publisher, MAX_SAMPLE_BYTES};
pub use segments::{Busy, Issued, Params, Segment, SegmentState, Segments, Timeref};
pub use stream::{ColumnDef, Stream, StreamDef};

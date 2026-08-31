//! Merging of small sample batches into larger ones.
//!
//! [`BatchCoalescer`] accumulates the batches of one stream into batches of
//! roughly `target_rows` rows. It is the one place that decides when rows may
//! share a batch: rows only merge while they carry the same metadata and
//! generations, and a batch that starts a boundary always stands alone.

use super::sample::{Generations, RowSource, SampleBatch, SampleBatchBuilder};
use std::{collections::VecDeque, ops::Range};

/// Accumulates one stream's batches into batches of about `target_rows` rows.
///
/// Pushed rows land in a buffered tail, which completes once it reaches the
/// target or once rows that cannot share a batch with it arrive. Completed
/// batches come back out in push order through
/// [`BatchCoalescer::next_completed_batch`].
pub(crate) struct BatchCoalescer {
    /// Rows to accumulate before completing a batch; `None` completes every
    /// pushed batch on its own.
    target_rows: Option<usize>,
    tail: Option<SampleBatchBuilder>,
    completed: VecDeque<SampleBatch>,
}

impl BatchCoalescer {
    pub fn new(target_rows: Option<usize>) -> BatchCoalescer {
        assert!(target_rows != Some(0), "batch row target must be nonzero");
        BatchCoalescer {
            target_rows,
            tail: None,
            completed: VecDeque::new(),
        }
    }

    /// Merge `rows` into the buffered tail, completing batches as needed.
    /// Empty inputs are ignored.
    pub(super) fn push(&mut self, rows: &impl RowSource) {
        if rows.len() == 0 {
            return;
        }
        if !self.tail_accepts(rows) {
            self.finish_buffered_batch();
        }
        let capacity = self.target_rows.unwrap_or(rows.len()).max(rows.len());
        let tail = self
            .tail
            .get_or_insert_with(|| rows.start_builder(capacity));
        rows.append_to(tail);
        let buffered = tail.len();
        // A boundary batch stands alone, so the rows after it start fresh.
        if rows.boundary().is_some() || self.target_rows.is_none_or(|target| buffered >= target) {
            self.finish_buffered_batch();
        }
    }

    /// Merge `batch` into the buffered tail, completing batches as needed.
    /// Empty batches are ignored.
    pub fn push_batch(&mut self, batch: &SampleBatch) {
        self.push(batch)
    }

    /// Pop the oldest completed batch.
    pub fn next_completed_batch(&mut self) -> Option<SampleBatch> {
        self.completed.pop_front()
    }

    /// Complete the buffered tail, however short it is.
    pub fn finish_buffered_batch(&mut self) {
        if let Some(tail) = self.tail.take() {
            self.completed.push_back(tail.finish());
        }
    }

    /// Rows held in the tail, not yet completed.
    pub fn buffered_len(&self) -> usize {
        self.tail.as_ref().map_or(0, SampleBatchBuilder::len)
    }

    /// The generations the buffered rows belong to, if any are buffered.
    pub fn buffered_generations(&self) -> Option<Generations> {
        self.tail.as_ref().map(SampleBatchBuilder::generations)
    }

    /// Freeze a copy of rows from the buffered tail, leaving it accumulating.
    pub(super) fn buffered_rows(&self, rows: Range<usize>) -> Option<SampleBatch> {
        self.tail.as_ref().map(|tail| tail.freeze_rows(rows))
    }

    /// Whether `rows` may continue the buffered tail. Rows merge only within
    /// one metadata and generation, and never across a boundary.
    fn tail_accepts(&self, rows: &impl RowSource) -> bool {
        rows.boundary().is_none() && self.tail.as_ref().is_some_and(|tail| tail.accepts(rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;
    use crate::data::sample::{BatchContext, Boundary, BoundaryReason, ColumnArray, ColumnData};
    use crate::data::StreamKey;
    use crate::data::StreamRecord;
    use crate::data::{BufferType, ColumnRecord, DeviceRecord, SegmentRecord};
    use crate::tio::proto::{DataType, DeviceRoute};
    use twinleaf_proto::data as wire;

    fn segment(segment_id: u8) -> SegmentRecord {
        SegmentRecord::encode(wire::Segment {
            segment_id: twinleaf_proto::SegmentId::new(segment_id),
            sampling_rate: 4,
            ..fixtures::segment(1)
        })
        .unwrap()
    }

    /// A float batch of `rows` rows numbered from `first`, whose values are the
    /// sample numbers.
    fn batch(
        segment: &SegmentRecord,
        first: u32,
        rows: u32,
        boundary: Option<Boundary>,
        generations: Generations,
    ) -> SampleBatch {
        let mut builder = SampleBatchBuilder::new(
            BatchContext::new(
                StreamKey::new(DeviceRoute::root(), twinleaf_proto::StreamId::new(1)),
                boundary,
                generations,
                segment.clone(),
                StreamRecord::encode(wire::Stream {
                    n_segments: 2,
                    sample_size: 8,
                    ..fixtures::stream(1)
                })
                .unwrap(),
                DeviceRecord::encode(fixtures::device()).unwrap(),
            ),
            [(
                ColumnRecord::encode(fixtures::column(1, 0, DataType::F64)).unwrap(),
                BufferType::Float,
            )],
            rows as usize,
        );
        for n in first..first + rows {
            builder.push_row(
                twinleaf_proto::SampleNumber::new(n),
                [ColumnData::Float(f64::from(n))],
            );
        }
        builder.finish()
    }

    fn generations(stream: u32) -> Generations {
        Generations {
            stream,
            device: 0,
            global: 0,
        }
    }

    fn values(batch: &SampleBatch) -> Vec<f64> {
        match batch.schema()[0].values() {
            ColumnArray::F64(v) => v.to_vec(),
            other => panic!("expected an f64 column, got {other:?}"),
        }
    }

    #[test]
    fn compatible_batches_merge_into_one() {
        let segment = segment(0);
        let mut coalescer = BatchCoalescer::new(Some(8));
        for first in [0, 2, 4] {
            coalescer.push_batch(&batch(&segment, first, 2, None, generations(1)));
        }
        assert!(coalescer.next_completed_batch().is_none());
        assert_eq!(coalescer.buffered_len(), 6);

        coalescer.finish_buffered_batch();
        let merged = coalescer.next_completed_batch().expect("the merged batch");
        assert_eq!(merged.sample_numbers(), [0, 1, 2, 3, 4, 5]);
        assert_eq!(values(&merged), [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(merged.timestamps(), [0.25, 0.5, 0.75, 1.0, 1.25, 1.5]);
        assert!(coalescer.next_completed_batch().is_none());
    }

    #[test]
    fn reaching_the_target_completes_a_batch() {
        let segment = segment(0);
        let mut coalescer = BatchCoalescer::new(Some(4));
        coalescer.push_batch(&batch(&segment, 0, 3, None, generations(1)));
        assert!(coalescer.next_completed_batch().is_none());
        // The target is approximate: rows arriving as a batch stay together.
        coalescer.push_batch(&batch(&segment, 3, 3, None, generations(1)));
        let completed = coalescer.next_completed_batch().expect("the full batch");
        assert_eq!(completed.len(), 6);
        assert_eq!(coalescer.buffered_len(), 0);
    }

    #[test]
    fn without_a_target_every_batch_completes_on_its_own() {
        let segment = segment(0);
        let mut coalescer = BatchCoalescer::new(None);
        coalescer.push_batch(&batch(&segment, 0, 2, None, generations(1)));
        coalescer.push_batch(&batch(&segment, 2, 2, None, generations(1)));
        assert_eq!(coalescer.buffered_len(), 0);
        let lengths: Vec<usize> = std::iter::from_fn(|| coalescer.next_completed_batch())
            .map(|b| b.len())
            .collect();
        assert_eq!(lengths, [2, 2]);
    }

    #[test]
    fn a_boundary_batch_stands_alone() {
        let segment = segment(0);
        let mut coalescer = BatchCoalescer::new(Some(8));
        coalescer.push_batch(&batch(&segment, 0, 2, None, generations(1)));
        coalescer.push_batch(&batch(
            &segment,
            2,
            2,
            Some(Boundary {
                reason: BoundaryReason::SamplesLost {
                    expected: twinleaf_proto::SampleNumber::new(2),
                    received: twinleaf_proto::SampleNumber::new(2),
                },
            }),
            generations(1),
        ));
        coalescer.push_batch(&batch(&segment, 4, 2, None, generations(1)));

        // The rows before the boundary complete first, then the boundary batch
        // itself; only the rows after it stay buffered.
        let held = coalescer
            .next_completed_batch()
            .expect("the pre-boundary rows");
        assert_eq!(held.sample_numbers(), [0, 1]);
        assert!(held.boundary().is_none());
        let boundary = coalescer
            .next_completed_batch()
            .expect("the boundary batch");
        assert_eq!(boundary.sample_numbers(), [2, 3]);
        assert!(boundary.boundary().is_some());
        assert!(coalescer.next_completed_batch().is_none());
        assert_eq!(coalescer.buffered_len(), 2);
    }

    #[test]
    fn differing_generations_complete_the_buffered_rows() {
        let segment = segment(0);
        let mut coalescer = BatchCoalescer::new(Some(8));
        coalescer.push_batch(&batch(&segment, 0, 2, None, generations(1)));
        coalescer.push_batch(&batch(&segment, 0, 2, None, generations(2)));
        let completed = coalescer.next_completed_batch().expect("the older rows");
        assert_eq!(completed.generations(), generations(1));
        assert_eq!(coalescer.buffered_generations(), Some(generations(2)));
    }

    #[test]
    fn a_different_segment_completes_the_buffered_rows() {
        let mut coalescer = BatchCoalescer::new(Some(8));
        coalescer.push_batch(&batch(&segment(0), 0, 2, None, generations(1)));
        coalescer.push_batch(&batch(&segment(1), 2, 2, None, generations(1)));
        assert_eq!(
            coalescer
                .next_completed_batch()
                .expect("the first segment's rows")
                .sample_numbers(),
            [0, 1]
        );
        assert_eq!(coalescer.buffered_len(), 2);
    }

    #[test]
    fn empty_batches_are_ignored() {
        let segment = segment(0);
        let mut coalescer = BatchCoalescer::new(Some(4));
        coalescer.push_batch(&batch(&segment, 0, 0, None, generations(1)));
        assert_eq!(coalescer.buffered_len(), 0);
        assert!(coalescer.next_completed_batch().is_none());
    }

    #[test]
    fn frozen_tail_rows_stay_buffered_and_independent() {
        let segment = segment(0);
        let mut coalescer = BatchCoalescer::new(Some(8));
        assert!(coalescer.buffered_rows(0..0).is_none());
        coalescer.push_batch(&batch(&segment, 0, 3, None, generations(1)));

        let frozen = coalescer.buffered_rows(1..3).expect("the buffered rows");
        assert_eq!(frozen.sample_numbers(), [1, 2]);
        assert_eq!(values(&frozen), [1.0, 2.0]);
        assert_eq!(coalescer.buffered_len(), 3);

        coalescer.push_batch(&batch(&segment, 3, 2, None, generations(1)));
        coalescer.finish_buffered_batch();
        let completed = coalescer.next_completed_batch().expect("every row");
        assert_eq!(completed.sample_numbers(), [0, 1, 2, 3, 4]);
        // The frozen rows are a batch of their own, unaffected by the rows
        // that followed them.
        assert_eq!(frozen.sample_numbers(), [1, 2]);
    }
}

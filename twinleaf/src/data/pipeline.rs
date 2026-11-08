//! Incremental per-column computation driven by a [`Buffer`].

use super::buffer::{Buffer, Run};
use super::continuity::Generations;
use super::metadata::ColumnKey;
use super::sample::ColumnArray;
use std::ops::Range;

/// What a [`ColumnOp`] needs of the rows it is fed between two resets.
///
/// A [`Run`] keeps time monotonic but may miss rows. A processor resets a
/// [`Continuous`](Continuity::Continuous) op at each such gap, so the op
/// only ever sees an unbroken stretch of samples. It feeds a
/// [`Monotonic`](Continuity::Monotonic) op across gaps, so the op sees
/// every retained row of the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continuity {
    /// No missing rows, so evenly spaced samples, as a filter, a spectral
    /// estimate or a sample counter needs.
    Continuous,
    /// Time that never moves backward, rows possibly missing, as a plot
    /// decimator, a running extreme or a time-windowed statistic needs.
    Monotonic,
}

/// An incremental computation over one column's sample stream, such as
/// decimation or a Welch periodogram.
///
/// Implementations are stateful. They are fed every sample exactly once, in
/// order, and never a span that crosses a gap in the run.
pub trait ColumnOp {
    /// What the operation accumulates and hands back.
    type Output;
    /// What the op needs of the rows it is fed between resets. The default,
    /// [`Continuity::Continuous`], resets it at every gap.
    fn continuity(&self) -> Continuity {
        Continuity::Continuous
    }
    /// Discards all state, as at a new run or after the op's parameters
    /// change.
    fn reset(&mut self);
    /// Consumes the next contiguous span of one run's rows. `timestamps` and
    /// `values` have the same length and line up row by row.
    fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray);
    /// Returns the current result, reflecting every row fed so far.
    fn output(&self) -> &Self::Output;
}

/// Drives a [`ColumnOp`] from a [`Buffer`], feeding only new rows and replaying
/// retained history after a run restart or retention overrun.
#[derive(Debug)]
pub struct ColumnProcessor<Op: ColumnOp> {
    key: ColumnKey,
    op: Op,
    /// Stamps of the run being followed. A different `stream` generation is a
    /// different run.
    cursor: Option<Generations>,
    next_row: u64,
}

impl<Op: ColumnOp> ColumnProcessor<Op> {
    /// Creates a processor that drives `op` from the column at `key`.
    pub fn new(key: ColumnKey, op: Op) -> Self {
        Self {
            key,
            op,
            cursor: None,
            next_row: 0,
        }
    }

    /// Returns the column this processor follows.
    pub fn key(&self) -> &ColumnKey {
        &self.key
    }

    /// Feeds the op every retained row it has not seen and returns its output.
    ///
    /// The op is reset and fed every retained row again after a new run, after
    /// rows it had not seen were evicted, or after
    /// [`invalidate`](Self::invalidate). A
    /// [`Continuous`](Continuity::Continuous) op is also reset at each gap in
    /// the run, and a replay feeds it only the rows after the last one. A call
    /// with no new data feeds nothing.
    pub fn catch_up(&mut self, buffer: &Buffer) -> &Op::Output {
        let Some(run) = buffer.run(&self.key.stream_key()) else {
            return self.op.output();
        };
        let retained = run.retained_rows();

        let needs_replay = self
            .cursor
            .is_none_or(|c| c.stream != run.generations().stream)
            || self.next_row < retained.start
            || self.next_row > retained.end;
        let rows = if needs_replay {
            self.op.reset();
            self.cursor = Some(run.generations());
            let from = match self.op.continuity() {
                // Rows before the last gap would be reset away.
                Continuity::Continuous => run
                    .boundaries()
                    .map(|(row, _, _)| row)
                    .next_back()
                    .map_or(retained.start, |row| row.max(retained.start)),
                Continuity::Monotonic => retained.start,
            };
            from..retained.end
        } else {
            self.next_row..retained.end
        };

        self.feed(run, rows, needs_replay);
        self.next_row = retained.end;
        self.op.output()
    }

    /// Feeds `rows` to the op, one contiguous span at a time, splitting
    /// spans at the run's gaps. A [`Continuous`](Continuity::Continuous) op
    /// is reset at each gap, except one at the first row when the op was
    /// `fresh`ly reset.
    fn feed(&mut self, run: &Run, rows: Range<u64>, fresh: bool) {
        let continuous = self.op.continuity() == Continuity::Continuous;
        let gaps: Vec<u64> = run
            .boundaries()
            .map(|(row, _, _)| row)
            .filter(|&row| row >= rows.start && row < rows.end)
            .collect();
        let mut gaps = gaps.into_iter().peekable();
        let mut at = rows.start;
        for span in run.row_spans(rows.clone()) {
            let end = at + span.len() as u64;
            let mut from = 0usize;
            while let Some(&gap) = gaps.peek().filter(|&&g| g < end) {
                gaps.next();
                let split = (gap - at) as usize;
                if split > from {
                    self.feed_span(&span.slice(from..split));
                }
                if continuous && !(fresh && gap == rows.start) {
                    self.op.reset();
                }
                from = split;
            }
            if from < span.len() {
                self.feed_span(&span.slice(from..span.len()));
            }
            at = end;
        }
    }

    fn feed_span(&mut self, span: &super::sample::SampleBatch) {
        if let Some(values) = span.column(self.key.column_id) {
            self.op.update_batch(span.timestamps(), values);
        }
    }

    /// Makes the next [`catch_up`](Self::catch_up) reset the op and feed it
    /// every retained row again. Call it after changing the op's parameters.
    pub fn invalidate(&mut self) {
        self.cursor = None;
    }

    /// Returns the operation being driven.
    pub fn op(&self) -> &Op {
        &self.op
    }

    /// Returns the operation's current output without feeding it any new
    /// rows.
    pub fn output(&self) -> &Op::Output {
        self.op.output()
    }

    /// Returns the operation for changing its parameters. After a change, call
    /// [`Self::invalidate`] and the next [`Self::catch_up`] rebuilds the op's
    /// state from the retained rows.
    pub fn op_mut(&mut self) -> &mut Op {
        &mut self.op
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::metadata::{DeviceRecord, SegmentRecord, StreamMetadataSnapshot};
    use crate::data::sample::{BatchContext, ColumnData, SampleBatchBuilder};
    use crate::data::tests::fixtures;
    use crate::proto::data as wire;
    use crate::proto::data::DataType;
    use crate::proto::DeviceRoute;

    /// Records every `(timestamp, value)` it is pushed, in push order. The
    /// simplest possible [`ColumnOp`], used to assert exactly-once,
    /// in-order delivery from [`ColumnProcessor`].
    #[derive(Default)]
    struct Recorder {
        samples: Vec<(f64, f64)>,
        reset_count: usize,
    }

    impl ColumnOp for Recorder {
        type Output = Vec<(f64, f64)>;

        fn reset(&mut self) {
            self.samples.clear();
            self.reset_count += 1;
        }

        fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray) {
            let ColumnArray::F64(values) = values else {
                panic!("expected f64 column, got {values:?}");
            };
            self.samples
                .extend(timestamps.iter().copied().zip(values.iter().copied()));
        }

        fn output(&self) -> &Self::Output {
            &self.samples
        }
    }

    struct Fixture {
        column_key: ColumnKey,
        metadata: StreamMetadataSnapshot,
    }

    fn fixture() -> Fixture {
        let route = DeviceRoute::root();
        let stream_id = 1;
        let column_id = crate::proto::ColumnId::new(0);

        let metadata = fixtures::metadata(
            route,
            fixtures::device(),
            wire::Stream {
                sample_size: 0,
                buf_samples: 1024,
                ..fixtures::stream(stream_id)
            },
            fixtures::segment(stream_id),
            &[fixtures::column(
                stream_id,
                column_id.value(),
                DataType::F64,
            )],
        );
        let column_key = ColumnKey::new(route, crate::proto::StreamId::new(stream_id), column_id);

        Fixture {
            column_key,
            metadata,
        }
    }

    impl Fixture {
        /// Push one batch of `f64` rows, with an explicit stream generation and
        /// explicit sample numbers so runs can be restarted deliberately.
        fn push(&self, buffer: &mut Buffer, stream_generation: u32, rows: &[(u32, f64)]) {
            let segment = self.metadata.segment_record().clone();
            self.push_in_segment(buffer, stream_generation, segment, rows);
        }

        fn push_in_segment(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            segment: SegmentRecord,
            rows: &[(u32, f64)],
        ) {
            let mut builder = SampleBatchBuilder::new(
                BatchContext {
                    metadata: StreamMetadataSnapshot::new(
                        self.metadata.schema().clone(),
                        DeviceRecord::encode(fixtures::device()).unwrap(),
                        segment,
                    ),
                    boundary: None,
                    generations: Generations {
                        stream: stream_generation,
                        device: 0,
                        global: 0,
                    },
                },
                rows.len(),
            );
            for &(sample_number, value) in rows {
                builder.push_row(
                    crate::proto::SampleNumber::new(sample_number),
                    [ColumnData::F64(value)],
                );
            }
            let batch = builder.finish();
            buffer.process_batch(&batch);
        }

        /// Push a contiguous run of samples numbered `0..values.len()`.
        fn push_contiguous(&self, buffer: &mut Buffer, values: &[f64]) {
            let rows: Vec<_> = values
                .iter()
                .enumerate()
                .map(|(i, &v)| (i as u32, v))
                .collect();
            self.push(buffer, 1, &rows);
        }
    }

    #[test]
    fn catch_up_feeds_each_sample_exactly_once_across_interleaved_batches() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut processor = ColumnProcessor::new(fx.column_key, Recorder::default());

        fx.push(&mut buffer, 1, &[(0, 0.0), (1, 1.0)]);
        let out = processor.catch_up(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0)]);

        fx.push(&mut buffer, 1, &[(2, 2.0), (3, 3.0)]);
        let out = processor.catch_up(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0), (3.0, 2.0), (4.0, 3.0)]);

        fx.push(&mut buffer, 1, &[(4, 4.0)]);
        fx.push(&mut buffer, 1, &[(5, 5.0), (6, 6.0)]);
        let out = processor.catch_up(&buffer).clone();
        assert_eq!(
            out,
            vec![
                (1.0, 0.0),
                (2.0, 1.0),
                (3.0, 2.0),
                (4.0, 3.0),
                (5.0, 4.0),
                (6.0, 5.0),
                (7.0, 6.0),
            ]
        );
        assert_eq!(
            processor.op().reset_count,
            1,
            "only the initial hydrate should reset"
        );
    }

    #[test]
    fn catch_up_uses_row_order_across_reused_segment_sample_numbers() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut processor = ColumnProcessor::new(fx.column_key, Recorder::default());

        fx.push(&mut buffer, 1, &[(0, 0.0), (1, 1.0)]);
        processor.catch_up(&buffer);

        let rolled = |segment_id, start_time| {
            SegmentRecord::encode(wire::Segment {
                segment_id,
                start_time,
                ..fx.metadata.segment().fields()
            })
            .unwrap()
        };
        // A seamless rollover keeps the stream generation, so the run continues.
        fx.push_in_segment(
            &mut buffer,
            1,
            rolled(crate::proto::SegmentId::new(1), 2),
            &[(0, 2.0), (1, 3.0)],
        );
        fx.push_in_segment(
            &mut buffer,
            1,
            rolled(crate::proto::SegmentId::new(0), 4),
            &[(0, 4.0), (1, 5.0)],
        );

        let out = processor.catch_up(&buffer).clone();
        assert_eq!(
            out,
            vec![
                (1.0, 0.0),
                (2.0, 1.0),
                (3.0, 2.0),
                (4.0, 3.0),
                (5.0, 4.0),
                (6.0, 5.0),
            ]
        );
        assert_eq!(
            processor.op().reset_count,
            1,
            "continuous segment rollovers must not reset the operation"
        );
    }

    #[test]
    fn falling_behind_retention_resets_and_replays_retained_rows() {
        let fx = fixture();
        let mut buffer = Buffer::new(3);
        let mut processor = ColumnProcessor::new(fx.column_key, Recorder::default());

        fx.push(&mut buffer, 1, &[(0, 0.0), (1, 1.0)]);
        processor.catch_up(&buffer);

        fx.push(&mut buffer, 1, &[(2, 2.0), (3, 3.0), (4, 4.0), (5, 5.0)]);
        let out = processor.catch_up(&buffer).clone();

        assert_eq!(out, vec![(4.0, 3.0), (5.0, 4.0), (6.0, 5.0)]);
        assert_eq!(processor.op().reset_count, 2);
    }

    #[test]
    fn run_restart_resets_and_rehydrates_with_only_new_run_samples() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut processor = ColumnProcessor::new(fx.column_key, Recorder::default());

        fx.push_contiguous(&mut buffer, &[0.0, 1.0, 2.0]);
        let out = processor.catch_up(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0), (3.0, 2.0)]);

        // A bumped stream generation starts a new run.
        fx.push(&mut buffer, 2, &[(0, 10.0), (1, 11.0)]);

        let out = processor.catch_up(&buffer).clone();
        assert_eq!(
            out,
            vec![(1.0, 10.0), (2.0, 11.0)],
            "rehydrate must discard the old run's samples entirely"
        );
        assert_eq!(
            processor.op().reset_count,
            2,
            "restart triggers a second reset"
        );
    }

    #[test]
    fn invalidate_forces_a_rehydrate() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut processor = ColumnProcessor::new(fx.column_key, Recorder::default());

        fx.push_contiguous(&mut buffer, &[0.0, 1.0, 2.0]);
        processor.catch_up(&buffer);
        assert_eq!(processor.op().reset_count, 1);

        // No new data arrived, but the caller changed op parameters.
        processor.invalidate();
        let out = processor.catch_up(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0), (3.0, 2.0)]);
        assert_eq!(
            processor.op().reset_count,
            2,
            "invalidate must force a reset"
        );
    }

    #[test]
    fn catch_up_with_no_new_data_pushes_nothing() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut processor = ColumnProcessor::new(fx.column_key, Recorder::default());

        fx.push_contiguous(&mut buffer, &[0.0, 1.0]);
        let out = processor.catch_up(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0)]);
        let reset_count_before = processor.op().reset_count;

        // Catch up again with the buffer unchanged.
        let out2 = processor.catch_up(&buffer).clone();
        assert_eq!(out2, out, "no new samples means no new pushes");
        assert_eq!(
            processor.op().reset_count,
            reset_count_before,
            "a no-op catch-up must not reset"
        );
    }

    #[test]
    fn catch_up_before_any_data_returns_default_output() {
        let fx = fixture();
        let buffer = Buffer::new(64);
        let mut processor = ColumnProcessor::new(fx.column_key, Recorder::default());
        assert!(processor.catch_up(&buffer).is_empty());
    }
}

//! Incremental per-column computation driven by a [`Buffer`].

use super::buffer::{Buffer, Run};
use super::sample::{ColumnArray, ColumnKey, Generations};
use std::ops::Range;

/// An incremental computation over one column's sample stream, such as
/// decimation or a Welch periodogram.
///
/// Implementations are stateful; fed every sample exactly once, in order.
pub trait ColumnOp {
    /// What the operation accumulates and hands back.
    type Output;
    /// Discard all state (new run, or view parameters changed).
    fn reset(&mut self);
    /// Consume the next contiguous span of one run's rows: `timestamps` and
    /// `values` are the same length and line up row by row.
    fn update_batch(&mut self, timestamps: &[f64], values: &ColumnArray);
    /// The current result, reflecting every row fed so far.
    fn output(&self) -> &Self::Output;
}

/// Drives a [`ColumnOp`] from a [`Buffer`], feeding only new rows and replaying
/// retained history after a run restart or retention overrun.
pub struct ColumnProcessor<Op: ColumnOp> {
    key: ColumnKey,
    op: Op,
    /// Stamps of the run being followed; a different `stream` generation is a
    /// different run.
    cursor: Option<Generations>,
    next_row: u64,
}

impl<Op: ColumnOp> ColumnProcessor<Op> {
    /// Drive `op` from the column at `key`.
    pub fn new(key: ColumnKey, op: Op) -> Self {
        Self {
            key,
            op,
            cursor: None,
            next_row: 0,
        }
    }

    /// The column this processor follows.
    pub fn key(&self) -> &ColumnKey {
        &self.key
    }

    /// Feed all retained rows this operation has not seen. Calling twice with
    /// no new data pushes nothing.
    pub fn catch_up(&mut self, buffer: &Buffer) -> &Op::Output {
        let Some(run) = buffer.get_run(&self.key.stream_key()) else {
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
            retained.clone()
        } else {
            self.next_row..retained.end
        };

        self.feed(run, rows);
        self.next_row = retained.end;
        self.op.output()
    }

    /// Push `rows` to the op, one contiguous span at a time.
    fn feed(&mut self, run: &Run, rows: Range<u64>) {
        for span in run.row_spans(rows) {
            if let Some(column) = span.column(self.key.column_id) {
                self.op.update_batch(span.timestamps(), column.values());
            }
        }
    }

    /// Force reset + replay on the next catch-up (e.g. op parameters changed).
    pub fn invalidate(&mut self) {
        self.cursor = None;
    }

    /// The operation being driven.
    pub fn op(&self) -> &Op {
        &self.op
    }

    /// The operation's current output, without feeding it any new rows.
    pub fn output(&self) -> &Op::Output {
        self.op.output()
    }

    /// Caller must call [`Self::invalidate`] after mutating op parameters so
    /// the next [`Self::catch_up`] rebuilds state from scratch.
    pub fn op_mut(&mut self) -> &mut Op {
        &mut self.op
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::fixtures;
    use crate::data::metadata::{
        buffer_type, ColumnRecord, DeviceRecord, SegmentRecord, StreamRecord,
    };
    use crate::data::sample::{BatchContext, ColumnData, SampleBatchBuilder};
    use crate::tio::proto::{DataType, DeviceRoute};
    use twinleaf_proto::data as wire;

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
        column_metadata: ColumnRecord,
        device: DeviceRecord,
        stream: StreamRecord,
        segment: SegmentRecord,
    }

    fn fixture() -> Fixture {
        let route = DeviceRoute::root();
        let stream_id = 1;
        let column_id = twinleaf_proto::ColumnId::new(0);

        let device = DeviceRecord::encode(fixtures::device()).unwrap();
        let stream = StreamRecord::encode(wire::Stream {
            sample_size: 0,
            buf_samples: 1024,
            ..fixtures::stream(stream_id)
        })
        .unwrap();
        let segment = SegmentRecord::encode(fixtures::segment(stream_id)).unwrap();
        let column_metadata = ColumnRecord::encode(fixtures::column(
            stream_id,
            column_id.value(),
            DataType::F64,
        ))
        .unwrap();
        let column_key = ColumnKey::new(route, twinleaf_proto::StreamId::new(stream_id), column_id);

        Fixture {
            column_key,
            column_metadata,
            device,
            stream,
            segment,
        }
    }

    impl Fixture {
        /// Push one batch of `f64` rows, with an explicit stream generation and
        /// explicit sample numbers so runs can be restarted deliberately.
        fn push(&self, buffer: &mut Buffer, stream_generation: u32, rows: &[(u32, f64)]) {
            self.push_in_segment(buffer, stream_generation, self.segment.clone(), rows);
        }

        fn push_in_segment(
            &self,
            buffer: &mut Buffer,
            stream_generation: u32,
            segment: SegmentRecord,
            rows: &[(u32, f64)],
        ) {
            let mut builder = SampleBatchBuilder::new(
                BatchContext::new(
                    self.column_key.stream_key(),
                    None,
                    Generations {
                        stream: stream_generation,
                        device: 0,
                        global: 0,
                    },
                    segment,
                    self.stream.clone(),
                    self.device.clone(),
                ),
                [(
                    self.column_metadata.clone(),
                    buffer_type(self.column_metadata.get().data_type),
                )],
                rows.len(),
            );
            for &(sample_number, value) in rows {
                builder.push_row(
                    twinleaf_proto::SampleNumber::new(sample_number),
                    [ColumnData::Float(value)],
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
                ..fx.segment.get()
            })
            .unwrap()
        };
        // A seamless rollover keeps the stream generation, so the run continues.
        fx.push_in_segment(
            &mut buffer,
            1,
            rolled(twinleaf_proto::SegmentId::new(1), 2),
            &[(0, 2.0), (1, 3.0)],
        );
        fx.push_in_segment(
            &mut buffer,
            1,
            rolled(twinleaf_proto::SegmentId::new(0), 4),
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

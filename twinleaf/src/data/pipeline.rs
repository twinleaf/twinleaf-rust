//! Incremental per-column computation driven by a [`Buffer`].
//!
//! [`ColumnOp`] is what app crates implement for stateful streaming
//! computations (decimation, Welch, ...). [`DerivedColumn`] pairs an op with
//! a cursor into a [`Buffer`]: [`DerivedColumn::sync`] feeds it exactly the
//! samples it hasn't seen yet, and transparently resets + replays available
//! history whenever the run restarts or the cursor falls out of the ring.

use crate::data::{Buffer, ColumnKey, ColumnWindow, RunId};
use crate::tio::proto::identifiers::SampleNumber;

/// An incremental computation over one column's sample stream.
/// Implementations are stateful; fed every sample exactly once, in order.
pub trait ColumnOp {
    type Output;
    /// Discard all state (new run, or view parameters changed).
    fn reset(&mut self);
    /// Consume the next contiguous chunk of samples.
    fn push(&mut self, chunk: &ColumnWindow);
    fn output(&self) -> &Self::Output;
}

/// Drives a [`ColumnOp`] from a [`Buffer`]: tracks a `(RunId, SampleNumber)`
/// cursor, feeds only new samples on sync, and hydrates (reset + replay
/// available history) on run restart or when the cursor has fallen off the
/// ring.
pub struct DerivedColumn<O: ColumnOp> {
    key: ColumnKey,
    op: O,
    cursor: Option<(RunId, SampleNumber)>,
}

impl<O: ColumnOp> DerivedColumn<O> {
    pub fn new(key: ColumnKey, op: O) -> Self {
        Self {
            key,
            op,
            cursor: None,
        }
    }

    pub fn key(&self) -> &ColumnKey {
        &self.key
    }

    /// Catch up with the buffer. Idempotent: calling twice with no new data
    /// pushes nothing. Returns the op output for convenience.
    pub fn sync(&mut self, buffer: &Buffer) -> &O::Output {
        let Some(run) = buffer.get_run(&self.key.stream_key()) else {
            return self.op.output();
        };
        let run_id = run.run_id;
        let last_sample_number = run.last_sample_number;

        let needs_hydrate = match self.cursor {
            None => true,
            Some((cursor_run, _)) => cursor_run != run_id,
        };

        if needs_hydrate {
            self.rehydrate(buffer, run_id, last_sample_number);
            return self.op.output();
        }

        let (_, after) = self.cursor.expect("checked by needs_hydrate above");
        match buffer.column_window_after(&self.key, run_id, after) {
            Ok(Some(chunk)) => {
                self.op.push(&chunk);
                self.cursor = Some((run_id, last_sample_number));
            }
            Ok(None) => {
                // Already caught up; nothing to push.
            }
            Err(_) => {
                // The cursor fell out of retention (or the run changed
                // underneath us, in a race with a concurrent writer). Reset
                // and replay whatever history is still available.
                self.rehydrate(buffer, run_id, last_sample_number);
            }
        }

        self.op.output()
    }

    /// Force reset + rehydrate on next sync (e.g. op parameters changed).
    pub fn invalidate(&mut self) {
        self.cursor = None;
    }

    pub fn op(&self) -> &O {
        &self.op
    }

    /// Caller must call [`Self::invalidate`] after mutating op parameters so
    /// the next [`Self::sync`] rebuilds state from scratch.
    pub fn op_mut(&mut self) -> &mut O {
        &mut self.op
    }

    fn rehydrate(&mut self, buffer: &Buffer, run_id: RunId, last_sample_number: SampleNumber) {
        self.op.reset();
        if let Some(window) = buffer.column_window_last_n(&self.key, usize::MAX) {
            self.op.push(&window);
        }
        self.cursor = Some((run_id, last_sample_number));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Boundary, BoundaryReason, ColumnData, ColumnVec, SampleBatch, Series};
    use crate::tio::proto::identifiers::{ColumnId, StreamKey};
    use crate::tio::proto::meta::{
        ColumnMetadata, DeviceMetadata, MetadataEpoch, MetadataFilter, SegmentMetadata,
        StreamMetadata,
    };
    use crate::tio::proto::{DataType, DeviceRoute};
    use std::sync::Arc;

    /// Records every `(timestamp, value)` it is pushed, in push order. The
    /// simplest possible [`ColumnOp`], used to assert exactly-once,
    /// in-order delivery from [`DerivedColumn`].
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

        fn push(&mut self, chunk: &ColumnWindow) {
            let (ta, tb) = chunk.timestamps;
            let timestamps = ta.iter().chain(tb.iter()).copied();
            let values = match chunk.values.to_owned() {
                ColumnVec::F64(v) => v,
                other => panic!("expected f64 column, got {other:?}"),
            };
            self.samples.extend(timestamps.zip(values));
        }

        fn output(&self) -> &Self::Output {
            &self.samples
        }
    }

    struct Fixture {
        stream_key: StreamKey,
        column_key: ColumnKey,
        column_metadata: Arc<ColumnMetadata>,
        device: Arc<DeviceMetadata>,
        stream: Arc<StreamMetadata>,
        segment: Arc<SegmentMetadata>,
    }

    fn fixture() -> Fixture {
        let route = DeviceRoute::root();
        let stream_id = 1;
        let stream_key = StreamKey::new(route, stream_id);
        let column_id: ColumnId = 0;

        let device = Arc::new(DeviceMetadata {
            serial_number: "SN123".to_string(),
            firmware_hash: "fw".to_string(),
            n_streams: 1,
            session_id: 42,
            name: "test-device".to_string(),
        });
        let stream = Arc::new(StreamMetadata {
            stream_id,
            name: "test-stream".to_string(),
            n_columns: 1,
            n_segments: 1,
            sample_size: 0,
            buf_samples: 1024,
        });
        let segment = Arc::new(SegmentMetadata {
            stream_id,
            segment_id: 0,
            flags: 0,
            time_ref_epoch: MetadataEpoch::Unix,
            time_ref_serial: "clock".to_string(),
            time_ref_session_id: 7,
            start_time: 0,
            sampling_rate: 1,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: MetadataFilter::Unfiltered,
        });
        let column_metadata = Arc::new(ColumnMetadata {
            stream_id,
            index: column_id,
            data_type: DataType::Float64,
            name: "col_0".to_string(),
            units: String::new(),
            description: String::new(),
        });
        let column_key = ColumnKey::new(route, stream_id, column_id);

        Fixture {
            stream_key,
            column_key,
            column_metadata,
            device,
            stream,
            segment,
        }
    }

    impl Fixture {
        /// Push one batch of `f64` rows, with an explicit boundary and
        /// explicit sample numbers so runs can be restarted deliberately.
        fn push(
            &self,
            buffer: &mut Buffer,
            boundary: Option<Boundary>,
            rows: &[(SampleNumber, f64)],
        ) {
            let sample_numbers: Vec<SampleNumber> = rows.iter().map(|(n, _)| *n).collect();
            let mut values = ColumnVec::empty_for(self.column_metadata.data_type.buffer_type());
            for (_, v) in rows {
                values.push_data(&ColumnData::Float(*v));
            }
            let series = vec![Series {
                index: self.column_key.column_id,
                metadata: self.column_metadata.clone(),
                values,
            }];
            let batch = SampleBatch::new(
                DeviceRoute::root(),
                boundary,
                sample_numbers,
                series,
                self.segment.clone(),
                self.stream.clone(),
                self.device.clone(),
            );
            buffer.process_batch(&batch, self.stream_key);
        }

        /// Push a contiguous run of samples numbered `0..values.len()`.
        fn push_contiguous(&self, buffer: &mut Buffer, values: &[f64]) {
            let rows: Vec<_> = values
                .iter()
                .enumerate()
                .map(|(i, &v)| (i as SampleNumber, v))
                .collect();
            self.push(buffer, None, &rows);
        }
    }

    #[test]
    fn sync_feeds_each_sample_exactly_once_across_interleaved_batches() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut derived = DerivedColumn::new(fx.column_key, Recorder::default());

        fx.push(&mut buffer, None, &[(0, 0.0), (1, 1.0)]);
        let out = derived.sync(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0)]);

        fx.push(&mut buffer, None, &[(2, 2.0), (3, 3.0)]);
        let out = derived.sync(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0), (3.0, 2.0), (4.0, 3.0)]);

        fx.push(&mut buffer, None, &[(4, 4.0)]);
        fx.push(&mut buffer, None, &[(5, 5.0), (6, 6.0)]);
        let out = derived.sync(&buffer).clone();
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
            derived.op().reset_count,
            1,
            "only the initial hydrate should reset"
        );
    }

    #[test]
    fn run_restart_resets_and_rehydrates_with_only_new_run_samples() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut derived = DerivedColumn::new(fx.column_key, Recorder::default());

        fx.push_contiguous(&mut buffer, &[0.0, 1.0, 2.0]);
        let out = derived.sync(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0), (3.0, 2.0)]);

        // A discontinuous boundary starts a new run.
        fx.push(
            &mut buffer,
            Some(Boundary {
                reason: BoundaryReason::SegmentChanged {
                    old_id: 0,
                    new_id: 1,
                },
            }),
            &[(0, 10.0), (1, 11.0)],
        );

        let out = derived.sync(&buffer).clone();
        assert_eq!(
            out,
            vec![(1.0, 10.0), (2.0, 11.0)],
            "rehydrate must discard the old run's samples entirely"
        );
        assert_eq!(
            derived.op().reset_count,
            2,
            "restart triggers a second reset"
        );
    }

    #[test]
    fn invalidate_forces_a_rehydrate() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut derived = DerivedColumn::new(fx.column_key, Recorder::default());

        fx.push_contiguous(&mut buffer, &[0.0, 1.0, 2.0]);
        derived.sync(&buffer);
        assert_eq!(derived.op().reset_count, 1);

        // No new data arrived, but the caller changed op parameters.
        derived.invalidate();
        let out = derived.sync(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0), (3.0, 2.0)]);
        assert_eq!(derived.op().reset_count, 2, "invalidate must force a reset");
    }

    #[test]
    fn sync_with_no_new_data_pushes_nothing() {
        let fx = fixture();
        let mut buffer = Buffer::new(64);
        let mut derived = DerivedColumn::new(fx.column_key, Recorder::default());

        fx.push_contiguous(&mut buffer, &[0.0, 1.0]);
        let out = derived.sync(&buffer).clone();
        assert_eq!(out, vec![(1.0, 0.0), (2.0, 1.0)]);
        let reset_count_before = derived.op().reset_count;

        // Sync again with the buffer unchanged.
        let out2 = derived.sync(&buffer).clone();
        assert_eq!(out2, out, "no new samples means no new pushes");
        assert_eq!(
            derived.op().reset_count,
            reset_count_before,
            "a no-op sync must not reset"
        );
    }

    #[test]
    fn sync_before_any_data_returns_default_output() {
        let fx = fixture();
        let buffer = Buffer::new(64);
        let mut derived = DerivedColumn::new(fx.column_key, Recorder::default());
        assert!(derived.sync(&buffer).is_empty());
    }
}
